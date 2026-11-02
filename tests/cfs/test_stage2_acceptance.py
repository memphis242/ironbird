"""Stage 2 acceptance against a real cFS instance: record, replay, export and repair.

Two scenarios, both driven only through the ``ib`` CLI against a cFS power-on started by this
module (``ib_test`` among its apps):

* ``test_the_whole_stage2_scenario``: ``ib record --duration 60`` runs while TO_LAB output is
  enabled, SAMPLE_APP gets K NOOPs, a RESET_COUNTERS and K more, and TIME gets commands. Then the
  recording is clean with a contiguous sequence and per-MID counts that match SCH_LAB's periods,
  ``ib replay --where`` finds exactly 2K NOOP events, and ``ib export`` of ``SAMPLE_APP_HK`` shows
  the counter rising by K, resetting and rising by K with as many rows as ``ib rec info`` counts.
* ``test_a_recorder_killed_mid_capture_leaves_a_recording_that_replays_and_repairs``: a second
  recorder, fed 1000 ``ib_test`` packets a second, is killed with SIGKILL. The torn tail is
  reported, replay plays up to it and ``ib rec repair`` writes exactly the readable prefix.

Every wait is for a condition the recording or the process shows, with a deadline; none sleeps to
let a race pass. Opt-in (marker ``cfs``), see ``tests/conftest.py``.
"""

from __future__ import annotations

import csv
import json
import os
import signal
import subprocess
import sys
import time
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from test_acceptance import (
    CONFIG,
    NOOP_EVENT,
    RECORDED_TIMEOUT_S,
    REPO,
    Ground,
    _artifact_safe_paths,
    _replayed,
    _runs,
    _tx_commands,
    ground,
    ground_session,
)
from test_cfs_variable_length import IB_TEST_VAR_MID, cfs

from conftest import without_host_warnings
from ironbird.errors import Code
from ironbird.recording.reader import RecordingReader

pytestmark = pytest.mark.cfs

# Fixtures of the shared modules; pytest finds them by name here.
SHARED_FIXTURES = (_artifact_safe_paths, cfs, ground, ground_session)

K = 3  # NOOPs before and after the reset
RECORD_SECONDS = 60
IB_TEST_GEN_MID = IB_TEST_VAR_MID + 1  # 0x08E2
GEN_RATE_PPS = 1000
CUT_BYTES = 100  # less than one frame record: the cut copy ends inside a record
KILL_AFTER_FRAMES = 2500  # recorded before the SIGKILL: a couple of seconds of generator output
# SCH_LAB's periodic housekeeping (tools/cfs/NOTES.md section 3), MID -> period in seconds.
PERIODS_S = {
    0x0800: 4.0,  # ES_HK
    0x0801: 4.2,  # EVS_HK
    0x0805: 4.4,  # TIME_HK
    0x0803: 4.6,  # SB_HK
    0x0804: 4.8,  # TBL_HK
    0x0884: 5.0,  # CI_LAB_HK
    0x0880: 5.2,  # TO_LAB_HK
    0x0883: 5.4,  # SAMPLE_APP_HK
}
# Housekeeping the scenario also asks for on demand, MID -> the request command.
REQUESTED = {0x0883: "SAMPLE_APP_SEND_HK", 0x0805: "TIME_SEND_HK"}
IB = [sys.executable, "-m", "ironbird.cli.main"]


# -- helpers --------------------------------------------------------------------------------


def _recording_config(directory: Path) -> Path:
    """``examples/ironbird.toml`` with absolute definition paths and no free-space floor."""
    examples = REPO / "examples"
    text = CONFIG.read_text().replace('"ccdd/', f'"{examples}/ccdd/')
    text = text.replace('"headers/', f'"{examples}/headers/')
    path = directory / "ironbird.toml"
    path.write_text(text + "\n[recording]\nmin_free_bytes = 0\n")
    return path


def _env(config: Path) -> dict[str, str]:
    return {**os.environ, "IRONBIRD_CONFIG": str(config)}


def _run_ib(env: dict[str, str], *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [*IB, *args], capture_output=True, text=True, env=env, check=False, timeout=120
    )


def _wait_until(
    what: str, condition: Callable[[], bool], *, timeout: float, poll: float = 0.1
) -> None:
    """Return once ``condition()`` holds; fail with ``what`` if it does not within ``timeout``."""
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, f"{what}: not within {timeout:g} s"
        time.sleep(poll)


def _wait_listening(recorder: subprocess.Popen[bytes], err_path: Path) -> None:
    def listening() -> bool:
        assert recorder.poll() is None, err_path.read_text()
        return "listening" in err_path.read_text()

    _wait_until("ib record never reported it was listening", listening, timeout=20)


def _info(env: dict[str, str], recording: Path) -> dict[str, Any]:
    done = _run_ib(env, "rec", "info", str(recording), "--format", "json")
    assert done.returncode == 0, done.stderr
    report: dict[str, Any] = json.loads(done.stdout)
    return report


def _hex_lines(env: dict[str, str], recording: Path) -> tuple[list[str], str]:
    """The frames ``ib replay --format hex`` plays (exit 0 required) and its stderr."""
    done = _run_ib(env, "replay", str(recording), "--format", "hex")
    assert done.returncode == 0, done.stderr
    return done.stdout.splitlines(), done.stderr


class _Requests:
    """Housekeeping requests sent on demand, counted per command, so the per-MID counts can be
    told from SCH_LAB's own periodic ones."""

    def __init__(self, ground: Ground) -> None:
        self.ground = ground
        self.sent: Counter[str] = Counter()

    def until_recorded(
        self,
        env: dict[str, str],
        recording: Path,
        recorder: subprocess.Popen[bytes],
        where: str,
        predicate: Callable[[list[dict[str, Any]]], bool],
        request: str,
    ) -> None:
        """Re-send ``request`` about once a second until the live ``recording``'s records
        selected by ``where`` satisfy ``predicate``."""
        deadline = time.monotonic() + RECORDED_TIMEOUT_S
        next_request = 0.0
        seen: list[dict[str, Any]] = []
        while time.monotonic() < deadline:
            assert recorder.poll() is None, "the recorder ended before the scenario did"
            if time.monotonic() >= next_request:
                self.ground.send(request)
                self.sent[request] += 1
                next_request = time.monotonic() + 1.0
            seen = _replayed(IB, recording, env, where)
            if predicate(seen):
                return
            time.sleep(0.2)
        shown = [(r["name"], r["fields"]) for r in seen[-6:]]
        raise AssertionError(f"{where!r}: the recording never held the expected records: {shown}")


# -- the scenario ---------------------------------------------------------------------------


def test_the_whole_stage2_scenario(
    ground_session: Ground, tmp_path: Path, _isolated_state: Path
) -> None:
    g = ground_session
    assert not g.output_enabled, "this module's cFS has a fresh power-on: no output yet"
    env = _env(_recording_config(tmp_path))
    out = tmp_path / "stage2.ibrec"
    err_path = tmp_path / "record.stderr"
    requests = _Requests(g)

    def hk_counters(records: list[dict[str, Any]]) -> list[int]:
        return _runs([int(r["fields"]["CommandCounter"]) for r in records])

    def recorded_counters(*values: int) -> Callable[[list[dict[str, Any]]], bool]:
        return lambda records: hk_counters(records) == list(values)

    expected = [*range(K + 1), *range(K + 1)]  # 0..K, then the reset, then 1..K again
    with g.listener_paused(), err_path.open("w") as err:  # one process owns UDP 2234
        recorder = subprocess.Popen(
            [*IB, "record", "-o", str(out), "--duration", str(RECORD_SECONDS), "--quiet"],
            stderr=err,
            env=env,
        )
        try:
            _wait_listening(recorder, err_path)
            started = time.monotonic()
            g.enable_output()  # TO_LAB: nothing flows until this
            where = "msg == SAMPLE_APP_HK"
            ask = "SAMPLE_APP_SEND_HK"
            requests.until_recorded(env, out, recorder, where, recorded_counters(0), ask)
            for n in range(1, K + 1):
                g.send("SAMPLE_APP_NOOP")
                requests.until_recorded(
                    env, out, recorder, where, recorded_counters(*range(n + 1)), ask
                )
            g.send("SAMPLE_APP_RESET_COUNTERS")
            requests.until_recorded(
                env, out, recorder, where, recorded_counters(*range(K + 1), 0), ask
            )
            for n in range(1, K + 1):
                g.send("SAMPLE_APP_NOOP")
                requests.until_recorded(
                    env,
                    out,
                    recorder,
                    where,
                    recorded_counters(*range(K + 1), *range(n + 1)),
                    ask,
                )
            assert hk_counters(_replayed(IB, out, env, where)) == expected

            g.send("TIME_NOOP")
            g.send("TIME_SET_STCF", "Seconds=1000", "MicroSeconds=500000")
            requests.until_recorded(
                env,
                out,
                recorder,
                "msg == TIME_HK",
                lambda records: any(r["fields"]["STCF_Seconds"] == 1000 for r in records),
                "TIME_SEND_HK",
            )
            scenario_s = time.monotonic() - started
            assert recorder.poll() is None, (
                f"the scenario took {scenario_s:.0f} s, more than the {RECORD_SECONDS} s recording"
            )
            code = recorder.wait(timeout=RECORD_SECONDS + 30)  # its --duration ends it
        finally:
            if recorder.poll() is None:
                recorder.send_signal(signal.SIGINT)
                recorder.wait(timeout=20)
    assert code == 0, err_path.read_text()  # --duration is a normal end
    print(f"\nscenario steps took {scenario_s:.0f} s of the {RECORD_SECONDS} s recording")

    # -- the recording is clean ----------------------------------------------------------
    check = _run_ib(env, "rec", "check", "--strict", str(out))
    assert check.returncode == 0, check.stderr
    assert without_host_warnings(check.stderr) == []
    report = _info(env, out)
    assert report["status"] == "closed" and report["source"] == "index"
    assert report["damage_total"] == 0 and report["findings_total"] == 0 and report["holes"] == []
    assert report["gaps"]["total"] == 0 and report["missing_frames"] == 0
    frames = int(report["frames"])
    assert frames > 0 and int(report["last_seq"]) - int(report["first_seq"]) + 1 == frames
    with RecordingReader(out) as reader:
        seqs = [item.seq for item in reader.frames()]
    assert seqs == list(range(int(report["first_seq"]), int(report["last_seq"]) + 1))

    # -- per-MID counts match SCH_LAB's periods ---------------------------------------------
    stops = [m for m in report["marks"] if m["text"] == "record ended"]
    assert len(stops) == 1
    window_s = (stops[0]["time_ns"] - report["first_time_ns"]) / 1e9
    by_mid = {m["mid"]: m["count"] for m in report["mids"]}
    for mid, period in PERIODS_S.items():
        extra = requests.sent[REQUESTED[mid]] if mid in REQUESTED else 0
        periodic = by_mid.get(mid, 0) - extra
        assert abs(periodic - window_s / period) <= 2, (
            f"MID {mid:#06x}: {by_mid.get(mid, 0)} recorded ({extra} asked for) in "
            f"{window_s:.1f} s, one expected every {period} s"
        )

    # -- replay finds exactly 2K NOOP events ------------------------------------------------
    events = _replayed(IB, out, env, NOOP_EVENT)
    assert len(events) == 2 * K and all(r["category"] == "event" for r in events)
    assert len(_tx_commands(_isolated_state, "SAMPLE_APP_NOOP")) == 2 * K  # the ledger agrees

    # -- export: the counter rises by K, resets, rises by K ---------------------------------
    tables = tmp_path / "tables"
    done = _run_ib(env, "export", str(out), "-o", str(tables))
    assert done.returncode == 0, done.stderr
    with (tables / "SAMPLE_APP_HK.csv").open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    runs = _runs([int(r["CommandCounter"]) for r in rows])
    assert runs == expected, runs
    assert runs[K] - runs[0] == K and runs[-1] - runs[K + 1] == K  # rising by K, twice
    assert len(rows) == by_mid[0x0883]  # one row per recorded frame of the MID
    with (tables / "TIME_HK.csv").open(newline="") as handle:
        time_rows = list(csv.DictReader(handle))
    assert len(time_rows) == by_mid[0x0805]
    assert any(int(r["STCF_Seconds"]) == 1000 for r in time_rows)
    total = sum(sum(1 for _ in csv.reader(p.open(newline=""))) - 1 for p in tables.iterdir())
    assert total == frames  # every frame is in exactly one table


# -- a recorder killed mid-capture --------------------------------------------------------------


def test_a_recorder_killed_mid_capture_leaves_a_recording_that_replays_and_repairs(
    ground: Ground, tmp_path: Path
) -> None:
    g = ground
    env = _env(_recording_config(tmp_path))
    torn = tmp_path / "killed.ibrec"
    err_path = tmp_path / "record.stderr"
    # The generator's MIDs are already in TO_LAB's subscription table, so no command is needed
    # (and a re-subscription would be rejected and counted as a TO_LAB command error).
    to_lab_errors = g.fresh("TO_LAB_HK", "TO_LAB_SEND_HK")["CommandErrorCounter"]
    with g.listener_paused(), err_path.open("w") as err:
        recorder = subprocess.Popen(
            [*IB, "record", "-o", str(torn), "--quiet"], stderr=err, env=env
        )
        try:
            _wait_listening(recorder, err_path)
            g.send("IB_TEST_GEN_START", f"Rate={GEN_RATE_PPS}", "Count=0")  # until stopped

            def enough_recorded() -> bool:
                assert recorder.poll() is None, err_path.read_text()
                return int(_info(env, torn)["frames"]) >= KILL_AFTER_FRAMES

            _wait_until("frames never reached the kill point", enough_recorded, timeout=60)
            recorder.send_signal(signal.SIGKILL)  # mid-capture: the generator is still running
            assert recorder.wait(timeout=10) == -signal.SIGKILL
        finally:
            if recorder.poll() is None:
                recorder.kill()
                recorder.wait(timeout=10)
            g.send("IB_TEST_GEN_STOP", check=False)
    assert g.fresh("TO_LAB_HK", "TO_LAB_SEND_HK")["CommandErrorCounter"] == to_lab_errors

    # SIGKILL stops the process between two writes, so the file ends on a record boundary: no
    # trailer, nothing cut. A power loss can also cut a record in two; that is a copy of the same
    # file with its last bytes removed.
    cut = tmp_path / "cut.ibrec"
    cut.write_bytes(torn.read_bytes()[:-CUT_BYTES])
    killed = _check_the_tail(env, torn, tmp_path / "killed-repaired.ibrec", {"unclosed", "torn"})
    assert killed >= KILL_AFTER_FRAMES
    sliced = _check_the_tail(env, cut, tmp_path / "cut-repaired.ibrec", {"torn"})
    assert killed - 1 <= sliced <= killed  # the cut ended inside the last frame record or chunk


def _check_the_tail(env: dict[str, str], damaged: Path, repaired: Path, statuses: set[str]) -> int:
    """The tail of ``damaged`` is reported, replay plays up to it, and ``ib rec repair`` writes
    exactly the readable prefix. Returns the number of frames in that prefix."""
    report = _info(env, damaged)
    assert report["status"] in statuses, report["status"]
    assert report["writer_active"] is False and report["gaps"]["total"] == 0
    prefix = int(report["frames"])
    print(f"\n{damaged.name}: {report['status']}, {prefix} frames readable")
    check = _run_ib(env, "rec", "check", str(damaged))
    assert check.returncode == 0 and Code.REC_TORN.value in check.stderr, check.stderr
    assert _run_ib(env, "rec", "check", "--strict", str(damaged)).returncode == 1

    played, replay_err = _hex_lines(env, damaged)
    assert len(played) == prefix
    assert Code.REC_TORN.value in replay_err, replay_err

    before = damaged.read_bytes()
    done = _run_ib(env, "rec", "repair", str(damaged), str(repaired))
    assert done.returncode == 0, done.stderr
    assert damaged.read_bytes() == before  # the original is untouched
    fixed = _info(env, repaired)
    assert fixed["status"] == "closed" and int(fixed["frames"]) == prefix
    assert _run_ib(env, "rec", "check", "--strict", str(repaired)).returncode == 0
    fixed_played, fixed_err = _hex_lines(env, repaired)
    assert fixed_played == played, "the repaired recording differs from the readable prefix"
    assert Code.REC_TORN.value not in fixed_err
    with RecordingReader(damaged) as before_reader, RecordingReader(repaired) as after_reader:
        assert [(f.seq, f.time_ns, f.mid, f.data) for f in after_reader.frames()] == [
            (f.seq, f.time_ns, f.mid, f.data) for f in before_reader.frames()
        ]
    return prefix
