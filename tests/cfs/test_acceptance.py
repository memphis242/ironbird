"""Stage 1 acceptance against a real cFS instance, driven only through the ``ib`` CLI.

Opt-in (marker ``cfs``): skipped unless ``IRONBIRD_CFS=1`` is set or ``-m cfs`` is given,
and then skipped (or, with ``IRONBIRD_CFS=1``, failed) when the cFS build is missing. See
``tests/conftest.py`` for the gate and ``tools/cfs/`` for the build and run scripts.

One cFS instance is shared by every test in this module, so each test measures counter
*deltas* against a fresh baseline instead of assuming a power-on state. Telemetry is read
from one long-running ``ib listen`` process; commands go out as separate ``ib send``
processes. Housekeeping is requested on demand (``*_SEND_HK`` through CI_LAB) and every
check re-requests until the expected state shows up, because SCH_LAB also publishes the same
HK packets periodically and a periodic one can arrive between a command and our request.
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.cfs

REPO = Path(__file__).resolve().parents[2]
CONFIG = REPO / "examples" / "ironbird.toml"
RUN_SH = REPO / "tools" / "cfs" / "run.sh"
CLEAN_SH = REPO / "tools" / "cfs" / "clean.sh"
RAM_DIR = Path("/dev/shm/osal:RAM")  # OSAL's backing directory for the cFE /ram volume

CI_LAB_PORT = 1234
TO_LAB_PORT = 2234
STARTUP_TIMEOUT_S = 60.0
SUBSEC = float(1 << 32)

Packet = dict[str, Any]


# -- helpers --------------------------------------------------------------------------------


def _udp_port_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        try:
            sock.bind(("0.0.0.0", port))
        except OSError:
            return False
        return True


def _wait_for_log(log: Path, needle: str, proc: subprocess.Popen[bytes], timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        text = log.read_text(errors="replace") if log.exists() else ""
        if needle in text:
            return
        if proc.poll() is not None:
            break
        time.sleep(0.1)
    tail = "\n".join((log.read_text(errors="replace") if log.exists() else "").splitlines()[-25:])
    raise AssertionError(f"cFS never printed {needle!r} (exit={proc.poll()}). Log tail:\n{tail}")


@pytest.fixture(scope="module")
def cfs(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Path]:
    """A running cFS (tools/cfs/run.sh); returns its console log. Stopped and cleaned after."""
    for port in (CI_LAB_PORT, TO_LAB_PORT):
        assert _udp_port_free(port), (
            f"UDP port {port} is already in use (another cFS instance running?); "
            "stop it before running the cfs tests"
        )
    ram_existed = RAM_DIR.exists()
    log = tmp_path_factory.mktemp("cfs") / "console.log"
    with log.open("wb") as sink:
        proc = subprocess.Popen(
            [str(RUN_SH)], stdout=sink, stderr=subprocess.STDOUT, start_new_session=True
        )
    try:
        # run.sh execs the cFS core, so proc is cFS itself (and SIGINT is its clean shutdown).
        _wait_for_log(log, "CI_LAB listening on UDP port: 1234", proc, STARTUP_TIMEOUT_S)
        _wait_for_log(log, "entering OPERATIONAL state", proc, STARTUP_TIMEOUT_S)
        _wait_for_log(log, "TO Lab subscribed to", proc, STARTUP_TIMEOUT_S)
        yield log
    finally:
        if proc.poll() is None:
            proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
        subprocess.run([str(CLEAN_SH)], check=False, timeout=30)  # the PSP's shm segments
        if not ram_existed:  # only remove what this run created
            shutil.rmtree(RAM_DIR, ignore_errors=True)


@dataclass
class Ground:
    """Drives the ``ib`` CLI: ``send`` per command, one long-running ``listen`` for telemetry."""

    env: dict[str, str]
    packets: list[tuple[float, Packet]] = field(default_factory=list)
    output_enabled: bool = False
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _proc: subprocess.Popen[str] | None = None
    _reader: threading.Thread | None = None
    _stderr_log: Path | None = None

    # -- lifecycle --

    def start_listener(self, stderr_log: Path) -> None:
        self._stderr_log = stderr_log
        with stderr_log.open("w") as err:
            self._proc = subprocess.Popen(
                [*self._ib(), "listen", "fc", "--format", "json"],
                stdout=subprocess.PIPE,
                stderr=err,
                text=True,
                env=self.env,
            )
        self._reader = threading.Thread(target=self._read, daemon=True)
        self._reader.start()
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if "listening" in stderr_log.read_text():
                return
            assert self._proc.poll() is None, f"ib listen died: {stderr_log.read_text()}"
            time.sleep(0.05)
        raise AssertionError("ib listen never reported that it is listening")

    def stop_listener(self) -> None:
        if self._proc is not None and self._proc.poll() is None:
            self._proc.send_signal(signal.SIGINT)
            try:
                self._proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self._proc.wait(timeout=5)
        if self._reader is not None:
            self._reader.join(timeout=5)

    def _read(self) -> None:
        assert self._proc is not None and self._proc.stdout is not None
        for line in self._proc.stdout:
            record = json.loads(line)
            with self._lock:
                self.packets.append((time.monotonic(), record))

    # -- sending --

    @staticmethod
    def _ib() -> list[str]:
        return [sys.executable, "-m", "ironbird.cli.main", "-c", str(CONFIG)]

    def send(
        self, command: str, *args: str, flags: tuple[str, ...] = (), check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        """``ib send fc COMMAND [name=value...]``; uses this test's own ledger directory."""
        done = subprocess.run(
            [*self._ib(), "send", "fc", command, *args, *flags],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        if check:
            assert done.returncode == 0, f"ib send {command} failed: {done.stderr}"
            assert done.stdout.startswith("sent "), done.stdout
        return done

    # -- telemetry --

    def count(self) -> int:
        with self._lock:
            return len(self.packets)

    def since(self, index: int, name: str | None = None) -> list[Packet]:
        with self._lock:
            return [p for _, p in self.packets[index:] if name is None or p["name"] == name]

    def wait_for(
        self,
        name: str,
        predicate: Callable[[dict[str, Any]], bool],
        *,
        request: str | None = None,
        timeout: float = 20.0,
        start: int | None = None,
    ) -> Packet:
        """First ``name`` packet (after index ``start``) whose fields satisfy ``predicate``.

        With ``request`` (a ``*_SEND_HK``/``*_SEND_DIAGNOSTIC`` command), re-sends it about
        once a second until the packet shows up.
        """
        first = self.count() if start is None else start
        deadline = time.monotonic() + timeout
        last: list[Packet] = []
        next_request = 0.0
        while time.monotonic() < deadline:
            if request is not None and time.monotonic() >= next_request:
                self.send(request)
                next_request = time.monotonic() + 1.0
            last = self.since(first, name)
            for packet in last:
                if predicate(packet["fields"]):
                    return packet
            time.sleep(0.05)
        seen = [p["fields"] for p in last[-3:]]
        raise AssertionError(
            f"no {name} satisfying the predicate within {timeout} s; last seen: {seen}"
        )

    def fresh(self, name: str, request: str) -> dict[str, Any]:
        """Fields of the next ``name`` packet produced after we request it."""
        return self.wait_for(name, lambda _: True, request=request, timeout=10.0)["fields"]

    def enable_output(self) -> None:
        self.send("TO_LAB_ENABLE_OUTPUT", "dest_IP=127.0.0.1")
        self.output_enabled = True


def _delta(after: int, before: int) -> int:
    """Difference of two u8 counters, tolerant of wrap-around."""
    return (after - before) % 256


@pytest.fixture(scope="module")
def ground_session(cfs: Path, tmp_path_factory: pytest.TempPathFactory) -> Iterator[Ground]:
    """cFS up and an ``ib listen fc`` running, but TO_LAB output not yet enabled."""
    base = tmp_path_factory.mktemp("listener")
    env = {**os.environ, "IRONBIRD_STATE_DIR": str(base / "state")}
    env.pop("IRONBIRD_CONFIG", None)
    ground = Ground(env=env)
    ground.start_listener(base / "listen.stderr")
    try:
        yield ground
    finally:
        ground.stop_listener()


@pytest.fixture
def ground(ground_session: Ground) -> Ground:
    """Telemetry flowing: TO_LAB output enabled (idempotent), and a quiet start."""
    if not ground_session.output_enabled:
        ground_session.enable_output()
    return ground_session


def _ledger_events(state_dir: Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for path in sorted(state_dir.glob("*.jsonl")):
        events.extend(json.loads(line) for line in path.read_text().splitlines())
    return events


def _tx_commands(state_dir: Path, command: str) -> list[dict[str, Any]]:
    return [
        e
        for e in _ledger_events(state_dir)
        if e["event"] == "tx.command" and e["command"] == command
    ]


def _requests(state_dir: Path, command: str) -> list[dict[str, Any]]:
    return [
        e
        for e in _ledger_events(state_dir)
        if e["event"] == "send.request" and e["command"] == command
    ]


# -- case 1: TO_LAB enable output -> telemetry flows, ES and TIME HK decode sanely -------------


def test_to_lab_enable_output_starts_telemetry_and_hk_decodes_sanely(
    ground_session: Ground,
) -> None:
    g = ground_session
    # TO_LAB sends nothing before it is told where to send.
    time.sleep(1.0)
    if not g.output_enabled:
        assert g.count() == 0, "telemetry arrived before output was enabled"

    mark = g.count()
    g.enable_output()
    # Without any request, the periodic SCH_LAB housekeeping reaches us (4.0 s ES, 4.4 s TIME).
    es = g.wait_for("ES_HK", lambda _: True, start=mark, timeout=20)
    time_hk = g.wait_for("TIME_HK", lambda _: True, start=mark, timeout=20)

    f = es["fields"]
    assert (f["CFEMajorVersion"], f["CFEMinorVersion"], f["CFERevision"]) == (7, 0, 1)
    assert (f["OSALMajorVersion"], f["PSPMajorVersion"]) == (7, 7)
    assert f["SysLogBytesUsed"] <= f["SysLogSize"]
    assert f["SysLogEntries"] > 0
    assert f["RegisteredCoreApps"] >= 4
    assert 1 <= f["RegisteredExternalApps"] <= 16  # trimmed startup script runs a few apps
    assert f["RegisteredTasks"] >= f["RegisteredCoreApps"]
    assert es["labels"]["ResetType"] == "POWERON"  # fresh run.sh start
    assert f["ProcessorResets"] <= f["MaxProcessorResets"]
    assert es["mid"] == "0x0800" and es["kind"] == "telemetry"

    t = time_hk["fields"]
    assert 0 <= t["LeapSeconds"] <= 100
    assert t["MET_Subseconds"] < 2**32
    assert t["CommandErrorCounter"] == 0


def test_time_met_runs_at_wall_clock_rate(ground: Ground) -> None:
    first_index = ground.count()
    first = ground.wait_for("TIME_HK", lambda _: True, request="TIME_SEND_HK", start=first_index)
    t0 = _arrival(ground, first)
    time.sleep(2.0)
    second_index = ground.count()
    second = ground.wait_for("TIME_HK", lambda _: True, request="TIME_SEND_HK", start=second_index)
    t1 = _arrival(ground, second)

    def met(packet: Packet) -> float:
        fields = packet["fields"]
        return fields["MET_Seconds"] + fields["MET_Subseconds"] / SUBSEC

    elapsed_met, elapsed_wall = met(second) - met(first), t1 - t0
    assert elapsed_met > 1.5
    assert abs(elapsed_met - elapsed_wall) < 0.75, (elapsed_met, elapsed_wall)
    assert second["sequence"] != first["sequence"]


def _arrival(ground: Ground, packet: Packet) -> float:
    with ground._lock:
        for when, candidate in ground.packets:
            if candidate is packet:
                return when
    raise AssertionError("packet not in the capture")


def test_to_lab_data_types_packet_decodes_every_scalar_type(ground: Ground) -> None:
    """TO_LAB's fixed test packet is a ground-truth vector for the codec on all scalar types."""
    mark = ground.count()
    ground.send("TO_LAB_SEND_DATA_TYPES")
    packet = ground.wait_for("TO_LAB_DATA_TYPES", lambda _: True, start=mark, timeout=10)
    f = packet["fields"]
    assert (f["synch"], f["bl1"], f["bl2"]) == (0x6969, 0, 1)
    assert (f["b1"], f["b2"], f["b3"], f["b4"]) == (16, 127, 0x7F, 0x45)
    assert (f["w1"], f["w2"]) == (0x2468, 0x7FFF)
    assert (f["dw1"], f["dw2"]) == (0x12345678, 0x87654321 - (1 << 32))  # signed int32
    assert f["f1"] == pytest.approx(90.01, rel=1e-6)
    assert f["f2"] == pytest.approx(4.5e-6, rel=1e-6)
    assert f["df1"] == 99.9 and f["df2"] == 0.4444
    assert f["str"] == "ABCDEFGHIJ"  # 10 chars, no NUL terminator


# -- case 2: SAMPLE_APP NOOP and RESET_COUNTERS --------------------------------------------------


def test_sample_app_noop_and_reset_counters_reflected_in_hk(ground: Ground) -> None:
    base = ground.fresh("SAMPLE_APP_HK", "SAMPLE_APP_SEND_HK")

    for _ in range(3):
        ground.send("SAMPLE_APP_NOOP")
    expected = (base["CommandCounter"] + 3) % 256
    after_noops = ground.wait_for(
        "SAMPLE_APP_HK",
        lambda f: f["CommandCounter"] == expected,
        request="SAMPLE_APP_SEND_HK",
    )["fields"]
    assert _delta(after_noops["CommandCounter"], base["CommandCounter"]) == 3
    assert after_noops["CommandErrorCounter"] == base["CommandErrorCounter"]

    ground.send("SAMPLE_APP_RESET_COUNTERS")
    ground.wait_for(
        "SAMPLE_APP_HK",
        lambda f: f["CommandCounter"] == 0 and f["CommandErrorCounter"] == 0,
        request="SAMPLE_APP_SEND_HK",
    )


def test_sample_app_display_param_accepts_mixed_argument_types(ground: Ground) -> None:
    """uint32 + negative int16 + char[10] payload, accepted and counted as valid."""
    base = ground.fresh("SAMPLE_APP_HK", "SAMPLE_APP_SEND_HK")
    ground.send("SAMPLE_APP_DISPLAY_PARAM", "ValU32=4000000000", "ValI16=-5", "ValStr=hello")
    expected = (base["CommandCounter"] + 1) % 256
    after = ground.wait_for(
        "SAMPLE_APP_HK",
        lambda f: f["CommandCounter"] == expected,
        request="SAMPLE_APP_SEND_HK",
    )["fields"]
    assert after["CommandErrorCounter"] == base["CommandErrorCounter"]


# -- case 3: TIME NOOP, an argument command, HK and diagnostic decode ---------------------------


def test_time_noop_set_stcf_and_adjust_are_reflected_in_hk_and_diag(ground: Ground) -> None:
    base = ground.fresh("TIME_HK", "TIME_SEND_HK")

    ground.send("TIME_NOOP")
    expected = (base["CommandCounter"] + 1) % 256
    ground.wait_for("TIME_HK", lambda f: f["CommandCounter"] == expected, request="TIME_SEND_HK")

    # STCF = 1000.5 s: seconds and 2^-32 s subseconds on the wire, little-endian payload.
    ground.send("TIME_SET_STCF", "Seconds=1000", "MicroSeconds=500000")
    stcf = ground.wait_for(
        "TIME_HK",
        lambda f: f["STCF_Seconds"] == 1000,
        request="TIME_SEND_HK",
    )["fields"]
    assert stcf["STCF_Subseconds"] == 0x80000000
    assert stcf["CommandErrorCounter"] == base["CommandErrorCounter"]

    # A 2 s clock adjustment folds into STCF and shows as the one-time adjustment in DIAG.
    ground.send("TIME_ADD_ADJUST", "Seconds=2", "MicroSeconds=0")
    ground.wait_for(
        "TIME_HK",
        lambda f: f["STCF_Seconds"] == 1002 and f["STCF_Subseconds"] == 0x80000000,
        request="TIME_SEND_HK",
    )
    diag = ground.wait_for(
        "TIME_DIAG",
        lambda f: f["OneTimeAdjust_Seconds"] == 2,
        request="TIME_SEND_DIAGNOSTIC",
    )["fields"]
    assert diag["OneTimeAdjust_Subseconds"] == 0
    assert diag["AtToneSTCF_Seconds"] == 1002
    assert diag["CurrentMET_Seconds"] >= base["MET_Seconds"]


def test_time_rejects_out_of_range_microseconds(ground: Ground) -> None:
    """The FSW range check (usec < 1_000_000) counts an error; ib sends the value verbatim."""
    base = ground.fresh("TIME_HK", "TIME_SEND_HK")
    ground.send("TIME_SET_STCF", "Seconds=7", "MicroSeconds=1000000")
    expected = (base["CommandErrorCounter"] + 1) % 256
    after = ground.wait_for(
        "TIME_HK",
        lambda f: f["CommandErrorCounter"] == expected,
        request="TIME_SEND_HK",
    )["fields"]
    assert after["STCF_Seconds"] == base["STCF_Seconds"]  # the rejected value was not applied


# -- case 4: ES write-system-log-to-file ---------------------------------------------------------


def test_es_write_syslog_creates_file_and_counts_the_command(ground: Ground) -> None:
    name = f"ib_{os.getpid()}.log"  # OSAL file names are at most 19 characters
    target = RAM_DIR / name
    assert not target.exists()
    base = ground.fresh("ES_HK", "ES_SEND_HK")
    try:
        ground.send("ES_WRITE_SYS_LOG", f"FileName=/ram/{name}")
        expected = (base["CommandCounter"] + 1) % 256
        after = ground.wait_for(
            "ES_HK", lambda f: f["CommandCounter"] == expected, request="ES_SEND_HK"
        )["fields"]
        assert after["CommandErrorCounter"] == base["CommandErrorCounter"]
        assert target.is_file()
        data = target.read_bytes()
        assert len(data) > 0
    finally:
        target.unlink(missing_ok=True)


@pytest.mark.parametrize(
    "path",
    ["/no_such_volume/x.dat", "/ram/" + "n" * 30 + ".log", "/ram/" + "d" * 59],
    ids=["unknown-volume", "name-too-long-for-osal", "full-width-field-without-nul"],
)
def test_es_write_syslog_to_an_invalid_path_counts_an_error_and_writes_nothing(
    ground: Ground, path: str
) -> None:
    before_files = set(RAM_DIR.iterdir()) if RAM_DIR.exists() else set()
    base = ground.fresh("ES_HK", "ES_SEND_HK")
    ground.send("ES_WRITE_SYS_LOG", f"FileName={path}")
    expected = (base["CommandErrorCounter"] + 1) % 256
    after = ground.wait_for(
        "ES_HK", lambda f: f["CommandErrorCounter"] == expected, request="ES_SEND_HK"
    )["fields"]
    assert after["CommandCounter"] == base["CommandCounter"]
    assert (set(RAM_DIR.iterdir()) if RAM_DIR.exists() else set()) == before_files


# -- case 5: deliberately malformed commands -----------------------------------------------------

# (command, HK request, HK message): each FSW app verifies the command length it expects.
LENGTH_CHECKING_APPS = [
    ("ES_NOOP", "ES_SEND_HK", "ES_HK"),
    ("TIME_NOOP", "TIME_SEND_HK", "TIME_HK"),
    ("SAMPLE_APP_NOOP", "SAMPLE_APP_SEND_HK", "SAMPLE_APP_HK"),
]


@pytest.mark.parametrize(("command", "request_hk", "hk_name"), LENGTH_CHECKING_APPS)
def test_wrong_length_command_increments_the_apps_error_counter(
    ground: Ground, _isolated_state: Path, command: str, request_hk: str, hk_name: str
) -> None:
    """Header length one byte short of what the app expects: rejected and counted, not run."""
    base = ground.fresh(hk_name, request_hk)
    done = ground.send(command, flags=("--length-delta", "-1"))
    assert "(mid 0x" in done.stdout  # ib itself reports the send as normal

    expected = (base["CommandErrorCounter"] + 1) % 256
    after = ground.wait_for(
        hk_name, lambda f: f["CommandErrorCounter"] == expected, request=request_hk
    )["fields"]
    assert after["CommandCounter"] == base["CommandCounter"]  # the command did not execute

    # The ledger holds the request with its tamper and the exact bytes put on the wire.
    sent = _tx_commands(_isolated_state, command)
    assert len(sent) == 1 and sent[0]["result"] == "sent"
    raw = bytes.fromhex(sent[0]["raw"])
    assert len(raw) == 8
    assert int.from_bytes(raw[4:6], "big") + 7 == len(raw) - 1  # header claims one byte less
    requests = _requests(_isolated_state, command)
    assert len(requests) == 1 and requests[0]["tamper"] == {"length": -1}


def test_header_length_beyond_datagram_is_dropped_by_ci_lab_and_counted(
    ground: Ground, _isolated_state: Path
) -> None:
    base_ci = ground.fresh("CI_LAB_HK", "CI_LAB_SEND_HK")
    base_es = ground.fresh("ES_HK", "ES_SEND_HK")

    ground.send("ES_NOOP", flags=("--length-delta", "2"))
    expected = base_ci["IngestErrors"] + 1
    after = ground.wait_for(
        "CI_LAB_HK", lambda f: f["IngestErrors"] == expected, request="CI_LAB_SEND_HK"
    )["fields"]
    assert after["IngestPackets"] >= base_ci["IngestPackets"]
    es_after = ground.fresh("ES_HK", "ES_SEND_HK")
    assert es_after["CommandCounter"] == base_es["CommandCounter"]  # never reached ES
    assert es_after["CommandErrorCounter"] == base_es["CommandErrorCounter"]

    sent = _tx_commands(_isolated_state, "ES_NOOP")
    assert len(sent) == 1
    raw = bytes.fromhex(sent[0]["raw"])
    assert int.from_bytes(raw[4:6], "big") + 7 > len(raw)  # claims more bytes than were sent


def test_bad_checksum_is_accepted_by_this_cfs_build(ground: Ground, _isolated_state: Path) -> None:
    """This cFS build never validates command checksums (tools/cfs/NOTES.md, section 1).

    The corrupted packet is therefore executed and counted as valid. This pins the property
    that the corruption really reaches the wire and the target ignores it; a validating
    mission would reject it instead.
    """
    base = ground.fresh("ES_HK", "ES_SEND_HK")
    ground.send("ES_NOOP", flags=("--bad-checksum",))
    expected = (base["CommandCounter"] + 1) % 256
    after = ground.wait_for(
        "ES_HK", lambda f: f["CommandCounter"] == expected, request="ES_SEND_HK"
    )["fields"]
    assert after["CommandErrorCounter"] == base["CommandErrorCounter"]

    sent = _tx_commands(_isolated_state, "ES_NOOP")
    raw = bytes.fromhex(sent[0]["raw"])
    good_sum = 0xFF
    for byte in raw:
        good_sum ^= byte
    assert good_sum != 0  # XOR over the packet is no longer zero: the checksum is wrong
    requests = _requests(_isolated_state, "ES_NOOP")
    assert len(requests) == 1 and requests[0]["tamper"] == {"checksum": 1}


def test_unknown_command_and_bad_argument_never_reach_the_wire(
    ground: Ground, _isolated_state: Path
) -> None:
    """Encoder-side rejections are one-line coded errors and send nothing to cFS."""
    base_ci = ground.fresh("CI_LAB_HK", "CI_LAB_SEND_HK")
    unknown = ground.send("ES_NO_SUCH_COMMAND", check=False)
    assert unknown.returncode != 0 and unknown.stderr.startswith("error IB-")
    assert len(unknown.stderr.strip().splitlines()) == 1
    bad_arg = ground.send("TIME_SET_STCF", "Seconds=notanumber", "MicroSeconds=0", check=False)
    assert bad_arg.returncode != 0 and bad_arg.stderr.startswith("error IB-")
    assert not _tx_commands(_isolated_state, "TIME_SET_STCF")
    after_ci = ground.fresh("CI_LAB_HK", "CI_LAB_SEND_HK")
    # Only our own CI_LAB_SEND_HK request packets arrived in between.
    assert after_ci["IngestErrors"] == base_ci["IngestErrors"]
