"""The ``ib_test`` throughput source against a real cFS instance.

The app's generator (``IB_TEST_GEN_START``) publishes fixed-size 200-byte telemetry at a rate up to
10k packets/s. These tests check its contract (a run publishes exactly ``Count`` packets with
consecutive serials, a filler derived from the serial, and a status packet; its commands are
validated) and what ironbird makes of the stream:

* at 1k packets/s nothing is lost anywhere between the app and the recording;
* at 10k packets/s ``ib record`` loses nothing it was handed (no kernel, queue or buffer drop, no
  duplicate, nothing out of order). cFS's own software bus can overflow TO_LAB's pipe when its
  forwarding task is descheduled for a few milliseconds; the SB housekeeping counts those drops
  (``PipeOverflowErrorCounter``), and every packet missing from the recording is one of them.

Opt-in (marker ``cfs``), like the other tests here. The gate with a loopback blaster, for ironbird
alone, is ``tests/bench``.
"""

from __future__ import annotations

import itertools
import signal
import struct
import subprocess
from pathlib import Path
from typing import Any

import pytest
from test_acceptance import (
    Ground,
    _artifact_safe_paths,
    _replayed,
    ground,
    ground_session,
)
from test_cfs_variable_length import IB_TEST_HK_MID, IB_TEST_VAR_MID, _hk, cfs
from test_stage2_acceptance import (
    IB,
    _env,
    _info,
    _recording_config,
    _Requests,
    _wait_listening,
)

from ironbird.recording.reader import RecordingReader

pytestmark = pytest.mark.cfs

SHARED_FIXTURES = (_artifact_safe_paths, cfs, ground, ground_session)

GEN_MID = IB_TEST_VAR_MID + 1  # 0x08E2
GEN_STATUS_MID = GEN_MID + 1  # 0x08E3
PACKET_BYTES = 200
FILL_BYTES = 176
SERIAL_OFFSET = 16
STATE_RUNNING, STATE_FINISHED, STATE_STOPPED = 1, 2, 3
SB_HK = "msg == SB_HK"
STATUS = "msg == IB_TEST_GEN_STATUS"


@pytest.fixture
def generator_output(ground: Ground) -> Ground:
    """TO_LAB told to forward the generator's two packets and the app's housekeeping (once per
    cFS instance)."""
    if not getattr(ground, "generator_added", False):
        for mid in (IB_TEST_HK_MID, GEN_MID, GEN_STATUS_MID):
            ground.send(
                "TO_LAB_ADD_PKT",
                f"Stream={mid:#x}",
                "QosPriority=0",
                "QosReliability=0",
                "BufLimit=49",  # the most TO_LAB accepts
                "Padding=00",
            )
        ground.generator_added = True  # type: ignore[attr-defined]
    return ground


def _generated(g: Ground, tmp_path: Path, name: str, rate: int, count: int) -> dict[str, Any]:
    """Record one generator run of ``count`` packets at ``rate``/s with ``ib record`` and return
    the recording's path, the run's status packet and the SB pipe-overflow counts around it."""
    env = _env(_recording_config(tmp_path))
    out = tmp_path / f"{name}.ibrec"
    err_path = tmp_path / f"{name}.stderr"
    requests = _Requests(g)
    with g.listener_paused(), err_path.open("w") as err:
        recorder = subprocess.Popen([*IB, "record", "-o", str(out), "--quiet"], stderr=err, env=env)
        try:
            _wait_listening(recorder, err_path)

            def ask(where: str, predicate: Any, request: str) -> None:
                requests.until_recorded(env, out, recorder, where, predicate, request)

            ask(SB_HK, lambda records: len(records) >= 1, "SB_SEND_HK")
            g.send("IB_TEST_GEN_START", f"Rate={rate}", f"Count={count}")
            ask(
                STATUS,
                lambda records: any(r["fields"]["State"] == STATE_FINISHED for r in records),
                "IB_TEST_GEN_SEND_STATUS",
            )
            done = next(
                r for r in _replayed(IB, out, env, STATUS) if r["fields"]["State"] == STATE_FINISHED
            )
            ask(SB_HK, lambda records: records[-1]["time"] > done["time"], "SB_SEND_HK")
        finally:
            if recorder.poll() is None:
                recorder.send_signal(signal.SIGINT)
                recorder.wait(timeout=30)
    summary = _replayed(IB, out, env, SB_HK)
    overflow = (
        int(summary[-1]["fields"]["PipeOverflowErrorCounter"])
        - int(summary[0]["fields"]["PipeOverflowErrorCounter"])
    ) % 65536
    return {"out": out, "env": env, "status": done["fields"], "sb_overflow": overflow}


def _counter(fields: dict[str, object], name: str) -> int:
    value = fields[name]
    assert isinstance(value, int)
    return value


def _packets(recording: Path) -> list[tuple[int, int, int, bytes]]:
    """(sequence count, serial, run, fill) of every generator packet in the recording, taken from
    the raw bytes, apart from the codec."""
    found: list[tuple[int, int, int, bytes]] = []
    with RecordingReader(recording) as reader:
        for item in reader.frames():
            if item.mid != GEN_MID:
                continue
            assert len(item.data) == PACKET_BYTES, len(item.data)
            (word,) = struct.unpack_from(">H", item.data, 2)
            serial, run = struct.unpack_from("<II", item.data, SERIAL_OFFSET)
            found.append((word & 0x3FFF, serial, run, item.data[SERIAL_OFFSET + 8 :]))
    return found


def _assert_intact(found: list[tuple[int, int, int, bytes]], run: int) -> None:
    """Each packet is the one its serial says, from the one run; none repeats or is out of order."""
    serials = [serial for _, serial, _, _ in found]
    assert all(r == run for _, _, r, _ in found)
    assert serials == sorted(set(serials)), "a serial repeats or arrives out of order"
    for _, serial, _, fill in found[:: max(1, len(found) // 500)]:
        assert fill == bytes((serial + i) & 0xFF for i in range(FILL_BYTES)), serial


def test_one_thousand_packets_a_second_reach_the_recording_exactly(
    generator_output: Ground, tmp_path: Path
) -> None:
    count = 3000
    result = _generated(generator_output, tmp_path, "floor", 1000, count)
    found = _packets(result["out"])
    status = result["status"]
    assert (status["Run"], status["Rate"], status["Target"], status["Sent"]) == (
        status["Run"],
        1000,
        count,
        count,
    )
    assert status["State"] == STATE_FINISHED
    assert [serial for _, serial, _, _ in found] == list(range(count))  # all of them, in order
    _assert_intact(found, int(status["Run"]))
    counts = [seq for seq, _, _, _ in found]
    assert all((b - a) % 16384 == 1 for a, b in itertools.pairwise(counts))
    assert result["sb_overflow"] == 0, "cFS dropped packets at 1k/s"
    report = _info(result["env"], result["out"])
    assert report["status"] == "closed" and report["gaps"]["total"] == 0
    one = _replayed(IB, result["out"], result["env"], "msg == IB_TEST_GEN and Serial == 17")
    assert len(one) == 1  # the codec decodes the packet the raw bytes describe
    assert one[0]["fields"]["Serial"] == 17 and one[0]["fields"]["Run"] == status["Run"]


def test_ten_thousand_packets_a_second_are_not_lost_by_ironbird(
    generator_output: Ground, tmp_path: Path
) -> None:
    count = 30_000
    result = _generated(generator_output, tmp_path, "target", 10_000, count)
    found = _packets(result["out"])
    status = result["status"]
    assert status["Sent"] == count and status["State"] == STATE_FINISHED
    _assert_intact(found, int(status["Run"]))
    report = _info(result["env"], result["out"])
    assert report["status"] == "closed" and report["gaps"]["total"] == 0, report["gaps"]
    assert report["missing_frames"] == 0
    lost = count - len(found)
    print(
        f"\ncFS generator at 10k/s: {len(found)}/{count} recorded, {lost} lost, "
        f"SB pipe overflows {result['sb_overflow']}, largest burst {status['MaxBurst']}"
    )
    assert lost >= 0
    # every missing packet is a software-bus overflow inside cFS; other MIDs overflow too, so the
    # counter can exceed the loss by a few
    assert 0 <= result["sb_overflow"] - lost <= 100, (lost, result["sb_overflow"])


def test_the_generator_validates_its_commands(generator_output: Ground) -> None:
    g = generator_output
    before = _hk(g)

    def errors() -> int:
        return (_counter(_hk(g), "err_counter") - _counter(before, "err_counter")) % 256

    g.send("IB_TEST_GEN_START", "Rate=0", "Count=10")
    g.send("IB_TEST_GEN_START", "Rate=10001", "Count=10")
    g.send("IB_TEST_GEN_STOP")  # no run is on
    assert errors() == 3

    mark = g.count()
    g.send("IB_TEST_GEN_START", "Rate=100", "Count=0")  # until stopped
    g.send("IB_TEST_GEN_START", "Rate=100", "Count=0")  # a run is on: refused
    assert errors() == 4
    g.send("IB_TEST_GEN_STOP")
    stopped = g.wait_for(
        "IB_TEST_GEN_STATUS",
        lambda f: f["State"] == STATE_STOPPED,
        start=mark,
        request="IB_TEST_GEN_SEND_STATUS",
    )["fields"]
    assert stopped["Rate"] == 100 and stopped["Target"] == 0 and stopped["Sent"] >= 1
    sent = stopped["Sent"]
    again = g.fresh("IB_TEST_GEN_STATUS", "IB_TEST_GEN_SEND_STATUS")
    assert again["Sent"] == sent and again["State"] == STATE_STOPPED  # stopped means stopped
    assert errors() == 4
