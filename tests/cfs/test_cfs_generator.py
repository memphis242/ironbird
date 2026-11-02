"""The ``ib_test`` throughput source against a real cFS instance.

The app's generator (``IB_TEST_GEN_START``) publishes fixed-size 200-byte telemetry at a rate up to
10k packets/s. These tests check its contract (a run publishes exactly ``Count`` packets with
consecutive serials, a filler derived from the serial, and a status packet; its commands are
validated) and what ironbird makes of the stream:

* at 1k packets/s ironbird loses nothing (no GAP of any reason, kernel drops included, nothing out
  of order) and the generator never overruns TO_LAB's telemetry pipe by itself. That pipe is
  ``fs.mqueue.msg_max`` deep (10 on a default Linux host: OSAL truncates every queue a non-root
  cFS creates to it), not the 50 cFE asks for, so it covers 10 ms of TO_LAB's task being off-CPU at
  1k packets/s. The generator's catch-up after its own stall is limited to what fits in the pipe
  (``IB_TEST_GEN_BURST_LIMIT``, asserted at compile time in ``ib_test.h``, and here through the
  run's ``MaxBurst``); a packet still missing was dropped by cFS's software bus while TO_LAB was
  off-CPU for longer than the pipe covers, a host-scheduling loss outside ironbird that SB
  housekeeping counts (``PipeOverflowErrorCounter``), and the test requires that count to cover it;
* at 10k packets/s ``ib record`` loses nothing silently: no duplicate, nothing out of order, no
  recorder-buffer or capture-queue loss, and every packet missing from the recording is accounted
  for, either by cFS (its software bus can overflow TO_LAB's pipe when the forwarding task is
  descheduled for a few milliseconds; SB housekeeping counts those drops in
  ``PipeOverflowErrorCounter``) or by a ``kernel_drop`` GAP record in the recording itself, which
  a loaded CI runner can produce. A run with zero kernel drops is enforced by the throughput gate
  (``tests/bench``), whose blaster is not subject to cFS's pipe.

Opt-in (marker ``cfs``), like the other tests here. The gate with a loopback blaster, for ironbird
alone, is ``tests/bench``.
"""

from __future__ import annotations

import functools
import itertools
import re
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

from conftest import REPO
from ironbird.recording.reader import RecordingReader

pytestmark = pytest.mark.cfs

IB_TEST_HEADER = REPO / "tools" / "cfs" / "apps" / "ib_test" / "fsw" / "src" / "ib_test.h"
# What the OSAL BSP prints when it truncates every queue to fs.mqueue.msg_max (a non-root cFS)
QUEUE_DEPTH_LINE = re.compile(rb"Maximum user msg queue depth = (\d+)")

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


@functools.cache
def _generator_define(name: str) -> int:
    """A numeric ``#define`` of the generator's header (the value cFS was built with)."""
    match = re.search(rf"^#define\s+{name}\s+(\d+)\b", IB_TEST_HEADER.read_text(), re.M)
    assert match is not None, f"{name} not defined in {IB_TEST_HEADER}"
    return int(match.group(1))


def _queue_depth_limit(console: Path) -> int | None:
    """The depth this cFS truncated every queue to (``fs.mqueue.msg_max``, as its OSAL BSP read
    it), or None when it did not truncate (cFS running as root: every pipe has its full depth)."""
    found = QUEUE_DEPTH_LINE.findall(console.read_bytes())
    assert len(found) <= 1, found  # printed once, at start-up
    return int(found[0]) if found else None


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


def test_one_thousand_packets_a_second_reach_the_recording_unless_cfs_counts_them_lost(
    cfs: Path, generator_output: Ground, tmp_path: Path
) -> None:
    rate, count = _generator_define("IB_TEST_GEN_FLOOR_RATE"), 3000
    min_depth = _generator_define("IB_TEST_GEN_MIN_PIPE_DEPTH")
    room = min_depth - _generator_define("IB_TEST_GEN_PIPE_RESERVE")
    limit = _queue_depth_limit(cfs)
    # the pipe is at least as deep as the generator is designed for (a host whose msg_max is below
    # the Linux default would make every pipe shallower than that)
    assert limit is None or limit >= min_depth, (
        f"cFS truncated its queues to fs.mqueue.msg_max = {limit}, below the {min_depth} the "
        "ib_test generator is designed for; raise fs.mqueue.msg_max on this host"
    )
    result = _generated(generator_output, tmp_path, "floor", rate, count)
    found = _packets(result["out"])
    status = result["status"]
    assert (status["Rate"], status["Target"], status["Sent"]) == (rate, count, count)
    assert status["State"] == STATE_FINISHED
    report = _info(result["env"], result["out"])
    lost = count - len(found)
    summary = (
        f"{lost} of {count} packets missing; SB pipe overflows {result['sb_overflow']}, "
        f"largest burst {status['MaxBurst']}, queue depth limit {limit}, "
        f"ironbird gaps {report['gaps']}"
    )
    print(f"\ncFS generator at {rate}/s: {summary}")
    # ironbird loses nothing: no GAP of any reason and none missing without one
    assert report["status"] == "closed" and report["gaps"]["total"] == 0, summary
    assert report["missing_frames"] == 0, summary
    assert found, summary
    # each recorded packet is the one its serial says, once, in order, and the sequence counts
    # advance by exactly the serials that are missing between two packets
    _assert_intact(found, int(status["Run"]))
    assert all(0 <= serial < count for _, serial, _, _ in found)
    assert all(
        (b[0] - a[0]) % 16384 == (b[1] - a[1]) % 16384 for a, b in itertools.pairwise(found)
    ), summary
    # the generator never published more in one tick than the pipe holds beside its reserve, so
    # it cannot have overflowed the pipe by itself
    assert 1 <= status["MaxBurst"] <= room, summary
    # what is missing, cFS's software bus dropped and counted (TO_LAB's task was off-CPU for longer
    # than its pipe covers; the counter also counts other telemetry, so it can exceed the loss)
    assert 0 <= lost <= result["sb_overflow"], summary
    serial = found[len(found) // 2][1]
    one = _replayed(IB, result["out"], result["env"], f"msg == IB_TEST_GEN and Serial == {serial}")
    assert len(one) == 1  # the codec decodes the packet the raw bytes describe
    assert one[0]["fields"]["Serial"] == serial and one[0]["fields"]["Run"] == status["Run"]


def test_ten_thousand_packets_a_second_are_not_lost_silently_by_ironbird(
    generator_output: Ground, tmp_path: Path
) -> None:
    count = 30_000
    result = _generated(generator_output, tmp_path, "target", 10_000, count)
    found = _packets(result["out"])
    status = result["status"]
    assert status["Sent"] == count and status["State"] == STATE_FINISHED
    _assert_intact(found, int(status["Run"]))
    report = _info(result["env"], result["out"])
    assert report["status"] == "closed"
    by_reason = report["gaps"]["lost_by_reason"]
    kernel = int(by_reason.get("kernel_drop", 0))
    # a frame ironbird lost is a recorded kernel drop; the recorder's buffer and the capture queue
    # lose none, and no frame is missing without a GAP record saying so
    assert set(by_reason) <= {"kernel_drop"}, by_reason
    assert report["missing_frames"] == 0
    lost = count - len(found)
    print(
        f"\ncFS generator at 10k/s: {len(found)}/{count} recorded, {lost} lost, "
        f"SB pipe overflows {result['sb_overflow']}, kernel drops {kernel}, "
        f"largest burst {status['MaxBurst']}"
    )
    # every missing packet is a software-bus overflow inside cFS (the counter also counts other
    # pipes, so it can exceed the loss) or a kernel drop that ironbird recorded
    assert 0 <= lost <= result["sb_overflow"] + kernel, (lost, result["sb_overflow"], kernel)


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


@pytest.mark.parametrize(
    ("command", "args", "delta"),
    [
        ("IB_TEST_GEN_START", ("Rate=100", "Count=10"), "-1"),
        ("IB_TEST_GEN_START", ("Rate=100", "Count=10"), "-4"),
        ("IB_TEST_GEN_STOP", (), "-1"),
        ("IB_TEST_GEN_SEND_STATUS", (), "-1"),
    ],
    ids=["start-1", "start-4", "stop-1", "send_status-1"],
)
def test_a_generator_command_of_the_wrong_length_is_rejected_and_not_run(
    generator_output: Ground, command: str, args: tuple[str, ...], delta: str
) -> None:
    """The header claims fewer bytes than the command has: the app counts an error, runs nothing
    (no run starts or stops, no status is published) and leaves the command counter alone."""
    g = generator_output
    status_before = g.fresh("IB_TEST_GEN_STATUS", "IB_TEST_GEN_SEND_STATUS")
    before = _hk(g)
    mark = g.count()

    done = g.send(command, *args, flags=("--length-delta", delta))
    assert "(mid 0x" in done.stdout  # ib itself reports the send as normal

    after = _hk(g)
    assert (_counter(after, "err_counter") - _counter(before, "err_counter")) % 256 == 1
    assert after["cmd_counter"] == before["cmd_counter"]  # nothing ran
    assert not g.since(mark, "IB_TEST_GEN"), "a rejected command started a run"
    assert not g.since(mark, "IB_TEST_GEN_STATUS"), "a rejected command published a status"
    status_after = g.fresh("IB_TEST_GEN_STATUS", "IB_TEST_GEN_SEND_STATUS")
    assert (status_after["Run"], status_after["State"], status_after["Sent"]) == (
        status_before["Run"],
        status_before["State"],
        status_before["Sent"],
    )
