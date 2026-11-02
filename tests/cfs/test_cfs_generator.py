"""The ``ib_test`` throughput source against a real cFS instance.

The app's generator (``IB_TEST_GEN_START``) publishes fixed-size 200-byte telemetry at a rate up to
10k packets/s. These tests check its contract (a run publishes exactly ``Count`` packets with
consecutive serials, a filler derived from the serial, and a status packet; its commands are
validated) and what ironbird makes of the stream:

* at 1k packets/s ironbird loses nothing (no GAP of any reason, kernel drops included, nothing out
  of order) and the generator never overruns TO_LAB's telemetry pipe by itself. That pipe is
  ``fs.mqueue.msg_max`` deep at most (10 on a default Linux host: OSAL truncates every queue a
  non-root cFS creates to it), not the depth TO_LAB asks for (1000 in this build:
  ``tools/cfs/sample_defs``), so on a default host it covers 10 ms of TO_LAB's task being off-CPU at
  1k packets/s. The generator's catch-up after its own stall is limited to what fits in the pipe
  (``IB_TEST_GEN_BURST_LIMIT``, asserted at compile time in ``ib_test.h``, and here through the
  run's ``MaxBurst``); a packet still missing was dropped by cFS's software bus while TO_LAB was
  off-CPU for longer than the pipe covers, a host-scheduling loss outside ironbird that SB
  housekeeping counts (``PipeOverflowErrorCounter``, plus ``MsgLimitErrorCounter`` when the
  subscription's own limit is hit first), and the test requires those counts to cover it. The
  generator's two messages are subscribed by TO_LAB's subscription table
  (``tools/cfs/sample_defs/tables/cpu1_to_lab_sub.c``) with a per-message limit equal to the pipe's
  depth, so the pipe alone bounds the stream. When the
  cFS console shows the pipe at the full depth TO_LAB asks for (``msg_max`` of at least that, which
  CI sets), the test is strict instead: zero SB drops and zero packets missing;
* at 10k packets/s ``ib record`` loses nothing silently: no duplicate, nothing out of order, no
  recorder-buffer or capture-queue loss, and every packet missing from the recording is accounted
  for, either by cFS (a pipe truncated to a default ``fs.mqueue.msg_max`` of 10 is overflowed by the
  software bus when the forwarding task is descheduled for a millisecond or so; SB housekeeping
  counts those drops in ``PipeOverflowErrorCounter`` and ``MsgLimitErrorCounter``) or by a
  ``kernel_drop`` GAP record in the recording itself, which a loaded CI runner can produce. At the
  full pipe depth the test is strict about cFS too: the pipe, and so each of the generator's
  subscriptions, holds 100 ms of the stream, so the software bus drops nothing, and every packet
  missing is a recorded kernel drop.

With ``IRONBIRD_CFS_REQUIRE_FULL_DEPTH=1`` (set by CI) both tests fail, naming
``fs.mqueue.msg_max``, instead of running in their lenient mode when the pipe is shallower than
TO_LAB asks for, so a CI host whose setting did not take effect cannot pass unnoticed.

Each test prints which mode it ran in ("strict" or "accounting").

Opt-in (marker ``cfs``), like the other tests here. The gate with a loopback blaster, for ironbird
alone, is ``tests/bench``.
"""

from __future__ import annotations

import functools
import itertools
import os
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

from conftest import CFS_BUILD_DIR, REPO
from ironbird.recording.reader import RecordingReader

pytestmark = pytest.mark.cfs

IB_TEST_HEADER_IN_REPO = REPO / "tools" / "cfs" / "apps" / "ib_test" / "fsw" / "src" / "ib_test.h"
# The source cFS was built from (staged into the build tree), when the build tree has it
IB_TEST_HEADER_IN_BUILD = CFS_BUILD_DIR / "src" / "apps" / "ib_test" / "fsw" / "src" / "ib_test.h"
# What the OSAL BSP prints when it truncates every queue to fs.mqueue.msg_max (a non-root cFS)
QUEUE_DEPTH_LINE = re.compile(rb"Maximum user msg queue depth = (\d+)")

# The cFS build tree this run uses, and what it was configured with: TO_LAB's telemetry pipe depth
# is TO_LAB_PLATFORM_TLM_PIPE_DEPTH (tools/cfs/sample_defs/cpu1_to_lab_internal_cfg_values.h, which
# build.sh stages into the checkout), never more than OSAL's OS_QUEUE_MAX_DEPTH
# (sample_defs/cpu1_osconfig.cmake). Either falls back to the other's default when not overridden.
CFS_CPU_BUILD = (
    CFS_BUILD_DIR
    / "src"
    / f"build-{os.environ.get('CFS_CONFIG', 'native_std')}"
    / "native"
    / "default_cpu1"
)
OS_QUEUE_MAX_DEPTH_DEFINE = re.compile(r"^#define\s+OS_QUEUE_MAX_DEPTH\s+(\d+)\b", re.M)
TO_LAB_TLM_PIPE_DEPTH_DEFINE = re.compile(
    r"^#define\s+IRONBIRD_TO_LAB_CFGVAL_TLM_PIPE_DEPTH\s+(\d+)\b", re.M
)
# The BufLimit a housekeeping subscription made with TO_LAB_ADD_PKT asks for (a uint8 there)
HK_BUF_LIMIT = 8
# Set to 1 by CI: a pipe shallower than TO_LAB asks for fails the generator tests
REQUIRE_FULL_DEPTH_ENV = "IRONBIRD_CFS_REQUIRE_FULL_DEPTH"

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
    """TO_LAB forwarding the generator's two packets, which its subscription table subscribes at
    start-up (cpu1_to_lab_sub.c: the per-message limit is the pipe's depth, more than
    TO_LAB_ADD_PKT can ask for), and the app's housekeeping, told to it here (once per cFS
    instance)."""
    if not getattr(ground, "generator_added", False):
        ground.send(
            "TO_LAB_ADD_PKT",
            f"Stream={IB_TEST_HK_MID:#x}",
            "QosPriority=0",
            "QosReliability=0",
            f"BufLimit={HK_BUF_LIMIT}",
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

    def grown(counter: str) -> int:
        return (
            int(summary[-1]["fields"][counter]) - int(summary[0]["fields"][counter])
        ) % 65536  # a 16-bit counter

    # The software bus drops a packet in two ways, each with its own counter: the destination
    # pipe is full (PipeOverflowErrorCounter), or the pipe holds as many of that message as the
    # subscription allows (MsgLimitErrorCounter; the generator's subscriptions allow the pipe's
    # whole depth, so only the pipe itself limits them).
    sb_dropped = grown("PipeOverflowErrorCounter") + grown("MsgLimitErrorCounter")
    return {"out": out, "env": env, "status": done["fields"], "sb_overflow": sb_dropped}


@functools.cache
def _to_lab_pipe_depth() -> int:
    """The depth of the telemetry pipe TO_LAB asks cFE for, as this cFS build was configured. A
    pipe the OSAL BSP truncated below it can overflow under host load; one at least this deep is
    the pipe as cFE configured it."""
    osconfig = CFS_CPU_BUILD / "osal" / "inc" / "osconfig.h"
    wrapper = CFS_CPU_BUILD / "inc" / "to_lab_internal_cfg_values.h"
    assert osconfig.is_file() and wrapper.is_file(), (
        f"{osconfig} or {wrapper} not found: the cFS build tree is missing or incomplete "
        "(tools/cfs/build.sh)"
    )
    found = OS_QUEUE_MAX_DEPTH_DEFINE.search(osconfig.read_text())
    assert found is not None, f"OS_QUEUE_MAX_DEPTH not defined in {osconfig}"
    depth = int(found.group(1))
    # the generated wrapper includes the file cFS picked for TO_LAB's values
    for included in re.findall(r'^#include "([^"]+)"', wrapper.read_text(), re.M):
        override = TO_LAB_TLM_PIPE_DEPTH_DEFINE.search(Path(included).read_text())
        if override is not None:
            depth = min(depth, int(override.group(1)))
    return depth


@functools.cache
def _generator_define(name: str) -> int:
    """A numeric ``#define`` of the generator's header: the copy in the cFS build tree (the source
    cFS was built from) when it exists, else the repository's."""
    header = (
        IB_TEST_HEADER_IN_BUILD if IB_TEST_HEADER_IN_BUILD.is_file() else IB_TEST_HEADER_IN_REPO
    )
    match = re.search(rf"^#define\s+{name}\s+(\d+)\b", header.read_text(), re.M)
    assert match is not None, f"{name} not defined in {header}"
    return int(match.group(1))


def _burst_limit(rate: int) -> int:
    """``IB_TEST_GEN_BURST_LIMIT(rate)``: the most packets one tick publishes at ``rate``."""
    share = -(-rate * _generator_define("IB_TEST_GEN_TICK_MS") // 1000)  # rounded up
    return min(
        _generator_define("IB_TEST_GEN_CATCHUP_TICKS") * share,
        _generator_define("IB_TEST_GEN_MAX_BURST"),
    )


def _queue_depth_limit(console: Path) -> int:
    """The depth this cFS truncated every queue to (``fs.mqueue.msg_max``, as its OSAL BSP read
    it). cFS runs non-root here, so OSAL always prints it (needs a build with ``DEBUG_PRINTF``);
    without the line the pipe depth is unknown and truncation may still apply."""
    found = QUEUE_DEPTH_LINE.findall(console.read_bytes())
    assert len(found) == 1, (
        f"the cFS console has no 'Maximum user msg queue depth' line ({len(found)} found in "
        f"{console}): the pipe depth is unknown (a cFS built without DEBUG_PRINTF does not print "
        "it, and OSAL may still truncate every queue to fs.mqueue.msg_max)"
    )
    return int(found[0])


def _strict_mode(queue_depth_limit: int) -> bool:
    """Whether the pipe is as deep as TO_LAB asks for, so cFS itself must lose nothing."""
    return queue_depth_limit >= _to_lab_pipe_depth()


def _require_full_depth_if_asked(queue_depth_limit: int) -> None:
    """Fail, naming the setting, when CI demands the full pipe depth and this cFS lacks it."""
    if os.environ.get(REQUIRE_FULL_DEPTH_ENV) != "1":
        return
    assert _strict_mode(queue_depth_limit), (
        f"{REQUIRE_FULL_DEPTH_ENV}=1 but cFS truncated its queues to fs.mqueue.msg_max = "
        f"{queue_depth_limit}, below the {_to_lab_pipe_depth()} deep telemetry pipe TO_LAB asks "
        "for: raise fs.mqueue.msg_max on this host (CI: the sysctl step or the container's "
        "--sysctl option in .github/workflows/ci.yml did not take effect)"
    )


def _mode_line(limit: int) -> str:
    """Which checks a test ran: strict (cFS must lose nothing) or accounting (every loss must be
    counted by the software bus or recorded as a kernel drop)."""
    depth = _to_lab_pipe_depth()
    if _strict_mode(limit):
        return f"strict (queue depth limit {limit} >= {depth}: no SB drop)"
    return (
        f"accounting (queue depth limit {limit} < {depth}: loss must be counted by SB or "
        "recorded as a kernel drop)"
    )


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
    limit = _queue_depth_limit(cfs)
    # the pipe is at least as deep as the generator is designed for (a host whose msg_max is below
    # the Linux default would make every pipe shallower than that)
    assert limit >= min_depth, (
        f"cFS truncated its queues to fs.mqueue.msg_max = {limit}, below the {min_depth} the "
        "ib_test generator is designed for; raise fs.mqueue.msg_max on this host"
    )
    _require_full_depth_if_asked(limit)
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
    print(f"\ncFS generator at {rate}/s, mode {_mode_line(limit)}: {summary}")
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
    # the generator never published more in one tick than its burst limit at this rate, which fits
    # the pipe beside its reserve (asserted at compile time), so it cannot have overflowed the pipe
    # by itself
    assert 1 <= status["MaxBurst"] <= _burst_limit(rate), summary
    if _strict_mode(limit):
        # the pipe is as deep as TO_LAB asks for: cFS overflows nothing and nothing is missing
        assert result["sb_overflow"] == 0, summary
        assert lost == 0, summary
    else:
        # what is missing, cFS's software bus dropped and counted (TO_LAB's task was off-CPU for
        # longer than its pipe covers; the counter also counts other telemetry, so it can exceed
        # the loss)
        assert 0 <= lost <= result["sb_overflow"], summary
    serial = found[len(found) // 2][1]
    one = _replayed(IB, result["out"], result["env"], f"msg == IB_TEST_GEN and Serial == {serial}")
    assert len(one) == 1  # the codec decodes the packet the raw bytes describe
    assert one[0]["fields"]["Serial"] == serial and one[0]["fields"]["Run"] == status["Run"]


def test_ten_thousand_packets_a_second_are_not_lost_silently_by_ironbird(
    cfs: Path, generator_output: Ground, tmp_path: Path
) -> None:
    count = 30_000
    limit = _queue_depth_limit(cfs)
    _require_full_depth_if_asked(limit)
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
        f"\ncFS generator at 10k/s, mode {_mode_line(limit)}: "
        f"{len(found)}/{count} recorded, {lost} lost, "
        f"SB pipe overflows {result['sb_overflow']}, kernel drops {kernel}, "
        f"largest burst {status['MaxBurst']}"
    )
    if _strict_mode(limit):
        # the pipe is as deep as TO_LAB asks for (100 ms of this stream) and the generator's
        # subscriptions allow all of it: the software bus drops nothing, and a packet missing is a
        # kernel drop that ironbird recorded
        assert result["sb_overflow"] == 0, (lost, result["sb_overflow"], kernel)
        assert 0 <= lost <= kernel, (lost, result["sb_overflow"], kernel)
    else:
        # every missing packet is a software-bus drop inside cFS (the counters also count other
        # telemetry, so they can exceed the loss) or a kernel drop that ironbird recorded
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
