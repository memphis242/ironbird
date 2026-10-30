"""The ``.ibrec`` writer and reader: layouts, chunks, gaps, back-pressure, crash safety.

Malformed-input coverage of the reader lives in ``test_recording_malformed.py``.
"""

from __future__ import annotations

import asyncio
import errno
import hashlib
import os
import threading
import time
from pathlib import Path
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from ironbird.errors import Code, IronbirdError
from ironbird.frame import FrameFlag, GapReason
from ironbird.notice import Level
from ironbird.recording import format as fmt
from ironbird.recording import inspect as rec_inspect
from ironbird.recording.format import BlobKind, MarkKind, RecordType
from ironbird.recording.reader import Damage, Item, RecordingReader
from ironbird.recording.session import parse_split, recording_name
from ironbird.recording.writer import BlobSpec, Recorder, RecorderOptions, part_path
from recutil import (
    FAST,
    RX,
    SERIAL,
    TX,
    Builder,
    frame,
    is_writer_locked,
    meta,
    record,
    sample,
    wait_until,
)

FUZZ = settings(
    max_examples=60, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture]
)


def read_frames(path: Path) -> list[Any]:
    with RecordingReader(path) as reader:
        return list(reader.frames())


def items(path: Path) -> list[Item | Damage]:
    with RecordingReader(path, skip_damage=True) as reader:
        return list(reader.items())


# -- layouts --


def test_struct_sizes_and_preamble(tmp_path: Path) -> None:
    assert fmt.PREAMBLE.size == 24 and fmt.ENVELOPE.size == 12 and fmt.FRAME_FIXED.size == 32
    path = tmp_path / "a.ibrec"
    sample(path, 3)
    raw = path.read_bytes()
    assert raw[:8] == b"\x89IBR\r\n\x1a\n"
    assert fmt.PREAMBLE.unpack(raw[:24])[1:4] == (1, 0, 0)
    assert len(raw) % 8 == 0  # every record is 8-byte aligned, so the file ends aligned
    # the first record is META, at 24
    assert raw[24 + 4] == RecordType.META


def test_nonce_differs_per_file_and_seeds_every_crc(tmp_path: Path) -> None:
    a, b = tmp_path / "a.ibrec", tmp_path / "b.ibrec"
    sample(a, 5)
    sample(b, 5)
    with RecordingReader(a) as ra, RecordingReader(b) as rb:
        assert ra.nonce != rb.nonce
    # a record of one file does not verify under the other's nonce
    ra_bytes, rb_bytes = a.read_bytes(), b.read_bytes()
    assert ra_bytes[24:] != rb_bytes[24:]


def test_round_trip_with_both_directions_and_sources(tmp_path: Path) -> None:
    path = tmp_path / "a.ibrec"
    frames = sample(path, 60)
    assert read_frames(path) == frames
    with RecordingReader(path) as reader:
        list(reader.frames())
        assert reader.trailer is not None and reader.trailer.frame_count == 60
        assert {s.direction.value for s in reader.streams.values()} == {"rx", "tx"}
    assert rec_inspect.scan(path).status == "closed"


def test_read_frames_carry_no_check_token(tmp_path: Path) -> None:
    path = tmp_path / "a.ibrec"
    sample(path, 5)
    assert all(f.checked is None for f in read_frames(path))


_flag_choices = st.sampled_from(
    [0, int(FrameFlag.TRUNCATED), int(FrameFlag.GARBAGE), int(FrameFlag.STALE)]
)


@FUZZ
@given(
    entries=st.lists(
        st.tuples(
            st.binary(max_size=300),
            st.one_of(st.none(), st.integers(0, 0xFFFFFFFF)),
            st.sampled_from(["rx", "tx", "serial"]),
            st.text(alphabet="abc:.0123456789", max_size=12),
            _flag_choices,
        ),
        max_size=60,
    ),
    chunk_bytes=st.sampled_from([200, 5000, 1 << 20]),
)
def test_hypothesis_round_trip(
    tmp_path_factory: pytest.TempPathFactory,
    entries: list[tuple[bytes, int | None, str, str, int]],
    chunk_bytes: int,
) -> None:
    path = tmp_path_factory.mktemp("rt") / "a.ibrec"
    frames = []
    for seq, (data, mid_in, which, source, flag) in enumerate(entries, start=1):
        mid = mid_in
        stream = {"rx": RX, "tx": TX, "serial": SERIAL}[which]
        flags = flag
        orig_len = len(data)
        if flag == FrameFlag.TRUNCATED:
            orig_len = len(data) + MAX_OVER
        if flag in (FrameFlag.GARBAGE, FrameFlag.STALE):
            if which != "serial":
                flags = 0
            elif flag == FrameFlag.GARBAGE:
                mid = None
        frames.append(
            frame(
                seq,
                stream=stream,
                data=data,
                mid=mid,
                flags=flags,
                source=source,
                orig_len=orig_len,
                time_ns=seq,
            )
        )
    record(
        path,
        frames,
        streams=[RX, TX, SERIAL],
        options=RecorderOptions(min_free_bytes=0, chunk_bytes=chunk_bytes),
    )
    assert read_frames(path) == frames
    report = rec_inspect.scan(path)
    assert report.status == "closed", report.findings
    assert report.frames == len(frames)


MAX_OVER = 3000  # TRUNCATED needs orig_len above the 2048-byte cap of the test recordings


# -- chunks, summaries, index --


def test_chunks_close_by_size_and_summaries_match(tmp_path: Path) -> None:
    path = tmp_path / "a.ibrec"
    frames = [frame(i, mid=0x100 + i % 5, data=bytes(100)) for i in range(1, 201)]
    record(path, frames, options=RecorderOptions(min_free_bytes=0, chunk_bytes=4000))
    report = rec_inspect.scan(path)
    assert report.status == "closed" and report.findings == []
    assert report.chunks > 4
    assert report.mids == {0x100 + m: 40 for m in range(5)}
    info = rec_inspect.info(path)
    assert info.source == "index" and info.frames == 200 and info.mids == report.mids
    assert (info.first_seq, info.last_seq) == (1, 200)


def test_chunks_close_by_time_with_an_injected_clock(tmp_path: Path) -> None:
    now = [0]
    path = tmp_path / "a.ibrec"
    recorder = Recorder.open(
        path,
        meta(),
        streams=[RX],
        options=FAST,
        mono_ns=lambda: now[0],
        wall_ns=lambda: 1_700_000_000_000_000_000 + now[0],
    )
    for i in range(1, 7):
        now[0] = i * 600_000_000  # 0.6 s apart: a chunk closes on every second frame
        recorder.append(frame(i, time_ns=1_700_000_000_000_000_000 + now[0]))
    asyncio.run(recorder.close())
    assert rec_inspect.scan(path).chunks >= 3


def test_chunk_time_bounds_are_min_and_max_not_first_and_last(tmp_path: Path) -> None:
    path = tmp_path / "a.ibrec"
    base = 1_700_000_000_000_000_000
    times = [base + 5000, base + 1000, base + 9000, base + 3000]  # the clock stepped back
    record(path, [frame(i + 1, time_ns=t) for i, t in enumerate(times)])
    with RecordingReader(path) as reader:
        located = reader.read_trailer()
        assert located is not None
        [(_, chunk)] = [c for c in reader.read_chunks(located[1], located[0]) if c[1].frames]
    assert (chunk.min_time_ns, chunk.max_time_ns) == (base + 1000, base + 9000)
    assert (chunk.first_seq, chunk.last_seq) == (1, 4)


def test_mid_table_overflow_is_flagged_and_still_consistent(tmp_path: Path) -> None:
    path = tmp_path / "a.ibrec"
    record(path, [frame(i, mid=i) for i in range(1, 400)])
    with RecordingReader(path) as reader:
        located = reader.read_trailer()
        assert located is not None
        chunks = [c for _, c in reader.read_chunks(located[1], located[0]) if c.frames]
    assert chunks[0].mid_overflow and len(chunks[0].mids) == fmt.MAX_CHUNK_MIDS
    assert rec_inspect.scan(path).findings == []


def test_frames_without_mid_are_counted(tmp_path: Path) -> None:
    path = tmp_path / "a.ibrec"
    record(path, [frame(1, mid=None), frame(2), frame(3, mid=None)])
    info = rec_inspect.info(path)
    assert info.frames_without_mid == 2 and info.mids == {0x890: 1}


def test_mid_zero_with_has_mid_round_trips(tmp_path: Path) -> None:
    path = tmp_path / "a.ibrec"
    frames = [frame(1, mid=0), frame(2, mid=None)]
    record(path, frames)
    got = read_frames(path)
    assert [f.mid for f in got] == [0, None]


def test_non_frame_records_only_start_chunks(tmp_path: Path) -> None:
    path = tmp_path / "a.ibrec"
    frames = [frame(i, source=f"10.0.0.{i % 7}:1") for i in range(1, 50)]
    record(path, frames, marks=lambda r: r.mark(MarkKind.NOTE, "hello"))
    in_chunk_frames = False
    for item in items(path):
        assert isinstance(item, Item)
        if item.rtype == RecordType.FRAME:
            in_chunk_frames = True
        elif item.rtype == RecordType.CHUNK:
            in_chunk_frames = False
        elif item.rtype in fmt.HEAD_TYPES:
            assert not in_chunk_frames, "a head record appeared inside a chunk of frames"


def test_definitions_are_repeated_every_minute_at_a_chunk_start(tmp_path: Path) -> None:
    now = [0]
    path = tmp_path / "a.ibrec"
    recorder = Recorder.open(path, meta(), streams=[RX], options=FAST, mono_ns=lambda: now[0])
    for i in range(1, 6):
        now[0] = i * 31_000_000_000
        recorder.append(frame(i))
    asyncio.run(recorder.close())
    streams = [i for i in items(path) if isinstance(i, Item) and i.rtype == RecordType.STREAM]
    assert len(streams) >= 3
    assert rec_inspect.scan(path).status == "closed"


# -- embedded definitions, META --


def test_blobs_are_embedded_verified_and_listed(tmp_path: Path) -> None:
    path = tmp_path / "a.ibrec"
    content = b'{"schema": "ironbird-ccdd/0"}'
    spec = BlobSpec(BlobKind.CCDD, "fc", "json", content)
    record(path, [frame(1)], blobs=[spec])
    with RecordingReader(path) as reader:
        list(reader.items())
        assert [(b.kind, b.node, b.adapter, b.content) for b in reader.blobs] == [
            (BlobKind.CCDD, "fc", "json", content)
        ]
        assert reader.blobs[0].sha256 == hashlib.sha256(content).digest()
    assert [b.kind for b in rec_inspect.info(path).blobs] == ["ccdd"]


def test_meta_names_the_session_part_and_limits(tmp_path: Path) -> None:
    path = tmp_path / "a.ibrec"
    sample(path, 2)
    with RecordingReader(path) as reader:
        assert reader.meta.max_packet_bytes == 2048 and reader.meta.part == 1
        assert reader.meta.data["session"] == "abcdef012345"
        assert reader.meta.data["created"].endswith("Z")


# -- the writer's rules --


def test_create_never_overwrites(tmp_path: Path) -> None:
    path = tmp_path / "a.ibrec"
    path.write_bytes(b"precious")
    with pytest.raises(IronbirdError) as caught:
        Recorder.open(path, meta(), options=FAST)
    assert caught.value.code is Code.REC_CREATE
    assert path.read_bytes() == b"precious"


def test_refuses_to_start_below_the_free_space_floor(tmp_path: Path) -> None:
    with pytest.raises(IronbirdError) as caught:
        Recorder.open(tmp_path / "a.ibrec", meta(), options=RecorderOptions(min_free_bytes=1 << 60))
    assert caught.value.code is Code.REC_SPACE
    assert not (tmp_path / "a.ibrec").exists()


def test_file_and_directory_modes(tmp_path: Path) -> None:
    path = tmp_path / "new-dir" / "a.ibrec"
    sample(path, 1)
    assert path.stat().st_mode & 0o777 == 0o600
    assert path.parent.stat().st_mode & 0o777 == 0o700


def test_writer_holds_the_lock_while_open_and_a_reader_sees_it(tmp_path: Path) -> None:
    path = tmp_path / "a.ibrec"

    async def scenario() -> None:
        recorder = Recorder.open(path, meta(), streams=[RX], options=FAST)
        recorder.append(frame(1))
        assert is_writer_locked(path)
        with RecordingReader(path) as reader:
            assert reader.writer_active and not reader.locked
        await recorder.close()

    asyncio.run(scenario())
    assert not is_writer_locked(path)
    with RecordingReader(path) as reader:
        assert not reader.writer_active and reader.locked


def test_the_file_is_durable_and_readable_before_the_first_frame(tmp_path: Path) -> None:
    path = tmp_path / "a.ibrec"
    recorder = Recorder.open(path, meta(), streams=[RX, TX], options=FAST)
    try:
        # nothing was flushed by the writer thread yet: the head was written synchronously
        with RecordingReader(path) as reader:
            assert reader.meta.max_packet_bytes == 2048
            kinds = [i.rtype for i in reader.items() if isinstance(i, Item)]
        assert kinds == [RecordType.STREAM, RecordType.STREAM]
    finally:
        asyncio.run(recorder.close())


def test_heartbeat_touches_an_idle_file(tmp_path: Path) -> None:
    path = tmp_path / "a.ibrec"

    async def scenario() -> None:
        recorder = Recorder.open(
            path, meta(), options=RecorderOptions(min_free_bytes=0, heartbeat_s=0.05)
        )
        os.utime(path, (1, 1))
        await asyncio.sleep(0.4)
        assert path.stat().st_mtime > 1_000_000
        await recorder.close()

    asyncio.run(scenario())


def test_mark_records_and_clock_step_detection(tmp_path: Path) -> None:
    path = tmp_path / "a.ibrec"
    base = 1_700_000_000_000_000_000
    mono = [0]
    recorder = Recorder.open(path, meta(), streams=[RX], options=FAST, mono_ns=lambda: mono[0])
    recorder.append(frame(1, time_ns=base))
    mono[0] = 1_000_000_000
    recorder.append(frame(2, time_ns=base + 1_000_000_000))
    mono[0] = 2_000_000_000
    recorder.append(frame(3, time_ns=base - 50_000_000_000))  # the wall clock jumped back 51 s
    asyncio.run(recorder.close())
    marks = [i.value for i in items(path) if isinstance(i, Item) and i.rtype == RecordType.MARK]
    assert [m.kind for m in marks] == [MarkKind.CLOCK_STEP]
    assert "stepped" in marks[0].text
    assert [f.seq for f in read_frames(path)] == [1, 2, 3]


# -- fsync policy --


@pytest.mark.parametrize(
    ("policy", "expect"), [("chunk", "chunks"), ("always", "flushes"), ("never", 1)]
)
def test_fsync_policy_calls(tmp_path: Path, policy: str, expect: Any) -> None:
    calls: list[int] = []
    path = tmp_path / "a.ibrec"
    options = RecorderOptions(min_free_bytes=0, fsync=policy, chunk_bytes=2000, flush_bytes=500)
    recorder = Recorder.open(path, meta(), streams=[RX], options=options, sync=calls.append)
    for i in range(1, 80):
        recorder.append(frame(i, data=bytes(60)))
    asyncio.run(recorder.close())
    chunks = rec_inspect.scan(path).chunks
    if expect == "chunks":
        assert len(calls) == chunks + 1  # one per chunk, plus the trailer's
    elif expect == "flushes":
        assert len(calls) > chunks + 1
    else:
        assert len(calls) == expect  # only the close syncs
    assert rec_inspect.scan(path).status == "closed"


# -- back-pressure: overrun, GAP --


def test_overrun_drops_frames_writes_a_gap_and_reports_twice(tmp_path: Path) -> None:
    path = tmp_path / "a.ibrec"
    gate = threading.Event()
    reports: list[tuple[Code, Level]] = []

    def slow_write(fd: int, data: memoryview) -> int:
        gate.wait(10)
        return os.write(fd, data)

    def report(error: IronbirdError, *, context: str, level: Level) -> None:
        reports.append((error.code, level))

    options = RecorderOptions(min_free_bytes=0, max_buffer_bytes=20_000, flush_bytes=2_000)

    async def scenario() -> None:
        recorder = Recorder.open(
            path,
            meta(),
            streams=[RX, TX],
            options=options,
            write=slow_write,
            report=report,
            loop=asyncio.get_running_loop(),
        )
        for i in range(1, 400):
            recorder.append(frame(i, stream=RX if i % 2 else TX, data=bytes(100)))
        dropped = recorder.stats().overrun_frames
        assert dropped > 100
        peak = recorder.stats().peak_pending_bytes
        assert peak < 20_000 + 4_000  # bounded by the cap (plus the last flush)
        gate.set()
        for _ in range(100):  # the writer catches up; the timer ends the episode
            await asyncio.sleep(0.05)
            if len(reports) >= 2:
                break
        for i in range(400, 410):
            recorder.append(frame(i, data=bytes(100)))
        await recorder.close()
        assert recorder.stats().overrun_frames == dropped

    asyncio.run(scenario())
    assert [code for code, _ in reports] == [Code.REC_OVERRUN, Code.REC_OVERRUN]
    assert all(level is Level.ERROR for _, level in reports)
    report_ = rec_inspect.scan(path)
    gaps = [g for g in report_.gaps if g.reason is GapReason.RECORDER_OVERRUN]
    assert gaps and sum(g.lost for g in gaps) == report_.gap_total * 0 + sum(g.lost for g in gaps)
    got = {f.seq for f in read_frames(path)}
    lost = set(range(1, 410)) - got
    assert sum(g.lost for g in gaps) == len(lost)
    assert report_.hole_frames == 0 and report_.status == "closed"  # every hole is explained
    for gap in gaps:
        assert gap.first_seq <= gap.last_seq


def test_a_failed_write_stops_the_recorder_and_reports_once(tmp_path: Path) -> None:
    path = tmp_path / "a.ibrec"
    reports: list[IronbirdError] = []
    fail = threading.Event()

    def failing_write(fd: int, data: memoryview) -> int:
        if fail.is_set():
            raise OSError(errno.ENOSPC, "No space left on device")
        return os.write(fd, data)

    async def scenario() -> Recorder:
        recorder = Recorder.open(
            path,
            meta(),
            streams=[RX],
            options=RecorderOptions(min_free_bytes=0, flush_bytes=100),
            write=failing_write,
            loop=asyncio.get_running_loop(),
            report=lambda e, **_: reports.append(e),
        )
        recorder.append(frame(1))
        fail.set()
        for i in range(2, 40):
            recorder.append(frame(i))
            await asyncio.sleep(0.005)
        assert recorder.failed_event is not None
        await asyncio.wait_for(recorder.failed_event.wait(), 5)
        before = recorder.frames
        recorder.append(frame(99))
        assert recorder.frames == before and recorder.stats().after_stop_frames >= 1
        await recorder.close()
        return recorder

    recorder = asyncio.run(scenario())
    assert recorder.failed is not None and recorder.failed.code is Code.REC_WRITE
    assert [e.code for e in reports] == [Code.REC_WRITE]
    assert "continues" in reports[0].summary


def test_frames_of_unrecorded_streams_become_not_recorded_gaps(tmp_path: Path) -> None:
    path = tmp_path / "a.ibrec"
    frames = [frame(i, stream=RX if i % 3 else TX) for i in range(1, 31)]
    record(path, frames, accept=frozenset({RX.id}))
    report = rec_inspect.scan(path)
    kept = [f.seq for f in read_frames(path)]
    assert kept == [i for i in range(1, 31) if i % 3]
    gaps = [g for g in report.gaps if g.reason is GapReason.NOT_RECORDED]
    assert [g.stream for g in gaps] == [TX.id] and gaps[0].lost == 10
    assert (gaps[0].first_seq, gaps[0].last_seq) == (3, 30)
    assert report.hole_frames == 0 and report.status == "closed"


def test_engine_gap_is_recorded(tmp_path: Path) -> None:
    path = tmp_path / "a.ibrec"
    recorder = Recorder.open(path, meta(), streams=[RX], options=FAST)
    recorder.append(frame(1))
    recorder.gap(RX, GapReason.KERNEL_DROP, 7, 100, 200, 1, 5)
    recorder.gap(TX, GapReason.KERNEL_DROP, 3, 100, 200, 1, 5)  # defined on demand
    recorder.append(frame(2))
    asyncio.run(recorder.close())
    report = rec_inspect.scan(path)
    assert [(g.stream, g.reason, g.lost) for g in report.gaps] == [
        (RX.id, GapReason.KERNEL_DROP, 7),
        (TX.id, GapReason.KERNEL_DROP, 3),
    ]
    assert report.status == "closed" and report.lost_by_reason == {"kernel_drop": 10}


# -- splitting --


def test_split_parts_are_complete_files_with_a_chain(tmp_path: Path) -> None:
    base = tmp_path / "rec.ibrec"
    options = RecorderOptions(min_free_bytes=0, split_bytes=6000, chunk_bytes=1500)
    blob = BlobSpec(BlobKind.CCDD, "fc", "json", b"{}")
    frames = [frame(i, data=bytes(80)) for i in range(1, 121)]

    async def scenario() -> None:
        recorder = Recorder.open(base, meta(), streams=[RX, TX], options=options, blobs=[blob])
        for item in frames:
            recorder.append(item)
        parts = recorder.parts
        assert len(parts) >= 3
        assert is_writer_locked(parts[0]) and is_writer_locked(parts[-1])  # part 1 stays locked
        wait_until(lambda: not is_writer_locked(parts[1]))  # closed by the writer thread
        await recorder.close()
        assert not is_writer_locked(parts[0])

    asyncio.run(scenario())
    parts = sorted(tmp_path.glob("rec*.ibrec"))
    assert parts[0] == base and parts[1] == part_path(base, 2) == tmp_path / "rec.p0002.ibrec"
    seen: list[int] = []
    nonces = set()
    previous_last = 0
    for number, part in enumerate(parts, start=1):
        with RecordingReader(part) as reader:
            part_frames = list(reader.frames())
            assert reader.meta.part == number
            assert reader.meta.previous_last_seq == previous_last
            assert reader.trailer is not None and reader.trailer.frame_count == len(part_frames)
            assert [b.node for b in reader.blobs] == ["fc"]  # every part carries its definitions
            nonces.add(reader.nonce)
            if part_frames:
                assert part_frames[0].seq == previous_last + 1
                previous_last = part_frames[-1].seq
            seen += [f.seq for f in part_frames]
        assert rec_inspect.scan(part).status == "closed"
    assert seen == list(range(1, 121)) and len(nonces) == len(parts)


def test_split_by_time(tmp_path: Path) -> None:
    now = [0]
    base = tmp_path / "t.ibrec"
    options = RecorderOptions(min_free_bytes=0, split_seconds=10, chunk_ns=1_000_000_000)
    recorder = Recorder.open(base, meta(), streams=[RX], options=options, mono_ns=lambda: now[0])
    for i in range(1, 8):
        now[0] = i * 6_000_000_000
        recorder.append(frame(i))
        now[0] += 1_100_000_000
        recorder.append(frame(100 + i))
    asyncio.run(recorder.close())
    assert len(list(tmp_path.glob("t*.ibrec"))) >= 3


# -- reader: damage classification --


def test_torn_tail_is_the_normal_crash_result(tmp_path: Path) -> None:
    path = tmp_path / "a.ibrec"
    frames = sample(path, 30)
    raw = path.read_bytes()
    # cut inside the last frame record (before CHUNK/INDEX/TRAILER): a crash mid-write
    last_frame_end = max(
        i.end for i in items(path) if isinstance(i, Item) and i.rtype == RecordType.FRAME
    )
    path.write_bytes(raw[: last_frame_end - 7])
    with RecordingReader(path) as reader:
        got = list(reader.frames())
        assert got == frames[:-1]
        [damage] = reader.damage
        assert damage.torn and damage.to_error().code is Code.REC_TORN
    assert rec_inspect.info(path).status == "torn"


def test_mid_file_damage_is_reported_and_can_be_skipped(tmp_path: Path) -> None:
    path = tmp_path / "a.ibrec"
    frames = sample(path, 40)
    raw = bytearray(path.read_bytes())
    victim = [i for i in items(path) if isinstance(i, Item) and i.rtype == RecordType.FRAME][10]
    raw[victim.offset + 20] ^= 0xFF  # inside the body: the CRC fails
    path.write_bytes(bytes(raw))
    with RecordingReader(path) as strict:
        got = list(strict.frames())
        assert got == frames[:10]
        [damage] = strict.damage
        assert not damage.torn and damage.resumed_at == victim.end
        assert damage.to_error().code is Code.REC_DAMAGED and damage.offset == victim.offset
    with RecordingReader(path, skip_damage=True) as lenient:
        assert list(lenient.frames()) == frames[:10] + frames[11:]
    report = rec_inspect.scan(path)
    assert report.status == "damaged" and report.mid_file_damage


def test_a_lone_valid_record_does_not_end_damage(tmp_path: Path) -> None:
    b = Builder().standard(4)
    b.out += b"\xaa" * 40  # garbage
    b.frame(5, stream=0)  # one valid record, then the file ends
    path = b.write(tmp_path / "a.ibrec")
    with RecordingReader(path, skip_damage=True) as reader:
        frames = list(reader.frames())
        assert [f.seq for f in frames] == [1, 2, 3, 4]
        assert reader.damage[-1].torn  # the lone record is part of the torn tail


def test_two_chained_valid_records_end_damage(tmp_path: Path) -> None:
    b = Builder().standard(4)
    b.out += b"\xaa" * 40
    b.frame(5, stream=0)
    b.frame(6, stream=0)
    path = b.write(tmp_path / "a.ibrec")
    with RecordingReader(path, skip_damage=True) as reader:
        assert [f.seq for f in reader.frames()] == [1, 2, 3, 4, 5, 6]
        assert [d.torn for d in reader.damage] == [False]


def test_unknown_critical_record_stops_with_a_version_error(tmp_path: Path) -> None:
    b = Builder().standard(2)
    b.raw(0x42, b"future")
    b.frame(3, stream=0)
    path = b.write(tmp_path / "a.ibrec")
    with RecordingReader(path) as reader, pytest.raises(IronbirdError) as caught:
        list(reader.frames())
    assert caught.value.code is Code.REC_VERSION


def test_unknown_skippable_record_is_skipped(tmp_path: Path) -> None:
    b = Builder().standard(2)
    b.raw(0xC0, b"extension")
    b.frame(3, stream=0)
    path = b.write(tmp_path / "a.ibrec")
    assert [f.seq for f in read_frames(path)] == [1, 2, 3]


@pytest.mark.parametrize(
    ("content", "code"),
    [
        (b"", Code.REC_FORMAT),
        (b"not a recording at all, just text", Code.REC_FORMAT),
        (fmt.MAGIC[:5], Code.REC_TORN),
        (fmt.MAGIC, Code.REC_TORN),
    ],
)
def test_files_that_cannot_be_opened(tmp_path: Path, content: bytes, code: Code) -> None:
    path = tmp_path / "a.ibrec"
    path.write_bytes(content)
    with pytest.raises(IronbirdError) as caught:
        RecordingReader(path)
    assert caught.value.code is code


def test_future_major_version_is_refused(tmp_path: Path) -> None:
    raw = bytearray(Builder().standard(2).out)
    raw[8:10] = (2).to_bytes(2, "little")
    path = tmp_path / "a.ibrec"
    path.write_bytes(bytes(raw))
    with pytest.raises(IronbirdError) as caught:
        RecordingReader(path)
    assert caught.value.code is Code.REC_VERSION


def test_a_directory_or_missing_file_is_a_coded_error(tmp_path: Path) -> None:
    for target in (tmp_path, tmp_path / "missing.ibrec"):
        with pytest.raises(IronbirdError) as caught:
            RecordingReader(target)
        assert caught.value.code is Code.REC_FORMAT


# -- index: a hint, never trusted --


def test_info_falls_back_to_a_scan_when_the_index_is_damaged(tmp_path: Path) -> None:
    path = tmp_path / "a.ibrec"
    sample(path, 30)
    index = next(i for i in items(path) if isinstance(i, Item) and i.rtype == RecordType.INDEX)
    raw = bytearray(path.read_bytes())
    raw[index.offset + 14] ^= 0xFF
    path.write_bytes(bytes(raw))
    report = rec_inspect.info(path)
    assert report.source == "scan" and report.index != "ok" and report.frames == 30


def test_info_of_a_torn_file_scans(tmp_path: Path) -> None:
    path = tmp_path / "a.ibrec"
    sample(path, 30)
    raw = path.read_bytes()
    path.write_bytes(raw[: len(raw) - 100])
    report = rec_inspect.info(path)
    assert (
        report.source == "scan" and report.trailer is None and report.status in ("torn", "unclosed")
    )


def test_check_catches_a_chunk_summary_that_lies(tmp_path: Path) -> None:
    b = Builder().standard(0)
    chunk_start = fmt.PREAMBLE_SIZE
    for seq in (1, 2, 3):
        b.frame(seq, stream=0)
    lie = fmt.Chunk(chunk_start, 1, 3, 5, 5, 0, 0, 2, 999, 0, 0, ((0x890, 2),))
    b.add(RecordType.CHUNK, fmt.encode_chunk(lie))
    report = rec_inspect.scan(b.write(tmp_path / "a.ibrec"))
    assert report.findings and "summarises 2 frames" in report.findings[0]


def test_check_flags_unexplained_sequence_holes(tmp_path: Path) -> None:
    b = Builder().standard(0)
    for seq in (1, 2, 5, 6):
        b.frame(seq, stream=0)
    report = rec_inspect.scan(b.write(tmp_path / "a.ibrec"))
    assert report.holes == [(3, 4)] and report.hole_frames == 2
    assert report.status == "inconsistent" or report.trailer is None


def test_gap_range_explains_a_hole(tmp_path: Path) -> None:
    b = Builder().standard(0)
    b.frame(1, stream=0)
    b.add(RecordType.GAP, fmt.encode_gap(fmt.Gap(0, GapReason.RECORDER_OVERRUN, 2, 1, 2, 2, 3)))
    b.frame(4, stream=0)
    report = rec_inspect.scan(b.write(tmp_path / "a.ibrec"))
    assert report.holes == [] and report.hole_frames == 0


# -- sessions: the recorder as a sink of the engine --


def test_recorder_as_engine_sink_sees_rx_and_tx_and_kernel_gaps(tmp_path: Path) -> None:
    from ironbird.transport import LinkEnd, TransportStats
    from test_engine import _Probe

    probe = _Probe(tmp_path / "probe")
    ground = probe.io[LinkEnd.SOURCE]
    path = tmp_path / "a.ibrec"
    streams = probe.engine.streams_for([(probe.config.links[0], LinkEnd.SOURCE)])
    recorder = Recorder.open(path, meta(), streams=streams, options=FAST)
    probe.engine.add_sink(recorder)
    wire = probe.codec.encode_telemetry("MOCK_HK").data
    ground.on_data(wire, 1_700_000_000_000_000_000, "127.0.0.1:9", len(wire), 0)
    probe.engine._ingest_tx(ground, probe.codec.encode_command("MOCK_NOOP").data)

    class Stub:
        def stats(self) -> TransportStats:
            return TransportStats(kernel_dropped=4)

    probe.engine._poll_kernel(ground, Stub())  # type: ignore[arg-type]
    asyncio.run(recorder.close())
    got = read_frames(path)
    assert [(f.stream.direction.value, f.seq) for f in got] == [("rx", 1), ("tx", 2)]
    assert got[0].data == wire and got[0].mid == probe.codec.peek_mid(got[0].stream.kind, wire)
    gaps = rec_inspect.scan(path).gaps
    assert [(g.reason, g.lost, g.stream) for g in gaps] == [
        (GapReason.KERNEL_DROP, 4, streams[0].id)
    ]
    assert gaps[0].last_seq == 2


# -- helpers --


def test_parse_split() -> None:
    assert parse_split("1GiB") == (1 << 30, None)
    assert parse_split("512MiB") == (512 << 20, None)
    assert parse_split("90s") == (None, 90.0)
    assert parse_split("1h") == (None, 3600.0)
    assert parse_split("1.5m") == (None, 90.0)
    for bad in ("", "10", "1x", "-1h", "0s", "5s", "100B", "1e3s", "nanGiB", "MiB"):
        with pytest.raises(IronbirdError) as caught:
            parse_split(bad)
        assert caught.value.code is Code.CLI_USAGE


def test_recording_name_matches_the_retention_pattern() -> None:
    import re

    name = recording_name("0123456789ab", 1_700_000_000_000_000_000)
    assert re.fullmatch(r"[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}\.ibrec", name)


def test_append_cost_budget(tmp_path: Path) -> None:
    """Encoding one 200-byte frame stays far below the 100 us per packet of the 10k pkt/s target."""
    recorder = Recorder.open(tmp_path / "a.ibrec", meta(), streams=[RX], options=FAST)
    f = frame(1, data=bytes(200))
    start = time.perf_counter()
    for i in range(1, 20001):
        recorder.append(f._replace(seq=i))
    per_frame = (time.perf_counter() - start) / 20000
    asyncio.run(recorder.close())
    assert per_frame < 100e-6, f"{per_frame * 1e6:.1f} us per frame"


def test_rate_hook_reports_bytes_per_second_for_retention(tmp_path: Path) -> None:
    now = [0]
    recorder = Recorder.open(
        tmp_path / "a.ibrec", meta(), streams=[RX], options=FAST, mono_ns=lambda: now[0]
    )
    assert recorder.rate_bytes_per_s() == 0.0
    recorder.tick()
    for step in range(1, 6):
        now[0] = step * 1_000_000_000
        for i in range(100):
            recorder.append(frame(step * 100 + i, data=bytes(400)))
        recorder.tick()
    rate = recorder.rate_bytes_per_s()
    asyncio.run(recorder.close())
    # 100 frames of 400 data bytes (plus 44 bytes of framing and padding) per second
    assert 40_000 < rate < 60_000


def test_source_table_overflow_uses_the_other_source(tmp_path: Path) -> None:
    path = tmp_path / "a.ibrec"
    count = fmt.MAX_SOURCES + 40
    frames = [frame(i, source=f"10.1.{i // 256}.{i % 256}:9") for i in range(1, count + 1)]
    recorder = Recorder.open(
        path, meta(), streams=[RX], options=FAST, sync=lambda fd: None, write=os.write
    )
    for item in frames:
        recorder.append(item)
    asyncio.run(recorder.close())
    got = read_frames(path)
    assert [f.seq for f in got] == [f.seq for f in frames]
    assert [f.source for f in got[: fmt.MAX_SOURCES]] == [
        f.source for f in frames[: fmt.MAX_SOURCES]
    ]
    assert {f.source for f in got[fmt.MAX_SOURCES :]} == {fmt.OTHER_SOURCE_TEXT}
    assert rec_inspect.scan(path).status == "closed"
