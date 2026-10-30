"""Regressions from the PR 7 antagonistic review: reader work bounds, check's hole accounting,
what ``rec repair`` leaves behind, and bounded output. Each test is a concrete failing input."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import struct
import time
import zlib
from pathlib import Path

import pytest

from ironbird.errors import Code, IronbirdError
from ironbird.frame import GapReason
from ironbird.recording import format as fmt
from ironbird.recording import inspect as rec_inspect
from ironbird.recording.format import BlobKind, Chunk, RecordType, Trailer
from ironbird.recording.reader import RecordingReader
from ironbird.recording.render import info_json, info_lines, info_record
from ironbird.recording.writer import Recorder
from recutil import FAST, RX, TX, Builder, frame, meta, run_ib


class Meter:
    """Counts the bytes a reader preads and CRCs (installed with :func:`meter`)."""

    def __init__(self) -> None:
        self.read = 0
        self.crc = 0


@pytest.fixture
def meter(monkeypatch: pytest.MonkeyPatch) -> Meter:
    counts = Meter()
    real_pread, real_crc = os.pread, zlib.crc32

    def pread(fd: int, size: int, offset: int) -> bytes:
        data = real_pread(fd, size, offset)
        counts.read += len(data)
        return data

    def crc32(data: bytes | memoryview, start: int = 0) -> int:
        counts.crc += len(data)
        return real_crc(data, start)

    monkeypatch.setattr(os, "pread", pread)
    monkeypatch.setattr(zlib, "crc32", crc32)
    return counts


def chunk_of(
    start: int, frames: int, first: int, last: int, mids: tuple[tuple[int, int], ...] | None = None
) -> bytes:
    table = ((0x890, frames),) if mids is None else mids
    return fmt.encode_chunk(
        Chunk(start, first, last, 5, 5, 0, 0, frames, frames * fmt.record_size(52), 0, 0, table)
    )


# -- B1: a declared body that runs past the end of the file --


def test_resync_over_bodies_that_run_past_the_end_reads_linearly(
    tmp_path: Path, meter: Meter
) -> None:
    b = Builder()
    b.stream(RX)
    b.out += struct.pack("<IBBH", 0xFFFFF0, 0xFF, 0, 0) * (256 * 1024 // 8)
    path = b.write(tmp_path / "a.ibrec")
    start = time.perf_counter()
    with RecordingReader(path, skip_damage=True) as reader:
        list(reader.items())
    assert [d.torn for d in reader.damage] == [True]
    assert meter.read < 4 * path.stat().st_size, f"{meter.read} bytes read"
    assert time.perf_counter() - start < 5


# -- B2: one budget for the whole pass --


def test_many_damage_spans_share_one_work_budget(tmp_path: Path, meter: Meter) -> None:
    body = 2 * 1024 * 1024 - 64
    b = Builder()
    b.stream(RX)
    b.source(0, "peer")
    seq = 0
    for _ in range(40):
        for _ in range(2):
            seq += 1
            b.frame(seq, stream=0)
        b.out += struct.pack("<IBBH", 0, 1, 1, 0)  # damage
        b.out += struct.pack("<IBBHI", body, 0xFF, 0, 0, 0) + bytes(4)  # a body that fails its CRC
    b.out += bytes(body + 64)
    path = b.write(tmp_path / "a.ibrec")
    size = path.stat().st_size
    with RecordingReader(path, skip_damage=True) as reader:
        list(reader.items())
    assert meter.crc < 8 * size, f"{meter.crc} bytes CRC-checked for a {size} byte file"
    assert "gave up" in reader.damage[-1].reason and not reader.damage[-1].torn


# -- B3: embedded definitions in info --


def blob_file(path: Path, count: int, repeat: int = 1) -> Path:
    b = Builder()
    for i in range(count):
        content = i.to_bytes(4, "little")
        body = fmt.encode_blob(
            BlobKind.CCDD, "fc", "csv", hashlib.sha256(content).digest(), content
        )
        for _ in range(repeat):
            b.add(RecordType.BLOB, body)
    c0 = b.add(RecordType.CHUNK, chunk_of(24, 0, 0, 0, ()))
    idx = b.add(RecordType.INDEX, fmt.encode_index([c0]))
    b.add(RecordType.TRAILER, fmt.encode_trailer(Trailer(idx, 0, 0)))
    return b.write(path)


def test_info_of_thousands_of_distinct_definitions_is_bounded(tmp_path: Path) -> None:
    path = blob_file(tmp_path / "a.ibrec", 6_000)
    start = time.perf_counter()
    report = rec_inspect.info(path)
    assert time.perf_counter() - start < 10
    assert report.blob_total == 6_000 and len(report.blobs) <= rec_inspect.MAX_LISTED
    assert len(info_record(report)["definitions"]) <= rec_inspect.MAX_LISTED
    assert info_record(report)["definitions_total"] == 6_000
    text = info_lines(report)
    assert len(text) < 200 and any("more" in line and "definition" in line for line in text)
    assert len(rec_inspect.scan(path).blobs) <= rec_inspect.MAX_LISTED


def test_a_definition_repeated_in_the_file_is_listed_once(tmp_path: Path) -> None:
    path = blob_file(tmp_path / "a.ibrec", 3, repeat=4)
    for report in (rec_inspect.scan(path), rec_inspect.info(path)):
        assert report.blob_total == 3 and len(report.blobs) == 3


def test_the_reader_does_not_retain_definition_contents(tmp_path: Path) -> None:
    path = blob_file(tmp_path / "a.ibrec", 50)
    with RecordingReader(path) as reader:
        list(reader.items())
    assert not hasattr(reader, "blobs")


# -- B4: sequence holes and the GAP records that explain them --


def holes_file(path: Path, count: int, *, late: bool, real_hole: bool = True) -> Path:
    """Odd sequence numbers are recorded; each even one is a hole that a GAP explains, either
    right after it or (``late``) with one GAP at the end. A real, unexplained hole closes it."""
    b = Builder().standard(0)
    seq = 0
    for _ in range(count):
        seq += 1
        b.frame(seq, stream=0)
        if not late:
            b.add(
                RecordType.GAP,
                fmt.encode_gap(fmt.Gap(0, GapReason.RECORDER_OVERRUN, 1, 1, 1, seq + 1, seq + 1)),
            )
        seq += 1
    last_explained = seq
    if late:
        gap = fmt.Gap(0, GapReason.NOT_RECORDED, count, 1, 1, 2, last_explained)
        b.add(RecordType.GAP, fmt.encode_gap(gap))
    seq += 2 if real_hole else 1  # a real hole: one more number that never reached the file
    b.frame(seq, stream=0)
    return b.write(path)


@pytest.mark.parametrize("late", [False, True])
def test_every_hole_is_explained_however_many_gaps_there_are(tmp_path: Path, late: bool) -> None:
    path = holes_file(tmp_path / "a.ibrec", 1500, late=late, real_hole=False)
    report = rec_inspect.scan(path)
    assert report.hole_frames == 0 and report.holes == []
    assert report.gap_total == (1 if late else 1500)


@pytest.mark.parametrize("late", [False, True])
def test_an_unexplained_hole_after_thousands_of_explained_ones_still_fails(
    tmp_path: Path, late: bool
) -> None:
    path = holes_file(tmp_path / "a.ibrec", 1500, late=late)
    report = rec_inspect.scan(path)
    assert report.hole_frames == 1 and len(report.holes) == 1
    assert report.holes[0][0] == report.holes[0][1]


def test_lost_by_reason_sums_every_gap_not_the_listed_ones(tmp_path: Path) -> None:
    path = holes_file(tmp_path / "a.ibrec", 300, late=False, real_hole=False)
    assert rec_inspect.scan(path).lost_by_reason == {"recorder_overrun": 300}


def test_writer_gaps_beyond_the_listing_cap_still_explain_and_sum(tmp_path: Path) -> None:
    clock = [0]
    path = tmp_path / "a.ibrec"
    recorder = Recorder.open(
        path, meta(), streams=(RX, TX), accept=frozenset({0}), options=FAST,
        mono_ns=lambda: clock[0],
    )  # fmt: skip
    seq = 0
    for _ in range(300):  # an unrecorded TX frame, then a recorded RX frame, every tick
        seq += 1
        recorder.append(frame(seq, stream=TX))
        seq += 1
        recorder.append(frame(seq, stream=RX))
        clock[0] += 250_000_000
        recorder.tick()
    asyncio.run(recorder.close())
    for report in (rec_inspect.scan(path), rec_inspect.info(path)):
        assert report.lost_by_reason == {"not_recorded": 300}, report.source
        assert report.hole_frames == 0 and report.status == "closed", report.source


# -- B5: rec repair never drops data in silence --


def stale_index_file(path: Path) -> Path:
    """An INDEX at the head of the second chunk (a stale one), then more frames and a real close."""
    b = Builder().standard(0)
    for seq in (1, 2, 3):
        b.frame(seq, stream=0)
    c0 = b.add(RecordType.CHUNK, chunk_of(24, 3, 1, 3))
    stale = b.add(RecordType.INDEX, fmt.encode_index([c0]))
    for seq in (4, 5, 6):
        b.frame(seq, stream=0)
    c1 = b.add(RecordType.CHUNK, chunk_of(stale, 3, 4, 6))
    index = b.add(RecordType.INDEX, fmt.encode_index([c0, c1]))
    b.add(RecordType.TRAILER, fmt.encode_trailer(Trailer(index, 6, 6)))
    return b.write(path)


def concatenated_file(path: Path) -> Path:
    b = Builder().standard(0)
    for seq in (1, 2, 3):
        b.frame(seq, stream=0)
    c0 = b.add(RecordType.CHUNK, chunk_of(24, 3, 1, 3))
    index = b.add(RecordType.INDEX, fmt.encode_index([c0]))
    b.add(RecordType.TRAILER, fmt.encode_trailer(Trailer(index, 3, 3)))
    path.write_bytes(bytes(b.out) * 2)
    return path


def mid_trailer_file(path: Path) -> Path:
    b = Builder().standard(0)
    for seq in (1, 2, 3):
        b.frame(seq, stream=0)
    b.add(RecordType.TRAILER, fmt.encode_trailer(Trailer(0, 3, 3)))
    for seq in (4, 5, 6):
        b.frame(seq, stream=0)
    return b.write(path)


@pytest.mark.parametrize(
    ("make", "kept", "beyond"),
    [(stale_index_file, 3, 3), (concatenated_file, 3, 0), (mid_trailer_file, 3, 3)],
)
def test_repair_reports_everything_after_the_valid_prefix(
    tmp_path: Path, make: object, kept: int, beyond: int
) -> None:
    source = make(tmp_path / "a.ibrec")  # type: ignore[operator]
    result = rec_inspect.repair(source, tmp_path / "b.ibrec")
    assert result.frames == kept and result.frames_beyond == beyond
    assert result.damage is not None and not result.damage.torn
    assert result.dropped_bytes > 0


@pytest.mark.parametrize("make", [stale_index_file, concatenated_file, mid_trailer_file])
def test_repair_cli_exits_5_when_data_was_left_out(
    tmp_path: Path, make: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = make(tmp_path / "a.ibrec")  # type: ignore[operator]
    env = {**os.environ, "XDG_STATE_HOME": str(tmp_path / "state")}
    done = run_ib(tmp_path, ["rec", "repair", str(source), str(tmp_path / "b.ibrec")], env)
    assert done.returncode == 5, done.stderr
    assert Code.REC_DAMAGED.value in done.stderr


def test_a_closed_recording_still_repairs_to_the_same_bytes_and_exit_0(tmp_path: Path) -> None:
    from recutil import sample

    source = tmp_path / "a.ibrec"
    sample(source, 30)
    result = rec_inspect.repair(source, tmp_path / "b.ibrec")
    assert result.damage is None
    assert (tmp_path / "b.ibrec").read_bytes() == source.read_bytes()


def strict(text: str) -> object:
    def refuse(name: str) -> object:
        raise AssertionError(f"{name} in JSON output")

    return json.loads(text, parse_constant=refuse)


def test_info_json_is_strict_json_and_sanitized_for_any_meta(tmp_path: Path) -> None:
    b = Builder()
    b.out = bytearray(fmt.encode_preamble(b.nonce))
    raw = (
        b'{"schema":"ironbird-rec-meta/1","max_packet_bytes":2048,"pid":NaN,'
        rb'"nodes":{"a":Infinity,"b":[-Infinity,"x\u001b[2J"]},"host":"h\u001b]0;t\u0007"}'
    )
    b.add(RecordType.META, raw)
    b.stream(RX)
    path = b.write(tmp_path / "a.ibrec")
    text = info_json(rec_inspect.scan(path))
    record = strict(text)
    assert isinstance(record, dict) and "\x1b" not in text and "\u001b" not in text
    assert record["meta"]["pid"] == "nan" or isinstance(record["meta"]["pid"], str)
    assert record["meta"]["nodes"]["a"] in ("inf", "Infinity")


def test_meta_nested_deeper_than_the_cap_is_unusable(tmp_path: Path) -> None:
    depth = fmt.MAX_META_DEPTH + 1
    raw = b'{"schema":"ironbird-rec-meta/1","max_packet_bytes":2048,"nodes":'
    raw += b"[" * depth + b"]" * depth + b"}"
    b = Builder()
    b.out = bytearray(fmt.encode_preamble(b.nonce))
    b.add(RecordType.META, raw)
    with pytest.raises(IronbirdError) as caught:
        RecordingReader(b.write(tmp_path / "a.ibrec"))
    assert caught.value.code is Code.REC_DAMAGED and "nested" in caught.value.summary


# -- the writer: what was written, how close ends, how a part starts --

from ironbird.notice import Level  # noqa: E402
from ironbird.recording import writer as writer_mod  # noqa: E402
from ironbird.recording.writer import RecorderOptions  # noqa: E402


class FakeLedger:
    def __init__(self) -> None:
        self.records: list[tuple[str, str, dict[str, object]]] = []

    def record(self, kind: str, event: str, **fields: object) -> None:
        self.records.append((kind, event, fields))

    def named(self, event: str) -> dict[str, object]:
        return next(fields for _, name, fields in self.records if name == event)


def read_frames(path: Path) -> list[object]:
    with RecordingReader(path) as reader:
        return list(reader.frames())


def test_stats_count_frames_written_not_frames_encoded(tmp_path: Path) -> None:
    import errno
    import threading

    path = tmp_path / "a.ibrec"
    reports: list[IronbirdError] = []
    ledger = FakeLedger()
    calls = [0]
    stopped = threading.Event()

    def flaky_write(fd: int, data: memoryview) -> int:
        calls[0] += 1
        if calls[0] > 2:
            stopped.set()
            raise OSError(errno.ENOSPC, "No space left on device")
        return os.write(fd, data)

    async def scenario() -> tuple[writer_mod.RecorderStats, int]:
        recorder = Recorder.open(
            path,
            meta(),
            streams=[RX],
            options=RecorderOptions(min_free_bytes=0, flush_bytes=100),
            write=flaky_write,
            loop=asyncio.get_running_loop(),
            report=lambda e, **_: reports.append(e),
            ledger=ledger,  # type: ignore[arg-type]
        )
        offered = 0
        for i in range(1, 12):
            recorder.append(frame(i))
            offered += 1
            await asyncio.sleep(0.01)
        assert recorder.failed_event is not None
        await asyncio.wait_for(recorder.failed_event.wait(), 5)
        for i in range(100, 105):  # after the stop
            recorder.append(frame(i))
            offered += 1
        return await recorder.close(), offered

    stats, offered = asyncio.run(scenario())
    on_disk = len(read_frames(path))
    assert stats.frames == on_disk and 0 < on_disk < offered
    assert stats.frames + stats.unwritten_frames + stats.after_stop_frames == offered
    assert stats.lost_frames == offered - on_disk and stats.after_stop_frames >= 5
    [error] = reports
    assert error.code is Code.REC_WRITE
    import re

    found = re.search(r"after (\d+) frame", error.summary)
    assert found and int(found.group(1)) == on_disk and "not recorded" in error.summary
    stop = ledger.named("recording.stop")
    assert stop["frames"] == on_disk and stop["lost_frames"] == offered - on_disk


def test_ib_record_does_not_claim_the_capture_continues(tmp_path: Path) -> None:
    reports: list[IronbirdError] = []
    import errno

    def failing(fd: int, data: memoryview) -> int:
        raise OSError(errno.EIO, "I/O error")

    async def scenario() -> None:
        recorder = Recorder.open(
            tmp_path / "a.ibrec",
            meta(),
            streams=[RX],
            options=RecorderOptions(min_free_bytes=0, flush_bytes=100),
            write=failing,
            loop=asyncio.get_running_loop(),
            report=lambda e, **_: reports.append(e),
            capture_continues=False,
        )
        recorder.append(frame(1))
        assert recorder.failed_event is not None
        await asyncio.wait_for(recorder.failed_event.wait(), 5)
        await recorder.close()

    asyncio.run(scenario())
    assert "continues" not in reports[0].summary


def test_close_waits_for_a_writer_that_is_still_making_progress(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(writer_mod, "CLOSE_STALL_S", 0.4)
    path = tmp_path / "a.ibrec"
    notices: list[tuple[Code, Level]] = []

    def slow_write(fd: int, data: memoryview) -> int:
        time.sleep(0.05)
        return os.write(fd, data[:8192])

    async def scenario() -> tuple[writer_mod.RecorderStats, int, float]:
        recorder = Recorder.open(
            path,
            meta(),
            streams=[RX],
            options=RecorderOptions(min_free_bytes=0, max_buffer_bytes=256 * 1024),
            write=slow_write,
            loop=asyncio.get_running_loop(),
            report=lambda e, *, level, **_: notices.append((e.code, level)),
        )
        offered = 0
        for i in range(1, 1500):
            recorder.append(frame(i, data=b"x" * 400))
            offered += 1
        start = time.monotonic()
        stats = await recorder.close()
        return stats, offered, time.monotonic() - start

    stats, offered, took = asyncio.run(scenario())
    assert took > 0.8, "the backlog was meant to outlast the stall limit"
    report = rec_inspect.scan(path)
    assert report.status == "closed" and report.trailer is not None
    assert stats.overrun_frames > 0
    lost = sum(g.lost for g in report.gaps if g.reason is GapReason.RECORDER_OVERRUN)
    assert report.frames + lost == offered and report.hole_frames == 0
    assert stats.frames == report.frames
    assert [c for c, _ in notices] == [Code.REC_OVERRUN]  # one notice, none made by the close


def test_close_gives_up_on_a_writer_that_makes_no_progress(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import threading

    monkeypatch.setattr(writer_mod, "CLOSE_STALL_S", 0.3)
    gate = threading.Event()
    notices: list[Code] = []

    def stuck_write(fd: int, data: memoryview) -> int:
        gate.wait(10)
        return os.write(fd, data)

    async def scenario() -> writer_mod.RecorderStats:
        recorder = Recorder.open(
            tmp_path / "a.ibrec",
            meta(),
            streams=[RX],
            options=RecorderOptions(min_free_bytes=0),
            write=stuck_write,
            loop=asyncio.get_running_loop(),
            report=lambda e, **_: notices.append(e.code),
        )
        for i in range(1, 50):
            recorder.append(frame(i))
        start = time.monotonic()
        stats = await recorder.close()
        assert time.monotonic() - start < 3
        gate.set()
        return stats

    stats = asyncio.run(scenario())
    assert notices == [Code.REC_WRITE] and stats.frames == 0 and stats.unwritten_frames == 49


def test_a_new_part_exists_with_its_head_before_the_writer_has_written_anything(
    tmp_path: Path,
) -> None:
    import threading

    gate = threading.Event()

    def blocked(fd: int, data: memoryview) -> int:
        gate.wait(10)
        return os.write(fd, data)

    base = tmp_path / "rec.ibrec"
    options = RecorderOptions(min_free_bytes=0, split_bytes=4000, chunk_bytes=1500, flush_bytes=800)
    recorder = Recorder.open(base, meta(), streams=[RX], options=options, write=blocked)
    for i in range(1, 200):
        recorder.append(frame(i, data=b"y" * 100))
    parts = sorted(tmp_path.glob("rec*.ibrec"))
    try:
        assert len(parts) >= 3
        for number, part in enumerate(parts[1:], start=2):
            with RecordingReader(part) as reader:  # a crash now must leave readable parts
                assert reader.meta.part == number
    finally:
        gate.set()
        asyncio.run(recorder.close())


def test_a_part_name_in_the_way_stops_the_recording_with_an_honest_message(
    tmp_path: Path,
) -> None:
    reports: list[IronbirdError] = []
    base = tmp_path / "rec.ibrec"
    (tmp_path / "rec.p0002.ibrec").write_bytes(b"someone else's file")
    options = RecorderOptions(min_free_bytes=0, split_bytes=4000, chunk_bytes=1500)
    recorder = Recorder.open(
        base, meta(), streams=[RX], options=options, report=lambda e, **_: reports.append(e)
    )
    for i in range(1, 200):
        recorder.append(frame(i, data=b"y" * 100))
    asyncio.run(recorder.close())
    assert recorder.failed is not None and recorder.failed.code is Code.REC_CREATE
    assert "part 2" in recorder.failed.summary
    assert "choose another -o" not in recorder.failed.summary
    assert (tmp_path / "rec.p0002.ibrec").read_bytes() == b"someone else's file"


def test_append_and_gap_failures_stop_the_recorder_instead_of_raising(tmp_path: Path) -> None:
    reports: list[IronbirdError] = []
    recorder = Recorder.open(
        tmp_path / "a.ibrec",
        meta(),
        streams=[RX],
        options=FAST,
        report=lambda e, **_: reports.append(e),
    )
    recorder.append(frame(1))
    recorder.append(frame(2)._replace(stream=None))  # type: ignore[arg-type]
    assert recorder.failed is not None and recorder.failed.code is Code.INTERNAL
    recorder.append(frame(3))  # counted as after the stop, no exception
    recorder.gap(RX, GapReason.KERNEL_DROP, 1, 1, 1, 1, 1)
    asyncio.run(recorder.close())
    assert [e.code for e in reports] == [Code.INTERNAL]
    again = Recorder.open(tmp_path / "b.ibrec", meta(), streams=[RX], options=FAST)
    again.gap(None, GapReason.KERNEL_DROP, 1, 1, 1, 1, 1)  # type: ignore[arg-type]
    assert again.failed is not None and again.failed.code is Code.INTERNAL
    asyncio.run(again.close())


def test_a_failed_start_removes_its_own_partial_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import errno

    def broken(_: object) -> None:
        raise OSError(errno.EIO, "I/O error")

    monkeypatch.setattr(os, "fsync", broken)
    path = tmp_path / "a.ibrec"
    with pytest.raises(IronbirdError) as caught:
        Recorder.open(path, meta(), streams=[RX], options=FAST)
    assert caught.value.code is Code.REC_WRITE and not path.exists()
    assert "removed" in caught.value.summary


def test_a_directory_that_cannot_be_synced_is_tolerated_with_a_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import errno

    def refuse(_: Path) -> None:
        raise OSError(errno.EINVAL, "Invalid argument")

    monkeypatch.setattr(writer_mod, "sync_directory", refuse)
    seen: list[tuple[Code, Level]] = []
    recorder = Recorder.open(
        tmp_path / "a.ibrec",
        meta(),
        streams=[RX],
        options=FAST,
        report=lambda e, *, level, **_: seen.append((e.code, level)),
    )
    recorder.append(frame(1))
    asyncio.run(recorder.close())
    assert seen == [(Code.REC_DIRSYNC, Level.WARNING)]
    assert rec_inspect.scan(tmp_path / "a.ibrec").status == "closed"


def test_repair_over_a_symlink_with_force_says_what_it_will_not_replace(tmp_path: Path) -> None:
    from recutil import sample

    source = tmp_path / "a.ibrec"
    sample(source, 10)
    target = tmp_path / "elsewhere.ibrec"
    target.write_bytes(b"keep me")
    link = tmp_path / "out.ibrec"
    link.symlink_to(target)
    for force in (False, True):
        with pytest.raises(IronbirdError) as caught:
            rec_inspect.repair(source, link, force=force)
        assert caught.value.code is Code.REC_CREATE
        assert ("regular file" in caught.value.summary) == force
    dangling = tmp_path / "dangling.ibrec"
    dangling.symlink_to(tmp_path / "nowhere")
    with pytest.raises(IronbirdError) as caught:
        rec_inspect.repair(source, dangling)
    assert "already exists" in caught.value.summary
    assert target.read_bytes() == b"keep me"


# -- staging: the file appears only once the capture is open --


def test_staged_frames_and_gaps_reach_the_recorder_in_order(tmp_path: Path) -> None:
    from ironbird.recording.session import _Staging

    staging = _Staging()
    staging.append(frame(1))
    staging.gap(RX, GapReason.KERNEL_DROP, 3, 1, 2, 1, 1)
    staging.append(frame(2))
    recorder = Recorder.open(tmp_path / "a.ibrec", meta(), streams=[RX], options=FAST)
    staging.replay(recorder)
    recorder.append(frame(3))
    asyncio.run(recorder.close())
    report = rec_inspect.scan(tmp_path / "a.ibrec")
    assert [f.seq for f in read_frames(tmp_path / "a.ibrec")] == [1, 2, 3]  # type: ignore[attr-defined]
    assert report.lost_by_reason == {"kernel_drop": 3} and report.status == "closed"


def test_frames_past_the_staging_limit_become_an_overrun_gap(tmp_path: Path) -> None:
    from ironbird.recording.session import _Staging

    staging = _Staging(limit=4)
    for seq in range(1, 11):
        staging.append(frame(seq))
    recorder = Recorder.open(tmp_path / "a.ibrec", meta(), streams=[RX], options=FAST)
    staging.replay(recorder)
    asyncio.run(recorder.close())
    report = rec_inspect.scan(tmp_path / "a.ibrec")
    assert report.frames == 4 and report.lost_by_reason == {"recorder_overrun": 6}
    assert report.hole_frames == 0


@pytest.fixture
def cli_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    from recutil import write_config

    path, command_port, telemetry_port = write_config(tmp_path)
    monkeypatch.setenv("IRONBIRD_CONFIG", str(path))
    monkeypatch.setenv("IRONBIRD_RECORD_DIR", str(tmp_path / "recordings"))
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    monkeypatch.setenv("IRONBIRD_TEST_PORTS", f"{command_port},{telemetry_port}")
    return dict(os.environ)


def test_a_busy_port_leaves_no_recording_behind(tmp_path: Path, cli_env: dict[str, str]) -> None:
    import socket

    telemetry = int(cli_env["IRONBIRD_TEST_PORTS"].split(",")[1])
    squatter = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        squatter.bind(("127.0.0.1", telemetry))
        done = run_ib(tmp_path, ["record", "--duration", "1"], cli_env)
    finally:
        squatter.close()
    assert done.returncode == 1 and "IB-TX-001" in done.stderr
    assert not list((tmp_path / "recordings").glob("*"))


def test_a_wrong_argument_with_record_leaves_no_recording_behind(
    tmp_path: Path, cli_env: dict[str, str]
) -> None:
    done = run_ib(
        tmp_path,
        ["listen", "fc", "--mid", "nonsense", "--record", "auto", "--timeout", "1"],
        cli_env,
    )
    assert done.returncode == 2
    assert not list((tmp_path / "recordings").glob("*"))


def test_an_existing_target_is_refused_before_anything_opens(
    tmp_path: Path, cli_env: dict[str, str]
) -> None:
    target = tmp_path / "mine.ibrec"
    target.write_bytes(b"precious")
    done = run_ib(tmp_path, ["record", "-o", str(target), "--duration", "1"], cli_env)
    assert done.returncode == 1 and Code.REC_CREATE.value in done.stderr
    assert target.read_bytes() == b"precious"


# -- datagrams left unread when a link end closes --


def test_datagrams_unread_at_close_are_a_gap_for_the_recorder(tmp_path: Path) -> None:
    from ironbird.transport import LinkEnd, TransportStats
    from test_engine import _Probe

    probe = _Probe(tmp_path / "probe")
    ground = probe.io[LinkEnd.SOURCE]
    path = tmp_path / "a.ibrec"
    streams = probe.engine.streams_for([(probe.config.links[0], LinkEnd.SOURCE)])
    recorder = Recorder.open(path, meta(), streams=streams, options=FAST)
    probe.engine.add_sink(recorder)

    class Closed:
        def stats(self) -> TransportStats:
            return TransportStats(unread_at_close=7)

    probe.engine._note_unread(ground, Closed())  # type: ignore[arg-type]
    asyncio.run(recorder.close())
    report = rec_inspect.scan(path)
    assert [(g.reason, g.lost, g.stream) for g in report.gaps] == [
        (GapReason.KERNEL_DROP, 7, streams[0].id)
    ]


# -- smaller findings --


def test_a_complete_final_record_that_fails_its_crc_is_damage_not_incomplete(
    tmp_path: Path,
) -> None:
    b = Builder().standard(3)
    data = bytearray(b.out)
    data[-6] ^= 0xFF  # inside the last record's body: complete, wrong CRC
    path = tmp_path / "a.ibrec"
    path.write_bytes(bytes(data))
    with RecordingReader(path) as reader:
        assert [f.seq for f in reader.frames()] == [1, 2]
    [damage] = reader.damage
    assert damage.torn and not damage.incomplete
    assert "incomplete" not in damage.to_error().summary and "damaged" in damage.to_error().summary
    cut = tmp_path / "b.ibrec"
    cut.write_bytes(bytes(b.out)[:-3])
    with RecordingReader(cut) as reader:
        list(reader.frames())
    assert reader.damage[0].incomplete and "incomplete" in reader.damage[0].to_error().summary


def test_stream_id_65535_is_refused(tmp_path: Path) -> None:
    from ironbird.ccdd.model import MessageKind
    from ironbird.frame import Direction, Stream

    body = bytearray(fmt.encode_stream(RX))
    body[0:2] = struct.pack("<H", 0xFFFF)
    with pytest.raises(fmt.FormatError, match="reserved"):
        fmt.decode_stream(bytes(body))
    b = Builder()
    b.add(RecordType.STREAM, bytes(body))
    b.frame(1, stream=0xFFFF)
    b.frame(2, stream=0xFFFF)
    with RecordingReader(b.write(tmp_path / "a.ibrec")) as reader:
        assert list(reader.frames()) == [] and reader.damage
    with pytest.raises(AssertionError):
        Stream(0xFFFF, "l", "n", MessageKind.TELEMETRY, Direction.RX, "udp", "x")


def test_an_index_must_end_where_the_trailer_begins(tmp_path: Path) -> None:
    b = Builder().standard(0)
    for seq in (1, 2, 3):
        b.frame(seq, stream=0)
    c0 = b.add(RecordType.CHUNK, chunk_of(24, 3, 1, 3))
    index = b.add(RecordType.INDEX, fmt.encode_index([c0]))
    b.add(RecordType.MARK, fmt.encode_mark(1, fmt.MarkKind.NOTE, "between"))  # index, MARK, trailer
    b.add(RecordType.TRAILER, fmt.encode_trailer(Trailer(index, 3, 3)))
    path = b.write(tmp_path / "a.ibrec")
    full = rec_inspect.scan(path)
    assert full.index == "invalid" and full.findings_total >= 1
    quick = rec_inspect.info(path)
    assert quick.source == "scan" and quick.index == "invalid"


def test_repair_refuses_counts_that_do_not_fit_instead_of_a_struct_error(tmp_path: Path) -> None:
    b = Builder().standard(2)
    path = b.write(tmp_path / "a.ibrec")
    with RecordingReader(path) as reader:
        end, offsets, frames, last_seq, tail = rec_inspect._valid_prefix(reader)
        tail.tally.frames = 2**32
        with pytest.raises(IronbirdError) as caught:
            rec_inspect._closing_records(reader, end, offsets, frames, last_seq, tail)
    assert caught.value.code is Code.REC_CREATE


def test_many_damaged_stretches_are_one_line_and_bounded(
    tmp_path: Path, cli_env: dict[str, str]
) -> None:
    b = Builder().standard(0)
    seq = 0
    for _ in range(500):
        for _ in range(2):
            seq += 1
            b.frame(seq, stream=0)
        b.out += struct.pack("<IBBH", 0, 1, 1, 0)
    seq += 1
    b.frame(seq, stream=0)
    seq += 1
    b.frame(seq, stream=0)
    path = b.write(tmp_path / "a.ibrec")
    report = rec_inspect.scan(path)
    assert report.damage_total == 500 and len(report.damage) <= rec_inspect.MAX_LISTED
    assert report.damage_bytes == 500 * 8
    done = run_ib(tmp_path, ["rec", "check", str(path)], cli_env)
    assert done.returncode == 1
    assert len([ln for ln in done.stderr.splitlines() if "IB-REC-006" in ln]) == 1
    ledger = [
        json.loads(line)
        for p in sorted((tmp_path / "state").rglob("*.jsonl"))
        for line in p.read_text().splitlines()
    ]
    check = next(e for e in ledger if e["event"] == "rec.check")
    assert check["damage_total"] == 500 and len(check["damage"]) <= rec_inspect.MAX_LISTED
    info = run_ib(tmp_path, ["rec", "info", str(path), "--format", "json"], cli_env)
    record = strict(info.stdout)
    assert isinstance(record, dict) and record["damage_total"] == 500
    assert len(record["damage"]) <= rec_inspect.MAX_LISTED


# -- rules that mutation testing showed nothing pinned down --

from conftest import CORPUS  # noqa: E402


def test_resync_passes_over_a_chain_whose_sequence_numbers_are_already_used() -> None:
    path = CORPUS / "rec-resync-stale-sequence-chain.ibrec"
    with RecordingReader(path, skip_damage=True) as reader:
        assert [f.seq for f in reader.frames()] == [1, 2, 3, 5, 6]
    assert len(reader.damage) == 1  # one stretch, ending at frame 5, not at the stale records


def test_info_does_not_trust_an_index_whose_chunks_leave_a_gap() -> None:
    report = rec_inspect.info(CORPUS / "rec-index-chunk-gap.ibrec")
    assert report.source == "scan" and report.index == "invalid"


def test_check_fails_a_closed_recording_whose_summaries_lie() -> None:
    for name in ("rec-chunk-time-bounds-lie", "rec-trailer-last-seq-lies", "rec-index-chunk-gap"):
        report = rec_inspect.scan(CORPUS / f"{name}.ibrec")
        assert report.status == "inconsistent" and report.findings_total >= 1, name


def test_a_stale_index_in_mid_file_is_a_finding_for_check() -> None:
    report = rec_inspect.scan(CORPUS / "rec-stale-index-mid-file.ibrec")
    assert report.status == "inconsistent"
    assert any("INDEX" in finding and "followed" in finding for finding in report.findings)
