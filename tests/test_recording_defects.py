"""Regression tests for the recording-stack defects found by the antagonistic review of the
writer, reader, replay, export and prune: each test is the review's reproduction, run against a
fake clock where the defect needs minutes to happen.
"""

from __future__ import annotations

import asyncio
import errno
import os
import subprocess
import sys
import time
import unicodedata
from dataclasses import replace
from pathlib import Path

import pytest

from ironbird.errors import Code, IronbirdError
from ironbird.fsops import REAL_FS
from ironbird.recording import format as fmt
from ironbird.recording import writer as writer_module
from ironbird.recording.defs import read_head
from ironbird.recording.export import FORMULA_LEAD, safe_cell
from ironbird.recording.format import MarkKind, RecordType
from ironbird.recording.inspect import info, scan
from ironbird.recording.reader import Damage, RecordingReader
from ironbird.recording.replay import Replay, _base_of
from ironbird.recording.session import _Staging
from ironbird.recording.writer import Recorder, part_path
from ironbird.retention import liveness, scan_directory
from recutil import CLI_MAIN, FAST, RX, TX, Builder, frame, meta, wait_until
from replayutil import T0, blob_specs, child_env, hk, meta_data

SECOND = 1_000_000_000


class Clock:
    """A settable monotonic clock for a recorder."""

    def __init__(self) -> None:
        self.now = 0

    def __call__(self) -> int:
        return self.now


def split_recording(
    path: Path, *, split_s: float, frames: int, session: str = "abcdef012345"
) -> Recorder:
    """``frames`` frames one second apart on a fake clock, split every ``split_s`` seconds."""
    clock = Clock()
    rec = Recorder.open(
        path,
        meta(session=session),
        streams=(RX, TX),
        options=replace(FAST, split_seconds=split_s),
        mono_ns=clock,
    )
    for i in range(1, frames + 1):
        clock.now = i * SECOND
        rec.append(frame(i, time_ns=T0 + clock.now))
    asyncio.run(rec.close())
    return rec


def run_ib(env: dict[str, str], *args: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", CLI_MAIN, *args],
        capture_output=True,
        text=True,
        env=env,
        cwd=cwd,
        timeout=60,
        check=False,
    )


def damage_of(path: Path) -> list[Damage]:
    with RecordingReader(path) as reader:
        return [item for item in reader.items() if isinstance(item, Damage)]


def marks_of(path: Path) -> list[fmt.Mark]:
    with RecordingReader(path) as reader:
        return [
            item.value
            for item in reader.items()
            if not isinstance(item, Damage) and item.rtype == RecordType.MARK
        ]


# -- 1. prune must not delete the parts of a split recording that a reader still needs --------


def aged_split_recording(tmp_path: Path) -> tuple[Path, Path, list[Path]]:
    recordings = tmp_path / "recordings"
    recordings.mkdir(mode=0o700)
    path = recordings / "20240101T000000Z-0123456789ab.ibrec"
    rec = split_recording(path, split_s=10.0, frames=39, session="0123456789ab")
    assert len(rec.parts) >= 4
    old = time.time() - 30 * 86400
    for part in rec.parts:
        os.utime(part, (old, old))
    return recordings, path, list(rec.parts)


def prune(tmp_path: Path) -> subprocess.CompletedProcess[str]:
    return run_ib(
        child_env(tmp_path),
        *("rec", "prune", "--older-than", "1d", "--keep", "0", "--yes"),
        cwd=tmp_path,
    )


def test_prune_leaves_a_split_recording_that_a_replay_has_moved_into(tmp_path: Path) -> None:
    _recordings, path, parts = aged_split_recording(tmp_path)
    replay = Replay([path])
    try:
        frames = replay.frames()
        total = 0
        for _ in frames:
            total += 1
            if replay.stats.parts == 2:  # part 1 is closed, the reader is in part 2
                break
        done = prune(tmp_path)
        assert all(p.exists() for p in parts), done.stdout + done.stderr
        total += sum(1 for _ in frames)  # the replay still reaches the last part
        assert total == 39
    finally:
        replay.close()
    done = prune(tmp_path)  # the replay is closed: the session may go now
    assert done.returncode == 0, done.stderr
    assert not any(p.exists() for p in parts)


def test_prune_leaves_a_split_recording_whose_later_part_is_named_alone(tmp_path: Path) -> None:
    _recordings, _path, parts = aged_split_recording(tmp_path)
    replay = Replay([parts[2]])  # `ib export r.p0003.ibrec`
    try:
        prune(tmp_path)
        assert all(p.exists() for p in parts)
    finally:
        replay.close()


def test_a_session_is_live_when_any_one_of_its_parts_is_locked(tmp_path: Path) -> None:
    recordings, _path, parts = aged_split_recording(tmp_path)
    now = time.time()
    listing = scan_directory(REAL_FS, recordings, "recording")
    assert liveness(REAL_FS, recordings, listing, now, None) == {}
    with RecordingReader(parts[3]):
        found = liveness(REAL_FS, recordings, listing, now, None)
    assert list(found.values()) == ["live"]


# -- 2. a rotation and the 60 s redefinition at the same chunk close --------------------------


def test_a_part_closed_at_the_redefinition_has_a_usable_index(tmp_path: Path) -> None:
    rec = split_recording(tmp_path / "r.ibrec", split_s=60.0, frames=130)
    assert len(rec.parts) >= 3
    for path in rec.parts:
        with RecordingReader(path) as reader:
            located = reader.read_trailer()
            assert located is not None
            offset, trailer = located
            reader.read_chunks(trailer, offset)  # raises ValueError when the index is refused
        report = info(path)
        assert report.source == "index" and report.index == "ok", (path, report.index_note)
        checked = scan(path)
        assert checked.index == "ok" and not checked.findings, (path, checked.findings)


def test_the_new_part_still_repeats_every_definition_after_such_a_rotation(tmp_path: Path) -> None:
    rec = split_recording(tmp_path / "r.ibrec", split_s=60.0, frames=130)
    for path in rec.parts[1:]:
        with RecordingReader(path) as reader:
            head = read_head(reader)
        assert set(head.streams) == {RX.id, TX.id} and not head.problem


def test_check_flags_records_between_the_last_chunk_and_the_index(tmp_path: Path) -> None:
    path = tmp_path / "r.ibrec"
    split_recording(path, split_s=1e9, frames=12)
    data = path.read_bytes()
    with RecordingReader(path) as reader:
        located = reader.read_trailer()
        assert located is not None
        trailer_offset, trailer = located
        seed = fmt.seed_of(reader.nonce)
    stray = fmt.encode_record(RecordType.STREAM, fmt.encode_stream(RX), seed)
    moved = trailer._replace(index_offset=trailer.index_offset + len(stray))
    forged = (
        data[: trailer.index_offset]
        + stray
        + data[trailer.index_offset : trailer_offset]
        + fmt.encode_record(RecordType.TRAILER, fmt.encode_trailer(moved), seed)
    )
    bad = tmp_path / "bad.ibrec"
    bad.write_bytes(forged)
    assert not scan(path).findings
    report = scan(bad)
    assert any("between the last chunk" in finding for finding in report.findings)
    assert report.status == "inconsistent"


# -- 3. a later part's head repeats every source -----------------------------------------------


def test_the_head_of_a_part_after_thousands_of_peers_still_decodes(tmp_path: Path) -> None:
    path = tmp_path / "r.ibrec"
    clock = Clock()
    rec = Recorder.open(
        path,
        meta_data(),
        blobs=blob_specs(),
        streams=(RX, TX),
        options=replace(FAST, split_seconds=10.0, fsync="never"),
        mono_ns=clock,
    )
    peers = fmt.MAX_SOURCES + 4
    for i in range(1, peers + 1):
        clock.now = i * 2_000_000
        rec.append(frame(i, data=hk(i), time_ns=T0 + clock.now, source=f"10.0.0.1:{10000 + i}"))
    for i in range(peers + 1, peers + 6):
        clock.now = 11 * SECOND + (i - peers) * SECOND  # past the split
        rec.append(frame(i, data=hk(i), time_ns=T0 + clock.now, source="10.0.0.1:10001"))
    asyncio.run(rec.close())
    assert len(rec.parts) >= 2
    with RecordingReader(rec.parts[1]) as reader:
        head = read_head(reader)
        sources = sum(
            1
            for item in reader.head_records(reader.body_start, reader.size())
            if item.rtype == RecordType.SOURCE
        )
    assert sources == fmt.MAX_SOURCES  # the head is as large as the table can make it
    assert head.problem == "" and set(head.streams) == {RX.id, TX.id}


def test_a_head_with_more_sources_than_the_table_holds_is_still_refused(tmp_path: Path) -> None:
    builder = Builder()
    builder.stream(RX)
    for source_id in range(fmt.MAX_SOURCES + 1):
        builder.source(source_id % fmt.MAX_SOURCES, f"peer{source_id}")
    builder.frame(1)
    path = builder.write(tmp_path / "many.ibrec")
    with RecordingReader(path) as reader:
        assert "head holds more than" in read_head(reader).problem


# -- 4. export of a recording that is still being written says so ------------------------------


def test_export_of_a_live_recording_says_it_is_still_being_written(tmp_path: Path) -> None:
    path = tmp_path / "live.ibrec"
    rec = Recorder.open(path, meta_data(), blobs=blob_specs(), streams=(RX, TX), options=FAST)
    env = child_env(tmp_path)
    try:
        for i in range(1, 2001):
            rec.append(frame(i, data=hk(i), time_ns=T0 + i))
        rec._flush(sync=False)
        wait_until(lambda: rec.stats().frames > 0)
        exported = run_ib(
            env,
            "export",
            str(path),
            "--format",
            "text",
            "-o",
            str(tmp_path / "o.txt"),
            cwd=tmp_path,
        )
        assert exported.returncode == 0, exported.stderr
        assert Code.REC_TORN.value in exported.stderr and "still being written" in exported.stderr
    finally:
        asyncio.run(rec.close())
    closed = run_ib(
        env, "export", str(path), "--format", "text", "-o", str(tmp_path / "all.txt"), cwd=tmp_path
    )
    assert closed.returncode == 0, closed.stderr
    assert "still being written" not in closed.stderr


# -- 5. a failed rotation must not write after the closed part's trailer -----------------------


@pytest.mark.parametrize("trigger", ["source", "clock_step", "stream"])
def test_a_failed_rotation_leaves_no_bytes_after_the_trailer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, trigger: str
) -> None:
    path = tmp_path / "r.ibrec"
    clock = Clock()
    rec = Recorder.open(
        path, meta(), streams=(RX,), options=replace(FAST, split_seconds=10.0), mono_ns=clock
    )
    for i in range(1, 10):
        clock.now = i * SECOND
        rec.append(frame(i, time_ns=T0 + clock.now))
    clock.now = 9_500_000_000
    rec.append(frame(10, time_ns=T0 + clock.now))  # opens a chunk; no rotation before 10.5 s

    def failing_sync_directory(directory: Path) -> None:
        clock.now += 300_000_000  # a slow sync that then fails
        raise OSError(errno.EIO, "Input/output error")

    monkeypatch.setattr(writer_module, "sync_directory", failing_sync_directory)
    clock.now = 10_500_000_000
    wall = T0 + clock.now
    match trigger:
        case "source":
            late = frame(11, time_ns=wall, source="10.0.0.9:7000")
        case "clock_step":
            late = frame(11, time_ns=wall + 5 * SECOND)
        case _:
            late = frame(11, time_ns=wall, stream=TX)  # a stream defined after the start
    rec.append(late)
    assert rec.failed is not None
    monkeypatch.undo()
    stats = asyncio.run(rec.close())
    assert stats.stopped and [p.name for p in rec.parts] == ["r.ibrec"]
    assert damage_of(path) == []
    with RecordingReader(path) as reader:
        assert reader.read_trailer() is not None
    assert stats.lost_frames >= 1  # the frame that was refused is counted, not silently gone


def test_a_stopped_recorder_writes_nothing_more(tmp_path: Path) -> None:
    path = tmp_path / "r.ibrec"
    rec = Recorder.open(path, meta(), streams=(RX, TX), options=FAST)
    rec.append(frame(1))
    rec._stop_with(IronbirdError(Code.REC_WRITE, "stopped for the test"))
    rec._buf += b"not a record"
    rec._flush(sync=False)
    asyncio.run(rec.close())
    assert b"not a record" not in path.read_bytes()


# -- 6. staged frames are not a clock step -----------------------------------------------------


def test_frames_staged_during_a_slow_head_sync_do_not_make_a_clock_step_mark(
    tmp_path: Path,
) -> None:
    wall0 = 1_700_000_000_000_000_000
    path = tmp_path / "r.ibrec"
    clock = Clock()
    staging = _Staging()
    stamps = iter([0, 5_000_000, 10_000_000])
    staging._mono = lambda: next(stamps)  # received at mono 0, 5 and 10 ms
    for i in range(1, 4):
        staging.append(frame(i, time_ns=wall0 + (i - 1) * 5_000_000))
    rec = Recorder.open(path, meta(), streams=(RX, TX), options=FAST, mono_ns=clock)
    clock.now = 400_000_000  # the head sync took 400 ms before the frames were handed over
    staging.replay(rec)
    for i in range(4, 8):  # live frames: stamped and appended together, no clock step at all
        clock.now = 400_000_000 + i * 10_000_000
        rec.append(frame(i, time_ns=wall0 + clock.now))
    asyncio.run(rec.close())
    assert [m for m in marks_of(path) if m.kind == MarkKind.CLOCK_STEP] == []


def test_a_real_clock_step_is_still_marked(tmp_path: Path) -> None:
    path = tmp_path / "r.ibrec"
    clock = Clock()
    rec = Recorder.open(path, meta(), streams=(RX, TX), options=FAST, mono_ns=clock)
    for i in range(1, 4):
        clock.now = i * 10_000_000
        rec.append(frame(i, time_ns=T0 + clock.now))
    clock.now += 10_000_000
    rec.append(frame(4, time_ns=T0 + clock.now + 2 * SECOND))  # the wall clock jumped
    asyncio.run(rec.close())
    assert MarkKind.CLOCK_STEP in [m.kind for m in marks_of(path)]


# -- 7. the base of a part is the inverse of part_path -----------------------------------------


@pytest.mark.parametrize("name", ["run1", "run1.ibrec", "a.b", "a.b.ibrec"])
def test_base_of_inverts_part_path(tmp_path: Path, name: str) -> None:
    base = tmp_path / name
    rec = split_recording(base, split_s=10.0, frames=25)
    assert len(rec.parts) >= 3
    for number, part in enumerate(rec.parts, start=1):
        assert part == part_path(base, number)
        assert _base_of(part, number, "abcdef012345") == base


def test_a_later_part_of_a_recording_without_the_extension_can_be_followed(tmp_path: Path) -> None:
    rec = split_recording(tmp_path / "run1", split_s=10.0, frames=25)
    replay = Replay([rec.parts[1]], follow=True)  # was refused as "no usable file locks"
    replay.close()


def test_a_first_part_of_another_session_is_not_taken_as_the_base(tmp_path: Path) -> None:
    rec = split_recording(tmp_path / "run1", split_s=10.0, frames=25)
    assert _base_of(rec.parts[1], 2, "another-session") is None
    assert _base_of(tmp_path / "plain.p0002.ibrec", 2, None) is None


# -- informational: formula leaders that NFKC folds --------------------------------------------


def test_every_character_that_nfkc_folds_to_a_formula_leader_is_neutralised() -> None:
    folded = [
        chr(code)
        for code in range(0x110000)
        if unicodedata.normalize("NFKC", chr(code))[:1] in "=+-@"
    ]
    assert "﹢" in folded and "﹦" in folded
    for char in folded:
        assert char in FORMULA_LEAD, hex(ord(char))
        assert safe_cell(char + "1+1").startswith("'")


# -- review of this PR: internal stop keeps buffered frames; probes do not fake a writer -------


class _BadStruct:
    size = fmt.FRAME_FIXED.size

    def pack(self, *_args: object) -> bytes:
        raise ValueError("encode failed for the test")


def test_an_internal_stop_still_writes_the_frames_buffered_before_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "r.ibrec"
    rec = Recorder.open(path, meta(), streams=(RX, TX), options=FAST)
    for i in range(1, 6):
        rec.append(frame(i))
    with monkeypatch.context() as patch:
        patch.setattr(fmt, "FRAME_FIXED", _BadStruct())
        rec.append(frame(6))  # its encode raises: an internal failure
    assert rec.stopped
    stats = asyncio.run(rec.close())
    with RecordingReader(path) as reader:
        seqs = [
            item.value.seq
            for item in reader.items()
            if not isinstance(item, Damage) and item.rtype == RecordType.FRAME
        ]
    assert seqs == [1, 2, 3, 4, 5]
    assert stats.unwritten_frames == 0


def test_a_brief_exclusive_probe_does_not_make_a_closed_part_look_live(tmp_path: Path) -> None:
    import fcntl
    import threading

    rec = split_recording(tmp_path / "run1", split_s=10.0, frames=25)
    fd = os.open(rec.parts[1], os.O_RDONLY)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        threading.Timer(0.002, fcntl.flock, (fd, fcntl.LOCK_UN)).start()
        with RecordingReader(rec.parts[1]) as reader:
            assert not reader.writer_active
    finally:
        time.sleep(0.01)
        os.close(fd)
