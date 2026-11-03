"""Recorder behaviour at its edges: an unusable directory, link events for streams it was not
given, and a recorder that stopped after a failure (it keeps quiet, closes cleanly, and what
reached the disk before the failure still reads).
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from ironbird.errors import Code, IronbirdError
from ironbird.recording import format as fmt
from ironbird.recording.format import MarkKind
from ironbird.recording.reader import Item, RecordingReader
from ironbird.recording.writer import Recorder
from recutil import FAST, RX, TX, frame, meta


def marks_of(path: Path) -> list[tuple[int, str]]:
    with RecordingReader(path) as reader:
        return [
            (item.value.kind, item.value.text)
            for item in reader.items()
            if isinstance(item, Item) and item.rtype == fmt.RecordType.MARK
        ]


def test_a_recording_directory_that_cannot_be_made_is_a_coded_create_error(
    tmp_path: Path,
) -> None:
    blocker = tmp_path / "file"
    blocker.write_text("not a directory")
    with pytest.raises(IronbirdError) as info:
        Recorder.open(blocker / "sub" / "a.ibrec", meta(), streams=(RX,), options=FAST)
    assert info.value.code is Code.REC_CREATE


def test_a_link_event_is_marked_only_for_a_stream_the_recorder_was_given(tmp_path: Path) -> None:
    path = tmp_path / "links.ibrec"
    recorder = Recorder.open(
        path, meta(), streams=(RX, TX), accept=frozenset({RX.id}), options=FAST
    )
    recorder.link_event(RX, False, 1_700_000_000_000_000_000, "serial down")
    recorder.link_event(TX, False, 1_700_000_000_000_000_001, "not ours")
    recorder.link_event(RX, True, 1_700_000_000_000_000_002, "serial up")
    asyncio.run(recorder.close())
    kinds = [kind for kind, _ in marks_of(path) if kind in (MarkKind.LINK_DOWN, MarkKind.LINK_UP)]
    assert kinds == [MarkKind.LINK_DOWN, MarkKind.LINK_UP]
    assert all("not ours" not in text for _, text in marks_of(path))


def test_a_stopped_recorder_ignores_marks_gaps_and_ticks_and_closes_once(tmp_path: Path) -> None:
    path = tmp_path / "stopped.ibrec"
    notices: list[IronbirdError] = []
    recorder = Recorder.open(
        path, meta(), streams=(RX,), options=FAST, report=lambda e, **_kw: notices.append(e)
    )
    for seq in range(1, 6):
        recorder.append(frame(seq))
    recorder._stop_with(IronbirdError(Code.REC_WRITE, "disk fell over"))
    assert recorder.stopped
    recorder._stop_with(IronbirdError(Code.REC_WRITE, "second failure is not reported again"))
    recorder.mark(MarkKind.NOTE, "after the stop")
    recorder.gap(RX, fmt.GapReason.KERNEL_DROP, 2, 1, 2, 1, 2)
    recorder.tick()
    recorder.append(frame(6))
    first = asyncio.run(recorder.close())
    second = asyncio.run(recorder.close())
    assert first == second
    assert [n.summary for n in notices].count("disk fell over") == 1
    assert all(text != "after the stop" for _, text in marks_of(path))
    assert recorder.nonce != 0


def test_a_gap_that_counts_no_frames_writes_nothing(tmp_path: Path) -> None:
    path = tmp_path / "nogap.ibrec"
    recorder = Recorder.open(path, meta(), streams=(RX,), options=FAST)
    recorder.append(frame(1))
    recorder.gap(RX, fmt.GapReason.KERNEL_DROP, 0, 1, 2, 1, 1)
    asyncio.run(recorder.close())
    with RecordingReader(path) as reader:
        assert not [i for i in reader.items() if getattr(i, "rtype", None) == fmt.RecordType.GAP]


# -- disk space --


def test_a_recording_directory_with_too_little_free_space_is_refused_before_a_file_exists(
    tmp_path: Path,
) -> None:
    from ironbird.recording.writer import RecorderOptions

    path = tmp_path / "rec" / "a.ibrec"
    with pytest.raises(IronbirdError) as info:
        Recorder.open(path, meta(), streams=(RX,), options=RecorderOptions(min_free_bytes=1 << 60))
    assert info.value.code is Code.REC_SPACE and "min_free_bytes" in info.value.summary
    assert not path.exists()


def test_a_disk_filling_at_the_recording_rate_is_warned_about_while_recording(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import os
    from collections import deque

    from ironbird.recording import writer as writer_module

    seen: list[tuple[IronbirdError, object]] = []
    path = tmp_path / "full.ibrec"
    recorder = Recorder.open(
        path,
        meta(),
        streams=(RX,),
        options=FAST,
        report=lambda e, level=None, **_kw: seen.append((e, level)),
    )
    recorder._rate_samples = deque([(0, 0), (1_000_000_000, 10_000_000)])  # 10 MB/s
    real = os.statvfs

    class Tiny:
        f_bavail = 100
        f_frsize = 1_000_000  # 100 MB free: ten seconds at that rate

    monkeypatch.setattr(
        writer_module.os, "statvfs", lambda p: Tiny() if p == path.parent else real(p)
    )
    recorder._check_space()
    asyncio.run(recorder.close())
    assert [e.code for e, _ in seen] == [Code.REC_SPACE]
    assert "full disk" in seen[0][0].summary
