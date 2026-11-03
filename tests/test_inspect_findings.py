"""``scan`` on closed recordings whose summaries (CHUNK, INDEX, TRAILER) disagree with their
records: each lie is a finding, the status says so, and the frames stay readable. A truthful
hand-built file is the control, so every finding below is the one lie and nothing else.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ironbird.recording import format as fmt
from ironbird.recording import inspect as rec_inspect
from ironbird.recording.format import Chunk, RecordType, Trailer
from ironbird.recording.reader import RecordingReader
from recutil import RX, Builder

FRAMES = 3


def build(
    tmp_path: Path,
    *,
    chunk: dict[str, object] | None = None,
    trailer_frames: int = FRAMES,
    index_extra: list[int] | None = None,
) -> Path:
    builder = Builder()
    builder.stream(RX)
    builder.source(0, "peer")
    frame_offsets = [builder.frame(seq, stream=0, source=0, mid=0x890) for seq in range(1, 4)]
    frame_bytes = sum(fmt.record_size(len(builder.frame_body(s))) for s in range(1, 4))
    assert frame_offsets  # the frames sit between the definitions and the chunk summary
    fields: dict[str, object] = {
        "chunk_start": fmt.PREAMBLE_SIZE,
        "first_seq": 1,
        "last_seq": FRAMES,
        "min_time_ns": 5,
        "max_time_ns": 5,
        "first_mono_ns": 0,
        "last_mono_ns": 0,
        "frames": FRAMES,
        "bytes": frame_bytes,
        "frames_without_mid": 0,
        "flags": 0,
        "mids": ((0x890, FRAMES),),
    }
    fields.update(chunk or {})
    chunk_offset = builder.add(RecordType.CHUNK, fmt.encode_chunk(Chunk(**fields)))  # type: ignore[arg-type]
    index_offset = builder.add(
        RecordType.INDEX, fmt.encode_index([chunk_offset, *(index_extra or [])])
    )
    builder.add(RecordType.TRAILER, fmt.encode_trailer(Trailer(index_offset, trailer_frames, 3)))
    return builder.write(tmp_path / "chunked.ibrec")


def test_the_control_file_is_closed_and_consistent(tmp_path: Path) -> None:
    report = rec_inspect.scan(build(tmp_path))
    assert report.findings == [] and report.status == "closed"
    assert report.frames == FRAMES


@pytest.mark.parametrize(
    ("chunk", "trailer_frames"),
    [
        pytest.param({"frames_without_mid": 1, "mids": ((0x890, 2),)}, FRAMES, id="no-mid-count"),
        pytest.param({"mids": ((0x890, 2), (0x891, 1))}, FRAMES, id="per-mid-counts"),
        pytest.param({"mids": ((0x891, FRAMES),)}, FRAMES, id="wrong-mid"),
        pytest.param({"last_seq": FRAMES + 4}, FRAMES, id="sequence-bounds"),
        pytest.param({"min_time_ns": 1}, FRAMES, id="time-bounds"),
        pytest.param({}, FRAMES + 9, id="trailer-frame-count"),
    ],
)
def test_a_summary_that_disagrees_with_the_records_is_a_finding_and_the_frames_still_read(
    tmp_path: Path, chunk: dict[str, object], trailer_frames: int
) -> None:
    path = build(tmp_path, chunk=chunk, trailer_frames=trailer_frames)
    report = rec_inspect.scan(path)
    assert report.findings, "a lying summary went unnoticed"
    assert report.status == "inconsistent"
    with RecordingReader(path) as reader:
        assert [f.seq for f in reader.frames()] == [1, 2, 3]


def test_an_index_listing_chunks_the_file_does_not_have_is_unusable_not_trusted(
    tmp_path: Path,
) -> None:
    path = build(tmp_path, index_extra=[10**6])
    report = rec_inspect.scan(path)
    assert report.index != "ok"
    assert report.frames == FRAMES  # the records were read, not the lying index
