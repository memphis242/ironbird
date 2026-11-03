"""Forged recordings the writer never produces: a wrong first record, an unreadable version,
contradictory definitions, forged INDEX and TRAILER records, and an index that lies about the
chunks. Every one is a coded error or a recorded damage; frames before the damage still read.
"""

from __future__ import annotations

import struct
from pathlib import Path
from typing import Any

import pytest

from ironbird.errors import Code, IronbirdError
from ironbird.recording import format as fmt
from ironbird.recording.format import Chunk, RecordType, Trailer
from ironbird.recording.reader import RecordingReader
from recutil import RX, Builder, frame, record


def frames_and_damage(path: Path) -> tuple[list[Any], list[Any]]:
    with RecordingReader(path, skip_damage=True) as reader:
        frames = list(reader.frames())
        return frames, list(reader.damage)


def preamble_then(*records: tuple[int, bytes], nonce: int = 0x1122334455667788) -> bytes:
    seed = fmt.seed_of(nonce)
    out = bytearray(fmt.encode_preamble(nonce))
    for rtype, body in records:
        out += fmt.encode_record(rtype, body, seed)
    return bytes(out)


def test_a_file_whose_first_record_is_not_meta_is_refused_as_damaged(tmp_path: Path) -> None:
    path = tmp_path / "nometa.ibrec"
    path.write_bytes(preamble_then((RecordType.STREAM, fmt.encode_stream(RX))))
    with pytest.raises(IronbirdError) as info:
        RecordingReader(path)
    assert info.value.code is Code.REC_DAMAGED and "not META" in info.value.summary


def test_a_future_major_version_is_refused_with_the_version_code(tmp_path: Path) -> None:
    path = tmp_path / "v2.ibrec"
    good = bytearray(Builder().standard(2).out)
    struct.pack_into("<H", good, 8, fmt.VERSION_MAJOR + 1)
    path.write_bytes(bytes(good))
    with pytest.raises(IronbirdError) as info:
        RecordingReader(path)
    assert info.value.code is Code.REC_VERSION


def test_a_critical_record_type_this_version_does_not_know_is_refused_at_open(
    tmp_path: Path,
) -> None:
    builder = Builder()
    path = tmp_path / "critical.ibrec"
    seed = builder.seed
    first_unknown = 0x0B  # below the skippable range, above the known types
    assert first_unknown < fmt.CRITICAL_LIMIT
    path.write_bytes(
        fmt.encode_preamble(builder.nonce) + fmt.encode_record(first_unknown, b"zz", seed)
    )
    with pytest.raises(IronbirdError) as info:
        RecordingReader(path)
    assert info.value.code is Code.REC_VERSION


def test_one_source_id_defined_twice_differently_is_damage_after_the_frames_before_it(
    tmp_path: Path,
) -> None:
    builder = Builder()
    builder.stream(RX)
    builder.source(0, "peer-a")
    builder.frame(1)
    builder.frame(2)
    builder.source(0, "peer-b")  # contradicts the first definition
    builder.frame(3)
    path = builder.write(tmp_path / "twice.ibrec")
    frames, damage = frames_and_damage(path)
    assert [f.seq for f in frames][:2] == [1, 2]
    assert damage and all(d.to_error().code in (Code.REC_DAMAGED, Code.REC_TORN) for d in damage)
    with RecordingReader(path) as strict:
        assert [f.seq for f in strict.frames()] == [1, 2]  # damage ends the iteration
        assert len(strict.damage) == 1  # the damage is reported, not raised


def test_a_repeated_identical_source_definition_is_harmless(tmp_path: Path) -> None:
    builder = Builder()
    builder.stream(RX)
    builder.source(0, "peer")
    builder.frame(1)
    builder.source(0, "peer")
    builder.frame(2)
    frames, damage = frames_and_damage(builder.write(tmp_path / "same.ibrec"))
    assert [f.seq for f in frames] == [1, 2] and not damage


def test_a_gap_naming_a_stream_the_file_never_declares_is_damage(tmp_path: Path) -> None:
    builder = Builder()
    builder.stream(RX)
    builder.source(0, "peer")
    builder.frame(1)
    builder.add(
        RecordType.GAP,
        fmt.encode_gap(fmt.Gap(7, fmt.GapReason.KERNEL_DROP, 3, 1, 2, 1, 1)),
    )
    builder.frame(2)
    frames, damage = frames_and_damage(builder.write(tmp_path / "gap.ibrec"))
    assert [f.seq for f in frames] == [1]
    assert "undefined stream 7" in damage[0].to_error().summary


@pytest.mark.parametrize(
    "tail",
    [
        pytest.param(("index", [10**9]), id="index-entry-past-the-file"),
        pytest.param(("index", [8]), id="index-entry-inside-the-preamble"),
        pytest.param(("trailer", Trailer(10**9, 1, 1)), id="trailer-index-past-the-file"),
    ],
)
def test_forged_index_and_trailer_records_are_damage_not_crashes(
    tmp_path: Path, tail: tuple[str, Any]
) -> None:
    builder = Builder().standard(3)
    kind, value = tail
    if kind == "index":
        builder.add(RecordType.INDEX, fmt.encode_index(value))
    else:
        builder.add(RecordType.TRAILER, fmt.encode_trailer(value))
    frames, damage = frames_and_damage(builder.write(tmp_path / "forged.ibrec"))
    assert [f.seq for f in frames] == [1, 2, 3]
    assert damage


# -- the index is a hint: read_chunks refuses every lie with ValueError --


def written(tmp_path: Path) -> Path:
    path = tmp_path / "w.ibrec"
    record(path, [frame(i) for i in range(1, 30)])
    return path


def test_a_closed_recording_has_a_trailer_and_chunks_that_match_it(tmp_path: Path) -> None:
    with RecordingReader(written(tmp_path)) as reader:
        found = reader.read_trailer()
        assert found is not None
        offset, trailer = found
        chunks = reader.read_chunks(trailer, offset)
        assert sum(c.frames for _, c in chunks) == trailer.frame_count == 29


def test_a_trailer_that_names_no_index_cannot_be_used_for_chunks(tmp_path: Path) -> None:
    with RecordingReader(written(tmp_path)) as reader:
        found = reader.read_trailer()
        assert found is not None
        with pytest.raises(ValueError, match="no index"):
            reader.read_chunks(Trailer(0, 1, 1), found[0])


def test_a_trailer_pointing_at_something_other_than_an_index_is_refused(tmp_path: Path) -> None:
    with RecordingReader(written(tmp_path)) as reader:
        found = reader.read_trailer()
        assert found is not None
        offset, trailer = found
        with pytest.raises(ValueError, match="index"):
            reader.read_chunks(trailer._replace(index_offset=fmt.PREAMBLE_SIZE), offset)
        with pytest.raises(ValueError, match="index"):
            reader.read_chunks(trailer, offset - 8)  # the index does not end where this begins


def chunked_file(
    tmp_path: Path, *, index_offsets: list[int] | None = None, chunk: Chunk | None = None
) -> tuple[Path, Trailer, int]:
    """A hand-built file: META, one chunk, an INDEX over it (forged on request), a TRAILER."""
    builder = Builder()
    builder.stream(RX)
    builder.source(0, "peer")
    start = fmt.PREAMBLE_SIZE  # the first chunk starts right after the preamble
    builder.frame(1)
    real = chunk or Chunk(start, 1, 1, 5, 5, 0, 0, 1, 0, 0, 0, ((0x890, 1),))
    chunk_offset = builder.add(RecordType.CHUNK, fmt.encode_chunk(real))
    offsets = [chunk_offset] if index_offsets is None else index_offsets
    index_offset = builder.add(RecordType.INDEX, fmt.encode_index(offsets))
    trailer = Trailer(index_offset, 1, 1)
    trailer_offset = builder.add(RecordType.TRAILER, fmt.encode_trailer(trailer))
    return builder.write(tmp_path / "chunked.ibrec"), trailer, trailer_offset


def test_a_hand_built_index_over_one_chunk_is_accepted(tmp_path: Path) -> None:
    path, trailer, trailer_offset = chunked_file(tmp_path)
    with RecordingReader(path) as reader:
        assert reader.read_trailer() == (trailer_offset, trailer)
        [(_, chunk)] = reader.read_chunks(trailer, trailer_offset)
        assert chunk.frames == 1


@pytest.mark.parametrize(
    ("offsets", "why"),
    [
        ([8], "outside the file"),  # before the first record
        ([10**6], "outside the file"),  # past the index
        ([fmt.PREAMBLE_SIZE], "not a CHUNK"),  # lands on META
    ],
)
def test_an_index_entry_that_does_not_point_at_a_chunk_is_refused(
    tmp_path: Path, offsets: list[int], why: str
) -> None:
    path, trailer, trailer_offset = chunked_file(tmp_path, index_offsets=offsets)
    with RecordingReader(path) as reader, pytest.raises(ValueError, match=why):
        reader.read_chunks(trailer, trailer_offset)


def test_a_chunk_that_does_not_start_where_the_last_one_ended_is_refused(tmp_path: Path) -> None:
    lying = Chunk(9999, 1, 1, 5, 5, 0, 0, 1, 0, 0, 0, ((0x890, 1),))
    path, trailer, trailer_offset = chunked_file(tmp_path, chunk=lying)
    with RecordingReader(path) as reader, pytest.raises(ValueError, match="start where"):
        reader.read_chunks(trailer, trailer_offset)


def test_an_index_missing_its_last_chunk_is_refused(tmp_path: Path) -> None:
    path, trailer, trailer_offset = chunked_file(tmp_path, index_offsets=[])
    with RecordingReader(path) as reader, pytest.raises(ValueError, match="last chunk"):
        reader.read_chunks(trailer, trailer_offset)
