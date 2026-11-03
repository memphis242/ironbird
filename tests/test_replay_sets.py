"""Replay of a set of recording files, and the time arguments that select part of it: files of
another session, out of order or recorded with another packet cap are refused up front with a
usage error that names the file, and absurd offsets are refused rather than overflowing."""

from __future__ import annotations

from pathlib import Path

import pytest

from ironbird.errors import Code, IronbirdError
from ironbird.recording import format as fmt
from ironbird.recording.format import RecordType
from ironbird.recording.replay import Replay, parse_time_spec
from recutil import RX, Builder, meta


def part_file(
    path: Path, *, part: int, session: str = "s1", cap: int = 2048, frames: int = 3
) -> Path:
    builder = Builder()
    builder.out = bytearray(fmt.encode_preamble(builder.nonce))
    builder.add(
        RecordType.META,
        fmt.encode_meta(meta(part=part, session=session, max_packet_bytes=cap)),
    )
    builder.stream(RX)
    builder.source(0, "peer")
    first = (part - 1) * frames + 1
    for seq in range(first, first + frames):
        builder.frame(seq)
    return builder.write(path)


def open_replay(*paths: Path) -> Replay:
    return Replay(list(paths))


def test_two_parts_of_one_session_play_in_order(tmp_path: Path) -> None:
    one = part_file(tmp_path / "a.ibrec", part=1)
    two = part_file(tmp_path / "a.p0002.ibrec", part=2)
    replay = open_replay(one, two)
    try:
        assert [f.seq for f in replay.frames()] == [1, 2, 3, 4, 5, 6]
    finally:
        replay.close()


def test_a_part_found_by_name_is_played_without_being_listed(tmp_path: Path) -> None:
    part_file(tmp_path / "a.ibrec", part=1)
    part_file(tmp_path / "a.p0002.ibrec", part=2)
    replay = open_replay(tmp_path / "a.ibrec")
    try:
        assert len(replay.parts) == 2
        assert [f.seq for f in replay.frames()][-1] == 6
    finally:
        replay.close()


@pytest.mark.parametrize(
    ("second", "needle"),
    [
        pytest.param({"part": 2, "session": "other"}, "another recording session", id="session"),
        pytest.param({"part": 1}, "follows part", id="part-not-increasing"),
        pytest.param({"part": 2, "cap": 1024}, "packet cap", id="packet-cap"),
    ],
)
def test_a_set_that_is_not_one_recording_is_refused_before_any_frame_plays(
    tmp_path: Path, second: dict[str, object], needle: str
) -> None:
    one = part_file(tmp_path / "a.ibrec", part=1)
    other = part_file(tmp_path / "b.ibrec", **second)  # type: ignore[arg-type]
    with pytest.raises(IronbirdError) as info:
        open_replay(one, other)
    assert info.value.code is Code.CLI_USAGE and needle in info.value.summary
    assert "b.ibrec" in info.value.summary


def test_a_missing_middle_part_is_recorded_not_fatal(tmp_path: Path) -> None:
    one = part_file(tmp_path / "a.ibrec", part=1)
    three = part_file(tmp_path / "a.p0003.ibrec", part=3)
    replay = open_replay(one, three)
    try:
        assert replay.stats.missing_parts == [2]
    finally:
        replay.close()


@pytest.mark.parametrize("text", ["+999999999999h", "+999999999999h999999999999h"])
def test_an_offset_of_more_than_a_hundred_years_is_refused(text: str) -> None:
    with pytest.raises(IronbirdError) as info:
        parse_time_spec(text, "--to")
    assert info.value.code is Code.CLI_USAGE and "100 years" in info.value.summary


@pytest.mark.parametrize("text", ["+", "+5", "+5x", "+1.5", "5s", "+-5s", "+5s ", "+0.0000000001s"])
def test_malformed_offsets_are_usage_errors_that_name_the_option(text: str) -> None:
    with pytest.raises(IronbirdError) as info:
        parse_time_spec(text, "--from")
    assert info.value.code is Code.CLI_USAGE and "--from" in info.value.summary
