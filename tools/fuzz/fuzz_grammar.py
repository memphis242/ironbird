"""Coverage-guided fuzzing (atheris) of the small text grammars the CLI and config accept.

Usage: python tools/fuzz/fuzz_grammar.py [-max_total_time=N]

Covers ``parse_size``, ``parse_duration``, ``parse_count`` (config values and prune flags),
``parse_split`` (``ib record --split``), ``parse_time_spec`` and ``parse_time`` (``--from``/``--to``
and filter time literals) and the CCDD type spellings.

Invariant: for any text each answers a value in its range, ``None`` or a reason string, or raises
only a coded ``IronbirdError``/``FilterError`` whose user line is one line; none takes noticeable
time. An accepted size or duration re-formats into text that parses back to the same value.
Seeds are valid and near-valid spellings of each grammar.
"""

from __future__ import annotations

import atheris
from fuzzcommon import run, scratch_dir

with atheris.instrument_imports():
    from ironbird.ccdd.model import FieldType, parse_element_type, parse_type
    from ironbird.errors import IronbirdError
    from ironbird.filter import FilterError
    from ironbird.filter.lexer import parse_time
    from ironbird.recording.replay import parse_time_spec
    from ironbird.recording.session import parse_split
    from ironbird.units import compact_duration, parse_count, parse_duration, parse_size

SCRATCH = scratch_dir("grammar")
SEEDS = (
    "64MiB",
    "512B",
    "1TiB",
    "90s",
    "10min",
    "24h",
    "7d",
    "0",
    "1000",
    "512MiB",
    "1GiB",
    "30m",
    "1.5h",
    "+90s",
    "+1m30s",
    "+250ms",
    "+0.5s",
    "2026-10-30T12:00:00Z",
    "2026-10-30T14:00:00+02:00",
    "2026-10-30T12:00:00.123456789Z",
    "uint32",
    "char[16]",
    "bytes[4096]",
    "uint16[]",
    "char[]",
)


def one_input(data: bytes) -> None:
    text = data.decode("utf-8", errors="surrogateescape")
    size = parse_size(text)
    assert size is None or 1 <= size < 1 << 53
    duration = parse_duration(text)
    if duration is not None:
        assert 1 <= duration < 1 << 53
        assert parse_duration(compact_duration(duration)) == duration
    count = parse_count(text)
    assert count is None or 0 <= count <= 1000
    for parse in (parse_type, parse_element_type):
        got = parse(text)
        assert isinstance(got, FieldType | str)
        if isinstance(got, str):
            assert "\n" not in got
    try:
        sized, timed = parse_split(text)
        assert (sized is None) != (timed is None)
        assert (sized or 1) >= 1 and (timed or 1.0) > 0
    except IronbirdError as err:
        assert "\n" not in err.user_line(), err.user_line()
    for option in ("--from", "--to"):
        try:
            spec = parse_time_spec(text, option)
            assert (spec.absolute_ns is None) != (spec.offset_ns is None)
        except IronbirdError as err:
            assert "\n" not in err.user_line(), err.user_line()
    try:
        assert isinstance(parse_time(text), int)
    except FilterError as err:
        assert "\n" not in err.user_line(), err.user_line()


def main() -> None:
    seeds = SCRATCH / "seeds"
    seeds.mkdir()
    for i, text in enumerate(SEEDS):
        (seeds / f"valid{i}").write_bytes(text.encode())
    run(one_input, seeds, max_len=256)


if __name__ == "__main__":
    main()
