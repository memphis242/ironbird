"""The size, duration and count grammar shared by the config file and the prune flags."""

from __future__ import annotations

import pytest

from ironbird import units


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("1B", 1),
        ("64MiB", 64 << 20),
        ("3KiB", 3 << 10),
        ("2GiB", 2 << 30),
        ("1TiB", 1 << 40),
        ("999999999999B", 999_999_999_999),
    ],
)
def test_sizes_in_the_grammar_parse(text: str, expected: int) -> None:
    assert units.parse_size(text) == expected


@pytest.mark.parametrize(
    "text",
    ["", "0B", "01B", "64", "64 MiB", " 64MiB", "64MiB ", "-1B", "+1B", "1.5GiB", "64mib",
     "64MB", "1000000000000B", "0x10B", "\uff16\uff14MiB", "64MiB\n"],
)  # fmt: skip
def test_sizes_outside_the_grammar_are_none(text: str) -> None:
    assert units.parse_size(text) is None


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("1s", 1),
        ("90s", 90),
        ("10min", 600),
        ("24h", 86400),
        ("7d", 604800),
        ("999999d", 999999 * 86400),
    ],
)
def test_durations_in_the_grammar_parse(text: str, expected: int) -> None:
    assert units.parse_duration(text) == expected


@pytest.mark.parametrize(
    "text", ["", "0s", "07d", "5", "5m", "5 min", "-5s", "1.5h", "1000000s", "5D", "5sec", "5d\n"]
)
def test_durations_outside_the_grammar_are_none(text: str) -> None:
    assert units.parse_duration(text) is None


@pytest.mark.parametrize(
    ("text", "high", "expected"),
    [
        ("0", 1000, 0),
        ("7", 1000, 7),
        ("1000", 1000, 1000),
        ("1001", 1000, None),
        ("9999", 9999, 9999),
        ("9999", 1000, None),
        ("", 1000, None),
        ("01", 1000, None),
        ("-1", 1000, None),
        ("10000", 99999, None),  # more digits than the grammar allows
        ("1e3", 1000, None),
    ],
)
def test_counts(text: str, high: int, expected: int | None) -> None:
    assert units.parse_count(text, high) == expected


def test_count_default_ceiling_is_one_thousand() -> None:
    assert units.parse_count("1000") == 1000
    assert units.parse_count("1001") is None


@pytest.mark.parametrize(
    ("count", "expected"),
    [
        (0, "0 B"),
        (12, "12 B"),
        (1023, "1023 B"),
        (1024, "1 KiB"),
        (1536, "1.5 KiB"),
        (812 * 1024, "812 KiB"),
        (310 * (1 << 20), "310 MiB"),
        (int(1.4 * (1 << 30)), "1.4 GiB"),
        (5 * (1 << 40), "5 TiB"),
        (5000 * (1 << 40), "5000 TiB"),  # the largest unit just grows
    ],
)
def test_format_size(count: float, expected: str) -> None:
    assert units.format_size(count) == expected


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [
        (0, "1s"),  # never zero: a zero duration is not in the grammar
        (0.4, "1s"),
        (45, "45s"),
        (60, "1min"),
        (90, "90s"),
        (600, "10min"),
        (3600, "1h"),
        (3600 * 5, "5h"),
        (3660, "61min"),
        (86400, "1d"),
        (86400 * 7, "7d"),
        (86400 + 3600, "25h"),
    ],
)
def test_compact_duration_is_in_the_grammar_and_round_trips(seconds: float, expected: str) -> None:
    text = units.compact_duration(seconds)
    assert text == expected
    assert units.parse_duration(text) == max(1, int(seconds))


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [
        (0, "0 s"),
        (42, "42 s"),
        (119, "119 s"),
        (120, "2 min"),
        (125, "125 s"),
        (3600, "1 h"),
        (7200, "2 h"),
        (86400, "24 h"),  # a single day reads as hours
        (86400 * 2, "2 d"),
        (86400 * 2 + 3600, "49 h"),
        (86400 * 7, "7 d"),
    ],
)
def test_format_duration(seconds: float, expected: str) -> None:
    assert units.format_duration(seconds) == expected
