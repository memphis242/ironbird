"""Sizes and durations: the one grammar of the config file and of the prune flags.

Sizes are ``^[1-9][0-9]{0,11}(B|KiB|MiB|GiB|TiB)$``; durations ``^[1-9][0-9]{0,5}(s|min|h|d)$``.
No signs, no whitespace, no leading zeros, no other units. The parsers return ``None`` for text
that does not match so each caller words its own coded error.
"""

from __future__ import annotations

import re
from typing import Final

SIZE_RE: Final = re.compile(r"[1-9][0-9]{0,11}(B|KiB|MiB|GiB|TiB)")
DURATION_RE: Final = re.compile(r"[1-9][0-9]{0,5}(s|min|h|d)")
COUNT_RE: Final = re.compile(r"(0|[1-9][0-9]{0,3})")
SIZE_UNITS: Final = {"B": 1, "KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30, "TiB": 1 << 40}
DURATION_UNITS: Final = {"s": 1, "min": 60, "h": 3600, "d": 86400}
SIZE_FORMS: Final = "a number of bytes or a string such as '64MiB' (B, KiB, MiB, GiB, TiB)"
DURATION_FORMS: Final = "a string such as '90s', '10min', '24h' or '7d' (more than zero)"
KIB: Final = 1 << 10
MIB: Final = 1 << 20
GIB: Final = 1 << 30
TIB: Final = 1 << 40


def parse_size(text: str) -> int | None:
    """Bytes in ``text`` (``"64MiB"``), or ``None`` when it is not in the size grammar."""
    if not SIZE_RE.fullmatch(text):
        return None
    for unit in ("KiB", "MiB", "GiB", "TiB"):
        if text.endswith(unit):
            return int(text[: -len(unit)]) * SIZE_UNITS[unit]
    return int(text[:-1])


def parse_duration(text: str) -> int | None:
    """Seconds in ``text`` (``"7d"``), or ``None`` when it is not in the duration grammar."""
    if not DURATION_RE.fullmatch(text):
        return None
    if text.endswith("min"):
        return int(text[:-3]) * 60
    return int(text[:-1]) * DURATION_UNITS[text[-1]]


_LOOSE_SIZE_RE: Final = re.compile(r"\s*([0-9]{1,12})\s*([A-Za-z]{0,8})\s*")
_SIZE_SPELLINGS: Final = {
    "b": "B",
    "k": "KiB",
    "kb": "KiB",
    "kib": "KiB",
    "m": "MiB",
    "mb": "MiB",
    "mib": "MiB",
    "g": "GiB",
    "gb": "GiB",
    "gib": "GiB",
    "t": "TiB",
    "tb": "TiB",
    "tib": "TiB",
}
_LOOSE_DURATION_RE: Final = re.compile(r"\s*([0-9]{1,12})\s*([A-Za-z]{1,8})\s*")
_DURATION_SPELLINGS: Final = {
    "s": "s",
    "sec": "s",
    "secs": "s",
    "second": "s",
    "seconds": "s",
    "m": "min",
    "min": "min",
    "mins": "min",
    "minute": "min",
    "minutes": "min",
    "h": "h",
    "hr": "h",
    "hrs": "h",
    "hour": "h",
    "hours": "h",
    "d": "d",
    "day": "d",
    "days": "d",
}


def suggest_size(text: str) -> str | None:
    """The size grammar's spelling of a common near miss (``"1GB"``, ``"64 mb"``), else ``None``."""
    match = _LOOSE_SIZE_RE.fullmatch(text)
    if match is None:
        return None
    unit = _SIZE_SPELLINGS.get(match.group(2).lower()) if match.group(2) else "B"
    candidate = f"{int(match.group(1))}{unit}" if unit else ""
    return candidate if candidate != text and parse_size(candidate) is not None else None


def suggest_duration(text: str) -> str | None:
    """The duration grammar's spelling of a common near miss (``"30 days"``, ``"2 hours"``).
    Never a guess between readings: see :func:`ambiguous_duration`."""
    match = _LOOSE_DURATION_RE.fullmatch(text)
    if match is None or ambiguous_duration(text) is not None:
        return None
    unit = _DURATION_SPELLINGS.get(match.group(2).lower())
    candidate = f"{int(match.group(1))}{unit}" if unit else ""
    return candidate if candidate != text and parse_duration(candidate) is not None else None


_AMBIGUOUS_DURATION_UNITS: Final = frozenset({"mo", "month", "months"})


def ambiguous_duration(text: str) -> str | None:
    """Advice for a duration whose unit can mean minutes or months (``"6M"``, ``"2 months"``):
    a wrong guess would keep data for minutes instead of months, so none is made."""
    match = _LOOSE_DURATION_RE.fullmatch(text)
    if match is None:
        return None
    unit = match.group(2)
    if unit != "M" and unit.lower() not in _AMBIGUOUS_DURATION_UNITS:
        return None
    count = int(match.group(1))
    return (
        f"{unit!r} is ambiguous (minutes or months); "
        f"write '{count}min' for minutes or '{count}d' for days"
    )


def parse_count(text: str, high: int = 1000) -> int | None:
    """A small non-negative integer (``--keep``), or ``None``."""
    if not COUNT_RE.fullmatch(text):
        return None
    value = int(text)
    return value if value <= high else None


def format_size(count: float) -> str:
    """A short human size (``1.4 GiB``, ``310 MiB``, ``812 KiB``, ``12 B``)."""
    value = float(count)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(value) < 1024 or unit == "TiB":
            if unit == "B":
                return f"{int(value)} B"
            if value >= 100 or abs(value - round(value)) < 0.05:
                return f"{value:.0f} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024
    raise AssertionError("unreachable")


def compact_duration(seconds: float) -> str:
    """The duration in the config grammar (``90s``, ``10min``, ``24h``, ``7d``): the largest unit
    that divides it exactly."""
    whole = max(1, int(seconds))
    for unit, length in (("d", 86400), ("h", 3600), ("min", 60)):
        if whole % length == 0:
            return f"{whole // length}{unit}"
    return f"{whole}s"


def format_duration(seconds: float) -> str:
    """``42 s``, ``10 min``, ``12 h``, ``7 d``: the largest unit that shows a whole number."""
    whole = int(seconds)
    if whole >= 86400 * 2 and whole % 86400 == 0:
        return f"{whole // 86400} d"
    if whole >= 3600 and whole % 3600 == 0:
        return f"{whole // 3600} h"
    if whole >= 120 and whole % 60 == 0:
        return f"{whole // 60} min"
    return f"{whole} s"
