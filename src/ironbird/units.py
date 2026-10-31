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
DURATION_FORMS: Final = "a string such as '90s', '10min', '24h' or '7d'"
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
