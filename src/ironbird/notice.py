"""Structured user notices: what a front-end renders, instead of a pre-formatted string.

A :class:`Notice` is one user-facing message after coalescing. The engine decides *what* to
say and how often; a front-end decides how it looks (a CLI line today, a TUI banner with a
details pane later) and can use ``ledger_n`` to open the full ledger record behind it.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import IntEnum
from types import MappingProxyType
from typing import Final

from ironbird.errors import Code


class Level(IntEnum):
    INFO = 1
    WARNING = 2
    ERROR = 3

    @property
    def label(self) -> str:
        """The word that starts a rendered notice line."""
        return _LABELS[self]


_LABELS: Final[Mapping[Level, str]] = MappingProxyType(
    {Level.INFO: "info", Level.WARNING: "warning", Level.ERROR: "error"}
)
assert _LABELS.keys() == set(Level), "every level needs a label"


@dataclass(frozen=True, slots=True)
class Notice:
    code: Code
    level: Level
    key: str  # coalescing key: code + MID + stream/context
    summary: str  # sanitized, one line, no count suffix
    count: int = 1  # occurrences this notice stands for (1 = first)
    window_s: float = 0.0  # span the count covers (0.0 for the first)
    ledger_n: int | None = (
        None  # record number queued for the latest occurrence (written unless the ledger fails)
    )

    def __post_init__(self) -> None:
        assert self.count >= 1
        assert self.window_s >= 0.0
        assert self.count > 1 or self.window_s == 0.0, "a first occurrence covers no window"
        assert "\n" not in self.summary and "\r" not in self.summary

    def line(self, *, verbose: bool = False) -> str:
        """``error IB-DEC-005: ... (x37 in 10 s)``; ``verbose`` appends ``[ledger n=1234]``."""
        text = f"{self.level.label} {self.code.value}: {self.summary}"
        if self.count > 1:
            text += f" (x{self.count} in {max(1, round(self.window_s))} s)"
        if verbose and self.ledger_n is not None:
            text += f" [ledger n={self.ledger_n}]"
        return text
