"""Coalescing, rate-limited user notification of repeated errors.

The first occurrence of a key is shown immediately as a :class:`~ironbird.notice.Notice`.
Repeats within the window are only counted; when the window closes (on the next report,
:meth:`tick`, or :meth:`flush`) one notice carrying the count and window is emitted.
Everything is still ledgered in full by the caller. The key table is bounded so a flood of
unique keys cannot exhaust memory.
"""

from __future__ import annotations

import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from typing import Final

from ironbird.errors import Code
from ironbird.notice import Level, Notice

DEFAULT_WINDOW_S: Final = 10.0
MAX_KEYS: Final = 256
MAX_NEW_PER_GROUP: Final = 5


@dataclass(slots=True)
class _Entry:
    """One open window. Holds raw fields; a :class:`Notice` is built only when one is emitted,
    so the suppressed path (the flood case) allocates nothing."""

    code: Code
    level: Level
    key: str
    summary: str  # of the latest occurrence
    ledger_n: int | None  # of the latest occurrence that had one
    started: float
    suppressed: int


class Coalescer:
    def __init__(
        self,
        emit: Callable[[Notice], None],
        *,
        window_s: float = DEFAULT_WINDOW_S,
        max_keys: int = MAX_KEYS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        assert window_s > 0 and max_keys > 0
        self._emit = emit
        self._window = window_s
        self._max_keys = max_keys
        self._clock = clock
        self._entries: OrderedDict[str, _Entry] = OrderedDict()
        # per group (error code): window start and distinct keys announced in that window
        self._groups: dict[Code, tuple[float, int]] = {}

    def _summary(self, entry: _Entry, now: float) -> None:
        if entry.suppressed:
            self._emit(
                Notice(
                    entry.code,
                    entry.level,
                    entry.key,
                    entry.summary,
                    count=entry.suppressed + 1,
                    window_s=max(0.0, now - entry.started),
                    ledger_n=entry.ledger_n,
                )
            )

    def report(
        self,
        code: Code,
        level: Level,
        key: str,
        summary: str,
        *,
        ledger_n: int | None = None,
        group: Code | None = None,
    ) -> None:
        """Report one occurrence.

        ``key`` groups repeats (e.g. code plus MID). ``group`` (e.g. the error code) rate-limits
        distinct keys: after ``MAX_NEW_PER_GROUP`` new keys per window the rest fold into one
        "more distinct occurrences" notice, so unique-garbage floods cannot spam the user.
        """
        now = self._clock()
        entry = self._entries.get(key)
        if entry is None and group is not None:
            started, count = self._groups.get(group, (now, 0))
            if now - started >= self._window:
                started, count = now, 0
            if count >= MAX_NEW_PER_GROUP:
                key = f"{group.value}/other"
                code, summary = group, "more distinct occurrences"
                entry = self._entries.get(key)
            else:
                self._groups[group] = (started, count + 1)
        if entry is not None:
            if now - entry.started < self._window:
                entry.suppressed += 1
                entry.summary = summary
                if ledger_n is not None:  # a later occurrence without a record keeps the pointer
                    entry.ledger_n = ledger_n
                return
            self._summary(entry, now)
            del self._entries[key]
        if len(self._entries) >= self._max_keys:
            old_key, old = self._entries.popitem(last=False)
            self._summary(old, now)
            assert old_key not in self._entries
        self._emit(Notice(code, level, key, summary, ledger_n=ledger_n))
        self._entries[key] = _Entry(code, level, key, summary, ledger_n, now, 0)

    def tick(self) -> None:
        """Close expired windows; call about once a second from the owning loop."""
        now = self._clock()
        expired = [k for k, e in self._entries.items() if now - e.started >= self._window]
        for key in expired:
            self._summary(self._entries[key], now)
            del self._entries[key]

    def flush(self) -> None:
        """Emit all pending summaries (shutdown)."""
        now = self._clock()
        for entry in self._entries.values():
            self._summary(entry, now)
        self._entries.clear()
