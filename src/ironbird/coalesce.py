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
    window_s: float = DEFAULT_WINDOW_S
    ledger_part: int = 1
    ledger_file: str = ""
    raw: bytes = b""  # of the latest occurrence
    raw_len: int | None = None
    shared: str | None = None  # the words every occurrence had in common, once they differed
    template: str | None = None  # a summary with ``{n}`` where the running total goes
    amount: int = 0  # the total of every occurrence's amount (with ``template``)
    folded: bool = False  # the distinct keys past the group's limit: shown once, when it closes


FOLD_SUMMARY: Final = (
    "more distinct problems of this kind, not listed one by one; all are in the ledger"
)
VARY: Final = " (details vary)"  # marks a summary that stands for several differing ones


def _shared_words(left: str, right: str) -> str:
    """``left`` and ``right`` with the words that differ replaced by ``...``, so a line that
    stands for several different occurrences (``packet of ... bytes``) states nothing that only
    some of them had. When they share too little for that to say what failed (under a third of
    the words), it is ``left`` as first seen with a ``(details vary)`` suffix."""
    if left.endswith(VARY):
        return left
    a, b = left.split(" "), right.split(" ")
    if len(a) == len(b):
        words = [x if x == y else "..." for x, y in zip(a, b, strict=True)]
    else:
        head = 0
        while head < min(len(a), len(b)) and a[head] == b[head]:
            head += 1
        tail = 0
        while tail < min(len(a), len(b)) - head and a[-1 - tail] == b[-1 - tail]:
            tail += 1
        words = [*a[:head], "...", *a[len(a) - tail :]]
    collapsed = [w for i, w in enumerate(words) if not (w == "..." and i and words[i - 1] == "...")]
    kept = sum(1 for w in collapsed if w != "...")
    if kept == 0 or 3 * kept < len(a):
        return left + VARY
    return " ".join(collapsed)


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
        if entry.suppressed or entry.folded:
            if entry.template is not None:
                text = entry.template.replace("{n}", str(entry.amount))
            else:
                text = entry.summary if entry.shared is None else entry.shared
            self._emit(
                Notice(
                    entry.code,
                    entry.level,
                    entry.key,
                    text,
                    count=entry.suppressed + 1,
                    window_s=max(0.0, now - entry.started) if entry.suppressed else 0.0,
                    ledger_n=entry.ledger_n,
                    ledger_part=entry.ledger_part,
                    ledger_file=entry.ledger_file,
                    raw=entry.raw,
                    raw_len=entry.raw_len,
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
        window_s: float | None = None,
        ledger_part: int = 1,
        ledger_file: str = "",
        raw: bytes = b"",
        raw_len: int | None = None,
        amount: int | None = None,
        template: str | None = None,
    ) -> None:
        """Report one occurrence.

        ``key`` groups repeats (e.g. code plus MID). ``group`` (e.g. the error code) rate-limits
        distinct keys: after ``MAX_NEW_PER_GROUP`` new keys per window the rest fold into one
        notice (``FOLD_SUMMARY``, shown when the window closes, with the number of occurrences it
        stands for), so unique-garbage floods cannot spam the user.
        ``window_s`` overrides the window for this key (the ledger's slow warnings use 10 min).
        ``raw`` is the start of the offending packet and ``raw_len`` its full size; a summary
        carries those of the latest occurrence.

        ``amount`` is how much this occurrence stands for (lines dropped, bytes skipped) and
        ``template`` the summary with ``{n}`` in the place of that figure: a line that stands for
        several occurrences then carries the total of their amounts, not the latest one's.
        """
        assert (amount is None) == (template is None), "an amount comes with its template"
        now = self._clock()
        entry = self._entries.get(key)
        folding = False
        if entry is None and group is not None:
            started, count = self._groups.get(group, (now, 0))
            if now - started >= self._window:
                started, count = now, 0
            if count >= MAX_NEW_PER_GROUP:
                key = f"{group.value}/other"
                code, summary = group, FOLD_SUMMARY
                raw, raw_len, amount, template = b"", None, None, None
                entry = self._entries.get(key)
                folding = True
            else:
                self._groups[group] = (started, count + 1)
        if entry is not None:
            if now - entry.started < entry.window_s:
                entry.suppressed += 1
                if template is not None and amount is not None:
                    entry.amount += amount
                    entry.template = template
                elif summary != entry.summary:  # a count says "the same again": keep what is true
                    entry.shared = _shared_words(
                        entry.summary if entry.shared is None else entry.shared, summary
                    )
                entry.summary = summary
                if ledger_n is not None:  # a later occurrence without a record keeps the pointer
                    entry.ledger_n = ledger_n
                    entry.ledger_part, entry.ledger_file = ledger_part, ledger_file
                entry.raw, entry.raw_len = raw, raw_len
                return
            self._summary(entry, now)
            del self._entries[key]
        if len(self._entries) >= self._max_keys:
            old_key, old = self._entries.popitem(last=False)
            self._summary(old, now)
            assert old_key not in self._entries
            assert len(self._entries) < self._max_keys, "an eviction makes room for one key"
        if not folding:  # a fold is announced once, when its window closes
            self._emit(
                Notice(
                    code,
                    level,
                    key,
                    summary,
                    ledger_n=ledger_n,
                    ledger_part=ledger_part,
                    ledger_file=ledger_file,
                    raw=raw,
                    raw_len=raw_len,
                )
            )
        self._entries[key] = _Entry(
            code,
            level,
            key,
            summary,
            ledger_n,
            now,
            0,
            window_s or self._window,
            ledger_part,
            ledger_file,
            raw,
            raw_len,
            None,
            template,
            amount or 0,
            folding,
        )
        assert len(self._entries) <= self._max_keys, "the table of open windows is bounded"

    def tick(self) -> None:
        """Close expired windows; call about once a second from the owning loop."""
        now = self._clock()
        expired = [k for k, e in self._entries.items() if now - e.started >= e.window_s]
        for key in expired:
            self._summary(self._entries[key], now)
            del self._entries[key]

    def discard(self, key: str) -> None:
        """Drop the open window of ``key`` without emitting its summary.

        For an error that ends the matter (a fatal failure that is raised to the user): a
        "(xN in T s)" line about the failures it ends would arrive after it and read as news.
        """
        self._entries.pop(key, None)

    def flush(self) -> None:
        """Emit all pending summaries (shutdown)."""
        now = self._clock()
        for entry in self._entries.values():
            self._summary(entry, now)
        self._entries.clear()
