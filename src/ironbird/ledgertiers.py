"""Who controls a record's rate: tiers, allowances, evidence sampling, token buckets.

Pure objects with an injected clock (no I/O), so the arithmetic of the ledger's budgets is
testable over simulated hours in milliseconds.

- :class:`Tier` - *essential* records have a rate bounded by the user's actions or ironbird's
  own timers (never sampled, never subject to an allowance); *evidence* is driven by the remote
  side or the environment (packet records, wire-origin errors); *events* are received cFS
  events (never sampled, a tier of their own).
- :class:`TierBudget` - the allowance ``bytes_per_hour x max(1 h, run time)`` compared with the
  tier's bytes on disk, with hysteresis on the way back.
- :class:`EvidenceSampler` - per key and 10 s window, every occurrence counted, the first
  eight kept in full, then one sample per second, the rest summarised at window close.
- :class:`TokenBucket` - the raw-evidence byte budget and the responder's reply rate.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import IntEnum
from typing import Any, Final

from ironbird.units import MIB


class Tier(IntEnum):
    ESSENTIAL = 0
    EVIDENCE = 1
    EVENTS = 2

    @property
    def label(self) -> str:
        return self.name.lower()


TIER_COUNT: Final = len(Tier)
HOUR_S: Final = 3600.0
MIN_HYSTERESIS_BYTES: Final = MIB

SAMPLE_WINDOW_S: Final = 10.0
SAMPLE_KEEP_FIRST: Final = 8
SAMPLE_PERIOD_S: Final = 1.0
SAMPLE_MAX_KEYS: Final = 1024
OTHER_KEY: Final = "other"
REPLY_RATE_PER_S: Final = 10.0
REPLY_BURST: Final = 600


class TokenBucket:
    """``rate`` tokens per second up to ``burst``; starts full."""

    def __init__(self, rate: float, burst: float, clock: Callable[[], float]) -> None:
        assert rate > 0 and burst > 0
        self._rate = rate
        self._burst = burst
        self._clock = clock
        self._tokens = burst
        self._last = clock()

    def take(self, amount: float = 1.0) -> bool:
        """Take ``amount`` tokens if the bucket has them; ``False`` leaves it untouched."""
        now = self._clock()
        elapsed = now - self._last
        if elapsed > 0:
            self._tokens = min(self._burst, self._tokens + elapsed * self._rate)
        self._last = max(self._last, now)
        if self._tokens >= amount:
            self._tokens -= amount
            return True
        return False


class TierBudget:
    """The allowance of one tier: ``bytes_per_hour x max(1 h, t)``, ``t`` since session start.

    The first hour's worth is a floor that absorbs bursts at the start; quiet seconds bank
    credit, and only a rate sustained above ``bytes_per_hour`` reaches the cap. Over the
    allowance the tier is *counted only*; writing resumes once its bytes are at most
    ``allowance - h`` with ``h = min(max(1 MiB, 1 % of the allowance), bytes_per_hour / 60)``
    so it does not flap per record and never waits longer than about 60 s of the allowance's
    growth. ``None`` for ``bytes_per_hour`` is ``"unlimited"``.
    """

    def __init__(self, bytes_per_hour: int | None, start: float) -> None:
        assert bytes_per_hour is None or bytes_per_hour > 0
        self.bytes_per_hour = bytes_per_hour
        self._start = start
        self.over = False

    def allowance(self, now: float) -> float | None:
        if self.bytes_per_hour is None:
            return None
        return self.bytes_per_hour * max(HOUR_S, now - self._start) / HOUR_S

    def hysteresis(self, now: float) -> float:
        allowance = self.allowance(now)
        assert allowance is not None and self.bytes_per_hour is not None
        return min(max(float(MIN_HYSTERESIS_BYTES), 0.01 * allowance), self.bytes_per_hour / 60)

    def update(self, written: int, now: float) -> bool:
        """Re-judge the tier against its bytes on disk; returns whether it is counted only."""
        allowance = self.allowance(now)
        if allowance is None:
            self.over = False
        elif self.over:
            if written <= allowance - self.hysteresis(now):
                self.over = False
        elif written >= allowance:
            self.over = True
        return self.over


@dataclass(slots=True)
class _KeyState:
    event: str
    count: int = 0
    bytes: int = 0
    kept: int = 0
    skipped: int = 0
    last_kept: float = float("-inf")
    first_ns: int = 0
    last_ns: int = 0
    first_seq: int | None = None
    last_seq: int | None = None


@dataclass(frozen=True, slots=True)
class SampleSummary:
    """What a closed window knows about one key that had occurrences not kept."""

    event: str
    key: str
    count: int
    bytes: int
    kept: int
    skipped: int
    first_ns: int
    last_ns: int
    first_seq: int | None
    last_seq: int | None

    def fields(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "count": self.count,
            "bytes": self.bytes,
            "kept": self.kept,
            "skipped": self.skipped,
            "first_ns": self.first_ns,
            "last_ns": self.last_ns,
            "first_seq": self.first_seq,
            "last_seq": self.last_seq,
        }


class Admit(IntEnum):
    FULL = 0  # write the record with its raw bytes
    NO_RAW = 1  # write the record, drop its raw bytes (the global raw budget is spent)
    SKIP = 2  # counted only; the window summary stands for it


class EvidenceSampler:
    """Per-key evidence sampling in fixed windows.

    Every occurrence is counted. The first ``keep_first`` of a key in a window are kept in
    full, then one per ``period_s``; the rest are summarised by one record per key when the
    window closes. A global byte budget (``raw_bytes_per_s``, a token bucket) drops *raw
    bytes*, not records, beyond it. The key table holds at most ``max_keys`` keys per window: a
    new key past that folds into the single :data:`OTHER_KEY` entry until the window closes,
    and the table is cleared at each close. Nothing is evicted mid-window, so no key's count is
    split across two entries.
    """

    def __init__(
        self,
        clock: Callable[[], float],
        raw_bytes_per_s: int,
        *,
        window_s: float = SAMPLE_WINDOW_S,
        keep_first: int = SAMPLE_KEEP_FIRST,
        period_s: float = SAMPLE_PERIOD_S,
        max_keys: int = SAMPLE_MAX_KEYS,
    ) -> None:
        assert window_s > 0 and keep_first >= 0 and max_keys > 0 and raw_bytes_per_s > 0
        self._clock = clock
        self._window_s = window_s
        self._keep_first = keep_first
        self._period_s = period_s
        self._max_keys = max_keys
        self._raw = TokenBucket(float(raw_bytes_per_s), float(raw_bytes_per_s), clock)
        self._keys: dict[str, _KeyState] = {}
        self._window_start = clock()
        self.admitted = 0
        self.sampled_away = 0
        self.raw_dropped = 0

    @property
    def keys(self) -> int:
        return len(self._keys)

    def admit(
        self,
        event: str,
        key: str,
        *,
        time_ns: int,
        seq: int | None = None,
        size: int = 0,
        raw_len: int = 0,
    ) -> Admit:
        """Count one occurrence and decide what to write for it."""
        now = self._clock()
        state = self._keys.get(key)
        if state is None:
            if len(self._keys) >= self._max_keys:
                key = f"{OTHER_KEY}|{event}"  # one fold-in entry per record kind, never more
                state = self._keys.get(key)
            if state is None:
                state = self._keys[key] = _KeyState(event, first_ns=time_ns, first_seq=seq)
        state.count += 1
        state.bytes += size
        state.last_ns = time_ns
        if seq is not None:
            state.last_seq = seq
            if state.first_seq is None:
                state.first_seq = seq
        if state.kept >= self._keep_first and now - state.last_kept < self._period_s:
            state.skipped += 1
            self.sampled_away += 1
            return Admit.SKIP
        state.kept += 1
        state.last_kept = now
        self.admitted += 1
        if raw_len and not self._raw.take(float(raw_len)):
            self.raw_dropped += 1
            return Admit.NO_RAW
        return Admit.FULL

    def window_due(self) -> bool:
        return self._clock() - self._window_start >= self._window_s

    def close_window(self) -> list[SampleSummary]:
        """End the window now: one summary per key that had occurrences not kept; the table is
        emptied and a new window starts."""
        summaries = [
            SampleSummary(
                s.event,
                key,
                s.count,
                s.bytes,
                s.kept,
                s.skipped,
                s.first_ns,
                s.last_ns,
                s.first_seq,
                s.last_seq,
            )
            for key, s in self._keys.items()
            if s.skipped
        ]
        self._keys.clear()
        self._window_start = self._clock()
        return summaries
