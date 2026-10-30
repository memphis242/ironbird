"""Action ledger: timestamped JSON Lines of actions, commands, state changes and errors.

Producers call :meth:`Ledger.record`, which only appends a tuple to a bounded queue (no
formatting, no I/O). Formatting happens in :meth:`Ledger.drain_task`, an asyncio task that runs
only while the loop has been idle, in small slices, with a bounded staleness (forced drain at
``FORCE_AGE_S`` age or ``FORCE_COUNT`` entries); the formatted lines go to one writer thread
(:mod:`ironbird.ledgerstore`) that owns every file system call, so a slow or hung disk never
blocks the loop or capture. :meth:`Ledger.flush` is the synchronous path used on shutdown and
signals, :meth:`Ledger.finish` and :meth:`Ledger.close` the last write attempt and the account
of what was not written.

Records belong to a *tier* by who controls their rate (:class:`~ironbird.ledgertiers.Tier`):
essential records are never sampled or capped; evidence and event records have per-hour
allowances (decision D2), evidence is sampled per key, and while the ledger writes into its
reserve or is stalled both are counted only. Memory is bounded by bytes as well as by count: a
bytes-like field (a packet's ``raw``) is cut to ``max_raw_bytes`` and copied out of any mutable
buffer when the record is *created*, and every queued record is charged an estimate of its size
against ``max_queue_bytes``. When the queue is full (by count or by bytes) the *new* record is
refused and counted (its call returns ``None``); a queued record is never evicted. Evidence and
event records each have a capped slice of the queue, so neither a wire flood nor an event storm
can take the other's room or the essential room.

When an essential record cannot be written the ledger is *stalled*: essential records stay
queued, a retry runs every 5 s, and nothing is silent (see :class:`ExitReport`).
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import json
import math
import os
import pwd
import threading
import time
import uuid
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final

import ironbird
from ironbird.coalesce import Coalescer
from ironbird.config import LedgerConfig
from ironbird.errors import Code, IronbirdError
from ironbird.fsops import REAL_FS, FileSystem, close_quietly, write_all
from ironbird.header import DEFAULT_MAX_PACKET_BYTES
from ironbird.ledgerstore import (
    POLL_S as WRITER_POLL_S,
)
from ironbird.ledgerstore import (
    ConfigFailure,
    LedgerStore,
    LedgerWriter,
    Line,
    StoreConfig,
    TxInfo,
    TxTally,
    part_name,
    stamp_of,
)
from ironbird.ledgertiers import (
    REPLY_BURST,
    REPLY_RATE_PER_S,
    TIER_COUNT,
    Admit,
    EvidenceSampler,
    SampleSummary,
    Tier,
    TierBudget,
    TokenBucket,
)
from ironbird.notice import Level, Notice
from ironbird.units import format_duration, format_size

FORCE_AGE_S: Final = 1.0
FORCE_COUNT: Final = 10_000
IDLE_S: Final = 0.02
POLL_S: Final = 0.01
SLICE: Final = 256
MAX_QUEUE: Final = 200_000
MAX_EVIDENCE_QUEUE: Final = 100_000  # slices of MAX_QUEUE; the rest is essential
MAX_EVENT_QUEUE: Final = 50_000
MAX_QUEUE_BYTES: Final = 64 * 1024 * 1024  # estimated size of everything queued
MAX_EVIDENCE_QUEUE_BYTES: Final = 32 * 1024 * 1024
MAX_EVENT_QUEUE_BYTES: Final = 16 * 1024 * 1024
# What a queued record costs besides its variable-length fields: the entry tuple, the field
# mapping and its small values. An estimate, deliberately on the high side.
ENTRY_OVERHEAD_BYTES: Final = 512
ERROR_FLUSH_MIN_INTERVAL_S: Final = 0.25
MAX_DEPTH: Final = 16
MAX_FALLBACK_REPR: Final = 2000
MAX_WRITER_JOBS: Final = 4  # formatted slices waiting for the writer before the drain waits
SLOW_WARN_WINDOW_S: Final = 600.0  # IB-LED-003/001/005/007/010 display at most once per 10 min
SUMMARY_INTERVAL_S: Final = 60.0
REPLY_WINDOW_S: Final = 10.0
TICK_MIN_S: Final = 0.5
MAX_EARLY_NOTICES: Final = 32
REFRESH_EVERY: Final = 256  # records of a capped tier between allowance checks
JOIN_TIMEOUT_S: Final = 5.0
MAX_REPLY_MESSAGES: Final = 64

_RESERVED: Final = frozenset({"ts", "session", "n", "kind", "event"})
_KINDS: Final = frozenset({"action", "command", "state", "packet", "event", "warning", "error"})
_BYTES_LIKE: Final = frozenset({bytes, bytearray, memoryview})

#: Tier of every record name the code base writes (a test checks each ``record`` call in
#: ``src/`` against this table, so a new record cannot land in the wrong tier unnoticed).
#: Names written under a formatted name are listed in :data:`DYNAMIC_RECORD_NAMES`.
RECORD_TIERS: Final[Mapping[str, Tier]] = MappingProxyType(
    {
        # evidence: a rate the remote side or the environment drives
        "rx.malformed": Tier.EVIDENCE,
        "rx.malformed.summary": Tier.EVIDENCE,
        "rx.first": Tier.EVIDENCE,
        "rx.command": Tier.EVIDENCE,
        "rx.command.summary": Tier.EVIDENCE,
        "rx.queue_drops": Tier.EVIDENCE,
        "rx.kernel_drops": Tier.EVIDENCE,
        # events: received cFS events, never sampled
        "rx.event": Tier.EVENTS,
        # essential: bounded by the user's actions or ironbird's own timers
        "rx.unread_at_close": Tier.ESSENTIAL,
        "rx.failed": Tier.ESSENTIAL,
        "cli.invoke": Tier.ESSENTIAL,
        "cli.signal": Tier.ESSENTIAL,
        "session.start": Tier.ESSENTIAL,
        "session.end": Tier.ESSENTIAL,
        "find": Tier.ESSENTIAL,
        "send.request": Tier.ESSENTIAL,
        "tx.command": Tier.ESSENTIAL,
        "tx.telemetry": Tier.ESSENTIAL,
        "respond.replies": Tier.ESSENTIAL,
        "respond.start": Tier.ESSENTIAL,
        "respond.stop": Tier.ESSENTIAL,
        "listen.start": Tier.ESSENTIAL,
        "listen.stop": Tier.ESSENTIAL,
        "events.start": Tier.ESSENTIAL,
        "events.stop": Tier.ESSENTIAL,
        "codec.loaded": Tier.ESSENTIAL,
        "sink.detached": Tier.ESSENTIAL,
        "output.pipe_closed": Tier.ESSENTIAL,
        "output.stderr_lost": Tier.ESSENTIAL,
        "rec.info": Tier.ESSENTIAL,
        "rec.check": Tier.ESSENTIAL,
        "rec.repair.start": Tier.ESSENTIAL,
        "rec.repair.done": Tier.ESSENTIAL,
        "recording.start": Tier.ESSENTIAL,
        "recording.not_started": Tier.ESSENTIAL,
        "recording.overrun": Tier.ESSENTIAL,
        "recording.overrun_end": Tier.ESSENTIAL,
        "recording.failed": Tier.ESSENTIAL,
        "recording.part": Tier.ESSENTIAL,
        "recording.stop": Tier.ESSENTIAL,
        "ledger.open": Tier.ESSENTIAL,
        "ledger.part": Tier.ESSENTIAL,
        "ledger.reserve": Tier.ESSENTIAL,
        "ledger.resumed": Tier.ESSENTIAL,
        "ledger.stalled": Tier.ESSENTIAL,
        "ledger.dropped": Tier.ESSENTIAL,
        "ledger.summary": Tier.ESSENTIAL,
        "ledger.tier_over": Tier.ESSENTIAL,
        "ledger.tier_gap": Tier.ESSENTIAL,
        "ledger.name_taken": Tier.ESSENTIAL,
        "ledger.warning": Tier.ESSENTIAL,
        "ledger.note": Tier.ESSENTIAL,
        "ledger.recovered": Tier.ESSENTIAL,
        "retention.scan": Tier.ESSENTIAL,
        "retention.warning": Tier.ESSENTIAL,
        "prune.plan": Tier.ESSENTIAL,
        "prune.confirmed": Tier.ESSENTIAL,
        "prune.declined": Tier.ESSENTIAL,
        "prune.delete": Tier.ESSENTIAL,
        "prune.gone": Tier.ESSENTIAL,
        "prune.skip": Tier.ESSENTIAL,
    }
)
#: Patterns of the record names built from a variable (``f"{name}.ending"``), for the same test.
DYNAMIC_RECORD_NAMES: Final = frozenset({"*.ending", "*.open", "*.summary"})
_KIND_TIER: Final[Mapping[str, Tier]] = MappingProxyType(
    {
        "action": Tier.ESSENTIAL,
        "command": Tier.ESSENTIAL,
        "state": Tier.ESSENTIAL,
        "warning": Tier.ESSENTIAL,
        "error": Tier.ESSENTIAL,
        "packet": Tier.EVIDENCE,
        "event": Tier.EVENTS,
    }
)
assert _KIND_TIER.keys() == _KINDS
#: Errors that received bytes (or the peer's commands) can cause at will are evidence.
EVIDENCE_ERRORS: Final = frozenset(
    {
        Code.DEC_SHORT,
        Code.DEC_OVERSIZE,
        Code.DEC_LENGTH,
        Code.DEC_FIXED,
        Code.DEC_CHECKSUM,
        Code.DEC_UNKNOWN,
        Code.DEC_PAYLOAD,
        Code.RSP_RULE,
        Code.TX_RECV,
        Code.TX_OVERRUN,
        Code.ENG_LAG,
    }
)

# (time_ns, number, kind, event, fields, tier, bytes charged against the queue, transmission)
_Entry = tuple[int, int, str, str, Mapping[str, Any], int, int, "TxInfo | None"]

_LENGTH_KEYS: Final[dict[str, str]] = {}
MAX_LENGTH_KEYS: Final = 64  # field names are code literals; the cache is bounded regardless


def _length_key(name: str) -> str:
    """The field that carries the full length of the bytes-like field ``name``."""
    key = f"{name}_len"
    if len(_LENGTH_KEYS) < MAX_LENGTH_KEYS:
        _LENGTH_KEYS[name] = key
    return key


@dataclass(frozen=True, slots=True)
class _Cut:
    """A bytes-like field cut at record creation: the kept head and how much was left out."""

    head: bytes
    omitted: int


def default_state_dir(environ: Mapping[str, str] | None = None) -> Path:
    """``$IRONBIRD_STATE_DIR``, else ``$XDG_STATE_HOME/ironbird``, else ``~/.local/state``.

    Per the XDG specification a relative ``XDG_STATE_HOME`` is invalid and ignored. A relative
    ``IRONBIRD_STATE_DIR`` and an empty or relative home would put the ledger in the current
    directory, so they are refused with a coded error that names the remedy.
    """
    env = os.environ if environ is None else environ
    override = env.get("IRONBIRD_STATE_DIR")
    if override:
        if not os.path.isabs(override):
            raise IronbirdError(
                Code.LED_DIR,
                "IRONBIRD_STATE_DIR is not an absolute path; set it to an absolute, "
                "writable directory",
            )
        return Path(override)
    xdg = env.get("XDG_STATE_HOME")
    if xdg and os.path.isabs(xdg):
        return Path(xdg) / "ironbird"
    home = env.get("HOME")
    if home is None:
        try:
            home = pwd.getpwuid(os.getuid()).pw_dir
        except KeyError:
            home = ""
    if not home or not os.path.isabs(home):
        raise IronbirdError(
            Code.LED_DIR,
            "cannot locate a state directory for the ledger (HOME is "
            f"{'empty' if not home else 'not an absolute path'}); "
            "set IRONBIRD_STATE_DIR to an absolute, writable directory",
        )
    return Path(home) / ".local" / "state" / "ironbird"


def format_ts(time_ns: int) -> str:
    """UTC ISO 8601 with microsecond resolution."""
    seconds, ns = divmod(time_ns, 1_000_000_000)
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(seconds)) + f".{ns // 1000:06d}Z"


def _make_default(max_raw_bytes: int) -> Callable[[object], object]:
    """JSON fallback for non-JSON values; raw bytes are kept up to ``max_raw_bytes``."""

    def default(value: object) -> object:
        if isinstance(value, _Cut):
            return f"{value.head.hex(' ')} ...(+{value.omitted}B)"
        if isinstance(value, bytes | bytearray | memoryview):
            raw = bytes(value)
            text = raw[:max_raw_bytes].hex(" ")
            return (
                text if len(raw) <= max_raw_bytes else f"{text} ...(+{len(raw) - max_raw_bytes}B)"
            )
        if isinstance(value, Path):
            return str(value)
        if isinstance(value, Mapping):  # read-only views (MappingProxyType) are not dicts
            return dict(value)
        if isinstance(value, BaseException):
            return f"{type(value).__name__}: {value}"
        return _safe_repr(value)

    return default


def _safe_repr(value: object) -> str:
    try:
        return repr(value)[:MAX_FALLBACK_REPR]
    except Exception:
        return f"<unrepresentable {type(value).__name__}>"


def _plain(value: object, seen: frozenset[int], depth: int) -> object:
    """Copy ``value`` into JSON-safe form: string keys, finite floats, no cycles."""
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        return "nan" if math.isnan(value) else ("inf" if value > 0 else "-inf")
    if isinstance(value, Mapping | list | tuple | set | frozenset):
        if id(value) in seen:
            return "<cycle>"
        if depth >= MAX_DEPTH:
            return _safe_repr(value)
        inner = seen | {id(value)}
        if isinstance(value, Mapping):
            return {
                (k if isinstance(k, str) else _safe_repr(k)): _plain(v, inner, depth + 1)
                for k, v in value.items()
            }
        return [_plain(v, inner, depth + 1) for v in value]
    return value


@dataclass(frozen=True, slots=True)
class ExitReport:
    """What the ledger owes the user at exit: the account of what was not written."""

    notices: tuple[Notice, ...] = ()
    unlogged: int = 0  # transmissions never written or possibly lost
    withheld: int = 0  # replies withheld under on_full = "refuse_commands"
    unwritten_records: int = 0
    unwritten_tx_lines: tuple[str, ...] = ()  # JSON lines of unwritten transmissions (verbose)
    stalled: bool = False

    @property
    def exit_code(self) -> int:
        """1 when replies were withheld, 6 when a transmission went out unlogged, else 0."""
        if self.withheld:
            return 1
        return 6 if self.unlogged else 0


@dataclass(slots=True)
class _ReplyWindow:
    count: int = 0
    bytes: int = 0
    by_message: dict[str, int] = field(default_factory=dict)
    first_ns: int = 0
    last_ns: int = 0
    first_seq: int | None = None
    last_seq: int | None = None
    started: float = 0.0


class Ledger:
    """Append-only action ledger for one session."""

    def __init__(
        self,
        directory: Path,
        *,
        session_id: str | None = None,
        on_failure: Callable[[IronbirdError], None] | None = None,
        enabled: bool = True,
        max_raw_bytes: int = DEFAULT_MAX_PACKET_BYTES,
        max_queue: int = MAX_QUEUE,
        max_evidence_queue: int = MAX_EVIDENCE_QUEUE,
        max_event_queue: int | None = None,
        max_queue_bytes: int = MAX_QUEUE_BYTES,
        max_evidence_queue_bytes: int = MAX_EVIDENCE_QUEUE_BYTES,
        max_event_queue_bytes: int | None = None,
        config: LedgerConfig | None = None,
        session_class: str = "short",
        command: str = "",
        fs: FileSystem = REAL_FS,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        join_timeout_s: float = JOIN_TIMEOUT_S,
        open_timeout_s: float = JOIN_TIMEOUT_S,
        store_options: Mapping[str, float] | None = None,
        writer_poll_s: float = WRITER_POLL_S,
        warn_gate: Callable[[Code], bool] | None = None,
    ) -> None:
        assert max_raw_bytes > 0
        assert session_class in ("long", "short")
        if max_event_queue is None:
            max_event_queue = max(1, min(MAX_EVENT_QUEUE, (max_queue - max_evidence_queue) // 2))
        if max_event_queue_bytes is None:
            max_event_queue_bytes = max(
                1, min(MAX_EVENT_QUEUE_BYTES, (max_queue_bytes - max_evidence_queue_bytes) // 2)
            )
        assert max_evidence_queue > 0 and max_event_queue > 0
        assert max_evidence_queue + max_event_queue < max_queue, "essential records need room"
        assert max_evidence_queue_bytes > 0 and max_event_queue_bytes > 0
        assert max_evidence_queue_bytes + max_event_queue_bytes < max_queue_bytes
        cfg = config or LedgerConfig()
        self._directory: Path = directory
        self._enabled = enabled
        self._on_failure = on_failure
        self._config = cfg
        self._long = session_class == "long"
        self._clock = clock
        self._join_timeout_s = join_timeout_s
        self._open_timeout_s = open_timeout_s
        self._max_raw_bytes = max_raw_bytes
        self._default = _make_default(max_raw_bytes)
        self._urgent = False
        self._max_queue = max_queue
        self._queue_caps = (max_queue, max_evidence_queue, max_event_queue)
        self._max_queue_bytes = max_queue_bytes
        self._byte_caps = (max_queue_bytes, max_evidence_queue_bytes, max_event_queue_bytes)
        self._queue: deque[_Entry] = deque()
        self._queued = [0] * TIER_COUNT  # entries per tier in ``_queue``
        self._queued_bytes = 0  # sum of the charges of ``_queue``
        self._tier_bytes_q = [0] * TIER_COUNT  # charges per tier in ``_queue``
        self._queued_tx = 0
        self._numbers = itertools.count(1)
        self._dropped = [0] * TIER_COUNT  # refused for lack of queue room, until the marker
        self._dropped_total = 0
        self._last_busy = 0.0
        self._last_flush = float("-inf")  # the first error flushes at once
        self._oldest_mono = 0.0
        self._start_ns = time.time_ns()
        self._start = clock()
        self._dead = not enabled
        self._any = False
        self._opened_by_caller = False
        self._exit_report: ExitReport | None = None
        self._notify: Callable[[Notice], None] | None = None
        self._early: list[Notice] = []
        self._coalescer = Coalescer(self._deliver, clock=clock)
        self._tickers: list[Callable[[], None]] = []
        self._warn_gate: Callable[[Code], bool] = warn_gate or (lambda _code: True)
        # tiers
        self._budgets: list[TierBudget | None] = [
            None,
            TierBudget(cfg.evidence_bytes_per_hour, self._start),
            TierBudget(cfg.event_bytes_per_hour, self._start),
        ]
        self._counted = [False] * TIER_COUNT  # over the allowance
        self._gap: list[list[int] | None] = [None] * TIER_COUNT  # records, bytes, first, last ns
        self._since_refresh = 0
        self._counted_records = [0] * TIER_COUNT
        self._counted_bytes = [0] * TIER_COUNT
        self._lost_total = 0
        self._lost_at_stall = 0
        self._sampler = EvidenceSampler(clock, cfg.evidence_raw_bytes_per_second)
        self._last_summary = clock()
        self._summary_base: dict[str, list[int]] = {}
        self._last_tick = float("-inf")
        # replies
        self._reply_bucket = TokenBucket(REPLY_RATE_PER_S, REPLY_BURST, clock)
        self._reply_window = _ReplyWindow()
        # transmissions the ledger could not take at all, and replies withheld
        self._tx_refused = TxTally()
        self._fs = fs
        self._late_lost = TxTally()
        self._withheld = 0
        self._refused = False  # a transmission was refused (its error line names the stall)
        self._tx_announced = 0
        self._hung_reported = False
        scfg = StoreConfig(
            directory,
            cfg.max_file_bytes,
            cfg.essential_reserve_bytes if self._long else 0,
            self._long,
            command,
            ironbird.__version__,
            **dict(store_options or {}),  # type: ignore[arg-type]
        )
        self._store = LedgerStore(
            scfg,
            session_id or uuid.uuid4().hex[:12],
            self._start_ns,
            self._format_line,
            lambda: next(self._numbers),
            fs=fs,
            clock=clock,
            sleep=sleep,
            resume_info=self._resume_info,
        )
        self._writer = LedgerWriter(self._store, writer_poll_s)
        self._jobs_lines: deque[list[Line]] = deque()
        if cfg.evidence_bytes_per_hour is None:
            self.warn(
                Code.LED_UNBOUNDED,
                'evidence_bytes_per_hour = "unlimited": a flood of malformed packets can fill '
                "the disk",
                slow=True,
                gated=True,
            )

    # -- read-only state ------------------------------------------------------------------

    @property
    def directory(self) -> Path:
        return self._directory

    @property
    def session_id(self) -> str:
        return self._store.session_id

    @property
    def session_class(self) -> str:
        return "long" if self._long else "short"

    @property
    def start_ns(self) -> int:
        return self._start_ns

    @property
    def path(self) -> Path:
        """Location of this session's ledger part 1 (created at :meth:`open`)."""
        return self._directory / part_name(stamp_of(self._start_ns), self.session_id, 1)

    @property
    def part_number(self) -> int:
        """The part new records are being written to (1 until the ledger rotates)."""
        return self._store.active_number

    @property
    def part_file(self) -> str:
        return self._store.active_name

    @property
    def config(self) -> LedgerConfig:
        return self._config

    @property
    def pending(self) -> int:
        return len(self._queue)

    @property
    def pending_bytes(self) -> int:
        """Estimated size of the queued records, the figure the byte bound applies to."""
        return self._queued_bytes

    @property
    def dropped(self) -> int:
        """Records refused (and not yet summarised in the ledger) because the queue was full."""
        return sum(self._dropped)

    @property
    def stalled(self) -> bool:
        """An essential record cannot be written right now (failed, or a call hangs)."""
        return self._store.stalled or self._hung_now()

    @property
    def degraded(self) -> bool:
        """Event and evidence records are counted only (stalled, or writing into the reserve)."""
        return self._store.degraded or self._hung_now()

    @property
    def enabled(self) -> bool:
        return not self._dead

    @property
    def counted(self) -> Mapping[Tier, tuple[int, int]]:
        """Per tier: records and raw-field bytes counted only so far."""
        return {Tier(t): (self._counted_records[t], self._counted_bytes[t]) for t in Tier}

    @property
    def written_bytes(self) -> Mapping[Tier, int]:
        return {Tier(t): self._store.tier_bytes[t] for t in Tier}

    @property
    def sampled_away(self) -> int:
        return self._sampler.sampled_away

    @property
    def withheld(self) -> int:
        return self._withheld

    def _hung_now(self) -> bool:
        return self._store.hung(self._clock())

    def set_max_raw_bytes(self, max_raw_bytes: int) -> None:
        """Keep raw packet evidence up to this many bytes per field (the packet size cap)."""
        assert max_raw_bytes > 0
        self._max_raw_bytes = max_raw_bytes
        self._default = _make_default(max_raw_bytes)

    def mark_busy(self) -> None:
        """Called by hot paths; the drain task waits for ``IDLE_S`` of quiet after this."""
        self._last_busy = time.monotonic()

    def set_notifier(self, notify: Callable[[Notice], None] | None) -> None:
        """Where the ledger's own warnings go (the front-end). Notices raised earlier are
        delivered at once."""
        self._notify = notify
        if notify is not None and self._early:
            early, self._early = self._early, []
            for notice in early:
                notify(notice)

    def set_warn_gate(self, gate: Callable[[Code], bool]) -> None:
        """Cross-process rate limit of the start-of-session warnings (``IB-LED-006/008/009``):
        ``gate(code)`` says whether this one may be displayed (it is ledgered either way)."""
        self._warn_gate = gate

    def flush_notices(self) -> None:
        """Emit the ledger's pending coalesced summaries now (shutdown)."""
        self._coalescer.flush()

    def add_ticker(self, func: Callable[[], None]) -> None:
        """Run ``func`` from :meth:`tick` (retention's periodic scan)."""
        self._tickers.append(func)

    # -- producer side --------------------------------------------------------------------------

    def _cut(self, fields: dict[str, Any], names: list[str]) -> int:
        """Cut the bytes-like values ``names`` of ``fields`` to ``max_raw_bytes``; returns their
        charge.

        Each ends up an immutable ``bytes`` of at most ``max_raw_bytes`` (a buffer the producer
        reuses cannot change a queued record), and its full length is added as ``<name>_len``.
        ``fields`` is the caller's own keyword dictionary.
        """
        limit = self._max_raw_bytes
        charge = 0
        for name in names:
            value = fields[name]
            kind = type(value)
            if kind is memoryview:  # measured and cut in bytes, whatever its item size
                value = value.cast("B") if value.contiguous else memoryview(value.tobytes())
            size = len(value)
            if size > limit:
                fields[name] = _Cut(bytes(value[:limit]), size - limit)
                charge += limit
            else:
                fields[name] = value if kind is bytes else bytes(value)
                charge += size
            length_key = _LENGTH_KEYS.get(name) or _length_key(name)
            assert length_key not in fields, f"field {length_key} collides with a length key"
            fields[length_key] = size
        return charge

    def record(
        self,
        kind: str,
        event: str,
        *,
        tier: Tier | None = None,
        sample: str | None = None,
        **fields: Any,
    ) -> int | None:
        """Queue one record and return its record number (the ``n`` it is written with).

        Cheap by design: no formatting, no I/O. ``None`` means there is no record for a notice
        to point at: the ledger is disabled, the queue was full and this record was dropped
        (counted, and summarised by one ``ledger.dropped`` record), the record's tier is over its
        allowance or the ledger is writing into its reserve or stalled (counted only), or
        evidence sampling skipped this occurrence.

        ``tier`` defaults from :data:`RECORD_TIERS`, else from ``kind``. ``sample`` names the
        evidence-sampling key (code, MID, stream) of a record a flood can repeat.
        """
        assert kind in _KINDS, f"unknown ledger kind {kind!r}"
        assert not (fields.keys() & _RESERVED), "field name collides with a ledger envelope key"
        if self._dead:
            return None
        if tier is None:
            tier = RECORD_TIERS.get(event)
            if tier is None:
                tier = _KIND_TIER[kind]
        return self._admit(kind, event, tier, sample, fields, None)

    def _admit(
        self,
        kind: str,
        event: str,
        tier: int,
        sample: str | None,
        fields: dict[str, Any],
        tx: TxInfo | None,
    ) -> int | None:
        if tier:  # evidence and events are capped by their allowance and by the ledger's state
            self._since_refresh += 1
            if self._since_refresh >= REFRESH_EVERY:
                self._refresh_tiers()
            if self._counted[tier] or self._store.degraded or self._hung_reported:
                self._count_only(tier, fields)
                return None
            if self._gap[tier] is not None:
                self._refresh_tiers()
            if sample is not None and tier == Tier.EVIDENCE:
                raw = sum(len(v) for v in fields.values() if type(v) in _BYTES_LIKE)
                verdict = self._sampler.admit(
                    event,
                    sample,
                    time_ns=time.time_ns(),
                    seq=fields.get("seq"),
                    size=raw,
                    raw_len=raw,
                )
                if verdict is Admit.SKIP:
                    return None
                if verdict is Admit.NO_RAW:
                    for name in [k for k, v in fields.items() if type(v) in _BYTES_LIKE]:
                        fields[f"{name}_len"] = len(fields.pop(name))
                    fields["raw_omitted"] = True
        return self._enqueue(kind, event, fields, tier, tx)

    def _count_only(self, tier: int, fields: Mapping[str, Any]) -> None:
        raw = 0
        for value in fields.values():
            if type(value) in _BYTES_LIKE:
                raw += len(value)
        self._counted_records[tier] += 1
        self._counted_bytes[tier] += raw
        self._lost_total += 1
        now_ns = time.time_ns()
        gap = self._gap[tier]
        if gap is None:
            self._gap[tier] = [1, raw, now_ns, now_ns]
        else:
            gap[0] += 1
            gap[1] += raw
            gap[3] = now_ns

    def _enqueue(
        self, kind: str, event: str, fields: dict[str, Any], tier: int, tx: TxInfo | None
    ) -> int | None:
        queue = self._queue
        charge = ENTRY_OVERHEAD_BYTES
        names: list[str] | None = None
        for name, value in fields.items():
            value_type = type(value)
            if value_type is str:
                charge += len(value)
            elif value_type in _BYTES_LIKE:
                if names is None:
                    names = [name]
                else:
                    names.append(name)
        if names is not None:
            charge += self._cut(fields, names)
        queued_bytes = self._queued_bytes + charge
        if (
            len(queue) >= self._max_queue
            or queued_bytes > self._max_queue_bytes
            or (
                tier
                and (
                    self._queued[tier] >= self._queue_caps[tier]
                    or self._tier_bytes_q[tier] + charge > self._byte_caps[tier]
                )
            )
        ):
            self._dropped[tier] += 1
            self._dropped_total += 1
            self._lost_total += 1
            return None
        if not queue:
            self._oldest_mono = time.monotonic()
        self._any = True
        self._queued[tier] += 1
        self._tier_bytes_q[tier] += charge
        self._queued_bytes = queued_bytes
        if tx is not None:
            self._queued_tx += tx.count
        number = next(self._numbers)
        queue.append((time.time_ns(), number, kind, event, fields, tier, charge, tx))
        return number

    def record_error(
        self,
        error: IronbirdError,
        *,
        tier: Tier | None = None,
        sample: str | None = None,
        **fields: Any,
    ) -> int | None:
        """Record an error in full and hand it to the writer promptly (rate-limited under
        floods). Returns the record number as :meth:`record` does. ``tier`` defaults from the
        code: errors that received bytes can cause at will are evidence."""
        if tier is None:
            tier = Tier.EVIDENCE if error.code in EVIDENCE_ERRORS else Tier.ESSENTIAL
        number = self.record(
            "error",
            error.code.value,
            tier=tier,
            sample=sample,
            summary=error.summary,
            details=error.detail_text(),
            **fields,
        )
        if time.monotonic() - self._last_flush >= ERROR_FLUSH_MIN_INTERVAL_S:
            # One bounded slice here; the drain task finishes the rest without waiting for idle,
            # so a flood of errors never blocks the loop (and its socket reads) for long.
            self._hand_over(SLICE)
            self._last_flush = time.monotonic()
            self._urgent = bool(self._queue)
        return number

    # -- transmissions ---------------------------------------------------------------------------

    def cause_text(self) -> str:
        return self._store.stall_text or "the ledger is stalled"

    def remedy(self) -> str:
        return f"free space in {self._directory} or set IRONBIRD_STATE_DIR"

    def check_transmit(self, subject: str, target: str = "") -> None:
        """Under ``on_full = "refuse_commands"`` a stalled ledger refuses a transmission before
        anything is sent (``IB-LED-004``)."""
        if self._config.on_full == "refuse_commands" and self._tx_gate_closed():
            what = TxInfo(subject, target, time.time_ns()).to()
            self._refused = True
            raise IronbirdError(
                Code.LED_UNLOGGED,
                f"{what} refused: ledger not writable ({self.cause_text()}); {self.remedy()}",
            )

    def withhold_reply(self) -> bool:
        """``True`` when a reply must not be sent (``"refuse_commands"`` and a stalled ledger);
        the ledger counts it for the exit line."""
        if self._config.on_full == "refuse_commands" and self._tx_gate_closed():
            self._withheld += 1
            return True
        return False

    def _tx_gate_closed(self) -> bool:
        if self._dead:
            return False
        self._pump_events()
        return self.stalled

    def record_tx(
        self,
        event: str,
        subject: str,
        target: str = "",
        *,
        kind: str = "command",
        count: int = 1,
        **fields: Any,
    ) -> int | None:
        """Record a transmission (an essential record) and keep its trace: while the ledger is
        stalled a long session announces it (``IB-LED-004``, coalesced by code), and every
        transmission whose record is never written is counted for the exit line and exit 6."""
        if self._dead:
            return None
        info = TxInfo(subject, target, time.time_ns(), count, kind)
        number = self._admit("command", event, Tier.ESSENTIAL, None, fields, info)
        if number is None:
            self._tx_refused.add(info)
        if self._long and (number is None or self.stalled):
            self._tx_announced += count
            state = "stalled" if self.stalled else "queue full"
            self._coalescer.report(
                Code.LED_UNLOGGED,
                Level.WARNING,
                Code.LED_UNLOGGED.value,
                f"{info.sent()} but not logged (ledger {state})",
                ledger_n=number,
                ledger_part=self.part_number,
                ledger_file=self.part_file,
            )
        return number

    def record_reply(
        self, message: str, *, size: int, seq: int | None = None, **fields: Any
    ) -> int | None:
        """A responder's reply is a transmission, but the peer's commands set its rate: within
        the reply rate (10/s, burst 600) its ``tx.telemetry`` record is essential; beyond it
        the reply is counted into one ``respond.replies`` record per 10 s window."""
        now = self._clock()
        window = self._reply_window
        if window.count and now - window.started >= REPLY_WINDOW_S:
            self._close_reply_window()
        if self._reply_bucket.take():
            return self.record_tx(
                "tx.telemetry",
                f"reply {message}",
                kind="reply",
                message=message,
                size=size,
                sequence=seq,
                **fields,
            )
        window = self._reply_window
        if not window.count:
            window.started = now
            window.first_ns = time.time_ns()
            window.first_seq = seq
        window.count += 1
        window.bytes += size
        window.last_ns = time.time_ns()
        window.last_seq = seq
        if message in window.by_message or len(window.by_message) < MAX_REPLY_MESSAGES:
            window.by_message[message] = window.by_message.get(message, 0) + 1
        return None

    def _close_reply_window(self) -> None:
        window, self._reply_window = self._reply_window, _ReplyWindow()
        if not window.count:
            return
        self.record_tx(
            "respond.replies",
            "replies",
            kind="reply",
            count=window.count,
            replies=window.count,
            bytes=window.bytes,
            by_message=window.by_message,
            first_ns=window.first_ns,
            last_ns=window.last_ns,
            first_seq=window.first_seq,
            last_seq=window.last_seq,
        )

    # -- tiers ------------------------------------------------------------------------------------

    def _refresh_tiers(self) -> None:
        """Judge each capped tier against its bytes on disk and close a counted-only episode
        that is over (the gap marker precedes the next record of the tier)."""
        self._since_refresh = 0
        now = self._clock()
        degraded = self._store.degraded
        for tier in (Tier.EVIDENCE, Tier.EVENTS):
            budget = self._budgets[tier]
            assert budget is not None
            was = budget.over
            over = budget.update(self._store.tier_bytes[tier], now)
            self._counted[tier] = over
            if over and not was:
                self._tier_over(Tier(tier), budget, now)
            if self._gap[tier] is not None and not over and not degraded:
                self._close_gap(Tier(tier))

    def _tier_over(self, tier: Tier, budget: TierBudget, now: float) -> None:
        assert budget.bytes_per_hour is not None
        written = self._store.tier_bytes[tier]
        allowance = budget.allowance(now)
        self.record(
            "state",
            "ledger.tier_over",
            tier=Tier.ESSENTIAL,
            counted_tier=tier.label,
            written_bytes=written,
            allowance_bytes=int(allowance or 0),
            bytes_per_hour=budget.bytes_per_hour,
        )
        self.warn(
            Code.LED_TIER,
            f"{tier.label} records are over their allowance "
            f"({format_size(budget.bytes_per_hour)}/h, {format_size(written)} written): counted "
            "only until they are back under it",
            slow=True,
            key=f"{Code.LED_TIER.value}:{tier.label}",
        )

    def _close_gap(self, tier: Tier) -> None:
        gap = self._gap[tier]
        if gap is None:
            return
        self._gap[tier] = None
        self.record(
            "state",
            "ledger.tier_gap",
            tier=Tier.ESSENTIAL,
            counted_tier=tier.label,
            records=gap[0],
            bytes=gap[1],
            first=format_ts(gap[2]),
            last=format_ts(gap[3]),
        )

    # -- warnings and notices --------------------------------------------------------------------

    def _deliver(self, notice: Notice) -> None:
        if self._notify is not None:
            self._notify(notice)
        elif len(self._early) < MAX_EARLY_NOTICES:
            self._early.append(notice)

    def warn(
        self,
        code: Code,
        summary: str,
        *,
        level: Level = Level.WARNING,
        slow: bool = False,
        gated: bool = False,
        key: str | None = None,
        event: str = "ledger.warning",
        display: bool = True,
        **fields: Any,
    ) -> None:
        """Ledger a warning in full and display it (coalesced; ``gated`` ones go through the
        cross-process rate limit but are ledgered whatever it says; ``display=False`` only
        ledgers)."""
        allowed = display and (not gated or self._warn_gate(code))
        number = self.record(
            "warning",
            event,
            tier=Tier.ESSENTIAL,
            code=code.value,
            summary=summary,
            shown=allowed,
            **fields,
        )
        if not allowed:
            return
        self._coalescer.report(
            code,
            level,
            key or code.value,
            summary,
            ledger_n=number,
            window_s=SLOW_WARN_WINDOW_S if slow else None,
            ledger_part=self.part_number,
            ledger_file=self.part_file,
        )

    def _note(self, what: str, **data: Any) -> None:
        self.record("state", "ledger.note", tier=Tier.ESSENTIAL, note=what, **data)

    def _pump_events(self) -> None:
        """Turn the writer thread's events into ledger records and warnings."""
        events = self._store.events
        while events:
            name, data = events.popleft()
            self._on_store_event(name, data)

    def _on_store_event(self, name: str, data: dict[str, Any]) -> None:
        directory = self._directory
        match name:
            case "stall":
                self._lost_at_stall = self._lost_total
                self.record("state", "ledger.stalled", tier=Tier.ESSENTIAL, **data)
                if self._long:
                    self.warn(
                        Code.LED_WRITE,
                        f"ledger not writable ({data['text']}): records queued, retrying "
                        f"every 5 s; free space in {directory} or set IRONBIRD_STATE_DIR",
                        slow=True,
                    )
            case "resumed":
                if self._long:
                    self.warn(
                        Code.LED_RESUMED,
                        f"ledger writing again after {format_duration(data['duration'])}; "
                        f"{data['delayed']} delayed records written, {data['lost']} lost",
                        level=Level.INFO,
                        slow=True,
                    )
            case "disk_full":
                self.warn(
                    Code.LED_DISK,
                    f"disk or quota full ({data['text']}): essential records now go to the "
                    "reserve, event and evidence records are counted only",
                    slow=True,
                )
            case "reserve_problem":
                self.warn(
                    Code.LED_RESERVE,
                    f"essential reserve not available: {data['text']}; a full disk will stall "
                    "the ledger at once",
                    slow=True,
                    gated=True,
                )
            case "rotation_blocked":
                self.warn(
                    Code.LED_ROTATE,
                    f"ledger rotation blocked ({data['text']}): the current part keeps growing "
                    "past max_file_bytes",
                    slow=True,
                )
            case "no_lock":
                self.warn(
                    Code.LED_NOLOCK,
                    f"file locks are unavailable in {directory} ({data['text']}): this session "
                    "runs unlocked and its liveness rests on freshness",
                    slow=True,
                    gated=True,
                )
            case "dropped":
                self._lost_total += data["records"]
                self._counted_records[data["tier"]] += data["records"]
                self._counted_bytes[data["tier"]] += data["bytes"]
            case _:  # space_returned, reserve_ready, sync_error, retire_*, slow_call, internal
                self._note(name, **data)

    def _resume_info(self) -> Mapping[str, int]:
        """Called by the writer thread when a stall ends (plain reads of loop-side counters)."""
        return {
            "queued": len(self._queue),
            "queued_tx": self._queued_tx,
            "lost": max(0, self._lost_total - self._lost_at_stall),
        }

    # -- writer side -----------------------------------------------------------------------------

    def _format(self, entry: _Entry) -> str:
        """One JSON line for ``entry``. Never raises: bad fields degrade to a repr fallback."""
        return self._json(self.session_id, entry[1], entry[0], entry[2], entry[3], entry[4])

    def _json(
        self,
        session: str,
        seq: int,
        time_ns: int,
        kind: str,
        event: str,
        fields: Mapping[str, Any],
    ) -> str:
        record: dict[str, Any] = {
            "ts": format_ts(time_ns),
            "session": session,
            "n": seq,
            "kind": kind,
            "event": event,
        }
        record.update(fields)
        # ensure_ascii escapes control and non-ASCII characters, so the file is inert
        # when viewed in a terminal.
        with contextlib.suppress(Exception):  # a hostile Mapping can raise anything
            return json.dumps(record, default=self._default, separators=(",", ":"), allow_nan=False)
        try:
            record.update({k: _plain(v, frozenset(), 0) for k, v in fields.items()})
            return json.dumps(record, default=self._default, separators=(",", ":"), allow_nan=False)
        except Exception:
            envelope = {
                k: v for k, v in record.items() if k in ("ts", "session", "n", "kind", "event")
            }
            envelope["format_error"] = "fields could not be serialized"
            envelope["fields_repr"] = _safe_repr(dict(fields))
            return json.dumps(envelope, default=repr, separators=(",", ":"), allow_nan=False)

    def _format_line(
        self,
        session: str,
        seq: int,
        time_ns: int,
        kind: str,
        event: str,
        fields: Mapping[str, Any],
    ) -> bytes:
        return (self._json(session, seq, time_ns, kind, event, fields) + "\n").encode("ascii")

    def open(self) -> None:
        """Create the ledger directory and file now (on the writer thread, bounded to 5 s).

        Raises ``IB-LED-002`` when the location is wrong (only the user can fix it). A transient
        failure (disk or quota full, I/O error, a hung file server) does not raise: the ledger
        starts stalled and retries.
        """
        if self._dead or self._opened_by_caller:
            return
        self._opened_by_caller = True
        done = threading.Event()
        box: list[Any] = []
        self._writer.submit("open", (done, box))
        if not done.wait(self._open_timeout_s):
            return  # an open step is blocked: transient, the stall watchdog takes over
        failure = box[0] if box else None
        if isinstance(failure, ConfigFailure):
            self._dead = True
            exc = failure.cause
            raise IronbirdError(
                Code.LED_DIR,
                f"cannot use ledger directory {self._directory}: {exc.strerror or exc}; "
                "set IRONBIRD_STATE_DIR to a writable directory",
                cause=exc,
            ) from exc
        self._pump_events()

    def disable(self) -> None:
        """Discard everything from now on (the caller has already told the user)."""
        self._dead = True
        self._queue.clear()
        self._queued = [0] * TIER_COUNT
        self._tier_bytes_q = [0] * TIER_COUNT
        self._queued_bytes = 0
        self._queued_tx = 0

    def _check_config_failure(self) -> None:
        exc = self._store.config_failure
        if exc is None or self._dead:
            return
        self.disable()
        if self._on_failure is not None:
            self._on_failure(
                IronbirdError(
                    Code.LED_DIR,
                    f"cannot use ledger directory {self._directory}: {exc.strerror or exc}; "
                    "set IRONBIRD_STATE_DIR to a writable directory",
                    cause=exc,
                )
            )

    def _ensure_opened(self) -> None:
        if not self._opened_by_caller and not self._dead:
            try:
                self.open()
            except IronbirdError as err:
                if self._on_failure is not None:
                    self._on_failure(err)

    def _hand_over(self, limit: int, *, force: bool = False) -> int:
        """Format up to ``limit`` queued records and give them to the writer thread."""
        if self._dead or not self._queue:
            return 0
        if not force and (
            self._store.stalled or self._hung_now() or self._writer.pending_jobs >= MAX_WRITER_JOBS
        ):
            return 0
        self._ensure_opened()
        if self._dead:
            return 0
        lines: list[Line] = []
        count = min(limit, len(self._queue))
        self._drain_into(lines, count)
        if not self._queue:
            self._queue_dropped_marker(lines)
        self._oldest_mono = time.monotonic()
        self._jobs_lines.append(lines)
        self._writer.submit("write", (lines, force))
        self._prune_jobs()
        return count

    def _drain_into(self, lines: list[Line], count: int) -> None:
        queue, fmt, session = self._queue, self._format_line, self.session_id
        freed = 0
        for _ in range(count):
            time_ns, number, kind, event, fields, tier, charge, tx = queue.popleft()
            self._queued[tier] -= 1
            self._tier_bytes_q[tier] -= charge
            freed += charge
            if tx is not None:
                self._queued_tx -= tx.count
            lines.append(Line(number, tier, fmt(session, number, time_ns, kind, event, fields), tx))
        self._queued_bytes -= freed
        assert min(self._queued) >= 0 and self._queued_bytes >= 0

    def _prune_jobs(self) -> None:
        """Forget the batches the writer has finished with (what remains is in its hands)."""
        pending = self._writer.pending_jobs
        while len(self._jobs_lines) > pending + 1:
            self._jobs_lines.popleft()

    def _queue_dropped_marker(self, lines: list[Line]) -> None:
        """One record summarising refused records; formatted after the queue emptied, so its
        number follows every number already handed out."""
        total = sum(self._dropped)
        if not total:
            return
        essential, evidence, events = self._dropped
        self._dropped = [0] * TIER_COUNT
        number = next(self._numbers)
        fields = {"count": total, "evidence": evidence, "essential": essential, "events": events}
        data = self._format_line(
            self.session_id, number, time.time_ns(), "state", "ledger.dropped", fields
        )
        lines.append(Line(number, Tier.ESSENTIAL, data, None))

    def settle(self) -> None:
        """Close the open sampling and reply windows now, so that every occurrence that was
        counted but not written is accounted for by a summary record (the end of a session)."""
        if self._dead:
            return
        for summary in self._sampler.close_window():
            self._record_sample_summary(summary)
        self._close_reply_window()

    def flush(self) -> None:
        """Synchronously hand everything queued to the writer and wait (bounded) for it."""
        if self._dead or not self._any:
            return
        if self._exit_report is not None:
            self._write_late()
            return
        self._tick_records(final=False)
        while self._queue and self._hand_over(1024, force=True):
            pass
        if not self._queue:
            tail: list[Line] = []
            self._queue_dropped_marker(tail)
            if tail:
                self._writer.submit("write", (tail, True))
        self._last_flush = time.monotonic()
        self._writer.wait(self._join_timeout_s)
        self._check_config_failure()
        self._pump_events()

    # -- time-driven upkeep ----------------------------------------------------------------------

    def tick(self) -> None:
        """Periodic upkeep (about once a second, from the session's ticker or the drain task):
        sampler windows, the reply window, the minute summary, tier allowances, the stall
        watchdog, the writer thread's events and the coalescer's windows."""
        now = self._clock()
        if now - self._last_tick < TICK_MIN_S:
            return
        self._last_tick = now
        self._coalescer.tick()
        if self._dead:
            return
        self._pump_events()
        hung = self._hung_now()
        if hung and not self._hung_reported:
            self._hung_reported = True
            self._lost_at_stall = self._lost_total
            if self._long:
                self.warn(
                    Code.LED_WRITE,
                    "ledger not writable (a write has not completed in 5 s): records queued, "
                    f"retrying; check the disk or file server of {self._directory}",
                    slow=True,
                )
        elif not hung:
            self._hung_reported = False
        self._tick_records(final=False)
        for func in self._tickers:
            func()

    def _tick_records(self, *, final: bool) -> None:
        if self._dead:
            return
        self._refresh_tiers()
        if final or self._sampler.window_due():
            for summary in self._sampler.close_window():
                self._record_sample_summary(summary)
        window = self._reply_window
        if window.count and (final or self._clock() - window.started >= REPLY_WINDOW_S):
            self._close_reply_window()
        now = self._clock()
        if final or now - self._last_summary >= SUMMARY_INTERVAL_S:
            self._write_summary(final)
            self._last_summary = now

    def _record_sample_summary(self, summary: SampleSummary) -> None:
        self.record("state", f"{summary.event}.summary", tier=Tier.EVIDENCE, **summary.fields())

    def _write_summary(self, final: bool) -> None:
        """``ledger.summary``: per tier, what was written, counted only, sampled away and
        dropped since the last one. Written only when something was counted, sampled or
        dropped in the interval."""
        store = self._store
        now: dict[str, list[int]] = {
            "counted_records": list(self._counted_records),
            "counted_bytes": list(self._counted_bytes),
            "written_bytes": list(store.tier_bytes),
            "written_records": list(store.tier_records),
            "other": [self._sampler.sampled_away, self._dropped_total],
        }
        base = self._summary_base or {k: [0] * len(v) for k, v in now.items()}
        delta = {k: [c - b for c, b in zip(v, base[k], strict=True)] for k, v in now.items()}
        self._summary_base = now
        sampled, dropped = delta["other"]
        if not (any(delta["counted_records"]) or sampled or dropped):
            return
        tiers = {
            Tier(t).label: {key: delta[key][t] for key in list(delta)[:4]}
            for t in range(TIER_COUNT)
        }
        self.record(
            "state",
            "ledger.summary",
            tier=Tier.ESSENTIAL,
            tiers=tiers,
            sampled_away=sampled,
            queue_dropped=dropped,
            final=final,
        )

    async def drain_task(self) -> None:
        """Idle-priority writer. Runs until cancelled; the caller flushes afterwards."""
        while True:
            await asyncio.sleep(POLL_S)
            self.tick()
            if not self._queue:
                self._urgent = False
                continue
            now = time.monotonic()
            forced = (
                self._urgent
                or len(self._queue) >= FORCE_COUNT
                or now - self._oldest_mono >= FORCE_AGE_S
            )
            if not forced and now - self._last_busy < IDLE_S:
                continue
            self._hand_over(SLICE)
            self._last_flush = time.monotonic()
            await asyncio.sleep(0)  # yield between slices

    # -- closing ----------------------------------------------------------------------------------

    def finish(self) -> ExitReport:
        """The last write attempt and the account of what was not written, with the writer left
        running for the few records that follow (the caller's own exit records).

        Hands the whole queue to the writer, which writes it and syncs (a deferred error
        surfaces here). The writer is waited for at most ``join_timeout_s``; a writer still
        blocked then is left alone and everything it had not confirmed counts as not written.
        """
        if self._dead or not (self._any or self._opened_by_caller):
            return self._build_report(finished=True)
        self._ensure_opened()
        self._pump_events()
        self._tick_records(final=False)
        lines: list[Line] = []
        self._drain_into(lines, len(self._queue))
        self._queue_dropped_marker(lines)
        self._jobs_lines.append(lines)
        done = threading.Event()
        self._writer.submit("finish", (lines, done))
        finished = done.wait(self._join_timeout_s)
        self._check_config_failure()
        self._pump_events()
        return self._build_report(finished=finished)

    def close(self) -> ExitReport:
        """Write the rest, sync (a deferred error surfaces here), let an unused reserve hold
        the closing records and retire as the final part, and close the files.

        The writer is joined for at most ``join_timeout_s``; a writer still blocked then is left
        to die with the process and everything it had not confirmed counts as not written.
        Idempotent: later calls return the first report (records made since are appended,
        best effort, to the file the session ended in).
        """
        if self._exit_report is not None:
            self._write_late()
            return self._exit_report
        if self._dead or not (self._any or self._opened_by_caller):
            report = self._build_report(finished=True)
            self._exit_report = report
            return report
        self._ensure_opened()
        self._pump_events()
        self._tick_records(final=True)
        for tier in (Tier.EVIDENCE, Tier.EVENTS):
            self._close_gap(Tier(tier))
        lines: list[Line] = []
        self._drain_into(lines, len(self._queue))
        self._queue_dropped_marker(lines)
        self._jobs_lines.append(lines)
        done = threading.Event()
        self._writer.submit("close", (lines, done))
        finished = done.wait(self._join_timeout_s)
        self._check_config_failure()
        self._pump_events()
        report = self._build_report(finished=finished)
        self._exit_report = report
        return report

    def _write_late(self) -> None:
        """Records made after :meth:`close` (a signal announced while the ledger was closing)
        are appended, synchronously and best effort, to the file the session ended in."""
        lines: list[Line] = []
        self._drain_into(lines, len(self._queue))
        self._queue_dropped_marker(lines)
        name = self._store.final_name
        if not lines or name is None:
            return
        fs = self._fs
        try:
            dir_fd = fs.open_dir(self._directory)
            try:
                flags = os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW | os.O_CLOEXEC
                fd = fs.open_at(dir_fd, name, flags)
                try:
                    write_all(fs, fd, b"".join(line.data for line in lines))
                finally:
                    close_quietly(fs, fd)
            finally:
                close_quietly(fs, dir_fd)
        except OSError:
            for line in lines:
                if line.tx is not None:
                    self._late_lost.add(line.tx)

    def _build_report(self, *, finished: bool) -> ExitReport:
        store = self._store
        tally = TxTally()
        tally.merge(self._tx_refused)
        tally.merge(store.possibly_lost_tx)
        tally.merge(self._late_lost)
        unwritten: list[Line] = list(store.backlog)
        if not finished:  # the writer is stuck inside a call: its whole current job is unconfirmed
            for batch in self._jobs_lines:
                unwritten.extend(batch)
        records = len(unwritten) + len(self._queue) + store.possibly_lost_records
        tx_lines: list[str] = []
        for line in unwritten:
            if line.tx is not None:
                tally.add(line.tx)
                tx_lines.append(line.data.decode("ascii").rstrip("\n"))
        for entry in self._queue:
            if entry[7] is not None:
                tally.add(entry[7])
        stalled = (store.stalled or not finished) and bool(records)
        notices: list[Notice] = []
        if self._withheld:
            notices.append(
                _exit_notice(
                    Code.LED_UNLOGGED,
                    Level.ERROR,
                    f"{self._withheld} replies withheld: ledger not writable",
                )
            )
        if tally.count:
            notice = self._unlogged_notice(tally)
            if notice is not None:
                notices.append(notice)
        elif stalled and not (self._refused and not self._long):
            notices.append(
                _exit_notice(
                    Code.LED_WRITE,
                    Level.WARNING,
                    f"ledger stalled at exit; {records} records not written",
                )
            )
        return ExitReport(
            notices=tuple(notices),
            unlogged=tally.count,
            withheld=self._withheld,
            unwritten_records=records,
            unwritten_tx_lines=tuple(tx_lines),
            stalled=stalled,
        )

    def _unlogged_notice(self, tally: TxTally) -> Notice | None:
        first, last = tally.first, tally.last
        assert first is not None and last is not None
        cause = self._store.stall_text or "a write or sync failed"
        if tally.count == 1:
            if self._long and self._tx_announced >= 1 and not self._store.possibly_lost_tx.count:
                return None  # already announced when it was sent
            text = f"{first.sent()} but not logged (ledger not writable: {cause}); {self.remedy()}"
            return _exit_notice(Code.LED_UNLOGGED, Level.WARNING, text)
        noun = "commands" if tally.kinds == {"command"} else "transmissions"
        text = (
            f"{tally.count} {noun} sent but not logged (first {first.to()} at "
            f"{_clock_text(first.time_ns)}, last {last.to()} at {_clock_text(last.time_ns)})"
        )
        return _exit_notice(Code.LED_UNLOGGED, Level.WARNING, text)


def _exit_notice(code: Code, level: Level, summary: str) -> Notice:
    return Notice(code, level, code.value, summary)


def _clock_text(time_ns: int) -> str:
    return format_ts(time_ns)[11:]
