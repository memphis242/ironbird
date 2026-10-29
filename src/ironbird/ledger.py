"""Action ledger: timestamped JSON Lines of actions, commands, state changes and errors.

Producers call :meth:`Ledger.record`, which only appends a tuple to a bounded deque (no
formatting, no I/O). Formatting and writing happen in :meth:`Ledger.drain_task`, an asyncio
task that runs only while the loop has been idle, in small slices, with a bounded staleness
(forced drain at ``FORCE_AGE_S`` age or ``FORCE_COUNT`` entries). :meth:`Ledger.flush` is
the synchronous path used on error, shutdown and signals.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import pwd
import time
import uuid
from collections import deque
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, Final

from ironbird.errors import Code, IronbirdError
from ironbird.header import DEFAULT_MAX_PACKET_BYTES

FORCE_AGE_S: Final = 1.0
FORCE_COUNT: Final = 10_000
IDLE_S: Final = 0.02
POLL_S: Final = 0.01
SLICE: Final = 256
MAX_QUEUE: Final = 200_000
ERROR_FLUSH_MIN_INTERVAL_S: Final = 0.25
MAX_DEPTH: Final = 16
MAX_FALLBACK_REPR: Final = 2000

_RESERVED: Final = frozenset({"ts", "session", "n", "kind", "event"})
_KINDS: Final = frozenset({"action", "command", "state", "packet", "event", "warning", "error"})

_Entry = tuple[int, int, str, str, Mapping[str, Any]]


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
        if isinstance(value, bytes | bytearray | memoryview):
            raw = bytes(value)
            text = raw[:max_raw_bytes].hex(" ")
            return (
                text if len(raw) <= max_raw_bytes else f"{text} ...(+{len(raw) - max_raw_bytes}B)"
            )
        if isinstance(value, Path):
            return str(value)
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
    ) -> None:
        assert max_raw_bytes > 0
        self.directory: Path = directory
        self.session_id: str = session_id or uuid.uuid4().hex[:12]
        self._on_failure = on_failure
        self._enabled = enabled
        self._default = _make_default(max_raw_bytes)
        self._urgent = False
        self._queue: deque[_Entry] = deque(maxlen=MAX_QUEUE)
        self._seq = 0
        self._dropped = 0
        self._last_busy = 0.0
        self._last_flush = float("-inf")  # the first error flushes at once
        self._oldest_mono = 0.0
        self._file: Any = None
        self._path: Path | None = None
        self._failed = False
        self._start_ns = time.time_ns()

    # -- producer side -------------------------------------------------------------------

    @property
    def path(self) -> Path:
        """Location of this session's ledger file (created on first write)."""
        if self._path is None:
            stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(self._start_ns // 1_000_000_000))
            self._path = self.directory / f"{stamp}-{self.session_id}.jsonl"
        return self._path

    @property
    def pending(self) -> int:
        return len(self._queue)

    @property
    def dropped(self) -> int:
        return self._dropped

    def set_max_raw_bytes(self, max_raw_bytes: int) -> None:
        """Keep raw packet evidence up to this many bytes per field (the packet size cap)."""
        assert max_raw_bytes > 0
        self._default = _make_default(max_raw_bytes)

    def mark_busy(self) -> None:
        """Called by hot paths; the drain task waits for ``IDLE_S`` of quiet after this."""
        self._last_busy = time.monotonic()

    def record(self, kind: str, event: str, **fields: Any) -> None:
        """Queue one record. Cheap by design: no formatting, no I/O."""
        assert kind in _KINDS, f"unknown ledger kind {kind!r}"
        assert not (fields.keys() & _RESERVED), "field name collides with a ledger envelope key"
        if not self._enabled or self._failed:
            return
        queue = self._queue
        if len(queue) == MAX_QUEUE:
            self._dropped += 1
        elif not queue:
            self._oldest_mono = time.monotonic()
        self._seq += 1
        queue.append((time.time_ns(), self._seq, kind, event, fields))

    def record_error(self, error: IronbirdError, **fields: Any) -> None:
        """Record an error in full and flush promptly (rate-limited under floods)."""
        self.record(
            "error",
            error.code.value,
            summary=error.summary,
            details=error.detail_text(),
            **fields,
        )
        if time.monotonic() - self._last_flush >= ERROR_FLUSH_MIN_INTERVAL_S:
            # One bounded slice here; the drain task finishes the rest without waiting for idle,
            # so a flood of errors never blocks the loop (and its socket reads) for long.
            self._drain(SLICE)
            self._last_flush = time.monotonic()
            self._urgent = bool(self._queue)

    # -- writer side ---------------------------------------------------------------------

    def _format(self, entry: _Entry) -> str:
        """One JSON line for ``entry``. Never raises: bad fields degrade to a repr fallback."""
        time_ns, seq, kind, event, fields = entry
        record: dict[str, Any] = {
            "ts": format_ts(time_ns),
            "session": self.session_id,
            "n": seq,
            "kind": kind,
            "event": event,
        }
        record.update(fields)
        # ensure_ascii escapes control and non-ASCII characters, so the file is inert
        # when viewed in a terminal.
        try:
            return json.dumps(record, default=self._default, separators=(",", ":"), allow_nan=False)
        except (TypeError, ValueError, RecursionError):
            pass
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

    def _fail(self, exc: OSError) -> None:
        self._failed = True
        error = IronbirdError(
            Code.LED_WRITE,
            f"cannot write ledger {self.path}: {exc.strerror or exc}; ledger disabled "
            "(set IRONBIRD_STATE_DIR to a writable directory)",
            details=f"directory {self.directory}",
            cause=exc,
        )
        if self._on_failure is not None:
            self._on_failure(error)

    def _open_file(self) -> None:
        if self._file is None:
            self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
            fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            self._file = os.fdopen(fd, "a", encoding="utf-8")

    def open(self) -> None:
        """Create the ledger directory and file now.

        Raises ``IB-LED-002`` when they cannot be created, so a caller that must not act without
        a trace (sending a command) can refuse before it does.
        """
        if not self._enabled:
            return
        try:
            self._open_file()
        except OSError as exc:
            raise IronbirdError(
                Code.LED_DIR,
                f"cannot use ledger directory {self.directory}: {exc.strerror or exc}; "
                "set IRONBIRD_STATE_DIR to a writable directory",
                cause=exc,
            ) from exc

    def disable(self) -> None:
        """Discard everything from now on (the caller has already told the user)."""
        self._failed = True
        self._queue.clear()

    def _write_lines(self, lines: list[str]) -> bool:
        if self._failed:
            return False
        try:
            self._open_file()
            self._file.write("\n".join(lines) + "\n")
            self._file.flush()
        except OSError as exc:
            self._fail(exc)
            return False
        return True

    def _drain(self, limit: int) -> int:
        count = min(limit, len(self._queue))
        if count == 0:
            return 0
        entries = [self._queue.popleft() for _ in range(count)]
        lines = [self._format(entry) for entry in entries]
        if self._dropped:
            dropped, self._dropped = self._dropped, 0
            lines.append(
                self._format(
                    (time.time_ns(), self._seq + 1, "state", "ledger.dropped", {"count": dropped})
                )
            )
            self._seq += 1
        self._write_lines(lines)
        self._oldest_mono = time.monotonic()
        return count

    def flush(self) -> None:
        """Synchronously write everything queued."""
        while self._queue:
            self._drain(1024)
        if self._dropped:
            self._drain(1)
        self._last_flush = time.monotonic()

    def close(self) -> None:
        self.flush()
        if self._file is not None:
            try:
                self._file.close()
            except OSError as exc:
                self._fail(exc)
            self._file = None

    async def drain_task(self) -> None:
        """Idle-priority writer. Runs until cancelled; the caller flushes afterwards."""
        while True:
            await asyncio.sleep(POLL_S)
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
            self._drain(SLICE)
            self._last_flush = time.monotonic()
            await asyncio.sleep(0)  # yield between slices
