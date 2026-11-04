"""``sequence-state.json``: the CCSDS sequence counts of sent packets, kept across processes.

A count is per ``(node, kind, MID)``: ``node`` is the flight node the packets are for (the
target of ``ib send``, the node ``ib respond`` acts as), ``kind`` is command or telemetry and
``MID`` is the full message ID. MID rather than the profile's APID because a header layout is
user-definable (the profile names which fields come from the MID, and no field is "the APID"),
and because a receiver that checks counts per message ID (cFE's software bus does) sees an
unbroken series per MID either way. Two MIDs that share an APID count independently.

The file holds, per key, the *reservation mark*: the lowest count no process has handed out or
reserved yet. A process takes a block of :data:`BLOCK` counts under the lock, advances the mark
past it and hands the counts out of memory, so a burst of sends costs one lock, one temporary
file and two ``fsync`` calls per block, not per packet. The mark is durable (file and directory
synced) before the first count of its block is used, so a crash can only skip the unused rest of a
block, never reuse a count; skipping is allowed. A process that ends cleanly gives its unused
counts back when its block is still the newest reservation (the mark equals the end of its block),
so successive ``ib send`` runs count 0, 1, 2, ...; when another process reserved meanwhile the
unused counts are left as a gap. Counts handed out by concurrent processes are unique but
interleave by block, not by time.

The file is replaced whole (:class:`~ironbird.atomicfile.TempOutput`), never rewritten in
place, so a crash leaves the old or the new content. The lock is ``flock`` on a separate
:data:`LOCK_NAME` file, because the state file's inode changes at every replacement.

The file is *untrusted* on read: opened ``O_NOFOLLOW | O_NONBLOCK`` and checked with ``fstat``
(regular, ours, one link), at most :data:`MAX_BYTES`, strict schema, at most :data:`MAX_ENTRIES`
keys of at most :data:`MAX_KEY_LEN` characters, marks integers in ``[0, 2**64)``. Anything else
is ``IB-SEQ-001``: the file is treated as empty and the counts restart at 0 (the file is
replaced at the next reservation). A state directory that cannot be used (read-only, no file
locks, a lock held for more than a second, a name that cannot be replaced) is ``IB-SEQ-002``,
once: the counts are then kept in memory, restarting at 0 in every process.
"""

from __future__ import annotations

import errno
import fcntl
import hashlib
import json
import os
import stat
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, TypeVar

from ironbird.atomicfile import TempOutput
from ironbird.errors import Code, IronbirdError
from ironbird.fsops import (
    INSPECT_FLAGS,
    REAL_FS,
    FileSystem,
    close_quietly,
)

STATE_NAME: Final = "sequence-state.json"
LOCK_NAME: Final = "sequence-state.lock"
SCHEMA: Final = 1
MAX_BYTES: Final = 32768
MAX_ENTRIES: Final = 128
MAX_KEY_LEN: Final = 120
SPAN: Final = 1 << 64  # marks are kept modulo this; any sequence field is narrower
BLOCK: Final = 64  # counts reserved per lock, write and sync
LOCK_TRIES: Final = 100  # x 10 ms: the one-second bound on waiting for another process
LOCK_STEP_S: Final = 0.01
_LOCK_FLAGS: Final = (
    os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_NOCTTY | os.O_CLOEXEC
)

assert MAX_ENTRIES * (MAX_KEY_LEN + 32) + 64 <= MAX_BYTES, "a full file fits the size cap"

_T = TypeVar("_T")


_STEM_LIMIT: Final = MAX_KEY_LEN - 32  # room for "/telemetry/0x" and a 64-bit MID
_KINDS: Final = ("command", "telemetry")


def node_stem(node: str) -> str:
    """The node's part of a key. A long node name is cut and tagged with a digest, so two names
    never share a stem and a key stays within :data:`MAX_KEY_LEN`."""
    if len(node) <= _STEM_LIMIT:
        return node
    digest = hashlib.sha256(node.encode("utf-8", "surrogatepass")).hexdigest()[:16]
    return node[: _STEM_LIMIT - 17] + "~" + digest


def key_of(node: str, kind: str, mid: int) -> str:
    """The state key of a stream: ``<node>/<kind>/<mid in hex>``."""
    assert kind in _KINDS and 0 <= mid < SPAN
    return f"{node_stem(node)}/{kind}/{mid:#06x}"


def parse_key(key: str) -> tuple[str, str, int] | None:
    """``(node stem, kind, MID)`` of a key, or ``None`` for a key this module did not write."""
    stem, _, rest = key.rpartition("/")
    stem, _, kind = stem.rpartition("/")
    if not stem or kind not in _KINDS or not rest.startswith("0x"):
        return None
    try:
        return stem, kind, int(rest, 16)
    except ValueError:
        return None


def _refuse(token: str) -> object:
    raise ValueError(f"non-standard JSON constant {token}")


def parse_state(raw: bytes) -> dict[str, int] | None:
    """The marks a file holds, or ``None`` for anything that is not exactly
    ``{"v": 1, "next": {"<key>": <int in [0, 2**64)>, ...}}`` within the size limits."""
    if not raw or len(raw) > MAX_BYTES:
        return None
    try:
        data = json.loads(raw.decode("utf-8"), parse_constant=_refuse)
    except (ValueError, RecursionError):
        return None
    if not isinstance(data, dict) or set(data) != {"v", "next"} or data["v"] != SCHEMA:
        return None
    marks = data["next"]
    if not isinstance(marks, dict) or len(marks) > MAX_ENTRIES:
        return None
    out: dict[str, int] = {}
    for key, value in marks.items():
        if not isinstance(key, str) or not key or len(key) > MAX_KEY_LEN:
            return None
        if type(value) is not int or not 0 <= value < SPAN:
            return None
        out[key] = value
    return out


def render_state(marks: Mapping[str, int]) -> bytes:
    """The file's content; the least recently reserved keys go first when there are too many."""
    kept = dict(list(marks.items())[-MAX_ENTRIES:])
    while True:
        data = json.dumps({"v": SCHEMA, "next": kept}, separators=(",", ":")).encode("ascii")
        if len(data) < MAX_BYTES or not kept:  # keys of escaped characters can be long
            return data + b"\n"
        kept.pop(next(iter(kept)))


@dataclass(slots=True)
class _Block:
    next: int  # the next count to hand out
    end: int  # one past the last count reserved for this process


class SequenceStore:
    """The counts of one process. Not thread-safe: one event loop uses it."""

    def __init__(
        self,
        directory: Path,
        *,
        report: Callable[[IronbirdError], None],
        record: Callable[[str, dict[str, Any]], object] = lambda event, fields: None,
        fs: FileSystem = REAL_FS,
        block: int = BLOCK,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        assert block > 0
        self._directory: Final = directory
        self._report: Final = report
        self._record: Final = record
        self._fs: Final = fs
        self._block: Final = block
        self._sleep: Final = sleep
        self._blocks: dict[str, _Block] = {}
        self._memory: dict[str, int] = {}
        self._persist = True
        self._saw_invalid = False  # the file read in this transaction was invalid

    @property
    def persistent(self) -> bool:
        """Whether counts still come from the state file (``False`` after ``IB-SEQ-002``)."""
        return self._persist

    def for_node(self, node: str) -> NodeSequences:
        return NodeSequences(self, node)

    # -- the two operations -----------------------------------------------------------

    def take(self, key: str) -> int:
        """The next count of ``key`` (not reduced to a field width)."""
        if self._persist:
            block = self._blocks.get(key)
            if block is None or block.next >= block.end:
                start = self._reserve(key, None)
                if start is not None:
                    block = self._blocks[key] = _Block(start, start + self._block)
            if self._persist:
                assert block is not None and block.next < block.end
                block.next += 1
                return block.next - 1
        return self._take_memory(key)

    def seat(self, key: str, value: int) -> None:
        """Make ``value`` the last count handed out: the next :meth:`take` gives ``value + 1``."""
        assert 0 <= value < SPAN
        if self._persist:
            start = self._reserve(key, value + 1)
            if start is not None:
                self._blocks[key] = _Block(start, start + self._block)
                return
        self._memory[key] = value + 1
        self._blocks.pop(key, None)

    def _take_memory(self, key: str) -> int:
        count = self._memory.get(key)
        if count is None:  # carries on from the reserved block, if there was one
            block = self._blocks.get(key)
            count = block.next if block is not None else 0
        self._memory[key] = count + 1
        return count

    def close(self) -> None:
        """Give back the counts no packet used, where nobody reserved after this process."""
        if not self._persist or not self._blocks:
            return
        held = {key: block for key, block in self._blocks.items() if block.next < block.end}
        self._blocks.clear()
        if not held:
            return

        def release(marks: dict[str, int]) -> list[str]:
            given: list[str] = []
            for key, block in held.items():
                if marks.get(key) == block.end % SPAN:
                    marks[key] = block.next % SPAN
                    given.append(key)
            return given

        try:
            given = self._transact(release)
        except OSError as exc:
            # skipped counts are harmless and nothing the user can act on: ledgered only
            self._record("sequence.release_failed", {"reason": _reason(exc)})
            return
        self._record("sequence.release", {"keys": given})

    # -- inspection and reset (ib seq) --------------------------------------------------

    def stored(self) -> list[tuple[str, str, int, int]]:
        """Every stored stream as ``(node stem, kind, MID, mark)``, sorted. Raises
        :class:`IronbirdError` (``IB-SEQ-003``) when the file cannot be read."""
        marks = self._ask(dict)
        found = [(*parsed, mark) for key, mark in marks.items() if (parsed := parse_key(key))]
        return sorted(found)

    def forget(
        self, node: str | None = None, mid: int | None = None
    ) -> list[tuple[str, str, int, int]]:
        """Remove the stored counts of the streams of ``node`` (all nodes when ``None``), of MID
        ``mid`` (all MIDs when ``None``), so the next send of each starts at 0, for a store that
        holds no block of its own (the command line's). Returns what was removed as in
        :meth:`stored`. Raises :class:`IronbirdError` (``IB-SEQ-003``)."""
        stem = None if node is None else node_stem(node)

        def drop(marks: dict[str, int]) -> list[tuple[str, str, int, int]]:
            gone: list[tuple[str, str, int, int]] = []
            for key in list(marks):
                parsed = parse_key(key)
                if stem is None and mid is None:
                    chosen = True  # everything, a key of another writer included
                elif parsed is None:
                    chosen = False
                else:
                    chosen = (stem is None or parsed[0] == stem) and (
                        mid is None or parsed[2] == mid
                    )
                if chosen:
                    mark = marks.pop(key)
                    if parsed is not None:
                        gone.append((*parsed, mark))
            return sorted(gone)

        return self._ask(drop)

    def _ask(self, change: Callable[[dict[str, int]], _T]) -> _T:
        try:
            return self._transact(change)
        except OSError as exc:
            raise IronbirdError(
                Code.SEQ_STATE,
                f"cannot use the sequence counts in {self._directory} ({_reason(exc)}); "
                "check the directory or set IRONBIRD_STATE_DIR",
                details=f"{type(exc).__name__}: {exc}",
                cause=exc,
            ) from exc

    # -- the state file ---------------------------------------------------------------

    def _reserve(self, key: str, start: int | None) -> int | None:
        """Reserve a block for ``key`` in the file and return its first count: the mark, or
        ``start`` when given (the mark then moves to the end of the block from there). ``None``
        when the file cannot be used; counting then goes on in memory."""

        def reserve(marks: dict[str, int]) -> int:
            first = marks.get(key, 0) if start is None else start
            marks.pop(key, None)  # the newest reservation goes last: the oldest drop first
            marks[key] = (first + self._block) % SPAN
            return first

        try:
            first = self._transact(reserve)
        except OSError as exc:
            self._fall_back(exc)
            return None
        self._record(
            "sequence.reserve", {"key": key, "first": first, "count": self._block, "seat": start}
        )
        return first

    def _fall_back(self, exc: BaseException) -> None:
        self._persist = False
        why = _reason(exc)
        self._record("sequence.memory_only", {"directory": str(self._directory), "reason": why})
        self._report(
            IronbirdError(
                Code.SEQ_UNSAVED,
                f"cannot keep sequence counts in {self._directory} ({why}); counting in memory, "
                "so every ib process starts at 0 (set IRONBIRD_STATE_DIR to a writable "
                "directory)",
                details=f"{type(exc).__name__}: {exc}",
                cause=exc,
            )
        )

    def _transact(self, change: Callable[[dict[str, int]], _T]) -> _T:
        """Under the lock: read the marks, apply ``change``, and replace the file if it changed
        them. Raises ``OSError`` when the directory, the lock or the replacement fails."""
        fs = self._fs
        fs.mkdir_p(self._directory)
        dir_fd = fs.open_dir(self._directory)
        lock_fd = -1
        try:
            lock_fd = self._lock(dir_fd)
            self._saw_invalid = False
            marks = self._read(dir_fd)
            before = dict(marks)
            result = change(marks)
            if self._saw_invalid or marks != before or list(marks) != list(before):
                self._write(marks)  # an invalid file is replaced by what is valid now
            return result
        finally:
            if lock_fd >= 0:
                close_quietly(fs, lock_fd)  # closing the descriptor releases the lock
            close_quietly(fs, dir_fd)

    def _lock(self, dir_fd: int) -> int:
        fs = self._fs
        fd = fs.open_at(dir_fd, LOCK_NAME, _LOCK_FLAGS, 0o600)
        try:
            st = fs.fstat(fd)
            if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid():
                raise OSError(errno.EPERM, f"{LOCK_NAME} is not a regular file of this user")
            for _ in range(LOCK_TRIES):
                try:
                    fs.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError as exc:
                    if exc.errno in (errno.EWOULDBLOCK, errno.EAGAIN):
                        self._sleep(LOCK_STEP_S)
                        continue
                    raise  # no usable lock here: handing out counts unlocked could repeat them
                return fd
            raise OSError(errno.ETIMEDOUT, "another process held the lock for over a second")
        except BaseException:
            close_quietly(fs, fd)
            raise

    def _read(self, dir_fd: int) -> dict[str, int]:
        """The marks in the file; an invalid file is reported (``IB-SEQ-001``) and counts as
        empty, a missing one is just empty."""
        fs = self._fs
        try:
            fd = fs.open_at(dir_fd, STATE_NAME, INSPECT_FLAGS)
        except FileNotFoundError:
            return {}
        except OSError as exc:
            if exc.errno == errno.ELOOP:  # a symbolic link
                return self._invalid("is a symbolic link")
            raise
        try:
            st = fs.fstat(fd)
            if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() or st.st_nlink != 1:
                return self._invalid("is not a plain file of this user")
            if st.st_size > MAX_BYTES:
                return self._invalid(f"is over {MAX_BYTES} bytes")
            raw = fs.pread(fd, MAX_BYTES + 1, 0)
        finally:
            close_quietly(fs, fd)
        marks = parse_state(raw)
        if marks is None:
            digest = hashlib.sha256(raw).hexdigest()[:16]
            return self._invalid(
                "is not valid sequence state", f"{len(raw)} bytes, sha256 {digest}..."
            )
        return marks

    def _invalid(self, why: str, details: str = "") -> dict[str, int]:
        path = self._directory / STATE_NAME
        self._saw_invalid = True
        self._record("sequence.reset", {"path": str(path), "why": why, "details": details})
        self._report(
            IronbirdError(
                Code.SEQ_RESET,
                f"{path} {why}; sequence counts restart at 0 and the file is recreated",
                details=details,
            )
        )
        return {}

    def _write(self, marks: Mapping[str, int]) -> None:
        data = render_state(marks)
        with TempOutput(self._directory / STATE_NAME, force=True, fs=self._fs) as out:
            out.write(data)
            out.commit()


def _reason(exc: BaseException) -> str:
    if isinstance(exc, OSError):
        return exc.strerror or type(exc).__name__
    return str(exc) or type(exc).__name__


class NodeSequences:
    """The store as one node's codec sees it (see :class:`ironbird.codec.SequenceSource`)."""

    def __init__(self, store: SequenceStore, node: str) -> None:
        self._store: Final = store
        self._node: Final = node

    def take(self, kind: str, mid: int) -> int:
        return self._store.take(key_of(self._node, kind, mid))

    def seat(self, kind: str, mid: int, value: int) -> None:
        self._store.seat(key_of(self._node, kind, mid), value)
