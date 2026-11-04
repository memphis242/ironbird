"""CCSDS sequence counts of sent packets: exact, in take order, shared by every ironbird process.

A count is per stream ``(node, kind, MID)``: ``node`` is the flight node the packets are for (the
target of ``ib send``, the node ``ib respond`` acts as), ``kind`` is command or telemetry and
``MID`` is the full message ID. MID rather than the profile's APID because a header layout is
user-definable (the profile names which fields come from the MID, and no field is "the APID"),
and because a receiver that checks counts per message ID (cFE's software bus does) sees an
unbroken series per MID either way. Two MIDs that share an APID count independently.

Two files in the state directory hold the counts.

- :data:`LIVE_NAME`, the *live table*: a fixed binary layout (:data:`HEAD_BYTES` of header, then
  :data:`LIVE_SLOTS` slots of :data:`SLOT_BYTES`, open addressing on a hash of the key). A slot
  holds a stream's key, the next count to hand out and its *ceiling*. Every count is taken under
  ``flock`` on this file by reading and rewriting one slot, so counts are exact and in take order
  across processes, and ``ib seq reset`` or ``--sequence`` from any process is seen by the next
  take of every process. The file is never synced: it lives in the page cache, which outlives
  any process, so a process crash loses nothing. The header names the boot it was written in
  (``/proc/sys/kernel/random/boot_id``); a table from another boot, whose page cache a power loss
  may have dropped, is rebuilt from the durable ceilings.
- :data:`STATE_NAME`, the *durable ceilings*: per stream, a count above every count ever handed
  out. It is replaced whole (:class:`~ironbird.atomicfile.TempOutput`, file and directory synced)
  under its own lock :data:`LOCK_NAME` *before* a slot's ceiling is raised to the new value, and
  is at most :data:`BLOCK` above the next count, so a power loss skips at most :data:`BLOCK`
  counts of a stream and never reuses one.

Invariant, per stream: ``next <= ceiling <= durable`` (a missing durable entry counts as 0).
Only a holder of the durable lock changes a slot's ceiling or the table's layout (insert,
rebuild, reset); a take only moves ``next`` up to the ceiling. Lock order is durable, then live;
the live lock is never held across a sync or a wait for the durable lock.

Off the event loop: a take on the loop is a non-blocking ``flock``, one ``fstatat`` (the live
file must still be the one this process opened), and a ``pread`` and ``pwrite`` of one slot.
Everything that can block (raising a ceiling, which syncs; inserting a stream; rebuilding;
opening the files) runs in a worker thread (:class:`~ironbird.offloop.OffLoop`) whose lock waits
are bounded by :data:`LOCK_WAIT_S`, and the loop stops waiting for it at that bound (an abandoned
worker ends on its own, and its records still reach the ledger). A contended live lock is polled
every :data:`POLL_FAST_S` by a helper thread (a sleep on the loop is at least a millisecond), so
the loop is never blocked and the wait ends within a poll of the holder's release. A long-lived
sender (:meth:`SequenceStore.keep_ahead`, ``ib respond``) raises a ceiling in the background once
half a step is left, so its takes do not wait for a sync.

A series of sends (``ib send --repeat/--every/--at``) builds its packets ahead of their slots on
the loop and sends them from the pacer thread, so it cannot hold a lease from take to hand-off.
It takes each packet's count when the packet is built, without waiting
(:meth:`SequenceStore.take_now`: the take above, committed and released at once) or, when that
would wait, in a task the series resumes from (:class:`SeriesCounts`); the pacer thread never
takes. Counts are unique and persistent across processes, but are handed to the kernel in count
order only within one process: two processes running series on one stream interleave their
packets, each in its own count order, as two processes on one serial line do. A packet built and
never sent (discarded, unsent, or a series that ended) spends its count: a gap.

Failure paths. A lock that cannot be had within the bound, a worker that does not answer within
it, or a state file that cannot be read or replaced once the store is open, fails the take
(``IB-SEQ-006``, nothing is sent): a count is never handed out unrecorded. Only a state directory
that cannot be used *from the start* (read-only, full, no file locks, a planted file in the way,
no boot id) makes the process count in memory (``IB-SEQ-002``, once), and then from every ceiling
that can still be read in the directory (the durable file, a refused one that still parses
included, and the live table), so it starts above every count handed out before; only a
directory with nothing readable starts at 0. :data:`MAX_STREAMS` streams fill the durable file;
another one is refused (``IB-SEQ-005``, ledgered), never evicted.

Both files are *untrusted* on read. :data:`STATE_NAME` is opened ``O_NOFOLLOW | O_NONBLOCK`` and
checked with ``fstat`` (regular, ours, one link), at most :data:`MAX_BYTES`, strict schema, at
most :data:`MAX_STREAMS` keys, each spelled as :func:`key_of` writes it, ceilings in
``[0, MAX_COUNT)`` - the one bound for every stored count, on read and on write (a ceiling is
never raised past it). An invalid one is ``IB-SEQ-001``: a send or reset moves it aside to
:data:`INVALID_NAME` (one slot, the newest kept) and rebuilds it from the live table, and from
the file itself when only its ``fstat`` checks failed and it still parses (a hard link, a file of
another user): its values are a floor, so a forger can make counts skip but never repeat. ``ib
seq show`` leaves it untouched. :data:`LIVE_NAME` must be a regular file of ours with one link;
a wrong size, magic, version, header checksum or boot, or a damaged slot (checksum, key,
``next > ceiling``), rebuilds the table (``IB-SEQ-004``), so every stream carries on from its
durable ceiling, or from a valid slot's ceiling when that is higher (a durable file that is gone
or older than the table it sat beside).
"""

from __future__ import annotations

import asyncio
import contextlib
import errno
import fcntl
import functools
import hashlib
import json
import os
import re
import stat
import struct
import threading
import time
import uuid
import zlib
from collections.abc import Callable, Generator, Iterable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import Any, Final, Literal, TypeVar

from ironbird.atomicfile import TempOutput
from ironbird.errors import Code, IronbirdError
from ironbird.fsops import INSPECT_FLAGS, REAL_FS, FileSystem, close_quietly, read_capped
from ironbird.offloop import OffLoop, OffLoopBusy

STATE_NAME: Final = "sequence-state.json"
LOCK_NAME: Final = "sequence-state.lock"
LIVE_NAME: Final = "sequence-live"
INVALID_NAME: Final = STATE_NAME + ".invalid"
SCHEMA: Final = 1
MAX_STREAMS: Final = 4096  # streams with a stored count, all nodes together
MAX_KEY_LEN: Final = 64
MAX_BYTES: Final = 384 * 1024
MAX_COUNT: Final = 1 << 62  # every stored count and ceiling, read or written, stays below this
SPAN: Final = 1 << 64  # what one slot field holds
BLOCK: Final = 64  # the step: a durable ceiling is at most this far above the next count
AHEAD_AT: Final = BLOCK // 2  # a long-lived sender raises a ceiling when this many are left
LOCK_WAIT_S: Final = 10.0  # the bound on every wait for a lock of the state files
CALL_GRACE_S: Final = 0.25  # how long past its own bound the loop waits for a worker's answer
POLL_FAST_S: Final = 50e-6  # the helper thread's poll of a contended live lock, at first
POLL_FAST_FOR_S: Final = 5e-3  # ...for this long, then slower
POLL_MID_S: Final = 200e-6
POLL_MID_FOR_S: Final = 0.1
POLL_SLOW_S: Final = 1e-3
LIVE_SLOTS: Final = 2 * MAX_STREAMS  # load at most one half: short probes
LIVE_MAGIC: Final = b"IBSQLIV1"
LIVE_VERSION: Final = 1
PROBE_ON_LOOP: Final = 16  # slots a take on the loop reads to find a stream it has not cached
BOOT_ID_PATH: Final = Path("/proc/sys/kernel/random/boot_id")

# header: magic, version, slot count, slot size, flags (0), boot id, host; then its CRC-32
_HEAD_BODY: Final = struct.Struct("<8sIIII16s64s20x")
_CRC: Final = struct.Struct("<I")
HEAD_BYTES: Final = _HEAD_BODY.size + _CRC.size
# slot: key (ASCII, NUL padded), next, ceiling; then its CRC-32 and four zero bytes
_SLOT_BODY: Final = struct.Struct(f"<{MAX_KEY_LEN}sQQ")
_COUNTS: Final = struct.Struct("<QQ")
SLOT_BYTES: Final = _SLOT_BODY.size + _CRC.size + 4
LIVE_BYTES: Final = HEAD_BYTES + LIVE_SLOTS * SLOT_BYTES
_EMPTY_SLOT: Final = bytes(SLOT_BYTES)
_PAD: Final = b"\0\0\0\0"

assert HEAD_BYTES == 128 and SLOT_BYTES == 88, "the live layout is fixed"
assert MAX_STREAMS * (MAX_KEY_LEN + 24) + 32 <= MAX_BYTES, "a full durable file fits its cap"
assert LIVE_SLOTS >= 2 * MAX_STREAMS and LIVE_SLOTS & (LIVE_SLOTS - 1) == 0
assert BLOCK >= 2 and 0 < AHEAD_AT < BLOCK and MAX_COUNT + BLOCK < SPAN

_OPEN_FLAGS: Final = (
    os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_NOCTTY | os.O_CLOEXEC
)
_BUSY: Final = frozenset({errno.EWOULDBLOCK, errno.EAGAIN, errno.EINTR})
# what makes a state directory unusable from the start, so that counting in memory is the only way
_UNUSABLE: Final = frozenset(
    {
        errno.EROFS,
        errno.EACCES,
        errno.EPERM,
        errno.ENOLCK,
        errno.EOPNOTSUPP,
        errno.ENOSPC,
        errno.EDQUOT,
        errno.ENOTDIR,
        errno.EISDIR,
        errno.ELOOP,
        errno.EEXIST,
        errno.EINVAL,
    }
)

_T = TypeVar("_T")
Row = tuple[str, str, int, int]  # (node stem, kind, MID, next count)
Stored = tuple[str, str, int, int, int]  # (node stem, kind, MID, next count, durable ceiling)

_KINDS: Final = ("command", "telemetry")
_KEY_RE: Final = re.compile(rf"[A-Za-z0-9_~/\-]{{1,{MAX_KEY_LEN}}}")
_STEM_RE: Final = re.compile(r"[A-Za-z0-9_\-]{1,32}")  # what a configured node name can be
_STEM_LIMIT: Final = MAX_KEY_LEN - len("/telemetry/0x") - 16  # room for a 64-bit MID
_STEM_KEY_RE: Final = re.compile(rf"[A-Za-z0-9_~\-]{{1,{_STEM_LIMIT}}}")  # what node_stem makes
assert _STEM_LIMIT >= 32


def node_stem(node: str) -> str:
    """The node's part of a key: the name itself when it is a configured node name. Any other
    text is cut to the key alphabet and tagged with a digest, so two names never share a stem."""
    if _STEM_RE.fullmatch(node):
        return node
    digest = hashlib.sha256(node.encode("utf-8", "surrogatepass")).hexdigest()[:16]
    return re.sub(r"[^A-Za-z0-9_\-]", "", node)[: _STEM_LIMIT - 17] + "~" + digest


def key_of(node: str, kind: str, mid: int) -> str:
    """The state key of a stream: ``<node>/<kind>/<mid in hex>``."""
    assert kind in _KINDS and 0 <= mid < SPAN
    key = f"{node_stem(node)}/{kind}/{mid:#06x}"
    assert _KEY_RE.fullmatch(key), key
    return key


def parse_key(key: str) -> tuple[str, str, int] | None:
    """``(node stem, kind, MID)`` of a key spelled exactly as :func:`key_of` writes it."""
    stem, _, rest = key.rpartition("/")
    stem, _, kind = stem.rpartition("/")
    if not _STEM_KEY_RE.fullmatch(stem) or kind not in _KINDS or not rest.startswith("0x"):
        return None
    try:
        mid = int(rest, 16)
    except ValueError:
        return None
    if mid >= SPAN or rest != f"{mid:#06x}" or not _KEY_RE.fullmatch(key):
        return None  # one stream has one spelling
    return stem, kind, mid


def stream_text(key: str) -> str:
    """A key as a message names it: ``fc command 0x1890``."""
    parsed = parse_key(key)
    return f"{parsed[0]} {parsed[1]} 0x{parsed[2]:04X}" if parsed else key


def _refuse(token: str) -> object:
    raise ValueError(f"non-standard JSON constant {token}")


def parse_state(raw: bytes) -> dict[str, int] | None:
    """The durable ceilings a file holds, or ``None`` for anything that is not exactly
    ``{"v": 1, "next": {"<key>": <int in [0, MAX_COUNT)>, ...}}`` within the limits, every key
    spelled as :func:`key_of` writes it (a key no stream has is not a stream to count)."""
    if not raw or len(raw) > MAX_BYTES:
        return None
    try:
        data = json.loads(raw.decode("utf-8"), parse_constant=_refuse)
    except (ValueError, RecursionError):
        return None
    if not isinstance(data, dict) or set(data) != {"v", "next"} or data["v"] != SCHEMA:
        return None
    marks = data["next"]
    if not isinstance(marks, dict) or len(marks) > MAX_STREAMS:
        return None
    out: dict[str, int] = {}
    for key, value in marks.items():
        if not isinstance(key, str) or parse_key(key) is None:
            return None
        if type(value) is not int or not 0 <= value < MAX_COUNT:
            return None
        out[key] = value
    return out


def render_state(marks: Mapping[str, int]) -> bytes:
    """The durable file's content. Never drops a key: the caller keeps to :data:`MAX_STREAMS`."""
    assert len(marks) <= MAX_STREAMS
    assert all(parse_key(k) is not None and 0 <= v < MAX_COUNT for k, v in marks.items())
    data = json.dumps({"v": SCHEMA, "next": dict(sorted(marks.items()))}, separators=(",", ":"))
    out = data.encode("ascii") + b"\n"
    assert len(out) <= MAX_BYTES  # the key alphabet needs no escapes
    return out


def read_boot_id() -> bytes:
    """16 bytes that name this boot of the machine. Raises ``OSError`` when there are none."""
    raw = read_capped(BOOT_ID_PATH, 64)
    try:
        return uuid.UUID(raw.decode("ascii").strip()).bytes
    except (UnicodeDecodeError, ValueError) as exc:
        raise OSError(errno.EINVAL, f"{BOOT_ID_PATH} holds no boot id") from exc


def _host() -> bytes:
    return os.uname().nodename.encode("utf-8", "replace")[:64]


# -- the live table's layout ---------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Slot:
    key: str  # "" for an empty slot
    next: int
    ceiling: int


_EMPTY: Final = _Slot("", 0, 0)


class _Garbage(Exception):
    """A slot of the live table that no ironbird process wrote."""


def _pack_slot(key: str, next_: int, ceiling: int) -> bytes:
    assert _KEY_RE.fullmatch(key) and 0 <= next_ <= ceiling < SPAN
    body = _SLOT_BODY.pack(key.encode("ascii"), next_, ceiling)
    return body + _CRC.pack(zlib.crc32(body)) + _PAD


def _unpack_slot(raw: bytes) -> _Slot:
    if raw == _EMPTY_SLOT:
        return _EMPTY
    if len(raw) != SLOT_BYTES or raw[-4:] != _PAD:
        raise _Garbage
    body = raw[: _SLOT_BODY.size]
    if _CRC.unpack_from(raw, _SLOT_BODY.size)[0] != zlib.crc32(body):
        raise _Garbage
    padded, next_, ceiling = _SLOT_BODY.unpack(body)
    try:
        key = padded.rstrip(b"\0").decode("ascii")
    except UnicodeDecodeError:
        raise _Garbage from None
    if parse_key(key) is None or not next_ <= ceiling < MAX_COUNT:
        raise _Garbage
    return _Slot(key, next_, ceiling)


@functools.lru_cache(maxsize=1024)
def _padded(key: str) -> bytes:
    """A key as a slot holds it (the take on the loop compares and writes it as bytes)."""
    assert _KEY_RE.fullmatch(key)
    return key.encode("ascii").ljust(MAX_KEY_LEN, b"\0")


def _own_slot(raw: bytes, padded: bytes) -> tuple[int, int] | None:
    """``(next, ceiling)`` of a slot that holds the key ``padded``, checked as
    :func:`_unpack_slot` checks it; ``None`` for any other slot (including a damaged one)."""
    if len(raw) != SLOT_BYTES or not raw.startswith(padded) or raw[-4:] != _PAD:
        return None
    if _CRC.unpack_from(raw, _SLOT_BODY.size)[0] != zlib.crc32(raw[: _SLOT_BODY.size]):
        return None
    next_, ceiling = _COUNTS.unpack_from(raw, MAX_KEY_LEN)
    if not next_ <= ceiling < MAX_COUNT:
        return None
    return next_, ceiling


def _pack_own_slot(padded: bytes, next_: int, ceiling: int) -> bytes:
    assert len(padded) == MAX_KEY_LEN and 0 <= next_ <= ceiling < SPAN
    body = padded + _COUNTS.pack(next_, ceiling)
    return body + _CRC.pack(zlib.crc32(body)) + _PAD


def _home(key: str) -> int:
    digest = hashlib.blake2b(key.encode("ascii"), digest_size=8).digest()
    return int.from_bytes(digest, "little") & (LIVE_SLOTS - 1)


def _offset(index: int) -> int:
    assert 0 <= index < LIVE_SLOTS
    return HEAD_BYTES + index * SLOT_BYTES


def _pack_head(boot: bytes) -> bytes:
    assert len(boot) == 16
    body = _HEAD_BODY.pack(LIVE_MAGIC, LIVE_VERSION, LIVE_SLOTS, SLOT_BYTES, 0, boot, _host())
    return body + _CRC.pack(zlib.crc32(body))


def _backoff(step: float, waited: float) -> float:
    """The next pause of a poll for a lock: short while the holder is likely a take or a sync
    (microseconds to milliseconds), longer once it has held on for a while."""
    if waited < 0.1:
        return min(max(step * 2, 50e-6), 1e-3)
    return 0.01


def _poll_step(waited: float) -> float:
    """The pause of the helper thread's poll of the live lock after ``waited`` seconds: short
    while the holder is a take (tens of microseconds), longer once it has held on for a while."""
    if waited < POLL_FAST_FOR_S:
        return POLL_FAST_S
    return POLL_MID_S if waited < POLL_MID_FOR_S else POLL_SLOW_S


def _scan_slots(raw: bytes) -> dict[str, int]:
    """The ceiling of each stream in the slots of ``raw`` (a table without its header) that are
    valid, whatever else is damaged; a stream in two slots keeps the higher. Never raises."""
    out: dict[str, int] = {}
    for start in range(0, len(raw) - SLOT_BYTES + 1, SLOT_BYTES):
        try:
            slot = _unpack_slot(raw[start : start + SLOT_BYTES])
        except _Garbage:
            continue
        if slot.ceiling > out.get(slot.key, 0):
            out[slot.key] = slot.ceiling
    return out


def _merged(into: dict[str, int], more: Mapping[str, int]) -> None:
    """Raise each count of ``into`` to the one in ``more``."""
    for key, value in more.items():
        if value > into.get(key, 0):
            into[key] = value


def _capped(marks: dict[str, int]) -> dict[str, int]:
    """``marks`` kept to :data:`MAX_STREAMS` entries: the streams with the highest ceilings."""
    if len(marks) <= MAX_STREAMS:
        return marks
    top = sorted(((v, k) for k, v in marks.items()), reverse=True)[:MAX_STREAMS]
    return {key: value for value, key in top}


def _pwrite_all(fs: FileSystem, fd: int, data: bytes, offset: int) -> None:
    view = memoryview(data)
    done = 0
    while done < len(view):
        count = fs.pwrite(fd, view[done:], offset + done)
        if count <= 0:
            raise OSError(errno.EIO, "write made no progress")
        done += count


class _Table:
    """The live table behind one descriptor whose ``flock`` the caller holds."""

    def __init__(self, fs: FileSystem, fd: int) -> None:
        self._fs: Final = fs
        self._fd: Final = fd

    def problem(self, boot: bytes) -> str | None:
        """Why the table cannot be used as it is: ``""`` for a new (empty) file, a phrase for a
        foreign, damaged or stale one, ``None`` when it is good."""
        size = self._fs.fstat(self._fd).st_size
        if size == 0:
            return ""
        if size != LIVE_BYTES:
            return f"is {size} bytes, not {LIVE_BYTES}"
        raw = self._fs.pread(self._fd, HEAD_BYTES, 0)
        if len(raw) != HEAD_BYTES:
            return "is not a sequence table"
        body = raw[: _HEAD_BODY.size]
        magic, version, slots, slot_size, flags, written, host = _HEAD_BODY.unpack(body)
        if _CRC.unpack_from(raw, _HEAD_BODY.size)[0] != zlib.crc32(body):
            return "is not a sequence table"
        if magic != LIVE_MAGIC or flags != 0:
            return "is not a sequence table"
        if (version, slots, slot_size) != (LIVE_VERSION, LIVE_SLOTS, SLOT_BYTES):
            return "is a sequence table of another version"
        if written != boot:
            if host.rstrip(b"\0") != _host():
                return "was written on another host (keep one state directory per host)"
            return "is from an earlier boot of this machine"
        return None

    def read(self, index: int) -> _Slot:
        return _unpack_slot(self._fs.pread(self._fd, SLOT_BYTES, _offset(index)))

    def write(self, index: int, key: str, next_: int, ceiling: int) -> None:
        assert ceiling < MAX_COUNT, "no slot is written above the bound the reader accepts"
        _pwrite_all(self._fs, self._fd, _pack_slot(key, next_, ceiling), _offset(index))

    def find(self, key: str) -> tuple[int, _Slot]:
        """The slot of ``key``, or the empty slot where it would go (``(-1, _EMPTY)`` when the
        table is full). Raises :class:`_Garbage` at a damaged slot on the way."""
        home = _home(key)
        for step in range(LIVE_SLOTS):
            index = (home + step) & (LIVE_SLOTS - 1)
            slot = self.read(index)
            if not slot.key or slot.key == key:
                return index, slot
        return -1, _EMPTY

    def entries(self) -> dict[str, tuple[int, _Slot]]:
        """Every stream in the table. Raises :class:`_Garbage` when any slot is damaged."""
        raw = self._fs.pread(self._fd, LIVE_BYTES - HEAD_BYTES, HEAD_BYTES)
        if len(raw) != LIVE_BYTES - HEAD_BYTES:
            raise _Garbage
        out: dict[str, tuple[int, _Slot]] = {}
        for index in range(LIVE_SLOTS):
            start = index * SLOT_BYTES
            slot = _unpack_slot(raw[start : start + SLOT_BYTES])
            if slot.key:
                if slot.key in out:
                    raise _Garbage  # one stream, two slots
                out[slot.key] = (index, slot)
        return out

    def ceilings(self) -> dict[str, int]:
        """The ceiling of every stream with a valid slot, damaged slots skipped."""
        raw = self._fs.pread(self._fd, LIVE_BYTES - HEAD_BYTES, HEAD_BYTES)
        return _scan_slots(raw)

    def wipe(self, boot: bytes) -> None:
        """Every slot empty, the header this boot's."""
        self._fs.ftruncate(self._fd, 0)
        self._fs.ftruncate(self._fd, LIVE_BYTES)
        _pwrite_all(self._fs, self._fd, _pack_head(boot), 0)


# -- worker-thread transactions ------------------------------------------------------------


class _LockTimeout(Exception):
    def __init__(self, name: str) -> None:
        super().__init__(f"{name} stayed locked")
        self.name = name


class _CallTimeout(Exception):
    """A worker did not answer within the bound (a hung disk)."""


class _Unusable(Exception):
    """The state directory cannot hold the counts at all (decided when the store opens).
    ``salvaged`` is what the live table held before a rebuild that then failed."""

    def __init__(
        self, why: str, cause: BaseException, salvaged: Mapping[str, int] | None = None
    ) -> None:
        super().__init__(why)
        self.why = why
        self.cause = cause
        self.salvaged: Mapping[str, int] = salvaged or {}


class _Full(Exception):
    def __init__(self, key: str) -> None:
        super().__init__(key)
        self.key = key


class _Exhausted(Exception):
    """A stream's count has reached :data:`MAX_COUNT`."""

    def __init__(self, key: str) -> None:
        super().__init__(key)
        self.key = key


@dataclass(frozen=True, slots=True)
class _Config:
    directory: Path
    fs: FileSystem
    block: int
    boot_id: Callable[[], bytes]


@dataclass(slots=True)
class _Durable:
    marks: dict[str, int]
    invalid: str | None = None  # why the file on disk was refused
    details: str = ""
    missing: bool = False  # there is no file
    floor: dict[str, int] = field(default_factory=dict)  # what a refused file still said


@dataclass(frozen=True, slots=True)
class _Main:
    """The descriptors a take on the loop uses: the directory and the live table."""

    dir_fd: int
    fd: int
    ident: tuple[int, int]  # (st_dev, st_ino) of the live file


@dataclass(slots=True)
class _Plan:
    index: int
    before: _Slot
    durable: int  # the stream's durable ceiling before the transaction
    ceiling: int  # the new ceiling, durable before any slot holds it
    seat_next: int | None = None


@dataclass(slots=True)
class _Outcome:
    result: Any = None
    failure: BaseException | None = None
    events: list[tuple[str, dict[str, Any]]] = field(default_factory=list)
    warnings: list[IronbirdError] = field(default_factory=list)
    delivered: bool = False  # events and warnings have reached the ledger and the user


def _check_ours(fs: FileSystem, fd: int, name: str) -> os.stat_result:
    st = fs.fstat(fd)
    if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() or st.st_nlink != 1:
        raise OSError(errno.EPERM, f"{name} is not a plain file of this user")
    return st


class _Files:
    """The state files as one worker-thread transaction sees them. It owns every descriptor it
    opens (except the ones :meth:`open_main` hands to the loop) and gives its records and
    warnings back with the outcome, for the loop to deliver."""

    def __init__(self, cfg: _Config, deadline: float, out: _Outcome) -> None:
        self._cfg: Final = cfg
        self._deadline: Final = deadline
        self._out: Final = out
        self._durable_held = False
        self._boot: bytes | None = None
        self._salvaged: dict[str, int] = {}  # what the tables this transaction rebuilt held
        self._fresh = False  # the table in hand was just emptied: nothing in it to read

    @property
    def boot(self) -> bytes:
        if self._boot is None:
            self._boot = self._cfg.boot_id()
        return self._boot

    def _event(self, event: str, **fields: Any) -> None:
        self._out.events.append((event, fields))

    def _flock(self, fd: int, operation: int, name: str) -> None:
        started = time.monotonic()
        step = 0.0
        while True:
            try:
                self._cfg.fs.flock(fd, operation | fcntl.LOCK_NB)
                return
            except OSError as exc:
                if exc.errno not in _BUSY:
                    raise
            now = time.monotonic()
            if now >= self._deadline:
                raise _LockTimeout(name)
            step = _backoff(step, now - started)
            time.sleep(min(step, self._deadline - now))

    def _present(self) -> bool:
        try:
            fd = self._cfg.fs.open_dir(self._cfg.directory)
        except FileNotFoundError:
            return False
        close_quietly(self._cfg.fs, fd)
        return True

    @contextmanager
    def _durable_lock(self) -> Generator[int]:
        """The durable lock, held for the block; yields the directory descriptor."""
        fs = self._cfg.fs
        fs.mkdir_p(self._cfg.directory)
        dir_fd = fs.open_dir(self._cfg.directory)
        try:
            lock_fd = fs.open_at(dir_fd, LOCK_NAME, _OPEN_FLAGS, 0o600)
            try:
                _check_ours(fs, lock_fd, LOCK_NAME)
                self._flock(lock_fd, fcntl.LOCK_EX, LOCK_NAME)
                self._durable_held = True
                try:
                    yield dir_fd
                finally:
                    self._durable_held = False
            finally:
                close_quietly(fs, lock_fd)  # closing the descriptor releases the lock
        finally:
            close_quietly(fs, dir_fd)

    def _open_live(self, dir_fd: int) -> tuple[int, os.stat_result]:
        fs = self._cfg.fs
        fd = fs.open_at(dir_fd, LIVE_NAME, _OPEN_FLAGS, 0o600)
        try:
            return fd, _check_ours(fs, fd, LIVE_NAME)
        except BaseException:
            close_quietly(fs, fd)
            raise

    @contextmanager
    def _table(self, dir_fd: int) -> Generator[_Table]:
        """The live table under its lock, rebuilt first when it cannot be used as it is."""
        assert self._durable_held, "the table's layout changes only under the durable lock"
        fs = self._cfg.fs
        fd, _ = self._open_live(dir_fd)
        try:
            self._flock(fd, fcntl.LOCK_EX, LIVE_NAME)
            table = _Table(fs, fd)
            self._fresh = False
            why = table.problem(self.boot)
            if why is not None:
                self._rebuild(table, why)
            yield table
        finally:
            close_quietly(fs, fd)

    def _rebuild(self, table: _Table, why: str) -> None:
        if why:  # whatever slots are valid still say what was handed out: a floor for the rebuild
            with contextlib.suppress(OSError):
                _merged(self._salvaged, table.ceilings())
        table.wipe(self.boot)
        self._fresh = True
        path = self._cfg.directory / LIVE_NAME
        if not why:
            self._event("sequence.live_created", path=str(path))
            return
        self._event("sequence.rebuilt", path=str(path), why=why)
        self._out.warnings.append(
            IronbirdError(
                Code.SEQ_LIVE,
                f"{path} {why}; rebuilt, so every stream carries on from its stored ceiling (up "
                f"to {self._cfg.block} counts skipped per stream)",
            )
        )

    def _read_durable(self, dir_fd: int, *, inspect: bool = False) -> _Durable:
        fs = self._cfg.fs
        try:
            fd = fs.open_at(dir_fd, STATE_NAME, INSPECT_FLAGS)
        except FileNotFoundError:
            return _Durable({}, missing=True)
        except OSError as exc:
            if exc.errno == errno.ELOOP:
                return self._invalid("is a symbolic link", "", inspect)
            raise
        try:
            st = fs.fstat(fd)
            if not stat.S_ISREG(st.st_mode):
                return self._invalid("is not a plain file of this user", "", inspect)
            if st.st_size > MAX_BYTES:
                return self._invalid(f"is over {MAX_BYTES} bytes", f"{st.st_size} bytes", inspect)
            raw = fs.pread(fd, MAX_BYTES + 1, 0)
        finally:
            close_quietly(fs, fd)
        marks = parse_state(raw)
        if st.st_uid != os.getuid() or st.st_nlink != 1:
            refused = self._invalid("is not a plain file of this user", "", inspect)
            refused.floor = marks or {}  # a hard link or a file of another user can only add
            return refused
        if marks is None:
            digest = hashlib.sha256(raw).hexdigest()[:16]
            return self._invalid(
                "is not valid sequence state", f"{len(raw)} bytes, sha256 {digest}...", inspect
            )
        return _Durable(marks)

    def _invalid(self, why: str, details: str, inspect: bool) -> _Durable:
        if inspect:
            path = self._cfg.directory / STATE_NAME
            self._out.warnings.append(
                IronbirdError(
                    Code.SEQ_RESET,
                    f"{path} {why}; left as it is (the next send moves it aside to "
                    f"{INVALID_NAME}); the counts shown come from the live table",
                    details=details,
                )
            )
        return _Durable({}, why, details)

    def _write_durable(self, dir_fd: int, durable: _Durable, marks: Mapping[str, int]) -> None:
        """Replace the durable file (synced). An invalid one is first moved aside, as evidence."""
        fs = self._cfg.fs
        path = self._cfg.directory / STATE_NAME
        if durable.invalid is not None:
            with contextlib.suppress(FileNotFoundError):  # gone meanwhile: nothing to keep
                fs.rename_at(dir_fd, STATE_NAME, INVALID_NAME)
            self._event(
                "sequence.reset",
                path=str(path),
                why=durable.invalid,
                details=durable.details,
                aside=str(self._cfg.directory / INVALID_NAME),
                kept=len(marks),
            )
            self._out.warnings.append(
                IronbirdError(
                    Code.SEQ_RESET,
                    f"{path} {durable.invalid}; moved aside to {INVALID_NAME} and rebuilt from "
                    "the live table (a stream not sent on since boot restarts at 0)",
                    details=durable.details,
                )
            )
            durable.invalid = None
        with TempOutput(path, force=True, fs=fs) as out:
            out.write(render_state(marks))
            out.commit()

    def _recover(self, durable: _Durable, table: _Table) -> dict[str, int]:
        """The durable ceilings, with what else is known to have been handed out merged in: the
        values of a refused file, the slots of a table this transaction rebuilt, and, when the
        file was refused or is gone, the slots of the live table in hand."""
        marks = dict(durable.marks)
        _merged(marks, durable.floor)
        _merged(marks, self._salvaged)
        if (durable.invalid is not None or durable.missing) and not self._fresh:
            _merged(marks, table.ceilings())
        return _capped(marks)

    def _locate(self, table: _Table, key: str, marks: Mapping[str, int]) -> tuple[int, _Slot]:
        """The slot of ``key``, inserted at its durable ceiling when new (next equal to the
        ceiling, so a take waits for this transaction to raise it). A damaged slot rebuilds the
        table; a full table drops the streams that hold nothing (reset ones) and tries again."""
        for _ in range(3):
            try:
                index, slot = table.find(key)
            except _Garbage:
                self._rebuild(table, "has a damaged slot")
                continue
            if slot.key:
                return index, slot
            if index < 0:
                self._compact(table)
                continue
            start = marks.get(key, 0)
            table.write(index, key, start, start)
            return index, _Slot(key, start, start)
        raise _Full(key)

    def _compact(self, table: _Table) -> None:
        try:
            kept = [(k, s) for k, (_, s) in table.entries().items() if s.ceiling]
        except _Garbage:
            kept = []
        table.wipe(self.boot)
        for key, slot in kept:
            index, empty = table.find(key)
            assert index >= 0 and not empty.key  # fewer streams than slots
            table.write(index, key, slot.next, slot.ceiling)
        self._event("sequence.compacted", kept=len(kept))

    # -- the transactions --------------------------------------------------------------

    def open_main(self) -> _Main:
        """The descriptors for takes on the loop, with the live table checked (rebuilt under
        the durable lock when needed, and the durable file brought up to what the table and the
        file's own leftovers show). Raises :class:`_Unusable` for a directory that cannot hold
        the counts at all."""
        fs = self._cfg.fs
        try:
            _ = self.boot  # no boot id, no way to tell a stale table from a live one
            fs.mkdir_p(self._cfg.directory)
            dir_fd = fs.open_dir(self._cfg.directory)
        except OSError as exc:
            raise _unusable(exc, self._salvaged) from exc
        try:
            fd, st = self._open_live(dir_fd)
            try:
                lock_fd = fs.open_at(dir_fd, LOCK_NAME, _OPEN_FLAGS, 0o600)
                try:
                    _check_ours(fs, lock_fd, LOCK_NAME)
                finally:
                    close_quietly(fs, lock_fd)
                self._flock(fd, fcntl.LOCK_SH, LIVE_NAME)
                try:
                    why = _Table(fs, fd).problem(self.boot)
                finally:
                    fs.flock(fd, fcntl.LOCK_UN)
                if why is not None or not self._has_durable(dir_fd):
                    self.extend([])  # rebuilds the table and settles the file, under both locks
                return _Main(dir_fd, fd, (st.st_dev, st.st_ino))
            except BaseException:
                close_quietly(fs, fd)
                raise
        except OSError as exc:
            close_quietly(fs, dir_fd)
            raise _unusable(exc, self._salvaged) from exc
        except BaseException:
            close_quietly(fs, dir_fd)
            raise

    def _has_durable(self, dir_fd: int) -> bool:
        try:
            self._cfg.fs.stat_at(dir_fd, STATE_NAME)
        except FileNotFoundError:
            return False
        except OSError:
            return True  # something is there; the reader says what
        return True

    def floors(self) -> tuple[dict[str, int], str]:
        """Every ceiling the state directory shows, read only and whatever else is wrong with
        it: the durable file (a refused one that parses too) and the slots of the live table (of
        any boot, damaged ones skipped). For a process that must count in memory: it starts above
        all of them. The second value says what could not be read."""
        fs = self._cfg.fs
        out: dict[str, int] = {}
        problems: list[str] = []
        try:
            dir_fd = fs.open_dir(self._cfg.directory)
        except OSError as exc:
            return out, f"{self._cfg.directory}: {exc.strerror or exc}"
        try:
            try:
                durable = self._read_durable(dir_fd)
                _merged(out, durable.marks)
                _merged(out, durable.floor)
            except OSError as exc:
                problems.append(f"{STATE_NAME}: {exc.strerror or exc}")
            try:
                fd = fs.open_at(dir_fd, LIVE_NAME, INSPECT_FLAGS)
            except FileNotFoundError:
                pass
            except OSError as exc:
                problems.append(f"{LIVE_NAME}: {exc.strerror or exc}")
            else:
                try:
                    if stat.S_ISREG(fs.fstat(fd).st_mode):
                        _merged(out, _scan_slots(fs.pread(fd, LIVE_BYTES - HEAD_BYTES, HEAD_BYTES)))
                except OSError as exc:
                    problems.append(f"{LIVE_NAME}: {exc.strerror or exc}")
                finally:
                    close_quietly(fs, fd)
        finally:
            close_quietly(fs, dir_fd)
        return _capped(out), "; ".join(problems)

    def extend(
        self,
        keys: Sequence[str],
        *,
        need: str | None = None,
        seat: tuple[str, int] | None = None,
        ahead: bool = False,
    ) -> dict[str, int]:
        """Raise the ceilings of ``keys``: of ``need`` when it has no count left, of the others
        when ``ahead`` and half a step or less is left; ``seat`` ``(key, n)`` makes the stream
        hand out ``n + 1`` next. The durable file is replaced (synced) before any slot holds a
        new ceiling. Returns the slot index of every key."""
        block = self._cfg.block
        top = MAX_COUNT - 1  # the highest ceiling that is stored
        indices: dict[str, int] = {}
        with self._durable_lock() as dir_fd:
            durable = self._read_durable(dir_fd)
            plans: dict[str, _Plan] = {}
            with self._table(dir_fd) as table:
                marks = self._recover(durable, table)
                scanned = durable.invalid is not None or durable.missing or self._fresh
                for key in keys:
                    if key not in marks and len(marks) >= MAX_STREAMS:
                        raise _Full(key)
                    index, slot = self._locate(table, key, marks)
                    indices[key] = index
                    seated = seat is not None and seat[0] == key
                    if not scanned and not seated and slot.ceiling > marks.get(key, 0):
                        # a slot above its durable ceiling: the file is older than the table
                        # (restored, replaced), so every stream may be short in it
                        _merged(marks, table.ceilings())
                        scanned = True
                    old = marks.get(key, 0)
                    if seat is not None and seated:
                        first = seat[1] + 1
                        plans[key] = _Plan(index, slot, old, min(first + block, top), first)
                        continue
                    left = slot.ceiling - slot.next
                    if (key == need and left > 0) or (ahead and left > AHEAD_AT):
                        continue  # raised by another process meanwhile
                    start = slot.next if slot.ceiling == old else max(slot.next, old)
                    new = min(max(old, start + block), top)
                    if new <= start:  # no count left below the bound
                        if key == need:
                            raise _Exhausted(key)
                        continue
                    plans[key] = _Plan(index, slot, old, new)
                repaired = marks != durable.marks
            if not plans and durable.invalid is None and not repaired:
                return indices
            for key, plan in plans.items():
                marks[key] = plan.ceiling
            self._write_durable(dir_fd, durable, marks)
            with self._table(dir_fd) as table:
                for key, plan in plans.items():
                    index, now = self._locate(table, key, marks)
                    indices[key] = index
                    if plan.seat_next is not None:
                        first = plan.seat_next
                    elif now.ceiling == plan.durable == plan.before.ceiling:
                        first = now.next  # contiguous: this table holds the newest step
                    else:
                        first = max(now.next, plan.durable)  # another table took up to it
                    table.write(index, key, first, max(plan.ceiling, first))
            self._event(
                "sequence.extend",
                why="seat" if seat else "ahead" if ahead else "need",
                streams=[
                    {"key": k, "from": p.durable, "to": p.ceiling, "next": p.before.next}
                    for k, p in plans.items()
                ],
            )
        return indices

    def forget(self, stem: str | None, mid: int | None) -> list[Row]:
        """Drop the stored counts of the chosen streams (everything when both are ``None``, a
        key of another writer included), so the next take of each starts at 0, in every process
        at once. Returns what was dropped, with the count each would have handed out next."""
        if not self._present():
            return []
        everything = stem is None and mid is None
        with self._durable_lock() as dir_fd:
            durable = self._read_durable(dir_fd)
            with self._table(dir_fd) as table:
                marks = self._recover(durable, table)
                try:
                    live = table.entries()
                except _Garbage:
                    self._rebuild(table, "has a damaged slot")
                    live = {}
            gone: list[Row] = []
            chosen: list[str] = []
            for key in sorted(set(marks) | {k for k, (_, s) in live.items() if s.ceiling}):
                parsed = parse_key(key)
                assert parsed is not None  # both files hold only keys of that spelling
                if not (stem is None or parsed[0] == stem) or not (mid is None or parsed[2] == mid):
                    continue
                chosen.append(key)
                nxt = live[key][1].next if key in live else marks.get(key, 0)
                gone.append((*parsed, nxt))
            for key in chosen:
                marks.pop(key, None)
            if chosen or durable.invalid is not None:
                self._write_durable(dir_fd, durable, marks)
            with self._table(dir_fd) as table:
                if everything:
                    table.wipe(self.boot)
                else:
                    for key in chosen:
                        try:
                            index, slot = table.find(key)
                        except _Garbage:
                            self._rebuild(table, "has a damaged slot")
                            break
                        if slot.key == key:
                            table.write(index, key, 0, 0)
            self._event("sequence.forget", streams=chosen)
        return gone

    def snapshot(self) -> list[Stored]:
        """Every stored stream, read only: no file is created, replaced or written."""
        fs = self._cfg.fs
        try:
            dir_fd = fs.open_dir(self._cfg.directory)
        except FileNotFoundError:
            return []
        live: dict[str, tuple[int, _Slot]] = {}
        try:
            durable = self._read_durable(dir_fd, inspect=True)
            try:
                fd = fs.open_at(dir_fd, LIVE_NAME, INSPECT_FLAGS)
            except FileNotFoundError:
                fd = -1
            if fd >= 0:
                try:
                    _check_ours(fs, fd, LIVE_NAME)
                    self._flock(fd, fcntl.LOCK_SH, LIVE_NAME)
                    table = _Table(fs, fd)
                    if table.problem(self.boot) is None:
                        try:
                            live = table.entries()
                        except _Garbage:
                            live = {}
                finally:
                    close_quietly(fs, fd)
        finally:
            close_quietly(fs, dir_fd)
        rows: list[Stored] = []
        for key in sorted(set(durable.marks) | {k for k, (_, s) in live.items() if s.ceiling}):
            parsed = parse_key(key)
            assert parsed is not None  # both files hold only keys of that spelling
            ceiling = durable.marks.get(key, 0)
            nxt = live[key][1].next if key in live else ceiling
            rows.append((*parsed, nxt, ceiling))
        return rows


def _unusable(exc: OSError, salvaged: Mapping[str, int]) -> BaseException:
    if exc.errno in _UNUSABLE:
        return _Unusable(exc.strerror or type(exc).__name__, exc, salvaged)
    return exc


def _extend_ahead(keys: Sequence[str], files: _Files) -> dict[str, int]:
    return files.extend(keys, ahead=True)


def _run(
    cfg: _Config,
    deadline: float,
    work: Callable[[_Files], Any],
    out: _Outcome,
    loop: asyncio.AbstractEventLoop,
    deliver: Callable[[_Outcome], None],
) -> _Outcome:
    try:
        out.result = work(_Files(cfg, deadline, out))
    except BaseException as exc:  # handed to the loop, with the records made on the way
        out.failure = exc
    with contextlib.suppress(RuntimeError):  # the loop may be closed: nobody left to tell
        loop.call_soon_threadsafe(deliver, out)  # also when the caller has stopped waiting
    return out


# -- the store -----------------------------------------------------------------------------


class _Waiter:
    """What a helper thread polling for the live lock and the task that started it agree on. The
    thread tries the lock only while the wait is on, and the task, once it gives the wait up,
    knows whether the lock came in before that. A ``flock`` on a descriptor that already holds
    the lock succeeds, so a thread that tried again after the task had gone on (and taken the
    lock itself for the next take) would think it had the lock, and release it."""

    def __init__(self) -> None:
        self._mutex = threading.Lock()
        self._state: Literal["waiting", "granted", "abandoned"] = "waiting"

    @property
    def abandoned(self) -> bool:
        return self._state == "abandoned"

    def attempt(self, fs: FileSystem, fd: int) -> Literal["granted", "busy", "abandoned"]:
        """One non-blocking try of the exclusive lock of ``fd``, while the wait is on."""
        with self._mutex:
            if self._state == "abandoned":
                return "abandoned"
            try:
                fs.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                if exc.errno not in _BUSY:
                    raise
                return "busy"
            self._state = "granted"
            return "granted"

    def abandon(self) -> bool:
        """Give the wait up. ``False`` when the lock had been granted already (the caller then
        holds it and must release it)."""
        with self._mutex:
            if self._state == "granted":
                return False
            self._state = "abandoned"
            return True


def _poll_lock(fs: FileSystem, fd: int, deadline: float, waiter: _Waiter) -> bool:
    """Take the exclusive lock of ``fd`` by polling, in a helper thread, until ``deadline`` or
    until ``waiter`` is abandoned. ``True`` once the lock is granted."""
    started = time.monotonic()
    while True:
        match waiter.attempt(fs, fd):
            case "granted":
                return True
            case "abandoned":
                return False
            case _:
                pass
        now = time.monotonic()
        if now >= deadline:
            return False
        time.sleep(min(_poll_step(now - started), deadline - now))


class Lease:
    """One count of one stream, held from the take to the send.

    :attr:`count` is the count to put in the packet (not reduced to a field width). The take's
    lock is held until :meth:`release`, or with ``span_send`` off until :meth:`commit`, so a
    sender that hands its packet to the kernel before releasing hands packets to the kernel in
    take order (what a network does to them after that is not ours). :meth:`commit` spends the
    count; a lease released without it hands out nothing."""

    __slots__ = (
        "_ceiling",
        "_committed",
        "_index",
        "_key",
        "_released",
        "_span",
        "_store",
        "count",
    )

    def __init__(
        self,
        store: SequenceStore,
        key: str,
        count: int,
        *,
        index: int | None,
        ceiling: int,
        span_send: bool,
    ) -> None:
        self._store: Final = store
        self._key: Final = key
        self.count: Final = count
        self._index: Final = index  # the slot to write at commit; None: nothing to write
        self._ceiling: Final = ceiling
        self._span: Final = span_send
        self._committed = False
        self._released = False

    def commit(self) -> None:
        """The count is spent from here: the stream's next take gives ``count + 1``. Raises
        ``IB-SEQ-006`` when the count cannot be recorded; send nothing then."""
        assert not self._committed and not self._released
        self._committed = True
        self._store._commit(self._key, self.count, self._index, self._ceiling)
        if not self._span:
            self._store._unlock_main()

    @property
    def holds_lock(self) -> bool:
        """Whether this lease holds the cross-process lock right now (UDP: from the take to the
        hand-off; serial: not after the commit). Nothing that can wait on the network, a name
        resolution included, belongs under it."""
        return not self._released and self._store._main_locked

    def release(self) -> None:
        """Let the next take go ahead. Safe to call more than once."""
        if not self._released:
            self._released = True
            self._store._end_lease()


class _UnusableError(Exception):
    def __init__(self, why: str, cause: BaseException, salvaged: Mapping[str, int]) -> None:
        super().__init__(why)
        self.why = why
        self.cause = cause
        self.salvaged = salvaged


class SequenceStore:
    """The counts of one process, taken on its event loop. With ``directory`` ``None`` (no
    state directory) it counts in memory and says so once, giving ``unusable`` as the reason."""

    def __init__(
        self,
        directory: Path | None,
        *,
        report: Callable[[IronbirdError], None],
        record: Callable[[str, dict[str, Any]], object] = lambda event, fields: None,
        fs: FileSystem = REAL_FS,
        block: int = BLOCK,
        lock_wait_s: float = LOCK_WAIT_S,
        boot_id: Callable[[], bytes] = read_boot_id,
        unusable: str = "no state directory",
    ) -> None:
        assert block >= 2 and lock_wait_s > 0
        self._directory: Final = directory
        self._report: Final = report
        self._record: Final = record
        self._fs: Final = fs
        self._wait: Final = lock_wait_s
        self._cfg: Final = _Config(directory, fs, block, boot_id) if directory is not None else None
        self._offloop: Final[OffLoop[_Outcome]] = OffLoop("sequence-state", 4)
        self._lockers: Final[OffLoop[bool]] = OffLoop("sequence-lock", 2)
        self._turn: Final = asyncio.Lock()
        self._main: _Main | None = None
        self._main_locked = False
        self._index: dict[str, int] = {}  # the slot of each stream, checked at every take
        self._memory: dict[str, int] | None = None
        self._memory_why: Final = unusable
        self._opened = False  # open once: from then on no falling back to memory
        self._ahead = False
        self._pending: set[str] = set()  # streams to raise ahead (bounded by the streams used)
        self._raising: frozenset[str] = frozenset()  # what the running raise ahead covers
        self._prefetch: asyncio.Task[None] | None = None

    @property
    def persistent(self) -> bool:
        """Whether counts come from the state directory (``False`` after ``IB-SEQ-002``)."""
        return self._memory is None and self._cfg is not None

    def for_node(self, node: str) -> NodeSequences:
        return NodeSequences(self, node)

    def keep_ahead(self) -> None:
        """Raise ceilings in the background before they are reached (a long-lived sender)."""
        self._ahead = True

    # -- taking a count ------------------------------------------------------------------

    async def lease(self, key: str, *, seat: int | None = None, span_send: bool = True) -> Lease:
        """The next count of ``key``, or ``seat`` when given (the stream then goes on from
        ``seat + 1`` in every process). Raises ``IB-SEQ-005`` or ``IB-SEQ-006``, handing out
        nothing. Release the lease once the packet is handed off or given up."""
        if seat is not None and not 0 <= seat < MAX_COUNT - 1:
            raise IronbirdError(
                Code.SEQ_TAKE,
                f"no sequence count for {stream_text(key)}: {seat} is beyond the highest stored "
                f"count ({MAX_COUNT - 2}); nothing sent",
            )
        deadline = time.monotonic() + self._wait
        await self._enter(key)
        try:
            return await self._take(key, seat, span_send, deadline)
        except BaseException:
            self._end_lease()
            raise

    def take_now(self, key: str) -> int | None:
        """The next count of ``key``, spent, taken without waiting: a non-blocking ``flock``, one
        ``fstatat`` and a ``pread`` and ``pwrite`` of one slot (or the in-memory count). ``None``
        when it would have to wait: the files are not open yet, a send of this process holds its
        turn, another process holds the live lock, the slot has no count left under its ceiling
        or was not found nearby, or the live file was replaced. Call :meth:`take` then. Raises
        ``IB-SEQ-006`` when a file operation fails. Only on the event loop."""
        if self._memory is not None:
            count = self._memory.get(key, 0)
            self._memory[key] = count + 1
            return count
        main = self._main
        if main is None or self._turn.locked():
            return None
        assert not self._main_locked
        try:
            self._fs.flock(main.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in _BUSY:
                return None
            raise self._failed(key, exc) from exc
        self._main_locked = True
        try:
            taken = self._try_take(key)
            if isinstance(taken, str):
                return None
            index, count, ceiling = taken
            self._commit(key, count, index, ceiling)
            return count
        finally:
            self._unlock_main()

    async def take(self, key: str, *, seat: int | None = None) -> int:
        """The next count of ``key`` (``seat`` when given: the stream goes on from ``seat + 1``),
        spent: :meth:`lease` and :meth:`Lease.commit` with nothing held after. Raises what they
        raise."""
        lease = await self.lease(key, seat=seat, span_send=False)
        try:
            lease.commit()
        finally:
            lease.release()
        return lease.count

    async def _enter(self, key: str) -> None:
        """This process's turn to take (one send at a time holds the lock of the live table)."""
        if self._turn.locked():  # another send of this process holds it: wait, bounded
            try:
                async with asyncio.timeout(self._wait):
                    await self._turn.acquire()
            except TimeoutError:
                raise self._busy(key, "the previous send of this process did not finish") from None
        else:
            await self._turn.acquire()  # free: taken without suspending

    async def preraise(self, keys: Sequence[str]) -> None:
        """Open the files and raise the ceiling of every stream of ``keys`` that has no count to
        spare, all in one sync, so that the first take of each does not wait for one (a
        responder's first reply of each message). Raises ``IB-SEQ-005`` or ``IB-SEQ-006``."""
        ordered = sorted(set(keys))
        if not ordered or self._cfg is None:
            return
        await self._enter(ordered[0])
        try:
            deadline = time.monotonic() + self._wait
            if await self._ready(ordered[0], deadline):
                self._remember(
                    await self._work(ordered[0], deadline, partial(_extend_ahead, ordered))
                )
        finally:
            self._end_lease()

    async def _take(self, key: str, seat: int | None, span_send: bool, deadline: float) -> Lease:
        if not await self._ready(key, deadline):
            assert self._memory is not None
            count = self._memory.get(key, 0) if seat is None else seat
            self._memory[key] = count  # commit moves it on
            return Lease(self, key, count, index=None, ceiling=0, span_send=span_send)
        if seat is not None:
            indices = await self._work(key, deadline, lambda f: f.extend([key], seat=(key, seat)))
            self._remember(indices)
            await self._lock_main(key, deadline)
            return Lease(self, key, seat, index=None, ceiling=0, span_send=span_send)
        while True:
            await self._lock_main(key, deadline)
            taken = self._try_take(key)
            if not isinstance(taken, str):
                index, count, ceiling = taken
                return Lease(self, key, count, index=index, ceiling=ceiling, span_send=span_send)
            self._unlock_main()
            if taken == "reopen":
                await self._reopen(key, deadline)
                continue
            task = self._prefetch
            if key in self._raising and task is not None and not task.done():
                await asyncio.wait({task}, timeout=max(0.0, deadline - time.monotonic()))
                continue  # raised ahead meanwhile: no second worker contending for the lock
            self._remember(await self._work(key, deadline, lambda f: f.extend([key], need=key)))

    def _try_take(self, key: str) -> tuple[int, int, int] | Literal["slow", "reopen"]:
        """Under the live lock: ``(slot, count, ceiling)`` from the stream's slot; ``"slow"``
        when a worker must act first (a stream not found nearby, no count left, a damaged slot);
        ``"reopen"`` when the live file is no longer the one this process holds."""
        main, fs = self._main, self._fs
        assert main is not None and self._main_locked
        try:
            st = fs.stat_at(main.dir_fd, LIVE_NAME)
            if (st.st_dev, st.st_ino) != main.ident:
                return "reopen"
            slot = _EMPTY
            index = self._index.get(key)
            if index is not None:
                raw = fs.pread(main.fd, SLOT_BYTES, _offset(index))
                own = _own_slot(raw, _padded(key))
                if own is not None:
                    if own[0] >= own[1]:
                        return "slow"
                    return index, own[0], own[1]
                slot = _unpack_slot(raw)  # another stream's slot now, or a damaged one
                index = None
            if index is None:
                home = _home(key)
                for step in range(PROBE_ON_LOOP):
                    at = (home + step) & (LIVE_SLOTS - 1)
                    slot = _unpack_slot(fs.pread(main.fd, SLOT_BYTES, _offset(at)))
                    if not slot.key:
                        return "slow"
                    if slot.key == key:
                        index = at
                        break
                else:
                    return "slow"
                self._remember({key: index})
        except FileNotFoundError:
            return "reopen"
        except _Garbage:
            return "slow"
        except OSError as exc:
            raise self._failed(key, exc) from exc
        if slot.next >= slot.ceiling:
            return "slow"
        return index, slot.next, slot.ceiling

    def _remember(self, indices: Mapping[str, int]) -> None:
        if len(self._index) + len(indices) > LIVE_SLOTS:
            self._index.clear()  # bounded; a stale entry only costs a probe
        self._index.update(indices)

    def _commit(self, key: str, count: int, index: int | None, ceiling: int) -> None:
        if self._memory is not None:
            self._memory[key] = count + 1
            return
        if index is None:
            return  # seated: the worker wrote the slot
        main = self._main
        assert main is not None and self._main_locked and count < ceiling < MAX_COUNT
        try:
            data = _pack_own_slot(_padded(key), count + 1, ceiling)
            _pwrite_all(self._fs, main.fd, data, _offset(index))
        except OSError as exc:
            raise self._failed(key, exc) from exc
        if self._ahead and ceiling - (count + 1) <= AHEAD_AT:
            self._queue_ahead(key)

    def _unlock_main(self) -> None:
        main = self._main
        if self._main_locked and main is not None:
            self._main_locked = False
            with contextlib.suppress(OSError):  # only a closed descriptor fails; closing unlocked
                self._fs.flock(main.fd, fcntl.LOCK_UN)

    def _end_lease(self) -> None:
        self._unlock_main()
        if self._turn.locked():
            self._turn.release()

    async def _lock_main(self, key: str, deadline: float) -> None:
        """The live lock for a take. A free lock is taken on the loop; a contended one is polled
        by a helper thread (a sleep on the loop lasts a millisecond at least, and a process
        that takes in a tight loop gets the lock back before such a sleep ends)."""
        main = self._main
        assert main is not None and not self._main_locked
        try:
            self._fs.flock(main.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._main_locked = True
            return
        except OSError as exc:
            if exc.errno not in _BUSY:
                raise self._failed(key, exc) from exc
        waiter = _Waiter()
        try:
            got = await self._lockers.call(partial(_poll_lock, self._fs, main.fd, deadline, waiter))
        except BaseException as exc:  # cancelled, or the helper failed
            if not waiter.abandon():  # the lock came in as the wait ended: give it back
                with contextlib.suppress(OSError):
                    self._fs.flock(main.fd, fcntl.LOCK_UN)
            if isinstance(exc, OSError | OffLoopBusy | RuntimeError):
                raise self._failed(key, exc) from exc
            raise
        if not got:
            raise self._busy(key, f"{LIVE_NAME} stayed locked {self._holder()}")
        self._main_locked = True

    # -- opening, workers ------------------------------------------------------------------

    async def _ready(self, key: str, deadline: float) -> bool:
        """``True`` once the files are open for takes, ``False`` when counting in memory."""
        if self._memory is not None:
            return False
        if self._main is not None:
            return True
        if self._cfg is None:
            self._record_memory(self._memory_why, None, {}, "")
            return False
        try:
            self._main = await self._work(key, deadline, lambda f: f.open_main())
        except _UnusableError as exc:
            if self._opened:  # counts were handed out from the files: memory could repeat them
                raise self._failed(key, exc.cause) from exc.cause
            await self._to_memory(exc.why, exc.cause, exc.salvaged)
            return False
        self._opened = True
        return True

    async def _reopen(self, key: str, deadline: float) -> None:
        old = self._main
        assert old is not None and not self._main_locked
        self._main = None
        self._index.clear()
        close_quietly(self._fs, old.fd)
        close_quietly(self._fs, old.dir_fd)
        self._record("sequence.reopened", {"path": str(self._directory)})
        self._report(
            IronbirdError(
                Code.SEQ_LIVE,
                f"{self._directory}/{LIVE_NAME} was removed or replaced while in use; counts "
                "carry on from the stored ceilings",
            )
        )
        if not await self._ready(key, deadline):
            raise AssertionError("open once, never memory")  # pragma: no cover

    async def _to_memory(self, why: str, cause: BaseException, salvaged: Mapping[str, int]) -> None:
        """Count in memory, starting every stream above the stored count that can still be read
        in the directory (it may be unusable for writing and still be readable)."""
        floors: dict[str, int] = dict(salvaged)
        unread = ""
        try:
            read, unread = await self._call(time.monotonic() + self._wait, lambda f: f.floors())
            _merged(floors, read)
        except Exception as exc:  # nothing readable: the message says so
            unread = f"{type(exc).__name__}: {exc}"
        self._record_memory(why, cause, floors, unread)

    def _record_memory(
        self, why: str, cause: BaseException | None, floors: dict[str, int], unread: str
    ) -> None:
        self._memory = dict(floors)
        where = self._directory if self._directory is not None else "the state directory"
        self._record(
            "sequence.memory_only",
            {"directory": str(where), "reason": why, "from_stored": len(floors), "unread": unread},
        )
        start = (
            f"above the {len(floors)} stream(s) with a stored count it can read (its own counts "
            "are not kept, so every ib process that does this repeats them)"
            if floors
            else "so every ib process starts at 0"
        )
        details = f"{type(cause).__name__}: {cause}" if cause is not None else ""
        if unread:
            details += ("\n" if details else "") + f"could not read the stored counts: {unread}"
        self._report(
            IronbirdError(
                Code.SEQ_UNSAVED,
                f"cannot keep sequence counts in {where} ({why}); counting in memory, {start} "
                "(set IRONBIRD_STATE_DIR to a writable directory)",
                details=details,
                cause=cause,
            )
        )

    async def _call(self, deadline: float, work: Callable[[_Files], _T]) -> _T:
        """``work`` in a worker thread, waited for until ``deadline`` (and a moment more, so
        that a lock wait the worker gives up at the same time is reported as that). Its records
        and warnings are delivered, then its result is returned or its failure raised
        (:class:`_Unusable` as ``_UnusableError``). A worker still running when the wait ends is
        abandoned: it ends on its own, and its records are delivered when it does."""
        cfg = self._cfg
        assert cfg is not None
        out = _Outcome()
        loop = asyncio.get_running_loop()
        try:
            async with asyncio.timeout(max(deadline - time.monotonic(), 0.0) + CALL_GRACE_S):
                await self._offloop.call(
                    lambda: _run(cfg, deadline, work, out, loop, self._deliver)
                )
        except TimeoutError as exc:
            raise _CallTimeout from exc
        self._deliver(out)
        if out.failure is not None:
            if isinstance(out.failure, _Unusable):
                raise _UnusableError(out.failure.why, out.failure.cause, out.failure.salvaged)
            raise out.failure
        return out.result  # type: ignore[no-any-return]

    def _deliver(self, out: _Outcome) -> None:
        """Hand a worker's records to the ledger and its warnings to the user, once, whether or
        not the task that started it is still waiting."""
        if out.delivered:
            return
        out.delivered = True
        for event, fields in out.events:
            self._record(event, fields)
        for warning in out.warnings:
            self._report(warning)

    async def _work(self, key: str, deadline: float, work: Callable[[_Files], _T]) -> _T:
        """:meth:`_call` for a take: every failure becomes the take's coded error."""
        try:
            return await self._call(deadline, work)
        except _UnusableError:
            raise
        except _LockTimeout as exc:
            raise self._busy(key, f"{exc.name} stayed locked {self._holder()}") from exc
        except _CallTimeout as exc:
            raise self._busy(key, "the sequence state files did not answer") from exc
        except _Full as exc:
            self._record("sequence.full", {"key": exc.key, "streams": MAX_STREAMS})
            raise IronbirdError(
                Code.SEQ_FULL,
                f"no sequence count for {stream_text(exc.key)}: {MAX_STREAMS} streams already have "
                "stored counts; nothing sent (ib seq reset the streams no longer used)",
            ) from exc
        except _Exhausted as exc:
            self._record("sequence.exhausted", {"key": exc.key, "limit": MAX_COUNT})
            raise IronbirdError(
                Code.SEQ_TAKE,
                f"no sequence count for {stream_text(exc.key)}: the stored count is at its limit "
                f"({MAX_COUNT - 1}); nothing sent (ib seq reset this stream)",
            ) from exc
        except (OSError, OffLoopBusy, RuntimeError) as exc:
            raise self._failed(key, exc) from exc

    def _holder(self) -> str:
        """Who a lock that stayed locked was held by, as far as this process can tell: a worker
        of its own that has not returned (a hung disk) rather than another process."""
        if self._offloop.running:
            return "by a worker of this process that has not returned"
        return "by another process"

    def _busy(self, key: str, what: str) -> IronbirdError:
        hint = (
            "is the disk hung?"
            if self._offloop.running
            else "is another ib process stopped, or the disk hung?"
        )
        return IronbirdError(
            Code.SEQ_TAKE,
            f"no sequence count for {stream_text(key)}: {what} within {self._wait:g} s; nothing "
            f"sent ({hint})",
        )

    def _failed(self, key: str, exc: BaseException) -> IronbirdError:
        why = exc.strerror if isinstance(exc, OSError) and exc.strerror else str(exc)
        return IronbirdError(
            Code.SEQ_TAKE,
            f"no sequence count for {stream_text(key)} ({why or type(exc).__name__}); nothing "
            f"sent: check {self._directory} or set IRONBIRD_STATE_DIR",
            details=f"{type(exc).__name__}: {exc}",
            cause=exc,
        )

    # -- keeping ahead ---------------------------------------------------------------------

    def _queue_ahead(self, key: str) -> None:
        self._pending.add(key)
        if self._prefetch is None or self._prefetch.done():
            self._prefetch = asyncio.get_running_loop().create_task(
                self._run_ahead(), name="sequence-ahead"
            )

    async def _run_ahead(self) -> None:
        while self._pending:
            keys = sorted(self._pending)
            self._pending.clear()
            self._raising = frozenset(keys)
            deadline = time.monotonic() + self._wait
            try:
                self._remember(await self._call(deadline, partial(_extend_ahead, keys)))
            except Exception as exc:  # a take that then finds no count left reports it, coded
                self._record(
                    "sequence.ahead_failed",
                    {"keys": keys, "reason": f"{type(exc).__name__}: {exc}"},
                )
            finally:
                self._raising = frozenset()

    # -- inspection and reset (ib seq) -----------------------------------------------------

    async def stored(self) -> list[Stored]:
        """Every stored stream, sorted, read without changing any file. Raises ``IB-SEQ-003``."""
        return await self._ask(lambda f: f.snapshot())

    async def forget(self, node: str | None = None, mid: int | None = None) -> list[Row]:
        """Drop the stored counts of the streams of ``node`` (all nodes when ``None``) and MID
        ``mid`` (all MIDs when ``None``): the next take of each, in every process, starts at 0.
        Returns what was dropped. Raises ``IB-SEQ-003``."""
        stem = None if node is None else node_stem(node)
        return await self._ask(lambda f: f.forget(stem, mid))

    async def _ask(self, work: Callable[[_Files], _T]) -> _T:
        if self._cfg is None:
            raise IronbirdError(
                Code.SEQ_STATE,
                "no usable state directory to keep sequence counts in; set IRONBIRD_STATE_DIR "
                "to a writable directory",
            )
        try:
            return await self._call(time.monotonic() + self._wait, work)
        except (
            OSError,
            OffLoopBusy,
            RuntimeError,
            _LockTimeout,
            _CallTimeout,
            _UnusableError,
            _Full,
        ) as exc:
            cause: BaseException = exc.cause if isinstance(exc, _UnusableError) else exc
            why = exc.strerror if isinstance(exc, OSError) and exc.strerror else str(exc)
            if isinstance(exc, _LockTimeout):
                why = f"{exc.name} stayed locked {self._holder()}"
            elif isinstance(exc, _CallTimeout):
                why = "the sequence state files did not answer"
            raise IronbirdError(
                Code.SEQ_STATE,
                f"cannot use the sequence counts in {self._directory} ({why}); check the "
                "directory or set IRONBIRD_STATE_DIR",
                details=f"{type(cause).__name__}: {cause}",
                cause=cause,
            ) from exc

    async def aclose(self) -> None:
        """Let a background raise finish (bounded), then close the files. Nothing is given back:
        a stream's next take, in any process, goes on from where this process stopped."""
        task = self._prefetch
        if task is not None and not task.done():
            await asyncio.wait({task}, timeout=self._wait)
            if not task.done():
                task.cancel()  # its worker ends on its own; at most a step is skipped
        self._unlock_main()
        main, self._main = self._main, None
        if main is not None:
            close_quietly(self._fs, main.fd)
            close_quietly(self._fs, main.dir_fd)


class NodeSequences:
    """The store as one node's sender sees it."""

    def __init__(self, store: SequenceStore, node: str) -> None:
        self._store: Final = store
        self._node: Final = node

    @property
    def store(self) -> SequenceStore:
        return self._store

    async def lease(
        self, kind: str, mid: int, *, seat: int | None = None, span_send: bool = True
    ) -> Lease:
        key = key_of(self._node, kind, mid)
        return await self._store.lease(key, seat=seat, span_send=span_send)

    async def preraise(self, kind: str, mids: Iterable[int]) -> None:
        """:meth:`SequenceStore.preraise` for the streams of ``kind`` and ``mids``."""
        await self._store.preraise([key_of(self._node, kind, mid) for mid in mids])

    def series(
        self, kind: str, mid: int, *, seat: int | None, wake: Callable[[], None]
    ) -> SeriesCounts:
        """The counts of one series of ``kind`` packets with ``mid`` (see :class:`SeriesCounts`).
        A series is long-lived, so ceilings are raised in the background."""
        self._store.keep_ahead()
        return SeriesCounts(self._store, key_of(self._node, kind, mid), seat=seat, wake=wake)


class SeriesCounts:
    """The counts of the packets of one series, one per :meth:`next`, on the event loop.

    A count is taken without waiting when it can be (:meth:`SequenceStore.take_now`). When it
    cannot (the first packet of a series, a ceiling to raise, a contended lock, the seat of
    ``--sequence`` for the first packet), a task takes it and ``wake`` is called when that
    task ends; :meth:`next` returns ``None`` until then and the caller asks again after the
    wake. A failed take is raised by the :meth:`next` that collects it, ``IB-SEQ-005`` or
    ``IB-SEQ-006``, with nothing taken."""

    def __init__(
        self, store: SequenceStore, key: str, *, seat: int | None, wake: Callable[[], None]
    ) -> None:
        self._store: Final = store
        self._key: Final = key
        self._seat = seat  # for the first packet only
        self._wake: Final = wake
        self._task: asyncio.Task[int] | None = None

    def next(self) -> int | None:
        """The count for the next packet, spent; ``None`` while it is being taken (``wake``
        follows)."""
        task = self._task
        if task is not None:
            if not task.done():
                return None
            self._task = None
            return task.result()
        seat, self._seat = self._seat, None
        if seat is None:
            count = self._store.take_now(self._key)
            if count is not None:
                return count
        self._task = asyncio.get_running_loop().create_task(
            self._store.take(self._key, seat=seat), name="sequence-series-take"
        )
        self._task.add_done_callback(lambda _: self._wake())
        return None

    async def aclose(self) -> None:
        """End a take still running (its count, if it is taken, is not used: a gap) and collect
        its outcome, so nothing is left unretrieved."""
        task, self._task = self._task, None
        if task is None:
            return
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
