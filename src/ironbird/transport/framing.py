"""Stream framing: cutting a serial byte stream into packets without trusting any of it.

A serial line has no datagram boundaries, so the framer finds them from the header profile's
length field and keeps every byte it is given: the bytes it emits, in order, are exactly the
bytes it received. The one exception is a configured ``sync`` marker, which it consumes in front
of each packet: that is the only thing absent from a packet's frame. Nothing is dropped or
reordered, so a recording of the frames holds the line as it was.

The rules, for a candidate that starts at the current position (after ``sync``, when one is
configured):

1. Read the length field once :attr:`HeaderProfile.length_window` bytes are there. The total it
   claims must lie in ``min_bytes .. max_packet_bytes`` or the candidate is rejected before any
   buffer is sized by it.
2. With the whole header: the fixed-field mask must match. The MID and the payload size the
   CCDD allows for it are supporting evidence, not a veto: a packet whose MID is unknown or
   whose size disagrees (a flight software struct that grew, a CCDD not yet updated) is still a
   packet when the rest of the evidence holds, so the decode boundary reports the mismatch
   (``IB-DEC-006``/``IB-DEC-007``) and the packets around it survive. A lying length is
   refused by rules 1, 3 and 4 and never by the CCDD.
3. With the whole candidate: the header and trailer checks of :func:`ironbird.header.check_word`
   (checksum included) must pass. A candidate that fails any step is a false sync: its first
   byte joins the garbage run and scanning restarts at the next byte, never after the candidate,
   so the bytes a false header covered are examined again.
4. A profile without a checksum proves little with a sane header, so the candidate is also
   confirmed: a sane header (after ``sync``) must follow it immediately, or the line must stay
   silent for ``max_idle_ns`` (the last packet before a pause waits that long). A header is sane
   by rules 1 and 2 alone, whatever its MID.

A candidate is judged, copied and checked once: while it waits for its confirmation the framer
remembers that it passed, so a stream read a byte at a time costs no more than one read in bulk.
Only a profile with a checksum is charged per byte for that pass.

Garbage is emitted as ``GARBAGE`` frames, never longer than the cap, when a packet follows, at the
cap, or on expiry; a partial candidate that the line leaves incomplete for ``max_idle_ns`` is
emitted as a ``STALE`` frame, after a look inside it for complete packets a lying header would
have covered. Work is bounded: scanning spends credit that input bytes earn, and when it runs
out the unexamined bytes become garbage (``stats().work_exhausted``), so hostile input cannot
hold the event loop.

The framer is pure: it does no I/O and reads no clock. The caller passes the wall time for the
frames' stamps and a monotonic time for the idle logic.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Final, NamedTuple, Protocol

from ironbird import header as hdr
from ironbird.errors import Code, DecodeError, IronbirdError
from ironbird.frame import FLAG_GARBAGE, FLAG_STALE
from ironbird.header import HeaderProfile

NS_PER_MS: Final = 1_000_000
SAMPLE_BYTES: Final = 32  # bytes of a skipped run kept for the ledger
MAX_PENDING_EPISODES: Final = 4096  # episodes held between two drains; more are counted

# Work units: about a quarter of a microsecond each. A candidate looked at costs EVAL_COST, a
# checksum pass half a unit per byte; input earns ``per_byte`` units a byte.
EVAL_COST: Final = 8
CHECK_COST_SHIFT: Final = 1  # a checksum pass over n bytes costs n >> 1

_REJECT: Final = -1  # _judge: not a packet header
_WAIT: Final = 0  # _judge: undecided until more bytes arrive


class Emit(NamedTuple):
    """One thing the framer hands on: ``flags`` is 0 for a packet, else ``FLAG_GARBAGE`` or
    ``FLAG_STALE``."""

    data: bytes
    time_ns: int
    flags: int


class Episode(NamedTuple):
    """One run of skipped bytes, as emitted: how many and the first few."""

    skipped: int
    sample: bytes
    time_ns: int


@dataclass(frozen=True, slots=True)
class FramerStats:
    """Cumulative counters of a framer."""

    packets: int = 0
    garbage_frames: int = 0
    garbage_bytes: int = 0
    stale_frames: int = 0
    stale_bytes: int = 0
    false_syncs: int = 0  # candidates with a sane header that failed a later check
    work_exhausted: int = 0  # times the work budget ran out and unexamined bytes became garbage
    mismatched: int = 0  # packets emitted although their MID or payload size is not in the CCDD
    work_spent: int = 0  # work units used
    work_earned: int = 0  # work units the budget granted (base plus input)
    episodes_dropped: int = 0  # episodes not kept because the transport did not drain them
    pending_bytes: int = 0  # received, not yet emitted


@dataclass(frozen=True, slots=True)
class WorkBudget:
    """How much scanning each input byte pays for (see the module docstring)."""

    per_byte: int = 24
    base: int = 1 << 16
    cap: int = 1 << 18

    def __post_init__(self) -> None:
        assert self.per_byte > 0 and 0 < self.base <= self.cap


class Framer(Protocol):
    """Turns received bytes into emitted frames. Between two reads at most ``2 * cap + 2 * len(sync)
    + header`` bytes are held (a garbage run under the cap, then a candidate of up to the cap)."""

    def feed(self, chunk: bytes, wall_ns: int, mono_ns: int) -> list[Emit]:
        """Take ``chunk`` (read at ``wall_ns``, ``mono_ns``); return what it completed."""
        ...

    def deadline_ns(self) -> int | None:
        """The monotonic time at which :meth:`expire` would emit something; ``None`` when
        nothing is pending."""
        ...

    def expire(self, mono_ns: int, wall_ns: int) -> list[Emit]:
        """Emit everything pending if the line has been idle since ``deadline_ns``."""
        ...

    def flush(self, wall_ns: int) -> list[Emit]:
        """No more bytes are coming (the link ended): emit everything pending."""
        ...

    def take_episodes(self) -> list[Episode]:
        """The skipped runs emitted since the last call, oldest first."""
        ...

    def stats(self) -> FramerStats: ...


def validate_stream_profile(profile: HeaderProfile, max_packet_bytes: int, context: str) -> None:
    """Refuse, up front, a header profile a stream cannot be framed with.

    The framer needs the length field inside the header (the first ``size_bytes`` bytes) and a
    packet cap that fits at least an empty packet.
    """
    window = profile.length_window
    if window is None or profile.length_field is None:
        raise IronbirdError(
            Code.CFG_LINK,
            f"{context}: serial framing needs a length field, but header profile "
            f"{profile.name!r} has none",
        )
    if window > profile.size_bytes:
        raise IronbirdError(
            Code.CFG_LINK,
            f"{context}: the length field of header profile {profile.name!r} lies outside its "
            "header, so a stream cannot be framed with it",
        )
    if profile.min_bytes > max_packet_bytes:
        raise IronbirdError(
            Code.CFG_LINK,
            f"{context}: header profile {profile.name!r} needs {profile.min_bytes} bytes, "
            f"above max_packet_bytes {max_packet_bytes}",
        )


class HeaderLengthFramer:
    """Frames a stream by the header profile's length field (see the module docstring)."""

    def __init__(
        self,
        profile: HeaderProfile,
        *,
        max_packet_bytes: int,
        sync: bytes = b"",
        max_idle_ns: int = 50 * NS_PER_MS,
        payload_bounds: Callable[[int], tuple[int, int] | None] | None = None,
        budget: WorkBudget = WorkBudget(),  # noqa: B008 - frozen
    ) -> None:
        validate_stream_profile(profile, max_packet_bytes, "framer")
        assert max_idle_ns > 0
        self._profile = profile
        self._cap = max_packet_bytes
        self._sync = sync
        self._idle_ns = max_idle_ns
        self._bounds = payload_bounds
        self._budget = budget
        window = profile.length_window
        assert window is not None
        self._window: int = window
        self._hsize: int = profile.size_bytes
        self._min: int = profile.min_bytes
        self._mask = profile.fixed_mask
        self._value = profile.fixed_value
        self._confirm = profile.checksum_field is None
        self._charge_bytes = profile.checksum_field is not None  # only a checksum pass is O(n)
        self._buf = bytearray()
        self._garb = 0  # buf[:_garb] is classified garbage; the candidate starts there
        self._cand = 0  # total size of the candidate at _garb if it passed rules 1-3, else 0
        self._last_mono = 0
        self._credit = budget.base
        self._episodes: list[Episode] = []
        self._packets = self._garbage_frames = self._garbage_bytes = 0
        self._stale_frames = self._stale_bytes = self._false_syncs = self._mismatched = 0
        self._exhausted = self._spent = self._episodes_dropped = 0
        self._earned = budget.base

    # -- API ---------------------------------------------------------------------------

    def feed(self, chunk: bytes, wall_ns: int, mono_ns: int) -> list[Emit]:
        if not chunk:
            return []
        grant = self._budget.per_byte * len(chunk)
        credit = min(self._budget.cap, self._credit + grant)
        self._earned += credit - self._credit
        self._credit = credit
        self._last_mono = mono_ns
        self._buf += chunk
        return self._pump(wall_ns, final=False)

    def deadline_ns(self) -> int | None:
        return self._last_mono + self._idle_ns if self._buf else None

    def expire(self, mono_ns: int, wall_ns: int) -> list[Emit]:
        if self._buf and mono_ns - self._last_mono >= self._idle_ns:
            return self._pump(wall_ns, final=True)
        return []

    def flush(self, wall_ns: int) -> list[Emit]:
        return self._pump(wall_ns, final=True) if self._buf else []

    def take_episodes(self) -> list[Episode]:
        taken, self._episodes = self._episodes, []
        return taken

    def stats(self) -> FramerStats:
        return FramerStats(
            packets=self._packets,
            garbage_frames=self._garbage_frames,
            garbage_bytes=self._garbage_bytes,
            stale_frames=self._stale_frames,
            stale_bytes=self._stale_bytes,
            false_syncs=self._false_syncs,
            work_exhausted=self._exhausted,
            mismatched=self._mismatched,
            work_spent=self._spent,
            work_earned=self._earned,
            episodes_dropped=self._episodes_dropped,
            pending_bytes=len(self._buf),
        )

    # -- judging a candidate -----------------------------------------------------------

    def _judge(self, buf: bytearray, head: int, n: int) -> int:
        """What the header at ``buf[head:]`` (``n`` bytes are held) says: ``_REJECT``, ``_WAIT``
        for more bytes, or the total packet size it claims when the header is sane (rules 1
        and 2: the MID is not looked at)."""
        avail = n - head
        if avail < self._window:
            return _WAIT
        size = self._hsize
        prefix = buf[head : head + size]
        total = self._profile.frame_length(prefix)
        assert total is not None, "the prefix holds the length window"
        if total < self._min or total > self._cap:
            return _REJECT
        if avail < size:
            return _WAIT
        if int.from_bytes(prefix, "big") & self._mask != self._value:
            return _REJECT
        return total

    def _agrees(self, packet: bytes) -> bool:
        """Whether the MID of ``packet`` is in the CCDD with a payload size inside its range
        (always, when the framer was given no bounds)."""
        bounds = self._bounds
        if bounds is None:
            return True
        mid = hdr.peek_mid(self._profile, packet)
        assert mid is not None, "the MID window lies inside the header"
        allowed = bounds(mid)
        return allowed is not None and allowed[0] <= len(packet) - self._min <= allowed[1]

    def _confirmed(self, buf: bytearray, end: int, n: int, *, final: bool) -> int:
        """Whether a sane header follows a complete candidate that ends at ``end``: 1 yes, 0 not
        yet known, -1 no. With no more bytes coming (``final``) silence confirms."""
        sync = self._sync
        head = end + len(sync)
        if n - end < len(sync):
            return 1 if final else 0
        if sync and bytes(buf[end:head]) != sync:
            return -1
        verdict = self._judge(buf, head, n)
        if verdict > 0:
            return 1
        if verdict == _WAIT:
            return 1 if final else 0
        return -1

    # -- the scan ------------------------------------------------------------------------

    def _partial_sync(self, buf: bytearray, pos: int, n: int) -> int:
        """Length of the longest proper prefix of the sync marker that ends the held bytes."""
        sync = self._sync
        for length in range(min(len(sync) - 1, n - pos), 0, -1):
            if buf.endswith(sync[:length]):
                return length
        return 0

    def _emit_run(self, out: list[Emit], run: bytes, stale_from: int, wall_ns: int) -> None:
        """Emit a run of skipped bytes in pieces of at most the cap: ``GARBAGE`` before
        ``stale_from`` and ``STALE`` from it (``-1``: all garbage)."""
        cap = self._cap
        split = len(run) if stale_from < 0 else stale_from
        for start, stop, flag in ((0, split, FLAG_GARBAGE), (split, len(run), FLAG_STALE)):
            for at in range(start, stop, cap):
                piece = run[at : min(at + cap, stop)]
                out.append(Emit(piece, wall_ns, flag))
                if flag == FLAG_GARBAGE:
                    self._garbage_frames += 1
                    self._garbage_bytes += len(piece)
                else:
                    self._stale_frames += 1
                    self._stale_bytes += len(piece)
        if len(self._episodes) < MAX_PENDING_EPISODES:
            self._episodes.append(Episode(len(run), run[:SAMPLE_BYTES], wall_ns))
        else:
            self._episodes_dropped += 1

    def _split_garbage(self, out: list[Emit], buf: bytearray, pos: int, wall_ns: int) -> int:
        """Emit the garbage run's leading pieces of exactly the cap; returns the new ``pos``."""
        cap = self._cap
        while pos >= cap:
            self._emit_run(out, bytes(buf[:cap]), -1, wall_ns)
            del buf[:cap]
            pos -= cap
        return pos

    def _pump(self, wall_ns: int, *, final: bool) -> list[Emit]:
        out: list[Emit] = []
        buf = self._buf
        sync, sync_len = self._sync, len(self._sync)
        cap, hsize = self._cap, self._hsize
        pos = self._garb
        passed = self._cand  # the candidate at ``pos`` already passed rules 1-3 on an earlier read
        self._cand = 0
        stale_from = -1  # start of the stale part of the run (final scans only)
        credit = self._credit
        spent = 0
        while True:
            n = len(buf)
            if credit < 0:  # out of work budget: what is left unexamined becomes garbage
                keep = sync_len + hsize - 1  # a header may still begin in the last bytes
                if n - keep > pos:
                    passed = 0
                pos = max(pos, n - keep)
                self._exhausted += 1
                grace = EVAL_COST * (keep + 2)  # enough to look at the bytes kept
                self._earned += grace - credit  # the debt is written off with the grant
                credit = grace
            if not final:
                pos = self._split_garbage(out, buf, pos, wall_ns)
                n = len(buf)
            if pos >= n:
                break
            if sync:
                at = buf.find(sync, pos)
                if at < 0:
                    pos = n - (0 if final else self._partial_sync(buf, pos, n))
                    passed = 0
                    break
                if at != pos:
                    passed = 0
                pos = at
            head = pos + sync_len
            if passed:
                verdict = passed
            else:
                credit -= EVAL_COST
                spent += EVAL_COST
                verdict = self._judge(buf, head, n)
            if verdict == _REJECT:
                pos += 1
                continue
            end = head + verdict
            if verdict == _WAIT or n < end:
                if not final:
                    break
                if verdict > 0 and stale_from < 0:
                    stale_from = pos  # a plausible header the line left incomplete
                pos += 1
                continue
            packet = b""
            if not passed:
                packet = bytes(buf[head:end])
                if self._charge_bytes:
                    cost = len(packet) >> CHECK_COST_SHIFT
                    credit -= cost
                    spent += cost
                if isinstance(hdr.check_word(self._profile, packet, max_size=cap), DecodeError):
                    self._false_syncs += 1
                    pos += 1
                    continue
            if self._confirm:
                credit -= EVAL_COST
                spent += EVAL_COST
                sure = self._confirmed(buf, end, n, final=final)
                if sure < 0:
                    self._false_syncs += 1
                    pos += 1
                    passed = 0
                    continue
                if sure == 0:
                    self._cand = verdict  # the packet is not copied: it is the buffer's
                    break
            if pos:
                self._emit_run(out, bytes(buf[:pos]), stale_from, wall_ns)
            packet = packet or bytes(buf[head:end])
            if not self._agrees(packet):
                self._mismatched += 1
            out.append(Emit(packet, wall_ns, 0))
            self._packets += 1
            del buf[:end]
            pos, stale_from, passed = 0, -1, 0
        if final:
            if buf:
                self._emit_run(out, bytes(buf), stale_from, wall_ns)
                buf.clear()
            pos = 0
            self._cand = 0
        else:
            pos = self._split_garbage(out, buf, pos, wall_ns)
        self._garb = pos
        self._spent += spent
        self._credit = credit  # a debt of one candidate at most is carried to the next feed
        return out
