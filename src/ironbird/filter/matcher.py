"""A compiled filter and the per-consumer matcher that runs it.

A :class:`Filter` is immutable and holds no state: it can be shared by any number of consumers.
A :class:`Matcher` binds one to a decoder and tests frames one at a time. It remembers what it
learned about the frame it is testing (the check result, the decoded packet), so each expensive
step runs at most once per frame however many predicates ask, and the consumer that wants the
decoded packet of a matching frame gets the one the filter already made.

The matcher is not thread-safe; it belongs to one consumer.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import IntEnum

from ironbird.codec import Codec, Resolved
from ironbird.errors import DecodeError
from ironbird.frame import FLAG_TRUNCATED, Decoded, Frame

FrameDecode = Callable[[Frame], Decoded | DecodeError]
MatchFn = Callable[[Frame, "Matcher"], bool]


class Cost(IntEnum):
    """What the dearest predicate of an expression needs; cheaper predicates run first."""

    FRAME = 0  # fields of the frame itself
    MODEL = 1  # a lookup of the frame's MID in the node's CCDD
    CHECK = 2  # Codec.check
    DECODE = 3  # a full decode


@dataclass(frozen=True, slots=True)
class Filter:
    """A compiled filter expression.

    ``match(frame, matcher)`` is the whole test; use :meth:`bind` rather than calling it with a
    hand-made matcher. ``mids`` is the candidate MID set: a frame whose MID is outside it can
    never match, ``match`` rejects it before anything else runs, and so it is never decoded on
    this filter's behalf (``None``: any MID may match). ``needs_decode`` says whether matching
    can decode a frame at all.
    """

    source: str
    mids: frozenset[int] | None
    needs_decode: bool
    match: MatchFn
    cost: Cost

    def bind(self, decode: FrameDecode, codec_for: Callable[[str], Codec]) -> Matcher:
        """A matcher for one consumer. ``decode`` turns a frame into a packet (or the error that
        stopped it) and ``codec_for`` gives the codec of a node. Both must use the definitions
        this filter was compiled against."""
        return Matcher(self, decode, codec_for)


class Matcher:
    """Tests frames against a :class:`Filter`; ``matcher(frame)`` is the verdict.

    :meth:`decoded` returns the decode of the frame last tested (made at most once), or decodes
    the frame it is given; a consumer calls it for each frame that matched.
    """

    __slots__ = (
        "_codec_for",
        "_codecs",
        "_decode",
        "_decoded",
        "_decoded_of",
        "_match",
        "_resolved",
        "_resolved_of",
        "decodes",
    )

    def __init__(self, flt: Filter, decode: FrameDecode, codec_for: Callable[[str], Codec]) -> None:
        self._match: MatchFn = flt.match
        self._decode = decode
        self._codec_for = codec_for
        self._codecs: dict[str, Codec] = {}
        self._decoded: Decoded | DecodeError | None = None
        self._decoded_of: Frame | None = None
        self._resolved: Resolved | DecodeError | None = None
        self._resolved_of: Frame | None = None
        self.decodes: int = 0  # frames this matcher decoded (for tests and statistics)

    def __call__(self, frame: Frame) -> bool:
        return self._match(frame, self)

    def decoded(self, frame: Frame) -> Decoded | DecodeError:
        """The decode of ``frame``, made once however many times it is asked for."""
        if self._decoded_of is frame:
            result = self._decoded
            assert result is not None
            return result
        result = self._decode(frame)
        self.decodes += 1
        self._decoded, self._decoded_of = result, frame
        return result

    def resolved(self, frame: Frame) -> Resolved | DecodeError:
        """What :meth:`Codec.resolve` says about ``frame`` (the check, without the fields): a
        frame that passed live ingest already carries it."""
        if self._resolved_of is frame:
            result = self._resolved
            assert result is not None
            return result
        checked = frame.checked
        result2: Resolved | DecodeError
        if checked is not None:
            result2 = checked
        else:
            stream = frame.stream
            codec = self._codecs.get(stream.node)
            if codec is None:
                codec = self._codecs[stream.node] = self._codec_for(stream.node)
            if int(frame.flags) & FLAG_TRUNCATED:
                result2 = codec.oversize_error(stream.kind, frame.data, frame.orig_len)
            else:
                result2 = codec.resolve(stream.kind, frame.data)
        self._resolved, self._resolved_of = result2, frame
        return result2
