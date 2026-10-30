"""The replay loop: frames from a :class:`~ironbird.recording.replay.Replay`, paced, filtered,
decoded with the recording's definitions and handed to an output as lines or JSON records.

This is ``ib listen`` over a file, with what a live listener hides shown: a frame that does not
decode (malformed, unknown to the CCDD, serial garbage, cut at the packet cap) is printed flagged,
with its bytes in hex, instead of being reported once and dropped. Nothing here touches a wire.

Decoding is one boundary, as everywhere: :class:`~ironbird.engine.FrameDecoder` with
``report_checked=True`` (there is no ingest to have reported a failure) over
``Codec.decode``; a filter is evaluated by a :class:`~ironbird.filter.Matcher` bound to that same
decoder, so a matching frame is decoded once. The codecs must be the ones the filter was compiled
against.
"""

from __future__ import annotations

import contextlib
import json
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum

from ironbird import render
from ironbird.codec import Codec
from ironbird.engine import FrameDecoder
from ironbird.errors import Code, DecodeError, IronbirdError
from ironbird.filter import Filter, FilterError, compile_filter
from ironbird.frame import Decoded, Frame, FrameFlag
from ironbird.recording.replay import Pacer, Replay


class OutputFormat(StrEnum):
    LINE = "line"
    JSON = "json"
    HEX = "hex"


class Ending(StrEnum):
    DONE = "done"  # every frame was read
    COUNT = "count"  # --count frames were shown
    OUTPUT = "output"  # the reader of the output went away, or stopped being there
    DAMAGE = "damage"  # mid-file damage ended the replay (the Replay says where)


@dataclass(slots=True)
class PlayResult:
    shown: int = 0
    malformed: int = 0
    unknown: int = 0
    garbage: int = 0
    undecoded: int = 0  # shown without a decode: no definitions for the frame's node
    skipped_no_definitions: int = 0  # not shown: a filter needed definitions the node lacks
    ending: Ending = Ending.DONE
    rejected: Counter[str] = field(default_factory=Counter)  # decode failures by code


def compile_where(
    replay: Replay,
    text: str,
    codecs: Mapping[str, Codec],
    *,
    aliases: Mapping[str, str] | None = None,
    degraded: IronbirdError | None = None,
) -> Filter:
    """Compile ``--where`` against the codecs frames will be decoded with and the recording's
    link names, and tell ``replay`` the candidate MIDs so it can skip chunks. ``degraded`` is the
    ``IB-REC-009`` that left ``codecs`` empty (hex output does not need definitions): a filter
    that then fails to compile fails with that as the reason, since the typo may not be one."""
    try:
        flt = compile_filter(text, codecs, links=replay.link_names, aliases=aliases)
    except FilterError as exc:
        if degraded is None:
            raise
        raise IronbirdError(
            Code.REC_DEFS,
            f"--where needs the recording's definitions to be checked ({exc.summary}); "
            f"{degraded.summary}",
            details=exc.detail_text(),
            cause=exc,
        ) from exc
    replay.select_mids(flt.mids)
    return flt


class _NoDefinitions(Exception):
    """A frame of a node with no codec reached a decoder (a stream the head did not declare)."""


def play(
    replay: Replay,
    codecs: Mapping[str, Codec],
    *,
    out: Callable[[str], bool],
    fmt: OutputFormat = OutputFormat.LINE,
    where: Filter | None = None,
    pacer: Pacer | None = None,
    count: int | None = None,
    stop: Callable[[], bool] = lambda: False,
) -> PlayResult:
    """Run ``replay`` to its end. ``out(text)`` writes one line and says whether it was taken
    (``False``: the output is gone, the run ends); ``stop()`` is asked while pacing waits. Never
    raises for anything a recording contains."""
    result = PlayResult()

    def codec_for(node: str) -> Codec:
        codec = codecs.get(node)
        if codec is None:
            raise _NoDefinitions(node)
        return codec

    def reject(frame: Frame, error: DecodeError) -> None:
        result.rejected[error.code.value] += 1

    decoder = FrameDecoder(codec_for, reject, report_checked=True)
    matcher = where.bind(decoder.decode, codec_for) if where is not None else None
    as_json = fmt is OutputFormat.JSON
    dump = fmt is OutputFormat.HEX
    with contextlib.closing(replay.frames()) as frames:
        for frame in frames:
            if pacer is not None and not pacer.wait(frame.time_ns, stop):
                result.ending = Ending.OUTPUT
                break
            decoded: Decoded | DecodeError | None = None
            try:
                if matcher is not None:
                    if not matcher(frame):
                        continue
                    if not dump:
                        decoded = matcher.decoded(frame)
                elif not dump:
                    decoded = decoder.decode(frame)
            except _NoDefinitions:
                if where is not None:
                    result.skipped_no_definitions += 1
                    continue
                result.undecoded += 1
                note = "no definitions for the node"
                text = (
                    json.dumps(render.undecoded_record(frame, None, note))
                    if as_json
                    else render.undecoded_line(frame, None, note)
                )
            else:
                if dump:
                    text = (
                        json.dumps(render.hex_record(frame)) if as_json else render.hex_line(frame)
                    )
                elif isinstance(decoded, Decoded):
                    node, time_ns = frame.stream.node, frame.time_ns
                    text = (
                        json.dumps(render.record(node, time_ns, decoded.packet))
                        if as_json
                        else render.line(node, time_ns, decoded.packet)
                    )
                else:
                    assert isinstance(decoded, DecodeError)
                    if frame.flags & FrameFlag.GARBAGE:
                        result.garbage += 1
                    elif decoded.code is Code.DEC_UNKNOWN:
                        result.unknown += 1
                    else:
                        result.malformed += 1
                    text = (
                        json.dumps(render.undecoded_record(frame, decoded))
                        if as_json
                        else render.undecoded_line(frame, decoded)
                    )
            if not out(text):
                result.ending = Ending.OUTPUT
                break
            result.shown += 1
            if count is not None and result.shown >= count:
                result.ending = Ending.COUNT
                break
        else:
            if replay.ended_by_damage:
                result.ending = Ending.DAMAGE
    return result


__all__ = ["Ending", "OutputFormat", "PlayResult", "compile_where", "play"]
