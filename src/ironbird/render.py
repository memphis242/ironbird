"""Turn decoded packets into display text. Every string from data is sanitized here."""

from __future__ import annotations

import math
import sys
from collections.abc import Sequence
from typing import Any

from ironbird.ccdd.model import FieldDef, MessageKind, TypeClass
from ironbird.codec import DecodedPacket
from ironbird.config import format_hostport
from ironbird.errors import Code, DecodeError
from ironbird.events import Event
from ironbird.frame import Frame, FrameFlag
from ironbird.ledger import format_ts
from ironbird.sanitize import DEFAULT_MAX_LEN, clean_json, clean_terminal


def term(text: str, max_len: int = DEFAULT_MAX_LEN, *, err: bool = False) -> str:
    """Inert, encodable text for the terminal for any string from a config, CCDD or packet.

    This is the one helper every printed untrusted string goes through (``err`` selects the
    stderr stream's encoding instead of stdout's).
    """
    stream = sys.stderr if err else sys.stdout
    return clean_terminal(text, max_len, encoding=getattr(stream, "encoding", None))


def jtext(text: str) -> str:
    """Whole, unescaped text for a string value inside ``--format json`` output."""
    return clean_json(text)


def _float_text(value: float) -> str:
    return repr(value) if math.isfinite(value) else str(value)


MAX_ELEMENTS_SHOWN = 16  # elements of an array a line shows; the rest is counted


def _element_text(value: object) -> str:
    if isinstance(value, float):
        return _float_text(value)
    assert isinstance(value, int)
    return str(value)


def _more(total: int) -> str:
    """``...(+N)`` for the elements past the first :data:`MAX_ELEMENTS_SHOWN`, else nothing."""
    return f"...(+{total - MAX_ELEMENTS_SHOWN})" if total > MAX_ELEMENTS_SHOWN else ""


def field_text(field: FieldDef, value: object) -> str:
    """Display form of one decoded field value (enum label, hex for bytes, sanitized text).

    A variable-length array shows its first :data:`MAX_ELEMENTS_SHOWN` elements and how many more
    there are (``[1 2 3 ...(+40)]``); a ``bytes[]`` shows that many bytes as hex."""
    var = field.var
    match field.type.type_class:
        case TypeClass.INT:
            assert isinstance(value, int)
            label = field.enum_label(value)
            return f"{term(label)}({value})" if label is not None else str(value)
        case TypeClass.FLOAT:
            assert isinstance(value, float)
            return _float_text(value)
        case TypeClass.CHAR:
            assert isinstance(value, str)
            return term(value)
        case TypeClass.BYTES:
            assert isinstance(value, bytes)
            if var is None:
                return value.hex()
            return " ".join(
                part for part in (value[:MAX_ELEMENTS_SHOWN].hex(), _more(len(value))) if part
            )
        case TypeClass.ARRAY:
            assert var is not None and isinstance(value, tuple)
            parts = [_element_text(v) for v in value[:MAX_ELEMENTS_SHOWN]]
            if more := _more(len(value)):
                parts.append(more)
            return f"[{' '.join(parts)}]"
    raise AssertionError("unreachable")  # pragma: no cover


def json_value(field: FieldDef, value: object) -> Any:
    """JSON-safe value: finite numbers stay numbers, everything else is text."""
    match field.type.type_class:
        case TypeClass.INT:
            return value
        case TypeClass.FLOAT:
            assert isinstance(value, float)
            return value if math.isfinite(value) else str(value)
        case TypeClass.CHAR:
            assert isinstance(value, str)
            return jtext(value)
        case TypeClass.ARRAY:
            assert isinstance(value, tuple)
            return [v if not isinstance(v, float) or math.isfinite(v) else str(v) for v in value]
        case TypeClass.BYTES:
            assert isinstance(value, bytes)
            return value.hex()
        case _:
            return field_text(field, value)


def line(node: str, time_ns: int, packet: DecodedPacket) -> str:
    """One human-readable line per packet."""
    msg = packet.message
    seq = packet.header.sequence
    head = f"{format_ts(time_ns)} {term(node)} {term(msg.name)} mid=0x{msg.mid:04X}"
    if msg.kind is MessageKind.TELEMETRY:
        head += f" cat={msg.category.value}"
    if seq is not None:
        head += f" seq={seq}"
    body = " ".join(
        f"{term(f.name)}={field_text(f, v)}" for f, v in zip(msg.fields, packet.values, strict=True)
    )
    return f"{head} {body}".rstrip()


def record(node: str, time_ns: int, packet: DecodedPacket) -> dict[str, Any]:
    """JSON-serialisable record for ``--format json``.

    Integers stay numeric; ``labels`` carries the enum name of every field that has one.
    """
    msg = packet.message
    pairs = list(zip(msg.fields, packet.values, strict=True))
    return {
        "time": format_ts(time_ns),
        "node": jtext(node),
        "name": jtext(msg.name),
        "kind": msg.kind.value,
        "category": msg.category.value if msg.kind is MessageKind.TELEMETRY else None,
        "mid": f"0x{msg.mid:04X}",
        "sequence": packet.header.sequence,
        "fields": {jtext(f.name): json_value(f, v) for f, v in pairs},
        "labels": {
            jtext(f.name): jtext(label) for f, v in pairs if (label := f.enum_label(v)) is not None
        },
    }


def _type_text(event: Event) -> str:
    return term(event.type_label) if event.type_label else f"UNKNOWN({event.type_value})"


def event_line(event: Event) -> str:
    """One line per event: time, node, app, type, id, message."""
    app = term(event.app) if event.app else "-"
    head = (
        f"{format_ts(event.time_ns)} {term(event.node)} {app} {_type_text(event)} "
        f"id={event.event_id}"
    )
    return f"{head} {term(event.text)}".rstrip()


def event_record(event: Event) -> dict[str, Any]:
    """JSON-serialisable record of an event for ``--format json``."""
    return {
        "time": format_ts(event.time_ns),
        "node": jtext(event.node),
        "app": jtext(event.app),
        "type": jtext(event.type_label),
        "type_value": event.type_value,
        "severity": event.severity.name if event.severity is not None else None,
        "id": event.event_id,
        "message": jtext(event.text),
        "name": jtext(event.name),
        "mid": f"0x{event.mid:04X}",
        "sequence": event.sequence,
    }


# -- command output built from config/CCDD strings -----------------------------------------


def _where(addr: tuple[str, int] | None) -> str:
    return f" on udp {format_hostport(*addr)}" if addr else ""


def listen_banner(node: str, addr: tuple[str, int] | None) -> str:
    return f"listening for {term(node, err=True)}{_where(addr)}"


def events_banner(endpoints: Sequence[tuple[str, tuple[str, int] | None]]) -> str:
    where = ", ".join(
        f"{term(name, err=True)} (udp {format_hostport(*addr)})" if addr else term(name, err=True)
        for name, addr in endpoints
    )
    return f"listening for events from {where}"


def respond_banner(node: str, addr: tuple[str, int] | None) -> str:
    return f"{term(node, err=True)} responding{_where(addr)}"


def respond_line(node: str, command: str, sequence: int | None, reply: str | None) -> str:
    arrow = f" -> {term(reply)}" if reply else ""
    return f"{term(node)} rx {term(command)} seq={sequence}{arrow}"


# -- frames that did not decode (replay shows what a live listener hides) ---------------------

MAX_HEX_SHOWN = 256  # bytes of a frame a line shows; the JSON record carries up to MAX_HEX_JSON
MAX_HEX_JSON = 1024


def flag_names(frame: Frame) -> list[str]:
    """Lower-case names of the flags of ``frame`` that say something to a reader (``HAS_MID`` is
    implied by the MID shown)."""
    return [
        flag.name.lower()
        for flag in FrameFlag
        if flag and flag is not FrameFlag.HAS_MID and flag in frame.flags and flag.name
    ]


def frame_status(frame: Frame, error: DecodeError | None) -> str:
    """``garbage``, ``unknown`` (a MID or function code the CCDD lacks), ``malformed`` or
    ``undecoded`` (no definitions to decode with)."""
    if frame.flags & FrameFlag.GARBAGE:
        return "garbage"
    if error is None:
        return "undecoded"
    return "unknown" if error.code is Code.DEC_UNKNOWN else "malformed"


def hex_dump(data: bytes, limit: int = MAX_HEX_SHOWN, mark: int | None = None) -> str:
    """Spaced hex of the first ``limit`` bytes, the byte at ``mark`` in brackets, and how many
    bytes were left out."""
    shown = data[:limit]
    parts = [f"{b:02x}" for b in shown]
    if mark is not None and 0 <= mark < len(parts):
        parts[mark] = f"[{parts[mark]}]"
    text = " ".join(parts)
    if len(data) > limit:
        text += f" ...(+{len(data) - limit} bytes)"
    return text


def _frame_head(frame: Frame) -> str:
    stream = frame.stream
    mid = f" mid=0x{frame.mid:04X}" if frame.mid is not None else ""
    flags = flag_names(frame)
    extra = f" flags={','.join(flags)}" if flags else ""
    cut = f" (of {frame.orig_len})" if frame.orig_len != len(frame.data) else ""
    return (
        f"{format_ts(frame.time_ns)} {term(stream.node)} {stream.direction.value} "
        f"{term(stream.link)} frame={frame.seq}{mid} len={len(frame.data)}{cut}{extra}"
    )


def undecoded_line(frame: Frame, error: DecodeError | None, note: str = "") -> str:
    """One line for a frame that did not decode: status, why, and its bytes in hex with the
    failing byte in brackets (a garbage frame has no failing byte)."""
    status = frame_status(frame, error)
    if error is None:
        why = term(note) if note else "no definitions"
    else:
        where = "" if status == "garbage" else f" at byte {error.offset}"
        why = f"{error.code.value}{where}: {term(error.reason)}"
    mark = None if status == "garbage" or error is None else error.offset
    return f"{_frame_head(frame)} {status.upper()} {why}  hex: {hex_dump(frame.data, mark=mark)}"


def hex_line(frame: Frame) -> str:
    """One line for ``--format hex``: the frame's fields and all of its bytes."""
    return f"{_frame_head(frame)} hex: {hex_dump(frame.data, len(frame.data))}"


def _frame_record(frame: Frame) -> dict[str, Any]:
    stream = frame.stream
    shown = frame.data[:MAX_HEX_JSON]
    return {
        "time": format_ts(frame.time_ns),
        "node": jtext(stream.node),
        "dir": stream.direction.value,
        "link": jtext(stream.link),
        "seq": frame.seq,
        "source": jtext(frame.source),
        "mid": f"0x{frame.mid:04X}" if frame.mid is not None else None,
        "len": len(frame.data),
        "orig_len": frame.orig_len,
        "flags": flag_names(frame),
        "hex": shown.hex(),
        "hex_truncated": len(frame.data) > len(shown),
    }


def undecoded_record(frame: Frame, error: DecodeError | None, note: str = "") -> dict[str, Any]:
    """JSON record of a frame that did not decode (``status`` tells which kind; a decoded
    packet's record has no ``status``)."""
    out = _frame_record(frame)
    out["status"] = frame_status(frame, error)
    out["error"] = (
        {"code": error.code.value, "offset": error.offset, "reason": jtext(error.reason)}
        if error is not None
        else {"code": None, "offset": None, "reason": jtext(note or "no definitions")}
    )
    return out


def hex_record(frame: Frame) -> dict[str, Any]:
    """JSON record for ``--format hex``."""
    return _frame_record(frame)
