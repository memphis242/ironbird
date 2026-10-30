"""Turn decoded packets into display text. Every string from data is sanitized here."""

from __future__ import annotations

import math
import sys
from collections.abc import Sequence
from typing import Any

from ironbird.ccdd.model import FieldDef, MessageKind, TypeClass
from ironbird.codec import DecodedPacket
from ironbird.config import format_hostport
from ironbird.events import Event
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


def field_text(field: FieldDef, value: object) -> str:
    """Display form of one decoded field value (enum label, hex for bytes, sanitized text)."""
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
            return value.hex()
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
