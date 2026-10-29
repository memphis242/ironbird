"""Turn decoded packets into display text. Every string from data is sanitized here."""

from __future__ import annotations

import math
from typing import Any

from ironbird.ccdd.model import FieldDef, TypeClass
from ironbird.codec import DecodedPacket
from ironbird.ledger import format_ts
from ironbird.sanitize import clean


def _float_text(value: float) -> str:
    return repr(value) if math.isfinite(value) else str(value)


def field_text(field: FieldDef, value: object) -> str:
    """Display form of one decoded field value (enum label, hex for bytes, sanitized text)."""
    match field.type.type_class:
        case TypeClass.INT:
            assert isinstance(value, int)
            label = field.enum_label(value)
            return f"{label}({value})" if label is not None else str(value)
        case TypeClass.FLOAT:
            assert isinstance(value, float)
            return _float_text(value)
        case TypeClass.CHAR:
            assert isinstance(value, str)
            return clean(value)
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
        case _:
            return field_text(field, value)


def line(node: str, time_ns: int, packet: DecodedPacket) -> str:
    """One human-readable line per packet."""
    msg = packet.message
    seq = packet.header.sequence
    head = f"{format_ts(time_ns)} {node} {clean(msg.name)} mid=0x{msg.mid:04X}"
    if seq is not None:
        head += f" seq={seq}"
    body = " ".join(
        f"{clean(f.name)}={field_text(f, v)}"
        for f, v in zip(msg.fields, packet.values, strict=True)
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
        "node": node,
        "name": clean(msg.name),
        "kind": msg.kind.value,
        "mid": f"0x{msg.mid:04X}",
        "sequence": packet.header.sequence,
        "fields": {clean(f.name): json_value(f, v) for f, v in pairs},
        "labels": {
            clean(f.name): clean(label) for f, v in pairs if (label := f.enum_label(v)) is not None
        },
    }
