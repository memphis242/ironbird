"""A second, independent reference evaluator for the filter language (docs/plan.md S4).

``filter_defs.Reference`` is the first. This one was written separately, from the documented
semantics alone, and shares with the compiler only the parser's syntax tree: every name,
operator and literal is interpreted here, per predicate, from a full decode of the frame. The
differential test runs both against the compiled filter, so a misreading by one author is a
disagreement, not a shared bug.
"""

from __future__ import annotations

import math
import re
import struct
from collections.abc import Mapping, Sequence
from typing import Any, Final

from ironbird.ccdd.model import FieldDef, MessageKind, TypeClass
from ironbird.codec import Codec, DecodedPacket
from ironbird.errors import Code, DecodeError
from ironbird.filter.lexer import Tok
from ironbird.filter.parser import And, Expr, Literal, Not, Or, Predicate
from ironbird.frame import Frame, FrameFlag

SEVERITY: Final = {"debug": 1, "info": 2, "information": 2, "warn": 3, "warning": 3}
SEVERITY.update({"error": 4, "critical": 5})
ORDERING: Final = ("<", "<=", ">", ">=")
LOWER_BOUNDS_AND_NE: Final = (">=", ">", "!=")


class Cannot(Exception):
    """The literal is not a value of the thing it is compared with."""


def compare(op: str, actual: Any, wanted: Sequence[Any]) -> bool:
    if actual is None:
        return False
    match op:
        case "==":
            return bool(actual == wanted[0])
        case "!=":
            return bool(actual != wanted[0])
        case "<":
            return bool(actual < wanted[0])
        case "<=":
            return bool(actual <= wanted[0])
        case ">":
            return bool(actual > wanted[0])
        case ">=":
            return bool(actual >= wanted[0])
        case "in":
            return any(actual == w for w in wanted)
        case "contains":
            return bool(wanted[0] in actual)
        case _:
            pass
    raise AssertionError(op)


def number(lit: Literal, op: str) -> Any:
    """An integer; a decimal only for ordering, kept exact (a Decimal compares exactly)."""
    if lit.kind in (Tok.INT, Tok.HEX):
        return lit.value
    if lit.kind is Tok.FLOAT and op in ORDERING:
        return lit.value
    raise Cannot


def word(lit: Literal) -> str:
    if lit.kind in (Tok.IDENT, Tok.STRING):
        return str(lit.value)
    raise Cannot


def text(lit: Literal) -> str:
    if lit.kind is Tok.STRING:
        return str(lit.value)
    raise Cannot


def at_width(value: float, size: int) -> float:
    if size != 4:
        return value
    try:
        return float(struct.unpack("<f", struct.pack("<f", value))[0])
    except OverflowError:
        return math.copysign(math.inf, value)


class IndependentReference:
    def __init__(self, codecs: Mapping[str, Codec]) -> None:
        self.codecs = codecs

    def matches(self, tree: Expr, frame: Frame) -> bool:
        if isinstance(tree, And):
            return all(self.matches(c, frame) for c in tree.children)
        if isinstance(tree, Or):
            return any(self.matches(c, frame) for c in tree.children)
        if isinstance(tree, Not):
            return not self.matches(tree.child, frame)
        assert isinstance(tree, Predicate)
        try:
            return self._predicate(tree, frame)
        except Cannot:
            return False

    def _decode(self, frame: Frame) -> tuple[DecodedPacket | None, str]:
        """The decoded packet (or ``None``) and the status a full decode gives."""
        if frame.flags & FrameFlag.GARBAGE or frame.flags & FrameFlag.TRUNCATED:
            return None, "malformed"
        result = self.codecs[frame.stream.node].decode(frame.stream.kind, frame.data)
        if isinstance(result, DecodeError):
            return None, ("unknown" if result.code is Code.DEC_UNKNOWN else "malformed")
        return result, "ok"

    def _predicate(self, p: Predicate, frame: Frame) -> bool:
        op, lits = p.op, p.values
        stream = frame.stream
        codec = self.codecs[stream.node]
        packet, status = self._decode(frame)
        if len(p.name) == 1:
            name = p.name[0]
            match name:
                case "seq":
                    return compare(op, frame.seq, [number(x, op) for x in lits])
                case "len":
                    return compare(op, frame.orig_len, [number(x, op) for x in lits])
                case "mid":
                    return compare(op, frame.mid, [number(x, op) for x in lits])
                case "time":
                    if any(x.kind is not Tok.TIME for x in lits):
                        raise Cannot
                    return compare(op, frame.time_ns, [x.value for x in lits])
                case "node" | "link":
                    actual = stream.node if name == "node" else stream.link
                    return compare(
                        op, actual, [text(x) if op == "contains" else word(x) for x in lits]
                    )
                case "source":
                    return compare(op, frame.source, [text(x) for x in lits])
                case "dir":
                    return compare(op, stream.direction.value, [word(x).lower() for x in lits])
                case "kind":
                    return compare(op, stream.kind.value, [word(x).lower() for x in lits])
                case "truncated" | "garbage":
                    bit = FrameFlag.TRUNCATED if name == "truncated" else FrameFlag.GARBAGE
                    flags = [{"true": True, "false": False}[word(x).lower()] for x in lits]
                    return compare(op, bool(frame.flags & bit), flags)
                case "status":
                    return compare(op, status, [word(x).lower() for x in lits])
                case "category":
                    found = codec.model.telemetry_by_mid.get(frame.mid)  # type: ignore[arg-type]
                    if stream.kind is not MessageKind.TELEMETRY or found is None:
                        return False
                    return compare(op, found.category.value, [word(x).lower() for x in lits])
                case "msg":
                    if stream.kind is MessageKind.TELEMETRY:
                        known = codec.model.telemetry_by_mid.get(frame.mid)  # type: ignore[arg-type]
                        shown = known.name if known else None
                    else:
                        shown = None
                        if not frame.flags & (FrameFlag.TRUNCATED | FrameFlag.GARBAGE):
                            resolved = codec.resolve(stream.kind, frame.data)
                            shown = (
                                None if isinstance(resolved, DecodeError) else resolved.message.name
                            )
                    return compare(
                        op, shown, [text(x) if op == "contains" else word(x) for x in lits]
                    )
                case "fc":
                    if packet is None or packet.message.function_code is None:
                        return False
                    return compare(op, packet.message.function_code, [number(x, op) for x in lits])
                case _:
                    pass  # a field name
            if packet is None:
                return False
            fld = packet.message.field(name)
            return fld is not None and self._field(packet, fld, op, lits)
        first, second = p.name
        if packet is None:
            return False
        if first == "hdr":
            return compare(op, packet.header.values.get(second), [number(x, op) for x in lits])
        if first == "event":
            return self._event(packet, second, op, lits)
        if packet.message.name != first:
            return False
        fld = packet.message.field(second)
        return fld is not None and self._field(packet, fld, op, lits)

    def _event(self, packet: DecodedPacket, part: str, op: str, lits: Sequence[Literal]) -> bool:
        spec = packet.message.event
        if spec is None:
            return False
        values = packet.as_dict()
        match part:
            case "id":
                return compare(op, values[spec.id], [number(x, op) for x in lits])
            case "type":
                ranked = spec.severities.get(values[spec.type])  # type: ignore[call-overload]
                wanted = []
                for lit in lits:
                    label = word(lit).lower()
                    if label not in SEVERITY:
                        raise Cannot
                    wanted.append(SEVERITY[label])
                if ranked is None:  # no rank: passes a lower bound as --min-type does; != is not ==
                    return op in LOWER_BOUNDS_AND_NE
                return compare(op, int(ranked), wanted)
            case "app" | "text":
                name = spec.app if part == "app" else spec.message
                return name is not None and compare(op, values[name], [text(x) for x in lits])
            case _:
                pass
        raise Cannot

    def _field(
        self, packet: DecodedPacket, fld: FieldDef, op: str, lits: Sequence[Literal]
    ) -> bool:
        actual = packet.as_dict()[fld.name]
        klass = fld.type.type_class
        if klass in (TypeClass.INT, TypeClass.FLOAT) and op == "contains":
            raise Cannot
        if klass in (TypeClass.CHAR, TypeClass.BYTES) and op in ORDERING:
            raise Cannot
        wanted: list[Any] = []
        for lit in lits:
            try:
                wanted.append(self._literal(fld, op, lit))
            except Cannot:
                continue  # an `in` item the field cannot hold is just never equal
        if not wanted:
            return op == "!="  # not ==: the field cannot hold the value, so it differs from it
        return compare(op, actual, wanted)

    @staticmethod
    def _literal(fld: FieldDef, op: str, lit: Literal) -> Any:
        klass = fld.type.type_class
        if klass is TypeClass.INT:
            if lit.kind in (Tok.IDENT, Tok.STRING):
                if fld.enum is None or lit.value not in fld.enum:
                    raise Cannot
                return fld.enum[lit.value]
            return number(lit, op)
        if klass is TypeClass.FLOAT:
            if lit.kind not in (Tok.INT, Tok.HEX, Tok.FLOAT):
                raise Cannot
            return at_width(float(lit.value), fld.type.size)
        if klass is TypeClass.CHAR:
            return text(lit)
        digits = text(lit)
        if not digits or len(digits) % 2 or not re.fullmatch(r"[0-9a-fA-F]+", digits):
            raise Cannot
        raw = bytes.fromhex(digits)
        if (op == "contains" and len(raw) > fld.type.size) or (
            op != "contains" and len(raw) != fld.type.size
        ):
            raise Cannot
        return raw
