"""Shared definitions for the filter tests and benchmark: two nodes' CCDDs, a frame factory, and a
naive reference evaluator that shares nothing with the compiler but the parser."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any, Final

from conftest import EXAMPLES
from ironbird.ccdd.loader import load_ccdd
from ironbird.ccdd.model import FieldDef, MessageKind, TypeClass
from ironbird.codec import Codec, DecodedPacket
from ironbird.errors import Code, DecodeError
from ironbird.filter import parse
from ironbird.filter.lexer import Tok
from ironbird.filter.parser import And, Expr, Literal, Not, Or, Predicate
from ironbird.frame import Decoded, Direction, Frame, FrameFlag, Stream
from ironbird.header import load_profile

EVENT_TYPES: Final = {"DEBUG": 1, "INFORMATION": 2, "ERROR": 3, "CRITICAL": 4}


def _tlm(name: str, mid: int, category: str, fields: list[dict[str, Any]], **extra: Any) -> Any:
    return {
        "name": name,
        "kind": "telemetry",
        "mid": hex(mid),
        "category": category,
        "fields": fields,
        **extra,
    }


def _cmd(name: str, fc: int, fields: list[dict[str, Any]]) -> Any:
    return {
        "name": name,
        "kind": "command",
        "mid": hex(0x1800 + (1 if name.startswith("N2") else 0)),
        "function_code": fc,
        "fields": fields,
    }


CCDD_N1: Final = {
    "schema": "ironbird-ccdd/0",
    "target": "n1",
    "messages": [
        _tlm(
            "TLM_A",
            0x0800,
            "housekeeping",
            [
                {"name": "counter", "type": "uint8"},
                {"name": "level", "type": "int16"},
                {"name": "ratio", "type": "float32"},
                {"name": "name", "type": "char[8]"},
                {"name": "blob", "type": "bytes[4]"},
                {"name": "mode", "type": "uint8", "enum": {"SAFE": 1, "NOMINAL": 2}},
            ],
        ),
        _tlm(
            "TLM_B",
            0x0801,
            "status",
            [
                {"name": "counter", "type": "uint16"},
                {"name": "ratio", "type": "float64"},
                {"name": "label", "type": "char[6]"},
            ],
        ),
        _tlm(
            "TLM_EV",
            0x0802,
            "event",
            [
                {"name": "app", "type": "char[8]"},
                {"name": "event_id", "type": "uint16"},
                {"name": "event_type", "type": "uint16", "enum": EVENT_TYPES},
                {"name": "text", "type": "char[16]"},
            ],
            event={"app": "app", "id": "event_id", "type": "event_type", "message": "text"},
        ),
        _cmd("CMD_X", 0, []),
        _cmd(
            "CMD_Y",
            1,
            [
                {"name": "counter", "type": "uint8"},
                {"name": "mode", "type": "uint8", "enum": {"SAFE": 1, "NOMINAL": 2}},
            ],
        ),
    ],
}

# The second node reuses names and MIDs with other meanings: MID 0x0801 is a status message
# called TLM_A here, and its `counter` is text.
CCDD_N2: Final = {
    "schema": "ironbird-ccdd/0",
    "target": "n2",
    "messages": [
        _tlm(
            "TLM_A",
            0x0801,
            "status",
            [
                {"name": "counter", "type": "char[4]"},
                {"name": "other", "type": "uint32", "endian": "big"},
            ],
        ),
        _tlm(
            "TLM_EV2",
            0x0810,
            "event",
            [
                {"name": "app", "type": "char[4]"},
                {"name": "event_id", "type": "uint8"},
                {"name": "event_type", "type": "uint8", "enum": {"INFO": 1, "ERROR": 2}},
                {"name": "text", "type": "char[8]"},
            ],
            event={"app": "app", "id": "event_id", "type": "event_type", "message": "text"},
        ),
        _cmd("N2_CMD", 0, [{"name": "level", "type": "int16"}]),
    ],
}

LINKS: Final = ("l1", "l2")


def build_codecs(directory: Path) -> dict[str, Codec]:
    """The two nodes' codecs (example header profiles) from CCDD files written to ``directory``."""
    command = load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml")
    telemetry = load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml")
    codecs: dict[str, Codec] = {}
    for node, ccdd in (("n1", CCDD_N1), ("n2", CCDD_N2)):
        path = directory / f"{node}.json"
        path.write_text(json.dumps(ccdd))
        codecs[node] = Codec(load_ccdd(path), command, telemetry, clock=lambda: 1_000_000.25)
    return codecs


def streams() -> dict[tuple[str, MessageKind], Stream]:
    out: dict[tuple[str, MessageKind], Stream] = {}
    for i, node in enumerate(("n1", "n2")):
        for j, kind in enumerate(MessageKind):
            out[(node, kind)] = Stream(
                id=i * 2 + j,
                link=f"l{i + 1}",
                node=node,
                kind=kind,
                direction=Direction.RX,
                transport="udp",
                endpoint="udp 127.0.0.1:1",
            )
    out[("n1", MessageKind.TELEMETRY)] = Stream(
        id=10,
        link="l1",
        node="n1",
        kind=MessageKind.TELEMETRY,
        direction=Direction.RX,
        transport="serial",
        endpoint="serial /dev/null",
    )
    return out


STREAMS: Final = streams()
SERIAL_STREAM: Final = STREAMS[("n1", MessageKind.TELEMETRY)]


def make_frame(
    codec: Codec,
    stream: Stream,
    data: bytes,
    *,
    seq: int = 1,
    time_ns: int = 1_800_000_000_000_000_000,
    source: str = "127.0.0.1:2234",
    cut: int | None = None,
    live: bool = False,
    garbage: bool = False,
) -> Frame:
    """A frame of ``data`` as ingest or replay would build it (``live``: with its check token;
    ``cut``: the datagram was longer on the wire and cut to ``len(data)``; ``garbage``: serial)."""
    kind = stream.kind
    orig_len = len(data) if cut is None else len(data) + cut
    flags = FrameFlag.NONE
    if cut:
        flags |= FrameFlag.TRUNCATED
    mid: int | None = None if garbage else codec.peek_mid(kind, data)
    if garbage:
        flags |= FrameFlag.GARBAGE
    if mid is not None:
        flags |= FrameFlag.HAS_MID
    checked = None
    if live and not cut and not garbage:
        resolved = codec.resolve(kind, data)
        checked = None if isinstance(resolved, DecodeError) else resolved
    return Frame(seq, time_ns, stream, source, mid, flags, orig_len, data, checked)


def decode_frame(codecs: Mapping[str, Codec], frame: Frame) -> Decoded | DecodeError:
    """The full decode of a frame, as a consumer without the ingest's token would do it."""
    codec = codecs[frame.stream.node]
    if frame.flags & FrameFlag.TRUNCATED:
        return codec.oversize_error(frame.stream.kind, frame.data, frame.orig_len)
    result = codec.decode(frame.stream.kind, frame.data)
    return result if isinstance(result, DecodeError) else Decoded(frame, result)


# -- the reference evaluator -------------------------------------------------------------


class _Inexpressible(Exception):
    """The literal cannot be a value of the thing it is compared with: the predicate is false."""


_SEVERITY: Final = {
    "debug": 1,
    "info": 2,
    "information": 2,
    "warn": 3,
    "warning": 3,
    "error": 4,
    "critical": 5,
}
_ORDER: Final = {
    "<": lambda a, b: a < b,
    "<=": lambda a, b: a <= b,
    ">": lambda a, b: a > b,
    ">=": lambda a, b: a >= b,
}


def _apply(op: str, actual: Any, wanted: Sequence[Any]) -> bool:
    if actual is None:
        return False
    if op == "==":
        return bool(actual == wanted[0])
    if op == "!=":
        return bool(actual != wanted[0])
    if op == "in":
        return any(actual == w for w in wanted)
    if op == "contains":
        return wanted[0] in actual
    return bool(_ORDER[op](actual, wanted[0]))


def _number(lit: Literal, op: str) -> Any:
    if lit.kind in (Tok.INT, Tok.HEX):
        return lit.value
    if lit.kind is Tok.FLOAT and op in _ORDER:
        return lit.value
    raise _Inexpressible


def _word(lit: Literal) -> str:
    if lit.kind in (Tok.IDENT, Tok.STRING):
        return str(lit.value)
    raise _Inexpressible


def _text(lit: Literal) -> str:
    if lit.kind is Tok.STRING:
        return str(lit.value)
    raise _Inexpressible


def _field_value(fld: FieldDef, lit: Literal, op: str) -> Any:
    match fld.type.type_class:
        case TypeClass.INT:
            if lit.kind in (Tok.IDENT, Tok.STRING):
                if fld.enum is None or lit.value not in fld.enum:
                    raise _Inexpressible
                return fld.enum[lit.value]
            return _number(lit, op)
        case TypeClass.FLOAT:
            if lit.kind in (Tok.INT, Tok.HEX, Tok.FLOAT):
                return float(lit.value)
            raise _Inexpressible
        case TypeClass.CHAR:
            return _text(lit)
        case TypeClass.BYTES:
            text = _text(lit)
            if not text or len(text) % 2 or any(c not in "0123456789abcdefABCDEF" for c in text):
                raise _Inexpressible
            raw = bytes.fromhex(text)
            if op == "contains":
                if len(raw) > fld.type.size:
                    raise _Inexpressible
            elif len(raw) != fld.type.size:
                raise _Inexpressible
            return raw
    raise AssertionError("unreachable")


def _field_allows(fld: FieldDef, op: str) -> bool:
    if fld.type.type_class in (TypeClass.INT, TypeClass.FLOAT):
        return op != "contains"
    return op in ("==", "!=", "in", "contains")


class Reference:
    """The documented semantics of S4, written the slow way: everything is computed from the
    frame's full decode, per predicate, with no tables, no ordering and no candidate set."""

    def __init__(self, codecs: Mapping[str, Codec]) -> None:
        self.codecs = codecs

    def matches(self, text: str, frame: Frame) -> bool:
        return self._eval(parse(text), frame)

    def _eval(self, tree: Expr, frame: Frame) -> bool:
        if isinstance(tree, And):
            return all(self._eval(c, frame) for c in tree.children)
        if isinstance(tree, Or):
            return any(self._eval(c, frame) for c in tree.children)
        if isinstance(tree, Not):
            return not self._eval(tree.child, frame)
        assert isinstance(tree, Predicate)
        try:
            return self._predicate(tree, frame)
        except _Inexpressible:
            return False

    def _predicate(self, p: Predicate, frame: Frame) -> bool:
        name, op = ".".join(p.name), p.op
        stream = frame.stream
        codec = self.codecs[stream.node]
        packet = decode_frame(self.codecs, frame)
        decoded: DecodedPacket | None = None if isinstance(packet, DecodeError) else packet.packet
        lits = p.values
        match name:
            case "seq":
                return _apply(op, frame.seq, [_number(x, op) for x in lits])
            case "len":
                return _apply(op, frame.orig_len, [_number(x, op) for x in lits])
            case "mid":
                return _apply(op, frame.mid, [_number(x, op) for x in lits])
            case "time":
                if any(x.kind is not Tok.TIME for x in lits):
                    raise _Inexpressible
                return _apply(op, frame.time_ns, [x.value for x in lits])
            case "node" | "link" | "source":
                actual = {
                    "node": stream.node,
                    "link": stream.link,
                    "source": frame.source,
                }[name]
                wanted = [_text(x) if name == "source" else _word(x) for x in lits]
                return _apply(op, actual, wanted)
            case "dir" | "kind":
                actual = stream.direction.value if name == "dir" else stream.kind.value
                return _apply(op, actual, [_word(x).lower() for x in lits])
            case "truncated" | "garbage":
                bit = FrameFlag.TRUNCATED if name == "truncated" else FrameFlag.GARBAGE
                wanted = [_word(x).lower() == "true" for x in lits]
                return _apply(op, bool(frame.flags & bit), wanted)
            case "status":
                if isinstance(packet, DecodeError):
                    actual = "unknown" if packet.code is Code.DEC_UNKNOWN else "malformed"
                else:
                    actual = "ok"
                return _apply(op, actual, [_word(x).lower() for x in lits])
            case "msg":
                if stream.kind is MessageKind.TELEMETRY:
                    found = codec.model.telemetry_by_mid.get(frame.mid)  # type: ignore[arg-type]
                    actual = found.name if found else None
                else:
                    actual = decoded.message.name if decoded else None
                wanted = [_text(x) if op == "contains" else _word(x) for x in lits]
                return _apply(op, actual, wanted)
            case "category":
                found = codec.model.telemetry_by_mid.get(frame.mid)  # type: ignore[arg-type]
                if stream.kind is not MessageKind.TELEMETRY or found is None:
                    return False
                return _apply(op, found.category.value, [_word(x).lower() for x in lits])
            case "fc":
                if decoded is None or decoded.message.function_code is None:
                    return False
                return _apply(op, decoded.message.function_code, [_number(x, op) for x in lits])
            case _:
                pass
        if decoded is None:
            return False
        if p.name[0] == "hdr" and len(p.name) == 2:
            return _apply(op, decoded.header.values.get(p.name[1]), [_number(x, op) for x in lits])
        if p.name[0] == "event" and len(p.name) == 2:
            spec = decoded.message.event
            if spec is None:
                return False
            return self._event(p.name[1], op, lits, decoded)
        values = decoded.as_dict()
        if len(p.name) == 2:  # MSG.field
            if decoded.message.name != p.name[0]:
                return False
            field_name = p.name[1]
        else:
            field_name = p.name[0]
        fld = decoded.message.field(field_name)
        if fld is None or not _field_allows(fld, op):
            return False
        return _apply(op, values[field_name], [_field_value(fld, x, op) for x in lits])

    @staticmethod
    def _event(part: str, op: str, lits: Sequence[Literal], decoded: DecodedPacket) -> bool:
        spec = decoded.message.event
        assert spec is not None
        values = decoded.as_dict()
        match part:
            case "id":
                return _apply(op, values[spec.id], [_number(x, op) for x in lits])
            case "type":
                severity = spec.severities.get(int(values[spec.type]))  # type: ignore[call-overload]
                if severity is None:
                    return False
                wanted = []
                for x in lits:
                    key = _word(x).lower()
                    if key not in _SEVERITY:
                        raise _Inexpressible
                    wanted.append(_SEVERITY[key])
                return _apply(op, int(severity), wanted)
            case "app" | "text":
                field = spec.app if part == "app" else spec.message
                if field is None:
                    return False
                return _apply(op, values[field], [_text(x) for x in lits])
            case _:
                raise _Inexpressible


def count_decodes(
    codecs: Mapping[str, Codec],
) -> tuple[Callable[[Frame], Decoded | DecodeError], list[Frame]]:
    """A decoder for a matcher that records every frame it is asked to decode."""
    seen: list[Frame] = []

    def decode(frame: Frame) -> Decoded | DecodeError:
        seen.append(frame)
        return decode_frame(codecs, frame)

    return decode, seen
