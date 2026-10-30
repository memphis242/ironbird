"""Placeholder JSON adapter (schema ``ironbird-ccdd/0``).

The real CCDD export format is not final; when it is, a second adapter replaces this one
and nothing else changes. See ``examples/ccdd/mock.json`` for the shape.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping
from itertools import pairwise
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final

from ironbird.ccdd.model import (
    MAX_ENUM_ENTRIES,
    MAX_PAYLOAD_BYTES,
    SEVERITY_LABELS,
    Category,
    CcddModel,
    Endian,
    EventSeverity,
    EventSpec,
    FieldDef,
    FieldType,
    Message,
    MessageKind,
    TypeClass,
    parse_type,
)
from ironbird.errors import Code, IronbirdError

SCHEMA_ID: Final = "ironbird-ccdd/0"
MAX_FILE_BYTES: Final = 8 * 1024 * 1024
MAX_MESSAGES: Final = 20_000
MAX_FIELDS: Final = 1024
MAX_TEXT: Final = 1024

_NAME_RE: Final = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,63}")
_ENUM_NAME_RE: Final = re.compile(r"[A-Za-z_][A-Za-z0-9_.\-]{0,63}")
_MID_RE: Final = re.compile(r"0[xX][0-9a-fA-F]{1,8}|[0-9]{1,10}")
MAX_MID: Final = 0xFFFFFFFF


def _no_duplicate_keys(source: str) -> Callable[[list[tuple[str, Any]]], dict[str, Any]]:
    """A ``json`` object hook that rejects repeated keys instead of letting the last win."""

    def hook(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for key, value in pairs:
            if key in out:
                siblings = ", ".join(repr(k[:24]) for k in list(out)[:6])
                raise IronbirdError(
                    Code.CCD_FILE,
                    f"{source}: duplicate key {key[:40]!r} in an object with keys {siblings}",
                )
            out[key] = value
        return out

    return hook


class _Ctx:
    def __init__(self, source: str) -> None:
        self.source = source

    def fail(self, path: str, message: str, code: Code = Code.CCD_SCHEMA) -> IronbirdError:
        return IronbirdError(code, f"{self.source}: {path}: {message}")


def _check_keys(
    ctx: _Ctx, table: Mapping[str, Any], path: str, allowed: set[str], required: set[str]
) -> None:
    for key in table:
        if key not in allowed:
            raise ctx.fail(f"{path}.{key}" if path else key, "unknown key")
    for key in sorted(required - table.keys()):
        raise ctx.fail(f"{path}.{key}" if path else key, "required key is missing")


def _text(ctx: _Ctx, table: Mapping[str, Any], key: str, path: str, default: str = "") -> str:
    value = table.get(key, default)
    if not isinstance(value, str):
        raise ctx.fail(f"{path}.{key}", "must be a string")
    if len(value) > MAX_TEXT:
        raise ctx.fail(f"{path}.{key}", f"longer than {MAX_TEXT} characters")
    return value


def _int(ctx: _Ctx, value: object, path: str, lo: int, hi: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ctx.fail(path, "must be an integer")
    if not lo <= value <= hi:
        raise ctx.fail(path, f"must be in {lo}..{hi}, got {value}")
    return value


def _parse_mid(ctx: _Ctx, value: object, path: str) -> int:
    if isinstance(value, str):
        if not _MID_RE.fullmatch(value):
            raise ctx.fail(path, 'must be a hex string like "0x1805" or a decimal integer')
        number = int(value, 0) if value.lower().startswith("0x") else int(value, 10)
        return _int(ctx, number, path, 0, MAX_MID)
    return _int(ctx, value, path, 0, MAX_MID)


def _parse_enum(ctx: _Ctx, raw: object, path: str, ftype: FieldType) -> Mapping[str, int]:
    if ftype.type_class is not TypeClass.INT:
        raise ctx.fail(path, "enum is only valid on integer fields")
    if not isinstance(raw, dict) or not raw:
        raise ctx.fail(path, "must be a non-empty object of NAME: integer")
    if len(raw) > MAX_ENUM_ENTRIES:
        raise ctx.fail(path, f"more than {MAX_ENUM_ENTRIES} enum entries")
    lo, hi = ftype.bit_range
    out: dict[str, int] = {}
    seen: dict[int, str] = {}
    for label, number in raw.items():
        if not _ENUM_NAME_RE.fullmatch(label):
            raise ctx.fail(f"{path}.{label}", "invalid enum name")
        value = _int(ctx, number, f"{path}.{label}", lo, hi)
        if value in seen:
            raise ctx.fail(
                f"{path}.{label}", f"value {value} duplicates {seen[value]!r}", Code.CCD_LAYOUT
            )
        seen[value] = label
        out[label] = value
    return MappingProxyType(out)


def _parse_field(ctx: _Ctx, raw: object, path: str, next_offset: int, names: set[str]) -> FieldDef:
    if not isinstance(raw, dict):
        raise ctx.fail(path, "must be an object")
    _check_keys(
        ctx,
        raw,
        path,
        {"name", "type", "offset", "endian", "enum", "units", "description"},
        {"name", "type"},
    )
    name = raw["name"]
    if not isinstance(name, str) or not _NAME_RE.fullmatch(name):
        raise ctx.fail(f"{path}.name", "must be an identifier (letters, digits, underscore)")
    if name in names:
        raise ctx.fail(f"{path}.name", f"duplicate field name {name!r}", Code.CCD_LAYOUT)
    names.add(name)
    type_text = raw["type"]
    if not isinstance(type_text, str):
        raise ctx.fail(f"{path}.type", "must be a string")
    parsed = parse_type(type_text)
    if isinstance(parsed, str):
        raise ctx.fail(f"{path}.type", f"{parsed} (got {type_text[:40]!r})")
    offset = next_offset
    if "offset" in raw:
        offset = _int(ctx, raw["offset"], f"{path}.offset", 0, MAX_PAYLOAD_BYTES)
    endian_text = raw.get("endian", "little")
    try:
        endian = Endian(endian_text)
    except ValueError:
        raise ctx.fail(f"{path}.endian", "must be 'little' or 'big'") from None
    enum = _parse_enum(ctx, raw["enum"], f"{path}.enum", parsed) if "enum" in raw else None
    return FieldDef(
        name=name,
        type=parsed,
        offset=offset,
        endian=endian,
        enum=enum,
        units=_text(ctx, raw, "units", path),
        description=_text(ctx, raw, "description", path),
    )


def _parse_category(ctx: _Ctx, raw: Mapping[str, Any], path: str, kind: MessageKind) -> Category:
    """The message's category: ``other`` unless given; ``event`` when an event mapping is."""
    has_event = "event" in raw
    if kind is MessageKind.COMMAND:
        for key in ("category", "event"):
            if key in raw:
                raise ctx.fail(f"{path}.{key}", "only valid on telemetry")
        return Category.OTHER
    if "category" not in raw:
        return Category.EVENT if has_event else Category.OTHER
    text = raw["category"]
    try:
        category = Category(text) if isinstance(text, str) else None
    except ValueError:
        category = None
    if category is None:
        names = ", ".join(c.value for c in Category)
        raise ctx.fail(f"{path}.category", f"must be one of: {names} (got {str(text)[:40]!r})")
    if has_event and category is not Category.EVENT:
        raise ctx.fail(f"{path}.event", f"only valid with category 'event', not {category.value!r}")
    if category is Category.EVENT and not has_event:
        raise ctx.fail(
            f"{path}.event", "required for category 'event' (names the app/id/type/message fields)"
        )
    return category


def _event_field(
    ctx: _Ctx,
    table: Mapping[str, Any],
    key: str,
    path: str,
    fields: tuple[FieldDef, ...],
    wanted: TypeClass,
    required: bool,
) -> FieldDef | None:
    if key not in table:
        if required:
            raise ctx.fail(f"{path}.{key}", "required key is missing")
        return None
    name = table[key]
    if not isinstance(name, str):
        raise ctx.fail(f"{path}.{key}", "must be a field name")
    fdef = next((f for f in fields if f.name == name), None)
    if fdef is None:
        raise ctx.fail(f"{path}.{key}", f"no field named {name[:40]!r} in this message")
    if fdef.type.type_class is not wanted:
        raise ctx.fail(
            f"{path}.{key}", f"field {name} must be of {wanted.value} type, not {fdef.type.text}"
        )
    return fdef


def _parse_event(ctx: _Ctx, raw: object, path: str, fields: tuple[FieldDef, ...]) -> EventSpec:
    if not isinstance(raw, dict):
        raise ctx.fail(path, 'must be an object like {"app": ..., "id": ..., "type": ...}')
    _check_keys(ctx, raw, path, {"app", "id", "type", "message"}, {"id", "type"})
    id_field = _event_field(ctx, raw, "id", path, fields, TypeClass.INT, True)
    type_field = _event_field(ctx, raw, "type", path, fields, TypeClass.INT, True)
    app_field = _event_field(ctx, raw, "app", path, fields, TypeClass.CHAR, False)
    message_field = _event_field(ctx, raw, "message", path, fields, TypeClass.CHAR, False)
    assert id_field is not None and type_field is not None
    if type_field.enum is None:
        raise ctx.fail(
            f"{path}.type", f"field {type_field.name} needs an enum naming the event types"
        )
    severities: dict[int, EventSeverity] = {}
    for label, number in type_field.enum.items():
        severity = SEVERITY_LABELS.get(label.upper())
        if severity is None:
            known = ", ".join(sorted(SEVERITY_LABELS))
            raise ctx.fail(
                f"{path}.type", f"enum label {label!r} is not an event type; use one of: {known}"
            )
        severities[number] = severity
    return EventSpec(
        id=id_field.name,
        type=type_field.name,
        app=app_field.name if app_field else None,
        message=message_field.name if message_field else None,
        severities=MappingProxyType(severities),
    )


def _parse_message(ctx: _Ctx, raw: object, index: int) -> Message:
    path = f"messages[{index}]"
    if not isinstance(raw, dict):
        raise ctx.fail(path, "must be an object")
    label = raw.get("name")
    if isinstance(label, str) and _NAME_RE.fullmatch(label):
        path = f"messages[{index}]({label})"
    _check_keys(
        ctx,
        raw,
        path,
        {
            "name",
            "kind",
            "mid",
            "function_code",
            "description",
            "fields",
            "payload_size",
            "category",
            "event",
        },
        {"name", "kind", "mid"},
    )
    name = raw["name"]
    if not isinstance(name, str) or not _NAME_RE.fullmatch(name):
        raise ctx.fail(f"{path}.name", "must be an identifier (letters, digits, underscore)")
    try:
        kind = MessageKind(raw["kind"])
    except ValueError:
        raise ctx.fail(f"{path}.kind", "must be 'command' or 'telemetry'") from None
    mid = _parse_mid(ctx, raw["mid"], f"{path}.mid")
    function_code: int | None = None
    if kind is MessageKind.COMMAND:
        if "function_code" not in raw:
            raise ctx.fail(f"{path}.function_code", "required for commands")
        function_code = _int(ctx, raw["function_code"], f"{path}.function_code", 0, 255)
    elif "function_code" in raw:
        raise ctx.fail(f"{path}.function_code", "only valid on commands")

    raw_fields = raw.get("fields", [])
    if not isinstance(raw_fields, list):
        raise ctx.fail(f"{path}.fields", "must be an array")
    if len(raw_fields) > MAX_FIELDS:
        raise ctx.fail(f"{path}.fields", f"more than {MAX_FIELDS} fields")
    fields: list[FieldDef] = []
    names: set[str] = set()
    next_offset = 0
    for i, rf in enumerate(raw_fields):
        fdef = _parse_field(ctx, rf, f"{path}.fields[{i}]", next_offset, names)
        fields.append(fdef)
        next_offset = fdef.end

    # Layout: no overlaps, bounded size.
    ordered = sorted(fields, key=lambda f: f.offset)
    for prev, cur in pairwise(ordered):
        if cur.offset < prev.end:
            raise ctx.fail(
                path,
                f"fields {prev.name} (bytes {prev.offset}..{prev.end - 1}) and {cur.name} "
                f"(starting at byte {cur.offset}) overlap",
                Code.CCD_LAYOUT,
            )
    extent = max((f.end for f in fields), default=0)
    payload_size = extent
    if "payload_size" in raw:
        payload_size = _int(ctx, raw["payload_size"], f"{path}.payload_size", 0, MAX_PAYLOAD_BYTES)
        if payload_size < extent:
            raise ctx.fail(
                f"{path}.payload_size",
                f"{payload_size} is smaller than the fields' extent ({extent})",
                Code.CCD_LAYOUT,
            )
    if payload_size > MAX_PAYLOAD_BYTES:
        raise ctx.fail(path, f"payload larger than {MAX_PAYLOAD_BYTES} bytes", Code.CCD_LAYOUT)
    category = _parse_category(ctx, raw, path, kind)
    event = (
        _parse_event(ctx, raw["event"], f"{path}.event", tuple(fields))
        if category is Category.EVENT
        else None
    )
    return Message(
        name=name,
        kind=kind,
        mid=mid,
        function_code=function_code,
        description=_text(ctx, raw, "description", path),
        fields=tuple(fields),
        payload_size=payload_size,
        category=category,
        event=event,
    )


def parse_model(data: object, source: str) -> CcddModel:
    """Validate an already-parsed JSON value and build the model."""
    ctx = _Ctx(source)
    if not isinstance(data, dict):
        raise ctx.fail("<root>", "must be an object")
    _check_keys(ctx, data, "", {"schema", "target", "messages"}, {"schema", "target", "messages"})
    if data["schema"] != SCHEMA_ID:
        raise ctx.fail("schema", f"must be {SCHEMA_ID!r}, got {str(data['schema'])[:40]!r}")
    target = data["target"]
    if not isinstance(target, str) or not _NAME_RE.fullmatch(target):
        raise ctx.fail("target", "must be an identifier")
    raw_messages = data["messages"]
    if not isinstance(raw_messages, list):
        raise ctx.fail("messages", "must be an array")
    if len(raw_messages) > MAX_MESSAGES:
        raise ctx.fail("messages", f"more than {MAX_MESSAGES} messages")
    messages = tuple(_parse_message(ctx, raw, i) for i, raw in enumerate(raw_messages))
    return CcddModel.build(target, source, messages)


class JsonAdapter:
    """Loads ``ironbird-ccdd/0`` JSON files."""

    name: str = "json"

    def load(self, path: Path) -> CcddModel:
        try:
            with path.open("rb") as handle:
                raw = handle.read(MAX_FILE_BYTES + 1)
        except OSError as exc:
            raise IronbirdError(
                Code.CCD_FILE, f"{path}: cannot read CCDD file ({exc.strerror or exc})", cause=exc
            ) from exc
        return self.load_bytes(raw, str(path))

    def load_bytes(self, raw: bytes, source: str) -> CcddModel:
        """The model of a file already read (``source`` labels errors). The size, JSON and schema
        rules are the ones :meth:`load` applies to a file on disk."""
        if len(raw) > MAX_FILE_BYTES:
            raise IronbirdError(Code.CCD_FILE, f"{source}: CCDD file larger than 8 MiB")
        try:
            data = json.loads(raw.decode("utf-8"), object_pairs_hook=_no_duplicate_keys(source))
        except (ValueError, RecursionError) as exc:
            # json.JSONDecodeError and UnicodeDecodeError are both ValueError subclasses.
            raise IronbirdError(
                Code.CCD_FILE, f"{source}: invalid JSON ({exc})", cause=exc
            ) from exc
        return parse_model(data, source)
