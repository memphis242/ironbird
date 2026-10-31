"""Placeholder JSON adapter (schemas ``ironbird-ccdd/0`` and ``ironbird-ccdd/1``).

``/1`` adds variable-length fields (``count``/``length``/``max``/``min`` on a ``T[]`` field);
``/0`` files load as before and may not use them.

The real CCDD export format is not final; when it is, a second adapter replaces this one
and nothing else changes. See ``examples/ccdd/mock.json`` for the shape.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping
from dataclasses import replace
from itertools import pairwise
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final

from ironbird.ccdd.model import (
    MAX_ENUM_ENTRIES,
    MAX_PAYLOAD_BYTES,
    MAX_VAR_FIELDS,
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
    VarSpec,
    VarUnit,
    parse_element_type,
    parse_type,
)
from ironbird.errors import Code, IronbirdError
from ironbird.fsops import read_capped

SCHEMA_ID: Final = "ironbird-ccdd/1"  # the schema new files are written in
SCHEMA_IDS: Final = ("ironbird-ccdd/0", SCHEMA_ID)  # every schema that loads
VARIABLE_SCHEMA: Final = SCHEMA_ID  # the only schema that may use variable-length fields
REMAINING: Final = "remaining"  # a ``count``/``length`` value: the rest of the packet
MAX_FILE_BYTES: Final = 8 * 1024 * 1024
MAX_MESSAGES: Final = 20_000
MAX_FIELDS: Final = 1024
MAX_TEXT: Final = 1024

_NAME_RE: Final = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,63}")
_ENUM_NAME_RE: Final = re.compile(r"[A-Za-z_][A-Za-z0-9_.\-]{0,63}")
_MID_RE: Final = re.compile(r"0[xX][0-9a-fA-F]{1,8}|[0-9]{1,10}")
MAX_MID: Final = 0xFFFFFFFF
MAX_HEADER_VALUES: Final = 64
MAX_HEADER_VALUE: Final = (1 << 64) - 1  # the widest header field


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
    def __init__(self, source: str, variable: bool = True) -> None:
        self.source = source
        self.variable = variable  # whether the file's schema allows variable-length fields

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


def _parse_header_values(ctx: _Ctx, raw: object, path: str) -> Mapping[str, int]:
    """``"header": {"field": N}``: values for the header profile's input fields. Which fields
    exist and what fits them is the header profile's to say, so the codec checks that."""
    if not isinstance(raw, dict):
        raise ctx.fail(path, "must be an object of header field name: integer")
    if len(raw) > MAX_HEADER_VALUES:
        raise ctx.fail(path, f"more than {MAX_HEADER_VALUES} header values")
    out: dict[str, int] = {}
    for key, value in raw.items():
        if not _NAME_RE.fullmatch(key):
            raise ctx.fail(f"{path}.{key[:40]}", "must be a header field name (an identifier)")
        out[key] = _int(ctx, value, f"{path}.{key}", 0, MAX_HEADER_VALUE)
    return MappingProxyType(out)


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


def _parse_var_field(
    ctx: _Ctx, raw: dict[str, Any], path: str, name: str, type_text: str, fixed: list[FieldDef]
) -> FieldDef:
    """A variable-length field (``type`` ends in ``[]``). ``fixed`` are the fixed fields declared
    before it; its offset is set once the message's fixed fields are all known."""
    if not ctx.variable:
        raise ctx.fail(
            f"{path}.type",
            f"variable-length types need schema {VARIABLE_SCHEMA!r} (got {type_text[:40]!r})",
        )
    _check_keys(
        ctx,
        raw,
        path,
        {"name", "type", "count", "length", "max", "min", "endian", "units", "description"},
        {"name", "type", "max"},
    )
    element = parse_element_type(type_text)
    if isinstance(element, str):
        raise ctx.fail(f"{path}.type", f"{element} (got {type_text[:40]!r})")
    if ("count" in raw) == ("length" in raw):
        raise ctx.fail(path, "needs exactly one of 'count' (elements) or 'length' (bytes)")
    key = "count" if "count" in raw else "length"
    target = raw[key]
    if not isinstance(target, str):
        raise ctx.fail(f"{path}.{key}", f'must be a field name or "{REMAINING}"')
    maximum = _int(ctx, raw["max"], f"{path}.max", 1, MAX_PAYLOAD_BYTES)
    minimum = _int(ctx, raw["min"], f"{path}.min", 0, maximum) if "min" in raw else 0
    if maximum * element.size > MAX_PAYLOAD_BYTES:
        raise ctx.fail(
            f"{path}.max",
            f"{maximum} x {element.size} bytes is larger than {MAX_PAYLOAD_BYTES} bytes",
            Code.CCD_LAYOUT,
        )
    count_field: str | None = None
    unit = VarUnit.REMAINING
    if target != REMAINING:
        unit = VarUnit.COUNT if key == "count" else VarUnit.LENGTH
        count_field = target
        holder = next((f for f in fixed if f.name == target), None)
        if holder is None:
            raise ctx.fail(
                f"{path}.{key}",
                f"no fixed field named {target[:40]!r} before this field; the count or length "
                "field must be declared before the field it sizes",
                Code.CCD_LAYOUT,
            )
        if holder.type.type_class is not TypeClass.INT or holder.type.signed:
            raise ctx.fail(
                f"{path}.{key}",
                f"{target} must be an unsigned integer field, not {holder.type.text}",
                Code.CCD_LAYOUT,
            )
        spec_max = maximum if unit is VarUnit.COUNT else maximum * element.size
        if spec_max > holder.type.bit_range[1]:
            raise ctx.fail(
                f"{path}.max",
                f"{spec_max} {'elements' if unit is VarUnit.COUNT else 'bytes'} cannot be "
                f"expressed by the {holder.type.text} field {target}",
                Code.CCD_LAYOUT,
            )
    endian_text = raw.get("endian", "little")
    try:
        endian = Endian(endian_text)
    except ValueError:
        raise ctx.fail(f"{path}.endian", "must be 'little' or 'big'") from None
    spec = VarSpec(element, unit, count_field, minimum, maximum)
    return FieldDef(
        name=name,
        type=spec.var_type(),
        offset=0,
        endian=endian,
        units=_text(ctx, raw, "units", path),
        description=_text(ctx, raw, "description", path),
        var=spec,
    )


def _parse_field(
    ctx: _Ctx, raw: object, path: str, next_offset: int, names: set[str], fixed: list[FieldDef]
) -> FieldDef:
    """One field. ``fixed`` are the fixed fields declared so far (a variable-length field names
    one as its count)."""
    if not isinstance(raw, dict):
        raise ctx.fail(path, "must be an object")
    name = raw.get("name")
    type_text = raw.get("type")
    is_var = isinstance(type_text, str) and type_text.endswith("[]")
    if not is_var:
        _check_keys(
            ctx,
            raw,
            path,
            {"name", "type", "offset", "endian", "enum", "units", "description"},
            {"name", "type"},
        )
    if not isinstance(name, str) or not _NAME_RE.fullmatch(name):
        raise ctx.fail(f"{path}.name", "must be an identifier (letters, digits, underscore)")
    if name in names:
        raise ctx.fail(f"{path}.name", f"duplicate field name {name!r}", Code.CCD_LAYOUT)
    names.add(name)
    if not isinstance(type_text, str):
        raise ctx.fail(f"{path}.type", "must be a string")
    if is_var:
        return _parse_var_field(ctx, raw, path, name, type_text, fixed)
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


def _parse_message_fields(
    ctx: _Ctx, raw_fields: list[Any], path: str
) -> tuple[list[FieldDef], list[FieldDef]]:
    """The fixed and the variable-length fields of a message, in declaration order. Fixed fields
    come first; a variable-length field's count or length field is one of them."""
    fixed: list[FieldDef] = []
    variable: list[FieldDef] = []
    names: set[str] = set()
    next_offset = 0
    for i, rf in enumerate(raw_fields):
        where = f"{path}.fields[{i}]"
        fdef = _parse_field(ctx, rf, where, next_offset, names, fixed)
        if fdef.var is None:
            if variable:
                raise ctx.fail(
                    where,
                    f"fixed field {fdef.name} follows the variable-length field "
                    f"{variable[0].name}; declare every fixed field first",
                    Code.CCD_LAYOUT,
                )
            fixed.append(fdef)
            next_offset = fdef.end
            continue
        if len(variable) >= MAX_VAR_FIELDS:
            raise ctx.fail(
                where, f"more than {MAX_VAR_FIELDS} variable-length fields", Code.CCD_LAYOUT
            )
        variable.append(fdef)
    return fixed, variable


def _variable_payload(
    ctx: _Ctx,
    raw: Mapping[str, Any],
    path: str,
    extent: int,
    fixed: list[FieldDef],
    variable: list[FieldDef],
) -> int:
    """The largest payload of a message with variable-length fields, after checking how its
    fields share count fields and where ``remaining`` may stand. ``min_payload`` and
    ``max_payload`` may be given as a check against the values the fields imply."""
    specs = [f.var for f in variable if f.var is not None]
    assert len(specs) == len(variable)
    if any(f.name == REMAINING for f in fixed):
        pairs = zip(variable, specs, strict=True)
        user = next((f for f, spec in pairs if spec.unit is VarUnit.REMAINING), None)
        if user is not None:
            raise ctx.fail(
                path,
                f'{user.name} uses "{REMAINING}" as the rest of the packet, but a fixed field '
                f'is named "{REMAINING}" too; rename that field',
                Code.CCD_LAYOUT,
            )
    users: dict[str, str] = {}
    for i, (fdef, spec) in enumerate(zip(variable, specs, strict=True)):
        if spec.unit is VarUnit.REMAINING and i != len(variable) - 1:
            raise ctx.fail(
                path,
                f'"{REMAINING}" is only allowed on the last field, not {fdef.name}',
                Code.CCD_LAYOUT,
            )
        if spec.count_field is not None:
            if spec.count_field in users:
                raise ctx.fail(
                    path,
                    f"{fdef.name} and {users[spec.count_field]} share the count field "
                    f"{spec.count_field}",
                    Code.CCD_LAYOUT,
                )
            users[spec.count_field] = fdef.name
    smallest = extent + sum(spec.min_bytes for spec in specs)
    largest = extent + sum(spec.max_bytes for spec in specs)
    if "payload_size" in raw:
        raise ctx.fail(
            f"{path}.payload_size",
            "not valid with variable-length fields; the size is implied by their min and max",
            Code.CCD_LAYOUT,
        )
    for key, implied in (("min_payload", smallest), ("max_payload", largest)):
        if key in raw:
            given = _int(ctx, raw[key], f"{path}.{key}", 0, MAX_PAYLOAD_BYTES)
            if given != implied:
                raise ctx.fail(
                    f"{path}.{key}",
                    f"{given} disagrees with the {implied} bytes the fields imply",
                    Code.CCD_LAYOUT,
                )
    return largest


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
            "header",
            *(("min_payload", "max_payload") if ctx.variable else ()),
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
    fixed, variable = _parse_message_fields(ctx, raw_fields, path)
    # Layout: no overlaps among the fixed fields, bounded size.
    ordered = sorted(fixed, key=lambda f: f.offset)
    for prev, cur in pairwise(ordered):
        if cur.offset < prev.end:
            raise ctx.fail(
                path,
                f"fields {prev.name} (bytes {prev.offset}..{prev.end - 1}) and {cur.name} "
                f"(starting at byte {cur.offset}) overlap",
                Code.CCD_LAYOUT,
            )
    extent = max((f.end for f in fixed), default=0)
    if variable:
        variable = [replace(f, offset=extent) for f in variable]
        payload_size = _variable_payload(ctx, raw, path, extent, fixed, variable)
    else:
        for key in ("min_payload", "max_payload"):
            if key in raw:
                raise ctx.fail(
                    f"{path}.{key}", "only valid on messages with variable-length fields"
                )
        payload_size = extent
        if "payload_size" in raw:
            payload_size = _int(
                ctx, raw["payload_size"], f"{path}.payload_size", 0, MAX_PAYLOAD_BYTES
            )
            if payload_size < extent:
                raise ctx.fail(
                    f"{path}.payload_size",
                    f"{payload_size} is smaller than the fields' extent ({extent})",
                    Code.CCD_LAYOUT,
                )
    if payload_size > MAX_PAYLOAD_BYTES:
        raise ctx.fail(path, f"payload larger than {MAX_PAYLOAD_BYTES} bytes", Code.CCD_LAYOUT)
    fields = [*fixed, *variable]
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
        header=_parse_header_values(ctx, raw["header"], f"{path}.header")
        if "header" in raw
        else MappingProxyType({}),
    )


def parse_model(data: object, source: str) -> CcddModel:
    """Validate an already-parsed JSON value and build the model."""
    ctx = _Ctx(source)
    if not isinstance(data, dict):
        raise ctx.fail("<root>", "must be an object")
    _check_keys(ctx, data, "", {"schema", "target", "messages"}, {"schema", "target", "messages"})
    if data["schema"] not in SCHEMA_IDS:
        known = " or ".join(repr(known) for known in SCHEMA_IDS)
        raise ctx.fail("schema", f"must be {known}, got {str(data['schema'])[:40]!r}")
    ctx.variable = data["schema"] == VARIABLE_SCHEMA
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
    """Loads ``ironbird-ccdd/0`` and ``ironbird-ccdd/1`` JSON files."""

    name: str = "json"

    def read(self, path: Path) -> bytes:
        """The content of the CCDD file ``path``, at most one byte past the size limit."""
        try:
            return read_capped(path, MAX_FILE_BYTES)
        except OSError as exc:
            raise IronbirdError(
                Code.CCD_FILE, f"{path}: cannot read CCDD file ({exc.strerror or exc})", cause=exc
            ) from exc

    def load(self, path: Path) -> CcddModel:
        return self.load_bytes(self.read(path), str(path))

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
