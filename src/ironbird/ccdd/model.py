"""Immutable in-memory model of a target's command and telemetry definitions."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import IntEnum, StrEnum
from types import MappingProxyType
from typing import Final

from ironbird.errors import Code, IronbirdError


class MessageKind(StrEnum):
    COMMAND = "command"
    TELEMETRY = "telemetry"


class Category(StrEnum):
    """What a telemetry message is for. The declaration order is the importance order.

    ``priority`` (1 = most important) drives default sorting, emphasis, filter presets and
    alert prioritisation in every view. Commands have no category.
    """

    EVENT = "event"
    COMMAND_RESPONSE = "command_response"
    HOUSEKEEPING = "housekeeping"
    STATUS = "status"
    COMMANDED = "commanded"  # sent in response to a request, e.g. diagnostics and dumps
    OTHER = "other"

    @property
    def priority(self) -> int:
        return _PRIORITY[self]

    @classmethod
    def by_priority(cls) -> tuple[Category, ...]:
        return tuple(cls)


_PRIORITY: Final[Mapping[Category, int]] = MappingProxyType(
    {category: rank for rank, category in enumerate(Category, start=1)}
)
assert _PRIORITY[Category.EVENT] == 1 and _PRIORITY[Category.OTHER] == len(Category)


class EventSeverity(IntEnum):
    """Normalised event type; higher is more severe, so ``>=`` filters by minimum severity."""

    DEBUG = 1
    INFO = 2
    WARNING = 3
    ERROR = 4
    CRITICAL = 5


# Spellings projects use for an event type enum label, case-insensitive.
SEVERITY_LABELS: Final[Mapping[str, EventSeverity]] = MappingProxyType(
    {
        "DEBUG": EventSeverity.DEBUG,
        "INFO": EventSeverity.INFO,
        "INFORMATION": EventSeverity.INFO,
        "WARN": EventSeverity.WARNING,
        "WARNING": EventSeverity.WARNING,
        "ERROR": EventSeverity.ERROR,
        "CRITICAL": EventSeverity.CRITICAL,
    }
)


class Endian(StrEnum):
    LITTLE = "little"
    BIG = "big"

    @property
    def struct_prefix(self) -> str:
        return "<" if self is Endian.LITTLE else ">"


class TypeClass(StrEnum):
    INT = "int"
    FLOAT = "float"
    CHAR = "char"
    BYTES = "bytes"


@dataclass(frozen=True, slots=True)
class FieldType:
    """A wire type: scalar (fixed width) or fixed-length char/bytes array."""

    text: str  # canonical spelling, e.g. "uint16" or "char[16]"
    type_class: TypeClass
    size: int  # bytes on the wire
    struct_code: str  # one struct format item, e.g. "H" or "16s"
    signed: bool = False

    @property
    def bit_range(self) -> tuple[int, int]:
        assert self.type_class is TypeClass.INT
        bits = self.size * 8
        return (-(1 << (bits - 1)), (1 << (bits - 1)) - 1) if self.signed else (0, (1 << bits) - 1)


MAX_ARRAY_LEN: Final = 4096
MAX_ENUM_ENTRIES: Final = 65536
MAX_PAYLOAD_BYTES: Final = 65535

_SCALARS: Final[Mapping[str, FieldType]] = MappingProxyType(
    {
        "uint8": FieldType("uint8", TypeClass.INT, 1, "B"),
        "uint16": FieldType("uint16", TypeClass.INT, 2, "H"),
        "uint32": FieldType("uint32", TypeClass.INT, 4, "I"),
        "uint64": FieldType("uint64", TypeClass.INT, 8, "Q"),
        "int8": FieldType("int8", TypeClass.INT, 1, "b", signed=True),
        "int16": FieldType("int16", TypeClass.INT, 2, "h", signed=True),
        "int32": FieldType("int32", TypeClass.INT, 4, "i", signed=True),
        "int64": FieldType("int64", TypeClass.INT, 8, "q", signed=True),
        "float32": FieldType("float32", TypeClass.FLOAT, 4, "f"),
        "float64": FieldType("float64", TypeClass.FLOAT, 8, "d"),
    }
)
_ARRAY_RE: Final = re.compile(r"(char|bytes)\[([0-9]{1,5})\]")


def parse_type(text: str) -> FieldType | str:
    """Parse a type spelling; returns the type or a reason string."""
    scalar = _SCALARS.get(text)
    if scalar is not None:
        return scalar
    match = _ARRAY_RE.fullmatch(text)
    if match is None:
        return "unknown type; use uint8/16/32/64, int8/16/32/64, float32/64, char[N] or bytes[N]"
    length = int(match.group(2))
    if not 1 <= length <= MAX_ARRAY_LEN:
        return f"array length must be 1..{MAX_ARRAY_LEN}"
    kind = TypeClass.CHAR if match.group(1) == "char" else TypeClass.BYTES
    return FieldType(f"{match.group(1)}[{length}]", kind, length, f"{length}s")


@dataclass(frozen=True, slots=True)
class FieldDef:
    name: str
    type: FieldType
    offset: int  # bytes from payload start
    endian: Endian = Endian.LITTLE
    enum: Mapping[str, int] | None = None
    units: str = ""
    description: str = ""
    # value -> label, built once from ``enum`` so lookups are O(1)
    enum_by_value: Mapping[int, str] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        by_value: dict[int, str] = {}
        if self.enum is not None:
            assert len(self.enum) <= MAX_ENUM_ENTRIES
            by_value = {number: label for label, number in self.enum.items()}
            assert len(by_value) == len(self.enum), "enum values must be unique"
        object.__setattr__(self, "enum_by_value", MappingProxyType(by_value))

    @property
    def end(self) -> int:
        return self.offset + self.type.size

    def enum_label(self, value: object) -> str | None:
        """Name for ``value`` if this field has an enum containing it."""
        if self.enum is None or not isinstance(value, int) or isinstance(value, bool):
            return None
        return self.enum_by_value.get(value)


@dataclass(frozen=True, slots=True)
class EventSpec:
    """Where an event message keeps the parts every event view shows.

    Names refer to fields of the same message. ``id`` and ``type`` are integer fields,
    ``app`` and ``message`` are char fields. ``type`` has an enum whose labels all map to an
    :class:`EventSeverity`; ``severities`` is that mapping by wire value.
    """

    id: str
    type: str
    app: str | None = None
    message: str | None = None
    severities: Mapping[int, EventSeverity] = field(default_factory=lambda: MappingProxyType({}))


@dataclass(frozen=True, slots=True)
class Message:
    name: str
    kind: MessageKind
    mid: int
    function_code: int | None
    description: str
    fields: tuple[FieldDef, ...]
    payload_size: int
    category: Category = Category.OTHER  # telemetry only
    event: EventSpec | None = None  # set exactly when category is EVENT

    def __post_init__(self) -> None:
        assert (self.function_code is not None) == (self.kind is MessageKind.COMMAND)
        assert self.payload_size >= max((f.end for f in self.fields), default=0)
        assert (self.event is not None) == (self.category is Category.EVENT)
        assert self.kind is MessageKind.TELEMETRY or self.category is Category.OTHER
        if self.event is not None:
            names = {f.name for f in self.fields}
            refs = (self.event.id, self.event.type, self.event.app, self.event.message)
            assert all(ref in names for ref in refs if ref is not None)

    def field(self, name: str) -> FieldDef | None:
        for f in self.fields:
            if f.name == name:
                return f
        return None


@dataclass(frozen=True, slots=True)
class CcddModel:
    """All messages of one target. Lookups are by name, (MID, function code) or MID."""

    target: str
    source: str
    messages: tuple[Message, ...]
    by_name: Mapping[str, Message]
    commands_by_key: Mapping[tuple[int, int], Message]
    telemetry_by_mid: Mapping[int, Message]

    @staticmethod
    def build(target: str, source: str, messages: tuple[Message, ...]) -> CcddModel:
        """Index messages, enforcing uniqueness (coded errors name both offenders)."""
        by_name: dict[str, Message] = {}
        commands: dict[tuple[int, int], Message] = {}
        telemetry: dict[int, Message] = {}
        mid_kind: dict[int, Message] = {}
        for msg in messages:
            if msg.name in by_name:
                raise IronbirdError(
                    Code.CCD_LAYOUT, f"{source}: duplicate message name {msg.name!r}"
                )
            by_name[msg.name] = msg
            other = mid_kind.get(msg.mid)
            if other is not None and other.kind is not msg.kind:
                raise IronbirdError(
                    Code.CCD_LAYOUT,
                    f"{source}: MID 0x{msg.mid:04X} used by both {other.kind.value} "
                    f"{other.name} and {msg.kind.value} {msg.name}",
                )
            mid_kind[msg.mid] = msg
            if msg.kind is MessageKind.COMMAND:
                assert msg.function_code is not None
                key = (msg.mid, msg.function_code)
                if key in commands:
                    raise IronbirdError(
                        Code.CCD_LAYOUT,
                        f"{source}: commands {commands[key].name} and {msg.name} share MID "
                        f"0x{msg.mid:04X} and function code {msg.function_code}",
                    )
                commands[key] = msg
            else:
                if msg.mid in telemetry:
                    raise IronbirdError(
                        Code.CCD_LAYOUT,
                        f"{source}: telemetry {telemetry[msg.mid].name} and {msg.name} share "
                        f"MID 0x{msg.mid:04X}",
                    )
                telemetry[msg.mid] = msg
        return CcddModel(
            target=target,
            source=source,
            messages=messages,
            by_name=MappingProxyType(by_name),
            commands_by_key=MappingProxyType(commands),
            telemetry_by_mid=MappingProxyType(telemetry),
        )

    def commands(self) -> tuple[Message, ...]:
        return tuple(m for m in self.messages if m.kind is MessageKind.COMMAND)

    def telemetry(self) -> tuple[Message, ...]:
        return tuple(m for m in self.messages if m.kind is MessageKind.TELEMETRY)

    def event_messages(self) -> tuple[Message, ...]:
        return tuple(m for m in self.messages if m.category is Category.EVENT)
