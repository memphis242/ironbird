"""CCDD command/telemetry definitions: model, adapters, loader."""

from ironbird.ccdd.loader import ADAPTERS, Adapter, load_ccdd
from ironbird.ccdd.model import (
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
)

__all__ = [
    "ADAPTERS",
    "Adapter",
    "Category",
    "CcddModel",
    "Endian",
    "EventSeverity",
    "EventSpec",
    "FieldDef",
    "FieldType",
    "Message",
    "MessageKind",
    "TypeClass",
    "load_ccdd",
]
