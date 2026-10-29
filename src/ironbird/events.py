"""Events: the special telemetry every view singles out.

An event message names, in the CCDD, which of its fields carry the app, id, type and text
(``Message.event``), so events look the same whatever the project's wire format. Extraction
keeps the decoded values as they are; sanitizing happens where text is displayed (``render``).
"""

from __future__ import annotations

from dataclasses import dataclass

from ironbird.ccdd.model import EventSeverity
from ironbird.codec import DecodedPacket


@dataclass(frozen=True, slots=True)
class Event:
    time_ns: int
    node: str  # node whose CCDD decoded the packet
    mid: int
    name: str  # CCDD message name
    sequence: int | None
    app: str  # empty when the message has no app field
    event_id: int
    type_value: int
    type_label: str  # enum label of the type, empty when the value has none
    severity: EventSeverity | None  # None when the type value is not in the CCDD enum
    text: str  # empty when the message has no text field


def extract_event(node: str, time_ns: int, packet: DecodedPacket) -> Event | None:
    """The event in ``packet``, or ``None`` when its message is not an event message."""
    spec = packet.message.event
    if spec is None:
        return None
    values = packet.as_dict()
    event_id, type_value = values[spec.id], values[spec.type]
    assert isinstance(event_id, int) and isinstance(type_value, int)  # validated by the CCDD
    app = values[spec.app] if spec.app is not None else ""
    text = values[spec.message] if spec.message is not None else ""
    assert isinstance(app, str) and isinstance(text, str)
    type_field = packet.message.field(spec.type)
    assert type_field is not None
    return Event(
        time_ns=time_ns,
        node=node,
        mid=packet.message.mid,
        name=packet.message.name,
        sequence=packet.header.sequence,
        app=app,
        event_id=event_id,
        type_value=type_value,
        type_label=type_field.enum_label(type_value) or "",
        severity=spec.severities.get(type_value),
        text=text,
    )
