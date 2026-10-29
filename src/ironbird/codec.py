"""Encode commands/telemetry and decode them, per target.

``Codec.decode`` is the single decode boundary: it returns a :class:`DecodedPacket` or a typed
:class:`~ironbird.errors.DecodeError` and never raises on malformed input. Message layouts are
compiled once into ``struct.Struct`` segments; decode is ``unpack_from`` on the datagram
with no slicing copies.
"""

from __future__ import annotations

import math
import re
import struct
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Final

from ironbird import header as hdr
from ironbird.ccdd.model import (
    CcddModel,
    FieldDef,
    Message,
    MessageKind,
    TypeClass,
)
from ironbird.errors import Code, DecodeError, IronbirdError
from ironbird.header import Header, HeaderProfile, ProfileKind

_KIND_TO_PROFILE: Final[Mapping[MessageKind, ProfileKind]] = {
    MessageKind.COMMAND: ProfileKind.COMMAND,
    MessageKind.TELEMETRY: ProfileKind.TELEMETRY,
}


@dataclass(frozen=True, slots=True)
class _Segment:
    """A run of fields with one byte order, decoded by one ``struct.Struct``."""

    struct: struct.Struct
    start: int
    indices: tuple[int, ...]  # definition-order field indices, in struct order


@dataclass(frozen=True, slots=True)
class _Layout:
    message: Message
    segments: tuple[_Segment, ...]
    text_indices: tuple[int, ...]  # char[N] fields needing bytes -> str
    identity: bool  # single segment already in definition order, no char fields


class DatagramClass(StrEnum):
    """Result of :meth:`Codec.classify`."""

    MALFORMED = "malformed"  # too short for the header, over the size cap, or no readable MID
    UNKNOWN_MID = "unknown_mid"  # well enough formed, but the MID is not in the CCDD
    KNOWN_MID = "known_mid"  # the MID is in the CCDD (decode may still reject the rest)


@dataclass(frozen=True, slots=True)
class DatagramInfo:
    kind: DatagramClass
    mid: int | None  # the MID when it could be read, even for a MALFORMED datagram


@dataclass(frozen=True, slots=True)
class DecodedPacket:
    message: Message
    header: Header
    values: tuple[object, ...]  # definition order, aligned with message.fields
    size: int

    def as_dict(self) -> dict[str, object]:
        return {f.name: v for f, v in zip(self.message.fields, self.values, strict=True)}


@dataclass(frozen=True, slots=True)
class EncodedPacket:
    data: bytes
    message: Message
    mid: int
    sequence: int
    function_code: int | None


def _compile_layout(message: Message) -> _Layout:
    order = sorted(range(len(message.fields)), key=lambda i: message.fields[i].offset)
    segments: list[_Segment] = []
    run: list[int] = []

    def close_run() -> None:
        if not run:
            return
        first = message.fields[run[0]]
        parts = [first.endian.struct_prefix]
        cursor = first.offset
        for i in run:
            f = message.fields[i]
            if f.offset > cursor:
                parts.append(f"{f.offset - cursor}x")
            parts.append(f.type.struct_code)
            cursor = f.end
        segments.append(_Segment(struct.Struct("".join(parts)), first.offset, tuple(run)))
        run.clear()

    for i in order:
        f = message.fields[i]
        if run and message.fields[run[0]].endian is not f.endian:
            close_run()
        run.append(i)
    close_run()
    text = tuple(i for i, f in enumerate(message.fields) if f.type.type_class is TypeClass.CHAR)
    identity = (
        len(segments) <= 1
        and not text
        and (not segments or segments[0].indices == tuple(range(len(message.fields))))
    )
    return _Layout(message, tuple(segments), text, identity)


def _decode_text(raw: bytes) -> str:
    return raw.split(b"\0", 1)[0].decode("ascii", errors="replace")


# ---------------------------------------------------------------------------------------
# Argument coercion (encode side)
# ---------------------------------------------------------------------------------------


def _arg_error(message: Message, field: FieldDef, why: str) -> IronbirdError:
    return IronbirdError(
        Code.ENC_ARG, f"{message.name}: argument {field.name}: {why} (type {field.type.text})"
    )


_SPELLED_NON_FINITE: Final = re.compile(r"[+-]?(inf|infinity|nan)", re.IGNORECASE)
MAX_ENUM_HINT_NAMES: Final = 16


def _enum_hint(field: FieldDef) -> str:
    """``; names: A, B, ...`` for an enum field, bounded however large the enum is."""
    if not field.enum:
        return ""
    names = list(field.enum)[:MAX_ENUM_HINT_NAMES]
    more = f", ... ({len(field.enum)} in all)" if len(field.enum) > len(names) else ""
    return f"; names: {', '.join(names)}{more}"


def coerce_value(message: Message, field: FieldDef, value: object) -> object:
    """Convert a CLI string (or a typed Python value) into the field's wire value."""
    ftype = field.type
    match ftype.type_class:
        case TypeClass.INT:
            number: int
            if isinstance(value, str):
                if field.enum is not None and value in field.enum:
                    number = field.enum[value]
                else:
                    try:
                        number = int(value, 0)
                    except ValueError:
                        hint = _enum_hint(field)
                        raise _arg_error(
                            message, field, f"{value[:40]!r} is not an integer{hint}"
                        ) from None
            elif isinstance(value, int) and not isinstance(value, bool):
                number = value
            else:
                raise _arg_error(message, field, f"unsupported value {value!r}")
            lo, hi = ftype.bit_range
            if not lo <= number <= hi:
                raise _arg_error(message, field, f"{number} is outside {lo}..{hi}")
            return number
        case TypeClass.FLOAT:
            if isinstance(value, str):
                try:
                    flt = float(value)
                except ValueError:
                    raise _arg_error(message, field, f"{value[:40]!r} is not a number") from None
                if not math.isfinite(flt) and not _SPELLED_NON_FINITE.fullmatch(value.strip()):
                    # Text such as "1e400" parses to inf; only the literal spellings are inf.
                    raise _arg_error(message, field, f"{value[:40]!r} overflows {ftype.text}")
            elif isinstance(value, int | float) and not isinstance(value, bool):
                try:
                    flt = float(value)
                except OverflowError:
                    raise _arg_error(message, field, "integer too large for a float") from None
            else:
                raise _arg_error(message, field, f"unsupported value {value!r}")
            if ftype.size == 4 and math.isfinite(flt):
                try:
                    struct.pack("<f", flt)  # rounds to nearest float32; overflow raises
                except OverflowError:
                    raise _arg_error(message, field, f"{flt} overflows float32") from None
            return flt
        case TypeClass.CHAR:
            if not isinstance(value, str):
                raise _arg_error(message, field, f"unsupported value {value!r}")
            try:
                encoded = value.encode("ascii")
            except UnicodeEncodeError:
                raise _arg_error(message, field, "text must be ASCII") from None
            if len(encoded) > ftype.size:
                raise _arg_error(message, field, f"{len(encoded)} characters exceed {ftype.size}")
            return encoded
        case TypeClass.BYTES:
            if isinstance(value, bytes):
                raw = value
            elif isinstance(value, str):
                try:
                    raw = bytes.fromhex(value)
                except ValueError:
                    raise _arg_error(message, field, "expected hex digits") from None
            else:
                raise _arg_error(message, field, f"unsupported value {value!r}")
            if len(raw) != ftype.size:
                raise _arg_error(
                    message, field, f"needs exactly {ftype.size} bytes, got {len(raw)}"
                )
            return raw
    raise AssertionError(f"unhandled type class {ftype.type_class}")  # pragma: no cover


class Codec:
    """Encode/decode for one target: its CCDD model plus its two header profiles."""

    def __init__(
        self,
        model: CcddModel,
        command_profile: HeaderProfile,
        telemetry_profile: HeaderProfile,
        *,
        max_packet_bytes: int = hdr.DEFAULT_MAX_PACKET_BYTES,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if command_profile.kind is not ProfileKind.COMMAND:
            raise IronbirdError(
                Code.ENC_FIT, f"{command_profile.source}: not a command header profile"
            )
        if telemetry_profile.kind is not ProfileKind.TELEMETRY:
            raise IronbirdError(
                Code.ENC_FIT, f"{telemetry_profile.source}: not a telemetry header profile"
            )
        for profile in (command_profile, telemetry_profile):
            self._validate_profile(profile)
        assert max_packet_bytes > 0
        self.model: CcddModel = model
        self.max_packet_bytes: int = max_packet_bytes
        self._clock = clock
        self._profiles: Mapping[MessageKind, HeaderProfile] = {
            MessageKind.COMMAND: command_profile,
            MessageKind.TELEMETRY: telemetry_profile,
        }
        self._sequence: dict[int, int] = {}
        self._command_mids: frozenset[int] = frozenset(m for m, _ in model.commands_by_key)
        layouts: dict[str, _Layout] = {}
        for msg in model.messages:
            self._validate_fit(msg)
            layouts[msg.name] = _compile_layout(msg)
        self._layouts: Mapping[str, _Layout] = layouts

    @staticmethod
    def _validate_profile(profile: HeaderProfile) -> None:
        """Reject, at load time, profiles the codec cannot drive or decode."""
        if not profile.mid_fields:
            raise IronbirdError(
                Code.HDR_SCHEMA,
                f"{profile.source}: profile {profile.name} defines no mid_fields; "
                "a codec needs them to route packets",
            )
        for f in profile.input_fields:
            if f.value is None and f.name != profile.function_code_field:
                raise IronbirdError(
                    Code.HDR_SCHEMA,
                    f"{profile.source}: input field {f.name} has no default and nothing "
                    "supplies it; add 'default = N'",
                )

    def profile(self, kind: MessageKind) -> HeaderProfile:
        return self._profiles[kind]

    def _validate_fit(self, msg: Message) -> None:
        """Reject, at load time, messages the header profile cannot carry."""
        profile = self._profiles[msg.kind]
        where = f"{self.model.source}: {msg.kind.value} {msg.name}"
        split = hdr.split_mid(profile, msg.mid)
        if isinstance(split, str):
            raise IronbirdError(
                Code.ENC_FIT, f"{where}: MID 0x{msg.mid:04X} {split}", details=profile.source
            )
        total = profile.size_bytes + msg.payload_size
        if total > self.max_packet_bytes:
            raise IronbirdError(
                Code.ENC_FIT,
                f"{where}: packet of {total} bytes exceeds the {self.max_packet_bytes}-byte cap",
            )
        lf = profile.length_field
        if lf is not None and not 0 <= total + lf.bias <= lf.mask:
            raise IronbirdError(
                Code.ENC_FIT,
                f"{where}: packet of {total} bytes does not fit the {lf.bits}-bit "
                f"{lf.name} field of profile {profile.name}",
                details=profile.source,
            )
        if msg.kind is MessageKind.COMMAND:
            assert profile.function_code_field is not None and msg.function_code is not None
            fc_field = profile.by_name[profile.function_code_field]
            if msg.function_code > fc_field.mask:
                raise IronbirdError(
                    Code.ENC_FIT,
                    f"{where}: function code {msg.function_code} does not fit "
                    f"{fc_field.bits}-bit field {fc_field.name}",
                )

    def reset_sequences(self) -> None:
        """Restart every per-MID sequence counter at zero."""
        self._sequence.clear()

    def _next_sequence(self, mid: int) -> int:
        value = self._sequence.get(mid, 0)
        self._sequence[mid] = value + 1
        return value

    # -- encode ----------------------------------------------------------------------

    def _lookup(self, name: str, kind: MessageKind) -> Message:
        msg = self.model.by_name.get(name)
        if msg is None or msg.kind is not kind:
            known = sorted(m.name for m in self.model.messages if m.kind is kind)
            hint = f"; {len(known)} known" if known else ""
            raise IronbirdError(
                Code.ENC_UNKNOWN,
                f"unknown {kind.value} {name[:64]!r} for target {self.model.target}{hint}",
                details="known: " + ", ".join(known),
            )
        return msg

    def _encode_payload(self, msg: Message, args: Mapping[str, object], *, strict: bool) -> bytes:
        names = {f.name for f in msg.fields}
        unexpected = sorted(set(args) - names)
        if unexpected:
            raise IronbirdError(
                Code.ENC_ARGSET,
                f"{msg.name}: unexpected argument(s) {', '.join(n[:32] for n in unexpected)}; "
                f"expected: {', '.join(f.name for f in msg.fields) or 'none'}",
            )
        missing = [f.name for f in msg.fields if f.name not in args]
        if missing and strict:
            raise IronbirdError(
                Code.ENC_ARGSET, f"{msg.name}: missing argument(s) {', '.join(missing)}"
            )
        layout = self._layouts[msg.name]
        values: list[object] = []
        for f in msg.fields:
            if f.name in args:
                values.append(coerce_value(msg, f, args[f.name]))
            else:
                values.append(_zero(f))
        buf = bytearray(msg.payload_size)
        for seg in layout.segments:
            seg.struct.pack_into(buf, seg.start, *(values[i] for i in seg.indices))
        return bytes(buf)

    def _frame(
        self,
        msg: Message,
        payload: bytes,
        inputs: Mapping[str, int],
        tamper: Mapping[str, int] | None,
    ) -> EncodedPacket:
        profile = self._profiles[msg.kind]
        sequence = self._next_sequence(msg.mid)
        data = hdr.pack(
            profile,
            payload,
            mid=msg.mid,
            inputs=inputs,
            sequence=sequence,
            now=self._clock(),
            tamper=tamper,
        )
        assert (
            len(data) <= self.max_packet_bytes
        )  # guaranteed by _validate_fit (+ tamper keeps size)
        return EncodedPacket(data, msg, msg.mid, sequence, msg.function_code)

    def encode_command(
        self,
        name: str,
        args: Mapping[str, object] | None = None,
        *,
        tamper: Mapping[str, int] | None = None,
    ) -> EncodedPacket:
        msg = self._lookup(name, MessageKind.COMMAND)
        payload = self._encode_payload(msg, args or {}, strict=True)
        profile = self._profiles[MessageKind.COMMAND]
        assert profile.function_code_field is not None and msg.function_code is not None
        return self._frame(msg, payload, {profile.function_code_field: msg.function_code}, tamper)

    def encode_telemetry(
        self,
        name: str,
        values: Mapping[str, object] | None = None,
        *,
        tamper: Mapping[str, int] | None = None,
    ) -> EncodedPacket:
        """Encode telemetry; fields not given are zero-filled."""
        msg = self._lookup(name, MessageKind.TELEMETRY)
        payload = self._encode_payload(msg, values or {}, strict=False)
        return self._frame(msg, payload, {}, tamper)

    # -- decode (the boundary) -----------------------------------------------------

    def peek_mid(self, kind: MessageKind, data: bytes | bytearray | memoryview) -> int | None:
        return hdr.peek_mid(self._profiles[kind], data)

    def classify(self, kind: MessageKind, data: bytes | bytearray | memoryview) -> DatagramInfo:
        """Cheaply sort one datagram without decoding it: malformed, unknown MID, or known MID.

        Reads only the MID window, so it is safe on the receive path. ``KNOWN_MID`` means the
        datagram is long enough to hold a header and its MID is in the CCDD; full validation
        (length field, checksum, payload size) is still ``decode``'s job and may reject it.
        """
        profile = self._profiles[kind]
        size = len(data)
        mid = hdr.peek_mid(profile, data)
        if mid is None or size < profile.size_bytes or size > self.max_packet_bytes:
            return DatagramInfo(DatagramClass.MALFORMED, mid)
        known = self.is_known_mid(kind, mid)
        return DatagramInfo(DatagramClass.KNOWN_MID if known else DatagramClass.UNKNOWN_MID, mid)

    def is_known_mid(self, kind: MessageKind, mid: int) -> bool:
        if kind is MessageKind.TELEMETRY:
            return mid in self.model.telemetry_by_mid
        return mid in self._command_mids

    def decode(
        self, kind: MessageKind, data: bytes | bytearray | memoryview
    ) -> DecodedPacket | DecodeError:
        """Parse one whole packet. Returns a result or a typed error; never raises."""
        profile = self._profiles[kind]
        parsed = hdr.unpack(profile, data, max_size=self.max_packet_bytes)
        if isinstance(parsed, DecodeError):
            return parsed
        mid = parsed.mid
        assert mid is not None  # profiles used with a codec always define MID fields
        msg: Message | None
        if kind is MessageKind.COMMAND:
            assert profile.function_code_field is not None
            fc_field = profile.by_name[profile.function_code_field]
            fc = parsed.values[fc_field.name]
            msg = self.model.commands_by_key.get((mid, fc))
            if msg is None:
                known_mid = self.is_known_mid(kind, mid)
                what = (
                    f"function code {fc} for MID 0x{mid:04X}" if known_mid else f"MID 0x{mid:04X}"
                )
                offset = profile.byte_offset(fc_field) if known_mid else 0
                return DecodeError(Code.DEC_UNKNOWN, offset, f"unknown command {what}", mid)
        else:
            msg = self.model.telemetry_by_mid.get(mid)
            if msg is None:
                return DecodeError(Code.DEC_UNKNOWN, 0, f"unknown telemetry MID 0x{mid:04X}", mid)
        size = len(data)
        expected = parsed.size + msg.payload_size
        if size != expected:
            return DecodeError(
                Code.DEC_PAYLOAD,
                parsed.size,
                f"{msg.name} payload is {size - parsed.size} bytes, CCDD says {msg.payload_size}",
                mid,
            )
        layout = self._layouts[msg.name]
        base = parsed.size
        try:
            if layout.identity:
                values: tuple[object, ...] = (
                    layout.segments[0].struct.unpack_from(data, base + layout.segments[0].start)
                    if layout.segments
                    else ()
                )
            else:
                out: list[object] = [None] * len(msg.fields)
                for seg in layout.segments:
                    for i, v in zip(
                        seg.indices, seg.struct.unpack_from(data, base + seg.start), strict=True
                    ):
                        out[i] = v
                for i in layout.text_indices:
                    raw = out[i]
                    assert isinstance(raw, bytes)
                    out[i] = _decode_text(raw)
                values = tuple(out)
        except struct.error as exc:  # pragma: no cover - sizes were validated above
            return DecodeError(Code.DEC_PAYLOAD, base, f"payload unpack failed: {exc}", mid)
        return DecodedPacket(msg, parsed, values, size)


def _zero(field: FieldDef) -> object:
    match field.type.type_class:
        case TypeClass.INT:
            return 0
        case TypeClass.FLOAT:
            return 0.0
        case TypeClass.CHAR | TypeClass.BYTES:
            return b""
    raise AssertionError("unreachable")  # pragma: no cover
