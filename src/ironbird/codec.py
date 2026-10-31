"""Encode commands/telemetry and decode them, per target.

The decode boundary has two halves. :meth:`Codec.check` is the cheap one: size bounds, the MID
known, the header (length field, fixed fields, checksum), the function code and the payload
size, with no field unpacking; the engine runs it on every frame at ingest. :meth:`Codec.decode`
runs ``check`` first and then unpacks the fields. Both return a typed
:class:`~ironbird.errors.DecodeError` and never raise on malformed input. Message layouts are
compiled once into ``struct.Struct`` segments; decode is ``unpack_from`` on the datagram
with no slicing copies.
"""

from __future__ import annotations

import math
import re
import struct
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import Final, NoReturn, cast, final

from ironbird import header as hdr
from ironbird.ccdd.model import (
    CcddModel,
    FieldDef,
    Message,
    MessageKind,
    TypeClass,
    VarSpec,
    VarUnit,
)
from ironbird.errors import Code, DecodeError, IronbirdError
from ironbird.header import Header, HeaderProfile, ProfileKind

MAX_MID_BITS: Final = 32  # a frame carries the MID as a u32

_KIND_TO_PROFILE: Final[Mapping[MessageKind, ProfileKind]] = MappingProxyType(
    {
        MessageKind.COMMAND: ProfileKind.COMMAND,
        MessageKind.TELEMETRY: ProfileKind.TELEMETRY,
    }
)


@dataclass(frozen=True, slots=True)
class _Segment:
    """A run of fields with one byte order, decoded by one ``struct.Struct``."""

    struct: struct.Struct
    start: int
    indices: tuple[int, ...]  # definition-order field indices, in struct order


@dataclass(frozen=True, slots=True)
class _VarSlot:
    """A variable-length field and what is needed to read and write it."""

    index: int  # definition-order index of the field
    field: FieldDef
    spec: VarSpec
    element: struct.Struct | None  # one scalar element; None for char and bytes
    count: struct.Struct | None  # reads the count or length field; None for "remaining"
    count_offset: int  # payload offset of the count or length field
    count_index: int  # definition-order index of the count or length field; -1 for "remaining"


@dataclass(frozen=True, slots=True)
class _Layout:
    message: Message
    segments: tuple[_Segment, ...]  # the fixed fields
    text_indices: tuple[int, ...]  # fixed char[N] fields needing bytes -> str
    identity: bool  # single segment already in definition order, no char fields
    variables: tuple[_VarSlot, ...] = ()  # variable-length fields, in wire order
    count_names: frozenset[str] = frozenset()  # count and length fields their data determines


_ISSUE = object()  # the key only :meth:`Codec.resolve` holds; a Resolved is built with it


@final
class Resolved:
    """What :meth:`Codec.resolve` established about a packet that passed the check: the message
    it is, the header word and the variable-length counts. :meth:`Codec.decode_resolved`
    unpacks the fields from it without repeating the check.

    Only :meth:`Codec.resolve` makes one: construction needs a key this module keeps to itself,
    and the fields are read-only (there is no ``_replace`` and no copy or pickle round trip). The
    token records the codec and the ``bytes`` object it was made from (its message knows the
    kind), and :meth:`Codec.decode_resolved` believes it only for exactly those. A mutable
    ``bytearray`` or ``memoryview`` packet gets a token that is never believed, because its
    content may have changed since the check.
    """

    __slots__ = ("codec", "counts", "data", "message", "word")

    message: Message
    word: int
    counts: tuple[int, ...]
    codec: Codec
    data: bytes | bytearray | memoryview

    def __init__(
        self,
        key: object,
        message: Message,
        word: int,
        codec: Codec,
        data: bytes | bytearray | memoryview,
        counts: tuple[int, ...] = (),
    ) -> None:
        if key is not _ISSUE:
            raise TypeError("a Resolved is made only by Codec.resolve")
        # the slot descriptors set the fields; __setattr__ refuses every later write
        _SET_MESSAGE(self, message)
        _SET_WORD(self, word)
        _SET_COUNTS(self, counts)
        _SET_CODEC(self, codec)
        _SET_DATA(self, data)

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError("a Resolved is read-only")

    def __delattr__(self, name: str) -> None:
        raise AttributeError("a Resolved is read-only")

    def __reduce__(self) -> NoReturn:
        raise TypeError("a Resolved cannot be copied or pickled")

    def __repr__(self) -> str:
        return f"Resolved({self.message.name}, word=0x{self.word:08X}, counts={self.counts})"


_SET_MESSAGE = Resolved.message.__set__  # type: ignore[attr-defined]
_SET_WORD = Resolved.word.__set__  # type: ignore[attr-defined]
_SET_COUNTS = Resolved.counts.__set__  # type: ignore[attr-defined]
_SET_CODEC = Resolved.codec.__set__  # type: ignore[attr-defined]
_SET_DATA = Resolved.data.__set__  # type: ignore[attr-defined]


@dataclass(frozen=True, slots=True)
class DecodedPacket:
    message: Message
    header: Header
    values: tuple[object, ...]  # definition order, aligned with message.fields
    size: int

    def as_dict(self) -> dict[str, object]:
        return {f.name: v for f, v in zip(self.message.fields, self.values, strict=True)}


@dataclass(frozen=True, slots=True)
class CountFault:
    """A count or length field sent with a value other than the one its data implies."""

    field: str  # the count or length field
    given: int  # the value the caller gave, which went out
    implied: int  # the value the data of the field it sizes implies
    unit: VarUnit  # elements (COUNT) or bytes (LENGTH)


@dataclass(frozen=True, slots=True)
class EncodedPacket:
    data: bytes
    message: Message
    mid: int
    sequence: int
    function_code: int | None
    count_faults: tuple[CountFault, ...] = ()  # injected with ``allow_count_mismatch``


def _compile_layout(message: Message) -> _Layout:
    fixed = [i for i, f in enumerate(message.fields) if f.var is None]
    order = sorted(fixed, key=lambda i: message.fields[i].offset)
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
    text = tuple(i for i in fixed if message.fields[i].type.type_class is TypeClass.CHAR)
    identity = (
        len(segments) <= 1
        and not text
        and not message.is_variable
        and (not segments or segments[0].indices == tuple(range(len(message.fields))))
    )
    slots: list[_VarSlot] = []
    for i, f in enumerate(message.fields):
        spec = f.var
        if spec is None:
            continue
        prefix = f.endian.struct_prefix
        scalar = spec.element.type_class in (TypeClass.INT, TypeClass.FLOAT)
        element = struct.Struct(prefix + spec.element.struct_code) if scalar else None
        count: struct.Struct | None = None
        count_index = -1
        if spec.count_field is not None:
            count_index = next(j for j in fixed if message.fields[j].name == spec.count_field)
            holder = message.fields[count_index]
            count = struct.Struct(holder.endian.struct_prefix + holder.type.struct_code)
        offset = message.fields[count_index].offset if count_index >= 0 else 0
        slots.append(_VarSlot(i, f, spec, element, count, offset, count_index))
    names = frozenset(message.fields[s.count_index].name for s in slots if s.count_index >= 0)
    return _Layout(message, tuple(segments), text, identity, tuple(slots), names)


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
MAX_LIST_TEXT: Final = 1 << 20  # characters of a spelled-out array or hex value a caller may pass


def _enum_hint(field: FieldDef) -> str:
    """``; names: A, B, ...`` for an enum field, bounded however large the enum is."""
    if not field.enum:
        return ""
    names = list(field.enum)[:MAX_ENUM_HINT_NAMES]
    more = f", ... ({len(field.enum)} in all)" if len(field.enum) > len(names) else ""
    return f"; names: {', '.join(names)}{more}"


def _text_bytes(message: Message, field: FieldDef, value: object, limit: int) -> bytes:
    """ASCII text as bytes, at most ``limit`` of them."""
    if not isinstance(value, str):
        raise _arg_error(message, field, f"unsupported value {value!r}")
    try:
        encoded = value.encode("ascii")
    except UnicodeEncodeError:
        raise _arg_error(message, field, "text must be ASCII") from None
    if len(encoded) > limit:
        raise _arg_error(message, field, f"{len(encoded)} characters exceed {limit}")
    return encoded


def _hex_bytes(message: Message, field: FieldDef, value: object) -> bytes:
    """Bytes, or hex digits as bytes; the caller checks the length."""
    if isinstance(value, bytes):
        return value
    if not isinstance(value, str):
        raise _arg_error(message, field, f"unsupported value {value!r}")
    if len(value) > MAX_LIST_TEXT:
        raise _arg_error(message, field, f"the hex text is longer than {MAX_LIST_TEXT} characters")
    try:
        return bytes.fromhex(value)
    except ValueError:
        raise _arg_error(message, field, "expected hex digits") from None


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
            return _text_bytes(message, field, value, ftype.size)
        case TypeClass.BYTES:
            raw = _hex_bytes(message, field, value)
            if len(raw) != ftype.size:
                raise _arg_error(
                    message, field, f"needs exactly {ftype.size} bytes, got {len(raw)}"
                )
            return raw
        case TypeClass.ARRAY:
            raise AssertionError("a variable-length field goes through coerce_variable")
    raise AssertionError(f"unhandled type class {ftype.type_class}")  # pragma: no cover


def _split_list(message: Message, field: FieldDef, text: str) -> list[str]:
    """The items of ``1,2,3``, ``[1, 2, 3]`` or ``1 2 3``; empty text is an empty array."""
    if len(text) > MAX_LIST_TEXT:
        raise _arg_error(message, field, f"the list is longer than {MAX_LIST_TEXT} characters")
    body = text.strip()
    if body.startswith("["):
        if not body.endswith("]"):
            raise _arg_error(message, field, "a list starting with [ must end with ]")
        body = body[1:-1]
    return body.replace(",", " ").split()


def coerce_variable(message: Message, field: FieldDef, value: object) -> tuple[int, bytes]:
    """The element count and the wire bytes of a variable-length field's value: text for
    ``char[]``, bytes or hex for ``bytes[]``, a list, tuple or ``1,2,3`` text for a scalar array.
    The count is checked against the field's ``min`` and ``max``."""
    spec = field.var
    assert spec is not None
    element = spec.element
    match element.type_class:
        case TypeClass.CHAR:
            blob = _text_bytes(message, field, value, spec.max)
            if b"\x00" in blob:  # decode ends text at a NUL, so the tail would not round-trip
                raise _arg_error(message, field, "text cannot contain a NUL character")
            count = len(blob)
        case TypeClass.BYTES:
            blob = _hex_bytes(message, field, value)
            count = len(blob)
        case _:
            if isinstance(value, str):
                items: Sequence[object] = _split_list(message, field, value)
            elif isinstance(value, list | tuple):
                items = cast("Sequence[object]", value)
            else:
                raise _arg_error(message, field, f"unsupported value {str(value)[:40]!r}")
            if len(items) > spec.max:
                raise _arg_error(message, field, f"{len(items)} elements exceed {spec.max}")
            plain = replace(field, type=element, var=None)
            packer = _element_packer(field)
            pieces: list[bytes] = []
            for i, item in enumerate(items):
                try:
                    number = coerce_value(message, plain, item)
                except IronbirdError:
                    # Name the element: the same coercion with the element's position in the name.
                    indexed = replace(plain, name=f"{field.name}[{i}]")
                    coerce_value(message, indexed, item)
                    raise
                pieces.append(packer.pack(number))
            count, blob = len(items), b"".join(pieces)
    if not spec.min <= count <= spec.max:
        raise _arg_error(
            message, field, f"{count} elements; the CCDD allows {spec.min} to {spec.max}"
        )
    return count, blob


def _element_packer(field: FieldDef) -> struct.Struct:
    assert field.var is not None and field.var.element.struct_code
    return struct.Struct(field.endian.struct_prefix + field.var.element.struct_code)


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
        self._profiles: Mapping[MessageKind, HeaderProfile] = MappingProxyType(
            {
                MessageKind.COMMAND: command_profile,
                MessageKind.TELEMETRY: telemetry_profile,
            }
        )
        self._sequence: dict[int, int] = {}
        self._command_mids: frozenset[int] = frozenset(m for m, _ in model.commands_by_key)
        self._payload_bounds: Mapping[MessageKind, Mapping[int, tuple[int, int]]] = (
            MappingProxyType(
                {
                    MessageKind.TELEMETRY: MappingProxyType(
                        {
                            mid: (msg.min_payload, msg.payload_size)
                            for mid, msg in model.telemetry_by_mid.items()
                        }
                    ),
                    MessageKind.COMMAND: MappingProxyType(self._command_payload_bounds(model)),
                }
            )
        )
        layouts: dict[str, _Layout] = {}
        for msg in model.messages:
            self._validate_fit(msg)
            layouts[msg.name] = _compile_layout(msg)
        self._layouts: Mapping[str, _Layout] = MappingProxyType(layouts)

    @staticmethod
    def _command_payload_bounds(model: CcddModel) -> dict[int, tuple[int, int]]:
        """Per command MID, the widest payload range of the commands that share it."""
        bounds: dict[int, tuple[int, int]] = {}
        for (mid, _code), msg in model.commands_by_key.items():
            low, high = bounds.get(mid, (msg.min_payload, msg.payload_size))
            bounds[mid] = (min(low, msg.min_payload), max(high, msg.payload_size))
        return bounds

    @staticmethod
    def _validate_profile(profile: HeaderProfile) -> None:
        """Reject, at load time, profiles the codec cannot drive or decode."""
        if not profile.mid_fields:
            raise IronbirdError(
                Code.HDR_SCHEMA,
                f"{profile.source}: profile {profile.name} defines no mid_fields; "
                "a codec needs them to route packets",
            )
        if profile.mid_bits > MAX_MID_BITS:
            raise IronbirdError(
                Code.HDR_SCHEMA,
                f"{profile.source}: profile {profile.name} has a {profile.mid_bits}-bit message "
                f"ID; at most {MAX_MID_BITS} bits are supported",
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
        total = profile.min_bytes + msg.payload_size
        if total > self.max_packet_bytes:
            raise IronbirdError(
                Code.ENC_FIT,
                f"{where}: packet of {total} bytes exceeds the {self.max_packet_bytes}-byte cap",
            )
        # Every size between the smallest and the largest packet must fit the header; a message
        # of one size has the one.
        smallest = profile.min_bytes + msg.min_payload
        sizes = sorted({smallest, total})
        lf = profile.length_field
        cf = profile.checksum_field
        for n in sizes:
            if lf is not None and not 0 <= n + lf.bias <= lf.mask:
                raise IronbirdError(
                    Code.ENC_FIT,
                    f"{where}: packet of {n} bytes does not fit the {lf.bits}-bit "
                    f"{lf.name} field of profile {profile.name}",
                    details=profile.source,
                )
        if cf is not None:
            # The span may depend on the size, so check every size where its answer can change.
            for n in hdr.checksum_probe_sizes(profile, cf, smallest, total):
                plan = hdr.checksum_plan(profile, cf, n)
                if isinstance(plan, str):
                    raise IronbirdError(
                        Code.ENC_FIT,
                        f"{where}: {plan} (profile {profile.name})",
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
        try:
            self._check_header_values(profile, msg.header, "header")
        except IronbirdError as exc:
            raise IronbirdError(
                Code.ENC_FIT, f"{where}: {exc.summary}", details=profile.source, cause=exc
            ) from exc

    @staticmethod
    def _check_header_values(profile: HeaderProfile, header_values: object, what: str) -> None:
        """Every name in ``values`` is a header field the caller may set and its value fits.
        The argument comes from a public API, so its shape is checked too."""
        if not isinstance(header_values, Mapping):
            raise IronbirdError(
                Code.HDR_PACK,
                f"{what} must be a mapping of field name to value, "
                f"not a {type(header_values).__name__}",
            )
        values = cast("Mapping[object, object]", header_values)
        settable = [f.name for f in profile.input_fields if f.name != profile.function_code_field]
        for name, value in values.items():
            if type(name) is not str:
                raise IronbirdError(
                    Code.HDR_PACK,
                    f"{what} field names must be strings, not {type(name).__name__}",
                )
            f = profile.by_name.get(name)
            if name == profile.function_code_field:
                raise IronbirdError(
                    Code.HDR_PACK,
                    f"{what} {name[:40]!r}: the function code comes from the command definition",
                )
            if f is None or f.kind is not hdr.FieldKind.INPUT:
                available = ", ".join(settable) if settable else "none"
                raise IronbirdError(
                    Code.HDR_PACK,
                    f"{what} {name[:40]!r} is not an input field of profile {profile.name} "
                    f"(settable: {available})",
                )
            if type(value) is not int or not 0 <= value <= f.mask:
                raise IronbirdError(
                    Code.HDR_PACK,
                    f"{what} {name}={str(value)[:40]} does not fit {f.bits} bits (max {f.mask})",
                )

    def reset_sequences(self) -> None:
        """Restart every per-MID sequence counter at zero."""
        self._sequence.clear()

    def _next_sequence(self, mid: int) -> int:
        value = self._sequence.get(mid, 0)
        self._sequence[mid] = value + 1
        return value

    # -- encode ----------------------------------------------------------------------

    def count_field_names(self, name: str, kind: MessageKind) -> tuple[str, ...]:
        """The count and length fields of message ``name`` that its variable-length data
        determines (empty for a fixed-size message); unknown names are refused as in encoding."""
        return tuple(sorted(self._layouts[self._lookup(name, kind).name].count_names))

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

    def _encode_payload(
        self,
        msg: Message,
        args: Mapping[str, object],
        *,
        strict: bool,
        allow_count_mismatch: bool = False,
        flag_hint: str = "",
    ) -> tuple[bytes, tuple[CountFault, ...]]:
        names = {f.name for f in msg.fields}
        unexpected = sorted(set(args) - names)
        if unexpected:
            raise IronbirdError(
                Code.ENC_ARGSET,
                f"{msg.name}: unexpected argument(s) {', '.join(n[:32] for n in unexpected)}; "
                f"expected: {', '.join(f.name for f in msg.fields) or 'none'}",
            )
        layout = self._layouts[msg.name]
        # A count or length field follows from the data it sizes, so it is not required.
        missing = [
            f.name for f in msg.fields if f.name not in args and f.name not in layout.count_names
        ]
        if missing and strict:
            raise IronbirdError(
                Code.ENC_ARGSET, f"{msg.name}: missing argument(s) {', '.join(missing)}"
            )
        values: list[object] = []
        for f in msg.fields:
            if f.var is not None:
                values.append(None)  # set below, once the data is known
            elif f.name in args:
                values.append(coerce_value(msg, f, args[f.name]))
            else:
                values.append(_zero(f))
        blobs: list[bytes] = []
        faults: list[CountFault] = []
        for slot in layout.variables:
            f, spec = slot.field, slot.spec
            if f.name in args:
                count, blob = coerce_variable(msg, f, args[f.name])
            else:  # not given: the smallest array the CCDD allows, zero-filled
                count = spec.min
                blob = bytes(spec.min_bytes)
            blobs.append(blob)
            if slot.count_index < 0:
                continue
            holder = msg.fields[slot.count_index]
            implied = count if spec.unit is VarUnit.COUNT else len(blob)
            assert implied <= holder.type.bit_range[1], "the CCDD load checked the count field"
            if holder.name not in args:
                values[slot.count_index] = implied
                continue
            given = values[slot.count_index]
            if given != implied and not allow_count_mismatch:
                raise IronbirdError(
                    Code.ENC_ARG,
                    f"{msg.name}: argument {holder.name}: {given} disagrees with the {implied} "
                    f"{'elements' if spec.unit is VarUnit.COUNT else 'bytes'} of {f.name}; "
                    f"leave it out to have it filled in{flag_hint}",
                )
            if given != implied:
                assert isinstance(given, int)  # a count field is an unsigned integer
                faults.append(CountFault(holder.name, given, implied, spec.unit))
        buf = bytearray(msg.fixed_extent if msg.is_variable else msg.payload_size)
        for seg in layout.segments:
            seg.struct.pack_into(buf, seg.start, *(values[i] for i in seg.indices))
        for blob in blobs:
            buf += blob
        assert msg.min_payload <= len(buf) <= msg.payload_size
        assert allow_count_mismatch or not faults
        return bytes(buf), tuple(faults)

    def _frame(
        self,
        msg: Message,
        payload: bytes,
        inputs: Mapping[str, int],
        tamper: Mapping[str, int] | None,
        header: Mapping[str, int] | None,
        faults: tuple[CountFault, ...] = (),
    ) -> EncodedPacket:
        profile = self._profiles[msg.kind]
        values: dict[str, int] = {**msg.header}
        if header:
            self._check_header_values(profile, header, "header")
            values.update(header)
        values.update(inputs)
        sequence = self._next_sequence(msg.mid)
        data = hdr.pack(
            profile,
            payload,
            mid=msg.mid,
            inputs=values,
            sequence=sequence,
            now=self._clock(),
            tamper=tamper,
        )
        assert (
            len(data) <= self.max_packet_bytes
        )  # guaranteed by _validate_fit (+ tamper keeps size)
        return EncodedPacket(data, msg, msg.mid, sequence, msg.function_code, faults)

    def encode_command(
        self,
        name: str,
        args: Mapping[str, object] | None = None,
        *,
        tamper: Mapping[str, int] | None = None,
        header: Mapping[str, int] | None = None,
        allow_count_mismatch: bool = False,
    ) -> EncodedPacket:
        """Encode a command. ``header`` sets input fields of the command header profile for this
        packet; it wins over the message's own ``header`` values, which win over the profile's
        defaults. A count or length field is filled from the data it sizes; a value given for it
        that disagrees is refused unless ``allow_count_mismatch`` (fault injection)."""
        msg = self._lookup(name, MessageKind.COMMAND)
        payload, faults = self._encode_payload(
            msg,
            args or {},
            strict=True,
            allow_count_mismatch=allow_count_mismatch,
            flag_hint=" (--allow-count-mismatch injects a fault)",
        )
        profile = self._profiles[MessageKind.COMMAND]
        assert profile.function_code_field is not None and msg.function_code is not None
        return self._frame(
            msg, payload, {profile.function_code_field: msg.function_code}, tamper, header, faults
        )

    def encode_telemetry(
        self,
        name: str,
        values: Mapping[str, object] | None = None,
        *,
        tamper: Mapping[str, int] | None = None,
        header: Mapping[str, int] | None = None,
        allow_count_mismatch: bool = False,
    ) -> EncodedPacket:
        """Encode telemetry; fields not given are zero-filled. ``header`` and
        ``allow_count_mismatch`` are as for :meth:`encode_command`; a refusal does not name the
        ``send`` option, which a telemetry sender (``respond``) does not have."""
        msg = self._lookup(name, MessageKind.TELEMETRY)
        payload, faults = self._encode_payload(
            msg, values or {}, strict=False, allow_count_mismatch=allow_count_mismatch
        )
        return self._frame(msg, payload, {}, tamper, header, faults)

    # -- check and decode (the boundary) -------------------------------------------

    def peek_mid(self, kind: MessageKind, data: bytes | bytearray | memoryview) -> int | None:
        return hdr.peek_mid(self._profiles[kind], data)

    def is_known_mid(self, kind: MessageKind, mid: int) -> bool:
        if kind is MessageKind.TELEMETRY:
            return mid in self.model.telemetry_by_mid
        return mid in self._command_mids

    def payload_bounds(self, kind: MessageKind, mid: int) -> tuple[int, int] | None:
        """The fewest and most payload bytes a packet of ``kind`` with this MID may carry
        (commands sharing a MID: the widest range among them), or ``None`` when the CCDD does
        not define the MID. A stream framer uses it to tell a plausible header from noise."""
        return self._payload_bounds[kind].get(mid)

    def largest_payload(self, kind: MessageKind) -> int:
        """The most payload bytes any message of ``kind`` may carry (0 when the CCDD has none).
        A stream framer bounds what a header may claim by it."""
        return max((high for _, high in self._payload_bounds[kind].values()), default=0)

    def oversize_error(
        self, kind: MessageKind, data: bytes | bytearray | memoryview, orig_len: int
    ) -> DecodeError:
        """The error for a packet that was ``orig_len`` bytes on the wire and was cut at the cap
        (``data`` is what was kept)."""
        assert orig_len > self.max_packet_bytes
        return hdr.oversize_error(self._profiles[kind], data, orig_len, self.max_packet_bytes)

    def resolve(
        self, kind: MessageKind, data: bytes | bytearray | memoryview
    ) -> Resolved | DecodeError:
        """Everything short of unpacking fields: the message and the header word, or the error.

        Errors come in a fixed order: size above the cap or below the header, MID not in the
        CCDD, length field, fixed fields, checksum, function code not in the CCDD, payload size.
        """
        profile = self._profiles[kind]
        size = len(data)
        mid: int | None = None
        if profile.size_bytes <= size <= self.max_packet_bytes:
            mid = hdr.peek_mid(profile, data)
            assert mid is not None  # the MID window lies inside the header
            if not self.is_known_mid(kind, mid):
                return DecodeError(
                    Code.DEC_UNKNOWN, 0, f"unknown {kind.value} MID 0x{mid:04X}", mid
                )
        word = hdr.check_word(profile, data, max_size=self.max_packet_bytes)
        if isinstance(word, DecodeError):
            return word
        assert mid is not None  # a valid header means the size was in range
        msg: Message | None
        if kind is MessageKind.COMMAND:
            assert profile.function_code_field is not None
            fc_field = profile.by_name[profile.function_code_field]
            fc = hdr.read_field(fc_field, word)
            msg = self.model.commands_by_key.get((mid, fc))
            if msg is None:
                return DecodeError(
                    Code.DEC_UNKNOWN,
                    profile.byte_offset(fc_field),
                    f"unknown command function code {fc} for MID 0x{mid:04X}",
                    mid,
                )
        else:
            msg = self.model.telemetry_by_mid.get(mid)
            assert msg is not None  # the MID was found in the CCDD above
        if not msg.is_variable:
            expected = profile.min_bytes + msg.payload_size
            if size != expected:
                return DecodeError(
                    Code.DEC_PAYLOAD,
                    profile.size_bytes,
                    f"{msg.name} payload is {size - profile.min_bytes} bytes, "
                    f"CCDD says {msg.payload_size}",
                    mid,
                )
            return Resolved(_ISSUE, msg, word, self, data)
        payload = size - profile.min_bytes
        if not msg.min_payload <= payload <= msg.payload_size:
            return DecodeError(
                Code.DEC_PAYLOAD,
                profile.size_bytes,
                f"{msg.name} payload is {payload} bytes, "
                f"CCDD says {msg.min_payload} to {msg.payload_size}",
                mid,
            )
        counts = self._variable_counts(
            self._layouts[msg.name], data, profile.size_bytes, size - profile.trailer_size
        )
        if isinstance(counts, DecodeError):
            return counts
        return Resolved(_ISSUE, msg, word, self, data, counts)

    @staticmethod
    def _variable_counts(
        layout: _Layout, data: bytes | bytearray | memoryview, base: int, end: int
    ) -> tuple[int, ...] | DecodeError:
        """The element count of each variable-length field of the packet ``data``, whose payload
        spans ``base`` to ``end``, or the first way the packet disagrees with the CCDD.

        The caller has checked that the payload is no smaller than ``min_payload``, so the
        fixed fields, with the count and length fields among them, are all inside the packet.
        The fields' data must tile the rest of the payload exactly.
        """
        msg = layout.message
        assert end - base >= msg.min_payload
        cursor = base + msg.fixed_extent
        counts: list[int] = []
        for slot in layout.variables:
            spec, name = slot.spec, slot.field.name
            width = spec.element.size
            if slot.count is None:  # "remaining": everything up to the end of the payload
                where, in_bytes, given = cursor, True, end - cursor
            else:
                where = base + slot.count_offset
                in_bytes = spec.unit is VarUnit.LENGTH
                given = slot.count.unpack_from(data, where)[0]
                assert isinstance(given, int)
            limit = spec.max_bytes if in_bytes else spec.max
            if given > limit:
                what = "length" if in_bytes else "count"
                return DecodeError(
                    Code.DEC_COUNT,
                    where,
                    f"{msg.name}.{name} {what} {given} is above the CCDD maximum {limit}",
                    msg.mid,
                )
            n, odd = divmod(given, width) if in_bytes else (given, 0)
            if odd:
                return DecodeError(
                    Code.DEC_PAYLOAD,
                    where,
                    f"{msg.name}.{name}: {given} bytes is not a whole number of "
                    f"{width}-byte elements",
                    msg.mid,
                )
            if n < spec.min:
                return DecodeError(
                    Code.DEC_PAYLOAD,
                    where,
                    f"{msg.name}.{name} has {n} elements, below the CCDD minimum {spec.min}",
                    msg.mid,
                )
            need = n * width
            if cursor + need > end:
                return DecodeError(
                    Code.DEC_OVERRUN,
                    cursor,
                    f"{msg.name}.{name} needs {need} bytes at byte {cursor} but only "
                    f"{end - cursor} remain",
                    msg.mid,
                )
            cursor += need
            counts.append(n)
        if cursor != end:
            return DecodeError(
                Code.DEC_PAYLOAD,
                cursor,
                f"{msg.name}: {end - cursor} bytes follow the last variable-length field",
                msg.mid,
            )
        return tuple(counts)

    def check(self, kind: MessageKind, data: bytes | bytearray | memoryview) -> DecodeError | None:
        """Validate one whole packet without unpacking its fields; ``None`` means well formed.

        Safe on the receive path: it reads the header word and, for a variable-length message,
        only the count and length fields of the payload, and never raises. ``decode`` accepts
        exactly the packets ``check`` accepts.
        """
        result = self.resolve(kind, data)
        return result if isinstance(result, DecodeError) else None

    def decode(
        self, kind: MessageKind, data: bytes | bytearray | memoryview
    ) -> DecodedPacket | DecodeError:
        """Parse one whole packet: ``resolve`` first, then the fields. Never raises."""
        resolved = self.resolve(kind, data)
        if isinstance(resolved, DecodeError):
            return resolved
        return self._unpack(kind, resolved, data)

    def decode_resolved(
        self, kind: MessageKind, resolved: Resolved, data: bytes | bytearray | memoryview
    ) -> DecodedPacket | DecodeError:
        """Unpack the fields of ``data``, which :meth:`resolve` already accepted as ``resolved``.

        This is the second half of :meth:`decode`, for a caller that kept the first half's
        result (the engine's ingest ran it on every frame). Only :meth:`resolve` makes a
        :class:`Resolved`, so its content is trusted without a second look, but only for the
        codec, kind and ``bytes`` object it was made from: the checks here are identities and
        nothing is re-derived. Any other pairing (another codec or kind, other or mutable bytes,
        or an object that is not a ``Resolved``) takes the whole :meth:`decode`, so the answer is
        always what ``decode(kind, data)`` gives, and this never raises.
        """
        if (
            type(resolved) is not Resolved
            or resolved.codec is not self
            or resolved.data is not data
            or resolved.message.kind is not kind
            or type(data) is not bytes
        ):
            return self.decode(kind, data)
        assert resolved.message.name in self._layouts
        return self._unpack(kind, resolved, data)

    def _unpack(
        self, kind: MessageKind, resolved: Resolved, data: bytes | bytearray | memoryview
    ) -> DecodedPacket | DecodeError:
        """The fields of ``data``, which ``resolved`` was made from by this codec."""
        msg, word = resolved.message, resolved.word
        size = len(data)
        profile = self._profiles[kind]
        trailer = hdr.trailer_word(profile, data) if profile.trailer_size else 0
        parsed = hdr.parse_word(profile, word, msg.mid, trailer)
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
                if layout.variables:
                    self._unpack_variable(layout, resolved, data, base, out)
                values = tuple(out)
        except struct.error as exc:  # pragma: no cover - sizes were validated above
            return DecodeError(Code.DEC_PAYLOAD, base, f"payload unpack failed: {exc}", msg.mid)
        return DecodedPacket(msg, parsed, values, size)

    @staticmethod
    def _unpack_variable(
        layout: _Layout,
        resolved: Resolved,
        data: bytes | bytearray | memoryview,
        base: int,
        out: list[object],
    ) -> None:
        """Set the variable-length fields of ``out``. ``resolved.counts`` was read from this very
        packet by the check, which also proved that the fields' data tile the end of the payload."""
        counts = resolved.counts
        assert len(counts) == len(layout.variables)
        cursor = base + layout.message.fixed_extent
        view = memoryview(data)
        for slot, n in zip(layout.variables, counts, strict=True):
            width = slot.spec.element.size
            end = cursor + n * width
            assert end <= len(data)
            chunk = view[cursor:end]
            match slot.spec.element.type_class:
                case TypeClass.CHAR:
                    out[slot.index] = _decode_text(bytes(chunk))
                case TypeClass.BYTES:
                    out[slot.index] = bytes(chunk)
                case _:
                    assert slot.element is not None
                    out[slot.index] = tuple(v for (v,) in slot.element.iter_unpack(chunk))
            cursor = end


def _zero(field: FieldDef) -> object:
    match field.type.type_class:
        case TypeClass.INT:
            return 0
        case TypeClass.FLOAT:
            return 0.0
        case TypeClass.CHAR | TypeClass.BYTES:
            return b""
        case TypeClass.ARRAY:
            return ()
    raise AssertionError("unreachable")  # pragma: no cover
