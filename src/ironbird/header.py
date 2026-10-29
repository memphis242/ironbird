"""User-defined header profiles and the generic bitfield codec that packs/unpacks them.

A profile (TOML) declares header fields in wire order with bit widths. Fields are big-endian
bitfields packed MSB-first, so a profile's total width must be a whole number of bytes.
Each field is one of:

* fixed: ``value = N`` (verified on unpack unless ``check = false``)
* input: no ``value``/``computed``; supplied by the caller by name, optional ``default``
* computed: ``computed = "<kind>"`` with kinds listed in ``COMPUTED_KINDS``

``unpack`` is the only place header bytes are parsed. It returns a :class:`Header` or a typed
:class:`~ironbird.errors.DecodeError`, never an exception, and checks every length first.
"""

from __future__ import annotations

import re
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final

from ironbird import checksum
from ironbird.errors import Code, DecodeError, IronbirdError

DEFAULT_MAX_PACKET_BYTES: Final = 2048
MAX_HEADER_BYTES: Final = 64
MAX_PROFILE_FILE_BYTES: Final = 64 * 1024
_NAME_RE: Final = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,63}")
MAX_DESCRIPTION_CHARS: Final = 1024


class FieldKind(StrEnum):
    FIXED = "fixed"
    INPUT = "input"
    COMPUTED = "computed"


class ProfileKind(StrEnum):
    COMMAND = "command"
    TELEMETRY = "telemetry"


COMPUTED_KINDS: Final[Mapping[str, str]] = MappingProxyType(
    {
        "length": "total packet bytes plus `bias` (CCSDS with a 6-byte primary header: bias = -7)",
        "sequence": "per-MID sequence counter, wraps at the field width",
        "from_mid": "bits taken from the message ID (field must be listed in mid_fields)",
        "checksum": "named algorithm over the whole packet with this field zeroed",
        "time_seconds": "whole seconds of the send time (Unix epoch), truncated to the field width",
        "time_subseconds": "fraction of a second scaled to the field width",
    }
)


@dataclass(frozen=True, slots=True)
class HeaderField:
    name: str
    bits: int
    shift: int  # bit position of the field's LSB within the header word
    kind: FieldKind
    value: int | None = None  # fixed value, or input default
    computed: str | None = None
    bias: int = 0
    algorithm: str | None = None
    check: bool = True
    description: str = ""

    @property
    def mask(self) -> int:
        return (1 << self.bits) - 1


@dataclass(frozen=True, slots=True)
class HeaderProfile:
    name: str
    kind: ProfileKind
    fields: tuple[HeaderField, ...]
    size_bytes: int
    total_bits: int
    function_code_field: str | None
    mid_fields: tuple[str, ...]
    mid_bits: int
    # (byte offset, byte count, right shift, mask) to read the MID straight from the wire
    mid_window: tuple[int, int, int, int] | None
    by_name: Mapping[str, HeaderField]
    length_field: HeaderField | None
    sequence_field: HeaderField | None
    checksum_field: HeaderField | None
    source: str = "<memory>"

    def byte_offset(self, field: HeaderField) -> int:
        return (self.total_bits - field.shift - field.bits) // 8

    @property
    def input_fields(self) -> tuple[HeaderField, ...]:
        return tuple(f for f in self.fields if f.kind is FieldKind.INPUT)


@dataclass(frozen=True, slots=True)
class Header:
    """A successfully unpacked and validated header."""

    values: Mapping[str, int]
    mid: int | None
    size: int
    sequence: int | None


# ---------------------------------------------------------------------------------------
# Profile loading and validation
# ---------------------------------------------------------------------------------------


class _Ctx:
    def __init__(self, source: str) -> None:
        self.source = source

    def fail(self, path: str, message: str) -> IronbirdError:
        return IronbirdError(Code.HDR_SCHEMA, f"{self.source}: {path}: {message}")


def _expect_keys(
    ctx: _Ctx, table: Mapping[str, Any], path: str, allowed: set[str], required: set[str]
) -> None:
    for key in table:
        if key not in allowed:
            raise ctx.fail(f"{path}.{key}" if path else key, "unknown key")
    for key in sorted(required - table.keys()):
        raise ctx.fail(f"{path}.{key}" if path else key, "required key is missing")


def _int(ctx: _Ctx, table: Mapping[str, Any], key: str, path: str, lo: int, hi: int) -> int:
    value = table[key]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ctx.fail(f"{path}.{key}", "must be an integer")
    if not lo <= value <= hi:
        raise ctx.fail(f"{path}.{key}", f"must be in {lo}..{hi}, got {value}")
    return value


def _str(ctx: _Ctx, table: Mapping[str, Any], key: str, path: str) -> str:
    value = table[key]
    if not isinstance(value, str):
        raise ctx.fail(f"{path}.{key}", "must be a string")
    if len(value) > MAX_DESCRIPTION_CHARS:
        raise ctx.fail(f"{path}.{key}", f"longer than {MAX_DESCRIPTION_CHARS} characters")
    return value


def parse_profile(data: Mapping[str, Any], source: str = "<memory>") -> HeaderProfile:
    """Validate a parsed TOML mapping and build an immutable profile."""
    ctx = _Ctx(source)
    _expect_keys(
        ctx,
        data,
        "",
        {
            "name",
            "kind",
            "description",
            "byte_order",
            "function_code_field",
            "mid_fields",
            "fields",
        },
        {"name", "kind", "fields"},
    )
    name = _str(ctx, data, "name", "")
    if not _NAME_RE.fullmatch(name):
        raise ctx.fail("name", "must be an identifier (letters, digits, underscore)")
    kind_text = _str(ctx, data, "kind", "")
    try:
        kind = ProfileKind(kind_text)
    except ValueError:
        raise ctx.fail("kind", "must be 'command' or 'telemetry'") from None
    if "description" in data:
        _str(ctx, data, "description", "")
    if "byte_order" in data and data["byte_order"] != "big":
        raise ctx.fail("byte_order", "only 'big' (MSB-first bitfields) is supported")

    raw_fields = data["fields"]
    if not isinstance(raw_fields, list) or not raw_fields:
        raise ctx.fail("fields", "must be a non-empty array of tables")
    if len(raw_fields) > 256:
        raise ctx.fail("fields", "too many fields (max 256)")

    # First pass: validate each field and total width.
    parsed: list[dict[str, Any]] = []
    names: set[str] = set()
    total_bits = 0
    for i, raw in enumerate(raw_fields):
        path = f"fields[{i}]"
        if not isinstance(raw, dict):
            raise ctx.fail(path, "must be a table")
        _expect_keys(
            ctx,
            raw,
            path,
            {
                "name",
                "bits",
                "value",
                "default",
                "computed",
                "bias",
                "algorithm",
                "check",
                "description",
            },
            {"name", "bits"},
        )
        fname = _str(ctx, raw, "name", path)
        if not _NAME_RE.fullmatch(fname):
            raise ctx.fail(f"{path}.name", "must be an identifier")
        if fname in names:
            raise ctx.fail(f"{path}.name", f"duplicate field name {fname!r}")
        names.add(fname)
        bits = _int(ctx, raw, "bits", path, 1, 64)
        total_bits += bits
        if "description" in raw:
            _str(ctx, raw, "description", path)
        parsed.append(raw)
    if total_bits % 8:
        raise ctx.fail("fields", f"total width is {total_bits} bits, not a whole number of bytes")
    if total_bits // 8 > MAX_HEADER_BYTES:
        raise ctx.fail("fields", f"header exceeds {MAX_HEADER_BYTES} bytes")

    # Second pass: classify fields and compute shifts.
    fields: list[HeaderField] = []
    consumed = 0
    for i, raw in enumerate(parsed):
        path = f"fields[{i}]"
        bits = raw["bits"]
        consumed += bits
        shift = total_bits - consumed
        has_value = "value" in raw
        has_computed = "computed" in raw
        if has_value and has_computed:
            raise ctx.fail(path, "'value' and 'computed' are mutually exclusive")
        value: int | None = None
        computed: str | None = None
        bias = 0
        algorithm: str | None = None
        if has_value:
            value = _int(ctx, raw, "value", path, 0, (1 << bits) - 1)
            if "default" in raw:
                raise ctx.fail(f"{path}.default", "only valid on input fields")
            fkind = FieldKind.FIXED
        elif has_computed:
            computed = _str(ctx, raw, "computed", path)
            if computed.startswith("python:"):
                raise ctx.fail(
                    f"{path}.computed", "the 'python:' escape hatch is not implemented yet"
                )
            if computed not in COMPUTED_KINDS:
                raise ctx.fail(
                    f"{path}.computed",
                    f"unknown kind {computed!r}; known: {', '.join(sorted(COMPUTED_KINDS))}",
                )
            if "default" in raw:
                raise ctx.fail(f"{path}.default", "only valid on input fields")
            fkind = FieldKind.COMPUTED
        else:
            if "default" in raw:
                value = _int(ctx, raw, "default", path, 0, (1 << bits) - 1)
            fkind = FieldKind.INPUT
        if "bias" in raw:
            if computed != "length":
                raise ctx.fail(f"{path}.bias", "only valid on computed = 'length'")
            bias = _int(ctx, raw, "bias", path, -1024, 1024)
        if "algorithm" in raw:
            if computed != "checksum":
                raise ctx.fail(f"{path}.algorithm", "only valid on computed = 'checksum'")
            algorithm = _str(ctx, raw, "algorithm", path)
        if computed == "checksum":
            if algorithm is None:
                raise ctx.fail(path, "checksum requires 'algorithm'")
            algo = checksum.REGISTRY.get(algorithm)
            if algo is None:
                raise ctx.fail(
                    f"{path}.algorithm",
                    f"unknown algorithm {algorithm!r}; "
                    f"known: {', '.join(sorted(checksum.REGISTRY))}",
                )
            if algo.width_bits != bits:
                raise ctx.fail(
                    f"{path}.bits", f"{algorithm} needs {algo.width_bits} bits, got {bits}"
                )
            if shift % 8 or bits % 8:
                raise ctx.fail(path, "checksum field must be byte aligned")
        check = True
        if "check" in raw:
            if not isinstance(raw["check"], bool):
                raise ctx.fail(f"{path}.check", "must be true or false")
            check = raw["check"]
            if not check and fkind is not FieldKind.FIXED:
                raise ctx.fail(f"{path}.check", "only valid on fixed fields (value = N)")
        fields.append(
            HeaderField(
                name=raw["name"],
                bits=bits,
                shift=shift,
                kind=fkind,
                value=value,
                computed=computed,
                bias=bias,
                algorithm=algorithm,
                check=check,
                description=raw.get("description", ""),
            )
        )

    by_name = MappingProxyType({f.name: f for f in fields})

    def single(kind_name: str) -> HeaderField | None:
        matches = [f for f in fields if f.computed == kind_name]
        if len(matches) > 1:
            raise ctx.fail("fields", f"at most one computed = '{kind_name}' field is allowed")
        return matches[0] if matches else None

    length_field = single("length")
    sequence_field = single("sequence")
    checksum_field = single("checksum")
    single("time_seconds")
    single("time_subseconds")

    function_code_field: str | None = None
    if "function_code_field" in data:
        function_code_field = _str(ctx, data, "function_code_field", "")
        target = by_name.get(function_code_field)
        if target is None or target.kind is not FieldKind.INPUT:
            raise ctx.fail("function_code_field", "must name an input field")
    elif kind is ProfileKind.COMMAND:
        raise ctx.fail("function_code_field", "required for command profiles")

    mid_fields: tuple[str, ...] = ()
    mid_bits = 0
    mid_window: tuple[int, int, int, int] | None = None
    if "mid_fields" in data:
        raw_mid = data["mid_fields"]
        if (
            not isinstance(raw_mid, list)
            or not raw_mid
            or not all(isinstance(n, str) for n in raw_mid)
        ):
            raise ctx.fail("mid_fields", "must be a non-empty array of field names")
        mid_fields = tuple(raw_mid)
        order = [f.name for f in fields]
        for n in mid_fields:
            if n not in by_name:
                raise ctx.fail("mid_fields", f"unknown field {n!r}")
            if by_name[n].kind is FieldKind.INPUT or by_name[n].computed not in (None, "from_mid"):
                raise ctx.fail("mid_fields", f"field {n!r} must be fixed or computed = 'from_mid'")
        start = order.index(mid_fields[0])
        if order[start : start + len(mid_fields)] != list(mid_fields):
            raise ctx.fail("mid_fields", "fields must be contiguous and in header order")
        mid_bits = sum(by_name[n].bits for n in mid_fields)
        if mid_bits > 64:
            raise ctx.fail("mid_fields", "message ID wider than 64 bits")
        bit_start = total_bits - by_name[mid_fields[0]].shift - by_name[mid_fields[0]].bits
        byte_o = bit_start // 8
        bit_end = bit_start + mid_bits
        nbytes = (bit_end + 7) // 8 - byte_o
        shift = nbytes * 8 - (bit_start - byte_o * 8) - mid_bits
        mid_window = (byte_o, nbytes, shift, (1 << mid_bits) - 1)
    for f in fields:
        if f.computed == "from_mid" and f.name not in mid_fields:
            raise ctx.fail(
                f"fields.{f.name}", "computed = 'from_mid' needs the field in mid_fields"
            )

    return HeaderProfile(
        name=name,
        kind=kind,
        fields=tuple(fields),
        size_bytes=total_bits // 8,
        total_bits=total_bits,
        function_code_field=function_code_field,
        mid_fields=mid_fields,
        mid_bits=mid_bits,
        mid_window=mid_window,
        by_name=by_name,
        length_field=length_field,
        sequence_field=sequence_field,
        checksum_field=checksum_field,
        source=source,
    )


def load_profile(path: Path) -> HeaderProfile:
    """Read, parse and validate a header profile TOML file."""
    try:
        with path.open("rb") as handle:
            raw = handle.read(MAX_PROFILE_FILE_BYTES + 1)
    except OSError as exc:
        raise IronbirdError(
            Code.HDR_FILE, f"{path}: cannot read header profile ({exc.strerror or exc})", cause=exc
        ) from exc
    if len(raw) > MAX_PROFILE_FILE_BYTES:
        raise IronbirdError(Code.HDR_FILE, f"{path}: header profile larger than 64 KiB")
    try:
        data = tomllib.loads(raw.decode("utf-8"))
    except (ValueError, RecursionError) as exc:
        # TOMLDecodeError and UnicodeDecodeError are ValueErrors; so is the int-digit limit.
        # RecursionError is deeply nested arrays/tables.
        raise IronbirdError(Code.HDR_FILE, f"{path}: invalid TOML ({exc})", cause=exc) from exc
    return parse_profile(data, str(path))


# ---------------------------------------------------------------------------------------
# MID helpers
# ---------------------------------------------------------------------------------------


def split_mid(profile: HeaderProfile, mid: int) -> dict[str, int] | str:
    """Map a message ID onto the profile's MID fields, or return why it does not fit."""
    if not profile.mid_fields:
        return "header profile defines no message ID fields"
    if not 0 <= mid < (1 << profile.mid_bits):
        return f"does not fit the {profile.mid_bits}-bit message ID of profile {profile.name}"
    out: dict[str, int] = {}
    remaining = profile.mid_bits
    for name in profile.mid_fields:
        f = profile.by_name[name]
        remaining -= f.bits
        part = (mid >> remaining) & f.mask
        if f.kind is FieldKind.FIXED and part != f.value:
            return (
                f"conflicts with header field {name} (profile {profile.name} fixes it to "
                f"{f.value}, the ID has {part})"
            )
        out[name] = part
    return out


def peek_mid(profile: HeaderProfile, data: bytes | bytearray | memoryview) -> int | None:
    """Read the MID without validating anything else; ``None`` if too short or no MID."""
    window = profile.mid_window
    if window is None:
        return None
    offset, count, shift, mask = window
    if len(data) < offset + count:
        return None
    return (int.from_bytes(data[offset : offset + count], "big") >> shift) & mask


# ---------------------------------------------------------------------------------------
# Pack
# ---------------------------------------------------------------------------------------


def pack(
    profile: HeaderProfile,
    payload: bytes | bytearray,
    *,
    mid: int | None = None,
    inputs: Mapping[str, int] | None = None,
    sequence: int = 0,
    now: float = 0.0,
    tamper: Mapping[str, int] | None = None,
) -> bytes:
    """Build header + payload. ``tamper`` adds a delta to computed fields (fault injection)."""
    supplied = inputs or {}
    tampered = tamper or {}
    for key in supplied:
        f = profile.by_name.get(key)
        if f is None or f.kind is not FieldKind.INPUT:
            raise IronbirdError(
                Code.HDR_PACK, f"profile {profile.name}: {key!r} is not an input field"
            )
    for key in tampered:
        f = profile.by_name.get(key)
        if f is None or f.kind is not FieldKind.COMPUTED:
            raise IronbirdError(
                Code.HDR_PACK, f"profile {profile.name}: cannot tamper with {key!r}"
            )

    total = profile.size_bytes + len(payload)
    mid_parts: dict[str, int] = {}
    if profile.mid_fields:
        if mid is None:
            raise IronbirdError(Code.ENC_FIT, f"profile {profile.name} needs a message ID")
        split = split_mid(profile, mid)
        if isinstance(split, str):
            raise IronbirdError(Code.ENC_FIT, f"MID 0x{mid:04X} {split}")
        mid_parts = split

    word = 0
    fractional = now - int(now)
    for f in profile.fields:
        value: int
        match f.kind:
            case FieldKind.FIXED:
                assert f.value is not None
                value = f.value
            case FieldKind.INPUT:
                if f.name in supplied:
                    value = supplied[f.name]
                elif f.value is not None:
                    value = f.value
                else:
                    raise IronbirdError(
                        Code.HDR_PACK, f"profile {profile.name}: no value for input field {f.name}"
                    )
                if not 0 <= value <= f.mask:
                    raise IronbirdError(
                        Code.HDR_PACK,
                        f"{f.name}={value} does not fit {f.bits} bits (max {f.mask})",
                    )
            case FieldKind.COMPUTED:
                match f.computed:
                    case "length":
                        value = total + f.bias
                        if not 0 <= value <= f.mask:
                            raise IronbirdError(
                                Code.ENC_FIT,
                                f"packet of {total} bytes does not fit the {f.bits}-bit "
                                f"{f.name} field",
                            )
                    case "sequence":
                        value = sequence & f.mask
                    case "from_mid":
                        value = mid_parts[f.name]
                    case "time_seconds":
                        value = int(now) & f.mask
                    case "time_subseconds":
                        value = int(fractional * (1 << f.bits)) & f.mask
                    case "checksum":
                        value = 0
                    case _:  # pragma: no cover - validated at profile load
                        raise AssertionError(f"unvalidated computed kind {f.computed!r}")
                if f.computed != "checksum" and f.name in tampered:
                    value = (value + tampered[f.name]) & f.mask
        word |= value << f.shift

    header_bytes = word.to_bytes(profile.size_bytes, "big")
    packet = header_bytes + bytes(payload)
    assert len(packet) == total

    cf = profile.checksum_field
    if cf is not None:
        assert cf.algorithm is not None
        value = checksum.REGISTRY[cf.algorithm].compute(packet)
        value = (value + tampered.get(cf.name, 0)) & cf.mask
        word |= value << cf.shift
        packet = word.to_bytes(profile.size_bytes, "big") + bytes(payload)
    return packet


# ---------------------------------------------------------------------------------------
# Unpack (decode boundary for headers)
# ---------------------------------------------------------------------------------------


def unpack(
    profile: HeaderProfile,
    data: bytes | bytearray | memoryview,
    *,
    max_size: int = DEFAULT_MAX_PACKET_BYTES,
) -> Header | DecodeError:
    """Validate and unpack the header of one whole packet (one datagram)."""
    n = len(data)
    mid = peek_mid(profile, data)
    if n > max_size:
        return DecodeError(
            Code.DEC_OVERSIZE, max_size, f"packet of {n} bytes exceeds cap {max_size}", mid
        )
    size = profile.size_bytes
    if n < size:
        return DecodeError(
            Code.DEC_SHORT, n, f"packet of {n} bytes is shorter than the {size}-byte header", mid
        )
    word = int.from_bytes(data[:size], "big")
    values = {f.name: (word >> f.shift) & f.mask for f in profile.fields}

    lf = profile.length_field
    if lf is not None:
        declared = values[lf.name] - lf.bias
        if declared != n:
            return DecodeError(
                Code.DEC_LENGTH,
                profile.byte_offset(lf),
                f"{lf.name} field says {declared} bytes, got {n}",
                mid,
            )
    for f in profile.fields:
        if f.kind is FieldKind.FIXED and f.check and values[f.name] != f.value:
            return DecodeError(
                Code.DEC_FIXED,
                profile.byte_offset(f),
                f"{f.name} is {values[f.name]}, expected {f.value}",
                mid,
            )
    cf = profile.checksum_field
    if cf is not None:
        assert cf.algorithm is not None
        zeroed = (word & ~(cf.mask << cf.shift)).to_bytes(size, "big")
        expected = checksum.REGISTRY[cf.algorithm].compute(zeroed + bytes(data[size:]))
        if expected != values[cf.name]:
            return DecodeError(
                Code.DEC_CHECKSUM,
                profile.byte_offset(cf),
                f"{cf.name} is 0x{values[cf.name]:02X}, expected 0x{expected:02X}",
                mid,
            )
    sf = profile.sequence_field
    return Header(
        values=values,
        mid=mid,
        size=size,
        sequence=values[sf.name] if sf is not None else None,
    )
