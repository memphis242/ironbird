"""User-defined header profiles and the generic bitfield codec that packs/unpacks them.

A profile (TOML) declares header fields in wire order with bit widths. Fields are big-endian
bitfields packed MSB-first, so a profile's total width must be a whole number of bytes; a field
that starts on a byte boundary and fills whole bytes may be little-endian (``endian =
"little"``). Each field is one of:

* fixed: ``value = N`` (verified on unpack unless ``check = false``)
* input: no ``value``/``computed``; supplied by the caller by name, optional ``default``
* computed: ``computed = "<kind>"`` with kinds listed in ``COMPUTED_KINDS``

An optional ``[trailer]`` table (``[[trailer.fields]]``, fixed fields and one checksum) follows
the payload. A checksum covers its ``span`` of the packet (default: all of it). ``[time]`` sets
the epoch and leap seconds that turn the header's time fields into a UTC time.

``unpack`` is the only place header bytes are parsed. It returns a :class:`Header` or a typed
:class:`~ironbird.errors.DecodeError`, never an exception, and checks every length first.
"""

from __future__ import annotations

import re
import tomllib
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final, Literal

from ironbird import checksum
from ironbird.errors import Code, DecodeError, IronbirdError

DEFAULT_MAX_PACKET_BYTES: Final = 2048
MAX_PACKET_BYTES: Final = 65535  # the CCSDS 16-bit length field bounds any packet
MAX_HEADER_BYTES: Final = 64
MAX_TRAILER_BYTES: Final = 64
MAX_PROFILE_FILE_BYTES: Final = 64 * 1024
_NAME_RE: Final = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,63}")
MAX_DESCRIPTION_CHARS: Final = 1024
MAX_EPOCH_TEXT_CHARS: Final = 64
EPOCH_MIN_S: Final = -2_208_988_800  # 1900-01-01T00:00:00Z
EPOCH_MAX_S: Final = 7_258_118_400  # 2200-01-01T00:00:00Z
MAX_LEAP_SECONDS: Final = 1000
_UNIX_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)
_NS: Final = 1_000_000_000
Buffer = bytes | bytearray | memoryview
ByteOrder = Literal["big", "little"]

# One read of the message ID: (byte offset, byte count, right shift, mask, output shift, order).
# The MID is the OR of the windows' ``(int.from_bytes(...) >> shift) & mask) << output shift``.
MidWindow = tuple[int, int, int, int, int, ByteOrder]


class FieldKind(StrEnum):
    FIXED = "fixed"
    INPUT = "input"
    COMPUTED = "computed"


class ProfileKind(StrEnum):
    COMMAND = "command"
    TELEMETRY = "telemetry"


class SpanKind(StrEnum):
    PACKET = "packet"  # the whole packet
    HEADER = "header"  # the header only
    PAYLOAD = "payload"  # between the header and the trailer
    RANGE = "range"  # { start = N, end = -M }: byte N up to M bytes before the packet end


COMPUTED_KINDS: Final[Mapping[str, str]] = MappingProxyType(
    {
        "length": "total packet bytes plus `bias` (CCSDS with a 6-byte primary header: bias = -7)",
        "sequence": "per-MID sequence counter, wraps at the field width",
        "from_mid": "bits taken from the message ID (field must be listed in mid_fields)",
        "checksum": "named algorithm over the field's span (default: the whole packet) with the "
        "field zeroed",
        "time_seconds": "whole seconds of the send time since the [time] epoch (default: the Unix "
        "epoch), truncated to the field width",
        "time_subseconds": "fraction of a second scaled to the field width",
    }
)


@dataclass(frozen=True, slots=True)
class Span:
    """The bytes of a packet a checksum covers."""

    kind: SpanKind = SpanKind.PACKET
    start: int = 0  # RANGE: bytes from the packet start
    end: int = 0  # RANGE: zero or negative, bytes before the packet end where the span stops


@dataclass(frozen=True, slots=True)
class HeaderField:
    name: str
    bits: int
    shift: int  # bit position of the field's LSB within its section's word
    kind: FieldKind
    value: int | None = None  # fixed value, or input default
    computed: str | None = None
    bias: int = 0
    algorithm: str | None = None
    check: bool = True
    description: str = ""
    little: bool = False  # byte-aligned whole-byte field stored least significant byte first
    trailer: bool = False  # in the trailer section (after the payload) rather than the header
    offset: int = 0  # byte offset of the field's first byte within its section
    span: Span = Span()  # checksum fields only
    mask: int = field(init=False, repr=False, compare=False)  # (1 << bits) - 1, set once
    swap: bool = field(init=False, repr=False, compare=False)  # little-endian over several bytes

    def __post_init__(self) -> None:
        object.__setattr__(self, "mask", (1 << self.bits) - 1)
        object.__setattr__(self, "swap", self.little and self.bits > 8)
        assert not self.little or (self.bits % 8 == 0 and self.shift % 8 == 0)

    def swapped(self, value: int) -> int:
        """``value`` with its bytes reversed (wire form of a little-endian field and back)."""
        assert self.swap and 0 <= value <= self.mask
        return int.from_bytes(value.to_bytes(self.bits // 8, "little"), "big")


@dataclass(frozen=True, slots=True)
class TimeSpec:
    """How the header's time fields become a UTC time."""

    epoch_s: int  # Unix seconds of the counter's zero
    leap_seconds: int  # counter = UTC - epoch + leap_seconds (TAI-like counters have > 0)
    seconds: HeaderField
    subseconds: HeaderField | None

    @property
    def offset_s(self) -> int:
        """Counter seconds minus Unix seconds."""
        return self.leap_seconds - self.epoch_s

    def to_ns(self, seconds: int, subseconds: int) -> int:
        """UTC nanoseconds since the Unix epoch of a counter reading."""
        fraction = 0
        if self.subseconds is not None:
            fraction = (subseconds * _NS) >> self.subseconds.bits
        return (seconds - self.offset_s) * _NS + fraction


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
    mid_windows: tuple[MidWindow, ...]
    by_name: Mapping[str, HeaderField]
    length_field: HeaderField | None
    sequence_field: HeaderField | None
    checksum_field: HeaderField | None
    source: str = "<memory>"
    # One compare checks every fixed field: ``(word & fixed_mask) == fixed_value`` over the
    # header word as it is on the wire (fixed fields with ``check = false`` are left out).
    fixed_mask: int = 0
    fixed_value: int = 0
    trailer_fields: tuple[HeaderField, ...] = ()
    trailer_size: int = 0
    trailer_fixed_mask: int = 0
    trailer_fixed_value: int = 0
    time: TimeSpec | None = None
    mid_need: int = 0  # the shortest prefix that holds every MID window
    # Bytes from the packet start that hold the whole length field, and the shift of the field
    # within a big-endian read of that many bytes; None without a length field.
    length_window: int | None = None
    length_shift: int = 0
    # Derived once, so the per-packet checks read attributes instead of computing:
    all_fields: tuple[HeaderField, ...] = field(init=False, repr=False, compare=False)
    swapped_fields: tuple[HeaderField, ...] = field(init=False, repr=False, compare=False)
    min_bytes: int = field(init=False, compare=False)  # header and trailer, empty payload
    # The one window of a MID read from a single run of bytes: (offset, count, shift, mask,
    # order), or None when the MID needs several windows (or there is none).
    mid_single: tuple[int, int, int, int, ByteOrder] | None = field(
        init=False, repr=False, compare=False
    )
    # Several big-endian windows read through one integer: (prefix bytes, ((shift, mask, output
    # shift), ...)); None for a MID of one window or with a little-endian window.
    mid_flat: tuple[int, tuple[tuple[int, int, int], ...]] | None = field(
        init=False, repr=False, compare=False
    )
    checksum_compute: Callable[[Buffer], int] | None = field(init=False, repr=False, compare=False)
    checksum_zeroed: Callable[[Buffer, int], int] | None = field(
        init=False, repr=False, compare=False
    )  # the algorithm's no-copy "field read as zero" form, when it has one
    checksum_whole: bool = field(init=False, repr=False, compare=False)  # span is the packet
    checksum_zero: bytes = field(init=False, repr=False, compare=False)  # an empty field's bytes

    def __post_init__(self) -> None:
        set_ = object.__setattr__
        set_(self, "all_fields", self.fields + self.trailer_fields)
        set_(self, "swapped_fields", tuple(f for f in self.fields if f.swap))
        set_(self, "min_bytes", self.size_bytes + self.trailer_size)
        single = None
        if len(self.mid_windows) == 1:
            offset, count, shift, mask, _, order = self.mid_windows[0]
            single = (offset, count, shift, mask, order)
        set_(self, "mid_single", single)
        flat = None
        if len(self.mid_windows) > 1 and all(w[5] == "big" for w in self.mid_windows):
            need = max(w[0] + w[1] for w in self.mid_windows)
            flat = (
                need,
                tuple(((need - w[0] - w[1]) * 8 + w[2], w[3], w[4]) for w in self.mid_windows),
            )
        set_(self, "mid_flat", flat)
        cf = self.checksum_field
        compute = zeroed = None
        if cf is not None:
            assert cf.algorithm is not None
            algorithm = checksum.REGISTRY[cf.algorithm]
            compute, zeroed = algorithm.compute, algorithm.compute_zeroed
        set_(self, "checksum_compute", compute)
        set_(self, "checksum_zeroed", zeroed)
        set_(self, "checksum_whole", cf is not None and cf.span.kind is SpanKind.PACKET)
        set_(self, "checksum_zero", bytes(cf.bits // 8) if cf is not None else b"")

    def byte_offset(self, field: HeaderField) -> int:
        """Offset of the field's first byte within its section (header or trailer)."""
        return field.offset

    @property
    def input_fields(self) -> tuple[HeaderField, ...]:
        return tuple(f for f in self.fields if f.kind is FieldKind.INPUT)

    def frame_length(self, prefix: Buffer) -> int | None:
        """The packet length the length field of ``prefix`` declares, from a stream's bytes.

        ``prefix`` starts at the first byte of a candidate packet. ``None`` means too few bytes
        to read the field yet (fewer than :attr:`length_window`); otherwise the total packet
        bytes the field claims (field value minus ``bias``). Untrusted input can make that
        negative (a positive ``bias`` with a small field), smaller than the header and trailer,
        or larger than any cap: this method does not judge it, and the caller must reject
        anything outside ``min_bytes`` .. its packet cap before it sizes a read. Needs a length
        field.
        """
        window = self.length_window
        assert window is not None and self.length_field is not None, "profile has no length field"
        if len(prefix) < window:
            return None
        lf = self.length_field
        value = (int.from_bytes(prefix[:window], "big") >> self.length_shift) & lf.mask
        if lf.swap:
            value = lf.swapped(value)
        return value - lf.bias


@dataclass(frozen=True, slots=True)
class Header:
    """A successfully unpacked and validated header."""

    values: Mapping[str, int]
    mid: int | None
    size: int
    sequence: int | None
    profile: HeaderProfile | None = field(default=None, repr=False, compare=False)

    @property
    def time_ns(self) -> int | None:
        """UTC nanoseconds since the Unix epoch of the header's time fields; ``None`` when the
        profile has none."""
        spec = None if self.profile is None else self.profile.time
        if spec is None:
            return None
        sub = self.values[spec.subseconds.name] if spec.subseconds is not None else 0
        return spec.to_ns(self.values[spec.seconds.name], sub)

    # ``values`` is a read-only view, which the copy module cannot copy; the header is
    # immutable, so sharing it is a faithful copy.
    def __copy__(self) -> Header:
        return self

    def __deepcopy__(self, memo: dict[int, object]) -> Header:
        return self


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


_FIELD_KEYS: Final = {
    "name",
    "bits",
    "value",
    "default",
    "computed",
    "bias",
    "algorithm",
    "check",
    "description",
    "endian",
    "span",
}


def _parse_span(ctx: _Ctx, raw: Mapping[str, Any], path: str) -> Span:
    value = raw["span"]
    where = f"{path}.span"
    if isinstance(value, str):
        match value:
            case "packet" | "header" | "payload":
                return Span(SpanKind(value))
            case _:
                raise ctx.fail(
                    where,
                    f"{value[:40]!r} is not a span; use 'packet', 'header', 'payload' or "
                    "{ start = N, end = -M }",
                )
    if not isinstance(value, dict):
        raise ctx.fail(
            where, "must be 'packet', 'header', 'payload' or a table { start = N, end = -M }"
        )
    _expect_keys(ctx, value, where, {"start", "end"}, {"start", "end"})
    start = _int(ctx, value, "start", where, 0, MAX_PACKET_BYTES)
    end = _int(ctx, value, "end", where, -MAX_PACKET_BYTES, 0)
    if start - end > MAX_PACKET_BYTES:
        raise ctx.fail(where, f"start {start} and end {end} leave no room in a packet")
    return Span(SpanKind.RANGE, start, end)


def _parse_section(
    ctx: _Ctx, raw_fields: object, path: str, *, trailer: bool, names: set[str]
) -> tuple[list[HeaderField], int]:
    """Validate one section's field tables (``path`` is e.g. ``fields``) and build its fields."""
    cap = 64 if trailer else 256
    limit = MAX_TRAILER_BYTES if trailer else MAX_HEADER_BYTES
    what = "trailer" if trailer else "header"
    if not isinstance(raw_fields, list) or not raw_fields:
        raise ctx.fail(path, "must be a non-empty array of tables")
    if len(raw_fields) > cap:
        raise ctx.fail(path, f"too many fields (max {cap})")

    # First pass: validate each field and the section's total width.
    parsed: list[dict[str, Any]] = []
    total_bits = 0
    for i, raw in enumerate(raw_fields):
        fpath = f"{path}[{i}]"
        if not isinstance(raw, dict):
            raise ctx.fail(fpath, "must be a table")
        _expect_keys(ctx, raw, fpath, _FIELD_KEYS, {"name", "bits"})
        fname = _str(ctx, raw, "name", fpath)
        if not _NAME_RE.fullmatch(fname):
            raise ctx.fail(f"{fpath}.name", "must be an identifier")
        if fname in names:
            raise ctx.fail(f"{fpath}.name", f"duplicate field name {fname!r}")
        names.add(fname)
        bits = _int(ctx, raw, "bits", fpath, 1, 64)
        total_bits += bits
        if "description" in raw:
            _str(ctx, raw, "description", fpath)
        parsed.append(raw)
    if total_bits % 8:
        raise ctx.fail(path, f"total width is {total_bits} bits, not a whole number of bytes")
    if total_bits // 8 > limit:
        raise ctx.fail(path, f"{what} exceeds {limit} bytes")

    # Second pass: classify fields and compute shifts.
    fields: list[HeaderField] = []
    consumed = 0
    for i, raw in enumerate(parsed):
        fpath = f"{path}[{i}]"
        bits = raw["bits"]
        consumed += bits
        shift = total_bits - consumed
        has_value = "value" in raw
        has_computed = "computed" in raw
        if has_value and has_computed:
            raise ctx.fail(fpath, "'value' and 'computed' are mutually exclusive")
        value: int | None = None
        computed: str | None = None
        bias = 0
        algorithm: str | None = None
        if has_value:
            value = _int(ctx, raw, "value", fpath, 0, (1 << bits) - 1)
            if "default" in raw:
                raise ctx.fail(f"{fpath}.default", "only valid on input fields")
            fkind = FieldKind.FIXED
        elif has_computed:
            computed = _str(ctx, raw, "computed", fpath)
            if computed.startswith("python:"):
                raise ctx.fail(
                    f"{fpath}.computed", "the 'python:' escape hatch is not implemented yet"
                )
            if computed not in COMPUTED_KINDS:
                raise ctx.fail(
                    f"{fpath}.computed",
                    f"unknown kind {computed!r}; known: {', '.join(sorted(COMPUTED_KINDS))}",
                )
            if "default" in raw:
                raise ctx.fail(f"{fpath}.default", "only valid on input fields")
            fkind = FieldKind.COMPUTED
        else:
            if "default" in raw:
                value = _int(ctx, raw, "default", fpath, 0, (1 << bits) - 1)
            fkind = FieldKind.INPUT
        if trailer and computed != "checksum" and fkind is not FieldKind.FIXED:
            raise ctx.fail(fpath, "a trailer field must be fixed (value = N) or a checksum")
        if "bias" in raw:
            if computed != "length":
                raise ctx.fail(f"{fpath}.bias", "only valid on computed = 'length'")
            bias = _int(ctx, raw, "bias", fpath, -1024, 1024)
        if "algorithm" in raw:
            if computed != "checksum":
                raise ctx.fail(f"{fpath}.algorithm", "only valid on computed = 'checksum'")
            algorithm = _str(ctx, raw, "algorithm", fpath)
        span = Span()
        if "span" in raw:
            if computed != "checksum":
                raise ctx.fail(f"{fpath}.span", "only valid on computed = 'checksum'")
            span = _parse_span(ctx, raw, fpath)
        if computed == "checksum":
            if algorithm is None:
                raise ctx.fail(fpath, "checksum requires 'algorithm'")
            algo = checksum.REGISTRY.get(algorithm)
            if algo is None:
                raise ctx.fail(
                    f"{fpath}.algorithm",
                    f"unknown algorithm {algorithm!r}; "
                    f"known: {', '.join(sorted(checksum.REGISTRY))}",
                )
            if algo.width_bits != bits:
                raise ctx.fail(
                    f"{fpath}.bits", f"{algorithm} needs {algo.width_bits} bits, got {bits}"
                )
            if shift % 8 or bits % 8:
                raise ctx.fail(fpath, "checksum field must be byte aligned")
        little = False
        if "endian" in raw:
            order = _str(ctx, raw, "endian", fpath)
            if order not in ("big", "little"):
                raise ctx.fail(f"{fpath}.endian", f"must be 'big' or 'little', got {order[:40]!r}")
            little = order == "little"
            if little and (bits % 8 or shift % 8):
                raise ctx.fail(
                    f"{fpath}.endian",
                    "a little-endian field must start on a byte boundary and fill whole bytes "
                    f"(this one is {bits} bits at bit {total_bits - shift - bits} of the "
                    f"{what}); little-endian bitfields are ambiguous",
                )
        check = True
        if "check" in raw:
            if not isinstance(raw["check"], bool):
                raise ctx.fail(f"{fpath}.check", "must be true or false")
            check = raw["check"]
            if not check and fkind is not FieldKind.FIXED:
                raise ctx.fail(f"{fpath}.check", "only valid on fixed fields (value = N)")
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
                little=little,
                trailer=trailer,
                offset=(total_bits - shift - bits) // 8,
                span=span,
            )
        )
    return fields, total_bits


def _parse_epoch(ctx: _Ctx, value: object, path: str) -> int:
    """Unix seconds of a TOML date-time or ISO 8601 string that names a whole second in UTC or
    with an offset."""
    moment: datetime
    if isinstance(value, datetime):
        moment = value
    elif isinstance(value, str):
        if len(value) > MAX_EPOCH_TEXT_CHARS:
            raise ctx.fail(path, f"longer than {MAX_EPOCH_TEXT_CHARS} characters")
        try:
            moment = datetime.fromisoformat(value)
        except ValueError:
            raise ctx.fail(
                path, f"{value[:40]!r} is not an ISO 8601 time such as 1980-01-01T00:00:00Z"
            ) from None
    else:
        raise ctx.fail(path, "must be an ISO 8601 time string such as 1980-01-01T00:00:00Z")
    offset = moment.utcoffset()
    if moment.tzinfo is None or offset is None:
        raise ctx.fail(path, "needs a zone (Z or +HH:MM): a naive time names no instant")
    if offset.total_seconds() % 60:
        raise ctx.fail(path, "zone offset must be a whole number of minutes (Z or +HH:MM)")
    if moment.microsecond:
        raise ctx.fail(path, "must be a whole second")
    try:
        delta = moment - _UNIX_EPOCH
    except OverflowError:  # pragma: no cover - datetime subtraction of aware values
        raise ctx.fail(path, "out of range") from None
    seconds = delta.days * 86400 + delta.seconds
    if not EPOCH_MIN_S <= seconds <= EPOCH_MAX_S:
        raise ctx.fail(path, "must be between the years 1900 and 2200")
    return seconds


def _parse_time(ctx: _Ctx, data: Mapping[str, Any], fields: list[HeaderField]) -> TimeSpec | None:
    seconds = next((f for f in fields if f.computed == "time_seconds"), None)
    sub = next((f for f in fields if f.computed == "time_subseconds"), None)
    epoch_s = 0
    leap = 0
    if "time" in data:
        table = data["time"]
        if not isinstance(table, dict):
            raise ctx.fail("time", "must be a table")
        _expect_keys(ctx, table, "time", {"epoch", "leap_seconds"}, set())
        if seconds is None:
            raise ctx.fail("time", "needs a field with computed = 'time_seconds'")
        if "epoch" in table:
            epoch_s = _parse_epoch(ctx, table["epoch"], "time.epoch")
        if "leap_seconds" in table:
            leap = _int(ctx, table, "leap_seconds", "time", -MAX_LEAP_SECONDS, MAX_LEAP_SECONDS)
    if seconds is None:
        return None
    return TimeSpec(epoch_s, leap, seconds, sub)


def _mid_windows(
    fields: list[HeaderField], names: tuple[str, ...], total_bits: int
) -> tuple[MidWindow, ...]:
    """Windows that read the MID (the fields' values joined in ``names`` order) from the wire."""
    by_name = {f.name: f for f in fields}
    segments: list[tuple[int, int, bool]] = []  # (header bit position, bits, little-endian)
    for name in names:
        f = by_name[name]
        start = total_bits - f.shift - f.bits
        if segments and not f.swap:
            prev_start, prev_bits, prev_swap = segments[-1]
            if not prev_swap and prev_start + prev_bits == start:  # the wire bits continue
                segments[-1] = (prev_start, prev_bits + f.bits, False)
                continue
        segments.append((start, f.bits, f.swap))
    windows: list[MidWindow] = []
    remaining = sum(bits for _, bits, _ in segments)
    for start, bits, swap in segments:
        remaining -= bits
        byte_o = start // 8
        if swap:
            windows.append((byte_o, bits // 8, 0, (1 << bits) - 1, remaining, "little"))
            continue
        nbytes = (start + bits + 7) // 8 - byte_o
        shift = nbytes * 8 - (start - byte_o * 8) - bits
        windows.append((byte_o, nbytes, shift, (1 << bits) - 1, remaining, "big"))
    return tuple(windows)


def _check_span_fits(ctx: _Ctx, cf: HeaderField, header_bytes: int, trailer_bytes: int) -> None:
    """Reject a span that cuts through its own checksum field wherever the packet ends."""
    span = cf.span
    if span.kind is not SpanKind.RANGE:
        return
    width = cf.bits // 8
    if not cf.trailer:
        # The field sits at a fixed distance from the packet start, so the start is exact.
        if cf.offset < span.start < cf.offset + width:
            raise ctx.fail(
                f"{cf.name}.span", f"starts at byte {span.start}, inside the checksum field itself"
            )
        return
    near = trailer_bytes - cf.offset - width  # bytes after the field
    far = trailer_bytes - cf.offset  # bytes from the field's start to the packet end
    if near < -span.end < far:
        raise ctx.fail(
            f"{cf.name}.span", f"ends {-span.end} bytes before the end, inside the checksum field"
        )


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
            "trailer",
            "time",
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
        raise ctx.fail(
            "byte_order",
            "only 'big' (MSB-first bitfields) is supported; set endian = 'little' on a "
            "byte-aligned whole-byte field instead",
        )

    names: set[str] = set()
    fields, total_bits = _parse_section(ctx, data["fields"], "fields", trailer=False, names=names)
    trailer_fields: list[HeaderField] = []
    trailer_bits = 0
    if "trailer" in data:
        table = data["trailer"]
        if not isinstance(table, dict):
            raise ctx.fail("trailer", "must be a table with a 'fields' array")
        _expect_keys(ctx, table, "trailer", {"fields"}, {"fields"})
        trailer_fields, trailer_bits = _parse_section(
            ctx, table["fields"], "trailer.fields", trailer=True, names=names
        )
    every = fields + trailer_fields
    by_name = MappingProxyType({f.name: f for f in every})

    def single(kind_name: str) -> HeaderField | None:
        matches = [f for f in every if f.computed == kind_name]
        if len(matches) > 1:
            raise ctx.fail("fields", f"at most one computed = '{kind_name}' field is allowed")
        return matches[0] if matches else None

    length_field = single("length")
    sequence_field = single("sequence")
    checksum_field = single("checksum")
    single("time_seconds")
    single("time_subseconds")
    if checksum_field is not None:
        _check_span_fits(ctx, checksum_field, total_bits // 8, trailer_bits // 8)
    time = _parse_time(ctx, data, fields)

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
    mid_windows: tuple[MidWindow, ...] = ()
    if "mid_fields" in data:
        raw_mid = data["mid_fields"]
        if (
            not isinstance(raw_mid, list)
            or not raw_mid
            or not all(isinstance(n, str) for n in raw_mid)
        ):
            raise ctx.fail("mid_fields", "must be a non-empty array of field names")
        mid_fields = tuple(raw_mid)
        in_header = {f.name for f in fields}
        seen: set[str] = set()
        for n in mid_fields:
            if n in seen:
                raise ctx.fail("mid_fields", f"field {n!r} is listed twice")
            seen.add(n)
            if n not in by_name:
                raise ctx.fail("mid_fields", f"unknown field {n!r}")
            if n not in in_header:
                raise ctx.fail("mid_fields", f"field {n!r} is in the trailer, not the header")
            if by_name[n].kind is FieldKind.INPUT or by_name[n].computed not in (None, "from_mid"):
                raise ctx.fail("mid_fields", f"field {n!r} must be fixed or computed = 'from_mid'")
        mid_bits = sum(by_name[n].bits for n in mid_fields)
        if mid_bits > 64:
            raise ctx.fail("mid_fields", "message ID wider than 64 bits")
        mid_windows = _mid_windows(fields, mid_fields, total_bits)
    for f in fields:
        if f.computed == "from_mid" and f.name not in mid_fields:
            raise ctx.fail(
                f"fields.{f.name}", "computed = 'from_mid' needs the field in mid_fields"
            )

    fixed_mask = fixed_value = 0
    for f in fields:
        if f.kind is FieldKind.FIXED and f.check:
            assert f.value is not None
            fixed_mask |= f.mask << f.shift
            fixed_value |= (f.swapped(f.value) if f.swap else f.value) << f.shift
    trailer_fixed_mask = trailer_fixed_value = 0
    for f in trailer_fields:
        if f.kind is FieldKind.FIXED and f.check:
            assert f.value is not None
            trailer_fixed_mask |= f.mask << f.shift
            trailer_fixed_value |= (f.swapped(f.value) if f.swap else f.value) << f.shift

    length_window: int | None = None
    length_shift = 0
    if length_field is not None:
        end_bit = total_bits - length_field.shift  # bit just past the field, from the start
        length_window = (end_bit + 7) // 8
        length_shift = length_window * 8 - end_bit

    return HeaderProfile(
        name=name,
        kind=kind,
        fields=tuple(fields),
        size_bytes=total_bits // 8,
        total_bits=total_bits,
        function_code_field=function_code_field,
        mid_fields=mid_fields,
        mid_bits=mid_bits,
        mid_windows=mid_windows,
        by_name=by_name,
        length_field=length_field,
        sequence_field=sequence_field,
        checksum_field=checksum_field,
        source=source,
        fixed_mask=fixed_mask,
        fixed_value=fixed_value,
        trailer_fields=tuple(trailer_fields),
        trailer_size=trailer_bits // 8,
        trailer_fixed_mask=trailer_fixed_mask,
        trailer_fixed_value=trailer_fixed_value,
        time=time,
        mid_need=max((w[0] + w[1] for w in mid_windows), default=0),
        length_window=length_window,
        length_shift=length_shift,
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
    return _profile_from_bytes(raw, str(path), trusted=True)


def load_profile_bytes(raw: bytes, source: str) -> HeaderProfile:
    """Parse and validate the content of a header profile read elsewhere (an embedded
    definition); ``source`` labels errors. The rules are those of :func:`load_profile`, and the
    ``python:`` escape hatch is refused outright: it is trusted local configuration, and this
    content is not."""
    return _profile_from_bytes(raw, source, trusted=False)


def _profile_from_bytes(raw: bytes, source: str, *, trusted: bool) -> HeaderProfile:
    if len(raw) > MAX_PROFILE_FILE_BYTES:
        raise IronbirdError(Code.HDR_FILE, f"{source}: header profile larger than 64 KiB")
    try:
        data = tomllib.loads(raw.decode("utf-8"))
    except (ValueError, RecursionError) as exc:
        # TOMLDecodeError and UnicodeDecodeError are ValueErrors; so is the int-digit limit.
        # RecursionError is deeply nested arrays/tables.
        raise IronbirdError(Code.HDR_FILE, f"{source}: invalid TOML ({exc})", cause=exc) from exc
    fields = data.get("fields")
    for index, entry in enumerate(fields if isinstance(fields, list) and not trusted else []):
        computed = entry.get("computed") if isinstance(entry, dict) else None
        if isinstance(computed, str) and computed.startswith("python:"):
            raise IronbirdError(
                Code.HDR_SCHEMA,
                f"{source}: fields[{index}].computed: the 'python:' escape hatch is not allowed "
                "in a profile that did not come from your own configuration",
            )
    return parse_profile(data, source)


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


def peek_mid(profile: HeaderProfile, data: Buffer) -> int | None:
    """Read the MID without validating anything else; ``None`` if too short or no MID."""
    single = profile.mid_single
    if single is not None:
        offset, count, shift, mask, order = single
        if len(data) < offset + count:
            return None
        return (int.from_bytes(data[offset : offset + count], order) >> shift) & mask
    flat = profile.mid_flat
    if flat is not None:
        need, parts = flat
        if len(data) < need:
            return None
        word = int.from_bytes(data[:need], "big")
        mid = 0
        for shift, mask, out in parts:
            mid |= ((word >> shift) & mask) << out
        return mid
    if not profile.mid_windows or len(data) < profile.mid_need:
        return None
    mid = 0
    for offset, count, shift, mask, out, order in profile.mid_windows:
        mid |= ((int.from_bytes(data[offset : offset + count], order) >> shift) & mask) << out
    return mid


# ---------------------------------------------------------------------------------------
# Checksum spans
# ---------------------------------------------------------------------------------------


def _checksum_lines(
    profile: HeaderProfile, cf: HeaderField
) -> tuple[tuple[int, int], tuple[int, int], tuple[int, int]]:
    """The span start, the span end and the checksum field offset of an ``n``-byte packet, each
    as ``(slope, constant)`` meaning ``slope * n + constant`` with a slope of 0 or 1."""
    hs = profile.size_bytes
    span = cf.span
    start, end = (0, 0), (1, 0)
    match span.kind:
        case SpanKind.PACKET:
            pass
        case SpanKind.HEADER:
            end = (0, hs)
        case SpanKind.PAYLOAD:
            start, end = (0, hs), (1, -profile.trailer_size)
        case SpanKind.RANGE:
            start, end = (0, span.start), (1, span.end)
    at = (1, cf.offset - profile.trailer_size) if cf.trailer else (0, cf.offset)
    return start, end, at


def checksum_plan(profile: HeaderProfile, cf: HeaderField, n: int) -> tuple[int, int, int] | str:
    """Where the checksum ``cf`` of an ``n``-byte packet looks: ``(start, end, zero_at)``, the
    bytes ``start:end`` it covers and the offset of the checksum field itself within them (-1
    when the field is outside the span), or the reason the span does not fit this packet."""
    (s_slope, s_const), (e_slope, e_const), (a_slope, a_const) = _checksum_lines(profile, cf)
    start, end = s_slope * n + s_const, e_slope * n + e_const
    if not 0 <= start <= end <= n:
        return f"checksum span {start}..{end} lies outside the {n}-byte packet"
    width = cf.bits // 8
    at = a_slope * n + a_const
    if start <= at and at + width <= end:
        return start, end, at - start
    if at + width <= start or at >= end:
        return start, end, -1
    return f"checksum span {start}..{end} cuts through the checksum field at byte {at}"


def checksum_probe_sizes(profile: HeaderProfile, cf: HeaderField, lo: int, hi: int) -> list[int]:
    """The packet sizes in ``lo..hi`` at which :func:`checksum_plan` can change its answer: the
    two ends and, for each pair of the span start, span end, checksum field and packet bounds,
    the size where they cross and its neighbours. Between two probes every comparison the plan
    makes keeps its result, so a plan that holds at all of them holds at every size."""
    start, end, at = _checksum_lines(profile, cf)
    width = cf.bits // 8
    lines = [
        (0, 0),
        (1, 0),
        start,
        end,
        at,
        (at[0], at[1] + width),
    ]
    found = {lo, hi}
    for i, (s1, c1) in enumerate(lines):
        for s2, c2 in lines[i + 1 :]:
            if s1 != s2:
                cross = (c2 - c1) // (s1 - s2)
                found.update(n for n in (cross - 1, cross, cross + 1) if lo <= n <= hi)
    return sorted(found)


def _checksum_of(profile: HeaderProfile, cf: HeaderField, data: Buffer, n: int) -> int | str:
    """The value the checksum field of the ``n``-byte packet ``data`` should hold, or why the
    span does not fit."""
    plan = checksum_plan(profile, cf, n)
    if isinstance(plan, str):
        return plan
    start, end, zero_at = plan
    covered = bytearray(memoryview(data)[start:end])
    if zero_at >= 0:
        covered[zero_at : zero_at + cf.bits // 8] = profile.checksum_zero
    assert profile.checksum_compute is not None
    return profile.checksum_compute(covered)


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
    """Build header + payload + trailer. ``tamper`` adds a delta to computed fields (fault
    injection)."""
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

    total = profile.size_bytes + len(payload) + profile.trailer_size
    mid_parts: dict[str, int] = {}
    if profile.mid_fields:
        if mid is None:
            raise IronbirdError(Code.ENC_FIT, f"profile {profile.name} needs a message ID")
        split = split_mid(profile, mid)
        if isinstance(split, str):
            raise IronbirdError(Code.ENC_FIT, f"MID 0x{mid:04X} {split}")
        mid_parts = split

    words = [0, 0]  # header word, trailer word, as on the wire
    fractional = now - int(now)
    for f in profile.all_fields:
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
                        assert profile.time is not None
                        counter = int(now) + profile.time.offset_s
                        if counter < 0:
                            raise IronbirdError(
                                Code.HDR_PACK,
                                f"profile {profile.name}: the send time precedes the time "
                                "epoch of its [time] table",
                            )
                        value = counter & f.mask
                    case "time_subseconds":
                        value = int(fractional * (1 << f.bits)) & f.mask
                    case "checksum":
                        value = 0
                    case _:  # pragma: no cover - validated at profile load
                        raise AssertionError(f"unvalidated computed kind {f.computed!r}")
                if f.computed != "checksum" and f.name in tampered:
                    value = (value + tampered[f.name]) & f.mask
        words[f.trailer] |= (f.swapped(value) if f.swap else value) << f.shift

    packet = bytearray(words[0].to_bytes(profile.size_bytes, "big"))
    packet += payload
    if profile.trailer_size:
        packet += words[1].to_bytes(profile.trailer_size, "big")
    assert len(packet) == total

    cf = profile.checksum_field
    if cf is not None:
        assert cf.algorithm is not None
        plan = checksum_plan(profile, cf, total)
        if isinstance(plan, str):
            raise IronbirdError(Code.ENC_FIT, f"profile {profile.name}: {plan}")
        start, end, _ = plan
        # The checksum field is still zero in ``packet``, so a span that holds it needs no
        # zeroing here.
        value = checksum.REGISTRY[cf.algorithm].compute(packet[start:end])
        value = (value + tampered.get(cf.name, 0)) & cf.mask
        at = cf.offset + (total - profile.trailer_size if cf.trailer else 0)
        width = cf.bits // 8
        packet[at : at + width] = value.to_bytes(width, "little" if cf.little else "big")
    return bytes(packet)


# ---------------------------------------------------------------------------------------
# Check and unpack (decode boundary for headers)
# ---------------------------------------------------------------------------------------


def oversize_error(profile: HeaderProfile, data: Buffer, size: int, max_size: int) -> DecodeError:
    """The error for a packet of ``size`` bytes on the wire above the cap (``data`` may be a
    prefix of it)."""
    return DecodeError(
        Code.DEC_OVERSIZE,
        max_size,
        f"packet of {size} bytes exceeds cap {max_size}",
        peek_mid(profile, data),
    )


def read_field(f: HeaderField, word: int) -> int:
    """The value of field ``f`` in its section's ``word`` (as on the wire), byte order applied."""
    value = (word >> f.shift) & f.mask
    return f.swapped(value) if f.swap else value


def trailer_word(profile: HeaderProfile, data: Buffer) -> int:
    """The trailer of a packet that is at least :attr:`HeaderProfile.min_bytes` long, as the
    big-endian integer it is on the wire (0 without a trailer)."""
    if not profile.trailer_size:
        return 0
    return int.from_bytes(data[len(data) - profile.trailer_size :], "big")


def check_word(
    profile: HeaderProfile,
    data: Buffer,
    *,
    max_size: int = DEFAULT_MAX_PACKET_BYTES,
) -> int | DecodeError:
    """Validate the header of one whole packet and return the header word, or the first error.

    This is the cheap half of the header boundary: size bounds, the length field, every fixed
    field (one mask compare per section) and the checksum. It builds no field table; the caller
    that wants one passes the word to :func:`parse_word`.
    """
    n = len(data)
    if n > max_size:
        return oversize_error(profile, data, n, max_size)
    size = profile.size_bytes
    tsize = profile.trailer_size
    if n < profile.min_bytes:
        around = f"{size}-byte header" + (f" and {tsize}-byte trailer" if tsize else "")
        return DecodeError(
            Code.DEC_SHORT,
            n,
            f"packet of {n} bytes is shorter than the {around}",
            peek_mid(profile, data),
        )
    word = int.from_bytes(data[:size], "big")

    lf = profile.length_field
    if lf is not None:
        declared = (word >> lf.shift) & lf.mask
        if lf.swap:
            declared = lf.swapped(declared)
        declared -= lf.bias
        if declared != n:
            return DecodeError(
                Code.DEC_LENGTH,
                lf.offset,
                f"{lf.name} field says {declared} bytes, got {n}",
                peek_mid(profile, data),
            )
    if word & profile.fixed_mask != profile.fixed_value:
        for f in profile.fields:  # the slow path only names the first offender
            got = read_field(f, word)
            if f.kind is FieldKind.FIXED and f.check and got != f.value:
                return DecodeError(
                    Code.DEC_FIXED,
                    f.offset,
                    f"{f.name} is {got}, expected {f.value}",
                    peek_mid(profile, data),
                )
        raise AssertionError("fixed-field mask disagrees with the fields")  # pragma: no cover
    tword = 0
    if tsize:
        tword = int.from_bytes(data[n - tsize :], "big")
        if tword & profile.trailer_fixed_mask != profile.trailer_fixed_value:
            for f in profile.trailer_fields:
                got = read_field(f, tword)
                if f.kind is FieldKind.FIXED and f.check and got != f.value:
                    return DecodeError(
                        Code.DEC_FIXED,
                        n - tsize + f.offset,
                        f"{f.name} is {got}, expected {f.value}",
                        peek_mid(profile, data),
                    )
            raise AssertionError("trailer mask disagrees with the fields")  # pragma: no cover
    cf = profile.checksum_field
    if cf is not None:
        compute = profile.checksum_compute
        assert compute is not None
        at = cf.offset + n - tsize if cf.trailer else cf.offset
        expected: int | str
        if profile.checksum_whole:
            zero = profile.checksum_zero
            zeroed = profile.checksum_zeroed
            if zeroed is not None:
                assert len(zero) == 1  # only the 8-bit XOR has a no-copy form
                expected = zeroed(data, at)
            else:
                covered = bytearray(data)
                covered[at : at + len(zero)] = zero
                expected = compute(covered)
        else:
            expected = _checksum_of(profile, cf, data, n)
        if isinstance(expected, str):
            return DecodeError(Code.DEC_CHECKSUM, at, expected, peek_mid(profile, data))
        got = (tword if cf.trailer else word) >> cf.shift & cf.mask
        if cf.swap:
            got = cf.swapped(got)
        if expected != got:
            return DecodeError(
                Code.DEC_CHECKSUM,
                at,
                f"{cf.name} is 0x{got:02X}, expected 0x{expected:02X}",
                peek_mid(profile, data),
            )
    return word


def check(
    profile: HeaderProfile,
    data: Buffer,
    *,
    max_size: int = DEFAULT_MAX_PACKET_BYTES,
) -> DecodeError | None:
    """:func:`check_word` without the word: ``None`` when the header is valid."""
    result = check_word(profile, data, max_size=max_size)
    return result if isinstance(result, DecodeError) else None


def parse_word(profile: HeaderProfile, word: int, mid: int | None, trailer: int = 0) -> Header:
    """Build the :class:`Header` of a header ``word`` (and ``trailer`` word) :func:`check_word`
    accepted."""
    values = {f.name: (word >> f.shift) & f.mask for f in profile.fields}
    for f in profile.swapped_fields:
        values[f.name] = f.swapped(values[f.name])
    for f in profile.trailer_fields:
        values[f.name] = read_field(f, trailer)
    sf = profile.sequence_field
    return Header(
        values=MappingProxyType(values),
        mid=mid,
        size=profile.size_bytes,
        sequence=values[sf.name] if sf is not None else None,
        profile=profile,
    )


def unpack(
    profile: HeaderProfile,
    data: Buffer,
    *,
    max_size: int = DEFAULT_MAX_PACKET_BYTES,
) -> Header | DecodeError:
    """Validate and unpack the header of one whole packet (one datagram)."""
    word = check_word(profile, data, max_size=max_size)
    if isinstance(word, DecodeError):
        return word
    return parse_word(profile, word, peek_mid(profile, data), trailer_word(profile, data))
