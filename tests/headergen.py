"""Hypothesis strategies for header profiles: valid by construction, with a model of what each
field must hold, so the tests can check pack and unpack against an independent expectation.

``Spec`` is the generator's own description of a profile (field order, widths, byte order, kind,
trailer, checksum span, time table, MID order). ``Spec.doc()`` renders it as the mapping
``parse_profile`` takes, and the ``expect_*`` helpers say what the wire must look like without
using the code under test.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from hypothesis import strategies as st

EPOCHS = (
    "1970-01-01T00:00:00Z",
    "1980-01-01T00:00:00Z",
    "1980-01-06T00:00:00Z",
    "1958-01-01T00:00:00Z",
    "2000-01-01T11:58:55+00:00",
    "2000-01-01T12:00:00+02:00",
)
EPOCH_SECONDS = {
    "1970-01-01T00:00:00Z": 0,
    "1980-01-01T00:00:00Z": 315_532_800,
    "1980-01-06T00:00:00Z": 315_964_800,
    "1958-01-01T00:00:00Z": -378_691_200,
    "2000-01-01T11:58:55+00:00": 946_727_935,
    "2000-01-01T12:00:00+02:00": 946_720_800,
}


@dataclass
class F:
    """One generated field."""

    name: str
    bits: int
    kind: str  # fixed | input | from_mid | sequence | length | checksum | time_seconds | time_sub
    little: bool = False
    value: int = 0  # fixed value, or input default
    bias: int = 0
    check: bool = True
    algorithm: str = ""
    span: Any = None  # checksum: None, "packet", "header", "payload" or (start, end)
    trailer: bool = False

    def table(self) -> dict[str, Any]:
        t: dict[str, Any] = {"name": self.name, "bits": self.bits}
        match self.kind:
            case "fixed":
                t["value"] = self.value
                if not self.check:
                    t["check"] = False
            case "input":
                t["default"] = self.value
            case "from_mid":
                t["computed"] = "from_mid"
            case "sequence":
                t["computed"] = "sequence"
            case "length":
                t["computed"] = "length"
                t["bias"] = self.bias
            case "checksum":
                t["computed"] = "checksum"
                t["algorithm"] = self.algorithm
                if self.span is not None:
                    if isinstance(self.span, tuple):
                        t["span"] = {"start": self.span[0], "end": self.span[1]}
                    else:
                        t["span"] = self.span
            case "time_seconds":
                t["computed"] = "time_seconds"
            case "time_sub":
                t["computed"] = "time_subseconds"
            case _:
                raise AssertionError(self.kind)
        if self.little:
            t["endian"] = "little"
        return t


@dataclass
class Spec:
    kind: str
    header: list[F]
    trailer: list[F] = field(default_factory=list)
    mid_order: list[str] = field(default_factory=list)
    fc_field: str | None = None
    epoch: str | None = None
    leap: int = 0
    legacy: bool = False

    def doc(self) -> dict[str, Any]:
        doc: dict[str, Any] = {
            "name": "gen",
            "kind": self.kind,
            "mid_fields": list(self.mid_order),
            "fields": [f.table() for f in self.header],
        }
        if self.fc_field is not None:
            doc["function_code_field"] = self.fc_field
        if self.trailer:
            doc["trailer"] = {"fields": [f.table() for f in self.trailer]}
        if self.epoch is not None or self.leap:
            time: dict[str, Any] = {}
            if self.epoch is not None:
                time["epoch"] = self.epoch
            if self.leap:
                time["leap_seconds"] = self.leap
            doc["time"] = time
        return doc

    # -- what the profile means, computed without the code under test --------------------

    @property
    def header_bytes(self) -> int:
        return sum(f.bits for f in self.header) // 8

    @property
    def trailer_bytes(self) -> int:
        return sum(f.bits for f in self.trailer) // 8

    def by_name(self) -> dict[str, F]:
        return {f.name: f for f in [*self.header, *self.trailer]}

    def mid_bits(self) -> int:
        by = self.by_name()
        return sum(by[n].bits for n in self.mid_order)

    def epoch_offset(self) -> int:
        """Counter seconds minus Unix seconds."""
        base = EPOCH_SECONDS[self.epoch] if self.epoch is not None else 0
        return self.leap - base


def expected_mid(spec: Spec, parts: dict[str, int]) -> int:
    """The MID that carries ``parts`` (field name to value) in ``mid_order``."""
    by = spec.by_name()
    mid = 0
    for name in spec.mid_order:
        mid = (mid << by[name].bits) | parts[name]
    return mid


def expect_wire(
    spec: Spec, values: dict[str, int], payload: bytes, checksum_fn: Any = None
) -> bytes:
    """The packet for ``values`` (every field's logical value), by plain bit arithmetic."""

    def section(fields: list[F]) -> bytes:
        bits = ""
        for f in fields:
            raw = values[f.name]
            if f.little:
                raw = int.from_bytes(raw.to_bytes(f.bits // 8, "big"), "little")
            bits += format(raw, f"0{f.bits}b")
        return int(bits, 2).to_bytes(len(bits) // 8, "big") if bits else b""

    packet = bytearray(section(spec.header) + payload + section(spec.trailer))
    cf = next((f for f in [*spec.header, *spec.trailer] if f.kind == "checksum"), None)
    if cf is not None and checksum_fn is not None:
        start, end = span_of(spec, cf, len(packet))
        at = offset_of(spec, cf, len(packet))
        width = cf.bits // 8
        covered = bytearray(packet[start:end])
        if start <= at and at + width <= end:
            covered[at - start : at - start + width] = bytes(width)
        value = checksum_fn(cf.algorithm, bytes(covered))
        packet[at : at + width] = value.to_bytes(width, "little" if cf.little else "big")
    return bytes(packet)


def offset_of(spec: Spec, f: F, n: int) -> int:
    """Byte offset in an ``n``-byte packet of the first byte of a byte-aligned field."""
    fields = spec.trailer if f.trailer else spec.header
    bit = 0
    for g in fields:
        if g is f:
            break
        bit += g.bits
    assert bit % 8 == 0
    return bit // 8 + (n - spec.trailer_bytes if f.trailer else 0)


def span_of(spec: Spec, cf: F, n: int) -> tuple[int, int]:
    match cf.span:
        case None | "packet":
            return 0, n
        case "header":
            return 0, spec.header_bytes
        case "payload":
            return spec.header_bytes, n - spec.trailer_bytes
        case (start, end):
            return start, n + end
        case _:
            raise AssertionError(cf.span)


# ---------------------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------------------

BYTE_WIDTHS = (8, 16, 24, 32, 48, 64)


@st.composite
def _bit_group(draw: st.DrawFn) -> list[int]:
    """Widths of 1..4 bitfields that fill 1 or 2 whole bytes."""
    total = draw(st.sampled_from((8, 16)))
    widths: list[int] = []
    left = total
    while left:
        w = draw(st.integers(1, left))
        widths.append(w)
        left -= w
        if len(widths) == 4 and left:
            widths.append(left)
            left = 0
    return widths


@st.composite
def specs(
    draw: st.DrawFn,
    kind: str = "telemetry",
    *,
    legacy: bool = False,
    with_checksum: bool | None = None,
    with_trailer: bool | None = None,
) -> Spec:
    """A valid profile. ``legacy`` keeps to what profiles could say before Stage 2i: big-endian
    bitfields, one contiguous MID run, a whole-packet checksum, no trailer, no [time]."""
    header: list[F] = []
    n = 0

    def fresh() -> str:
        nonlocal n
        n += 1
        return f"f{n}"

    for _ in range(draw(st.integers(2, 7))):
        if draw(st.booleans()):
            for w in draw(_bit_group()):
                header.append(
                    F(
                        fresh(),
                        w,
                        draw(st.sampled_from(("fixed", "input", "from_mid", "sequence"))),
                        value=draw(st.integers(0, (1 << w) - 1)),
                        check=draw(st.booleans()),
                    )
                )
        else:
            w = draw(st.sampled_from(BYTE_WIDTHS))
            header.append(
                F(
                    fresh(),
                    w,
                    draw(
                        st.sampled_from(
                            ("fixed", "input", "from_mid", "sequence", "time_seconds", "time_sub")
                        )
                    ),
                    little=False if legacy else draw(st.booleans()),
                    value=draw(st.integers(0, (1 << w) - 1)),
                    check=draw(st.booleans()),
                )
            )
    if draw(st.booleans()):
        cuts = [i for i in range(len(header) + 1) if sum(f.bits for f in header[:i]) % 8 == 0]
        header.insert(
            draw(st.sampled_from(cuts)),
            F(
                fresh(),
                16,
                "length",
                little=False if legacy else draw(st.booleans()),
                bias=draw(st.integers(-8, 8)),
            ),
        )
    want_checksum = draw(st.booleans()) if with_checksum is None else with_checksum
    use_trailer = not legacy and (draw(st.booleans()) if with_trailer is None else with_trailer)
    trailer: list[F] = []
    if use_trailer:
        for _ in range(draw(st.integers(0, 2))):
            w = draw(st.sampled_from((8, 16)))
            trailer.append(
                F(
                    fresh(),
                    w,
                    "fixed",
                    little=draw(st.booleans()),
                    value=draw(st.integers(0, (1 << w) - 1)),
                    check=draw(st.booleans()),
                    trailer=True,
                )
            )
    cf: F | None = None
    if want_checksum:
        algorithm = draw(st.sampled_from(("xor8_cfs", "crc16_ccitt")))
        width = 8 if algorithm == "xor8_cfs" else 16
        in_trailer = use_trailer and draw(st.booleans())
        cf = F(
            fresh(),
            width,
            "checksum",
            little=False if legacy else (draw(st.booleans()) and width > 8),
            algorithm=algorithm,
            trailer=in_trailer,
        )
        if in_trailer:
            trailer.insert(draw(st.integers(0, len(trailer))), cf)
        else:
            cuts = [i for i in range(len(header) + 1) if sum(f.bits for f in header[:i]) % 8 == 0]
            header.insert(draw(st.sampled_from(cuts)), cf)

    # Singleton kinds: keep the first of each, turn the rest into inputs.
    seen: set[str] = set()
    for f in header:
        if f.kind in ("sequence", "time_seconds", "time_sub", "length", "checksum"):
            if f.kind in seen:
                f.kind = "input"
            seen.add(f.kind)

    # Message ID: an ordered subset of the fixed and from_mid fields, at most 32 bits.
    candidates = [f for f in header if f.kind in ("fixed", "from_mid")]
    if not candidates:
        extra = F(fresh(), 16, "from_mid")
        header.insert(0, extra)
        candidates = [extra]
    chosen: list[F]
    if legacy:
        start = draw(st.integers(0, len(candidates) - 1))
        chosen = [candidates[start]]
        for f in candidates[start + 1 :]:
            if header.index(f) != header.index(chosen[-1]) + 1:
                break
            chosen.append(f)
    else:
        shuffled = draw(st.permutations(candidates))
        chosen = list(shuffled[: draw(st.integers(1, len(shuffled)))])
    kept: list[F] = []
    total = 0
    for f in chosen:
        if total + f.bits > 32:
            if legacy:
                break
            continue
        kept.append(f)
        total += f.bits
    if not kept:
        extra = F(fresh(), 8, "from_mid")
        header.insert(0, extra)
        kept = [extra]
    names = {f.name for f in kept}
    for f in header:
        if f.kind == "from_mid" and f.name not in names:
            f.kind = "input"

    fc: str | None = None
    if kind == "command":
        inputs = [f for f in header if f.kind == "input"]
        if inputs:
            fc = inputs[0].name
        else:
            extra = F(fresh(), 8, "input", value=0)
            header.append(extra)
            fc = extra.name

    spec = Spec(kind, header, trailer, [f.name for f in kept], fc, legacy=legacy)
    assert sum(f.bits for f in header) % 8 == 0

    if cf is not None and not legacy:
        hs, ts = spec.header_bytes, spec.trailer_bytes
        choice = draw(st.sampled_from(("none", "packet", "header", "payload", "range")))
        if choice == "range":
            start = draw(st.integers(0, hs))
            end = draw(st.integers(max(-ts, start - hs - ts), 0))
            width = cf.bits // 8
            if not cf.trailer:
                at = offset_of(spec, cf, 0)
                if at < start < at + width:
                    start = at  # a span may not start inside its own checksum field
            else:
                near = ts - (offset_of(spec, cf, ts) + width)
                if near < -end < near + width:
                    end = -near
            cf.span = (start, end)
        elif choice != "none":
            cf.span = choice

    if not legacy and any(f.kind == "time_seconds" for f in header):
        if draw(st.booleans()):
            spec.epoch = draw(st.sampled_from(EPOCHS))
        if draw(st.booleans()):
            spec.leap = draw(st.integers(-40, 40))
    return spec
