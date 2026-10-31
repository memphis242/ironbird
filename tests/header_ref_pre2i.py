"""The header boundary as it stood before Stage 2i, kept as a differential oracle.

It handles what a pre-2i profile can say: one section of big-endian bitfields, one contiguous
run of MID fields, a checksum over the whole packet, Unix-epoch time fields. It reads only the
attributes a profile has always had (``fields``, ``by_name``, ``mid_fields``, ``size_bytes`` and
the fields' ``shift``, ``bits``, ``mask``, ``kind``, ``value``, ``check``, ``computed``,
``bias``, ``algorithm``), so the current loader can feed it any pre-2i profile and the two
implementations must agree packet for packet.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ironbird import checksum
from ironbird.errors import Code, DecodeError, IronbirdError
from ironbird.header import FieldKind, HeaderProfile, split_mid


def peek_mid(profile: HeaderProfile, data: bytes) -> int | None:
    if not profile.mid_fields:
        return None
    first = profile.by_name[profile.mid_fields[0]]
    total = profile.size_bytes * 8
    bit_start = total - first.shift - first.bits
    byte_o = bit_start // 8
    bits = sum(profile.by_name[n].bits for n in profile.mid_fields)
    nbytes = (bit_start + bits + 7) // 8 - byte_o
    shift = nbytes * 8 - (bit_start - byte_o * 8) - bits
    if len(data) < byte_o + nbytes:
        return None
    return (int.from_bytes(data[byte_o : byte_o + nbytes], "big") >> shift) & ((1 << bits) - 1)


def check_word(profile: HeaderProfile, data: bytes, max_size: int) -> int | DecodeError:
    n = len(data)
    if n > max_size:
        return DecodeError(
            Code.DEC_OVERSIZE,
            max_size,
            f"packet of {n} bytes exceeds cap {max_size}",
            peek_mid(profile, data),
        )
    size = profile.size_bytes
    if n < size:
        return DecodeError(
            Code.DEC_SHORT,
            n,
            f"packet of {n} bytes is shorter than the {size}-byte header",
            peek_mid(profile, data),
        )
    word = int.from_bytes(data[:size], "big")
    lf = profile.length_field
    if lf is not None:
        declared = ((word >> lf.shift) & lf.mask) - lf.bias
        if declared != n:
            return DecodeError(
                Code.DEC_LENGTH,
                (profile.total_bits - lf.shift - lf.bits) // 8,
                f"{lf.name} field says {declared} bytes, got {n}",
                peek_mid(profile, data),
            )
    for f in profile.fields:
        got = (word >> f.shift) & f.mask
        if f.kind is FieldKind.FIXED and f.check and got != f.value:
            return DecodeError(
                Code.DEC_FIXED,
                (profile.total_bits - f.shift - f.bits) // 8,
                f"{f.name} is {got}, expected {f.value}",
                peek_mid(profile, data),
            )
    cf = profile.checksum_field
    if cf is not None:
        assert cf.algorithm is not None
        zeroed = (word & ~(cf.mask << cf.shift)).to_bytes(size, "big")
        expected = checksum.REGISTRY[cf.algorithm].compute(zeroed + bytes(data[size:]))
        got = (word >> cf.shift) & cf.mask
        if expected != got:
            return DecodeError(
                Code.DEC_CHECKSUM,
                (profile.total_bits - cf.shift - cf.bits) // 8,
                f"{cf.name} is 0x{got:02X}, expected 0x{expected:02X}",
                peek_mid(profile, data),
            )
    return word


def values_of(profile: HeaderProfile, word: int) -> dict[str, int]:
    return {f.name: (word >> f.shift) & f.mask for f in profile.fields}


def pack(
    profile: HeaderProfile,
    payload: bytes,
    *,
    mid: int | None,
    inputs: Mapping[str, int],
    sequence: int,
    now: float,
) -> bytes:
    total = profile.size_bytes + len(payload)
    parts: Any = {}
    if profile.mid_fields:
        assert mid is not None
        parts = split_mid(profile, mid)
        if isinstance(parts, str):
            raise IronbirdError(Code.ENC_FIT, parts)
    word = 0
    fractional = now - int(now)
    for f in profile.fields:
        match f.kind:
            case FieldKind.FIXED:
                value = f.value or 0
            case FieldKind.INPUT:
                value = inputs.get(f.name, f.value or 0)
            case FieldKind.COMPUTED:
                match f.computed:
                    case "length":
                        value = total + f.bias
                    case "sequence":
                        value = sequence & f.mask
                    case "from_mid":
                        value = parts[f.name]
                    case "time_seconds":
                        value = int(now) & f.mask
                    case "time_subseconds":
                        value = int(fractional * (1 << f.bits)) & f.mask
                    case _:
                        value = 0
        word |= value << f.shift
    packet = word.to_bytes(profile.size_bytes, "big") + bytes(payload)
    cf = profile.checksum_field
    if cf is not None:
        assert cf.algorithm is not None
        value = checksum.REGISTRY[cf.algorithm].compute(packet)
        word |= (value & cf.mask) << cf.shift
        packet = word.to_bytes(profile.size_bytes, "big") + bytes(payload)
    return packet
