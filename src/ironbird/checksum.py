"""Named checksum algorithms selectable from a header profile.

Every algorithm is computed over the bytes a checksum field's span covers (default: the whole
packet) with the checksum field zeroed if it lies inside the span, and returns the value to
store in the field. Verification recomputes the same way, so a new algorithm needs only
``compute``.
"""

from __future__ import annotations

import binascii
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from functools import reduce
from operator import xor
from types import MappingProxyType
from typing import Final


@dataclass(frozen=True, slots=True)
class ChecksumAlgorithm:
    name: str
    width_bits: int
    description: str
    compute: Callable[[bytes | bytearray | memoryview], int]

    def __post_init__(self) -> None:
        assert self.width_bits in (8, 16, 32)


def _xor8_cfs(data: bytes | bytearray | memoryview) -> int:
    """cFE command checksum: stored byte makes the XOR of the covered bytes equal 0xFF."""
    return reduce(xor, data, 0xFF)


def _crc16_ccitt(data: bytes | bytearray | memoryview) -> int:
    """CRC-16/CCITT-FALSE: poly 0x1021, init 0xFFFF, no reflection, no final XOR. This is the
    CCSDS / ECSS packet error control code."""
    return binascii.crc_hqx(data, 0xFFFF)


REGISTRY: Final[Mapping[str, ChecksumAlgorithm]] = MappingProxyType(
    {
        algo.name: algo
        for algo in (
            ChecksumAlgorithm(
                "xor8_cfs",
                8,
                "8-bit XOR of the covered bytes equals 0xFF (cFE commands)",
                _xor8_cfs,
            ),
            ChecksumAlgorithm(
                "crc16_ccitt", 16, "CRC-16/CCITT-FALSE over the covered bytes", _crc16_ccitt
            ),
        )
    }
)

assert _crc16_ccitt(b"123456789") == 0x29B1  # standard check value
assert _xor8_cfs(b"") == 0xFF
