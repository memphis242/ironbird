"""Named checksum algorithms selectable from a header profile.

Every algorithm is computed over the *whole packet with the checksum field zeroed* and
returns the value to store in the field. Verification recomputes the same way, so a new
algorithm needs only ``compute``.
"""

from __future__ import annotations

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
    compute: Callable[[bytes | memoryview], int]

    def __post_init__(self) -> None:
        assert self.width_bits in (8, 16, 32)


def _xor8_cfs(data: bytes | memoryview) -> int:
    """cFE command checksum: stored byte makes the XOR of the whole packet equal 0xFF."""
    return reduce(xor, data, 0xFF)


def _build_crc16_table() -> tuple[int, ...]:
    table: list[int] = []
    for byte in range(256):
        crc = byte << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) & 0xFFFF if crc & 0x8000 else (crc << 1) & 0xFFFF
        table.append(crc)
    return tuple(table)


_CRC16_TABLE: Final = _build_crc16_table()


def _crc16_ccitt(data: bytes | memoryview) -> int:
    """CRC-16/CCITT-FALSE: poly 0x1021, init 0xFFFF, no reflection, no final XOR."""
    crc = 0xFFFF
    for byte in data:
        crc = ((crc << 8) & 0xFFFF) ^ _CRC16_TABLE[(crc >> 8) ^ byte]
    return crc


REGISTRY: Final[Mapping[str, ChecksumAlgorithm]] = MappingProxyType(
    {
        algo.name: algo
        for algo in (
            ChecksumAlgorithm(
                "xor8_cfs", 8, "8-bit XOR of the whole packet equals 0xFF (cFE commands)", _xor8_cfs
            ),
            ChecksumAlgorithm(
                "crc16_ccitt", 16, "CRC-16/CCITT-FALSE over the whole packet", _crc16_ccitt
            ),
        )
    }
)

assert _crc16_ccitt(b"123456789") == 0x29B1  # standard check value
assert _xor8_cfs(b"") == 0xFF
