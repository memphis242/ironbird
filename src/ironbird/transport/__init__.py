"""Transports move raw datagrams; they never interpret them."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class Datagram:
    data: bytes
    time_ns: int  # receive timestamp, taken as early as possible
    source: str  # human-readable peer, e.g. "127.0.0.1:50000"


class Transport(Protocol):
    """Minimal contract shared by UDP now and serial / 1553 later."""

    async def open(self) -> None: ...

    async def send(self, data: bytes) -> None: ...

    def receive(self) -> AsyncIterator[Datagram]: ...

    @property
    def dropped(self) -> int:
        """Datagrams discarded because the consumer fell behind (cumulative)."""
        ...

    async def close(self) -> None: ...
