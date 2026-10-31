"""Pluggable CCDD loading: an ``Adapter`` turns one on-disk format into a ``CcddModel``."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType
from typing import Final, Protocol

from ironbird.ccdd.json_adapter import JsonAdapter
from ironbird.ccdd.model import CcddModel
from ironbird.errors import Code, IronbirdError


class Adapter(Protocol):
    """Reads one CCDD export format. Must raise ``IronbirdError`` (IB-CCD-*), never crash."""

    name: str

    def read(self, path: Path) -> bytes: ...

    def load(self, path: Path) -> CcddModel: ...

    def load_bytes(self, raw: bytes, source: str) -> CcddModel: ...


ADAPTERS: Final[Mapping[str, Adapter]] = MappingProxyType(
    {adapter.name: adapter for adapter in (JsonAdapter(),)}
)


def _adapter(source: str, adapter: str) -> Adapter:
    impl = ADAPTERS.get(adapter)
    if impl is None:
        raise IronbirdError(
            Code.CCD_FILE,
            f"{source}: unknown CCDD adapter {adapter[:40]!r}; known: "
            f"{', '.join(sorted(ADAPTERS))}",
        )
    return impl


def read_ccdd(path: Path, adapter: str = "json") -> bytes:
    """The content of a CCDD file, read once, for :func:`load_ccdd_bytes` and for whoever must
    hold exactly the bytes that were parsed."""
    return _adapter(str(path), adapter).read(path)


def load_ccdd(path: Path, adapter: str = "json") -> CcddModel:
    return _adapter(str(path), adapter).load(path)


def load_ccdd_bytes(raw: bytes, source: str, adapter: str = "json") -> CcddModel:
    """Build a model from the content of a CCDD file read elsewhere (an embedded definition),
    through the same adapter and the same strict rules as :func:`load_ccdd`. ``source`` labels
    the errors."""
    return _adapter(source, adapter).load_bytes(raw, source)
