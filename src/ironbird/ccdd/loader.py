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

    def load(self, path: Path) -> CcddModel: ...


ADAPTERS: Final[Mapping[str, Adapter]] = MappingProxyType(
    {adapter.name: adapter for adapter in (JsonAdapter(),)}
)


def load_ccdd(path: Path, adapter: str = "json") -> CcddModel:
    impl = ADAPTERS.get(adapter)
    if impl is None:
        raise IronbirdError(
            Code.CCD_FILE,
            f"{path}: unknown CCDD adapter {adapter!r}; known: {', '.join(sorted(ADAPTERS))}",
        )
    return impl.load(path)
