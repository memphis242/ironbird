"""Review 7 regressions: wire-origin errors share the evidence tier so a malformed-packet flood
cannot crowd out essential records, and the ledger's JSON formatter survives hostile mappings."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

import pytest

from ironbird import ledger as ledger_module
from ironbird.ccdd.model import MessageKind
from ironbird.config import load_config
from ironbird.errors import Code, IronbirdError
from ironbird.ledger import Ledger
from ironbird.session import Session
from ironbird.transport import Datagram

FLOOD = 500


def test_malformed_packet_flood_does_not_crowd_out_essential_records(
    mock_config_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # No prompt flush between errors, so only the tier caps decide what is accepted.
    monkeypatch.setattr(ledger_module, "ERROR_FLUSH_MIN_INTERVAL_S", 1e9)
    config = load_config(mock_config_path)
    ledger = Ledger(tmp_path / "l", session_id="t", max_queue=100, max_evidence_queue=75)

    async def main() -> tuple[int | None, int, int]:
        async with Session(config, role="ground", ledger=ledger, notify=lambda _: None) as ground:
            codec = ground.codec_for("fc")
            for _ in range(FLOOD):
                result = ground._ingest(
                    codec, MessageKind.TELEMETRY, Datagram(b"\x00", 0, "127.0.0.1:1"), None, "fc"
                )
                assert result is not None
            number = ledger.record("command", "tx.command")
            return number, ledger.dropped, ledger._dropped_essential  # before close() summarises

    number, dropped, dropped_essential = asyncio.run(main())
    assert number is not None, "an operator command record was refused"
    assert dropped > 0  # the flood itself was capped and counted
    assert dropped_essential == 0


def test_explicit_evidence_tier_overrides_the_kind_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ledger_module, "ERROR_FLUSH_MIN_INTERVAL_S", 1e9)
    ledger = Ledger(tmp_path, session_id="e", max_queue=10, max_evidence_queue=2)
    error = IronbirdError(Code.DEC_CHECKSUM, "bad")
    ledger.record_error(error)  # the first error flushes at once; later ones only queue
    accepted = [ledger.record_error(error, evidence=True) for _ in range(5)]
    assert [n is not None for n in accepted] == [True, True, False, False, False]
    assert ledger.record("action", "a") is not None  # essential room is untouched


# -- _format survives mappings that raise while being serialised -------------------------


class _RaisingGetitem(Mapping[str, Any]):
    def __getitem__(self, key: str) -> Any:
        raise RuntimeError("boom")

    def __iter__(self) -> Iterator[str]:
        return iter(["a"])

    def __len__(self) -> int:
        return 1


class _RaisingIter(Mapping[str, Any]):
    def __getitem__(self, key: str) -> Any:
        return 1

    def __iter__(self) -> Iterator[str]:
        raise KeyError("x")

    def __len__(self) -> int:
        return 1


@pytest.mark.parametrize("bad", [_RaisingGetitem(), _RaisingIter()], ids=["getitem", "iter"])
def test_a_raising_mapping_degrades_to_the_fallback_instead_of_raising(
    tmp_path: Path, bad: Mapping[str, Any]
) -> None:
    ledger = Ledger(tmp_path, session_id="f")
    ledger.record("state", "bad", value=bad)
    ledger.record("state", "after")
    ledger.flush()
    records = [json.loads(line) for line in ledger.path.read_text().splitlines()]
    assert [r["event"] for r in records] == ["bad", "after"]
    assert "format_error" in records[0]
