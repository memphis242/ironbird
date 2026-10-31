"""Review 7 regressions: wire-origin errors share the evidence tier so a malformed-packet flood
cannot crowd out essential records, and the ledger's JSON formatter survives hostile mappings."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

import pytest

from ironbird import ledger as ledger_module
from ironbird.config import load_config
from ironbird.errors import Code, IronbirdError
from ironbird.frame import FrameFlag
from ironbird.ledger import Ledger
from ironbird.ledgertiers import Tier
from ironbird.session import Session
from ironbird.transport import LinkEnd
from ledgerutil import records as ledger_records
from ledgerutil import session_records

FLOOD = 500


def test_malformed_packet_flood_does_not_crowd_out_essential_records(
    mock_config_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # No prompt flush between errors, so only the tier caps and the sampler decide what is kept.
    monkeypatch.setattr(ledger_module, "ERROR_FLUSH_MIN_INTERVAL_S", 1e9)
    config = load_config(mock_config_path)
    ledger = Ledger(tmp_path / "l", session_id="t", max_queue=100, max_evidence_queue=75)

    async def main() -> tuple[int | None, int, int, int]:
        async with Session(config, role="ground", ledger=ledger, notify=lambda _: None) as ground:
            capture = await ground.capture([ground.route("fc")])
            io = ground._engine._ios[(config.links[0].name, LinkEnd.SOURCE)]
            for _ in range(FLOOD):  # one-byte garbage frames, as the transport would deliver them
                io.on_data(b"\x00", 0, "127.0.0.1:1", 1, FrameFlag.NONE)
            assert ground.malformed_total == FLOOD
            await capture.close()
            number = ledger.record("command", "tx.command")
            # before close() summarises
            return number, ledger.dropped, ledger._dropped[Tier.ESSENTIAL], ledger.sampled_away

    number, dropped, dropped_essential, sampled_away = asyncio.run(main())
    assert number is not None, "an operator command record was refused"
    assert dropped_essential == 0
    # The rejected frames went to the evidence tier end to end: sampled per key (the same key
    # for all of them), what the queue took plus what was sampled away or refused is the flood.
    assert sampled_away + dropped > 0 and sampled_away + dropped + ledger.pending >= FLOOD
    ledger.close()
    events = [r["event"] for r in session_records(tmp_path / "l", bookkeeping=True)]
    assert "rx.malformed.summary" in events  # and the sampler accounted for the rest


def test_explicit_evidence_tier_overrides_the_kind_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ledger_module, "ERROR_FLUSH_MIN_INTERVAL_S", 1e9)
    ledger = Ledger(tmp_path, session_id="e", max_queue=10, max_evidence_queue=2)
    error = IronbirdError(Code.DEC_CHECKSUM, "bad")
    ledger.record_error(error, tier=Tier.ESSENTIAL)  # the first error flushes at once
    accepted = [ledger.record_error(error, tier=Tier.EVIDENCE) for _ in range(5)]
    assert [n is not None for n in accepted] == [True, True, False, False, False]
    assert ledger.record("action", "a") is not None  # essential room is untouched


# -- _format survives mappings that raise while being serialised --


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
    records = ledger_records(ledger)
    assert [r["event"] for r in records] == ["bad", "after"]
    assert "format_error" in records[0]
