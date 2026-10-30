"""Stage 2d, layer 1: the ledger under floods - a commanding peer, a garbage stream, an event
storm - through the real session code on loopback.

What is asserted is accounting and bounds, never text: every occurrence is counted somewhere
(written, or summarised, or refused and counted), essential records are never crowded out, and
the ledger grows by at most its allowances.
"""

from __future__ import annotations

import asyncio
import socket
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from ironbird.config import LedgerConfig, SystemConfig, load_config
from ironbird.frame import FrameFlag
from ironbird.ledger import Ledger
from ironbird.ledgertiers import Tier
from ironbird.session import Session
from ironbird.transport import LinkEnd
from ironbird.units import MIB
from ledgerutil import all_records


def run_pair(
    config: SystemConfig,
    tmp_path: Path,
    scenario: Callable[[Session, Session], Awaitable[None]],
    *,
    ledger_config: LedgerConfig | None = None,
) -> tuple[Ledger, Ledger]:
    ground = Ledger(tmp_path / "g", session_id="ground", config=ledger_config)
    fc = Ledger(tmp_path / "f", session_id="fc", config=ledger_config, session_class="long")

    async def main() -> None:
        async with (
            Session(config, role="ground", ledger=ground, notify=lambda _n: None) as g,
            Session(config, role="fc", ledger=fc, notify=lambda _n: None) as f,
        ):
            await asyncio.wait_for(scenario(g, f), timeout=60)

    asyncio.run(main())
    ground.close()
    fc.close()
    return ground, fc


def test_a_peer_commanding_flat_out_gets_sampled_commands_and_counted_replies(
    mock_config_path: Path, tmp_path: Path
) -> None:
    """A responder under a command flood: ``rx.command`` records are sampled per command name
    with exact counts in the summary; reply records are individual up to the reply rate and
    counted in ``respond.replies`` beyond it; the sum equals the replies sent."""
    config = load_config(mock_config_path)
    flood = 4000
    sent_replies = {"n": 0}

    async def scenario(ground: Session, fc: Session) -> None:
        responder = await fc.respond()
        telemetry = await ground.listen("fc")
        addr = responder.local_address
        assert addr is not None
        raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        good = ground.codec_for("fc").encode_command("MOCK_NOOP").data
        try:
            handled = aiter(responder)
            for i in range(flood):
                raw.sendto(good, addr)
                if i % 200 == 199:
                    await asyncio.sleep(0.005)
            for _ in range(flood):
                event = await asyncio.wait_for(handled.__anext__(), timeout=20)
                sent_replies["n"] += event.reply is not None
        finally:
            raw.close()
            await responder.close()
            await telemetry.close()

    _, fc_ledger = run_pair(config, tmp_path, scenario)
    rows = all_records(fc_ledger)
    commands = [r for r in rows if r["event"] == "rx.command"]
    summaries = [r for r in rows if r["event"] == "rx.command.summary"]
    assert 8 <= len(commands) < flood
    assert len(commands) + sum(s["skipped"] for s in summaries) == flood  # every command accounted
    individual = [r for r in rows if r["event"] == "tx.telemetry"]
    counted = [r for r in rows if r["event"] == "respond.replies"]
    assert sent_replies["n"] == flood
    assert len(individual) + sum(r["replies"] for r in counted) == flood
    assert len(individual) >= 600  # the burst is written individually
    assert counted, "the rest of 4000 replies in a second are counted, not written one by one"
    assert all(r["first_seq"] <= r["last_seq"] for r in counted)
    essential_bytes = fc_ledger.written_bytes[Tier.ESSENTIAL]
    assert essential_bytes < 4 * MIB  # bounded by the reply rate, not by the peer


def test_an_event_storm_and_a_garbage_flood_together_never_crowd_out_essential_records(
    tmp_path: Path,
) -> None:
    ledger = Ledger(
        tmp_path / "l",
        session_id="q",
        max_queue=1000,
        max_evidence_queue=300,
        max_event_queue=300,
    )
    for i in range(5000):  # the drain is paused: nothing leaves the queue
        ledger.record("event", "rx.event", text="e" * 50, i=i)
        ledger.record("packet", "rx.other", raw=b"\x00" * 64, i=i)
    essential = [ledger.record("action", "a", i=i) for i in range(300)]
    assert all(n is not None for n in essential)  # their room is untouched
    assert ledger.pending == 300 + 300 + 300
    assert ledger.dropped == 2 * (5000 - 300)
    ledger.close()
    rows = all_records(ledger)
    dropped = [r for r in rows if r["event"] == "ledger.dropped"]
    assert len(dropped) == 1 and dropped[0]["events"] == 4700 and dropped[0]["evidence"] == 4700


def test_garbage_floods_to_a_listener_stay_within_the_evidence_allowance(
    mock_config_path: Path, tmp_path: Path
) -> None:
    """Unique garbage MIDs through the real ingest path: the ledger's growth is bounded by
    sampling and the allowance, every frame is counted, nothing refuses an essential record."""
    config = load_config(mock_config_path)
    limits = LedgerConfig(evidence_bytes_per_hour=MIB)
    frames = 6000

    async def scenario(ground: Session, fc: Session) -> None:
        capture = await ground.capture([ground.route("fc")])
        io = ground._engine._ios[(config.links[0].name, LinkEnd.SOURCE)]
        for i in range(frames):
            mid = 0x0900 + i
            io.on_data(
                bytes([mid >> 8, mid & 0xFF, 0xC0, 0x00, 0x00, 0x01, 0x00]),
                0,
                "127.0.0.1:1",
                7,
                FrameFlag.NONE,
            )
        assert ground.malformed_total == frames
        await capture.close()
        ground.ledger.record("command", "tx.command", note="still accepted")

    ground, _ = run_pair(config, tmp_path, scenario, ledger_config=limits)
    rows: list[dict[str, Any]] = all_records(ground)
    kept = [r for r in rows if r["event"] == "rx.malformed"]
    summarised = sum(r["skipped"] for r in rows if r["event"] == "rx.malformed.summary")
    assert len(kept) + summarised == frames  # every datagram is counted
    assert ground.written_bytes[Tier.EVIDENCE] <= MIB * 1.1  # within the allowance
    assert any(r["event"] == "tx.command" for r in rows)
