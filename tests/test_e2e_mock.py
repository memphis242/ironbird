"""ironbird-vs-ironbird over loopback UDP: the mock flight node is the test target."""

from __future__ import annotations

import asyncio
import json
import socket
import subprocess
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from ironbird.cli.main import app
from ironbird.config import SystemConfig, load_config
from ironbird.errors import Code, IronbirdError
from ironbird.ledger import Ledger
from ironbird.session import Handled, Received, Session


def _ledger(tmp_path: Path, name: str) -> Ledger:
    return Ledger(tmp_path / "ledgers", session_id=name)


def _run(
    config: SystemConfig,
    tmp_path: Path,
    scenario: Callable[[Session, Session, list[str], list[str]], Awaitable[None]],
) -> tuple[Ledger, Ledger]:
    """Run ``scenario(ground, fc, ground_notes, fc_notes)`` with two live sessions."""
    ground_notes: list[str] = []
    fc_notes: list[str] = []
    ground_ledger, fc_ledger = _ledger(tmp_path, "ground"), _ledger(tmp_path, "fc")

    async def main() -> None:
        async with (
            Session(config, role="ground", ledger=ground_ledger, notify=ground_notes.append) as g,
            Session(config, role="fc", ledger=fc_ledger, notify=fc_notes.append) as f,
        ):
            await asyncio.wait_for(scenario(g, f, ground_notes, fc_notes), timeout=20)

    asyncio.run(main())
    return ground_ledger, fc_ledger


def _events(ledger: Ledger) -> list[dict[str, Any]]:
    ledger.flush()
    return [json.loads(line) for line in ledger.path.read_text().splitlines()]


async def _next(iterator: Any) -> Any:
    return await asyncio.wait_for(iterator.__anext__(), timeout=5)


def test_command_telemetry_loop_with_decoded_fields(mock_config_path: Path, tmp_path: Path) -> None:
    config = load_config(mock_config_path)

    async def scenario(ground: Session, fc: Session, *_: Any) -> None:
        responder = await fc.respond()
        listener = await ground.listen("fc")
        handled = aiter(responder)
        received = aiter(listener)
        try:
            await ground.send_command("fc", "MOCK_NOOP")
            event: Handled = await _next(handled)
            assert event.command is not None and event.command.message.name == "MOCK_NOOP"
            hk: Received = await _next(received)
            assert hk.packet.message.name == "MOCK_HK"
            assert hk.packet.as_dict()["cmd_counter"] == 1

            sent = await ground.send_command("fc", "MOCK_SET_MODE", {"mode": "SAFE", "pad": "0"})
            assert sent.function_code == 1 and sent.size == 10
            event = await _next(handled)
            assert event.command is not None and event.command.as_dict() == {"mode": 1, "pad": 0}
            hk = await _next(received)
            assert hk.packet.as_dict()["mode"] == 1  # echoed through the response rule
            assert hk.packet.as_dict()["cmd_counter"] == 2
            assert hk.packet.header.sequence == 1  # telemetry sequence counter advances

            await ground.send_command("fc", "MOCK_SET_LABEL", {"label": "hello"})
            await _next(handled)
            hk = await _next(received)
            assert hk.packet.as_dict()["label"] == "hello"
        finally:
            await responder.close()
            await listener.close()

    ground_ledger, fc_ledger = _run(config, tmp_path, scenario)
    ground_events = _events(ground_ledger)
    sent = [e for e in ground_events if e["event"] == "tx.command"]
    assert [e["command"] for e in sent] == ["MOCK_NOOP", "MOCK_SET_MODE", "MOCK_SET_LABEL"]
    assert all(e["result"] == "sent" and e["raw"] for e in sent)  # full exchange is recorded
    assert [e["event"] for e in _events(fc_ledger)].count("rx.command") == 3


def test_listen_mid_filter(mock_config_path: Path, tmp_path: Path) -> None:
    config = load_config(mock_config_path)

    async def scenario(ground: Session, fc: Session, *_: Any) -> None:
        responder = await fc.respond()
        only_status = await ground.listen("fc", mids=frozenset({0x0891}))
        try:
            await ground.send_command("fc", "MOCK_NOOP")
            await _next(aiter(responder))
            with pytest.raises(asyncio.TimeoutError):  # HK is filtered out
                await asyncio.wait_for(_next(aiter(only_status)), timeout=0.3)
        finally:
            await responder.close()
            await only_status.close()

    _run(config, tmp_path, scenario)


def test_closed_port_bad_argument_and_unknown_command_are_one_line_coded_errors(
    mock_config_path: Path, tmp_path: Path
) -> None:
    config = load_config(mock_config_path)

    async def scenario(ground: Session, *_: Any) -> None:
        expectations = [
            (("MOCK_NOOP", {}), Code.TX_SEND),  # nobody listening
            (("MOCK_SET_MODE", {"mode": "NOPE", "pad": "0"}), Code.ENC_ARG),
            (("NOT_A_COMMAND", {}), Code.ENC_UNKNOWN),
        ]
        for (command, args), code in expectations:
            with pytest.raises(IronbirdError) as info:
                await ground.send_command("fc", command, args)
            assert info.value.code is code
            assert "\n" not in info.value.user_line()

    ground_ledger, _ = _run(config, tmp_path, scenario)
    refused = [e for e in _events(ground_ledger) if e["event"] == "tx.command"]
    assert refused[0]["result"] == "IB-TX-002"  # attempted exchange is still ledgered


def test_responder_survives_a_flood_of_malformed_commands_and_coalesces(
    mock_config_path: Path, tmp_path: Path
) -> None:
    config = load_config(mock_config_path)
    flood = 400

    async def scenario(
        ground: Session, fc: Session, ground_notes: list[str], fc_notes: list[str]
    ) -> None:
        responder = await fc.respond()
        listener = await ground.listen("fc")
        handled, received = aiter(responder), aiter(listener)
        good = ground.codec_for("fc").encode_command("MOCK_NOOP").data
        corrupt = good[:7] + bytes([good[7] ^ 0xFF]) + good[8:]
        addr = responder.local_address
        assert addr is not None
        raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            for i in range(flood):
                raw.sendto(corrupt, addr)
                if i % 25 == 24:
                    await asyncio.sleep(0.002)  # let the loop drain; a real flood is a stream
            for _ in range(flood):
                event: Handled = await _next(handled)
                assert event.error is not None and event.error.code is Code.DEC_CHECKSUM
            await ground.send_command("fc", "MOCK_NOOP")  # still alive and capturing
            event = await _next(handled)
            assert event.command is not None
            hk: Received = await _next(received)
            assert hk.packet.as_dict()["err_counter"] == flood % 256
            assert responder.rejected == flood and responder.accepted == 1
        finally:
            raw.close()
            await responder.close()
            await listener.close()
        fc.coalescer.flush()
        shown = [n for n in fc_notes if "IB-DEC-005" in n]
        assert 1 <= len(shown) <= 3, shown  # one announcement plus at most one summary
        assert any(f"x{flood}" in n for n in shown)

    _, fc_ledger = _run(config, tmp_path, scenario)
    errors = [e for e in _events(fc_ledger) if e["kind"] == "error"]
    assert len(errors) == flood  # the ledger keeps every one in full


def test_ground_survives_malformed_telemetry_and_reports_unknown_mid_once(
    mock_config_path: Path, tmp_path: Path
) -> None:
    config = load_config(mock_config_path)

    async def scenario(ground: Session, fc: Session, ground_notes: list[str], _: list[str]) -> None:
        responder = await fc.respond()
        listener = await ground.listen("fc")
        received = aiter(listener)
        addr = listener.local_address
        assert addr is not None
        fc_codec = fc.codec_for("fc")
        good_tlm = fc_codec.encode_telemetry("MOCK_HK").data
        unknown_mid = bytearray(good_tlm)
        unknown_mid[0:2] = bytes.fromhex("08ee")
        raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            for i in range(200):
                raw.sendto(bytes(unknown_mid), addr)
                raw.sendto(good_tlm[:9], addr)  # truncated
                raw.sendto(b"\xff" * 40, addr)  # garbage
                if i % 10 == 9:
                    await asyncio.sleep(0.002)
            raw.sendto(good_tlm, addr)
            rx: Received = await _next(received)
            assert rx.packet.message.name == "MOCK_HK"
        finally:
            raw.close()
            await responder.close()
            await listener.close()
        ground.coalescer.flush()
        unknown_lines = [n for n in ground_notes if "IB-DEC-006" in n and "0x08EE" in n]
        assert 1 <= len(unknown_lines) <= 2, ground_notes
        assert len(ground_notes) < 12  # alert-fatigue guard: nowhere near 600 lines

    _run(config, tmp_path, scenario)


def test_respond_refuses_wrong_role_with_hint(mock_config_path: Path, tmp_path: Path) -> None:
    config = load_config(mock_config_path)

    async def scenario(ground: Session, *_: Any) -> None:
        with pytest.raises(IronbirdError) as info:
            await ground.respond()
        assert info.value.code is Code.CFG_LINK
        with pytest.raises(IronbirdError) as info2:
            await ground.send_command("ghost", "MOCK_NOOP")
        assert info2.value.code is Code.CFG_REF

    _run(config, tmp_path, scenario)


def test_cli_commands_against_mock_config(mock_config_path: Path) -> None:
    runner = CliRunner()
    base = ["-c", str(mock_config_path)]
    nodes = runner.invoke(app, [*base, "nodes"])
    assert nodes.exit_code == 0, nodes.output
    assert "fc" in nodes.output and "ground" in nodes.output

    found = runner.invoke(app, [*base, "find", "SET_MODE"])
    assert found.exit_code == 0 and "MOCK_SET_MODE" in found.output
    assert runner.invoke(app, [*base, "find", "zzz-no-such-thing"]).exit_code == 1

    refused = runner.invoke(app, [*base, "send", "fc", "MOCK_NOOP"])
    assert refused.exit_code == 1
    assert "IB-TX-002" in refused.output

    bad = runner.invoke(app, [*base, "send", "fc", "MOCK_SET_MODE", "mode"])
    assert bad.exit_code == 2 and "IB-CLI-001" in bad.output

    missing = runner.invoke(app, ["-c", "/nonexistent.toml", "nodes"])
    assert missing.exit_code == 2 and "IB-CFG-001" in missing.output


def _wait_for_line(proc: subprocess.Popen[str], needle: str) -> None:
    assert proc.stderr is not None
    for _ in range(200):
        line = proc.stderr.readline()
        if needle in line:
            return
        if not line and proc.poll() is not None:
            break
    raise AssertionError(f"process never printed {needle!r}")


def test_cli_processes_end_to_end(mock_config_path: Path, tmp_path: Path) -> None:
    """Real processes: respond and listen in the background, send from a third."""
    env_cmd = [sys.executable, "-m", "ironbird.cli.main", "-c", str(mock_config_path)]
    kwargs: dict[str, Any] = {"stdout": subprocess.PIPE, "stderr": subprocess.PIPE, "text": True}
    listen = subprocess.Popen(
        [*env_cmd, "listen", "fc", "--count", "2", "--timeout", "20", "--format", "json"], **kwargs
    )
    respond = subprocess.Popen(
        [*env_cmd, "respond", "--as", "fc", "--count", "2", "--timeout", "20"], **kwargs
    )
    try:
        _wait_for_line(listen, "listening")
        _wait_for_line(respond, "responding")
        for args in (["MOCK_NOOP"], ["MOCK_SET_MODE", "mode=SAFE", "pad=0"]):
            sent = subprocess.run(
                [*env_cmd, "send", "fc", *args],
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
            assert sent.returncode == 0, sent.stderr
            assert sent.stdout.startswith("sent ")
        listen_out, _ = listen.communicate(timeout=20)
        respond_out, _ = respond.communicate(timeout=20)
    finally:
        for proc in (listen, respond):
            if proc.poll() is None:
                proc.kill()
    records = [json.loads(line) for line in listen_out.splitlines()]
    assert [r["name"] for r in records] == ["MOCK_HK", "MOCK_HK"]
    assert records[1]["fields"]["mode"] == 1 and records[1]["labels"]["mode"] == "SAFE"
    assert respond_out.count("rx MOCK_") == 2
    assert listen.returncode == 0 and respond.returncode == 0


def test_wildcard_rule_with_echo_reports_rsp_rule_and_responder_survives(
    mock_config_path: Path, tmp_path: Path
) -> None:
    """Config validation rejects this; the runtime guard covers a rule that slips past it."""
    import dataclasses

    config = load_config(mock_config_path)

    async def scenario(ground: Session, fc: Session, _gn: list[str], fc_notes: list[str]) -> None:
        responder = await fc.respond()
        rules = dict(responder._rules)
        rules["*"] = dataclasses.replace(rules["*"], echo={"mode": "mode"})
        responder._rules = rules
        handled = aiter(responder)
        try:
            await ground.send_command("fc", "MOCK_NOOP")
            event: Handled = await _next(handled)
            assert event.command is not None and event.reply is None
            await ground.send_command("fc", "MOCK_NOOP")  # the responder is still alive
            event = await _next(handled)
            assert event.command is not None
        finally:
            await responder.close()
        assert any(Code.RSP_RULE.value in note for note in fc_notes)

    _, fc_ledger = _run(config, tmp_path, scenario)
    assert any(e.get("event") == Code.RSP_RULE.value for e in _events(fc_ledger))


def test_session_exit_flushes_and_reports_when_a_background_task_died(
    mock_config_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(mock_config_path)
    ledger = _ledger(tmp_path, "ground")
    notes: list[str] = []

    async def broken_drain(self: Ledger) -> None:
        raise RuntimeError("drain exploded")

    monkeypatch.setattr(Ledger, "drain_task", broken_drain)

    async def main() -> None:
        async with Session(config, role="ground", ledger=ledger, notify=notes.append) as ground:
            ground.ledger.record("action", "marker")
            await asyncio.sleep(0.1)

    asyncio.run(main())  # must not raise
    assert any(Code.INTERNAL.value in n for n in notes)
    assert any(e["event"] == "marker" for e in _events(ledger))
