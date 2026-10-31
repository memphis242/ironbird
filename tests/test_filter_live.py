"""``--where`` on ``ib listen`` and ``ib events`` over loopback UDP (ironbird as both ends)."""

from __future__ import annotations

import asyncio
import json
import random
import socket
import subprocess
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from conftest import EXAMPLES, without_host_warnings
from ironbird.ccdd.model import EventSeverity
from ironbird.cli.main import app
from ironbird.config import SystemConfig, load_config
from ironbird.errors import Code
from ironbird.filter import FilterError
from ironbird.frame import Decoded
from ironbird.fsops import FileSystem
from ironbird.ledger import Ledger
from ironbird.session import Responder, Session

PORT_RANGE = (53000, 53999)  # the range mock tests use, away from cFS's fixed ports


def mock_ports(count: int) -> list[int]:
    """Distinct free UDP ports on loopback from :data:`PORT_RANGE` (closed again on return)."""
    found: list[int] = []
    candidates = random.Random().sample(range(PORT_RANGE[0], PORT_RANGE[1] + 1), 200)
    for port in candidates:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.bind(("127.0.0.1", port))
        except OSError:
            continue
        finally:
            sock.close()
        found.append(port)
        if len(found) == count:
            return found
    raise AssertionError("no free port in the mock range")


@pytest.fixture
def config_path(tmp_path: Path) -> Path:
    """examples/mock.toml with absolute paths and ports from the mock range."""
    command_port, telemetry_port = mock_ports(2)
    text = (EXAMPLES / "mock.toml").read_text()
    text = text.replace('"ccdd/', f'"{EXAMPLES}/ccdd/').replace(
        '"headers/', f'"{EXAMPLES}/headers/'
    )
    text = text.replace("127.0.0.1:41234", f"127.0.0.1:{command_port}")
    text = text.replace("127.0.0.1:41235", f"127.0.0.1:{telemetry_port}")
    path = tmp_path / "mock.toml"
    path.write_text(text)
    return path


def run_two(
    config: SystemConfig,
    tmp_path: Path,
    scenario: Callable[[Session, Session], Awaitable[None]],
) -> Ledger:
    ledger = Ledger(tmp_path / "ledgers", session_id="ground")

    async def main() -> None:
        async with (
            Session(config, role="ground", ledger=ledger, notify=lambda _: None) as ground,
            Session(
                config,
                role="fc",
                ledger=Ledger(tmp_path / "ledgers", session_id="fc"),
                notify=lambda _: None,
            ) as fc,
        ):
            await asyncio.wait_for(scenario(ground, fc), timeout=20)

    asyncio.run(main())
    ledger.flush()
    return ledger


def serve(responder: Responder) -> asyncio.Task[None]:
    async def drain() -> None:
        async for _ in responder:
            pass

    return asyncio.get_running_loop().create_task(drain())


async def next_of(source: Any) -> Any:
    """The next item of ``source`` (a stream or listener); every call iterates afresh, because
    a timed-out ``__anext__`` ends its generator while the unread items stay queued."""
    return await asyncio.wait_for(aiter(source).__anext__(), timeout=5)


async def nothing_more(source: Any) -> None:
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(aiter(source).__anext__(), timeout=0.4)


def records_of(ledger: Ledger) -> list[dict[str, Any]]:
    return [json.loads(line) for line in ledger.path.read_text().splitlines()]


def test_listen_where_shows_only_matching_packets_and_ledgers_the_filter(
    config_path: Path, tmp_path: Path
) -> None:
    config = load_config(config_path)

    async def scenario(ground: Session, fc: Session) -> None:
        responder = await fc.respond()
        server = serve(responder)
        listener = await ground.listen("fc", where="msg == MOCK_HK and cmd_counter >= 2")
        try:
            got = listener
            await ground.send_command("fc", "MOCK_NOOP")  # HK with cmd_counter 1: filtered out
            await nothing_more(got)
            await ground.send_command("fc", "MOCK_SET_MODE", {"mode": "SAFE", "pad": "0"})
            shown: Decoded = await next_of(got)
            assert shown.packet.message.name == "MOCK_HK"
            assert shown.packet.as_dict()["cmd_counter"] == 2
            matcher = listener._matcher
            assert matcher is not None and matcher.decodes == 2  # one decode per candidate frame
        finally:
            await listener.close()
            server.cancel()
            await responder.close()

    ledger = run_two(config, tmp_path, scenario)
    start = next(r for r in records_of(ledger) if r["event"] == "listen.start")
    assert start["where"] == "msg == MOCK_HK and cmd_counter >= 2"
    assert start["where_mids"] == [0x0890]


def test_a_filter_that_excludes_the_message_never_decodes_it(
    config_path: Path, tmp_path: Path
) -> None:
    config = load_config(config_path)

    async def scenario(ground: Session, fc: Session) -> None:
        responder = await fc.respond()
        server = serve(responder)
        listener = await ground.listen("fc", where="msg == MOCK_STATUS and temperature_c > 1")
        try:
            got = listener
            for _ in range(3):
                await ground.send_command("fc", "MOCK_NOOP")  # HK, outside the candidate MIDs
            await nothing_more(got)
            assert listener._matcher is not None and listener._matcher.decodes == 0
        finally:
            await listener.close()
            server.cancel()
            await responder.close()

    run_two(config, tmp_path, scenario)


def test_listen_where_combines_with_mid_by_and(config_path: Path, tmp_path: Path) -> None:
    config = load_config(config_path)

    async def scenario(ground: Session, fc: Session) -> None:
        responder = await fc.respond()
        server = serve(responder)
        both = await ground.listen("fc", mids=frozenset({0x0890}), where="cmd_counter == 1")
        try:
            await ground.send_command("fc", "MOCK_NOOP")
            assert (await next_of(both)).packet.as_dict()["cmd_counter"] == 1
            # No MID can satisfy both: refused up front, with nothing opened for it.
            with pytest.raises(FilterError) as info:
                await ground.listen("fc", mids=frozenset({0x0891}), where="cmd_counter == 1")
            assert info.value.code is Code.FLT_TYPE and "--mid" in info.value.summary
            assert "can never match" in info.value.summary
        finally:
            await both.close()
            server.cancel()
            await responder.close()

    run_two(config, tmp_path, scenario)


def test_a_bad_filter_fails_before_anything_is_opened(config_path: Path, tmp_path: Path) -> None:
    config = load_config(config_path)
    telemetry_port = config.links[0].telemetry_endpoint.port

    async def scenario(ground: Session, fc: Session) -> None:
        for text, code in [
            ("cmd_counter >", Code.FLT_SYNTAX),
            ("no_such_field == 1", Code.FLT_NAME),
            ("mode == NOPE", Code.FLT_TYPE),
            ("not " * 40 + "seq == 1", Code.FLT_LIMIT),
        ]:
            with pytest.raises(FilterError) as info:
                await ground.listen("fc", where=text)
            assert info.value.code is code
        with pytest.raises(FilterError):
            await ground.events(where="event.type >= loud")
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            probe.bind(("127.0.0.1", telemetry_port))  # still free: nothing was left open
        finally:
            probe.close()

    ledger = run_two(config, tmp_path, scenario)
    assert not [r for r in records_of(ledger) if r["event"] == "listen.start"]


def test_events_where_matches_min_type_and_the_ledger_keeps_everything(
    config_path: Path, tmp_path: Path
) -> None:
    config = load_config(config_path)

    async def scenario(ground: Session, fc: Session) -> None:
        responder = await fc.respond()
        server = serve(responder)
        by_floor = await ground.events(min_severity=EventSeverity.ERROR)
        by_where = await ground.events(where="event.type >= error")
        by_app = await ground.events(where='event.app == "BETA" and event.id in [8, 9]')
        try:
            floor, where, app_only = by_floor, by_where, by_app
            plan = [
                ("ALPHA", 7, "ERROR"),
                ("BETA", 8, "INFORMATION"),
                ("BETA", 9, "CRITICAL"),
                ("ALPHA", 10, "DEBUG"),
            ]
            for app_name, event_id, kind in plan:
                args = {"app": app_name, "event_id": event_id, "event_type": kind, "message": "t"}
                await ground.send_command("fc", "MOCK_RAISE_EVENT", args)
            ids_floor = [(await next_of(floor)).event_id for _ in range(2)]
            ids_where = [(await next_of(where)).event_id for _ in range(2)]
            assert ids_floor == ids_where == [7, 9]
            assert [(await next_of(app_only)).event_id for _ in range(2)] == [8, 9]
            await nothing_more(floor)
            await nothing_more(where)
            await nothing_more(app_only)
        finally:
            for stream in (by_floor, by_where, by_app):
                await stream.close()
            server.cancel()
            await responder.close()

    ledger = run_two(config, tmp_path, scenario)
    seen = [r["event_id"] for r in records_of(ledger) if r["event"] == "rx.event"]
    assert sorted(set(seen)) == [7, 8, 9, 10]  # hidden events are ledgered in full
    starts = [r for r in records_of(ledger) if r["event"] == "events.start"]
    assert sorted(str(r.get("where")) for r in starts) == sorted(
        ["None", "event.type >= error", 'event.app == "BETA" and event.id in [8, 9]']
    )
    assert "where" not in next(r for r in starts if "where" not in r) and len(starts) == 3


def test_events_where_that_cannot_match_an_event_message_is_refused(
    config_path: Path, tmp_path: Path
) -> None:
    config = load_config(config_path)

    async def scenario(ground: Session, fc: Session) -> None:
        with pytest.raises(FilterError) as info:
            await ground.events(where="msg == MOCK_HK")  # housekeeping is not an event message
        assert info.value.code is Code.FLT_TYPE and "event" in info.value.summary
        stream = await ground.events(where="mid in [0x0890, 0x0891] or event.id == 1")
        await stream.close()  # an event MID is among the candidates: fine

    ledger = run_two(config, tmp_path, scenario)
    assert len([r for r in records_of(ledger) if r["event"] == "events.start"]) == 1


def test_node_in_a_filter_accepts_the_nodes_alias(config_path: Path, tmp_path: Path) -> None:
    config = load_config(config_path)
    assert config.nodes["fc"].alias == "flight"

    async def scenario(ground: Session, fc: Session) -> None:
        responder = await fc.respond()
        server = serve(responder)
        by_alias = await ground.listen("fc", where="node == flight and msg == MOCK_HK")
        by_name = await ground.listen("fc", where="node in [fc, flight]")
        try:
            await ground.send_command("fc", "MOCK_NOOP")
            assert (await next_of(by_alias)).packet.message.name == "MOCK_HK"
            assert (await next_of(by_name)).packet.message.name == "MOCK_HK"
            with pytest.raises(FilterError) as info:
                await ground.listen("fc", where="node == gnd")  # an alias of a node not in use
            assert info.value.code is Code.FLT_TYPE
        finally:
            await by_alias.close()
            await by_name.close()
            server.cancel()
            await responder.close()

    run_two(config, tmp_path, scenario)


def test_the_ledger_records_a_filter_only_when_there_is_one_and_bounds_its_mids(
    config_path: Path, tmp_path: Path
) -> None:
    config = load_config(config_path)
    many = "mid in [" + ", ".join(str(0x1000 + i) for i in range(100)) + "]"

    async def scenario(ground: Session, fc: Session) -> None:
        plain = await ground.listen("fc")
        filtered = await ground.listen("fc", where=many)
        events = await ground.events()
        try:
            pass
        finally:
            await plain.close()
            await filtered.close()
            await events.close()

    ledger = run_two(config, tmp_path, scenario)
    records = records_of(ledger)
    starts = [r for r in records if r["event"] == "listen.start"]
    plain = [r for r in starts if "where" not in r]
    [filtered] = [r for r in starts if "where" in r]
    assert len(plain) == 2  # the plain listener and the events link
    for record in plain:
        assert not {"where", "where_mids", "where_mid_count"} & record.keys()
    assert filtered["where_mid_count"] == 100
    assert filtered["where_mids"] == [0x1000 + i for i in range(32)]
    stops = [r for r in records if r["event"] == "listen.stop"]
    assert sum("where_undecodable" in r for r in stops) == 1
    [events_start] = [r for r in records if r["event"] == "events.start"]
    assert "where" not in events_start


# -- the CLI ---------------------------------------------------------------------------------


@pytest.mark.parametrize("reserve_fs", [None, 0x794C7630], ids=["host-fs", "overlayfs"])
def test_cli_filter_errors_are_one_coded_line_with_details_on_verbose(
    config_path: Path, reserve_fs: int | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    if reserve_fs is not None:  # the state directory as a container's overlayfs shows it
        monkeypatch.setattr(FileSystem, "fstatfs_type", lambda _self, _fd: reserve_fs)
    runner = CliRunner()
    base = ["-c", str(config_path)]
    for command, text, code in [
        ("listen", "cmd_counter >", "IB-FLT-001"),
        ("listen", "cmd_countr > 1", "IB-FLT-002"),
        ("listen", "mode == NOPE", "IB-FLT-003"),
        ("listen", "(" * 40 + "seq == 1" + ")" * 40, "IB-FLT-004"),
        ("events", "event.tpe >= error", "IB-FLT-002"),
    ]:
        argv = [*base, command, *(["fc"] if command == "listen" else []), "--where", text]
        done = runner.invoke(app, [*argv, "--timeout", "1"])
        assert done.exit_code == 2, (text, done.output)
        assert f"error {code}:" in done.output and "listening" not in done.output, done.output
        assert len(without_host_warnings(done.output)) == 1, done.output
    verbose = runner.invoke(
        app, [*base, "listen", "fc", "--where", "cmd_countr > 1", "--verbose", "--timeout", "1"]
    )
    assert "did you mean" in verbose.output and "^" in verbose.output


def wait_for(proc: subprocess.Popen[str], needle: str) -> None:
    assert proc.stderr is not None
    for _ in range(300):
        text = proc.stderr.readline()
        if needle in text:
            return
        if not text and proc.poll() is not None:
            break
    raise AssertionError(f"process never printed {needle!r}")


def test_cli_listen_and_events_where_end_to_end(config_path: Path, tmp_path: Path) -> None:
    """Real processes: a mock flight node, ``ib listen --where`` and ``ib events --where``
    (separate ports would be needed for two listeners, so they run one after the other on the
    same node)."""
    ib = [sys.executable, "-m", "ironbird.cli.main", "-c", str(config_path)]
    pipes: dict[str, Any] = {"stdout": subprocess.PIPE, "stderr": subprocess.PIPE, "text": True}

    def send(*args: str) -> None:
        done = subprocess.run(
            [*ib, "send", "fc", *args], capture_output=True, text=True, timeout=15, check=False
        )
        assert done.returncode == 0, done.stderr

    responder = subprocess.Popen([*ib, "respond", "--as", "fc", "--timeout", "40"], **pipes)
    try:
        wait_for(responder, "responding")
        listener = subprocess.Popen(
            [
                *ib,
                "listen",
                "fc",
                "--where",
                "msg == MOCK_HK and cmd_counter >= 2",
                "--count",
                "1",
                "--timeout",
                "20",
                "--format",
                "json",
            ],
            **pipes,
        )
        try:
            wait_for(listener, "listening for")
            send("MOCK_NOOP")  # cmd_counter 1: not shown
            send("MOCK_NOOP")  # cmd_counter 2: shown
            out, err = listener.communicate(timeout=20)
        finally:
            if listener.poll() is None:
                listener.kill()
                listener.communicate()
        assert listener.returncode == 0, err
        [shown] = [json.loads(ln) for ln in out.splitlines()]
        assert shown["name"] == "MOCK_HK" and shown["fields"]["cmd_counter"] == 2

        events = subprocess.Popen(
            [
                *ib,
                "events",
                "--where",
                'event.app == "BETA" or event.id == 7',
                "--count",
                "2",
                "--timeout",
                "20",
                "--format",
                "json",
            ],
            **pipes,
        )
        try:
            wait_for(events, "listening for events")
            for app_name, event_id in (("ALPHA", 1), ("ALPHA", 7), ("BETA", 2), ("GAMMA", 3)):
                send(
                    "MOCK_RAISE_EVENT",
                    f"app={app_name}",
                    f"event_id={event_id}",
                    "event_type=ERROR",
                    "message=x",
                )
            out, err = events.communicate(timeout=20)
        finally:
            if events.poll() is None:
                events.kill()
                events.communicate()
        assert events.returncode == 0, err
        assert [(r["app"], r["id"]) for r in map(json.loads, out.splitlines())] == [
            ("ALPHA", 7),
            ("BETA", 2),
        ]
    finally:
        if responder.poll() is None:
            responder.kill()
            responder.communicate()
