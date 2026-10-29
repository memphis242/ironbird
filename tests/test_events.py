"""Telemetry categories, event mapping, extraction and ``ib events`` over loopback UDP."""

from __future__ import annotations

import asyncio
import copy
import json
import struct
import subprocess
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from typer.testing import CliRunner

from conftest import EXAMPLES, free_udp_ports
from ironbird.ccdd.json_adapter import parse_model
from ironbird.ccdd.loader import load_ccdd
from ironbird.ccdd.model import Category, EventSeverity, MessageKind
from ironbird.cli.main import app
from ironbird.codec import Codec, DecodedPacket
from ironbird.config import SystemConfig, load_config
from ironbird.errors import Code, IronbirdError
from ironbird.events import Event, extract_event
from ironbird.header import load_profile
from ironbird.ledger import Ledger
from ironbird.render import event_line, event_record, line, record
from ironbird.session import Responder, Session

TLM = MessageKind.TELEMETRY


def _mock_json() -> dict[str, Any]:
    return json.loads((EXAMPLES / "ccdd" / "mock.json").read_text())


def _codec(name: str) -> Codec:
    return Codec(
        load_ccdd(EXAMPLES / "ccdd" / name),
        load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml"),
        load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml"),
        clock=lambda: 1_000_000.25,
    )


def _decode(codec: Codec, data: bytes) -> DecodedPacket:
    result = codec.decode(TLM, data)
    assert isinstance(result, DecodedPacket), result
    return result


# -- category model ----------------------------------------------------------------------


def test_category_priority_is_the_documented_order() -> None:
    assert [c.value for c in Category.by_priority()] == [
        "event",
        "command_response",
        "housekeeping",
        "status",
        "commanded",
        "other",
    ]
    assert [c.priority for c in Category.by_priority()] == [1, 2, 3, 4, 5, 6]
    assert Category.EVENT.priority < Category.HOUSEKEEPING.priority < Category.OTHER.priority


def test_severity_orders_from_debug_to_critical() -> None:
    assert EventSeverity.DEBUG < EventSeverity.INFO < EventSeverity.ERROR < EventSeverity.CRITICAL


def test_categories_default_to_other_and_events_are_inferred() -> None:
    data = _mock_json()
    # Drop every explicit category; the event mapping alone makes MOCK_EVENT an event.
    for msg in data["messages"]:
        msg.pop("category", None)
    model = parse_model(data, "t.json")
    assert model.by_name["MOCK_HK"].category is Category.OTHER
    assert model.by_name["MOCK_EVENT"].category is Category.EVENT
    assert model.by_name["MOCK_NOOP"].category is Category.OTHER  # commands carry no category


def test_example_ccdd_categories() -> None:
    mock = load_ccdd(EXAMPLES / "ccdd" / "mock.json")
    assert mock.by_name["MOCK_HK"].category is Category.HOUSEKEEPING
    assert mock.by_name["MOCK_STATUS"].category is Category.STATUS
    assert [m.name for m in mock.event_messages()] == ["MOCK_EVENT"]

    cfs = load_ccdd(EXAMPLES / "ccdd" / "cfs.json")
    assert {m.name: m.category.value for m in cfs.telemetry()} == {
        "ES_HK": "housekeeping",
        "EVS_HK": "housekeeping",
        "EVS_LONG_EVENT": "event",
        "EVS_SHORT_EVENT": "event",
        "TIME_HK": "housekeeping",
        "TIME_DIAG": "commanded",
        "SB_HK": "housekeeping",
        "TBL_HK": "housekeeping",
        "SAMPLE_APP_HK": "housekeeping",
        "CI_LAB_HK": "housekeeping",
        "TO_LAB_HK": "housekeeping",
        "TO_LAB_DATA_TYPES": "commanded",
    }
    long_event = cfs.by_name["EVS_LONG_EVENT"]
    assert long_event.event is not None
    assert (long_event.event.app, long_event.event.id) == ("AppName", "EventID")
    assert dict(long_event.event.severities) == {
        1: EventSeverity.DEBUG,
        2: EventSeverity.INFO,
        3: EventSeverity.ERROR,
        4: EventSeverity.CRITICAL,
    }
    short_event = cfs.by_name["EVS_SHORT_EVENT"]
    assert short_event.event is not None and short_event.event.message is None


@pytest.mark.parametrize(
    ("edit", "code", "needle"),
    [
        (lambda m: m[3].update(category="urgent"), Code.CCD_SCHEMA, "category"),
        (lambda m: m[3].update(category=7), Code.CCD_SCHEMA, "category"),
        (lambda m: m[0].update(category="housekeeping"), Code.CCD_SCHEMA, "only valid on telem"),
        (
            lambda m: m[3].update(event={"id": "cmd_counter", "type": "mode"}),
            Code.CCD_SCHEMA,
            "only valid with category",
        ),
        (lambda m: m[6].pop("event"), Code.CCD_SCHEMA, "event: required for category"),
        (lambda m: m[6].update(event=[]), Code.CCD_SCHEMA, "must be an object"),
        (lambda m: m[6]["event"].pop("id"), Code.CCD_SCHEMA, "event.id: required"),
        (lambda m: m[6]["event"].pop("type"), Code.CCD_SCHEMA, "event.type: required"),
        (lambda m: m[6]["event"].update(extra="x"), Code.CCD_SCHEMA, "extra: unknown key"),
        (lambda m: m[6]["event"].update(id="nope"), Code.CCD_SCHEMA, "no field named"),
        (lambda m: m[6]["event"].update(id=3), Code.CCD_SCHEMA, "must be a field name"),
        (lambda m: m[6]["event"].update(app="event_id"), Code.CCD_SCHEMA, "must be of char type"),
        (lambda m: m[6]["event"].update(id="message"), Code.CCD_SCHEMA, "must be of int type"),
        (lambda m: m[6]["fields"][2].pop("enum"), Code.CCD_SCHEMA, "needs an enum"),
        (
            lambda m: m[6]["fields"][2].update(enum={"INFORMATION": 2, "PANIC": 9}),
            Code.CCD_SCHEMA,
            "'PANIC' is not an event type",
        ),
    ],
)
def test_category_and_event_validation_errors(edit: Any, code: Code, needle: str) -> None:
    data = copy.deepcopy(_mock_json())
    edit(data["messages"])
    with pytest.raises(IronbirdError) as info:
        parse_model(data, "t.json")
    assert info.value.code is code
    assert info.value.summary.startswith("t.json: ")
    assert needle in info.value.summary
    assert "\n" not in info.value.user_line()


def test_event_type_labels_are_matched_case_insensitively() -> None:
    data = _mock_json()
    data["messages"][6]["fields"][2]["enum"] = {"info": 2, "Warn": 5, "fatal_not": 6}
    with pytest.raises(IronbirdError, match="fatal_not"):
        parse_model(data, "t.json")
    data["messages"][6]["fields"][2]["enum"] = {"info": 2, "Warn": 5, "Critical": 6}
    event = parse_model(data, "t.json").by_name["MOCK_EVENT"].event
    assert event is not None
    assert dict(event.severities) == {
        2: EventSeverity.INFO,
        5: EventSeverity.WARNING,
        6: EventSeverity.CRITICAL,
    }


# -- extraction and rendering ---------------------------------------------------------------


def _long_event_bytes(app: bytes, event_id: int, event_type: int, text: bytes) -> bytes:
    """A cFE long event packet built by hand from the C layout (tools/cfs/NOTES.md)."""
    codec = _codec("cfs.json")
    header = codec.encode_telemetry("EVS_LONG_EVENT").data[:16]
    payload = struct.pack("<20sHHII122s2x", app, event_id, event_type, 0x42, 1, text)
    assert len(payload) == 156
    return header + payload


def test_cfs_long_event_decodes_to_an_event() -> None:
    codec = _codec("cfs.json")
    packet = _decode(codec, _long_event_bytes(b"SAMPLE_APP", 3, 2, b"SAMPLE: NOOP command 1.0"))
    event = extract_event("fc", 1_700_000_000_123_456_000, packet)
    assert event is not None
    assert (event.app, event.event_id, event.type_label) == ("SAMPLE_APP", 3, "INFORMATION")
    assert event.severity is EventSeverity.INFO and event.text == "SAMPLE: NOOP command 1.0"
    assert event_line(event) == (
        "2023-11-14T22:13:20.123456Z fc SAMPLE_APP INFORMATION id=3 SAMPLE: NOOP command 1.0"
    )
    rec = event_record(event)
    assert (rec["app"], rec["id"], rec["type"], rec["severity"]) == (
        "SAMPLE_APP",
        3,
        "INFORMATION",
        "INFO",
    )
    assert rec["mid"] == "0x0808" and rec["node"] == "fc"


def test_cfs_short_event_has_no_text_and_unknown_type_is_shown_not_hidden() -> None:
    codec = _codec("cfs.json")
    raw = codec.encode_telemetry(
        "EVS_SHORT_EVENT", {"AppName": "CFE_ES", "EventID": 9, "EventType": 77}
    ).data
    event = extract_event("fc", 0, _decode(codec, raw))
    assert event is not None and event.text == "" and event.severity is None
    assert event_line(event).endswith("CFE_ES UNKNOWN(77) id=9")


def test_non_event_telemetry_is_not_an_event() -> None:
    codec = _codec("mock.json")
    assert extract_event("fc", 0, _decode(codec, codec.encode_telemetry("MOCK_HK").data)) is None


def test_listen_lines_and_records_carry_the_category() -> None:
    codec = _codec("mock.json")
    packet = _decode(codec, codec.encode_telemetry("MOCK_HK").data)
    assert " cat=housekeeping " in line("fc", 0, packet)
    assert record("fc", 0, packet)["category"] == "housekeeping"
    command = codec.decode(MessageKind.COMMAND, codec.encode_command("MOCK_NOOP").data)
    assert isinstance(command, DecodedPacket)
    assert "cat=" not in line("fc", 0, command) and record("fc", 0, command)["category"] is None


def test_event_text_is_inert_when_displayed() -> None:
    codec = _codec("mock.json")
    evil = "\x1b[31mred\x07\nline2\x7f"
    raw = codec.encode_telemetry(
        "MOCK_EVENT", {"app": "A\x1bB", "event_id": 1, "event_type": 2, "message": evil}
    ).data
    event = extract_event("fc", 0, _decode(codec, raw))
    assert event is not None
    text = event_line(event)
    assert "\n" not in text and "\x1b" not in text and "\x07" not in text and "\x9b" not in text
    assert "\\x1b" in text and "\\x0a" in text
    payload = json.dumps(event_record(event))
    assert "\x1b" not in payload


EVENT_PAYLOAD_SIZE = 44  # MOCK_EVENT: char[8] + u16 + u16 + char[32]


@settings(max_examples=300, deadline=1000, suppress_health_check=[HealthCheck.too_slow])
@given(st.binary(min_size=EVENT_PAYLOAD_SIZE, max_size=EVENT_PAYLOAD_SIZE))
def test_any_event_payload_extracts_without_raising_and_renders_inert(payload: bytes) -> None:
    codec = _codec("mock.json")
    header = codec.encode_telemetry("MOCK_EVENT").data[:-EVENT_PAYLOAD_SIZE]
    packet = _decode(codec, header + payload)
    event = extract_event("fc", 1, packet)
    assert event is not None
    line = event_line(event)
    assert "\n" not in line and line.isprintable()
    # JSON values carry the data as is; json.dumps makes the serialised record inert.
    serialised = json.dumps(event_record(event))
    assert "\n" not in serialised and serialised.isprintable()
    assert event.severity is None or event.type_label != ""


# -- ib events over loopback UDP: two nodes ---------------------------------------------------


@pytest.fixture
def two_node_config_path(mock_config_path: Path) -> Path:
    """The mock config plus a second flight node ``fc2`` on its own ports."""
    command_port, telemetry_port = free_udp_ports(2)
    text = mock_config_path.read_text()
    first_rules = text[text.index("# The mock reports") : text.index("[[links]]")]
    rules = first_rules[: first_rules.index('[[nodes.fc.responses]]\non = "*"')]
    second = (
        f'\n[nodes.fc2]\nalias = "flight2"\ndescription = "Second mock flight computer"\n'
        f'ccdd = "{EXAMPLES}/ccdd/mock.json"\n'
        f'command_header = "{EXAMPLES}/headers/ccsds_v1_cfs_cmd.toml"\n'
        f'telemetry_header = "{EXAMPLES}/headers/ccsds_v1_cfs_tlm.toml"\n\n'
        + rules.replace("nodes.fc.", "nodes.fc2.")
        + f'[[links]]\nname = "ground-fc2"\nfrom = "ground"\nto = "fc2"\ntransport = "udp"\n'
        f'command_endpoint = "127.0.0.1:{command_port}"\n'
        f'telemetry_endpoint = "127.0.0.1:{telemetry_port}"\n'
    )
    path = mock_config_path.with_name("two.toml")
    path.write_text(text + second)
    return path


def _ledger(tmp_path: Path, name: str) -> Ledger:
    return Ledger(tmp_path / "ledgers", session_id=name)


async def _next_event(iterator: Any) -> Event:
    return await asyncio.wait_for(iterator.__anext__(), timeout=5)


def _run3(
    config: SystemConfig,
    tmp_path: Path,
    scenario: Callable[[Session, Session, Session], Awaitable[None]],
) -> Ledger:
    ledger = _ledger(tmp_path, "ground")

    async def main() -> None:
        async with (
            Session(config, role="ground", ledger=ledger, notify=lambda _: None) as ground,
            Session(config, role="fc", ledger=_ledger(tmp_path, "fc"), notify=lambda _: None) as fc,
            Session(
                config, role="fc2", ledger=_ledger(tmp_path, "fc2"), notify=lambda _: None
            ) as fc2,
        ):
            await asyncio.wait_for(scenario(ground, fc, fc2), timeout=20)

    asyncio.run(main())
    ledger.flush()
    return ledger


def _serve(responder: Responder) -> asyncio.Task[None]:
    """Process commands in the background until cancelled (a responder acts only when iterated)."""

    async def drain() -> None:
        async for _ in responder:
            pass

    return asyncio.get_running_loop().create_task(drain())


def _raise_event(
    session: Session, node: str, app: str, event_id: int, kind: str, text: str
) -> Awaitable[Any]:
    args = {"app": app, "event_id": event_id, "event_type": kind, "message": text}
    return session.send_command(node, "MOCK_RAISE_EVENT", args)


def test_events_from_all_nodes_arrive_in_one_stream_and_are_ledgered_in_full(
    two_node_config_path: Path, tmp_path: Path
) -> None:
    config = load_config(two_node_config_path)

    async def scenario(ground: Session, fc: Session, fc2: Session) -> None:
        responders = [await fc.respond(), await fc2.respond()]
        servers = [_serve(r) for r in responders]
        stream = await ground.events()
        try:
            assert sorted(n for n, _ in stream.endpoints) == ["fc", "fc2"]
            events = aiter(stream)
            await _raise_event(ground, "fc", "ALPHA", 7, "ERROR", "bus voltage low")
            await _raise_event(ground, "fc2", "BETA", 9, "INFORMATION", "all quiet")
            await ground.send_command("fc", "MOCK_NOOP")  # answered with HK: not an event
            got = sorted(
                [await _next_event(events), await _next_event(events)], key=lambda e: e.node
            )
            assert [(e.node, e.app, e.event_id, e.type_label) for e in got] == [
                ("fc", "ALPHA", 7, "ERROR"),
                ("fc2", "BETA", 9, "INFORMATION"),
            ]
            assert got[0].severity is EventSeverity.ERROR and got[0].text == "bus voltage low"
            with pytest.raises(asyncio.TimeoutError):  # the HK packet never shows up
                await asyncio.wait_for(_next_event(events), timeout=0.3)
        finally:
            await stream.close()
            for server in servers:
                server.cancel()
            for responder in responders:
                await responder.close()

    ledger = _run3(config, tmp_path, scenario)
    records = [json.loads(ln) for ln in ledger.path.read_text().splitlines()]
    seen = [r for r in records if r["event"] == "rx.event"]
    assert {(r["node"], r["app"], r["event_id"], r["text"]) for r in seen} == {
        ("fc", "ALPHA", 7, "bus voltage low"),
        ("fc2", "BETA", 9, "all quiet"),
    }
    assert all(r["kind"] == "event" and r["raw"] and r["mid"] == 0x0892 for r in seen)
    assert [r["event"] for r in records if r["event"].startswith("events.")] == [
        "events.start",
        "events.stop",
    ]


def test_min_severity_hides_lower_events_but_the_ledger_keeps_them(
    two_node_config_path: Path, tmp_path: Path
) -> None:
    config = load_config(two_node_config_path)

    async def scenario(ground: Session, fc: Session, fc2: Session) -> None:
        responder = await fc.respond()
        server = _serve(responder)
        stream = await ground.events(["fc"], min_severity=EventSeverity.ERROR)
        try:
            events = aiter(stream)
            await _raise_event(ground, "fc", "A", 1, "DEBUG", "chatter")
            await _raise_event(ground, "fc", "A", 2, "INFORMATION", "fyi")
            await _raise_event(ground, "fc", "A", 3, "CRITICAL", "overheat")
            assert (await _next_event(events)).event_id == 3
            await _raise_event(ground, "fc", "A", 4, "ERROR", "fault")
            assert (await _next_event(events)).event_id == 4
        finally:
            await stream.close()
            server.cancel()
            await responder.close()

    ledger = _run3(config, tmp_path, scenario)
    ids = [
        json.loads(ln)["event_id"]
        for ln in ledger.path.read_text().splitlines()
        if '"rx.event"' in ln
    ]
    assert ids == [1, 2, 3, 4]


def test_node_selection_and_errors(two_node_config_path: Path, tmp_path: Path) -> None:
    config = load_config(two_node_config_path)

    async def scenario(ground: Session, fc: Session, fc2: Session) -> None:
        responder = await fc2.respond()
        server = _serve(responder)
        only_fc = await ground.events(["flight"])  # alias; fc2's events are not listened to
        try:
            assert [n for n, _ in only_fc.endpoints] == ["fc"]
            await _raise_event(ground, "fc2", "BETA", 1, "ERROR", "elsewhere")
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(_next_event(aiter(only_fc)), timeout=0.3)
        finally:
            await only_fc.close()
            server.cancel()
            await responder.close()
        with pytest.raises(IronbirdError) as unknown:
            await ground.events(["ghost"])
        assert unknown.value.code is Code.CFG_REF
        with pytest.raises(IronbirdError) as wrong_side:
            await fc.events()  # fc only receives commands
        assert wrong_side.value.code is Code.CFG_LINK

    _run3(config, tmp_path, scenario)


def test_node_without_event_definitions_is_an_error(mock_config_path: Path, tmp_path: Path) -> None:
    data = _mock_json()
    data["messages"] = [
        m for m in data["messages"] if "event" not in m and m["name"] != "MOCK_RAISE_EVENT"
    ]
    plain = tmp_path / "plain.json"
    plain.write_text(json.dumps(data))
    rule = (
        '[[nodes.fc.responses]]\non = "MOCK_RAISE_EVENT"\nreply = "MOCK_EVENT"\n'
        'echo = { app = "app", event_id = "event_id", event_type = "event_type", '
        'message = "message" }\n'
    )
    text = mock_config_path.read_text()
    assert rule in text
    mock_config_path.write_text(
        text.replace(rule, "").replace(f"{EXAMPLES}/ccdd/mock.json", str(plain))
    )
    config = load_config(mock_config_path)

    async def main() -> None:
        async with Session(
            config, role="ground", ledger=_ledger(tmp_path, "g"), notify=lambda _: None
        ) as ground:
            with pytest.raises(IronbirdError) as info:
                await ground.events()
            assert info.value.code is Code.EVT_NONE and info.value.exit_code == 2

    asyncio.run(main())


# -- the CLI ----------------------------------------------------------------------------------


def test_cli_events_usage_errors(mock_config_path: Path) -> None:
    runner = CliRunner()
    base = ["-c", str(mock_config_path)]
    ghost = runner.invoke(app, [*base, "events", "--node", "ghost", "--timeout", "1"])
    assert ghost.exit_code == 2 and "IB-CFG-004" in ghost.output
    wrong = runner.invoke(app, [*base, "--as", "fc", "events", "--timeout", "1"])
    assert wrong.exit_code == 2 and "IB-CFG-005" in wrong.output
    bad_type = runner.invoke(app, [*base, "events", "--min-type", "loud"])
    assert bad_type.exit_code != 0


def _wait_for_line(proc: subprocess.Popen[str], needle: str) -> None:
    assert proc.stderr is not None
    for _ in range(200):
        text = proc.stderr.readline()
        if needle in text:
            return
        if not text and proc.poll() is not None:
            break
    raise AssertionError(f"process never printed {needle!r}")


def test_cli_events_across_two_nodes_end_to_end(two_node_config_path: Path) -> None:
    """Real processes: two mock flight nodes, one ``ib events``, commands from a fourth."""
    ib = [sys.executable, "-m", "ironbird.cli.main", "-c", str(two_node_config_path)]
    pipes: dict[str, Any] = {"stdout": subprocess.PIPE, "stderr": subprocess.PIPE, "text": True}
    events = subprocess.Popen(
        [
            *ib,
            "events",
            "--count",
            "2",
            "--timeout",
            "20",
            "--format",
            "json",
            "--min-type",
            "error",
        ],
        **pipes,
    )
    responders = [
        subprocess.Popen([*ib, "respond", "--as", node, "--timeout", "20"], **pipes)
        for node in ("fc", "fc2")
    ]
    try:
        _wait_for_line(events, "listening for events from")
        for responder in responders:
            _wait_for_line(responder, "responding")
        plan = [
            ("fc", ["app=ALPHA", "event_id=7", "event_type=ERROR", "message=bus voltage low"]),
            (
                "fc",
                ["app=ALPHA", "event_id=8", "event_type=INFORMATION", "message=below the floor"],
            ),
            ("fc2", ["app=BETA", "event_id=9", "event_type=CRITICAL", "message=\x1b[2Jreset"]),
        ]
        for node, args in plan:
            sent = subprocess.run(
                [*ib, "send", node, "MOCK_RAISE_EVENT", *args],
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
            assert sent.returncode == 0, sent.stderr
        out, _ = events.communicate(timeout=20)
    finally:
        for proc in (events, *responders):
            if proc.poll() is None:
                proc.kill()
                proc.communicate()
    records = sorted((json.loads(ln) for ln in out.splitlines()), key=lambda r: r["node"])
    assert [(r["node"], r["app"], r["id"], r["type"], r["severity"]) for r in records] == [
        ("fc", "ALPHA", 7, "ERROR", "ERROR"),
        ("fc2", "BETA", 9, "CRITICAL", "CRITICAL"),
    ]
    assert "\x1b" not in out  # the serialised record is inert
    assert records[1]["message"] == "\x1b[2Jreset"  # the data itself is not altered
    assert events.returncode == 0
