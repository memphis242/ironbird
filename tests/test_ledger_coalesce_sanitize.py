from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import json
import os
import stat
from pathlib import Path

import pytest

from conftest import EXAMPLES
from ironbird import render
from ironbird.ccdd.model import MessageKind
from ironbird.coalesce import MAX_NEW_PER_GROUP, Coalescer
from ironbird.codec import Codec, DecodedPacket
from ironbird.errors import Code, IronbirdError
from ironbird.ledger import Ledger, default_state_dir, format_ts
from ironbird.notice import Level, Notice
from ironbird.sanitize import clean


class FakeClock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


# -- coalescer --


def _report(co: Coalescer, key: str, summary: str, *, group: Code | None = None) -> None:
    co.report(Code.DEC_CHECKSUM, Level.ERROR, key, summary, group=group)


def test_coalescer_shows_first_then_one_summary() -> None:
    notices: list[Notice] = []
    clock = FakeClock()
    co = Coalescer(notices.append, window_s=10.0, clock=clock)
    for _ in range(37):
        _report(co, "k", "bad")
    assert [n.line() for n in notices] == ["error IB-DEC-005: bad"]
    assert notices[0].count == 1 and notices[0].window_s == 0.0
    clock.now += 10.5
    co.tick()
    assert len(notices) == 2
    assert notices[1].count == 37 and notices[1].window_s == 10.5
    assert notices[1].line() == "error IB-DEC-005: bad (x37 in 10 s)"
    _report(co, "k", "bad")  # new window announces again
    assert len(notices) == 3 and notices[2].count == 1


def test_coalescer_flush_and_distinct_keys() -> None:
    notices: list[Notice] = []
    co = Coalescer(notices.append, clock=FakeClock())
    _report(co, "a", "A")
    _report(co, "b", "B")
    _report(co, "a", "A")
    co.flush()
    assert [n.line() for n in notices] == [
        "error IB-DEC-005: A",
        "error IB-DEC-005: B",
        "error IB-DEC-005: A (x2 in 1 s)",
    ]


def test_coalescer_summary_carries_the_latest_ledger_record() -> None:
    notices: list[Notice] = []
    co = Coalescer(notices.append, clock=FakeClock())
    for n in (10, 11, 12):
        co.report(Code.DEC_CHECKSUM, Level.ERROR, "k", "bad", ledger_n=n)
    co.flush()
    assert [n.ledger_n for n in notices] == [10, 12]
    assert notices[1].line(verbose=True).endswith("(x3 in 1 s) [ledger n=12]")
    assert "ledger" not in notices[1].line()


def test_coalescer_rate_limits_distinct_keys_per_group_and_bounds_memory() -> None:
    notices: list[Notice] = []
    co = Coalescer(notices.append, max_keys=8, clock=FakeClock())
    for i in range(1000):
        co.report(Code.DEC_UNKNOWN, Level.ERROR, f"mid{i}", f"unknown {i}", group=Code.DEC_UNKNOWN)
    shown = [n for n in notices if "unknown" in n.summary]
    assert len(shown) == MAX_NEW_PER_GROUP
    assert len(co._entries) <= 8
    co.flush()
    folded = [n for n in notices if n.summary == "more distinct occurrences"]
    assert [n.count for n in folded] == [1, 995]  # announced once, then summarised
    assert all(n.code is Code.DEC_UNKNOWN for n in folded)
    assert folded[1].line().startswith("error IB-DEC-006: more distinct occurrences (x995")


def test_notice_line_uses_its_level_and_rejects_inconsistent_values() -> None:
    warning = Notice(Code.TX_RCVBUF, Level.WARNING, "k", "small buffer")
    assert warning.line() == "warning IB-TX-005: small buffer"
    assert Level.INFO.label == "info"
    with pytest.raises(AssertionError):
        Notice(Code.TX_RCVBUF, Level.WARNING, "k", "two\nlines")
    with pytest.raises(AssertionError):
        Notice(Code.TX_RCVBUF, Level.WARNING, "k", "x", count=0)
    with pytest.raises(AssertionError):
        Notice(Code.TX_RCVBUF, Level.WARNING, "k", "x", count=1, window_s=2.0)
    with pytest.raises(dataclasses.FrozenInstanceError):
        warning.count = 5  # type: ignore[misc]


# -- ledger --


def test_ledger_record_is_deferred_and_flush_writes_valid_jsonl(tmp_path: Path) -> None:
    ledger = Ledger(tmp_path / "led", session_id="abc")
    ledger.record("action", "cli.invoke", argv=["send"], raw=b"\x01\x02")
    ledger.record("state", "x", weird=object())
    assert not (tmp_path / "led").exists()  # nothing touched the disk yet
    assert ledger.pending == 2
    ledger.flush()
    lines = ledger.path.read_text().splitlines()
    records = [json.loads(line) for line in lines]
    assert [r["event"] for r in records] == ["cli.invoke", "x"]
    assert records[0]["session"] == "abc" and records[0]["raw"] == "01 02"
    assert records[0]["ts"].endswith("Z") and len(records[0]["ts"]) == len(
        "2026-10-29T12:00:00.123456Z"
    )
    assert stat.S_IMODE(os.stat(ledger.path).st_mode) == 0o600
    assert stat.S_IMODE(os.stat(ledger.directory).st_mode) == 0o700


def test_ledger_record_returns_the_number_it_is_written_with(tmp_path: Path) -> None:
    ledger = Ledger(tmp_path / "state")
    first = ledger.record("action", "a")
    err = ledger.record_error(IronbirdError(Code.DEC_CHECKSUM, "bad"))
    ledger.flush()
    ledger.close()
    written = [json.loads(line) for line in ledger.path.read_text().splitlines()]
    assert (first, err) == (1, 2)
    assert [r["n"] for r in written] == [1, 2]
    assert written[1]["event"] == Code.DEC_CHECKSUM.value


def test_disabled_ledger_has_no_record_number(tmp_path: Path) -> None:
    ledger = Ledger(tmp_path / "state", enabled=False)
    assert ledger.record("action", "a") is None
    assert ledger.record_error(IronbirdError(Code.DEC_CHECKSUM, "bad")) is None


def test_ledger_error_record_has_code_summary_and_details(tmp_path: Path) -> None:
    ledger = Ledger(tmp_path, session_id="s")
    err = IronbirdError(Code.TX_SEND, "boom", details="more", cause=OSError("low level"))
    ledger.record_error(err, role="ground")
    record = json.loads(ledger.path.read_text().splitlines()[0])
    assert record["kind"] == "error" and record["event"] == "IB-TX-002"
    assert "more" in record["details"] and "low level" in record["details"]


def test_ledger_strings_with_escapes_stay_inert_in_file(tmp_path: Path) -> None:
    ledger = Ledger(tmp_path, session_id="s")
    ledger.record("action", "evil", text="\x1b[2J\x9b31m")
    ledger.flush()
    assert "\x1b" not in ledger.path.read_text()


def test_ledger_queue_is_bounded_and_reports_drops(tmp_path: Path) -> None:
    ledger = Ledger(tmp_path, session_id="s", max_queue=10, max_evidence_queue=5)
    for i in range(25):
        ledger.record("state", "n", i=i)
    assert ledger.pending == 10 and ledger.dropped == 15
    ledger.flush()
    records = [json.loads(x) for x in ledger.path.read_text().splitlines()]
    assert [r["i"] for r in records if r["event"] == "n"] == list(range(10))  # oldest kept
    assert [r["event"] for r in records].count("ledger.dropped") == 1
    assert ledger.dropped == 0


def test_ledger_write_failure_is_reported_once(tmp_path: Path) -> None:
    blocker = tmp_path / "file"
    blocker.write_text("x")
    seen: list[IronbirdError] = []
    ledger = Ledger(blocker / "sub", session_id="s", on_failure=seen.append)
    ledger.record("action", "a")
    ledger.flush()
    ledger.record("action", "b")
    ledger.flush()
    assert len(seen) == 1 and seen[0].code is Code.LED_WRITE


def test_ledger_idle_drain_task_writes_without_explicit_flush(tmp_path: Path) -> None:
    async def scenario() -> None:
        ledger = Ledger(tmp_path, session_id="s")
        task = asyncio.create_task(ledger.drain_task())
        ledger.record("action", "hello")
        for _ in range(100):
            await asyncio.sleep(0.02)
            if ledger.path.exists() and ledger.path.read_text():
                break
        task.cancel()
        assert "hello" in ledger.path.read_text()

    asyncio.run(scenario())


def test_state_dir_resolution() -> None:
    assert default_state_dir({"IRONBIRD_STATE_DIR": "/a"}) == Path("/a")
    assert default_state_dir({"XDG_STATE_HOME": "/x"}) == Path("/x/ironbird")
    assert format_ts(1_700_000_000_123_456_789) == "2023-11-14T22:13:20.123456Z"


# -- terminal injection --

HOSTILE = "\x1b[2J\x1b]0;pwn\x07" + chr(0x9B) + "31m" + chr(0x202E) + "[bold]x"


def _no_active_chars(text: str) -> bool:
    return all(ch.isprintable() or ch == "\n" for ch in text) and "\x1b" not in text


def test_clean_neutralizes_controls_c1_bidi_and_bounds_length() -> None:
    out = clean(HOSTILE)
    assert _no_active_chars(out)
    assert "\\x1b" in out and "\\x9b" in out and "\\u202e" in out
    assert len(clean("a" * 10_000)) <= 260


def test_telemetry_string_with_escape_sequences_renders_inert(mock_codec: Codec) -> None:
    # Build a MOCK_HK whose 8-byte label is raw ESC/BEL/control bytes.
    wire = bytearray(mock_codec.encode_telemetry("MOCK_HK", {"label": "abcdefgh"}).data)
    wire[-8:] = b"\x1b[2J\x07\x1b]0"
    decoded = mock_codec.decode(MessageKind.TELEMETRY, bytes(wire))
    assert isinstance(decoded, DecodedPacket)
    line = render.line("fc", 0, decoded)
    assert _no_active_chars(line) and "\\x1b" in line
    record = json.dumps(render.record("fc", 0, decoded))
    assert "\x1b" not in record and "\x07" not in record
    assert json.loads(record)["fields"]["label"] == "\x1b[2J\x07\x1b]0"  # data as is


def test_error_lines_sanitize_untrusted_text() -> None:
    err = IronbirdError(Code.CCD_SCHEMA, f"file.json: {HOSTILE}")
    assert _no_active_chars(err.user_line())


def test_find_output_sanitizes_ccdd_descriptions(tmp_path: Path) -> None:
    from typer.testing import CliRunner

    from ironbird.cli.main import app

    data = json.loads((EXAMPLES / "ccdd" / "mock.json").read_text())
    data["messages"][0]["description"] = "evil \x1b[2J\x1b]0;pwn\x07 text"
    ccdd = tmp_path / "evil.json"
    ccdd.write_text(json.dumps(data))
    config = (EXAMPLES / "mock.toml").read_text()
    config = config.replace('"ccdd/mock.json"', f'"{ccdd}"').replace(
        '"headers/', f'"{EXAMPLES}/headers/'
    )
    config_path = tmp_path / "evil.toml"
    config_path.write_text(config)
    result = CliRunner().invoke(app, ["-c", str(config_path), "find", "evil"])
    assert result.exit_code == 0, result.output
    assert "\x1b" not in result.output and "\x07" not in result.output


# -- ledger robustness (formatting never raises, bounded error flush, raw evidence) --


def _strict_loads(line: str) -> dict[str, object]:
    def refuse(token: str) -> object:
        raise AssertionError(f"non-standard JSON constant {token}")

    return json.loads(line, parse_constant=refuse)


def test_ledger_survives_unformattable_fields_and_keeps_neighbours(tmp_path: Path) -> None:
    ledger = Ledger(tmp_path, session_id="s")
    cycle: list[object] = []
    cycle.append(cycle)
    ledger.record("state", "before")
    ledger.record("state", "tuple-keys", table={(1, 2): "x", None: 1})
    ledger.record("state", "cycle", loop=cycle)
    ledger.record("state", "nan", values=[float("nan"), float("inf"), -float("inf"), 1.5])
    ledger.record("state", "after")
    ledger.flush()
    records = [_strict_loads(line) for line in ledger.path.read_text().splitlines()]
    assert [r["event"] for r in records] == ["before", "tuple-keys", "cycle", "nan", "after"]
    assert records[3]["values"] == ["nan", "inf", "-inf", 1.5]


def test_ledger_unformattable_record_falls_back_to_repr(tmp_path: Path) -> None:
    class Hostile:
        def __repr__(self) -> str:
            raise RuntimeError("no repr for you")

    ledger = Ledger(tmp_path, session_id="s")
    ledger.record("state", "hostile", thing=Hostile())
    ledger.record("state", "after")
    ledger.flush()
    records = [_strict_loads(line) for line in ledger.path.read_text().splitlines()]
    assert [r["event"] for r in records] == ["hostile", "after"]


def test_record_error_flushes_in_bounded_slices_and_drain_task_finishes(tmp_path: Path) -> None:
    ledger = Ledger(tmp_path, session_id="s")
    for i in range(5000):
        ledger.record("packet", "rx", i=i)
    ledger.record_error(IronbirdError(Code.DEC_CHECKSUM, "bad"))
    assert 0 < ledger.pending < 5001  # one bounded slice written, the rest left to the loop

    async def finish() -> None:
        task = asyncio.get_running_loop().create_task(ledger.drain_task())
        for _ in range(500):
            if not ledger.pending:
                break
            await asyncio.sleep(0.01)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    asyncio.run(finish())
    assert ledger.pending == 0
    lines = ledger.path.read_text().splitlines()
    assert len(lines) == 5001 and json.loads(lines[-1])["event"] == "IB-DEC-005"


def test_ledger_raw_evidence_is_kept_up_to_the_max_packet_size(tmp_path: Path) -> None:
    ledger = Ledger(tmp_path, session_id="s", max_raw_bytes=2048)
    ledger.record("packet", "rx", raw=bytes(range(256)) * 8)
    ledger.record("packet", "rx", raw=bytes(2049))
    ledger.flush()
    first, second = (json.loads(x) for x in ledger.path.read_text().splitlines())
    assert len(bytes.fromhex(first["raw"])) == 2048
    assert "...(+1B)" in second["raw"]
