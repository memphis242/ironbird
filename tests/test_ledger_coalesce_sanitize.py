from __future__ import annotations

import asyncio
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
from ironbird.sanitize import clean


class FakeClock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


# -- coalescer ------------------------------------------------------------------------


def test_coalescer_shows_first_then_one_summary() -> None:
    lines: list[str] = []
    clock = FakeClock()
    co = Coalescer(lines.append, window_s=10.0, clock=clock)
    for _ in range(37):
        co.report("k", "error IB-DEC-005: bad")
    assert lines == ["error IB-DEC-005: bad"]
    clock.now += 10.5
    co.tick()
    assert lines[-1] == "error IB-DEC-005: bad (x37 in 10 s)" or lines[-1].startswith(
        "error IB-DEC-005: bad (x37 in"
    )
    assert len(lines) == 2
    co.report("k", "error IB-DEC-005: bad")  # new window announces again
    assert len(lines) == 3


def test_coalescer_flush_and_distinct_keys() -> None:
    lines: list[str] = []
    co = Coalescer(lines.append, clock=FakeClock())
    co.report("a", "A")
    co.report("b", "B")
    co.report("a", "A")
    co.flush()
    assert lines == ["A", "B", "A (x2 in 1 s)"]


def test_coalescer_rate_limits_distinct_keys_per_group_and_bounds_memory() -> None:
    lines: list[str] = []
    co = Coalescer(lines.append, max_keys=8, clock=FakeClock())
    for i in range(1000):
        co.report(f"mid{i}", f"error IB-DEC-006: unknown {i}", group="IB-DEC-006")
    shown = [ln for ln in lines if "unknown" in ln]
    assert len(shown) == MAX_NEW_PER_GROUP
    assert len(co._entries) <= 8
    co.flush()
    assert any("more distinct occurrences" in ln and "x995" in ln for ln in lines)


# -- ledger ---------------------------------------------------------------------------


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


def test_ledger_queue_is_bounded_and_reports_drops(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import ironbird.ledger as mod

    monkeypatch.setattr(mod, "MAX_QUEUE", 10)
    ledger = Ledger(tmp_path, session_id="s")
    ledger._queue = type(ledger._queue)(maxlen=10)
    for i in range(25):
        ledger.record("state", "n", i=i)
    assert ledger.pending == 10 and ledger.dropped == 15
    ledger.flush()
    events = [json.loads(x)["event"] for x in ledger.path.read_text().splitlines()]
    assert events.count("ledger.dropped") == 1


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


# -- terminal injection ---------------------------------------------------------------

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
    assert _no_active_chars(json.loads(record)["fields"]["label"])


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
