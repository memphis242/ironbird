"""Review 6 regressions for Stage 2a: ledger record numbers are only handed out for records that
are (or will be) written, the coalescer keeps the last real pointer, the last transmit attempt is
immutable, and immutable mappings serialise as JSON objects."""

from __future__ import annotations

import asyncio
import contextlib
import copy
import dataclasses
import socket
from collections.abc import Generator
from pathlib import Path
from types import MappingProxyType
from typing import Any

import pytest

from conftest import EXAMPLES
from ironbird import header, notice
from ironbird.coalesce import Coalescer
from ironbird.config import load_config
from ironbird.errors import Code, IronbirdError
from ironbird.ledger import Ledger
from ironbird.notice import Level, Notice
from ironbird.session import Session, TxAttempt
from ironbird.transport.udp import UdpTransport
from ledgerutil import all_records

# -- C1: record numbers --


def _written(ledger: Ledger) -> list[dict[str, Any]]:
    """Part 1 without the ledger's own ``ledger.open`` header."""
    return [r for r in all_records(ledger) if r["event"] != "ledger.open"]


def test_a_ledger_in_an_unusable_location_stops_taking_records_after_one_report(
    tmp_path: Path,
) -> None:
    blocker = tmp_path / "blk"
    blocker.write_text("a file, not a directory")
    failures: list[IronbirdError] = []
    ledger = Ledger(blocker / "sub", session_id="f", on_failure=failures.append)
    ledger.record_error(IronbirdError(Code.DEC_CHECKSUM, "bad"))
    ledger.flush()  # the writer's first open finds the location wrong
    assert [f.code for f in failures] == [Code.LED_DIR]
    assert ledger.record_error(IronbirdError(Code.DEC_CHECKSUM, "bad")) is None
    assert not ledger.path.exists()  # nothing was written, so there is nothing to point at


def test_overflow_drops_the_new_record_and_every_handed_out_number_is_written(
    tmp_path: Path,
) -> None:
    ledger = Ledger(tmp_path, session_id="o", max_queue=8, max_evidence_queue=4)
    numbers = [ledger.record("state", "x", i=i) for i in range(12)]
    assert numbers == [*range(1, 9), None, None, None, None]
    assert ledger.dropped == 4
    ledger.flush()
    records = _written(ledger)
    handed_out = [n for n in numbers if n is not None]
    assert [r["n"] for r in records if r["event"] == "x"] == handed_out
    markers = [r for r in records if r["event"] == "ledger.dropped"]
    assert len(markers) == 1 and markers[0]["count"] == 4
    assert [r["n"] for r in records] == sorted(r["n"] for r in records)  # file order = n order
    assert len({r["n"] for r in records}) == len(records)


def test_an_evidence_flood_never_evicts_or_blocks_essential_records(tmp_path: Path) -> None:
    ledger = Ledger(tmp_path, session_id="e", max_queue=10, max_evidence_queue=4)
    packet_numbers = [ledger.record("packet", "rx", i=i) for i in range(50)]
    assert [n for n in packet_numbers if n is not None] == [1, 2, 3, 4]
    essential = [
        ledger.record(kind, "act")
        for kind in ("command", "action", "state", "command", "action", "state")
    ]
    assert all(n is not None for n in essential)  # a full evidence tier leaves their room
    assert ledger.record("packet", "rx") is None
    ledger.flush()
    records = _written(ledger)
    kinds = [r["kind"] for r in records if r["event"] != "ledger.dropped"]
    assert kinds.count("command") == 2 and kinds.count("packet") == 4
    dropped = next(r for r in records if r["event"] == "ledger.dropped")
    assert dropped["count"] == dropped["evidence"] == 47  # 46 + the late packet
    assert [r["n"] for r in records] == sorted(r["n"] for r in records)


def test_essential_overflow_is_counted_not_silent(tmp_path: Path) -> None:
    ledger = Ledger(tmp_path, session_id="s", max_queue=5, max_evidence_queue=2)
    numbers = [ledger.record("command", "tx") for _ in range(8)]
    assert numbers[5:] == [None, None, None]
    ledger.flush()
    dropped = next(r for r in _written(ledger) if r["event"] == "ledger.dropped")
    assert dropped["count"] == 3 and dropped["essential"] == 3


# -- C2: the coalescer keeps the last real pointer --


def test_coalesced_summary_keeps_the_last_non_none_ledger_pointer() -> None:
    out: list[Notice] = []
    co = Coalescer(out.append)
    for n in (5, 7, None, None):
        co.report(Code.DEC_CHECKSUM, Level.ERROR, "k", "bad", ledger_n=n)
    co.flush()
    first, summary = out
    assert first.ledger_n == 5
    assert summary.count == 4 and summary.ledger_n == 7


def test_summary_has_no_pointer_when_no_occurrence_had_one() -> None:
    out: list[Notice] = []
    co = Coalescer(out.append)
    for _ in range(3):
        co.report(Code.DEC_CHECKSUM, Level.ERROR, "k", "bad")
    co.flush()
    assert out[1].ledger_n is None


@contextlib.contextmanager
def _command_listener(config: Any) -> Generator[socket.socket]:
    """A bound UDP socket where the command link points, so a send is not refused."""
    target = config.links[0].command_endpoint
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.bind((target.host, target.port))
        yield sock
    finally:
        sock.close()


# -- C3: the transmit attempt is immutable --


def test_tx_attempt_is_frozen_and_replaced_as_the_send_progresses(
    mock_config_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(dataclasses.FrozenInstanceError):
        TxAttempt("X").went_out = True  # type: ignore[misc]
    config = load_config(mock_config_path)
    seen: list[TxAttempt | None] = []
    real = UdpTransport.send_nowait

    async def main() -> Session:
        async with Session(
            config, role="ground", ledger=Ledger(tmp_path / "l"), notify=lambda _: None
        ) as ground:

            def spy(self: UdpTransport, data: bytes) -> None:
                seen.append(ground.last_tx)  # what a signal handler would read before the send
                real(self, data)

            monkeypatch.setattr(UdpTransport, "send_nowait", spy)
            await ground.send_command("fc", "MOCK_NOOP")
            return ground

    with _command_listener(config):
        asyncio.run(main())
    before = seen[0]
    assert before is not None and before.command == "MOCK_NOOP" and not before.went_out


def test_last_tx_after_a_send_reports_it_went_out(mock_config_path: Path, tmp_path: Path) -> None:
    config = load_config(mock_config_path)

    async def main() -> tuple[TxAttempt | None, TxAttempt | None]:
        async with Session(
            config, role="ground", ledger=Ledger(tmp_path / "l"), notify=lambda _: None
        ) as ground:
            first = ground.last_tx
            await ground.send_command("fc", "MOCK_NOOP")
            return first, ground.last_tx

    with _command_listener(config):
        first, after = asyncio.run(main())
    assert first is None
    assert after is not None and after.went_out and after.command == "MOCK_NOOP"


# -- F2: immutable mappings serialise as objects --


def test_mapping_proxy_values_are_ledgered_as_json_objects(tmp_path: Path) -> None:
    profile = header.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml")
    parsed = header.unpack(
        profile, header.pack(profile, b"", mid=0x1882, inputs={"function_code": 3})
    )
    assert isinstance(parsed, header.Header)
    nested = MappingProxyType({"inner": MappingProxyType({"a": 1}), "list": [MappingProxyType({})]})
    ledger = Ledger(tmp_path, session_id="m")
    ledger.record("state", "hdr", values=parsed.values, nested=nested)
    ledger.record("state", "tuple-keys", table=MappingProxyType({(1, 2): "x"}))
    ledger.flush()
    first, second = _written(ledger)
    assert first["values"]["function_code"] == 3
    assert first["nested"] == {"inner": {"a": 1}, "list": [{}]}
    assert "mappingproxy" not in ledger.path.read_text()
    assert second["table"] == {"(1, 2)": "x"}


def test_unpacked_header_survives_copy_and_deepcopy() -> None:
    profile = header.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml")
    parsed = header.unpack(
        profile, header.pack(profile, b"", mid=0x1882, inputs={"function_code": 3})
    )
    assert isinstance(parsed, header.Header)
    assert copy.deepcopy(parsed) == parsed and copy.copy(parsed) == parsed
    with pytest.raises(TypeError):
        copy.deepcopy(parsed).values["function_code"] = 9  # type: ignore[index]


# -- F4: label table and error summary are read-only ------------------------------------


def test_level_labels_and_error_summary_are_read_only() -> None:
    with pytest.raises(TypeError):
        notice._LABELS[Level.INFO] = "x"  # type: ignore[index]
    err = IronbirdError(Code.TX_SEND, "boom")
    with pytest.raises(AttributeError):
        err.summary = "other"  # type: ignore[misc]
    with pytest.raises(AttributeError):
        err.code = Code.TX_OPEN  # type: ignore[misc]
    assert err.summary == "boom" and err.user_line() == "error IB-TX-002: boom"


# -- F3: --verbose names the ledger file once -------------------------------------------


@pytest.mark.parametrize("verbose", [False, True])
def test_verbose_names_the_ledger_file_once_at_start(
    mock_config_path: Path, tmp_path: Path, verbose: bool
) -> None:
    import os
    import subprocess
    import sys

    state = tmp_path / "state"
    cmd = [sys.executable, "-m", "ironbird.cli.main", "-c", str(mock_config_path)]
    cmd += ["--verbose", "nodes"] if verbose else ["nodes"]
    proc = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
        env={**os.environ, "IRONBIRD_STATE_DIR": str(state)},
    )
    assert proc.returncode == 0, proc.stderr
    (ledger_file,) = state.glob("*.jsonl")
    assert (proc.stderr.count(ledger_file.name) == 1) is verbose
    assert ledger_file.name not in proc.stdout
