"""Stage 2a: structured notices reach the front-end with a ledger pointer, and the data the
engine shares (headers, registries, profiles, statistics) cannot be mutated by its readers."""

from __future__ import annotations

import asyncio
import dataclasses
import json
import socket
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from conftest import EXAMPLES
from ironbird import checksum, header
from ironbird.ccdd import ADAPTERS
from ironbird.ccdd.model import MessageKind
from ironbird.codec import Codec
from ironbird.config import load_config
from ironbird.errors import Code, DecodeError, IronbirdError
from ironbird.ledger import Ledger
from ironbird.notice import Level, Notice
from ironbird.session import MidStatSnapshot, Session

# -- notices from a live session --


def test_session_notices_point_at_their_ledger_records(
    mock_config_path: Path, tmp_path: Path
) -> None:
    config = load_config(mock_config_path)
    ledger = Ledger(tmp_path / "ledgers", session_id="g")
    notices: list[Notice] = []

    async def main() -> None:
        async with Session(config, role="ground", ledger=ledger, notify=notices.append) as ground:
            for _ in range(5):
                ground.report(DecodeError(Code.DEC_CHECKSUM, 7, "bad sum", 0x0881), context="x")
            ground.report(IronbirdError(Code.TX_RCVBUF, "small buffer"), level=Level.WARNING)
            ground.flush_notices()

    asyncio.run(main())
    ledger.flush()
    records = {r["n"]: r for r in map(json.loads, ledger.path.read_text().splitlines())}
    first, warning, summary = notices
    assert (first.count, first.level) == (1, Level.ERROR)
    assert (summary.count, summary.code) == (5, Code.DEC_CHECKSUM)
    assert first.ledger_n is not None and summary.ledger_n is not None
    assert summary.ledger_n > first.ledger_n  # the summary points at the latest occurrence
    for notice in (first, summary, warning):
        assert notice.ledger_n is not None
        assert records[notice.ledger_n]["event"] == notice.code.value
    assert warning.line() == "warning IB-TX-005: small buffer"
    assert records[summary.ledger_n]["mid"] == 0x0881


def test_notices_without_a_ledger_have_no_pointer(mock_config_path: Path, tmp_path: Path) -> None:
    config = load_config(mock_config_path)
    ledger = Ledger(tmp_path / "ledgers", enabled=False)
    notices: list[Notice] = []

    async def main() -> None:
        async with Session(config, role="ground", ledger=ledger, notify=notices.append) as ground:
            ground.report(IronbirdError(Code.TX_RCVBUF, "small buffer"))

    asyncio.run(main())
    assert [n.ledger_n for n in notices] == [None]
    assert "ledger" not in notices[0].line(verbose=True)


@pytest.mark.parametrize("verbose", [False, True])
def test_cli_verbose_notice_names_its_ledger_record(
    mock_config_path: Path, tmp_path: Path, verbose: bool
) -> None:
    config = load_config(mock_config_path)
    target = config.links[0].command_endpoint
    cmd = [sys.executable, "-m", "ironbird.cli.main", "-c", str(mock_config_path)]
    if verbose:
        cmd.append("--verbose")
    proc = subprocess.Popen(
        [*cmd, "respond", "--as", "fc", "--timeout", "20"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert proc.stderr is not None
        for _ in range(200):
            if "responding" in proc.stderr.readline():
                break
        else:
            raise AssertionError("responder never started")
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.sendto(b"\x00\x01garbage", (target.host, target.port))
        finally:
            sock.close()
        line = ""
        for _ in range(200):
            line = proc.stderr.readline()
            if "IB-DEC-" in line:
                break
        else:
            raise AssertionError("no decode notice was printed")
    finally:
        proc.kill()
        proc.wait(timeout=5)
    assert line.startswith("error IB-DEC-")
    assert ("[ledger n=" in line) is verbose


# -- shared data is immutable --


def test_unpacked_header_values_reject_mutation() -> None:
    profile = header.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml")
    packet = header.pack(profile, b"", mid=0x1882, inputs={"function_code": 3})
    parsed = header.unpack(profile, packet)
    assert isinstance(parsed, header.Header)
    assert parsed.values["function_code"] == 3
    with pytest.raises(TypeError):
        parsed.values["function_code"] = 4  # type: ignore[index]
    with pytest.raises(dataclasses.FrozenInstanceError):
        parsed.mid = 1  # type: ignore[misc]


def test_registries_and_profile_tables_reject_mutation(mock_codec: Codec) -> None:
    profile = mock_codec.profile(MessageKind.TELEMETRY)
    tables: list[Any] = [
        checksum.REGISTRY,
        ADAPTERS,
        profile.by_name,
        header.COMPUTED_KINDS,
        mock_codec._profiles,
        mock_codec._layouts,
        mock_codec.model.by_name,
        mock_codec.model.telemetry_by_mid,
        mock_codec.model.commands_by_key,
    ]
    for table in tables:
        with pytest.raises(TypeError):
            table["new"] = 1
        with pytest.raises(TypeError):
            del table[next(iter(table))]
    assert isinstance(profile.fields, tuple)


# -- session state is read-only ---------------------------------------------------------


def test_session_state_is_read_only_and_stats_are_snapshots(
    mock_config_path: Path, tmp_path: Path
) -> None:
    config = load_config(mock_config_path)
    ledger = Ledger(tmp_path / "ledgers")

    async def main() -> None:
        async with Session(config, role="ground", ledger=ledger, notify=lambda _: None) as ground:
            for name in (
                "config",
                "node",
                "ledger",
                "last_tx",
                "stats_overflow",
                "malformed_total",
            ):
                with pytest.raises(AttributeError):
                    setattr(ground, name, None)
            assert not hasattr(ground, "coalescer") and not hasattr(ground, "stats")
            ground._engine._count(0x0881, 5)
            before = ground.stats_snapshot()
            ground._engine._count(0x0881, 9)
            assert before[0x0881] == MidStatSnapshot(1, 5)  # a snapshot does not follow
            assert ground.stats_snapshot()[0x0881] == MidStatSnapshot(2, 9)
            with pytest.raises(TypeError):
                before[0x0881] = MidStatSnapshot(0, 0)  # type: ignore[index]
            with pytest.raises(dataclasses.FrozenInstanceError):
                before[0x0881].count = 0  # type: ignore[misc]
            found = ground.find("MOCK")
            assert isinstance(found, tuple) and found
            assert isinstance(ground.find(" "), tuple)

    asyncio.run(main())


def test_ledger_identity_is_read_only(tmp_path: Path) -> None:
    ledger = Ledger(tmp_path / "ledgers", session_id="abc")
    assert (ledger.directory, ledger.session_id) == (tmp_path / "ledgers", "abc")
    for name in ("directory", "session_id"):
        with pytest.raises(AttributeError):
            setattr(ledger, name, None)
