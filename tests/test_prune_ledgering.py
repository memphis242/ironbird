"""``ib ledger prune`` leaves a trace of what it says about the directory, and recovers nothing
without a ledger to leave one in."""

from __future__ import annotations

import io
import json
import time
from pathlib import Path

import pytest

from ironbird.cli import prunecmd
from ironbird.cli.output import Console
from ironbird.ledger import Ledger
from ledgerutil import session_records
from retutil import make_reserve, make_session, reserve_header, sid
from test_retention_cli import run

NOW = time.time()
FLAGS = prunecmd.PruneFlags(None, None, 0, True, False, False)


@pytest.fixture
def state(_isolated_state: Path) -> Path:
    _isolated_state.mkdir(mode=0o700, exist_ok=True)
    return _isolated_state


def _damaged_reserve(state: Path, number: int = 1) -> Path:
    record = json.dumps({"session": sid(number), "n": 10, "kind": "action", "event": "a"})
    body = (reserve_header(sid(number)) + record + "\n").encode() + b"\x00" * 16 + b"junk\n"
    return make_reserve(state, number, 1, body, now=NOW)


def test_a_dry_run_without_a_usable_ledger_recovers_nothing_and_says_so(
    tmp_path: Path, state: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    make_session(state, 1, age_days=3, now=NOW)
    reserve = _damaged_reserve(state)
    before = sorted(p.name for p in state.iterdir())
    monkeypatch.setenv("IRONBIRD_STATE_DIR", str(state))
    err = io.StringIO()
    console = Console(io.StringIO(), err)
    dead = Ledger(tmp_path / "no-ledger", session_id="a" * 12, enabled=False)
    code = prunecmd.work("ledger", FLAGS, None, None, dead, console, verbose=False)
    console.close()
    assert code == 0
    assert reserve.exists() and sorted(p.name for p in state.iterdir()) == before
    assert "IB-LED-002" in err.getvalue() and "not recovered" in err.getvalue()


def test_the_scan_warnings_of_a_prune_are_in_the_ledger_too(state: Path) -> None:
    make_reserve(
        state, 7, 1, (reserve_header(sid(7))).encode(), now=NOW
    )  # no part 1: unrecoverable
    done = run(state, "ledger", "prune", "--keep", "0", "--dry-run")
    assert "IB-RET-004" in done.stderr, done.stderr
    warnings = [
        r
        for r in session_records(state, bookkeeping=True)
        if r["event"] == "retention.warning" and r.get("code") == "IB-RET-004"
    ]
    assert warnings and sid(7) in json.dumps(warnings), warnings
