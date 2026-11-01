"""Regression tests for the fourth round of PR #10 review findings on retention and prune: a dry
run without a usable ledger protects the sessions a real prune protects, the ``IB-LED-002``
warning counts the reserves it is about, a warning prune displayed itself is recorded as shown,
and a recovery's rename stops being ``stepping`` before its record is handed out."""

from __future__ import annotations

import io
import json
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from ironbird.cli import prunecmd
from ironbird.cli.output import Console
from ironbird.ledger import Ledger
from ironbird.retention import DAMAGED_KEPT, UNRECOVERABLE, full_scan
from ledgerutil import session_records
from retutil import DAY, make_reserve, make_session, reserve_header, sid
from test_pr10_review2_retention import _clean_reserve, _final_records, _long_rig, rec
from test_retention_cli import run

NOW = time.time()
DRY_RUN = prunecmd.PruneFlags(None, None, 0, True, False, False)
DRY_RUN_JSON = prunecmd.PruneFlags(86400, None, 0, True, False, True)


@pytest.fixture
def state(_isolated_state: Path) -> Path:
    _isolated_state.mkdir(mode=0o700, exist_ok=True)
    return _isolated_state


def _foreign_reserve(state: Path) -> Path:
    """A reserve of session 1 whose header names session 2."""
    body = (reserve_header(sid(2)) + json.dumps({"session": sid(2), "n": 1}) + "\n").encode()
    return make_reserve(state, 1, 2, body, now=NOW - 3 * DAY)


def _dry_run(
    state: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, enabled: bool
) -> tuple[list[str], int, str]:
    """The sessions a JSON dry run lists, its protected bytes and its stderr."""
    monkeypatch.setenv("IRONBIRD_STATE_DIR", str(state))
    out, err = io.StringIO(), io.StringIO()
    console = Console(out, err)
    ledger = Ledger(tmp_path / ("on" if enabled else "off"), session_id="a" * 12, enabled=enabled)
    if enabled:
        ledger.open()
    code = prunecmd.work("ledger", DRY_RUN_JSON, None, ledger, console, verbose=False)
    console.close()
    if enabled:
        ledger.finish()
        ledger.close()
    assert code == 0
    plan = json.loads(out.getvalue().strip().splitlines()[-1])
    return [s["session"] for s in plan["sessions"]], plan["protected_bytes"], err.getvalue()


def test_dry_run_without_ledger_lists_a_session_a_real_prune_protects(
    tmp_path: Path, state: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    make_session(state, 1, age_days=3, now=NOW)
    _foreign_reserve(state)
    with_ledger = _dry_run(state, tmp_path, monkeypatch, enabled=True)
    without = _dry_run(state, tmp_path, monkeypatch, enabled=False)
    assert with_ledger[0] == []  # the real prune protects the session
    assert without[0] == with_ledger[0]
    assert without[1] == with_ledger[1]


def test_check_only_marks_the_sessions_a_recovery_would_protect_and_changes_nothing(
    state: Path,
) -> None:
    make_session(state, 1, age_days=3, now=NOW)
    foreign = _foreign_reserve(state)
    clean_body = (reserve_header(sid(3)) + rec(1).replace(sid(1), sid(3))).encode()
    make_session(state, 3, age_days=3, now=NOW)
    clean = make_reserve(state, 3, 1, clean_body, now=NOW - 3 * DAY)
    damaged_body = (reserve_header(sid(4)) + rec(1).replace(sid(1), sid(4))).encode() + b"\0" * 8
    damaged_body += b"junk"
    make_session(state, 4, age_days=3, now=NOW)
    damaged = make_reserve(state, 4, 1, damaged_body, now=NOW - 3 * DAY)
    before = {p.name: p.read_bytes() for p in state.iterdir()}
    result = full_scan(
        prunecmd.REAL_FS,
        state,
        "ledger",
        now=time.time(),
        own_session=None,
        keep_recent=0,
        recover=False,
        check_only=True,
    )
    assert {p.name: p.read_bytes() for p in state.iterdir()} == before
    assert foreign.exists() and clean.exists() and damaged.exists()
    assert UNRECOVERABLE in result.sessions[sid(1)].protected
    assert result.sessions[sid(3)].protected == []  # a recovery would make it prunable
    assert DAMAGED_KEPT in result.sessions[sid(4)].protected
    assert result.recovered == 0 and result.recoveries == []


def test_led002_warning_with_nothing_to_recover(
    tmp_path: Path, state: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    make_session(state, 1, age_days=3, now=NOW)  # no reserve at all
    assert "IB-LED-002" not in _dry_run(state, tmp_path, monkeypatch, enabled=False)[2]


def test_led002_warning_counts_the_reserves_it_is_about(
    tmp_path: Path, state: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    make_session(state, 1, age_days=3, now=NOW)
    _foreign_reserve(state)
    make_session(state, 3, age_days=3, now=NOW)
    make_reserve(state, 3, 1, (reserve_header(sid(3)) + rec(1)).encode(), now=NOW - 3 * DAY)
    err = _dry_run(state, tmp_path, monkeypatch, enabled=False)[2]
    assert "IB-LED-002" in err
    assert "2 leftover reserve(s)" in err


def test_shown_field_of_displayed_scan_warning(state: Path) -> None:
    make_reserve(state, 7, 1, reserve_header(sid(7)).encode(), now=NOW)
    done = run(state, "ledger", "prune", "--keep", "0", "--dry-run")
    assert "IB-RET-004" in done.stderr
    warnings = [
        r for r in session_records(state, bookkeeping=True) if r["event"] == "retention.warning"
    ]
    assert warnings
    assert all(w.get("shown") is True for w in warnings), warnings


def test_ledger_warn_shown_overrides_display(tmp_path: Path) -> None:
    from ironbird.errors import Code

    ledger = Ledger(tmp_path / "led", session_id="b" * 12)
    ledger.open()
    ledger.warn(Code.RET_SCAN, "one", display=False, shown=True)
    ledger.warn(Code.RET_SCAN, "two", display=False)
    ledger.finish()
    ledger.close()
    rows = [
        r
        for r in session_records(tmp_path / "led", bookkeeping=True)
        if r["event"] == "ledger.warning"
    ]
    assert [(r["summary"], r["shown"]) for r in rows] == [("one", True), ("two", False)]


def test_commit_reported_then_abandoned(tmp_path: Path) -> None:
    """The rename is done and its record posted while the scan thread is held right after the
    post: the exit's wait must not see a step under way and write ``recovery_incomplete``."""
    ledger, retention, _fs, state = _long_rig(tmp_path)
    make_session(state, 1, age_days=3, now=NOW)
    _clean_reserve(state)
    posted = threading.Event()
    gate = threading.Event()
    real: Any = retention._post_record

    def post(event: str, fields: dict[str, Any]) -> None:
        real(event, fields)
        if event == "ledger.recovered":
            posted.set()
            gate.wait(10)

    retention._post_record = post
    ledger.begin_exit()
    ledger.record("action", "x")
    retention.start()
    assert posted.wait(10)
    retention.finish()
    ledger.finish()
    ledger.close()
    gate.set()
    thread = retention._thread
    assert thread is not None
    thread.join(10)
    events = [r["event"] for r in _final_records(state) if r["event"].startswith("ledger.recover")]
    assert "ledger.recovered" in events
    assert "ledger.recovery_incomplete" not in events
