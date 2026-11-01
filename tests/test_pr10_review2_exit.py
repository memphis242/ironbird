"""Regression tests for the second round of PR #10 review findings on the exit: the budget that
keeps the ledger's last writes safe from retention and from rate limits, the lines that go with a
close that did not finish, one signal ending every wait, gated warnings ledgered when raised, a
sync that fails with a bug, the dropped-records marker in the exit account, and a usage error that
is reported before anything is opened."""

from __future__ import annotations

import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from conftest import REPO
from ironbird.cli import main as cli
from ironbird.errors import Code
from ironbird.ledger import ExitReport
from ironbird.ledgergate import WaitBudget
from ledgerutil import session_records
from support.faultfs import Rule
from test_pr10_ledger import Rig

TESTS = REPO / "tests"


# -- F1: one signal ends every wait of the exit ----


def test_a_signal_that_ended_one_wait_ends_the_ones_after_it() -> None:
    answers = iter([True])
    budget = WaitBudget(5.0, abort=lambda: next(answers, False))
    never = threading.Event()
    start = time.monotonic()
    assert not budget.wait(never)  # the signal arrives during this wait
    hold = threading.Event()
    other = threading.Thread(target=hold.wait, daemon=True)
    other.start()
    assert not budget.wait(never) and not budget.join(other)
    hold.set()
    assert time.monotonic() - start < 0.5 and budget.aborted
    done = threading.Event()
    done.set()
    assert budget.wait(done)  # a job that has finished is still seen


def test_the_signal_poll_of_the_exit_keeps_answering_after_it_saw_the_signal() -> None:
    state = cli._STATE
    budget = WaitBudget(5.0, abort=cli._signal_since(state.received))
    state.detach()  # the exit is under way: a signal is only recorded
    state._handle(int(signal.SIGTERM), None)
    never = threading.Event()
    assert not budget.wait(never)
    start = time.monotonic()
    assert not budget.wait(never)
    assert time.monotonic() - start < 0.2
    assert state.signum == signal.SIGTERM and state.unannounced() == signal.SIGTERM


# -- B2: retention may not spend the ledger's share; gates have their own cap ----


def test_a_waited_for_thread_leaves_the_named_share_of_the_budget() -> None:
    budget = WaitBudget(1.0)
    hold = threading.Event()
    thread = threading.Thread(target=hold.wait, daemon=True)
    thread.start()
    assert not budget.join(thread, keep=0.6)
    assert 0.55 <= budget.left <= 0.65
    hold.set()


def test_a_hung_rate_limit_does_not_starve_the_exit_into_a_false_stall_alarm(
    tmp_path: Path,
) -> None:
    release = threading.Event()
    rig = Rig(tmp_path, warn_gate=lambda _code: release.wait(10) or True)
    rig.ledger.begin_exit()
    rig.ledger.warn(Code.LED_RESERVE, "x", gated=True)
    rig.ledger.record("action", "last")
    start = time.monotonic()
    report = rig.ledger.finish()
    elapsed = time.monotonic() - start
    release.set()
    assert elapsed < 0.6, elapsed  # a tenth of 1.0 s for the gate, not all of it
    assert not report.stalled and not report.notices and report.unwritten_records == 0
    assert rig.ledger.close().notices == ()


# -- F2: a gated warning is in the ledger when it is raised ----


def test_a_gated_warning_is_ledgered_before_its_gate_answers(tmp_path: Path) -> None:
    release = threading.Event()
    rig = Rig(tmp_path, warn_gate=lambda _code: (release.wait(10), False)[1])
    rig.ledger.warn(Code.LED_RESERVE, "reserve trouble", gated=True, directory="d")
    rig.ledger.flush()
    rows = session_records(tmp_path / "state", bookkeeping=True)
    (row,) = [r for r in rows if r.get("code") == Code.LED_RESERVE.value]
    assert row["summary"] == "reserve trouble" and row["directory"] == "d"
    assert not rig.shown  # the display waits for the answer; the record did not
    release.set()
    rig.ledger.close()
    assert not rig.shown  # and the answer was no: ledgered, not displayed


# -- B2: the lines of a close that did not finish ----


def _ledger_with_unwritten_records(tmp_path: Path) -> tuple[Rig, ExitReport, Rule]:
    rig = Rig(tmp_path)
    rig.ledger.begin_exit()
    rig.ledger.record("action", "first")
    report = rig.ledger.finish()
    assert not report.stalled and not report.notices
    rig.ledger.record("state", "output.stderr_lost", lost=1)  # made after the last write attempt
    return rig, report, rig.fs.block("write")


def test_a_close_that_does_not_finish_prints_its_line_and_the_earlier_ones_are_not_repeated(
    tmp_path: Path,
) -> None:
    rig, report, gate = _ledger_with_unwritten_records(tmp_path)
    closing = rig.ledger.close()
    gate.gate.set()  # type: ignore[union-attr]
    (news,) = cli._closing_news(report, closing)
    assert news.code is Code.LED_WRITE and "stalled at exit" in news.summary
    assert "records not written" in news.summary
    assert cli._closing_news(closing, closing) == ()  # nothing printed twice


def test_the_close_folds_a_lost_transmission_into_the_exit_code(tmp_path: Path) -> None:
    rig = Rig(tmp_path)
    rig.ledger.begin_exit()
    rig.ledger.record_tx("tx.command", "NOOP", "fc", node="fc")
    gate = rig.fs.block("write")
    closing = rig.ledger.close()
    gate.gate.set()  # type: ignore[union-attr]
    assert cli._apply_ledger_report(0, closing) == cli.EXIT_UNLOGGED


# -- F3: a sync that fails with a bug is an unconfirmed write ----


def test_a_bug_in_the_exit_sync_leaves_the_sent_command_unlogged_and_names_the_remedy(
    tmp_path: Path,
) -> None:
    rig = Rig(tmp_path)
    rig.ledger.record_tx("tx.command", "NOOP", "fc", node="fc")
    rig.fs.rules.append(Rule("fdatasync", exc=RuntimeError("bug"), times=1))
    report = rig.ledger.finish()
    assert report.unlogged == 1 and report.exit_code == 6
    (line,) = report.notices
    assert "internal error RuntimeError" in line.summary
    assert "free space" not in line.summary and "bug in ironbird" in line.summary


def test_a_bug_in_the_sync_at_close_is_also_exit_6(tmp_path: Path) -> None:
    rig = Rig(tmp_path)
    rig.ledger.record_tx("tx.command", "NOOP", "fc", node="fc")
    rig.fs.rules.append(Rule("fdatasync", exc=ValueError("bug")))
    closing = rig.ledger.close()
    assert closing.unlogged == 1 and closing.exit_code == 6


# -- F6: the dropped-records marker handed over by flush is in the exit account ----


def test_the_marker_for_refused_records_that_flush_hands_over_is_counted_when_unwritten(
    tmp_path: Path,
) -> None:
    rig = Rig(
        tmp_path,
        max_queue_bytes=200_000,
        max_evidence_queue_bytes=40_000,
        max_event_queue_bytes=40_000,
    )
    rig.ledger.begin_exit(abort=lambda: True)  # no waiting: the writer is stuck on purpose
    gate = rig.fs.block("write")
    rig.ledger.record("action", "small")
    rig.ledger.flush()  # the first batch: the writer takes it and blocks
    assert rig.ledger.record("action", "huge", text="x" * 300_000) is None  # refused: over the cap
    rig.ledger.flush()  # the queue is empty, so the marker for the refused one goes alone
    report = rig.ledger.finish()
    gate.gate.set()  # type: ignore[union-attr]
    assert report.unwritten_records == 2, report  # the record and the marker


# -- a usage error is reported alone ----


def _ib(config: Path, state: Path, *args: str) -> subprocess.CompletedProcess[str]:
    env = {
        "PYTHONPATH": str(REPO / "src"),
        "PATH": "/usr/bin:/bin",
        "IRONBIRD_STATE_DIR": str(state),
    }
    return subprocess.run(  # noqa: PLW1510 - the tests read the return code
        [sys.executable, "-m", "ironbird.cli.main", "-c", str(config), *args],
        capture_output=True,
        text=True,
        env=env,
        timeout=60,
    )


@pytest.mark.parametrize(
    "args",
    [("listen", "fc", "--mid", "NO_SUCH_MESSAGE"), ("events", "--where", "this is not ((")],
    ids=["listen-mid", "events-where"],
)
def test_a_usage_error_of_listen_or_events_is_reported_before_the_ledger_is_opened(
    mock_config_path: Path, tmp_path: Path, args: tuple[str, ...]
) -> None:
    state = tmp_path / "state"
    done = _ib(mock_config_path, state, *args)
    assert done.returncode == 2, done.stderr
    assert not state.exists() or not list(state.iterdir())  # no ledger, reserve or scan
    lines = [x for x in done.stderr.splitlines() if x.strip()]
    assert len(lines) == 1 and lines[0].startswith("error IB-"), done.stderr
    assert "IB-LED" not in done.stderr


def test_a_listen_that_is_fine_still_opens_its_ledger(
    mock_config_path: Path, tmp_path: Path
) -> None:
    state = tmp_path / "state"
    done = _ib(mock_config_path, state, "listen", "fc", "--timeout", "0.3")
    assert done.returncode == 0, done.stderr
    assert list(state.glob("*.jsonl"))


# -- the early path is for usage errors only ----


@pytest.mark.parametrize(
    "args",
    [("events",), ("listen", "fc", "--mid", "MOCK_HK")],
    ids=["events", "listen-mid"],
)
def test_a_broken_ccdd_found_by_the_precheck_is_ledgered_like_any_other_error(
    mock_config_path: Path, tmp_path: Path, args: tuple[str, ...]
) -> None:
    ccdd = tmp_path / "mock.json"
    ccdd.write_bytes((REPO / "examples" / "ccdd" / "mock.json").read_bytes()[:200])  # truncated
    config = tmp_path / "broken.toml"
    config.write_text(
        mock_config_path.read_text().replace(f"{REPO / 'examples'}/ccdd/mock.json", str(ccdd))
    )
    assert str(ccdd) in config.read_text()
    state = tmp_path / "state"
    done = _ib(config, state, *args)
    assert done.returncode == 2, done.stderr
    errors = [r for r in session_records(state) if r["kind"] == "error"]
    assert errors and errors[0]["event"].startswith("IB-CCD-"), done.stderr
    assert any(r["event"] == "cli.invoke" for r in session_records(state))
    lines = [x for x in done.stderr.splitlines() if x.startswith("error IB-CCD-")]
    assert len(lines) == 1, done.stderr  # reported once, by the command itself
