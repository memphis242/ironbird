"""Regression tests for the third round of PR #10 review findings on the exit: the final ledger
write after a signal, the append to the file the session ended in, the one stall line, and the
fields of an abandoned salvage record."""

from __future__ import annotations

import os
import threading
import time
from pathlib import Path

import pytest

from ironbird.cli import main as cli
from ironbird.errors import Code
from ironbird.ledger import ExitReport
from ironbird.ledgergate import WaitBudget
from ironbird.notice import Level, Notice
from test_pr10_ledger import Rig

# -- F1: a signal leaves the final ledger write a short floor ----


def test_a_wait_after_the_signal_gets_only_its_floor() -> None:
    budget = WaitBudget(5.0, abort=lambda: True)
    never = threading.Event()
    assert not budget.wait(never)  # the signal ends this wait; the budget is aborted
    started = time.monotonic()
    assert not budget.wait(never)
    assert time.monotonic() - started < 0.1  # no floor asked: no wait
    started = time.monotonic()
    assert not budget.wait(never, floor=0.2)
    assert 0.15 < time.monotonic() - started < 0.6  # the floor, once


def test_a_floor_lets_a_job_that_ends_within_it_count_as_finished() -> None:
    budget = WaitBudget(5.0, abort=lambda: True)
    done = threading.Event()
    threading.Timer(0.1, done.set).start()
    assert budget.wait(done, floor=0.25)
    assert budget.aborted


def test_a_signal_during_the_final_write_still_lets_a_quick_disk_finish_it(tmp_path: Path) -> None:
    rig = Rig(tmp_path)
    rig.ledger.begin_exit(abort=lambda: True)
    rig.ledger.record("action", "last")
    gate = rig.fs.block("write")
    threading.Timer(0.12, gate.gate.set).start()  # type: ignore[union-attr]
    report = rig.ledger.finish()
    assert report.unwritten_records == 0 and not report.stalled, report


def test_the_line_for_a_write_cut_short_by_a_signal_does_not_blame_the_disk(
    tmp_path: Path,
) -> None:
    rig = Rig(tmp_path)
    rig.ledger.begin_exit(abort=lambda: True)
    rig.ledger.record_tx("tx.command", "NOOP", "fc", node="fc")
    gate = rig.fs.block("write")
    report = rig.ledger.finish()
    gate.gate.set()  # type: ignore[union-attr]
    assert report.unlogged == 1
    (line,) = report.notices
    assert "signal" in line.summary and "check the disk" not in line.summary, line.summary


def test_a_stall_cut_short_by_a_signal_is_worded_for_the_signal(tmp_path: Path) -> None:
    rig = Rig(tmp_path)
    rig.ledger.begin_exit(abort=lambda: True)
    rig.ledger.record("action", "x")
    gate = rig.fs.block("write")
    report = rig.ledger.finish()
    gate.gate.set()  # type: ignore[union-attr]
    (line,) = report.notices
    assert "signal" in line.summary and "check the disk" not in line.summary, line.summary


# -- F2: the append to the final file checks what it opens ----


def _replace(final: Path, tmp_path: Path) -> None:
    """Remove ``final`` while keeping its inode alive, so the file written in its place cannot
    be given the same inode number."""
    os.link(final, tmp_path / "kept")
    final.unlink()


def _closed(tmp_path: Path) -> tuple[Rig, Path]:
    rig = Rig(tmp_path)
    rig.ledger.record("action", "first")
    rig.ledger.close()
    name = rig.ledger._store.final_name
    assert name is not None
    return rig, tmp_path / "state" / name


def test_a_late_record_is_appended_to_the_file_the_session_ended_in(tmp_path: Path) -> None:
    rig, final = _closed(tmp_path)
    rig.ledger.post("state", "late.record")
    assert b"late.record" in final.read_bytes() and rig.ledger.late_skipped == 0


def test_a_late_record_is_not_written_to_a_file_that_replaced_the_final_one(
    tmp_path: Path,
) -> None:
    rig, final = _closed(tmp_path)
    _replace(final, tmp_path)
    final.write_bytes(b"somebody else's file\n")
    rig.ledger.post("state", "late.record")
    assert final.read_bytes() == b"somebody else's file\n"
    assert rig.ledger.late_skipped == 1


def test_a_late_transmission_that_cannot_be_appended_is_counted_as_lost(tmp_path: Path) -> None:
    rig, final = _closed(tmp_path)
    _replace(final, tmp_path)
    final.write_bytes(b"x\n")
    rig.ledger.record_tx("tx.command", "NOOP", "fc", node="fc")
    rig.ledger.close()  # appends what was made since
    assert final.read_bytes() == b"x\n" and rig.ledger._late_lost.count == 1


def test_a_late_record_is_not_written_to_a_file_of_another_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig, final = _closed(tmp_path)
    before = final.read_bytes()
    import ironbird.ledger as ledger_module

    monkeypatch.setattr(ledger_module.os, "getuid", lambda: 4_000_000)
    rig.ledger.post("state", "late.record")
    monkeypatch.undo()
    assert final.read_bytes() == before and rig.ledger.late_skipped == 1


# -- F5: one stall line ----


def _stall(records: int) -> ExitReport:
    note = Notice(
        Code.LED_WRITE,
        Level.WARNING,
        Code.LED_WRITE.value,
        f"ledger stalled at exit; {records} records not written",
    )
    return ExitReport(notices=(note,), unwritten_records=records, stalled=True)


def test_the_close_does_not_print_a_second_stall_line_with_another_count() -> None:
    assert cli._closing_news(_stall(2), _stall(3)) == ()


def test_the_close_prints_a_line_of_a_code_the_earlier_account_did_not_show() -> None:
    news = cli._closing_news(ExitReport(), _stall(3))
    assert len(news) == 1 and news[0].code is Code.LED_WRITE
