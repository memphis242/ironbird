"""Regression tests for the PR #10 review findings on the ledger's exit account and writer."""

from __future__ import annotations

import errno
import threading
import time
from pathlib import Path
from typing import Any

from ironbird.config import LedgerConfig
from ironbird.errors import Code
from ironbird.ledger import Ledger
from ironbird.ledgergate import GateWorker, WaitBudget
from ironbird.ledgertiers import Admit, EvidenceSampler
from ironbird.notice import Notice
from ironbird.units import MIB
from support.faultfs import FakeClock, FaultFs, Rule


class Rig:
    def __init__(self, tmp_path: Path, *, long: bool = False, **options: Any) -> None:
        self.fs = FaultFs()
        self.clock = FakeClock()
        self.shown: list[Notice] = []
        if long:
            self.fs.override("fstatfs_type", lambda fd: 0xEF53)
        config = LedgerConfig(max_file_bytes=MIB, essential_reserve_bytes=MIB)
        self.ledger = Ledger(
            tmp_path / "state",
            session_id="s",
            session_class="long" if long else "short",
            command="respond" if long else "send",
            config=config,
            fs=self.fs,
            clock=self.clock,
            join_timeout_s=1.0,
            open_timeout_s=1.0,
            sleep=lambda _s: None,
            writer_poll_s=3600.0,
            **options,
        )
        self.ledger.set_notifier(self.shown.append)
        self.ledger.open()

    def step(self, seconds: float) -> None:
        self.clock.advance(seconds)
        self.ledger.tick()
        self.ledger._writer.wait(5)
        self.ledger._writer.wait(5)
        self.ledger.tick()


# -- B4: the exit account counts each unwritten transmission once, and never a synced one ----


def test_a_transmission_that_was_written_and_synced_is_not_unlogged_when_a_later_write_hangs(
    tmp_path: Path,
) -> None:
    rig = Rig(tmp_path)
    rig.ledger.record_tx("tx.command", "NOOP", "fc", node="fc")
    rig.ledger.flush()
    rig.step(2)  # the periodic sync makes the record durable
    assert rig.fs.count("fdatasync") >= 1
    rig.ledger.record("action", "session.end")
    gate = rig.fs.block("write")
    report = rig.ledger.finish()
    gate.gate.set()  # type: ignore[union-attr]
    assert report.unlogged == 0 and report.exit_code == 0


def test_a_transmission_in_a_backlog_is_counted_once_when_the_exit_write_hangs(
    tmp_path: Path,
) -> None:
    rig = Rig(tmp_path)
    fail = rig.fs.fail("write", errno.ENOSPC)
    for i in range(50):
        rig.ledger.record("action", "filler", i=i)
    rig.ledger.record_tx("tx.command", "NOOP", "fc", node="fc")
    rig.ledger.flush()
    assert rig.ledger.stalled
    fail.off = True
    gate = rig.fs.block("write")
    report = rig.ledger.finish()
    gate.gate.set()  # type: ignore[union-attr]
    assert report.unlogged == 1 and report.exit_code == 6
    (line,) = report.notices
    assert line.summary.startswith("NOOP sent to fc but not logged")


def test_a_written_transmission_is_not_blamed_for_a_later_unannounced_one(tmp_path: Path) -> None:
    rig = Rig(tmp_path, long=True)
    errors = [rig.fs.fail("write", errno.EIO), rig.fs.fail("pwrite", errno.EIO)]
    rig.ledger.record("action", "x")
    rig.ledger.flush()
    rig.step(0.6)
    assert rig.ledger.stalled
    rig.ledger.record_tx("tx.command", "SET_MODE", "fc", node="fc")  # announced, written later
    for rule in errors:
        rule.off = True
    rig.step(6)
    rig.ledger.flush()
    rig.step(1)
    assert not rig.ledger.stalled
    rig.ledger.record("action", "pad1")
    rig.ledger.flush()
    rig.ledger.record_tx("tx.command", "NOOP", "fc", node="fc")  # healthy when sent
    gate = rig.fs.block("write")
    report = rig.ledger.finish()
    gate.gate.set()  # type: ignore[union-attr]
    (line,) = report.notices  # exit 6 always comes with its line
    assert report.unlogged == 1 and report.exit_code == 6
    assert line.summary.startswith("NOOP sent to fc but not logged")


def test_a_hung_disk_is_named_as_hung_with_the_right_remedy(tmp_path: Path) -> None:
    rig = Rig(tmp_path)
    rig.ledger.record_tx("tx.command", "NOOP", "fc", node="fc")
    gate = rig.fs.block("write")
    report = rig.ledger.finish()
    gate.gate.set()  # type: ignore[union-attr]
    (line,) = report.notices
    assert "not responding" in line.summary and "check the disk or file server" in line.summary
    assert "free space" not in line.summary


# -- B2: a bug in the writer is an unlogged batch and a report, never just a note ----


def test_an_unexpected_exception_in_a_write_keeps_the_batch_and_is_reported(
    tmp_path: Path,
) -> None:
    rig = Rig(tmp_path)
    boom = Rule("write", exc=RuntimeError("boom"), times=1)
    rig.fs.rules.append(boom)
    rig.ledger.record_tx("tx.command", "NOOP", "fc", node="fc")
    rig.ledger.flush()
    rig.ledger.tick()
    assert any(n.code is Code.INTERNAL for n in rig.shown), [n.line() for n in rig.shown]
    boom.off = True
    rig.fs.fail("write", errno.EIO)  # and the retry at exit cannot write either
    report = rig.ledger.finish()
    assert report.unlogged == 1 and report.exit_code == 6
    assert any(n.code is Code.LED_UNLOGGED for n in report.notices)


def test_an_exception_after_a_job_does_not_end_the_writer_thread(tmp_path: Path) -> None:
    rig = Rig(tmp_path)
    rig.fs.rules.append(Rule("fdatasync", exc=ValueError("bug"), times=1))
    rig.ledger.record("action", "a")
    rig.ledger.flush()
    rig.step(2)  # the periodic sync raises a non-OSError inside the writer
    rig.ledger.record("action", "b")
    rig.ledger.flush()
    assert rig.ledger._writer._thread.is_alive()
    rig.ledger.close()
    text = next((tmp_path / "state").glob("*.jsonl")).read_text()
    assert '"event":"a"' in text and '"event":"b"' in text


# -- the exit budget: one bound for the whole exit, honouring a signal ----


def test_the_exit_waits_share_one_budget_instead_of_each_taking_the_full_time(
    tmp_path: Path,
) -> None:
    rig = Rig(tmp_path)
    rig.ledger.begin_exit()
    rig.fs.block("write")
    rig.ledger.record_tx("tx.command", "NOOP", "fc", node="fc")
    start = time.monotonic()
    first = rig.ledger.finish()
    second = rig.ledger.close()
    elapsed = time.monotonic() - start
    assert elapsed < 1.6, elapsed  # join_timeout_s is 1.0: not 2.0
    assert first.unlogged == 1 and second.unlogged == 1


def test_a_signal_during_the_exit_wait_ends_it_early(tmp_path: Path) -> None:
    rig = Rig(tmp_path)
    aborted = threading.Event()
    rig.ledger.begin_exit(abort=aborted.is_set)
    rig.fs.block("write")
    rig.ledger.record_tx("tx.command", "NOOP", "fc", node="fc")
    threading.Timer(0.15, aborted.set).start()
    start = time.monotonic()
    report = rig.ledger.finish()
    assert time.monotonic() - start < 0.8
    assert report.unlogged == 1


def test_wait_budget_spends_time_once_and_reports_timeouts() -> None:
    budget = WaitBudget(0.2)
    never = threading.Event()
    start = time.monotonic()
    assert not budget.wait(never) and not budget.wait(never)
    assert time.monotonic() - start < 0.5 and budget.left == 0.0
    done = threading.Event()
    done.set()
    assert budget.wait(done)  # an already finished job is seen even with no time left


# -- B1: the cross-process rate limit is asked off the calling thread ----


def test_a_gated_warning_never_runs_its_gate_on_the_calling_thread(tmp_path: Path) -> None:
    release = threading.Event()
    seen: list[str] = []

    def slow_gate(code: Code) -> bool:
        seen.append(threading.current_thread().name)
        release.wait(5)
        return True

    rig = Rig(tmp_path, warn_gate=slow_gate)
    start = time.monotonic()
    rig.ledger.warn(Code.LED_RESERVE, "x", gated=True)
    rig.ledger.tick()
    assert time.monotonic() - start < 0.5
    assert not rig.shown  # not decided yet, nothing blocks
    release.set()
    rig.ledger.close()
    assert [n.code for n in rig.shown] == [Code.LED_RESERVE]
    assert seen and set(seen) == {"gate-worker"}


def test_a_gate_that_never_answers_does_not_hold_the_exit_past_the_budget(tmp_path: Path) -> None:
    release = threading.Event()
    rig = Rig(tmp_path, warn_gate=lambda _code: release.wait(10))
    rig.ledger.begin_exit()
    rig.ledger.warn(Code.LED_RESERVE, "x", gated=True)
    start = time.monotonic()
    rig.ledger.finish()
    assert time.monotonic() - start < 1.6
    assert [n.code for n in rig.shown] == [Code.LED_RESERVE]  # a rate limit never causes silence
    release.set()
    rig.ledger.close()


def test_a_failing_gate_displays(tmp_path: Path) -> None:
    def broken(_code: Code) -> bool:
        raise RuntimeError("bug")

    rig = Rig(tmp_path, warn_gate=broken)
    rig.ledger.warn(Code.LED_RESERVE, "x", gated=True)
    rig.ledger.close()
    assert [n.code for n in rig.shown] == [Code.LED_RESERVE]


def test_gate_worker_answers_in_order() -> None:
    worker = GateWorker()
    for token in range(3):
        worker.submit(token, lambda token=token: token != 1)
    results: list[tuple[int, bool]] = []
    deadline = time.monotonic() + 5
    while len(results) < 3 and time.monotonic() < deadline:
        worker.progress.wait(0.05)
        worker.progress.clear()
        results += worker.take()
    assert results == [(0, True), (1, False), (2, True)]


# -- the evidence key table counts a packet key and its error key as one ----


def test_a_key_and_its_error_companion_share_one_slot_of_the_key_table() -> None:
    now = [0.0]
    sampler = EvidenceSampler(lambda: now[0], 1 << 30)
    for mid in range(1024):
        key = f"IB-DEC-006:{mid}:fc"
        assert sampler.admit("rx.malformed", key, time_ns=0, size=16, raw_len=16) is Admit.FULL
        assert sampler.admit("IB-DEC-006", key + "|error", time_ns=0) is Admit.FULL
    assert sampler.keys == 2048
    overflow = sampler.admit("rx.malformed", "IB-DEC-006:5000:fc", time_ns=0)
    assert overflow is Admit.FULL  # first of the folded "other" entry, counted there
    assert "other|rx.malformed" in sampler._keys
    assert not any(k.startswith("IB-DEC-006:5000") for k in sampler._keys)


# -- a long session says the ledger stalled before it says a transmission went unlogged ----


def test_the_stall_line_comes_before_the_lines_about_transmissions_sent_into_it(
    tmp_path: Path,
) -> None:
    rig = Rig(tmp_path, long=True)
    rig.fs.fail("write", errno.EIO)
    rig.fs.fail("pwrite", errno.EIO)
    rig.ledger.record("action", "x")
    rig.ledger._hand_over(10, force=True)  # as the drain task does: no event is looked at
    rig.ledger._writer.wait(5)  # the writer thread has stalled; nobody has read its events
    assert rig.ledger._store.events and not rig.shown
    rig.ledger.record_tx("tx.command", "SET_MODE", "fc", node="fc")
    codes = [n.code for n in rig.shown]
    assert codes[:2] == [Code.LED_WRITE, Code.LED_UNLOGGED], codes
