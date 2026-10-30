"""Stage 2d, layers 1 and 2: the ledger's writer under faults, rotation and the essential reserve.

Every file system call of the ledger goes through ``fs`` (:mod:`support.faultfs`), so disk-full,
I/O errors, missing locks and hung disks are scripted without privileges; the 5 s thresholds read
an injected clock, so a stall is reached by advancing it, not by sleeping.
"""

from __future__ import annotations

import errno
import json
import os
import stat
from pathlib import Path
from typing import Any

import pytest

from ironbird.config import LedgerConfig
from ironbird.errors import Code, IronbirdError
from ironbird.fsops import FileSystem
from ironbird.ledger import ExitReport, Ledger
from ironbird.ledgerstore import part_name, reserve_name, stamp_of
from ironbird.notice import Level, Notice
from ironbird.units import MIB
from ledgerutil import session_records
from support.faultfs import FakeClock, FaultFs

EXT4 = 0xEF53
DELETING_CALLS = ("unlink_at", "unlink_path", "rmdir_at")


class Rig:
    """A ledger over a scripted file system and a clock the test moves."""

    def __init__(
        self,
        tmp_path: Path,
        *,
        long: bool = False,
        config: LedgerConfig | None = None,
        fs: FaultFs | None = None,
        reserve_fs_type: int | None = EXT4,
    ) -> None:
        self.fs = fs or FaultFs()
        self.clock = FakeClock()
        self.notices: list[Notice] = []
        self.dir = tmp_path / "state"
        if reserve_fs_type is not None:
            self.fs.override("fstatfs_type", lambda _fd: reserve_fs_type)
        self.ledger = Ledger(
            self.dir,
            session_id="s",
            session_class="long" if long else "short",
            command="test",
            config=config or LedgerConfig(essential_reserve_bytes=MIB, max_file_bytes=MIB),
            fs=self.fs,
            clock=self.clock,
            join_timeout_s=3.0,
            open_timeout_s=3.0,
            sleep=lambda _s: None,
            writer_poll_s=3600.0,  # upkeep runs when the test asks: the fake clock is exact
        )
        self.ledger.set_notifier(self.notices.append)

    def step(self, seconds: float) -> None:
        """Move the clock, run the ledger's and the writer's periodic upkeep."""
        self.clock.advance(seconds)
        self.ledger.tick()
        self.ledger._writer.wait(5)
        self.ledger._writer.wait(5)
        self.ledger.tick()

    def rows(self, *, bookkeeping: bool = True) -> list[dict[str, Any]]:
        return session_records(self.dir, bookkeeping=bookkeeping)

    def events(self, name: str) -> list[dict[str, Any]]:
        return [r for r in self.rows() if r["event"] == name]

    def codes(self) -> list[Code]:
        return [n.code for n in self.notices]

    def files(self) -> list[str]:
        return sorted(p.name for p in self.dir.iterdir())

    def deleted_nothing(self) -> bool:
        return not any(self.fs.count(name) for name in DELETING_CALLS)


def stalled_by_write_errors(rig: Rig, err: int = errno.EIO) -> None:
    rig.fs.fail("write", err)
    rig.fs.fail("pwrite", err)


# -- the open boundary ----

CONFIG_ERRNOS = [
    errno.EACCES,
    errno.EPERM,
    errno.EROFS,
    errno.ENOTDIR,
    errno.ENOENT,
    errno.ELOOP,
    errno.ENAMETOOLONG,
    errno.EISDIR,
    errno.EXDEV,  # any errno not listed as transient
]
TRANSIENT_ERRNOS = [
    errno.ENOSPC,
    errno.EDQUOT,
    errno.EIO,
    errno.EMFILE,
    errno.ENFILE,
    errno.ENOMEM,
    errno.ESTALE,
    errno.ETIMEDOUT,
]
OPEN_STEPS = [
    ("mkdir_p", None),
    ("open_dir", None),
    ("open_at", lambda _dir_fd, name, *_: name.endswith(".jsonl")),
    ("write", None),
]


@pytest.mark.parametrize("step", OPEN_STEPS, ids=[s[0] for s in OPEN_STEPS])
@pytest.mark.parametrize("err", CONFIG_ERRNOS, ids=lambda e: errno.errorcode[e])
def test_a_configuration_errno_at_any_open_step_refuses(
    tmp_path: Path, step: tuple[str, Any], err: int
) -> None:
    rig = Rig(tmp_path)
    rig.fs.fail(step[0], err, when=step[1])
    with pytest.raises(IronbirdError) as info:
        rig.ledger.open()
    assert info.value.code is Code.LED_DIR and not rig.ledger.enabled
    assert rig.ledger.record("action", "a") is None  # nothing is queued for a dead ledger


@pytest.mark.parametrize("step", OPEN_STEPS, ids=[s[0] for s in OPEN_STEPS])
@pytest.mark.parametrize("err", TRANSIENT_ERRNOS, ids=lambda e: errno.errorcode[e])
def test_a_transient_errno_at_any_open_step_starts_the_ledger_stalled(
    tmp_path: Path, step: tuple[str, Any], err: int
) -> None:
    rig = Rig(tmp_path, long=True)
    rule = rig.fs.fail(step[0], err, when=step[1])
    rig.ledger.open()  # does not raise: the location is right, the disk is not
    assert rig.ledger.stalled and rig.ledger.enabled
    number = rig.ledger.record("action", "queued")
    assert number is not None  # record keeps returning numbers while stalled
    assert Code.LED_WRITE in rig.codes()
    rule.off = True
    rig.step(5.0)  # the retry repeats every open step from mkdir
    assert not rig.ledger.stalled
    rig.ledger.close()
    events = [r["event"] for r in rig.rows()]
    assert "queued" in events and "ledger.resumed" in events and events[0] == "ledger.open"
    assert rig.deleted_nothing()


def test_an_open_step_blocked_past_5_seconds_is_a_stall_and_the_loop_keeps_running(
    tmp_path: Path,
) -> None:
    rig = Rig(tmp_path, long=True)
    rig.ledger._open_timeout_s = 0.2
    rule = rig.fs.block("mkdir_p")
    rig.ledger.open()  # the command waits for the open at most its bound
    assert rule.entered.wait(5)
    rig.clock.advance(6.0)
    assert rig.ledger.stalled  # the watchdog reads the injected clock
    rig.ledger.tick()
    assert Code.LED_WRITE in rig.codes()
    assert rig.ledger.record("action", "meanwhile") is not None
    rule.gate.set()  # type: ignore[union-attr]
    rig.ledger.flush()
    rig.step(5.0)
    assert not rig.ledger.stalled
    rig.ledger.close()
    assert "meanwhile" in [r["event"] for r in rig.rows()]


def test_a_directory_removed_during_an_open_stall_is_recreated_by_the_retry(
    tmp_path: Path,
) -> None:
    rig = Rig(tmp_path)
    first_write = rig.fs.fail("write", errno.ENOSPC)  # the open's first record cannot be written
    rig.ledger.open()
    assert rig.ledger.stalled
    rig.ledger.record("action", "queued")
    for path in rig.dir.iterdir():  # the test, not the product, removes the files
        path.unlink()
    rig.dir.rmdir()
    first_write.off = True
    rig.step(5.0)  # every open step repeats from mkdir
    assert not rig.ledger.stalled
    rig.ledger.close()
    assert stat.S_IMODE(os.stat(rig.dir).st_mode) == 0o700
    assert "queued" in [r["event"] for r in rig.rows()]
    assert [f for f in rig.files() if f.endswith(".jsonl")] == [
        part_name(stamp_of(rig.ledger.start_ns), "s", 1)
    ]


def test_files_are_owner_only_and_the_ledger_deletes_nothing(tmp_path: Path) -> None:
    rig = Rig(tmp_path, long=True)
    rig.ledger.open()
    for i in range(50):
        rig.ledger.record("action", "x", i=i)
    rig.ledger.close()
    for path in rig.dir.iterdir():
        assert stat.S_IMODE(path.stat().st_mode) == 0o600, path
    assert rig.deleted_nothing()


# -- locks ----


def test_a_lock_held_briefly_by_a_checker_is_retried_and_the_session_locks(tmp_path: Path) -> None:
    rig = Rig(tmp_path)
    rig.fs.fail("flock", errno.EWOULDBLOCK, times=25)  # 0.5 s of a probing checker
    rig.ledger.open()
    rig.ledger.close()
    assert rig.fs.count("flock") == 26 and Code.LED_NOLOCK not in rig.codes()


@pytest.mark.parametrize(
    "err",
    [errno.EWOULDBLOCK, errno.ENOLCK, errno.EINVAL, errno.EOPNOTSUPP],
    ids=lambda e: errno.errorcode[e],
)
def test_no_usable_lock_runs_unlocked_with_one_warning_and_still_writes(
    tmp_path: Path, err: int
) -> None:
    rig = Rig(tmp_path)
    rig.fs.fail("flock", err)
    rig.ledger.open()
    rig.ledger.record("action", "still-written")
    rig.ledger.close()
    assert rig.codes().count(Code.LED_NOLOCK) == 1
    assert "still-written" in [r["event"] for r in rig.rows()]
    warning = [r for r in rig.events("ledger.warning") if r["code"] == Code.LED_NOLOCK.value]
    assert len(warning) == 1


def test_the_start_of_session_warnings_go_through_the_cross_process_gate(tmp_path: Path) -> None:
    rig = Rig(tmp_path)
    shown: list[Code] = []
    rig.ledger.set_warn_gate(lambda code: shown.append(code) is None and False)
    rig.fs.fail("flock", errno.ENOLCK)
    rig.ledger.open()
    rig.ledger.close()
    assert shown == [Code.LED_NOLOCK] and Code.LED_NOLOCK not in rig.codes()  # not displayed
    assert rig.events("ledger.warning")  # but ledgered


# -- stall, resume, exit lines ----


def test_a_write_error_stalls_queues_and_a_retry_writes_late_with_original_timestamps(
    tmp_path: Path,
) -> None:
    rig = Rig(tmp_path, long=True)
    rig.ledger.open()
    rig.ledger.record("action", "before")
    rig.ledger.flush()
    rule = rig.fs.fail("write", errno.EIO)
    rig.fs.fail("pwrite", errno.EIO)
    n1 = rig.ledger.record("action", "during-1")
    rig.ledger.flush()
    assert rig.ledger.stalled
    n2 = rig.ledger.record("action", "during-2")
    assert n1 is not None and n2 is not None and n2 > n1  # numbers keep coming while stalled
    # essential records are not lost to the stall; event and evidence records are counted only
    assert rig.ledger.record("event", "rx.event", text="e") is None
    assert rig.ledger.record("packet", "rx.other", raw=b"\x01") is None
    rule.off = True
    rig.fs.clear()
    rig.step(5.0)
    assert not rig.ledger.stalled
    rig.ledger.close()
    rows = rig.rows()
    names = [r["event"] for r in rows]
    assert names.index("during-1") < names.index("ledger.resumed")
    resumed = rig.events("ledger.resumed")[0]
    assert resumed["delayed"] >= 2 and resumed["lost"] == 2
    during = next(r for r in rows if r["event"] == "during-1")
    assert during["ts"] < resumed["ts"]  # the delayed record keeps its own time
    assert rig.codes().count(Code.LED_WRITE) == 1
    resumed_notices = [n for n in rig.notices if n.code is Code.LED_RESUMED]
    assert len(resumed_notices) == 1 and resumed_notices[0].level is Level.INFO
    assert "delayed records written" in resumed_notices[0].summary


def test_stall_episodes_within_ten_minutes_are_ledgered_each_but_displayed_once(
    tmp_path: Path,
) -> None:
    rig = Rig(tmp_path, long=True)
    rig.ledger.open()
    for episode in range(3):
        rule = rig.fs.fail("write", errno.EIO)
        rig.ledger.record("action", f"e{episode}")
        rig.ledger.flush()
        assert rig.ledger.stalled
        rule.off = True
        rig.step(5.0)
        assert not rig.ledger.stalled
    rig.ledger.close()
    rig.ledger.flush_notices()
    assert len(rig.events("ledger.resumed")) == 3  # every episode ledgered
    assert len(rig.events("ledger.stalled")) == 3
    firsts = [n for n in rig.notices if n.code is Code.LED_WRITE and n.count == 1]
    folded = [n for n in rig.notices if n.code is Code.LED_WRITE and n.count > 1]
    assert len(firsts) == 1 and folded and folded[0].count == 3
    resumed = [n for n in rig.notices if n.code is Code.LED_RESUMED]
    assert [n.count for n in resumed] == [1, 3]


def test_a_short_command_displays_no_stall_or_resume_line_while_it_runs(tmp_path: Path) -> None:
    rig = Rig(tmp_path, long=False)
    rig.ledger.open()
    rule = rig.fs.fail("write", errno.ENOSPC)
    rig.ledger.record("action", "x")
    rig.ledger.flush()
    rule.off = True
    rig.step(5.0)
    report = rig.ledger.close()
    assert rig.notices == [] and report.notices == () and report.exit_code == 0
    assert rig.events("ledger.resumed")  # nothing to act on, but the trace says what happened


def test_a_long_session_stalled_at_exit_says_so_once(tmp_path: Path) -> None:
    rig = Rig(tmp_path, long=True)
    rig.ledger.open()
    stalled_by_write_errors(rig, errno.ENOSPC)
    for i in range(7):
        rig.ledger.record("action", "x", i=i)
    rig.ledger.flush()
    report = rig.ledger.close()
    (notice,) = report.notices
    assert notice.code is Code.LED_WRITE
    assert notice.summary.startswith("ledger stalled at exit; ")
    assert int(notice.summary.split("; ")[1].split()[0]) >= 7  # the ledger's own notes count too
    assert report.stalled and report.unlogged == 0 and report.exit_code == 0


def test_a_stalled_short_send_prints_one_line_naming_cause_and_remedy_and_exits_6(
    tmp_path: Path,
) -> None:
    rig = Rig(tmp_path, long=False)
    rig.ledger.open()
    stalled_by_write_errors(rig, errno.ENOSPC)
    rig.ledger.record_tx("tx.command", "NOOP", "fc", node="fc")
    report = rig.ledger.close()
    (notice,) = report.notices
    assert notice.code is Code.LED_UNLOGGED and notice.level is Level.WARNING
    assert notice.summary == (
        "NOOP sent to fc but not logged (ledger not writable: No space left on device); "
        f"free space in {rig.dir} or set IRONBIRD_STATE_DIR"
    )
    assert (report.unlogged, report.exit_code) == (1, 6)
    assert rig.notices == []  # no separate stall-entry line from a short command


def test_a_short_send_whose_record_is_written_late_is_not_unlogged(tmp_path: Path) -> None:
    rig = Rig(tmp_path, long=False)
    rig.ledger.open()
    rule = rig.fs.fail("write", errno.EIO)
    rig.ledger.record_tx("tx.command", "NOOP", "fc", node="fc")
    rig.ledger.flush()
    rule.off = True
    rig.step(5.0)
    report = rig.ledger.close()
    assert report == ExitReport() and report.exit_code == 0
    assert [r["event"] for r in rig.rows() if r["event"] == "tx.command"] == ["tx.command"]
    assert rig.events("ledger.resumed")[0]["delayed"] >= 1


def test_fifty_transmissions_while_stalled_give_bounded_lines_then_one_summary(
    tmp_path: Path,
) -> None:
    rig = Rig(tmp_path, long=True)
    rig.ledger.open()
    stalled_by_write_errors(rig)
    rig.ledger.record("action", "trigger")
    rig.ledger.flush()
    assert rig.ledger.stalled
    commands = [f"CMD{i:02d}" for i in range(50)]
    for command in commands:
        rig.ledger.record_tx("tx.command", command, "fc", node="fc")
    shown = [n for n in rig.notices if n.code is Code.LED_UNLOGGED]
    assert len(shown) == 1 and shown[0].summary.startswith("CMD00 sent to fc but not logged")
    rig.step(10.0)  # the window closes; the later 49 fold into one line naming the latest
    rig.ledger.flush_notices()
    folded = [n for n in rig.notices if n.code is Code.LED_UNLOGGED and n.count > 1]
    assert len(folded) == 1 and folded[0].count == 50
    assert folded[0].summary.startswith("CMD49 sent to fc but not logged")
    report = rig.ledger.close()
    (summary,) = report.notices
    assert summary.summary.startswith("50 commands sent but not logged (first CMD00 to fc at ")
    assert ", last CMD49 to fc at " in summary.summary
    assert (report.unlogged, report.exit_code) == (50, 6)


def test_exit_6_is_never_silent_even_for_a_transmission_announced_when_it_was_sent(
    tmp_path: Path,
) -> None:
    rig = Rig(tmp_path, long=True)
    rig.ledger.open()
    stalled_by_write_errors(rig)
    rig.ledger.record("action", "trigger")
    rig.ledger.flush()
    rig.ledger.record_tx("tx.command", "NOOP", "fc", node="fc")
    report = rig.ledger.close()
    (line,) = report.notices  # one line with the exit code, announced earlier or not
    assert line.code is Code.LED_UNLOGGED and line.summary.startswith("NOOP sent to fc but not")
    assert report.unlogged == 1 and report.exit_code == 6


def test_refuse_commands_refuses_before_anything_is_sent(tmp_path: Path) -> None:
    config = LedgerConfig(on_full="refuse_commands")
    rig = Rig(tmp_path, config=config)
    rig.ledger.open()
    rig.ledger.check_transmit("NOOP", "fc")  # a healthy ledger lets it through
    stalled_by_write_errors(rig, errno.ENOSPC)
    rig.ledger.record("action", "trigger")
    rig.ledger.flush()
    with pytest.raises(IronbirdError) as info:
        rig.ledger.check_transmit("NOOP", "fc")
    assert info.value.code is Code.LED_UNLOGGED and info.value.exit_code == 1
    assert info.value.summary == (
        "NOOP to fc refused: ledger not writable (No space left on device); "
        f"free space in {rig.dir} or set IRONBIRD_STATE_DIR"
    )
    rig.fs.clear()
    rig.step(5.0)
    rig.ledger.check_transmit("NOOP", "fc")  # the seam recovers: accepted
    assert rig.ledger.close().unlogged == 0


def test_refuse_commands_withholds_replies_counts_them_and_exits_1(tmp_path: Path) -> None:
    rig = Rig(tmp_path, long=True, config=LedgerConfig(on_full="refuse_commands"))
    rig.ledger.open()
    stalled_by_write_errors(rig)
    rig.ledger.record("action", "trigger")
    rig.ledger.flush()
    assert all(rig.ledger.withhold_reply() for _ in range(12))
    report = rig.ledger.close()
    (notice,) = [n for n in report.notices if n.level is Level.ERROR]
    assert notice.summary == "12 replies withheld: ledger not writable"
    assert (report.withheld, report.exit_code) == (12, 1)


def test_continue_sends_replies_and_never_withholds(tmp_path: Path) -> None:
    rig = Rig(tmp_path, long=True)
    rig.ledger.open()
    stalled_by_write_errors(rig)
    rig.ledger.record("action", "trigger")
    rig.ledger.flush()
    assert not rig.ledger.withhold_reply()
    rig.ledger.check_transmit("NOOP", "fc")  # no raise under the default policy


# -- deferred errors: sync and close ----


@pytest.mark.parametrize("err", [errno.ENOSPC, errno.EIO], ids=lambda e: errno.errorcode[e])
def test_a_deferred_sync_error_at_close_makes_the_transmission_possibly_lost(
    tmp_path: Path, err: int
) -> None:
    rig = Rig(tmp_path)
    rig.ledger.open()
    rig.fs.fail("fdatasync", err)
    rig.ledger.record_tx("tx.command", "NOOP", "fc", node="fc")
    report = rig.ledger.close()  # ib send syncs at close: never a silent 0
    assert (report.unlogged, report.exit_code) == (1, 6)
    assert report.notices[0].code is Code.LED_UNLOGGED


def test_a_deferred_sync_error_at_a_periodic_sync_is_counted_and_stalls(tmp_path: Path) -> None:
    rig = Rig(tmp_path, long=True)
    rig.ledger.open()
    rig.fs.fail("fdatasync", errno.EIO, times=1)
    rig.ledger.record_tx("tx.command", "NOOP", "fc", node="fc")
    rig.ledger.flush()
    rig.step(1.5)  # the sync is due once a second
    store = rig.ledger._store
    assert store.possibly_lost_tx.count == 1
    rig.step(5.0)  # the retry resumes
    report = rig.ledger.close()
    assert report.unlogged == 1 and report.exit_code == 6
    resumed = rig.events("ledger.resumed")[0]
    assert resumed["possibly_lost"] >= 1 and resumed["unlogged_sent"] >= 1


def test_sync_runs_at_most_once_a_second(tmp_path: Path) -> None:
    rig = Rig(tmp_path)
    rig.ledger.open()
    for i in range(10):
        rig.ledger.record("action", "x", i=i)
        rig.ledger.flush()
    before = rig.fs.count("fdatasync")
    rig.step(0.4)
    assert rig.fs.count("fdatasync") == before
    rig.step(1.0)
    assert rig.fs.count("fdatasync") == before + 1


def test_an_idle_session_heartbeats_every_minute_and_stops_when_the_writer_hangs(
    tmp_path: Path,
) -> None:
    rig = Rig(tmp_path)
    rig.ledger.open()
    rig.ledger.record("action", "x")
    rig.ledger.flush()
    rig.step(1.5)  # synced
    utimes = rig.fs.count("utime")
    rig.step(30.0)
    assert rig.fs.count("utime") == utimes
    rig.step(31.0)
    assert rig.fs.count("utime") == utimes + 1
    # a writer blocked on the disk cannot heartbeat (it runs on that thread)
    rule = rig.fs.block("utime")
    rig.clock.advance(61.0)
    rig.ledger._writer.submit("tick")
    assert rule.entered.wait(5)
    rig.clock.advance(120.0)
    assert rig.fs.count("utime") == utimes + 2  # one call, blocked: no further heartbeats
    rule.gate.set()  # type: ignore[union-attr]
    rig.ledger.close()


# -- a hung disk ----


def test_a_write_blocked_past_five_seconds_is_a_stall_and_exit_is_bounded(tmp_path: Path) -> None:
    rig = Rig(tmp_path, long=True)
    rig.ledger._join_timeout_s = 0.3
    rig.ledger.open()
    rule = rig.fs.block("write")
    rig.ledger.record_tx("tx.command", "NOOP", "fc", node="fc")
    rig.ledger.flush()  # bounded wait: the write is stuck
    assert rule.entered.wait(5)
    rig.clock.advance(6.0)
    rig.ledger.tick()
    assert rig.ledger.stalled and rig.ledger.degraded
    assert rig.ledger.record("event", "rx.event", text="e") is None  # counted only
    assert Code.LED_WRITE in rig.codes()
    rig.ledger.record_tx("tx.command", "SECOND", "fc", node="fc")
    report = rig.ledger.close()  # the writer is still blocked: joined for the bound, then left
    assert report.unlogged == 2 and report.exit_code == 6 and report.stalled
    rule.gate.set()  # type: ignore[union-attr]


def test_a_blocked_write_that_returns_late_resumes_without_losing_the_record(
    tmp_path: Path,
) -> None:
    rig = Rig(tmp_path, long=True)
    rig.ledger.open()
    rule = rig.fs.block("write", times=1)
    rig.ledger.record("action", "slow")
    rig.ledger._hand_over(10)
    assert rule.entered.wait(5)
    rig.clock.advance(6.0)
    assert rig.ledger.stalled
    rule.gate.set()  # type: ignore[union-attr]
    rig.ledger.flush()
    assert not rig.ledger.stalled
    rig.ledger.close()
    assert "slow" in [r["event"] for r in rig.rows()]
    assert any(r["note"] == "slow_call" for r in rig.events("ledger.note"))


# -- rotation ----


def _fill(rig: Rig, count: int, size: int = 2000) -> None:
    for i in range(count):
        rig.ledger.record("action", "fill", i=i, pad="x" * size)


def test_the_ledger_rotates_at_max_file_bytes_with_a_part_record_naming_the_previous(
    tmp_path: Path,
) -> None:
    rig = Rig(tmp_path)
    rig.ledger.open()
    _fill(rig, 1500)  # about 3 MB: three parts of 1 MiB
    rig.ledger.close()
    parts = [f for f in rig.files() if f.endswith(".jsonl")]
    assert len(parts) >= 3 and parts[0].endswith("-s.jsonl") and ".p0002." in parts[1]
    stamp = stamp_of(rig.ledger.start_ns)
    for number in (2, 3):
        rows = [
            json.loads(x)
            for x in (rig.dir / part_name(stamp, "s", number)).read_text().splitlines()
        ]
        assert rows[0]["event"] == "ledger.part" and rows[0]["reason"] == "rotation"
        assert rows[0]["previous"] == part_name(stamp, "s", number - 1)
    fills = [r["i"] for r in rig.rows(bookkeeping=False) if r["event"] == "fill"]
    assert fills == list(range(1500))  # nothing lost, nothing reordered across parts
    for part in parts:
        assert (rig.dir / part).stat().st_size <= MIB + 4096
    assert rig.deleted_nothing()


def test_part_numbers_widen_past_9999(tmp_path: Path) -> None:
    assert part_name("S", "x", 9999) == "S-x.p9999.jsonl"
    assert part_name("S", "x", 10000) == "S-x.p10000.jsonl"
    assert part_name("S", "x", 1) == "S-x.jsonl"
    assert reserve_name("S", "x", 1) == "S-x.r0001.reserve"
    rig = Rig(tmp_path)
    rig.ledger.open()
    rig.ledger.flush()
    rig.ledger._store._highest_part = 9998  # as if 9997 parts had been written
    _fill(rig, 1100)
    rig.ledger.close()
    assert any(f.endswith(".p9999.jsonl") for f in rig.files())
    assert any(f.endswith(".p10000.jsonl") for f in rig.files())


def test_a_foreign_file_at_the_next_part_name_is_skipped_and_ledgered(tmp_path: Path) -> None:
    rig = Rig(tmp_path)
    rig.ledger.open()
    stamp = stamp_of(rig.ledger.start_ns)
    foreign = rig.dir / part_name(stamp, "s", 2)
    foreign.write_bytes(b"not ours\n")
    _fill(rig, 700)
    rig.ledger.close()
    assert foreign.read_bytes() == b"not ours\n"  # untouched
    assert (rig.dir / part_name(stamp, "s", 3)).exists()
    (taken,) = rig.events("ledger.name_taken")
    assert taken["name"] == foreign.name and taken["inode"] == foreign.stat().st_ino


def test_sixteen_taken_part_names_block_rotation_without_a_stall(tmp_path: Path) -> None:
    rig = Rig(tmp_path, long=True)
    rig.ledger.open()
    stamp = stamp_of(rig.ledger.start_ns)
    for number in range(2, 18):
        (rig.dir / part_name(stamp, "s", number)).write_bytes(b"x")
    _fill(rig, 700)
    rig.ledger.flush()
    assert not rig.ledger.stalled
    assert rig.codes().count(Code.LED_ROTATE) == 1
    part1 = rig.dir / part_name(stamp, "s", 1)
    assert part1.stat().st_size > MIB  # the current part keeps growing
    assert len(rig.events("ledger.name_taken")) == 16
    _fill(rig, 100)
    rig.ledger.flush()
    assert (
        rig.codes().count(Code.LED_ROTATE) == 1
        and not (rig.dir / part_name(stamp, "s", 18)).exists()
    )
    rig.step(61.0)  # retried once a minute, continuing from the next number
    _fill(rig, 10)
    rig.ledger.flush()
    assert (rig.dir / part_name(stamp, "s", 18)).exists()
    rig.ledger.close()


# -- the essential reserve ----


def _reserve_works(tmp_path: Path) -> None:
    probe = tmp_path / "probe"
    fd = os.open(probe, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        os.posix_fallocate(fd, 0, MIB)
        if os.fstat(fd).st_blocks * 512 < MIB:
            pytest.skip("this file system does not allocate blocks for posix_fallocate")
    except OSError:
        pytest.skip("posix_fallocate is not supported here")
    finally:
        os.close(fd)
        probe.unlink()


def test_a_long_session_allocates_a_verified_reserve_written_in_place(tmp_path: Path) -> None:
    _reserve_works(tmp_path)
    rig = Rig(tmp_path, long=True)
    rig.ledger.open()
    stamp = stamp_of(rig.ledger.start_ns)
    reserve = rig.dir / reserve_name(stamp, "s", 1)
    assert reserve.exists() and reserve.stat().st_size == MIB
    assert reserve.stat().st_blocks * 512 >= MIB
    first = reserve.read_bytes().split(b"\n", 1)[0]
    assert json.loads(first)["event"] == "ledger.reserve"
    opens = [c for c in rig.fs.calls if c[0] == "open_at" and str(c[1][1]).endswith(".reserve")]
    assert opens and all(
        c[1][2] & os.O_RDWR and not c[1][2] & os.O_APPEND and c[1][2] & os.O_EXCL for c in opens
    )
    rig.ledger.close()
    assert not any(f.endswith(".reserve") for f in rig.files())  # retired at close
    assert rig.deleted_nothing()


def test_a_short_command_holds_no_reserve(tmp_path: Path) -> None:
    rig = Rig(tmp_path, long=False)
    rig.ledger.open()
    assert not any(f.endswith(".reserve") for f in rig.files())
    assert rig.fs.count("fstatfs_type") == 0 and rig.fs.count("posix_fallocate") == 0
    rig.ledger.close()


def test_a_clean_close_retires_the_reserve_as_the_final_part_holding_the_closing_records(
    tmp_path: Path,
) -> None:
    _reserve_works(tmp_path)
    rig = Rig(tmp_path, long=True)
    rig.ledger.open()
    rig.ledger.record("action", "work")
    rig.ledger.flush()
    rig.ledger.record("action", "session.end")
    rig.ledger.close()
    stamp = stamp_of(rig.ledger.start_ns)
    final = rig.dir / part_name(stamp, "s", 2)
    rows = [json.loads(x) for x in final.read_text().splitlines()]
    assert [r["event"] for r in rows][:2] == ["ledger.reserve", "ledger.part"]
    assert rows[1]["reason"] == "closing" and rows[-1]["event"] == "session.end"
    assert final.stat().st_size == len(final.read_bytes())  # cut at its last complete record
    assert b"\x00" not in final.read_bytes() and final.read_bytes().endswith(b"\n")


@pytest.mark.parametrize("err", [errno.ENOSPC, errno.EDQUOT], ids=lambda e: errno.errorcode[e])
def test_disk_full_switches_to_the_reserve_cuts_back_the_part_and_essential_continues(
    tmp_path: Path, err: int
) -> None:
    _reserve_works(tmp_path)
    rig = Rig(tmp_path, long=True)
    rig.ledger.open()
    rig.ledger.record("action", "before")
    rig.ledger.flush()
    stamp = stamp_of(rig.ledger.start_ns)
    part1 = rig.dir / part_name(stamp, "s", 1)
    rig.fs.fail("write", err, partial=3)  # a write that lands 3 bytes of the line, then fails
    rig.ledger.record("action", "interrupted", pad="y" * 100)
    rig.ledger.flush()
    assert part1.read_bytes().endswith(b"\n")  # cut back to its last complete line
    assert not rig.ledger.stalled and rig.ledger.degraded
    assert rig.codes().count(Code.LED_DISK) == 1
    # event and evidence records are counted only now; essential ones continue in the reserve
    assert rig.ledger.record("event", "rx.event", text="e") is None
    assert rig.ledger.record("packet", "rx.other", raw=b"\x00") is None
    for i in range(20):
        assert rig.ledger.record("command", "tx.command", i=i) is not None
    rig.ledger.flush()
    reserve = rig.dir / reserve_name(stamp, "s", 1)
    inside = [json.loads(x) for x in reserve.read_bytes().rstrip(b"\x00").decode().splitlines()]
    assert [r["event"] for r in inside[:3]] == ["ledger.reserve", "ledger.part", "interrupted"]
    assert inside[1]["previous"] == part1.name and "full" in inside[1]["reason"]
    assert [r["event"] for r in inside].count("tx.command") == 20
    assert reserve.stat().st_size == MIB  # written in place: the extent did not change
    assert reserve.stat().st_blocks * 512 >= MIB
    rows = rig.rows()  # every line of every file parses
    assert rows
    report = rig.ledger.close()
    assert report.unlogged == 0 and report.exit_code == 0
    assert rig.deleted_nothing()


def test_a_reserve_used_up_stalls_and_the_next_reserve_resumes_it(tmp_path: Path) -> None:
    _reserve_works(tmp_path)
    rig = Rig(tmp_path, long=True)
    rig.ledger.open()
    rig.fs.fail("write", errno.ENOSPC)
    rig.ledger.record("action", "enter-reserve")
    rig.ledger.flush()
    assert not rig.ledger.stalled
    _fill(rig, 700, size=2000)  # more than 1 MiB of essential records
    rig.ledger.flush()
    assert rig.ledger.stalled  # the reserve is used up: one IB-LED-001 for the episode
    assert Code.LED_WRITE in rig.codes()
    rig.fs.clear()
    rig.step(5.0)  # space is back: the next reserve allocates, the used one retires
    assert not rig.ledger.stalled
    rig.ledger.close()
    fills = [r["i"] for r in rig.rows(bookkeeping=False) if r["event"] == "fill"]
    assert fills == list(range(700))  # every record written, in order, none lost
    assert rig.events("ledger.resumed")
    assert not any(f.endswith(".reserve") for f in rig.files())


def test_leaving_the_reserve_is_tried_once_a_minute_while_it_is_in_use(tmp_path: Path) -> None:
    _reserve_works(tmp_path)
    rig = Rig(tmp_path, long=True)
    rig.ledger.open()
    rule = rig.fs.fail("write", errno.ENOSPC)
    rig.ledger.record("action", "enter-reserve")
    rig.ledger.flush()
    rig.step(30.0)
    stamp = stamp_of(rig.ledger.start_ns)
    assert not (rig.dir / reserve_name(stamp, "s", 3)).exists()
    rule.off = True
    rig.step(31.0)  # a minute passed: the next reserve allocates and the one in use retires
    assert not rig.ledger.degraded
    assert (rig.dir / part_name(stamp, "s", 2)).exists()  # the retired reserve
    rig.ledger.record("event", "rx.event", text="e")  # tiers write again
    rig.ledger.close()
    assert [r["text"] for r in rig.rows(bookkeeping=False) if r["event"] == "rx.event"] == ["e"]


def test_unlimited_tiers_are_still_counted_only_while_the_reserve_is_in_use(
    tmp_path: Path,
) -> None:
    _reserve_works(tmp_path)
    config = LedgerConfig(
        event_bytes_per_hour=None,
        evidence_bytes_per_hour=None,
        essential_reserve_bytes=MIB,
        max_file_bytes=MIB,
    )
    rig = Rig(tmp_path, long=True, config=config)
    rig.ledger.open()
    rig.fs.fail("write", errno.ENOSPC)
    rig.ledger.record("action", "enter")
    rig.ledger.flush()
    assert rig.ledger.record("event", "rx.event", text="e") is None
    assert rig.ledger.record("packet", "rx.other", raw=b"\x01") is None
    rig.ledger.close()


# -- reserve allocation: refusals and verification ----


@pytest.mark.parametrize(
    "f_type", [0x9123683E, 0x6969, 0x794C7630, 0x1234], ids=["btrfs", "nfs", "overlay", "unknown"]
)
def test_a_file_system_outside_the_allowlist_creates_no_reserve_file(
    tmp_path: Path, f_type: int
) -> None:
    rig = Rig(tmp_path, long=True, reserve_fs_type=f_type)
    rig.ledger.open()
    assert not any(f.endswith(".reserve") for f in rig.files())
    assert rig.codes().count(Code.LED_RESERVE) == 1
    assert not any(c[0] == "open_at" and str(c[1][1]).endswith(".reserve") for c in rig.fs.calls)
    rig.step(61.0)  # the check repeats once a minute but the warning is not repeated
    assert rig.codes().count(Code.LED_RESERVE) == 1 and rig.fs.count("fstatfs_type") >= 2
    rig.ledger.close()


def test_a_failing_fstatfs_creates_no_reserve_and_warns_once(tmp_path: Path) -> None:
    fs = FaultFs()
    fs.fail("fstatfs_type", errno.EIO)
    rig = Rig(tmp_path, long=True, fs=fs, reserve_fs_type=None)
    rig.ledger.open()
    assert not any(f.endswith(".reserve") for f in rig.files())
    assert rig.codes().count(Code.LED_RESERVE) == 1
    rig.ledger.close()


@pytest.mark.parametrize("err", [errno.ENOSPC, errno.EOPNOTSUPP], ids=lambda e: errno.errorcode[e])
def test_a_failed_allocation_is_cut_back_to_the_header_and_retried_each_minute(
    tmp_path: Path, err: int
) -> None:
    _reserve_works(tmp_path)
    rig = Rig(tmp_path, long=True)
    rig.fs.fail("posix_fallocate", err, times=1)
    rig.ledger.open()
    stamp = stamp_of(rig.ledger.start_ns)
    reserve = rig.dir / reserve_name(stamp, "s", 1)
    assert rig.codes().count(Code.LED_RESERVE) == 1
    assert 0 < reserve.stat().st_size < 1024  # only the header
    rig.step(61.0)
    assert reserve.stat().st_size == MIB  # the retry allocated it
    rig.ledger.close()


def test_a_short_allocation_is_not_trusted(tmp_path: Path) -> None:
    _reserve_works(tmp_path)

    class Short:
        def __init__(self, real: os.stat_result) -> None:
            self._real = real
            self.st_blocks = 1

        def __getattr__(self, name: str) -> Any:
            return getattr(self._real, name)

    fs = FaultFs()
    calls = {"n": 0}

    def short_once(fd: int) -> Any:
        calls["n"] += 1
        real = os.fstat(fd)
        return Short(real) if calls["n"] == 1 else real  # the allocation check

    rig = Rig(tmp_path, long=True, fs=fs)
    fs.override("fstat", short_once)
    rig.ledger.open()
    assert rig.codes().count(Code.LED_RESERVE) == 1
    assert (
        "could not be allocated"
        in next(n for n in rig.notices if n.code is Code.LED_RESERVE).summary
    )
    rig.ledger.close()


def test_a_foreign_file_at_the_next_reserve_name_is_skipped(tmp_path: Path) -> None:
    _reserve_works(tmp_path)
    rig = Rig(tmp_path, long=True)
    stamp = stamp_of(rig.ledger.start_ns)
    rig.dir.mkdir(parents=True)
    foreign = rig.dir / reserve_name(stamp, "s", 1)
    foreign.write_bytes(b"not ours")
    rig.ledger.open()
    assert foreign.read_bytes() == b"not ours"
    assert (rig.dir / reserve_name(stamp, "s", 2)).stat().st_size == MIB
    (taken,) = rig.events("ledger.name_taken")
    assert taken["name"] == foreign.name
    rig.ledger.close()


def test_the_reserve_is_never_opened_with_append_so_pwrite_lands_at_its_offset(
    tmp_path: Path,
) -> None:
    _reserve_works(tmp_path)
    path = tmp_path / "reserve"
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
    os.posix_fallocate(fd, 0, MIB)
    before = os.fstat(fd)
    FileSystem().pwrite(fd, b"hello\n", 100)
    after = os.fstat(fd)
    assert (after.st_size, after.st_blocks) == (before.st_size, before.st_blocks)
    assert os.pread(fd, 6, 100) == b"hello\n"
    os.close(fd)


# -- the explicit tier of every record ----


def test_events_and_evidence_have_their_own_queue_slices_and_essential_room_remains(
    tmp_path: Path,
) -> None:
    ledger = Ledger(tmp_path / "l", session_id="q", max_queue=100, max_evidence_queue=40)
    ledger._writer  # noqa: B018 - never started: the drain is paused for this test
    events = [ledger.record("event", "rx.event", text="e") for _ in range(100)]
    packets = [ledger.record("packet", "rx.malformed", raw=b"\x00") for _ in range(100)]
    essential = [ledger.record("action", "a") for _ in range(30)]
    assert events.count(None) > 0 and packets.count(None) > 0
    assert sum(n is not None for n in events) + sum(n is not None for n in packets) < 100
    assert all(n is not None for n in essential)  # a flood of either never takes their room
    ledger.close()


def test_the_default_tier_follows_the_record_table_then_the_kind(tmp_path: Path) -> None:
    ledger = Ledger(tmp_path / "l", session_id="t", enabled=False)
    assert ledger.record("packet", "rx.other") is None  # disabled: nothing, whatever the tier
    live = Ledger(tmp_path / "m", session_id="u")
    live.record("command", "rx.command", sample=None)  # a table entry beats the kind default
    assert live._queue[0][5] == 1  # evidence
    live.record("command", "tx.command")
    assert live._queue[1][5] == 0  # essential
    live.record("event", "anything")
    assert live._queue[2][5] == 2  # the kind default
    live.close()


# -- notice pointers after rotation ---------------------------------------------------------------


def test_after_rotation_a_notice_pointer_names_the_part(tmp_path: Path) -> None:
    rig = Rig(tmp_path)
    rig.ledger.open()
    first = Notice(Code.LED_UNLOGGED, Level.WARNING, "k", "x", ledger_n=7)
    assert first.line(verbose=True).endswith("[ledger n=7]")  # part 1: no file=
    _fill(rig, 1500)
    rig.ledger.flush()
    assert rig.ledger.part_number >= 2
    stem = rig.ledger.part_file
    assert ".p000" in stem
    later = Notice(
        Code.LED_UNLOGGED,
        Level.WARNING,
        "k",
        "x",
        ledger_n=1234,
        ledger_part=rig.ledger.part_number,
        ledger_file=stem,
    )
    assert later.line(verbose=True).endswith(f"[ledger n=1234 file={stem}]")
    assert "file=" not in later.line(verbose=False)
    rig.ledger.close()
