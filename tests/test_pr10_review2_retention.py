"""Regression tests for the second round of PR #10 review findings on retention: a salvage that
is cut short (exit budget, a kill after the set-aside, a failing disk) leaves records only in
the ``.damaged`` original, and that file, its dot-temporary and a file the scan could not examine
are states every scan recognises - protected, counted, reported and finished."""

from __future__ import annotations

import errno
import json
import os
import time
from pathlib import Path
from typing import Any

import pytest

from ironbird.config import RetentionConfig
from ironbird.fsops import REAL_FS, FileSystem
from ironbird.ledger import Ledger
from ironbird.prune import make_plan
from ironbird.retention import (
    DAMAGED_KEPT,
    UNREADABLE,
    UNRECOVERABLE,
    DirScan,
    full_scan,
)
from ironbird.retnotices import Notices
from ironbird.retservice import Retention
from retutil import DAY, lines_of, make_reserve, make_session, reserve_header, sid, stamp
from support.faultfs import FakeClock, FaultFs, Rule

NOW = time.time()


def rec(n: int, event: str = "tx.command") -> str:
    return json.dumps({"session": sid(1), "n": n, "kind": "action", "event": event}) + "\n"


def scan(state: Path, fs: FileSystem = REAL_FS, **kw: Any) -> DirScan:
    kw.setdefault("keep_recent", 0)
    return full_scan(fs, state, "ledger", now=time.time(), own_session=None, **kw)


@pytest.fixture
def state(tmp_path: Path) -> Path:
    path = tmp_path / "st"
    path.mkdir(mode=0o700)
    make_session(path, 1, age_days=3, now=NOW)
    return path


def _age(path: Path, days: float = 3.0) -> None:
    old = NOW - days * DAY
    os.utime(path, (old, old))


def names(state: Path) -> list[str]:
    return sorted(p.name for p in state.iterdir())


def damaged_reserve(state: Path, records: int = 3) -> Path:
    body = (reserve_header(sid(1)) + "".join(rec(10 + i) for i in range(records))).encode()
    body += b"\x00" * 64 + b"JUNK"
    return make_reserve(state, 1, 1, body, now=NOW)


def parts(state: Path) -> list[Path]:
    return sorted(p for p in state.iterdir() if ".p0002." in p.name and not p.name.startswith("."))


def cut_short(state: Path) -> tuple[Path, bytes]:
    """A reserve whose salvage set it aside and then could not copy (the disk refused)."""
    reserve = damaged_reserve(state)
    original = reserve.read_bytes()
    fs = FaultFs()
    fs.fail("write", errno.ENOSPC)
    first = scan(state, fs)
    assert first.incompletes and not parts(state)
    return state / f"{reserve.name}.damaged", original


# -- B1: the copy is finished by a later scan ----


def test_a_cut_short_salvage_is_finished_by_the_next_scan_and_only_once(state: Path) -> None:
    damaged, original = cut_short(state)
    later = scan(state)
    (recovery,) = later.recoveries
    assert recovery["damaged"] == damaged.name and recovery["new"] == parts(state)[0].name
    assert [r["n"] for r in lines_of(parts(state)[0])] == [9, 10, 11, 12]
    assert damaged.read_bytes() == original  # never deleted, never changed
    assert not any(n.startswith(".") for n in names(state))
    again = scan(state)
    assert again.recoveries == [] and again.incompletes == [] and len(parts(state)) == 1


def test_a_kill_after_the_set_aside_leaves_a_file_every_scan_protects_counts_and_reports(
    state: Path,
) -> None:
    damaged, _ = cut_short(state)
    for _ in range(2):  # a scan that may not recover (a short command) still sees it
        result = scan(state, recover=False)
        assert UNRECOVERABLE in result.sessions[sid(1)].protected
        assert result.total_bytes() >= damaged.stat().st_size
        assert any(damaged.name in m and "ib ledger prune" in m for m in result.messages)
    plan = make_plan(
        scan(state, recover=False), older_than_s=1.0, until_under=None, now=time.time() + 99 * 86400
    )
    assert plan.candidates == []  # prune never proposes the session that holds the only copy


def test_the_originals_of_a_finished_salvage_keep_their_session_and_their_bytes(
    state: Path,
) -> None:
    damaged, _ = cut_short(state)
    scan(state)  # finishes it
    for path in state.iterdir():
        _age(path)  # the new part is not fresh any more
    result = scan(state)
    session = result.sessions[sid(1)]
    assert DAMAGED_KEPT in session.protected and UNRECOVERABLE not in session.protected
    assert damaged.stat().st_size <= session.size and damaged.name in {
        f.name for f in session.files
    }
    assert result.messages == []  # saved: nothing for the user to act on at every scan
    plan = make_plan(result, older_than_s=1.0, until_under=None, now=time.time() + 99 * 86400)
    assert plan.candidates == [] and plan.protected.get(DAMAGED_KEPT)


def test_the_copy_resumes_from_the_original_in_a_long_scan_and_a_prune_scan(
    state: Path,
) -> None:
    cut_short(state)
    seen: list[tuple[str, dict[str, Any]]] = []
    scan(state, on_record=lambda event, fields: seen.append((event, fields)))
    assert [event for event, _ in seen] == ["ledger.recovered"]


def test_a_crash_mid_copy_leaves_a_temporary_the_next_scan_continues(state: Path) -> None:
    class Crash(BaseException):
        pass

    reserve = damaged_reserve(state)
    original = reserve.read_bytes()
    fs = FaultFs()
    fs.rules.append(Rule("write", exc=Crash()))
    fs.rules.append(Rule("unlink_at", err=errno.EIO))  # a dying process removes nothing
    with pytest.raises(Crash):
        scan(state, fs)
    (temporary,) = [p for p in state.iterdir() if p.name.startswith(".")]
    prefix = original[: original.index(b"\n") + 1 + len(rec(10))]
    temporary.write_bytes(prefix)  # the part of the copy that reached the disk
    _age(temporary)  # the crash is long ago: the session is idle again
    inode = temporary.stat().st_ino
    result = scan(state)
    (part,) = parts(state)
    assert part.stat().st_ino == inode  # the same file, continued and committed
    assert [r["n"] for r in lines_of(part)] == [9, 10, 11, 12]
    assert not any(n.startswith(".") for n in names(state))
    assert result.recoveries and (state / f"{reserve.name}.damaged").read_bytes() == original


def test_a_temporary_that_is_not_the_start_of_the_original_is_never_used_or_touched(
    state: Path,
) -> None:
    damaged, _ = cut_short(state)
    foreign = state / f".{stamp(NOW - 3 * DAY)}-{sid(1)}.p0002.jsonl.{'a' * 16}.tmp"
    foreign.write_bytes(b"somebody else's bytes")
    _age(foreign)
    result = scan(state)
    assert foreign.read_bytes() == b"somebody else's bytes"  # left exactly as it was
    (part,) = parts(state)
    assert part.read_bytes() != foreign.read_bytes() and result.recoveries
    assert UNRECOVERABLE in result.sessions[sid(1)].protected  # a leftover file needs a person
    assert any(foreign.name in m for m in result.messages)
    assert damaged.exists()


def test_a_failed_resume_is_reported_and_ledgered_as_incomplete_every_time(state: Path) -> None:
    cut_short(state)
    records: list[tuple[str, dict[str, Any]]] = []
    fs = FaultFs()
    fs.fail("write", errno.ENOSPC)
    result = scan(state, fs, on_record=lambda e, f: records.append((e, f)))
    assert [e for e, _ in records] == ["ledger.recovery_incomplete"]
    assert records[0][1]["error"] and result.incompletes
    assert any("tries again" in m for m in result.messages)
    assert not parts(state)


def test_a_damaged_file_that_is_not_a_reserve_of_the_session_is_kept_and_reported(
    state: Path,
) -> None:
    stray = state / f"{stamp(NOW - 3 * DAY)}-{sid(1)}.r0009.reserve.damaged"
    stray.write_bytes(b"not a reserve at all")
    os.utime(stray, (NOW - 3 * 86400, NOW - 3 * 86400))
    result = scan(state)
    assert stray.read_bytes() == b"not a reserve at all"
    assert UNRECOVERABLE in result.sessions[sid(1)].protected
    assert any("could not be checked" in m and stray.name in m for m in result.messages)


# -- B1: whoever ends the step writes its record, also a thread the exit gave up on ----


def _long_rig(tmp_path: Path) -> tuple[Ledger, Retention, FaultFs, Path]:
    state = tmp_path / "st"
    state.mkdir(mode=0o700)
    fs = FaultFs()
    ledger = Ledger(state, session_id="a" * 12, session_class="long", join_timeout_s=1.0)
    ledger.set_notifier(lambda _n: None)
    retention = Retention(
        ledger,
        RetentionConfig(),
        state,
        tmp_path / "rec",
        long_session=True,
        notices=Notices(state),
        fs=fs,
        clock=FakeClock(),
    )
    return ledger, retention, fs, state


def _final_records(state: Path) -> list[dict[str, Any]]:
    """The records of the ledger the rig writes (session ``aaaaaaaaaaaa``), all its files."""
    rows: list[dict[str, Any]] = []
    for path in sorted(state.iterdir()):
        if "a" * 12 in path.name and path.suffix == ".jsonl":
            rows += [json.loads(x) for x in path.read_text().splitlines()]
    return rows


def test_a_scan_that_outlives_the_exit_wait_writes_its_own_recovered_record(
    tmp_path: Path,
) -> None:
    ledger, retention, fs, state = _long_rig(tmp_path)
    make_session(state, 1, age_days=3, now=NOW)
    damaged_reserve(state)
    gate = fs.block("write")
    ledger.begin_exit()
    ledger.record("action", "x")
    retention.start()
    assert gate.entered.wait(10)  # the scan thread is inside the copy
    started = time.monotonic()
    retention.finish()  # gives up after its share of the exit budget
    assert time.monotonic() - started < 0.9
    ledger.finish()
    ledger.close()
    gate.gate.set()  # type: ignore[union-attr]
    thread = retention._thread
    assert thread is not None
    thread.join(10)
    events = [r["event"] for r in _final_records(state)]
    assert "ledger.recovery_incomplete" in events  # the exit abandoned the wait, and said so
    assert "ledger.recovered" in events  # the scan finished later and wrote its own record


def test_the_scan_thread_hands_its_record_over_as_each_step_ends(tmp_path: Path) -> None:
    ledger, retention, _fs, state = _long_rig(tmp_path)
    make_session(state, 1, age_days=3, now=NOW)
    damaged_reserve(state)
    retention.start()
    thread = retention._thread
    assert thread is not None
    thread.join(10)
    # nothing was collected on this thread yet, and the record is already on its way
    assert any(item[1] == "ledger.recovered" for item in list(ledger._mailbox))
    retention.finish()
    ledger.close()


def test_the_exit_leaves_the_ledger_its_share_of_the_budget(tmp_path: Path) -> None:
    ledger, retention, fs, state = _long_rig(tmp_path)
    make_session(state, 1, age_days=3, now=NOW)
    damaged_reserve(state)
    gate = fs.block("write")
    ledger.begin_exit()
    retention.start()
    assert gate.entered.wait(10)
    retention.finish()
    budget = ledger.wait_budget
    assert budget is not None and budget.left >= 0.45  # of 1.0 s: retention took at most half
    gate.gate.set()  # type: ignore[union-attr]
    report = ledger.finish()
    assert not report.stalled
    ledger.close()


# -- F4: a file the scan could not examine is not "foreign" ----


def test_a_transient_fstat_error_marks_the_session_unreadable_not_foreign(tmp_path: Path) -> None:
    state = tmp_path / "st"
    state.mkdir(mode=0o700)
    make_session(state, 1, age_days=30, now=NOW)
    (_p1, p2) = make_session(state, 2, age_days=30, parts=2, now=NOW)
    make_session(state, 3, age_days=30, now=NOW)
    target = p2.stat().st_ino
    fs = FaultFs()
    real = REAL_FS.fstat

    def flaky(fd: int) -> os.stat_result:
        st = real(fd)
        if st.st_ino == target:
            raise OSError(errno.EIO, "Input/output error")
        return st

    fs.override("fstat", flaky)
    result = scan(state, fs)
    assert result.foreign == 0
    assert (
        result.unreadable == {sid(2)}
        and result.partial
        and "could not be examined" in result.partial
    )
    assert UNREADABLE in result.sessions[sid(2)].protected
    plan = make_plan(result, older_than_s=1.0, until_under=None, now=time.time() + 99 * 86400)
    assert sid(2) not in {c.session.id for c in plan.candidates}
    assert {c.session.id for c in plan.candidates} == {sid(1), sid(3)}


def test_a_file_that_is_not_ours_is_still_foreign(tmp_path: Path) -> None:
    state = tmp_path / "st"
    state.mkdir(mode=0o700)
    (path,) = make_session(state, 1, age_days=30, now=NOW)
    link = state / path.name.replace(sid(1), sid(2))
    link.symlink_to(path)
    result = scan(state)
    assert result.foreign == 1 and not result.unreadable and result.partial is None
