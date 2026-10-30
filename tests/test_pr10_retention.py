"""Regression tests for the PR #10 review findings on retention: reserve salvage that can run
twice or be cut off, protection that missed short sessions inside a long one, sessions stamped by
a clock that ran ahead, and recovery working from a stale listing."""

from __future__ import annotations

import errno
import fcntl
import json
import os
import time
from pathlib import Path
from typing import Any

import pytest

from ironbird.fsops import REAL_FS, FileSystem
from ironbird.retention import (
    INSIDE_LONG,
    KEEP_RECENT,
    DirScan,
    ScanControl,
    full_scan,
    liveness,
    recover_directory,
    scan_directory,
)
from retutil import DAY, lines_of, make_reserve, make_session, reserve_header, sid, stamp
from support.faultfs import FaultFs, Rule

NOW = time.time()


def rec(n: int, event: str = "tx.command") -> str:
    return json.dumps({"session": sid(1), "n": n, "kind": "action", "event": event}) + "\n"


def scan(state: Path, fs: FileSystem = REAL_FS, **kw: Any) -> DirScan:
    kw.setdefault("keep_recent", 0)
    return full_scan(fs, state, "ledger", now=NOW, own_session=None, **kw)


@pytest.fixture
def state(tmp_path: Path) -> Path:
    path = tmp_path / "st"
    path.mkdir(mode=0o700)
    make_session(path, 1, age_days=3, now=NOW)
    return path


def names(state: Path) -> list[str]:
    return sorted(p.name for p in state.iterdir())


def damaged_reserve(state: Path, records: int = 3) -> Path:
    body = (reserve_header(sid(1)) + "".join(rec(10 + i) for i in range(records))).encode()
    body += b"\x00" * 64 + b"JUNK"
    return make_reserve(state, 1, 1, body, now=NOW)


# -- A1: salvage is idempotent, atomic and never abandoned half-way ----


def test_a_salvage_whose_set_aside_name_is_taken_copies_nothing_on_any_scan(state: Path) -> None:
    reserve = damaged_reserve(state)
    taken = state / f"{reserve.name}.damaged"
    taken.write_bytes(b"user data")
    for i in range(3):
        result = scan(state)
        assert result.messages and "already exists" in result.messages[0], i
    assert not any(".p0002." in n for n in names(state))  # no copy was made, so none repeats
    assert reserve.exists() and taken.read_bytes() == b"user data"


def test_a_salvage_on_a_full_disk_sets_the_original_aside_once_and_ledgers_it(
    state: Path,
) -> None:
    reserve = damaged_reserve(state)
    before = reserve.read_bytes()
    fs = FaultFs()
    fs.fail("write", errno.ENOSPC)
    first = scan(state, fs)
    damaged = state / f"{reserve.name}.damaged"
    assert damaged.read_bytes() == before and not reserve.exists()
    (recovery,) = first.recoveries  # ledger.recovered is written even when the copy failed
    assert recovery["old"] == reserve.name and recovery["new"] is None
    assert recovery["kept"] == 0 and recovery["cut"] == len(before) and recovery["error"]
    assert first.messages and "could not be saved" in first.messages[0]
    # no torn part, no temporary file left behind, and nothing repeats on later scans
    assert not any(n.startswith(".") or ".p0002." in n for n in names(state))
    for _ in range(2):
        later = scan(state)
        assert later.recoveries == [] and later.messages == []
    assert not any(".p0002." in n for n in names(state))


class Crash(BaseException):
    """A process dying: not an OSError, not an Exception."""


def test_a_crash_mid_copy_leaves_only_a_dot_temporary_and_the_intact_original(
    state: Path,
) -> None:
    reserve = damaged_reserve(state)
    before = reserve.read_bytes()
    fs = FaultFs()
    fs.rules.append(Rule("write", exc=Crash()))
    fs.rules.append(Rule("unlink_at", err=errno.EIO))  # the dying process removes nothing
    with pytest.raises(Crash):
        scan(state, fs)
    found = names(state)
    (temporary,) = [n for n in found if n.startswith(".")]
    assert temporary.endswith(".tmp") and ".p0002." in temporary
    assert (state / f"{reserve.name}.damaged").read_bytes() == before
    again = scan(state)  # the next scan: nothing to salvage, nothing is made twice
    assert again.recoveries == [] and not any(
        n.endswith(".jsonl") and ".p0002." in n and not n.startswith(".") for n in names(state)
    )


def test_a_salvage_copies_the_prefix_through_a_temporary_and_commits_it_once(state: Path) -> None:
    reserve = damaged_reserve(state)
    fs = FaultFs()
    result = scan(state, fs)
    (recovery,) = result.recoveries
    new = state / recovery["new"]
    assert [r["n"] for r in lines_of(new)] == [9, 10, 11, 12]
    assert (state / f"{reserve.name}.damaged").exists() and not reserve.exists()
    renames = [c for c in fs.calls if c[0] in ("renameat2_noreplace", "link_at", "rename_at")]
    assert len(renames) == 2  # the set-aside, then the commit of the temporary
    assert not any(n.startswith(".") for n in names(state))


def test_a_stop_request_ends_recovery_before_the_next_salvage_and_leaves_the_reserve(
    state: Path,
) -> None:
    reserve = damaged_reserve(state)
    before = reserve.read_bytes()
    control = ScanControl()
    control.stop.set()
    result = scan(state, control=control)
    assert result.recoveries == [] and reserve.read_bytes() == before


def test_the_copy_of_a_salvage_runs_to_its_end_and_says_it_is_stepping(state: Path) -> None:
    damaged_reserve(state)
    control = ScanControl()
    fs = FaultFs()
    seen: list[bool] = []

    def watch(fd: int, data: Any) -> int:
        seen.append(control.stepping.is_set())
        control.stop.set()  # asked to stop in the middle of the copy: it still ends cleanly
        return REAL_FS.write(fd, data)

    fs.override("write", watch)
    result = scan(state, fs, control=control)
    assert seen and all(seen) and not control.stepping.is_set()
    assert result.recoveries[0]["new"] is not None


def test_a_short_commands_scan_does_not_recover(state: Path) -> None:
    reserve = damaged_reserve(state)
    result = scan(state, recover=False)
    assert reserve.exists() and result.recoveries == []


# -- recovery works from the directory as it is when it holds the lock ----


def test_a_session_another_scan_recovered_meanwhile_is_left_alone_without_a_report(
    tmp_path: Path,
) -> None:
    root = tmp_path / "two"
    root.mkdir(mode=0o700)
    (p1,) = make_session(root, 1, age_days=30, now=NOW)
    make_reserve(root, 1, 1, (reserve_header(sid(1)) + rec(10) * 3).encode(), age_days=30, now=NOW)

    class A(FileSystem):
        n = 0
        b: DirScan | None = None

        def flock(self, fd: int, operation: int) -> None:
            if (
                operation == fcntl.LOCK_EX | fcntl.LOCK_NB
                and os.fstat(fd).st_ino == p1.stat().st_ino
            ):
                A.n += 1
                if A.n == 2:  # A's liveness probe is the first; its recovery lock the second
                    A.b = full_scan(
                        REAL_FS, root, "ledger", now=NOW, own_session=None, keep_recent=0
                    )
            super().flock(fd, operation)

    a = full_scan(A(), root, "ledger", now=NOW, own_session=None, keep_recent=0)
    assert A.b is not None and A.b.recovered == 1
    assert a.messages == [] and a.recoveries == [] and not a.unrecoverable


def test_a_stale_second_name_is_not_taken_for_the_reserves_other_link(state: Path) -> None:
    body = (reserve_header(sid(1)) + rec(10)).encode()
    reserve = make_reserve(state, 1, 1, body, now=NOW, pad_to=len(body))
    renamed = state / f"{stamp(NOW - 3 * DAY)}-{sid(1)}.p0002.jsonl"
    os.link(reserve, renamed)
    listing = scan_directory(REAL_FS, state, "ledger")
    live = liveness(REAL_FS, state, listing, NOW, None)
    os.unlink(renamed)  # the second name goes away after the listing: one link is left
    recover_directory(REAL_FS, listing, live, None)
    survivors = [p for p in state.iterdir() if p.read_bytes().startswith(body[:20])]
    assert survivors, "the only name of the records was unlinked"
    assert body in {p.read_bytes() for p in survivors}


# -- A2: protection ----


def _ledger_sessions(state: Path) -> dict[str, list[str]]:
    result = full_scan(REAL_FS, state, "ledger", now=NOW, own_session=None, keep_recent=3)
    return {s.id: s.protected for s in result.sessions.values()}


def test_short_sessions_inside_a_live_long_session_are_protected_even_when_old(
    tmp_path: Path,
) -> None:
    state = tmp_path / "st"
    state.mkdir(mode=0o700)
    (long_part,) = make_session(state, 1, age_days=10, klass="long", now=NOW)
    os.utime(long_part, (NOW, NOW))  # its heartbeat: the session is still running
    for n in (2, 3, 4):
        make_session(state, n, age_days=9.5, klass="short", now=NOW)
    for n in (8, 9, 10):
        make_session(state, n, age_days=2, klass="long", now=NOW)
    protected = _ledger_sessions(state)
    for n in (2, 3, 4):
        assert INSIDE_LONG in protected[sid(n)], n


def test_short_sessions_inside_a_long_session_with_its_recording_are_protected(
    tmp_path: Path,
) -> None:
    state = tmp_path / "st"
    state.mkdir(mode=0o700)
    make_session(state, 5, age_days=8, klass="long", now=NOW)
    for n in (6, 7):
        make_session(state, n, age_days=8 - 1 / 24, klass="short", now=NOW)
    for n in (8, 9, 10):
        make_session(state, n, age_days=2, klass="long", now=NOW)
    long_ledger = next(p for p in state.iterdir() if sid(5) in p.name)
    os.utime(long_ledger, (NOW - 7.5 * DAY, NOW - 7.5 * DAY))  # ran for twelve hours
    result = full_scan(
        REAL_FS,
        state,
        "ledger",
        now=NOW,
        own_session=None,
        keep_recent=3,
        has_recording=lambda session: session == sid(5),
    )
    protected = {s.id: s.protected for s in result.sessions.values()}
    for n in (6, 7):
        assert INSIDE_LONG in protected[sid(n)], n


def test_a_short_session_outside_every_long_span_is_still_proposable(tmp_path: Path) -> None:
    state = tmp_path / "st"
    state.mkdir(mode=0o700)
    make_session(state, 1, age_days=20, klass="short", now=NOW)
    (live,) = make_session(state, 2, age_days=10, klass="long", now=NOW)
    os.utime(live, (NOW, NOW))
    for n in (8, 9, 10):
        make_session(state, n, age_days=2, klass="long", now=NOW)
    assert _ledger_sessions(state)[sid(1)] == []


def test_sessions_stamped_by_a_clock_that_ran_ahead_take_no_keep_recent_slot(
    tmp_path: Path,
) -> None:
    state = tmp_path / "st"
    state.mkdir(mode=0o700)
    for n in range(1, 13):
        make_session(state, n, age_days=n, klass="long", now=NOW)
    for n in range(100, 110):
        make_session(state, n, age_days=-365, klass="long", now=NOW)
    result = full_scan(REAL_FS, state, "ledger", now=NOW, own_session=None, keep_recent=10)
    protected = {s.id: s.protected for s in result.sessions.values()}
    kept = {n for n in range(1, 13) if KEEP_RECENT in protected[sid(n)]}
    assert kept == set(range(1, 11))
    assert protected[sid(11)] == [] and protected[sid(12)] == []


# -- A4: --force never replaces a managed name or a file another process holds ----


def _recording(path: Path) -> bytes:
    from recutil import sample

    sample(path, 5)
    return path.read_bytes()


def test_force_refuses_an_output_named_like_a_ledger_or_recording_file(tmp_path: Path) -> None:
    from ironbird.errors import Code, IronbirdError
    from ironbird.recording import inspect as rec_inspect

    source = tmp_path / "in.ibrec"
    _recording(source)
    for name in (
        f"{stamp(NOW)}-{sid(5)}.ibrec",
        f"{stamp(NOW)}-{sid(5)}.p0002.ibrec",
        f"{stamp(NOW)}-{sid(5)}.jsonl",
        f"{stamp(NOW)}-{sid(5)}.r0001.reserve",
    ):
        out = tmp_path / name
        out.write_bytes(b"precious")
        with pytest.raises(IronbirdError) as info:
            rec_inspect.repair(source, out, force=True)
        assert info.value.code is Code.REC_CREATE and "--force never replaces" in info.value.summary
        assert out.read_bytes() == b"precious"


def test_force_refuses_an_output_another_process_holds_under_a_lock(tmp_path: Path) -> None:
    from ironbird.errors import Code, IronbirdError
    from ironbird.recording import inspect as rec_inspect

    source = tmp_path / "in.ibrec"
    _recording(source)
    out = tmp_path / "somewhere.ibrec"
    out.write_bytes(b"live")
    holder = os.open(out, os.O_RDONLY)
    try:
        fcntl.flock(holder, fcntl.LOCK_EX)
        with pytest.raises(IronbirdError) as info:
            rec_inspect.repair(source, out, force=True)
    finally:
        os.close(holder)
    assert info.value.code is Code.REC_CREATE and "in use" in info.value.summary
    assert out.read_bytes() == b"live"
    assert [p.name for p in tmp_path.iterdir() if p.name.startswith(".")] == []  # no temporary
    rec_inspect.repair(source, out, force=True)  # released: the one replacement is allowed
    assert out.read_bytes() != b"live"


def test_a_failed_directory_sync_after_the_rename_is_success_with_a_warning(
    tmp_path: Path,
) -> None:
    from ironbird.recording import inspect as rec_inspect

    source = tmp_path / "in.ibrec"
    _recording(source)
    out = tmp_path / "out.ibrec"
    fs = FaultFs()
    fs.fail("fsync", errno.EIO)
    result = rec_inspect.repair(source, out, fs=fs)
    assert out.exists() and result.sync_warning
    assert [p.name for p in tmp_path.iterdir() if p.name.startswith(".")] == []


def test_a_second_prune_of_an_already_deleted_session_counts_its_files_as_gone(
    tmp_path: Path,
) -> None:
    from ironbird.prune import execute_plan, make_plan

    state = tmp_path / "pruned"
    state.mkdir(mode=0o700)
    make_session(state, 1, age_days=30, parts=3, now=NOW)
    plans = [make_plan(scan(state), older_than_s=DAY, until_under=None, now=NOW) for _ in range(2)]
    log: list[str] = []
    first = execute_plan(REAL_FS, plans[0], lambda event, **_f: log.append(event))
    second = execute_plan(REAL_FS, plans[1], lambda event, **_f: log.append(event))
    assert first.deleted_files == 3 and first.skipped == []
    assert second.deleted_files == 0 and second.gone_files == 3 and second.skipped == []
    assert log.count("prune.gone") == 3 and "prune.skip" not in log
