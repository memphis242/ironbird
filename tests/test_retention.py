"""Stage 2d, layer 3: retention scans, protection, the prune plan and its execution.

Directories are built by hand (:mod:`retutil`); the scan, the plan and the deletion run against
them with the real file system or a scripted one. Nothing here is deleted by anything but the
confirmed prune the test itself invokes.
"""

from __future__ import annotations

import errno
import fcntl
import os
import time
from pathlib import Path
from typing import Any

import pytest

import retutil
from ironbird.fsops import REAL_FS
from ironbird.prune import execute_plan, list_lines, make_plan, unreachable_summary
from ironbird.retention import (
    FUTURE,
    HARDLINKED,
    HAS_RECORDING,
    INCOMPLETE,
    INSIDE_LONG,
    KEEP_RECENT,
    LIVE,
    DirScan,
    full_scan,
    recording_blocks,
    scan_directory,
)
from ironbird.units import GIB, MIB
from retutil import DAY, head, sid, stamp
from support.faultfs import FaultFs

NOW = time.time()


def make_session(*args: Any, **kwargs: Any) -> list[Path]:
    """``retutil.make_session`` anchored to ``NOW``, the clock every scan here runs at. ``NOW``
    is taken when the module is collected, which can be minutes before a test runs (xdist on a
    slow runner); a file aged from the wall clock at run time would then lie in the future of
    the scan and be read as a clock step instead of as fresh."""
    return retutil.make_session(*args, now=NOW, **kwargs)


def scan(
    directory: Path,
    kind: str = "ledger",
    *,
    keep: int = 10,
    fs: Any = REAL_FS,
    own: str | None = None,
    has_recording: Any = None,
) -> DirScan:
    return full_scan(
        fs, directory, kind, now=NOW, own_session=own, keep_recent=keep, has_recording=has_recording
    )


def protected(result: DirScan) -> dict[str, list[str]]:
    return {s.id: s.protected for s in result.sessions.values()}


@pytest.fixture
def state(tmp_path: Path) -> Path:
    path = tmp_path / "st"
    path.mkdir(mode=0o700)
    return path


# -- names and lookalikes ----


def test_only_anchored_names_count_and_lookalikes_are_ignored(state: Path) -> None:
    (real,) = make_session(state, 1, age_days=30)
    start = stamp(NOW - 30 * DAY)
    for lookalike in (
        f"{start}-{sid(2)}.jsonl.bak",
        f"{start}-{sid(0xABC).upper()}.jsonl",
        f"{start}-{sid(3)}0.jsonl",  # a 13-digit id
        f"{start}-{sid(4)}.p00005.jsonl",  # a leading zero beyond the padding
        f"{start}-{sid(5)}.p0001.jsonl",  # part 1 has no number
        f"{start}-{sid(6)}.r0001.reserve.damaged",
        f".{start}-{sid(7)}.jsonl.0123456789abcdef.tmp",
        "retention-notices.json",
    ):
        (state / lookalike).write_text("x")
    (state / f"{start}-{sid(8)}.jsonl").mkdir()  # a directory with a matching name
    result = scan(state)
    assert list(result.sessions) == [sid(1)] and result.sessions[sid(1)].files[0].name == real.name
    assert result.foreign == 1  # the directory


def test_an_invalid_stamp_is_unparseable(state: Path) -> None:
    for bad in ("20261340T250000Z", "20261030T126000Z", "19691231T235959Z"):
        (state / f"{bad}-{sid(1)}.jsonl").write_text("x")
    result = scan(state)
    assert result.unparseable == 3 and not result.sessions


def test_part_numbers_widen_and_order_after_9999(state: Path) -> None:
    start = stamp(NOW - 30 * DAY)
    for name in (".jsonl", ".p9999.jsonl", ".p10000.jsonl", ".p123456789.jsonl"):
        (state / f"{start}-{sid(1)}{name}").write_text("x")
    numbers = sorted(f.number for f in scan(state).sessions[sid(1)].files)
    assert numbers == [1, 9999, 10000, 123456789]


def test_a_fifo_a_symlink_and_a_foreign_owner_are_foreign_and_never_block(state: Path) -> None:
    start = stamp(NOW - 30 * DAY)
    os.mkfifo(state / f"{start}-{sid(1)}.jsonl")
    target = state / "target"
    target.write_text("x")
    (state / f"{start}-{sid(2)}.jsonl").symlink_to(target)
    (mine,) = make_session(state, 3, age_days=30)
    fs = FaultFs()
    real_fstat = REAL_FS.fstat

    def other_owner(fd: int) -> Any:
        st = real_fstat(fd)
        if st.st_ino == mine.stat().st_ino:
            values = list(st)
            values[4] = os.getuid() + 1  # st_uid
            return os.stat_result(values)
        return st

    fs.override("fstat", other_owner)
    result = scan_directory(fs, state, "ledger")
    assert result.foreign == 3 and not result.sessions


def test_a_hard_linked_file_is_counted_once_and_protects_its_session(state: Path) -> None:
    (path,) = make_session(state, 1, age_days=30, size=1000)
    os.link(path, state / "somewhere-else")
    result = scan(state, keep=0)
    session = result.sessions[sid(1)]
    assert HARDLINKED in session.protected
    assert session.size == path.stat().st_size  # one inode, one count
    plan = make_plan(result, older_than_s=DAY, until_under=None, now=NOW)
    assert plan.candidates == []


# -- protection ----


def test_keep_recent_protects_the_most_recent_sessions_of_any_class(state: Path) -> None:
    for i in range(1, 8):
        make_session(state, i, age_days=40 - i, klass="short")
    result = scan(state, keep=3)
    kept = sorted(s for s, reasons in protected(result).items() if KEEP_RECENT in reasons)
    assert kept == [sid(5), sid(6), sid(7)]


def test_thirty_short_sends_after_an_overnight_run_keep_the_long_session(state: Path) -> None:
    make_session(state, 1, age_days=20, klass="long", parts=3)
    for i in range(2, 32):  # 30 short sessions, newer than the long one
        make_session(state, i, age_days=19 - i * 0.01, klass="short")
    result = scan(state, keep=10)
    assert KEEP_RECENT in result.sessions[sid(1)].protected  # the 10 most recent *long* ones
    plan = make_plan(result, older_than_s=7 * 86400, until_under=None, now=NOW)
    assert sid(1) not in {c.session.id for c in plan.candidates}
    assert len(plan.candidates) == 30 - 10  # shorts beyond the 10 most recent of any class


def test_a_short_session_inside_a_protected_long_ones_span_is_kept(state: Path) -> None:
    long_paths = make_session(state, 1, age_days=30, klass="long")
    start = NOW - 30 * DAY
    os.utime(long_paths[0], (start + 20 * 3600, start + 20 * 3600))  # ran for 20 hours
    make_session(state, 2, age_days=30 - 5 / 24, klass="short")  # started during the run
    for i in range(3, 40):  # newer sessions push both out of the keep window
        make_session(state, i, age_days=10, klass="short")
    result = scan(state, keep=0 + 1)
    assert KEEP_RECENT not in result.sessions[sid(2)].protected
    result = scan(state, keep=2)
    assert (
        INSIDE_LONG in result.sessions[sid(2)].protected
        or KEEP_RECENT in result.sessions[sid(1)].protected
    )


def test_the_class_comes_from_the_head_and_an_invalid_head_counts_as_long(state: Path) -> None:
    make_session(state, 1, age_days=20, klass="short")
    make_session(state, 2, age_days=21, stage1=True, command="send")  # a Stage 1 ledger
    make_session(state, 3, age_days=22, stage1=True, command="listen")
    for i, junk in enumerate((b"not json\n", b"[1]\n", b"x" * 5000, b"{}\n"), start=4):
        (path,) = make_session(state, i, age_days=23 + i)
        path.write_bytes(junk)
        os.utime(path, (NOW - (23 + i) * DAY, NOW - (23 + i) * DAY))
    other = make_session(state, 20, age_days=40)[0]
    other.write_text(head(sid(21), "short"))  # another session's head
    os.utime(other, (NOW - 40 * DAY, NOW - 40 * DAY))
    result = scan(state, keep=100)
    classes = {s.id: s.klass for s in result.sessions.values() if s.klass}
    assert classes[sid(1)] == "short" and classes[sid(2)] == "short" and classes[sid(3)] == "long"
    assert all(classes[sid(i)] == "long" for i in (4, 5, 6, 7, 20))


def test_a_ledger_whose_recording_is_present_is_kept_until_the_recording_is_proposed(
    state: Path, tmp_path: Path
) -> None:
    rec_dir = tmp_path / "rec"
    make_session(state, 1, age_days=30)
    make_session(state, 2, age_days=30)
    make_session(rec_dir, 1, age_days=1, recording=True)  # a young recording: not proposed
    make_session(rec_dir, 2, age_days=30, recording=True)  # an old one: proposed by rec prune
    recordings = scan(rec_dir, "recording", keep=0)
    blocks = recording_blocks(recordings, NOW, 7 * 86400)
    result = scan(state, keep=0, has_recording=blocks)
    assert HAS_RECORDING in result.sessions[sid(1)].protected
    assert HAS_RECORDING not in result.sessions[sid(2)].protected


def test_a_live_session_is_never_proposed_by_lock_by_freshness_or_future_mtime(state: Path) -> None:
    (locked,) = make_session(state, 1, age_days=30)
    (fresh,) = make_session(state, 2, age_days=0.001)  # a minute old
    (future,) = make_session(state, 3, age_days=-2)  # a clock step: mtime in the future
    (idle,) = make_session(state, 4, age_days=30)
    holder = os.open(locked, os.O_RDONLY)
    try:
        fcntl.flock(holder, fcntl.LOCK_EX)
        result = scan(state, keep=0)
    finally:
        os.close(holder)
    assert LIVE in result.sessions[sid(1)].protected
    assert LIVE in result.sessions[sid(2)].protected
    assert result.sessions[sid(3)].protected[0] == FUTURE
    assert result.sessions[sid(4)].protected == []
    plan = make_plan(result, older_than_s=DAY, until_under=None, now=NOW)
    assert [c.session.id for c in plan.candidates] == [sid(4)]
    assert fresh.exists() and future.exists() and idle.exists()


def test_the_checkers_own_session_is_live_by_definition_and_never_probed(state: Path) -> None:
    (mine,) = make_session(state, 1, age_days=30)
    fs = FaultFs()
    result = scan(state, keep=0, fs=fs, own=sid(1))
    assert result.sessions[sid(1)].protected == [LIVE]
    assert not any(c[0] == "flock" for c in fs.calls if c[1] and mine.name in str(c[1]))
    assert fs.count("flock") == 0


def test_a_probe_that_proves_nothing_leaves_liveness_to_freshness(state: Path) -> None:
    (old,) = make_session(state, 1, age_days=30)
    fs = FaultFs()
    fs.fail("flock", errno.EBADF)  # the probe proves nothing
    result = scan(state, keep=0, fs=fs)
    assert result.sessions[sid(1)].protected == []
    (fresh,) = make_session(state, 2, age_days=0.001)
    fs2 = FaultFs()
    fs2.fail("flock", errno.ENOLCK)
    assert LIVE in scan(state, keep=0, fs=fs2).sessions[sid(2)].protected
    assert old.exists() and fresh.exists()


def test_a_session_without_part_1_is_incomplete_and_never_proposed(state: Path) -> None:
    start = stamp(NOW - 30 * DAY)
    path = state / f"{start}-{sid(1)}.p0002.jsonl"
    path.write_text("x")
    os.utime(path, (NOW - 30 * DAY, NOW - 30 * DAY))
    result = scan(state, keep=0)
    assert result.sessions[sid(1)].protected == [INCOMPLETE]
    assert make_plan(result, older_than_s=DAY, until_under=None, now=NOW).candidates == []


# -- the plan ----


def test_the_plan_is_exactly_age_then_size_oldest_first_and_never_protected(state: Path) -> None:
    for i in range(1, 11):
        make_session(state, i, age_days=60 - i, size=10_000, klass="short")  # 10 old sessions
    make_session(state, 11, age_days=1, size=10_000, klass="short")
    result = scan(state, keep=1)
    plan = make_plan(result, older_than_s=7 * 86400, until_under=None, now=NOW)
    assert {c.session.id for c in plan.candidates} == {sid(i) for i in range(1, 11)}
    assert all(c.reason == "age" for c in plan.candidates)
    # --until-under: young sessions are not old enough, so the size rule takes the oldest
    young = scan(state, keep=0)
    capped = make_plan(young, older_than_s=100 * 86400, until_under=45_000, now=NOW)
    assert [c.session.id for c in capped.candidates] == [
        sid(1),
        sid(2),
        sid(3),
        sid(4),
        sid(5),
        sid(6),
        sid(7),
    ]
    assert all(c.reason == "size" for c in capped.candidates)
    assert capped.total_bytes - capped.selected_bytes <= 45_000 and not capped.unreachable


def test_nothing_selectable_with_unlimited_age_and_no_flag(state: Path) -> None:
    make_session(state, 1, age_days=400)
    plan = make_plan(scan(state, keep=0), older_than_s=None, until_under=None, now=NOW)
    assert plan.candidates == []


def test_an_unreachable_target_names_every_protected_reason(state: Path) -> None:
    make_session(state, 1, age_days=40, size=50_000, klass="short")  # before any long session
    (live,) = make_session(state, 2, age_days=30, size=30_000)
    make_session(state, 3, age_days=-1, size=20_000)
    holder = os.open(live, os.O_RDONLY)
    try:
        fcntl.flock(holder, fcntl.LOCK_EX)
        result = scan(state, keep=0)
    finally:
        os.close(holder)
    plan = make_plan(result, older_than_s=DAY, until_under=1000, now=NOW)
    assert plan.unreachable and [c.session.id for c in plan.candidates] == [sid(1)]
    text = unreachable_summary(plan)
    assert text.startswith("cannot get the ledger under 1000 B: ") and "live" in text
    assert "future mtime" in text
    lines = list_lines(plan, NOW)
    assert lines[0].startswith("START (UTC)") and "protected:" in lines[-1]


def test_a_group_writable_directory_without_the_sticky_bit_is_unsafe(state: Path) -> None:
    make_session(state, 1, age_days=30)
    state.chmod(0o770)
    assert scan_directory(REAL_FS, state, "ledger").unsafe
    state.chmod(0o1770)
    assert not scan_directory(REAL_FS, state, "ledger").unsafe


def test_a_directory_over_the_entry_bound_gives_a_partial_scan(state: Path) -> None:
    for i in range(50):
        (state / f"junk{i}").write_text("x")
    result = scan_directory(REAL_FS, state, "ledger", entry_limit=20)
    assert result.partial and "partial" in result.partial and result.entries == 21


# -- executing a plan ----


def _executed(plan: Any, fs: Any = REAL_FS) -> tuple[Any, list[tuple[str, dict[str, Any]]]]:
    events: list[tuple[str, dict[str, Any]]] = []
    outcome = execute_plan(fs, plan, lambda event, **fields: events.append((event, fields)))
    return outcome, events


def _plan_for(state: Path, **kw: Any) -> Any:
    return make_plan(scan(state, keep=0), older_than_s=DAY, until_under=None, now=NOW, **kw)


def test_exactly_the_listed_sessions_are_deleted_parts_before_part_1(state: Path) -> None:
    make_session(state, 1, age_days=30, parts=3)
    make_session(state, 2, age_days=30)
    (keep,) = make_session(state, 3, age_days=0.001)
    plan = _plan_for(state)
    fs = FaultFs()
    outcome, events = _executed(plan, fs)
    assert (outcome.deleted_sessions, outcome.deleted_files, outcome.skipped) == (2, 4, [])
    assert sorted(os.listdir(state)) == [keep.name]
    unlinked = [c[1][1] for c in fs.calls if c[0] == "unlink_at"]
    first = [n for n in unlinked if sid(1) in n]
    assert first[-1].endswith(f"{sid(1)}.jsonl")  # part 1 last
    assert [e for e, _ in events].count("prune.delete") == 4


def test_a_file_removed_between_listing_and_deletion_is_already_gone(state: Path) -> None:
    paths = make_session(state, 1, age_days=30, parts=2)
    plan = _plan_for(state)
    paths[1].unlink()  # the test removes it, as someone else might
    outcome, events = _executed(plan)
    assert outcome.skipped == [] and outcome.gone_files == 1 and outcome.deleted_files == 1
    assert "prune.gone" in [e for e, _ in events] and os.listdir(state) == []


@pytest.mark.parametrize(
    "change",
    ["grown", "symlink", "inode_swapped", "became_live", "touched", "hard_linked"],
)
def test_a_session_that_changed_since_the_listing_is_skipped_and_nothing_is_deleted(
    state: Path, tmp_path: Path, change: str
) -> None:
    paths = make_session(state, 1, age_days=30, parts=2)
    other = make_session(state, 2, age_days=30)[0]
    plan = _plan_for(state)
    victim = paths[1]
    holder = -1
    if change == "grown":
        with victim.open("ab") as f:
            f.write(b"more")
    elif change == "symlink":
        keep = tmp_path / "keep"
        keep.write_text("precious")
        victim.unlink()
        victim.symlink_to(keep)
    elif change == "inode_swapped":
        data = victim.read_bytes()
        os.utime(victim, (NOW - 30 * DAY, NOW - 30 * DAY))
        replacement = victim.with_name("tmp")
        replacement.write_bytes(data)
        os.utime(replacement, (os.stat(victim).st_mtime, os.stat(victim).st_mtime))
        os.rename(replacement, victim)
    elif change == "became_live":
        holder = os.open(paths[0], os.O_RDONLY)
        fcntl.flock(holder, fcntl.LOCK_EX)
    elif change == "touched":
        os.utime(victim, (NOW, NOW))
    elif change == "hard_linked":
        os.link(victim, tmp_path / "second-name")
    try:
        outcome, events = _executed(plan)
    finally:
        if holder >= 0:
            os.close(holder)
    assert [s for s, _ in outcome.skipped] == [sid(1)]
    assert paths[0].exists() and (victim.exists() or victim.is_symlink())
    assert not other.exists()  # the unchanged session was still deleted
    assert [e for e, _ in events].count("prune.skip") == 1
    if change == "symlink":
        assert (tmp_path / "keep").read_text() == "precious"


def test_an_unlink_failure_skips_the_session_before_part_1(state: Path) -> None:
    paths = make_session(state, 1, age_days=30, parts=2)
    plan = _plan_for(state)
    fs = FaultFs()
    fs.fail("unlink_at", errno.EPERM, when=lambda _fd, name: ".p0002." in name)
    outcome, _ = _executed(plan, fs)
    assert outcome.skipped and "could not be deleted" in outcome.skipped[0][1]
    assert paths[0].exists() and paths[1].exists()  # part 1 is only removed last, and not now


def test_a_dry_run_and_a_scan_delete_nothing_even_with_everything_old(state: Path) -> None:
    for i in range(1, 6):
        make_session(state, i, age_days=400, parts=2, klass="short")
    before = {p.name: p.read_bytes() for p in state.iterdir()}
    fs = FaultFs()
    plan = make_plan(
        full_scan(fs, state, "ledger", now=NOW, own_session=None, keep_recent=0),
        older_than_s=DAY,
        until_under=MIB,
        now=NOW,
    )
    assert len(plan.candidates) == 5
    assert {p.name: p.read_bytes() for p in state.iterdir()} == before
    assert not any(fs.count(name) for name in ("unlink_at", "unlink_path", "rmdir_at"))
    assert GIB > MIB
