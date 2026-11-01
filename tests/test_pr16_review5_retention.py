"""Regression tests for the fifth round of PR #16 review findings on retention and prune: a
recovery keeps the age of the file it cuts or copies, a check-only pass and a real recovery agree
on the sessions to list (but for one accepted divergence), only prune's no-ledger dry run checks
(a short command's scan only looks), a lock held elsewhere protects its session for the pass, and
the ``IB-LED-002`` warning counts the ``.damaged`` originals a recovery would finish."""

from __future__ import annotations

import fcntl
import io
import json
import os
import threading
import time
from pathlib import Path

import pytest

from ironbird.cli import prunecmd
from ironbird.cli.output import Console
from ironbird.config import RetentionConfig
from ironbird.fsops import REAL_FS, FileSystem
from ironbird.ledger import Ledger
from ironbird.prune import execute_plan, make_plan
from ironbird.retention import (
    BUSY,
    DirScan,
    apply_protection,
    classify_damaged,
    full_scan,
    liveness,
    recover_directory,
    scan_directory,
)
from ironbird.retnotices import Notices
from ironbird.retservice import Retention
from retutil import DAY, make_reserve, make_session, reserve_header, sid, stamp

NOW = time.time()


def rec(s: int, n: int) -> str:
    return json.dumps({"session": sid(s), "n": n, "kind": "action", "event": "tx.command"}) + "\n"


def _clean(s: int) -> bytes:
    return (reserve_header(sid(s)) + rec(s, 10) + rec(s, 11)).encode()


def _damaged(s: int) -> bytes:
    return (reserve_header(sid(s)) + rec(s, 10)).encode() + b"\0" * 8 + b"junk"


def _part_name(n: int, number: int) -> str:
    return f"{stamp(NOW - 3 * DAY)}-{sid(n)}.p{number:04d}.jsonl"


def _build(d: Path, case: str) -> None:
    d.mkdir(mode=0o700)
    make_session(d, 1, age_days=3, now=NOW)
    if case == "clean":
        make_reserve(d, 1, 1, _clean(1), now=NOW)
    elif case == "foreign":
        make_reserve(d, 1, 1, _clean(2), now=NOW)
    elif case == "damaged_prefix":
        make_reserve(d, 1, 1, _damaged(1), now=NOW)
    elif case == "damaged_no_record":
        make_reserve(d, 1, 1, reserve_header(sid(1)).encode() + b"\0" * 8 + b"junk", now=NOW)
    elif case == "header_only":
        make_reserve(d, 1, 1, reserve_header(sid(1)).encode(), now=NOW)
    elif case == "no_header":
        make_reserve(d, 1, 1, b"garbage\n" + rec(1, 1).encode(), now=NOW)
    elif case == "collision_dir":
        make_reserve(d, 1, 1, _clean(1), now=NOW)
        (d / _part_name(1, 2)).mkdir()
    elif case == "interrupted_rename":
        r = make_reserve(d, 1, 1, _clean(1), now=NOW)
        os.link(r, d / _part_name(1, 2))
    elif case == "interrupted_setaside":
        r = make_reserve(d, 1, 1, _damaged(1), now=NOW)
        os.link(r, d / (r.name + ".damaged"))
    elif case == "damaged_file_pending":
        r = make_reserve(d, 1, 1, _damaged(1), now=NOW)
        r.rename(d / (r.name + ".damaged"))
    elif case == "two_clean":
        make_reserve(d, 1, 1, _clean(1), now=NOW)
        make_reserve(d, 1, 2, _clean(1), now=NOW)
    elif case == "clean_then_damaged":
        make_reserve(d, 1, 1, _clean(1), now=NOW)
        make_reserve(d, 1, 2, _damaged(1), now=NOW)
    elif case == "damaged_then_clean":
        make_reserve(d, 1, 1, _damaged(1), now=NOW)
        make_reserve(d, 1, 2, _clean(1), now=NOW)
    elif case == "no_part1":
        next(d.glob("*.jsonl")).unlink()
        make_reserve(d, 1, 1, _clean(1), now=NOW)
    elif case == "clean_unpadded":
        make_reserve(d, 1, 1, _clean(1), now=NOW, pad_to=0)
    elif case == "setaside_exists":
        r = make_reserve(d, 1, 1, _damaged(1), now=NOW)
        (d / (r.name + ".damaged")).mkdir()
    elif case == "short_inside_long":
        make_session(d, 2, age_days=3.5, now=NOW, klass="long")
        make_reserve(d, 2, 1, _clean(2), now=NOW, age_days=3.5)
        recent = NOW - 2.9 * DAY
        os.utime(next(d.glob(f"*{sid(2)}.jsonl")), (recent, recent))
    else:
        raise AssertionError(case)


CASES = [
    "clean",
    "foreign",
    "damaged_prefix",
    "damaged_no_record",
    "header_only",
    "no_header",
    "collision_dir",
    "interrupted_rename",
    "interrupted_setaside",
    "damaged_file_pending",
    "two_clean",
    "clean_then_damaged",
    "damaged_then_clean",
    "no_part1",
    "clean_unpadded",
    "setaside_exists",
    "short_inside_long",
]
# Check-only protects an interrupted rename (a reserve name that is a second name of a part) as
# hard-linked, while a real recovery unlinks the reserve name and the session becomes prunable:
# the divergence is on the safe side, and accepted (docs/plan.md, PR #16 review).
ACCEPTED_DIVERGENCE = frozenset({"interrupted_rename"})


def _snapshot(d: Path) -> dict[str, tuple[object, ...]]:
    out: dict[str, tuple[object, ...]] = {}
    for p in sorted(d.iterdir()):
        st = p.lstat()
        out[p.name] = (
            st.st_ino,
            st.st_size,
            st.st_mtime_ns,
            st.st_ctime_ns,
            st.st_nlink,
            st.st_mode,
            p.read_bytes() if p.is_file() else None,
        )
    return out


def _candidates(scan: DirScan, now: float) -> list[str]:
    plan = make_plan(scan, older_than_s=86400, until_under=None, now=now)
    return sorted(c.session.id for c in plan.candidates)


def _protected(scan: DirScan) -> dict[str, bool]:
    return {k: bool(v.protected) for k, v in scan.sessions.items()}


@pytest.mark.parametrize("case", CASES)
def test_check_only_and_a_real_recovery_list_the_same_sessions(tmp_path: Path, case: str) -> None:
    check_dir, real_dir = tmp_path / "check", tmp_path / "real"
    _build(check_dir, case)
    _build(real_dir, case)
    before = _snapshot(check_dir)
    now = time.time()
    checked = full_scan(
        REAL_FS,
        check_dir,
        "ledger",
        now=now,
        own_session=None,
        keep_recent=0,
        recover=False,
        check_only=True,
    )
    assert _snapshot(check_dir) == before, "check-only changed a file"
    real = full_scan(REAL_FS, real_dir, "ledger", now=now, own_session=None, keep_recent=0)
    report = (
        f"{case}: check-only protected={_protected(checked)} plan={_candidates(checked, now)}; "
        f"real protected={_protected(real)} plan={_candidates(real, now)}"
    )
    if case in ACCEPTED_DIVERGENCE:
        # the safe side: check-only never lists what a real recovery protects
        assert set(_candidates(checked, now)) <= set(_candidates(real, now)), report
        assert _candidates(checked, now) != _candidates(real, now), report
        return
    assert _candidates(checked, now) == _candidates(real, now), report
    assert _protected(checked) == _protected(real), report


def test_a_recovered_reserve_keeps_the_age_of_the_file(tmp_path: Path) -> None:
    d = tmp_path / "st"
    _build(d, "clean")
    old = next(d.glob("*.reserve")).stat().st_mtime_ns
    scan = full_scan(REAL_FS, d, "ledger", now=time.time(), own_session=None, keep_recent=0)
    (part,) = d.glob("*.p0002.jsonl")
    assert scan.recovered == 1
    assert part.stat().st_mtime_ns == old


def test_a_salvaged_part_keeps_the_age_of_the_original(tmp_path: Path) -> None:
    d = tmp_path / "st"
    _build(d, "damaged_prefix")
    reserve = next(d.glob("*.reserve"))
    old = reserve.stat().st_mtime_ns
    full_scan(REAL_FS, d, "ledger", now=time.time(), own_session=None, keep_recent=0)
    (part,) = d.glob("*.p0002.jsonl")
    assert part.stat().st_mtime_ns == old
    assert (d / (reserve.name + ".damaged")).stat().st_mtime_ns == old


def test_a_just_recovered_session_is_judged_by_its_true_age(tmp_path: Path) -> None:
    d = tmp_path / "st"
    _build(d, "clean")
    now = time.time()
    scan = full_scan(REAL_FS, d, "ledger", now=now, own_session=None, keep_recent=0)
    assert _candidates(scan, now) == [sid(1)]  # three days old, not "just now"


# -- only prune's no-ledger dry run checks; a short command only looks ----------------------


class _CountingFs(FileSystem):
    """Counts the locks taken on, and the bytes read from, reserve files (the liveness probe's
    locks on parts are not a recovery's)."""

    def __init__(self) -> None:
        self.flocks = 0
        self.reads = 0

    @staticmethod
    def _is_reserve(fd: int) -> bool:
        return os.readlink(f"/proc/self/fd/{fd}").split(" (deleted)")[0].endswith(".reserve")

    def flock(self, fd: int, operation: int) -> None:
        self.flocks += self._is_reserve(fd)
        super().flock(fd, operation)

    def pread(self, fd: int, length: int, offset: int) -> bytes:
        self.reads += self._is_reserve(fd)
        return super().pread(fd, length, offset)


def test_a_scan_that_only_looks_takes_no_lock_and_reads_no_reserve(tmp_path: Path) -> None:
    d = tmp_path / "st"
    _build(d, "clean")
    probe = _CountingFs()  # the liveness probe locks reserves too: that is the baseline
    now = time.time()
    liveness(probe, d, scan_directory(probe, d, "ledger"), now, None)
    fs = _CountingFs()
    scan = full_scan(fs, d, "ledger", now=now, own_session=None, keep_recent=0, recover=False)
    assert fs.flocks == probe.flocks and fs.reads == 0
    assert scan.recoveries == [] and len(list(d.glob("*.reserve"))) == 1


def test_a_check_only_scan_is_an_explicit_choice(tmp_path: Path) -> None:
    d = tmp_path / "st"
    _build(d, "foreign")
    fs = _CountingFs()
    scan = full_scan(
        fs,
        d,
        "ledger",
        now=time.time(),
        own_session=None,
        keep_recent=0,
        recover=False,
        check_only=True,
    )
    assert fs.flocks > 0 and fs.reads > 0
    assert sid(1) in scan.unrecoverable


def test_a_short_commands_retention_scan_only_looks(tmp_path: Path) -> None:
    state = tmp_path / "st"
    _build(state, "clean")
    counting = _CountingFs()
    ledger = Ledger(tmp_path / "led", session_id="a" * 12)
    retention = Retention(
        ledger,
        RetentionConfig(),
        state,
        None,
        long_session=False,
        notices=Notices(tmp_path / "led"),
        fs=counting,
    )
    probe = _CountingFs()
    liveness(probe, state, scan_directory(probe, state, "ledger"), time.time(), None)
    result = retention._scan_directories()
    assert result.ledger is not None
    assert counting.flocks == probe.flocks and counting.reads == 0
    assert len(list(state.glob("*.reserve"))) == 1


# -- a lock held elsewhere protects the session for the pass --------------------------------


class _SlowFs(FileSystem):
    def __init__(self, entered: threading.Event, release: threading.Event) -> None:
        self.entered, self.release = entered, release

    def pread(self, fd: int, length: int, offset: int) -> bytes:
        self.entered.set()
        self.release.wait(10)
        return super().pread(fd, length, offset)


@pytest.mark.parametrize("holder_check_only", [True, False])
def test_a_session_whose_lock_is_held_elsewhere_is_not_pruned(
    tmp_path: Path, holder_check_only: bool
) -> None:
    """Another scan holds the locks while this prune recovers: the prune skips the session, never
    learns that its reserve is unrecoverable, and must not delete it."""
    d = tmp_path / "st"
    _build(d, "foreign")
    foreign = next(d.glob("*.reserve"))
    now = time.time()
    scan = scan_directory(REAL_FS, d, "ledger")
    live = liveness(REAL_FS, d, scan, now, None)
    entered, release = threading.Event(), threading.Event()
    holder = threading.Thread(
        target=lambda: full_scan(
            _SlowFs(entered, release),
            d,
            "ledger",
            now=now,
            own_session=None,
            keep_recent=0,
            recover=not holder_check_only,
            check_only=holder_check_only,
        )
    )
    holder.start()
    try:
        assert entered.wait(10)
        recover_directory(REAL_FS, scan, live, None)
        classify_damaged(REAL_FS, scan)
        apply_protection(REAL_FS, scan, keep_recent=0, live=live)
    finally:
        release.set()
        holder.join(10)
    assert BUSY in scan.sessions[sid(1)].protected
    plan = make_plan(scan, older_than_s=86400, until_under=None, now=now)
    assert plan.candidates == []
    execute_plan(REAL_FS, plan, lambda *a, **k: None)
    assert foreign.exists()


@pytest.mark.parametrize("check_only", [True, False])
def test_a_reserve_locked_elsewhere_protects_its_session(tmp_path: Path, check_only: bool) -> None:
    d = tmp_path / "st"
    _build(d, "foreign")
    now = time.time()
    scan = scan_directory(REAL_FS, d, "ledger")
    live = liveness(REAL_FS, d, scan, now, None)  # nobody holds a lock yet
    assert sid(1) not in live
    other = os.open(next(d.glob("*.reserve")), os.O_RDONLY)
    try:
        fcntl.flock(other, fcntl.LOCK_EX)  # taken after the probe
        recover_directory(REAL_FS, scan, live, None, check_only=check_only)
        classify_damaged(REAL_FS, scan)
        apply_protection(REAL_FS, scan, keep_recent=0, live=live)
    finally:
        os.close(other)
    assert BUSY in scan.sessions[sid(1)].protected


# -- IB-LED-002 counts the .damaged originals a recovery would finish ----------------------


def _dry_run_stderr(state: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    monkeypatch.setenv("IRONBIRD_STATE_DIR", str(state))
    out, err = io.StringIO(), io.StringIO()
    console = Console(out, err)
    ledger = Ledger(tmp_path / "off", session_id="a" * 12, enabled=False)
    flags = prunecmd.PruneFlags(86400, None, 0, True, False, True)
    prunecmd.work("ledger", flags, None, None, ledger, console, verbose=False)
    console.close()
    return err.getvalue()


def test_led002_warning_counts_a_pending_damaged_original(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = tmp_path / "state"
    _build(state, "damaged_file_pending")
    err = _dry_run_stderr(state, tmp_path, monkeypatch)
    assert "IB-LED-002" in err
    assert "1 leftover reserve(s)" in err


def test_led002_warning_counts_a_reserve_that_is_also_a_damaged_original_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = tmp_path / "state"
    _build(state, "interrupted_setaside")
    err = _dry_run_stderr(state, tmp_path, monkeypatch)
    assert "1 leftover reserve(s)" in err
