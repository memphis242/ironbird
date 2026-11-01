"""Regression tests for the fifth round of PR #16 review findings on retention and prune: a
recovery keeps the age of the file it cuts or copies, a check-only pass and a real recovery agree
on the sessions to list (but for one accepted divergence), only prune's no-ledger dry run checks
(a short command's scan only looks), a lock held elsewhere protects its session for the pass, and
the ``IB-LED-002`` warning counts the ``.damaged`` originals a recovery would finish."""

from __future__ import annotations

import errno
import fcntl
import io
import json
import os
import threading
import time
from pathlib import Path

import pytest

from ironbird import retention
from ironbird.cli import prunecmd
from ironbird.cli.output import Console
from ironbird.config import RetentionConfig
from ironbird.fsops import REAL_FS, FileSystem
from ironbird.ledger import Ledger
from ironbird.prune import Plan, execute_plan, list_lines, make_plan
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
from ironbird.units import MIB
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
    elif case == "until_under":
        # a crashed session's 32 MiB preallocated reserve, the oldest, and a clean newer session
        make_reserve(d, 1, 1, _clean(1), now=NOW, pad_to=32 * MIB)
        make_session(d, 2, age_days=2, size=1 * MIB, now=NOW)
    elif case == "until_under_damaged":
        make_reserve(d, 1, 1, _damaged(1), now=NOW, pad_to=32 * MIB)
        make_session(d, 2, age_days=2, size=1 * MIB, now=NOW)
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
    "until_under",
    "until_under_damaged",
]
# Check-only protects an interrupted rename (a reserve name that is a second name of a part) as
# hard-linked, while a real recovery unlinks the reserve name and the session becomes prunable:
# the divergence is on the safe side, and accepted (docs/plan.md, PR #16 review).
ACCEPTED_DIVERGENCE = frozenset({"interrupted_rename"})
# The same cause where the session is protected either way: the protected-bytes line says
# "hard-linked" for the check-only pass and "damaged reserve kept" after a real recovery.
HARD_LINK_LABEL = frozenset({"interrupted_setaside"})


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


def _listing(plan: Plan, now: float) -> list[str]:
    """What ``ib ledger prune`` prints for a plan, without the directory's name."""
    return [line.replace(str(plan.scan.path), "<dir>") for line in list_lines(plan, now)]


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
    # the figures agree too: a check-only pass counts a clean reserve at the size a recovery
    # leaves it, so a size-driven plan, the totals and the protected-bytes line are the same
    assert checked.total_bytes() == real.total_bytes(), report
    for until in (1, 2 * MIB, 8 * MIB):
        a = make_plan(checked, older_than_s=None, until_under=until, now=now)
        b = make_plan(real, older_than_s=None, until_under=until, now=now)
        assert [c.session.id for c in a.candidates] == [c.session.id for c in b.candidates], report
        assert (a.total_bytes, a.selected_bytes, a.unreachable) == (
            b.total_bytes,
            b.selected_bytes,
            b.unreachable,
        ), report
        if case in HARD_LINK_LABEL:
            assert sum(a.protected.values()) == sum(b.protected.values()), report
            continue
        assert a.protected == b.protected, report
        assert _listing(a, now) == _listing(b, now), report


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
    """Counts the locks taken on, and the bytes read from, reserve files and the ``.damaged``
    originals of salvaged ones (the liveness probe's locks on parts are not a recovery's)."""

    def __init__(self) -> None:
        self.flocks = 0
        self.reads = 0  # bytes read from a reserve or a .damaged original
        self.damaged_reads = 0  # of those, from a .damaged original

    @staticmethod
    def _target(fd: int) -> str:
        return os.readlink(f"/proc/self/fd/{fd}").split(" (deleted)")[0]

    @classmethod
    def _is_reserve(cls, fd: int) -> bool:
        return cls._target(fd).endswith((".reserve", ".reserve.damaged"))

    def flock(self, fd: int, operation: int) -> None:
        self.flocks += self._is_reserve(fd)
        super().flock(fd, operation)

    def pread(self, fd: int, length: int, offset: int) -> bytes:
        data = super().pread(fd, length, offset)
        if self._is_reserve(fd):
            self.reads += len(data)
            self.damaged_reads += len(data) * self._target(fd).endswith(".damaged")
        return data


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


def test_a_scan_that_only_looks_reads_damaged_originals_within_its_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No reserve a writer may hold is read, but each salvaged original is checked: a 4 KiB head
    and then at most ``MAX_DAMAGED_CHECK_BYTES`` for the whole scan."""
    budget = 1 * MIB
    monkeypatch.setattr(retention, "MAX_DAMAGED_CHECK_BYTES", budget)
    d = tmp_path / "st"
    d.mkdir(mode=0o700)
    for s in (1, 2, 3):
        make_session(d, s, age_days=3, now=NOW)
        body = (reserve_header(sid(s)) + "".join(rec(s, n) for n in range(5000))).encode()
        body += b"\0" * 8 + b"junk"  # about 650 KiB of records, then damage
        r = make_reserve(d, s, 1, body, now=NOW)
        r.rename(d / (r.name + ".damaged"))
    make_session(d, 4, age_days=3, now=NOW)
    make_reserve(d, 4, 1, _clean(4), now=NOW, pad_to=3 * MIB)  # a clean reserve
    fs = _CountingFs()
    probe = _CountingFs()
    now = time.time()
    liveness(probe, d, scan_directory(probe, d, "ledger"), now, None)
    scan = full_scan(fs, d, "ledger", now=now, own_session=None, keep_recent=0, recover=False)
    assert fs.flocks == probe.flocks
    assert fs.reads == fs.damaged_reads, "a reserve (not a .damaged original) was read"
    assert 0 < fs.damaged_reads <= budget + 3 * (retention.HEAD_BYTES + 1)
    assert scan.recoveries == [] and len(list(d.glob("*.reserve"))) == 1
    assert scan.check_budget <= 0  # the budget really was the limit here


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


# -- a pending .damaged original is projected only where a recovery would resume it --------------


class _NoLocksFs(FileSystem):
    def flock(self, fd: int, operation: int) -> None:
        raise OSError(errno.ENOLCK, os.strerror(errno.ENOLCK))


def _pending_damaged_bytes(s: int) -> bytes:
    pad = "x" * 1000
    records = "".join(
        json.dumps({"session": sid(s), "n": 10 + i, "kind": "action", "event": "t", "p": pad})
        + "\n"
        for i in range(2048)
    )
    return (reserve_header(sid(s)) + records).encode() + b"\0" * 8 + b"junk"


def _build_pending(d: Path, now: float, scenario: str) -> Path:
    """Session 1 (the oldest) holds a pending ``.damaged`` original of about 2 MiB; sessions 2 and
    3 are plain. Returns session 1's part 1."""
    d.mkdir(mode=0o700)
    (part1,) = make_session(d, 1, age_days=30, now=now)
    original = make_reserve(d, 1, 1, _pending_damaged_bytes(1), age_days=30, now=now, pad_to=0)
    original.rename(d / (original.name + ".damaged"))
    make_session(d, 2, age_days=20, size=1 * MIB, now=now)
    make_session(d, 3, age_days=10, size=1 * MIB, now=now)
    if scenario == "future":
        os.utime(part1, (now + 3600, now + 3600))
    elif scenario == "no_part1":
        part1.unlink()
    return part1


def _staged_scan(
    fs: FileSystem, d: Path, now: float, *, check_only: bool, lock_after_probe: Path | None
) -> DirScan:
    """``full_scan`` with the lock of ``lock_after_probe`` taken between the liveness probe and
    the recovery: a writer that started after the probe."""
    scan = scan_directory(fs, d, "ledger")
    live = liveness(fs, d, scan, now, None)
    holder = -1
    if lock_after_probe is not None:
        holder = os.open(lock_after_probe, os.O_RDONLY)
        fcntl.flock(holder, fcntl.LOCK_EX)
    try:
        if recover_directory(fs, scan, live, None, check_only=check_only):
            again = scan_directory(fs, d, "ledger")
            scan.carry_to(again)
            scan = again
            live = liveness(fs, d, scan, now, None)
    finally:
        if holder >= 0:
            os.close(holder)
    scan.check_only = check_only
    classify_damaged(fs, scan)
    apply_protection(fs, scan, keep_recent=0, live=live)
    return scan


PENDING_SCENARIOS = [
    "resumable",
    "future",
    "no_part1",
    "enolck",
    "held_at_probe",
    "busy_after_probe",
]


@pytest.mark.parametrize("scenario", PENDING_SCENARIOS)
def test_a_pending_damaged_original_is_projected_only_where_a_recovery_resumes_it(
    tmp_path: Path, scenario: str
) -> None:
    now = time.time()
    scans: list[DirScan] = []
    holders: list[int] = []
    try:
        for name in ("check", "real"):
            d = tmp_path / name
            part1 = _build_pending(d, now, scenario)
            fs: FileSystem = _NoLocksFs() if scenario == "enolck" else REAL_FS
            if scenario == "held_at_probe":
                holder = os.open(part1, os.O_RDONLY)
                fcntl.flock(holder, fcntl.LOCK_EX)
                holders.append(holder)
            scans.append(
                _staged_scan(
                    fs,
                    d,
                    now,
                    check_only=name == "check",
                    lock_after_probe=part1 if scenario == "busy_after_probe" else None,
                )
            )
    finally:
        for holder in holders:
            os.close(holder)
    checked, real = scans
    report = f"{scenario}: check {_protected(checked)} real {_protected(real)}"
    assert _protected(checked) == _protected(real), report
    assert _candidates(checked, now) == _candidates(real, now), report
    assert checked.total_bytes() == real.total_bytes(), report
    for until in (1, 1 * MIB, 3 * MIB, 5 * MIB):
        a = make_plan(checked, older_than_s=None, until_under=until, now=now)
        b = make_plan(real, older_than_s=None, until_under=until, now=now)
        assert [c.session.id for c in a.candidates] == [c.session.id for c in b.candidates], report
        assert (a.total_bytes, a.selected_bytes, a.unreachable) == (
            b.total_bytes,
            b.selected_bytes,
            b.unreachable,
        ), report  # IB-RET-005 is shown for an unreachable target
        assert a.protected == b.protected, report
        assert _listing(a, now) == _listing(b, now), report
    if scenario == "resumable":
        assert checked.projected == 1  # the one place the projection is a prediction
    else:
        assert checked.projected == 0, report


def _prune_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, scenario: str, *, ledger_enabled: bool
) -> tuple[list[str], int, bool, dict[str, int]]:
    name = "on" if ledger_enabled else "off"
    state = tmp_path / name / "state"
    state.parent.mkdir()
    _build_pending(state, time.time(), scenario)
    monkeypatch.setenv("IRONBIRD_STATE_DIR", str(state))
    monkeypatch.setenv("IRONBIRD_RECORDING_DIR", str(tmp_path / name / "rec"))
    out, err = io.StringIO(), io.StringIO()
    console = Console(out, err)
    ledger = Ledger(tmp_path / f"ledger-{name}", session_id="a" * 12, enabled=ledger_enabled)
    flags = prunecmd.PruneFlags(365 * 86400, 5 * MIB, 0, True, False, True)
    prunecmd.work("ledger", flags, None, None, ledger, console, verbose=False)
    console.close()
    plan = json.loads(out.getvalue().splitlines()[-1])
    return (
        [s["session"] for s in plan["sessions"]],
        plan["total_bytes"],
        plan["unreachable"],
        plan["protected_bytes"],
    )


@pytest.mark.parametrize("scenario", ["future", "no_part1"])
def test_a_dry_run_without_a_ledger_lists_what_one_with_a_ledger_lists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, scenario: str
) -> None:
    off = _prune_json(tmp_path, monkeypatch, scenario, ledger_enabled=False)
    on = _prune_json(tmp_path, monkeypatch, scenario, ledger_enabled=True)
    assert off == on


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
    assert "1 leftover reserve(s) or set-aside original(s)" in err


def test_led002_warning_counts_a_reserve_that_is_also_a_damaged_original_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = tmp_path / "state"
    _build(state, "interrupted_setaside")
    err = _dry_run_stderr(state, tmp_path, monkeypatch)
    assert "1 leftover reserve(s) or set-aside original(s)" in err


def test_led002_count_includes_the_reserves_passed_before_a_lock_held_elsewhere(
    tmp_path: Path,
) -> None:
    """The lock is taken after the liveness probe (a probe would call the session live), so the
    check-only recovery passes the first reserve and stops at the second."""
    d = tmp_path / "state"
    d.mkdir(mode=0o700)
    make_session(d, 1, age_days=3, now=NOW)
    make_reserve(d, 1, 1, _clean(1), now=NOW)
    second = make_reserve(d, 1, 2, _clean(1), now=NOW)
    scan = _staged_scan(REAL_FS, d, NOW, check_only=True, lock_after_probe=second)
    assert BUSY in scan.sessions[sid(1)].protected
    assert prunecmd._unrecovered_reserves(scan) == 1
