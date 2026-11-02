"""Regression tests for the sixth round of PR #16 review findings on retention and prune: a
check-only pass counts a clean reserve at the size a recovery leaves it (so ``--until-under`` and
``IB-RET-005`` agree with a real prune), nothing is deleted from such a listing, a recovery whose
``utime`` fails goes on and says so, a part's access time is the original's from before the read,
and the ``IB-LED-002`` count leaves out the sessions another process holds."""

from __future__ import annotations

import errno
import fcntl
import io
import json
import os
import time
from pathlib import Path

import pytest

from ironbird.cli import prunecmd
from ironbird.cli.output import Console
from ironbird.errors import Code, IronbirdError
from ironbird.fsops import REAL_FS, FileSystem
from ironbird.ledger import Ledger
from ironbird.prune import Plan, execute_plan, list_lines, make_plan
from ironbird.retention import (
    DirScan,
    apply_protection,
    classify_damaged,
    full_scan,
    liveness,
    recover_directory,
    scan_directory,
)
from ironbird.units import MIB
from retutil import DAY, make_reserve, make_session, reserve_header, sid


def rec(s: int, n: int) -> str:
    return json.dumps({"session": sid(s), "n": n, "kind": "action", "event": "tx.command"}) + "\n"


def _clean(s: int) -> bytes:
    return (reserve_header(sid(s)) + rec(s, 10) + rec(s, 11)).encode()


def _damaged(s: int) -> bytes:
    return (reserve_header(sid(s)) + rec(s, 10)).encode() + b"\0" * 8 + b"junk"


# -- F1: check-only counts a clean reserve at the size a recovery leaves ------------------------


def _scan_both(
    tmp_path: Path, build: object, now: float, keep_recent: int
) -> tuple[DirScan, DirScan]:
    check_dir, real_dir = tmp_path / "check", tmp_path / "real"
    for d in (check_dir, real_dir):
        d.mkdir(mode=0o700)
        build(d, now)  # type: ignore[operator]
    checked = full_scan(
        REAL_FS,
        check_dir,
        "ledger",
        now=now,
        own_session=None,
        keep_recent=keep_recent,
        recover=False,
        check_only=True,
    )
    real = full_scan(
        REAL_FS, real_dir, "ledger", now=now, own_session=None, keep_recent=keep_recent
    )
    return checked, real


def _lines(plan: Plan, now: float) -> list[str]:
    return [line.replace(str(plan.scan.path), "<dir>") for line in list_lines(plan, now)]


def test_until_under_plans_the_same_with_a_crashed_sessions_full_size_reserve(
    tmp_path: Path,
) -> None:
    """A crashed session's 32 MiB preallocated reserve holds a few hundred bytes of records: the
    dry run must not count the tail, or it lists sessions a real prune does not need."""
    now = time.time()

    def build(d: Path, now: float) -> None:
        make_session(d, 1, age_days=3, size=2000, now=now)
        make_reserve(d, 1, 1, _clean(1), now=now, pad_to=32 * MIB)
        make_session(d, 2, age_days=2, size=1 * MIB, now=now)

    checked, real = _scan_both(tmp_path, build, now, keep_recent=0)
    assert checked.total_bytes() == real.total_bytes() < 2 * MIB
    a = make_plan(checked, older_than_s=None, until_under=2 * MIB, now=now)
    b = make_plan(real, older_than_s=None, until_under=2 * MIB, now=now)
    assert a.candidates == [] and b.candidates == []
    assert (a.unreachable, a.selected_bytes) == (b.unreachable, b.selected_bytes)
    assert _lines(a, now) == _lines(b, now)


def test_ret005_unreachable_agrees_with_a_real_prune(tmp_path: Path) -> None:
    now = time.time()

    def build(d: Path, now: float) -> None:
        make_session(d, 1, age_days=1, now=now)  # crashed, newest: kept by keep_recent=1
        make_reserve(d, 1, 1, _clean(1), age_days=1, pad_to=32 * MIB, now=now)
        make_session(d, 2, age_days=5, size=4 * MIB, now=now)

    checked, real = _scan_both(tmp_path, build, now, keep_recent=1)
    a = make_plan(checked, older_than_s=None, until_under=8 * MIB, now=now)
    b = make_plan(real, older_than_s=None, until_under=8 * MIB, now=now)
    assert a.unreachable is b.unreachable is False
    assert a.protected == b.protected
    assert _lines(a, now) == _lines(b, now)
    # a target the protected bytes cannot reach is unreachable in both
    a = make_plan(checked, older_than_s=None, until_under=1, now=now)
    b = make_plan(real, older_than_s=None, until_under=1, now=now)
    assert a.unreachable is b.unreachable is True
    assert a.protected == b.protected


def test_nothing_is_deleted_from_a_check_only_listing(tmp_path: Path) -> None:
    now = time.time()

    def build(d: Path, now: float) -> None:
        make_session(d, 1, age_days=30, now=now)
        make_reserve(d, 1, 1, _clean(1), age_days=30, now=now)

    checked, _ = _scan_both(tmp_path, build, now, keep_recent=0)
    assert checked.projected > 0
    plan = make_plan(checked, older_than_s=DAY, until_under=None, now=now)
    assert [c.session.id for c in plan.candidates] == [sid(1)]
    with pytest.raises(IronbirdError) as raised:
        execute_plan(REAL_FS, plan, lambda *a, **k: None)
    assert raised.value.code is Code.INTERNAL
    assert len(list((tmp_path / "check").iterdir())) == 2


# -- N1: a utime that fails does not stop the recovery ------------------------------------------


class _NoUtime(FileSystem):
    def utime_ns(self, fd: int, atime_ns: int, mtime_ns: int) -> None:
        raise OSError(errno.EPERM, os.strerror(errno.EPERM))


@pytest.mark.parametrize("case", ["clean", "damaged"])
def test_a_recovery_goes_on_when_the_times_cannot_be_restored(tmp_path: Path, case: str) -> None:
    d = tmp_path / "st"
    d.mkdir(mode=0o700)
    make_session(d, 1, age_days=30)
    reserve = make_reserve(d, 1, 1, _clean(1) if case == "clean" else _damaged(1), age_days=30)
    records: list[tuple[str, dict[str, object]]] = []
    start = time.time()
    scan = full_scan(
        _NoUtime(),
        d,
        "ledger",
        now=start,
        own_session=None,
        keep_recent=0,
        on_record=lambda event, fields: records.append((event, fields)),
    )
    assert scan.recovered == 1 and sid(1) not in scan.unrecoverable
    (part,) = d.glob("*.p0002.jsonl")
    assert part.stat().st_mtime >= start - 5  # looks fresh: protected, never wrongly old
    ((event, fields),) = records
    assert event == "ledger.recovered" and fields["new"] == part.name
    carrier = fields if case == "clean" else fields["copy"]
    assert isinstance(carrier, dict) and carrier["utime_error"] == os.strerror(errno.EPERM)
    assert case == "damaged" or not reserve.exists()
    assert scan.sessions[sid(1)].protected  # fresh (or kept, for the original): not listed


# -- N3: the part's access time is the original's from before the read --------------------------


@pytest.mark.parametrize("case", ["clean", "damaged"])
def test_a_recovered_part_keeps_the_original_access_time(tmp_path: Path, case: str) -> None:
    d = tmp_path / "st"
    d.mkdir(mode=0o700)
    make_session(d, 1, age_days=30)
    reserve = make_reserve(d, 1, 1, _clean(1) if case == "clean" else _damaged(1), age_days=30)
    old_atime = time.time() - 40 * DAY  # older than the mtime: a read would move it (relatime)
    os.utime(reserve, (old_atime, reserve.stat().st_mtime))
    before = reserve.stat()
    full_scan(REAL_FS, d, "ledger", now=time.time(), own_session=None, keep_recent=0)
    (part,) = d.glob("*.p0002.jsonl")
    assert part.stat().st_atime_ns == before.st_atime_ns
    assert part.stat().st_mtime_ns == before.st_mtime_ns


# -- N4: the IB-LED-002 count --------------------------------------------------------------------


def test_led002_count_leaves_out_a_session_another_process_holds(tmp_path: Path) -> None:
    d = tmp_path / "st"
    d.mkdir(mode=0o700)
    make_session(d, 1, age_days=3)
    make_reserve(d, 1, 1, _clean(1))
    make_session(d, 2, age_days=3)
    make_reserve(d, 2, 1, _clean(2))
    now = time.time()
    scan = scan_directory(REAL_FS, d, "ledger")
    live = liveness(REAL_FS, d, scan, now, None)  # nobody holds a lock yet
    other = os.open(next(d.glob(f"*{sid(1)}.r0001.reserve")), os.O_RDONLY)
    try:
        fcntl.flock(other, fcntl.LOCK_EX)  # taken after the probe
        recover_directory(REAL_FS, scan, live, None, check_only=True)
        classify_damaged(REAL_FS, scan)
        apply_protection(REAL_FS, scan, keep_recent=0, live=live)
    finally:
        os.close(other)
    assert scan.busy == {sid(1)}
    assert prunecmd._unrecovered_reserves(scan) == 1  # session 2's reserve alone


def test_led002_message_names_what_it_counts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = tmp_path / "state"
    state.mkdir(mode=0o700)
    make_session(state, 1, age_days=3)
    r = make_reserve(state, 1, 1, _damaged(1))
    r.rename(state / (r.name + ".damaged"))
    monkeypatch.setenv("IRONBIRD_STATE_DIR", str(state))
    out, err = io.StringIO(), io.StringIO()
    console = Console(out, err)
    ledger = Ledger(tmp_path / "off", session_id="a" * 12, enabled=False)
    flags = prunecmd.PruneFlags(86400, None, 0, True, False, True)
    prunecmd.work("ledger", flags, None, ledger, console, verbose=False)
    console.close()
    assert "IB-LED-002" in err.getvalue()
    assert "set-aside original(s)" in err.getvalue()
