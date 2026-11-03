"""Prune under file-system trouble: every failure while deleting is a recorded skip that leaves
the session's remaining files alone - never an exception, never a deletion of anything that was
not listed - and an unopenable directory skips the whole plan with a reason per session."""

from __future__ import annotations

import errno
import os
import time
from pathlib import Path
from typing import Any

import pytest

import retutil
from ironbird.fsops import REAL_FS
from ironbird.prune import Plan, directory_label, execute_plan, make_plan
from ironbird.retention import full_scan
from retutil import DAY, sid
from support.faultfs import FaultFs, Rule

NOW = time.time()


@pytest.fixture
def state(tmp_path: Path) -> Path:
    path = tmp_path / "st"
    path.mkdir(mode=0o700)
    return path


def session(state: Path, n: int, *, parts: int = 1) -> list[Path]:
    return retutil.make_session(state, n, age_days=30, parts=parts, now=NOW)


def plan_for(state: Path) -> Plan:
    scan = full_scan(REAL_FS, state, "ledger", now=NOW, own_session=None, keep_recent=0)
    return make_plan(scan, older_than_s=DAY, until_under=None, now=NOW)


def run(plan: Plan, fs: Any) -> tuple[Any, list[tuple[str, dict[str, Any]]]]:
    events: list[tuple[str, dict[str, Any]]] = []
    outcome = execute_plan(fs, plan, lambda event, **fields: events.append((event, fields)))
    return outcome, events


def test_a_directory_that_cannot_be_opened_skips_every_session_with_a_reason(state: Path) -> None:
    session(state, 1)
    session(state, 2)
    plan = plan_for(state)
    fs = FaultFs()
    fs.rules.append(Rule("open_dir", err=errno.EACCES))
    outcome, events = run(plan, fs)
    assert outcome.deleted_files == 0
    assert sorted(s for s, _ in outcome.skipped) == [sid(1), sid(2)]
    assert [e for e, _ in events] == ["prune.skip", "prune.skip"]
    assert len(os.listdir(state)) == 2


def test_part_1_that_cannot_be_opened_for_locking_skips_the_session_untouched(state: Path) -> None:
    session(state, 1, parts=2)
    plan = plan_for(state)
    fs = FaultFs()
    fs.rules.append(
        Rule("open_at", err=errno.EACCES, when=lambda _d, name, *_a: ".p0002" not in name)
    )
    outcome, _ = run(plan, fs)
    assert [r for _, r in outcome.skipped] == [
        f"part 1 cannot be opened ({os.strerror(errno.EACCES)})"
    ]
    assert len(os.listdir(state)) == 2


def test_a_file_that_cannot_be_examined_stops_the_session_before_any_deletion(state: Path) -> None:
    session(state, 1, parts=2)
    plan = plan_for(state)
    fs = FaultFs()
    fs.rules.append(Rule("stat_at", err=errno.EIO))
    outcome, _ = run(plan, fs)
    assert outcome.deleted_files == 0 and len(outcome.skipped) == 1
    assert "cannot be examined" in outcome.skipped[0][1]
    assert len(os.listdir(state)) == 2


def test_a_part_that_vanishes_at_the_moment_of_deletion_is_counted_as_gone(state: Path) -> None:
    paths = session(state, 1, parts=2)
    plan = plan_for(state)
    fs = FaultFs()

    fs.rules.append(Rule("unlink_at", exc=FileNotFoundError(errno.ENOENT, "gone"), times=1))
    outcome, events = run(plan, fs)
    assert outcome.gone_files == 1
    assert "prune.gone" in [e for e, _ in events]
    assert outcome.skipped == []
    assert paths[0].exists() is False


def test_a_second_prune_finding_the_session_already_deleted_counts_it_gone_not_an_error(
    state: Path,
) -> None:
    paths = session(state, 1, parts=2)
    plan = plan_for(state)
    for path in paths:
        path.unlink()
    outcome, events = run(plan, REAL_FS)
    assert outcome.skipped == [] and outcome.gone_files == 2
    assert [e for e, _ in events] == ["prune.gone", "prune.gone"]


def test_a_session_whose_part_1_is_gone_but_later_parts_remain_is_left_alone(state: Path) -> None:
    paths = session(state, 1, parts=3)
    plan = plan_for(state)
    paths[0].unlink()
    outcome, _ = run(plan, REAL_FS)
    assert [r for _, r in outcome.skipped] == [
        "part 1 is gone but other files of the session remain"
    ]
    assert paths[1].exists() and paths[2].exists()
    assert outcome.deleted_files == 0


def test_a_leftover_that_cannot_be_examined_counts_as_remaining(state: Path) -> None:
    paths = session(state, 1, parts=2)
    plan = plan_for(state)
    paths[0].unlink()
    fs = FaultFs()
    fs.rules.append(Rule("stat_at", err=errno.EIO))
    outcome, _ = run(plan, fs)
    assert len(outcome.skipped) == 1 and paths[1].exists()


def test_the_directory_label_is_the_plain_path(state: Path) -> None:
    assert directory_label(state) == str(state)


def test_remaining_bytes_is_what_a_prune_would_leave(state: Path) -> None:
    session(state, 1)
    retutil.make_session(state, 2, age_days=0.001, now=NOW)
    plan = plan_for(state)
    assert plan.remaining_bytes == plan.total_bytes - plan.selected_bytes > 0


# -- scanning when the directory misbehaves --


@pytest.mark.parametrize("method", ["open_at", "pread", "open_dir"])
def test_a_scan_over_a_failing_file_system_completes_and_never_selects_more_than_a_healthy_one(
    state: Path, method: str
) -> None:
    for n in range(1, 5):
        session(state, n, parts=2)
    healthy = {c.session.id for c in plan_for(state).candidates}
    fs = FaultFs()
    fs.rules.append(Rule(method, err=errno.EIO, skip=1 if method == "open_dir" else 0))
    scan = full_scan(fs, state, "ledger", now=NOW, own_session=None, keep_recent=0)
    faulted = {
        c.session.id
        for c in make_plan(scan, older_than_s=DAY, until_under=None, now=NOW).candidates
    }
    assert faulted <= healthy
    assert len(os.listdir(state)) == 8, "a scan deleted something"
