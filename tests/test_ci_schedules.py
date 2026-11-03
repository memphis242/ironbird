"""Each scheduled cron in the CI workflow drives exactly the jobs meant for it: the weekly audit
runs only the audit jobs, the nightly cron runs fuzzing on top of the ordinary jobs, and a push
or pull request runs neither. A cron string edited in ``on.schedule`` but not in a job's ``if``
would silently turn a job off (or on for every cron)."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

WORKFLOW = Path(__file__).resolve().parent.parent / ".github" / "workflows" / "ci.yml"
NIGHTLY_JOBS = {"fuzz"}
WEEKLY_JOBS = {"audit", "el8-audit"}


def _workflow() -> dict[Any, Any]:
    document = yaml.safe_load(WORKFLOW.read_text())
    assert isinstance(document, dict)
    return document


def _triggers() -> dict[Any, Any]:
    # PyYAML (YAML 1.1) reads the key `on` as the boolean True.
    triggers = _workflow().get("on", _workflow().get(True))
    assert isinstance(triggers, dict)
    return triggers


def _crons() -> list[str]:
    return [entry["cron"] for entry in _triggers()["schedule"]]


def _cron_of(jobs: set[str]) -> str:
    """The one cron that starts every job in ``jobs``, found by evaluation, not list order."""
    found = [c for c in _crons() if jobs <= _jobs_run(event="schedule", schedule=c)]
    assert len(found) == 1, (jobs, found)
    return found[0]


def _runs(condition: object, *, event: str, schedule: str) -> bool:
    """Evaluate the small ``if:`` grammar the workflow uses: ``a == 'x'``, ``a != 'x'``, ``&&``
    and ``||`` (``&&`` binds tighter, no parentheses)."""
    if condition is None:
        return True
    assert isinstance(condition, str)
    context = {"github.event_name": event, "github.event.schedule": schedule}
    body = condition.split("#", 1)[0]
    return any(_all_hold(group, context) for group in body.split("||"))


def _all_hold(group: str, context: dict[str, str]) -> bool:
    for clause in group.split("&&"):
        match = re.fullmatch(r"\s*([\w.]+)\s*(==|!=)\s*'([^']*)'\s*", clause)
        assert match is not None, f"unsupported if: clause {clause!r}"
        key, operator, literal = match.group(1), match.group(2), match.group(3)
        if (context[key] == literal) != (operator == "=="):
            return False
    return True


def _jobs_run(*, event: str, schedule: str) -> set[str]:
    jobs = _workflow()["jobs"]
    return {
        name for name, job in jobs.items() if _runs(job.get("if"), event=event, schedule=schedule)
    }


def test_there_are_two_distinct_crons() -> None:
    crons = _crons()
    assert len(crons) == 2
    assert len(set(crons)) == 2


def test_each_cron_runs_only_its_own_jobs() -> None:
    nightly, weekly = _cron_of(NIGHTLY_JOBS), _cron_of(WEEKLY_JOBS)
    assert nightly != weekly
    every = set(_workflow()["jobs"])
    ordinary = every - NIGHTLY_JOBS - WEEKLY_JOBS
    assert _jobs_run(event="schedule", schedule=nightly) == ordinary | NIGHTLY_JOBS
    assert _jobs_run(event="schedule", schedule=weekly) == WEEKLY_JOBS


def test_push_and_pull_request_run_neither_scheduled_family() -> None:
    every = set(_workflow()["jobs"])
    for event in ("push", "pull_request"):
        ran = _jobs_run(event=event, schedule="")
        assert ran == every - NIGHTLY_JOBS - WEEKLY_JOBS, event


def test_manual_dispatch_runs_the_audit_jobs_but_not_fuzzing() -> None:
    assert "workflow_dispatch" in _triggers()
    every = set(_workflow()["jobs"])
    assert _jobs_run(event="workflow_dispatch", schedule="") == every - NIGHTLY_JOBS
