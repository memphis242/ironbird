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


def _crons() -> list[str]:
    # PyYAML (YAML 1.1) reads the key `on` as the boolean True.
    triggers = _workflow().get("on", _workflow().get(True))
    assert isinstance(triggers, dict)
    return [entry["cron"] for entry in triggers["schedule"]]


def _runs(condition: object, *, event: str, schedule: str) -> bool:
    """Evaluate the small ``if:`` grammar the workflow uses: ``a == 'x'``, ``a != 'x'``, ``&&``."""
    if condition is None:
        return True
    assert isinstance(condition, str)
    context = {"github.event_name": event, "github.event.schedule": schedule}
    for clause in condition.split("&&"):
        match = re.fullmatch(r"\s*([\w.]+)\s*(==|!=)\s*'([^']*)'\s*(#.*)?", clause)
        assert match is not None, f"unsupported if: clause {clause!r}"
        key, operator, literal = match.group(1), match.group(2), match.group(3)
        equal = context[key] == literal
        if equal != (operator == "=="):
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
    nightly, weekly = _crons()
    every = set(_workflow()["jobs"])
    ordinary = every - NIGHTLY_JOBS - WEEKLY_JOBS
    assert _jobs_run(event="schedule", schedule=nightly) == ordinary | NIGHTLY_JOBS
    assert _jobs_run(event="schedule", schedule=weekly) == WEEKLY_JOBS


def test_push_and_pull_request_run_neither_scheduled_family() -> None:
    every = set(_workflow()["jobs"])
    for event in ("push", "pull_request"):
        ran = _jobs_run(event=event, schedule="")
        assert ran == every - NIGHTLY_JOBS - WEEKLY_JOBS, event
