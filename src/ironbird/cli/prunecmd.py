"""``ib ledger prune`` and ``ib rec prune``: list, confirm, delete exactly what was listed.

The only deletion in ironbird. Nothing is deleted without an explicit confirmation: ``y`` or
``yes`` at an interactive prompt, or ``--yes`` when no person is there to ask; a decline, an end of
input or any other answer deletes nothing (exit 0). A prune needs a ledger (the deletion must
leave a trace), so with ``IB-LED-002`` it refuses to delete; ``--dry-run`` still lists.
"""

from __future__ import annotations

import json
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

from ironbird.cli.output import Console
from ironbird.config import RecordingConfig, RetentionConfig, SystemConfig
from ironbird.errors import Code, IronbirdError
from ironbird.fsops import REAL_FS, FileSystem
from ironbird.ledger import Ledger, default_state_dir
from ironbird.prune import (
    Plan,
    confirmed,
    execute_plan,
    interactive,
    list_lines,
    make_plan,
    unreachable_summary,
)
from ironbird.recording.session import default_recording_dir
from ironbird.render import term
from ironbird.retention import DirScan, full_scan, recording_blocks
from ironbird.units import format_size, parse_count, parse_duration, parse_size

MAX_SKIPS_SHOWN: Final = 5


@dataclass(frozen=True, slots=True)
class PruneFlags:
    older_than_s: int | None
    until_under: int | None
    keep: int | None
    dry_run: bool
    yes: bool
    json: bool


def parse_flags(
    older_than: str | None,
    until_under: str | None,
    keep: str | None,
    *,
    dry_run: bool,
    yes: bool,
    fmt_json: bool,
) -> PruneFlags:
    """The flags in the config grammars (never ``"unlimited"``); anything else is ``IB-CLI-001``."""

    def bad(flag: str, value: str, forms: str) -> IronbirdError:
        return IronbirdError(
            Code.CLI_USAGE, f"{flag} {term(value, 40, err=True)!r} must be {forms}"
        )

    age = size = count = None
    if older_than is not None:
        age = parse_duration(older_than)
        if age is None:
            raise bad("--older-than", older_than, "a duration such as '90s', '10min', '24h', '7d'")
    if until_under is not None:
        size = parse_size(until_under)
        if size is None:
            raise bad("--until-under", until_under, "a size such as '512MiB' or '1GiB'")
    if keep is not None:
        count = parse_count(keep)
        if count is None:
            raise bad("--keep", keep, "a whole number from 0 to 1000")
    return PruneFlags(age, size, count, dry_run, yes, fmt_json)


def _directory(kind: str, config: SystemConfig | None) -> Path:
    if kind == "ledger":
        return default_state_dir()
    return default_recording_dir(config.recording if config is not None else RecordingConfig())


def _json_plan(plan: Plan, now: float) -> dict[str, Any]:
    return {
        "directory": str(plan.scan.path),
        "kind": plan.scan.kind,
        "total_bytes": plan.total_bytes,
        "selected_bytes": plan.selected_bytes,
        "target_bytes": plan.target,
        "unreachable": plan.unreachable,
        "protected_bytes": plan.protected,
        "sessions": [
            {
                "session": c.session.id,
                "start": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(c.session.start)),
                "parts": c.session.parts,
                "bytes": c.session.size,
                "age_s": max(0, int(now - c.session.newest_mtime)),
                "reason": c.reason,
                "files": [f.name for f in c.session.files],
            }
            for c in plan.candidates
        ],
    }


def work(
    kind: str,
    flags: PruneFlags,
    config: SystemConfig | None,
    config_error: IronbirdError | None,
    ledger: Ledger,
    console: Console,
    *,
    verbose: bool,
    fs: FileSystem = REAL_FS,
    ask: Callable[[str], str] | None = None,
) -> int:
    """Run one prune; returns the exit code (0, 1 for a failure or an unreachable target, 2
    for a usage error)."""
    assert kind in ("ledger", "recording")
    if config_error is not None:
        console.err(term(config_error.user_line(), 1000, err=True))
        return config_error.exit_code
    retention: RetentionConfig = config.retention if config is not None else RetentionConfig()
    try:
        directory = _directory(kind, config)
        recordings_dir = _directory("recording", config) if kind == "ledger" else None
    except IronbirdError as exc:
        console.err(term(exc.user_line(), 1000, err=True))
        return exc.exit_code
    if not ledger.enabled and not flags.dry_run:
        console.err(
            "error IB-LED-002: refusing to delete: the action ledger is required and cannot be "
            "used; nothing was deleted (--dry-run still lists)"
        )
        return 1
    now = time.time()
    older = flags.older_than_s if flags.older_than_s is not None else retention.max_age_s
    keep = flags.keep if flags.keep is not None else retention.keep_recent
    recordings: DirScan | None = None
    if recordings_dir is not None:
        recordings = full_scan(
            fs, recordings_dir, "recording", now=now, own_session=None, keep_recent=keep
        )
    scan = full_scan(
        fs,
        directory,
        kind,
        now=now,
        own_session=ledger.session_id if kind == "ledger" else None,
        keep_recent=keep,
        has_recording=recording_blocks(recordings, now, older),
    )
    for recovery in scan.recoveries:
        ledger.record("state", "ledger.recovered", **recovery)
    for incomplete in scan.incompletes:
        ledger.record("state", "ledger.recovery_incomplete", **incomplete)
    for message in scan.messages:
        console.err(f"warning {Code.RET_SCAN.value}: {term(message, 1000, err=True)}")
    if scan.partial:
        console.err(f"warning {Code.RET_SCAN.value}: {term(scan.partial, 1000, err=True)}")
    plan = make_plan(scan, older_than_s=older, until_under=flags.until_under, now=now)
    ledger.record(
        "action",
        "prune.plan",
        directory=str(directory),
        what=kind,
        older_than_s=older,
        until_under=flags.until_under,
        keep=keep,
        dry_run=flags.dry_run,
        sessions=[
            {"session": c.session.id, "reason": c.reason, "bytes": c.session.size}
            for c in plan.candidates
        ],
    )
    if flags.json:
        console.out(json.dumps(_json_plan(plan, now)))
    else:
        for line in list_lines(plan, now):
            console.out(term(line, 400))
    unreachable = plan.unreachable
    if unreachable:
        console.err(
            f"warning {Code.RET_TARGET.value}: {term(unreachable_summary(plan), 1000, err=True)}"
        )
    if scan.unsafe:
        console.err(
            f"error {Code.RET_UNSAFE.value}: {term(str(directory), 400, err=True)} is writable by "
            "others without the sticky bit, so another user could swap names under a prune; "
            f"run 'chmod +t {term(str(directory), 400, err=True)}' or "
            f"'chmod go-w {term(str(directory), 400, err=True)}'"
            + (" (nothing deleted)" if not flags.dry_run else " (a real prune would be refused)")
        )
        if not flags.dry_run:
            return 1
    if flags.dry_run:
        return 0
    if not plan.candidates:
        if not flags.json:
            console.out("nothing to prune")
        return 1 if unreachable else 0
    return _confirm_and_delete(kind, plan, flags, ledger, console, fs, ask, unreachable)


def _confirm_and_delete(
    kind: str,
    plan: Plan,
    flags: PruneFlags,
    ledger: Ledger,
    console: Console,
    fs: FileSystem,
    ask: Callable[[str], str] | None,
    unreachable: bool,
) -> int:
    what = "ledger sessions" if kind == "ledger" else "recordings"
    question = f"delete {len(plan.candidates)} {what} ({format_size(plan.selected_bytes)})? [y/N] "
    if flags.yes:
        answer = "yes"
    elif ask is not None or interactive():
        answer = (ask or _ask)(question)
    else:
        console.err(
            f"error {Code.CLI_USAGE.value}: nothing was deleted: this is not a terminal, so "
            "pass --yes to confirm the list above"
        )
        return 2
    if not confirmed(answer):
        ledger.record("action", "prune.declined", directory=str(plan.scan.path))
        console.err("declined: nothing was deleted")
        return 0
    ledger.record("action", "prune.confirmed", directory=str(plan.scan.path), by_flag=flags.yes)

    def record(event: str, **fields: Any) -> None:
        ledger.record("action", event, **fields)

    outcome = execute_plan(fs, plan, record)
    summary = (
        f"deleted {outcome.deleted_sessions} {what} ({format_size(outcome.deleted_bytes)}, "
        f"{outcome.deleted_files} files)"
    )
    if outcome.gone_files:
        summary += f"; {outcome.gone_files} files were already gone"
    console.out(summary)
    for session, reason in outcome.skipped[:MAX_SKIPS_SHOWN]:
        console.err(
            f"warning {Code.RET_PRUNE.value}: session {term(session, 40, err=True)} not deleted: "
            f"{term(reason, 300, err=True)}"
        )
    if len(outcome.skipped) > MAX_SKIPS_SHOWN:
        console.err(
            f"warning {Code.RET_PRUNE.value}: {len(outcome.skipped) - MAX_SKIPS_SHOWN} more "
            "sessions not deleted (see the ledger)"
        )
    return 1 if outcome.skipped or unreachable else 0


def _ask(question: str) -> str:
    sys.stderr.write(question)
    sys.stderr.flush()
    try:
        return sys.stdin.readline()
    except (OSError, ValueError):
        return ""
