"""``ib ledger prune`` and ``ib rec prune``: the only code in ironbird that deletes a ledger part,
a reserve or a recording, and only after an explicit confirmation.

A prune proposes sessions of *one* directory - non-protected ones older than an age, plus (with
``--until-under``) the oldest until the directory is at or under a size - lists exactly what it
would delete, and deletes exactly that. Per session it takes ``LOCK_EX | LOCK_NB`` on part 1,
checks that every listed file still matches the listing (device, inode, size, mtime, one link,
owner) with ``fstat`` relative to the directory descriptor and no symbolic link followed, then
``unlink``\\s relative to that descriptor, higher parts first and part 1 last, under the lock. A
session that became live or changed since the listing is skipped and reported.
"""

from __future__ import annotations

import errno
import fcntl
import os
import stat
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final

from ironbird.fsops import INSPECT_FLAGS, FileSystem, close_quietly
from ironbird.retention import (
    DirScan,
    FileEntry,
    Session,
    describe_reasons,
    protected_bytes_by_reason,
)
from ironbird.units import format_duration, format_size

LIST_HEADER: Final = "START (UTC)        SESSION       PARTS  SIZE       AGE     REASON"


@dataclass(frozen=True, slots=True)
class Candidate:
    session: Session
    reason: str  # "age" | "size"


@dataclass(slots=True)
class Plan:
    scan: DirScan
    candidates: list[Candidate]
    total_bytes: int
    selected_bytes: int
    target: int | None = None
    unreachable: bool = False
    protected: dict[str, int] = field(default_factory=dict)

    @property
    def remaining_bytes(self) -> int:
        return self.total_bytes - self.selected_bytes


def make_plan(
    scan: DirScan, *, older_than_s: float | None, until_under: int | None, now: float
) -> Plan:
    """Select what a prune would delete from ``scan`` (whose protections are already decided).

    Never a protected session. ``older_than_s`` ``None`` selects no session by age.
    ``until_under`` adds the oldest non-protected sessions until the directory would be at or
    under that many bytes; when even that cannot reach it, ``unreachable`` is set.
    """
    selectable = [s for s in scan.ordered() if not s.protected]
    chosen: dict[str, str] = {}
    if older_than_s is not None:
        for session in selectable:
            if now - session.newest_mtime > older_than_s:
                chosen[session.id] = "age"
    total = scan.total_bytes()
    plan = Plan(scan, [], total, 0, until_under)

    def selected_size() -> int:
        return _bytes_of([s for s in selectable if s.id in chosen])

    if until_under is not None:
        for session in sorted(selectable, key=lambda s: s.newest_mtime):
            if total - selected_size() <= until_under:
                break
            chosen.setdefault(session.id, "size")
        plan.unreachable = total - selected_size() > until_under
    ordered = [s for s in sorted(selectable, key=lambda s: s.newest_mtime) if s.id in chosen]
    plan.candidates = [Candidate(s, chosen[s.id]) for s in ordered]
    plan.selected_bytes = selected_size()
    plan.protected = protected_bytes_by_reason(scan)
    return plan


def _bytes_of(sessions: list[Session]) -> int:
    seen: set[tuple[int, int]] = set()
    total = 0
    for session in sessions:
        for f in session.files:
            if (f.dev, f.ino) not in seen:
                seen.add((f.dev, f.ino))
                total += f.size
    return total


def list_lines(plan: Plan, now: float) -> list[str]:
    """One line per session to delete, then the total and the protected bytes by reason."""
    lines = [LIST_HEADER] if plan.candidates else []
    for cand in plan.candidates:
        s = cand.session
        start = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(s.start))
        age = format_duration(max(0.0, now - s.newest_mtime))
        lines.append(
            f"{start}  {s.id}  {s.parts:>5}  {format_size(s.size):<9}  {age:<6}  {cand.reason}"
        )
    kind = "ledger sessions" if plan.scan.kind == "ledger" else "recordings"
    lines.append(
        f"{len(plan.candidates)} {kind} to delete, {format_size(plan.selected_bytes)} of "
        f"{format_size(plan.total_bytes)} in {plan.scan.path}"
    )
    if plan.protected:
        lines.append(f"protected: {describe_reasons(plan.protected)}")
    return lines


def unreachable_summary(plan: Plan) -> str:
    assert plan.target is not None
    what = "recordings" if plan.scan.kind == "recording" else "the ledger"
    return (
        f"cannot get {what} under {format_size(plan.target)}: "
        f"{format_size(sum(plan.protected.values()))} protected "
        f"({describe_reasons(plan.protected)})"
    )


@dataclass(slots=True)
class Outcome:
    deleted_files: int = 0
    deleted_bytes: int = 0
    deleted_sessions: int = 0
    gone_files: int = 0
    skipped: list[tuple[str, str]] = field(default_factory=list)  # (session id, reason)


def _matches(entry: FileEntry, st: os.stat_result) -> bool:
    return (
        stat.S_ISREG(st.st_mode)
        and st.st_uid == os.getuid()
        and (st.st_dev, st.st_ino, st.st_size, st.st_mtime, st.st_nlink)
        == (entry.dev, entry.ino, entry.size, entry.mtime, 1)
        and entry.nlink == 1
    )


def execute_plan(
    fs: FileSystem,
    plan: Plan,
    record: Callable[..., Any],
) -> Outcome:
    """Delete exactly the listed sessions, or skip (and report) the ones that changed.

    ``record(event, **fields)`` writes the ledger trail: one ``prune.delete`` per file,
    ``prune.gone`` for a name already absent, ``prune.skip`` with the reason.
    """
    outcome = Outcome()
    try:
        dir_fd = fs.open_dir(plan.scan.path)
    except OSError as exc:
        for cand in plan.candidates:
            outcome.skipped.append(
                (cand.session.id, f"the directory cannot be opened ({exc.strerror})")
            )
            record("prune.skip", target=cand.session.id, reason=str(exc))
        return outcome
    try:
        for cand in plan.candidates:
            _delete_session(fs, dir_fd, cand.session, outcome, record)
    finally:
        close_quietly(fs, dir_fd)
    return outcome


def _delete_session(
    fs: FileSystem, dir_fd: int, session: Session, outcome: Outcome, record: Callable[..., Any]
) -> None:
    def skip(reason: str) -> None:
        outcome.skipped.append((session.id, reason))
        record("prune.skip", target=session.id, reason=reason)

    part1 = session.part1()
    if part1 is None:
        skip("it has no part 1")  # protected as incomplete; a plan never includes it
        return
    try:
        lock_fd = fs.open_at(dir_fd, part1.name, INSPECT_FLAGS)
    except FileNotFoundError:
        _already_gone(fs, dir_fd, session, outcome, record, skip)
        return
    except OSError as exc:
        skip(f"part 1 cannot be opened ({exc.strerror})")
        return
    try:
        try:
            fs.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in (errno.EWOULDBLOCK, errno.EAGAIN):
                skip("it became live (its lock is held)")
                return
            # Locks unsupported: freshness and the fstat checks below are what is left.
        files = sorted(session.files, key=lambda f: (f.number == 1 and not f.is_reserve, -f.number))
        present: list[FileEntry] = []
        for entry in files:
            try:
                st = fs.stat_at(dir_fd, entry.name)
            except FileNotFoundError:
                outcome.gone_files += 1
                record("prune.gone", target=session.id, name=entry.name)
                continue
            except OSError as exc:
                skip(f"{entry.name} cannot be examined ({exc.strerror})")
                return
            if not _matches(entry, st):
                skip(f"{entry.name} changed since it was listed")
                return
            present.append(entry)
        for entry in present:
            try:
                fs.unlink_at(dir_fd, entry.name)
            except FileNotFoundError:
                outcome.gone_files += 1
                record("prune.gone", target=session.id, name=entry.name)
                continue
            except OSError as exc:
                skip(f"{entry.name} could not be deleted ({exc.strerror})")
                return
            outcome.deleted_files += 1
            outcome.deleted_bytes += entry.size
            record("prune.delete", target=session.id, name=entry.name, bytes=entry.size)
        outcome.deleted_sessions += 1
    finally:
        close_quietly(fs, lock_fd)


def _already_gone(
    fs: FileSystem,
    dir_fd: int,
    session: Session,
    outcome: Outcome,
    record: Callable[..., Any],
    skip: Callable[[str], None],
) -> None:
    """Part 1 is not there: another prune (or the user) got to the session first. When none of
    its listed files remains it is simply gone - counted, not an error. A file that does remain
    belongs to a session that is no longer complete, which prune leaves alone."""
    remaining = 0
    for entry in session.files:
        try:
            fs.stat_at(dir_fd, entry.name)
        except FileNotFoundError:
            outcome.gone_files += 1
            record("prune.gone", target=session.id, name=entry.name)
        except OSError:
            remaining += 1
        else:
            remaining += 1
    if remaining:
        skip("part 1 is gone but other files of the session remain")


def interactive() -> bool:
    """Whether a person can be asked: stdin and stderr are terminals."""
    try:
        return os.isatty(0) and os.isatty(2)
    except OSError:
        return False


def confirmed(answer: str) -> bool:
    """Only ``y`` or ``yes`` in any case confirms; anything else is a no."""
    return answer.strip().lower() in ("y", "yes")


def directory_label(path: Path) -> str:
    return str(path)
