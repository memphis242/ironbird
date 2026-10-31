"""Helpers that build state and recording directories for the retention tests."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

DAY = 86400.0


def stamp(ts: float) -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(ts))


def sid(n: int) -> str:
    return f"{n:012x}"


def head(
    session: str, klass: str = "long", *, stage1: bool = False, command: str = "listen"
) -> str:
    if stage1:
        record = {
            "session": session,
            "n": 1,
            "kind": "action",
            "event": "cli.invoke",
            "command": command,
        }
    else:
        record = {
            "session": session,
            "n": 0,
            "kind": "state",
            "event": "ledger.open",
            "class": klass,
        }
    return json.dumps(record) + "\n"


def make_session(
    directory: Path,
    n: int,
    *,
    age_days: float,
    parts: int = 1,
    size: int = 2000,
    klass: str = "long",
    stage1: bool = False,
    command: str = "listen",
    recording: bool = False,
    mtime_offset: float = 0.0,
    now: float | None = None,
) -> list[Path]:
    """Create one session's files; the newest mtime is ``age_days`` ago (plus the offset)."""
    now = time.time() if now is None else now
    start = now - age_days * DAY
    name_id = sid(n)
    paths: list[Path] = []
    directory.mkdir(parents=True, exist_ok=True)
    for part in range(1, parts + 1):
        if recording:
            suffix = ".ibrec" if part == 1 else f".p{part:04d}.ibrec"
            body = b"\x89IBR\r\n\x1a\n" + b"R" * size
        else:
            suffix = ".jsonl" if part == 1 else f".p{part:04d}.jsonl"
            first = head(name_id, klass, stage1=stage1, command=command) if part == 1 else "{}\n"
            body = (first + "x" * size + "\n").encode()
        path = directory / f"{stamp(start)}-{name_id}{suffix}"
        path.write_bytes(body)
        path.chmod(0o600)
        os.utime(path, (start + mtime_offset, start + mtime_offset))
        paths.append(path)
    return paths


def reserve_header(session: str, name: str = "r") -> str:
    return (
        json.dumps(
            {
                "session": session,
                "n": 9,
                "kind": "state",
                "event": "ledger.reserve",
                "reserve": name,
            }
        )
        + "\n"
    )


def make_reserve(
    directory: Path,
    n: int,
    number: int,
    body: bytes,
    *,
    age_days: float = 3.0,
    start_age_days: float | None = None,
    header_session: str | None = None,
    pad_to: int = 4096,
    now: float | None = None,
) -> Path:
    """A leftover ``.rNNNN.reserve``: ``body`` (the header and records, then maybe damage) padded
    with NULs to ``pad_to``."""
    now = time.time() if now is None else now
    start = now - (start_age_days if start_age_days is not None else age_days) * DAY
    path = directory / f"{stamp(start)}-{sid(n)}.r{number:04d}.reserve"
    data = body + b"\x00" * max(0, pad_to - len(body))
    path.write_bytes(data)
    path.chmod(0o600)
    mtime = now - age_days * DAY
    os.utime(path, (mtime, mtime))
    return path


def lines_of(path: Path) -> list[dict[str, object]]:
    return [json.loads(x) for x in path.read_bytes().decode("ascii").splitlines()]
