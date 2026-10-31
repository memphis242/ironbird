"""Regression tests for the third round of PR #10 review findings on retention: a ``.damaged``
original is judged ``saved`` only when the comparison with the session's parts finished and
found a part; budget exhaustion, an unreadable candidate or too many candidates leave it
``unverified`` (protected, reported, never copied again)."""

from __future__ import annotations

import errno
import json
import os
import time
from pathlib import Path
from typing import Any

import pytest

from ironbird import retention
from ironbird.fsops import REAL_FS, FileSystem
from ironbird.retention import UNRECOVERABLE, DirScan, full_scan
from retutil import DAY, make_reserve, make_session, reserve_header, sid
from support.faultfs import FaultFs

NOW = time.time()


def rec(n: int) -> str:
    return json.dumps({"session": sid(1), "n": n, "kind": "action", "event": "tx.command"}) + "\n"


def scan(state: Path, fs: FileSystem = REAL_FS, **kw: Any) -> DirScan:
    kw.setdefault("keep_recent", 0)
    return full_scan(fs, state, "ledger", now=time.time(), own_session=None, **kw)


@pytest.fixture
def state(tmp_path: Path) -> Path:
    path = tmp_path / "st"
    path.mkdir(mode=0o700)
    make_session(path, 1, age_days=3, now=NOW)
    return path


def parts(state: Path) -> list[Path]:
    return sorted(p for p in state.iterdir() if ".p0002." in p.name and not p.name.startswith("."))


def salvaged(state: Path) -> tuple[Path, int, int]:
    """A damaged reserve salvaged by one scan: the ``.damaged`` original, its size, and the size
    of the saved prefix (the part's size)."""
    body = (reserve_header(sid(1)) + "".join(rec(10 + i) for i in range(20))).encode()
    body += b"\x00" * 64 + b"JUNK"
    reserve = make_reserve(state, 1, 1, body, now=NOW)
    first = scan(state)
    assert first.recovered == 1
    (part,) = parts(state)
    damaged = state / f"{reserve.name}.damaged"
    for path in (part, damaged):
        old = NOW - 3 * DAY
        os.utime(path, (old, old))
    return damaged, damaged.stat().st_size, part.stat().st_size


def with_budget(monkeypatch: pytest.MonkeyPatch, budget: int) -> None:
    monkeypatch.setattr(retention, "MAX_DAMAGED_CHECK_BYTES", budget)


def test_a_budget_spent_before_the_comparison_never_makes_a_second_part(
    state: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    damaged, size, _ = salvaged(state)
    with_budget(monkeypatch, size)  # reading the original uses it all
    for _ in range(3):
        result = scan(state)
        assert len(parts(state)) == 1 and result.recoveries == []
        assert UNRECOVERABLE in result.sessions[sid(1)].protected
        assert any(damaged.name in m and "could not be checked" in m for m in result.messages)


def test_the_budget_boundary_decides_unverified_or_saved_never_a_copy(
    state: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, size, cut = salvaged(state)
    with_budget(monkeypatch, size + cut - 1)  # one byte short of the comparison
    short = scan(state)
    assert len(parts(state)) == 1 and short.recoveries == []
    assert UNRECOVERABLE in short.sessions[sid(1)].protected
    with_budget(monkeypatch, size + cut)  # exactly enough
    enough = scan(state)
    assert len(parts(state)) == 1 and enough.recoveries == [] and enough.messages == []
    assert UNRECOVERABLE not in enough.sessions[sid(1)].protected


def test_a_budget_smaller_than_the_original_is_unverified_and_reads_no_more_than_it_has(
    state: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, size, _ = salvaged(state)
    with_budget(monkeypatch, size // 2)
    result = scan(state)
    assert len(parts(state)) == 1 and result.recoveries == []
    assert UNRECOVERABLE in result.sessions[sid(1)].protected
    assert result.check_budget >= 0  # nothing is read past what the budget allows


def test_an_unreadable_saved_part_is_unverified_not_a_reason_to_copy_again(state: Path) -> None:
    damaged, _, _ = salvaged(state)

    def on_part(fd: int, *args: Any) -> bool:
        try:
            return os.readlink(f"/proc/self/fd/{fd}").endswith(".p0002.jsonl")
        except OSError:
            return False

    fs = FaultFs()
    fs.fail("pread", errno.EIO, when=on_part)
    result = scan(state, fs)
    assert len(parts(state)) == 1 and result.recoveries == []
    assert UNRECOVERABLE in result.sessions[sid(1)].protected
    assert any(damaged.name in m for m in result.messages)
    again = scan(state)  # the disk is well again: the comparison finishes, nothing to report
    assert len(parts(state)) == 1 and again.messages == []


def test_a_candidate_that_cannot_be_opened_is_unverified(state: Path) -> None:
    damaged, _, _ = salvaged(state)
    (part,) = parts(state)
    fs = FaultFs()

    def after_the_original(_dir: int, name: str, *args: Any) -> bool:
        opens = [a[1] for n, a in fs.calls if n == "open_at"]
        return name == part.name and len(opens) > 1 and opens[-2] == damaged.name

    fs.fail("open_at", errno.EIO, when=after_the_original)
    result = scan(state, fs)
    assert len(parts(state)) == 1 and result.recoveries == []
    assert UNRECOVERABLE in result.sessions[sid(1)].protected


def test_more_candidates_than_are_compared_is_unverified(state: Path) -> None:
    _, _, cut = salvaged(state)
    (part,) = parts(state)
    # ten parts of the right size that are not the prefix, all numbered before the real one
    stamp_part = part.name.split(".p0002.")[0]
    for number in range(3, 3 + retention.MAX_PART_CANDIDATES + 2):
        other = state / f"{stamp_part}.p{number:04d}.jsonl"
        other.write_bytes(b"y" * cut)
        os.utime(other, (NOW - 3 * DAY, NOW - 3 * DAY))
    part.unlink()  # the real one is gone: the candidates cannot be exhausted
    result = scan(state)
    assert not any(p.stat().st_size == cut and p.read_bytes()[:1] == b"{" for p in parts(state))
    assert result.recoveries == []
    assert UNRECOVERABLE in result.sessions[sid(1)].protected
