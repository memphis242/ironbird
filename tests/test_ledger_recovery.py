"""Stage 2d, layer 3: what a scan does with the reserve a crashed session left behind.

A leftover reserve of a non-live session is recovered - cut at its last complete record and
renamed to the session's next part - or, when a valid prefix is followed by damage, salvaged into
the next part with the damaged original kept byte-identical as ``.damaged``; anything else is left
untouched and reported, and its session is protected. Nothing is ever deleted.
"""

from __future__ import annotations

import errno
import fcntl
import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from conftest import REPO
from ironbird.fsops import REAL_FS
from ironbird.retention import UNRECOVERABLE, DirScan, full_scan
from retutil import DAY, lines_of, make_reserve, make_session, reserve_header, sid, stamp
from support.faultfs import FaultFs

NOW = time.time()


def make_reserve_at(*args: Any, **kwargs: Any) -> Path:
    return make_reserve(*args, now=NOW, **kwargs)


def rec(n: int, event: str = "x") -> str:
    return json.dumps({"session": sid(1), "n": n, "kind": "action", "event": event}) + "\n"


def scan(state: Path, fs: Any = REAL_FS, **kw: Any) -> DirScan:
    return full_scan(fs, state, "ledger", now=NOW, own_session=None, keep_recent=0, **kw)


@pytest.fixture
def state(tmp_path: Path) -> Path:
    path = tmp_path / "st"
    path.mkdir(mode=0o700)
    make_session(path, 1, age_days=3, now=NOW)  # part 1 of session 1, old enough to be idle
    return path


def names(state: Path) -> list[str]:
    return sorted(p.name for p in state.iterdir())


def snapshot(state: Path) -> dict[str, bytes]:
    return {p.name: p.read_bytes() for p in state.iterdir()}


def part(state: Path, number: int) -> Path:
    (found,) = [p for p in state.iterdir() if p.name.endswith(f".p{number:04d}.jsonl")]
    return found


# -- recovered ----


def test_a_reserve_written_into_then_crashed_is_cut_at_its_last_record_and_renamed(
    state: Path,
) -> None:
    body = (reserve_header(sid(1)) + rec(10) + rec(11)).encode() + b'{"ts":"partial'
    reserve = make_reserve_at(state, 1, 1, body)
    result = scan(state)
    assert not reserve.exists()
    recovered = part(state, 2)
    assert [r["n"] for r in lines_of(recovered)] == [9, 10, 11]  # every line parses
    assert recovered.read_bytes().endswith(b"\n") and b"\x00" not in recovered.read_bytes()
    assert result.recovered == 1 and result.messages == []
    (recovery,) = result.recoveries
    assert recovery["old"] == reserve.name and recovery["new"] == recovered.name
    assert (
        recovery["kept"] == len(reserve_header(sid(1)) + rec(10) + rec(11)) and recovery["cut"] > 0
    )
    assert scan(state).recovered == 0  # a second scan does nothing


def test_a_reserve_retired_but_not_renamed_has_no_nul_and_is_just_renamed(state: Path) -> None:
    body = (reserve_header(sid(1)) + rec(10)).encode()
    make_reserve_at(state, 1, 1, body, pad_to=len(body))
    scan(state)
    assert [r["n"] for r in lines_of(part(state, 2))] == [9, 10]


def test_a_header_only_reserve_becomes_a_part_holding_just_its_header(state: Path) -> None:
    make_reserve_at(state, 1, 1, reserve_header(sid(1)).encode())
    scan(state)
    assert [r["event"] for r in lines_of(part(state, 2))] == ["ledger.reserve"]


def test_two_reserves_of_one_session_are_recovered_in_order_into_consecutive_parts(
    state: Path,
) -> None:
    (state / f"{stamp(NOW - 3 * DAY)}-{sid(1)}.p0002.jsonl").write_text(rec(1))
    os.utime(state / f"{stamp(NOW - 3 * DAY)}-{sid(1)}.p0002.jsonl", (NOW - 3 * DAY, NOW - 3 * DAY))
    for number, marker in ((2, 20), (1, 10)):  # created out of order on purpose
        make_reserve_at(state, 1, number, (reserve_header(sid(1)) + rec(marker)).encode())
    scan(state)
    assert [r["n"] for r in lines_of(part(state, 3))][-1] == 10  # r0001 first
    assert [r["n"] for r in lines_of(part(state, 4))][-1] == 20
    assert not any(n.endswith(".reserve") for n in names(state))


def test_an_interrupted_rename_is_completed_leaving_one_name(state: Path) -> None:
    body = (reserve_header(sid(1)) + rec(10)).encode()
    reserve = make_reserve_at(state, 1, 1, body, pad_to=len(body))
    renamed = state / f"{stamp(NOW - 3 * DAY)}-{sid(1)}.p0002.jsonl"
    os.link(reserve, renamed)  # crashed between link and unlink
    result = scan(state)
    assert not reserve.exists() and renamed.read_bytes() == body
    assert result.recovered == 1


def scan_later(state: Path) -> DirScan:
    """A scan an hour on, when the files the last recovery wrote no longer make it live."""
    return full_scan(
        REAL_FS, state, "ledger", now=time.time() + 3600, own_session=None, keep_recent=0
    )


def _no_renameat2_and_unlink_fails(suffix: str) -> FaultFs:
    """A file system with no non-replacing rename (NFS), where unlinking a name ending in
    ``suffix`` fails: a hard-link commit then leaves its old name behind."""
    fs = FaultFs()
    fs.override("renameat2_noreplace", lambda _dir_fd, _old, _new: None)
    fs.fail("unlink_at", errno.EIO, when=lambda _dir_fd, name: name.endswith(suffix))
    return fs


def test_a_recovery_that_leaves_the_reserve_name_records_it_and_the_next_scan_removes_it(
    state: Path,
) -> None:
    body = (reserve_header(sid(1)) + rec(10)).encode()
    reserve = make_reserve_at(state, 1, 1, body, pad_to=len(body))
    result = scan(state, _no_renameat2_and_unlink_fails(".reserve"))
    recovered = part(state, 2)
    assert reserve.exists() and os.path.samefile(reserve, recovered)
    (recovery,) = result.recoveries
    assert recovery["new"] == recovered.name and recovery["leftover"] == reserve.name
    assert recovery["leftover_error"] and result.recovered == 1 and result.messages == []
    again = scan_later(state)
    assert not reserve.exists() and recovered.read_bytes() == body
    assert [r["new"] for r in again.recoveries] == [recovered.name]


def test_a_salvage_that_leaves_the_reserve_name_records_it_and_the_next_scan_removes_it(
    state: Path,
) -> None:
    """The set-aside to ``.damaged`` committed by hard link but the reserve name stayed: the next
    scan completes that rename, and does not salvage the file a second time."""
    original = (reserve_header(sid(1)) + rec(10)).encode() + b'{"partial\x00\x00junk\n'
    reserve = make_reserve_at(state, 1, 1, original, pad_to=len(original) + 100)
    damaged = state / f"{reserve.name}.damaged"
    result = scan(state, _no_renameat2_and_unlink_fails(".reserve"))
    assert reserve.exists() and os.path.samefile(reserve, damaged)
    assert [r["n"] for r in lines_of(part(state, 2))] == [9, 10]
    (recovery,) = result.recoveries
    assert recovery["damaged"] == damaged.name and recovery["leftover"] == reserve.name
    again = scan_later(state)
    assert not reserve.exists() and damaged.exists()
    assert not [p for p in state.iterdir() if p.name.endswith(".p0003.jsonl")]
    assert UNRECOVERABLE not in again.sessions[sid(1)].protected, again.messages
    (completed,) = again.recoveries
    assert completed["old"] == reserve.name and completed["damaged"] == damaged.name
    assert completed["new"] is None and again.recovered == 0


def test_a_salvage_whose_copy_leaves_its_temporary_name_says_so(state: Path) -> None:
    original = (reserve_header(sid(1)) + rec(10)).encode() + b'{"partial\x00\x00junk\n'
    make_reserve_at(state, 1, 1, original, pad_to=len(original) + 100)
    result = scan(state, _no_renameat2_and_unlink_fails(".tmp"))
    saved = part(state, 2)
    (temp,) = [p for p in state.iterdir() if p.name.endswith(".tmp")]
    assert os.path.samefile(temp, saved)
    (recovery,) = result.recoveries
    assert recovery["new"] == saved.name and recovery["copy"]["leftover"] == temp.name
    (message,) = result.messages  # one line for the file, not a second one from the accounting
    assert temp.name in message and "could not be removed" in message


# -- salvaged ----


@pytest.mark.parametrize(
    "damage",
    [b"\x00\x00\x00junk after the nul run\n", b'{"a"\x00"b"}\n', b'{"p1\x00\x00\x00{"p2'],
    ids=["non-nul-after-nul", "nul-inside-a-line", "two-partial-lines"],
)
def test_a_valid_prefix_followed_by_damage_is_salvaged_and_the_original_kept(
    state: Path, damage: bytes
) -> None:
    good = (reserve_header(sid(1)) + rec(10) + rec(11)).encode()
    original = good + b'{"partial' + damage
    reserve = make_reserve_at(state, 1, 1, original, pad_to=len(original) + 100)
    before = reserve.read_bytes()
    result = scan(state)
    damaged = state / f"{reserve.name}.damaged"
    assert damaged.read_bytes() == before and not reserve.exists()  # byte-identical, renamed
    assert [r["n"] for r in lines_of(part(state, 2))] == [9, 10, 11]
    (message,) = result.messages
    assert message.startswith(f"reserve {reserve.name} was damaged: ")
    assert f"the rest kept as {damaged.name}" in message and "inspect or delete" in message
    assert scan(state).recoveries == [] and damaged.exists()  # outside the patterns: ignored
    assert UNRECOVERABLE not in {r for s in result.sessions.values() for r in s.protected}


# -- left untouched, reported, session protected ----


def _untouched(state: Path, result: DirScan, *, message: str) -> None:
    assert result.messages and message in result.messages[0], result.messages
    assert "inspect it and move it away" in result.messages[0]
    assert UNRECOVERABLE in result.sessions[sid(1)].protected
    assert result.recovered == 0


def test_another_sessions_header_leaves_the_reserve_untouched(state: Path) -> None:
    make_reserve_at(state, 1, 1, (reserve_header(sid(2)) + rec(10)).encode())
    before = snapshot(state)
    result = scan(state)
    _untouched(state, result, message="header is another session's")
    assert snapshot(state) == before


@pytest.mark.parametrize(
    "body",
    [
        b"garbage with no newline",
        b"not json\n",
        b"[1,2]\n",
        b'{"event":"x"}\n',
        b"x" * 5000 + b"\n",
    ],
    ids=["no-newline", "not-json", "not-an-object", "wrong-event", "oversize-first-line"],
)
def test_no_valid_header_leaves_the_reserve_untouched(state: Path, body: bytes) -> None:
    make_reserve_at(state, 1, 1, body)
    before = snapshot(state)
    result = scan(state)
    _untouched(state, result, message="no valid ledger.reserve header")
    assert snapshot(state) == before


def test_a_header_with_no_record_and_damage_after_it_is_not_salvaged(state: Path) -> None:
    make_reserve_at(state, 1, 1, reserve_header(sid(1)).encode() + b"\x00\x00junk")
    before = snapshot(state)
    result = scan(state)
    _untouched(state, result, message="damaged, with no record after its header")
    assert snapshot(state) == before


def test_a_session_without_part_1_is_not_recovered(tmp_path: Path) -> None:
    state = tmp_path / "st"
    state.mkdir(mode=0o700)
    make_reserve_at(state, 1, 1, (reserve_header(sid(1)) + rec(10)).encode())
    before = snapshot(state)
    result = scan(state)
    assert "its session has no part 1" in result.messages[0]
    assert snapshot(state) == before


def test_a_foreign_file_at_the_next_part_name_stays_and_the_reserve_is_left(state: Path) -> None:
    make_reserve_at(state, 1, 1, (reserve_header(sid(1)) + rec(10)).encode())
    (part1,) = [p for p in state.iterdir() if p.name.endswith(f"-{sid(1)}.jsonl")]
    foreign = state / part1.name.replace(".jsonl", ".p0002.jsonl")
    fs = FaultFs()
    real_flock = REAL_FS.flock
    exclusive = {"calls": 0}

    def plant_after_the_probes(fd: int, op: int) -> None:
        # two liveness probes (part 1, the reserve), then the recovery locks part 1: by then the
        # listing is done, so a file that appears now is a name taken after the next number was
        # chosen
        real_flock(fd, op)
        if op & fcntl.LOCK_EX:
            exclusive["calls"] += 1
            if exclusive["calls"] == 4:
                foreign.write_bytes(b"not ours")
                os.utime(foreign, (NOW - 3 * DAY, NOW - 3 * DAY))

    fs.override("flock", plant_after_the_probes)
    result = scan(state, fs)
    assert foreign.read_bytes() == b"not ours"  # it stays byte-identical
    assert any(n.endswith(".reserve") for n in names(state))
    assert "already has the next part name" in result.messages[0]
    assert UNRECOVERABLE in result.sessions[sid(1)].protected


def test_a_lookalike_and_a_symlink_are_not_reserves(state: Path, tmp_path: Path) -> None:
    start = stamp(NOW - 3 * DAY)
    (state / f"{start}-{sid(1)}.r0001.reserve.bak").write_bytes(reserve_header(sid(1)).encode())
    target = tmp_path / "t"
    target.write_bytes((reserve_header(sid(1)) + rec(10)).encode())
    (state / f"{start}-{sid(1)}.r0002.reserve").symlink_to(target)
    before = snapshot(state)
    result = scan(state)
    assert result.recovered == 0 and result.messages == [] and result.foreign == 1
    assert snapshot(state) == before and target.read_bytes().startswith(b'{"session"')


def test_a_reserve_over_1_gib_is_left_alone_even_when_sparse(state: Path) -> None:
    reserve = make_reserve_at(state, 1, 1, reserve_header(sid(1)).encode(), pad_to=0)
    os.truncate(reserve, (1 << 30) + 4096)  # sparse
    os.utime(reserve, (NOW - 3 * DAY, NOW - 3 * DAY))
    result = scan(state)
    _untouched(state, result, message="over 1 GiB")
    assert reserve.exists() and reserve.stat().st_size == (1 << 30) + 4096


def test_a_foreign_owner_is_left_alone(state: Path) -> None:
    reserve = make_reserve_at(state, 1, 1, (reserve_header(sid(1)) + rec(10)).encode())
    fs = FaultFs()
    real_fstat = REAL_FS.fstat

    def other(fd: int) -> Any:
        st = real_fstat(fd)
        if st.st_ino == reserve.stat().st_ino:
            values = list(st)
            values[4] = os.getuid() + 1
            return os.stat_result(values)
        return st

    fs.override("fstat", other)
    result = scan(state, fs)
    assert result.foreign == 1 and reserve.exists()  # not even part of a session


def test_where_locks_are_unsupported_nothing_is_recovered_and_it_is_reported(state: Path) -> None:
    reserve = make_reserve_at(state, 1, 1, (reserve_header(sid(1)) + rec(10)).encode())
    fs = FaultFs()
    fs.fail("flock", errno.ENOLCK)
    before = snapshot(state)
    result = scan(state, fs)
    _untouched(state, result, message="file locks are unavailable here")
    assert snapshot(state) == before and reserve.exists() and result.unlocked


# -- not touched at all ----


def test_a_live_reserve_and_a_fresh_one_are_left_alone(state: Path) -> None:
    held = make_reserve_at(state, 1, 1, (reserve_header(sid(1)) + rec(10)).encode())
    fd = os.open(held, os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        result = scan(state)
    finally:
        os.close(fd)
    assert result.recovered == 0 and held.exists()
    fresh_dir = state.parent / "fresh"
    fresh_dir.mkdir(mode=0o700)
    make_session(fresh_dir, 1, age_days=0.0001, now=NOW)
    fresh = make_reserve_at(
        fresh_dir,
        1,
        1,
        (reserve_header(sid(1)) + rec(10)).encode(),
        age_days=0.0001,
        start_age_days=0.01,
    )
    assert scan(fresh_dir).recovered == 0 and fresh.exists()


def test_a_directory_writable_by_others_without_the_sticky_bit_recovers_nothing(
    state: Path,
) -> None:
    reserve = make_reserve_at(state, 1, 1, (reserve_header(sid(1)) + rec(10)).encode())
    state.chmod(0o770)
    result = scan(state)
    assert result.unsafe and result.recovered == 0 and reserve.exists()


def test_lock_held_on_part_1_at_recovery_skips_the_session_without_a_report(state: Path) -> None:
    reserve = make_reserve_at(state, 1, 1, (reserve_header(sid(1)) + rec(10)).encode())
    fs = FaultFs()
    # probes take and release the lock (two EX calls); the recovery's lock on part 1 is the third
    fs.fail(
        "flock", errno.EWOULDBLOCK, skip=2, times=1, when=lambda _fd, op: bool(op & fcntl.LOCK_EX)
    )
    result = scan(state, fs)
    assert result.messages == [] and result.recovered == 0 and reserve.exists()
    assert scan(state).recovered == 1  # the next scan recovers it


def test_two_scans_racing_on_one_session_recover_it_once(state: Path) -> None:
    make_reserve_at(state, 1, 1, (reserve_header(sid(1)) + rec(10)).encode())
    barrier = threading.Barrier(2)
    results: list[DirScan] = []

    def run() -> None:
        barrier.wait()
        results.append(scan(state))

    threads = [threading.Thread(target=run) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)
    assert sum(r.recovered for r in results) == 1
    assert len([n for n in names(state) if ".p0002." in n]) == 1
    assert not any(n.endswith(".reserve") for n in names(state))


# -- a real crash ----


def test_kill_9_of_a_long_session_leaves_a_reserve_the_next_scan_recovers(
    mock_config_path: Path, _isolated_state: Path
) -> None:
    env = {"IRONBIRD_STATE_DIR": str(_isolated_state), "PATH": "/usr/bin:/bin"}
    proc = subprocess.Popen(
        [sys.executable, "-m", "ironbird.cli.main", "-c", str(mock_config_path), "listen", "fc"],
        cwd=REPO,
        env=env,
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    try:
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if any(p.name.endswith(".reserve") for p in _isolated_state.glob("*")):
                break
            time.sleep(0.05)
        else:
            pytest.skip("this file system does not give a long session a reserve")
        time.sleep(0.5)
    finally:
        proc.send_signal(signal.SIGKILL)
        proc.wait(10)
    (reserve,) = [p for p in _isolated_state.iterdir() if p.name.endswith(".reserve")]
    session = reserve.name.split("-")[1][:12]
    old = NOW - 3 * DAY
    for p in _isolated_state.iterdir():  # the freshness window has passed
        os.utime(p, (old, old))
    result = scan(_isolated_state)
    assert result.recovered == 1 and not reserve.exists()
    parts = sorted(p for p in _isolated_state.iterdir() if p.name.endswith(".jsonl"))
    assert len(parts) == 2 and session in parts[0].name
    for path in parts:
        for line in path.read_bytes().decode("ascii").splitlines():
            json.loads(line)  # every line of the result parses


# -- the corpus ------------------------------------------------------------------------------------

RESERVE_OUTCOMES = {
    "led-reserve-valid-partial-tail": "recovered",
    "led-reserve-valid-retired": "recovered",
    "led-reserve-salvage-nul-mid-line": "salvaged",
    "led-reserve-salvage-after-nul-run": "salvaged",
    "led-reserve-salvage-two-partial-lines": "salvaged",
    "led-reserve-other-session-header": "untouched",
    "led-reserve-no-newline": "untouched",
    "led-reserve-oversize-first-line": "untouched",
    "led-reserve-header-then-junk": "untouched",
    "led-reserve-garbage": "untouched",
}


@pytest.mark.parametrize("name", sorted(RESERVE_OUTCOMES))
def test_the_reserve_corpus(state: Path, name: str) -> None:
    from conftest import CORPUS

    data = (CORPUS / f"{name}.bin").read_bytes()
    reserve = make_reserve_at(state, 1, 1, data, pad_to=0)
    before = reserve.read_bytes()
    result = scan(state)
    outcome = RESERVE_OUTCOMES[name]
    if outcome == "untouched":
        assert reserve.read_bytes() == before and result.recovered == 0
        assert UNRECOVERABLE in result.sessions[sid(1)].protected
    else:
        assert not reserve.exists() and result.recovered == 1
        for line in part(state, 2).read_bytes().decode("ascii").splitlines():
            json.loads(line)  # whatever was kept parses
        assert (state / f"{reserve.name}.damaged").exists() == (outcome == "salvaged")
        if outcome == "salvaged":
            assert (state / f"{reserve.name}.damaged").read_bytes() == before
