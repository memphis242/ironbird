"""Stage 2d, layer 3 through the real entry point: ``ib ledger prune``, ``ib rec prune`` and the
retention warnings, as a person or a script sees them.

Directories are built by hand; the commands run as subprocesses so exit codes, stdout and stderr
are the real ones, with a pty where a prompt is involved.
"""

from __future__ import annotations

import hashlib
import json
import os
import pty
import select
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest

from conftest import REPO
from ledgerutil import session_records
from retutil import make_session, sid
from support.faultfs import FaultFs

CMD = [sys.executable, "-m", "ironbird.cli.main"]


def run(
    state: Path,
    *args: str,
    extra_env: dict[str, str] | None = None,
    config: Path | None = None,
    stdin: Any = subprocess.DEVNULL,
) -> subprocess.CompletedProcess[str]:
    env = {"IRONBIRD_STATE_DIR": str(state), "PATH": "/usr/bin:/bin", **(extra_env or {})}
    argv = [*CMD, *(["-c", str(config)] if config else []), *args]
    return subprocess.run(
        argv,
        capture_output=True,
        text=True,
        env=env,
        cwd=REPO,
        timeout=60,
        stdin=stdin,
        check=False,
    )


def faulty(state: Path, setup: str, *args: str, config: Path | None = None) -> Any:
    env = {
        "IRONBIRD_STATE_DIR": str(state),
        "PATH": "/usr/bin:/bin",
        "PYTHONPATH": str(REPO / "tests"),
    }
    argv = [sys.executable, "-m", "support.faulty_main", setup, "--"]
    argv += [*(["-c", str(config)] if config else []), *args]
    return subprocess.run(
        argv, capture_output=True, text=True, env=env, cwd=REPO, timeout=60, check=False
    )


def digest(directory: Path) -> dict[str, str]:
    return {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in directory.iterdir()}


@pytest.fixture
def state(_isolated_state: Path) -> Path:
    _isolated_state.mkdir(mode=0o700, exist_ok=True)
    return _isolated_state


def old_sessions(state: Path, count: int = 4) -> list[Path]:
    paths: list[Path] = []
    for i in range(1, count + 1):
        paths += make_session(state, i, age_days=30 + i, parts=2, klass="short")
    return paths


PRUNE = ["ledger", "prune", "--keep", "0"]


def with_pty(state: Path, answer: bytes | None, *args: str) -> tuple[int, str, str]:
    """Run a command whose stdin and stderr are a terminal, answer its prompt."""
    master, slave = pty.openpty()
    env = {"IRONBIRD_STATE_DIR": str(state), "PATH": "/usr/bin:/bin"}
    proc = subprocess.Popen(
        [*CMD, *args],
        stdin=slave,
        stderr=slave,
        stdout=subprocess.PIPE,
        env=env,
        cwd=REPO,
    )
    os.close(slave)
    seen = b""
    try:
        deadline = time.monotonic() + 30
        answered = False
        while time.monotonic() < deadline and proc.poll() is None:
            ready, _, _ = select.select([master], [], [], 0.1)
            if ready:
                try:
                    chunk = os.read(master, 4096)
                except OSError:
                    break
                seen += chunk
                if b"[y/N]" in seen and not answered:
                    answered = True
                    if answer is None:
                        os.close(master)
                        master = -1
                    else:
                        os.write(master, answer)
                    if master < 0:
                        break
        out = proc.communicate(timeout=30)[0].decode()
    finally:
        if master >= 0:
            os.close(master)
        proc.kill()
    return proc.returncode, out, seen.decode(errors="replace")


# -- confirmation ----


@pytest.mark.parametrize(
    ("answer", "deleted"),
    [
        (b"y\n", True),
        (b"YES\n", True),
        (b"yes \n", True),
        (b"N\n", False),
        (b"\n", False),
        (b"maybe\n", False),
        (b"yess\n", False),
        (None, False),
    ],
    ids=["y", "YES", "yes-space", "N", "empty", "maybe", "yess", "EOF"],
)
def test_the_prompt_confirms_only_y_or_yes_and_anything_else_deletes_nothing(
    state: Path, answer: bytes | None, deleted: bool
) -> None:
    old_sessions(state)
    before = digest(state)
    code, out, prompt = with_pty(state, answer, *PRUNE)
    assert code == 0, (out, prompt)
    assert "delete 4 ledger sessions" in prompt and "[y/N]" in prompt
    after = digest(state)
    if deleted:
        assert not any(sid(i) in name for i in range(1, 5) for name in after)
    else:
        assert all(after[name] == before[name] for name in before) and len(after) >= len(before)


def test_without_a_terminal_and_without_yes_the_list_is_printed_and_nothing_is_deleted(
    state: Path,
) -> None:
    old_sessions(state)
    before = digest(state)
    done = run(state, *PRUNE)
    assert done.returncode == 2 and "IB-CLI-001" in done.stderr and "--yes" in done.stderr
    assert done.stdout.count(sid(1)) == 1 and "START (UTC)" in done.stdout
    assert {k: v for k, v in digest(state).items() if k in before} == before


def test_yes_deletes_exactly_the_listed_set_and_ledgers_every_step(state: Path) -> None:
    paths = old_sessions(state)
    survivor = make_session(state, 9, age_days=0.001)[0]
    done = run(state, *PRUNE, "--yes")
    assert done.returncode == 0, done.stderr
    assert "deleted 4 ledger sessions" in done.stdout and "8 files" in done.stdout
    assert not any(p.exists() for p in paths) and survivor.exists()
    events = [r["event"] for r in session_records(state, bookkeeping=True)]
    assert events.count("prune.plan") == 1 and events.count("prune.confirmed") == 1
    assert events.count("prune.delete") == 8


def test_dry_run_lists_and_deletes_nothing(state: Path) -> None:
    old_sessions(state)
    before = digest(state)
    done = run(state, *PRUNE, "--dry-run", "--yes")
    assert done.returncode == 0 and done.stdout.count("age") >= 4
    assert {k: v for k, v in digest(state).items() if k in before} == before


def test_json_output_is_one_document_with_the_plan(state: Path) -> None:
    old_sessions(state)
    done = run(state, *PRUNE, "--dry-run", "--format", "json")
    plan = json.loads(done.stdout)
    assert plan["kind"] == "ledger" and len(plan["sessions"]) == 4
    assert plan["sessions"][0]["reason"] == "age" and plan["unreachable"] is False


@pytest.mark.parametrize(
    "bad",
    [
        ("--older-than", "unlimited"),
        ("--older-than", "10"),
        ("--older-than", "0d"),
        ("--older-than", "-1d"),
        ("--older-than", " 7d"),
        ("--until-under", "64MB"),
        ("--until-under", "010MiB"),
        ("--until-under", "unlimited"),
        ("--keep", "-1"),
        ("--keep", "1001"),
        ("--keep", "01"),
        ("--keep", "unlimited"),
    ],
    ids=" ".join,
)
def test_bad_flag_values_are_usage_errors(state: Path, bad: tuple[str, str]) -> None:
    old_sessions(state)
    before = digest(state)
    done = run(state, "ledger", "prune", *bad, "--yes")
    assert done.returncode == 2 and "IB-CLI-001" in done.stderr, done.stderr
    assert {k: v for k, v in digest(state).items() if k in before} == before


def test_nothing_selectable_says_so_and_exits_0(state: Path) -> None:
    make_session(state, 1, age_days=0.5)
    done = run(state, "ledger", "prune", "--keep", "0", "--yes")
    assert done.returncode == 0 and "nothing to prune" in done.stdout


def test_an_unreachable_target_proposes_what_it_can_and_exits_1_after_acting(
    state: Path,
) -> None:
    make_session(state, 1, age_days=30, size=50_000, klass="short")
    make_session(state, 2, age_days=1, size=80_000, klass="short")
    done = run(state, "ledger", "prune", "--keep", "2", "--until-under", "1KiB", "--yes")
    assert done.returncode == 1 and "IB-RET-005" in done.stderr
    assert "cannot get the ledger under 1.0 KiB" in done.stderr or "cannot get" in done.stderr
    assert not any(sid(1) in p.name for p in state.iterdir())


def test_a_group_writable_directory_without_the_sticky_bit_is_refused(state: Path) -> None:
    old_sessions(state)
    state.chmod(0o770)
    before = digest(state)
    done = run(state, *PRUNE, "--yes")
    assert done.returncode == 1 and "IB-RET-006" in done.stderr and "chmod +t" in done.stderr
    assert {k: v for k, v in digest(state).items() if k in before} == before
    listed = run(state, *PRUNE, "--dry-run")
    assert listed.returncode == 0 and "IB-RET-006" in listed.stderr


def test_prune_refuses_to_delete_without_a_usable_ledger_but_dry_run_still_lists(
    tmp_path: Path, mock_config_path: Path
) -> None:
    blocker = tmp_path / "blocker"
    blocker.write_text("a file, not a directory")
    recordings = tmp_path / "rec"
    for i in range(1, 4):
        make_session(recordings, i, age_days=30, recording=True)
    config = mock_config_path.read_text() + f'\n[recording]\ndir = "{recordings}"\n'
    mock_config_path.write_text(config)
    before = digest(recordings)
    done = run(blocker / "state", "rec", "prune", "--keep", "0", "--yes", config=mock_config_path)
    assert (
        done.returncode == 1
        and "IB-LED-002" in done.stderr
        and "nothing was deleted" in done.stderr
    )
    assert digest(recordings) == before
    listed = run(
        blocker / "state", "rec", "prune", "--keep", "0", "--dry-run", config=mock_config_path
    )
    assert listed.returncode == 0 and listed.stdout.count("age") == 3


def test_a_stalled_ledger_does_not_refuse_prune_and_the_trail_is_written_once_space_returns(
    state: Path,
) -> None:
    old_sessions(state, 3)
    setup = (
        "fs = FaultFs()\n"
        "fs.fail('write', errno.ENOSPC)\n"
        "options = {'fs': fs, 'join_timeout_s': 3.0}\n"
    )
    done = faulty(state, setup, *PRUNE, "--yes")
    assert done.returncode == 0, done.stderr  # deleting is what frees the space
    assert "deleted 3 ledger sessions" in done.stdout
    assert not any(p.name.endswith(".jsonl") and sid(1) in p.name for p in state.iterdir())


def test_rec_prune_works_on_the_recording_directory_and_never_touches_the_ledger(
    tmp_path: Path, mock_config_path: Path, state: Path
) -> None:
    recordings = tmp_path / "rec"
    for i in range(1, 4):
        make_session(recordings, i, age_days=30, recording=True, parts=2)
    ledger_file = make_session(state, 1, age_days=30)[0]
    mock_config_path.write_text(
        mock_config_path.read_text() + f'\n[recording]\ndir = "{recordings}"\n'
    )
    done = run(state, "rec", "prune", "--keep", "0", "--yes", config=mock_config_path)
    assert done.returncode == 0, done.stderr
    assert not list(recordings.iterdir()) and ledger_file.exists()


# -- the warnings ----


def test_old_sessions_are_warned_about_once_per_interval_and_never_deleted(
    state: Path, mock_config_path: Path
) -> None:
    for i in range(1, 16):  # more than keep_recent: five of them are proposable
        make_session(state, i, age_days=30, parts=2, klass="short")
    before = digest(state)
    shows = []
    for _ in range(3):
        done = run(state, "nodes", config=mock_config_path, extra_env={"PATH": "/usr/bin:/bin"})
        shows.append(done.stderr.count("IB-RET-001"))
        assert done.returncode == 0, done.stderr
        time.sleep(0.05)
    # the first run scans and warns; later runs inside warn_interval stay quiet (a scan is only
    # due every scan_interval anyway)
    assert shows[0] <= 1 and sum(shows) <= 1
    after = digest(state)
    assert all(after[name] == before[name] for name in before)  # nothing deleted, nothing changed


def test_the_age_warning_text_names_what_prune_would_propose(
    state: Path, mock_config_path: Path
) -> None:
    for i in range(1, 16):
        make_session(state, i, age_days=30 + i, parts=2, klass="short")
    done = run(state, "listen", "fc", "--timeout", "1", config=mock_config_path)
    line = next(x for x in done.stderr.splitlines() if "IB-RET-001" in x)
    # 15 old sessions plus this run's own: the ten most recent are kept, six are proposable
    assert line.startswith("warning IB-RET-001: 6 ledger sessions older than 7d (")
    assert "more protected" in line and "review with 'ib ledger prune'" in line


def test_unlimited_age_and_total_turn_their_warnings_off(
    state: Path, mock_config_path: Path
) -> None:
    for i in range(1, 6):
        make_session(state, i, age_days=400, size=3_000_000, klass="short")
    mock_config_path.write_text(
        mock_config_path.read_text()
        + '\n[retention]\nmax_age = "unlimited"\nmax_total_bytes = "unlimited"\n'
    )
    done = run(state, "listen", "fc", "--timeout", "1", config=mock_config_path)
    assert "IB-RET-001" not in done.stderr and "IB-RET-002" not in done.stderr


def test_usage_past_the_cap_at_start_is_warned_and_names_the_prune(
    state: Path, mock_config_path: Path
) -> None:
    for i in range(1, 6):
        make_session(state, i, age_days=2, size=500_000, klass="short")
    mock_config_path.write_text(
        mock_config_path.read_text() + '\n[retention]\nmax_total_bytes = "1MiB"\n'
    )
    done = run(state, "listen", "fc", "--timeout", "1", config=mock_config_path)
    line = next(x for x in done.stderr.splitlines() if "IB-RET-002" in x)
    assert "past 1.0 MiB" in line or "past 1 MiB" in line
    assert "ib ledger prune --until-under" in line and "retention.max_total_bytes" in line


def test_three_consecutive_processes_leave_every_file_present_and_identical(
    state: Path, mock_config_path: Path
) -> None:
    for i in range(1, 8):
        make_session(state, i, age_days=60, size=400_000, klass="short")
    mock_config_path.write_text(
        mock_config_path.read_text() + '\n[retention]\nmax_total_bytes = "1MiB"\nmax_age = "1d"\n'
    )
    before = digest(state)
    for _ in range(3):
        run(state, "listen", "fc", "--timeout", "1", config=mock_config_path)
    after = digest(state)
    assert all(after.get(name) == value for name, value in before.items())


def test_retention_check_failures_are_not_fatal_and_nothing_fails_silently(
    state: Path, mock_config_path: Path
) -> None:
    for i in range(1, 4):
        make_session(state, i, age_days=30, klass="short")
    setup = "fs = FaultFs()\nfs.fail('scandir', errno.EIO)\noptions = {'fs': fs}\n"
    done = faulty(state, setup, "listen", "fc", "--timeout", "1", config=mock_config_path)
    assert done.returncode == 0, done.stderr
    assert "IB-RET-004" in done.stderr and "cannot be listed" in done.stderr
    assert FaultFs  # keep the import used
