"""``ib export`` through the worker pool, end to end: the same files and messages as one process,
the ``--jobs`` option and the ``[export]`` table, and what happens to the workers and the
temporary output when a signal comes, a worker dies, or the parent is killed."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest

from exportutil import hk_and_status
from poolutil import clean_recording
from recutil import CLI_MAIN, write_config
from replayutil import child_env

# Named semaphores in /dev/shm belong to whichever process made them: they can only be told apart
# when no other test runs beside this one.
QUIET = "PYTEST_XDIST_WORKER" not in os.environ
POOL_TABLE = "[export]\npool_min_bytes = 0\nrange_bytes = 4096\n"


@pytest.fixture
def env(tmp_path: Path) -> dict[str, str]:
    for name in ("state", "recordings"):
        (tmp_path / name).mkdir(mode=0o700)
    return child_env(tmp_path)


@pytest.fixture
def config(tmp_path: Path) -> Path:
    path, _, _ = write_config(tmp_path, POOL_TABLE)
    return path


@pytest.fixture
def big(tmp_path: Path) -> Path:
    return clean_recording(tmp_path / "big.ibrec", 3000)


def ib(
    tmp_path: Path, env: dict[str, str], config: Path | None, *args: str
) -> subprocess.CompletedProcess[str]:
    head = ["--config", str(config)] if config is not None else []
    return subprocess.run(
        [sys.executable, "-c", CLI_MAIN, *head, "export", *args],
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
        check=False,
        cwd=tmp_path,
    )


def ledger_events(tmp_path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for path in sorted((tmp_path / "state").glob("*.jsonl")):
        out += [json.loads(line) for line in path.read_text().splitlines()]
    return out


def tree(path: Path) -> dict[str, bytes]:
    """The files under ``path`` by name (a lone file: under the empty name)."""
    if path.is_dir():
        return {p.name: p.read_bytes() for p in sorted(path.iterdir())}
    return {"": path.read_bytes()}


# -- the same output ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "args",
    [
        [],
        ["--format", "text"],
        ["--format", "jsonl", "--where", "mid == 0x890"],
        ["--message", "MOCK_HK", "--header-fields", "--raw-enums"],
    ],
    ids=["csv-dir", "text", "jsonl-where", "one-table"],
)
def test_the_cli_writes_the_same_files_with_workers(
    tmp_path: Path, env: dict[str, str], config: Path, big: Path, args: list[str]
) -> None:
    single = ib(tmp_path, env, config, str(big), "-o", str(tmp_path / "one"), "--jobs", "1", *args)
    pooled = ib(tmp_path, env, config, str(big), "-o", str(tmp_path / "many"), "--jobs", "3", *args)
    assert single.returncode == pooled.returncode == 0, pooled.stderr
    assert tree(tmp_path / "one") == tree(tmp_path / "many")
    assert single.stderr.replace("one", "X") == pooled.stderr.replace("many", "X")
    events = [e for e in ledger_events(tmp_path) if e["event"] == "export.pool"]
    assert len(events) == 1 and events[0]["workers"] == 3 and events[0]["ranges"] > 10
    assert [p.name for p in tmp_path.iterdir() if p.name.endswith(".tmp")] == []


def test_the_current_configurations_definitions_are_used_by_the_workers_too(
    tmp_path: Path, env: dict[str, str], config: Path, big: Path
) -> None:
    common = [str(big), "--defs", "config", "--format", "jsonl"]
    single = ib(tmp_path, env, config, *common, "-o", str(tmp_path / "one"), "--jobs", "1")
    pooled = ib(tmp_path, env, config, *common, "-o", str(tmp_path / "many"), "--jobs", "3")
    assert single.returncode == pooled.returncode == 0, pooled.stderr
    assert (tmp_path / "one").read_bytes() == (tmp_path / "many").read_bytes()
    pool = [e for e in ledger_events(tmp_path) if e["event"] == "export.pool"]
    assert len(pool) == 1 and pool[0]["ranges"] > 10


def test_stdout_is_the_same_with_workers(
    tmp_path: Path, env: dict[str, str], config: Path, big: Path
) -> None:
    for args in (["--format", "text"], ["--message", "MOCK_HK"]):
        single = ib(tmp_path, env, config, str(big), "--jobs", "1", *args)
        pooled = ib(tmp_path, env, config, str(big), "--jobs", "3", *args)
        assert single.returncode == pooled.returncode == 0, pooled.stderr
        assert single.stdout == pooled.stdout and len(single.stdout.splitlines()) > 2000
        assert single.stderr == pooled.stderr


def test_a_damaged_recording_ends_the_same_way_with_workers(
    tmp_path: Path, env: dict[str, str], config: Path, big: Path
) -> None:
    data = bytearray(big.read_bytes())
    at = len(data) // 2
    data[at : at + 200] = bytes(200)
    damaged = tmp_path / "damaged.ibrec"
    damaged.write_bytes(bytes(data))
    for extra in ([], ["--skip-damage"]):
        common = [str(damaged), "--format", "text", *extra]
        single = ib(tmp_path, env, config, *common, "-o", str(tmp_path / "a.txt"), "--jobs", "1")
        pooled = ib(tmp_path, env, config, *common, "-o", str(tmp_path / "b.txt"), "--jobs", "3")
        assert single.returncode == pooled.returncode == 5, pooled.stderr
        assert (tmp_path / "a.txt").read_bytes() == (tmp_path / "b.txt").read_bytes()
        assert single.stderr.replace("a.txt", "X") == pooled.stderr.replace("b.txt", "X")
        (tmp_path / "a.txt").unlink()
        (tmp_path / "b.txt").unlink()


# -- when the workers are used ------------------------------------------------------------------


def test_a_small_recording_stays_in_one_process_by_default(
    tmp_path: Path, env: dict[str, str]
) -> None:
    small = hk_and_status(tmp_path / "small.ibrec", 50, 10)
    base, _, _ = write_config(tmp_path, "")
    done = ib(tmp_path, env, base, str(small), "-o", str(tmp_path / "out"))
    assert done.returncode == 0, done.stderr
    assert [e for e in ledger_events(tmp_path) if e["event"] == "export.pool"] == []


def test_jobs_one_never_starts_workers_and_the_table_sets_the_default(
    tmp_path: Path, env: dict[str, str], big: Path
) -> None:
    config, _, _ = write_config(tmp_path, POOL_TABLE + "jobs = 2\n")
    one = ib(tmp_path, env, config, str(big), "-o", str(tmp_path / "one"), "--jobs", "1")
    assert one.returncode == 0, one.stderr
    assert [e for e in ledger_events(tmp_path) if e["event"] == "export.pool"] == []
    two = ib(tmp_path, env, config, str(big), "-o", str(tmp_path / "two"))
    assert two.returncode == 0, two.stderr
    events = [e for e in ledger_events(tmp_path) if e["event"] == "export.pool"]
    assert [e["workers"] for e in events] == [2]
    start = [e for e in ledger_events(tmp_path) if e["event"] == "export.start"]
    assert sorted(e["jobs"] for e in start) == [1, 2]


@pytest.mark.parametrize("jobs", ["0", "65", "-1", "many"])
def test_a_bad_jobs_option_is_a_usage_error(
    tmp_path: Path, env: dict[str, str], config: Path, big: Path, jobs: str
) -> None:
    done = ib(tmp_path, env, config, str(big), "-o", str(tmp_path / "out"), "--jobs", jobs)
    assert done.returncode == 2 and "Traceback" not in done.stderr
    assert not (tmp_path / "out").exists()


# -- signals, a dead worker, a dead parent ------------------------------------------------------

# Child-side hook: the parent stops at the first range it writes, with the workers up and the
# temporary output made, until the test lets it go (or a signal comes: the wait is the one every
# wait of the tool is, so a signal is acted on in it).
_STALL = """
import os
from pathlib import Path
from ironbird import sigmask
from ironbird.recording import pexport
MARK, GO = Path(os.environ["IB_TEST_MARK"]), Path(os.environ["IB_TEST_GO"])
_write = pexport._Runner._write

def write(self, outcome, reader, path):
    if not MARK.exists():
        MARK.write_text(str(os.getpid()))
        while not GO.exists():
            sigmask.sleep(0.02)
    return _write(self, outcome, reader, path)

pexport._Runner._write = write
"""


def workers_of(pid: int) -> list[int]:
    """The worker processes (not the resource tracker) that are children of ``pid``."""
    found = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            stat = (entry / "stat").read_text()
            command = (entry / "cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace")
        except OSError:
            continue
        parent = int(stat.rsplit(")", 1)[1].split()[1])
        if parent == pid and "multiprocessing" in command and "resource_tracker" not in command:
            found.append(int(entry.name))
    return found


def alive(pid: int) -> bool:
    try:
        text = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return False
    return text.rsplit(")", 1)[1].split()[0] != "Z"


def wait_until(predicate: Any, timeout: float = 20.0, what: str = "") -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, f"timed out waiting for {what}"
        time.sleep(0.02)


def semaphores() -> set[str]:
    shm = Path("/dev/shm")
    return {p.name for p in shm.glob("sem.mp-*")} if shm.is_dir() else set()


class Stalled:
    """An export of a large recording with the parent held at its first write."""

    def __init__(
        self,
        tmp_path: Path,
        env: dict[str, str],
        config: Path,
        source: Path,
        fmt: str,
        *,
        new_session: bool = False,
    ) -> None:
        self.out = tmp_path / "out"
        self.mark, self.go = tmp_path / "mark", tmp_path / "go"
        self.semaphores = semaphores()
        self.proc = subprocess.Popen(
            [
                sys.executable,
                "-c",
                _STALL + CLI_MAIN,
                "--config",
                str(config),
                "export",
                str(source),
                "--format",
                fmt,
                "-o",
                str(self.out),
                "--jobs",
                "3",
            ],
            env={**env, "IB_TEST_MARK": str(self.mark), "IB_TEST_GO": str(self.go)},
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=new_session,
            cwd=tmp_path,
        )
        wait_until(lambda: self.mark.exists() or self.proc.poll() is not None, what="the stall")
        assert self.proc.poll() is None, "the export ended before the stall"
        wait_until(lambda: len(workers_of(self.proc.pid)) >= 2, what="the workers")
        self.workers = workers_of(self.proc.pid)
        self.tmp_path = tmp_path

    def temporaries(self) -> list[str]:
        return sorted(p.name for p in self.tmp_path.iterdir() if p.name.endswith(".tmp"))

    def finish(self) -> str:
        try:
            _, err = self.proc.communicate(timeout=60)
        finally:
            if self.proc.poll() is None:
                self.proc.kill()
        return err

    def assert_clean(self) -> None:
        """No output under its name, no temporary, no worker, no semaphore left behind."""
        assert not self.out.exists() and self.temporaries() == []
        wait_until(lambda: not any(alive(p) for p in self.workers), 10, "the workers to end")
        if QUIET:
            wait_until(lambda: semaphores() <= self.semaphores, 10, "the semaphores to go")


@pytest.mark.parametrize("fmt", ["text", "csv"])
@pytest.mark.parametrize(
    ("sig", "code"), [(signal.SIGINT, 130), (signal.SIGTERM, 143), (signal.SIGHUP, 129)]
)
def test_a_signal_ends_the_export_and_its_workers_without_publishing(
    tmp_path: Path,
    env: dict[str, str],
    config: Path,
    big: Path,
    sig: signal.Signals,
    code: int,
    fmt: str,
) -> None:
    run = Stalled(tmp_path, env, config, big, fmt)
    assert run.temporaries(), "the temporary output is made before the first write"
    run.proc.send_signal(sig)
    err = run.finish()
    assert run.proc.returncode == code, err
    assert "Traceback" not in err and "leaked" not in err
    run.assert_clean()


def test_ctrl_c_at_a_terminal_reaches_every_process_and_only_the_parent_acts(
    tmp_path: Path, env: dict[str, str], config: Path, big: Path
) -> None:
    """SIGINT goes to the whole foreground process group: the workers ignore it, the parent ends
    the export (130), terminates them and leaves nothing."""
    run = Stalled(tmp_path, env, config, big, "text", new_session=True)
    os.killpg(os.getpgid(run.proc.pid), signal.SIGINT)
    time.sleep(0.3)
    assert all(alive(p) for p in run.workers) or run.proc.poll() is not None
    err = run.finish()
    assert run.proc.returncode == 130, err
    assert "Traceback" not in err
    run.assert_clean()


def test_a_worker_that_dies_is_a_coded_error_and_leaves_nothing(
    tmp_path: Path, env: dict[str, str], config: Path, big: Path
) -> None:
    run = Stalled(tmp_path, env, config, big, "csv")
    os.kill(run.workers[0], signal.SIGKILL)
    run.go.write_text("go")
    err = run.finish()
    assert run.proc.returncode == 1, err
    lines = [line for line in err.splitlines() if line.startswith("error ")]
    assert len(lines) == 1 and lines[0].startswith("error IB-EXP-003:"), err
    assert "--jobs 1" in lines[0] and "Traceback" not in err
    run.assert_clean()
    ledger = ledger_events(tmp_path)
    assert any(e.get("event") == "IB-EXP-003" and "details" in e for e in ledger), (
        "the ledger has the error"
    )


def test_workers_do_not_outlive_a_parent_that_is_killed(
    tmp_path: Path, env: dict[str, str], config: Path, big: Path
) -> None:
    run = Stalled(tmp_path, env, config, big, "text")
    run.proc.kill()
    run.proc.wait()
    wait_until(lambda: not any(alive(p) for p in run.workers), 15, "orphaned workers to exit")
    if QUIET:
        wait_until(lambda: semaphores() <= run.semaphores, 15, "the semaphores to go")


def test_the_export_commits_only_when_the_last_range_is_written(
    tmp_path: Path, env: dict[str, str], config: Path, big: Path
) -> None:
    run = Stalled(tmp_path, env, config, big, "text")
    assert not run.out.exists() and run.temporaries()
    run.go.write_text("go")
    err = run.finish()
    assert run.proc.returncode == 0, err
    assert run.out.is_file() and run.temporaries() == []
    assert "leaked" not in err
    wait_until(lambda: not any(alive(p) for p in run.workers), 10, "the workers to end")
