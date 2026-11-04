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
from ironbird.recording import format as fmt
from ironbird.recording.format import MarkKind, RecordType
from ironbird.recording.writer import RecorderOptions
from poolutil import RX, SERIAL, TX, builder_recording, clean_recording, mixed_frames, write_builder
from poolwide import wide_recording
from recutil import CLI_MAIN, write_config
from replayutil import STEP_NS, T0, child_env, hk, write_recording

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


def stopped(pid: int) -> bool:
    """Every thread of ``pid`` is stopped by a job-control signal (a signal sent now waits)."""
    states = []
    for task in Path(f"/proc/{pid}/task").iterdir():
        try:
            states.append((task / "stat").read_text().rsplit(")", 1)[1].split()[0])
        except OSError:
            continue  # a thread that ended
    return bool(states) and all(state == "T" for state in states)


def signal_set(pid: int, *fields: str) -> set[int]:
    """The signals in the ``fields`` masks of ``/proc/PID/status`` (none once it is gone)."""
    try:
        lines = Path(f"/proc/{pid}/status").read_text().splitlines()
    except OSError:
        return set()
    mask = 0
    for line in lines:
        name, _, value = line.partition(":")
        if name in fields:
            mask |= int(value, 16)
    return {n for n in range(1, mask.bit_length() + 1) if mask >> (n - 1) & 1}


def pending(pid: int) -> set[int]:
    return signal_set(pid, "SigPnd", "ShdPnd")


def ignored(pid: int) -> set[int]:
    return signal_set(pid, "SigIgn")


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
    the export (130), terminates them and leaves nothing.

    The parent is held stopped while the group is interrupted, so nothing but a worker itself
    can end a worker in that window: the interrupt waits, pending, in the parent, and each worker
    is seen to ignore it (one still starting has it blocked, and drops it as it sets it ignored).
    Only then is the parent let go to act on it."""
    run = Stalled(tmp_path, env, config, big, "text", new_session=True)
    group = os.getpgid(run.proc.pid)
    assert all(os.getpgid(p) == group for p in run.workers), "the workers share the group"
    os.kill(run.proc.pid, signal.SIGSTOP)
    try:
        wait_until(lambda: stopped(run.proc.pid), what="the parent to stop")
        os.killpg(group, signal.SIGINT)
        assert signal.SIGINT in pending(run.proc.pid), "the interrupt waits in the parent"
        for pid in run.workers:
            wait_until(
                lambda pid=pid: not alive(pid) or signal.SIGINT in ignored(pid),
                what=f"worker {pid} to ignore SIGINT",
            )
        assert all(alive(p) for p in run.workers), "a worker acted on the interrupt"
        assert not any(signal.SIGINT in pending(p) for p in run.workers), "nothing left to act"
    finally:
        os.kill(run.proc.pid, signal.SIGCONT)
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


# -- the findings of the review of the pull request ---------------------------------------------


def piped(
    tmp_path: Path, env: dict[str, str], config: Path, source: Path, jobs: str, *args: str
) -> tuple[str, list[str]]:
    """``ib export SOURCE --jobs N ARGS | head -n 1 | md5sum``: what the pipe delivered with the
    exit status of ``ib``, and what ``ib`` said (but for its summary)."""
    command = (
        f"{sys.executable} -c {CLI_MAIN!r} --config {config} export {source} --jobs {jobs} "
        f"{' '.join(args)} 2>{tmp_path / 'err'} | head -n 1 | md5sum; echo rc=${{PIPESTATUS[0]}}"
    )
    done = subprocess.run(
        ["bash", "-c", command],
        env=env,
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    errors = (tmp_path / "err").read_text().splitlines()
    # the summary says how many rows were written before the pipe was seen to be closed
    return done.stdout, [line for line in errors if not line.startswith("export: read")]


@pytest.mark.parametrize("extra", [[], ["--skip-damage"]])
@pytest.mark.parametrize("fmt", ["text", "jsonl"])
def test_a_reader_that_goes_away_ends_the_export_with_the_same_status_and_messages(
    tmp_path: Path, env: dict[str, str], config: Path, fmt: str, extra: list[str]
) -> None:
    """``| head -n 1`` on a recording with damage much further on than a pipe holds: the single
    process stops at the broken pipe and never reads the damage; the damage must not be reported
    (nor change the exit status) because a worker read the range that holds it."""
    big = clean_recording(tmp_path / "big.ibrec", 40000)
    data = bytearray(big.read_bytes())
    at = 900_000  # inside the first range, a megabyte of text further on than a pipe takes
    data[at : at + 200] = bytes(200)
    damaged = tmp_path / "damaged.ibrec"
    damaged.write_bytes(bytes(data))
    table = '[export]\npool_min_bytes = 0\nrange_bytes = "1MiB"\n'
    wide, _, _ = write_config(tmp_path, table)
    single = piped(tmp_path, env, wide, damaged, "1", "--format", fmt, *extra)
    pooled = piped(tmp_path, env, wide, damaged, "3", "--format", fmt, *extra)
    assert single == pooled
    assert "rc=0" in single[0] and not any("IB-REC" in line for line in single[1]), single


def test_a_recording_renamed_during_the_export_is_exported_from_the_file_opened(
    tmp_path: Path, env: dict[str, str], config: Path, big: Path
) -> None:
    """Retention or a user files the recording away (and another takes its name) while the export
    runs: one process holds the file open and carries on, and so must the pool."""
    expected = tmp_path / "expected.txt"
    done = ib(
        tmp_path, env, config, str(big), "--format", "text", "-o", str(expected), "--jobs", "1"
    )
    assert done.returncode == 0, done.stderr
    other = tmp_path / "other.ibrec"
    options = RecorderOptions(min_free_bytes=0, chunk_bytes=6000)
    write_recording(other, mixed_frames(3000, start=7), options=options, streams=(RX, TX, SERIAL))
    for how in ("replaced", "gone"):
        source = tmp_path / f"{how}.ibrec"
        source.write_bytes(big.read_bytes())
        run = Stalled(tmp_path, env, config, source, "text")
        source.rename(tmp_path / f"{how}.moved")
        if how == "replaced":
            os.link(other, source)
        run.go.write_text("go")
        err = run.finish()
        assert run.proc.returncode == 0, err
        assert run.out.read_bytes() == expected.read_bytes(), how
        assert "Traceback" not in err and "IB-EXP" not in err
        run.out.unlink()
        run.mark.unlink()
        run.go.unlink()
        pools = [e for e in ledger_events(tmp_path) if e["event"] == "export.pool"]
        assert pools[-1]["local"] >= 1, pools[-1]


def test_a_huge_core_count_is_capped_at_the_most_workers_there_can_be(
    tmp_path: Path, env: dict[str, str], config: Path, big: Path
) -> None:
    program = (
        "import os; os.sched_getaffinity = lambda pid: set(range(200)); "
        "from ironbird.recording import pexport; pexport.cpu_quota = lambda: None; " + CLI_MAIN
    )
    args = ["--format", "text", "-o", str(tmp_path / "out.txt")]
    done = subprocess.run(
        [sys.executable, "-c", program, "--config", str(config), "export", str(big), *args],
        env=env,
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert done.returncode == 0, done.stderr
    events = [e for e in ledger_events(tmp_path) if e["event"] == "export.pool"]
    assert events and events[0]["workers"] == 64


def test_a_hangup_sent_to_the_whole_process_group_ends_the_export_with_one_line(
    tmp_path: Path, env: dict[str, str], config: Path, big: Path
) -> None:
    """A closed terminal hangs up every process of the session at once, the workers and the
    resource tracker among them: the export ends 129 with its one line, not a page of leaked
    semaphore warnings and tracebacks."""
    run = Stalled(tmp_path, env, config, big, "text", new_session=True)
    os.killpg(os.getpgid(run.proc.pid), signal.SIGHUP)
    err = run.finish()
    assert run.proc.returncode == 129, err
    assert [line for line in err.splitlines() if line.strip()] == ["interrupted (SIGHUP)"], err
    run.assert_clean()


@pytest.mark.parametrize("jobs", ["2", "3"])
def test_a_marks_only_stretch_then_a_sequence_reset_exports_the_same_through_the_cli(
    tmp_path: Path, env: dict[str, str], config: Path, jobs: str
) -> None:
    builder = builder_recording(300)
    seq = 301
    while len(builder.out) < 60_000:
        builder.out += b""
        builder.add(RecordType.MARK, fmt.encode_mark(T0, MarkKind.SIGNAL, "operator note " * 4))
    for reset in range(1, 200):
        builder.frame(
            reset,
            data=hk(reset),
            stream=0,
            source=0,
            mid=0x890,
            time_ns=T0 + (seq + reset) * STEP_NS,
        )
    source = write_builder(builder, tmp_path / "marks.ibrec")
    codes: list[tuple[int, str, bytes]] = []
    for extra in ([], ["--skip-damage"]):
        out = tmp_path / f"out-{jobs}.txt"
        done = ib(
            tmp_path,
            env,
            config,
            str(source),
            "--format",
            "text",
            "-o",
            str(out),
            "--jobs",
            jobs,
            *extra,
        )
        base = ib(
            tmp_path,
            env,
            config,
            str(source),
            "--format",
            "text",
            "-o",
            str(tmp_path / "ref.txt"),
            "--jobs",
            "1",
            *extra,
        )
        codes.append((done.returncode, done.stderr.replace(out.name, "X"), out.read_bytes()))
        assert (done.returncode, done.stderr.replace(out.name, "X"), out.read_bytes()) == (
            base.returncode,
            base.stderr.replace("ref.txt", "X"),
            (tmp_path / "ref.txt").read_bytes(),
        )
        out.unlink()
        (tmp_path / "ref.txt").unlink()
    assert "IB-REC-007" in codes[0][1] and codes[0][2].count(b"\n") == 300


def test_a_long_run_of_zeros_does_not_cost_a_scan_per_range(
    tmp_path: Path, env: dict[str, str]
) -> None:
    """A crash can leave a file extended with zeros, and records may follow them. Every range that
    begins in the zeros looks for a first record; each must give up at the end of its own range,
    not scan on to the records past the zeros: the bytes the scans of the pool read come to a few
    times the zeros, not the zeros once per range. (Zeros at the very end of the file end the walk
    at the first of them, so the ranges behind it are never waited for, and what they scan is not
    in the report.)"""
    config, _, _ = write_config(tmp_path, "[export]\npool_min_bytes = 0\nrange_bytes = 32768\n")
    builder = builder_recording(600)
    zeros = 8 << 20
    builder.out += bytes(zeros)
    for seq in range(601, 1201):
        builder.frame(seq, data=hk(seq), stream=0, source=0, mid=0x890, time_ns=T0 + seq * STEP_NS)
    rec = write_builder(builder, tmp_path / "zeros.ibrec")
    done: dict[str, subprocess.CompletedProcess[str]] = {}
    for jobs in ("1", "4"):
        args = ["--format", "text", "-o", str(tmp_path / f"o{jobs}"), "--jobs", jobs]
        done[jobs] = ib(tmp_path, env, config, str(rec), "--skip-damage", *args)
    assert done["1"].returncode == done["4"].returncode == 5, (done["1"].stderr, done["4"].stderr)
    assert (tmp_path / "o1").read_bytes() == (tmp_path / "o4").read_bytes()
    pool = [e for e in ledger_events(tmp_path) if e["event"] == "export.pool"]
    assert len(pool) == 1 and pool[0]["ranges"] > zeros // 32768 // 2, pool
    # The range holding the start of the zeros finds the next record by one scan of them, and
    # each range inside them scans its own length: about twice the zeros are scanned.
    assert zeros <= pool[0]["scanned"] < 2.5 * zeros, (pool[0]["scanned"], zeros)


def test_wide_rows_do_not_grow_the_memory_of_the_export(
    tmp_path: Path, env: dict[str, str]
) -> None:
    """Rows that are tens of KiB each from a frame of one KiB: a range makes hundreds of MiB of
    text. With the bounds of a result and of the text waiting scaled down to a few hundred KiB,
    the peak memory of the parent is that of one process plus a small constant."""
    config, _, _ = write_config(tmp_path, "[export]\npool_min_bytes = 0\nrange_bytes = 131072\n")
    rec = wide_recording(tmp_path / "wide.ibrec", 1400)
    peaks: dict[str, int] = {}
    for jobs in ("1", "4"):
        peaks[jobs] = peak_rss_kib(
            tmp_path,
            env,
            config,
            [
                "export",
                str(rec),
                "--format",
                "text",
                "-o",
                str(tmp_path / f"o{jobs}"),
                "--jobs",
                jobs,
            ],
        )
    assert (tmp_path / "o1").read_bytes() == (tmp_path / "o4").read_bytes()
    assert peaks["4"] <= peaks["1"] + 16 * 1024, peaks


# The parent's peak resident size, with the pool's bounds scaled down: ``main()`` of the CLI, then
# ``ru_maxrss`` to a file. (``make_options`` takes the bounds; a version without them runs as is.)
_MEASURED = """
import functools, inspect, os, resource, sys
from ironbird.cli import main as cli
if "text_cap" in inspect.signature(cli.make_options).parameters:
    cli.make_options = functools.partial(cli.make_options, text_cap=256 * 1024, text_budget=1 << 20)
try:
    code = cli.main()
finally:
    with open(os.environ["IB_RSS"], "w") as f:
        f.write(str(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
sys.exit(code)
"""


def peak_rss_kib(tmp_path: Path, env: dict[str, str], config: Path, args: list[str]) -> int:
    marker = tmp_path / "rss"
    done = subprocess.run(
        [sys.executable, "-c", _MEASURED, "--config", str(config), *args],
        env={**env, "IB_RSS": str(marker)},
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=900,
        check=False,
    )
    assert done.returncode == 0, done.stderr
    return int(marker.read_text())
