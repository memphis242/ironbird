"""The stall limit of the export pool (``[export] worker_stall``) ends an export for a worker that
has gone silent and idle, and for nothing else: not for time the export itself spent suspended
(Ctrl-Z, ``SIGSTOP``), and not for a range that takes longer to decode than the limit (no byte
moves until a range is done, but the worker uses CPU time).

The pool tests use a fake worker (the other end of a pipe the test holds) or a real child process
that burns CPU or sleeps; the CLI tests run real exports whose workers are slowed by a
``sitecustomize`` hook (the limit's lower bound is lowered to one second there, so that the tests
take seconds, not minutes)."""

from __future__ import annotations

import multiprocessing
import os
import selectors
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from ironbird.config import load_config
from ironbird.errors import Code, IronbirdError
from ironbird.recording import pexport
from ironbird.recording.poolmsg import EmbeddedDefinitions
from poolframe_corpus import JOB, LAYOUTS, embedded
from poolutil import clean_recording
from recutil import CLI_MAIN, write_config
from replayutil import child_env

# -- the pool, with a fake worker or a real child -------------------------------------------------


class Clock:
    """A clock the test moves."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def make_pool(stall_s: float | None, clock: Callable[[], float] = time.monotonic) -> Any:
    definitions = embedded()
    assert isinstance(definitions, EmbeddedDefinitions)
    return pexport.WorkerPool(definitions, 1, stall_s=stall_s, clock=clock)


def add_worker(pool: Any, process: Any = None) -> tuple[pexport.Ticket, Any]:
    """A worker that is the other end of a pipe held by the test (and ``process``, for the probe
    of its CPU time), holding ``JOB``."""
    ours, theirs = multiprocessing.Pipe()
    slot = pexport._Slot(process, ours, pool.clock())
    ticket = pexport.Ticket(JOB, b"", LAYOUTS)
    slot.sent.append(ticket)
    pool._slots.append(slot)
    return ticket, theirs


@pytest.fixture
def children() -> Iterator[list[subprocess.Popen[bytes]]]:
    """Child processes the test started, ended with the test."""
    started: list[subprocess.Popen[bytes]] = []
    yield started
    for child in started:
        if child.poll() is None:
            child.kill()
        child.wait()


def child(children: list[subprocess.Popen[bytes]], code: str) -> subprocess.Popen[bytes]:
    process = subprocess.Popen([sys.executable, "-I", "-c", code])
    children.append(process)
    return process


def poll_for(pool: Any, seconds: float) -> None:
    """Poll ``pool`` for ``seconds``, or until it is broken."""
    end = time.monotonic() + seconds
    while time.monotonic() < end and not pool.broken:
        pool.poll(pexport.WAIT_SLICE_S)


def the_error(ticket: pexport.Ticket) -> IronbirdError:
    with pytest.raises(IronbirdError) as caught:
        ticket.result()
    assert caught.value.code is Code.EXP_WORKER
    return caught.value


# -- time the parent was not watching is not the worker's silence ---------------------------------


def test_time_the_parent_was_not_watching_is_not_silence() -> None:
    """A parent that was stopped (Ctrl-Z, a debugger, a frozen cgroup) finds, when it runs again,
    that the clock has gone on while its workers were stopped too: the worker was not silent for
    that long, so the stall limit does not count it."""
    clock = Clock()
    pool = make_pool(5.0, clock)
    ticket, theirs = add_worker(pool)
    try:
        pool.poll(0.01)
        clock.now += 400.0  # the whole process group was stopped for 400 s
        pool.poll(0.01)
        assert not pool.broken and not ticket.done(), "the suspension was taken for silence"
        for _ in range(8):  # but silence while the parent watches still counts
            clock.now += 0.7
            pool.poll(0.01)
        assert pool.broken
        assert "no progress" in the_error(ticket).summary
    finally:
        theirs.close()
        pool.close(abnormal=True)


def test_a_wait_the_parent_asked_for_is_not_a_gap() -> None:
    """A caller that waits longer than a second in one poll is watching, not away."""
    clock = Clock()
    pool = make_pool(5.0, clock)
    ticket, theirs = add_worker(pool)
    real_select = selectors.PollSelector.select

    def long_wait(self: selectors.PollSelector, timeout: float | None = None) -> Any:
        clock.now += 6.0  # the wait the caller asked for, 10 s, went by
        return real_select(self, 0.0)

    try:
        pexport.selectors.PollSelector.select = long_wait  # type: ignore[method-assign]
        pool.poll(10.0)
        assert pool.broken, "a long wait hid a silent worker"
        assert "no progress" in the_error(ticket).summary
    finally:
        pexport.selectors.PollSelector.select = real_select  # type: ignore[method-assign]
        theirs.close()
        pool.close(abnormal=True)


# -- a worker that is using CPU time is working --------------------------------------------------

BURN = "while True: pass"
SLEEP = "import time; time.sleep(60)"
# The main thread sleeps (hung) while a side thread uses a tenth of a core, as a worker's
# parent-watch thread does at a far lower rate.
SIDE_THREAD = """
import threading, time
def side():
    while True:
        time.sleep(0.01)
        began = time.thread_time()
        while time.thread_time() - began < 0.001:
            pass
threading.Thread(target=side, daemon=True).start()
time.sleep(60)
"""


def test_a_worker_that_uses_cpu_time_but_moves_no_byte_is_not_a_stall(
    children: list[subprocess.Popen[bytes]],
) -> None:
    """A range takes as long as it takes: a worker decoding it replies at the end, but its CPU time
    rises meanwhile. Three times the limit goes by."""
    pool = make_pool(0.6)
    worker = child(children, BURN)
    ticket, theirs = add_worker(pool, worker)
    try:
        poll_for(pool, 2.0)
        assert not pool.broken and not ticket.done()
    finally:
        theirs.close()
        pool.close(abnormal=True)


def test_a_worker_that_is_asleep_is_a_stall_that_names_the_setting(
    children: list[subprocess.Popen[bytes]],
) -> None:
    pool = make_pool(0.6)
    worker = child(children, SLEEP)
    ticket, theirs = add_worker(pool, worker)
    try:
        began = time.monotonic()
        poll_for(pool, 10.0)
        assert pool.broken and 0.6 <= time.monotonic() - began < 5.0
        error = the_error(ticket)
        assert "no progress" in error.summary and "\n" not in error.summary
        assert "[export] worker_stall" in error.summary and "stopped" not in error.summary
        assert "--jobs 1" in error.summary
        assert f"pid {worker.pid}" in (error.details or "")
    finally:
        theirs.close()
        pool.close(abnormal=True)


def test_cpu_time_of_a_helper_thread_does_not_make_a_hung_worker_look_busy(
    children: list[subprocess.Popen[bytes]],
) -> None:
    """The work of a worker runs on its main thread. A helper thread (the worker's parent watch
    wakes once a second) adds to the process's CPU time, which must not count as progress: at the
    default limit it would otherwise hide a hung worker for good."""
    pool = make_pool(1.5)
    worker = child(children, SIDE_THREAD)
    ticket, theirs = add_worker(pool, worker)
    try:
        began = time.monotonic()
        poll_for(pool, 15.0)
        assert pool.broken and 1.5 <= time.monotonic() - began < 10.0
        assert "no progress" in the_error(ticket).summary
        with open(f"/proc/{worker.pid}/stat", "rb") as whole_process:
            whole = pexport.parse_stat(whole_process.read(4096).decode("ascii", "replace"))
        main_thread = pexport.probe(worker.pid)
        assert whole is not None and main_thread is not None
        assert whole.ticks > main_thread.ticks, "the helper thread used no CPU time: no test"
    finally:
        theirs.close()
        pool.close(abnormal=True)


def test_a_gap_moves_the_cpu_baseline_on_with_the_silence() -> None:
    """Time the parent was not watching is no evidence either way: the CPU reading taken earlier
    stays, and its time moves on by the gap, as the worker's silence does."""
    slot = pexport._Slot(None, multiprocessing.Pipe()[0], 100.0)
    slot.cpu, slot.cpu_at = 7, 150.0
    slot.defer(30.0, 200.0)
    assert slot.cpu == 7 and slot.cpu_at == 180.0 and slot.progress == 130.0
    slot.defer(1000.0, 200.0)  # never past now
    assert slot.cpu == 7 and slot.cpu_at == 200.0 and slot.progress == 200.0


def test_a_worker_that_is_stopped_is_a_stall_that_says_so(
    children: list[subprocess.Popen[bytes]],
) -> None:
    pool = make_pool(0.6)
    worker = child(children, BURN)
    ticket, theirs = add_worker(pool, worker)
    try:
        poll_for(pool, 1.0)
        assert not pool.broken
        os.kill(worker.pid, signal.SIGSTOP)  # our own child
        poll_for(pool, 10.0)
        assert pool.broken
        error = the_error(ticket)
        assert "was stopped" in error.summary and "[export] worker_stall" in error.summary
    finally:
        theirs.close()
        pool.close(abnormal=True)


def test_a_worker_that_burns_cpu_instead_of_reading_what_it_is_sent_is_a_stall(
    children: list[subprocess.Popen[bytes]],
) -> None:
    """CPU time counts only for a worker with nothing left unsent: one that spins and never reads
    its pipe is not working on what it was sent."""
    pool = make_pool(0.6)
    worker = child(children, BURN)
    ticket, theirs = add_worker(pool, worker)
    pool._slots[0].outbox.put(bytes(8 * 1024 * 1024))  # far more than the pipe takes
    try:
        poll_for(pool, 10.0)
        assert pool.broken
        assert "no progress" in the_error(ticket).summary
    finally:
        theirs.close()
        pool.close(abnormal=True)


def test_no_limit_waits_for_a_silent_worker(children: list[subprocess.Popen[bytes]]) -> None:
    clock = Clock()
    pool = make_pool(None, clock)
    worker = child(children, SLEEP)
    ticket, theirs = add_worker(pool, worker)
    try:
        for _ in range(5):
            clock.now += 10_000.0
            pool.poll(0.01)
        assert not pool.broken and not ticket.done()
    finally:
        theirs.close()
        pool.close(abnormal=True)


def test_the_cpu_time_of_a_process_is_read_from_its_stat_line() -> None:
    mine = pexport.probe(os.getpid())
    assert mine is not None and mine.state in "RSD" and mine.ticks >= 0
    assert pexport.probe(2**22 + 12345) is None  # no such process
    # fields 14 and 15 after a command name that holds spaces and parentheses
    line = "42 (a) b) c) T 1 2 3 4 5 6 7 8 9 10 111 22 0 0 20 0 1 0 5 6 7\n"
    assert pexport.parse_stat(line) == pexport._Probe("T", 133)
    for bad in ("", "42 (x)", "42 (x) T 1 2", "42 (x) T " + "a " * 20):
        assert pexport.parse_stat(bad) is None


# -- no poll waits long, or makes a descriptor ----------------------------------------------------


def test_a_write_to_a_worker_takes_at_most_a_chunk(monkeypatch: pytest.MonkeyPatch) -> None:
    """One call of ``write`` copies as much as the pipe takes, however much is queued; at most a
    chunk, so the deadline between two writes is looked at often enough."""
    ours, theirs = multiprocessing.Pipe()
    os.set_blocking(ours.fileno(), False)
    outbox = pexport._Outbox(ours.fileno())
    sizes: list[int] = []
    real_write = os.write

    def write(fd: int, data: Any) -> int:
        count = real_write(fd, data)
        sizes.append(count)
        return count

    monkeypatch.setattr(pexport.os, "write", write)
    outbox.put(bytes(4 * 1024 * 1024))
    got: list[bytes] = []
    reader = threading.Thread(target=lambda: got.append(theirs.recv_bytes()), daemon=True)
    reader.start()
    try:
        while outbox.unsent:
            outbox.flush(time.monotonic() + 1.0)
        reader.join(10.0)
        assert len(got) == 1 and len(got[0]) == 4 * 1024 * 1024
        assert max(sizes) <= pexport._Outbox.CHUNK and len(sizes) >= 16
    finally:
        ours.close()
        theirs.close()


def test_a_poll_makes_no_descriptor(monkeypatch: pytest.MonkeyPatch) -> None:
    """The wait uses ``poll``, which needs no descriptor of its own (an epoll selector made one in
    every poll, which fails with ``EMFILE`` when a directory export has used the limit up)."""
    pool = make_pool(300.0)
    ticket, theirs = add_worker(pool)
    seen: list[int] = []
    real_select = selectors.PollSelector.select

    def count(self: selectors.PollSelector, timeout: float | None = None) -> Any:
        seen.append(len(os.listdir("/proc/self/fd")))
        return real_select(self, timeout)

    try:
        before = len(os.listdir("/proc/self/fd"))
        monkeypatch.setattr(selectors.PollSelector, "select", count)
        pool.poll(0.01)
        assert seen and seen[0] == before
        assert not ticket.done()
    finally:
        theirs.close()
        pool.close(abnormal=True)


# -- the configuration ---------------------------------------------------------------------------


def test_the_stall_limit_may_be_unlimited(tmp_path: Path) -> None:
    def parse(table: str) -> Any:
        path, _, _ = write_config(tmp_path, table)
        return load_config(path)

    assert parse('[export]\nworker_stall = "unlimited"\n').export.worker_stall_s is None
    assert parse('[export]\nworker_stall = "90s"\n').export.worker_stall_s == 90
    assert parse("").export.worker_stall_s == 300
    with pytest.raises(IronbirdError):
        parse('[export]\nworker_stall = "Unlimited"\n')


# -- real exports ---------------------------------------------------------------------------------

#: Loaded by every process of the export (``PYTHONPATH``): the limit's lower bound is one second,
#: and the first range of each worker costs ``IB_TEST_BURN`` seconds of CPU (or, ``hang``, never
#: ends), after leaving a mark in ``IB_TEST_MARKS`` so that the test knows the worker is busy.
_HOOK = """
import os, time
from ironbird import config
config.MIN_WORKER_STALL_S = 1
from ironbird.recording import pexport

_run_job = pexport.run_job
_first = [True]

def run_job(job):
    if _first[0] and os.environ.get("IB_TEST_BURN"):
        _first[0] = False
        open(os.path.join(os.environ["IB_TEST_MARKS"], str(os.getpid())), "w").close()
        if os.environ["IB_TEST_BURN"] == "hang":
            time.sleep(3600)
        start = time.process_time()
        while time.process_time() - start < float(os.environ["IB_TEST_BURN"]):
            pass
    return _run_job(job)

pexport.run_job = run_job
"""
STALL_TABLE = '[export]\npool_min_bytes = 0\nrange_bytes = 4096\nworker_stall = "1s"\n'


@pytest.fixture
def export_env(tmp_path: Path) -> dict[str, str]:
    for name in ("state", "recordings", "marks", "hook"):
        (tmp_path / name).mkdir(mode=0o700)
    (tmp_path / "hook" / "sitecustomize.py").write_text(_HOOK)
    env = child_env(tmp_path)
    env["PYTHONPATH"] = os.pathsep.join(
        [str(tmp_path / "hook"), *filter(None, [env.get("PYTHONPATH")])]
    )
    env["IB_TEST_MARKS"] = str(tmp_path / "marks")
    return env


def start_export(
    tmp_path: Path, env: dict[str, str], recording: Path, *args: str
) -> subprocess.Popen[str]:
    config, _, _ = write_config(tmp_path, STALL_TABLE)
    return subprocess.Popen(
        [sys.executable, "-c", CLI_MAIN, "--config", str(config), "export", str(recording), *args],
        env=env,
        cwd=tmp_path,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,  # a group of its own, to stop and continue as Ctrl-Z does
    )


def wait_for_marks(tmp_path: Path, count: int, process: subprocess.Popen[str]) -> None:
    deadline = time.monotonic() + 60.0
    while len(list((tmp_path / "marks").iterdir())) < count:
        assert process.poll() is None, "the export ended before its workers were busy"
        assert time.monotonic() < deadline, "the workers never got to work"
        time.sleep(0.02)


def one_process_output(tmp_path: Path, env: dict[str, str], recording: Path) -> str:
    plain = {k: v for k, v in env.items() if k != "IB_TEST_BURN"}
    run = start_export(tmp_path, plain, recording, "--jobs", "1", "--format", "text")
    out, err = run.communicate(timeout=120)
    assert run.returncode == 0, err
    return out


@pytest.fixture
def recording(tmp_path: Path) -> Path:
    return clean_recording(tmp_path / "run.ibrec", 3000)


def test_an_export_that_was_suspended_and_continued_finishes_as_if_it_had_not_been(
    tmp_path: Path, export_env: dict[str, str], recording: Path
) -> None:
    """Ctrl-Z, other work, ``fg``: the stopped group (the parent and every worker, mid-range) is
    longer stopped than the limit, and the export carries on to the same rows (it ended with
    ``IB-EXP-003`` after ``fg``). ``SIGSTOP`` stands in for Ctrl-Z, whose ``SIGTSTP`` the kernel
    drops for a process group that has no terminal."""
    expected = one_process_output(tmp_path, export_env, recording)
    env = {**export_env, "IB_TEST_BURN": "0.8"}
    run = start_export(tmp_path, env, recording, "--jobs", "2", "--format", "text")
    try:
        wait_for_marks(tmp_path, 1, run)
        os.killpg(run.pid, signal.SIGSTOP)
        time.sleep(3.5)  # three times the limit
        os.killpg(run.pid, signal.SIGCONT)
        out, err = run.communicate(timeout=120)
    finally:
        if run.poll() is None:
            os.killpg(run.pid, signal.SIGKILL)
            run.wait()
    assert run.returncode == 0, err
    assert out == expected


def test_a_range_that_takes_longer_than_the_limit_to_decode_does_not_end_the_export(
    tmp_path: Path, export_env: dict[str, str], recording: Path
) -> None:
    """A filter that decodes every frame and matches almost none leaves a range's worker silent
    for as long as the range takes, which may be minutes; its CPU time shows it is working (the
    export ended with ``IB-EXP-003`` and advice that fails the same way again)."""
    expected = one_process_output(tmp_path, export_env, recording)
    env = {**export_env, "IB_TEST_BURN": "3.5"}  # more than three times the limit, per worker
    run = start_export(tmp_path, env, recording, "--jobs", "2", "--format", "text")
    out, err = run.communicate(timeout=120)
    assert run.returncode == 0, err
    assert out == expected


def test_a_worker_that_does_nothing_ends_the_export_with_one_line_that_names_the_setting(
    tmp_path: Path, export_env: dict[str, str], recording: Path
) -> None:
    env = {**export_env, "IB_TEST_BURN": "hang"}
    run = start_export(tmp_path, env, recording, "--jobs", "2", "--format", "text")
    began = time.monotonic()
    _, err = run.communicate(timeout=120)
    assert run.returncode == 1, err
    assert 1.0 <= time.monotonic() - began < 60.0
    lines = [line for line in err.splitlines() if line.startswith("error ")]
    assert len(lines) == 1 and lines[0].startswith("error IB-EXP-003:"), err
    assert "[export] worker_stall" in lines[0] and "Traceback" not in err
