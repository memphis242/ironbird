"""The export ends, promptly and with the status of one process, when it is stopped part way
while its workers are busy: the output closes (``| head``) or a write fails (a full disk).

The workers are terminated mid-send then. A pool with a thread that reads their results waits for
the rest of a message that will never come, and the interpreter waits for that thread at exit;
the pool of :mod:`ironbird.recording.pexport` reads on the calling thread and sees an end of file.
The race is narrow, so each case runs an export in a fresh process several times, with the stop at
a different point each time: through the library (the exit of the interpreter is what is
exercised) and through the CLI."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from poolutil import clean_recording
from recutil import CLI_MAIN, write_config
from replayutil import child_env

#: Where the output stops, so the workers are stopped at different points of their work: the
#: rows an output takes before it closes (the recording has 60000), and the bytes of text a full
#: disk takes (the whole export is about 8 MB).
CLOSE_ROWS = (
    1_000, 3_700, 6_100, 8_900, 11_300, 14_800, 17_200, 20_900,
    24_300, 27_700, 31_100, 35_600, 40_200, 45_900, 51_300, 57_800,
)  # fmt: skip
FULL_BYTES = (2_000, 40_000, 200_000, 700_000, 1_500_000, 3_000_000, 5_000_000, 7_500_000)
CLI_STOPS = (2_000, 700_000, 3_000_000, 7_500_000)
PROMPT_S = 15.0  # a hung export is killed at TIMEOUT_S; an export that ends takes a few seconds
TIMEOUT_S = 60.0
TABLE = '[export]\npool_min_bytes = 0\nrange_bytes = "1MiB"\n'

#: Fails the write that would take the output past ``IB_TEST_LIMIT`` bytes, as a full disk does.
_FULL_DISK = """
import errno, os
from ironbird import atomicfile
LIMIT = int(os.environ["IB_TEST_LIMIT"])
_written = [0]
_write = atomicfile.TempOutput.write

def write(self, data):
    if _written[0] + len(data) > LIMIT:
        raise OSError(errno.ENOSPC, "No space left on device", self.name)
    _written[0] += len(data)
    return _write(self, data)

atomicfile.TempOutput.write = write
"""


#: An export through the pool, in a process of its own that ends when the export does. ``close``:
#: the output refuses the line after ``stop`` characters; ``full``: its write fails at ``stop``.
_CHILD = """
import sys
from pathlib import Path
import poolutil
from ironbird.errors import Code, IronbirdError
from ironbird.recording.export import ExportFormat

path, jobs, mode, stop = Path(sys.argv[1]), int(sys.argv[2]), sys.argv[3], int(sys.argv[4])


class Full(poolutil.Capture):
    chars = 0

    def write(self, handle, text):
        if self.chars + len(text) > stop:
            raise IronbirdError(Code.EXP_WRITE, "disk full")
        self.chars += len(text)
        return super().write(handle, text)


if mode == "full":
    poolutil.Capture = Full
run = poolutil.run_export(
    path,
    ExportFormat.TEXT,
    jobs=jobs,
    per_line=mode == "close",
    refuse_after=stop if mode == "close" else None,
    range_bytes=1 << 20,
    pool=None,
)
print(run.error[0] if run.error else "ok", flush=True)
sys.exit(0 if run.error is None else 1)
"""


def child(recording: Path, jobs: int, mode: str, stop: int) -> tuple[int, str, float]:
    """``(status, what it printed, seconds)`` of an export in a fresh process."""
    here = str(Path(__file__).resolve().parent)
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([here, os.environ.get("PYTHONPATH", "")])}
    began = time.monotonic()
    done = subprocess.run(
        [sys.executable, "-c", _CHILD, str(recording), str(jobs), mode, str(stop)],
        capture_output=True,
        text=True,
        env=env,
        timeout=TIMEOUT_S,
        check=False,
    )
    return done.returncode, done.stdout.strip(), time.monotonic() - began


@pytest.mark.parametrize("mode", ["close", "full"])
def test_a_pooled_export_stopped_part_way_leaves_the_interpreter_free_to_exit(
    recording: Path, mode: str
) -> None:
    """The output closes, or a write fails (``IB-EXP-001``), while the workers are sending: the
    process ends within seconds with what one process ends with, whatever the point."""
    expected = child(recording, 1, mode, 1000)[:2]
    assert expected == ((0, "ok") if mode == "close" else (1, "IB-EXP-001")), expected
    for stop in CLOSE_ROWS if mode == "close" else FULL_BYTES:
        status, said, took = child(recording, 4, mode, stop)
        assert (status, said) == expected, (stop, status, said)
        assert took < PROMPT_S, (stop, took)


@pytest.fixture(scope="module")
def recording(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return clean_recording(tmp_path_factory.mktemp("exit") / "big.ibrec", 60000)


@pytest.fixture
def work(tmp_path: Path) -> tuple[Path, dict[str, str], Path]:
    for name in ("state", "recordings"):
        (tmp_path / name).mkdir(mode=0o700)
    config, _, _ = write_config(tmp_path, TABLE)
    return tmp_path, child_env(tmp_path), config


def timed(
    command: list[str], work: tuple[Path, dict[str, str], Path]
) -> tuple[subprocess.CompletedProcess[str], float]:
    directory, env, _ = work
    began = time.monotonic()
    done = subprocess.run(
        command,
        capture_output=True,
        text=True,
        env=env,
        cwd=directory,
        timeout=TIMEOUT_S,
        check=False,
    )
    return done, time.monotonic() - began


def test_an_output_that_closes_part_way_ends_the_export_promptly_with_the_status_of_one_process(
    recording: Path, work: tuple[Path, dict[str, str], Path]
) -> None:
    _, _, config = work

    def piped(jobs: str, size: int) -> tuple[str, float]:
        command = (
            f"{sys.executable} -c {CLI_MAIN!r} --config {config} export {recording} "
            f"--format text --jobs {jobs} 2>/dev/null | head -c {size} >/dev/null; "
            "echo rc=${PIPESTATUS[0]}"
        )
        done, took = timed(["bash", "-c", command], work)
        return done.stdout.strip(), took

    expected, _ = piped("1", 100)
    assert expected == "rc=0"
    for size in CLI_STOPS:
        got, took = piped("4", size)
        assert got == expected, size
        assert took < PROMPT_S, (size, took)


def test_a_write_that_fails_part_way_ends_the_export_promptly_with_the_status_of_one_process(
    recording: Path, work: tuple[Path, dict[str, str], Path]
) -> None:
    directory, env, config = work
    env = {**env}

    def failing(jobs: str, limit: int) -> tuple[int, str, float]:
        env["IB_TEST_LIMIT"] = str(limit)
        out = directory / f"out-{jobs}-{limit}.txt"
        done, took = timed(
            [
                sys.executable,
                "-c",
                _FULL_DISK + CLI_MAIN,
                "--config",
                str(config),
                "export",
                str(recording),
                "--format",
                "text",
                "--jobs",
                jobs,
                "-o",
                str(out),
            ],
            (directory, env, config),
        )
        assert not out.exists() and not list(directory.glob(".out-*"))  # nothing is left behind
        return done.returncode, done.stderr, took

    status, message, _ = failing("1", 1 << 20)
    assert status == 1 and "IB-EXP-001" in message, message
    for limit in CLI_STOPS:
        got, message, took = failing("4", limit)
        assert (got, "IB-EXP-001" in message) == (status, True), (limit, message)
        assert took < PROMPT_S, (limit, took)
