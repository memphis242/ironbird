"""Parallel export decode (plan 2m), the findings of the antagonistic review of the pull request:
each test is a recording or a situation that made the pool write something other than one
process writes, hold more than it should, or fail where one process succeeds. The command-line
side of the findings (signals, pipes, renamed files, memory) is in ``test_export_pool_cli.py``."""

from __future__ import annotations

import dataclasses
import os
import random
import signal
import struct
from pathlib import Path
from typing import Any

import pytest

from ironbird.recording import pexport
from ironbird.recording import reader as reader_module
from ironbird.recording.export import ExportFormat
from ironbird.recording.format import MarkKind
from ironbird.recording.pexport import (
    EmbeddedDefinitions,
    Job,
    Outcome,
    Seed,
    Shape,
    _Runner,
    _Walk,
    _Worker,
)
from ironbird.recording.reader import Damage, RecordingReader
from ironbird.recording.replay import Replay
from ironbird.recording.writer import RecorderOptions
from poolutil import (
    RANGE,
    RX,
    SERIAL,
    TX,
    RecordType,
    assert_same,
    builder_recording,
    clean_recording,
    fmt,
    mixed_frames,
    mutate,
    run_export,
    write_builder,
)
from poolwide import wide_recording
from replayutil import STEP_NS, T0, hk, write_recording

JOBS = 3


@pytest.fixture(scope="module")
def clean(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return clean_recording(tmp_path_factory.mktemp("clean") / "clean.ibrec")


# -- a range without frames does not reset the sequence check -----------------------------------


@pytest.mark.parametrize("skip", [False, True])
@pytest.mark.parametrize("mark_bytes", [5000, 9000, 13000])
def test_a_stretch_of_records_without_frames_leaves_the_sequence_check_alone(
    tmp_path: Path, skip: bool, mark_bytes: int
) -> None:
    """Whole ranges of MARK records, then sequence numbers that go back: the single process meets
    the reset at the first frame after the marks, and so must the pool, whichever range it falls
    in (a range without frames is not a range that starts the numbering again)."""
    builder = builder_recording(60)
    start = len(builder.out)
    while len(builder.out) - start < mark_bytes:
        builder.add(RecordType.MARK, fmt.encode_mark(T0, MarkKind.SIGNAL, "m" * 40))
    for seq in range(1, 120):
        builder.frame(
            seq, data=hk(seq), stream=0, source=0, mid=0x890, time_ns=T0 + (200 + seq) * STEP_NS
        )
    path = write_builder(builder, tmp_path / "marks.ibrec")
    single = run_export(path, skip_damage=skip)
    pooled = run_export(path, jobs=JOBS, skip_damage=skip)
    assert single.stats["damage"] == 1, "the reset is damage in the single process"
    assert_same(single, pooled, label=f"skip={skip} marks={mark_bytes}")


# -- the budget of a range read again -----------------------------------------------------------

BUDGET_RECORDS = 149  # skippable records of BUDGET_BYTES, then one of BUDGET_LAST bytes
BUDGET_BYTES = 30000
BUDGET_LAST = 19472


def budget_recording(path: Path) -> Path:
    """A damaged frame, then skippable records with a plausible length each, as many as use the
    resync budget of the whole pass up to a few bytes (the META record's bytes among them), then
    valid frames. The pass gives up its scan for the end of the damage only if all of the work
    done since the start of the file is charged."""
    builder = builder_recording(150)
    offset = builder.frame(151, data=hk(151), stream=0, source=0, mid=0x890, time_ns=T0)
    builder.out[offset + 8] ^= 0xFF  # a bad CRC in a valid envelope
    while len(builder.out) % 8:
        builder.out.append(0)
    for i in range(BUDGET_RECORDS):
        last = i == BUDGET_RECORDS - 1
        builder.out += struct.pack("<IBBH", BUDGET_LAST if last else BUDGET_BYTES, 0x80, 0, 0)
    for seq in range(152, 900):
        builder.frame(seq, data=hk(seq), stream=0, source=0, mid=0x890, time_ns=T0 + seq * STEP_NS)
    return write_builder(builder, path)


def gives_up(path: Path, charged: int | None = None) -> bool:
    """Whether the scan for the end of the damage in ``path`` gives up (``charged``: the work to
    start from instead of the META record's)."""
    with RecordingReader(path, skip_damage=True) as reader:
        if charged is not None:
            reader.restart_work(charged)
        for item in reader.items():
            if isinstance(item, Damage):
                return item.resumed_at is None
    raise AssertionError("no damage")


@pytest.mark.parametrize("skip", [False, True])
def test_a_range_read_again_has_the_resync_budget_of_the_single_process(
    tmp_path: Path, skip: bool
) -> None:
    path = budget_recording(tmp_path / "budget.ibrec")
    assert gives_up(path) and not gives_up(path, 0), "the recording is tuned to the META bytes"
    single = run_export(path, skip_damage=skip)
    pooled = run_export(path, jobs=JOBS, skip_damage=skip)
    assert pooled.report.reruns >= 1
    assert_same(single, pooled, label=f"skip={skip}")


# -- the scan for a first record stops at the end of the range ----------------------------------


def test_the_scan_for_a_boundary_reads_no_further_than_its_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = clean_recording(tmp_path / "tail.ibrec", 300)
    size = path.stat().st_size
    with open(path, "r+b") as handle:
        handle.truncate(size + (64 << 20))  # a sparse tail of zeros, as a crash can leave
    read = 0
    pread = os.pread

    def counting(fd: int, length: int, offset: int) -> bytes:
        nonlocal read
        data = pread(fd, length, offset)
        read += len(data)
        return data

    monkeypatch.setattr(reader_module.os, "pread", counting)
    with RecordingReader(path) as reader:
        start = size + (1 << 20)
        assert reader.find_boundary(start, start + 65536) is None
        assert read <= 65536 + 2 * reader_module.READ_BLOCK, read
        read = 0
        assert reader.find_boundary(start) is None  # unlimited: to the end of the file
        assert read >= 60 << 20
        read = 0
        assert reader.find_boundary(8, size) is not None  # a record before the limit is found
        assert reader.find_boundary(size, size) is None  # nothing begins before an empty limit


def test_a_read_that_halts_at_damage_does_not_scan_past_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A range read from a guess is used only if it holds no damage: it ends at the first and
    leaves the scan for what follows to the read that is used."""
    path = clean_recording(tmp_path / "tail.ibrec", 300)
    size = path.stat().st_size
    with open(path, "r+b") as handle:
        handle.truncate(size + (64 << 20))
    read = 0
    pread = os.pread

    def counting(fd: int, length: int, offset: int) -> bytes:
        nonlocal read
        data = pread(fd, length, offset)
        read += len(data)
        return data

    monkeypatch.setattr(reader_module.os, "pread", counting)
    with RecordingReader(path, skip_damage=True) as reader:
        items = list(reader.items(halt_on_damage=True))
        assert isinstance(items[-1], Damage) and items[-1].offset == size
        assert items[-1].span == 0 and items[-1].resumed_at is None
        assert sum(isinstance(item, Damage) for item in items) == 1
        assert read < 2 * size + 4 * reader_module.READ_BLOCK, read
    with RecordingReader(path, skip_damage=True) as reader:
        read = 0
        assert any(isinstance(item, Damage) for item in reader.items())
        assert read >= 60 << 20


def job_of(**kwargs: Any) -> Job:
    shape = Shape(ExportFormat.TEXT, None, frozenset(), None, (), False, False, False, False)
    fields: dict[str, Any] = {
        "index": 3,
        "path": Path("x"),
        "guess": 100,
        "stop": 200,
        "exact": False,
        "seed": Seed({}, {}, 0, 0),
        "start_ns": None,
        "end_ns": None,
        "shape": shape,
        "identity": (1, 2),
        "text_cap": 1,
        "event_cap": 1,
    }
    return Job(**{**fields, **kwargs})


def test_a_range_with_no_record_in_it_is_accepted_only_if_the_walk_is_past_it() -> None:
    empty = Outcome(3, start=None, end=100)
    job = job_of()
    assert _Runner._accepted(job, empty, _Walk(end=200), 200)
    assert _Runner._accepted(job, empty, _Walk(end=900), 200)
    assert not _Runner._accepted(job, empty, _Walk(end=199), 200)
    # nothing is taken from a range that was not clean
    assert not _Runner._accepted(job, Outcome(3, start=None, crash="x"), _Walk(end=900), 200)
    assert not _Runner._accepted(job, Outcome(3, start=None, moved=True), _Walk(end=900), 200)


def test_a_record_longer_than_many_ranges_exports_the_same(tmp_path: Path) -> None:
    """Ranges that begin inside a record that is longer than they are hold no record: none is
    found in them, and the walk is already past each."""
    builder = builder_recording(400)
    builder.add(0x80, bytes(60000))  # a skippable record of unknown type
    for seq in range(401, 800):
        builder.frame(seq, data=hk(seq), stream=0, source=0, mid=0x890, time_ns=T0 + seq * STEP_NS)
    path = write_builder(builder, tmp_path / "long.ibrec")
    single = run_export(path)
    pooled = run_export(path, jobs=JOBS)
    assert_same(single, pooled)
    assert pooled.report.reruns == 0, pooled.report


# -- results are cut at the caps and continued exactly ------------------------------------------


@pytest.mark.parametrize("fmt_", list(ExportFormat))
def test_a_result_cut_at_the_text_cap_is_continued_into_the_same_export(
    clean: Path, fmt_: ExportFormat
) -> None:
    single = run_export(clean, fmt_)
    pooled = run_export(clean, fmt_, jobs=JOBS, text_cap=700)
    assert_same(single, pooled)
    assert pooled.report.continued > pooled.report.ranges, pooled.report


def test_a_result_holds_no_more_than_the_cap_and_one_row(clean: Path) -> None:
    cap = 2000
    pooled = run_export(clean, ExportFormat.JSONL, jobs=JOBS, text_cap=cap, text_budget=cap * 4)
    assert_same(run_export(clean, ExportFormat.JSONL), pooled)
    # a row of this recording is about 150 characters
    assert pooled.report.max_text_bytes <= 12 * (cap + 300), pooled.report


def test_wide_rows_make_shorter_ranges_that_stay_under_the_bounds(tmp_path: Path) -> None:
    """Rows 60 times as long as the frame: after the first window of ranges has shown it, ranges
    are planned short enough that their results fit the bounds (nothing is cut, nothing waits
    longer than the budget allows) and the export is still spread over all the workers."""
    path = wide_recording(tmp_path / "wide.ibrec", 1000, fields=200)
    cap, budget = 1 << 20, 4 << 20
    single = run_export(path, ExportFormat.TEXT)
    pooled = run_export(
        path,
        ExportFormat.TEXT,
        jobs=JOBS,
        pool=False,
        range_bytes=65536,
        text_cap=cap,
        text_budget=budget,
    )
    assert_same(single, pooled)
    report = pooled.report
    assert report.ranges >= 15, report  # 220 KiB of file, ranges of about 10 KiB after the first
    assert report.max_text_bytes <= budget + 2 * cap, report
    assert report.continued <= report.ranges // 4, report


@pytest.mark.parametrize("cap", [1, 2])
@pytest.mark.parametrize("skip", [False, True])
def test_damage_notes_are_cut_at_their_cap_and_continued(
    tmp_path: Path, cap: int, skip: bool
) -> None:
    rng = random.Random(cap)
    path = tmp_path / "many.ibrec"
    data = clean_recording(tmp_path / "base.ibrec", 1500).read_bytes()
    out = bytearray(data)
    for _ in range(25):
        at = rng.randrange(len(out) // 6, len(out) - 300)
        out[at : at + rng.choice((8, 40, 200))] = bytes(8)
    path.write_bytes(bytes(out))
    single = run_export(path, skip_damage=skip)
    pooled = run_export(path, jobs=JOBS, skip_damage=skip, event_cap=cap)
    assert_same(single, pooled, label=f"cap={cap} skip={skip}")
    if skip:
        assert single.stats["damage"] > 3 and pooled.report.continued >= 1, pooled.report


def test_gap_notes_are_cut_at_their_cap_and_continued(tmp_path: Path) -> None:
    path = write_builder(builder_recording(900, gaps_every=3), tmp_path / "gaps.ibrec")
    single = run_export(path, ExportFormat.TEXT, per_line=True)
    pooled = run_export(path, ExportFormat.TEXT, jobs=JOBS, per_line=True, event_cap=4)
    assert single.stats["gaps"] == 300
    assert_same(single, pooled)
    assert pooled.report.continued >= 20, pooled.report


def test_a_damage_flood_in_a_range_is_cut_and_reported_the_same(tmp_path: Path) -> None:
    """Thousands of stretches of damage in one range (a garbage-filled file read with
    ``--skip-damage``) are not all held by one result."""
    builder = builder_recording(40)
    for seq in range(41, 6041, 2):
        builder.out += bytes(16)  # zeros: damage; a valid record needs another behind it
        for step in (0, 1):
            builder.frame(
                seq + step,
                data=hk(seq + step),
                stream=0,
                source=0,
                mid=0x890,
                time_ns=T0 + (seq + step) * STEP_NS,
            )
    path = write_builder(builder, tmp_path / "flood.ibrec")
    single = run_export(path, skip_damage=True)
    pooled = run_export(path, jobs=JOBS, skip_damage=True, range_bytes=65536, event_cap=100)
    assert_same(single, pooled)
    assert single.stats["damage"] >= 2000
    assert pooled.report.continued >= 20, pooled.report


# -- an output that closes ends the export where the single process ends ------------------------


FIRST_RANGE = 65536  # the first range of these exports holds the damage and the rows before it


def damaged_at(tmp_path: Path, offset: int, count: int = 2500) -> Path:
    data = bytearray(clean_recording(tmp_path / "base.ibrec", count).read_bytes())
    data[offset : offset + 300] = bytes(300)
    path = tmp_path / "damaged.ibrec"
    path.write_bytes(bytes(data))
    return path


@pytest.mark.parametrize("refuse_after", [0, 1, 50, 600, 2400])
@pytest.mark.parametrize("fmt_", [ExportFormat.TEXT, ExportFormat.JSONL])
def test_damage_after_the_row_the_output_refused_is_not_reported(
    tmp_path: Path, fmt_: ExportFormat, refuse_after: int
) -> None:
    """``| head``: the single process never reads past the row the output refused, so damage
    further on, in the range that holds that row or any other, is neither noted nor counted."""
    path = damaged_at(tmp_path, 45_000)  # a few hundred frames in, in the first range
    for skip in (False, True):
        kwargs: dict[str, Any] = {
            "per_line": True,
            "refuse_after": refuse_after,
            "skip_damage": skip,
        }
        single = run_export(path, fmt_, **kwargs)
        pooled = run_export(path, fmt_, jobs=JOBS, range_bytes=FIRST_RANGE, **kwargs)
        label = f"skip={skip} refuse_after={refuse_after}"
        assert_same(single, pooled, counts=False, closed=True, label=label)
        assert single.stats["damage"] == (0 if refuse_after < 100 else 1), label  # the premise


@pytest.mark.parametrize("refuse_after", [0, 30, 111, 400])
def test_gaps_after_the_row_the_output_refused_are_not_counted(
    tmp_path: Path, refuse_after: int
) -> None:
    path = write_builder(builder_recording(1200, gaps_every=40), tmp_path / "gaps.ibrec")
    single = run_export(path, ExportFormat.TEXT, per_line=True, refuse_after=refuse_after)
    pooled = run_export(
        path,
        ExportFormat.TEXT,
        jobs=JOBS,
        per_line=True,
        refuse_after=refuse_after,
    )
    assert single.stats["gaps"] < 30
    assert_same(single, pooled, counts=False, closed=True)


# -- the file read is the file opened -----------------------------------------------------------


@pytest.mark.parametrize("how", ["replaced", "gone"])
def test_a_recording_moved_during_the_export_is_read_from_the_file_opened(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, how: str
) -> None:
    """The name is taken away from the file (renamed; and then given to another recording, or
    not) once the first range is written. One process holds its descriptor and reads on; the
    workers, which open by name, must not read the other file or fail."""
    path = clean_recording(tmp_path / "run.ibrec", 3000)
    other = tmp_path / "other.ibrec"
    options = RecorderOptions(min_free_bytes=0, chunk_bytes=6000)
    write_recording(other, mixed_frames(3000, start=7), options=options, streams=(RX, TX, SERIAL))
    single = run_export(path, ExportFormat.TEXT)
    original = _Runner._write
    state = {"moved": False}

    def write(self: _Runner, outcome: Outcome, reader: RecordingReader, where: Path) -> bool:
        if not state["moved"]:
            state["moved"] = True
            path.rename(tmp_path / "moved.ibrec")
            if how == "replaced":
                os.link(other, path)
        return original(self, outcome, reader, where)

    monkeypatch.setattr(_Runner, "_write", write)
    pooled = run_export(path, ExportFormat.TEXT, jobs=JOBS, pool=False, range_bytes=RANGE)
    assert state["moved"]
    assert pooled.error is None, pooled.error
    assert pooled.content == single.content
    assert pooled.report.local >= 1, pooled.report


def test_a_worker_whose_name_leads_elsewhere_reads_nothing(tmp_path: Path) -> None:
    path = clean_recording(tmp_path / "run.ibrec", 400)
    twin = clean_recording(tmp_path / "twin.ibrec", 400)  # the same bytes, another file
    replay = Replay([path])
    try:
        definitions = EmbeddedDefinitions.of(replay, "")
    finally:
        replay.close()
    worker = _Worker(definitions)
    with RecordingReader(path) as reader:
        job = dataclasses.replace(
            job_of(),
            path=path,
            guess=reader.body_start,
            stop=None,
            exact=True,
            identity=reader.identity,
            text_cap=1 << 30,
            event_cap=1000,
        )
    here = worker.run(job)
    assert not here.moved and here.start is not None and here.blocks
    for name in (twin, tmp_path / "gone", tmp_path):
        elsewhere = worker.run(dataclasses.replace(job, path=name))
        assert elsewhere.moved and elsewhere.start is None and not elsewhere.blocks, name


# -- how many workers, when nothing says -------------------------------------------------------


def cgroup(root: Path, membership: str, limits: dict[str, str]) -> tuple[Path, Path]:
    """A fake unified hierarchy under ``root``: ``limits`` maps a group's path to its cpu.max."""
    for group, limit in limits.items():
        directory = root / group
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "cpu.max").write_text(limit)
    listing = root.parent / "cgroup-of-process"
    listing.write_text(membership)
    return root, listing


def test_the_cpu_quota_of_the_control_group_is_the_smallest_up_the_tree(tmp_path: Path) -> None:
    root, listing = cgroup(
        tmp_path / "fs",
        "12:cpu:/ignored\n0::/a/b\n",
        {"": "max 100000\n", "a": "250000 100000\n", "a/b": "100000 100000\n"},
    )
    assert pexport.cpu_quota(root, listing) == 1
    root, listing = cgroup(
        tmp_path / "fs2", "0::/a/b\n", {"a": "250000 100000\n", "a/b": "max 100000\n"}
    )
    assert pexport.cpu_quota(root, listing) == 3  # rounded up
    root, listing = cgroup(tmp_path / "fs3", "0::/a/b\n", {"": "max 100000\n", "a/b": "max 100000"})
    assert pexport.cpu_quota(root, listing) is None


@pytest.mark.parametrize(
    "text",
    ["", "garbage", "0 100000", "-5 100000", "100 0", "100", "1 2 3", "99999999999999999999 1"],
)
def test_a_cpu_quota_that_is_not_one_is_ignored(tmp_path: Path, text: str) -> None:
    root, listing = cgroup(tmp_path / "fs", "0::/a\n", {"a": text})
    quota = pexport.cpu_quota(root, listing)
    assert quota is None or quota >= 1


def test_the_quota_of_a_group_that_is_not_under_the_root_falls_back_to_the_root(
    tmp_path: Path,
) -> None:
    root, listing = cgroup(tmp_path / "fs", "0::/../../etc\n", {"": "200000 100000"})
    assert pexport.cpu_quota(root, listing) == 2
    assert pexport.cpu_quota(tmp_path / "nothing", tmp_path / "no-such-file") is None


def test_the_default_number_of_workers_is_capped_and_respects_the_quota(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(os, "sched_getaffinity", lambda pid: set(range(200)))
    monkeypatch.setattr(pexport, "cpu_quota", lambda: None)
    assert pexport.default_jobs() == pexport.MAX_JOBS
    monkeypatch.setattr(pexport, "cpu_quota", lambda: 3)
    assert pexport.default_jobs() == 3
    monkeypatch.setattr(os, "sched_getaffinity", lambda pid: {0})
    assert pexport.default_jobs() == 1


# -- the pool is made with the shutdown signals blocked -----------------------------------------


def test_the_pool_and_the_helpers_it_starts_are_made_with_the_shutdown_signals_blocked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import concurrent.futures as cf

    seen: list[set[int]] = []
    real = cf.ProcessPoolExecutor

    def building(*args: Any, **kwargs: Any) -> cf.ProcessPoolExecutor:
        seen.append({int(s) for s in signal.pthread_sigmask(signal.SIG_BLOCK, [])})
        return real(*args, **kwargs)

    monkeypatch.setattr(cf, "ProcessPoolExecutor", building)
    definitions = pexport.EmbeddedDefinitions(
        Path("x"),
        {},
        "",
        (),
        {"schema": "x"},
        2048,
        "",  # type: ignore[arg-type]
    )
    pool = pexport.WorkerPool(definitions, 2)
    try:
        # a job that no worker will accept is enough to build the executor
        future = pool.submit(job_of())
        assert future is not None
    finally:
        pool.close(abnormal=True)
    assert len(seen) == 1
    for signum in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM):
        assert int(signum) in seen[0], signum


_MASKS = """
import json, signal, sys
import multiprocessing.context as context
from pathlib import Path
from ironbird.recording import pexport
seen = []
start = context.SpawnProcess.start
def recording(self):
    seen.append(sorted(int(s) for s in signal.pthread_sigmask(signal.SIG_BLOCK, [])))
    return start(self)
context.SpawnProcess.start = recording
definitions = pexport.EmbeddedDefinitions(Path("x"), {}, "", (), {}, 2048, "")
pool = pexport.WorkerPool(definitions, 2)
job = pexport.Job(0, Path("x"), 0, None, True, pexport.Seed({}, {}, 0, 0), None, None,
    pexport.Shape(pexport.ExportFormat.TEXT, None, frozenset(), None, (), False, False, False,
                  False), (0, 0), 1, 1)
try:
    pool.submit(job)
    pool.submit(job)
finally:
    pool.close(abnormal=True)
json.dump(seen, sys.stdout)
"""


def test_workers_are_started_with_the_shutdown_signals_blocked_even_by_the_first_pool(
    tmp_path: Path,
) -> None:
    """The first pool of a process starts the resource tracker, and multiprocessing unblocks
    SIGINT and SIGTERM in the thread that did it: the workers started after must still have all
    three blocked (a group-wide Ctrl-C otherwise kills one that is starting, with a traceback)."""
    import json
    import subprocess
    import sys

    done = subprocess.run(
        [sys.executable, "-c", _MASKS],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
        cwd=tmp_path,
    )
    assert done.returncode == 0, done.stderr
    seen = json.loads(done.stdout)
    assert len(seen) >= 2, "workers were started"
    for mask in seen:
        for signum in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM):
            assert int(signum) in mask, (signum, mask)


_PENDING = """
import os, signal
from pathlib import Path
from ironbird.recording import pexport
signal.pthread_sigmask(signal.SIG_BLOCK, [signal.SIGINT, signal.SIGHUP, signal.SIGTERM])
os.kill(os.getpid(), signal.SIGINT)
os.kill(os.getpid(), signal.SIGHUP)
pexport._init_worker(pexport.EmbeddedDefinitions(Path("x"), {}, "", (), {}, 2048, ""), os.getppid())
print(*(signal.getsignal(s) == signal.SIG_IGN for s in (signal.SIGINT, signal.SIGHUP)))
"""


def test_a_worker_drops_the_interrupt_that_came_while_it_started() -> None:
    """A group-wide Ctrl-C that lands while a worker is starting is pending behind the mask the
    worker inherits: lifting the mask must not raise it in the middle of the worker's setup."""
    import subprocess
    import sys

    done = subprocess.run(
        [sys.executable, "-c", _PENDING], capture_output=True, text=True, timeout=60, check=False
    )
    assert done.returncode == 0 and done.stdout.split() == ["True", "True"], done.stderr


# -- the differential on a recording damaged at random ------------------------------------------


@pytest.mark.parametrize("seed", range(6))
def test_random_damage_with_tiny_caps_and_an_output_that_closes_exports_the_same(
    tmp_path: Path, seed: int
) -> None:
    rng = random.Random(seed)
    path = tmp_path / "mutated.ibrec"
    base = clean_recording(tmp_path / "base.ibrec", 1500)
    path.write_bytes(mutate(base.read_bytes(), rng))
    refuse = rng.choice((None, 5, 200, 900))
    for skip in (False, True):
        single = run_export(
            path, ExportFormat.TEXT, per_line=True, refuse_after=refuse, skip_damage=skip
        )
        pooled = run_export(
            path,
            ExportFormat.TEXT,
            jobs=JOBS,
            per_line=True,
            refuse_after=refuse,
            skip_damage=skip,
            text_cap=rng.choice((1, 500, 5000)),
            event_cap=rng.choice((1, 3, 1000)),
        )
        assert_same(single, pooled, counts=False, closed=refuse is not None, label=f"{seed} {skip}")
