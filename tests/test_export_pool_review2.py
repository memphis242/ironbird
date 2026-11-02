"""Parallel export decode (plan 2m), the findings of the second review: an output that fails part
way is told of the damage and gaps the single process had met by then, a boundary is before its
limit, the control group is read defensively, and a range the parent reads itself decodes with the
parent's codecs."""

from __future__ import annotations

import errno
import random
from pathlib import Path
from typing import Any

import pytest

import poolutil
from ironbird.errors import Code, IronbirdError
from ironbird.recording import export as export_module
from ironbird.recording import pexport
from ironbird.recording.export import FILE_BUFFER_BYTES, ExportFormat, FileDestination
from ironbird.recording.reader import RecordingReader
from poolutil import (
    JOBS,
    RANGE,
    Capture,
    assert_same,
    builder_recording,
    clean_recording,
    run_export,
    write_builder,
)

# -- an output that fails part way ---------------------------------------------------------------


class Disk:
    """The temporary file of a :class:`FileDestination`, with room for ``limit`` bytes."""

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.written: list[bytes] = []
        self.size = 0

    def write(self, data: bytes) -> None:
        if self.size + len(data) > self.limit:
            raise OSError(errno.ENOSPC, "No space left on device")
        self.size += len(data)
        self.written.append(data)


class FullDisk(Capture):
    """The real file destination over a disk that fills up: what reaches the disk is the content,
    and a flush that fails is ``IB-EXP-001``."""

    limit = 1 << 40

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.disk = Disk(self.limit)
        self.file = FileDestination(self.disk)  # type: ignore[arg-type]

    def member(self, name: str) -> int:
        self.members.append(name)
        return self.file.member(name)

    def write(self, handle: int, text: str) -> bool:
        try:
            return self.file.write(handle, text)
        except export_module.OutputError as exc:
            raise IronbirdError(Code.EXP_WRITE, f"cannot write: {exc}") from exc

    def flush(self) -> None:
        try:
            self.file.flush()
        except export_module.OutputError as exc:
            raise IronbirdError(Code.EXP_WRITE, f"cannot write: {exc}") from exc

    def content(self) -> dict[int, str]:
        return {0: b"".join(self.disk.written).decode("utf-8")}


def text_size(path: Path) -> int:
    run = run_export(path, ExportFormat.TEXT)
    assert run.error is None
    return sum(len(t) for t in run.content.values())


def failing_disk(monkeypatch: pytest.MonkeyPatch, limit: int) -> None:
    monkeypatch.setattr("poolutil.Capture", FullDisk)
    monkeypatch.setattr(FullDisk, "limit", limit)


def test_a_disk_that_fills_up_is_told_of_the_damage_the_single_process_had_met(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = clean_recording(tmp_path / "damaged.ibrec", count=40000)
    data = bytearray(path.read_bytes())
    at = int(len(data) * 0.6) & ~7
    data[at : at + 64] = bytes(64)
    path.write_bytes(bytes(data))
    total = text_size(path)
    assert total > 4 * FILE_BUFFER_BYTES
    for fraction in (0.1, 0.35, 0.42, 0.46, 0.5, 0.54, 0.58, 0.65, 0.8, 0.97):
        failing_disk(monkeypatch, int(total * fraction))
        single = run_export(path, ExportFormat.TEXT)
        pooled = run_export(path, ExportFormat.TEXT, jobs=JOBS, range_bytes=1 << 20)
        assert single.error is not None and single.error[0] == Code.EXP_WRITE.value, fraction
        assert_same(single, pooled, counts=False, label=f"{fraction}")


def test_a_disk_that_fills_up_is_told_of_the_gaps_the_single_process_had_met(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = write_builder(builder_recording(12000, gaps_every=40), tmp_path / "gaps.ibrec")
    total = text_size(path)
    assert total > 2 * FILE_BUFFER_BYTES
    failures = 0
    for fraction in (0.1, 0.3, 0.5, 0.7, 0.9, 0.99):
        failing_disk(monkeypatch, int(total * fraction))
        single = run_export(path, ExportFormat.TEXT)
        pooled = run_export(path, ExportFormat.TEXT, jobs=JOBS, range_bytes=RANGE)
        failures += single.error is not None
        assert_same(single, pooled, counts=False, label=f"{fraction}")
    assert failures


def test_a_block_of_lines_reaches_the_file_in_the_pieces_the_same_lines_one_at_a_time_do() -> None:
    rng = random.Random(7)
    lines = ["x" * rng.randrange(1, 5000) + "\n" for _ in range(400)]

    def pieces(blocks: list[list[str]]) -> list[bytes]:
        disk = Disk(1 << 40)
        destination = FileDestination(disk)  # type: ignore[arg-type]
        destination.member("a")
        for block in blocks:
            destination.write(0, "".join(block))
        destination.flush()
        return disk.written

    one_by_one = pieces([[line] for line in lines])
    assert len(one_by_one) > 3
    for size in (1, 2, 7, 50, 400):
        blocks = [lines[i : i + size] for i in range(0, len(lines), size)]
        assert pieces(blocks) == one_by_one, size


# -- a boundary is before its limit --------------------------------------------------------------


def test_a_record_that_begins_at_the_limit_is_not_before_it(tmp_path: Path) -> None:
    path = clean_recording(tmp_path / "c.ibrec", 300)
    with RecordingReader(path) as reader:
        first = reader.find_boundary(reader.body_start)
        assert first is not None
        assert reader.find_boundary(reader.body_start, first + 8) == first
        assert reader.find_boundary(reader.body_start, first) is None
        assert reader.find_boundary(first, first + 8) == first
        assert reader.find_boundary(first + 1, first + 8) is None


# -- the control group ---------------------------------------------------------------------------


def test_a_control_group_that_cannot_be_examined_is_no_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "fs"
    (root / "a").mkdir(parents=True)
    (root / "a" / "cpu.max").write_text("100000 100000\n")
    listing = tmp_path / "membership"
    listing.write_text("0::/a\n")
    assert pexport.cpu_quota(root, listing) == 1
    real = Path.is_dir

    def refused(self: Path) -> bool:
        if self == root / "a":
            raise PermissionError(errno.EACCES, "Permission denied", str(self))
        return real(self)

    monkeypatch.setattr(Path, "is_dir", refused)
    assert pexport.cpu_quota(root, listing) is None
    monkeypatch.setattr(
        Path, "is_dir", lambda self: (_ for _ in ()).throw(OSError(errno.EIO, "I/O error"))
    )
    assert pexport.cpu_quota(root, listing) is None


def test_the_membership_of_a_host_with_many_hierarchies_is_read_past_its_first_bytes(
    tmp_path: Path,
) -> None:
    root = tmp_path / "fs"
    (root / "a").mkdir(parents=True)
    (root / "a" / "cpu.max").write_text("200000 100000\n")
    listing = tmp_path / "membership"
    legacy = "".join(f"{n}:name={'v' * 60}{n}:/some/path\n" for n in range(1, 40))
    assert len(legacy) > 256 * 4  # the unified line is far beyond where a short read stops
    listing.write_text(legacy + "0::/a\n")
    assert pexport.cpu_quota(root, listing) == 2


# -- a range the parent reads itself -------------------------------------------------------------


class Unbuildable:
    """Definitions that cannot be built again: the files they came from changed."""

    def build(self) -> dict[str, Any]:
        raise AssertionError("the parent decodes with the codecs it has")


def test_a_worker_given_codecs_does_not_build_them_again() -> None:
    codecs: dict[str, Any] = {}
    worker = pexport._Worker(Unbuildable(), codecs)  # type: ignore[arg-type]
    assert worker.codecs is codecs


def test_the_options_of_an_export_carry_the_codecs_it_decodes_with(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = clean_recording(tmp_path / "c.ibrec", 300)
    seen: list[Any] = []
    real = pexport.make_options

    def spying(*args: Any, **kwargs: Any) -> pexport.ParallelOptions:
        options = real(*args, **kwargs)
        seen.append(options.codecs)
        return options

    monkeypatch.setattr(poolutil, "make_options", spying)
    run_export(path, ExportFormat.TEXT, jobs=JOBS, pool=False)
    assert seen and seen[0]
