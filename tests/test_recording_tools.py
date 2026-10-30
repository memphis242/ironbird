"""``rec repair`` and the temporary-file commit it uses."""

from __future__ import annotations

import errno
import hashlib
import os
import re
from pathlib import Path

import pytest

from ironbird import atomicfile, fsops
from ironbird.errors import Code, IronbirdError
from ironbird.recording import format as fmt
from ironbird.recording import inspect as rec_inspect
from ironbird.recording.format import MarkKind, RecordType
from ironbird.recording.reader import Item, RecordingReader
from recutil import Builder, sample


def frames_of(path: Path) -> list[object]:
    with RecordingReader(path) as reader:
        return list(reader.frames())


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


# -- the temporary commit --


def test_commit_creates_the_final_name_and_leaves_no_temporary(tmp_path: Path) -> None:
    final = tmp_path / "out.bin"
    with atomicfile.TempOutput(final) as out:
        assert re.fullmatch(r"\.out\.bin\.[0-9a-f]{16}\.tmp", out.name)
        assert (tmp_path / out.name).stat().st_mode & 0o777 == 0o600
        out.write(b"hello")
        out.commit()
    assert final.read_bytes() == b"hello" and not list(tmp_path.glob(".*"))


def test_an_existing_final_name_is_never_replaced_without_force(tmp_path: Path) -> None:
    final = tmp_path / "out.bin"
    final.write_bytes(b"old")
    with pytest.raises(atomicfile.Collision), atomicfile.TempOutput(final) as out:
        out.write(b"new")
        out.commit()
    assert final.read_bytes() == b"old" and not list(tmp_path.glob(".*"))


def test_force_replaces_a_regular_file_but_not_a_directory(tmp_path: Path) -> None:
    final = tmp_path / "out.bin"
    final.write_bytes(b"old")
    with atomicfile.TempOutput(final, force=True) as out:
        out.write(b"new")
        out.commit()
    assert final.read_bytes() == b"new"
    directory = tmp_path / "dir"
    directory.mkdir()
    with pytest.raises(OSError), atomicfile.TempOutput(directory, force=True) as out:
        out.write(b"x")
        out.commit()
    assert directory.is_dir() and not list(tmp_path.glob(".*"))


def test_failure_removes_only_our_temporary(tmp_path: Path) -> None:
    bystander = tmp_path / ".out.bin.0000000000000000.tmp"
    bystander.write_bytes(b"not ours")
    with pytest.raises(RuntimeError), atomicfile.TempOutput(tmp_path / "out.bin") as out:
        out.write(b"partial")
        raise RuntimeError("boom")
    assert [p.name for p in tmp_path.glob(".*")] == [bystander.name]


def test_a_renamed_temporary_is_not_unlinked_by_name(tmp_path: Path) -> None:
    """If the temporary name was swapped for another file, the failure helper leaves it alone."""
    with pytest.raises(RuntimeError), atomicfile.TempOutput(tmp_path / "out.bin") as out:
        os.rename(tmp_path / out.name, tmp_path / "moved")
        (tmp_path / out.name).write_bytes(b"someone else's file")
        raise RuntimeError("boom")
    assert (tmp_path / "moved").exists()
    leftovers = list(tmp_path.glob(".out.bin.*.tmp"))
    assert len(leftovers) == 1 and leftovers[0].read_bytes() == b"someone else's file"


class _NoRenameat2(fsops.FileSystem):
    def renameat2_noreplace(self, dir_fd: int, old: str, new: str) -> bool | None:
        return None


class _NoLinks(_NoRenameat2):
    def link_at(self, dir_fd: int, old: str, new: str) -> None:
        raise OSError(errno.EPERM, "no hard links here")


@pytest.mark.parametrize("path_taken", ["renameat2", "link", "placeholder"])
def test_every_commit_path_refuses_a_collision_and_commits_otherwise(
    tmp_path: Path, path_taken: str
) -> None:
    fs = {
        "renameat2": fsops.REAL_FS,
        "link": _NoRenameat2(),
        "placeholder": _NoLinks(),
    }[path_taken]
    final = tmp_path / "out.bin"
    with atomicfile.TempOutput(final, fs=fs) as out:
        out.write(b"first")
        out.commit()
    assert final.read_bytes() == b"first"
    with pytest.raises(atomicfile.Collision), atomicfile.TempOutput(final, fs=fs) as out:
        out.write(b"second")
        out.commit()
    assert final.read_bytes() == b"first" and not list(tmp_path.glob(".*"))


# -- repair --


def test_repair_of_a_clean_recording_is_byte_identical(tmp_path: Path) -> None:
    source = tmp_path / "a.ibrec"
    sample(source, 50, marks=lambda r: r.mark(MarkKind.NOTE, "x"))
    result = rec_inspect.repair(source, tmp_path / "b.ibrec")
    assert (tmp_path / "b.ibrec").read_bytes() == source.read_bytes()
    assert result.damage is None and result.frames == 50 and result.dropped_bytes == 0


def test_repair_of_a_torn_recording_keeps_exactly_the_readable_prefix(tmp_path: Path) -> None:
    source = tmp_path / "a.ibrec"
    written = sample(source, 60)
    raw = source.read_bytes()
    with RecordingReader(source) as reader:
        records = [i for i in reader.items() if isinstance(i, Item)]
    frames = [i for i in records if i.rtype == RecordType.FRAME]
    for cut in (frames[20].end - 5, frames[20].end, frames[40].offset + 1, len(raw) - 3):
        torn = tmp_path / "torn.ibrec"
        torn.write_bytes(raw[:cut])
        before = digest(torn)
        out = tmp_path / f"fixed-{cut}.ibrec"
        result = rec_inspect.repair(torn, out)
        assert digest(torn) == before  # the original is never modified
        assert frames_of(out) == frames_of(torn) == written[: result.frames]
        report = rec_inspect.scan(out)
        assert report.status == "closed" and not report.findings and not report.holes


def test_repair_summarises_the_unfinished_chunk(tmp_path: Path) -> None:
    b = Builder().standard(0)
    for seq in range(1, 8):
        b.frame(seq, stream=0, mid=0x100 + seq % 2)
    source = b.write(tmp_path / "a.ibrec")  # no chunk, index or trailer at all
    out = tmp_path / "b.ibrec"
    result = rec_inspect.repair(source, out)
    assert result.frames == 7
    info = rec_inspect.info(out)
    assert info.source == "index" and info.frames == 7 and info.mids == {0x100: 3, 0x101: 4}
    assert rec_inspect.scan(out).findings == []


def test_repair_stops_at_mid_file_damage_and_counts_what_lies_beyond(tmp_path: Path) -> None:
    source = tmp_path / "a.ibrec"
    sample(source, 40)
    with RecordingReader(source) as reader:
        victim = [i for i in reader.items() if isinstance(i, Item) and i.rtype == RecordType.FRAME]
    raw = bytearray(source.read_bytes())
    raw[victim[9].offset + 25] ^= 0x55
    source.write_bytes(bytes(raw))
    result = rec_inspect.repair(source, tmp_path / "b.ibrec")
    assert result.frames == 9 and result.frames_beyond == 30
    assert result.damage is not None and not result.damage.torn


def test_repair_refuses_the_input_as_output_even_with_force(tmp_path: Path) -> None:
    source = tmp_path / "a.ibrec"
    sample(source, 5)
    before = digest(source)
    (tmp_path / "hard.ibrec").hardlink_to(source)
    (tmp_path / "soft.ibrec").symlink_to(source)
    for alias in (
        source,
        tmp_path / "hard.ibrec",
        tmp_path / "soft.ibrec",
        tmp_path / "x" / ".." / "a.ibrec",
    ):
        with pytest.raises(IronbirdError) as caught:
            rec_inspect.repair(source, alias, force=True)
        assert caught.value.code is Code.REC_CREATE
    assert digest(source) == before and not list(tmp_path.glob(".*"))


def test_repair_failure_removes_its_temporary_and_keeps_an_existing_output(tmp_path: Path) -> None:
    source = tmp_path / "a.ibrec"
    sample(source, 5)
    existing = tmp_path / "o.ibrec"
    existing.write_bytes(b"keep")
    with pytest.raises(IronbirdError) as caught:
        rec_inspect.repair(source, existing)
    assert caught.value.code is Code.REC_CREATE and existing.read_bytes() == b"keep"
    with pytest.raises(IronbirdError):
        rec_inspect.repair(source, tmp_path / "no-such-dir" / "o.ibrec")
    assert not list(tmp_path.glob(".*"))


def test_repair_of_a_split_part_and_empty_recording(tmp_path: Path) -> None:
    source = tmp_path / "e.ibrec"
    sample(source, 0)
    out = tmp_path / "f.ibrec"
    result = rec_inspect.repair(source, out)
    assert result.frames == 0 and out.read_bytes() == source.read_bytes()
    assert out.stat().st_size > fmt.PREAMBLE_SIZE
