"""``ib export`` end to end: where the output may go, how it is committed, exit codes, damaged
input, and the temporary files that never outlive a failure. The cells and tables themselves are in
test_export.py."""

from __future__ import annotations

import contextlib
import errno
import fcntl
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest

from exportutil import (
    NullDestination,
    export_in_process,
    hk_and_status,
    many_message_recording,
    read_dicts,
    two_node_recording,
)
from ironbird import atomicfile, fsops
from ironbird.atomicfile import Collision, TempDirOutput, TempOutput
from ironbird.errors import Code, IronbirdError
from ironbird.recording.defs import embedded_codecs
from ironbird.recording.export import (
    ExportFormat,
    ExportResult,
    export,
    input_guard,
    write_to_directory,
    write_to_file,
)
from ironbird.recording.format import RecordType
from ironbird.recording.reader import RecordingReader
from ironbird.recording.replay import Replay
from recutil import CLI_MAIN, run_ib
from replayutil import child_env, hk_frames, write_recording
from support.faultfs import FaultFs
from test_replay import split_set

LEDGER_NAME = "20261031T010203Z-0123456789ab.jsonl"
RECORDING_NAME = "20261031T010203Z-0123456789ab.ibrec"
RESERVE_NAME = "20261031T010203Z-0123456789ab.r0001.reserve"
SALVAGE_TMP = ".20261031T010203Z-0123456789ab.p0002.jsonl.0123456789abcdef.tmp"


@pytest.fixture
def env(tmp_path: Path) -> dict[str, str]:
    for name in ("state", "recordings"):
        (tmp_path / name).mkdir(mode=0o700)
    return child_env(tmp_path)


@pytest.fixture
def rec(tmp_path: Path) -> Path:
    return hk_and_status(tmp_path / "in.ibrec", 6, 4)


def ib(tmp_path: Path, env: dict[str, str], *args: str) -> subprocess.CompletedProcess[str]:
    return run_ib(tmp_path, ["export", *args], env)


def temporaries(directory: Path) -> list[str]:
    return sorted(p.name for p in directory.iterdir() if p.name.endswith(".tmp"))


def ledger_events(tmp_path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for path in sorted((tmp_path / "state").glob("*.jsonl")):
        out += [json.loads(line) for line in path.read_text().splitlines()]
    return out


# -- the command ----


def test_a_csv_directory_a_single_file_stdout_and_the_other_formats(
    tmp_path: Path, env: dict[str, str], rec: Path
) -> None:
    done = ib(tmp_path, env, str(rec), "-o", str(tmp_path / "out"))
    assert done.returncode == 0, done.stderr
    assert sorted(p.name for p in (tmp_path / "out").iterdir()) == [
        "MOCK_HK.csv",
        "MOCK_STATUS.csv",
        "malformed.csv",
    ]
    assert "wrote 10 row(s) to directory" in done.stderr and not done.stdout
    assert oct((tmp_path / "out").stat().st_mode & 0o777) == "0o700"
    assert oct((tmp_path / "out" / "MOCK_HK.csv").stat().st_mode & 0o777) == "0o600"

    one = ib(tmp_path, env, str(rec), "--message", "MOCK_HK", "-o", str(tmp_path / "hk.csv"))
    assert one.returncode == 0 and len(read_dicts(tmp_path / "hk.csv")) == 6

    to_stdout = ib(tmp_path, env, str(rec), "--message", "MOCK_STATUS")
    assert to_stdout.returncode == 0, to_stdout.stderr
    lines = to_stdout.stdout.splitlines()
    assert len(lines) == 5 and lines[0].startswith("rx_time_utc,rx_time_ns,seq,node,link,mid,")

    text = ib(tmp_path, env, str(rec), "--format", "text")
    assert text.returncode == 0 and len(text.stdout.splitlines()) == 10
    jsonl = ib(tmp_path, env, str(rec), "--format", "jsonl", "-o", str(tmp_path / "e.jsonl"))
    assert jsonl.returncode == 0
    records = [json.loads(line) for line in (tmp_path / "e.jsonl").read_text().splitlines()]
    assert len(records) == 10 and records[0]["name"] == "MOCK_HK"
    assert temporaries(tmp_path) == []


def test_the_ledger_has_the_start_and_the_stop_with_the_counts(
    tmp_path: Path, env: dict[str, str], rec: Path
) -> None:
    ib(tmp_path, env, str(rec), "-o", str(tmp_path / "out"), "--where", "cmd_counter > 2")
    events = ledger_events(tmp_path)
    start = next(e for e in events if e["event"] == "export.start")
    stop = next(e for e in events if e["event"] == "export.stop")
    assert start["where"] == "cmd_counter > 2" and start["format"] == "csv"
    assert stop["reason"] == "done" and stop["rows"] == 4 and stop["decoded"] == 4
    assert stop["tables"] == {"malformed": 0, "MOCK_HK": 4} and stop["read"] == 10


@pytest.mark.parametrize(
    ("args", "needle"),
    [
        (["--format", "text", "--header-fields"], "csv only"),
        (["--format", "jsonl", "--raw-enums"], "csv only"),
        ([], "pass -o DIR"),  # several tables need a directory
        (["--message", "MOCK_HK", "--message", "MOCK_STATUS"], "pass -o DIR"),
        (["--message", "MOCK_HK", "--force"], "--force needs -o"),
        (["--message", "MOCK_NOPE"], "no such message"),
        (["-o", "x", "--from", "+5s", "--to", "+1s"], "earlier"),
        (["-o", "x", "--where", "cmd_counter >>> 1"], "IB-FLT-001"),
        (["-o", "x", "--where", "nosuch == 1"], "IB-FLT-002"),
    ],
)
def test_usage_errors_exit_2_and_write_nothing(
    tmp_path: Path, env: dict[str, str], rec: Path, args: list[str], needle: str
) -> None:
    done = ib(tmp_path, env, str(rec), *args)
    assert done.returncode == 2, done.stderr
    assert needle in done.stderr and "Traceback" not in done.stderr
    assert sorted(p.name for p in tmp_path.iterdir()) == ["in.ibrec", "recordings", "state"]


def test_a_nonexistent_input_and_a_file_that_is_not_a_recording_create_nothing(
    tmp_path: Path, env: dict[str, str]
) -> None:
    (tmp_path / "junk").write_bytes(b"not a recording" * 10)
    for source, code in (("missing.ibrec", "IB-REC-004"), ("junk", "IB-REC-004")):
        done = ib(tmp_path, env, str(tmp_path / source), "-o", str(tmp_path / "out"))
        assert done.returncode == 1 and f"error {code}" in done.stderr, done.stderr
    assert not (tmp_path / "out").exists() and temporaries(tmp_path) == []


# -- the output rules ----


def test_an_existing_output_is_refused_without_force_and_replaced_with_it(
    tmp_path: Path, env: dict[str, str], rec: Path
) -> None:
    out = tmp_path / "hk.csv"
    out.write_text("precious")
    done = ib(tmp_path, env, str(rec), "--message", "MOCK_HK", "-o", str(out))
    assert done.returncode == 1 and "error IB-EXP-002" in done.stderr and "--force" in done.stderr
    assert out.read_text() == "precious" and temporaries(tmp_path) == []
    forced = ib(tmp_path, env, str(rec), "--message", "MOCK_HK", "-o", str(out), "--force")
    assert forced.returncode == 0, forced.stderr
    assert len(read_dicts(out)) == 6 and temporaries(tmp_path) == []


@pytest.mark.parametrize("how", ["same", "relative", "dotdot", "symlink", "hardlink"])
@pytest.mark.parametrize("force", [False, True])
def test_the_output_may_never_be_the_input(
    tmp_path: Path, env: dict[str, str], rec: Path, how: str, force: bool
) -> None:
    before = rec.read_bytes()
    (tmp_path / "sub").mkdir()
    target = {
        "same": rec,
        "relative": Path("in.ibrec"),
        "dotdot": tmp_path / "sub" / ".." / "in.ibrec",
        "symlink": tmp_path / "link.csv",
        "hardlink": tmp_path / "hard.csv",
    }[how]
    if how == "symlink":
        target.symlink_to(rec)
    if how == "hardlink":
        os.link(rec, target)
    args = [str(rec), "--format", "text", "-o", str(target)]
    done = ib(tmp_path, env, *args, *(["--force"] if force else []))
    assert done.returncode == 1 and "error IB-EXP-002" in done.stderr, done.stderr
    assert rec.read_bytes() == before
    assert temporaries(tmp_path) == [] and temporaries(rec.parent) == []


def test_the_output_may_not_be_a_part_of_the_split_recording_being_exported(
    tmp_path: Path, env: dict[str, str]
) -> None:
    from ironbird.recording.writer import part_path

    base, _ = split_set(tmp_path)
    second = part_path(base, 2)
    before = second.read_bytes()
    done = ib(tmp_path, env, str(base), "--format", "text", "-o", str(second), "--force")
    assert done.returncode == 1 and "error IB-EXP-002" in done.stderr, done.stderr
    assert second.read_bytes() == before and temporaries(tmp_path) == []


@pytest.mark.parametrize(
    "name",
    [
        LEDGER_NAME,
        RECORDING_NAME,
        RESERVE_NAME,
        "20261031T010203Z-0123456789ab.p0002.ibrec",
        SALVAGE_TMP,
    ],
)
@pytest.mark.parametrize("where", ["state", "recordings"])
@pytest.mark.parametrize("force", [False, True])
def test_the_output_may_not_be_named_like_a_file_retention_manages(
    tmp_path: Path, env: dict[str, str], rec: Path, name: str, where: str, force: bool
) -> None:
    out = tmp_path / where / name
    done = ib(
        tmp_path, env, str(rec), "--format", "text", "-o", str(out), *(["--force"] if force else [])
    )
    assert done.returncode == 1 and "error IB-EXP-002" in done.stderr, done.stderr
    assert ("state directory" if where == "state" else "manages") in done.stderr
    assert not out.exists() and temporaries(tmp_path / where) == []


def test_the_managed_directory_is_found_by_inode_so_a_symlinked_path_does_not_hide_it(
    tmp_path: Path, env: dict[str, str], rec: Path
) -> None:
    (tmp_path / "alias").symlink_to(tmp_path / "state")
    done = ib(
        tmp_path, env, str(rec), "--format", "text", "-o", str(tmp_path / "alias" / LEDGER_NAME)
    )
    assert done.returncode == 1 and "error IB-EXP-002" in done.stderr
    relative = ib(
        tmp_path, env, str(rec), "--format", "text", "-o", f"state/../state/{LEDGER_NAME}"
    )
    assert relative.returncode == 1 and "error IB-EXP-002" in relative.stderr


def test_managed_looking_names_elsewhere_and_ordinary_names_in_the_recording_directory_are_fine(
    tmp_path: Path, env: dict[str, str], rec: Path
) -> None:
    (tmp_path / "elsewhere").mkdir()
    for out in (
        tmp_path / "elsewhere" / LEDGER_NAME,
        tmp_path / "recordings" / "export.txt",
    ):
        done = ib(tmp_path, env, str(rec), "--format", "text", "-o", str(out))
        assert done.returncode == 0, done.stderr
        assert len(out.read_text().splitlines()) == 10


@pytest.mark.parametrize(
    "name", ["export.txt", "notes.csv", "out", f".{LEDGER_NAME}.0123456789abcdef.tmp", SALVAGE_TMP]
)
@pytest.mark.parametrize("force", [False, True])
@pytest.mark.parametrize("as_directory", [False, True])
def test_nothing_may_be_exported_into_the_state_directory_under_any_name(
    tmp_path: Path, env: dict[str, str], rec: Path, name: str, force: bool, as_directory: bool
) -> None:
    out = tmp_path / "state" / name
    args = [str(rec), "-o", str(out)] if as_directory else [str(rec), "--format", "text"]
    if not as_directory:
        args += ["-o", str(out)]
    before = sorted(p.name for p in (tmp_path / "state").iterdir())
    done = ib(tmp_path, env, *args, *(["--force"] if force else []))
    assert done.returncode == 1 and "error IB-EXP-002" in done.stderr, done.stderr
    assert "state directory" in done.stderr and not out.exists()
    assert temporaries(tmp_path / "state") == [n for n in before if n.endswith(".tmp")]


def test_a_directory_export_never_replaces_anything(
    tmp_path: Path, env: dict[str, str], rec: Path
) -> None:
    existing = tmp_path / "out"
    existing.mkdir()
    (existing / "keep.txt").write_text("mine")
    for force in (False, True):
        done = ib(tmp_path, env, str(rec), "-o", str(existing), *(["--force"] if force else []))
        assert done.returncode == 1 and "error IB-EXP-002" in done.stderr, done.stderr
        assert sorted(p.name for p in existing.iterdir()) == ["keep.txt"]
    a_file = tmp_path / "file"
    a_file.write_text("x")
    forced = ib(tmp_path, env, str(rec), "-o", str(a_file), "--force")
    assert forced.returncode == 1 and a_file.read_text() == "x"
    assert temporaries(tmp_path) == []


def test_force_replaces_only_a_regular_file(tmp_path: Path, env: dict[str, str], rec: Path) -> None:
    (tmp_path / "dir").mkdir()
    (tmp_path / "dir" / "keep").write_text("mine")
    (tmp_path / "target").write_text("target")
    (tmp_path / "link").symlink_to(tmp_path / "target")
    for out in (tmp_path / "dir", tmp_path / "link"):
        done = ib(tmp_path, env, str(rec), "--format", "text", "-o", str(out), "--force")
        assert done.returncode == 1 and "error IB-EXP-002" in done.stderr, done.stderr
        assert "regular file" in done.stderr
    assert (tmp_path / "dir" / "keep").read_text() == "mine"
    assert (tmp_path / "target").read_text() == "target" and (tmp_path / "link").is_symlink()
    assert temporaries(tmp_path) == []


def test_force_never_replaces_a_file_another_process_holds_locked(
    tmp_path: Path, env: dict[str, str], rec: Path
) -> None:
    out = tmp_path / "live.txt"
    out.write_text("being written")
    with out.open("rb") as holder:
        fcntl.flock(holder, fcntl.LOCK_EX)
        done = ib(tmp_path, env, str(rec), "--format", "text", "-o", str(out), "--force")
    assert done.returncode == 1 and "error IB-EXP-002" in done.stderr
    assert "in use" in done.stderr and out.read_text() == "being written"
    assert temporaries(tmp_path) == []


def test_a_missing_directory_is_a_write_error_and_leaves_nothing(
    tmp_path: Path, env: dict[str, str], rec: Path
) -> None:
    done = ib(tmp_path, env, str(rec), "--format", "text", "-o", str(tmp_path / "no" / "x.txt"))
    assert done.returncode == 1 and "error IB-EXP-001" in done.stderr, done.stderr
    assert "Traceback" not in done.stderr and not (tmp_path / "no").exists()
    assert "does not exist" in done.stderr and "nothing was created" in done.stderr
    assert "mkdir -p" in done.stderr and "partial output" not in done.stderr
    in_dir = ib(tmp_path, env, str(rec), "-o", str(tmp_path / "no" / "out"))
    assert in_dir.returncode == 1 and "error IB-EXP-001" in in_dir.stderr, in_dir.stderr
    assert "does not exist" in in_dir.stderr and not (tmp_path / "no").exists()


# -- committing: faults and races, in process ----


def run_into(rec: Path, **kwargs: Any) -> Any:
    def run(dest: Any) -> ExportResult:
        result, _ = export_in_process(rec, None, ExportFormat.TEXT, destination=dest, **kwargs)
        return result

    return run


def test_an_output_that_appears_while_exporting_is_never_overwritten(
    tmp_path: Path, rec: Path
) -> None:
    out = tmp_path / "out.txt"

    def run(dest: Any) -> ExportResult:
        result = run_into(rec)(dest)
        out.write_text("appeared meanwhile")
        return result

    with pytest.raises(IronbirdError) as caught:
        write_to_file(out, force=False, run=run)
    assert caught.value.code is Code.EXP_EXISTS and "appeared" in caught.value.summary
    assert out.read_text() == "appeared meanwhile" and temporaries(tmp_path) == []


def test_a_directory_that_appears_while_exporting_is_never_replaced(
    tmp_path: Path, rec: Path
) -> None:
    out = tmp_path / "out"

    def run(dest: Any) -> ExportResult:
        dest.member("a.csv")
        out.mkdir()
        (out / "theirs").write_text("x")
        return ExportResult()

    with pytest.raises(IronbirdError) as caught:
        write_to_directory(out, run=run)
    assert caught.value.code is Code.EXP_EXISTS
    assert [p.name for p in out.iterdir()] == ["theirs"] and temporaries(tmp_path) == []


def test_force_replaces_the_file_that_was_there_at_the_commit(tmp_path: Path, rec: Path) -> None:
    out = tmp_path / "out.txt"
    out.write_text("old")
    result, sync = write_to_file(out, force=True, run=run_into(rec))
    assert sync is None and result.rows == 10 and len(out.read_text().splitlines()) == 10
    assert temporaries(tmp_path) == []


def test_a_disk_full_during_the_write_is_ib_exp_001_and_removes_the_temporary_file(
    tmp_path: Path, rec: Path
) -> None:
    fs = FaultFs()
    fs.fail("write", errno.ENOSPC)
    out = tmp_path / "out.txt"
    with pytest.raises(IronbirdError) as caught:
        write_to_file(out, force=False, run=run_into(rec), fs=fs)
    assert caught.value.code is Code.EXP_WRITE and "No space left" in caught.value.summary
    assert not out.exists() and temporaries(tmp_path) == []


@pytest.mark.parametrize("method", ["write", "fdatasync", "fsync", "open_at", "mkdir_at"])
def test_every_failing_step_of_a_directory_export_leaves_nothing_behind(
    tmp_path: Path, rec: Path, method: str
) -> None:
    fs = FaultFs()
    fs.fail(method, errno.EIO)
    out = tmp_path / "out"
    with pytest.raises(IronbirdError) as caught:
        write_to_directory(out, run=lambda dest: run_dirs(rec, dest), fs=fs)
    assert caught.value.code is Code.EXP_WRITE, caught.value
    assert not out.exists() and temporaries(tmp_path) == []


def run_dirs(rec: Path, dest: Any) -> ExportResult:
    result, _ = export_in_process(rec, None, ExportFormat.CSV, destination=dest)
    return result


def test_a_failure_after_some_members_were_written_removes_exactly_those(
    tmp_path: Path, rec: Path
) -> None:
    fs = FaultFs()
    fs.fail("open_at", errno.EMFILE, skip=3)  # the 3rd member's file cannot be created
    with pytest.raises(IronbirdError) as caught:
        write_to_directory(tmp_path / "out", run=lambda dest: run_dirs(rec, dest), fs=fs)
    assert caught.value.code is Code.EXP_WRITE
    assert sorted(p.name for p in tmp_path.iterdir()) == ["in.ibrec"]


def test_a_failure_to_sync_the_directory_after_the_rename_keeps_the_output(
    tmp_path: Path, rec: Path
) -> None:
    fs = FaultFs()
    fs.fail("fsync", errno.EIO, skip=0, when=lambda fd: os.path.isdir(f"/proc/self/fd/{fd}"))
    out = tmp_path / "out.txt"
    _, sync = write_to_file(out, force=False, run=run_into(rec), fs=fs)
    assert sync is not None and "Input/output" in sync
    assert len(out.read_text().splitlines()) == 10 and temporaries(tmp_path) == []


def test_an_interrupted_export_leaves_no_temporary_file_or_directory(
    tmp_path: Path, rec: Path
) -> None:
    def boom(dest: Any) -> ExportResult:
        dest.member("x.csv")
        dest.write(0, "partial\n")
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        write_to_directory(tmp_path / "out", run=boom)
    with pytest.raises(KeyboardInterrupt):
        write_to_file(tmp_path / "f", force=False, run=boom)
    assert sorted(p.name for p in tmp_path.iterdir()) == ["in.ibrec"]


@pytest.mark.parametrize("path_taken", ["renameat2", "placeholder"])
def test_a_directory_commit_refuses_a_collision_by_every_path(
    tmp_path: Path, path_taken: str
) -> None:
    class NoRenameat2(fsops.FileSystem):
        def renameat2_noreplace(self, dir_fd: int, old: str, new: str) -> bool | None:
            return None

    fs = fsops.REAL_FS if path_taken == "renameat2" else NoRenameat2()
    final = tmp_path / "out"
    with TempDirOutput(final, fs=fs) as out:
        out.write(out.create("a.csv"), b"first")
        out.commit()
    assert (final / "a.csv").read_bytes() == b"first"
    with pytest.raises(Collision), TempDirOutput(final, fs=fs) as out:
        out.write(out.create("b.csv"), b"second")
        out.commit()
    assert [p.name for p in final.iterdir()] == ["a.csv"] and temporaries(tmp_path) == []


def test_more_members_than_open_descriptors_all_get_every_byte_in_order(tmp_path: Path) -> None:
    final = tmp_path / "out"
    with TempDirOutput(final, max_open=3) as out:
        handles = [out.create(f"m{n}.csv") for n in range(12)]
        for round_ in range(5):
            for n, handle in enumerate(handles):
                out.write(handle, f"{n}:{round_};".encode())
        assert len(out._open) <= 3
        out.commit()
    for n in range(12):
        assert (final / f"m{n}.csv").read_text() == "".join(f"{n}:{r};" for r in range(5))
    assert temporaries(tmp_path) == []


def test_a_member_swapped_for_another_file_is_not_written_to(tmp_path: Path) -> None:
    final = tmp_path / "out"
    with (
        pytest.raises(OSError, match="no longer denotes"),
        TempDirOutput(final, max_open=1) as out,
    ):
        first = out.create("a.csv")
        out.create("b.csv")  # gives up a.csv's descriptor
        os.link(out.path / "a.csv", tmp_path / "kept")  # the old inode stays allocated
        os.unlink(out.path / "a.csv")
        (out.path / "a.csv").write_text("an imposter")
        out.write(first, b"data")
    assert not final.exists() and out.leftover is not None
    imposter = tmp_path / out.leftover / "a.csv"  # not ours: left alone, and so is its directory
    assert imposter.read_text() == "an imposter"
    assert [p.name for p in (tmp_path / out.leftover).iterdir()] == ["a.csv"]


def test_a_foreign_file_in_the_temporary_directory_is_left_alone_and_named(tmp_path: Path) -> None:
    final = tmp_path / "out"
    out = TempDirOutput(final)
    with pytest.raises(RuntimeError), out:
        out.create("a.csv")
        (out.path / "foreign").write_text("not ours")
        raise RuntimeError
    assert out.leftover is not None
    left = tmp_path / out.leftover
    assert [p.name for p in left.iterdir()] == ["foreign"]


def test_directory_members_are_buffered_in_bounded_memory(
    tmp_path: Path, rec: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from ironbird.recording import export as export_module

    monkeypatch.setattr(export_module, "DIR_BUFFER_BYTES", 2048)
    sizes: list[int] = []
    real_write = TempDirOutput.write

    def spying(self: TempDirOutput, handle: int, data: bytes | bytearray | memoryview) -> None:
        sizes.append(len(data))
        real_write(self, handle, data)

    monkeypatch.setattr(TempDirOutput, "write", spying)
    hk_and_status(tmp_path / "big.ibrec", 400, 300)
    result, _ = write_to_directory(
        tmp_path / "out",
        run=lambda dest: run_dirs(tmp_path / "big.ibrec", dest),
        max_open=1,
    )
    assert result.rows == 700 and len(sizes) > 10 and max(sizes) < 2048 + 512
    assert len(read_dicts(tmp_path / "out" / "MOCK_HK.csv")) == 400


# -- damaged input ----


def damaged_copy(source: Path, dest: Path, ordinal: int) -> Path:
    with RecordingReader(source) as reader:
        frames = [i for i in reader.items() if getattr(i, "rtype", 0) == RecordType.FRAME]
    raw = bytearray(source.read_bytes())
    raw[frames[ordinal].offset + 30] ^= 0xFF
    dest.write_bytes(bytes(raw))
    return dest


def test_mid_file_damage_exits_5_with_the_output_before_it_and_skip_damage_goes_past(
    tmp_path: Path, env: dict[str, str]
) -> None:
    write_recording(tmp_path / "r.ibrec", hk_frames(30))
    bad = damaged_copy(tmp_path / "r.ibrec", tmp_path / "bad.ibrec", 10)
    done = ib(tmp_path, env, str(bad), "--message", "MOCK_HK", "-o", str(tmp_path / "a.csv"))
    assert done.returncode == 5, done.stderr
    assert len(read_dicts(tmp_path / "a.csv")) == 10  # committed, up to the damage
    assert "error IB-REC-006" in done.stderr and "--skip-damage" in done.stderr
    assert "export stopped there" in done.stderr and "1 damaged stretch" in done.stderr
    resumed = ib(
        tmp_path,
        env,
        str(bad),
        "--skip-damage",
        "--message",
        "MOCK_HK",
        "-o",
        str(tmp_path / "b.csv"),
    )
    assert resumed.returncode == 5 and len(read_dicts(tmp_path / "b.csv")) == 29
    assert "--skip-damage resumes" not in resumed.stderr
    assert any(e["event"] == "IB-REC-006" for e in ledger_events(tmp_path))
    stop = [e for e in ledger_events(tmp_path) if e["event"] == "export.stop"]
    assert {s["damage"] for s in stop} == {1}
    assert temporaries(tmp_path) == []


def test_a_torn_tail_is_a_warning_and_exit_0_with_every_frame_before_it(
    tmp_path: Path, env: dict[str, str]
) -> None:
    write_recording(tmp_path / "r.ibrec", hk_frames(30))
    data = (tmp_path / "r.ibrec").read_bytes()
    with RecordingReader(tmp_path / "r.ibrec") as reader:
        frames = [i for i in reader.items() if getattr(i, "rtype", 0) == RecordType.FRAME]
    (tmp_path / "torn.ibrec").write_bytes(data[: frames[20].offset + 10])
    done = ib(
        tmp_path, env, str(tmp_path / "torn.ibrec"), "--format", "text", "-o", str(tmp_path / "t")
    )
    assert done.returncode == 0, done.stderr
    assert "warning IB-REC-007" in done.stderr
    assert len((tmp_path / "t").read_text().splitlines()) == 20


def test_a_missing_part_of_a_split_recording_is_exit_5_and_the_later_parts_still_export(
    tmp_path: Path, env: dict[str, str]
) -> None:
    from ironbird.recording.writer import part_path

    base, frames = split_set(tmp_path)
    part_path(base, 2).unlink()
    done = ib(tmp_path, env, str(base), "--format", "text", "-o", str(tmp_path / "t"))
    assert done.returncode == 5, done.stderr
    assert "part 2 of the split recording is missing" in done.stderr
    shown = len((tmp_path / "t").read_text().splitlines())
    assert 0 < shown < len(frames)


def test_a_split_set_exports_every_part_and_the_count_matches_rec_info(
    tmp_path: Path, env: dict[str, str]
) -> None:
    base, frames = split_set(tmp_path)
    done = ib(tmp_path, env, str(base), "-o", str(tmp_path / "out"))
    assert done.returncode == 0, done.stderr
    assert len(read_dicts(tmp_path / "out" / "MOCK_HK.csv")) == len(frames)
    assert [int(r["seq"]) for r in read_dicts(tmp_path / "out" / "MOCK_HK.csv")] == [
        f.seq for f in frames
    ]


def test_a_recording_that_is_still_being_written_is_not_called_torn(
    tmp_path: Path, env: dict[str, str]
) -> None:
    write_recording(tmp_path / "r.ibrec", hk_frames(8))
    data = (tmp_path / "r.ibrec").read_bytes()
    # no trailer: a recorder killed at a record boundary
    with RecordingReader(tmp_path / "r.ibrec") as reader:
        trailer = next(i for i in reader.items() if getattr(i, "rtype", 0) == RecordType.INDEX)
    (tmp_path / "cut.ibrec").write_bytes(data[: trailer.offset])
    done = ib(
        tmp_path, env, str(tmp_path / "cut.ibrec"), "--format", "text", "-o", str(tmp_path / "t")
    )
    assert done.returncode == 0 and "without a trailer" in done.stderr
    assert "every record before the end was exported" in done.stderr


# -- signals and a closed reader ----


def test_sigterm_during_an_export_leaves_no_temporary_file(
    tmp_path: Path, env: dict[str, str]
) -> None:
    hk_and_status(tmp_path / "big.ibrec", 60_000, 20_000)
    out = tmp_path / "out.txt"
    proc = subprocess.Popen(
        [
            sys.executable,
            "-c",
            CLI_MAIN,
            "export",
            str(tmp_path / "big.ibrec"),
            "--format",
            "text",
            "-o",
            str(out),
        ],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 60
        while not temporaries(tmp_path):
            assert proc.poll() is None, "finished before it could be interrupted"
            assert time.monotonic() < deadline
            time.sleep(0.002)
        proc.send_signal(signal.SIGTERM)
        _, err = proc.communicate(timeout=60)
    finally:
        if proc.poll() is None:
            proc.kill()
    assert proc.returncode == 143, err
    assert not out.exists() and temporaries(tmp_path) == []


def test_a_reader_that_goes_away_ends_a_stdout_export_quietly(
    tmp_path: Path, env: dict[str, str]
) -> None:
    hk_and_status(tmp_path / "big.ibrec", 5000, 5000)
    proc = subprocess.Popen(
        [sys.executable, "-c", CLI_MAIN, "export", str(tmp_path / "big.ibrec"), "--format", "text"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert proc.stdout is not None
    first = [proc.stdout.readline() for _ in range(2)]
    proc.stdout.close()
    _, err = proc.communicate(timeout=60)
    assert all(line.endswith("\n") for line in first)
    assert proc.returncode == 0 and "Traceback" not in err, err


def test_the_malformed_table_is_opened_first_then_one_table_per_message_seen(rec: Path) -> None:
    dest = NullDestination()
    result, _ = export_in_process(rec, None, ExportFormat.CSV, destination=dest)
    assert dest.members == ["malformed.csv", "MOCK_HK.csv", "MOCK_STATUS.csv"]
    assert result.rows == 10


# -- one message, two layouts ----


@pytest.fixture
def two_nodes(tmp_path: Path) -> Path:
    return two_node_recording(tmp_path / "two.ibrec")


@pytest.mark.parametrize(
    "where",
    [
        None,
        "seq > 2",
        "node in [fc, gc]",
        "node == fc or node == gc",
        "not node == fc",
    ],
)
@pytest.mark.parametrize("to_file", [False, True])
def test_a_single_table_export_of_a_message_with_two_layouts_is_refused_before_any_row(
    tmp_path: Path, env: dict[str, str], two_nodes: Path, where: str | None, to_file: bool
) -> None:
    out = tmp_path / "hk.csv"
    args = [str(two_nodes), "--message", "HK", *(["--where", where] if where else [])]
    done = ib(tmp_path, env, *args, *(["-o", str(out)] if to_file else []))
    assert done.returncode == 2 and "error IB-CLI-001" in done.stderr, done.stderr
    assert "layout" in done.stderr and "Traceback" not in done.stderr
    assert done.stdout == "" and not out.exists() and temporaries(tmp_path) == []


@pytest.mark.parametrize(
    ("where", "node", "rows"),
    [
        ("node == fc", "fc", 4),
        ("node == gc", "gc", 4),
        ("node in [fc]", "fc", 4),
        ("node != fc", "gc", 4),
        ("node != gc", "fc", 4),
        ("node == fc and seq > 2", "fc", 3),
        ("(node == fc or node == gc) and node == gc", "gc", 4),
    ],
)
def test_a_node_predicate_that_leaves_one_layout_exports_it(
    tmp_path: Path, env: dict[str, str], two_nodes: Path, where: str, node: str, rows: int
) -> None:
    out = tmp_path / "hk.csv"
    done = ib(tmp_path, env, str(two_nodes), "--message", "HK", "--where", where, "-o", str(out))
    assert done.returncode == 0, done.stderr
    table = read_dicts(out)
    assert len(table) == rows and {r["node"] for r in table} == {node}
    assert ("b" in table[0]) == (node == "gc")
    piped = ib(tmp_path, env, str(two_nodes), "--message", "HK", "--where", where)
    assert piped.returncode == 0 and len(piped.stdout.splitlines()) == rows + 1


def test_the_same_message_in_a_directory_keeps_both_layouts(
    tmp_path: Path, env: dict[str, str], two_nodes: Path
) -> None:
    done = ib(tmp_path, env, str(two_nodes), "-o", str(tmp_path / "all"))
    assert done.returncode == 0, done.stderr
    assert sorted(p.name for p in (tmp_path / "all").iterdir()) == [
        "HK.2.csv",
        "HK.csv",
        "malformed.csv",
    ]


# -- descriptors ----


def run_ib_with_descriptor_limit(
    tmp_path: Path, env: dict[str, str], limit: int, *args: str
) -> subprocess.CompletedProcess[str]:
    import resource

    def lower() -> None:
        resource.setrlimit(resource.RLIMIT_NOFILE, (limit, limit))

    return subprocess.run(
        [sys.executable, "-c", CLI_MAIN, "export", *args],
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
        check=False,
        cwd=tmp_path,
        preexec_fn=lower,
    )


@pytest.mark.parametrize("limit", [40, 64, 96])
def test_a_directory_export_of_many_messages_fits_the_descriptor_limit_it_runs_under(
    tmp_path: Path, env: dict[str, str], limit: int
) -> None:
    many = many_message_recording(tmp_path / "many.ibrec", 90, 3)
    done = run_ib_with_descriptor_limit(tmp_path, env, limit, str(many), "-o", str(tmp_path / "o"))
    assert done.returncode == 0, done.stderr
    tables = sorted(p.name for p in (tmp_path / "o").iterdir())
    assert len(tables) == 91  # 90 messages and malformed.csv
    assert all(len(read_dicts(tmp_path / "o" / n)) == 3 for n in tables if n != "malformed.csv")
    assert temporaries(tmp_path) == []


def test_the_default_number_of_open_members_follows_the_descriptor_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import resource

    from ironbird.recording import export as export_module

    monkeypatch.setattr(export_module, "_descriptors_in_use", lambda: 10)
    for soft, expected in (
        (1024, 64),
        (200, 64),
        (100, 100 - 10 - export_module.DESCRIPTOR_MARGIN),
    ):
        monkeypatch.setattr(resource, "getrlimit", lambda _which, soft=soft: (soft, soft))
        assert export_module.default_max_open() == expected
    for soft in (10, 3, 0):  # nothing to spare: one member at a time, never zero
        monkeypatch.setattr(resource, "getrlimit", lambda _which, soft=soft: (soft, soft))
        assert export_module.default_max_open() == 1
    monkeypatch.setattr(
        resource, "getrlimit", lambda _which: (resource.RLIM_INFINITY, resource.RLIM_INFINITY)
    )
    assert export_module.default_max_open() == 64


def test_running_out_of_descriptors_gives_one_up_and_goes_on(tmp_path: Path, rec: Path) -> None:
    fs = FaultFs()
    fs.fail("open_at", errno.EMFILE, skip=3, times=1)  # the 3rd member: the limit is reached
    result, _ = write_to_directory(tmp_path / "out", run=lambda dest: run_dirs(rec, dest), fs=fs)
    assert result.rows == 10 and sorted(p.name for p in (tmp_path / "out").iterdir())
    assert temporaries(tmp_path) == []


def test_a_member_reopened_at_the_limit_is_retried_after_giving_one_up(tmp_path: Path) -> None:
    fs = FaultFs()
    final = tmp_path / "out"
    with TempDirOutput(final, fs=fs, max_open=3) as out:
        handles = [out.create(f"m{n}.csv") for n in range(6)]
        fs.fail("open_at", errno.EMFILE, times=1)
        out.write(
            handles[0], b"again"
        )  # evicted earlier: reopened, and the first try hits the limit
        out.commit()
    assert (final / "m0.csv").read_bytes() == b"again"


def test_without_a_descriptor_to_give_up_the_error_names_the_remedy(
    tmp_path: Path, rec: Path
) -> None:
    fs = FaultFs()
    fs.fail("open_at", errno.EMFILE, skip=1)  # nothing is open yet when it hits
    with pytest.raises(IronbirdError) as caught:
        write_to_directory(tmp_path / "out", run=lambda dest: run_dirs(rec, dest), fs=fs)
    assert caught.value.code is Code.EXP_WRITE
    assert "ulimit -n" in caught.value.summary and "Traceback" not in caught.value.summary
    assert sorted(p.name for p in tmp_path.iterdir()) == ["in.ibrec"]


# -- the input, again at the commit ----


def export_text_of(rec: Path, fmt: ExportFormat = ExportFormat.TEXT) -> tuple[Replay, Any]:
    replaying = Replay([rec])
    codecs = embedded_codecs(
        replaying.parts[0],
        replaying.head,
        replaying.nodes,
        replaying.meta.data,
        replaying.max_packet_bytes,
    )

    def run(dest: Any) -> ExportResult:
        return export(replaying, codecs, dest, fmt, directory=fmt is ExportFormat.CSV)

    return replaying, run


@pytest.mark.parametrize("how", ["hardlink", "symlink"])
def test_an_output_that_became_the_input_while_exporting_is_refused_at_the_commit(
    tmp_path: Path, rec: Path, how: str
) -> None:
    out = tmp_path / "out.txt"
    out.write_text("old export")  # an ordinary file at the start, so --force may replace it
    replaying, inner = export_text_of(rec)
    before = rec.read_bytes()

    def run(dest: Any) -> ExportResult:
        result = inner(dest)
        out.unlink()
        if how == "hardlink":
            os.link(rec, out)  # the name now denotes the recording (same inode)
        else:
            out.symlink_to(rec)
        return result

    with pytest.raises(IronbirdError) as caught, contextlib.closing(replaying):
        write_to_file(out, force=True, run=run, before_commit=input_guard(out, replaying))
    assert (
        caught.value.code is Code.EXP_EXISTS and "recording being exported" in caught.value.summary
    )
    assert rec.read_bytes() == before and temporaries(tmp_path) == []
    assert out.samefile(rec)


def test_a_directory_output_that_became_the_input_is_refused_at_the_commit(
    tmp_path: Path, rec: Path
) -> None:
    out = tmp_path / "out"
    replaying, inner = export_text_of(rec, ExportFormat.CSV)

    def run(dest: Any) -> ExportResult:
        result = inner(dest)
        out.symlink_to(rec)
        return result

    with pytest.raises(IronbirdError) as caught, contextlib.closing(replaying):
        write_to_directory(out, run=run, before_commit=input_guard(out, replaying))
    assert caught.value.code is Code.EXP_EXISTS and temporaries(tmp_path) == []
    assert out.is_symlink() and out.samefile(rec)


def test_the_guard_lets_an_ordinary_output_through(tmp_path: Path, rec: Path) -> None:
    out = tmp_path / "out.txt"
    replaying, inner = export_text_of(rec)
    with contextlib.closing(replaying):
        write_to_file(out, force=False, run=inner, before_commit=input_guard(out, replaying))
    assert len(out.read_text().splitlines()) == 10


# -- leftovers of a killed export ----


def test_stale_temporaries_of_a_killed_export_are_warned_about_once_and_kept(
    tmp_path: Path, env: dict[str, str], rec: Path
) -> None:
    out = tmp_path / "hk.txt"
    stale_file = tmp_path / ".hk.txt.0123456789abcdef.tmp"
    stale_dir = tmp_path / ".hk.txt.fedcba9876543210.tmp"
    stale_file.write_text("half an export")
    stale_dir.mkdir()
    unrelated = [
        tmp_path / ".other.txt.0123456789abcdef.tmp",  # another output's
        tmp_path / ".hk.txt.0123456789abcdeg.tmp",  # not a temporary name
        tmp_path / "hk.txt.0123456789abcdef.tmp",  # no leading dot
    ]
    for item in unrelated:
        item.write_text("x")
    done = ib(tmp_path, env, str(rec), "--format", "text", "-o", str(out))
    assert done.returncode == 0, done.stderr
    assert done.stderr.count("earlier export") == 1, done.stderr
    assert "IB-EXP-001" in done.stderr and "2 " in done.stderr and "not removed" in done.stderr
    assert stale_file.read_text() == "half an export" and stale_dir.is_dir()
    assert all(item.exists() for item in unrelated)
    warned = [e for e in ledger_events(tmp_path) if e.get("event") == "IB-EXP-001"]
    assert len(warned) == 1 and warned[0]["kind"] == "warning"


def test_no_warning_without_stale_temporaries(
    tmp_path: Path, env: dict[str, str], rec: Path
) -> None:
    (tmp_path / ".hk.txt.0123456789abcdef.tmp").write_text("not ours to warn about")
    done = ib(tmp_path, env, str(rec), "--format", "text", "-o", str(tmp_path / "other.txt"))
    assert done.returncode == 0 and "IB-EXP-001" not in done.stderr, done.stderr


# -- the count of rows ----


class Closing:
    """A destination whose output goes away at the ``limit``-th line it is given."""

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.accepted_rows = 0  # data lines taken (a CSV header line is not a row)
        self.seen = 0
        self.members = 0

    def member(self, name: str) -> int:
        self.members += 1
        return self.members - 1

    def write(self, handle: int, text: str) -> bool:
        self.seen += 1
        if self.seen >= self.limit:
            return False
        if not text.startswith("rx_time_utc,"):
            self.accepted_rows += 1
        return True

    def flush(self) -> None:
        pass


@pytest.mark.parametrize("fmt", list(ExportFormat))
@pytest.mark.parametrize("limit", [1, 2, 3, 5, 9])
def test_the_row_that_the_output_refused_is_not_counted(
    rec: Path, fmt: ExportFormat, limit: int
) -> None:
    dest = Closing(limit)
    result, _ = export_in_process(rec, None, fmt, destination=dest)
    assert result.ending.value == "output"
    assert result.rows == dest.accepted_rows
    if fmt is ExportFormat.CSV:
        assert result.rows == sum(result.per_table.values())
    else:
        assert result.rows == result.decoded + result.malformed + result.unknown + result.garbage


# -- exclusive creation, and the members at the commit ----


def test_a_member_swapped_after_it_was_given_up_is_caught_at_the_commit(tmp_path: Path) -> None:
    final = tmp_path / "out"
    with (
        pytest.raises(OSError, match="the member changed"),
        TempDirOutput(final, max_open=1) as out,
    ):
        first = out.create("a.csv")
        out.write(first, b"mine")
        second = out.create("b.csv")  # a.csv's descriptor is given up
        out.write(second, b"b")
        os.link(out.path / "a.csv", tmp_path / "kept")  # the original inode stays allocated
        os.unlink(out.path / "a.csv")
        (out.path / "a.csv").write_text("an imposter")
        out.commit()
    assert not final.exists()
    assert (tmp_path / out.leftover / "a.csv").read_text() == "an imposter"  # type: ignore[operator]


def test_a_member_that_still_holds_its_descriptor_commits_and_an_untouched_evicted_one_too(
    tmp_path: Path,
) -> None:
    final = tmp_path / "out"
    with TempDirOutput(final, max_open=1) as out:
        out.write(out.create("a.csv"), b"a")
        out.write(out.create("b.csv"), b"b")
        out.commit()
    assert (final / "a.csv").read_bytes() == b"a" and (final / "b.csv").read_bytes() == b"b"


def test_a_temporary_file_is_created_exclusively_never_opened_over_an_existing_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(atomicfile.os, "urandom", lambda n: b"\x01" * n)
    squatter = tmp_path / f".out.txt.{'01' * 8}.tmp"
    squatter.write_text("someone else's data")
    with pytest.raises(OSError, match="no free temporary name"), TempOutput(tmp_path / "out.txt"):
        raise AssertionError("must not get a temporary file")
    assert squatter.read_text() == "someone else's data"
    squat_dir = tmp_path / f".out.{'01' * 8}.tmp"
    squat_dir.mkdir()
    (squat_dir / "keep").write_text("kept")
    with pytest.raises(OSError, match="no free temporary name"), TempDirOutput(tmp_path / "out"):
        raise AssertionError("must not get a temporary directory")
    assert (squat_dir / "keep").read_text() == "kept"


def test_a_member_is_created_exclusively_never_opened_over_an_existing_file(
    tmp_path: Path,
) -> None:
    final = tmp_path / "out"
    out = TempDirOutput(final)
    with pytest.raises(FileExistsError), out:
        (out.path / "a.csv").write_text("already here")
        out.create("a.csv")
    assert out.leftover is not None
    assert (tmp_path / out.leftover / "a.csv").read_text() == "already here"
