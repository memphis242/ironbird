"""``ib replay`` end to end: the mock round trip, ranges, damage and exit codes, signals, a closed
output, a slow reader, and following a recording while it is written.

Ports are in 58000-58999; nothing here touches the fixed cFS ports.
"""

from __future__ import annotations

import fcntl
import json
import os
import signal
import struct
import subprocess
import sys
import termios
import time
from pathlib import Path
from typing import IO, Any

import pytest

from ironbird.errors import Code
from ironbird.recording.format import RecordType
from ironbird.recording.reader import RecordingReader
from ironbird.recording.writer import RecorderOptions, part_path
from recutil import CLI_MAIN, Proc, run_ib, send_datagrams
from replayutil import (
    T0,
    child_env,
    crafted,
    good_blobs,
    hk,
    hk_frames,
    last_frame_end,
    write_config_58k,
    write_recording,
)


@pytest.fixture
def env(tmp_path: Path) -> dict[str, str]:
    path, command_port, telemetry_port = write_config_58k(tmp_path)
    environment = child_env(tmp_path)
    environment["IRONBIRD_CONFIG"] = str(path)
    environment["PORTS"] = f"{command_port},{telemetry_port}"
    return environment


def pipe_bytes(pipe: IO[bytes]) -> int:
    """Bytes written into ``pipe`` and not yet read (the reading end of a child's stdout)."""
    return struct.unpack("i", fcntl.ioctl(pipe.fileno(), termios.FIONREAD, b"\0" * 4))[0]


def ports(env: dict[str, str]) -> tuple[int, int]:
    command, telemetry = env["PORTS"].split(",")
    return int(command), int(telemetry)


def ledger_events(tmp_path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for path in sorted((tmp_path / "state").glob("*.jsonl")):
        out += [json.loads(line) for line in path.read_text().splitlines()]
    return out


def lines_of(done: subprocess.CompletedProcess[str]) -> list[dict[str, Any]]:
    return [json.loads(line) for line in done.stdout.splitlines()]


def without_time(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{k: v for k, v in r.items() if k != "time"} for r in records]


# -- the round trip: what replay prints is what listen printed live --


def test_replay_prints_the_records_a_live_listen_printed(
    tmp_path: Path, env: dict[str, str]
) -> None:
    rec = tmp_path / "live.ibrec"
    responder = Proc(tmp_path, "respond", ["respond", "--as", "fc"], env)
    listener = None
    try:
        responder.wait_for("responding")
        listener = Proc(
            tmp_path,
            "listen",
            ["listen", "fc", "--format", "json", "--count", "6", "--record", str(rec)],
            env,
        )
        listener.wait_for("recording to")
        for _ in range(3):
            assert run_ib(tmp_path, ["send", "fc", "MOCK_NOOP"], env).returncode == 0
        assert (
            run_ib(tmp_path, ["send", "fc", "MOCK_SET_MODE", "mode=SAFE", "pad=0"], env).returncode
            == 0
        )
        for _ in range(2):
            assert run_ib(tmp_path, ["send", "fc", "MOCK_NOOP"], env).returncode == 0
        assert listener.finish() == 0, listener.stderr
    finally:
        responder.kill()
        if listener is not None:
            listener.kill()
    assert listener is not None
    live = [json.loads(line) for line in listener.stdout.splitlines()]
    assert len(live) == 6

    done = run_ib(tmp_path, ["replay", str(rec), "--format", "json"], env)
    assert done.returncode == 0, done.stderr
    replayed = lines_of(done)
    assert without_time(replayed) == without_time(live)  # by record, not by text
    assert [r["sequence"] for r in replayed] == [r["sequence"] for r in live]
    assert "replay: read 6 frame(s)" in done.stderr

    text = run_ib(tmp_path, ["replay", str(rec)], env)
    assert text.returncode == 0 and len(text.stdout.splitlines()) == 6
    assert all("MOCK_HK" in line for line in text.stdout.splitlines())

    filtered = run_ib(
        tmp_path, ["replay", str(rec), "--where", "cmd_counter >= 4", "--format", "json"], env
    )
    assert filtered.returncode == 0, filtered.stderr
    assert [r["fields"]["cmd_counter"] for r in lines_of(filtered)] == [4, 5, 6]

    events = ledger_events(tmp_path)
    start = next(e for e in events if e["event"] == "replay.start" and e["where"] is None)
    # One ledger file per process, in no fixed order: the stops are looked at as a set.
    stops = [e for e in events if e["event"] == "replay.stop"]
    assert start["files"] == [str(rec)] and all(e["reason"] == "done" for e in stops)
    assert sorted(e["shown"] for e in stops) == [3, 6, 6]


def test_nothing_is_ever_sent_by_replay(tmp_path: Path, env: dict[str, str]) -> None:
    import socket

    command, telemetry = ports(env)
    write_recording(tmp_path / "r.ibrec", hk_frames(5))
    sockets = []
    for port in (command, telemetry):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("127.0.0.1", port))
        sock.settimeout(0.5)
        sockets.append(sock)
    try:
        done = run_ib(tmp_path, ["replay", str(tmp_path / "r.ibrec")], env)
        assert done.returncode == 0
        for sock in sockets:
            with pytest.raises(socket.timeout):
                sock.recvfrom(4096)
    finally:
        for sock in sockets:
            sock.close()


# -- what listen hides --


def test_malformed_unknown_and_garbage_frames_are_shown_flagged(
    tmp_path: Path, env: dict[str, str]
) -> None:
    from recutil import RX, frame

    good = hk(1)
    frames = [
        frame(1, stream=RX, data=good, mid=0x890, time_ns=T0),
        frame(2, stream=RX, data=good[:5], mid=0x890, time_ns=T0 + 1),
        frame(3, stream=RX, data=b"\xee" * 20, mid=0xEEE, time_ns=T0 + 2),
    ]
    write_recording(tmp_path / "r.ibrec", frames, streams=(RX,))
    done = run_ib(tmp_path, ["replay", str(tmp_path / "r.ibrec")], env)
    assert done.returncode == 0, done.stderr
    assert "MALFORMED" in done.stdout and "UNKNOWN" in done.stdout and "hex: " in done.stdout
    assert "1 malformed" in done.stderr and "1 unknown" in done.stderr
    only = run_ib(
        tmp_path, ["replay", str(tmp_path / "r.ibrec"), "--where", "status == malformed"], env
    )
    assert len(only.stdout.splitlines()) == 1 and only.returncode == 0


# -- definitions --


def test_the_embedded_definitions_win_and_a_difference_is_one_information_line(
    tmp_path: Path, env: dict[str, str]
) -> None:
    write_recording(tmp_path / "r.ibrec", hk_frames(4))
    same = run_ib(tmp_path, ["replay", str(tmp_path / "r.ibrec"), "--count", "1"], env)
    assert same.returncode == 0 and "IB-REC-009" not in same.stderr
    config = Path(env["IRONBIRD_CONFIG"])
    changed = tmp_path / "changed.json"
    changed.write_text(
        (Path(__file__).parent.parent / "examples/ccdd/mock.json")
        .read_text()
        .replace("No-operation", "Nothing")
    )
    text = config.read_text().replace(
        str(Path(__file__).parent.parent / "examples/ccdd/mock.json"), str(changed)
    )
    other = tmp_path / "other.toml"
    other.write_text(text)
    differs = run_ib(
        tmp_path, ["-c", str(other), "replay", str(tmp_path / "r.ibrec"), "--count", "1"], env
    )
    assert differs.returncode == 0, differs.stderr
    assert differs.stderr.count("info IB-REC-009") == 1 and "fc:ccdd" in differs.stderr
    assert len(differs.stdout.splitlines()) == 1  # decoded with the recording's own


def test_defs_config_decodes_with_the_current_configuration(
    tmp_path: Path, env: dict[str, str]
) -> None:
    write_recording(tmp_path / "r.ibrec", hk_frames(4))
    done = run_ib(
        tmp_path, ["replay", str(tmp_path / "r.ibrec"), "--defs", "config", "--format", "json"], env
    )
    assert done.returncode == 0, done.stderr
    assert [r["fields"]["cmd_counter"] for r in lines_of(done)] == [1, 2, 3, 4]
    # A recording of a node the configuration does not define cannot use it.
    from ironbird.ccdd.model import MessageKind
    from ironbird.frame import Direction
    from recutil import Stream

    stranger = Stream(
        0, "ground-zz", "zz", MessageKind.TELEMETRY, Direction.RX, "udp", "udp 127.0.0.1:1"
    )
    from recutil import frame

    write_recording(
        tmp_path / "z.ibrec",
        [frame(1, stream=stranger, data=hk(1), mid=0x890)],
        streams=(stranger,),
    )
    refused = run_ib(tmp_path, ["replay", str(tmp_path / "z.ibrec"), "--defs", "config"], env)
    assert (
        refused.returncode == 1 and "error IB-REC-009" in refused.stderr and "zz" in refused.stderr
    )
    assert refused.stdout == ""


def test_defs_config_that_does_not_fit_the_recordings_cap_says_the_cap_is_the_recordings(
    tmp_path: Path, env: dict[str, str]
) -> None:
    (tmp_path / "small.ibrec").write_bytes(
        bytes(crafted(good_blobs(), max_packet=20, frames=0).out)
    )
    done = run_ib(tmp_path, ["replay", str(tmp_path / "small.ibrec"), "--defs", "config"], env)
    assert done.returncode != 0 and "error IB-ENC-004" in done.stderr, done.stderr
    assert "cap" in done.stderr and "recording" in done.stderr.split("IB-ENC-004", 1)[1]
    assert "Traceback" not in done.stderr and done.stdout == ""


def test_a_recording_made_without_definitions_says_what_to_do(
    tmp_path: Path, env: dict[str, str]
) -> None:
    write_recording(tmp_path / "r.ibrec", hk_frames(3), blobs=False)
    done = run_ib(tmp_path, ["replay", str(tmp_path / "r.ibrec")], env)
    assert done.returncode == 1 and "error IB-REC-009" in done.stderr
    assert "--defs config" in done.stderr and "--format hex" in done.stderr
    assert (
        run_ib(tmp_path, ["replay", str(tmp_path / "r.ibrec"), "--format", "hex"], env).returncode
        == 0
    )
    config = run_ib(
        tmp_path, ["replay", str(tmp_path / "r.ibrec"), "--defs", "config", "--count", "1"], env
    )
    assert config.returncode == 0, config.stderr


# -- usage errors, before anything is printed --


@pytest.mark.parametrize(
    "args, needle",
    [
        (["--from", "tomorrow"], "IB-CLI-001"),
        (["--from", "+90"], "IB-CLI-001"),
        (["--from", "2026-10-30T12:00:00Z", "--to", "2026-10-30T11:00:00Z"], "earlier than --to"),
        (["--from", "+90s", "--to", "+10s"], "earlier than --to"),
        (["--from", "+10s", "--to", "+10s"], "earlier than --to"),
        (["--from", "+10s", "--to", "2020-01-01T00:00:00Z"], "earlier than --to"),
        (["--from", "2099-01-01T00:00:00Z", "--to", "+1s"], "earlier than --to"),
        (["--from", "yesterday"], "is not a time: use a UTC time"),
        (["--speed", "-1"], "IB-CLI-001"),
        (["--speed", "nan"], "IB-CLI-001"),
        (["--follow-idle", "5"], "needs --follow"),
        (["--where", "cmd_counter >"], "IB-FLT-001"),
        (["--where", "nosuchthing == 1"], "IB-FLT-"),
        (["--format", "xml"], "IB-CLI-001"),
        (["--defs", "both"], "IB-CLI-001"),
        (["--count", "0"], "IB-CLI-001"),
    ],
)
def test_bad_options_are_one_coded_line_and_exit_2(
    tmp_path: Path, env: dict[str, str], args: list[str], needle: str
) -> None:
    write_recording(tmp_path / "r.ibrec", hk_frames(3))
    done = run_ib(tmp_path, ["replay", str(tmp_path / "r.ibrec"), *args], env)
    assert done.returncode == 2, done.stderr
    assert needle in done.stderr and done.stdout == ""
    assert len([row for row in done.stderr.splitlines() if row.startswith("error")]) == 1


def test_a_file_that_is_not_a_recording_is_ib_rec_004(tmp_path: Path, env: dict[str, str]) -> None:
    (tmp_path / "t.txt").write_text("hello\n")
    done = run_ib(tmp_path, ["replay", str(tmp_path / "t.txt")], env)
    assert done.returncode == 1 and "error IB-REC-004" in done.stderr
    missing = run_ib(tmp_path, ["replay", str(tmp_path / "nope.ibrec")], env)
    assert missing.returncode == 1 and "error IB-REC-004" in missing.stderr


# -- ranges --


def test_from_and_to_select_a_range_and_the_index_skips_chunks(
    tmp_path: Path, env: dict[str, str]
) -> None:
    write_recording(
        tmp_path / "r.ibrec",
        hk_frames(300),
        options=RecorderOptions(min_free_bytes=0, chunk_bytes=1500),
    )
    path = str(tmp_path / "r.ibrec")
    done = run_ib(
        tmp_path, ["replay", path, "--from", "+1s", "--to", "+1.5s", "--format", "json"], env
    )
    assert done.returncode == 0, done.stderr
    assert [r["fields"]["cmd_counter"] for r in lines_of(done)] == [
        (i % 256) for i in range(101, 151)
    ]
    assert "chunk(s) skipped by the index" in done.stderr
    absolute = run_ib(
        tmp_path,
        [
            "replay",
            path,
            "--from",
            "2023-11-14T22:13:21Z",
            "--to",
            "2023-11-14T22:13:21.5Z",
            "--format",
            "json",
        ],
        env,
    )
    assert lines_of(absolute) == lines_of(done)


# -- damage and exit codes --


def damaged_copy(source: Path, dest: Path, ordinal: int) -> Path:
    with RecordingReader(source) as reader:
        frames = [i for i in reader.items() if getattr(i, "rtype", 0) == RecordType.FRAME]
    raw = bytearray(source.read_bytes())
    raw[frames[ordinal].offset + 30] ^= 0xFF
    dest.write_bytes(bytes(raw))
    return dest


def test_mid_file_damage_is_exit_5_with_the_output_before_it(
    tmp_path: Path, env: dict[str, str]
) -> None:
    write_recording(tmp_path / "r.ibrec", hk_frames(30))
    bad = damaged_copy(tmp_path / "r.ibrec", tmp_path / "bad.ibrec", 10)
    done = run_ib(tmp_path, ["replay", str(bad)], env)
    assert done.returncode == 5, done.stderr
    assert len(done.stdout.splitlines()) == 10
    assert "error IB-REC-006" in done.stderr and "--skip-damage" in done.stderr
    assert "1 damaged stretch" in done.stderr
    resumed = run_ib(tmp_path, ["replay", str(bad), "--skip-damage"], env)
    assert resumed.returncode == 5 and len(resumed.stdout.splitlines()) == 29
    assert "--skip-damage resumes" not in resumed.stderr
    assert any(e["event"] == "IB-REC-006" for e in ledger_events(tmp_path))
    counted = run_ib(tmp_path, ["replay", str(bad), "--count", "5"], env)
    assert counted.returncode == 0 and len(counted.stdout.splitlines()) == 5  # never reached it


def test_an_offset_range_with_damage_before_the_first_frame_still_reads_and_exits_5(
    tmp_path: Path, env: dict[str, str]
) -> None:
    write_recording(
        tmp_path / "r.ibrec",
        hk_frames(300),
        options=RecorderOptions(min_free_bytes=0, chunk_bytes=900),
    )
    bad = damaged_copy(tmp_path / "r.ibrec", tmp_path / "bad.ibrec", 0)
    for flag in ("--from", "--to"):
        done = run_ib(tmp_path, ["replay", str(bad), flag, "+1s"], env)
        assert done.returncode == 5, done.stderr
        assert "IB-REC-006" in done.stderr and "1 damaged stretch" in done.stderr
    resumed = run_ib(tmp_path, ["replay", str(bad), "--skip-damage", "--to", "+1s"], env)
    assert resumed.returncode == 5 and len(resumed.stdout.splitlines()) == 100
    from_ = run_ib(tmp_path, ["replay", str(bad), "--skip-damage", "--from", "+1s"], env)
    assert len(from_.stdout.splitlines()) == 199  # the damaged chunk is outside the range


def test_a_torn_tail_is_a_warning_and_exit_0(tmp_path: Path, env: dict[str, str]) -> None:
    write_recording(tmp_path / "r.ibrec", hk_frames(20))
    raw = (tmp_path / "r.ibrec").read_bytes()
    end = last_frame_end(tmp_path / "r.ibrec")
    (tmp_path / "torn.ibrec").write_bytes(raw[: end - 9])
    done = run_ib(tmp_path, ["replay", str(tmp_path / "torn.ibrec")], env)
    assert done.returncode == 0, done.stderr
    assert len(done.stdout.splitlines()) == 19 and "warning IB-REC-007" in done.stderr


def test_a_recording_that_was_never_closed_says_so_and_exits_0(
    tmp_path: Path, env: dict[str, str]
) -> None:
    builder = crafted(good_blobs(), frames=3)
    (tmp_path / "open.ibrec").write_bytes(bytes(builder.out))
    done = run_ib(tmp_path, ["replay", str(tmp_path / "open.ibrec")], env)
    assert done.returncode == 0 and len(done.stdout.splitlines()) == 3
    assert "warning IB-REC-007" in done.stderr and "without a trailer" in done.stderr


def test_a_bad_index_falls_back_to_a_read_with_an_information_line(
    tmp_path: Path, env: dict[str, str]
) -> None:
    write_recording(
        tmp_path / "r.ibrec",
        hk_frames(100),
        options=RecorderOptions(min_free_bytes=0, chunk_bytes=1500),
    )
    raw = bytearray((tmp_path / "r.ibrec").read_bytes())
    with RecordingReader(tmp_path / "r.ibrec") as reader:
        index = next(i for i in reader.items() if getattr(i, "rtype", 0) == RecordType.INDEX)
    raw[index.offset + 14] ^= 0xFF  # a byte of the index body: its CRC fails
    (tmp_path / "noindex.ibrec").write_bytes(bytes(raw))
    done = run_ib(tmp_path, ["replay", str(tmp_path / "noindex.ibrec"), "--from", "+0.5s"], env)
    assert "IB-REC-008" in done.stderr and "every record is read instead" in done.stderr
    assert done.returncode in (0, 5), done.stderr
    assert len(done.stdout.splitlines()) == 50


def test_a_split_recording_plays_every_part_and_a_missing_one_is_exit_5(
    tmp_path: Path, env: dict[str, str]
) -> None:
    from ironbird.recording.writer import part_path

    base = tmp_path / "run.ibrec"
    options = RecorderOptions(min_free_bytes=0, split_bytes=2500, chunk_bytes=1000)
    write_recording(base, hk_frames(60), options=options)
    done = run_ib(tmp_path, ["replay", str(base), "--format", "json"], env)
    assert done.returncode == 0 and len(done.stdout.splitlines()) == 60
    part_path(base, 2).unlink()
    gap = run_ib(tmp_path, ["replay", str(base)], env)
    assert gap.returncode == 5 and "part 2" in gap.stderr and "IB-REC-006" in gap.stderr
    assert 0 < len(gap.stdout.splitlines()) < 60
    # Later parts still play: more than part 1 alone holds.
    first_part = RecordingReader(base)
    try:
        part_one_frames = sum(1 for _ in first_part.frames())
    finally:
        first_part.close()
    assert len(gap.stdout.splitlines()) > part_one_frames
    # --count reached first: the run did not get to the hole, so it is a normal end (0); a count
    # that is not reached leaves the missing part standing (5).
    counted = run_ib(tmp_path, ["replay", str(base), "--count", "5"], env)
    assert counted.returncode == 0 and len(counted.stdout.splitlines()) == 5, counted.stderr
    unreached = run_ib(tmp_path, ["replay", str(base), "--count", "59"], env)
    assert unreached.returncode == 5, unreached.stderr


# -- signals, a closed output, a slow reader --


def test_sigterm_ends_a_paced_replay_at_once_with_143_and_a_ledger_record(
    tmp_path: Path, env: dict[str, str]
) -> None:
    write_recording(tmp_path / "r.ibrec", hk_frames(20, step_ns=1_000_000_000))
    proc = Proc(tmp_path, "paced", ["replay", str(tmp_path / "r.ibrec"), "--speed", "1"], env)
    try:
        deadline = time.monotonic() + 15
        while len(proc.stdout.splitlines()) < 2:
            assert proc.popen.poll() is None, proc.stderr
            assert time.monotonic() < deadline
            time.sleep(0.05)
        started = time.monotonic()
        proc.popen.send_signal(signal.SIGTERM)
        assert proc.finish(10) == 128 + signal.SIGTERM
        assert time.monotonic() - started < 5
    finally:
        proc.kill()
    events = [e["event"] for e in ledger_events(tmp_path)]
    assert "cli.signal" in events and "replay.stop" in events
    assert 2 <= len(proc.stdout.splitlines()) < 20


def test_speed_one_takes_about_the_recorded_time(tmp_path: Path, env: dict[str, str]) -> None:
    write_recording(tmp_path / "r.ibrec", hk_frames(11, step_ns=100_000_000))
    started = time.monotonic()
    done = run_ib(tmp_path, ["replay", str(tmp_path / "r.ibrec"), "--speed", "1"], env)
    took = time.monotonic() - started
    assert done.returncode == 0 and len(done.stdout.splitlines()) == 11
    assert 1.0 <= took < 10.0  # the recording spans 1 s; the rest is process start-up
    fast = time.monotonic()
    run_ib(tmp_path, ["replay", str(tmp_path / "r.ibrec"), "--speed", "100"], env)
    assert time.monotonic() - fast < took


def test_a_closed_pipe_ends_the_replay_quietly(tmp_path: Path, env: dict[str, str]) -> None:
    write_recording(tmp_path / "r.ibrec", hk_frames(30_000, step_ns=1000))
    proc = subprocess.Popen(
        [sys.executable, "-c", CLI_MAIN, "replay", str(tmp_path / "r.ibrec")],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        text=True,
    )
    assert proc.stdout is not None and proc.stderr is not None
    first = proc.stdout.readline()
    proc.stdout.close()
    code = proc.wait(timeout=60)
    err = proc.stderr.read()
    proc.stderr.close()
    assert "MOCK_HK" in first and code == 0, err
    assert "Traceback" not in err and "IB-OUT-002" not in err


def test_a_slow_reader_loses_nothing(tmp_path: Path, env: dict[str, str]) -> None:
    count = 20_000
    write_recording(tmp_path / "r.ibrec", hk_frames(count, step_ns=1000))
    proc = subprocess.Popen(
        [sys.executable, "-c", CLI_MAIN, "replay", str(tmp_path / "r.ibrec")],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )
    assert proc.stdout is not None and proc.stderr is not None
    time.sleep(2.0)  # the whole queue and the pipe fill while nobody reads
    total = 0
    while chunk := proc.stdout.read(65536):
        total += chunk.count(b"\n")
        time.sleep(0.01)
    code = proc.wait(timeout=60)
    err = proc.stderr.read().decode()
    proc.stderr.close()
    assert code == 0, err
    assert total == count and "IB-OUT-" not in err


def test_a_signal_ends_a_replay_that_is_blocked_on_a_reader_that_never_reads(
    tmp_path: Path, env: dict[str, str]
) -> None:
    write_recording(tmp_path / "r.ibrec", hk_frames(40_000, step_ns=1000))
    proc = subprocess.Popen(
        [sys.executable, "-c", CLI_MAIN, "replay", str(tmp_path / "r.ibrec")],
        stdout=subprocess.PIPE,  # nobody reads it: the pipe and the output queue fill
        stderr=subprocess.PIPE,
        env=env,
    )
    assert proc.stdout is not None and proc.stderr is not None
    try:
        # Output in the pipe means the command runs, its signal handlers in place since before
        # it began. A wait timed from the spawn would race the interpreter's start on a busy
        # host: a signal that lands before the handlers ends the process by the signal (-15).
        deadline = time.monotonic() + 60
        while not pipe_bytes(proc.stdout):
            assert proc.poll() is None, proc.stderr.read().decode()
            assert time.monotonic() < deadline, "the replay wrote nothing"
            time.sleep(0.01)
        time.sleep(2.0)  # the pipe and the output queue fill
        assert proc.poll() is None  # held up by the reader, not finished and not dropping lines
        started = time.monotonic()
        proc.send_signal(signal.SIGTERM)
        code = proc.wait(timeout=20)
        assert time.monotonic() - started < 10
        err = proc.stderr.read().decode()
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
        proc.stdout.close()
        proc.stderr.close()
    assert code == 128 + signal.SIGTERM, err
    assert "IB-OUT-001" not in err  # nothing was dropped while it ran; the abandoned are counted


def test_closed_stdout_is_exit_4_with_the_loss_counted(tmp_path: Path, env: dict[str, str]) -> None:
    write_recording(tmp_path / "r.ibrec", hk_frames(5))
    done = subprocess.run(
        [
            "sh",
            "-c",
            'exec "$@" >&-',
            "sh",
            sys.executable,
            "-c",
            CLI_MAIN,
            "replay",
            str(tmp_path / "r.ibrec"),
        ],
        stderr=subprocess.PIPE,
        text=True,
        env=env,
        timeout=60,
        check=False,
    )
    assert done.returncode == 4 and "IB-OUT-002" in done.stderr, done.stderr


# -- following a recording while it is written --


def test_follow_shows_frames_as_the_recorder_writes_them_and_ends_with_it(
    tmp_path: Path, env: dict[str, str]
) -> None:
    _, telemetry = ports(env)
    out = tmp_path / "live.ibrec"
    recorder = Proc(tmp_path, "rec", ["record", "-o", str(out), "--quiet"], env)
    follower = None
    try:
        recorder.wait_for("listening")
        follower = Proc(
            tmp_path, "follow", ["replay", str(out), "--follow", "--format", "json"], env
        )
        send_datagrams(telemetry, [hk(i) for i in range(1, 4)], pause=0.02)
        deadline = time.monotonic() + 20
        while len(follower.stdout.splitlines()) < 3:
            assert follower.popen.poll() is None, follower.stderr
            assert time.monotonic() < deadline, follower.stdout
            time.sleep(0.05)
        send_datagrams(telemetry, [hk(i) for i in range(4, 6)], pause=0.02)
        while len(follower.stdout.splitlines()) < 5:
            assert time.monotonic() < deadline, follower.stdout
            time.sleep(0.05)
        assert follower.popen.poll() is None  # the recorder is still writing
        recorder.popen.send_signal(signal.SIGINT)  # a clean close: trailer, then the lock goes
        assert recorder.finish(30) == 128 + signal.SIGINT
        assert follower.finish(30) == 0, follower.stderr
    finally:
        recorder.kill()
        if follower is not None:
            follower.kill()
    assert follower is not None
    assert [json.loads(row)["fields"]["cmd_counter"] for row in follower.stdout.splitlines()] == [
        1,
        2,
        3,
        4,
        5,
    ]
    assert "IB-REC-007" not in follower.stderr


def test_follow_ends_when_the_recorder_is_killed_and_says_the_file_was_not_closed(
    tmp_path: Path, env: dict[str, str]
) -> None:
    _, telemetry = ports(env)
    out = tmp_path / "killed.ibrec"
    recorder = Proc(tmp_path, "rec", ["record", "-o", str(out), "--quiet"], env)
    follower = None
    try:
        recorder.wait_for("listening")
        follower = Proc(tmp_path, "follow", ["replay", str(out), "--follow"], env)
        send_datagrams(telemetry, [hk(i) for i in range(1, 4)], pause=0.02)
        deadline = time.monotonic() + 20
        while len(follower.stdout.splitlines()) < 3:
            assert follower.popen.poll() is None, follower.stderr
            assert time.monotonic() < deadline
            time.sleep(0.05)
        started = time.monotonic()
        recorder.popen.kill()  # the kernel drops its lock
        recorder.popen.wait()
        code = follower.finish(30)
        assert time.monotonic() - started < 10
    finally:
        recorder.kill()
        if follower is not None:
            follower.kill()
    assert follower is not None
    assert code == 0, follower.stderr
    assert "warning IB-REC-007" in follower.stderr and "not closed cleanly" in follower.stderr


def test_follow_idle_ends_a_quiet_follow(tmp_path: Path, env: dict[str, str]) -> None:
    import fcntl

    builder = crafted(good_blobs(), frames=2)
    path = tmp_path / "quiet.ibrec"
    path.write_bytes(bytes(builder.out))
    fd = os.open(path, os.O_RDONLY)
    fcntl.flock(fd, fcntl.LOCK_EX)
    try:
        started = time.monotonic()
        done = run_ib(tmp_path, ["replay", str(path), "--follow", "--follow-idle", "1"], env)
        took = time.monotonic() - started
        live = run_ib(tmp_path, ["replay", str(path)], env)  # without --follow: the end of the file
    finally:
        os.close(fd)
    assert done.returncode == 0, done.stderr
    assert len(done.stdout.splitlines()) == 2 and took >= 1.0
    assert live.returncode == 0 and "still being written" in live.stderr
    assert "not closed cleanly" not in done.stderr and "IB-REC-007" in done.stderr


# Child-side hooks: the follow says when it waits for the next part, a check of that part that
# fails says so too, and the head of a new part is awaited for a minute, so the signal lands
# while the retry is handling the error.
_HEAD_WAIT_HOOKS = """
import os
from pathlib import Path
from ironbird.recording import replay
WAITING, MARK = Path(os.environ["IB_TEST_WAITING"]), Path(os.environ["IB_TEST_MARK"])
replay.HEAD_WAIT_POLLS = 600
real_next, real_check = replay.Replay._next_part, replay.Replay._check_set

def next_part(self, closed):
    WAITING.touch()
    return real_next(self, closed)

def check_set(self, *args):
    try:
        return real_check(self, *args)
    except Exception:
        MARK.touch()
        raise

replay.Replay._next_part, replay.Replay._check_set = next_part, check_set
"""


def test_a_signal_while_the_head_of_a_new_part_is_awaited_reports_no_error(
    tmp_path: Path, env: dict[str, str]
) -> None:
    """The empty next part is retried on purpose; Ctrl-C during the retry ends the follow with
    the interruption alone, not with the error the retry was handling."""
    import fcntl

    path = tmp_path / "live.ibrec"
    write_recording(path, hk_frames(3))
    waiting, mark = tmp_path / "waiting", tmp_path / "head-retried"
    lock = os.open(path, os.O_RDONLY)
    fcntl.flock(lock, fcntl.LOCK_EX)  # a live writer: the follow goes on past the trailer
    proc = subprocess.Popen(
        [sys.executable, "-c", _HEAD_WAIT_HOOKS + CLI_MAIN, "replay", str(path), "--follow"],
        env={**env, "IB_TEST_WAITING": str(waiting), "IB_TEST_MARK": str(mark)},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 60

        def wait_for(reached: Path) -> None:
            while not reached.exists():
                assert proc.poll() is None, proc.communicate()[1]
                assert time.monotonic() < deadline
                time.sleep(0.005)

        wait_for(waiting)
        part_path(path, 2).write_bytes(b"")  # created, its head not written yet
        wait_for(mark)
        proc.send_signal(signal.SIGINT)
        _, err = proc.communicate(timeout=60)
    finally:
        os.close(lock)
        if proc.poll() is None:
            proc.kill()
    assert proc.returncode == 130, err
    assert "interrupted (SIGINT)" in err and Code.REC_FORMAT.value not in err, err
    errors = [e for e in ledger_events(tmp_path) if e["kind"] == "error"]
    assert errors == [], errors


# Child-side hooks: the follow says when it starts waiting for more of the recording, either for
# the file to grow (``grow``: the file has no trailer yet) or for its next part (``part``).
_FOLLOW_WAIT_HOOKS = """
import os
from pathlib import Path
from ironbird.recording import replay
WHERE, MARK = os.environ["IB_TEST_WINDOW"], Path(os.environ["IB_TEST_MARK"])
real_waiter, real_wait_for_file = replay.Follower.waiter, replay.Follower.wait_for_file

def waiter(self, size):
    wait = real_waiter(self, size)
    def marked():
        if WHERE == "grow":
            MARK.touch()
        return wait()
    return marked

def wait_for_file(self, path):
    if WHERE == "part":
        MARK.touch()
    return real_wait_for_file(self, path)

replay.Follower.waiter, replay.Follower.wait_for_file = waiter, wait_for_file
"""


@pytest.mark.parametrize("where", ["grow", "part"])
def test_a_signal_ends_a_follow_that_waits_for_more_of_a_live_recording(
    tmp_path: Path, env: dict[str, str], where: str
) -> None:
    """A live writer holds its lock and writes nothing more: the follow waits with no end of
    its own, and SIGTERM ends it at once, by the signal."""
    path = tmp_path / "live.ibrec"
    write_recording(path, hk_frames(3))
    if where == "grow":  # no trailer yet: the writer is still in the middle of the file
        reader = RecordingReader(path)
        try:
            trailer = reader.read_trailer()
        finally:
            reader.close()
        assert trailer is not None
        os.truncate(path, trailer[0])
    mark = tmp_path / "waiting"
    lock = os.open(path, os.O_RDONLY)
    fcntl.flock(lock, fcntl.LOCK_EX)  # a live writer
    proc = subprocess.Popen(
        [sys.executable, "-c", _FOLLOW_WAIT_HOOKS + CLI_MAIN, "replay", str(path), "--follow"],
        env={**env, "IB_TEST_WINDOW": where, "IB_TEST_MARK": str(mark)},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 60
        while not mark.exists():
            assert proc.poll() is None, proc.communicate()[1]
            assert time.monotonic() < deadline
            time.sleep(0.005)
        started = time.monotonic()
        proc.send_signal(signal.SIGTERM)
        _, err = proc.communicate(timeout=60)
        took = time.monotonic() - started
    finally:
        os.close(lock)
        if proc.poll() is None:
            proc.kill()
    assert proc.returncode == 128 + signal.SIGTERM, err
    assert took < 30 and "interrupted (SIGTERM)" in err and "Traceback" not in err, err
    stops = [e["reason"] for e in ledger_events(tmp_path) if e["event"] == "replay.stop"]
    assert stops == ["interrupted"], err


def test_replay_never_modifies_the_recording(tmp_path: Path, env: dict[str, str]) -> None:
    write_recording(tmp_path / "r.ibrec", hk_frames(5))
    before = (tmp_path / "r.ibrec").read_bytes()
    stamp = (tmp_path / "r.ibrec").stat().st_mtime_ns
    run_ib(tmp_path, ["replay", str(tmp_path / "r.ibrec"), "--skip-damage"], env)
    assert (tmp_path / "r.ibrec").read_bytes() == before
    assert (tmp_path / "r.ibrec").stat().st_mtime_ns == stamp
