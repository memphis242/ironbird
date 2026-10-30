"""``ib record``, ``--record``, ``ib rec info|check|repair`` end to end over loopback UDP.

Ports are in 52000-52999; nothing here touches the fixed cFS ports.
"""

from __future__ import annotations

import json
import os
import signal
import stat
import time
from pathlib import Path
from typing import Any

import pytest

from ironbird.codec import Codec
from ironbird.config import load_config
from ironbird.errors import Code, IronbirdError
from ironbird.frame import Direction
from ironbird.recording import format as fmt
from ironbird.recording import inspect as rec_inspect
from ironbird.recording.format import MarkKind, RecordType
from ironbird.recording.reader import Item, RecordingReader
from recutil import Proc, run_ib, send_datagrams, write_config


@pytest.fixture
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    path, command_port, telemetry_port = write_config(tmp_path)
    monkeypatch.setenv("IRONBIRD_CONFIG", str(path))
    monkeypatch.setenv("IRONBIRD_RECORD_DIR", str(tmp_path / "recordings"))
    monkeypatch.setenv("IRONBIRD_TEST_PORTS", f"{command_port},{telemetry_port}")
    return dict(os.environ)


def ports(env: dict[str, str]) -> tuple[int, int]:
    command, telemetry = env["IRONBIRD_TEST_PORTS"].split(",")
    return int(command), int(telemetry)


def hk(mock_codec: Codec, counter: int = 1) -> bytes:
    return mock_codec.encode_telemetry("MOCK_HK", {"cmd_counter": counter}).data


def ledger_events(tmp_path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for path in sorted((tmp_path / "state").glob("*.jsonl")):
        out += [json.loads(line) for line in path.read_text().splitlines()]
    return out


def read(path: Path) -> list[Any]:
    with RecordingReader(path) as reader:
        return list(reader.frames())


# -- ib record --


def test_record_count_writes_a_closed_recording_and_the_ledger_notes_it(
    tmp_path: Path, env: dict[str, str], mock_codec: Codec
) -> None:
    out = tmp_path / "r.ibrec"
    _, telemetry = ports(env)
    rec = Proc(tmp_path, "rec", ["record", "-o", str(out), "--count", "5", "--quiet"], env)
    try:
        rec.wait_for("listening")
        sent = [hk(mock_codec, i) for i in range(1, 6)]
        send_datagrams(telemetry, sent, pause=0.01)
        assert rec.finish() == 0, rec.stderr
    finally:
        rec.kill()
    assert f"recording to {out}" in rec.stderr and "recorded 5 frame(s)" in rec.stderr
    frames = read(out)
    assert [f.seq for f in frames] == [1, 2, 3, 4, 5]
    assert all(f.stream.direction is Direction.RX and f.mid == 0x890 for f in frames)
    assert [f.data for f in frames] == sent
    report = rec_inspect.info(out)
    assert report.status == "closed" and report.frames == 5 and report.mids == {0x890: 5}
    assert [b.kind for b in report.blobs] == ["ccdd", "command_profile", "telemetry_profile"]
    assert stat.S_IMODE(out.stat().st_mode) == 0o600
    events = ledger_events(tmp_path)
    start = next(e for e in events if e["event"] == "recording.start")
    stop = next(e for e in events if e["event"] == "recording.stop")
    assert start["path"] == str(out) and stop["frames"] == 5 and stop["failed"] is None


def test_record_duration_is_a_normal_end_and_status_lines_appear(
    tmp_path: Path, env: dict[str, str]
) -> None:
    done = run_ib(tmp_path, ["record", "-o", str(tmp_path / "d.ibrec"), "--duration", "0.4"], env)
    assert done.returncode == 0, done.stderr
    assert "recorded 0 frame(s)" in done.stderr
    assert rec_inspect.scan(tmp_path / "d.ibrec").status == "closed"


def test_record_refuses_to_overwrite_and_leaves_the_file_alone(
    tmp_path: Path, env: dict[str, str]
) -> None:
    out = tmp_path / "x.ibrec"
    out.write_bytes(b"evidence")
    done = run_ib(tmp_path, ["record", "-o", str(out), "--duration", "0.2"], env)
    assert done.returncode == 1 and "error IB-REC-001" in done.stderr
    assert out.read_bytes() == b"evidence"


def test_record_default_location_name_and_modes(tmp_path: Path, env: dict[str, str]) -> None:
    done = run_ib(tmp_path, ["record", "--duration", "0.2"], env)
    assert done.returncode == 0, done.stderr
    directory = tmp_path / "recordings"
    [file] = list(directory.glob("*.ibrec"))
    import re

    assert re.fullmatch(r"[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}\.ibrec", file.name)
    assert stat.S_IMODE(directory.stat().st_mode) == 0o700
    assert f"recording to {file}" in done.stderr
    with RecordingReader(file) as reader:  # the session id in the name is the ledger's
        assert file.name.endswith(f"-{reader.meta.data['session']}.ibrec")
    assert any(
        e["event"] == "recording.start" and e["path"] == str(file) for e in ledger_events(tmp_path)
    )


def test_record_into_an_existing_directory_uses_the_default_name(
    tmp_path: Path, env: dict[str, str]
) -> None:
    target = tmp_path / "somewhere"
    target.mkdir()
    done = run_ib(tmp_path, ["record", "-o", str(target), "--duration", "0.2"], env)
    assert done.returncode == 0, done.stderr
    assert len(list(target.glob("*.ibrec"))) == 1


def test_relative_record_dir_environment_is_refused(tmp_path: Path, env: dict[str, str]) -> None:
    env = {**env, "IRONBIRD_RECORD_DIR": "relative/dir"}
    done = run_ib(tmp_path, ["record", "--duration", "0.2"], env)
    assert done.returncode == 1 and "IRONBIRD_RECORD_DIR is not an absolute path" in done.stderr


def test_record_split_makes_parts(tmp_path: Path, env: dict[str, str], mock_codec: Codec) -> None:
    out = tmp_path / "s.ibrec"
    _, telemetry = ports(env)
    rec = Proc(tmp_path, "rec", ["record", "-o", str(out), "--split", "1MiB", "--quiet"], env)
    big = mock_codec.encode_telemetry("MOCK_HK").data + bytes(1300)  # oversize-free, junk tail
    try:
        rec.wait_for("listening")
        for _ in range(4):
            send_datagrams(telemetry, [big] * 400, pause=0.0004)
            time.sleep(0.2)
        rec.popen.send_signal(signal.SIGTERM)
        assert rec.finish() == 128 + signal.SIGTERM
    finally:
        rec.kill()
    parts = sorted(tmp_path.glob("s*.ibrec"))
    assert len(parts) >= 2 and parts[1].name == "s.p0002.ibrec"
    total = 0
    for number, part in enumerate(parts, start=1):
        report = rec_inspect.scan(part)
        assert report.status == "closed", (part, report.findings)
        with RecordingReader(part) as reader:
            assert reader.meta.part == number
        total += report.frames
    assert total >= 1000


def test_record_usage_errors_exit_2(tmp_path: Path, env: dict[str, str]) -> None:
    for args in (
        ["--split", "tomorrow"],
        ["--split", "100"],
        ["--duration", "0"],
        ["--count", "0"],
    ):
        done = run_ib(tmp_path, ["record", "-o", str(tmp_path / "u.ibrec"), *args], env)
        assert done.returncode == 2, (args, done.stderr)
    assert not (tmp_path / "u.ibrec").exists()
    done = run_ib(tmp_path, ["record", "nosuchnode", "--duration", "1"], env)
    assert done.returncode == 2 and "IB-CFG-004" in done.stderr
    done = run_ib(tmp_path, ["record", "--link", "nosuchlink", "--duration", "1"], env)
    assert done.returncode == 2 and "IB-CFG-005" in done.stderr


def test_sigterm_closes_the_recording_with_a_trailer_and_a_signal_mark(
    tmp_path: Path, env: dict[str, str], mock_codec: Codec
) -> None:
    out = tmp_path / "t.ibrec"
    _, telemetry = ports(env)
    rec = Proc(tmp_path, "rec", ["record", "-o", str(out), "--quiet"], env)
    try:
        rec.wait_for("listening")
        send_datagrams(telemetry, [hk(mock_codec)] * 10, pause=0.005)
        time.sleep(0.2)
        rec.popen.send_signal(signal.SIGTERM)
        assert rec.finish() == 128 + signal.SIGTERM
    finally:
        rec.kill()
    report = rec_inspect.info(out)
    assert report.status == "closed" and report.frames == 10
    kinds = [m.kind for m in report.marks]
    assert kinds[0] == MarkKind.START and MarkKind.SIGNAL in kinds


def test_kill_minus_nine_leaves_a_torn_recording_with_every_flushed_frame(
    tmp_path: Path, env: dict[str, str], mock_codec: Codec
) -> None:
    out = tmp_path / "k.ibrec"
    _, telemetry = ports(env)
    rec = Proc(tmp_path, "rec", ["record", "-o", str(out), "--quiet"], env)
    try:
        rec.wait_for("listening")
        send_datagrams(telemetry, [hk(mock_codec, i % 200) for i in range(60)], pause=0.003)
        time.sleep(1.5)  # past the 250 ms flush and the 1 s chunk
        send_datagrams(telemetry, [hk(mock_codec)] * 5)  # these may be lost with the buffer
        rec.popen.kill()
        rec.popen.wait()
    finally:
        rec.kill()
    info = run_ib(tmp_path, ["rec", "info", str(out), "--format", "json"], env)
    assert info.returncode == 0, info.stderr
    record = json.loads(info.stdout)
    assert record["status"] in ("torn", "unclosed") and record["source"] == "scan"
    assert 60 <= record["frames"] <= 65  # everything flushed before the kill is readable
    assert record["writer_active"] is False
    check = run_ib(tmp_path, ["rec", "check", str(out)], env)
    assert check.returncode == 0, check.stderr
    assert "IB-REC-007" in check.stderr  # the torn tail (or the missing trailer) is a warning
    strict = run_ib(tmp_path, ["rec", "check", str(out), "--strict"], env)
    assert strict.returncode == 1
    fixed = tmp_path / "fixed.ibrec"
    repaired = run_ib(tmp_path, ["rec", "repair", str(out), str(fixed)], env)
    assert repaired.returncode == 0, repaired.stderr
    assert rec_inspect.scan(fixed).status == "closed"
    assert read(fixed) == read(out)  # the repaired output equals the readable prefix


# -- --record on listen, events and respond --


def test_listen_record_captures_what_listen_prints(
    tmp_path: Path, env: dict[str, str], mock_codec: Codec
) -> None:
    out = tmp_path / "l.ibrec"
    _, telemetry = ports(env)
    listen = Proc(
        tmp_path,
        "listen",
        ["listen", "fc", "--count", "4", "--format", "json", "--record", str(out)],
        env,
    )
    try:
        listen.wait_for("istening")
        sent = [hk(mock_codec, i) for i in range(1, 5)]
        send_datagrams(telemetry, sent, pause=0.01)
        assert listen.finish() == 0, listen.stderr
    finally:
        listen.kill()
    printed = [json.loads(line) for line in listen.stdout.splitlines()]
    assert len(printed) == 4
    frames = read(out)
    assert [f.data for f in frames] == sent
    assert "recorded 4 frame(s)" in listen.stderr


def test_listen_where_with_record_still_records_every_frame(
    tmp_path: Path, env: dict[str, str], mock_codec: Codec
) -> None:
    """The filter limits what is shown; the recording holds every frame of the link."""
    out = tmp_path / "w.ibrec"
    _, telemetry = ports(env)
    listen = Proc(
        tmp_path,
        "listen",
        ["listen", "fc", "--count", "1", "--format", "json", "--where",
         "msg == MOCK_HK and cmd_counter == 5", "--record", str(out)],
        env,
    )  # fmt: skip
    status = mock_codec.encode_telemetry("MOCK_STATUS").data  # not a candidate of the filter
    sent = [
        hk(mock_codec, 1),
        status,
        hk(mock_codec, 2),
        hk(mock_codec, 3),
        b"\x01\x02",
        hk(mock_codec, 5),
    ]
    try:
        listen.wait_for("istening")
        send_datagrams(telemetry, sent, pause=0.01)
        assert listen.finish() == 0, listen.stderr
    finally:
        listen.kill()
    printed = [json.loads(line) for line in listen.stdout.splitlines()]
    assert len(printed) == 1  # one frame passed the filter
    assert [f.data for f in read(out)] == sent  # every frame, malformed one included
    assert "recorded 6 frame(s)" in listen.stderr
    assert rec_inspect.scan(out).status == "closed"


def test_events_where_with_record_still_records_every_frame(
    tmp_path: Path, env: dict[str, str], mock_codec: Codec
) -> None:
    out = tmp_path / "ew.ibrec"
    _, telemetry = ports(env)
    events = Proc(
        tmp_path,
        "events",
        ["events", "--count", "1", "--where", "event.id == 7", "--record", str(out)],
        env,
    )
    other = mock_codec.encode_telemetry(
        "MOCK_EVENT", {"app": "DEMO", "event_id": 3, "event_type": "ERROR", "message": "x"}
    ).data
    wanted = mock_codec.encode_telemetry(
        "MOCK_EVENT", {"app": "DEMO", "event_id": 7, "event_type": "ERROR", "message": "y"}
    ).data
    sent = [hk(mock_codec), other, wanted]
    try:
        events.wait_for("istening")
        send_datagrams(telemetry, sent, pause=0.02)
        assert events.finish() == 0, events.stderr
    finally:
        events.kill()
    assert len(events.stdout.splitlines()) == 1
    assert [f.data for f in read(out)] == sent


def test_a_bad_where_with_record_exits_2_and_leaves_no_recording(
    tmp_path: Path, env: dict[str, str]
) -> None:
    out = tmp_path / "bad.ibrec"
    done = run_ib(
        tmp_path, ["listen", "fc", "--where", "cmd_countr > 1", "--record", str(out)], env
    )
    assert done.returncode == 2 and "IB-FLT-002" in done.stderr, done.stderr
    assert not out.exists()


def test_events_record_and_default_location(
    tmp_path: Path, env: dict[str, str], mock_codec: Codec
) -> None:
    _, telemetry = ports(env)
    events = Proc(tmp_path, "events", ["events", "--count", "1", "--record", "auto"], env)
    try:
        events.wait_for("istening")
        packet = mock_codec.encode_telemetry(
            "MOCK_EVENT", {"app": "DEMO", "event_id": 7, "event_type": "ERROR", "message": "low"}
        ).data
        send_datagrams(telemetry, [hk(mock_codec), packet, hk(mock_codec)], pause=0.02)
        assert events.finish() == 0, events.stderr
    finally:
        events.kill()
    [file] = list((tmp_path / "recordings").glob("*.ibrec"))
    frames = read(file)
    assert len(frames) >= 2 and packet in [f.data for f in frames]  # all telemetry, not only events


def test_respond_record_holds_both_directions(tmp_path: Path, env: dict[str, str]) -> None:
    out = tmp_path / "resp.ibrec"
    respond = Proc(tmp_path, "respond", ["respond", "--as", "fc", "--record", str(out)], env)
    try:
        respond.wait_for("responding")
        for command in ("MOCK_NOOP", "MOCK_NOOP"):
            sent = run_ib(tmp_path, ["send", "fc", command], env)
            assert sent.returncode == 0, sent.stderr
        time.sleep(0.3)
        respond.popen.send_signal(signal.SIGTERM)
        assert respond.finish() == 128 + signal.SIGTERM
    finally:
        respond.kill()
    frames = read(out)
    directions = [f.stream.direction.value for f in frames]
    assert directions.count("rx") == 2 and directions.count("tx") == 2
    assert [f.stream.kind.value for f in frames if f.stream.direction.value == "rx"] == [
        "command"
    ] * 2
    assert [f.seq for f in frames] == list(range(1, 5))
    assert rec_inspect.scan(out).status == "closed"


def test_a_recording_that_cannot_be_created_ends_the_command_with_one_error(
    tmp_path: Path, env: dict[str, str]
) -> None:
    out = tmp_path / "exists.ibrec"
    out.write_bytes(b"x")
    done = run_ib(tmp_path, ["listen", "fc", "--timeout", "0.3", "--record", str(out)], env)
    assert done.returncode == 1 and done.stderr.count("IB-REC-001") == 1
    assert out.read_bytes() == b"x"


# -- ib rec --


def make_recording(
    tmp_path: Path, env: dict[str, str], mock_codec: Codec, name: str = "m.ibrec"
) -> Path:
    out = tmp_path / name
    _, telemetry = ports(env)
    rec = Proc(tmp_path, "mk", ["record", "-o", str(out), "--count", "12", "--quiet"], env)
    try:
        rec.wait_for("listening")
        send_datagrams(telemetry, [hk(mock_codec, i) for i in range(12)], pause=0.005)
        assert rec.finish() == 0, rec.stderr
    finally:
        rec.kill()
    return out


def test_rec_info_text_and_json(tmp_path: Path, env: dict[str, str], mock_codec: Codec) -> None:
    out = make_recording(tmp_path, env, mock_codec)
    text = run_ib(tmp_path, ["rec", "info", str(out)], env)
    assert text.returncode == 0 and "status:    closed" in text.stdout
    assert "0x00000890  12" in text.stdout
    data = json.loads(run_ib(tmp_path, ["rec", "info", str(out), "--format", "json"], env).stdout)
    assert data["frames"] == 12 and data["mids"] == [{"mid": 0x890, "count": 12}]
    assert data["streams"][0]["direction"] == "rx" and data["gaps"]["total"] == 0
    assert [d["kind"] for d in data["definitions"]] == [
        "ccdd",
        "command_profile",
        "telemetry_profile",
    ]


def test_rec_check_clean_and_damaged(
    tmp_path: Path, env: dict[str, str], mock_codec: Codec
) -> None:
    out = make_recording(tmp_path, env, mock_codec)
    ok = run_ib(tmp_path, ["rec", "check", str(out)], env)
    assert ok.returncode == 0 and "closed, 12 frame(s)" in ok.stdout
    victim = next(
        i
        for i in RecordingReader(out, skip_damage=True).items()
        if isinstance(i, Item) and i.rtype == RecordType.FRAME
    )
    raw = bytearray(out.read_bytes())
    raw[victim.offset + 30] ^= 0xFF
    bad = tmp_path / "bad.ibrec"
    bad.write_bytes(bytes(raw))
    done = run_ib(tmp_path, ["rec", "check", str(bad)], env)
    assert done.returncode == 1 and "error IB-REC-006" in done.stderr
    info = run_ib(tmp_path, ["rec", "info", str(bad)], env)
    # info reads the index and chunk summaries, not frames: only check finds this damage
    assert info.returncode == 0 and "damaged" not in info.stdout


def test_rec_repair_commands(tmp_path: Path, env: dict[str, str], mock_codec: Codec) -> None:
    out = make_recording(tmp_path, env, mock_codec)
    original = out.read_bytes()
    fixed = tmp_path / "fixed.ibrec"
    done = run_ib(tmp_path, ["rec", "repair", str(out), str(fixed)], env)
    assert done.returncode == 0, done.stderr
    assert fixed.read_bytes() == original  # a clean recording repairs to the same bytes
    again = run_ib(tmp_path, ["rec", "repair", str(out), str(fixed)], env)
    assert again.returncode == 1 and "IB-REC-001" in again.stderr and "--force" in again.stderr
    forced = run_ib(tmp_path, ["rec", "repair", str(out), str(fixed), "--force"], env)
    assert forced.returncode == 0
    same = run_ib(tmp_path, ["rec", "repair", str(out), str(out), "--force"], env)
    assert same.returncode == 1 and "never modified" in same.stderr
    assert out.read_bytes() == original
    # damaged mid-file: the output holds the prefix, exit 5, the original is untouched
    victim = [
        i
        for i in RecordingReader(out, skip_damage=True).items()
        if isinstance(i, Item) and i.rtype == RecordType.FRAME
    ][5]
    raw = bytearray(original)
    raw[victim.offset + 30] ^= 0xFF
    damaged = tmp_path / "damaged.ibrec"
    damaged.write_bytes(bytes(raw))
    prefix = tmp_path / "prefix.ibrec"
    result = run_ib(tmp_path, ["rec", "repair", str(damaged), str(prefix)], env)
    assert result.returncode == 5, result.stderr
    assert "valid frame(s) after the damage are in the original only" in result.stderr
    assert [f.seq for f in read(prefix)] == [1, 2, 3, 4, 5]
    assert rec_inspect.scan(prefix).status == "closed"
    assert damaged.read_bytes() == bytes(raw)
    assert not list(tmp_path.glob(".*.tmp"))  # no temporary file is left behind


def test_rec_tools_report_unreadable_files_with_codes(tmp_path: Path, env: dict[str, str]) -> None:
    missing = run_ib(tmp_path, ["rec", "info", str(tmp_path / "nope.ibrec")], env)
    assert missing.returncode == 1 and "error IB-REC-004" in missing.stderr
    junk = tmp_path / "junk.ibrec"
    junk.write_bytes(b"x" * 100)
    for command in ("info", "check"):
        done = run_ib(tmp_path, ["rec", command, str(junk)], env)
        assert done.returncode == 1 and "IB-REC-004" in done.stderr, command
    done = run_ib(tmp_path, ["rec", "repair", str(junk), str(tmp_path / "o.ibrec")], env)
    assert done.returncode == 1 and not (tmp_path / "o.ibrec").exists()


def test_rec_commands_work_without_any_config(tmp_path: Path, mock_codec: Codec) -> None:
    from recutil import sample

    out = tmp_path / "a.ibrec"
    sample(out, 10)
    clean_env = {k: v for k, v in os.environ.items() if k != "IRONBIRD_CONFIG"}
    done = run_ib(tmp_path, ["rec", "check", str(out)], clean_env)
    assert done.returncode == 0, done.stderr


# -- config --


@pytest.mark.parametrize(
    "table",
    [
        'fsync = "sometimes"',
        "fsync = 1",
        "max_buffer_bytes = 100",
        "max_buffer_bytes = true",
        'max_buffer_bytes = "256MiB"',
        "min_free_bytes = -1",
        "min_free_bytes = 1.5",
        'embed_definitions = "yes"',
        "dir = ''",
        "unknown_key = 1",
        "dir = 5",
    ],
)
def test_recording_config_is_validated(tmp_path: Path, table: str) -> None:
    path, _, _ = write_config(tmp_path, f"[recording]\n{table}\n")
    with pytest.raises(IronbirdError) as caught:
        load_config(path)
    assert caught.value.code is Code.CFG_SCHEMA and "recording" in caught.value.summary


def test_recording_config_values(tmp_path: Path) -> None:
    path, _, _ = write_config(
        tmp_path,
        '[recording]\ndir = "out"\nfsync = "always"\nmax_buffer_bytes = 2097152\n'
        "min_free_bytes = 0\nembed_definitions = false\n",
    )
    cfg = load_config(path).recording
    assert cfg.dir == tmp_path / "out" and cfg.fsync == "always"
    assert (cfg.max_buffer_bytes, cfg.min_free_bytes, cfg.embed_definitions) == (2097152, 0, False)


def test_embed_definitions_false_records_hashes_only(tmp_path: Path, mock_codec: Codec) -> None:
    path, _, telemetry = write_config(
        tmp_path, "[recording]\nmin_free_bytes = 0\nembed_definitions = false\n"
    )
    env = {**os.environ, "IRONBIRD_CONFIG": str(path)}
    done = run_ib(tmp_path, ["record", "-o", str(tmp_path / "n.ibrec"), "--duration", "0.2"], env)
    assert done.returncode == 0, done.stderr
    report = rec_inspect.info(tmp_path / "n.ibrec")
    assert report.blobs == []
    nodes = report.meta["nodes"]["fc"]
    assert len(nodes["ccdd_sha256"]) == 64 and report.meta["schema"] == fmt.META_SCHEMA
    _ = telemetry
