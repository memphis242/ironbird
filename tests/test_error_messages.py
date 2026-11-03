"""User-facing error reporting: the false or silent outcomes, split codes, coalesced totals.

These tests assert codes, exit statuses and key facts (a number, a name, a pointer to the next
step), never exact wording, so the text can be improved without touching them.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from ironbird.codec import Codec
from ironbird.recording.format import RecordType
from ironbird.recording.reader import Item, RecordingReader
from recutil import run_ib
from replayutil import child_env, hk_frames, write_config_58k, write_recording


@pytest.fixture
def env(tmp_path: Path) -> dict[str, str]:
    path, _, _ = write_config_58k(tmp_path)
    environment = child_env(tmp_path)
    environment["IRONBIRD_CONFIG"] = str(path)
    return environment


def first_record_offset(path: Path, rtype: int) -> int:
    with RecordingReader(path) as reader:
        for item in reader.items():
            if isinstance(item, Item) and item.rtype == rtype:
                return item.offset
    raise AssertionError("no such record")


def flipped_definitions(tmp_path: Path, frames: int = 11) -> Path:
    """A closed recording whose embedded CCDD record fails its CRC (the streams after it are
    intact), so the definitions are lost and the frames are not."""
    good = tmp_path / "good.ibrec"
    write_recording(good, hk_frames(frames))
    raw = bytearray(good.read_bytes())
    raw[first_record_offset(good, RecordType.BLOB) + 30] ^= 0xFF
    bad = tmp_path / "flip.ibrec"
    bad.write_bytes(bytes(raw))
    return bad


def codes_in(text: str) -> list[str]:
    return [word.rstrip(":") for word in text.split() if word.startswith("IB-")]


def events(tmp_path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for path in sorted((tmp_path / "state").glob("*.jsonl")):
        out += [json.loads(line) for line in path.read_text().splitlines()]
    return out


# -- replay --skip-damage after damage to the embedded definitions --


def test_replay_skip_damage_with_config_defs_decodes_after_damaged_definitions(
    tmp_path: Path, env: dict[str, str]
) -> None:
    bad = flipped_definitions(tmp_path)
    done = run_ib(tmp_path, ["replay", "--skip-damage", "--defs", "config", str(bad)], env)
    assert done.returncode == 5, done.stderr  # damage was skipped
    assert "UNDECODED" not in done.stdout
    assert len(done.stdout.splitlines()) == 11
    assert "without definitions" not in done.stderr
    assert "IB-REC-009" in done.stderr and "damaged" in done.stderr  # says what it decoded with


def test_replay_skip_damage_with_embedded_defs_names_the_damage_not_a_missing_setting(
    tmp_path: Path, env: dict[str, str]
) -> None:
    bad = flipped_definitions(tmp_path)
    done = run_ib(tmp_path, ["replay", "--skip-damage", str(bad)], env)
    assert done.returncode == 1, done.stderr
    assert "IB-REC-009" in done.stderr
    assert "--defs config" in done.stderr
    assert "embed_definitions" not in done.stderr
    assert "UNDECODED" not in done.stdout


# -- rec info, check and repair agree, and none reads as success when it is not --


def test_rec_info_of_a_closed_file_does_not_claim_the_contents_were_checked(
    tmp_path: Path, env: dict[str, str]
) -> None:
    bad = flipped_definitions(tmp_path)
    info = run_ib(tmp_path, ["rec", "info", str(bad), "--format", "json"], env)
    assert info.returncode == 0, info.stderr
    assert json.loads(info.stdout)["source"] == "index"  # summaries, not records
    text = run_ib(tmp_path, ["rec", "info", str(bad)], env)
    assert "closed cleanly" not in text.stdout
    assert "rec check" in text.stdout  # where the contents are verified
    check = run_ib(tmp_path, ["rec", "check", str(bad)], env)
    assert check.returncode == 1 and "IB-REC-006" in check.stderr
    assert "--skip-damage" in check.stderr and "rec repair" in check.stderr  # next steps


def test_rec_repair_that_keeps_no_frame_is_not_success_and_points_to_replay(
    tmp_path: Path, env: dict[str, str]
) -> None:
    bad = flipped_definitions(tmp_path, frames=11)
    out = tmp_path / "fixed.ibrec"
    done = run_ib(tmp_path, ["rec", "repair", str(bad), str(out)], env)
    assert done.returncode == 5, done.stderr
    assert "0 of 11" in done.stdout  # what was kept out of what exists
    assert not done.stdout.startswith("repaired")
    assert "IB-REC-013" in done.stderr and "11" in done.stderr
    assert "replay --skip-damage" in done.stderr
    assert out.exists() and bad.read_bytes() != out.read_bytes()


def test_rec_check_and_rec_repair_call_a_cut_closing_the_same_thing(
    tmp_path: Path, env: dict[str, str]
) -> None:
    good = tmp_path / "good.ibrec"
    write_recording(good, hk_frames(11))
    raw = good.read_bytes()
    torn = tmp_path / "torn.ibrec"
    torn.write_bytes(raw[:-37])  # the trailer is cut short, the index before it is whole
    check = run_ib(tmp_path, ["rec", "check", str(torn)], env)
    out = tmp_path / "t2.ibrec"
    repair = run_ib(tmp_path, ["rec", "repair", str(torn), str(out)], env)
    assert check.returncode == 0 and repair.returncode == 0, (check.stderr, repair.stderr)
    assert "IB-REC-007" in check.stderr and "IB-REC-007" in repair.stderr
    assert "IB-REC-006" not in repair.stderr and "IB-REC-013" not in repair.stderr
    assert "11 frame" in repair.stdout


# -- a recording that cannot be opened is not a recording of the wrong format --


def test_a_missing_recording_and_a_file_that_is_not_one_have_different_codes(
    tmp_path: Path, env: dict[str, str]
) -> None:
    missing = run_ib(tmp_path, ["rec", "info", str(tmp_path / "nope.ibrec")], env)
    junk = tmp_path / "junk.ibrec"
    junk.write_bytes(b"not a recording " * 8)
    wrong = run_ib(tmp_path, ["rec", "info", str(junk)], env)
    assert missing.returncode == 1 and wrong.returncode == 1
    assert "IB-REC-014" in missing.stderr and "IB-REC-004" not in missing.stderr
    assert "IB-REC-004" in wrong.stderr and "IB-REC-014" not in wrong.stderr


# -- coalesced lines carry totals; a fold is one line with a subject --


def test_a_coalesced_line_carries_the_total_of_the_amounts_not_the_latest() -> None:
    from ironbird.coalesce import Coalescer
    from ironbird.errors import Code, counted
    from ironbird.notice import Level, Notice

    notices: list[Notice] = []
    now = [0.0]
    co = Coalescer(notices.append, clock=lambda: now[0])
    for amount in (28, 40, 72):
        error = counted(Code.TX_RESYNC, "serial X: {n} byte(s) skipped", amount)
        assert error.tally is not None
        co.report(
            error.code,
            Level.WARNING,
            "k",
            error.summary,
            amount=error.tally[0],
            template=error.tally[1],
        )
        now[0] += 1.0
    co.flush()
    assert [n.count for n in notices] == [1, 3]
    assert "28" in notices[0].summary and "140" in notices[1].summary  # 28 + 40 + 72
    assert "x3" in notices[1].line()


def test_dropped_stdout_lines_are_reported_as_unsaved_with_a_total() -> None:
    from ironbird.cli.main import _report_output_drops
    from ironbird.errors import Code, IronbirdError

    reported: list[IronbirdError] = []

    class Session:
        def report(self, error: IronbirdError, **_: object) -> None:
            reported.append(error)

    class Console:
        def take_new_drops(self) -> tuple[int, int]:
            return 6869, 0

    _report_output_drops(Session(), Console())  # type: ignore[arg-type]
    [error] = reported
    assert error.code is Code.OUT_DROPPED and error.tally is not None and error.tally[0] == 6869
    assert "6869" in error.summary
    assert "ledger" not in error.summary  # the lines are not kept there


# -- filters that can never be true for a message --


def test_an_ordering_outside_a_fields_range_warns_but_still_compiles(
    mock_codec: Codec,
) -> None:
    from ironbird.errors import Code
    from ironbird.filter import compile_filter

    codecs = {"fc": mock_codec}
    never = compile_filter("cmd_counter > 300", codecs)
    assert [w.code for w in never.warnings] == [Code.FLT_TYPE]
    assert "300" in never.warnings[0].summary and "uint8" in never.warnings[0].summary
    for text in ("cmd_counter > 100", "cmd_counter >= 255", "cmd_counter < 1", "cmd_counter <= 0"):
        assert compile_filter(text, codecs).warnings == (), text
    for text in ("cmd_counter >= 256", "cmd_counter < 0", "cmd_counter <= -1"):
        assert compile_filter(text, codecs).warnings, text


def test_a_filter_that_cannot_match_is_a_warning_on_replay_with_exit_0(
    tmp_path: Path, env: dict[str, str]
) -> None:
    good = tmp_path / "good.ibrec"
    write_recording(good, hk_frames(5))
    done = run_ib(tmp_path, ["replay", "--where", "cmd_counter > 300", str(good)], env)
    assert done.returncode == 0, done.stderr
    assert "warning IB-FLT-003" in done.stderr and "300" in done.stderr
    assert done.stdout == ""
    quiet = run_ib(tmp_path, ["replay", "--where", "cmd_counter > 2", str(good)], env)
    assert "IB-FLT-003" not in quiet.stderr and quiet.stdout


# -- configuration errors echo the value, name the key and suggest the nearest name --


def config_with(tmp_path: Path, old: str, new: str, env: dict[str, str]) -> Path:
    path = Path(env["IRONBIRD_CONFIG"])
    text = path.read_text()
    assert old in text, old
    edited = tmp_path / "edited.toml"
    edited.write_text(text.replace(old, new, 1))
    return edited


@pytest.mark.parametrize(
    ("old", "new", "key", "written", "suggestion"),
    [
        ('alias = "flight"', 'alais = "flight"', "nodes.fc.alais", "", "alias"),
        (
            "[nodes.fc]",
            '[ledger]\nmax_file_bytes = "1GB"\n\n[nodes.fc]',
            "ledger.max_file_bytes",
            "1GB",
            "1GiB",
        ),
        (
            "[nodes.fc]",
            "[ledger]\nmax_file_bytes = 100\n\n[nodes.fc]",
            "ledger.max_file_bytes",
            "100",
            "1 MiB",
        ),
        (
            "[nodes.fc]",
            '[retention]\nmax_age = "30 days"\n\n[nodes.fc]',
            "retention.max_age",
            "30 days",
            "30d",
        ),
        ('to = "fc"', 'to = "fcx"', "links[0].to", "fcx", "fc"),
    ],
)
def test_config_errors_say_what_was_written_and_what_to_write(
    tmp_path: Path,
    env: dict[str, str],
    old: str,
    new: str,
    key: str,
    written: str,
    suggestion: str,
) -> None:
    edited = config_with(tmp_path, old, new, env)
    done = run_ib(tmp_path, ["-c", str(edited), "nodes"], env)
    assert done.returncode == 2, done.stderr
    line = done.stderr.splitlines()[0]
    assert line.startswith("error IB-CFG-00") and key in line
    assert written in line and suggestion in line
    assert len(done.stderr.splitlines()) == 1


def test_responder_values_are_reported_against_their_config_key_not_as_an_argument(
    tmp_path: Path, env: dict[str, str]
) -> None:
    edited = config_with(tmp_path, "fields = { mode = 2 }", 'fields = { mode = "BAD" }', env)
    done = run_ib(tmp_path, ["-c", str(edited), "respond", "--as", "fc", "--count", "1"], env)
    assert done.returncode == 2, done.stderr
    assert "IB-CFG-003" in done.stderr and "IB-ENC-002" not in done.stderr
    assert "responses[4].fields.mode" in done.stderr and "BAD" in done.stderr


def test_a_message_over_the_packet_cap_names_the_key_to_change(
    tmp_path: Path, env: dict[str, str]
) -> None:
    small = config_with(
        tmp_path, 'acts_as = "ground"', 'acts_as = "ground"\nmax_packet_bytes = 64', env
    )
    done = run_ib(tmp_path, ["-c", str(small), "send", "fc", "MOCK_NOOP"], env)
    assert done.returncode == 2, done.stderr
    assert "ironbird.max_packet_bytes" in done.stderr
    assert "MOCK_LOAD_DATA" in done.stderr  # the message that does not fit, not every command


def test_a_missing_definition_file_names_the_config_key_that_pulled_it_in(
    tmp_path: Path, env: dict[str, str]
) -> None:
    edited = config_with(tmp_path, "ccdd/mock.json", "ccdd/absent.json", env)
    done = run_ib(tmp_path, ["-c", str(edited), "send", "fc", "MOCK_NOOP"], env)
    assert done.returncode == 2, done.stderr
    assert "IB-CCD-001" in done.stderr and "nodes.fc.ccdd" in done.stderr


# -- the retention warning does not recommend a prune that cannot help --


def test_past_the_cap_with_everything_protected_says_raise_the_cap(tmp_path: Path) -> None:
    from ironbird.config import RetentionConfig
    from ironbird.errors import Code
    from ironbird.units import MIB
    from retutil import make_session
    from test_retention_service import Rig

    rig = Rig(tmp_path, RetentionConfig(max_total_bytes=MIB, keep_recent=10))
    for i in range(1, 6):
        make_session(rig.state, i, age_days=2, size=300_000, klass="short")
    rig.retention.start()
    rig.settle()
    (warning,) = [n for n in rig.shown if n.code is Code.RET_TOTAL]
    assert "retention.max_total_bytes" in warning.summary
    assert "--until-under" not in warning.summary  # that prune would delete nothing
    assert "protected" in warning.summary
    rig.ledger.close()


def test_past_the_cap_with_sessions_to_delete_names_the_prune(tmp_path: Path) -> None:
    from ironbird.config import RetentionConfig
    from ironbird.errors import Code
    from ironbird.units import MIB
    from retutil import make_session
    from test_retention_service import Rig

    rig = Rig(tmp_path, RetentionConfig(max_total_bytes=MIB, keep_recent=0))
    for i in range(1, 6):
        make_session(rig.state, i, age_days=2, size=300_000, klass="short")
    rig.retention.start()
    rig.settle()
    (warning,) = [n for n in rig.shown if n.code is Code.RET_TOTAL]
    assert "ib ledger prune --until-under" in warning.summary
    rig.ledger.close()


# -- the ledger directory that cannot be used --


def test_an_unusable_state_directory_is_one_line_per_outcome(
    tmp_path: Path, env: dict[str, str]
) -> None:
    blocker = tmp_path / "afile"
    blocker.write_text("x")
    bad = dict(env, IRONBIRD_STATE_DIR=str(blocker / "state"))
    sent = run_ib(tmp_path, ["send", "fc", "MOCK_NOOP"], bad)
    assert sent.returncode == 1, sent.stderr
    lines = sent.stderr.splitlines()
    assert len(lines) == 1 and lines[0].startswith("error IB-LED-002")  # and says nothing was sent
    assert "sent" in lines[0] and "IRONBIRD_STATE_DIR" in lines[0]
    read = run_ib(tmp_path, ["find", "MOCK"], bad)
    assert read.returncode == 0, read.stderr  # a command that only reads goes on
    assert "warning IB-LED-002" in read.stderr and "error IB-LED-002" not in read.stderr


def test_a_fatal_error_with_detail_points_to_verbose(tmp_path: Path, env: dict[str, str]) -> None:
    from ironbird.cli.main import _print_error
    from ironbird.errors import Code, IronbirdError

    lines: list[str] = []

    class Console:
        def err(self, text: str) -> None:
            lines.append(text)

    _print_error(Console(), IronbirdError(Code.TX_OPEN, "cannot open", details="more"), False)  # type: ignore[arg-type]
    _print_error(Console(), IronbirdError(Code.TX_OPEN, "cannot open"), False)  # type: ignore[arg-type]
    assert "-v" in lines[0] and "-v" not in lines[1]
    assert all("\n" not in line for line in lines)


# -- frames left out of an export are not a write failure --


def test_frames_a_single_csv_cannot_hold_are_exp_004_not_a_write_error(
    tmp_path: Path, env: dict[str, str]
) -> None:
    from recutil import RX, frame
    from replayutil import hk

    recording = tmp_path / "mal.ibrec"
    write_recording(recording, [*hk_frames(3), frame(4, stream=RX, data=hk(4)[:10], mid=0x890)])
    out = tmp_path / "o.csv"
    done = run_ib(tmp_path, ["export", str(recording), "-o", str(out), "--message", "MOCK_HK"], env)
    assert done.returncode == 0, done.stderr
    assert "warning IB-EXP-004" in done.stderr and "IB-EXP-001" not in done.stderr
    assert out.exists()


# -- the head walk past damage (the parser change behind the replay fix) --


def test_the_head_walk_finds_the_streams_after_damaged_definitions_only_when_skipping(
    tmp_path: Path,
) -> None:
    from ironbird.recording.defs import read_head

    bad = flipped_definitions(tmp_path)
    with RecordingReader(bad, skip_damage=True) as reader:
        skipping = read_head(reader, skip_damage=True)
    with RecordingReader(bad) as reader:
        stopping = read_head(reader)
    assert skipping.damaged and skipping.streams and not skipping.problem
    assert len(skipping.blobs) < 3  # the damaged definition is gone
    assert not stopping.damaged and not stopping.streams  # it ends where the damage begins


def test_a_head_that_is_damaged_end_to_end_never_raises_anything_but_a_coded_error(
    tmp_path: Path,
) -> None:
    from ironbird.errors import IronbirdError
    from ironbird.recording.defs import read_head

    good = tmp_path / "good.ibrec"
    write_recording(good, hk_frames(3))
    raw = good.read_bytes()
    for offset in range(40, len(raw) - 100, 211):  # a flip in every part of the file
        mutated = bytearray(raw)
        mutated[offset] ^= 0x5A
        path = tmp_path / "m.ibrec"
        path.write_bytes(bytes(mutated))
        try:
            with RecordingReader(path, skip_damage=True) as reader:
                read_head(reader, skip_damage=True)
        except IronbirdError:
            continue
