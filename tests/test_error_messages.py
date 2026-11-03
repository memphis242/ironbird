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


def test_replay_skip_damage_with_lost_definitions_keeps_the_frames_and_says_why(
    tmp_path: Path, env: dict[str, str]
) -> None:
    bad = flipped_definitions(tmp_path)
    done = run_ib(tmp_path, ["replay", "--skip-damage", str(bad)], env)
    assert done.returncode == 5, done.stderr  # the damage was skipped: evidence is kept
    assert len(done.stdout.splitlines()) == 11 and "UNDECODED" in done.stdout
    [line] = [ln for ln in done.stderr.splitlines() if "IB-REC-009" in ln]  # one line, not 11
    assert "--defs config" in line and "embed_definitions" not in line
    # Without --skip-damage the replay stops at the damage and shows nothing.
    stopped = run_ib(tmp_path, ["replay", str(bad)], env)
    assert stopped.returncode == 5 and stopped.stdout == ""
    # Export has nothing to write without definitions: it is refused, with the way out.
    out = tmp_path / "o.csv"
    refused = run_ib(
        tmp_path, ["export", "--skip-damage", str(bad), "-o", str(out), "--message", "MOCK_HK"], env
    )
    assert refused.returncode == 1 and "IB-REC-009" in refused.stderr
    assert "--defs config" in refused.stderr and "embed_definitions" not in refused.stderr


def first_frame_flipped(tmp_path: Path, name: str, **write: Any) -> Path:
    good = tmp_path / f"{name}-good.ibrec"
    write_recording(good, hk_frames(11), **write)
    raw = bytearray(good.read_bytes())
    raw[first_record_offset(good, RecordType.FRAME) + 30] ^= 0xFF
    bad = tmp_path / f"{name}.ibrec"
    bad.write_bytes(bytes(raw))
    return bad


def test_damage_to_a_frame_is_not_damage_to_the_definitions(
    tmp_path: Path, env: dict[str, str]
) -> None:
    from ironbird.recording.defs import lost_definitions, read_head

    bad = first_frame_flipped(tmp_path, "frame0")
    with RecordingReader(bad, skip_damage=True) as reader:
        head = read_head(reader, skip_damage=True)
        meta = reader.meta.data
    assert len(head.blobs) == 3 and head.streams
    assert lost_definitions(head, ["fc"], meta) == []
    for args in (["--defs", "config"], []):
        done = run_ib(tmp_path, ["replay", "--skip-damage", *args, str(bad)], env)
        assert done.returncode == 5, done.stderr  # the frame damage
        assert len(done.stdout.splitlines()) == 10 and "UNDECODED" not in done.stdout
        assert "IB-REC-009" not in done.stderr, done.stderr  # the definitions are whole


def test_a_recording_made_without_definitions_never_blames_damage_for_their_absence(
    tmp_path: Path, env: dict[str, str]
) -> None:
    from replayutil import nodes_table

    table = nodes_table()
    table["fc"]["embedded"] = False  # what a recorder writes under embed_definitions = false
    for damaged in (False, True):
        if damaged:
            path = first_frame_flipped(tmp_path, "nob-f0", blobs=False, meta_extra={"nodes": table})
        else:
            path = tmp_path / "nob.ibrec"
            write_recording(path, hk_frames(11), blobs=False, meta_extra={"nodes": table})
        done = run_ib(tmp_path, ["replay", "--skip-damage", str(path)], env)
        assert done.returncode == 1, done.stderr
        [line] = [ln for ln in done.stderr.splitlines() if "IB-REC-009" in ln]
        assert "embed_definitions" in line and "damaged" not in line, line


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


def test_a_recording_still_being_written_is_not_called_unclosed_or_sent_to_repair(
    tmp_path: Path, env: dict[str, str]
) -> None:
    import asyncio

    from ironbird.recording.writer import Recorder
    from recutil import FAST, RX, TX
    from replayutil import blob_specs, meta_data

    live = tmp_path / "live.ibrec"
    recorder = Recorder.open(live, meta_data(), blobs=blob_specs(), streams=(RX, TX), options=FAST)
    for item in hk_frames(5):
        recorder.append(item)

    async def while_open() -> tuple[Any, Any]:
        await asyncio.sleep(0.5)  # the writer flushes its buffer
        found = (
            run_ib(tmp_path, ["rec", "check", str(live)], env),
            run_ib(tmp_path, ["rec", "info", str(live)], env),
        )
        await recorder.close()
        return found

    check, info = asyncio.run(while_open())
    assert check.returncode == 0, check.stderr
    assert "still being written" in check.stderr
    assert "rec repair" not in check.stderr and "rec repair" not in info.stderr
    assert "warning IB-REC-007" not in check.stderr
    closed = run_ib(tmp_path, ["rec", "check", str(live)], env)
    assert closed.returncode == 0 and "IB-REC-007" not in closed.stderr


def test_rec_repair_that_keeps_no_frame_is_not_success_and_points_to_replay(
    tmp_path: Path, env: dict[str, str]
) -> None:
    bad = flipped_definitions(tmp_path, frames=11)
    out = tmp_path / "fixed.ibrec"
    done = run_ib(tmp_path, ["rec", "repair", str(bad), str(out)], env)
    assert done.returncode == 5, done.stderr
    assert "kept 0 frame" in done.stdout and "11" in done.stdout  # kept, and what is left behind
    assert not done.stdout.startswith("repaired")
    assert "IB-REC-013" in done.stderr and "11" in done.stderr
    assert "replay --skip-damage" in done.stderr
    assert out.exists() and bad.read_bytes() != out.read_bytes()


def test_rec_repair_is_partial_only_when_valid_frames_were_left_in_the_original(
    tmp_path: Path, env: dict[str, str]
) -> None:
    good = tmp_path / "good.ibrec"
    write_recording(good, hk_frames(11))
    frames = [
        item.offset
        for item in RecordingReader(good).items()
        if isinstance(item, Item) and item.rtype == RecordType.FRAME
    ]
    raw = good.read_bytes()
    last = bytearray(raw)
    last[frames[-1] + 30] ^= 0xFF  # only INDEX and TRAILER follow the damage
    (tmp_path / "last.ibrec").write_bytes(bytes(last))
    junk = tmp_path / "junk.ibrec"
    junk.write_bytes(raw + b"\x00" * 64)  # nothing lost, bytes after the trailer
    for name, kept in (("last", 10), ("junk", 11)):
        done = run_ib(
            tmp_path,
            ["rec", "repair", str(tmp_path / f"{name}.ibrec"), str(tmp_path / "o.ibrec")],
            env,
        )
        assert done.returncode == 5, (name, done.stderr)  # the damage is reported, as before
        assert done.stdout.startswith("repaired"), (name, done.stdout)
        assert f"kept {kept} frame" in done.stdout and " of " not in done.stdout.split(",")[0]
        assert "IB-REC-013" not in done.stderr  # no valid frame was left behind
        (tmp_path / "o.ibrec").unlink()
    middle = bytearray(raw)
    middle[frames[5] + 30] ^= 0xFF
    spaced = tmp_path / "with space.ibrec"
    spaced.write_bytes(bytes(middle))
    done = run_ib(tmp_path, ["rec", "repair", str(spaced), str(tmp_path / "o2.ibrec")], env)
    assert done.stdout.startswith("partial repair") and "kept 5 frame" in done.stdout
    assert "5 more valid frame" in done.stdout, done.stdout
    assert "IB-REC-013" in done.stderr and "'" + str(spaced) + "'" in done.stderr  # copy-pasteable


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


def test_a_fold_counts_occurrences_and_does_not_call_them_distinct_problems() -> None:
    from ironbird.coalesce import FOLD_SUMMARY, MAX_NEW_PER_GROUP, Coalescer
    from ironbird.errors import Code
    from ironbird.notice import Level, Notice

    now = [0.0]
    shown: list[Notice] = []
    coalescer = Coalescer(shown.append, clock=lambda: now[0])
    for mid in range(MAX_NEW_PER_GROUP):
        key = f"k{mid}"
        coalescer.report(Code.DEC_UNKNOWN, Level.ERROR, key, f"MID {mid}", group=Code.DEC_UNKNOWN)
    for _ in range(1000):  # one more problem, over and over
        coalescer.report(Code.DEC_UNKNOWN, Level.ERROR, "late", "MID 99", group=Code.DEC_UNKNOWN)
    now[0] = 11.0
    coalescer.tick()
    [fold] = [n for n in shown if n.summary == FOLD_SUMMARY]
    assert fold.count == 1000
    assert "distinct" not in FOLD_SUMMARY and "occurrences" in FOLD_SUMMARY


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


def wide_codec(extra_groups: int = 0) -> Codec:
    """The mock node plus MOCK_WIDE, where ``cmd_counter`` is a uint16 (so a uint8 and a uint16
    group share the name), and ``extra_groups`` more uint8 ``v`` fields that differ by enum."""
    from ironbird.ccdd.loader import load_ccdd_bytes
    from ironbird.header import load_profile
    from replayutil import (
        CCDD_PATH,
        COMMAND_PROFILE_PATH,
        TELEMETRY_PROFILE_PATH,
    )

    data = json.loads(CCDD_PATH.read_text())

    def message(name: str, mid: int, field: dict[str, Any]) -> dict[str, Any]:
        return {
            "name": name,
            "kind": "telemetry",
            "mid": f"0x{mid:04x}",
            "description": "wide",
            "category": "status",
            "fields": [field],
        }

    data["messages"].append(message("MOCK_WIDE", 0x894, {"name": "cmd_counter", "type": "uint16"}))
    for i in range(extra_groups):
        field = {"name": "v", "type": "uint8", "enum": {f"L{i}": i}}
        data["messages"].append(message(f"MOCK_V{i}", 0x900 + i, field))
    if extra_groups:
        data["messages"].append(message("MOCK_VW", 0x9FF, {"name": "v", "type": "uint16"}))
    model = load_ccdd_bytes(json.dumps(data).encode(), "wide.json", "json")
    return Codec(
        model,
        load_profile(COMMAND_PROFILE_PATH),
        load_profile(TELEMETRY_PROFILE_PATH),
        max_packet_bytes=2048,
    )


def test_a_never_true_equality_and_ordering_are_refused_alike_when_no_message_can_match(
    mock_codec: Codec,
) -> None:
    from ironbird.errors import Code
    from ironbird.filter import FilterError, compile_filter

    codecs = {"fc": mock_codec}
    for text in (
        "cmd_counter == 300",
        "cmd_counter > 300",
        "cmd_counter >= 256",
        "cmd_counter < 0",
    ):
        with pytest.raises(FilterError) as raised:
            compile_filter(text, codecs)
        assert raised.value.code is Code.FLT_TYPE, text
    # Every uint8 differs from 300, so `!=` is true of all of them: not an error, not a warning.
    always = compile_filter("cmd_counter != 300", codecs)
    assert always.warnings == ()
    for text in ("cmd_counter > 100", "cmd_counter >= 255", "cmd_counter < 1", "cmd_counter <= 0"):
        assert compile_filter(text, codecs).warnings == (), text


def test_a_never_true_part_for_some_messages_only_is_a_warning_naming_them() -> None:
    from ironbird.errors import Code
    from ironbird.filter import compile_filter

    codecs = {"fc": wide_codec()}
    for text in ("cmd_counter == 300", "cmd_counter > 300", "cmd_counter in [1, 300]"):
        flt = compile_filter(text, codecs)
        expected = 0 if text.endswith("[1, 300]") else 1  # 1 fits a uint8
        assert len(flt.warnings) == expected, text
        for warning in flt.warnings:
            assert warning.code is Code.FLT_TYPE
            assert "MOCK_HK" in warning.summary and "uint8" in warning.summary
    assert compile_filter("cmd_counter != 300", codecs).warnings == ()  # true of every uint8
    written = compile_filter("cmd_counter > 255.5", codecs).warnings
    assert len(written) == 1 and "255.5" in written[0].summary  # the bound as the user wrote it


def test_a_never_true_warning_skips_messages_the_filter_already_excludes() -> None:
    from ironbird.filter import compile_filter

    codecs = {"fc": wide_codec()}
    assert compile_filter("msg == MOCK_WIDE and cmd_counter > 300", codecs).warnings == ()
    assert (
        compile_filter("cmd_counter > 300", codecs, restrict_mids=frozenset({0x894})).warnings == ()
    )
    assert compile_filter("cmd_counter > 300", codecs, restrict_mids=frozenset({0x890})).warnings
    # Reachable through the other branch of an `or`, so it is still worth saying.
    assert compile_filter("msg == MOCK_HK or cmd_counter > 300", codecs).warnings
    # Under a `not` the never-true part makes the whole true there: nothing to warn about.
    assert compile_filter("not cmd_counter > 300", codecs).warnings == ()


def test_never_true_warnings_past_the_cap_say_how_many_more_there_are() -> None:
    from ironbird.filter import compile_filter
    from ironbird.filter.compiler import MAX_WARNINGS

    flt = compile_filter("v > 300", {"fc": wide_codec(extra_groups=MAX_WARNINGS + 3)})
    assert len(flt.warnings) == MAX_WARNINGS + 1
    assert "3 more" in flt.warnings[-1].summary


def test_a_filter_that_cannot_match_is_refused_on_replay_with_exit_2(
    tmp_path: Path, env: dict[str, str]
) -> None:
    good = tmp_path / "good.ibrec"
    write_recording(good, hk_frames(5))
    done = run_ib(tmp_path, ["replay", "--where", "cmd_counter > 300", str(good)], env)
    assert done.returncode == 2, done.stderr
    assert "IB-FLT-003" in done.stderr and "300" in done.stderr
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
    # The cause of the failure is one step away, as for any other fatal error.
    assert "(more: -v)" in sent.stderr
    verbose = run_ib(tmp_path, ["-v", "send", "fc", "MOCK_NOOP"], bad)
    assert "caused by" in verbose.stderr and "(more: -v)" not in verbose.stderr
    quiet_read = run_ib(tmp_path, ["-v", "find", "MOCK"], bad)
    assert "caused by" in quiet_read.stderr


def test_a_fatal_error_with_detail_points_to_verbose(tmp_path: Path, env: dict[str, str]) -> None:
    from ironbird.cli.main import _print_error
    from ironbird.errors import Code, IronbirdError

    lines: list[str] = []

    class Console:
        def err(self, text: str) -> None:
            lines.append(text)

    _print_error(Console(), IronbirdError(Code.TX_OPEN, "cannot open", details="more"), False)  # type: ignore[arg-type]
    _print_error(Console(), IronbirdError(Code.TX_OPEN, "cannot open"), False)  # type: ignore[arg-type]
    caused = IronbirdError(Code.TX_OPEN, "cannot open", cause=OSError(2, "gone"))
    _print_error(Console(), caused, False)  # type: ignore[arg-type]
    assert "-v" in lines[0] and "-v" not in lines[1]
    assert "-v" in lines[2]  # a cause chain is detail too
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
    from ironbird.recording.defs import lost_definitions, read_head

    bad = flipped_definitions(tmp_path)
    with RecordingReader(bad, skip_damage=True) as reader:
        skipping = read_head(reader, skip_damage=True)
        meta = reader.meta.data
    with RecordingReader(bad) as reader:
        stopping = read_head(reader)
    assert skipping.streams and not skipping.problem
    assert len(skipping.blobs) < 3  # the damaged definition is gone
    assert lost_definitions(skipping, ["fc"], meta) == ["fc:ccdd"]  # and META says it was there
    assert not stopping.streams  # it ends where the damage begins


def test_the_head_walk_leaves_a_record_a_writer_is_still_on_alone(tmp_path: Path) -> None:
    from ironbird.recording.defs import read_head

    good = tmp_path / "good.ibrec"
    write_recording(good, hk_frames(3))
    blob = first_record_offset(good, RecordType.BLOB)
    cut = tmp_path / "cut.ibrec"
    cut.write_bytes(good.read_bytes()[: blob + 20])  # inside the first BLOB, nothing after it
    with RecordingReader(cut, skip_damage=True) as reader:
        head = read_head(reader, skip_damage=True)
    assert not head.blobs and not head.problem


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


# -- a queue that overflows is worded for what drains it --


@pytest.mark.parametrize(
    ("consumer", "level", "word"),
    [
        ("DISPLAY", "WARNING", "not shown"),
        ("RESPONDER", "ERROR", "not answered"),
        ("EVENT_STREAM", "ERROR", "events"),
        ("RECORDER", None, ""),
    ],
)
def test_a_full_capture_queue_is_worded_and_levelled_for_its_consumer(
    consumer: str,
    level: str | None,
    word: str,
    mock_config_path: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import asyncio

    from ironbird import engine
    from ironbird.config import load_config
    from ironbird.errors import Code
    from ironbird.notice import Level, Notice
    from ironbird.session import Session
    from test_engine import ListSink, blast, run_ground_and_fc, until

    monkeypatch.setattr("ironbird.session.TICK_S", 0.05)
    config = load_config(mock_config_path)

    async def scenario(ground: Session, fc: Session, notices: list[Notice]) -> None:
        sink = ListSink()
        capture = await ground.capture(
            [ground.route("fc")],
            sinks=[sink],
            queue_frames=2,
            consumer=getattr(engine, consumer),
        )
        [(_, addr)] = capture.addresses
        assert addr is not None
        blast(addr, [fc.codec_for("fc").encode_telemetry("MOCK_HK").data] * 50)
        await until(lambda: len(sink.frames) == 50)
        await asyncio.sleep(0.2)
        lag = [n for n in notices if n.code is Code.ENG_LAG]
        if level is None:
            assert lag == []  # the recorder's sink saw every frame: nothing was lost
        else:
            assert len(lag) == 1 and lag[0].level is Level[level] and word in lag[0].summary
        await capture.close()

    run_ground_and_fc(config, tmp_path, scenario)


def test_a_gap_is_only_called_marked_when_a_sink_would_write_it(tmp_path: Path) -> None:
    import asyncio
    from types import SimpleNamespace

    from ironbird.engine import Engine
    from ironbird.recording.writer import Recorder
    from recutil import FAST, RX, TX
    from replayutil import blob_specs, meta_data

    recorder = Recorder.open(
        tmp_path / "g.ibrec", meta_data(), blobs=blob_specs(), streams=(RX, TX), options=FAST
    )

    class Plain:
        def gap(self, *args: Any) -> None: ...

    class NoGaps: ...

    def taken(*sinks: object) -> bool:
        return Engine._gap_taken(SimpleNamespace(_sinks=list(sinks)), RX)  # type: ignore[arg-type]

    assert taken(recorder) and taken(Plain()) and not taken(NoGaps()) and not taken()
    recorder._stopped = True  # as after IB-REC-002: the recorder takes no more records
    assert not taken(recorder)  # so it drops the gap
    recorder._stopped = False
    asyncio.run(recorder.close())


# -- a duration suggestion never guesses between readings --


def test_an_ambiguous_duration_unit_gets_no_guess_and_lists_the_unambiguous_forms() -> None:
    from ironbird.units import ambiguous_duration, parse_duration, suggest_duration

    for text in ("6M", "2 months", "3mo"):
        assert suggest_duration(text) is None, text
        advice = ambiguous_duration(text)
        assert advice is not None and "min" in advice and "d'" in advice, text
        assert "ambiguous" in advice
    # Lower-case m is the usual short form of minutes, and says so; the rest are unchanged.
    for text, expected in (
        ("10m", "10min"),
        ("30 days", "30d"),
        ("2 hours", "2h"),
        ("5 SEC", "5s"),
    ):
        assert suggest_duration(text) == expected, text
        assert ambiguous_duration(text) is None
    assert parse_duration("6M") is None  # and the grammar itself never took it


def test_a_config_duration_written_with_a_capital_m_is_not_told_to_mean_minutes(
    tmp_path: Path, env: dict[str, str]
) -> None:
    edited = config_with(tmp_path, "[nodes.fc]", '[retention]\nmax_age = "6M"\n\n[nodes.fc]', env)
    done = run_ib(tmp_path, ["nodes"], dict(env, IRONBIRD_CONFIG=str(edited)))
    assert done.returncode == 2, done.stderr
    assert "retention.max_age" in done.stderr and "ambiguous" in done.stderr
    assert "'6d'" in done.stderr  # the choice is the user's, with both forms listed
