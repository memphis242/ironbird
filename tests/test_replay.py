"""Replay over files, in process: the frames, the ranges, chunk selection, pacing, split sets,
damage, definitions and following. (The command line and the corpus are in test_replay_cli.py
and test_replay_malformed.py.)"""

from __future__ import annotations

import fcntl
import os
import threading
import time
from pathlib import Path

import pytest

from ironbird import render
from ironbird.errors import Code, IronbirdError
from ironbird.frame import Frame, FrameFlag
from ironbird.recording import format as fmt
from ironbird.recording.format import RecordType
from ironbird.recording.player import Ending, OutputFormat
from ironbird.recording.reader import RecordingReader
from ironbird.recording.replay import (
    Follower,
    Pacer,
    Replay,
    TimeSpec,
    check_speed,
    parse_time_spec,
)
from ironbird.recording.writer import RecorderOptions, part_path
from recutil import RX, SERIAL, TX, frame
from replayutil import (
    STEP_NS,
    T0,
    codec_status_frames,
    crafted,
    good_blobs,
    hk,
    hk_frames,
    last_frame_end,
    mock_codec,
    records,
    replay_in_process,
    write_recording,
)

SMALL_CHUNKS = RecorderOptions(min_free_bytes=0, chunk_bytes=1500)


def frames_of(path: Path, **kwargs: object) -> list[Frame]:
    replaying = Replay([path], **kwargs)  # type: ignore[arg-type]
    try:
        return list(replaying.frames())
    finally:
        replaying.close()


# -- the same records a live listener prints --


def test_replay_prints_what_listen_prints_for_every_decoded_frame(tmp_path: Path) -> None:
    frames = hk_frames(30)
    write_recording(tmp_path / "r.ibrec", frames)
    codec = mock_codec()
    run = replay_in_process(tmp_path / "r.ibrec")
    expected = [
        render.line("fc", f.time_ns, codec.decode(f.stream.kind, f.data))  # type: ignore[arg-type]
        for f in frames
    ]
    assert run.lines == expected
    assert run.result.shown == 30 and run.result.malformed == 0 and run.result.unknown == 0
    as_json = replay_in_process(tmp_path / "r.ibrec", fmt_=OutputFormat.JSON)
    want = [
        render.record("fc", f.time_ns, codec.decode(f.stream.kind, f.data))  # type: ignore[arg-type]
        for f in frames
    ]
    assert records(as_json.lines) == want


def test_both_directions_are_replayed_with_their_own_kind(tmp_path: Path) -> None:
    codec = mock_codec()
    command = codec.encode_command("MOCK_NOOP", {}).data
    frames = [
        frame(1, stream=RX, data=hk(1), mid=0x890, time_ns=T0),
        frame(2, stream=TX, data=command, mid=codec.model.by_name["MOCK_NOOP"].mid, time_ns=T0 + 1),
    ]
    write_recording(tmp_path / "r.ibrec", frames)
    run = replay_in_process(tmp_path / "r.ibrec", fmt_=OutputFormat.JSON)
    assert [r["kind"] for r in records(run.lines)] == ["telemetry", "command"]


# -- what listen hides --


def damaged_frames() -> list[Frame]:
    good = hk(1)
    return [
        frame(1, stream=RX, data=good, mid=0x890, time_ns=T0),
        frame(2, stream=RX, data=good[:3], mid=0x890, time_ns=T0 + 1),  # shorter than a header
        frame(3, stream=RX, data=b"\xff" * 30, mid=0xFFF, time_ns=T0 + 2),  # unknown MID
        frame(4, stream=RX, data=good[:-2], mid=0x890, time_ns=T0 + 3),  # length lies
        frame(5, stream=RX, data=good, mid=0x890, time_ns=T0 + 4),
    ]


def test_frames_that_do_not_decode_are_shown_flagged_with_their_bytes(tmp_path: Path) -> None:
    write_recording(tmp_path / "r.ibrec", damaged_frames())
    run = replay_in_process(tmp_path / "r.ibrec")
    assert len(run.lines) == 5
    assert "MALFORMED" in run.lines[1] and "IB-DEC-001" in run.lines[1]
    assert run.lines[1].endswith("hex: 08 90 c0")
    assert "UNKNOWN" in run.lines[2] and "IB-DEC-006" in run.lines[2]
    assert "MALFORMED" in run.lines[3] and "IB-DEC-003" in run.lines[3] and "[" in run.lines[3]
    assert (run.result.malformed, run.result.unknown, run.result.shown) == (2, 1, 5)
    assert dict(run.result.rejected) == {"IB-DEC-001": 1, "IB-DEC-006": 1, "IB-DEC-003": 1}
    as_json = records(replay_in_process(tmp_path / "r.ibrec", fmt_=OutputFormat.JSON).lines)
    assert [r.get("status") for r in as_json] == [None, "malformed", "unknown", "malformed", None]
    assert as_json[1]["hex"] == "0890c0" and as_json[1]["error"]["code"] == "IB-DEC-001"


def test_a_filter_can_select_the_frames_listen_hides(tmp_path: Path) -> None:
    write_recording(tmp_path / "r.ibrec", damaged_frames())
    for where, count in (
        ("status == malformed", 2),
        ("status == unknown", 1),
        ("status != ok", 3),
        ("status == ok", 2),
    ):
        assert len(replay_in_process(tmp_path / "r.ibrec", where=where).lines) == count, where


def test_serial_garbage_and_stale_frames_are_flagged_and_never_decoded(tmp_path: Path) -> None:
    good = hk(1)
    frames = [
        frame(1, stream=SERIAL, data=good, mid=0x890, time_ns=T0),
        # valid-looking telemetry flagged as garbage is still garbage
        Frame(2, T0 + 1, SERIAL, "", None, FrameFlag.GARBAGE, len(good), good),
        frame(3, stream=SERIAL, data=good[:10], mid=0x890, time_ns=T0 + 2, flags=0x08),
    ]
    write_recording(tmp_path / "r.ibrec", frames, streams=(SERIAL,))
    run = replay_in_process(tmp_path / "r.ibrec")
    assert "GARBAGE" in run.lines[1] and "garbage" in run.lines[1].lower()
    assert " at byte " not in run.lines[1]
    assert "stale" in run.lines[2] and "MALFORMED" in run.lines[2]
    assert run.result.garbage == 1 and run.result.malformed == 1
    where = replay_in_process(tmp_path / "r.ibrec", where="garbage == true")
    assert len(where.lines) == 1 and "GARBAGE" in where.lines[0]


def test_a_frame_cut_at_the_packet_cap_is_shown_as_oversize(tmp_path: Path) -> None:
    data = hk(1) + bytes(MAX_PACKET_DATA)
    cut = data[: fmt_cap()]
    big = frame(1, stream=RX, data=cut, mid=0x890, time_ns=T0, flags=0x02, orig_len=len(data) + 50)
    write_recording(tmp_path / "r.ibrec", [big])
    run = replay_in_process(tmp_path / "r.ibrec")
    assert "IB-DEC-002" in run.lines[0] and "truncated" in run.lines[0]
    assert f"(of {len(data) + 50})" in run.lines[0]


MAX_PACKET_DATA = 4000


def fmt_cap() -> int:
    from recutil import MAX_PACKET

    return MAX_PACKET


def test_hex_format_dumps_every_frame_without_definitions(tmp_path: Path) -> None:
    frames = damaged_frames()
    write_recording(tmp_path / "r.ibrec", frames, blobs=False)
    run = replay_in_process(tmp_path / "r.ibrec", fmt_=OutputFormat.HEX)
    assert len(run.lines) == 5 and all("hex: " in line for line in run.lines)
    assert run.lines[1].endswith("hex: 08 90 c0")
    assert run.result.shown == 5 and run.result.malformed == 0  # nothing was decoded
    filtered = replay_in_process(tmp_path / "r.ibrec", fmt_=OutputFormat.HEX, where="seq > 3")
    assert len(filtered.lines) == 2  # a frame-level filter needs no definitions
    with pytest.raises(IronbirdError) as caught:  # a name it cannot check says why it cannot
        replay_in_process(tmp_path / "r.ibrec", fmt_=OutputFormat.HEX, where="node == fc")
    assert caught.value.code is Code.REC_DEFS and "embeds no" in caught.value.summary


# -- time ranges and chunk selection --


def times(path: Path, **kwargs: object) -> list[int]:
    return [f.time_ns for f in frames_of(path, **kwargs)]


def test_time_range_is_half_open_and_offsets_count_from_the_first_frame(tmp_path: Path) -> None:
    write_recording(tmp_path / "r.ibrec", hk_frames(50))
    path = tmp_path / "r.ibrec"
    start, end = T0 + 10 * STEP_NS, T0 + 20 * STEP_NS
    got = times(path, start=TimeSpec(absolute_ns=start), end=TimeSpec(absolute_ns=end))
    assert got == [T0 + i * STEP_NS for i in range(10, 20)]
    by_offset = times(
        path, start=parse_time_spec("+100ms", "--from"), end=parse_time_spec("+200ms", "--to")
    )
    assert by_offset == got
    assert times(path, start=TimeSpec(offset_ns=10**12)) == []
    assert times(path, end=TimeSpec(offset_ns=0)) == []


@pytest.mark.parametrize(
    "text, ns",
    [
        ("+90s", 90 * 10**9),
        ("+1m30s", 90 * 10**9),
        ("+0.5s", 5 * 10**8),
        ("+250ms", 250 * 10**6),
        ("+1h", 3600 * 10**9),
        ("+1h1m1s1ms", (3661 * 1000 + 1) * 10**6),
    ],
)
def test_offsets_parse(text: str, ns: int) -> None:
    assert parse_time_spec(text, "--from") == TimeSpec(offset_ns=ns)


@pytest.mark.parametrize(
    "text",
    ["+", "+s", "+5", "+5x", "+-5s", "+1.s", "+1e3s", "5s", "+99999999999999s", "", "+ 5s", "+5s "],
)
def test_bad_offsets_are_usage_errors(text: str) -> None:
    with pytest.raises(IronbirdError) as caught:
        parse_time_spec(text, "--from")
    assert caught.value.code is Code.CLI_USAGE and caught.value.exit_code == 2


def test_absolute_times_need_a_zone_and_seconds() -> None:
    spec = parse_time_spec("2023-11-14T22:13:20.5Z", "--from")
    assert spec.absolute_ns == 1_700_000_000_500_000_000
    assert parse_time_spec("2023-11-14T23:13:20+01:00", "--to").absolute_ns == 1_700_000_000 * 10**9
    for bad in (
        "2023-11-14T22:13:20",
        "2023-11-14T22:13Z",
        "yesterday",
        "2023-13-40T00:00:00Z",
        "2023-11-14T22:13:20Z junk",
        "9" * 200,
    ):
        with pytest.raises(IronbirdError) as caught:
            parse_time_spec(bad, "--from")
        assert caught.value.code is Code.CLI_USAGE, bad


def test_speed_is_zero_or_a_positive_finite_multiple() -> None:
    assert check_speed(0) == 0 and check_speed(1.5) == 1.5
    for bad in (-1.0, float("nan"), float("inf"), 1e10):
        with pytest.raises(IronbirdError):
            check_speed(bad)


def test_chunks_outside_the_range_are_skipped_and_the_output_is_the_same(tmp_path: Path) -> None:
    frames = hk_frames(400)
    write_recording(tmp_path / "r.ibrec", frames, options=SMALL_CHUNKS)
    path = tmp_path / "r.ibrec"
    start, end = TimeSpec(absolute_ns=T0 + 150 * STEP_NS), TimeSpec(absolute_ns=T0 + 170 * STEP_NS)
    run = replay_in_process(path, start=start, end=end)
    stats = run.replay.stats  # type: ignore[union-attr]
    assert stats.chunks_skipped > 0
    assert stats.frames_read < 400 and stats.frames_in_range == 20
    # What a straight read, with the range applied to every frame, gives.
    with RecordingReader(path) as reader:
        straight = [
            f for f in reader.frames() if T0 + 150 * STEP_NS <= f.time_ns < T0 + 170 * STEP_NS
        ]
    codec = mock_codec()
    assert run.lines == [
        render.line("fc", f.time_ns, codec.decode(f.stream.kind, f.data))  # type: ignore[arg-type]
        for f in straight
    ]


def test_chunks_without_a_candidate_mid_are_skipped(tmp_path: Path) -> None:
    quiet = hk_frames(150, mid=0x890)
    status = codec_status_frames(150, start=151)
    write_recording(tmp_path / "r.ibrec", quiet + status, options=SMALL_CHUNKS)
    run = replay_in_process(tmp_path / "r.ibrec", where="msg == MOCK_STATUS")
    stats = run.replay.stats  # type: ignore[union-attr]
    assert run.result.shown == 150
    assert stats.chunks_skipped > 0 and stats.frames_read < 300
    unfiltered = replay_in_process(tmp_path / "r.ibrec", where="seq > 150 and msg == MOCK_STATUS")
    assert unfiltered.lines == run.lines


def test_skipping_chunks_changes_no_frame_the_range_selects(tmp_path: Path) -> None:
    """Every chunk boundary as the start of the range: the same frames as a straight read."""
    write_recording(tmp_path / "r.ibrec", hk_frames(120), options=SMALL_CHUNKS)
    path = tmp_path / "r.ibrec"
    with RecordingReader(path) as reader:
        everything = list(reader.frames())
    for first in range(0, 120, 7):
        got = frames_of(path, start=TimeSpec(absolute_ns=everything[first].time_ns))
        assert got == everything[first:], first


def test_a_lying_index_cannot_make_replay_read_a_frame_that_was_not_written(
    tmp_path: Path,
) -> None:
    write_recording(tmp_path / "r.ibrec", hk_frames(200), options=SMALL_CHUNKS)
    raw = bytearray((tmp_path / "r.ibrec").read_bytes())
    # Damage the trailer's index offset region: the index is a hint and is dropped.
    raw[-9] ^= 0xFF
    (tmp_path / "bad.ibrec").write_bytes(bytes(raw))
    run = replay_in_process(tmp_path / "bad.ibrec", start=TimeSpec(absolute_ns=T0 + 100 * STEP_NS))
    assert run.result.shown <= 100


# -- pacing --


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0
        self.slept: list[float] = []

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds


def paced(speed: float, stamps: list[int], step: float = 0.1) -> FakeClock:
    fake = FakeClock()
    pacer = Pacer(speed, clock=fake.clock, sleep=fake.sleep, slice_s=step)
    for stamp in stamps:
        assert pacer.wait(stamp, lambda: False)
    return fake


def test_speed_one_waits_the_recorded_time_and_speed_two_half_of_it() -> None:
    stamps = [T0 + i * 500_000_000 for i in range(5)]  # 2 s of recording
    assert sum(paced(1.0, stamps).slept) == pytest.approx(2.0, abs=1e-6)
    assert sum(paced(2.0, stamps).slept) == pytest.approx(1.0, abs=1e-6)
    assert paced(0.0, stamps).slept == []


def test_a_clock_step_backwards_counts_as_no_time_and_later_frames_keep_their_schedule() -> None:
    stamps = [T0, T0 + 10**9, T0 - 5 * 10**9, T0 + 2 * 10**9]
    fake = paced(1.0, stamps)
    assert sum(fake.slept) == pytest.approx(2.0, abs=1e-6)  # never waits for the negative step


def test_a_slow_consumer_does_not_stretch_the_schedule() -> None:
    fake = FakeClock()
    pacer = Pacer(1.0, clock=fake.clock, sleep=fake.sleep)
    pacer.wait(T0, lambda: False)
    fake.now += 5.0  # the consumer was busy for longer than the whole recording
    pacer.wait(T0 + 10**9, lambda: False)
    assert fake.slept == []  # already late: no wait
    pacer.wait(T0 + 8 * 10**9, lambda: False)
    assert sum(fake.slept) == pytest.approx(3.0, abs=1e-6)  # due at +8 s, now at +5 s


def test_pacing_ends_when_the_output_goes_away() -> None:
    fake = FakeClock()
    pacer = Pacer(1.0, clock=fake.clock, sleep=fake.sleep)
    pacer.wait(T0, lambda: False)
    assert pacer.wait(T0 + 3600 * 10**9, lambda: True) is False
    assert fake.slept == []


def test_a_paced_replay_through_play_waits_between_frames(tmp_path: Path) -> None:
    write_recording(tmp_path / "r.ibrec", hk_frames(11, step_ns=100_000_000))
    fake = FakeClock()
    run = replay_in_process(
        tmp_path / "r.ibrec", pacer=Pacer(1.0, clock=fake.clock, sleep=fake.sleep)
    )
    assert run.result.shown == 11 and sum(fake.slept) == pytest.approx(1.0, abs=1e-6)


# -- count --


def test_count_stops_the_replay_after_n_frames(tmp_path: Path) -> None:
    write_recording(tmp_path / "r.ibrec", hk_frames(30))
    run = replay_in_process(tmp_path / "r.ibrec", count=7)
    assert len(run.lines) == 7 and run.result.ending is Ending.COUNT


# -- split sets --


def split_set(tmp_path: Path, count: int = 60) -> tuple[Path, list[Frame]]:
    frames = hk_frames(count)
    options = RecorderOptions(min_free_bytes=0, split_bytes=2500, chunk_bytes=1000)
    base = tmp_path / "run.ibrec"
    recorder = write_recording(base, frames, options=options)
    assert len(recorder.parts) >= 3
    return base, frames


def test_part_one_plays_every_part_in_order(tmp_path: Path) -> None:
    base, frames = split_set(tmp_path)
    assert frames_of(base) == frames
    run = replay_in_process(base)
    assert run.result.shown == len(frames) and run.replay.stats.parts >= 3  # type: ignore[union-attr]
    assert not run.damage


def test_naming_the_parts_plays_them_as_given_and_a_later_part_plays_alone(
    tmp_path: Path,
) -> None:
    base, frames = split_set(tmp_path)
    second, third = part_path(base, 2), part_path(base, 3)
    replaying = Replay([second, third])
    try:
        played = list(replaying.frames())
    finally:
        replaying.close()
    first = frames.index(played[0])
    assert first > 0 and played == frames[first : first + len(played)]
    alone = frames_of(second)
    assert alone == played[: len(alone)]


def test_parts_out_of_order_or_of_another_session_are_a_usage_error(tmp_path: Path) -> None:
    base, _ = split_set(tmp_path)
    with pytest.raises(IronbirdError) as caught:
        Replay([base, part_path(base, 3), part_path(base, 2)])
    assert caught.value.code is Code.CLI_USAGE
    other = tmp_path / "other.ibrec"
    write_recording(other, hk_frames(3), meta_extra={"session": "999999999999"})
    with pytest.raises(IronbirdError) as caught:
        Replay([base, other])
    assert (
        caught.value.code is Code.CLI_USAGE and "another recording session" in caught.value.summary
    )


def test_a_missing_part_is_reported_and_the_rest_still_plays(tmp_path: Path) -> None:
    base, frames = split_set(tmp_path)
    kept = frames_of(base)
    part_path(base, 2).unlink()
    replaying = Replay([base])
    try:
        assert replaying.stats.missing_parts == [2]
        played = list(replaying.frames())
    finally:
        replaying.close()
    assert played == kept[: len(played)] and len(played) < len(frames)
    named = Replay([base, part_path(base, 3)])
    try:
        assert named.stats.missing_parts == [2]
    finally:
        named.close()


# -- damage --


def test_a_torn_tail_ends_the_replay_with_a_warning_after_everything_before_it(
    tmp_path: Path,
) -> None:
    frames = hk_frames(20)
    write_recording(tmp_path / "r.ibrec", frames)
    raw = (tmp_path / "r.ibrec").read_bytes()
    last_end = last_frame_end(tmp_path / "r.ibrec")
    (tmp_path / "torn.ibrec").write_bytes(raw[: last_end - 10])
    run = replay_in_process(tmp_path / "torn.ibrec")
    assert run.result.shown == 19
    assert [d.torn for d in run.damage] == [True]
    stats = run.replay.stats  # type: ignore[union-attr]
    assert stats.torn_tail and not stats.mid_file_damage


def flip_in_frame(path: Path, ordinal: int, out: Path) -> Path:
    with RecordingReader(path) as reader:
        items = [i for i in reader.items() if getattr(i, "rtype", 0) == RecordType.FRAME]
    raw = bytearray(path.read_bytes())
    raw[items[ordinal].offset + 30] ^= 0xFF
    out.write_bytes(bytes(raw))
    return out


def test_mid_file_damage_stops_the_replay_unless_skip_damage_resumes(tmp_path: Path) -> None:
    write_recording(tmp_path / "r.ibrec", hk_frames(30))
    bad = flip_in_frame(tmp_path / "r.ibrec", 10, tmp_path / "bad.ibrec")
    stopped = replay_in_process(bad)
    assert stopped.result.shown == 10 and stopped.result.ending is Ending.DAMAGE
    assert stopped.replay.stats.mid_file_damage  # type: ignore[union-attr]
    resumed = replay_in_process(bad, skip_damage=True)
    assert resumed.result.shown == 29 and resumed.result.ending is Ending.DONE
    assert resumed.replay.stats.mid_file_damage and resumed.replay.stats.damage == 1  # type: ignore[union-attr]


def test_a_recording_still_being_written_is_not_called_torn(tmp_path: Path) -> None:
    write_recording(tmp_path / "r.ibrec", hk_frames(8))
    raw = (tmp_path / "r.ibrec").read_bytes()
    live = tmp_path / "live.ibrec"
    live.write_bytes(raw[:-45])
    fd = os.open(live, os.O_RDONLY)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        run = replay_in_process(live)
    finally:
        os.close(fd)
    assert run.replay.stats.live_end and not run.damage  # type: ignore[union-attr]


# -- definitions --


def test_replay_decodes_with_the_embedded_definitions_not_the_files_on_disk(
    tmp_path: Path,
) -> None:
    write_recording(tmp_path / "r.ibrec", hk_frames(3))
    run = replay_in_process(tmp_path / "r.ibrec")
    assert run.result.shown == 3 and run.result.malformed == 0


def test_a_recording_without_definitions_needs_hex_or_the_configuration(tmp_path: Path) -> None:
    write_recording(tmp_path / "r.ibrec", hk_frames(3), blobs=False)
    with pytest.raises(IronbirdError) as caught:
        replay_in_process(tmp_path / "r.ibrec")
    assert caught.value.code is Code.REC_DEFS and caught.value.exit_code == 1
    assert "embed" in caught.value.summary and "--defs config" in caught.value.summary
    assert replay_in_process(tmp_path / "r.ibrec", fmt_=OutputFormat.HEX).result.shown == 3
    run = replay_in_process(tmp_path / "r.ibrec", codecs={"fc": mock_codec()})
    assert run.result.shown == 3


def test_a_filter_is_compiled_against_the_recordings_definitions(tmp_path: Path) -> None:
    write_recording(tmp_path / "r.ibrec", hk_frames(10))
    run = replay_in_process(tmp_path / "r.ibrec", where="cmd_counter >= 8")
    assert run.result.shown == 3
    with pytest.raises(IronbirdError) as caught:
        replay_in_process(tmp_path / "r.ibrec", where="no_such_field == 1")
    assert caught.value.code is Code.FLT_NAME
    with pytest.raises(IronbirdError) as caught:
        replay_in_process(tmp_path / "r.ibrec", where="link == nope")
    assert caught.value.code.value.startswith("IB-FLT-")
    assert len(replay_in_process(tmp_path / "r.ibrec", where='link == "ground-fc"').lines) == 10


# -- following --


def test_follow_reads_what_is_appended_and_ends_when_the_writer_closes(tmp_path: Path) -> None:
    builder = crafted(good_blobs(), frames=0)
    path = tmp_path / "live.ibrec"
    path.write_bytes(bytes(builder.out))
    writer_fd = os.open(path, os.O_WRONLY | os.O_APPEND)
    fcntl.flock(writer_fd, fcntl.LOCK_EX)
    seen: list[str] = []
    result: dict[str, object] = {}

    def run() -> None:
        try:
            result["run"] = replay_in_process(path, follow=True)
        except BaseException as exc:  # pragma: no cover - reported through result
            result["error"] = exc

    thread = threading.Thread(target=run)
    thread.start()
    try:
        for seq in range(1, 6):
            builder.frame(seq, data=hk(seq), stream=0, source=0, mid=0x890, time_ns=T0 + seq)
            record = bytes(builder.out[builder.offsets[-1] :])
            half = len(record) // 2
            os.write(writer_fd, record[:half])  # a partial record is pending, not torn
            time.sleep(0.15)
            os.write(writer_fd, record[half:])
            time.sleep(0.05)
        assert thread.is_alive(), "the replay ended while the writer was alive"
        trailer = fmt.encode_record(
            RecordType.TRAILER, fmt.encode_trailer(fmt.Trailer(0, 5, 5)), builder.seed
        )
        os.write(writer_fd, trailer)
    finally:
        os.close(writer_fd)  # the lock goes with the descriptor
    thread.join(20)
    assert not thread.is_alive() and "error" not in result, result
    run_result = result["run"]
    assert run_result.result.shown == 5 and not run_result.damage  # type: ignore[attr-defined]
    del seen


def test_follow_ends_with_a_torn_tail_when_the_writer_dies_mid_record(tmp_path: Path) -> None:
    builder = crafted(good_blobs(), frames=3)
    path = tmp_path / "dead.ibrec"
    raw = bytes(builder.out)
    path.write_bytes(raw[:-12])  # the last record is incomplete, and nobody holds the lock
    started = time.monotonic()
    run = replay_in_process(path, follow=True)
    assert time.monotonic() - started < 5
    assert run.result.shown == 2
    assert [d.torn for d in run.damage] == [True]
    assert run.replay.stats.torn_tail  # type: ignore[union-attr]


def test_follow_waits_for_a_writer_that_is_quiet_and_idle_ends_it(tmp_path: Path) -> None:
    builder = crafted(good_blobs(), frames=2)
    path = tmp_path / "quiet.ibrec"
    path.write_bytes(bytes(builder.out))
    fd = os.open(path, os.O_RDONLY)
    fcntl.flock(fd, fcntl.LOCK_EX)  # a live (quiet) writer
    try:
        started = time.monotonic()
        run = replay_in_process(path, follow=True, follow_idle_s=0.4)
        took = time.monotonic() - started
    finally:
        os.close(fd)
    assert 0.35 <= took < 5 and run.result.shown == 2


def test_follow_of_a_closed_recording_ends_at_its_trailer(tmp_path: Path) -> None:
    write_recording(tmp_path / "r.ibrec", hk_frames(5))
    started = time.monotonic()
    run = replay_in_process(tmp_path / "r.ibrec", follow=True)
    assert time.monotonic() - started < 5 and run.result.shown == 5 and not run.damage


def test_follower_reports_a_writer_by_its_lock(tmp_path: Path) -> None:
    path = tmp_path / "f.ibrec"
    path.write_bytes(b"x")
    fd = os.open(path, os.O_RDONLY)
    fcntl.flock(fd, fcntl.LOCK_EX)
    follower = Follower(path, None)
    try:
        assert follower.alive() is True and follower.lockable
        os.close(fd)
        fd = -1
        assert follower.alive() is False
    finally:
        if fd >= 0:
            os.close(fd)
        follower.close()
    assert Follower(tmp_path / "missing", None).lockable is False


def test_follow_cannot_run_where_locks_do_not_work_unless_idle_is_given(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_recording(tmp_path / "r.ibrec", hk_frames(2))
    monkeypatch.setattr(Follower, "alive", lambda self: None)
    with pytest.raises(IronbirdError) as caught:
        replay_in_process(tmp_path / "r.ibrec", follow=True)
    assert caught.value.code is Code.CLI_USAGE and "--follow-idle" in caught.value.summary
    run = replay_in_process(tmp_path / "r.ibrec", follow=True, follow_idle_s=0.2)
    assert run.result.shown == 2


def test_follow_continues_into_the_next_part_of_a_split_recording(tmp_path: Path) -> None:
    base, frames = split_set(tmp_path, 40)
    run = replay_in_process(base, follow=True)
    assert run.result.shown == len(frames)


# -- the output a replay writes to never drops a line --


def test_write_wait_waits_for_room_instead_of_dropping_and_stops_when_the_reader_leaves() -> None:
    from ironbird.cli.output import BoundedWriter

    read_end, write_end = os.pipe()
    stream = os.fdopen(write_end, "w")
    writer = BoundedWriter(stream, max_bytes=2000)
    lines = [f"line {i:05d} " + "x" * 90 for i in range(300)]  # ~30 KB: far more than the queue
    taken: list[bool] = []

    def produce() -> None:
        taken.extend(writer.write_wait(text) for text in lines)

    producer = threading.Thread(target=produce)
    producer.start()
    time.sleep(0.3)  # the pipe fills (64 KiB would take longer: the queue is what holds it)
    received = bytearray()
    deadline = time.monotonic() + 20
    while len(received.splitlines()) < len(lines) and time.monotonic() < deadline:
        received += os.read(read_end, 4096)
    producer.join(10)
    assert not producer.is_alive() and all(taken) and writer.dropped_total == 0
    assert received.decode().splitlines() == lines
    os.close(read_end)  # the reader goes away
    deadline = time.monotonic() + 10
    while writer.write_wait("more") and time.monotonic() < deadline:
        time.sleep(0.01)
    assert writer.reader_gone and writer.write_wait("more") is False
    writer.close()
    stream.close()


def test_write_wait_accepts_a_line_longer_than_the_queue_into_an_empty_one() -> None:
    from ironbird.cli.output import BoundedWriter

    read_end, write_end = os.pipe()
    writer = BoundedWriter(os.fdopen(write_end, "w"), max_bytes=100)
    assert writer.write_wait("y" * 5000)
    writer.begin_close()
    data = b""
    while len(data) < 5001:
        data += os.read(read_end, 65536)
    os.close(read_end)
    assert data == b"y" * 5000 + b"\n"
    writer.close()
