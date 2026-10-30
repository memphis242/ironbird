"""Replay as an untrusted-input boundary: whatever a recording holds, ``ib replay`` ends in a coded
error or a normal end, never a traceback, an assert or a hang.

Two things are untrusted here beyond the reader's own records (see test_recording_malformed.py):
the *definitions* a recording embeds, which become codecs through the strict loaders, and the
*frames* it holds, which are decoded with them. The checked-in corpus ``rpl-*.ibrec`` holds the
minimised bad cases; ``replay_corpus.py`` builds them.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from conftest import CORPUS
from ironbird.errors import Code, IronbirdError
from ironbird.frame import Frame
from ironbird.recording import format as fmt
from ironbird.recording.defs import Head, embedded_codecs
from ironbird.recording.format import BlobKind
from ironbird.recording.player import OutputFormat
from ironbird.recording.replay import TimeSpec
from ironbird.recording.writer import RecorderOptions
from recutil import RX, SERIAL, flip_bit, frame, run_ib
from replay_corpus import cases
from replayutil import (
    STEP_NS,
    T0,
    child_env,
    definition_bytes,
    hk,
    hk_frames,
    meta_data,
    replay_in_process,
    write_recording,
)

FUZZ = settings(
    max_examples=120, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture]
)
ALLOWED = {
    Code.REC_FORMAT,
    Code.REC_VERSION,
    Code.REC_DAMAGED,
    Code.REC_TORN,
    Code.REC_DEFS,
    Code.CLI_USAGE,
}
NAMES = sorted(cases())


# -- the checked-in corpus --


def test_the_checked_in_corpus_is_what_the_builder_builds() -> None:
    built = {name: data for name, (data, _) in cases().items()}
    on_disk = {p.name: p.read_bytes() for p in CORPUS.glob("rpl-*.ibrec")}
    assert on_disk == built


@pytest.mark.parametrize("name", NAMES, ids=lambda n: n.removeprefix("rpl-").removesuffix(".ibrec"))
def test_corpus_files_end_in_a_coded_error_and_hex_still_shows_the_frames(
    name: str, tmp_path: Path
) -> None:
    _, expect = cases()[name]
    path = CORPUS / name
    env = child_env(tmp_path)
    done = run_ib(tmp_path, ["replay", str(path)], env)
    assert "Traceback" not in done.stderr and "IB-INT-001" not in done.stderr, done.stderr
    assert done.returncode == expect.exit_code, done.stderr
    if expect.code is not None:
        assert f"error {expect.code.value}" in done.stderr, done.stderr
        # The reason is the loader's own, not the digest check or another stage standing in front.
        assert expect.reason in done.stderr, done.stderr
        assert done.stdout == "" or expect.exit_code == 5
    hexed = run_ib(tmp_path, ["replay", str(path), "--format", "hex"], env)
    assert "Traceback" not in hexed.stderr and "IB-INT-001" not in hexed.stderr, hexed.stderr
    assert hexed.returncode == expect.hex_exit, hexed.stderr


# -- definitions: arbitrary content never gets past IB-REC-009 --


def head_with(ccdd: bytes, command: bytes, telemetry: bytes, adapter: str = "json") -> Head:
    blobs = {
        ("fc", BlobKind.CCDD): fmt.Blob(
            BlobKind.CCDD, "fc", adapter, hashlib.sha256(ccdd).digest(), ccdd
        ),
        ("fc", BlobKind.COMMAND_PROFILE): fmt.Blob(
            BlobKind.COMMAND_PROFILE, "fc", "toml", hashlib.sha256(command).digest(), command
        ),
        ("fc", BlobKind.TELEMETRY_PROFILE): fmt.Blob(
            BlobKind.TELEMETRY_PROFILE, "fc", "toml", hashlib.sha256(telemetry).digest(), telemetry
        ),
    }
    return Head({}, blobs)


def mutate(data: bytes, edits: list[tuple[int, int, int]]) -> bytes:
    """Byte flips (kind 0), deletions (1) and insertions (2) at positions of ``data``."""
    out = bytearray(data)
    for position, kind, value in edits:
        if not out:
            break
        at = position % len(out)
        if kind == 0:
            out[at] ^= (value % 255) + 1
        elif kind == 1:
            del out[at : at + 1 + value % 16]
        else:
            out[at:at] = bytes([value % 256]) * (1 + value % 8)
    return bytes(out)


EDITS = st.lists(
    st.tuples(st.integers(0, 1_000_000), st.integers(0, 2), st.integers(0, 1_000_000)),
    min_size=1,
    max_size=6,
)


@FUZZ
@given(which=st.integers(0, 2), edits=EDITS)
def test_mutated_embedded_definitions_load_or_end_in_ib_rec_009(
    which: int, edits: list[tuple[int, int, int]]
) -> None:
    parts = list(definition_bytes())
    parts[which] = mutate(parts[which], edits)
    head = head_with(parts[0], parts[1], parts[2])
    try:
        codecs = embedded_codecs(Path("r.ibrec"), head, ["fc"], {}, 2048)
    except IronbirdError as exc:
        assert exc.code is Code.REC_DEFS and exc.exit_code == 1
        assert "\n" not in exc.summary
        return
    assert set(codecs) == {"fc"}


@FUZZ
@given(
    ccdd=st.binary(max_size=400),
    command=st.binary(max_size=400),
    telemetry=st.binary(max_size=400),
    adapter=st.sampled_from(["json", "xml", "", "JSON", "toml"]),
)
def test_arbitrary_embedded_bytes_never_escape_as_anything_but_ib_rec_009(
    ccdd: bytes, command: bytes, telemetry: bytes, adapter: str
) -> None:
    head = head_with(ccdd, command, telemetry, adapter)
    with pytest.raises(IronbirdError) as caught:
        embedded_codecs(Path("r.ibrec"), head, ["fc"], {}, 2048)
    assert caught.value.code is Code.REC_DEFS


@FUZZ
@given(cap=st.integers(1, 70_000))
def test_the_recordings_packet_cap_is_enforced_on_its_definitions(cap: int) -> None:
    head = head_with(*definition_bytes())
    try:
        embedded_codecs(Path("r.ibrec"), head, ["fc"], {}, cap)
    except IronbirdError as exc:
        assert exc.code is Code.REC_DEFS


# -- whole files --


def mixed_recording(path: Path) -> list[Frame]:
    good = hk(1)
    frames: list[Frame] = []
    for seq in range(1, 31):
        match seq % 5:
            case 0:
                data, mid = good[:5], 0x890
            case 1:
                data, mid = b"\xaa" * 40, 0xFFF
            case _:
                data, mid = hk(seq), 0x890
        frames.append(frame(seq, stream=RX, data=data, mid=mid, time_ns=T0 + seq * STEP_NS))
    write_recording(
        path, frames, options=RecorderOptions(min_free_bytes=0, chunk_bytes=1200), streams=(RX,)
    )
    return frames


def run_all_ways(path: Path, count: int, tmp: Path) -> None:
    """Every output format, with and without ``skip_damage`` and a filter, over ``path``."""
    del tmp
    for fmt_ in OutputFormat:
        for skip in (False, True):
            for where in (None, "status != ok", "cmd_counter > 3 or msg == MOCK_HK"):
                try:
                    run = replay_in_process(path, fmt_=fmt_, skip_damage=skip, where=where)
                except IronbirdError as exc:
                    assert exc.code in ALLOWED or exc.code.value.startswith("IB-FLT-"), exc
                    continue
                assert run.result.shown <= count
                assert all(isinstance(line, str) and "\n" not in line for line in run.lines)


@FUZZ
@given(
    flips=st.lists(st.integers(0, 10_000_000), min_size=1, max_size=5),
    cut=st.one_of(st.none(), st.integers(0, 100_000)),
    head_bias=st.booleans(),
)
def test_arbitrary_corruption_of_a_recording_with_definitions_is_handled(
    tmp_path_factory: pytest.TempPathFactory,
    flips: list[int],
    cut: int | None,
    head_bias: bool,
) -> None:
    tmp = tmp_path_factory.mktemp("rpl")
    source = tmp / "a.ibrec"
    frames = mixed_recording(source)
    raw = source.read_bytes()
    span = min(len(raw), 24_000) if head_bias else len(raw)  # the head holds the definitions
    for bit in flips:
        raw = flip_bit(raw, bit % (span * 8))
    if cut is not None:
        raw = raw[: cut % (len(raw) + 1)]
    bad = tmp / "bad.ibrec"
    bad.write_bytes(raw)
    run_all_ways(bad, len(frames), tmp)


def test_every_third_bit_flip_of_the_head_is_handled(tmp_path: Path) -> None:
    source = tmp_path / "a.ibrec"
    frames = mixed_recording(source)
    raw = source.read_bytes()
    bad = tmp_path / "bad.ibrec"
    for bit in range(0, 9000 * 8, 97):
        bad.write_bytes(flip_bit(raw, bit))
        try:
            run = replay_in_process(bad, skip_damage=True)
        except IronbirdError as exc:
            assert exc.code in ALLOWED, exc
            continue
        assert run.result.shown <= len(frames)


def test_truncation_at_every_offset_of_the_head_is_handled(tmp_path: Path) -> None:
    source = tmp_path / "a.ibrec"
    frames = mixed_recording(source)
    raw = source.read_bytes()
    cut = tmp_path / "cut.ibrec"
    previous = 0
    for length in range(0, 9000, 37):
        cut.write_bytes(raw[:length])
        try:
            run = replay_in_process(cut)
        except IronbirdError as exc:
            assert exc.code in ALLOWED, exc
            continue
        assert previous <= run.result.shown <= len(frames) or length < 1
        previous = max(previous, run.result.shown)


@FUZZ
@given(
    data=st.lists(st.binary(max_size=80), min_size=1, max_size=25),
    mids=st.lists(st.one_of(st.none(), st.integers(0, 0xFFFFF)), min_size=25, max_size=25),
    serial=st.booleans(),
)
def test_any_payload_in_any_frame_is_shown_exactly_once(
    tmp_path_factory: pytest.TempPathFactory,
    data: list[bytes],
    mids: list[int | None],
    serial: bool,
) -> None:
    tmp = tmp_path_factory.mktemp("payload")
    stream = SERIAL if serial else RX
    frames = [
        frame(i + 1, stream=stream, data=payload, mid=mids[i], time_ns=T0 + i)
        for i, payload in enumerate(data)
    ]
    write_recording(tmp / "r.ibrec", frames, streams=(stream,))
    for fmt_ in OutputFormat:
        run = replay_in_process(tmp / "r.ibrec", fmt_=fmt_)
        assert run.result.shown == len(frames) == len(run.lines)
    shown = replay_in_process(tmp / "r.ibrec", where="status != ok")
    ok = replay_in_process(tmp / "r.ibrec", where="status == ok")
    assert len(shown.lines) + len(ok.lines) == len(frames)


# -- chunk selection never changes what a straight read selects --


@FUZZ
@given(
    low=st.integers(-5, 130),
    span=st.integers(1, 140),
    only_mid=st.booleans(),
    clock_steps=st.lists(st.integers(-40, 40), max_size=4),
)
def test_selection_by_the_index_equals_selection_frame_by_frame(
    tmp_path_factory: pytest.TempPathFactory,
    low: int,
    span: int,
    only_mid: bool,
    clock_steps: list[int],
) -> None:
    tmp = tmp_path_factory.mktemp("select")
    frames = hk_frames(120)
    for at, delta in enumerate(clock_steps):  # clock steps put frames out of time order
        index = (at + 1) * 25
        frames[index:] = [
            frame(
                f.seq,
                stream=RX,
                data=f.data,
                mid=f.mid,
                time_ns=f.time_ns + delta * STEP_NS,
            )
            for f in frames[index:]
        ]
    write_recording(
        tmp / "r.ibrec", frames, options=RecorderOptions(min_free_bytes=0, chunk_bytes=900)
    )
    start, end = T0 + low * STEP_NS, T0 + (low + span) * STEP_NS
    where = "msg == MOCK_STATUS" if only_mid else None
    indexed = replay_in_process(
        tmp / "r.ibrec",
        where=where,
        start=TimeSpec(absolute_ns=start),
        end=TimeSpec(absolute_ns=end),
    )
    straight = replay_in_process(
        tmp / "r.ibrec",
        where=(f"({where}) and " if where else "")
        + f"time >= {_iso(start)} and time < {_iso(end)}",
    )
    assert indexed.lines == straight.lines


def _iso(time_ns: int) -> str:
    import time as _time

    seconds, ns = divmod(time_ns, 10**9)
    return _time.strftime("%Y-%m-%dT%H:%M:%S", _time.gmtime(seconds)) + f".{ns:09d}Z"


def test_a_recording_with_fewer_bytes_than_a_head_does_not_hang(tmp_path: Path) -> None:
    for blob in (b"", fmt.MAGIC, fmt.MAGIC + bytes(40)):
        (tmp_path / "x.ibrec").write_bytes(blob)
        with pytest.raises(IronbirdError) as caught:
            replay_in_process(tmp_path / "x.ibrec")
        assert caught.value.code in ALLOWED


def test_meta_with_a_lying_node_table_is_ignored_when_it_is_not_text(tmp_path: Path) -> None:
    write_recording(
        tmp_path / "r.ibrec",
        hk_frames(3),
        meta_extra={"nodes": {"fc": {"ccdd_sha256": 7, "command_profile_sha256": ["x"]}}},
    )
    assert replay_in_process(tmp_path / "r.ibrec").result.shown == 3
    assert meta_data()["nodes"]["fc"]["ccdd_sha256"]  # the helper's own table is a real digest
