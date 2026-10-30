"""The recording reader as a decode boundary: no recording, however damaged or forged, may raise
anything but a coded ``IronbirdError`` at open, yield a frame that was not written, trip a
``Frame`` assertion, or hang.

The corpus ``tests/corpus/malformed/rec-*.ibrec`` holds the minimised bad files; ``rec_corpus.py``
builds them, and a test here proves the checked-in bytes are what it builds.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from conftest import CORPUS, EXAMPLES
from ironbird.ccdd.loader import load_ccdd
from ironbird.ccdd.model import MessageKind
from ironbird.codec import Codec
from ironbird.errors import Code, IronbirdError
from ironbird.frame import FrameFlag
from ironbird.header import load_profile
from ironbird.recording import format as fmt
from ironbird.recording import inspect as rec_inspect
from ironbird.recording.format import RecordType
from ironbird.recording.reader import Damage, Item, RecordingReader
from rec_corpus import cases
from recutil import SERIAL, Builder, flip_bit, sample

FUZZ = settings(
    max_examples=150, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture]
)
OK_CODES = {Code.REC_FORMAT, Code.REC_VERSION, Code.REC_DAMAGED, Code.REC_TORN}


def frames_of(path: Path, *, skip: bool = True) -> tuple[list[Any], list[Damage]]:
    """Every frame the reader yields plus its damage, or ([], []) for a file that cannot open."""
    try:
        with RecordingReader(path, skip_damage=skip) as reader:
            return list(reader.frames()), list(reader.damage)
    except IronbirdError as exc:
        assert exc.code in OK_CODES | {Code.REC_VERSION}, exc
        return [], []


def exercise_everything(path: Path, tmp: Path) -> None:
    """Run every consumer over ``path``: only coded errors may come out."""
    for skip in (False, True):
        frames_of(path, skip=skip)
    for func in (rec_inspect.scan, rec_inspect.info):
        try:
            func(path)
        except IronbirdError as exc:
            assert exc.code in OK_CODES, exc
    try:
        rec_inspect.repair(path, tmp / "repaired.ibrec", force=True)
    except IronbirdError as exc:
        assert exc.code in OK_CODES | {Code.REC_CREATE}, exc


# -- truncation at every offset --


def test_truncation_at_every_offset_yields_a_prefix(tmp_path: Path) -> None:
    source = tmp_path / "a.ibrec"
    written = sample(source, 12)
    raw = source.read_bytes()
    boundaries = {0, fmt.PREAMBLE_SIZE}
    with RecordingReader(source) as reader:
        boundaries.add(reader.body_start)
        boundaries |= {i.end for i in reader.items() if isinstance(i, Item)}
    cut = tmp_path / "cut.ibrec"
    previous = 0
    for length in range(len(raw)):
        cut.write_bytes(raw[:length])
        try:
            reader = RecordingReader(cut)
        except IronbirdError as exc:
            assert exc.code in OK_CODES
            continue
        with reader:
            got = list(reader.frames())
            assert got == written[: len(got)]  # a prefix, in order
            assert len(got) >= previous  # more bytes never give fewer frames
            previous = len(got)
            if length in boundaries:
                assert not reader.damage, f"a cut at a record boundary ({length}) is not damage"
            else:
                assert [d.torn for d in reader.damage] == [True]
                assert reader.damage[0].offset in boundaries  # reported at a record boundary
                assert reader.damage[0].to_error().code is Code.REC_TORN


# -- bit flips and arbitrary corruption --


def test_every_single_bit_flip_never_yields_an_unwritten_frame(tmp_path: Path) -> None:
    source = tmp_path / "a.ibrec"
    written = sample(source, 5)
    raw = source.read_bytes()
    allowed = set(written)
    bad = tmp_path / "bad.ibrec"
    for bit in range(0, len(raw) * 8, 3):  # every third bit keeps the run short, all bytes hit
        bad.write_bytes(flip_bit(raw, bit))
        for skip in (False, True):
            got, _ = frames_of(bad, skip=skip)
            assert set(got) <= allowed, f"bit {bit} produced a frame that was not written"
            seqs = [f.seq for f in got]
            assert seqs == sorted(set(seqs))


@FUZZ
@given(
    flips=st.lists(st.integers(min_value=0, max_value=10_000_000), min_size=1, max_size=6),
    cut=st.one_of(st.none(), st.integers(min_value=0, max_value=10_000)),
)
def test_arbitrary_corruption_is_handled_by_every_consumer(
    tmp_path_factory: pytest.TempPathFactory, flips: list[int], cut: int | None
) -> None:
    tmp = tmp_path_factory.mktemp("flip")
    source = tmp / "a.ibrec"
    written = sample(source, 25)
    raw = source.read_bytes()
    for bit in flips:
        raw = flip_bit(raw, bit % (len(raw) * 8))
    if cut is not None:
        raw = raw[: cut % (len(raw) + 1)]
    bad = tmp / "bad.ibrec"
    bad.write_bytes(raw)
    got, _ = frames_of(bad)
    assert set(got) <= set(written)
    exercise_everything(bad, tmp)


@FUZZ
@given(tail=st.binary(max_size=3000))
def test_arbitrary_bytes_after_valid_records(
    tmp_path_factory: pytest.TempPathFactory, tail: bytes
) -> None:
    tmp = tmp_path_factory.mktemp("tail")
    builder = Builder().standard(4)
    path = tmp / "a.ibrec"
    path.write_bytes(bytes(builder.out) + tail)
    got, _ = frames_of(path)
    assert [f.seq for f in got][:4] == [1, 2, 3, 4]
    exercise_everything(path, tmp)


@FUZZ
@given(blob=st.binary(max_size=4000))
def test_arbitrary_bytes_after_the_preamble(
    tmp_path_factory: pytest.TempPathFactory, blob: bytes
) -> None:
    tmp = tmp_path_factory.mktemp("blob")
    path = tmp / "a.ibrec"
    path.write_bytes(fmt.encode_preamble(0x1234) + blob)
    exercise_everything(path, tmp)


@FUZZ
@given(
    rtype=st.integers(0, 255),
    body=st.binary(max_size=400),
    claim=st.one_of(st.none(), st.integers(0, 2**32 - 1)),
)
def test_arbitrary_records_with_valid_crcs_never_break_the_reader(
    tmp_path_factory: pytest.TempPathFactory, rtype: int, body: bytes, claim: int | None
) -> None:
    """A record whose CRC is right (a hostile file knows its own nonce) but whose body is
    arbitrary: every type's decoder must reject it with damage, never an exception."""
    tmp = tmp_path_factory.mktemp("rec")
    b = Builder().standard(2)
    b.raw(rtype, body, body_len=claim)
    b.frame(3, stream=0, source=0)
    b.frame(4, stream=0, source=0)
    path = tmp / "a.ibrec"
    b.write(path)
    got, _ = frames_of(path)
    assert {f.seq for f in got} >= {1, 2} or not got
    exercise_everything(path, tmp)


# -- the rules, one lie at a time --


def _file(tmp_path: Path, build: Builder) -> Path:
    return build.write(tmp_path / "a.ibrec")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"reserved": 1},
        {"flags": 0x31},
        {"flags": 0x81},
        {"stream": 9},
        {"source": 3},
        {"flags": 0, "mid": 5},
        {"flags": 3, "orig_len": 20},
        {"flags": 3, "orig_len": 21},  # truncated, but not above the cap
        {"flags": 3, "orig_len": 2048},  # at the cap, not above it
        {"flags": 1, "orig_len": 21},
        {"flags": 1, "orig_len": 19},
        {"flags": 1, "orig_len": 0xFFFFFFFF},
        {"flags": 0x05, "mid": 0},  # GARBAGE on a udp stream
        {"flags": 0x09},  # STALE on a udp stream
        {"stream": 2, "flags": 0x0C, "mid": 0},  # GARBAGE with STALE
        {"stream": 2, "flags": 0x05},  # GARBAGE with HAS_MID
        {"stream": 2, "flags": 0x06, "mid": 0, "orig_len": 5000},  # GARBAGE with TRUNCATED
    ],
)
def test_a_frame_that_breaks_a_rule_is_damage_never_a_frame(
    tmp_path: Path, kwargs: dict[str, Any]
) -> None:
    b = Builder().standard(2)
    b.stream(SERIAL)
    b.frame(3, **kwargs)
    b.frame(4)
    b.frame(5)
    path = _file(tmp_path, b)
    got, damage = frames_of(path, skip=False)
    assert [f.seq for f in got] == [1, 2] and len(damage) == 1 and not damage[0].torn
    lenient, _ = frames_of(path, skip=True)
    assert [f.seq for f in lenient] == [1, 2, 4, 5]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"flags": 3, "orig_len": 2049},  # the smallest valid truncation
        {"flags": 3, "orig_len": 0xFFFFFFFF},
        {"flags": 1, "mid": 0xFFFFFFFF},
        {"flags": 1, "mid": 0},
        {"flags": 0, "mid": 0},
        {"source": 0xFFFF},
        {"stream": 2, "flags": 0x04, "mid": 0},  # GARBAGE on serial
        {"stream": 2, "flags": 0x08, "mid": 0},  # STALE on serial
        {"data": b""},
        {"data": bytes(2048), "flags": 1},
    ],
)
def test_frames_that_pass_every_rule_are_valid_frames(
    tmp_path: Path, kwargs: dict[str, Any]
) -> None:
    b = Builder().standard(2)
    b.stream(SERIAL)
    b.frame(3, **kwargs)
    got, damage = frames_of(_file(tmp_path, b))
    assert [f.seq for f in got] == [1, 2, 3] and not damage
    last = got[-1]
    assert (FrameFlag.TRUNCATED in last.flags) == (last.orig_len > len(last.data))
    if FrameFlag.TRUNCATED in last.flags:  # the codec's oversize assertion holds for it
        codec = Codec(
            load_ccdd(EXAMPLES / "ccdd" / "mock.json"),
            load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml"),
            load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml"),
            max_packet_bytes=2048,
        )
        assert codec.oversize_error(MessageKind.TELEMETRY, last.data, last.orig_len)


@pytest.mark.parametrize("claimed", [0, 31, 33 + 2048, 0xFFFFFFFF, 1 << 31])
def test_a_lying_body_length_is_damage_without_allocating(tmp_path: Path, claimed: int) -> None:
    b = Builder().standard(2)
    b.raw(RecordType.FRAME, b.frame_body(3), body_len=claimed)
    b.frame(4)
    b.frame(5)
    got, damage = frames_of(_file(tmp_path, b), skip=False)
    assert [f.seq for f in got] == [1, 2] and len(damage) == 1


def test_a_body_length_that_is_too_small_for_the_data_is_damage_both_ways(tmp_path: Path) -> None:
    for delta in (-5, +9):
        b = Builder().standard(2)
        body = b.frame_body(3, data=b"q" * 30)
        b.raw(RecordType.FRAME, body, body_len=len(body) + delta)
        b.frame(4)
        b.frame(5)
        got, damage = frames_of(_file(tmp_path, b), skip=False)
        assert [f.seq for f in got] == [1, 2] and len(damage) == 1


def test_a_frame_on_a_stream_defined_only_later_is_damage(tmp_path: Path) -> None:
    b = Builder()
    b.source()
    b.frame(1, stream=0)
    b.stream(RX_STREAM)
    b.frame(2, stream=0)
    b.frame(3, stream=0)
    got, damage = frames_of(_file(tmp_path, b), skip=True)
    assert [f.seq for f in got] == [2, 3] and len(damage) == 1


from recutil import RX as RX_STREAM  # noqa: E402


def test_oversize_meta_and_unusable_meta_cannot_open(tmp_path: Path) -> None:
    for name in (
        "rec-meta-oversize",
        "rec-meta-not-json",
        "rec-meta-array",
        "rec-meta-deep-nesting",
        "rec-meta-huge-int",
        "rec-meta-negative-cap",
        "rec-meta-cap-too-big",
    ):
        with pytest.raises(IronbirdError) as caught:
            RecordingReader(CORPUS / f"{name}.ibrec")
        assert caught.value.code is Code.REC_DAMAGED, name


def test_a_bad_blob_digest_is_damage(tmp_path: Path) -> None:
    got, damage = frames_of(CORPUS / "rec-blob-bad-digest.ibrec", skip=False)
    assert [f.seq for f in got] == [1, 2] and "SHA-256" in damage[0].reason


def test_conflicting_stream_redefinition_is_damage() -> None:
    got, damage = frames_of(CORPUS / "rec-stream-redefined.ibrec", skip=False)
    assert [f.seq for f in got] == [1, 2] and "twice" in damage[0].reason


# -- forged records --


def test_records_forged_inside_frame_data_never_end_damage_or_get_read(tmp_path: Path) -> None:
    """Frame 2's data holds complete records valid under another nonce. With damage in front, the
    scan must pass over them (their CRCs fail under this file's nonce) and resume at frame 2."""
    path = CORPUS / "rec-forged-in-frame-data.ibrec"
    with RecordingReader(path, skip_damage=True) as reader:
        items = list(reader.items())
        frames = [i.value for i in items if isinstance(i, Item) and i.rtype == RecordType.FRAME]
        damage = [i for i in items if isinstance(i, Damage)]
    assert [f.seq for f in frames] == [2, 3, 4]
    assert len(damage) == 1 and not damage[0].torn
    forged = frames[0].data
    assert len(forged) > 100  # the forgery is there, as data
    # nothing in the yielded stream is a record parsed from the forgery
    assert all(
        i.rtype in (RecordType.FRAME, RecordType.STREAM, RecordType.SOURCE)
        for i in items
        if isinstance(i, Item)
    )


def test_a_forged_record_cannot_carry_a_valid_crc_for_a_nonce_chosen_later(tmp_path: Path) -> None:
    a = Builder(0x1111)
    a.standard(0)
    forged = fmt.encode_record(RecordType.FRAME, a.frame_body(2, stream=0), fmt.seed_of(0x1111))
    b = Builder(0x2222)
    b.standard(0)
    b.frame(1)
    b.frame(2, data=b"\x00" * 4 + forged * 3)
    path = b.write(tmp_path / "a.ibrec")
    with RecordingReader(path) as reader:
        got = list(reader.frames())
    assert [f.seq for f in got] == [1, 2] and got[1].data.endswith(forged)


# -- bounded work --


@pytest.mark.parametrize(
    "name", ["rec-dense-fake-envelopes", "rec-fake-chains-wrong-nonce", "rec-zero-filled-tail"]
)
def test_resync_over_hostile_candidates_is_bounded(name: str) -> None:
    start = time.perf_counter()
    with RecordingReader(CORPUS / f"{name}.ibrec", skip_damage=True) as reader:
        list(reader.frames())
    assert time.perf_counter() - start < 20


def test_resync_gives_up_instead_of_going_quadratic(tmp_path: Path) -> None:
    import struct

    b = Builder().standard(1)
    b.out += b"\xaa" * 8
    dense = struct.pack("<IBBHI", 2000, 5, 0, 0, 0xDEADBEEF) + bytes(4)
    b.out += dense * 200_000  # 3.2 MB of envelope-shaped candidates, each claiming 2 kB
    path = b.write(tmp_path / "a.ibrec")
    start = time.perf_counter()
    with RecordingReader(path) as reader:
        assert [f.seq for f in reader.frames()] == [1]
        [damage] = reader.damage
    assert time.perf_counter() - start < 20
    assert "gave up" in damage.reason and not damage.torn


# -- the corpus --


def test_the_checked_in_corpus_is_what_the_generator_builds() -> None:
    built = cases()
    on_disk = {p.name: p.read_bytes() for p in CORPUS.glob("rec-*.ibrec")}
    assert on_disk.keys() == built.keys(), "run: python tests/rec_corpus.py"
    assert all(on_disk[name] == built[name] for name in built), "run: python tests/rec_corpus.py"


@pytest.mark.parametrize("path", sorted(CORPUS.glob("rec-*.ibrec")), ids=lambda p: p.stem)
def test_corpus_file_is_handled_by_every_consumer(path: Path, tmp_path: Path) -> None:
    exercise_everything(path, tmp_path)


EXPECT_CODE = {
    "rec-empty": Code.REC_FORMAT,
    "rec-magic-only": Code.REC_TORN,
    "rec-short-preamble": Code.REC_TORN,
    "rec-text-file": Code.REC_FORMAT,
    "rec-version-2": Code.REC_VERSION,
    "rec-preamble-flags": Code.REC_VERSION,
    "rec-wrong-nonce-meta": Code.REC_DAMAGED,
    "rec-ends-after-preamble": Code.REC_TORN,
    "rec-ends-inside-meta": Code.REC_TORN,
}


@pytest.mark.parametrize(("name", "code"), sorted(EXPECT_CODE.items()))
def test_unopenable_corpus_files_have_their_codes(name: str, code: Code) -> None:
    with pytest.raises(IronbirdError) as caught:
        RecordingReader(CORPUS / f"{name}.ibrec")
    assert caught.value.code is code


def test_unknown_critical_type_is_a_version_error_and_skippable_is_skipped() -> None:
    with RecordingReader(CORPUS / "rec-unknown-critical-type.ibrec") as reader:
        with pytest.raises(IronbirdError) as caught:
            list(reader.frames())
        assert caught.value.code is Code.REC_VERSION
    got, damage = frames_of(CORPUS / "rec-unknown-skippable-type.ibrec", skip=False)
    assert [f.seq for f in got] == [1, 2, 3] and not damage


def test_trailing_bytes_after_the_trailer_are_reported() -> None:
    got, damage = frames_of(CORPUS / "rec-bytes-after-trailer.ibrec", skip=False)
    assert len(got) == 3 and len(damage) == 1 and damage[0].trailing


def test_torn_and_zero_filled_tails_are_torn_not_mid_file() -> None:
    for name in ("rec-torn-inside-last-record", "rec-zero-filled-tail"):
        got, damage = frames_of(CORPUS / f"{name}.ibrec", skip=False)
        assert [d.torn for d in damage] == [True], name
        assert len(got) >= 2
