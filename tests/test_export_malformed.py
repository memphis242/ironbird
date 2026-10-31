"""Export as an untrusted-input boundary: whatever a recording holds, ``ib export`` ends in a coded
error or a normal end, never a traceback, an assert or a hang, and every frame it reads lands in
exactly one row of one table (or in the count of what a single-file CSV has no place for).

The recording reader and the definitions loaders have their own suites (test_recording_malformed.py,
test_replay_malformed.py); here the checked-in corpora of both are run through the export, and
hostile frames and corrupted files through the tables."""

from __future__ import annotations

import csv
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import export_corpus
from conftest import CORPUS
from exportutil import NullDestination, export_in_process, inert, read_dicts
from ironbird.errors import Code, IronbirdError
from ironbird.recording.export import ExportFormat
from recutil import RX, SERIAL, flip_bit, frame, run_ib
from replay_corpus import cases
from replayutil import T0, child_env, write_recording
from test_replay_malformed import mixed_recording

FUZZ = settings(
    max_examples=80, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture]
)
ALLOWED = {
    Code.REC_FORMAT,
    Code.REC_VERSION,
    Code.REC_DAMAGED,
    Code.REC_TORN,
    Code.REC_DEFS,
    Code.CLI_USAGE,
}
REC_CORPUS = sorted(p.name for p in CORPUS.glob("rec-*.ibrec"))
RPL_CORPUS = sorted(cases())


# -- the checked-in corpora ----


@pytest.mark.parametrize(
    "name", REC_CORPUS, ids=lambda n: n.removeprefix("rec-").removesuffix(".ibrec")
)
def test_the_recording_corpus_exports_or_ends_in_a_coded_error(name: str) -> None:
    for fmt in ExportFormat:
        for skip in (False, True):
            dest = NullDestination()
            try:
                result, replaying = export_in_process(
                    CORPUS / name, None, fmt, skip_damage=skip, destination=dest
                )
            except IronbirdError as exc:
                assert exc.code in ALLOWED or exc.code.value.startswith("IB-FLT-"), exc
                continue
            headers = len(dest.members) if fmt is ExportFormat.CSV else 0
            assert dest.lines - headers == result.rows
            assert result.rows <= replaying.stats.frames_read


@pytest.mark.parametrize(
    "name", RPL_CORPUS, ids=lambda n: n.removeprefix("rpl-").removesuffix(".ibrec")
)
def test_the_definitions_corpus_ends_the_way_replay_does(name: str, tmp_path: Path) -> None:
    _, expect = cases()[name]
    done = run_ib(
        tmp_path, ["export", str(CORPUS / name), "-o", str(tmp_path / "out")], child_env(tmp_path)
    )
    assert "Traceback" not in done.stderr and "IB-INT-001" not in done.stderr, done.stderr
    assert done.returncode == expect.exit_code, done.stderr
    if expect.code is not None:
        assert f"error {expect.code.value}" in done.stderr and expect.reason in done.stderr
        if expect.code is Code.REC_DEFS:
            assert "export with --defs config" in done.stderr
            assert not (tmp_path / "out").exists()  # nothing was created for a refused recording
    assert [p.name for p in tmp_path.iterdir() if p.name.endswith(".tmp")] == []


# -- hostile frames ----


@FUZZ
@given(
    data=st.lists(st.binary(max_size=80), min_size=1, max_size=25),
    mids=st.lists(st.one_of(st.none(), st.integers(0, 0xFFFFF)), min_size=25, max_size=25),
    serial=st.booleans(),
)
def test_any_payload_in_any_frame_lands_in_exactly_one_row(
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
    result, _ = export_in_process(tmp / "r.ibrec", tmp / "out")
    assert result.rows == len(frames)
    seen: list[int] = []
    for table in (tmp / "out").iterdir():
        header = table.read_text().splitlines()[0].split(",")
        rows = list(csv.reader(table.read_text().splitlines()[1:]))
        assert all(len(row) == len(header) for row in rows)
        seen += [int(row[header.index("seq")]) for row in rows]
    assert sorted(seen) == list(range(1, len(frames) + 1))
    for cell in (
        c for table in (tmp / "out").iterdir() for r in read_dicts(table) for c in r.values()
    ):
        assert inert(cell), cell
    text, _ = export_in_process(tmp / "r.ibrec", tmp / "t.txt", ExportFormat.TEXT)
    assert text.rows == len(frames) and len((tmp / "t.txt").read_text().splitlines()) == len(frames)
    where, _ = export_in_process(tmp / "r.ibrec", tmp / "w", where="status == ok")
    other, _ = export_in_process(tmp / "r.ibrec", tmp / "o", where="status != ok")
    assert where.rows + other.rows == len(frames)
    assert where.decoded == where.rows and other.decoded == 0


# -- corrupted recordings ----


@FUZZ
@given(
    flips=st.lists(st.integers(0, 10_000_000), min_size=1, max_size=5),
    cut=st.one_of(st.none(), st.integers(0, 100_000)),
    head_bias=st.booleans(),
)
def test_arbitrary_corruption_of_a_recording_is_exported_or_refused_with_a_code(
    tmp_path_factory: pytest.TempPathFactory,
    flips: list[int],
    cut: int | None,
    head_bias: bool,
) -> None:
    tmp = tmp_path_factory.mktemp("exp")
    source = tmp / "a.ibrec"
    frames = mixed_recording(source)
    raw = source.read_bytes()
    span = min(len(raw), 24_000) if head_bias else len(raw)
    for bit in flips:
        raw = flip_bit(raw, bit % (span * 8))
    if cut is not None:
        raw = raw[: cut % (len(raw) + 1)]
    bad = tmp / "bad.ibrec"
    bad.write_bytes(raw)
    for fmt in ExportFormat:
        for skip in (False, True):
            dest = NullDestination()
            try:
                result, _ = export_in_process(bad, None, fmt, skip_damage=skip, destination=dest)
            except IronbirdError as exc:
                assert exc.code in ALLOWED or exc.code.value.startswith("IB-FLT-"), exc
                continue
            assert result.rows <= len(frames)


def test_every_third_bit_flip_of_the_head_is_exported_or_refused(tmp_path: Path) -> None:
    source = tmp_path / "a.ibrec"
    frames = mixed_recording(source)
    raw = source.read_bytes()
    bad = tmp_path / "bad.ibrec"
    for bit in range(0, 9000 * 8, 193):
        bad.write_bytes(flip_bit(raw, bit))
        try:
            result, _ = export_in_process(
                bad, None, skip_damage=True, destination=NullDestination()
            )
        except IronbirdError as exc:
            assert exc.code in ALLOWED, exc
            continue
        assert result.rows <= len(frames)


def test_a_range_selected_through_the_index_equals_the_same_rows_of_the_full_export(
    tmp_path: Path,
) -> None:
    from ironbird.recording.replay import TimeSpec
    from ironbird.recording.writer import RecorderOptions
    from replayutil import STEP_NS, hk_frames

    write_recording(
        tmp_path / "r.ibrec",
        hk_frames(300),
        options=RecorderOptions(min_free_bytes=0, chunk_bytes=900),
    )
    export_in_process(tmp_path / "r.ibrec", tmp_path / "full.csv", messages=["MOCK_HK"])
    start, end = T0 + 100 * STEP_NS, T0 + 220 * STEP_NS
    result, replaying = export_in_process(
        tmp_path / "r.ibrec",
        tmp_path / "part.csv",
        messages=["MOCK_HK"],
        start=TimeSpec(absolute_ns=start),
        end=TimeSpec(absolute_ns=end),
    )
    wanted = [r for r in read_dicts(tmp_path / "full.csv") if start <= int(r["rx_time_ns"]) < end]
    assert read_dicts(tmp_path / "part.csv") == wanted and result.rows == 120
    assert replaying.stats.frames_read < 300  # the index skipped whole chunks


# -- the exp-* corpus: well-formed recordings with hostile content ----

EXP_CASES = export_corpus.cases()


def test_the_checked_in_export_corpus_is_what_the_builder_builds() -> None:
    built = {name: data for name, (data, _) in EXP_CASES.items()}
    on_disk = {p.name: p.read_bytes() for p in CORPUS.glob("exp-*.ibrec")}
    assert on_disk == built


@pytest.mark.parametrize("name", sorted(EXP_CASES), ids=lambda n: n[4:-6])
def test_hostile_content_exports_inert_in_every_format(name: str, tmp_path: Path) -> None:
    _, count = EXP_CASES[name]
    env = child_env(tmp_path)
    out = tmp_path / "out"
    done = run_ib(tmp_path, ["export", str(CORPUS / name), "-o", str(out)], env)
    assert done.returncode == 0, done.stderr
    assert f"wrote {count} row(s)" in done.stderr
    raw_tables = [p.read_bytes() for p in out.iterdir()]
    for raw in raw_tables:
        assert b"\x1b" not in raw and b"\x07" not in raw and b"\t" not in raw and b"\r" not in raw
        assert b"\xe2\x80\xae" not in raw  # the bidi override is spelled out, not carried
    rows = 0
    for table in out.iterdir():
        for row in read_dicts(table):
            rows += 1
            assert all(inert(cell) for cell in row.values()), row
    assert rows == count
    for fmt in ("text", "jsonl"):
        flat = tmp_path / f"flat.{fmt}"
        again = run_ib(
            tmp_path, ["export", str(CORPUS / name), "--format", fmt, "-o", str(flat)], env
        )
        assert again.returncode == 0, again.stderr
        assert len(flat.read_bytes().splitlines()) == count
        assert b"\x1b" not in flat.read_bytes()
