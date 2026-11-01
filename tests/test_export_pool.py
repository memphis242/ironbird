"""Parallel export decode (plan 2m): the pool writes what one process writes.

The differential tests run one export twice, in this process and through the pool in the
smallest ranges (a few hundred frames), and compare everything an export leaves: the files and
their bytes, the tally, the damage reported, the replay's own counts, the error it ends in. The
corpus is clean recordings of every shape (split sets, filters, time ranges, every format and
option), recordings crafted record by record (definitions that appear late, records of an unknown
critical type, sequence numbers that go back), the checked-in malformed corpora, and recordings
damaged at random and exactly at the edges of the ranges.
"""

from __future__ import annotations

import random
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pytest

import export_corpus
from conftest import CORPUS
from ironbird.config import parse_config
from ironbird.errors import Code
from ironbird.recording.export import ExportFormat
from ironbird.recording.pexport import WINDOW_PER_WORKER, ConfigDefinitions, _Changed
from ironbird.recording.reader import Damage, Item, RecordingReader
from ironbird.recording.replay import TimeSpec
from ironbird.recording.writer import RecorderOptions
from poolutil import (
    RANGE,
    assert_same,
    builder_recording,
    clean_recording,
    frame_offsets,
    mixed_frames,
    mutate,
    run_export,
    unsupported_record,
    write_builder,
)
from rec_corpus import cases as rec_cases
from recutil import RX, SERIAL, TX
from replayutil import write_recording

JOBS = 3
FORMATS = list(ExportFormat)


@pytest.fixture(scope="module")
def clean(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return clean_recording(tmp_path_factory.mktemp("clean") / "clean.ibrec")


def both(
    files: Path | list[Path],
    fmt: ExportFormat = ExportFormat.CSV,
    *,
    pooled_part: bool = True,
    **kwargs: Any,
) -> None:
    """Export both ways and require the same, and (``pooled_part``) the pool to have been used."""
    single = run_export(files, fmt, jobs=1, **kwargs)
    pooled = run_export(files, fmt, jobs=JOBS, **kwargs)
    assert_same(single, pooled)
    if pooled_part:
        assert pooled.report.parts_pooled >= 1, "the pool was not used"


# -- clean recordings, every format and option --------------------------------------------------


@pytest.mark.parametrize("fmt", FORMATS)
def test_a_clean_recording_exports_the_same_in_every_format(clean: Path, fmt: ExportFormat) -> None:
    single = run_export(clean, fmt, jobs=1)
    pooled = run_export(clean, fmt, jobs=JOBS)
    assert_same(single, pooled)
    assert single.result is not None and single.result.rows > 2000
    assert pooled.report.ranges > 20 and pooled.report.reruns == 0


@pytest.mark.parametrize(
    "options",
    [
        {"header_fields": True},
        {"raw_enums": True},
        {"where": "mid == 0x890"},
        {"where": "node == fc and MOCK_HK.cmd_counter > 100"},
        {"where": "link == 'ground-fc' and dir == rx"},
        {"where": "len > 40 and len < 70"},
        {"where": "mid == 0x7FF or seq > 2000"},
        {"messages": ["MOCK_HK"]},
        {"messages": ["MOCK_HK", "MOCK_STATUS"]},
        {"messages": ["MOCK_STATUS"], "where": "mid == 0x891"},
        {"header_fields": True, "raw_enums": True, "where": "mid != 0x890"},
    ],
    ids=lambda o: "-".join(f"{k}={v}" for k, v in o.items()),
)
@pytest.mark.parametrize("fmt", FORMATS)
def test_options_and_filters_export_the_same(
    clean: Path, fmt: ExportFormat, options: dict[str, Any]
) -> None:
    if fmt is not ExportFormat.CSV and (options.get("header_fields") or options.get("raw_enums")):
        pytest.skip("CSV options")
    both(clean, fmt, **options)


def test_a_time_range_exports_the_same(clean: Path) -> None:
    """A range that rules chunks out is read through the index in this process (the pool shares
    only reads that go record by record from the start of a part)."""
    both(
        clean,
        pooled_part=False,
        start=TimeSpec(offset_ns=3_000_000_000),
        end=TimeSpec(offset_ns=9_000_000_000),
    )
    both(clean, pooled_part=False, start=TimeSpec(offset_ns=20_000_000_000))
    both(clean, pooled_part=False, end=TimeSpec(offset_ns=1))
    # a range that takes in every chunk skips none, and shares
    both(clean, end=TimeSpec(offset_ns=10**15))


def test_a_range_that_selects_nothing_exports_the_same(clean: Path) -> None:
    single = run_export(clean, jobs=1, start=TimeSpec(offset_ns=10**15))
    pooled = run_export(clean, jobs=JOBS, start=TimeSpec(offset_ns=10**15))
    assert_same(single, pooled)


def test_the_output_stream_of_text_to_a_line_at_a_time_destination_is_the_same(clean: Path) -> None:
    for fmt in (ExportFormat.TEXT, ExportFormat.JSONL):
        single = run_export(clean, fmt, jobs=1, per_line=True)
        pooled = run_export(clean, fmt, jobs=JOBS, per_line=True)
        assert_same(single, pooled)


@pytest.mark.parametrize("refuse_after", [0, 1, 777, 2000])
@pytest.mark.parametrize("fmt", [ExportFormat.TEXT, ExportFormat.CSV])
def test_an_output_that_closes_ends_the_export_with_the_same_rows(
    clean: Path, fmt: ExportFormat, refuse_after: int
) -> None:
    """A reader that goes away (``| head``): the rows taken and their tally agree. (What was read
    ahead is not compared: workers read whole ranges.)"""
    kwargs: dict[str, Any] = {"per_line": True, "refuse_after": refuse_after}
    if fmt is ExportFormat.CSV:
        kwargs["messages"] = ["MOCK_HK"]
    single = run_export(clean, fmt, jobs=1, **kwargs)
    pooled = run_export(clean, fmt, jobs=JOBS, **kwargs)
    assert_same(single, pooled, counts=False, closed=True)
    assert pooled.result is not None
    if refuse_after < 1500:
        assert pooled.result.ending.value == "output"


# -- several files and parts --------------------------------------------------------------------


def test_a_split_recording_exports_the_same(tmp_path: Path) -> None:
    base = tmp_path / "run.ibrec"
    options = RecorderOptions(min_free_bytes=0, split_bytes=40_000, chunk_bytes=5000)
    recorder = write_recording(base, mixed_frames(1500), options=options, streams=(RX, TX, SERIAL))
    assert len(recorder.parts) >= 3
    both(base)
    both(base, ExportFormat.JSONL)
    both(list(recorder.parts))  # the parts named one by one
    both(list(recorder.parts)[1:])  # a recording that starts with a later part


def test_a_split_recording_with_a_missing_part_exports_the_same(tmp_path: Path) -> None:
    base = tmp_path / "run.ibrec"
    options = RecorderOptions(min_free_bytes=0, split_bytes=40_000, chunk_bytes=5000)
    recorder = write_recording(base, mixed_frames(1500), options=options, streams=(RX, TX, SERIAL))
    parts = list(recorder.parts)
    parts[1].unlink()
    both(base)


def test_a_part_with_damage_stops_the_set_where_the_single_process_does(tmp_path: Path) -> None:
    base = tmp_path / "run.ibrec"
    options = RecorderOptions(min_free_bytes=0, split_bytes=40_000, chunk_bytes=5000)
    recorder = write_recording(base, mixed_frames(1500), options=options, streams=(RX, TX, SERIAL))
    second = list(recorder.parts)[1]
    data = bytearray(second.read_bytes())
    data[len(data) // 2 : len(data) // 2 + 300] = bytes(300)
    second.write_bytes(bytes(data))
    for skip in (False, True):
        both(base, skip_damage=skip)


# -- crafted recordings -------------------------------------------------------------------------


def test_gaps_marks_and_time_ranges_count_the_same(tmp_path: Path) -> None:
    """GAP and MARK records between the frames, counted per range of time as the single process
    counts them (a recording with no index is read record by record, so the ranges share it)."""
    path = write_builder(builder_recording(2000, gaps_every=37), tmp_path / "gaps.ibrec")
    first = 1_700_000_000_000_000_000
    cases: list[dict[str, Any]] = [
        {},
        {"start": TimeSpec(absolute_ns=first + 5_000_000_000)},
        {"end": TimeSpec(absolute_ns=first + 12_000_000_000)},
        {
            "start": TimeSpec(absolute_ns=first + 3_000_000_000),
            "end": TimeSpec(absolute_ns=first + 14_000_000_000),
        },
        {"start": TimeSpec(offset_ns=2_000_000_000), "end": TimeSpec(offset_ns=9_000_000_000)},
    ]
    whole = run_export(path, jobs=1)
    assert whole.stats["gaps"] > 20 and whole.stats["gap_frames"] > 20
    for kwargs in cases:
        for fmt in FORMATS:
            single = run_export(path, fmt, jobs=1, **kwargs)
            pooled = run_export(path, fmt, jobs=JOBS, **kwargs)
            assert_same(single, pooled, label=str(kwargs))
            assert pooled.report.parts_pooled == 1


@pytest.mark.parametrize("every", [3, 40, 400])
def test_definitions_that_appear_late_export_the_same(tmp_path: Path, every: int) -> None:
    """A stream and a source defined in the middle of the file: ranges seeded from the head do not
    know them, and are read again with what the walk knows. Used by every ``every``-th frame, so
    a range that starts well finds out it is wrong only further in."""
    path = write_builder(
        builder_recording(2400, late_stream_at=700, late_every=every), tmp_path / "late.ibrec"
    )
    single = run_export(path, jobs=1)
    pooled = run_export(path, jobs=JOBS)
    assert_same(single, pooled)
    assert single.error is None and single.result is not None and single.result.rows == 2400
    assert pooled.report.reruns >= 1


def test_a_stream_defined_again_differently_is_damage_as_in_one_process(tmp_path: Path) -> None:
    """A range seeded without the stream defines it without complaint; the walk knows better."""
    path = write_builder(
        builder_recording(1600, late_stream_at=700, late_every=30, redefine_at=960),
        tmp_path / "redefined.ibrec",
    )
    for skip in (False, True):
        single = run_export(path, jobs=1, skip_damage=skip)
        pooled = run_export(path, jobs=JOBS, skip_damage=skip)
        assert_same(single, pooled)
        assert single.damage, "the redefinition is not damage"


def test_a_sequence_number_that_goes_back_at_the_start_of_a_range_exports_the_same(
    tmp_path: Path,
) -> None:
    """Each range's first frame is tested against the last frame before it, which a range cannot
    know: put a repeat exactly at the first record of each of several ranges."""
    probe = builder_recording(1500)
    offsets = frame_offsets(probe)
    first = RecordingReader(write_builder(probe, tmp_path / "probe.ibrec")).body_start
    for edge in (4, 7, 11, 15):
        guess = (first + edge * RANGE + 7) & ~7
        at = next(i for i, offset in enumerate(offsets) if offset >= guess)
        builder = builder_recording(1500, repeat_seq_at=at + 1)  # frame number at + 1 (1-based)
        path = write_builder(builder, tmp_path / f"repeat{edge}.ibrec")
        for skip in (False, True):
            single = run_export(path, jobs=1, skip_damage=skip)
            pooled = run_export(path, jobs=JOBS, skip_damage=skip)
            assert_same(single, pooled)
            assert single.damage, "the repeated sequence number is not damage"


def test_a_record_of_an_unknown_critical_type_ends_the_export_the_same(tmp_path: Path) -> None:
    builder = builder_recording(600)
    unsupported_record(builder)
    for seq in range(601, 900):
        builder.frame(seq, data=b"z" * 30, mid=0x890)
    path = write_builder(builder, tmp_path / "unsupported.ibrec")
    single = run_export(path, jobs=1)
    pooled = run_export(path, jobs=JOBS)
    assert_same(single, pooled)
    assert single.error is not None and single.error[0] == Code.REC_VERSION.value


def test_a_frame_of_an_undefined_stream_is_damage_the_same(tmp_path: Path) -> None:
    builder = builder_recording(800)
    builder.frame(801, data=b"q" * 30, stream=9, mid=0x890)
    for seq in range(802, 1100):
        builder.frame(seq, data=b"q" * 30, mid=0x890)
    path = write_builder(builder, tmp_path / "undefined.ibrec")
    for skip in (False, True):
        both(path, skip_damage=skip)


# -- damage -------------------------------------------------------------------------------------


def damaged_cases(base: Path) -> list[tuple[str, bytes]]:
    data = base.read_bytes()
    rng = random.Random(0x2B)
    return [(f"seed{n}", mutate(data, rng)) for n in range(36)]


@pytest.mark.parametrize("skip_damage", [False, True])
def test_a_damaged_recording_exports_the_same(
    clean: Path, tmp_path: Path, skip_damage: bool
) -> None:
    """Damage of every kind at random places: the export ends where the single process ends, with
    the same coded error and the same rows before it; with ``--skip-damage`` it goes past."""
    reruns = 0
    for index, (name, data) in enumerate(damaged_cases(clean)):
        path = tmp_path / f"{name}.ibrec"
        path.write_bytes(data)
        fmt = FORMATS[index % len(FORMATS)]
        single = run_export(path, fmt, jobs=1, skip_damage=skip_damage)
        pooled = run_export(path, fmt, jobs=JOBS, skip_damage=skip_damage)
        assert_same(single, pooled, label=name)
        reruns += pooled.report.reruns
    assert reruns > 0, "no case reached the re-reading of a damaged range"


def test_damage_exactly_at_the_edges_of_the_ranges_exports_the_same(
    clean: Path, tmp_path: Path
) -> None:
    data = clean.read_bytes()
    body = RecordingReader(clean).body_start
    for edge in (1, 2, 3, 7, 12):
        at = (body + edge * RANGE) & ~7
        for delta in (-9, -1, 0, 1, 8):
            for how in ("flip", "zero"):
                out = bytearray(data)
                if how == "flip":
                    out[at + delta] ^= 0x40
                else:
                    out[at + delta : at + delta + 24] = bytes(24)
                path = tmp_path / f"edge{edge}_{delta}_{how}.ibrec"
                path.write_bytes(bytes(out))
                for skip in (False, True):
                    single = run_export(path, jobs=1, skip_damage=skip)
                    pooled = run_export(path, jobs=JOBS, skip_damage=skip)
                    assert_same(single, pooled)


def test_a_recording_torn_at_every_kind_of_place_exports_the_same(
    clean: Path, tmp_path: Path
) -> None:
    data = clean.read_bytes()
    for cut in (len(data) - 1, len(data) - 30, len(data) * 2 // 3, len(data) // 2 + 3, 9000):
        path = tmp_path / f"torn{cut}.ibrec"
        path.write_bytes(data[:cut])
        both(path)


# -- the checked-in corpora ---------------------------------------------------------------------


def test_the_malformed_corpora_export_the_same(tmp_path: Path) -> None:
    """Every file of the corpora, which are small: one range, so the pool is not used, and the
    answer is the single process's; the same files enlarged with frames after the damage are
    in the damaged cases above."""
    names = sorted(p.name for p in CORPUS.glob("*.ibrec"))
    assert len(names) > 50
    for name in names:
        for skip in (False, True):
            single = run_export(CORPUS / name, jobs=1, skip_damage=skip)
            pooled = run_export(CORPUS / name, jobs=JOBS, skip_damage=skip)
            assert_same(single, pooled, label=name)


def test_crafted_damage_cases_are_exported_the_same_inside_a_large_file(tmp_path: Path) -> None:
    """The rec corpus's files are tiny; put the pooled path under them by writing each between
    long runs of valid frames (the head is the corpus file's, the frames the base's)."""
    cases = rec_cases()
    assert cases and export_corpus.CORPUS.exists()
    base = clean_recording(tmp_path / "base.ibrec", 600)
    data = base.read_bytes()
    for index, (name, content) in enumerate(sorted(cases.items())):
        if not content.startswith(data[:8]) or len(content) < 100:
            continue
        # the damaged record bytes of the case, placed in the middle of the base's frames
        middle = len(data) // 2 & ~7
        spliced = data[:middle] + content[24:] + data[middle:]
        path = tmp_path / f"c{index}.ibrec"
        path.write_bytes(spliced)
        for skip in (False, True):
            single = run_export(path, jobs=1, skip_damage=skip)
            pooled = run_export(path, jobs=JOBS, skip_damage=skip)
            assert_same(single, pooled, label=name)


# -- bounded memory -----------------------------------------------------------------------------


def test_results_wait_in_a_bounded_window(clean: Path) -> None:
    pooled = run_export(clean, ExportFormat.JSONL, jobs=2)
    report = pooled.report
    assert report.max_pending <= 2 * WINDOW_PER_WORKER
    # a range of a few KiB of frames: the rows of a window of ranges, nothing like the recording
    size = clean.stat().st_size
    assert report.max_text_bytes < size, (report, size)
    assert report.ranges > 3 * report.max_pending


# -- the reader's range read --------------------------------------------------------------------


def records(items: Iterable[Item | Damage]) -> list[tuple[int, int, int]]:
    out = []
    for item in items:
        assert isinstance(item, Item)
        out.append((item.offset, item.end, item.rtype))
    return out


def test_reading_a_file_in_ranges_gives_the_records_of_one_read(clean: Path) -> None:
    """The pieces the pool relies on: a read that begins at a record boundary with the state the
    records before it left, and ends before a given offset."""
    with RecordingReader(clean) as whole:
        every = records(whole.items())
    rng = random.Random(7)
    for _ in range(20):
        cut = rng.randrange(every[0][0], every[-1][1])
        with RecordingReader(clean) as head:
            first = records(head.items(stop=cut))
            assert head.reached_stop
            seed = (dict(head.streams), dict(head.sources), head.last_seq, head.work)
            resume = head.position
        with RecordingReader(clean) as tail:
            tail.seed_state(*seed)
            second = records(tail.items(start=resume))
        assert first + second == every
        assert resume >= cut
        with RecordingReader(clean) as probe:
            assert probe.find_boundary(cut) is not None


# -- definitions of a worker --------------------------------------------------------------------


def test_a_worker_refuses_definitions_that_changed_under_the_export(tmp_path: Path) -> None:
    from ironbird.config import load_config

    ccdd = tmp_path / "ccdd.json"
    ccdd.write_text((Path(__file__).parent.parent / "examples/ccdd/mock.json").read_text())
    from conftest import EXAMPLES

    text = (EXAMPLES / "mock.toml").read_text()
    text = text.replace('"ccdd/', f'"{EXAMPLES}/ccdd/').replace(
        '"headers/', f'"{EXAMPLES}/headers/'
    )
    text = text.replace(f'"{EXAMPLES}/ccdd/mock.json"', f'"{ccdd}"')
    config_path = tmp_path / "ironbird.toml"
    config_path.write_text(text)
    config = load_config(config_path)
    nodes = tuple(n for n, node in config.nodes.items() if node.ccdd is not None)
    recipe = ConfigDefinitions.of(config.path, config.sha256, config.nodes, nodes, 2048, "export")
    assert recipe.build()  # as the parent saw it
    ccdd.write_text(ccdd.read_text() + "\n")  # a byte changes
    with pytest.raises(_Changed):
        recipe.build()
    config_path.write_text(text + "\n# edited\n")
    with pytest.raises(_Changed):
        recipe.build()


def test_the_export_table_of_the_configuration(tmp_path: Path) -> None:
    from ironbird.errors import IronbirdError

    base = (Path(__file__).parent.parent / "examples" / "mock.toml").read_text()
    path = tmp_path / "c.toml"

    def parse(extra: str):  # type: ignore[no-untyped-def]
        path.write_text(base + "\n" + extra)
        import tomllib

        return parse_config(tomllib.loads(path.read_text()), path)

    default = parse("")
    assert default.export.jobs is None and default.export.pool_min_bytes > 0
    got = parse('[export]\njobs = 3\npool_min_bytes = "1MiB"\nrange_bytes = 8192\n')
    assert (got.export.jobs, got.export.pool_min_bytes, got.export.range_bytes) == (
        3,
        1 << 20,
        8192,
    )
    assert parse("[export]\npool_min_bytes = 0\n").export.pool_min_bytes == 0
    for bad in (
        "[export]\njobs = 0\n",
        "[export]\njobs = 1000\n",
        "[export]\njobs = true\n",
        '[export]\njobs = "4"\n',
        "[export]\nrange_bytes = 100\n",
        "[export]\nrange_bytes = 4099\n",
        "[export]\nunknown = 1\n",
        "export = 3\n",
    ):
        with pytest.raises(IronbirdError) as caught:
            parse(bad)
        assert caught.value.code is Code.CFG_SCHEMA, bad


def test_a_range_is_accepted_only_if_the_walk_agrees_with_what_it_relied_on() -> None:
    from dataclasses import replace

    from ironbird.recording.pexport import Outcome, Seed, _Walk

    stream = RX
    other = replace(RX, link="elsewhere")
    seed = Seed({0: stream}, {0: "peer"}, 0, 0)

    def outcome(**kwargs: object) -> Outcome:
        return Outcome(0, **kwargs)  # type: ignore[arg-type]

    walk = _Walk(streams={0: stream}, sources={0: "peer"})
    assert walk.agrees_with(seed, outcome(used_streams=[0], used_sources=[0]))
    assert walk.agrees_with(seed, outcome())
    # relied on a stream or source the walk does not have, or has differently
    assert not _Walk().agrees_with(seed, outcome(used_streams=[0]))
    assert not _Walk(streams={0: other}).agrees_with(seed, outcome(used_streams=[0]))
    assert not _Walk(sources={0: "else"}).agrees_with(seed, outcome(used_sources=[0]))
    # defined something the walk has defined differently
    assert not _Walk(streams={0: other}).agrees_with(
        Seed({}, {}, 0, 0), outcome(definitions=[("stream", stream)])
    )
    assert not _Walk(sources={0: "else"}).agrees_with(
        Seed({}, {}, 0, 0), outcome(definitions=[("source", 0, "peer")])
    )
    assert _Walk(streams={0: stream}).agrees_with(
        Seed({}, {}, 0, 0), outcome(definitions=[("stream", stream)])
    )


def test_workers_that_cannot_start_are_a_coded_error(
    clean: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import concurrent.futures as cf

    def refuse(self: object, *args: object, **kwargs: object) -> None:
        raise BlockingIOError(11, "Resource temporarily unavailable")

    monkeypatch.setattr(cf.ProcessPoolExecutor, "submit", refuse)
    pooled = run_export(clean, jobs=JOBS, pool=False)
    assert pooled.error is not None and pooled.error[0] == Code.EXP_WORKER.value
    assert "--jobs 1" in pooled.error[1] and "Resource temporarily" in pooled.error[1]
