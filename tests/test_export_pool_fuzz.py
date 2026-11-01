"""The pool against the single process on adversarial recordings (plan 2m).

Each seed builds a recording from a random mix of frames, sequence resets, runs of records without
frames, late and changed definitions, forged chains of valid records hidden inside others, raw
damage and a torn end (``poolfuzz.py``), exports it both ways in the smallest ranges, with and
without ``--skip-damage``, in every format, with filters, selections, time ranges and an output
that takes a line at a time, and requires everything an export leaves to be the same.

The CI run is a few seeds of each kind. ``IRONBIRD_POOL_FUZZ_SEEDS=N`` runs N of each
(a few seconds per seed; for a change to the pool or the reader's range reads).
"""

from __future__ import annotations

import os
import random
from pathlib import Path
from typing import Any

import pytest

from ironbird.recording.export import ExportFormat
from ironbird.recording.replay import TimeSpec
from poolfuzz import build
from poolutil import T0, assert_same, run_export

SEEDS = int(os.environ.get("IRONBIRD_POOL_FUZZ_SEEDS", "6"))
STEP_NS = 10_000_000
FORMATS: list[tuple[ExportFormat, dict[str, Any]]] = [
    (ExportFormat.TEXT, {}),
    (ExportFormat.CSV, {}),
    (ExportFormat.JSONL, {"where": "mid == 0x890"}),
]
VARIANTS: list[dict[str, Any]] = [
    {"fmt_": ExportFormat.CSV, "messages": ("MOCK_HK",)},
    {"fmt_": ExportFormat.CSV, "header_fields": True, "raw_enums": True},
    {"fmt_": ExportFormat.TEXT, "per_line": True},
    {"fmt_": ExportFormat.CSV, "messages": ("MOCK_HK",), "per_line": True},
    {
        "fmt_": ExportFormat.JSONL,
        "start": TimeSpec(absolute_ns=T0 + 300 * STEP_NS),
        "end": TimeSpec(absolute_ns=T0 + 2000 * STEP_NS),
    },
    {"fmt_": ExportFormat.TEXT, "where": "node == 'fc' and mid != 0x7FF"},
]


@pytest.mark.parametrize("seed", range(SEEDS))
def test_every_format_of_a_random_recording_exports_the_same(tmp_path: Path, seed: int) -> None:
    rng = random.Random(seed)
    path = build(tmp_path / "f.ibrec", rng, steps=rng.choice((100, 300, 600)))
    for skip in (False, True):
        for fmt_, extra in FORMATS:
            single = run_export(path, fmt_, skip_damage=skip, **extra)
            for range_bytes in (4096, 8192):
                pooled = run_export(
                    path, fmt_, jobs=3, skip_damage=skip, range_bytes=range_bytes, **extra
                )
                label = f"seed={seed} skip={skip} {fmt_} range={range_bytes}"
                assert_same(single, pooled, label=f"{label} reruns={pooled.report.reruns}")


@pytest.mark.parametrize("seed", range(1000, 1000 + SEEDS))
def test_options_and_destinations_of_a_random_recording_export_the_same(
    tmp_path: Path, seed: int
) -> None:
    rng = random.Random(seed)
    path = build(tmp_path / "f.ibrec", rng, steps=rng.choice((200, 500, 900)))
    for skip in (False, True):
        for variant in VARIANTS:
            single = run_export(path, skip_damage=skip, **variant)
            pooled = run_export(
                path,
                jobs=3,
                skip_damage=skip,
                range_bytes=rng.choice((4096, 8192, 16384)),
                **variant,
            )
            assert_same(single, pooled, label=f"seed={seed} skip={skip} {variant}")


@pytest.mark.parametrize("seed", range(2000, 2000 + SEEDS))
def test_caps_and_a_closing_output_on_a_random_recording_end_the_same(
    tmp_path: Path, seed: int
) -> None:
    """Results cut short at tiny caps and an output that closes at a random row: the rows taken,
    the tally and the damage and gaps noted are those of the single process."""
    rng = random.Random(seed)
    path = build(tmp_path / "f.ibrec", rng, steps=rng.choice((200, 500)))
    for skip in (False, True):
        for fmt_ in (ExportFormat.TEXT, ExportFormat.CSV):
            extra: dict[str, Any] = {"messages": ("MOCK_HK",)} if fmt_ is ExportFormat.CSV else {}
            refuse = rng.choice((None, 0, 3, 40, 400))
            single = run_export(
                path, fmt_, skip_damage=skip, per_line=True, refuse_after=refuse, **extra
            )
            pooled = run_export(
                path,
                fmt_,
                jobs=3,
                skip_damage=skip,
                per_line=True,
                refuse_after=refuse,
                range_bytes=rng.choice((4096, 8192)),
                text_cap=rng.choice((1, 300, 4000)),
                event_cap=rng.choice((1, 2, 5, 1000)),
                **extra,
            )
            label = f"seed={seed} skip={skip} {fmt_} refuse={refuse}"
            assert_same(
                single, pooled, counts=refuse is None, closed=refuse is not None, label=label
            )
