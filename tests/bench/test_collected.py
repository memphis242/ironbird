"""The benchmarks Stage 2 promised are all there.

docs/plan.md (2l) lists what 2b-2k were to measure. Each item maps to the benchmarks that measure
it in ``tests/test_benchmarks.py``; a renamed or deleted benchmark fails here instead of leaving
the plan's table pointing at nothing. The numbers themselves are in docs/plan.md, from
``pytest tests/test_benchmarks.py -n0 -s``.
"""

from __future__ import annotations

import ast

import pytest

from conftest import REPO

BENCHMARKS = REPO / "tests" / "test_benchmarks.py"

PLANNED: dict[str, tuple[str, ...]] = {
    "ingest frames/s": (
        "test_ingest_only_per_frame_cost",
        "test_ingest_only_saturation_rate_on_loopback",
        "test_ingest_only_at_the_target_rate_loses_nothing_on_loopback",
        "test_listen_cli_at_the_target_rate_loses_no_output",
    ),
    "Recorder.append cost": ("test_recorder_append_cost",),
    "writer MB/s": ("test_recorder_writer_throughput_and_reader_rate",),
    "reader frames/s": ("test_recorder_writer_throughput_and_reader_rate",),
    "filter evaluations/s": (
        "test_filter_evaluation_rate",
        "test_filter_cost_per_predicate",
    ),
    "framer MB/s, clean": ("test_framer_clean_stream_rate", "test_framer_line_noise_rate"),
    "framer MB/s, adversarial": ("test_framer_hostile_stream_stays_within_its_work_budget",),
    "variable-length decode/s": (
        "test_decode_and_check_variable_length_telemetry_rate",
        "test_check_variable_length_frame_rate",
    ),
    "export rows/s": (
        "test_export_writes_every_row_at_the_target_rate_in_every_format",
        "test_export_cli_writes_a_csv_directory_at_the_target_rate",
    ),
}


def _test_names() -> set[str]:
    tree = ast.parse(BENCHMARKS.read_text())
    return {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}


@pytest.mark.parametrize("item", list(PLANNED))
def test_every_planned_benchmark_exists(item: str) -> None:
    missing = [name for name in PLANNED[item] if name not in _test_names()]
    assert not missing, f"{item}: no benchmark named {missing} in tests/test_benchmarks.py"
