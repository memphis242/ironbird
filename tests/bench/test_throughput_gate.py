"""Throughput gate: a loopback blaster sends 200-byte valid telemetry into an ``ib record``
subprocess, and the recording, the ledger and /proc say what happened.

Two runs, one set of checks:

* the floor, 1k packets/s, on any runner (``FLOOR_SECONDS`` of it, to fit CI time): every
  datagram is recorded in order, nothing is dropped by the kernel or the recorder's buffer, and the
  ``--count`` loop left no frame unread;
* the design target, 10k packets/s for 60 s, on the development host (opt in with
  ``IRONBIRD_BENCH_10K=1``): the same checks, a bound on the recorder's CPU share, and a
  comparison with ``baseline.json``, where a CPU cost per frame or a peak RSS more than 20% above
  the baseline is a regression and fails. ``IRONBIRD_BENCH_UPDATE=1`` rewrites the baseline from
  the run instead (after a deliberate change, on the host the baseline names).

Both are host-timing guards: under xdist they skip, and CI runs them in the serial step
``pytest tests/bench -n0``. Every run prints its numbers and writes them to
``$IRONBIRD_BENCH_RESULTS`` (a JSON file path) when that is set.
"""

from __future__ import annotations

import json
import os
import platform
from pathlib import Path
from typing import Any

import pytest
import yaml
from gate import GateResult, run_gate

from conftest import REPO, without_host_warnings
from hosttiming import needs_a_quiet_host

FLOOR_PPS = 1_000
FLOOR_SECONDS = 15
TARGET_PPS = 10_000
TARGET_SECONDS = 60
BASELINE = Path(__file__).with_name("baseline.json")
REGRESSION_LIMIT = 1.20  # a measure more than 20% above the baseline fails
# Bounds that hold on any host. The recorder's buffer is capped at max_buffer_bytes (256 MiB by
# default); a healthy run keeps a small fraction of it, and the process stays a small fraction
# of the cap above its start-up size.
PEAK_PENDING_LIMIT_BYTES = 16 * 1024 * 1024
RSS_GROWTH_LIMIT_KIB = 96 * 1024
TARGET_CPU_SHARE_LIMIT = 0.75  # cores' worth, at 10k packets/s; the design leaves room


def _report(name: str, result: GateResult) -> None:
    print(
        f"\n{name}: {result.recorded}/{result.sent} recorded at {result.rate:,} pkt/s over "
        f"{result.blast_s:.1f} s (blaster at most {result.blast_max_lag_ms:.1f} ms late); "
        f"kernel drops {result.kernel_gap_lost} (socket counter {result.udp_socket_drops}), "
        f"unread by the --count loop {result.queue_not_shown}, "
        f"buffer peak {result.peak_pending_bytes / 1024:.0f} "
        f"KiB of {result.max_buffer_bytes >> 20} MiB, RSS {result.rss_before_kib / 1024:.0f} -> "
        f"{result.rss_peak_kib / 1024:.0f} MiB, CPU {result.cpu_share * 100:.0f}% of a core "
        f"({result.cpu_us_per_frame:.0f} us per frame), file {result.file_bytes >> 20} MiB"
    )
    target = os.environ.get("IRONBIRD_BENCH_RESULTS")
    if target:
        path = Path(target)
        previous: dict[str, Any] = json.loads(path.read_text()) if path.exists() else {}
        previous[name] = result.as_dict()
        path.write_text(json.dumps(previous, indent=2, sort_keys=True) + "\n")


def _assert_lossless(result: GateResult) -> None:
    """Every checked count is exact: a datagram sent is a frame recorded, in order."""
    host_free = without_host_warnings("\n".join(result.stderr_lines))
    assert result.exit_code == 0, host_free
    assert not any("IB-" in line for line in host_free), host_free
    assert result.sent == result.rate * (result.sent // result.rate)
    assert result.recorded == result.sent, (
        f"{result.sent - result.recorded} of {result.sent} datagrams are not in the recording"
    )
    assert result.recorded_total == result.sent and result.in_order
    assert result.kernel_gap_lost == 0 and result.udp_socket_drops == 0
    assert result.queue_not_shown == 0  # the loop kept up; the recorder sees every frame anyway
    assert result.overrun_frames == 0 and result.gap_records == 0
    assert result.status == "closed"
    # the blaster kept to the schedule, so the rate that was measured is the rate that was asked
    assert result.blast_s <= result.sent / result.rate * 1.1 + 0.5, result.blast_s


def _assert_bounded(result: GateResult) -> None:
    assert result.peak_pending_bytes <= PEAK_PENDING_LIMIT_BYTES, result.peak_pending_bytes
    assert result.peak_pending_bytes < result.max_buffer_bytes
    growth = result.rss_peak_kib - result.rss_before_kib
    assert growth <= RSS_GROWTH_LIMIT_KIB, f"RSS grew {growth} KiB"


def _host() -> dict[str, Any]:
    model = ""
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                model = line.partition(":")[2].strip()
                break
    except OSError:
        pass
    return {"cpus": os.cpu_count(), "cpu_model": model, "machine": platform.machine()}


@needs_a_quiet_host
def test_floor_rate_is_recorded_with_zero_loss(tmp_path: Path) -> None:
    result = run_gate(tmp_path, FLOOR_PPS, FLOOR_SECONDS)
    _report("floor", result)
    _assert_lossless(result)
    _assert_bounded(result)


@needs_a_quiet_host
@pytest.mark.skipif(
    os.environ.get("IRONBIRD_BENCH_10K") != "1",
    reason="the 10k pkt/s target runs for 60 s on the development host: set IRONBIRD_BENCH_10K=1",
)
def test_target_rate_is_recorded_with_zero_loss_and_no_regression(tmp_path: Path) -> None:
    result = run_gate(tmp_path, TARGET_PPS, TARGET_SECONDS)
    _report("target", result)
    _assert_lossless(result)
    _assert_bounded(result)
    assert result.cpu_share <= TARGET_CPU_SHARE_LIMIT, f"{result.cpu_share:.2f} cores"

    measures = {
        "cpu_us_per_frame": result.cpu_us_per_frame,
        "rss_peak_kib": float(result.rss_peak_kib),
    }
    if os.environ.get("IRONBIRD_BENCH_UPDATE") == "1":
        baseline = {"host": _host(), "target": {k: round(v, 1) for k, v in measures.items()}}
        BASELINE.write_text(json.dumps(baseline, indent=2, sort_keys=True) + "\n")
        return
    baseline = json.loads(BASELINE.read_text())
    if baseline["host"] != _host():
        pytest.skip(f"the baseline is for {baseline['host']}, this host is {_host()}")
    regressions = {
        name: (value, baseline["target"][name])
        for name, value in measures.items()
        if value > baseline["target"][name] * REGRESSION_LIMIT
    }
    assert not regressions, f"more than 20% above the baseline (measured, baseline): {regressions}"


# -- the CI step that enforces the gate ---------------------------------------------------------

WORKFLOW = REPO / ".github" / "workflows" / "ci.yml"
PARALLEL_RUN = "pytest -n auto"
GATE_RUN = "pytest tests/bench -n0"


def _jobs_without_the_gate(workflow_text: str) -> list[str]:
    """Jobs that run the suite under xdist (where the gate skips) without a later serial run of
    it that is allowed to fail the job."""
    document = yaml.safe_load(workflow_text)
    assert isinstance(document, dict) and isinstance(document.get("jobs"), dict)
    problems: list[str] = []
    for name, job in document["jobs"].items():
        steps = job.get("steps", []) if isinstance(job, dict) else []
        runs = [(str(step.get("run", "")), step) for step in steps if isinstance(step, dict)]
        parallel = [i for i, (run, _) in enumerate(runs) if PARALLEL_RUN in run]
        if not parallel:
            continue
        gated = [
            i
            for i, (run, step) in enumerate(runs)
            if GATE_RUN in run and not step.get("continue-on-error")
        ]
        if not any(i > parallel[-1] for i in gated):
            problems.append(name)
    return problems


def test_every_parallel_ci_job_runs_the_throughput_gate_serially() -> None:
    assert _jobs_without_the_gate(WORKFLOW.read_text()) == []


@pytest.mark.parametrize(
    "old, new",
    [
        (GATE_RUN, "pytest tests/bench -n auto"),
        (f"run: {GATE_RUN} -s\n", f"run: {GATE_RUN} -s\n        continue-on-error: true\n"),
    ],
    ids=["gate made parallel", "gate allowed to fail"],
)
def test_the_check_catches_a_lost_gate(old: str, new: str) -> None:
    text = WORKFLOW.read_text()
    assert old in text, f"mutation anchor not found: {old!r}"
    assert _jobs_without_the_gate(text.replace(old, new))
