"""Does the worker pool bring a one-hour export under a minute? (docs/plan.md, 2m.)

``ib export`` decodes in ranges over a pool of worker processes when the recording is large
enough. These benchmarks use the shape of the 10k packets/s target, not the 75-byte housekeeping
packet of ``test_export_hour.py``: 200-byte packets of one message with 62 fields
(``targetshape.py``). Each exports the same recording with ``--jobs 1`` and with the cores of the
host, requires the files to be byte for byte the same, and prints the rates and what a one-hour
recording would take at the 1k packets/s floor (3.6 M frames) and the 10k target (36 M frames).

- ``test_the_pool_is_faster_and_writes_the_same_file`` is the CI-sized check, run in the serial
  step (``pytest tests/bench -n0``): a recording of about 10 MiB, ranges of 1 MiB, the pool no
  slower than one process, and faster by a margin that a pool which does nothing would not reach.
- ``test_a_one_hour_target_export_single_and_pooled`` is the measurement
  (``IRONBIRD_BENCH_EXPORT=1``, ``IRONBIRD_BENCH_EXPORT_FRAMES`` frames, 400,000 by default):
  the rates, the speedup, and the parent's peak memory with and without workers.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest
from targetshape import write_target_recording

from hosttiming import needs_a_quiet_host
from recutil import CLI_MAIN, write_config
from replayutil import child_env

FRAMES = int(os.environ.get("IRONBIRD_BENCH_EXPORT_FRAMES", "400000"))
CI_FRAMES = 40_000  # about 10 MiB of 200-byte packets
HOUR_S = 3600
FLOOR_PPS = 1_000
TARGET_PPS = 10_000
BUDGET_S = 60.0  # the plan's bar for a one-hour recording
# The pool on a quiet host with 3 or more cores: far above 1.0, with room for a busy CI runner.
CI_MIN_SPEEDUP = 1.3

# ``main()`` of the CLI, then the peak resident size of this process (not the workers') to a file.
_MEASURED = f"""
import os, resource, sys
try:
    {CLI_MAIN.replace("; ", chr(10) + "    ")}
finally:
    with open(os.environ["IB_BENCH_RSS"], "w") as f:
        f.write(str(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
"""


def cores() -> int:
    return len(os.sched_getaffinity(0))


def _export(
    recording: Path, out: Path, config: Path, jobs: int, env: dict[str, str]
) -> tuple[float, int]:
    """Seconds an export took with ``jobs`` workers, and the parent's peak RSS (KiB)."""
    rss = out.parent / f"{out.name}.rss"
    start = time.perf_counter()
    done = subprocess.run(
        [
            sys.executable,
            "-c",
            _MEASURED,
            "--config",
            str(config),
            "export",
            str(recording),
            "-o",
            str(out),
            "--jobs",
            str(jobs),
        ],
        capture_output=True,
        text=True,
        env={**env, "IB_BENCH_RSS": str(rss)},
        check=False,
        timeout=3600,
    )
    took = time.perf_counter() - start
    assert done.returncode == 0, done.stderr
    return took, int(rss.read_text())


def _digest(directory: Path) -> dict[str, str]:
    return {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(directory.iterdir())}


def _config(directory: Path, range_bytes: str) -> Path:
    path, _, _ = write_config(
        directory, f'[export]\npool_min_bytes = 0\nrange_bytes = "{range_bytes}"\n'
    )
    return path


@needs_a_quiet_host
def test_the_pool_is_faster_and_writes_the_same_file(tmp_path: Path) -> None:
    for name in ("state", "recordings"):
        (tmp_path / name).mkdir(mode=0o700)
    env = child_env(tmp_path)
    recording = write_target_recording(tmp_path / "ci.ibrec", CI_FRAMES)
    config = _config(tmp_path, "1MiB")
    workers = min(cores(), 4)
    single_s, _ = _export(recording, tmp_path / "one", config, 1, env)
    pooled_s, _ = _export(recording, tmp_path / "many", config, workers, env)
    assert _digest(tmp_path / "one") == _digest(tmp_path / "many")
    speedup = single_s / pooled_s
    print(
        f"\nib export, {CI_FRAMES:,} target frames ({recording.stat().st_size >> 20} MiB): "
        f"{CI_FRAMES / single_s:,.0f} rows/s in one process, {CI_FRAMES / pooled_s:,.0f} rows/s "
        f"with {workers} workers (x{speedup:.2f})"
    )
    events = [
        json.loads(line)
        for path in sorted((tmp_path / "state").glob("*.jsonl"))
        for line in path.read_text().splitlines()
    ]
    pool = [e for e in events if e["event"] == "export.pool"]
    assert len(pool) == 1 and pool[0]["workers"] == workers and pool[0]["reruns"] == 0
    assert pool[0]["continued"] == 0 and pool[0]["local"] == 0
    # With fewer than three cores the pool cannot be much faster; it must not be slower.
    assert speedup >= (CI_MIN_SPEEDUP if cores() >= 3 else 0.8), (single_s, pooled_s)


@needs_a_quiet_host
@pytest.mark.skipif(
    os.environ.get("IRONBIRD_BENCH_EXPORT") != "1",
    reason="builds and exports a large recording: set IRONBIRD_BENCH_EXPORT=1",
)
def test_a_one_hour_target_export_single_and_pooled(tmp_path: Path) -> None:
    for name in ("state", "recordings"):
        (tmp_path / name).mkdir(mode=0o700)
    env = child_env(tmp_path)
    recording = write_target_recording(tmp_path / "target.ibrec", FRAMES)
    size = recording.stat().st_size
    config = _config(tmp_path, "4MiB")
    workers = cores()
    single_s, single_rss = _export(recording, tmp_path / "one", config, 1, env)
    pooled_s, pooled_rss = _export(recording, tmp_path / "many", config, workers, env)
    assert _digest(tmp_path / "one") == _digest(tmp_path / "many")
    single_rate, pooled_rate = FRAMES / single_s, FRAMES / pooled_s

    def hour(pps: int, rate: float) -> float:
        return pps * HOUR_S / rate

    print(
        f"\nib export, {FRAMES:,} frames of 200 bytes, 62 fields ({size >> 20} MiB): "
        f"{single_rate:,.0f} rows/s in one process, {pooled_rate:,.0f} rows/s with {workers} "
        f"workers (x{single_s / pooled_s:.2f}); a one-hour recording takes "
        f"{hour(FLOOR_PPS, single_rate):.0f} s -> {hour(FLOOR_PPS, pooled_rate):.0f} s at "
        f"{FLOOR_PPS:,} pkt/s and {hour(TARGET_PPS, single_rate):.0f} s -> "
        f"{hour(TARGET_PPS, pooled_rate):.0f} s at {TARGET_PPS:,} pkt/s (the bar is "
        f"{BUDGET_S:.0f} s); peak RSS of the parent {single_rss >> 10} MiB -> "
        f"{pooled_rss >> 10} MiB"
    )
    assert pooled_rate >= single_rate * (1.5 if workers >= 3 else 0.8)
