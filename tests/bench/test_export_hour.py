"""Does a one-hour recording export in under a minute in one process? (docs/plan.md, 2h: the
decode worker pool stayed unbuilt until a 2l benchmark said it did not; the pool is 2m, and
``test_export_pool_bench.py`` measures it on packets of the target's shape.)

``ib export`` is one streaming pass, so its cost is linear in the rows. This builds a recording
of ``IRONBIRD_BENCH_EXPORT_FRAMES`` frames (600,000 by default: ten minutes at the 1k packets/s
floor), exports it and a tenth of it with the real command, checks that the rate does not fall as
the file grows (what the extrapolation rests on), and prints the time a one-hour recording would
take at the floor (3.6 M frames) and at the 10k packets/s target (36 M frames). Nothing is
asserted about the verdict: the plan records it. The frames are the small housekeeping message
(about 75 bytes), not the 200-byte packets of the 10k target, so the rate is optimistic for
those. Opt in with ``IRONBIRD_BENCH_EXPORT=1``.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from hosttiming import needs_a_quiet_host
from ironbird.frame import Frame
from ironbird.recording.writer import RecorderOptions
from recutil import CLI_MAIN
from replayutil import child_env, hk_frames, write_recording

FRAMES = int(os.environ.get("IRONBIRD_BENCH_EXPORT_FRAMES", "600000"))
CHUNK = 50_000
HOUR_S = 3600
FLOOR_PPS = 1_000
TARGET_PPS = 10_000
BUDGET_S = 60.0  # the plan's bar for a one-hour recording
LINEARITY = 0.75  # the larger export may be at most a quarter slower per row


def _frames(count: int) -> Iterator[Frame]:
    for start in range(1, count + 1, CHUNK):
        yield from hk_frames(min(CHUNK, count - start + 1), start=start)


def _export_rate(recording: Path, out: Path, rows: int, env: dict[str, str]) -> float:
    start = time.perf_counter()
    done = subprocess.run(
        [sys.executable, "-c", CLI_MAIN, "export", str(recording), "-o", str(out), "--jobs", "1"],
        capture_output=True,
        text=True,
        env=env,
        check=False,
        timeout=3600,
    )
    took = time.perf_counter() - start
    assert done.returncode == 0, done.stderr
    written = sum(p.read_bytes().count(b"\n") - 1 for p in out.iterdir())
    assert written == rows
    return rows / took


@needs_a_quiet_host
@pytest.mark.skipif(
    os.environ.get("IRONBIRD_BENCH_EXPORT") != "1",
    reason="builds and exports a large recording: set IRONBIRD_BENCH_EXPORT=1",
)
def test_a_one_hour_recording_exports_at_a_steady_rate(tmp_path: Path) -> None:
    env = child_env(tmp_path)
    options = RecorderOptions(min_free_bytes=0)
    small, large = FRAMES // 10, FRAMES
    write_recording(tmp_path / "small.ibrec", _frames(small), options=options)
    write_recording(tmp_path / "large.ibrec", _frames(large), options=options)
    size = (tmp_path / "large.ibrec").stat().st_size
    small_rate = _export_rate(tmp_path / "small.ibrec", tmp_path / "small", small, env)
    large_rate = _export_rate(tmp_path / "large.ibrec", tmp_path / "large", large, env)
    floor_hour_s = FLOOR_PPS * HOUR_S / large_rate
    target_hour_s = TARGET_PPS * HOUR_S / large_rate
    print(
        f"\nib export: {small:,} rows at {small_rate:,.0f} rows/s, {large:,} rows "
        f"({size >> 20} MiB) at {large_rate:,.0f} rows/s; a one-hour recording takes about "
        f"{floor_hour_s:.0f} s at {FLOOR_PPS:,} pkt/s and {target_hour_s:.0f} s at "
        f"{TARGET_PPS:,} pkt/s (the bar is {BUDGET_S:.0f} s)"
    )
    assert large_rate >= LINEARITY * small_rate, "the rate falls as the file grows"
