"""Host-timing guard for ``ib replay --speed``: a paced replay takes about the recorded time over
the speed, not just at least it.

The lower bound holds on any host and is enforced everywhere (tests/test_replay_cli.py); an upper
bound measures the host as much as the tool, so this skips under xdist and runs, enforced, in the
serial CI step ``pytest tests/bench -n0``. The span is the replay's own, from its ``replay.start``
to its ``replay.stop`` ledger record (the interpreter's start-up and exit left out); on a quiet
host it is the recorded time plus about 10 ms of opening, decoding and writing.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from hosttiming import needs_a_quiet_host
from ironbird.filter.lexer import parse_time
from recutil import run_ib
from replayutil import child_env, hk_frames, write_config_58k, write_recording

SECOND = 1_000_000_000
OVER_MAX_NS = 250_000_000  # beyond the paced time, for a replay of 11 frames


@needs_a_quiet_host
@pytest.mark.parametrize(("speed", "step_ns"), [(1, SECOND // 10), (100, 10 * SECOND)])
def test_a_paced_replay_takes_about_the_recorded_time_over_the_speed(
    tmp_path: Path, speed: int, step_ns: int
) -> None:
    config, _, _ = write_config_58k(tmp_path)
    env = child_env(tmp_path)
    env["IRONBIRD_CONFIG"] = str(config)
    write_recording(tmp_path / "r.ibrec", hk_frames(11, step_ns=step_ns))
    paced_ns = 10 * step_ns // speed  # 1 s for both
    done = run_ib(tmp_path, ["replay", str(tmp_path / "r.ibrec"), "--speed", str(speed)], env)
    assert done.returncode == 0 and len(done.stdout.splitlines()) == 11, done.stderr
    events = [
        json.loads(line)
        for path in sorted((tmp_path / "state").glob("*.jsonl"))
        for line in path.read_text().splitlines()
    ]
    start = next(e for e in events if e["event"] == "replay.start")
    stop = next(e for e in events if e["event"] == "replay.stop")
    span = parse_time(stop["ts"]) - parse_time(start["ts"])
    print(f"\nreplay --speed {speed}: {span / 1e6:.1f} ms for {paced_ns / 1e6:.0f} ms paced")
    assert paced_ns <= span < paced_ns + OVER_MAX_NS
