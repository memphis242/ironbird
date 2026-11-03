"""The nightly atheris harnesses in ``tools/fuzz`` keep working, and every one of them is run by
the nightly job.

A harness nobody runs rots (an import moves, an API changes shape) and its findings are lost, so
each is started for a handful of inputs here when ``atheris`` is installed (the ``fuzz`` extra),
and the workflow is parsed to prove that the scheduled job names each of them.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

from conftest import REPO

HARNESSES = sorted((REPO / "tools" / "fuzz").glob("fuzz_*.py"))
WORKFLOW = REPO / ".github" / "workflows" / "ci.yml"
# Inputs per smoke run: enough to run every seed once, little enough for a PR check. The
# export and replay harnesses decode whole recordings per input.
RUNS = {"fuzz_export": 120, "fuzz_replay": 120, "fuzz_recording": 400}
DEFAULT_RUNS = 300


def test_there_are_harnesses() -> None:
    assert len(HARNESSES) >= 10


@pytest.mark.parametrize("path", HARNESSES, ids=lambda p: p.stem)
def test_harness_runs_its_seeds_without_a_finding(path: Path, tmp_path: Path) -> None:
    pytest.importorskip("atheris")
    crashes = tmp_path / "crashes"
    crashes.mkdir()
    scratch = tmp_path / "scratch"  # libFuzzer exits from C, past atexit: keep the scratch here
    done = subprocess.run(
        [
            sys.executable,
            str(path),
            f"-runs={RUNS.get(path.stem, DEFAULT_RUNS)}",
            "-timeout=5",
            f"-artifact_prefix={crashes}/",
        ],
        cwd=tmp_path,
        env={**os.environ, "IRONBIRD_FUZZ_SCRATCH": str(scratch)},
        capture_output=True,
        text=True,
        timeout=90,
        check=False,
    )
    assert done.returncode == 0, done.stderr[-3000:]
    assert list(crashes.iterdir()) == [], "the harness wrote a crash input"


def test_the_nightly_job_runs_every_harness() -> None:
    document: dict[str, Any] = yaml.safe_load(WORKFLOW.read_text())
    job = document["jobs"]["fuzz"]
    assert "schedule" in job["if"]
    listed = {entry["target"] for entry in job["strategy"]["matrix"]["include"]}
    assert listed == {p.stem.removeprefix("fuzz_") for p in HARNESSES}
    command = "\n".join(str(step.get("run", "")) for step in job["steps"])
    assert "tools/fuzz/fuzz_${{ matrix.target }}.py" in command
