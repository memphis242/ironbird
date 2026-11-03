"""Coverage-guided fuzzing (atheris) of the recording reader.

Usage: python tools/fuzz/fuzz_recording.py [-max_total_time=N]

Invariant: whatever bytes a file holds, opening it raises only a coded ``IronbirdError``, and
every consumer (iteration with and without ``skip_damage``, ``scan``, ``info``) returns; a frame
it yields passes the ``Frame`` invariants by construction. Seeds are the checked-in malformed
recordings plus valid ones.
"""

from __future__ import annotations

import contextlib
import sys
from pathlib import Path

import atheris
from fuzzcommon import run, scratch_dir

with atheris.instrument_imports():
    from ironbird.errors import IronbirdError
    from ironbird.recording import inspect as rec_inspect
    from ironbird.recording.reader import RecordingReader

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "tests"))
SCRATCH = scratch_dir("rec")
TARGET = SCRATCH / "input.ibrec"


def one_input(data: bytes) -> None:
    TARGET.write_bytes(data)
    for skip in (False, True):
        with contextlib.suppress(IronbirdError), RecordingReader(TARGET, skip_damage=skip) as rd:
            for _ in rd.frames():
                pass
    for consume in (rec_inspect.scan, rec_inspect.info):
        with contextlib.suppress(IronbirdError):
            consume(TARGET)


def main() -> None:
    """Fuzz in a scratch corpus seeded from the checked-in recordings (the repo stays clean)."""
    from recutil import sample  # noqa: PLC0415

    seeds = SCRATCH / "seeds"
    seeds.mkdir()
    for i, path in enumerate((REPO / "tests" / "corpus" / "malformed").glob("rec-*.ibrec")):
        (seeds / f"seed{i}").write_bytes(path.read_bytes())
    sample(SCRATCH / "valid.ibrec", 20)
    (seeds / "valid").write_bytes((SCRATCH / "valid.ibrec").read_bytes())
    run(one_input, seeds, max_len=65536)


if __name__ == "__main__":
    main()
