"""Coverage-guided fuzzing (atheris) of replay: a recording and the definitions it embeds.

Usage: python tools/fuzz/fuzz_replay.py [-max_total_time=N]

Invariant: whatever bytes a file holds, replaying it (every output format, with and without
``skip_damage``, with and without a filter) raises only a coded ``IronbirdError``: damage ends
in ``IB-REC-004..007``, definitions that cannot be used in ``IB-REC-009``, a filter that does not
compile in ``IB-FLT-*``. A frame is never shown more than once. Seeds are the checked-in malformed
recordings (``rec-*`` and ``rpl-*``) plus a valid recording with definitions embedded.
"""

from __future__ import annotations

import contextlib
import sys
import tempfile
from pathlib import Path

import atheris

with atheris.instrument_imports():
    from ironbird.errors import IronbirdError
    from ironbird.recording.player import OutputFormat

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "tests"))
SCRATCH = Path(tempfile.mkdtemp(prefix="ironbird-fuzz-replay-"))
TARGET = SCRATCH / "input.ibrec"
WHERES = (None, "status != ok", "cmd_counter > 3 or msg == MOCK_HK")


def one_input(data: bytes) -> None:
    from replayutil import replay_in_process  # noqa: PLC0415

    TARGET.write_bytes(data)
    for fmt in OutputFormat:
        for skip in (False, True):
            for where in WHERES:
                with contextlib.suppress(IronbirdError):
                    run = replay_in_process(TARGET, fmt_=fmt, skip_damage=skip, where=where)
                    assert len(run.lines) == run.result.shown


def main() -> None:
    """Fuzz in a scratch corpus seeded from the checked-in recordings (the repo stays clean)."""
    from test_replay_malformed import mixed_recording  # noqa: PLC0415

    seeds = SCRATCH / "seeds"
    seeds.mkdir()
    corpus = REPO / "tests" / "corpus" / "malformed"
    for i, path in enumerate([*corpus.glob("rec-*.ibrec"), *corpus.glob("rpl-*.ibrec")]):
        (seeds / f"seed{i}").write_bytes(path.read_bytes())
    mixed_recording(SCRATCH / "valid.ibrec")
    (seeds / "valid").write_bytes((SCRATCH / "valid.ibrec").read_bytes())
    args = [a for a in sys.argv if a.startswith("-") or a == sys.argv[0]]
    atheris.Setup([*args, str(seeds)], one_input)
    atheris.Fuzz()


if __name__ == "__main__":
    main()
