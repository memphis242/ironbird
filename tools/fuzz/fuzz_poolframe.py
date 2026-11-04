"""Coverage-guided fuzzing (atheris) of the export pool's wire format (``poolframe``).

Usage: python tools/fuzz/fuzz_poolframe.py [-max_total_time=N]

Invariant: for any bytes, ``decode`` returns a message or raises ``FrameError``, and a message it
returns is exactly what ``encode`` writes for it (the format is canonical); ``read_reply``, the
parent's whole path for a worker's answer, returns an outcome that ``check_reply`` accepts or
raises ``FrameError``. No other exception escapes, and nothing allocates more than the caps allow
(the harness's RSS limit is the check).

The first input byte picks how the rest is used: raw (bit 7 clear), or as ``(offset, value)``
byte pairs written over a valid message (bit 7 set; bits 0-6 pick the sample), with the header's
body length fixed up when bit 6 is set, so the mutations land in the fields rather than in a
length the header check refuses at once. Seeds are the checked-in ``poolframe-*`` replies and
every valid sample of ``tests/poolframe_corpus.py``.
"""

from __future__ import annotations

import sys
from pathlib import Path

import atheris
from fuzzcommon import run, scratch_dir, seed_corpus

with atheris.instrument_imports():
    from ironbird.recording import poolframe as pf

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "tests"))
import poolframe_corpus  # noqa: E402

JOB = poolframe_corpus.JOB
SAMPLES = [pf.encode(m) for _, m in sorted(poolframe_corpus.samples().items())]


def check(data: bytes) -> None:
    try:
        message = pf.decode(data)
    except pf.FrameError:
        pass
    else:
        assert pf.encode(message) == data, "a frame read back is not what was read"
    try:
        reply = pf.read_reply(JOB, data)
    except pf.FrameError:
        return
    pf.check_reply(JOB, reply)


def one_input(data: bytes) -> None:
    if not data:
        return
    mode, rest = data[0], data[1:]
    if not mode & 0x80:
        check(rest)
        return
    frame = bytearray(SAMPLES[(mode & 0x3F) % len(SAMPLES)])
    for i in range(0, len(rest) - 2, 3):
        at = int.from_bytes(rest[i : i + 2], "little") % len(frame)
        frame[at] = rest[i + 2]
    if mode & 0x40:
        frame[8:12] = (len(frame) - pf.HEADER.size).to_bytes(4, "little")
    check(bytes(frame))


def main() -> None:
    """Fuzz in a scratch corpus seeded from the checked-in replies (the repo stays clean)."""
    seeds = seed_corpus(
        scratch_dir("poolframe"),
        ["poolframe-*.bin"],
        valid=SAMPLES,
        prefixes=[b"\x00"],
    )
    run(one_input, seeds, max_len=4096)


if __name__ == "__main__":
    main()
