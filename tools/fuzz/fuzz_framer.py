"""Coverage-guided fuzzing (atheris) of the serial stream framer.

Usage: python tools/fuzz/fuzz_framer.py [-max_total_time=N]

Invariant: for any bytes, in any chunking and with idle gaps anywhere, the framer never raises,
every byte it was given comes out in order inside a frame no longer than the packet cap (a
configured sync marker is consumed in front of packets), and the work it did stays within the
budget it earned. The first input byte picks the profile (a checksummed one or not), the sync
marker and the chunk size; the rest is the stream. Seeds are the checked-in ``ser-*`` streams.

A second oracle takes the input as the description of valid packets (the top bit of the first
byte selects it): however they are read, a byte at a time included, every packet is emitted
unchanged and in order, nothing is skipped, and the work budget is never exhausted.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import atheris

with atheris.instrument_imports():
    from ironbird import header
    from ironbird.ccdd.model import MessageKind
    from ironbird.transport.framing import EVAL_COST, HeaderLengthFramer, WorkBudget

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "tests"))
import ser_corpus  # noqa: E402

CAP = ser_corpus.CAP
CODEC = ser_corpus.codec()
TLM = MessageKind.TELEMETRY
PEC = header.load_profile(REPO / "examples" / "headers" / "ccsds_pec_tlm.toml")
SYNCS = (b"", b"\x1a\xcf\xfc\x1d", b"\xaa")
IDLE_NS = 50_000_000
VALID_MIDS = {1: (0x100, 0x101), 0: (0x893, 0x8FF)}  # by selector bit 0: the CCDD's, then not
STINGY = WorkBudget(base=2048, cap=4096)  # a stream that costs more than its bytes pay runs out


def make(selector: int) -> tuple[HeaderLengthFramer, bytes]:
    sync = SYNCS[(selector >> 1) % len(SYNCS)]
    if selector & 1:
        framer = HeaderLengthFramer(
            PEC, max_packet_bytes=CAP, sync=sync, payload_bounds={0x100: (0, 200)}.get
        )
    else:
        framer = HeaderLengthFramer(
            CODEC.profile(TLM),
            max_packet_bytes=CAP,
            sync=sync,
            payload_bounds=lambda mid: CODEC.payload_bounds(TLM, mid),
        )
    return framer, sync


def valid_input(data: bytes) -> None:
    """The oracle for valid packets read in any way (see the module docstring)."""
    selector = data[0]
    if selector & 1:
        profile, bounds = PEC, {0x100: (0, 200)}.get
    else:
        profile, bounds = CODEC.profile(TLM), lambda mid: CODEC.payload_bounds(TLM, mid)
    sync = SYNCS[(selector >> 1) % len(SYNCS)]
    mids = VALID_MIDS[selector & 1]
    # a few reads of one byte, the rest of every size
    size = 1 if data[1] % 3 == 0 else 1 + data[1] % 97
    packets = [
        header.pack(
            profile,
            bytes([data[i + 1]]) * (data[i] % 48),
            mid=mids[data[i + 1] & 1],
        )
        for i in range(2, len(data) - 1, 2)
    ]
    if not packets:
        return
    framer = HeaderLengthFramer(
        profile, max_packet_bytes=CAP, sync=sync, payload_bounds=bounds, budget=STINGY
    )
    stream = b"".join(sync + p for p in packets)
    out = []
    for n, start in enumerate(range(0, len(stream), size)):
        out += framer.feed(stream[start : start + size], n, 1_000 * (n + 1))
    out += framer.flush(len(stream))
    assert [(e.flags, e.data) for e in out] == [(0, p) for p in packets], "a packet was not framed"
    stats = framer.stats()
    assert stats.work_exhausted == 0 and stats.garbage_bytes == 0 and stats.false_syncs == 0


def one_input(data: bytes) -> None:
    if len(data) < 2:
        return
    if data[0] & 0x80:
        valid_input(data)
        return
    framer, sync = make(data[0])
    size, stream = 1 + data[1] % 97, data[2:]
    out = []
    mono = 0
    for n, start in enumerate(range(0, len(stream), size)):
        mono += 1_000
        out += framer.feed(stream[start : start + size], n, mono)
        if n % 5 == 4:
            out += framer.expire(mono + IDLE_NS, n)
    out += framer.flush(len(stream))
    rebuilt = b"".join((sync + e.data) if e.flags == 0 else e.data for e in out)
    assert rebuilt == stream, "a byte was lost, duplicated or reordered"
    assert all(0 < len(e.data) <= CAP for e in out)
    stats = framer.stats()
    assert stats.work_spent <= stats.work_earned + CAP // 2 + 4 * EVAL_COST
    assert stats.pending_bytes == 0 and framer.deadline_ns() is None


def main() -> None:
    """Fuzz in a scratch corpus seeded from the checked-in streams (the repo stays clean)."""
    scratch = Path(tempfile.mkdtemp(prefix="ironbird-fuzz-framer-"))
    corpus = REPO / "tests" / "corpus" / "malformed"
    for i, path in enumerate(sorted(corpus.glob("ser-*.bin"))):
        (scratch / f"seed{i}").write_bytes(b"\x00\x07" + path.read_bytes())
    args = [a for a in sys.argv if a.startswith("-") or a == sys.argv[0]]
    atheris.Setup([*args, str(scratch)], one_input)
    atheris.Fuzz()


if __name__ == "__main__":
    main()
