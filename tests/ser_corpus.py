"""The checked-in corpus of malformed serial streams (``tests/corpus/malformed/ser-*.bin``).

Each case is a byte stream a serial framer must survive. A case with ``expect`` pins exactly what
the framer emits for it: the stream is the concatenation of those frames. A case without one
(``expect=None``) is hostile input where only the invariants matter (everything is accounted for,
the work stays within its budget). Every case is built here, so a test proves the checked-in
files are exactly these; a new bug adds a case and regenerates: ``python tests/ser_corpus.py``.

The framer is the mock node's telemetry framer (header profile ``ccsds_v1_cfs_tlm``, which has no
checksum), the packet cap is 1024, the MID must be in the CCDD, and the stream ends in silence.
"""

from __future__ import annotations

import random
from pathlib import Path
from typing import NamedTuple

from ironbird import header
from ironbird.ccdd.loader import load_ccdd
from ironbird.ccdd.model import MessageKind
from ironbird.codec import Codec
from ironbird.frame import FLAG_GARBAGE, FLAG_STALE

REPO = Path(__file__).resolve().parent.parent
EXAMPLES = REPO / "examples"
CORPUS = Path(__file__).resolve().parent / "corpus" / "malformed"
CAP = 1024
PACKET, GARBAGE, STALE = 0, FLAG_GARBAGE, FLAG_STALE


class Case(NamedTuple):
    stream: bytes
    expect: tuple[tuple[int, bytes], ...] | None  # (flags, data) of each frame, in order


def codec() -> Codec:
    return Codec(
        load_ccdd(EXAMPLES / "ccdd" / "mock.json"),
        header.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml"),
        header.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml"),
        max_packet_bytes=CAP,
        clock=lambda: 1_000_000.25,
    )


def _hk(c: Codec, counter: int = 1) -> bytes:
    return c.encode_telemetry("MOCK_HK", {"cmd_counter": counter}).data


def _data_header(c: Codec, total: int) -> bytes:
    """The header of a MOCK_DATA packet that claims ``total`` bytes (it is shorter or longer
    than what follows: the length field lies)."""
    profile = c.profile(MessageKind.TELEMETRY)
    full = header.pack(profile, bytes(total - profile.min_bytes), mid=0x893)
    return full[: profile.size_bytes]


def cases() -> dict[str, Case]:
    c = codec()
    profile = c.profile(MessageKind.TELEMETRY)
    hk = [_hk(c, i) for i in range(1, 7)]
    rng = random.Random(2026)  # fixed seed: the files are reproducible
    # bytes whose top five bits are not 00001, so no offset can begin a telemetry header
    allowed = [b for b in range(256) if b >> 3 != 1]
    noise = bytes(rng.choice(allowed) for _ in range(600))
    out: dict[str, Case] = {}

    out["ser-noise-no-header.bin"] = Case(noise, ((GARBAGE, noise),))
    out["ser-all-ff.bin"] = Case(
        b"\xff" * 3000,
        ((GARBAGE, b"\xff" * CAP), (GARBAGE, b"\xff" * CAP), (GARBAGE, b"\xff" * 952)),
    )

    huge = bytearray(_data_header(c, 100))
    huge[4:6] = (0xFFFF).to_bytes(2, "big")  # the length field says 65535 + 7 bytes
    out["ser-length-field-huge.bin"] = Case(
        bytes(huge) + hk[0] + hk[1],
        ((GARBAGE, bytes(huge)), (PACKET, hk[0]), (PACKET, hk[1])),
    )

    small = bytearray(_data_header(c, 100))
    small[4:6] = (0).to_bytes(2, "big")  # the length field says 7 bytes: below the header
    out["ser-length-field-below-header.bin"] = Case(
        bytes(small) + hk[0], ((GARBAGE, bytes(small)), (PACKET, hk[0]))
    )

    # A plausible header (a known MID, a payload size its CCDD allows) whose length covers the
    # packets after it and ends inside one of them, where no header begins: it is not a packet.
    covers = _data_header(c, profile.min_bytes + 100)
    out["ser-length-covers-next-packets.bin"] = Case(
        covers + b"".join(hk), ((GARBAGE, covers), *((PACKET, p) for p in hk[:6]))
    )

    # The same lie ending exactly where a real header begins: with no checksum to go by, the
    # framer takes it for a packet. The case pins that this is the limit of a checksum-less profile.
    lands = _data_header(c, len(hk[0]) + len(hk[1]) + profile.size_bytes)
    out["ser-length-lands-on-next-header.bin"] = Case(
        lands + hk[0] + hk[1] + hk[2],
        ((PACKET, lands + hk[0] + hk[1]), (PACKET, hk[2])),
    )

    partial = _data_header(c, profile.min_bytes + 100) + bytes(range(20))
    out["ser-header-then-silence.bin"] = Case(partial, ((STALE, partial),))
    out["ser-packet-cut-short.bin"] = Case(hk[0][:20], ((STALE, hk[0][:20]),))
    window = hk[0][: profile.length_window or 6]
    out["ser-length-window-only.bin"] = Case(window, ((GARBAGE, window),))

    out["ser-sane-header-repeated.bin"] = Case(_data_header(c, profile.min_bytes + 10) * 300, None)
    out["ser-header-like-staircase.bin"] = Case(
        b"".join(bytes([0x08 | (i & 1), i & 0xFF]) * 4 for i in range(500)), None
    )
    return out


def regenerate() -> None:
    for name, case in cases().items():
        (CORPUS / name).write_bytes(case.stream)


if __name__ == "__main__":
    regenerate()
