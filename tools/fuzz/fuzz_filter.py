"""Coverage-guided fuzzing (atheris) of the filter language.

Usage: python tools/fuzz/fuzz_filter.py [-max_total_time=N]

Invariant: for any text, ``parse`` and ``compile_filter`` return a result or raise only
``FilterError`` (an ``IB-FLT-*`` code with a one-line summary), quickly; a compiled filter runs on
a frame without raising. Seeds are the checked-in ``flt-*`` corpus files plus valid expressions.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import atheris

with atheris.instrument_imports():
    from ironbird import header
    from ironbird.ccdd.loader import load_ccdd
    from ironbird.ccdd.model import MessageKind
    from ironbird.codec import Codec
    from ironbird.errors import DecodeError
    from ironbird.filter import FilterError, compile_filter
    from ironbird.frame import Decoded, Direction, Frame, FrameFlag, Stream

REPO = Path(__file__).resolve().parents[2]
EXAMPLES = REPO / "examples"
CODEC = Codec(
    load_ccdd(EXAMPLES / "ccdd" / "mock.json"),
    header.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml"),
    header.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml"),
    clock=lambda: 1_000_000.25,
)
CODECS = {"fc": CODEC}
STREAM = Stream(1, "ground-fc", "fc", MessageKind.TELEMETRY, Direction.RX, "udp", "udp 127.0.0.1:1")
WIRE = CODEC.encode_telemetry("MOCK_HK", {"cmd_counter": 5, "label": "x"}).data
FRAME = Frame(
    1,
    1,
    STREAM,
    "127.0.0.1:1",
    CODEC.peek_mid(MessageKind.TELEMETRY, WIRE),
    FrameFlag.HAS_MID,
    len(WIRE),
    WIRE,
)
VALID = (
    "msg == MOCK_HK and cmd_counter > 0",
    "event.type >= error or category == event",
    'label contains "ab" and not mode in [SAFE, NOMINAL]',
    "mid in [0x890, 2193] and time >= 2026-10-29T12:00:00.5Z",
)


def decode(frame: Frame) -> Decoded | DecodeError:
    packet = CODEC.decode(frame.stream.kind, frame.data)
    return packet if isinstance(packet, DecodeError) else Decoded(frame, packet)


def one_input(data: bytes) -> None:
    text = data.decode("utf-8", errors="surrogateescape")
    try:
        flt = compile_filter(text, CODECS, links=["ground-fc"])
    except FilterError as err:
        assert "\n" not in err.user_line()
        return
    flt.bind(decode, lambda _node: CODEC)(FRAME)


def main() -> None:
    """Fuzz in a scratch corpus seeded from the checked-in filters (the repo stays clean)."""
    scratch = Path(tempfile.mkdtemp(prefix="ironbird-fuzz-"))
    for i, path in enumerate((REPO / "tests" / "corpus" / "malformed").glob("flt-*.txt")):
        (scratch / f"seed{i}").write_bytes(path.read_bytes())
    for i, text in enumerate(VALID):
        (scratch / f"valid{i}").write_bytes(text.encode())
    args = [a for a in sys.argv if a.startswith("-") or a == sys.argv[0]]
    atheris.Setup([*args, str(scratch)], one_input)
    atheris.Fuzz()


if __name__ == "__main__":
    main()
