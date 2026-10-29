"""Coverage-guided fuzzing (atheris) of the decode boundary.

Usage: python tools/fuzz/fuzz_decode.py [-max_total_time=N]

Invariant: Codec.decode and header.unpack never raise and return a typed result for any
bytes, in either direction. Seeds are the checked-in malformed packets plus valid packets.
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
    from ironbird.codec import Codec, DecodedPacket
    from ironbird.errors import DecodeError

REPO = Path(__file__).resolve().parents[2]
EXAMPLES = REPO / "examples"
CODEC = Codec(
    load_ccdd(EXAMPLES / "ccdd" / "mock.json"),
    header.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml"),
    header.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml"),
    clock=lambda: 1_000_000.25,
)
VALID = (
    CODEC.encode_command("MOCK_NOOP").data,
    CODEC.encode_command("MOCK_SET_LABEL", {"label": "abc"}).data,
    CODEC.encode_telemetry("MOCK_HK", {"cmd_counter": 5, "label": "x"}).data,
)
KINDS = (MessageKind.COMMAND, MessageKind.TELEMETRY)


def one_input(data: bytes) -> None:
    for kind in KINDS:
        result = CODEC.decode(kind, data)
        assert isinstance(result, DecodedPacket | DecodeError)
        parsed = header.unpack(CODEC.profile(kind), data, max_size=CODEC.max_packet_bytes)
        assert isinstance(parsed, header.Header | DecodeError)


def main() -> None:
    """Fuzz in a scratch corpus seeded from the checked-in packets (the repo stays clean)."""
    scratch = Path(tempfile.mkdtemp(prefix="ironbird-fuzz-"))
    for i, packet in enumerate((REPO / "tests" / "corpus" / "malformed").glob("packet-*.bin")):
        (scratch / f"seed{i}").write_bytes(packet.read_bytes())
    for i, packet in enumerate(VALID):
        (scratch / f"valid{i}").write_bytes(packet)
    args = [a for a in sys.argv if a.startswith("-") or a == sys.argv[0]]
    atheris.Setup([*args, str(scratch)], one_input)
    atheris.Fuzz()


if __name__ == "__main__":
    main()
