"""Coverage-guided fuzzing (atheris) of the decode boundary.

Usage: python tools/fuzz/fuzz_decode.py [-max_total_time=N]

Invariant: Codec.decode and header.unpack never raise and return a typed result for any
bytes, in either direction, through the shipped headers, the cFE MsgId V2 headers (a MID read
from three places) and a header with a CRC trailer; frame_length answers an int or None.
Seeds are the checked-in malformed packets (including the variable-length ones) plus valid
packets.
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

import atheris

with atheris.instrument_imports():
    from ironbird import header
    from ironbird.ccdd.json_adapter import parse_model
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
_SHIPPED_VALID = (
    CODEC.encode_command("MOCK_NOOP").data,
    CODEC.encode_command("MOCK_SET_LABEL", {"label": "abc"}).data,
    CODEC.encode_telemetry("MOCK_HK", {"cmd_counter": 5, "label": "x"}).data,
    CODEC.encode_command("MOCK_LOAD_DATA", {"Entries": "1,2,3", "Text": "abc"}).data,
    CODEC.encode_telemetry("MOCK_DATA", {"Entries": list(range(40)), "Text": "hello"}).data,
)
KINDS = (MessageKind.COMMAND, MessageKind.TELEMETRY)


def _msgid_v2_codec() -> Codec:
    """The mock CCDD on the cFE MsgId V2 headers: a MID read from three places."""
    doc = json.loads((EXAMPLES / "ccdd" / "mock.json").read_text())
    for message in doc["messages"]:
        if message["kind"] == "command":
            message["mid"] = 0x2A90
        else:
            message["mid"] = 0x2A10 + (int(message["mid"], 0) & 0xF)
    return Codec(
        parse_model(doc, "mock-msgid-v2.json"),
        header.load_profile(EXAMPLES / "headers" / "cfe_msgid_v2_cmd.toml"),
        header.load_profile(EXAMPLES / "headers" / "cfe_msgid_v2_tlm.toml"),
        clock=lambda: 1_700_000_000.0,
    )


def _pec_codec() -> Codec:
    """A message on a header with a CRC-16 trailer (packet error control)."""
    model = parse_model(
        {
            "schema": "ironbird-ccdd/0",
            "target": "pec",
            "messages": [
                {
                    "name": "PEC_HK",
                    "kind": "telemetry",
                    "mid": 0x0123,
                    "category": "housekeeping",
                    "fields": [{"name": "counter", "type": "uint32"}],
                }
            ],
        },
        "pec.json",
    )
    return Codec(
        model,
        header.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml"),
        header.load_profile(EXAMPLES / "headers" / "ccsds_pec_tlm.toml"),
    )


V2 = _msgid_v2_codec()
PEC = _pec_codec()
CODECS = (CODEC, V2, PEC)
VALID = (
    *_SHIPPED_VALID,
    V2.encode_command("MOCK_NOOP").data,
    V2.encode_telemetry("MOCK_HK", {"cmd_counter": 5}).data,
    PEC.encode_telemetry("PEC_HK", {"counter": 9}).data,
)


def one_input(data: bytes) -> None:
    for codec in CODECS:
        for kind in KINDS:
            result = codec.decode(kind, data)
            assert isinstance(result, DecodedPacket | DecodeError)
            assert (codec.check(kind, data) is None) == isinstance(result, DecodedPacket)
            profile = codec.profile(kind)
            parsed = header.unpack(profile, data, max_size=codec.max_packet_bytes)
            assert isinstance(parsed, header.Header | DecodeError)
            if profile.length_window is not None:
                assert profile.frame_length(data) is None or isinstance(
                    profile.frame_length(data), int
                )


def main() -> None:
    """Fuzz in a scratch corpus seeded from the checked-in packets (the repo stays clean)."""
    scratch = Path(tempfile.mkdtemp(prefix="ironbird-fuzz-"))
    for i, packet in enumerate((REPO / "tests" / "corpus" / "malformed").glob("*packet-*.bin")):
        (scratch / f"seed{i}").write_bytes(packet.read_bytes())
    for i, packet in enumerate(VALID):
        (scratch / f"valid{i}").write_bytes(packet)
    args = [a for a in sys.argv if a.startswith("-") or a == sys.argv[0]]
    atheris.Setup([*args, str(scratch)], one_input)
    atheris.Fuzz()


if __name__ == "__main__":
    main()
