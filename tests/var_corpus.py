"""The checked-in corpus of malformed variable-length input (Stage 2j).

``ccdd-var-*.json`` are CCDD files whose variable-length fields break a layout rule;
``varpacket-cmd-*.bin`` and ``varpacket-tlm-*.bin`` are packets of the mock CCDD's
``MOCK_LOAD_DATA`` and ``MOCK_DATA`` whose counts or sizes disagree with it. Each case is built
here, so a test proves the checked-in files are exactly these; a new bug adds a case and
regenerates: ``python tests/var_corpus.py``.
"""

from __future__ import annotations

import copy
import json
import struct
from pathlib import Path
from typing import Any, NamedTuple

from ironbird import header
from ironbird.ccdd.loader import load_ccdd
from ironbird.ccdd.model import MessageKind
from ironbird.codec import Codec
from ironbird.errors import Code

REPO = Path(__file__).resolve().parent.parent
EXAMPLES = REPO / "examples"
CORPUS = Path(__file__).resolve().parent / "corpus" / "malformed"


def codec() -> Codec:
    return Codec(
        load_ccdd(EXAMPLES / "ccdd" / "mock.json"),
        header.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml"),
        header.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml"),
        clock=lambda: 1_000_000.25,
    )


# -- CCDD files ---------------------------------------------------------------------------------

COUNT = {"name": "Count", "type": "uint16"}
ENTRIES = {"name": "Entries", "type": "uint32[]", "count": "Count", "max": 64}
TEXT = {"name": "Text", "type": "char[]", "length": "remaining", "max": 122}


def _file(*fields: dict[str, Any], schema: str = "ironbird-ccdd/1", **message: Any) -> str:
    body: dict[str, Any] = {
        "name": "V",
        "kind": "telemetry",
        "mid": "0x0801",
        "fields": [copy.deepcopy(f) for f in fields],
    }
    body.update(message)
    return json.dumps({"schema": schema, "target": "t", "messages": [body]}, separators=(",", ":"))


def _with(field: dict[str, Any], **change: Any) -> dict[str, Any]:
    out = copy.deepcopy(field)
    for key, value in change.items():
        if value is None:
            out.pop(key, None)
        else:
            out[key] = value
    return out


def ccdd_cases() -> dict[str, str]:
    """File name to content; every one must be refused with an ``IB-CCD-*`` error."""
    nine = [
        {"name": f"A{i}", "type": "uint8[]", "length": "remaining" if i == 8 else "n", "max": 1}
        for i in range(9)
    ]
    return {
        "ccdd-var-count-after-array.json": _file(ENTRIES, COUNT),
        "ccdd-var-missing-max.json": _file(COUNT, _with(ENTRIES, max=None)),
        "ccdd-var-signed-count.json": _file(_with(COUNT, type="int16"), ENTRIES),
        "ccdd-var-remaining-not-last.json": _file(
            COUNT, _with(TEXT, name="First"), _with(ENTRIES, name="Second")
        ),
        "ccdd-var-count-too-narrow.json": _file(
            _with(COUNT, type="uint8"), _with(ENTRIES, max=300)
        ),
        "ccdd-var-length-too-narrow.json": _file(
            _with(COUNT, type="uint8"), _with(ENTRIES, count=None, length="Count", max=64)
        ),
        "ccdd-var-fixed-after-variable.json": _file(
            COUNT, ENTRIES, {"name": "Tail", "type": "uint8"}
        ),
        "ccdd-var-count-shared.json": _file(
            COUNT, ENTRIES, _with(ENTRIES, name="More", max=4), TEXT
        ),
        "ccdd-var-schema-0.json": _file(COUNT, ENTRIES, schema="ironbird-ccdd/0"),
        "ccdd-var-payload-size.json": _file(COUNT, ENTRIES, payload_size=300),
        "ccdd-var-payload-bounds-disagree.json": _file(COUNT, ENTRIES, max_payload=5),
        "ccdd-var-nine-fields.json": _file({"name": "n", "type": "uint8"}, *nine),
        "ccdd-var-count-and-length.json": _file(COUNT, _with(ENTRIES, length="Count")),
        "ccdd-var-neither-count-nor-length.json": _file(COUNT, _with(ENTRIES, count=None)),
        "ccdd-var-oversize.json": _file(COUNT, _with(ENTRIES, max=16384)),
        "ccdd-var-count-is-float.json": _file(_with(COUNT, type="float32"), ENTRIES),
        "ccdd-var-count-is-array.json": _file(
            COUNT, ENTRIES, _with(ENTRIES, name="More", count="Entries", max=4)
        ),
        "ccdd-var-count-unknown.json": _file(COUNT, _with(ENTRIES, count="Nope")),
        "ccdd-var-count-not-string.json": _file(COUNT, _with(ENTRIES, count=3)),
        "ccdd-var-bad-element-type.json": _file(COUNT, _with(ENTRIES, type="uint7[]")),
        "ccdd-var-fixed-array-spelling.json": _file(COUNT, _with(ENTRIES, type="uint32[4]")),
        "ccdd-var-max-zero.json": _file(COUNT, _with(ENTRIES, max=0)),
        "ccdd-var-max-bool.json": _file(COUNT, _with(ENTRIES, max=True)),
        "ccdd-var-min-above-max.json": _file(COUNT, _with(ENTRIES, min=65)),
        "ccdd-var-enum.json": _file(COUNT, _with(ENTRIES, enum={"A": 1})),
        "ccdd-var-offset.json": _file(COUNT, _with(ENTRIES, offset=2)),
        "ccdd-var-min-payload-on-fixed.json": _file(COUNT, min_payload=2),
    }


# -- packets ------------------------------------------------------------------------------------


class Packet(NamedTuple):
    data: bytes
    code: Code
    offset: int  # the byte the error points at


def _wrap(c: Codec, kind: MessageKind, name: str, payload: bytes) -> bytes:
    """A packet of message ``name`` with a right header (length, checksum) around ``payload``."""
    msg = c.model.by_name[name]
    profile = c.profile(kind)
    inputs: dict[str, int] = {}
    if kind is MessageKind.COMMAND:
        assert profile.function_code_field is not None and msg.function_code is not None
        inputs[profile.function_code_field] = msg.function_code
    return header.pack(profile, payload, mid=msg.mid, inputs=inputs, now=1_000_000.25)


def _payload(count: int, entries: int, text: bytes) -> bytes:
    return struct.pack("<H", count) + b"\x07\x00\x00\x00" * entries + text


# case name, payload, the error, and where it points past the header
_PACKET_CASES = (
    ("count-above-max", _payload(65, 65, b""), Code.DEC_COUNT, 0),
    ("count-ffff", _payload(0xFFFF, 3, b"abc"), Code.DEC_COUNT, 0),
    ("count-overruns-packet", _payload(64, 3, b""), Code.DEC_OVERRUN, 2),
    ("count-one-too-many", _payload(4, 3, b""), Code.DEC_OVERRUN, 2),
    ("text-above-max", _payload(0, 0, b"x" * 123), Code.DEC_COUNT, 2),
    ("payload-above-max", _payload(64, 64, b"x" * 123), Code.DEC_PAYLOAD, -1),
    ("payload-below-min", b"\x00", Code.DEC_PAYLOAD, -1),
    ("payload-empty", b"", Code.DEC_PAYLOAD, -1),
)


def packet_cases() -> dict[str, Packet]:
    """File name to packet and the error its decode must return."""
    c = codec()
    out: dict[str, Packet] = {}
    for prefix, kind, name in (
        ("cmd", MessageKind.COMMAND, "MOCK_LOAD_DATA"),
        ("tlm", MessageKind.TELEMETRY, "MOCK_DATA"),
    ):
        size = c.profile(kind).size_bytes
        for case, payload, code, past in _PACKET_CASES:
            offset = size if past < 0 else size + past  # -1: the packet as a whole
            out[f"varpacket-{prefix}-{case}.bin"] = Packet(
                _wrap(c, kind, name, payload), code, offset
            )
    return out


def regenerate() -> None:
    for name, text in ccdd_cases().items():
        (CORPUS / name).write_text(text + "\n")
    for name, packet in packet_cases().items():
        (CORPUS / name).write_bytes(packet.data)


if __name__ == "__main__":
    regenerate()
