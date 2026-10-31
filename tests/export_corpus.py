"""The checked-in corpus of recordings that carry hostile *content* for the exporters
(``tests/corpus/malformed/exp-*.ibrec``).

Every file is a well-formed recording (valid records and CRCs, the wide CCDD embedded) whose frames
hold text, floats and bytes that would hurt a spreadsheet or a terminal if they were exported as
they came: formula leads, newlines, escape sequences, bidi controls, NULs and non-finite floats.
Built deterministically (fixed nonce): a test proves the checked-in files are exactly these, and a
new bug adds a case and regenerates: ``python tests/export_corpus.py``.
"""

from __future__ import annotations

import hashlib
import math
import struct
from pathlib import Path

from exportutil import WIDE_BYTES, packet, wide_codec, wide_meta
from ironbird.recording import format as fmt
from ironbird.recording.format import BlobKind, RecordType
from recutil import MAX_PACKET, RX, TX, Builder
from replayutil import COMMAND_PROFILE_PATH, STEP_NS, T0, TELEMETRY_PROFILE_PATH

CORPUS = Path(__file__).resolve().parent / "corpus" / "malformed"
NONCE = 0x0123456789ABCDEF


def event(app: bytes, event_id: int, kind: int, text: bytes) -> bytes:
    return app.ljust(8, b"\0")[:8] + struct.pack("<HH", event_id, kind) + text.ljust(32, b"\0")[:32]


def wide(
    i8: int = 0,
    f32: float = 0.0,
    f64: float = 0.0,
    mode: int = 0,
    name: bytes = b"",
    blob: bytes = b"\0" * 4,
) -> bytes:
    """A WIDE payload with the given fields (the others zero)."""
    msg = wide_codec().model.by_name["WIDE"]
    raw = bytearray(msg.payload_size)

    def put(field_name: str, content: bytes) -> None:
        field = msg.field(field_name)
        assert field is not None
        raw[field.offset : field.offset + len(content)] = content

    put("i8", struct.pack("<b", i8))
    put("f32", struct.pack("<f", f32))
    put("f64", struct.pack("<d", f64))
    put("mode", bytes([mode]))
    put("name", name.ljust(8, b"\0")[:8])
    put("blob", blob)
    return bytes(raw)


def _build(frames: list[tuple[str, bytes]]) -> bytes:
    """A recording of (message name, payload) frames from the wide node, definitions embedded."""
    codec = wide_codec()
    builder = Builder(NONCE, max_packet=MAX_PACKET)
    builder.out = bytearray(fmt.encode_preamble(NONCE))
    builder.offsets = []
    builder.add(RecordType.META, fmt.encode_meta(wide_meta()))
    for kind, adapter, content in (
        (BlobKind.CCDD, "json", WIDE_BYTES),
        (BlobKind.COMMAND_PROFILE, "toml", COMMAND_PROFILE_PATH.read_bytes()),
        (BlobKind.TELEMETRY_PROFILE, "toml", TELEMETRY_PROFILE_PATH.read_bytes()),
    ):
        digest = hashlib.sha256(content).digest()
        builder.add(RecordType.BLOB, fmt.encode_blob(kind, "fc", adapter, digest, content))
    builder.stream(RX)
    builder.stream(TX)
    builder.source(0, "peer")
    for seq, (name, payload) in enumerate(frames, start=1):
        builder.frame(
            seq,
            data=packet(codec, name, payload),
            stream=0,
            source=0,
            mid=codec.model.by_name[name].mid,
            time_ns=T0 + seq * STEP_NS,
        )
    return bytes(builder.out)


def cases() -> dict[str, tuple[bytes, int]]:
    """File name to (content, number of frames in it)."""
    out: dict[str, tuple[bytes, int]] = {}
    hostile_events = [
        ("EVT", event(b"=APP", 1, 3, b"=cmd|' /C calc'!A0\nnext line")),
        ("EVT", event(b"+x", 2, 2, b"\x1b]0;title\x07\x1b[2J\x1b[31m")),
        ("EVT", event(b"-y", 3, 1, b"@SUM(1+1)*cmd|' /C calc'!A0")),
        ("EVT", event(b"\t=z", 4, 4, b"\r=1+1\r\n=2")),
        ("EVT", event(b"\xe2\x80\xaeevil", 5, 9, b"\xff\xfe\xc3\x28 bidi \xe2\x80\xae")),
        ("EVT", event(b"", 0, 0, b'"quote", comma, and\nnewline')),
    ]
    out["exp-formula-event.ibrec"] = (_build(hostile_events), len(hostile_events))
    hostile_wide = [
        (
            "WIDE",
            wide(
                f32=float("inf"), f64=float("-inf"), mode=1, name=b"=1+1", blob=b"\x3d\x2b\x2d\x40"
            ),
        ),
        ("WIDE", wide(f32=math.nan, f64=-0.0, mode=7, name=b"-2+3")),
        (
            "WIDE",
            wide(i8=-128, f32=-1.5e30, f64=5e-324, mode=2, name=b"@A1\x1b[0m", blob=b"\xff" * 4),
        ),
        ("WIDE", wide(name=b"\n\n\n", f64=1.7976931348623157e308)),
    ]
    out["exp-formula-wide.ibrec"] = (_build(hostile_wide), len(hostile_wide))
    return out


def regenerate() -> None:
    for name, (data, _) in cases().items():
        (CORPUS / name).write_bytes(data)


if __name__ == "__main__":
    regenerate()
