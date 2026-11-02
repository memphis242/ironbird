"""A recording whose embedded definitions make very wide rows: one telemetry message of many
fields, each an enum with a long label, so a few KiB of frame become tens of KiB of text. For the
tests of what a pooled export holds in memory."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from ironbird.ccdd.loader import load_ccdd
from ironbird.codec import Codec
from ironbird.header import load_profile
from ironbird.recording.format import BlobKind
from recutil import MAX_PACKET
from replayutil import (
    COMMAND_PROFILE_PATH,
    STEP_NS,
    T0,
    TELEMETRY_PROFILE_PATH,
    crafted,
    meta_data,
)

FIELDS = 1000  # fields of the default message
LABEL_CHARS = 64


def wide_ccdd(count: int = FIELDS) -> bytes:
    fields = [
        {"name": f"f{i}", "type": "uint8", "enum": {f"L{i:03d}_".ljust(LABEL_CHARS, "x"): 1}}
        for i in range(count)
    ]
    doc = {
        "schema": "ironbird-ccdd/1",
        "target": "mock",
        "messages": [
            {
                "name": "WIDE",
                "kind": "telemetry",
                "mid": "0x0890",
                "description": "wide",
                "fields": fields,
            }
        ],
    }
    return json.dumps(doc).encode()


def wide_recording(path: Path, frames: int, fields: int = FIELDS) -> Path:
    """``frames`` frames of the wide message of ``fields`` fields (by default about 1 KiB each, and
    about 70 KiB of text each)."""
    ccdd = wide_ccdd(fields)
    scratch = path.parent / "wide.json"
    scratch.write_bytes(ccdd)
    codec = Codec(
        load_ccdd(scratch),
        load_profile(COMMAND_PROFILE_PATH),
        load_profile(TELEMETRY_PROFILE_PATH),
        max_packet_bytes=MAX_PACKET,
        clock=lambda: 1.0,
    )
    scratch.unlink()
    data = codec.encode_telemetry("WIDE", {f"f{i}": 1 for i in range(fields)}).data
    command, telemetry = COMMAND_PROFILE_PATH.read_bytes(), TELEMETRY_PROFILE_PATH.read_bytes()
    blobs = [
        (BlobKind.CCDD, "fc", "json", ccdd),
        (BlobKind.COMMAND_PROFILE, "fc", "toml", command),
        (BlobKind.TELEMETRY_PROFILE, "fc", "toml", telemetry),
    ]
    meta = meta_data()
    meta["nodes"] = {
        "fc": {
            "adapter": "json",
            "ccdd_sha256": hashlib.sha256(ccdd).hexdigest(),
            "command_profile_sha256": hashlib.sha256(command).hexdigest(),
            "telemetry_profile_sha256": hashlib.sha256(telemetry).hexdigest(),
        }
    }
    builder = crafted(blobs, meta=meta, frames=0)
    for seq in range(1, frames + 1):
        builder.frame(seq, data=data, stream=0, source=0, mid=0x890, time_ns=T0 + seq * STEP_NS)
    return builder.write(path)
