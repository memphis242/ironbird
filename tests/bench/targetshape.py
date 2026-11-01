"""Recordings of the 10k packets/s target's shape for the export benchmarks: 200-byte telemetry
packets of one message with 62 fields of every width (counters, voltages, floats, a string, flags
with an enum), not the 75-byte housekeeping packet of the mock definitions."""

from __future__ import annotations

import asyncio
import hashlib
import json
import random
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from conftest import EXAMPLES
from ironbird.ccdd.loader import load_ccdd_bytes
from ironbird.codec import Codec
from ironbird.frame import Frame
from ironbird.header import load_profile
from ironbird.recording.format import BlobKind
from ironbird.recording.writer import BlobSpec, Recorder, RecorderOptions
from recutil import MAX_PACKET, RX, TX, frame
from replayutil import STEP_NS, T0, meta_data

COMMAND_PROFILE = EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml"
TELEMETRY_PROFILE = EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml"
PACKET_BYTES = 200
HEADER_BYTES = 16
MID = 0x0899
VARIANTS = 128  # distinct packets the frames cycle through


def _fields() -> list[dict[str, Any]]:
    fields: list[dict[str, Any]] = []
    for i in range(24):
        fields.append({"name": f"counter{i}", "type": "uint16"})
    for i in range(12):
        fields.append({"name": f"total{i}", "type": "uint32"})
    for i in range(8):
        fields.append({"name": f"sensor{i}", "type": "int16"})
    for i in range(6):
        fields.append({"name": f"volts{i}", "type": "float32"})
    for i in range(3):
        fields.append({"name": f"attitude{i}", "type": "float64"})
    fields.append({"name": "label", "type": "char[16]"})
    fields.append(
        {"name": "mode", "type": "uint8", "enum": {"SAFE": 0, "NOMINAL": 1, "SCIENCE": 2}}
    )
    for i in range(7):
        fields.append({"name": f"flags{i}", "type": "uint8"})
    return fields


def target_ccdd() -> bytes:
    fields = _fields()
    assert len(fields) == 62
    return json.dumps(
        {
            "schema": "ironbird-ccdd/0",
            "target": "target",
            "messages": [
                {
                    "name": "TARGET_HK",
                    "kind": "telemetry",
                    "mid": hex(MID),
                    "category": "housekeeping",
                    "description": "a 200-byte housekeeping packet",
                    "fields": fields,
                }
            ],
        }
    ).encode()


def target_codec() -> Codec:
    return Codec(
        load_ccdd_bytes(target_ccdd(), "target.json", "json"),
        load_profile(COMMAND_PROFILE),
        load_profile(TELEMETRY_PROFILE),
        max_packet_bytes=MAX_PACKET,
        clock=lambda: 1_000_000.25,
    )


def _packets() -> list[bytes]:
    codec = target_codec()
    rng = random.Random(2025)
    out: list[bytes] = []
    for n in range(VARIANTS):
        values: dict[str, Any] = {}
        for spec in _fields():
            name, kind = spec["name"], spec["type"]
            if name == "label":
                values[name] = f"unit-{n:03d}"
            elif name == "mode":
                values[name] = ("SAFE", "NOMINAL", "SCIENCE")[n % 3]
            elif kind.startswith("float"):
                values[name] = round(rng.uniform(-1000, 1000), 3)
            elif kind == "uint8":
                values[name] = rng.randrange(256)
            elif kind == "int16":
                values[name] = rng.randrange(-30000, 30000)
            elif kind == "uint16":
                values[name] = rng.randrange(65536)
            else:
                values[name] = rng.randrange(1 << 32)
        codec.reset_sequences()
        data = codec.encode_telemetry("TARGET_HK", values).data
        assert len(data) == PACKET_BYTES, len(data)
        out.append(data)
    return out


def target_frames(count: int, *, start: int = 1) -> Iterator[Frame]:
    packets = _packets()
    for seq in range(start, start + count):
        yield frame(
            seq,
            stream=RX,
            data=packets[seq % VARIANTS],
            mid=MID,
            time_ns=T0 + (seq - start) * STEP_NS,
        )


def write_target_recording(path: Path, count: int, options: RecorderOptions | None = None) -> Path:
    """A recording of ``count`` frames of the target's shape, its definitions embedded."""
    ccdd = target_ccdd()
    command, telemetry = COMMAND_PROFILE.read_bytes(), TELEMETRY_PROFILE.read_bytes()
    meta = meta_data(
        nodes={
            "fc": {
                "adapter": "json",
                "ccdd_sha256": hashlib.sha256(ccdd).hexdigest(),
                "command_profile_sha256": hashlib.sha256(command).hexdigest(),
                "telemetry_profile_sha256": hashlib.sha256(telemetry).hexdigest(),
            }
        }
    )
    blobs = [
        BlobSpec(BlobKind.CCDD, "fc", "json", ccdd),
        BlobSpec(BlobKind.COMMAND_PROFILE, "fc", "toml", command),
        BlobSpec(BlobKind.TELEMETRY_PROFILE, "fc", "toml", telemetry),
    ]
    recorder = Recorder.open(
        path,
        meta,
        blobs=blobs,
        streams=(RX, TX),
        options=options or RecorderOptions(min_free_bytes=0),
    )
    for item in target_frames(count):
        recorder.append(item)
    asyncio.run(recorder.close())
    return path
