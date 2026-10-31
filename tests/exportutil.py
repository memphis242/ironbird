"""Helpers for the export tests: a CCDD with every field type, recordings of frames built from raw
payload bytes, and a reader that turns an exported table back into rows of cells."""

from __future__ import annotations

import csv
import hashlib
import json
import math
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

from conftest import EXAMPLES
from ironbird.ccdd.loader import load_ccdd_bytes
from ironbird.ccdd.model import MessageKind
from ironbird.codec import Codec
from ironbird.frame import Frame
from ironbird.header import load_profile
from ironbird.recording.defs import embedded_codecs
from ironbird.recording.export import (
    Destination,
    ExportFormat,
    ExportResult,
    Selection,
    check_messages,
    check_single_layout,
    export,
    write_to_directory,
    write_to_file,
)
from ironbird.recording.format import BlobKind
from ironbird.recording.player import compile_where
from ironbird.recording.replay import Replay, TimeSpec
from ironbird.recording.writer import BlobSpec
from recutil import MAX_PACKET, RX, frame
from replayutil import STEP_NS, T0, meta_data, write_recording

COMMAND_PROFILE = EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml"
TELEMETRY_PROFILE = EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml"

WIDE_MID = 0x0893
EVENT_MID = 0x0894
WIDE_CCDD: dict[str, Any] = {
    "schema": "ironbird-ccdd/0",
    "target": "wide",
    "messages": [
        {
            "name": "WIDE",
            "kind": "telemetry",
            "mid": hex(WIDE_MID),
            "category": "other",
            "description": "one field of every type",
            "fields": [
                {"name": "i8", "type": "int8"},
                {"name": "u16", "type": "uint16"},
                {"name": "i32", "type": "int32", "endian": "big"},
                {"name": "u64", "type": "uint64"},
                {"name": "i64", "type": "int64"},
                {"name": "f32", "type": "float32"},
                {"name": "f64", "type": "float64"},
                {"name": "mode", "type": "uint8", "enum": {"OFF": 0, "ON": 1, "AUTO": 2}},
                {"name": "name", "type": "char[8]"},
                {"name": "blob", "type": "bytes[4]"},
                {"name": "seq", "type": "uint8"},  # clashes with the fixed column
            ],
        },
        {
            "name": "EVT",
            "kind": "telemetry",
            "mid": hex(EVENT_MID),
            "category": "event",
            "event": {"app": "app", "id": "event_id", "type": "event_type", "message": "text"},
            "description": "an event",
            "fields": [
                {"name": "app", "type": "char[8]"},
                {"name": "event_id", "type": "uint16"},
                {
                    "name": "event_type",
                    "type": "uint16",
                    "enum": {"DEBUG": 1, "INFORMATION": 2, "ERROR": 3, "CRITICAL": 4},
                },
                {"name": "text", "type": "char[32]"},
            ],
        },
    ],
}
WIDE_BYTES = json.dumps(WIDE_CCDD).encode()


def wide_codec() -> Codec:
    return Codec(
        load_ccdd_bytes(WIDE_BYTES, "wide.json", "json"),
        load_profile(COMMAND_PROFILE),
        load_profile(TELEMETRY_PROFILE),
        max_packet_bytes=MAX_PACKET,
        clock=lambda: 1_000_000.25,
    )


def payload_size(codec: Codec, name: str) -> int:
    return codec.model.by_name[name].payload_size


def packet(codec: Codec, name: str, payload: bytes) -> bytes:
    """A telemetry packet of message ``name`` whose payload bytes are exactly ``payload``."""
    msg = codec.model.by_name[name]
    assert len(payload) == msg.payload_size
    codec.reset_sequences()
    whole = codec.encode_telemetry(name, {}).data
    return whole[: len(whole) - msg.payload_size] + payload


def wide_frames(
    codec: Codec, payloads: Iterable[bytes], *, name: str = "WIDE", start: int = 1
) -> list[Frame]:
    mid = codec.model.by_name[name].mid
    return [
        frame(
            seq,
            stream=RX,
            data=packet(codec, name, payload),
            mid=mid,
            time_ns=T0 + (seq - start) * STEP_NS,
        )
        for seq, payload in enumerate(payloads, start=start)
    ]


def wide_blobs(node: str = "fc") -> list[BlobSpec]:
    return [
        BlobSpec(BlobKind.CCDD, node, "json", WIDE_BYTES),
        BlobSpec(BlobKind.COMMAND_PROFILE, node, "toml", COMMAND_PROFILE.read_bytes()),
        BlobSpec(BlobKind.TELEMETRY_PROFILE, node, "toml", TELEMETRY_PROFILE.read_bytes()),
    ]


def wide_meta(node: str = "fc") -> dict[str, Any]:
    return meta_data(
        nodes={
            node: {
                "adapter": "json",
                "ccdd_sha256": hashlib.sha256(WIDE_BYTES).hexdigest(),
                "command_profile_sha256": hashlib.sha256(COMMAND_PROFILE.read_bytes()).hexdigest(),
                "telemetry_profile_sha256": hashlib.sha256(
                    TELEMETRY_PROFILE.read_bytes()
                ).hexdigest(),
            }
        }
    )


def many_message_recording(path: Path, messages: int, rounds: int = 2) -> Path:
    """A recording of ``messages`` telemetry messages ``M000``..., ``rounds`` frames each,
    interleaved (so every table is written to again and again)."""
    import asyncio

    from ironbird.recording.writer import Recorder
    from recutil import FAST

    names = [f"M{i:03d}" for i in range(messages)]
    ccdd = json.dumps(
        {
            "schema": "ironbird-ccdd/0",
            "target": "many",
            "messages": [
                {
                    "name": name,
                    "kind": "telemetry",
                    "mid": hex(0x0900 + i),
                    "category": "other",
                    "description": "x",
                    "fields": [{"name": "v", "type": "uint16"}],
                }
                for i, name in enumerate(names)
            ],
        }
    ).encode()
    command, telemetry = COMMAND_PROFILE.read_bytes(), TELEMETRY_PROFILE.read_bytes()
    codec = Codec(
        load_ccdd_bytes(ccdd, "many.json", "json"),
        load_profile(COMMAND_PROFILE),
        load_profile(TELEMETRY_PROFILE),
        max_packet_bytes=MAX_PACKET,
        clock=lambda: 1_000_000.25,
    )
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
    from recutil import TX

    recorder = Recorder.open(path, meta, blobs=blobs, streams=(RX, TX), options=FAST)
    seq = 1
    for round_ in range(rounds):
        for i, name in enumerate(names):
            codec.reset_sequences()
            data = codec.encode_telemetry(name, {"v": round_}).data
            recorder.append(
                frame(seq, stream=RX, data=data, mid=0x0900 + i, time_ns=T0 + seq * STEP_NS)
            )
            seq += 1
    asyncio.run(recorder.close())
    return path


def two_node_recording(path: Path, per_node: int = 4) -> Path:
    """A recording whose nodes ``fc`` and ``gc`` both define telemetry ``HK`` (MID 0x890), with
    one field on ``fc`` and two on ``gc``: the same message in two table layouts. Frames
    alternate between the nodes."""
    import asyncio

    from ironbird.frame import Direction, Stream
    from ironbird.recording.writer import Recorder
    from recutil import FAST

    def ccdd(fields: list[dict[str, str]]) -> bytes:
        return json.dumps(
            {
                "schema": "ironbird-ccdd/0",
                "target": "t",
                "messages": [
                    {
                        "name": "HK",
                        "kind": "telemetry",
                        "mid": "0x0890",
                        "category": "other",
                        "description": "x",
                        "fields": fields,
                    }
                ],
            }
        ).encode()

    one = [{"name": "a", "type": "uint16"}]
    defs = {"fc": ccdd(one), "gc": ccdd([*one, {"name": "b", "type": "uint8"}])}
    command, telemetry = COMMAND_PROFILE.read_bytes(), TELEMETRY_PROFILE.read_bytes()
    nodes: dict[str, Any] = {}
    blobs: list[BlobSpec] = []
    codecs: dict[str, Codec] = {}
    streams: list[Stream] = []
    for index, (node, definition) in enumerate(defs.items()):
        nodes[node] = {
            "adapter": "json",
            "ccdd_sha256": hashlib.sha256(definition).hexdigest(),
            "command_profile_sha256": hashlib.sha256(command).hexdigest(),
            "telemetry_profile_sha256": hashlib.sha256(telemetry).hexdigest(),
        }
        blobs += [
            BlobSpec(BlobKind.CCDD, node, "json", definition),
            BlobSpec(BlobKind.COMMAND_PROFILE, node, "toml", command),
            BlobSpec(BlobKind.TELEMETRY_PROFILE, node, "toml", telemetry),
        ]
        codecs[node] = Codec(
            load_ccdd_bytes(definition, f"{node}.json", "json"),
            load_profile(COMMAND_PROFILE),
            load_profile(TELEMETRY_PROFILE),
            max_packet_bytes=MAX_PACKET,
            clock=lambda: 1_000_000.25,
        )
        streams.append(
            Stream(
                index,
                f"ground-{node}",
                node,
                MessageKind.TELEMETRY,
                Direction.RX,
                "udp",
                "udp 127.0.0.1:1",
            )
        )
    recorder = Recorder.open(
        path, meta_data(nodes=nodes), blobs=blobs, streams=streams, options=FAST
    )
    seq = 1
    for round_ in range(per_node):
        for index, node in enumerate(defs):
            data = codecs[node].encode_telemetry("HK", {"a": round_}).data
            recorder.append(
                frame(
                    seq,
                    stream=streams[index],
                    data=data,
                    mid=0x890,
                    time_ns=T0 + seq * STEP_NS,
                )
            )
            seq += 1
    asyncio.run(recorder.close())
    return path


def write_wide(path: Path, frames: Iterable[Frame]) -> None:
    """Record ``frames`` with the wide CCDD embedded as the definitions of node ``fc``."""
    import asyncio

    from ironbird.recording.writer import Recorder
    from recutil import FAST, TX

    recorder = Recorder.open(path, wide_meta(), blobs=wide_blobs(), streams=(RX, TX), options=FAST)
    for item in frames:
        recorder.append(item)
    asyncio.run(recorder.close())


class NullDestination:
    """A destination that keeps nothing (throughput and invariants, not content)."""

    def __init__(self) -> None:
        self.lines = 0
        self.members: list[str] = []

    def member(self, name: str) -> int:
        self.members.append(name)
        return len(self.members) - 1

    def write(self, handle: int, text: str) -> bool:
        self.lines += 1
        return True

    def flush(self) -> None:
        pass


def export_in_process(
    paths: Path | Sequence[Path],
    output: Path | None,
    fmt: ExportFormat = ExportFormat.CSV,
    *,
    where: str | None = None,
    messages: Sequence[str] = (),
    header_fields: bool = False,
    raw_enums: bool = False,
    force: bool = False,
    skip_damage: bool = False,
    start: TimeSpec | None = None,
    end: TimeSpec | None = None,
    codecs: dict[str, Codec] | None = None,
    destination: Destination | None = None,
) -> tuple[ExportResult, Replay]:
    """Do what ``ib export`` does (open, definitions, filter, selection, write) and return the
    result. ``output`` ``None`` with a ``destination`` writes there instead of a file."""
    files = [paths] if isinstance(paths, Path) else list(paths)
    replaying = Replay(files, skip_damage=skip_damage, start=start, end=end)
    try:
        if codecs is None:
            codecs = embedded_codecs(
                replaying.parts[0],
                replaying.head,
                replaying.nodes,
                replaying.meta.data,
                replaying.max_packet_bytes,
            )
        flt = compile_where(replaying, where, codecs) if where is not None else None
        selection = None
        if messages:
            check_messages(messages, codecs)
            selection = Selection(messages, codecs)
        mids = [m for m in (flt.mids if flt else None, selection.mids if selection else None) if m]
        if mids:
            replaying.select_mids(frozenset.intersection(*mids))
        as_csv = fmt is ExportFormat.CSV
        single = as_csv and len(set(messages)) == 1
        directory = as_csv and not single
        if single:
            check_single_layout(
                messages[0],
                codecs,
                replaying.nodes,
                header_fields=header_fields,
                selected=flt.nodes if flt else None,
            )

        def run(dest: Destination) -> ExportResult:
            return export(
                replaying,
                codecs or {},
                dest,
                fmt,
                where=flt,
                selection=selection,
                header_fields=header_fields,
                raw_enums=raw_enums,
                single=single,
                directory=directory,
            )

        if destination is not None:
            return run(destination), replaying
        assert output is not None
        if directory:
            return write_to_directory(output, run=run)[0], replaying
        return write_to_file(output, force=force, run=run)[0], replaying
    finally:
        replaying.close()


LEADS = frozenset("=+-@\t\r\n\uff1d\uff0b\uff0d\uff20")  # ASCII and fullwidth = + - @, tab, CR, LF


def inert(cell: str) -> bool:
    """Whether ``cell`` is safe to open in a spreadsheet: after any leading whitespace (which an
    importer may trim) it does not lead like a formula, unless it is a plain finite number
    (numbers are typed cells and may be negative)."""
    lead = cell.lstrip()[:1]
    if not lead or lead not in LEADS:
        return True
    try:
        return math.isfinite(float(cell)) and cell[0] == "-"
    except ValueError:
        return False


def read_table(path: Path) -> tuple[list[str], list[list[str]]]:
    """The header and the rows of a CSV file, parsed by the standard reader."""
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.reader(handle))
    assert rows, f"{path} is empty"
    return rows[0], rows[1:]


def read_dicts(path: Path) -> list[dict[str, str]]:
    header, rows = read_table(path)
    assert len(set(header)) == len(header), f"duplicate columns in {header}"
    return [dict(zip(header, row, strict=True)) for row in rows]


def kinds(codec: Codec) -> Sequence[MessageKind]:
    return [m.kind for m in codec.model.messages]


__all__ = [
    "EVENT_MID",
    "WIDE_MID",
    "NullDestination",
    "export_in_process",
    "hk_and_status",
    "inert",
    "many_message_recording",
    "packet",
    "payload_size",
    "read_dicts",
    "read_table",
    "two_node_recording",
    "wide_codec",
    "wide_frames",
    "write_recording",
    "write_wide",
]


def hk_and_status(path: Path, hk_count: int = 5, status_count: int = 3) -> Path:
    """A recording of mock HK and STATUS frames (the mock definitions embedded)."""
    from replayutil import codec_status_frames, hk_frames

    write_recording(path, hk_frames(hk_count) + codec_status_frames(status_count, hk_count + 1))
    return path
