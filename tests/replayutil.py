"""Helpers for the replay tests: recordings of real mock frames with their definitions embedded,
crafted ones with bad definitions, and an in-process driver that does what ``ib replay`` does."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import socket
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from conftest import EXAMPLES
from ironbird.ccdd.loader import load_ccdd
from ironbird.codec import Codec
from ironbird.errors import IronbirdError
from ironbird.frame import Frame
from ironbird.header import load_profile
from ironbird.recording import format as fmt
from ironbird.recording.defs import embedded_codecs
from ironbird.recording.format import BlobKind, RecordType
from ironbird.recording.player import OutputFormat, PlayResult, compile_where, play
from ironbird.recording.reader import Damage
from ironbird.recording.replay import Pacer, Replay, TimeSpec
from ironbird.recording.writer import BlobSpec, Recorder, RecorderOptions
from recutil import FAST, MAX_PACKET, RX, TX, Builder, frame

CCDD_PATH = EXAMPLES / "ccdd" / "mock.json"
COMMAND_PROFILE_PATH = EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml"
TELEMETRY_PROFILE_PATH = EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml"
T0 = 1_700_000_000_000_000_000
STEP_NS = 10_000_000  # 10 ms between frames


def mock_codec() -> Codec:
    return Codec(
        load_ccdd(CCDD_PATH),
        load_profile(COMMAND_PROFILE_PATH),
        load_profile(TELEMETRY_PROFILE_PATH),
        max_packet_bytes=MAX_PACKET,
        clock=lambda: 1_000_000.25,  # the same packet bytes in every run
    )


def definition_bytes() -> tuple[bytes, bytes, bytes]:
    return (
        CCDD_PATH.read_bytes(),
        COMMAND_PROFILE_PATH.read_bytes(),
        TELEMETRY_PROFILE_PATH.read_bytes(),
    )


def blob_specs(node: str = "fc") -> list[BlobSpec]:
    ccdd, command, telemetry = definition_bytes()
    return [
        BlobSpec(BlobKind.CCDD, node, "json", ccdd),
        BlobSpec(BlobKind.COMMAND_PROFILE, node, "toml", command),
        BlobSpec(BlobKind.TELEMETRY_PROFILE, node, "toml", telemetry),
    ]


def nodes_table(node: str = "fc") -> dict[str, Any]:
    """The per-node digest table a recorder puts in META."""
    ccdd, command, telemetry = definition_bytes()
    return {
        node: {
            "adapter": "json",
            "ccdd_sha256": hashlib.sha256(ccdd).hexdigest(),
            "command_profile_sha256": hashlib.sha256(command).hexdigest(),
            "telemetry_profile_sha256": hashlib.sha256(telemetry).hexdigest(),
        }
    }


def meta_data(**extra: Any) -> dict[str, Any]:
    return {
        "schema": fmt.META_SCHEMA,
        "session": "abcdef012345",
        "max_packet_bytes": MAX_PACKET,
        "nodes": nodes_table(),
        **extra,
    }


def hk(counter: int, codec: Codec | None = None) -> bytes:
    """A housekeeping packet carrying ``counter``; the same bytes every time it is asked for."""
    codec = codec or _CODEC
    codec.reset_sequences()
    return codec.encode_telemetry("MOCK_HK", {"cmd_counter": counter % 256}).data


_CODEC = mock_codec()


def hk_frames(
    count: int, *, start: int = 1, step_ns: int = STEP_NS, mid: int = 0x890
) -> list[Frame]:
    """``count`` housekeeping frames from the mock node, ``step_ns`` apart."""
    return [
        frame(
            seq,
            stream=RX,
            data=hk(seq),
            mid=mid,
            time_ns=T0 + (seq - start) * step_ns,
        )
        for seq in range(start, start + count)
    ]


def codec_status_frames(count: int, start: int) -> list[Frame]:
    """``count`` MOCK_STATUS frames from the mock node, numbered from ``start``."""
    codec = mock_codec()
    mid = codec.model.by_name["MOCK_STATUS"].mid
    data = codec.encode_telemetry("MOCK_STATUS", {}).data
    return [
        frame(seq, stream=RX, data=data, mid=mid, time_ns=T0 + seq * STEP_NS)
        for seq in range(start, start + count)
    ]


def last_frame_end(path: Path) -> int:
    """The offset just past the last frame record of the recording at ``path``."""
    from ironbird.recording.reader import Item, RecordingReader

    with RecordingReader(path) as reader:
        ends = [
            item.end
            for item in reader.items()
            if isinstance(item, Item) and item.rtype == RecordType.FRAME
        ]
    return ends[-1]


def write_recording(
    path: Path,
    frames: Iterable[Frame],
    *,
    blobs: bool = True,
    options: RecorderOptions = FAST,
    streams: Sequence[Any] = (RX, TX),
    meta_extra: dict[str, Any] | None = None,
    marks: Callable[[Recorder], None] | None = None,
) -> Recorder:
    """Write ``frames`` to ``path`` through a real recorder, with the mock definitions embedded."""
    meta = meta_data(**(meta_extra or {}))
    if not blobs:
        meta["nodes"] = {}
    recorder = Recorder.open(
        path, meta, blobs=blob_specs() if blobs else [], streams=streams, options=options
    )
    for item in frames:
        recorder.append(item)
    if marks is not None:
        marks(recorder)
    asyncio.run(recorder.close())
    return recorder


# -- crafting files with bad definitions --


def crafted(
    blobs: Iterable[tuple[BlobKind, str, str, bytes]],
    *,
    meta: dict[str, Any] | None = None,
    max_packet: int = MAX_PACKET,
    frames: int = 4,
    nonce: int = 0x1122334455667788,
    raw_blobs: Sequence[bytes] = (),
) -> Builder:
    """A file whose head embeds ``blobs`` (kind, node, adapter, content; each with a correct
    digest of its own content) and that holds ``frames`` valid mock frames."""
    builder = Builder(nonce, max_packet=max_packet)
    builder.out = bytearray(fmt.encode_preamble(nonce))
    builder.offsets = []
    data = meta_data(max_packet_bytes=max_packet) if meta is None else dict(meta)
    builder.add(RecordType.META, fmt.encode_meta(data))
    for kind, node, adapter, content in blobs:
        digest = hashlib.sha256(content).digest()
        builder.add(RecordType.BLOB, fmt.encode_blob(kind, node, adapter, digest, content))
    for body in raw_blobs:  # verbatim BLOB bodies, whatever they claim
        builder.add(RecordType.BLOB, body)
    builder.stream(RX)
    builder.stream(TX)
    builder.source(0, "peer")
    for seq in range(1, frames + 1):
        builder.frame(seq, data=hk(seq), stream=0, source=0, mid=0x890, time_ns=T0 + seq * STEP_NS)
    return builder


def good_blobs() -> list[tuple[BlobKind, str, str, bytes]]:
    ccdd, command, telemetry = definition_bytes()
    return [
        (BlobKind.CCDD, "fc", "json", ccdd),
        (BlobKind.COMMAND_PROFILE, "fc", "toml", command),
        (BlobKind.TELEMETRY_PROFILE, "fc", "toml", telemetry),
    ]


# -- driving a replay in process --


@dataclass(slots=True)
class Run:
    lines: list[str] = field(default_factory=list)
    result: PlayResult = field(default_factory=PlayResult)
    replay: Replay | None = None
    damage: list[Damage] = field(default_factory=list)
    notes: list[tuple[Any, str]] = field(default_factory=list)


def replay_in_process(
    paths: Sequence[Path] | Path,
    *,
    fmt_: OutputFormat = OutputFormat.LINE,
    where: str | None = None,
    skip_damage: bool = False,
    start: TimeSpec | None = None,
    end: TimeSpec | None = None,
    pacer: Pacer | None = None,
    count: int | None = None,
    follow: bool = False,
    follow_idle_s: float | None = None,
    codecs: dict[str, Codec] | None = None,
) -> Run:
    """Do what ``ib replay`` does (open, definitions, filter, play) and return what came out.
    Raises only what the command would turn into an error line."""
    run = Run()
    files = [paths] if isinstance(paths, Path) else list(paths)
    replaying = Replay(
        files,
        skip_damage=skip_damage,
        follow=follow,
        follow_idle_s=follow_idle_s,
        start=start,
        end=end,
        on_damage=lambda damage, _path: run.damage.append(damage),
        on_note=lambda code, text: run.notes.append((code, text)),
    )
    run.replay = replaying
    try:
        needed = fmt_ is not OutputFormat.HEX or where is not None
        degraded: IronbirdError | None = None
        if codecs is None:
            codecs = {}
            if needed:
                try:
                    codecs = embedded_codecs(
                        replaying.parts[0],
                        replaying.head,
                        replaying.nodes,
                        replaying.meta.data,
                        replaying.max_packet_bytes,
                    )
                except IronbirdError as exc:
                    if fmt_ is not OutputFormat.HEX:
                        raise
                    degraded = exc
        flt = (
            compile_where(replaying, where, codecs, degraded=degraded)
            if where is not None
            else None
        )

        def out(text: str) -> bool:
            run.lines.append(text)
            return True

        run.result = play(replaying, codecs, out=out, fmt=fmt_, where=flt, pacer=pacer, count=count)
    finally:
        replaying.close()
    return run


def records(lines: Iterable[str]) -> list[dict[str, Any]]:
    return [json.loads(line) for line in lines]


# -- ports 58000-58999 --


def ports_58k(count: int) -> list[int]:
    import random

    found: list[int] = []
    candidates = list(range(58000, 59000))
    random.shuffle(candidates)
    for port in candidates:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.bind(("127.0.0.1", port))
        except OSError:
            continue
        finally:
            sock.close()
        found.append(port)
        if len(found) == count:
            return found
    raise AssertionError("no free port in 58000-58999")


def write_config_58k(directory: Path, extra: str = "") -> tuple[Path, int, int]:
    """examples/mock.toml with absolute paths and ports in 58000-58999."""
    command_port, telemetry_port = ports_58k(2)
    text = (EXAMPLES / "mock.toml").read_text()
    text = text.replace('"ccdd/', f'"{EXAMPLES}/ccdd/').replace(
        '"headers/', f'"{EXAMPLES}/headers/'
    )
    text = text.replace("127.0.0.1:41234", f"127.0.0.1:{command_port}")
    text = text.replace("127.0.0.1:41235", f"127.0.0.1:{telemetry_port}")
    if "[recording]" not in extra:
        extra = "[recording]\nmin_free_bytes = 0\n" + extra
    path = directory / "mock.toml"
    path.write_text(text + "\n" + extra)
    return path, command_port, telemetry_port


def child_env(directory: Path) -> dict[str, str]:
    env = dict(os.environ)
    env["IRONBIRD_STATE_DIR"] = str(directory / "state")
    env["IRONBIRD_RECORD_DIR"] = str(directory / "recordings")
    env.pop("IRONBIRD_CONFIG", None)
    return env


def wait_for(predicate: Callable[[], bool], timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.02)


__all__ = [
    "T0",
    "IronbirdError",
    "blob_specs",
    "child_env",
    "codec_status_frames",
    "crafted",
    "definition_bytes",
    "good_blobs",
    "hk",
    "hk_frames",
    "last_frame_end",
    "meta_data",
    "mock_codec",
    "nodes_table",
    "records",
    "replay_in_process",
    "wait_for",
    "write_config_58k",
    "write_recording",
]
