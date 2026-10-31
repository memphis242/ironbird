"""The definitions a recording carries (decision D1) and the codecs built from them.

A recording embeds, for each node it decodes with, the CCDD file and the two header profiles as
they were when it was captured (BLOB records at the head of the file). They are as untrusted as
the rest of the file: a BLOB is read back through the same strict loaders as a file on disk
(:func:`~ironbird.ccdd.loader.load_ccdd_bytes`, :func:`~ironbird.header.load_profile_bytes`),
labelled ``FILE#ccdd/<node>`` in every error, with the recording's own ``max_packet_bytes``. A
definition that cannot be used is ``IB-REC-009`` whatever loader refused it, so a bad recording is
never a configuration error (exit 1, not 2), and the message says what to do instead.

Reading the head is bounded (records, bytes, nodes), a BLOB's content has to match the digest
META names for it, and nothing here raises anything but :class:`IronbirdError`.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

from ironbird.ccdd.loader import load_ccdd, load_ccdd_bytes
from ironbird.codec import Codec
from ironbird.config import SystemConfig
from ironbird.errors import Code, IronbirdError
from ironbird.frame import Stream
from ironbird.header import MAX_PROFILE_FILE_BYTES, load_profile, load_profile_bytes
from ironbird.recording import format as fmt
from ironbird.recording.format import BlobKind, RecordType
from ironbird.recording.reader import RecordingReader
from ironbird.sanitize import clean

MAX_HEAD_RECORDS: Final = 4096  # head records looked at before the definitions are taken as read
MAX_HEAD_BYTES: Final = 64 * 1024 * 1024
MAX_DEFINED_NODES: Final = 256
PROFILE_ADAPTER: Final = "toml"
_KIND_NAMES: Final = {
    BlobKind.CCDD: "ccdd",
    BlobKind.COMMAND_PROFILE: "command_profile",
    BlobKind.TELEMETRY_PROFILE: "telemetry_profile",
}
_ALL_KINDS: Final = tuple(BlobKind)

Blobs = Mapping[tuple[str, BlobKind], fmt.Blob]


@dataclass(frozen=True, slots=True)
class Head:
    """What the head of a recording declares: its streams and the definitions it embeds."""

    streams: Mapping[int, Stream]
    blobs: Blobs
    problem: str = ""  # why the definitions cannot be trusted as a set (empty: nothing wrong)


def label_of(path: Path) -> str:
    """The recording's name as errors show it."""
    return clean(str(path), 200)


def read_head(reader: RecordingReader) -> Head:
    """The streams and embedded definitions at the head of the file (the records before the first
    frame), without moving ``reader``. It does not raise for what the file contains: a repeated
    definition keeps the first and a contradiction, or more nodes than are allowed, is recorded
    in :attr:`Head.problem` for :func:`embedded_codecs` to refuse, so output that needs no
    definitions (hex) still works."""
    streams: dict[int, Stream] = {}
    blobs: dict[tuple[str, BlobKind], fmt.Blob] = {}
    nodes: set[str] = set()
    problem = ""
    seen_bytes = 0
    for count, item in enumerate(reader.head_records(reader.body_start, reader.size())):
        seen_bytes += item.end - item.offset
        if count >= MAX_HEAD_RECORDS or seen_bytes > MAX_HEAD_BYTES:
            # Streams and definitions past the limit are not known; say so rather than let
            # frames of those streams look like frames of nodes the recording never had.
            problem = problem or (
                f"its head holds more than {MAX_HEAD_RECORDS} records or "
                f"{MAX_HEAD_BYTES // (1024 * 1024)} MiB, past what is read for definitions"
            )
            break
        if item.rtype == RecordType.STREAM:
            streams.setdefault(item.value.id, item.value)
        elif item.rtype == RecordType.BLOB:
            blob: fmt.Blob = item.value
            key = (blob.node, blob.kind)
            known = blobs.get(key)
            if known is not None:
                if known.sha256 != blob.sha256 and not problem:
                    problem = (
                        f"it embeds two different {_KIND_NAMES[blob.kind]} definitions for "
                        f"node {clean(blob.node, 64)}"
                    )
            elif blob.node not in nodes and len(nodes) >= MAX_DEFINED_NODES:
                problem = (
                    problem or f"it embeds definitions for more than {MAX_DEFINED_NODES} nodes"
                )
            else:
                nodes.add(blob.node)
                blobs[key] = blob
    return Head(streams, blobs, problem)


def _meta_digest(meta: Mapping[str, Any], node: str, kind: BlobKind) -> str | None:
    """The digest META names for a node's definition, if it names one (it is untrusted JSON)."""
    nodes = meta.get("nodes")
    entry = nodes.get(node) if isinstance(nodes, dict) else None
    value = entry.get(f"{_KIND_NAMES[kind]}_sha256") if isinstance(entry, dict) else None
    return value.lower() if isinstance(value, str) else None


REPLAY_HINT: Final = (
    "replay with --defs config to use the current configuration, or --format hex to see the "
    "frames undecoded"
)
EXPORT_HINT: Final = "export with --defs config to use the current configuration"


def _unusable(label: str, why: str, hint: str) -> IronbirdError:
    return IronbirdError(Code.REC_DEFS, f"{label}: {why}; {hint}")


def embedded_codecs(
    path: Path,
    head: Head,
    nodes: Sequence[str],
    meta: Mapping[str, Any],
    max_packet_bytes: int,
    hint: str = REPLAY_HINT,
) -> dict[str, Codec]:
    """A codec for each of ``nodes`` from the definitions the recording embeds.

    Raises ``IB-REC-009`` for a node with a definition missing (recorded with
    ``embed_definitions = false``), a digest that disagrees with META, an unknown adapter, or
    anything the strict loaders refuse (malformed JSON or TOML, duplicate keys, oversize,
    a profile of the wrong kind, a message that does not fit its header or the cap). ``hint``
    is what the error tells the user to do instead."""
    label = label_of(path)
    if head.problem:
        raise _unusable(label, head.problem, hint)
    codecs: dict[str, Codec] = {}
    for node in dict.fromkeys(nodes):
        found = {kind: head.blobs.get((node, kind)) for kind in _ALL_KINDS}
        missing = [_KIND_NAMES[kind] for kind, blob in found.items() if blob is None]
        if missing:
            raise _unusable(
                label,
                f"it embeds no {', '.join(missing)} for node {clean(node, 64)} "
                "(recorded with embed_definitions = false, or by a node without them)",
                hint,
            )
        for kind, blob in found.items():
            assert blob is not None
            named = _meta_digest(meta, node, kind)
            if named is not None and named != blob.sha256.hex():
                raise _unusable(
                    label,
                    f"the embedded {_KIND_NAMES[kind]} of node {clean(node, 64)} does not match "
                    "the digest the recording's META names for it",
                    hint,
                )
        ccdd, command, telemetry = (found[kind] for kind in _ALL_KINDS)
        assert ccdd is not None and command is not None and telemetry is not None
        try:
            for blob in (command, telemetry):
                if blob.adapter != PROFILE_ADAPTER:
                    raise IronbirdError(
                        Code.HDR_FILE,
                        f"{label}#{_KIND_NAMES[blob.kind]}/{node}: unknown header profile "
                        f"format {clean(blob.adapter, 32)!r}",
                    )
            model = load_ccdd_bytes(ccdd.content, f"{label}#ccdd/{node}", ccdd.adapter)
            codecs[node] = Codec(
                model,
                load_profile_bytes(command.content, f"{label}#command_profile/{node}"),
                load_profile_bytes(telemetry.content, f"{label}#telemetry_profile/{node}"),
                max_packet_bytes=max_packet_bytes,
            )
        except IronbirdError as exc:
            raise IronbirdError(
                Code.REC_DEFS,
                f"an embedded definition cannot be used: {exc.summary}; {hint}",
                details=exc.detail_text(),
                cause=exc,
            ) from exc
    return codecs


def config_codecs(
    config: SystemConfig, nodes: Sequence[str], max_packet_bytes: int, command: str = "replay"
) -> dict[str, Codec]:
    """A codec for each of ``nodes`` from the current configuration's definition files, with the
    recording's packet cap (the frames were cut at it, not at the configuration's). ``command``
    names the command whose ``--defs`` the error advice is about."""
    codecs: dict[str, Codec] = {}
    for name in dict.fromkeys(nodes):
        node = config.nodes.get(name)
        if node is None:
            raise IronbirdError(
                Code.REC_DEFS,
                f"the recording has frames of node {clean(name, 64)}, which {config.path} does "
                f"not define; {command} without --defs config to use the recording's own",
            )
        if node.ccdd is None or node.command_header is None or node.telemetry_header is None:
            raise IronbirdError(
                Code.REC_DEFS,
                f"node {clean(name, 64)} has no ccdd/command_header/telemetry_header in "
                f"{config.path}; {command} without --defs config to use the recording's own",
            )
        try:
            codecs[name] = Codec(
                load_ccdd(node.ccdd, node.ccdd_adapter),
                load_profile(node.command_header),
                load_profile(node.telemetry_header),
                max_packet_bytes=max_packet_bytes,
            )
        except IronbirdError as exc:
            if exc.code is not Code.ENC_FIT or "-byte cap" not in exc.summary:
                raise
            raise IronbirdError(
                exc.code,
                f"{exc.summary}; the {max_packet_bytes}-byte cap is the recording's, not the "
                f"configuration's; {command} without --defs config to use the recording's own "
                "definitions",
                details=exc.detail_text(),
                cause=exc,
            ) from exc
    return codecs


def _file_digest(path: Path, limit: int) -> str | None:
    try:
        with path.open("rb") as handle:
            raw = handle.read(limit + 1)
    except OSError:
        return None
    return hashlib.sha256(raw).hexdigest() if len(raw) <= limit else None


def differences_from_config(head: Head, config: SystemConfig) -> list[str]:
    """``node:kind`` of each embedded definition whose file in the current configuration has a
    different digest (a file that cannot be read is not a difference). Bounded: at most one
    read of each file the configuration names, each capped at the format's own limit."""
    differing: list[str] = []
    for (node_name, kind), blob in sorted(head.blobs.items(), key=lambda it: (it[0][0], it[0][1])):
        node = config.nodes.get(node_name)
        if node is None:
            continue
        path = {
            BlobKind.CCDD: node.ccdd,
            BlobKind.COMMAND_PROFILE: node.command_header,
            BlobKind.TELEMETRY_PROFILE: node.telemetry_header,
        }[kind]
        if path is None:
            continue
        limit = fmt.MAX_BLOB_CONTENT if kind is BlobKind.CCDD else MAX_PROFILE_FILE_BYTES
        digest = _file_digest(path, limit)
        if digest is not None and digest != blob.sha256.hex():
            differing.append(f"{node_name}:{_KIND_NAMES[kind]}")
    return differing
