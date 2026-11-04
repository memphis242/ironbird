"""The messages of the export worker pool (plan 2m): what the parent sends a worker (the
definitions it decodes with, then jobs) and what a worker sends back (outcomes).

These are plain data. They travel only in the fixed binary framing of
:mod:`ironbird.recording.poolframe`; nothing here is ever serialized any other way. The parent
trusts none of an outcome until :func:`~ironbird.recording.poolframe.check_reply` and the walk of
:mod:`ironbird.recording.pexport` have checked it.
"""

from __future__ import annotations

import array
import hashlib
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ironbird.codec import Codec
from ironbird.config import load_config
from ironbird.frame import Stream
from ironbird.fsops import read_capped
from ironbird.header import MAX_PROFILE_FILE_BYTES
from ironbird.recording import format as fmt
from ironbird.recording.defs import Blobs, Head, config_codecs, embedded_codecs
from ironbird.recording.export import ExportFormat, ExportResult
from ironbird.recording.reader import Damage

if TYPE_CHECKING:
    from ironbird.recording.replay import Replay


# -- definitions, rebuilt in each worker ------------------------------------------------------


def _plain(value: object) -> object:
    """``value`` (read-only mappings and sequences from JSON) as plain dicts and lists."""
    if isinstance(value, Mapping):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_plain(v) for v in value]
    return value


@dataclass(frozen=True, slots=True)
class EmbeddedDefinitions:
    """The codecs of ``--defs recording``: built again from the definitions the recording embeds
    (their bytes travel, so every worker builds from what the parent built from). Two recordings
    that embed the same definitions have equal recipes."""

    path: Path = field(compare=False)  # names the recording in an error
    blobs: Blobs
    problem: str
    nodes: tuple[str, ...]
    meta: dict[str, Any]
    max_packet_bytes: int
    hint: str

    @staticmethod
    def of(replay: Replay, hint: str) -> EmbeddedDefinitions:
        meta = _plain(replay.meta.data)
        assert isinstance(meta, dict)
        head = replay.head
        return EmbeddedDefinitions(
            replay.parts[0],
            dict(head.blobs),
            head.problem,
            tuple(replay.nodes),
            meta,
            replay.max_packet_bytes,
            hint,
        )

    def build(self) -> dict[str, Codec]:
        head = Head({}, self.blobs, self.problem)
        return embedded_codecs(
            self.path, head, self.nodes, self.meta, self.max_packet_bytes, self.hint
        )


def config_fingerprint(config_nodes: Mapping[str, Any], nodes: tuple[str, ...]) -> str:
    """A digest of the definition files the configuration names for ``nodes``, to tell a worker
    that the files are not what the parent decoded with."""
    digest = hashlib.sha256()
    for name in sorted(set(nodes)):
        digest.update(name.encode("utf-8", "surrogateescape") + b"\0")
        node = config_nodes.get(name)
        if node is None:
            continue
        for path, limit in (
            (node.ccdd, fmt.MAX_BLOB_CONTENT),
            (node.command_header, MAX_PROFILE_FILE_BYTES),
            (node.telemetry_header, MAX_PROFILE_FILE_BYTES),
        ):
            if path is None:
                digest.update(b"-\0")
                continue
            try:
                content = read_capped(path, limit)
            except OSError as exc:
                digest.update(f"unreadable:{exc.errno}\0".encode())
                continue
            digest.update(len(content).to_bytes(8, "little") + hashlib.sha256(content).digest())
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class ConfigDefinitions:
    """The codecs of ``--defs config``: built again from the configuration's files. A worker
    refuses to decode if those files are not what the parent saw (``fingerprint``)."""

    config_path: Path
    config_sha256: str | None
    nodes: tuple[str, ...]
    max_packet_bytes: int
    command: str
    fingerprint: str

    @staticmethod
    def of(
        config_path: Path,
        config_sha256: str | None,
        config_nodes: Mapping[str, Any],
        nodes: tuple[str, ...],
        max_packet_bytes: int,
        command: str,
    ) -> ConfigDefinitions:
        return ConfigDefinitions(
            config_path,
            config_sha256,
            nodes,
            max_packet_bytes,
            command,
            config_fingerprint(config_nodes, nodes),
        )

    def build(self) -> dict[str, Codec]:
        config = load_config(self.config_path)
        if self.config_sha256 is not None and config.sha256 != self.config_sha256:
            raise DefinitionsChanged(f"{self.config_path} changed while the export ran")
        before = config_fingerprint(config.nodes, self.nodes)
        codecs = config_codecs(config, self.nodes, self.max_packet_bytes, self.command)
        if before != self.fingerprint or config_fingerprint(config.nodes, self.nodes) != before:
            raise DefinitionsChanged(
                "a definition file named by the configuration changed during the export"
            )
        return codecs


class DefinitionsChanged(Exception):
    """The definitions on disk are not the ones the parent decoded with."""


Definitions = EmbeddedDefinitions | ConfigDefinitions


# -- what the parent sends and gets back ------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Shape:
    """What an export writes: the format and options, the filter as text (a compiled filter does
    not travel) and the messages asked for. It travels with each range, so a pool built for some
    definitions serves any export that decodes with them."""

    fmt: ExportFormat
    where: str | None
    links: frozenset[str]
    aliases: tuple[tuple[str, str], ...] | None
    messages: tuple[str, ...]
    header_fields: bool
    raw_enums: bool
    directory: bool
    skip_damage: bool
    categories: bool = False  # the output takes rows one at a time and may refuse one


@dataclass(frozen=True, slots=True)
class Seed:
    """The state a range begins with: the streams and sources defined before it, the last
    sequence number, the work charged to the resync budget."""

    streams: dict[int, Stream]
    sources: dict[int, str]
    last_seq: int
    work: int


@dataclass(frozen=True, slots=True)
class Job:
    """A range: records from the first at or after ``guess`` (``exact``: the record at ``guess``)
    until the first at or after ``stop`` (``None``: the end of the file)."""

    index: int
    path: Path
    guess: int
    stop: int | None
    exact: bool
    seed: Seed
    start_ns: int | None
    end_ns: int | None
    shape: Shape
    identity: tuple[int, int]  # (device, inode) of the file the parent has open
    text_cap: int  # characters of rows after which the range is cut short
    event_cap: int  # damage, gap and definition notes after which the range is cut short


@dataclass(frozen=True, slots=True)
class Block:
    """The rows of one table from one range: ``layout`` identifies the table by its index among
    the export's layouts (:class:`~ironbird.recording.poolframe.Layouts`; ``None``: the malformed
    table, or the only stream of text and JSON Lines), ``text`` has no header row."""

    layout: int | None
    malformed: bool
    rows: int
    text: str
    handle: int = 0  # the table in the worker that made it (what ``Outcome.order`` names)


@dataclass(slots=True)
class Outcome:
    """What a worker made of a :class:`Job`. Plain data; the parent trusts none of it until it has
    been checked against the job and the walk."""

    index: int
    start: int | None = None  # where the first record was read (None: no record boundary found)
    end: int = 0  # where reading ended
    reached_stop: bool = False
    first_seq: int | None = None
    last_seq: int = 0
    definitions: list[tuple[Any, ...]] = field(default_factory=list)
    used_streams: list[int] = field(default_factory=list)  # ids the range found in its seed
    used_sources: list[int] = field(default_factory=list)
    work: int = 0  # bytes charged to the resync budget by the range
    # bytes the range's resync scans read, the search for its first record included
    scanned: int = 0
    trailer: bool = False
    seen: bool = False  # the parent has learned from its size (the parent's own; never sent)
    partial: bool = False  # cut short at a cap: the range goes on from ``end``
    moved: bool = False  # the name does not lead to the file the parent has open: nothing was read
    damage: list[Damage] = field(default_factory=list)
    damage_rows: list[int] = field(default_factory=list)  # rows made before each damage
    failure: tuple[str, str, str] | None = None  # (code, summary, details) the reader raised
    crash: str | None = None  # a worker fault, as text
    frames_read: int = 0
    frames_in_range: int = 0
    gap_marks: list[tuple[int, int, int]] = field(default_factory=list)
    """``(rows made before, gaps, frames lost)``: the gap records in range, run together when no
    row came between them."""
    counts: ExportResult = field(default_factory=ExportResult)
    layouts: int = 0
    """The digest of the layouts the blocks are numbered by (zero when none is numbered)."""
    blocks: list[Block] = field(default_factory=list)
    categories: bytes | None = None
    """The kind of each row, in the order they were made (``NO_LINE`` set: no line); ``None``
    unless the parent writes row by row or the range holds damage or gaps."""
    order: array.array[int] | None = None
    """With ``categories`` and more than one block: the :attr:`Block.handle` each line belongs to,
    line by line in the order they were made."""

    @property
    def clean(self) -> bool:
        return self.crash is None and self.failure is None and not self.damage and not self.moved


@dataclass(frozen=True, slots=True)
class Fault:
    """A worker's answer to a message it could not read: why. The pool is then of no use."""

    reason: str


__all__ = [
    "Block",
    "ConfigDefinitions",
    "Definitions",
    "DefinitionsChanged",
    "EmbeddedDefinitions",
    "Fault",
    "Job",
    "Outcome",
    "Seed",
    "Shape",
    "config_fingerprint",
]
