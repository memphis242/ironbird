"""Recording a live session: where the file goes, what it embeds, and its lifecycle.

:func:`recording_for` is what ``ib record`` and ``--record`` use: it declares the streams of the
routes being captured, embeds the definitions the nodes were configured with (decision D1),
creates the file, attaches the recorder as a session sink and, on the way out, detaches it and
writes the closing records, whether the command ended normally, by ``--duration`` or on a
signal.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import os
import pwd
import re
import socket
import sys
import time
from collections.abc import AsyncGenerator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

import ironbird
from ironbird.config import RecordingConfig
from ironbird.errors import Code, IronbirdError
from ironbird.frame import Stream
from ironbird.recording import format as fmt
from ironbird.recording.format import BlobKind, MarkKind
from ironbird.recording.writer import (
    MIN_SPLIT_BYTES,
    MIN_SPLIT_SECONDS,
    BlobSpec,
    Recorder,
    RecorderOptions,
)
from ironbird.sanitize import clean
from ironbird.session import Route, Session

MAX_PROFILE_BYTES: Final = 64 * 1024
MAX_ARGV: Final = 64
_SPLIT_RE: Final = re.compile(r"([0-9]{1,12}(?:\.[0-9]{1,6})?)([a-z]{1,3})")
_SIZE_UNITS: Final = {"b": 1, "kib": 1024, "mib": 1024**2, "gib": 1024**3, "tib": 1024**4}
_TIME_UNITS: Final = {"s": 1.0, "m": 60.0, "h": 3600.0}


@dataclass(frozen=True, slots=True)
class RecordRequest:
    """What the command line asked for: a path (``None``: the default location and name), a
    split, and the command that asked (for META)."""

    path: Path | None = None
    split_bytes: int | None = None
    split_seconds: float | None = None
    command: str = "record"


def parse_split(text: str) -> tuple[int | None, float | None]:
    """``--split`` as ``(bytes, seconds)``: a size (``512MiB``, ``1GiB``) or a time (``90s``,
    ``30m``, ``1h``). Raises ``IB-CLI-001`` for anything else."""
    found = _SPLIT_RE.fullmatch(text.strip().lower())
    number = float(found.group(1)) if found else -1.0
    unit = found.group(2) if found else ""
    if number > 0 and unit in _SIZE_UNITS:
        size = int(number * _SIZE_UNITS[unit])
        if size < MIN_SPLIT_BYTES:
            raise IronbirdError(Code.CLI_USAGE, "--split size must be at least 1 MiB")
        return size, None
    if number > 0 and unit in _TIME_UNITS:
        seconds = number * _TIME_UNITS[unit]
        if seconds < MIN_SPLIT_SECONDS:
            raise IronbirdError(Code.CLI_USAGE, "--split time must be at least 10 s")
        return None, seconds
    raise IronbirdError(
        Code.CLI_USAGE,
        f"--split {clean(text, 40)!r} must be a size (512MiB, 1GiB) or a time (90s, 30m, 1h)",
    )


def default_recording_dir(
    config: RecordingConfig, environ: Mapping[str, str] | None = None
) -> Path:
    """``[recording] dir``, else ``$IRONBIRD_RECORD_DIR``, else
    ``$XDG_DATA_HOME/ironbird/recordings``, else ``~/.local/share/ironbird/recordings``."""
    env = os.environ if environ is None else environ
    if config.dir is not None:
        return config.dir
    override = env.get("IRONBIRD_RECORD_DIR")
    if override:
        if not os.path.isabs(override):
            raise IronbirdError(
                Code.REC_CREATE,
                "IRONBIRD_RECORD_DIR is not an absolute path; set it to an absolute, writable "
                "directory",
            )
        return Path(override)
    xdg = env.get("XDG_DATA_HOME")
    if xdg and os.path.isabs(xdg):
        return Path(xdg) / "ironbird" / "recordings"
    home = env.get("HOME")
    if home is None:
        try:
            home = pwd.getpwuid(os.getuid()).pw_dir
        except KeyError:
            home = ""
    if not home or not os.path.isabs(home):
        raise IronbirdError(
            Code.REC_CREATE,
            "cannot locate a recording directory (HOME is unusable); pass -o PATH or set "
            "IRONBIRD_RECORD_DIR to an absolute, writable directory",
        )
    return Path(home) / ".local" / "share" / "ironbird" / "recordings"


def recording_name(session_id: str, time_ns: int) -> str:
    """``<UTC stamp>-<session>.ibrec``, the name retention recognises."""
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(time_ns // 1_000_000_000))
    return f"{stamp}-{session_id}.ibrec"


def resolve_path(
    request: RecordRequest, config: RecordingConfig, session_id: str, time_ns: int
) -> Path:
    """The file to create: the request's path (an existing directory gets the default name
    inside it), or the default directory and name."""
    name = recording_name(session_id, time_ns)
    if request.path is None:
        return default_recording_dir(config) / name
    path = Path(os.path.abspath(request.path))
    return path / name if path.is_dir() else path


def _read_definition(path: Path, limit: int, what: str) -> bytes:
    try:
        with path.open("rb") as handle:
            raw = handle.read(limit + 1)
    except OSError as exc:
        raise IronbirdError(
            Code.REC_CREATE,
            f"cannot read the {what} {path} to embed it: {exc.strerror or exc}; set "
            "recording.embed_definitions = false to record without definitions",
            cause=exc,
        ) from exc
    if len(raw) > limit:
        raise IronbirdError(
            Code.REC_CREATE,
            f"the {what} {path} is larger than {limit // 1024} KiB and cannot be embedded; set "
            "recording.embed_definitions = false to record without definitions",
        )
    return raw


def collect_definitions(
    session: Session, nodes: Sequence[str], *, embed: bool
) -> tuple[list[BlobSpec], dict[str, Any]]:
    """The definition files of ``nodes``: blobs to embed (none when ``embed`` is false) and the
    per-node table for META with the digest of each file, so a recording names exactly what it
    was captured with even when it does not carry it."""
    blobs: list[BlobSpec] = []
    table: dict[str, Any] = {}
    for name in nodes:
        node = session.config.nodes[name]
        entry: dict[str, Any] = {"adapter": node.ccdd_adapter}
        files = (
            (BlobKind.CCDD, node.ccdd, node.ccdd_adapter, fmt.MAX_BLOB_CONTENT, "CCDD file"),
            (
                BlobKind.COMMAND_PROFILE,
                node.command_header,
                "toml",
                MAX_PROFILE_BYTES,
                "command header profile",
            ),
            (
                BlobKind.TELEMETRY_PROFILE,
                node.telemetry_header,
                "toml",
                MAX_PROFILE_BYTES,
                "telemetry header profile",
            ),
        )
        for kind, path, adapter, limit, what in files:
            if path is None:
                continue
            content = _read_definition(path, limit, what)
            entry[f"{kind.name.lower()}_sha256"] = hashlib.sha256(content).hexdigest()
            if embed:
                blobs.append(BlobSpec(kind, name, adapter, content))
        table[name] = entry
    return blobs, table


def _user() -> str:
    try:
        return pwd.getpwuid(os.getuid()).pw_name
    except KeyError:
        return str(os.getuid())


def build_meta(
    session: Session, command: str, nodes: Mapping[str, Any], created_ns: int
) -> dict[str, Any]:
    """The session facts META records (the writer adds the creation time and the part)."""
    config_hash: str | None = None
    with contextlib.suppress(OSError):
        config_hash = hashlib.sha256(session.config.path.read_bytes()).hexdigest()
    return {
        "schema": fmt.META_SCHEMA,
        "session": session.ledger.session_id,
        "version": ironbird.__version__,
        "host": socket.gethostname(),
        "user": _user(),
        "pid": os.getpid(),
        "command": command,
        "argv": [clean(arg, 256) for arg in sys.argv[1 : MAX_ARGV + 1]],
        "acts_as": session.node.name,
        "config": {"path": str(session.config.path), "sha256": config_hash},
        "max_packet_bytes": session.config.max_packet_bytes,
        "nodes": dict(nodes),
        "created_ns": created_ns,
    }


async def start_recording(
    session: Session,
    request: RecordRequest,
    *,
    nodes: Sequence[str],
    routes: Sequence[Route] = (),
) -> Recorder:
    """Create the recording for a capture and attach it to ``session``.

    ``nodes`` are the nodes whose definitions decode what is captured (embedded unless
    ``recording.embed_definitions`` is false). ``routes``, when known before the capture opens,
    declare their streams up front; any other stream is defined when its first frame arrives.
    """
    config = session.config.recording
    created = time.time_ns()
    path = resolve_path(request, config, session.ledger.session_id, created)
    streams: tuple[Stream, ...] = session.streams_for(routes)
    blobs, node_table = collect_definitions(
        session, sorted(set(nodes)), embed=config.embed_definitions
    )
    options = RecorderOptions(
        fsync=config.fsync,
        max_buffer_bytes=config.max_buffer_bytes,
        min_free_bytes=config.min_free_bytes,
        split_bytes=request.split_bytes,
        split_seconds=request.split_seconds,
    )
    recorder = Recorder.open(
        path,
        build_meta(session, request.command, node_table, created),
        blobs=blobs,
        streams=streams,
        options=options,
        report=session.report,
        ledger=session.ledger,
        loop=asyncio.get_running_loop(),
    )
    session.add_sink(recorder)
    recorder.mark(MarkKind.START, f"{request.command} started")
    session.ledger.record(
        "action",
        "recording.start",
        path=path,
        streams=[stream.endpoint for stream in streams if stream.direction.value == "rx"],
        embedded=[f"{blob.node}:{blob.kind.name.lower()}" for blob in blobs],
        fsync=config.fsync,
        max_buffer_bytes=config.max_buffer_bytes,
        split_bytes=request.split_bytes,
        split_seconds=request.split_seconds,
    )
    return recorder


@contextlib.asynccontextmanager
async def recording_for(
    session: Session,
    request: RecordRequest,
    *,
    nodes: Sequence[str],
    routes: Sequence[Route] = (),
) -> AsyncGenerator[Recorder]:
    """Record for the duration of the block, then close the recording.

    The closing records are written in the exit path, so a signal that cancels the block still
    leaves a complete file (the close is bounded to 5 s by the recorder).
    """
    recorder = await start_recording(session, request, nodes=nodes, routes=routes)
    try:
        yield recorder
    finally:
        interrupted = isinstance(sys.exc_info()[1], asyncio.CancelledError)
        session.remove_sink(recorder)
        recorder.mark(
            MarkKind.SIGNAL if interrupted else MarkKind.STOP,
            "interrupted by a signal" if interrupted else f"{request.command} ended",
        )
        await recorder.close()
