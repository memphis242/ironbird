"""Recording a live session: where the file goes, what it embeds, and its lifecycle.

:func:`recording_for` is what ``ib record`` and ``--record`` use: it declares the streams of the
routes being captured, embeds the definitions the nodes were configured with (decision D1),
creates the file, attaches the recorder as a session sink and, on the way out, detaches it and
writes the closing records, whether the command ended normally, by ``--duration`` or on a
signal.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import pwd
import re
import socket
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

import ironbird
from ironbird.config import RecordingConfig
from ironbird.errors import Code, IronbirdError
from ironbird.frame import Frame, GapReason, Stream
from ironbird.recording import format as fmt
from ironbird.recording.format import BlobKind, MarkKind
from ironbird.recording.writer import (
    MIN_SPLIT_BYTES,
    MIN_SPLIT_SECONDS,
    BlobSpec,
    Recorder,
    RecorderOptions,
    RecorderStats,
)
from ironbird.sanitize import clean
from ironbird.session import Route, Session

MAX_ARGV: Final = 64
MAX_STAGED_EVENTS: Final = 1 << 16  # frames kept between the capture opening and the file
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
        table[name] = entry
        if node.ccdd is None or node.command_header is None or node.telemetry_header is None:
            continue
        held = session.definitions_of(name)  # the bytes the codec was built from, never re-read
        files = (
            (BlobKind.CCDD, held.ccdd, node.ccdd_adapter),
            (BlobKind.COMMAND_PROFILE, held.command_header, "toml"),
            (BlobKind.TELEMETRY_PROFILE, held.telemetry_header, "toml"),
        )
        for kind, content, adapter in files:
            assert len(content) <= fmt.MAX_BLOB_CONTENT, "the loaders refuse larger definitions"
            entry[f"{kind.name.lower()}_sha256"] = hashlib.sha256(content).hexdigest()
            if embed:
                blobs.append(BlobSpec(kind, name, adapter, content))
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
        "config": {"path": str(session.config.path), "sha256": session.config.sha256},
        "max_packet_bytes": session.config.max_packet_bytes,
        "nodes": dict(nodes),
        "created_ns": created_ns,
    }


class _Staging:
    """Holds what a capture ingests from the moment it is attached until the recording exists.

    The recording file is created only after the transport has opened (a port that is busy, or
    an argument that is wrong, leaves no file behind), yet no frame may be missed while the file
    is being made. This sink is attached before the capture opens; :meth:`replay` gives its frames
    and gaps to the recorder in order, and the recorder takes over in the same event-loop turn.
    It is bounded: past ``limit`` frames the rest are counted per stream and become GAP records
    (``recorder_overrun``) at the recorder.
    """

    def __init__(self, limit: int = MAX_STAGED_EVENTS) -> None:
        self.events: list[tuple[str, Any]] = []
        self._limit = limit
        self._lost: dict[int, list[Any]] = {}  # stream id -> [stream, count, first, last frames]

    def append(self, frame: Frame) -> None:
        if len(self.events) < self._limit:
            self.events.append(("frame", frame))
            return
        entry = self._lost.get(frame.stream.id)
        if entry is None:
            self._lost[frame.stream.id] = [frame.stream, 1, frame, frame]
        else:
            entry[1] += 1
            entry[3] = frame

    def gap(self, *args: Any) -> None:
        if len(self.events) < self._limit:
            self.events.append(("gap", args))

    def replay(self, recorder: Recorder) -> None:
        for kind, payload in self.events:
            if kind == "frame":
                recorder.append(payload)
            else:
                recorder.gap(*payload)
        for stream, count, first, last in self._lost.values():
            recorder.gap(
                stream,
                GapReason.RECORDER_OVERRUN,
                count,
                first.time_ns,
                last.time_ns,
                first.seq,
                last.seq,
            )
        self.events = []
        self._lost = {}


class PendingRecording:
    """A recording for one command: :meth:`begin` before the capture opens, :meth:`commit` right
    after it has, :meth:`finish` on the way out (normal end, ``--duration`` or a signal).

    ``begin`` creates nothing; it checks the target name and attaches a staging sink, so the
    frames of the first moments are kept. ``commit`` creates the file, embeds the definitions the
    nodes were configured with (decision D1), declares the streams of the captured routes, hands
    over the staged frames and attaches the recorder. A command that fails before ``commit``
    leaves no file.
    """

    def __init__(
        self,
        session: Session,
        request: RecordRequest,
        *,
        nodes: Sequence[str],
        routes: Sequence[Route] = (),
    ) -> None:
        self._session = session
        self._request = request
        self._nodes = sorted(set(nodes))
        self._routes = tuple(routes)
        self._staging: _Staging | None = None
        self._created_ns = 0
        self.path: Path | None = None
        self.recorder: Recorder | None = None

    def begin(self) -> None:
        """Choose the file name (refusing one that exists) and start staging frames."""
        session = self._session
        self._created_ns = time.time_ns()
        self.path = resolve_path(
            self._request, session.config.recording, session.ledger.session_id, self._created_ns
        )
        if os.path.lexists(self.path):
            raise IronbirdError(
                Code.REC_CREATE,
                f"{self.path} already exists; a recording is never overwritten (choose another -o)",
            )
        self._staging = _Staging()
        session.add_sink(self._staging)

    def commit(self) -> Recorder:
        """Create the recording and take over from the staging sink. Raises ``IB-REC-001``,
        ``IB-REC-002`` or ``IB-REC-010`` (the file, if it was created, is removed)."""
        assert self.path is not None and self._staging is not None and self.recorder is None
        session, request = self._session, self._request
        config = session.config.recording
        streams: tuple[Stream, ...] = session.streams_for(self._routes)
        blobs, node_table = collect_definitions(
            session, self._nodes, embed=config.embed_definitions
        )
        options = RecorderOptions(
            fsync=config.fsync,
            max_buffer_bytes=config.max_buffer_bytes,
            min_free_bytes=config.min_free_bytes,
            split_bytes=request.split_bytes,
            split_seconds=request.split_seconds,
        )
        recorder = Recorder.open(
            self.path,
            build_meta(session, request.command, node_table, self._created_ns),
            blobs=blobs,
            streams=streams,
            options=options,
            report=session.report,
            ledger=session.ledger,
            loop=asyncio.get_running_loop(),
            capture_continues=request.command != "record",
        )
        # No await from here to the end: no frame is ingested between the replay and the swap.
        self._staging.replay(recorder)
        session.add_sink(recorder)
        session.remove_sink(self._staging)
        self._staging = None
        self.recorder = recorder
        recorder.mark(MarkKind.START, f"{request.command} started")
        session.ledger.record(
            "action",
            "recording.start",
            path=self.path,
            streams=[stream.endpoint for stream in streams if stream.direction.value == "rx"],
            embedded=[f"{blob.node}:{blob.kind.name.lower()}" for blob in blobs],
            fsync=config.fsync,
            max_buffer_bytes=config.max_buffer_bytes,
            split_bytes=request.split_bytes,
            split_seconds=request.split_seconds,
        )
        return recorder

    async def finish(self, *, interrupted: bool = False) -> RecorderStats | None:
        """Detach and close what :meth:`begin` and :meth:`commit` set up. The closing records are
        written here, so a signal that cancelled the command still leaves a complete file (the
        close ends when the disk makes no progress for 5 s). ``None``: nothing was created."""
        session = self._session
        if self._staging is not None:
            session.remove_sink(self._staging)
            self._staging = None
            session.ledger.record(
                "state", "recording.not_started", path=self.path, command=self._request.command
            )
        recorder = self.recorder
        if recorder is None:
            return None
        session.remove_sink(recorder)
        command = self._request.command
        recorder.mark(
            MarkKind.SIGNAL if interrupted else MarkKind.STOP,
            "interrupted by a signal" if interrupted else f"{command} ended",
        )
        return await recorder.close()
