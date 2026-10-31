"""Export a recording to CSV, text or JSON Lines (plan 2h).

The frames come from a :class:`~ironbird.recording.replay.Replay` (ranges, chunk selection and
damage handling are replay's), are filtered and decoded exactly as ``ib replay`` does, and are
written as they arrive: one pass, memory bounded by a small write buffer, whatever the size of
the recording.

- **CSV** is one table per message, because the columns differ by message: ``rx_time_utc``,
  ``rx_time_ns``, ``seq`` (the recording's frame number), ``node``, ``link``, ``mid``, optionally
  the header fields (``hdr.<name>``), then the payload fields in CCDD order. Frames that did not
  decode go to ``malformed.csv`` in a directory export. Several messages (or none named) make a
  directory, one message a single file.
- **text** and **jsonl** are the lines and records ``ib replay`` prints (a frame that did not
  decode is shown flagged), one file or stdout.

Every string cell is untrusted (names, enum labels, hex and event text come from files and the
wire): :func:`safe_cell` escapes control characters and prefixes a cell that a spreadsheet would
read as a formula (``=``, ``+``, ``-``, ``@`` and their fullwidth forms, tab, carriage return,
behind any leading whitespace) with ``'``. Numbers are
typed and are never touched.

Nothing here opens a file by name: the destinations are given a
:class:`~ironbird.atomicfile.TempOutput` or :class:`~ironbird.atomicfile.TempDirOutput`, or the
stdout writer.
"""

from __future__ import annotations

import contextlib
import csv
import difflib
import errno
import json
import math
import os
import re
import resource
import time
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Final, Protocol

from ironbird import render
from ironbird.atomicfile import (
    Collision,
    LeftoverAfterCommit,
    Locked,
    SyncAfterCommit,
    TempDirOutput,
    TempOutput,
)
from ironbird.ccdd.model import FieldDef, Message, TypeClass
from ironbird.codec import Codec
from ironbird.engine import FrameDecoder
from ironbird.errors import Code, DecodeError, IronbirdError
from ironbird.filter import Filter
from ironbird.frame import Decoded, Frame, FrameFlag, Stream
from ironbird.fsops import REAL_FS, FileSystem
from ironbird.recording.player import Ending
from ironbird.recording.replay import Replay
from ironbird.retention import is_managed_name
from ironbird.sanitize import clean
from ironbird.sigmask import Hold, naming_replaced, signals_held

#: A text cell is never cut: the longest field is a ``char[]`` of up to 65535 characters, each
#: of which may escape to ten.
MAX_CELL_CHARS: Final = 1 << 20
#: What starts a formula in a spreadsheet: ``= + - @`` and their fullwidth forms (some importers
#: fold those), and tab, CR and LF.
FORMULA_LEAD: Final = frozenset("=+-@\t\r\n\uff1d\uff0b\uff0d\uff20")
FORMULA_PREFIX: Final = "'"
#: Hex cells lead with it so a spreadsheet keeps them as text (``00123456`` would lose its zeros
#: and ``1e300000`` would become a number); the digits themselves are always inert.
HEX_PREFIX: Final = "0x"
MALFORMED_STEM: Final = "malformed"
FILE_BUFFER_BYTES: Final = 256 * 1024  # a single destination flushes at this size
DIR_BUFFER_BYTES: Final = 4 * 1024 * 1024  # all members of a directory together
MAX_STREAM_CELLS: Final = 65536  # stream ids are u16
MAX_OPEN_MEMBERS: Final = 64  # most members of a directory export that hold a descriptor
DESCRIPTOR_MARGIN: Final = 48  # descriptors left for everything else (parts, ledger, ...)
FIXED_COLUMNS: Final = ("rx_time_utc", "rx_time_ns", "seq", "node", "link", "mid")
MALFORMED_COLUMNS: Final = (
    "rx_time_utc",
    "rx_time_ns",
    "seq",
    "node",
    "link",
    "dir",
    "mid",
    "status",
    "code",
    "offset",
    "reason",
    "len",
    "orig_len",
    "flags",
    "hex",
)
assert len(set(FIXED_COLUMNS)) == len(FIXED_COLUMNS)
assert len(set(MALFORMED_COLUMNS)) == len(MALFORMED_COLUMNS)
assert not any(c[0] in FORMULA_LEAD for c in FIXED_COLUMNS + MALFORMED_COLUMNS)


class ExportFormat(StrEnum):
    CSV = "csv"
    TEXT = "text"
    JSONL = "jsonl"


# -- cells ------------------------------------------------------------------------------------


def safe_cell(text: str) -> str:
    """``text`` as a CSV string cell: control characters and terminal escapes escaped (so a
    cell is one inert line), and a cell that starts like a formula prefixed with ``'``. Leading
    whitespace (ASCII or Unicode: an importer may trim it) does not hide the lead."""
    if not (text.isascii() and text.isprintable()):
        text = clean(text, MAX_CELL_CHARS)
    if not text:
        return text
    lead = text[0]
    if lead.isspace():
        lead = text.lstrip()[:1]
    if lead in FORMULA_LEAD:
        return FORMULA_PREFIX + text
    return text


class UtcFormatter:
    """``ledger.format_ts`` with the date and time of the current second kept, so a run of frames
    pays for ``gmtime`` once per second rather than once per frame."""

    __slots__ = ("_prefix", "_second")

    def __init__(self) -> None:
        self._second = -1
        self._prefix = ""

    def __call__(self, time_ns: int) -> str:
        second, ns = divmod(time_ns, 1_000_000_000)
        if second != self._second:
            self._prefix = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(second))
            self._second = second
        return f"{self._prefix}.{ns // 1000:06d}Z"


def _int_cell(value: object) -> object:
    return value


def _float_cell(value: object) -> object:
    assert isinstance(value, float)
    return value if math.isfinite(value) else safe_cell(str(value))


def _text_cell(value: object) -> object:
    assert isinstance(value, str)
    return safe_cell(value)


def _bytes_cell(value: object) -> object:
    assert isinstance(value, bytes)
    return HEX_PREFIX + value.hex()


def _array_cell(value: object) -> object:
    """A variable-length array as one cell, ``[1 2 3]``: the elements are numbers, so the text
    needs no sanitizing (a non-finite float is spelled ``nan`` or ``inf``)."""
    assert isinstance(value, tuple)
    return "[" + " ".join(repr(v) if isinstance(v, float) else str(v) for v in value) + "]"


def _converter(field_def: FieldDef, raw_enums: bool) -> Callable[[object], object]:
    """How one decoded value becomes a cell: a number stays a number, an integer with an enum
    becomes its label (the number when the enum lacks the value, or with ``raw_enums``), text and
    bytes become sanitized strings."""
    match field_def.type.type_class:
        case TypeClass.INT:
            if raw_enums or not field_def.enum_by_value:
                return _int_cell
            labels = {number: safe_cell(label) for number, label in field_def.enum_by_value.items()}
            get = labels.get

            def label_cell(value: object) -> object:
                found = get(value)  # type: ignore[arg-type]
                return value if found is None else found

            return label_cell
        case TypeClass.FLOAT:
            return _float_cell
        case TypeClass.CHAR:
            return _text_cell
        case TypeClass.BYTES:
            return _bytes_cell
        case TypeClass.ARRAY:
            return _array_cell
    raise AssertionError("unreachable")  # pragma: no cover


class _LineOf:
    """The ``file`` of a ``csv.writer`` that keeps no state: ``writerow`` returns what ``write``
    returns, which is the line."""

    def write(self, text: str) -> str:
        return text


_WRITER: Final = csv.writer(
    _LineOf(),  # type: ignore[arg-type]
    lineterminator="\n",
    quoting=csv.QUOTE_MINIMAL,
)


def csv_line(cells: Sequence[object]) -> str:
    """One RFC 4180 row (LF-terminated) of already safe cells."""
    line = _WRITER.writerow(cells)
    assert isinstance(line, str)
    return line


class OutputError(Exception):
    """A write to the output failed (the file system said ``cause``)."""

    def __init__(self, cause: OSError) -> None:
        super().__init__(str(cause))
        self.cause = cause


# -- destinations -----------------------------------------------------------------------------


class Destination(Protocol):
    """Where lines go. ``member`` opens a named output (a single destination has one)."""

    def member(self, name: str) -> int: ...

    def write(self, handle: int, text: str) -> bool:
        """Queue ``text`` (whole lines, each ending in LF); ``False``: the output is gone."""
        ...

    def flush(self) -> None: ...


class StdoutDestination:
    """Lines to the console's stdout, one at a time (the writer adds the line end)."""

    def __init__(self, out: Callable[[str], bool]) -> None:
        self._out = out

    def member(self, name: str) -> int:
        return 0

    def write(self, handle: int, text: str) -> bool:
        assert text.endswith("\n") and text.count("\n") == 1
        return self._out(text[:-1])

    def flush(self) -> None:
        pass


class FileDestination:
    """One temporary file, written in large blocks."""

    def __init__(self, temp: TempOutput) -> None:
        self._temp = temp
        self._buffer: list[str] = []
        self._size = 0
        self._named = False

    def member(self, name: str) -> int:
        assert not self._named, "a single file has one member"
        self._named = True
        return 0

    def write(self, handle: int, text: str) -> bool:
        self._buffer.append(text)
        self._size += len(text)
        if self._size >= FILE_BUFFER_BYTES:
            self.flush()
        return True

    def flush(self) -> None:
        if self._buffer:
            data = "".join(self._buffer).encode("utf-8", "backslashreplace")
            self._buffer.clear()
            self._size = 0
            try:
                self._temp.write(data)
            except OSError as exc:
                raise OutputError(exc) from exc


class DirDestination:
    """The files of a temporary directory; each has its own buffer, and all of them together are
    flushed whenever they hold more than :data:`DIR_BUFFER_BYTES`."""

    def __init__(self, temp: TempDirOutput) -> None:
        self._temp = temp
        self._buffers: list[list[str]] = []
        self._size = 0

    def member(self, name: str) -> int:
        try:
            handle = self._temp.create(name)
        except OSError as exc:
            raise OutputError(exc) from exc
        assert handle == len(self._buffers)
        self._buffers.append([])
        return handle

    def write(self, handle: int, text: str) -> bool:
        self._buffers[handle].append(text)
        self._size += len(text)
        if self._size >= DIR_BUFFER_BYTES:
            self.flush()
        return True

    def flush(self) -> None:
        for handle, parts in enumerate(self._buffers):
            if parts:
                data = "".join(parts).encode("utf-8", "backslashreplace")
                parts.clear()
                try:
                    self._temp.write(handle, data)
                except OSError as exc:
                    raise OutputError(exc) from exc
        self._size = 0


# -- the tables -------------------------------------------------------------------------------


@dataclass(slots=True)
class _Plan:
    """How the packets of one message become rows."""

    message: Message
    table: _Table
    mid_cell: str
    header_names: tuple[str, ...]
    converters: tuple[Callable[[object], object], ...]


@dataclass(slots=True)
class _Table:
    """One output file of a directory (or the single file): its handle and rows so far."""

    stem: str
    handle: int
    rows: int = 0


def payload_columns(message: Message, taken: set[str]) -> list[str]:
    """The payload column names in CCDD order. A field named like a column already taken (a
    fixed or header column, or an earlier field) is spelled ``<MESSAGE>.<field>``; the result
    has no duplicates."""
    names: list[str] = []
    for field_def in message.fields:
        name = field_def.name
        if name in taken:
            name = f"{message.name}.{name}"
        while name in taken:
            name += "_"
        taken.add(name)
        names.append(name)
    return names


def header_names_of(message: Message, codec: Codec) -> tuple[str, ...]:
    return tuple(f.name for f in codec.profile(message.kind).fields)


def table_columns(message: Message, codec: Codec, header_fields: bool) -> tuple[str, ...]:
    """The columns of the table of ``message``: fixed, header fields if asked for, payload."""
    taken: set[str] = set(FIXED_COLUMNS)
    header = [f"hdr.{name}" for name in header_names_of(message, codec)] if header_fields else []
    taken.update(header)
    return (*FIXED_COLUMNS, *header, *payload_columns(message, taken))


def stem_for(name: str, variant: int) -> str:
    """The file stem of a table: the message name for its first layout, ``<name>.<n>`` for the
    n-th layout of the same name (message names hold no dot, so a stem never equals one), and
    ``<name>.<n>`` from the first for a message that is called ``malformed``."""
    if variant == 1 and name != MALFORMED_STEM:
        return name
    return f"{name}.{variant}"


@dataclass(slots=True)
class ExportResult:
    rows: int = 0  # lines written (data rows; headers excluded)
    decoded: int = 0  # decoded frames written
    malformed: int = 0
    unknown: int = 0
    garbage: int = 0
    undecoded: int = 0  # frames of a node with no definitions
    skipped_no_definitions: int = 0  # not tested by the filter: it needed definitions
    not_written: int = 0  # frames that did not decode, in a format with no place for them
    files: int = 0
    ending: Ending = Ending.DONE
    rejected: Counter[str] = field(default_factory=Counter)
    per_table: dict[str, int] = field(default_factory=dict)  # rows by file stem


class Layouts:
    """The CSV tables of one export: opens a file the first time a message appears and writes
    its header row. ``single`` means the destination holds one table; a second layout is then a
    usage error (the message has different layouts on different nodes). ``directory`` opens the
    ``malformed`` table first."""

    def __init__(
        self,
        dest: Destination,
        *,
        header_fields: bool,
        raw_enums: bool,
        single: bool,
        directory: bool,
    ) -> None:
        assert not (single and directory)
        self._dest = dest
        self._header_fields = header_fields
        self._raw_enums = raw_enums
        self._single = single
        self._plans: dict[int, _Plan] = {}
        self._tables: dict[tuple[str, tuple[str, ...]], _Table] = {}
        self._layouts: Counter[str] = Counter()
        self._used_stems: set[str] = set()
        self.all: list[_Table] = []
        self.malformed: _Table | None = None
        if directory:
            self.malformed = self._open(MALFORMED_STEM, MALFORMED_COLUMNS)

    def _open(self, stem: str, columns: Sequence[str]) -> _Table:
        assert stem.casefold() not in self._used_stems
        self._used_stems.add(stem.casefold())
        table = _Table(stem, self._dest.member(f"{stem}.csv"))
        self.all.append(table)
        self._dest.write(table.handle, csv_line([safe_cell(c) for c in columns]))
        return table

    def plan_for(self, message: Message, codec: Codec) -> _Plan:
        plan = self._plans.get(id(message))
        if plan is not None:
            assert plan.message is message
            return plan
        columns = table_columns(message, codec, self._header_fields)
        key = (message.name, columns)
        table = self._tables.get(key)
        if table is None:
            if self._single and self._tables:
                raise IronbirdError(
                    Code.CLI_USAGE,
                    f"{render.term(message.name, 64)} has more than one layout (different "
                    "nodes define it differently), which one CSV file cannot hold; narrow the "
                    "export with --where 'node == NAME' or export to a directory",
                )
            variant = self._layouts[message.name] + 1
            while stem_for(message.name, variant).casefold() in self._used_stems:
                variant += 1
            self._layouts[message.name] = variant
            table = self._open(stem_for(message.name, variant), columns)
            self._tables[key] = table
        header = header_names_of(message, codec) if self._header_fields else ()
        plan = _Plan(
            message,
            table,
            f"0x{message.mid:04X}",
            header,
            tuple(_converter(f, self._raw_enums) for f in message.fields),
        )
        self._plans[id(message)] = plan
        return plan


def check_single_layout(
    name: str,
    codecs: Mapping[str, Codec],
    nodes: Sequence[str],
    *,
    header_fields: bool,
    selected: frozenset[str] | None = None,
) -> None:
    """``IB-CLI-001`` before anything is written when the message ``name`` has more than one
    table layout across ``nodes`` (the recording's), which a single-table CSV cannot hold.
    ``selected`` narrows ``nodes`` to the ones the ``--where`` expression can match (``None``:
    any); nodes that agree on the layout never conflict."""
    layouts: dict[tuple[str, ...], list[str]] = {}
    for node in nodes:
        codec = codecs.get(node)
        if codec is None or (selected is not None and node not in selected):
            continue
        message = codec.model.by_name.get(name)
        if message is not None:
            columns = table_columns(message, codec, header_fields)
            layouts.setdefault(columns, []).append(node)
    if len(layouts) > 1:
        shown = "; ".join(
            ", ".join(render.term(n, 32) for n in found[:3]) for found in list(layouts.values())[:4]
        )
        raise IronbirdError(
            Code.CLI_USAGE,
            f"{render.term(name, 64)} has more than one layout (different nodes define it "
            f"differently: {shown}), which one CSV file cannot hold; narrow the export with "
            "--where 'node == NAME' or export to a directory (no --message, or several)",
        )


class Selection:
    """``--message``: the names asked for, and which MIDs of which nodes carry them."""

    def __init__(self, names: Sequence[str], codecs: Mapping[str, Codec]) -> None:
        self.names = frozenset(names)
        self._mids_by_node: dict[str, frozenset[int]] = {}
        for node, codec in codecs.items():
            self._mids_by_node[node] = frozenset(
                m.mid for m in codec.model.messages if m.name in self.names
            )
        self.mids = frozenset().union(*self._mids_by_node.values())

    def wants(self, frame: Frame) -> bool:
        """Whether a frame that did not decode could be one of the messages asked for: a
        telemetry frame with the MID of one of them."""
        if frame.mid is None or frame.stream.kind.value != "telemetry":
            return False
        return frame.mid in self._mids_by_node.get(frame.stream.node, frozenset())


def check_messages(names: Sequence[str], codecs: Mapping[str, Codec]) -> None:
    """``IB-CLI-001`` for a ``--message`` that no node of the recording defines."""
    known = sorted({name for c in codecs.values() for name in c.model.by_name})
    for name in names:
        if name not in known:
            close = difflib.get_close_matches(name[:64], known[:5000], n=3)
            hint = f"; did you mean {', '.join(close)}?" if close else ""
            raise IronbirdError(
                Code.CLI_USAGE,
                f"--message {render.term(name, 64)}: no such message in the recording's "
                f"definitions{hint}",
            )


# -- the export loop --------------------------------------------------------------------------


class _NoDefinitions(Exception):
    """A frame of a node with no codec reached the decoder."""


def export(
    replay: Replay,
    codecs: Mapping[str, Codec],
    dest: Destination,
    fmt: ExportFormat,
    *,
    where: Filter | None = None,
    selection: Selection | None = None,
    header_fields: bool = False,
    raw_enums: bool = False,
    single: bool = False,
    directory: bool = False,
) -> ExportResult:
    """Run ``replay`` to its end into ``dest``. ``single``: the CSV destination holds the table
    of one message (frames that did not decode have no place and are counted in
    :attr:`ExportResult.not_written`); ``directory``: a ``malformed`` table is opened first for
    them. Never raises for anything a recording contains; ``IronbirdError`` for a layout that
    cannot share one file, and whatever the destination raises for a failed write."""
    result = ExportResult()
    assert fmt is ExportFormat.CSV or not (single or directory)

    def codec_for(node: str) -> Codec:
        codec = codecs.get(node)
        if codec is None:
            raise _NoDefinitions(node)
        return codec

    def reject(frame: Frame, error: DecodeError) -> None:
        result.rejected[error.code.value] += 1

    decoder = FrameDecoder(codec_for, reject, report_checked=True)
    matcher = where.bind(decoder.decode, codec_for) if where is not None else None
    tables = (
        Layouts(
            dest,
            header_fields=header_fields,
            raw_enums=raw_enums,
            single=single,
            directory=directory,
        )
        if fmt is ExportFormat.CSV
        else None
    )
    as_json = fmt is ExportFormat.JSONL
    utc = UtcFormatter()
    stream_cells: dict[int, tuple[Stream, str, str, str]] = {}

    def cells_of(stream: Stream) -> tuple[str, str, str]:
        entry = stream_cells.get(stream.id)
        if entry is None or (entry[0] is not stream and entry[0] != stream):
            if len(stream_cells) >= MAX_STREAM_CELLS:
                stream_cells.clear()
            entry = (
                stream,
                safe_cell(stream.node),
                safe_cell(stream.link),
                safe_cell(stream.direction.value),
            )
            stream_cells[stream.id] = entry
        return entry[1], entry[2], entry[3]

    def write_decoded(decoded: Decoded) -> bool:
        frame, packet = decoded.frame, decoded.packet
        if tables is None:
            node, time_ns = frame.stream.node, frame.time_ns
            text = (
                json.dumps(render.record(node, time_ns, packet))
                if as_json
                else render.line(node, time_ns, packet)
            )
            return dest.write(0, text + "\n")
        plan = tables.plan_for(packet.message, codecs[frame.stream.node])
        node_cell, link_cell, _ = cells_of(frame.stream)
        time_ns = frame.time_ns
        cells: list[object] = [
            utc(time_ns),
            time_ns,
            frame.seq,
            node_cell,
            link_cell,
            plan.mid_cell,
        ]
        if plan.header_names:
            values = packet.header.values
            cells.extend(values.get(name, "") for name in plan.header_names)
        cells.extend(conv(v) for conv, v in zip(plan.converters, packet.values, strict=True))
        taken = dest.write(plan.table.handle, csv_line(cells))
        if taken:
            plan.table.rows += 1
        return taken

    def write_undecoded(frame: Frame, error: DecodeError | None, note: str) -> bool:
        if tables is None:
            text = (
                json.dumps(render.undecoded_record(frame, error, note))
                if as_json
                else render.undecoded_line(frame, error, note)
            )
            return dest.write(0, text + "\n")
        table = tables.malformed
        if table is None:
            result.not_written += 1
            return True
        node_cell, link_cell, dir_cell = cells_of(frame.stream)
        time_ns = frame.time_ns
        cells: list[object] = [
            utc(time_ns),
            time_ns,
            frame.seq,
            node_cell,
            link_cell,
            dir_cell,
            f"0x{frame.mid:04X}" if frame.mid is not None else "",
            render.frame_status(frame, error),
            error.code.value if error is not None else "",
            error.offset if error is not None else "",
            safe_cell(error.reason if error is not None else (note or "no definitions")),
            len(frame.data),
            frame.orig_len,
            safe_cell(",".join(render.flag_names(frame))),
            HEX_PREFIX + frame.data.hex(),
        ]
        taken = dest.write(table.handle, csv_line(cells))
        if taken:
            table.rows += 1
        return taken

    frames = replay.frames()
    with contextlib.closing(frames):
        for frame in frames:
            note = ""
            decoded: Decoded | DecodeError | None
            try:
                if matcher is not None and not matcher(frame):
                    continue
            except _NoDefinitions:
                result.skipped_no_definitions += 1  # the filter could not be tested
                continue
            try:
                decoded = matcher.decoded(frame) if matcher is not None else decoder.decode(frame)
            except _NoDefinitions:
                decoded, note = None, "no definitions for the node"
            if isinstance(decoded, Decoded):
                if selection is not None and decoded.packet.message.name not in selection.names:
                    continue
                taken = write_decoded(decoded)
                result.decoded += taken  # a row the output refused is not counted
            else:
                if selection is not None and not selection.wants(frame):
                    continue
                taken = write_undecoded(frame, decoded, note)
                if taken:
                    if note:
                        result.undecoded += 1
                    elif frame.flags & FrameFlag.GARBAGE:
                        result.garbage += 1
                    elif decoded is not None and decoded.code is Code.DEC_UNKNOWN:
                        result.unknown += 1
                    else:
                        result.malformed += 1
            if not taken:
                result.ending = Ending.OUTPUT
                break
        else:
            if replay.ended_by_damage:
                result.ending = Ending.DAMAGE
    dest.flush()
    if tables is not None:
        result.files = len(tables.all)
        result.per_table = {t.stem: t.rows for t in tables.all}
        result.rows = sum(t.rows for t in tables.all)
    else:
        result.files = 1
        result.rows = result.decoded + result.malformed + result.unknown
        result.rows += result.garbage + result.undecoded
    return result


# -- where the output may go ------------------------------------------------------------------


def _identity(path: Path) -> tuple[int, int] | None:
    """``(device, inode)`` of what ``path`` leads to (symbolic links and ``..`` resolved), or
    ``None`` when there is nothing there."""
    try:
        st = os.stat(path)
    except OSError:
        return None
    return (st.st_dev, st.st_ino)


def refuse_unsafe_output(
    output: Path,
    inputs: Sequence[Path],
    *,
    state_dirs: Sequence[Path] = (),
    managed_dirs: Sequence[Path] = (),
    force: bool,
    directory: bool,
) -> None:
    """The refusals that come before anything is read or created (``IB-EXP-002``).

    Never overridden by ``force``: ``output`` is one of the ``inputs`` (the same device and
    inode, whatever path leads there); it is inside the state directory ``state_dirs`` (the same
    device and inode as the output's directory), which holds ironbird's own files and nothing
    else; or it is named like a file retention manages (a ledger part, a reserve, a temporary of
    a salvage, a recording) inside the recording directory ``managed_dirs``, since a prune would
    later delete the export as if it were one of those.
    Otherwise an existing ``output`` is refused unless ``force`` allows replacing a regular file;
    a directory export never replaces anything.
    """
    here = _identity(output)
    if here is not None and any(_identity(p) == here for p in inputs):
        raise IronbirdError(
            Code.EXP_EXISTS,
            f"{render.term(str(output), 300)} is the recording being exported; "
            "the original is never modified, choose another output",
        )
    parent = _identity(output.parent)
    if parent is not None and any(_identity(d) == parent for d in state_dirs):
        raise IronbirdError(
            Code.EXP_EXISTS,
            f"{render.term(output.name, 200)} would be inside the ironbird state directory, "
            "which holds the ledger and its bookkeeping only; choose another output directory",
        )
    if (
        is_managed_name(output.name)
        and parent is not None
        and any(_identity(d) == parent for d in managed_dirs)
    ):
        raise IronbirdError(
            Code.EXP_EXISTS,
            f"{render.term(output.name, 200)} is named like a ledger or recording file that "
            "ironbird manages, and a prune would delete it; choose another output name",
        )
    if os.path.lexists(output):
        if directory:
            raise IronbirdError(
                Code.EXP_EXISTS,
                f"{render.term(str(output), 300)} already exists; a directory export never "
                "replaces anything (--force does not apply), choose another output",
            )
        if not force:
            raise IronbirdError(
                Code.EXP_EXISTS,
                f"{render.term(str(output), 300)} already exists; pass --force to replace it",
            )
        if not output.is_file() or output.is_symlink():
            raise IronbirdError(
                Code.EXP_EXISTS,
                f"{render.term(str(output), 300)} is not a regular file; --force replaces only "
                "a regular file",
            )


# -- committing the output --------------------------------------------------------------------


def _descriptors_in_use() -> int:
    """How many descriptors this process holds now (``/proc/self/fd``, else ``/dev/fd``); when
    neither can be read, a count that leaves the margin generous."""
    for directory in ("/proc/self/fd", "/dev/fd"):
        try:
            return len(os.listdir(directory))
        except OSError:
            continue
    return MAX_OPEN_MEMBERS


def default_max_open() -> int:
    """How many members of a directory export may hold a descriptor: at most
    :data:`MAX_OPEN_MEMBERS`, and what the soft ``RLIMIT_NOFILE`` leaves after the descriptors in
    use and :data:`DESCRIPTOR_MARGIN`; never less than one."""
    try:
        soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    except (OSError, ValueError):
        return MAX_OPEN_MEMBERS
    if soft == resource.RLIM_INFINITY:
        return MAX_OPEN_MEMBERS
    return max(1, min(MAX_OPEN_MEMBERS, soft - _descriptors_in_use() - DESCRIPTOR_MARGIN))


_NO_DESCRIPTORS: Final = frozenset({errno.EMFILE, errno.ENFILE})


MAX_SCANNED_NAMES: Final = 100_000  # directory entries looked at for stale temporaries


def stale_temporaries(output: Path) -> tuple[list[str], bool]:
    """Names beside ``output`` that look like the temporary file or directory of an export of the
    same output that never finished (``.<name>.<16 hex>.tmp``), and whether the scan stopped at
    :data:`MAX_SCANNED_NAMES` entries. Nothing is removed: whether a live export owns one is not
    known from here."""
    pattern = re.compile(r"\." + re.escape(output.name) + r"\.[0-9a-f]{16}\.tmp")
    found: list[str] = []
    try:
        with os.scandir(output.parent) as entries:
            for scanned, entry in enumerate(entries, start=1):
                if pattern.fullmatch(entry.name):
                    found.append(entry.name)
                if scanned >= MAX_SCANNED_NAMES:
                    return sorted(found), True
    except OSError:
        return [], False  # an unreadable or missing directory is the write's error to report
    return sorted(found), False


def input_guard(output: Path, replay: Replay) -> Callable[[], None]:
    """The check to make just before the commit: ``output`` is not (any longer) one of the files
    the replay read, or one of the paths it was given. A name that became the recording meanwhile
    (a link made during a long export) is refused, ``force`` or not (``IB-EXP-002``)."""

    def guard() -> None:
        here = _identity(output)
        if here is not None and (
            here in replay.stats.opened or any(_identity(p) == here for p in replay.parts)
        ):
            raise IronbirdError(
                Code.EXP_EXISTS,
                f"{render.term(str(output), 300)} became the recording being exported while the "
                "export ran; nothing was written to it, choose another output",
            )

    return guard


def _write_error(output: Path, exc: OSError, *, created: bool = True) -> IronbirdError:
    """``IB-EXP-001`` for a failed write. ``created``: the export had begun writing under a
    temporary name (now removed); otherwise nothing was created."""
    where = render.term(str(output), 300)
    reason = exc.strerror or str(exc)
    if exc.errno in _NO_DESCRIPTORS:
        advice = (
            "the process has too many files open; raise the limit (`ulimit -n 4096`) and "
            "run the export again"
        )
    elif exc.errno == errno.ENOENT and not created:
        advice = (
            f"the directory {render.term(str(output.parent), 300)} does not exist; nothing was "
            "created. Create it (mkdir -p) or choose another -o"
        )
    elif created:
        advice = "the partial output was removed"
    else:
        advice = "nothing was created"
    return IronbirdError(
        Code.EXP_WRITE,
        f"cannot write the export {where}: {reason}; {advice}",
        details=f"errno {exc.errno}" if exc.errno is not None else "",
        cause=exc,
    )


@dataclass(slots=True)
class WriteProgress:
    """What a :func:`write_to_file` or :func:`write_to_directory` got done, readable after any
    exit of it (an exception, a signal's included): the temporary output once it is made, and the
    result once ``run`` has returned."""

    temp: TempOutput | TempDirOutput | None = None
    result: ExportResult | None = None

    @property
    def committed(self) -> bool:
        """Whether the output has its final name (complete, whatever came after)."""
        return self.temp is not None and self.temp.committed


def _after_commit_note(output: Path, exc: SyncAfterCommit, what: str) -> str:
    """Why a committed output deserves a warning: its directory could not be synced, or the
    temporary name of a hard-link commit could not be removed. ``what``: the output's kind."""
    where = render.term(str(output), 300)
    reason = render.term(exc.strerror or "unknown error", 200)
    if isinstance(exc, LeftoverAfterCommit):
        note = (
            f"the {what} {where} was written, but the temporary file "
            f"{render.term(exc.leftover, 100)} beside it could not be removed ({reason}); "
            "remove it by hand"
        )
        if exc.sync_error is not None:
            note += f"; the directory also could not be synced ({render.term(exc.sync_error, 200)})"
        return note
    return (
        f"the directory of {where} could not be synced ({reason}); the {what} exists, "
        "but its name may not survive a power loss"
    )


def _publish_failure(output: Path, exc: OSError, *, force: bool, directory: bool) -> IronbirdError:
    """The error for a failed publish (anything but :class:`SyncAfterCommit`, which is a warning
    about an output that exists)."""
    assert not isinstance(exc, SyncAfterCommit)
    name = render.term(str(output), 300)
    if isinstance(exc, Locked) and not directory:
        return IronbirdError(
            Code.EXP_EXISTS,
            f"{name} is in use by another process (it holds a lock on it); it was not "
            "replaced, choose another output",
            cause=exc,
        )
    if isinstance(exc, Collision):
        if directory:
            summary = (
                f"{name} appeared while exporting; a directory export never replaces "
                "anything, choose another output"
            )
        elif force:
            summary = f"{name} is not a regular file; --force replaces only a regular file"
        else:
            summary = f"{name} appeared while exporting; it was not replaced (pass --force)"
        return IronbirdError(Code.EXP_EXISTS, summary, cause=exc)
    return _write_error(output, exc)


def _replaced_failure(
    output: Path, replaced: BaseException, *, force: bool, directory: bool, created: bool
) -> BaseException | None:
    """What the failure ``replaced`` (raw, from a held step of the write) is reported as when a
    signal took its place: the error :func:`write_to_file` or :func:`write_to_directory` raises
    for it without the signal, ``None`` for a committed output whose only trouble is a warning.
    ``created``: the temporary output was made."""
    match replaced:
        case SyncAfterCommit():
            return None
        case OutputError():
            return _write_error(output, replaced.cause)
        case OSError() if not created:
            return _write_error(output, replaced, created=False)
        case OSError():
            return _publish_failure(output, replaced, force=force, directory=directory)
        case _:
            return replaced


def write_to_file(
    output: Path,
    *,
    force: bool,
    run: Callable[[Destination], ExportResult],
    fs: FileSystem = REAL_FS,
    before_publish: Callable[[], None] | None = None,
    hold: Hold | None = None,
    progress: WriteProgress | None = None,
) -> tuple[ExportResult, str | None]:
    """Run ``run`` against a temporary file beside ``output`` and give it the name ``output``
    when ``run`` returns: refused (``IB-EXP-002``) if the name is taken by then, unless ``force``
    allows replacing a regular file of ours that no one else holds locked. ``before_publish``,
    if given, runs once the data is synced, just before the rename, and may refuse; ``hold`` is
    begun right before the rename (see :meth:`~ironbird.atomicfile.TempOutput.publish`). Any
    error leaves no file behind (``IB-EXP-001`` for a failed write). The second value is a
    warning for a file that exists all the same: its directory could not be synced after the
    commit, or the temporary name of a hard-link commit could not be removed. ``progress`` is
    filled as the write goes."""
    sync: str | None = None
    entered = False  # the temporary file is made

    def replaced_as(replaced: BaseException) -> BaseException | None:
        return _replaced_failure(output, replaced, force=force, directory=False, created=entered)

    with naming_replaced(replaced_as), contextlib.ExitStack() as stack:
        try:
            # held until the stack holds the cleanup: a signal cannot leave the file behind
            with signals_held():
                temp = stack.enter_context(TempOutput(output, force=force, fs=fs))
                if progress is not None:
                    progress.temp = temp
        except OSError as exc:
            raise _write_error(output, exc, created=False) from exc
        entered = True
        try:
            result = run(FileDestination(temp))
        except OutputError as exc:
            raise _write_error(output, exc.cause) from exc
        if progress is not None:
            progress.result = result
        try:
            temp.prepare()
            if before_publish is not None:
                before_publish()
            temp.publish(hold=hold)
        except SyncAfterCommit as exc:
            sync = _after_commit_note(output, exc, "file")  # committed all the same
        except OSError as exc:
            raise _publish_failure(output, exc, force=force, directory=False) from exc
    return result, sync


def write_to_directory(
    output: Path,
    *,
    run: Callable[[Destination], ExportResult],
    fs: FileSystem = REAL_FS,
    max_open: int | None = None,
    before_publish: Callable[[], None] | None = None,
    hold: Hold | None = None,
    progress: WriteProgress | None = None,
) -> tuple[ExportResult, str | None]:
    """:func:`write_to_file` for a directory of tables: the directory is built beside ``output``
    and renamed to it without replacing anything, whatever ``output`` has become meanwhile.
    ``max_open``: how many tables hold a descriptor at once (default: :func:`default_max_open`)."""
    sync: str | None = None
    entered = False  # the temporary directory is made
    temp = TempDirOutput(
        output, fs=fs, max_open=default_max_open() if max_open is None else max_open
    )

    def replaced_as(replaced: BaseException) -> BaseException | None:
        return _replaced_failure(output, replaced, force=False, directory=True, created=entered)

    try:
        with naming_replaced(replaced_as), contextlib.ExitStack() as stack:
            try:
                with signals_held():  # until the stack holds the cleanup (see write_to_file)
                    stack.enter_context(temp)
                    if progress is not None:
                        progress.temp = temp
            except OSError as exc:
                raise _write_error(output, exc, created=False) from exc
            entered = True
            try:
                result = run(DirDestination(temp))
            except OutputError as exc:
                raise _write_error(output, exc.cause) from exc
            if progress is not None:
                progress.result = result
            try:
                temp.prepare()
                if before_publish is not None:
                    before_publish()
                temp.publish(hold=hold)
            except SyncAfterCommit as exc:
                sync = _after_commit_note(output, exc, "directory")
            except OSError as exc:
                raise _publish_failure(output, exc, force=False, directory=True) from exc
    except IronbirdError as exc:
        if temp.leftover is None:
            raise
        raise IronbirdError(
            exc.code,
            f"{exc.summary} (the temporary directory {render.term(temp.leftover, 100)} beside "
            "it could not be removed)",
            details=exc.details,
            cause=exc.cause,
        ) from exc
    return result, sync


__all__ = [
    "Destination",
    "DirDestination",
    "ExportFormat",
    "ExportResult",
    "FileDestination",
    "OutputError",
    "Selection",
    "StdoutDestination",
    "WriteProgress",
    "check_messages",
    "check_single_layout",
    "csv_line",
    "export",
    "input_guard",
    "refuse_unsafe_output",
    "safe_cell",
    "stale_temporaries",
    "write_to_directory",
    "write_to_file",
]
