"""Messages of the export pool's wire format (``ironbird.recording.poolframe``): valid samples of
every type, and the checked-in corpus of malformed replies (``tests/corpus/malformed/poolframe-*``).

Every corpus case is a message a worker could send the parent in answer to :data:`JOB`, and the
parent must refuse it: :func:`~ironbird.recording.poolframe.read_reply` (given :data:`LAYOUTS`)
raises ``FrameError`` (``IB-EXP-003``), whether the frame is broken (truncated, a lying length,
bad UTF-8, an unknown type or version) or well formed and not an answer the parent can apply (the
wrong range, rows that disagree with their kinds, a table no definition makes). Every case is
built here, so a test proves the checked-in files are exactly these; a new bug adds a case and
regenerates: ``python tests/poolframe_corpus.py``.
"""

from __future__ import annotations

import array
import dataclasses
import hashlib
import struct
from collections import Counter
from pathlib import Path

from ironbird.ccdd.model import MessageKind
from ironbird.frame import Direction, Stream
from ironbird.recording import format as fmt
from ironbird.recording import poolframe as pf
from ironbird.recording.export import ExportFormat, ExportResult
from ironbird.recording.player import Ending
from ironbird.recording.poolmsg import (
    Block,
    ConfigDefinitions,
    EmbeddedDefinitions,
    Fault,
    Job,
    Outcome,
    Seed,
    Shape,
)
from ironbird.recording.reader import Damage

CORPUS = Path(__file__).resolve().parent / "corpus" / "malformed"
PREFIX = "poolframe-"

RX = Stream(1, "uplink", "fsw", MessageKind.TELEMETRY, Direction.RX, "udp", "udp 0.0.0.0:2234")
TX = Stream(2, "uplink", "fsw", MessageKind.COMMAND, Direction.TX, "serial", "serial /dev/ttyS0")
SHAPE = Shape(
    ExportFormat.CSV,
    "mid == 0x0881 and node == 'fsw'",
    frozenset({"uplink", "downlink"}),
    (("ground", "fsw"),),
    ("HK_TLM",),
    header_fields=True,
    raw_enums=False,
    directory=True,
    skip_damage=True,
    categories=True,
)
JOB = Job(
    7,
    Path("/data/run1.ibrec"),
    4096,
    12288,
    False,
    Seed({1: RX, 2: TX}, {0: "", 3: "10.0.0.1:5000"}, 41, 512),
    1_700_000_000_000_000_000,
    None,
    SHAPE,
    (64769, 1234567),
    1000,
    10,
)
#: The tables the definitions of :data:`JOB` make: what a reply's tables are checked against.
LAYOUTS = pf.Layouts([("HK_TLM", ("seq", "CmdCounter"))])
REASON = "FRAME record fails its CRC"
ROWS = "1,2\n3,4\n"


def outcome() -> Outcome:
    """A reply to :data:`JOB` with every field in use, which the parent accepts."""
    return Outcome(
        7,
        start=4096,
        end=8000,
        reached_stop=False,
        first_seq=42,
        last_seq=57,
        definitions=[("stream", RX), ("source", 4, "10.0.0.2:5001")],
        used_streams=[1, 2],
        used_sources=[0, 3],
        work=96,
        scanned=4200,
        trailer=False,
        partial=True,
        damage=[Damage(5000, 64, False, REASON, 5064, False, False)],
        damage_rows=[2],
        frames_read=5,
        frames_in_range=4,
        gap_marks=[(3, 1, 7)],
        counts=ExportResult(
            rows=3,
            decoded=3,
            malformed=1,
            not_written=1,
            rejected=Counter({"IB-DEC-001": 2}),
            per_table={"HK_TLM": 2},
        ),
        blocks=[
            Block(0, False, 2, ROWS, 1),
            Block(None, True, 1, "5,6\n", 0),
        ],
        layouts=LAYOUTS.digest,
        categories=bytes([0, 1, 0x80 | 2, 0]),
        order=array.array("I", [1, 0, 1]),
    )


def embedded() -> EmbeddedDefinitions:
    ccdd = b'{"messages": []}'
    blob = fmt.Blob(fmt.BlobKind.CCDD, "fsw", "json", hashlib.sha256(ccdd).digest(), ccdd)
    meta = {"schema": "ironbird-recording/1", "part": 1, "nodes": {"fsw": {"x": [1, 2.5, None]}}}
    return EmbeddedDefinitions(
        Path("run1.ibrec"), {("fsw", fmt.BlobKind.CCDD): blob}, "", ("fsw",), meta, 2048, "hint"
    )


def samples() -> dict[str, pf.Message]:
    """One valid message of every type and variant."""
    return {
        "embedded": embedded(),
        "config": ConfigDefinitions(
            Path("/etc/ib.toml"), "ab" * 32, ("fsw", "gse"), 65535, "export", "cd" * 32
        ),
        "config-no-digest": ConfigDefinitions(Path("ib.toml"), None, (), 1, "export", ""),
        "job": JOB,
        "job-exact": dataclasses.replace(
            JOB,
            exact=True,
            stop=None,
            start_ns=None,
            end_ns=-5,
            shape=dataclasses.replace(SHAPE, where=None, aliases=None, fmt=ExportFormat.JSONL),
        ),
        "job-wide-times": dataclasses.replace(
            JOB, start_ns=-62_135_596_800 * 10**9, end_ns=253_402_300_799 * 10**9
        ),
        "outcome": outcome(),
        "outcome-crash": Outcome(7, crash="Traceback (most recent call last):\nValueError: x"),
        "outcome-moved": Outcome(7, moved=True),
        "outcome-failure": dataclasses.replace(
            outcome(), failure=("IB-REC-002", "damaged recording", "at offset 5000"), partial=False
        ),
        "fault": Fault("a JOB came where a DEFINITIONS must"),
    }


def _frame(kind: int, body: bytes, *, version: int = pf.VERSION, reserved: int = 0) -> bytes:
    return pf.HEADER.pack(pf.MAGIC, version, kind, reserved, len(body)) + body


def _replace_once(data: bytes, old: bytes, new: bytes) -> bytes:
    assert data.count(old) == 1, old
    return data.replace(old, new)


def _length_prefixed(text: str) -> bytes:
    raw = text.encode()
    return struct.pack("<I", len(raw)) + raw


def _with_body(frame: bytes, body: bytes) -> bytes:
    """``frame``'s header (length fixed up) over ``body``."""
    magic, version, kind, reserved, _ = pf.HEADER.unpack_from(frame)
    return pf.HEADER.pack(magic, version, kind, reserved, len(body)) + body


def cases() -> dict[str, bytes]:
    good = pf.encode(outcome())
    body = good[pf.HEADER.size :]
    out: dict[str, bytes] = {}

    def case(name: str, data: bytes) -> None:
        out[f"{PREFIX}{name}.bin"] = data

    # the frame
    case("empty", b"")
    case("header-cut", good[: pf.HEADER.size - 1])
    case("bad-magic", b"IBPG" + good[4:])
    case("unknown-version", _frame(pf.Kind.OUTCOME, body, version=pf.VERSION + 1))
    case("unknown-type", _frame(9, body))
    case("reserved-set", _frame(pf.Kind.OUTCOME, body, reserved=1))
    case("body-length-lies-high", good[:8] + struct.pack("<I", len(body) + 1) + body)
    case("body-length-lies-low", good[:8] + struct.pack("<I", len(body) - 1) + body)
    case("body-cut-in-rows", _with_body(good, body[: body.index(ROWS.encode()) + 3]))
    case("trailing-bytes", _with_body(good, body + b"\0"))
    # lengths and counts
    huge = struct.pack("<I", 0xFFFFFFFF) + ROWS.encode()
    case("rows-length-huge", _replace_once(good, _length_prefixed(ROWS), huge))
    lying = struct.pack("<I", len(ROWS) + 400) + ROWS.encode()
    case("rows-length-past-end", _replace_once(good, _length_prefixed(ROWS), lying))
    zeros = pf._OUTCOME.pack(7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    case(
        "definitions-over-cap",
        _frame(pf.Kind.OUTCOME, zeros + struct.pack("<I", pf.MAX_NOTES + 1)),
    )
    rejections = struct.pack("<I", 1) + _length_prefixed("IB-DEC-001")
    case(
        "rejections-over-cap",
        _replace_once(good, rejections, struct.pack("<I", len(pf._CODES) + 1) + rejections[4:]),
    )
    case("fault-length-over-cap", _frame(pf.Kind.FAULT, struct.pack("<I", pf.MAX_DIAG_BYTES + 1)))
    # text
    case("rows-bad-utf8", _replace_once(good, ROWS.encode(), b"1,2\n3\xff4\n"))
    case("reason-surrogate", _replace_once(good, REASON.encode(), b"\xed\xa0\x80" + b"x" * 23))
    case("reason-overlong", _replace_once(good, REASON.encode(), b"\xc0\xaf" + b"x" * 24))
    case("reason-cut-sequence", _replace_once(good, REASON.encode(), b"x" * 24 + b"\xe2\x82"))
    # fixed fields, flags and code tables
    flags_at = pf.HEADER.size + pf._OUTCOME.size - 6  # the flags, then the layouts digest
    (flags,) = struct.unpack_from("<H", good, flags_at)
    case(
        "undefined-flags",
        good[:flags_at] + struct.pack("<H", flags | 0x8000) + good[flags_at + 2 :],
    )
    no_start = struct.pack("<H", flags & ~0x001)
    case("absent-start-not-zero", good[:flags_at] + no_start + good[flags_at + 2 :])
    case(
        "unknown-error-code",
        pf.encode(dataclasses.replace(outcome(), failure=("IB-EXP-003", "x", "y"))).replace(
            b"IB-EXP-003", b"IB-XYZ-999"
        ),
    )
    reserved_stream = struct.pack("<HBB", 0xFFFF, 2, 1)
    case("stream-id-reserved", _replace_once(good, struct.pack("<HBB", 1, 2, 1), reserved_stream))
    endings = pf._COUNTS.pack(3, 3, 1, 0, 0, 0, 0, 1, 0, pf._ENDINGS[Ending.DONE])
    bad_ending = endings[:-1] + b"\x09"
    case("unknown-ending", _replace_once(good, endings, bad_ending))
    # messages that are not an answer
    case("rejection-code-unknown", _replace_once(good, b"IB-DEC-001", b"IB-XYZ-999"))
    case("job-as-reply", pf.encode(JOB))
    case("definitions-as-reply", pf.encode(embedded()))
    case("fault", pf.encode(Fault("the job could not be read")))
    # well formed, and not what the parent can apply
    bad = outcome()

    def semantic(name: str, **changes: object) -> None:
        case(name, pf.encode(dataclasses.replace(bad, **changes)))  # type: ignore[arg-type]

    semantic("wrong-range", index=8)
    semantic("start-before-range", start=4088)
    semantic("start-past-range", start=12288)
    semantic("ends-before-start", end=4000, partial=False)
    semantic("cut-without-moving-on", end=4096)
    semantic("row-kind-undefined", categories=bytes([0, 1, 5, 0]))
    semantic("lines-disagree-with-kinds", categories=bytes([0, 1, 0x82, 0x80]))
    semantic("order-disagrees-with-blocks", order=array.array("I", [1, 1, 1]))
    semantic("order-names-no-block", order=array.array("I", [1, 0, 9]))
    semantic("several-tables-no-order", order=None)
    semantic("damage-without-kinds", categories=None, order=None)
    semantic("block-not-whole-line", blocks=[bad.blocks[0], Block(None, True, 1, "5,6", 0)])
    semantic(
        "two-blocks-one-table", blocks=[bad.blocks[0], dataclasses.replace(bad.blocks[1], handle=1)]
    )
    semantic(
        "two-blocks-one-key",
        blocks=[bad.blocks[0], bad.blocks[1], dataclasses.replace(bad.blocks[0], handle=7)],
    )
    semantic("block-of-undefined-table", blocks=[Block(1, False, 2, ROWS, 1), bad.blocks[1]])
    semantic(
        "block-of-table-index-huge", blocks=[Block(0xFFFFFFFF, False, 2, ROWS, 1), bad.blocks[1]]
    )
    semantic("blocks-numbered-by-other-definitions", layouts=LAYOUTS.digest ^ 1)
    semantic(
        "malformed-block-with-key",
        blocks=[bad.blocks[0], dataclasses.replace(bad.blocks[1], layout=0)],
    )
    return out


def regenerate() -> None:
    for name, data in cases().items():
        (CORPUS / name).write_bytes(data)


if __name__ == "__main__":
    regenerate()
