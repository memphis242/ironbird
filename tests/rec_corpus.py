"""The checked-in corpus of malformed recordings (``tests/corpus/malformed/rec-*.ibrec``).

Each case is built deterministically here (fixed nonce), so a test can prove the checked-in files
are exactly these, and a new bug adds a case and regenerates: ``python tests/rec_corpus.py``.
"""

from __future__ import annotations

import json
import struct
import zlib
from pathlib import Path
from typing import NamedTuple

from ironbird.ccdd.model import MessageKind
from ironbird.errors import Code
from ironbird.frame import Direction, GapReason, Stream
from ironbird.recording import format as fmt
from ironbird.recording.format import RecordType
from recutil import RX, SERIAL, Builder

CORPUS = Path(__file__).resolve().parent / "corpus" / "malformed"
OTHER_NONCE = 0x0F0E0D0C0B0A0908


def _meta_text(cap: int) -> bytes:
    return json.dumps({"schema": fmt.META_SCHEMA, "max_packet_bytes": cap}).encode()


def _good(count: int = 6) -> Builder:
    return Builder().standard(count)


def _flip(data: bytes, offset: int, mask: int = 0xFF) -> bytes:
    out = bytearray(data)
    out[offset] ^= mask
    return bytes(out)


def _forged() -> bytes:
    """A frame whose data is two complete, well-formed records under another nonce."""
    other = Builder(OTHER_NONCE)
    other.standard(0)
    forged = bytes(other.out[24:])
    forged += fmt.encode_record(
        RecordType.FRAME, other.frame_body(3, stream=0), fmt.seed_of(OTHER_NONCE)
    )
    forged += fmt.encode_record(
        RecordType.FRAME, other.frame_body(4, stream=0), fmt.seed_of(OTHER_NONCE)
    )
    b = _good(0)
    first = b.frame(1)
    b.frame(2, data=b"\x00" * 4 + forged)  # the forged records start 8-byte aligned
    b.frame(3)
    b.frame(4)
    return _flip(bytes(b.out), first + 20)  # damage in front of the forgery


def cases() -> dict[str, bytes]:
    out: dict[str, bytes] = {}
    good = bytes(_good().out)
    out["rec-empty.ibrec"] = b""
    out["rec-magic-only.ibrec"] = fmt.MAGIC
    out["rec-short-preamble.ibrec"] = good[:19]
    out["rec-text-file.ibrec"] = b"timestamp,mid,count\n1,2,3\n" * 4
    out["rec-version-2.ibrec"] = good[:8] + struct.pack("<H", 2) + good[10:]
    out["rec-preamble-flags.ibrec"] = good[:12] + struct.pack("<I", 1) + good[16:]
    out["rec-wrong-nonce-meta.ibrec"] = _flip(good, 17)
    out["rec-ends-after-preamble.ibrec"] = good[:24]
    out["rec-ends-inside-meta.ibrec"] = good[:40]

    b = Builder()
    b.out = bytearray(fmt.encode_preamble(b.nonce))
    b.raw(RecordType.META, b"x", body_len=fmt.MAX_META_BODY + 1)
    out["rec-meta-oversize.ibrec"] = bytes(b.out)
    huge = b'{"schema":"ironbird-rec-meta/1","max_packet_bytes":' + b"9" * 5000 + b"}"
    bad_meta = {
        "rec-meta-not-json.ibrec": b"{not json",
        "rec-meta-array.ibrec": b"[1,2,3]",
        "rec-meta-deep-nesting.ibrec": b"[" * 100_000,
        "rec-meta-huge-int.ibrec": huge,
        "rec-meta-negative-cap.ibrec": _meta_text(-5),
        "rec-meta-cap-too-big.ibrec": _meta_text(70000),
    }
    for name, text in bad_meta.items():
        b = Builder()
        b.out = bytearray(fmt.encode_preamble(b.nonce))
        b.raw(RecordType.META, text)
        out[name] = bytes(b.out)

    def with_tail(name: str, build: Builder) -> None:
        out[name] = bytes(build.out)

    b = _good(2)
    b.frame(3, stream=9)
    b.frame(4)
    b.frame(5)
    with_tail("rec-frame-undefined-stream.ibrec", b)
    b = _good(2)
    b.frame(3, source=77)
    b.frame(4)
    b.frame(5)
    with_tail("rec-frame-undefined-source.ibrec", b)
    b = _good(2)
    b.frame(3, flags=3, orig_len=20)
    b.frame(4)
    b.frame(5)
    with_tail("rec-frame-truncated-without-excess.ibrec", b)
    b = _good(2)
    b.frame(3, flags=1, orig_len=99)
    b.frame(4)
    b.frame(5)
    with_tail("rec-frame-orig-len-lies-high.ibrec", b)
    b = _good(2)
    b.frame(3, flags=1, orig_len=3)
    b.frame(4)
    b.frame(5)
    with_tail("rec-frame-orig-len-lies-low.ibrec", b)
    b = _good(2)
    b.frame(3, flags=3, orig_len=500)  # truncated, but within the 2048 byte cap
    b.frame(4)
    b.frame(5)
    with_tail("rec-frame-truncated-within-cap.ibrec", b)
    b = _good(2)
    b.frame(3, flags=0x04, mid=0)  # GARBAGE on a udp stream
    b.frame(4)
    b.frame(5)
    with_tail("rec-frame-garbage-on-udp.ibrec", b)
    b = _good(2)
    b.stream(SERIAL)
    b.frame(3, stream=2, flags=0x0C, mid=0)  # GARBAGE and STALE together
    b.frame(4)
    b.frame(5)
    with_tail("rec-frame-garbage-and-stale.ibrec", b)
    b = _good(2)
    b.frame(3, flags=0x30)  # unknown flag bits
    b.frame(4)
    b.frame(5)
    with_tail("rec-frame-unknown-flags.ibrec", b)
    b = _good(2)
    b.frame(3, reserved=7)
    b.frame(4)
    b.frame(5)
    with_tail("rec-frame-reserved-set.ibrec", b)
    b = _good(2)
    b.frame(3, flags=0, mid=9)  # a MID without HAS_MID
    b.frame(4)
    b.frame(5)
    with_tail("rec-frame-mid-without-flag.ibrec", b)
    b = _good(2)
    b.frame(2)  # sequence does not increase
    b.frame(4)
    b.frame(5)
    with_tail("rec-frame-seq-repeats.ibrec", b)
    b = _good(2)
    b.frame(0)
    b.frame(4)
    b.frame(5)
    with_tail("rec-frame-seq-zero.ibrec", b)
    b = _good(2)
    b.raw(RecordType.FRAME, b.frame_body(3, data=b"y" * 5000))  # above max_packet_bytes
    b.frame(4)
    b.frame(5)
    with_tail("rec-frame-over-cap.ibrec", b)
    b = _good(2)
    b.raw(RecordType.FRAME, b"z" * 40, body_len=0xFFFFFFFF)
    b.frame(4)
    b.frame(5)
    with_tail("rec-body-len-huge.ibrec", b)
    b = _good(2)
    b.raw(RecordType.FRAME, b.frame_body(3), body_len=20)  # below the fixed part
    b.frame(4)
    b.frame(5)
    with_tail("rec-body-len-below-minimum.ibrec", b)
    b = _good(2)
    b.raw(RecordType.FRAME, b.frame_body(3, data=b"q" * 30), body_len=60)  # lies high
    b.frame(4)
    b.frame(5)
    with_tail("rec-body-len-lies-high.ibrec", b)
    b = _good(2)
    b.raw(RecordType.FRAME, b.frame_body(3, data=b"q" * 30), body_len=40)  # lies low
    b.frame(4)
    b.frame(5)
    with_tail("rec-body-len-lies-low.ibrec", b)
    b = _good(2)
    body = fmt.encode_blob(fmt.BlobKind.CCDD, "fc", "json", b"\x00" * 32, b"content")
    b.add(RecordType.BLOB, body)  # the digest does not match
    b.frame(3)
    b.frame(4)
    with_tail("rec-blob-bad-digest.ibrec", b)
    b = _good(2)
    b.add(RecordType.BLOB, bytes([9]) + b"\x02fc\x04json" + b"\x00" * 32)
    b.frame(3)
    b.frame(4)
    with_tail("rec-blob-bad-kind.ibrec", b)
    b = _good(2)
    b.add(RecordType.STREAM, fmt.encode_stream(RX))  # the same definition again is fine
    redefined = Stream(0, "other", "fc", MessageKind.TELEMETRY, Direction.RX, "udp", "x")
    b.add(RecordType.STREAM, fmt.encode_stream(redefined))
    b.frame(3)
    b.frame(4)
    with_tail("rec-stream-redefined.ibrec", b)
    b = _good(2)
    b.add(RecordType.STREAM, b"\x05\x00\x01\x01\xff" + b"ab")  # a string runs past the record
    b.frame(3)
    b.frame(4)
    with_tail("rec-stream-string-overruns.ibrec", b)
    b = _good(2)
    b.add(RecordType.GAP, fmt.encode_gap(fmt.Gap(9, GapReason.KERNEL_DROP, 1, 1, 1, 1, 1)))
    b.frame(3)
    b.frame(4)
    with_tail("rec-gap-undefined-stream.ibrec", b)
    b = _good(2)
    b.add(RecordType.INDEX, struct.pack("<I", 2) + struct.pack("<QQ", 500, 100))
    b.frame(3)
    b.frame(4)
    with_tail("rec-index-not-increasing.ibrec", b)
    b = _good(2)
    b.add(RecordType.INDEX, struct.pack("<I", 1000))  # claims more entries than it holds
    b.frame(3)
    b.frame(4)
    with_tail("rec-index-count-lies.ibrec", b)
    b = _good(0)
    for seq in (1, 2, 3):
        b.frame(seq)
    lie = fmt.Chunk(24, 1, 3, 5, 5, 0, 0, 2, 999, 0, 0, ((0x890, 2),))
    b.add(RecordType.CHUNK, fmt.encode_chunk(lie))
    with_tail("rec-chunk-lies.ibrec", b)
    b = _good(0)
    b.frame(1)
    b.add(
        RecordType.CHUNK,
        fmt.encode_chunk(fmt.Chunk(10**12, 1, 1, 5, 5, 0, 0, 1, 80, 0, 0, ((0x890, 1),))),
    )
    b.frame(2)
    b.frame(3)
    with_tail("rec-chunk-start-beyond-file.ibrec", b)
    b = _good(3)
    b.add(RecordType.TRAILER, fmt.encode_trailer(fmt.Trailer(0, 3, 3)))
    b.out += b"trailing junk after the trailer"
    with_tail("rec-bytes-after-trailer.ibrec", b)
    b = _good(2)
    b.raw(0x42, b"from the future")
    b.frame(3)
    with_tail("rec-unknown-critical-type.ibrec", b)
    b = _good(2)
    b.raw(0xC0, b"an extension")
    b.frame(3)
    with_tail("rec-unknown-skippable-type.ibrec", b)
    b = _good(3)
    b.out += b"\x00" * 64
    with_tail("rec-zero-filled-tail.ibrec", b)
    b = _good(3)
    b.out[-3:] = b""
    with_tail("rec-torn-inside-last-record.ibrec", b)
    out["rec-bit-flip-in-envelope.ibrec"] = _flip(bytes(_good(8).out), len(_good(3).out) + 5, 0x40)
    out["rec-bit-flip-in-padding.ibrec"] = _flip(bytes(_good(8).out), len(_good(3).out) - 2, 0x01)
    out["rec-forged-in-frame-data.ibrec"] = _forged()
    b = _good(1)
    dense = struct.pack("<IBBHI", 2000, 5, 0, 0, 0xDEADBEEF) + b"\x00" * 4
    b.out += b"\xaa" * 8 + dense * 12000  # an envelope-shaped candidate every 16 bytes
    with_tail("rec-dense-fake-envelopes.ibrec", b)
    b = _good(1)
    b.out += b"\xaa" * 8
    for _ in range(2000):
        head = struct.pack("<IBBH", 40, 5, 0, 0)
        b.out += head + struct.pack("<I", zlib.crc32(head, 7)) + b"\x00" * 40
    with_tail("rec-fake-chains-wrong-nonce.ibrec", b)

    # Found in the PR 7 review.
    b = _good(0)
    b.out += struct.pack("<IBBH", 0xFFFFF0, 0xFF, 0, 0) * 512  # bodies that run past the end
    with_tail("rec-resync-bodies-past-eof.ibrec", b)
    b = _good(0)
    for seq in (1, 2, 3):
        b.frame(seq)
    c0 = b.add(RecordType.CHUNK, _chunk(24, 3, 1, 3))
    stale = b.add(RecordType.INDEX, fmt.encode_index([c0]))
    for seq in (4, 5, 6):
        b.frame(seq)
    c1 = b.add(RecordType.CHUNK, _chunk(stale, 3, 4, 6))
    index = b.add(RecordType.INDEX, fmt.encode_index([c0, c1]))
    b.add(RecordType.TRAILER, fmt.encode_trailer(fmt.Trailer(index, 6, 6)))
    with_tail("rec-stale-index-mid-file.ibrec", b)
    b = _good(0)
    for seq in (1, 2, 3):
        b.frame(seq)
    c0 = b.add(RecordType.CHUNK, _chunk(24, 3, 1, 3))
    index = b.add(RecordType.INDEX, fmt.encode_index([c0]))
    b.add(RecordType.TRAILER, fmt.encode_trailer(fmt.Trailer(index, 3, 3)))
    out["rec-concatenated.ibrec"] = bytes(b.out) * 2
    b = _good(0)
    for seq in (1, 2, 3):
        b.frame(seq)
    b.add(RecordType.TRAILER, fmt.encode_trailer(fmt.Trailer(0, 3, 3)))
    for seq in (4, 5, 6):
        b.frame(seq)
    with_tail("rec-trailer-mid-file.ibrec", b)
    b = _good(0)
    for seq in (1, 2, 3):
        b.frame(seq)
    c0 = b.add(RecordType.CHUNK, _chunk(24, 3, 1, 3))
    index = b.add(RecordType.INDEX, fmt.encode_index([c0]))
    b.add(RecordType.MARK, fmt.encode_mark(1, fmt.MarkKind.NOTE, "between"))
    b.add(RecordType.TRAILER, fmt.encode_trailer(fmt.Trailer(index, 3, 3)))
    with_tail("rec-index-not-at-trailer.ibrec", b)
    meta_nan = (
        b'{"schema":"ironbird-rec-meta/1","max_packet_bytes":2048,"pid":NaN,'
        b'"nodes":{"a":Infinity,"b":[-Infinity]}}'
    )
    meta_deep = (
        b'{"schema":"ironbird-rec-meta/1","max_packet_bytes":2048,"nodes":'
        + b"[" * (fmt.MAX_META_DEPTH + 1)
        + b"]" * (fmt.MAX_META_DEPTH + 1)
        + b"}"
    )
    for name, text in {
        "rec-meta-non-finite-numbers.ibrec": meta_nan,
        "rec-meta-nested-past-cap.ibrec": meta_deep,
    }.items():
        b = Builder()
        b.out = bytearray(fmt.encode_preamble(b.nonce))
        b.raw(RecordType.META, text)
        b.stream(RX)
        b.source(0, "peer")
        b.frame(1)
        out[name] = bytes(b.out)
    b = _good(2)
    reserved = bytearray(fmt.encode_stream(RX))
    reserved[0:2] = struct.pack("<H", 0xFFFF)
    b.add(RecordType.STREAM, bytes(reserved))
    b.frame(3, stream=0xFFFF)
    b.frame(4)
    b.frame(5)
    with_tail("rec-stream-id-reserved.ibrec", b)
    b = _good(3)
    b.out[-6] ^= 0xFF  # inside the last record, which is complete
    out["rec-crc-fails-final-record.ibrec"] = bytes(b.out)

    # Mutation findings: rules the first corpus did not pin down.
    b = _good(2)
    odd = b.frame(3, data=b"x" * 21)  # 12 + 32 + 21 bytes: 7 bytes of padding
    b.frame(4)
    b.frame(5)
    padded = bytearray(b.out)
    padded[odd + 12 + 32 + 21] = 0x5A  # padding is outside the CRC
    out["rec-padding-not-zero.ibrec"] = bytes(padded)
    b = _good(3)
    broken = b.frame(4)
    b.out[broken + 8 : broken + 12] = b"\xff\xff\xff\xff"  # its CRC no longer matches
    b.frame(2)  # a chain of valid records whose sequence numbers are already used
    b.frame(3)
    b.frame(5)
    b.frame(6)
    out["rec-resync-stale-sequence-chain.ibrec"] = bytes(b.out)
    b = _good(0)
    b.frame(1)
    b.add(RecordType.CHUNK, _chunk(24, 1, 1, 1, mids=((9, 1), (3, 1))))  # not sorted
    b.frame(2)
    b.frame(3)
    with_tail("rec-chunk-mids-unsorted.ibrec", b)
    b = _good(0)
    b.frame(1)
    b.add(RecordType.MARK, fmt.encode_mark(1, fmt.MarkKind.NOTE, "x")[:9] + b"\x01\x00\x00" + b"x")
    b.frame(2)
    b.frame(3)
    with_tail("rec-mark-reserved-set.ibrec", b)
    out["rec-chunk-time-bounds-lie.ibrec"] = _closed(chunk_times=(5, 99))
    out["rec-trailer-last-seq-lies.ibrec"] = _closed(trailer_last_seq=9)
    out["rec-index-chunk-gap.ibrec"] = _closed(second_chunk_start_shift=8)
    return out


def _closed(
    *,
    chunk_times: tuple[int, int] = (5, 5),
    trailer_last_seq: int = 6,
    second_chunk_start_shift: int = 0,
) -> bytes:
    """A closed recording of two chunks of three frames, with one summary made to lie."""
    b = _good(0)
    for seq in (1, 2, 3):
        b.frame(seq)
    first = b.add(RecordType.CHUNK, _chunk(24, 3, 1, 3, times=chunk_times))
    start = len(b.out)
    for seq in (4, 5, 6):
        b.frame(seq)
    second = b.add(RecordType.CHUNK, _chunk(start + second_chunk_start_shift, 3, 4, 6))
    index = b.add(RecordType.INDEX, fmt.encode_index([first, second]))
    b.add(RecordType.TRAILER, fmt.encode_trailer(fmt.Trailer(index, 6, trailer_last_seq)))
    return bytes(b.out)


def _chunk(
    start: int,
    frames: int,
    first: int,
    last: int,
    *,
    times: tuple[int, int] = (5, 5),
    mids: tuple[tuple[int, int], ...] | None = None,
) -> bytes:
    table = ((0x890, frames),) if mids is None else mids
    return fmt.encode_chunk(
        fmt.Chunk(
            start, first, last, times[0], times[1], 0, 0, frames, frames * fmt.record_size(52),
            0, 0, table,
        )
    )  # fmt: skip


class Outcome(NamedTuple):
    """What reading a corpus file must come to (asserted for every ``rec-*`` file).

    ``status`` is the report status of ``ib rec check`` (``None``: the file cannot be read and
    ``open_code`` is the error), ``frames`` the frames read before the damage, ``offset`` where
    the first damage starts, ``reason`` text the first damage (or finding, or error) must contain,
    and ``check_exit`` the exit code of ``ib rec check``.
    """

    status: str | None
    frames: int
    offset: int | None
    reason: str
    check_exit: int
    open_code: Code | None = None


EXPECT: dict[str, Outcome] = {
    "rec-bit-flip-in-envelope": Outcome(
        "damaged", 7, 456, "envelope flags or reserved bytes are not zero", 1
    ),
    "rec-bit-flip-in-padding": Outcome("damaged", 7, 392, "FRAME record fails its CRC", 1),
    "rec-blob-bad-digest": Outcome("damaged", 4, 392, "BLOB content does not match its SHA-", 1),
    "rec-blob-bad-kind": Outcome("damaged", 4, 392, "BLOB kind is not", 1),
    "rec-body-len-below-minimum": Outcome("damaged", 4, 392, "FRAME body length", 1),
    "rec-body-len-huge": Outcome("damaged", 4, 392, "FRAME body length", 1),
    "rec-body-len-lies-high": Outcome("damaged", 4, 392, "FRAME record fails its CRC", 1),
    "rec-body-len-lies-low": Outcome("damaged", 4, 392, "FRAME record fails its CRC", 1),
    "rec-bytes-after-trailer": Outcome("damaged", 3, 496, "bytes after the trailer", 1),
    "rec-chunk-lies": Outcome("unclosed", 3, None, "chunk at", 1),
    "rec-chunk-start-beyond-file": Outcome("damaged", 3, 328, "CHUNK starts at", 1),
    "rec-concatenated": Outcome("damaged", 3, 616, "bytes after the trailer", 1),
    "rec-crc-fails-final-record": Outcome("torn", 2, 392, "FRAME record fails its CRC", 0),
    "rec-dense-fake-envelopes": Outcome(
        "damaged",
        1,
        328,
        "envelope flags or reserved bytes are not zero (the scan for a valid record gave up)",
        1,
    ),
    "rec-empty": Outcome(
        None, 0, None, "is not an ironbird recording (the file is empty", 1, Code.REC_FORMAT
    ),
    "rec-ends-after-preamble": Outcome(
        None,
        0,
        None,
        "ends before its first record is complete; it holds no frames",
        1,
        Code.REC_TORN,
    ),
    "rec-ends-inside-meta": Outcome(
        None,
        0,
        None,
        "ends before its first record is complete; it holds no frames",
        1,
        Code.REC_TORN,
    ),
    "rec-fake-chains-wrong-nonce": Outcome(
        "torn", 1, 328, "envelope flags or reserved bytes are not zero", 0
    ),
    "rec-forged-in-frame-data": Outcome("damaged", 3, 264, "FRAME record fails its CRC", 1),
    "rec-frame-garbage-and-stale": Outcome(
        "damaged", 4, 448, "frame flags contradict each other", 1
    ),
    "rec-frame-garbage-on-udp": Outcome(
        "damaged", 4, 392, "frame has serial flags on a stream that is not serial", 1
    ),
    "rec-frame-mid-without-flag": Outcome(
        "damaged", 4, 392, "frame has a MID but no HAS_MID flag", 1
    ),
    "rec-frame-orig-len-lies-high": Outcome("damaged", 4, 392, "frame original length", 1),
    "rec-frame-orig-len-lies-low": Outcome("damaged", 4, 392, "frame original length", 1),
    "rec-frame-over-cap": Outcome("damaged", 4, 392, "FRAME body length", 1),
    "rec-frame-reserved-set": Outcome("damaged", 4, 392, "frame reserved field is not zero", 1),
    "rec-frame-seq-repeats": Outcome("damaged", 4, 392, "frame sequence", 1),
    "rec-frame-seq-zero": Outcome("damaged", 4, 392, "frame sequence", 1),
    "rec-frame-truncated-within-cap": Outcome(
        "damaged", 4, 392, "frame is marked truncated but its original length", 1
    ),
    "rec-frame-truncated-without-excess": Outcome(
        "damaged", 4, 392, "frame is marked truncated but its original length", 1
    ),
    "rec-frame-undefined-source": Outcome("damaged", 4, 392, "frame names undefined source", 1),
    "rec-frame-undefined-stream": Outcome("damaged", 4, 392, "frame names undefined stream", 1),
    "rec-frame-unknown-flags": Outcome("damaged", 4, 392, "frame has unknown flag bits", 1),
    "rec-gap-undefined-stream": Outcome("damaged", 4, 392, "GAP names undefined stream", 1),
    "rec-index-count-lies": Outcome(
        "damaged", 4, 392, "INDEX length disagrees with its entry count", 1
    ),
    "rec-index-not-at-trailer": Outcome("inconsistent", 3, None, "the INDEX at", 1),
    "rec-index-not-increasing": Outcome("damaged", 4, 392, "INDEX offsets do not increase", 1),
    "rec-magic-only": Outcome(None, 0, None, "ends inside its preamble", 1, Code.REC_TORN),
    "rec-meta-array": Outcome(
        None, 0, None, "is unusable: META is not a JSON object", 1, Code.REC_DAMAGED
    ),
    "rec-meta-cap-too-big": Outcome(
        None,
        0,
        None,
        "is unusable: META max_packet_bytes must be an integer in",
        1,
        Code.REC_DAMAGED,
    ),
    "rec-meta-deep-nesting": Outcome(
        None, 0, None, "is unusable: META is not valid JSON (RecursionError)", 1, Code.REC_DAMAGED
    ),
    "rec-meta-huge-int": Outcome(
        None, 0, None, "is unusable: META is not valid JSON (ValueError)", 1, Code.REC_DAMAGED
    ),
    "rec-meta-negative-cap": Outcome(
        None,
        0,
        None,
        "is unusable: META max_packet_bytes must be an integer in",
        1,
        Code.REC_DAMAGED,
    ),
    "rec-meta-nested-past-cap": Outcome(
        None, 0, None, "is unusable: META is nested deeper than", 1, Code.REC_DAMAGED
    ),
    "rec-meta-non-finite-numbers": Outcome("unclosed", 1, None, "", 0),
    "rec-meta-not-json": Outcome(
        None, 0, None, "is unusable: META is not valid JSON (JSONDecodeError)", 1, Code.REC_DAMAGED
    ),
    "rec-meta-oversize": Outcome(
        None, 0, None, "is damaged (META body length", 1, Code.REC_DAMAGED
    ),
    "rec-preamble-flags": Outcome(None, 0, None, "is recording format", 1, Code.REC_VERSION),
    "rec-resync-bodies-past-eof": Outcome("torn", 0, 264, "ends inside a type", 0),
    "rec-short-preamble": Outcome(None, 0, None, "ends inside its preamble", 1, Code.REC_TORN),
    "rec-stale-index-mid-file": Outcome("inconsistent", 6, None, "the INDEX at", 1),
    "rec-stream-id-reserved": Outcome("damaged", 4, 392, "STREAM id", 1),
    "rec-stream-redefined": Outcome("damaged", 4, 448, "is defined twice, differently", 1),
    "rec-stream-string-overruns": Outcome("damaged", 4, 392, "STREAM body length", 1),
    "rec-text-file": Outcome(None, 0, None, "is not an ironbird recording", 1, Code.REC_FORMAT),
    "rec-torn-inside-last-record": Outcome("torn", 2, 392, "ends inside a FRAME record", 0),
    "rec-trailer-mid-file": Outcome("damaged", 6, 496, "bytes after the trailer", 1),
    "rec-unknown-critical-type": Outcome(None, 0, None, "record type", 1, Code.REC_VERSION),
    "rec-unknown-skippable-type": Outcome("unclosed", 3, None, "", 0),
    "rec-version-2": Outcome(None, 0, None, "is recording format", 1, Code.REC_VERSION),
    "rec-wrong-nonce-meta": Outcome(
        None,
        0,
        None,
        "is damaged (META record fails its CRC); the recording cannot be read",
        1,
        Code.REC_DAMAGED,
    ),
    "rec-zero-filled-tail": Outcome("torn", 3, 456, "record fails its CRC", 0),
    "rec-padding-not-zero": Outcome("damaged", 4, 392, "FRAME record padding is not zero", 1),
    "rec-resync-stale-sequence-chain": Outcome("damaged", 5, 456, "FRAME record fails its CRC", 1),
    "rec-chunk-mids-unsorted": Outcome("damaged", 3, 328, "CHUNK MID table is not sorted", 1),
    "rec-mark-reserved-set": Outcome("damaged", 3, 328, "MARK reserved bytes are not zero", 1),
    "rec-chunk-time-bounds-lie": Outcome("inconsistent", 6, None, "has sequence or time bounds", 1),
    "rec-trailer-last-seq-lies": Outcome("inconsistent", 6, None, "the trailer's last sequence", 1),
    "rec-index-chunk-gap": Outcome("inconsistent", 6, None, "says it starts at", 1),
}


def main() -> None:
    CORPUS.mkdir(parents=True, exist_ok=True)
    for name, data in cases().items():
        (CORPUS / name).write_bytes(data)
    print(f"wrote {len(cases())} recordings to {CORPUS}")


if __name__ == "__main__":
    main()
