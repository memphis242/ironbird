"""The checked-in corpus of malformed recordings (``tests/corpus/malformed/rec-*.ibrec``).

Each case is built deterministically here (fixed nonce), so a test can prove the checked-in files
are exactly these, and a new bug adds a case and regenerates: ``python tests/rec_corpus.py``.
"""

from __future__ import annotations

import json
import struct
import zlib
from pathlib import Path

from ironbird.ccdd.model import MessageKind
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
    return out


def main() -> None:
    CORPUS.mkdir(parents=True, exist_ok=True)
    for name, data in cases().items():
        (CORPUS / name).write_bytes(data)
    print(f"wrote {len(cases())} recordings to {CORPUS}")


if __name__ == "__main__":
    main()
