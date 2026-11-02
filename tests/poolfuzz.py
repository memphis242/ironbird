"""Adversarial recordings for the pool differential (``test_export_pool_fuzz.py``): random mixes of
frames, sequence resets, runs of records without frames, definitions that come late or change,
large skippable records hiding forged chains of valid records, raw damage, undefined streams, bad
CRCs and a torn end."""

from __future__ import annotations

import dataclasses
import random
from pathlib import Path

from ironbird.frame import GapReason
from ironbird.recording import format as fmt
from ironbird.recording.format import MarkKind, RecordType
from recutil import SERIAL, Builder
from replayutil import STEP_NS, T0, crafted, good_blobs, hk


def forged(builder: Builder, seq: int, n: int, stream: int = 0) -> bytes:
    """n valid FRAME records (right CRC seed), as bytes to hide in a payload."""
    out = b""
    for i in range(n):
        out += fmt.encode_record(
            RecordType.FRAME,
            builder.frame_body(
                seq + i,
                data=hk(seq + i),
                stream=stream,
                source=0,
                mid=0x890,
                time_ns=T0 + (seq + i) * STEP_NS,
            ),
            builder.seed,
        )
    return out


def build(path: Path, rng: random.Random, steps: int = 400) -> Path:
    b = crafted(good_blobs(), frames=0)
    seq = 0
    serial = False
    for _ in range(steps):
        r = rng.random()
        t = T0 + (seq + 1) * STEP_NS
        if r < 0.55:
            n = rng.randrange(1, 30)
            for _ in range(n):
                seq += 1
                stream = 2 if serial and rng.random() < 0.2 else rng.choice((0, 0, 0, 1))
                src = 1 if serial and rng.random() < 0.3 else rng.choice((0, 0xFFFF))
                b.frame(
                    seq,
                    data=hk(seq),
                    stream=stream,
                    source=src,
                    mid=0x890,
                    time_ns=T0 + seq * STEP_NS,
                )
        elif r < 0.62:
            # sequence goes back (or repeats)
            seq = max(1, seq - rng.choice((0, 1, 5, 50, 1000)))
            b.frame(seq, data=hk(seq), stream=0, source=0, mid=0x890, time_ns=t)
        elif r < 0.67:
            for _ in range(rng.randrange(20, 200)):
                b.add(
                    RecordType.MARK,
                    fmt.encode_mark(t, MarkKind.SIGNAL, "m" * rng.randrange(0, 200)),
                )
        elif r < 0.70 and not serial:
            serial = True
            b.stream(SERIAL)
            b.source(1, "other-peer")
        elif r < 0.72:
            b.stream(dataclasses.replace(SERIAL, link="uart2") if rng.random() < 0.5 else SERIAL)
        elif r < 0.75:
            gap = fmt.Gap(
                rng.choice((0, 2, 0xFFFF)),
                rng.choice(tuple(GapReason)),
                rng.randrange(0, 9),
                t - STEP_NS,
                t,
                seq,
                seq,
            )
            b.add(RecordType.GAP, fmt.encode_gap(gap))
        elif r < 0.82:
            # a large skippable record hiding a forged chain (aligned inside the file)
            pad = rng.randrange(0, 600) * 8
            body = (
                bytes(4)
                + bytes(pad)
                + forged(b, seq + rng.choice((1, 100000)), rng.randrange(2, 30))
            )
            body += bytes(rng.randrange(0, 9000))
            b.add(0x80 + rng.randrange(0, 0x7F), body)
        elif r < 0.86:
            # a frame whose payload holds a forged chain
            seq += 1
            data = bytes(4) + forged(b, seq + 1, 4)
            data = data[:2048]
            b.frame(seq, data=data, stream=0, source=0, mid=0x7FF, time_ns=t)
        elif r < 0.88:
            # raw damage: random bytes, aligned or not
            b.out += rng.randbytes(rng.choice((8, 16, 100, 4000)))
            while len(b.out) % 8:
                b.out.append(0)
        elif r < 0.885:
            b.raw(0x1F, b"\x00" * 16)  # unknown critical
        elif r < 0.90:
            # a frame of an undefined stream
            seq += 1
            b.frame(seq, data=hk(seq), stream=7, source=0, mid=0x890, time_ns=t)
        elif r < 0.92:
            # damaged CRC
            seq += 1
            off = b.frame(seq, data=hk(seq), stream=0, source=0, mid=0x890, time_ns=t)
            b.out[off + 8] ^= 0x55
        else:
            b.source(rng.choice((0, 1, 3)), rng.choice(("peer", "other-peer", "x")))
    if rng.random() < 0.3:
        del b.out[len(b.out) - rng.randrange(1, 40) :]
    return b.write(path)
