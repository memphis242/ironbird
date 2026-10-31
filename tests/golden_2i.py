"""Golden vectors of the shipped header profiles, recorded before Stage 2i.

``generate()`` encodes every message of the shipped CCDD files with the shipped
``ccsds_v1_cfs_*`` profiles, then decodes, checks and unpacks a deterministic population of
those packets, truncations, extensions, bit flips and the malformed-packet corpus, and reduces
every result to one canonical line. ``tests/golden/pre2i_header_vectors.json`` holds the output
of the code as it stood before 2i (recorded by ``python tests/golden_2i.py``); the test compares
the current code with it, so a shipped profile that encodes or decodes differently fails.

Only the interfaces that exist both before and after 2i are used, and the canonical form leaves
out anything 2i added (``Header.time_ns``).
"""

from __future__ import annotations

import hashlib
import json
import random
import sys
from pathlib import Path
from typing import Any

from ironbird import header
from ironbird.ccdd.loader import load_ccdd
from ironbird.ccdd.model import CcddModel, MessageKind
from ironbird.codec import Codec, DecodedPacket
from ironbird.errors import DecodeError

REPO = Path(__file__).resolve().parent.parent
EXAMPLES = REPO / "examples"
CORPUS = Path(__file__).resolve().parent / "corpus" / "malformed"
GOLDEN = Path(__file__).resolve().parent / "golden" / "pre2i_header_vectors.json"
CLOCK = 1_000_000.25
SEED = 20260217
MUTANTS_PER_PACKET = 40


def _recorded(name: str, model: CcddModel) -> CcddModel:
    """``model`` limited to the messages the golden file recorded, in file order. Messages added
    to the shipped files later (Stage 2j's variable-length ones) are not part of the proof: they
    would also shift the seeded mutants of every message after them."""
    if not GOLDEN.exists():
        return model
    keep = {
        key.split("/", 1)[1]
        for key in json.loads(GOLDEN.read_text())["encodes"]
        if key.startswith(f"{name}/")
    }
    kept = tuple(m for m in model.messages if m.name in keep)
    return CcddModel.build(model.target, model.source, kept)


def codecs() -> dict[str, Codec]:
    cmd = header.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml")
    tlm = header.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml")
    return {
        name: Codec(
            _recorded(name, load_ccdd(EXAMPLES / "ccdd" / f"{name}.json")),
            cmd,
            tlm,
            clock=lambda: CLOCK,
        )
        for name in ("mock", "cfs")
    }


def _zero_args(model: CcddModel, name: str) -> dict[str, object]:
    msg = model.by_name[name]
    out: dict[str, object] = {}
    for f in msg.fields:
        match f.type.type_class.value:
            case "bytes":
                out[f.name] = bytes(f.type.size)
            case "char":
                out[f.name] = ""
            case "float":
                out[f.name] = 0.0
            case _:
                out[f.name] = 0
    return out


def canonical(result: object) -> str:
    """One line that determines what a decode result says, without anything added in 2i."""
    if isinstance(result, DecodeError):
        return f"E {result.code.value} {result.offset} {result.reason} {result.mid}"
    if isinstance(result, DecodedPacket):
        h = result.header
        values = sorted(h.values.items())
        return f"P {result.message.name} {values} {h.mid} {h.size} {h.sequence} {result.values!r}"
    if isinstance(result, header.Header):
        values = sorted(result.values.items())
        return f"H {values} {result.mid} {result.size} {result.sequence}"
    return f"N {result!r}"


def encodes() -> dict[str, str]:
    """Every message of every shipped CCDD, as the hex of its first and second encode."""
    out: dict[str, str] = {}
    for cname, codec in codecs().items():
        for msg in codec.model.messages:
            if msg.kind is MessageKind.COMMAND:
                one = codec.encode_command(msg.name, _zero_args(codec.model, msg.name))
                two = codec.encode_command(msg.name, _zero_args(codec.model, msg.name))
            else:
                one = codec.encode_telemetry(msg.name)
                two = codec.encode_telemetry(msg.name)
            out[f"{cname}/{msg.name}"] = f"{one.data.hex()} {two.data.hex()}"
    return out


def _population(codec: Codec, rng: random.Random) -> list[tuple[MessageKind, bytes]]:
    packets: list[tuple[MessageKind, bytes]] = []
    for msg in codec.model.messages:
        if msg.kind is MessageKind.COMMAND:
            valid = codec.encode_command(msg.name, _zero_args(codec.model, msg.name)).data
        else:
            valid = codec.encode_telemetry(msg.name).data
        kinds = [msg.kind, MessageKind.TELEMETRY, MessageKind.COMMAND]
        for kind in kinds:
            packets.append((kind, valid))
        for cut in range(0, min(len(valid), 40) + 1):
            packets.append((msg.kind, valid[:cut]))
        packets.append((msg.kind, valid + b"\x00"))
        packets.append((msg.kind, valid + bytes(rng.getrandbits(8) for _ in range(9))))
        for _ in range(MUTANTS_PER_PACKET):
            buf = bytearray(valid)
            for _ in range(1 + rng.randrange(3)):
                match rng.randrange(3):
                    case 0 if buf:
                        buf[rng.randrange(len(buf))] ^= 1 << rng.randrange(8)
                    case 1 if buf:
                        buf[rng.randrange(len(buf))] = rng.getrandbits(8)
                    case _:
                        buf.append(rng.getrandbits(8))
            packets.append((rng.choice([MessageKind.COMMAND, MessageKind.TELEMETRY]), bytes(buf)))
    return packets


def results() -> list[str]:
    lines: list[str] = []
    rng = random.Random(SEED)
    for cname, codec in codecs().items():
        population = _population(codec, rng)
        for path in sorted(CORPUS.glob("packet-*.bin")):
            data = path.read_bytes()
            population.append((MessageKind.COMMAND, data))
            population.append((MessageKind.TELEMETRY, data))
        for kind, data in population:
            profile = codec.profile(kind)
            lines.append(f"{cname} {kind.value} {data.hex()}")
            lines.append(canonical(codec.decode(kind, data)))
            lines.append(canonical(codec.check(kind, data)))
            lines.append(canonical(header.unpack(profile, data)))
            lines.append(canonical(header.check(profile, data)))
            lines.append(canonical(header.peek_mid(profile, data)))
    return lines


def generate() -> dict[str, Any]:
    lines = results()
    digest = hashlib.sha256("\n".join(lines).encode()).hexdigest()
    return {
        "encodes": encodes(),
        "result_lines": len(lines),
        "result_sha256": digest,
        "result_sample": lines[:12] + lines[-6:],
    }


if __name__ == "__main__":
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else GOLDEN
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(generate(), indent=1, sort_keys=True) + "\n")
    print(f"wrote {out}")
