"""Compatibility check of a working tree against another ironbird tree (usually `main`).

Dev tool, not part of CI. Two checks, each run once under the other tree's `src` and once under
this tree's:

1. The golden vectors of the shipped header profiles (`tests/golden_2i.py`) come out byte for
   byte the same, and equal `tests/golden/pre2i_header_vectors.json`.
2. A differential over N seeded random header profiles (pack, unpack, check and `peek_mid` of
   valid, truncated, extended and bit-flipped packets). Every profile the other tree accepts
   must behave identically here; a profile it refuses may be accepted here (a new feature), and
   anything else is a regression.

    python tools/dev/diff_vs_main.py /path/to/main-checkout [--profiles 20000] [--seed 1]

Exit status 0: compatible; 1: a difference; 2: usage error.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import random
import subprocess
import sys
import tempfile
from collections import Counter
from collections.abc import Sequence
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[2]
GOLDEN = REPO / "tests" / "golden" / "pre2i_header_vectors.json"


def make_profile(rng: random.Random) -> dict[str, Any]:
    """A random profile document made only of keys that exist in every tree: some valid, some
    deliberately broken."""
    fields: list[dict[str, Any]] = []
    bits = 0
    for i in range(rng.randint(1, 9)):
        kind = rng.choice(
            ["fixed", "input", "len", "seq", "mid", "mid", "ck", "ts", "tss", "fixed", "bad"]
        )
        width = rng.choice([1, 2, 3, 4, 5, 7, 8, 8, 11, 16, 16, 24, 32, 64])
        f: dict[str, Any] = {"name": f"f{i}", "bits": width}
        if kind == "fixed":
            f["value"] = rng.getrandbits(width)
            if rng.random() < 0.2:
                f["check"] = False
        elif kind == "input" and rng.random() < 0.8:
            f["default"] = rng.getrandbits(width)
        elif kind == "len":
            f.update(computed="length", bias=rng.randint(-10, 3))
        elif kind == "seq":
            f["computed"] = "sequence"
        elif kind == "mid":
            f["computed"] = "from_mid"
        elif kind == "ck":
            f.update(
                computed="checksum",
                algorithm=rng.choice(["xor8_cfs", "crc16_ccitt"]),
                bits=rng.choice([8, 16]),
            )
        elif kind == "ts":
            f["computed"] = "time_seconds"
        elif kind == "tss":
            f["computed"] = "time_subseconds"
        elif kind == "bad":
            f[rng.choice(["check", "bias", "default", "value"])] = rng.choice(
                [True, -1, 1 << 70, "x", 3]
            )
        fields.append(f)
        bits += f["bits"]
    if bits % 8 and rng.random() < 0.9:
        fields.append({"name": "pad", "bits": (-bits) % 8, "value": 0})
    doc: dict[str, Any] = {
        "name": "p",
        "kind": rng.choice(["telemetry", "command"]),
        "fields": fields,
    }
    if doc["kind"] == "command" and rng.random() < 0.9:
        fields.append({"name": "fc", "bits": 8})
        doc["function_code_field"] = "fc"
    names = [f["name"] for f in fields]
    mids = [f["name"] for f in fields if f.get("computed") == "from_mid"]
    if mids and rng.random() < 0.95:
        start = names.index(mids[0])
        run = names[start : start + rng.randint(1, 3)]
        doc["mid_fields"] = run if rng.random() < 0.9 else list(reversed(run))
    return doc


def worker(seed: str, count: int, out: Path) -> None:
    """Behaviour of the `ironbird.header` importable in this process over `count` profiles."""
    from ironbird import header as h  # noqa: PLC0415 - the tree is chosen by PYTHONPATH
    from ironbird.errors import DecodeError, IronbirdError  # noqa: PLC0415

    rng = random.Random()
    lines: list[str] = []
    for n in range(count):
        rng.seed(f"{seed}-{n}")
        lines.append(f"# {n}")
        doc = make_profile(rng)
        try:
            p = h.parse_profile(doc, "x")
        except IronbirdError as exc:
            lines.append(f"R {exc.code.value}")
            continue
        except Exception as exc:  # any other exception is a finding too
            lines.append(f"X {type(exc).__name__}")
            continue
        lines.append(f"A {p.size_bytes} {p.mid_bits}")
        for _ in range(4):
            payload = bytes(rng.getrandbits(8) for _ in range(rng.randint(0, 12)))
            mid = None
            if p.mid_fields:
                mid = 0
                for name in p.mid_fields:
                    f = p.by_name[name]
                    part = f.value if f.kind is h.FieldKind.FIXED else rng.getrandbits(f.bits)
                    mid = (mid << f.bits) | (part or 0)
            inputs = {f.name: rng.getrandbits(f.bits) for f in p.input_fields if rng.random() < 0.7}
            try:
                pkt = h.pack(
                    p,
                    payload,
                    mid=mid,
                    inputs=inputs,
                    sequence=rng.getrandbits(16),
                    now=rng.uniform(0, 4e9),
                    tamper=None,
                )
                lines.append("P " + pkt.hex())
            except IronbirdError as exc:
                lines.append(f"PR {exc.code.value} {exc}")
                pkt = bytes(rng.getrandbits(8) for _ in range(p.size_bytes + 4))
            flip = rng.randrange(len(pkt) or 1)
            bit = 1 << rng.randrange(8)
            flipped = bytes(b ^ bit if i == flip else b for i, b in enumerate(pkt))
            for data in (pkt, pkt[:-1], pkt + b"\0", flipped):
                r = h.unpack(p, data, max_size=65535)
                if isinstance(r, DecodeError):
                    lines.append(f"E {r.code.value} {r.offset} {r.reason} {r.mid}")
                else:
                    lines.append(f"H {sorted(r.values.items())} {r.mid} {r.size} {r.sequence}")
                lines.append(f"K {h.peek_mid(p, data)}")
    out.write_text("\n".join(lines))
    digest = hashlib.sha256("\n".join(lines).encode()).hexdigest()
    print(f"{len(lines)} lines {digest}")


def run(tree: Path, args: Sequence[str]) -> None:
    env = {**os.environ, "PYTHONPATH": str(tree / "src")}
    subprocess.run([sys.executable, *args], check=True, env=env, cwd=REPO)  # noqa: S603


def blocks(path: Path) -> list[list[str]]:
    result: list[list[str]] = []
    for line in path.read_text().split("\n"):
        if line.startswith("# "):
            result.append([])
        elif result:
            result[-1].append(line)
    return result


def main(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("other", type=Path, help="the other checkout (its src/ is used)")
    parser.add_argument("--profiles", type=int, default=20000)
    parser.add_argument("--seed", default="1")
    parser.add_argument(
        "--worker", nargs=3, metavar=("SEED", "COUNT", "OUT"), help=argparse.SUPPRESS
    )
    args = parser.parse_args(argv)
    if args.worker:
        worker(args.worker[0], int(args.worker[1]), Path(args.worker[2]))
        return 0
    if not (args.other / "src" / "ironbird").is_dir():
        parser.error(f"{args.other} has no src/ironbird")

    ok = True
    with tempfile.TemporaryDirectory(prefix="ib-diff-") as tmp:
        scratch = Path(tmp)
        for label, tree in (("other", args.other), ("this", REPO)):
            run(tree, [str(REPO / "tests" / "golden_2i.py"), str(scratch / f"golden-{label}.json")])
        theirs = (scratch / "golden-other.json").read_bytes()
        ours = (scratch / "golden-this.json").read_bytes()
        same = theirs == ours == GOLDEN.read_bytes()
        print(f"golden vectors: {'equal' if same else 'DIFFERENT'} ({len(ours)} bytes)")
        ok &= same

        for label, tree in (("other", args.other), ("this", REPO)):
            run(
                tree,
                [
                    str(Path(__file__).resolve()),
                    str(args.other),
                    "--worker",
                    args.seed,
                    str(args.profiles),
                    str(scratch / f"diff-{label}.txt"),
                ],
            )
        old, new = blocks(scratch / "diff-other.txt"), blocks(scratch / "diff-this.txt")
        assert len(old) == len(new) == args.profiles
        identical = changed = 0
        became: Counter[str] = Counter()
        for index, (a, b) in enumerate(zip(old, new, strict=True)):
            if a == b:
                identical += 1
            elif a[0].startswith("R IB-HDR-002") and b[0].startswith("A "):
                became[a[0]] += 1  # refused there, accepted here: a new feature
            else:
                changed += 1
                print(f"REGRESSION in profile {index}: {a[:2]} -> {b[:2]}")
        print(
            f"{args.profiles} profiles: {identical} identical, "
            f"{sum(became.values())} newly accepted ({dict(became)}), {changed} regressions"
        )
        ok &= changed == 0
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
