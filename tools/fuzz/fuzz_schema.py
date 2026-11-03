"""Coverage-guided, structure-aware fuzzing (atheris) of the schema validators.

Usage: python tools/fuzz/fuzz_schema.py [-max_total_time=N]

Byte-level mutation of a JSON or TOML file mostly breaks the syntax before a validator sees it.
Here the input drives mutations of an already-parsed, valid document instead: the first byte picks
the document family (the CCDD JSON model, the config, the header profiles) and the rest picks a
few edits - replace a value (with extreme integers, non-finite floats, odd strings, containers
and names taken from the document itself), delete a key, add a key, duplicate a subtree - which
the validator then judges.

Invariant: ``parse_model``, ``parse_config`` and ``parse_profile`` return a result or raise only a
coded ``IronbirdError`` of their own domain with a one-line user text (never an ``AssertionError``,
``KeyError``, ``TypeError``, ...). What loads is used like a real one: a model drives a codec
(encode every message, decode it back), a profile packs and unpacks.
"""

from __future__ import annotations

import copy
import json
import math
import tomllib
from pathlib import Path
from typing import Any

import atheris
from fuzzcommon import EXAMPLES, run, scratch_dir, seed_corpus

with atheris.instrument_imports():
    from fuzz_ccdd import COMMAND_PROFILE, TELEMETRY_PROFILE
    from fuzz_ccdd import drive as drive_codec
    from fuzz_header_profile import use as use_profile

    from ironbird import header
    from ironbird.ccdd.json_adapter import parse_model
    from ironbird.codec import Codec
    from ironbird.config import parse_config
    from ironbird.errors import Code, IronbirdError

SCRATCH = scratch_dir("schema")
MAX_EDITS = 5
CODES = {
    "ccdd": {Code.CCD_FILE, Code.CCD_SCHEMA, Code.CCD_LAYOUT},
    "config": {Code.CFG_FILE, Code.CFG_SYNTAX, Code.CFG_SCHEMA, Code.CFG_REF, Code.CFG_LINK},
    "profile": {Code.HDR_FILE, Code.HDR_SCHEMA, Code.HDR_PACK},
}
POOL: tuple[Any, ...] = (
    None,
    True,
    False,
    0,
    1,
    -1,
    2,
    7,
    8,
    255,
    256,
    4096,
    65535,
    65536,
    2**31,
    2**32,
    2**63,
    2**64,
    -(2**63),
    10**30,
    0.5,
    -0.0,
    1e308,
    math.inf,
    -math.inf,
    math.nan,
    "",
    "x",
    " ",
    "\n",
    "name",
    "uint8",
    "uint16",
    "uint32[]",
    "char[8]",
    "char[]",
    "bytes[4]",
    "0x10",
    "0x",
    "-1",
    "a" * 300,
    "\x00",
    "ml",
    "little",
    "big",
    "telemetry",
    "command",
    [],
    {},
    [1, 2],
    ["a", "b"],
    {"a": 1},
    [[]],
    [{}],
)


def paths_of(doc: Any, prefix: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    """Every path to a value inside ``doc`` (not the root)."""
    found: list[tuple[Any, ...]] = []
    if isinstance(doc, dict):
        children = list(doc.items())
    elif isinstance(doc, list):
        children = list(enumerate(doc))
    else:
        return found
    for key, value in children:
        found.append((*prefix, key))
        found += paths_of(value, (*prefix, key))
    return found


def strings_of(doc: Any) -> list[str]:
    """Every string in ``doc``, keys included: names a mutation can point at."""
    found: list[str] = []
    if isinstance(doc, dict):
        for key, value in doc.items():
            found.append(str(key))
            found += strings_of(value)
    elif isinstance(doc, list):
        for value in doc:
            found += strings_of(value)
    elif isinstance(doc, str):
        found.append(doc)
    return found


def walk(doc: Any, path: tuple[Any, ...]) -> Any:
    for step in path:
        doc = doc[step]
    return doc


def mutate(fdp: atheris.FuzzedDataProvider, base: Any) -> Any:
    doc = copy.deepcopy(base)
    names = strings_of(base)
    for _ in range(fdp.ConsumeIntInRange(1, MAX_EDITS)):
        paths = paths_of(doc)
        if not paths:
            break
        path = paths[fdp.ConsumeIntInRange(0, len(paths) - 1)]
        parent, key = walk(doc, path[:-1]), path[-1]
        match fdp.ConsumeIntInRange(0, 5):
            case 0 | 1:
                parent[key] = copy.deepcopy(POOL[fdp.ConsumeIntInRange(0, len(POOL) - 1)])
            case 2:
                parent[key] = names[fdp.ConsumeIntInRange(0, len(names) - 1)] if names else "x"
            case 3:
                del parent[key]
            case 4:
                inner = parent[key]
                value = copy.deepcopy(POOL[fdp.ConsumeIntInRange(0, len(POOL) - 1)])
                if isinstance(inner, dict):
                    inner[names[fdp.ConsumeIntInRange(0, len(names) - 1)] if names else "k"] = value
                elif isinstance(inner, list):
                    inner.append(value)
            case _:
                if isinstance(parent, list):
                    parent.append(copy.deepcopy(parent[key]))
                else:
                    parent[fdp.ConsumeUnicodeNoSurrogates(6)] = copy.deepcopy(parent[key])
    return doc


def families() -> list[tuple[str, Any]]:
    out: list[tuple[str, Any]] = []
    for path in sorted((EXAMPLES / "ccdd").glob("*.json")):
        out.append(("ccdd", json.loads(path.read_text())))
    for path in sorted(EXAMPLES.glob("*.toml")):
        out.append(("config", tomllib.loads(path.read_text())))
    for path in sorted((EXAMPLES / "headers").glob("*.toml")):
        out.append(("profile", tomllib.loads(path.read_text())))
    return out


BASES = families()


def one_input(data: bytes) -> None:
    if len(data) < 2:
        return
    family, base = BASES[data[0] % len(BASES)]
    doc = mutate(atheris.FuzzedDataProvider(data[1:]), base)
    try:
        match family:
            case "ccdd":
                model = parse_model(doc, "fuzz.json")
                drive_codec(Codec(model, COMMAND_PROFILE, TELEMETRY_PROFILE))
            case "config":
                parse_config(doc, Path("fuzz.toml"))
            case _:
                use_profile(header.parse_profile(doc, "fuzz.toml"))
    except IronbirdError as err:
        if family == "ccdd" and not err.code.value.startswith(("IB-CCD-", "IB-ENC-", "IB-HDR-")):
            raise
        if family != "ccdd":
            assert err.code in CODES[family], err.code
        assert "\n" not in err.user_line(), err.user_line()


def main() -> None:
    seeds = seed_corpus(
        SCRATCH, [], [bytes([i]) + bytes(range(i, i + 40)) for i in range(len(BASES))]
    )
    run(one_input, seeds, max_len=256)


if __name__ == "__main__":
    main()
