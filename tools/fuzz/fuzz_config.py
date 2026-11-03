"""Coverage-guided fuzzing (atheris) of the config file.

Usage: python tools/fuzz/fuzz_config.py [-max_total_time=N]

Invariant: whatever bytes a config file holds, ``load_config`` returns a ``SystemConfig`` or raises
only a coded ``IronbirdError`` (``IB-CFG-*``) whose user line is one line, quickly; a config that
loads reports the digest of the bytes that were parsed. Seeds are the checked-in ``cfg-*`` corpus
plus the shipped example configs.
"""

from __future__ import annotations

import hashlib

import atheris
from fuzzcommon import EXAMPLES, run, scratch_dir, seed_corpus

with atheris.instrument_imports():
    from ironbird.config import load_config
    from ironbird.errors import Code, IronbirdError

SCRATCH = scratch_dir("config")
TARGET = SCRATCH / "ironbird.toml"
_CFG_CODES = {Code.CFG_FILE, Code.CFG_SYNTAX, Code.CFG_SCHEMA, Code.CFG_REF, Code.CFG_LINK}


def one_input(data: bytes) -> None:
    TARGET.write_bytes(data)
    try:
        config = load_config(TARGET)
    except IronbirdError as err:
        assert err.code in _CFG_CODES, err.code
        assert "\n" not in err.user_line(), err.user_line()
        return
    assert config.sha256 == hashlib.sha256(data).hexdigest()


def main() -> None:
    seeds = seed_corpus(
        SCRATCH, ["cfg-*.toml"], [p.read_bytes() for p in sorted(EXAMPLES.glob("*.toml"))]
    )
    run(one_input, seeds, max_len=4096, dictionary="toml.dict")


if __name__ == "__main__":
    main()
