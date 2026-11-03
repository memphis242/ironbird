"""Coverage-guided fuzzing (atheris) of what the ledger writes.

Usage: python tools/fuzz/fuzz_ledger.py [-max_total_time=N]

Every field of a ledger record can hold text, bytes or structure that came from the wire or a file.
The input becomes the event name and the fields of one record (a JSON object when it parses as
one: nested values, non-finite numbers, huge integers; otherwise raw text and bytes under fixed
names, with the bytes also as a ``bytes`` value).

Invariant: ``Ledger.record`` never raises for such fields, and the line it writes is exactly one
line of ASCII JSON (so the file is inert in any viewer) that parses to an object with the
envelope keys, whose record numbers increase by one per record written, and whose length stays
bounded. The ledger is replaced every few hundred inputs to keep the scratch directory small.
Seeds are valid and hostile field sets.
"""

from __future__ import annotations

import json
import shutil
from typing import Any

import atheris
from fuzzcommon import run, scratch_dir

with atheris.instrument_imports():
    from ironbird.ledger import Ledger

SCRATCH = scratch_dir("ledger")
DIRECTORY = SCRATCH / "state"
RENEW_EVERY = 300
MAX_LINE = 1 << 20
ENVELOPE = ("ts", "session", "n", "kind", "event")
RESERVED = frozenset({*ENVELOPE, "tier", "sample"})


class State:
    ledger: Ledger | None = None
    offset = 0
    last_n = -1
    inputs = 0


def renew() -> Ledger:
    if State.ledger is not None:
        State.ledger.close()
    shutil.rmtree(DIRECTORY, ignore_errors=True)
    ledger = Ledger(DIRECTORY, session_id="fuzz", session_class="short")
    ledger.open()
    State.ledger, State.offset, State.last_n, State.inputs = ledger, 0, -1, 0
    return ledger


def fields_of(data: bytes) -> tuple[str, dict[str, Any]]:
    text = data.decode("utf-8", errors="surrogateescape")
    try:
        parsed = json.loads(data)
    except (ValueError, RecursionError):
        parsed = None
    if isinstance(parsed, dict):
        return text[:40], {k: v for k, v in parsed.items() if k not in RESERVED}
    return text[:40], {"text": text, "blob": bytes(data), "number": len(data) ** 40}


def one_input(data: bytes) -> None:
    ledger = State.ledger if State.inputs < RENEW_EVERY and State.ledger else renew()
    State.inputs += 1
    event, fields = fields_of(data)
    ledger.record("action", event, **fields)
    ledger.flush()
    path = ledger.path
    with path.open("rb") as handle:
        handle.seek(State.offset)
        chunk = handle.read()
    assert chunk == b"" or chunk.endswith(b"\n"), chunk[-80:]
    State.offset += len(chunk)
    for line in chunk.splitlines():
        assert len(line) <= MAX_LINE, len(line)
        line.decode("ascii")
        record = json.loads(line)
        assert isinstance(record, dict) and all(k in record for k in ENVELOPE), record
        assert record["n"] > State.last_n
        State.last_n = record["n"]


def main() -> None:
    seeds = SCRATCH / "seeds"
    seeds.mkdir()
    samples = (
        b'{"mid": 2193, "reason": "bad checksum", "raw": "\\u001b[2J"}',
        b'{"a": {"b": [1, 2, {"c": NaN}]}, "d": 1e999, "e": 123456789012345678901234567890}',
        b'{"x": "\\ud800", "y": "\\u202e\\u0000"}',
        b"\x1b[31mred\x07\xff\xfe plain text with a newline\nand more",
        b"[" * 300 + b"]" * 300,
        b"",
    )
    for i, blob in enumerate(samples):
        (seeds / f"valid{i}").write_bytes(blob)
    run(one_input, seeds, max_len=4096)


if __name__ == "__main__":
    main()
