"""Coverage-guided fuzzing (atheris) of what retention reads from disk.

Usage: python tools/fuzz/fuzz_retention.py [-max_total_time=N]

The first input byte picks the target; the rest is its content:

* 0: the notices file (``retention-notices.json``). ``parse_notices`` answers a state or the empty
  one, never raises, and a state it accepts renders and parses back to itself; a ``Notices``
  transaction on a file holding the input answers a bool and leaves a file of at most
  ``MAX_BYTES``.
* 1: the first line of a ledger part (``parse_head``: a JSON object or None, never an exception),
  then a ledger directory whose part 1 holds the input, scanned the way ``ib`` scans it.
* 2: a leftover reserve file holding the input, scanned with recovery.
* 3: hostile directory names (the input split at newlines), scanned.

Invariant: no scan raises anything but a coded ``IronbirdError``; a look-only scan
(``recover=False``, or ``check_only``) leaves the directory exactly as it found it, and no scan
deletes a file whose name is not ours (nothing is spent on the strength of a file's content).
Seeds are the checked-in ``ret-notices-*``, ``led-head-*`` and ``led-reserve-*`` files.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import time
from pathlib import Path

import atheris
from fuzzcommon import CORPUS, run, scratch_dir

with atheris.instrument_imports():
    from ironbird.errors import IronbirdError
    from ironbird.fsops import REAL_FS
    from ironbird.retention import full_scan, is_managed_name, parse_head
    from ironbird.retnotices import FILE_NAME, MAX_BYTES, Notices, parse_notices, render_notices

SCRATCH = scratch_dir("retention")
STATE = SCRATCH / "state"
DAY = 86400.0
NOW = time.time()
OLD = NOW - 30 * DAY
STAMP = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(OLD))
SESSION = "000000000001"
PART1 = f"{STAMP}-{SESSION}.jsonl"
RESERVE = f"{STAMP}-{SESSION}.r0001.reserve"
MAX_NAMES = 8


def fresh_state() -> Path:
    shutil.rmtree(STATE, ignore_errors=True)
    STATE.mkdir(mode=0o700)
    return STATE


def put(directory: Path, name: bytes | str, content: bytes) -> None:
    path = os.path.join(os.fsencode(directory), os.fsencode(name))
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, content)
    finally:
        os.close(fd)
    os.utime(path, (OLD, OLD))


def listing(directory: Path) -> dict[bytes, bytes]:
    root = os.fsencode(directory)
    return {
        name: Path(os.fsdecode(os.path.join(root, name))).read_bytes() for name in os.listdir(root)
    }


def notices(data: bytes) -> None:
    shown, last = parse_notices(data, NOW)
    assert isinstance(shown, dict) and (last is None or isinstance(last, float))
    again = parse_notices(render_notices(shown, last), NOW)
    assert again == (shown, last)
    state = fresh_state()
    put(state, FILE_NAME, data)
    assert isinstance(Notices(state).allow("IB-RET-001", state, 86400.0), bool)
    assert isinstance(Notices(state).scan_due(3600.0), bool)
    assert len((state / FILE_NAME).read_bytes()) <= max(MAX_BYTES, len(data))


def scan_dir(state: Path) -> None:
    for kwargs in (
        {"recover": False},
        {"recover": False, "check_only": True},
        {"recover": True},
    ):
        before = listing(state)
        with contextlib.suppress(IronbirdError):
            full_scan(REAL_FS, state, "ledger", now=NOW, own_session=None, keep_recent=5, **kwargs)  # type: ignore[arg-type]
        after = listing(state)
        if kwargs != {"recover": True}:
            assert after == before, f"{kwargs} changed the directory"
        else:
            foreign = {n for n in before if not is_managed_name(os.fsdecode(n))}
            assert foreign <= after.keys(), "a recovering scan deleted a file that is not ours"


def ledger_head(data: bytes) -> None:
    line = data.split(b"\n", 1)[0] + b"\n"
    assert parse_head(data) is None or isinstance(parse_head(data), dict)
    assert parse_head(line) is None or isinstance(parse_head(line), dict)
    state = fresh_state()
    put(state, PART1, data)
    scan_dir(state)


def reserve(data: bytes) -> None:
    state = fresh_state()
    put(state, RESERVE, data)
    scan_dir(state)


def names(data: bytes) -> None:
    state = fresh_state()
    chosen = 0
    for raw in data.split(b"\n"):
        name = raw.replace(b"/", b"").replace(b"\0", b"")[:255]
        if not name or name in (b".", b".."):
            continue
        put(state, name, b"{}\n")
        chosen += 1
        if chosen == MAX_NAMES:
            break
    foreign = {n for n in os.listdir(os.fsencode(state)) if not is_managed_name(os.fsdecode(n))}
    scan_dir(state)
    # Names that fit none of our patterns are not ours: they survive every scan.
    assert foreign <= set(os.listdir(os.fsencode(state)))


TARGETS = (notices, ledger_head, reserve, names)


def one_input(data: bytes) -> None:
    if not data:
        return
    TARGETS[data[0] % len(TARGETS)](data[1:])


def main() -> None:
    seeds = SCRATCH / "seeds"
    seeds.mkdir()
    index = 0
    for mode, pattern in enumerate(["ret-notices-*.json", "led-head-*.jsonl", "led-reserve-*.bin"]):
        for path in sorted(CORPUS.glob(pattern)):
            (seeds / f"seed{index}").write_bytes(bytes([mode]) + path.read_bytes())
            index += 1
    hostile = f"{PART1}\x1b[2J\n{STAMP}-{SESSION}.p0002.jsonl\n{STAMP}-{SESSION}.r0001.reserve\n"
    (seeds / "names").write_bytes(b"\x03" + hostile.encode())
    run(one_input, seeds, max_len=6000, dictionary="retention.dict")


if __name__ == "__main__":
    main()
