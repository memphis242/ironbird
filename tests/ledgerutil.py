"""Helpers for reading ledger files back in tests."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ironbird.filter.lexer import parse_time
from ironbird.ledger import Ledger


def all_records(source: Ledger | Path) -> list[dict[str, Any]]:
    """Every record of the ledger's part 1, bookkeeping included."""
    path = source.path if isinstance(source, Ledger) else source
    return [json.loads(line) for line in path.read_text().splitlines()]


def records(source: Ledger | Path) -> list[dict[str, Any]]:
    """The records of part 1 other than the ledger's own ``ledger.*`` bookkeeping."""
    return [r for r in all_records(source) if not r["event"].startswith("ledger.")]


def session_records(directory: Path, *, bookkeeping: bool = False) -> list[dict[str, Any]]:
    """The records of every ledger file in ``directory`` (parts, reserves), in file name order
    then position; ``bookkeeping`` keeps the ``ledger.*`` ones."""
    rows: list[dict[str, Any]] = []
    for path in sorted(directory.iterdir()):
        if path.suffix in (".jsonl", ".reserve"):
            for line in path.read_bytes().rstrip(b"\x00").decode("ascii", "replace").splitlines():
                try:
                    row = json.loads(line)
                except ValueError:
                    continue  # a foreign file a test planted at one of our names
                if isinstance(row, dict) and "event" in row:
                    rows.append(row)
    return rows if bookkeeping else [r for r in rows if not r["event"].startswith("ledger.")]


def records_in_order(directory: Path) -> list[dict[str, Any]]:
    """Every record of the ``*.jsonl`` ledger files in ``directory``, the files in the order their
    first records were written, each file's records in place. A file's name is its session's
    start to the second, then its random session id, so sessions begun within one second sort
    by chance in name order: two invocations in a row often do."""
    files: list[tuple[int, str, list[dict[str, Any]]]] = []
    for path in directory.glob("*.jsonl"):
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        if rows:
            files.append((parse_time(rows[0]["ts"]), path.name, rows))
    return [row for _, _, rows in sorted(files, key=lambda f: (f[0], f[1])) for row in rows]
