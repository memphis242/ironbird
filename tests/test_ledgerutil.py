"""The ledger-reading helper of the tests puts invocations in the order they ran."""

from __future__ import annotations

import json
from pathlib import Path

from ledgerutil import records_in_order


def test_sessions_begun_in_one_second_are_read_in_the_order_they_ran(tmp_path: Path) -> None:
    # Two invocations within one second: the names share the stamp, and the later session's
    # random id sorts first.
    stamp = "20261105T023500Z"
    later = [{"ts": "2026-11-05T02:35:00.900000Z", "event": "send.plan", "n": i} for i in (1, 2)]
    earlier = [{"ts": "2026-11-05T02:35:00.100000Z", "event": "send.plan", "n": i} for i in (1, 2)]
    for session, rows in (("0aaaaaaaaaaa", later), ("fbbbbbbbbbbb", earlier)):
        lines = "".join(json.dumps(r) + "\n" for r in rows)
        (tmp_path / f"{stamp}-{session}.jsonl").write_text(lines)
    assert sorted(p.name for p in tmp_path.iterdir())[0].startswith(f"{stamp}-0a")
    assert records_in_order(tmp_path) == [*earlier, *later]
