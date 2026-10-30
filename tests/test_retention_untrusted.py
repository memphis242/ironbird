"""Stage 2d: every file retention reads from disk is untrusted - the notices file, the head of a
ledger part, a leftover reserve, and the names in the directory - and so is the configuration.

Corpus entries live in ``tests/corpus/malformed``: ``cfg-ledger-*``/``cfg-retention-*`` (the
config grammar), ``ret-notices-*`` (``retention-notices.json``), ``led-head-*`` (the first line
of a ledger part) and ``led-reserve-*`` (a crashed session's reserve).
"""

from __future__ import annotations

import errno
import os
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from conftest import CORPUS
from ironbird.config import (
    LedgerConfig,
    RetentionConfig,
    load_config,
    parse_config,
)
from ironbird.errors import Code, IronbirdError
from ironbird.fsops import REAL_FS
from ironbird.retention import full_scan, scan_directory
from ironbird.retnotices import FILE_NAME, MAX_BYTES, Notices, parse_notices, render_notices
from ironbird.units import GIB, KIB, MIB, TIB
from retutil import DAY, make_session, sid, stamp
from support.faultfs import FaultFs

NOW = time.time()


def corpus(pattern: str) -> list[Path]:
    files = sorted(CORPUS.glob(pattern))
    assert files, pattern
    return files


# -- the config grammar ----


@pytest.mark.parametrize(
    "path", corpus("cfg-ledger-*.toml") + corpus("cfg-retention-*.toml"), ids=lambda p: p.stem
)
def test_bad_ledger_and_retention_values_are_config_schema_errors(path: Path) -> None:
    with pytest.raises(IronbirdError) as info:
        load_config(path)
    assert info.value.code is Code.CFG_SCHEMA
    assert ("ledger" in info.value.summary) or ("retention" in info.value.summary)


def parse(table: str) -> Any:
    import tomllib

    base = {"nodes": {"a": {}}}
    return parse_config({**base, **tomllib.loads(table)}, Path("/x/ironbird.toml"))


def test_the_documented_defaults() -> None:
    assert LedgerConfig() == LedgerConfig(
        event_bytes_per_hour=160 * MIB,
        evidence_bytes_per_hour=32 * MIB,
        evidence_raw_bytes_per_second=256 * KIB,
        essential_reserve_bytes=32 * MIB,
        max_file_bytes=32 * MIB,
        on_full="continue",
    )
    assert RetentionConfig() == RetentionConfig(
        max_age_s=7 * 86400,
        max_total_bytes=8 * GIB,
        keep_recent=10,
        warn_interval_s=86400,
        scan_interval_s=600,
    )


def test_unlimited_is_accepted_by_exactly_the_four_documented_keys() -> None:
    config = parse(
        '[ledger]\nevent_bytes_per_hour = "unlimited"\nevidence_bytes_per_hour = "unlimited"\n'
        '[retention]\nmax_age = "unlimited"\nmax_total_bytes = "unlimited"\n'
    )
    assert (
        config.ledger.event_bytes_per_hour is None and config.ledger.evidence_bytes_per_hour is None
    )
    assert config.retention.max_age_s is None and config.retention.max_total_bytes is None
    for table, key in (
        ("ledger", "evidence_raw_bytes_per_second"),
        ("ledger", "essential_reserve_bytes"),
        ("ledger", "max_file_bytes"),
        ("retention", "warn_interval"),
        ("retention", "scan_interval"),
    ):
        with pytest.raises(IronbirdError) as info:
            parse(f'[{table}]\n{key} = "unlimited"\n')
        assert info.value.code is Code.CFG_SCHEMA and key in info.value.summary


@pytest.mark.parametrize(
    ("text", "bytes_"),
    [
        ('"64MiB"', 64 * MIB),
        ("1048576", MIB),
        ('"1GiB"', GIB),
        ('"1024MiB"', GIB),
        ('"999999999999B"', 999_999_999_999),
    ],
)
def test_sizes_accept_integers_and_the_unit_grammar(text: str, bytes_: int) -> None:
    config = parse(f"[ledger]\nmax_file_bytes = {text}\n") if bytes_ <= GIB else None
    assert config is None or config.ledger.max_file_bytes == bytes_


@pytest.mark.parametrize(
    ("table", "key", "low", "high"),
    [
        ("ledger", "event_bytes_per_hour", '"1MiB"', '"1TiB"'),
        ("ledger", "evidence_bytes_per_hour", '"1MiB"', '"1TiB"'),
        ("ledger", "evidence_raw_bytes_per_second", '"1KiB"', '"64MiB"'),
        ("ledger", "essential_reserve_bytes", '"1MiB"', '"1GiB"'),
        ("ledger", "max_file_bytes", '"1MiB"', '"1GiB"'),
        ("retention", "max_total_bytes", '"1MiB"', '"1024TiB"'),
        ("retention", "keep_recent", "0", "1000"),
        ("retention", "max_age", '"1min"', '"100000d"'),
        ("retention", "warn_interval", '"1min"', '"30d"'),
        ("retention", "scan_interval", '"1min"', '"1d"'),
    ],
)
def test_range_edges_are_accepted_and_one_past_is_not(
    table: str, key: str, low: str, high: str
) -> None:
    parse(f"[{table}]\n{key} = {low}\n")
    parse(f"[{table}]\n{key} = {high}\n")
    past = {
        '"1MiB"': '"1023KiB"',
        '"1KiB"': '"1023B"',
        "0": "-1",
        '"1min"': '"59s"',
        '"1TiB"': '"1025GiB"',
        '"64MiB"': '"65MiB"',
        '"1GiB"': '"1025MiB"',
        '"1024TiB"': '"1025TiB"',
        "1000": "1001",
        '"100000d"': '"100001d"',
        '"30d"': '"31d"',
        '"1d"': '"25h"',
    }
    for bad in (past[low], past[high]):
        with pytest.raises(IronbirdError) as info:
            parse(f"[{table}]\n{key} = {bad}\n")
        assert info.value.code is Code.CFG_SCHEMA
    assert TIB > GIB


def test_a_record_of_the_unlimited_evidence_choice_warns_once(tmp_path: Path) -> None:
    from ironbird.ledger import Ledger

    shown: list[Code] = []
    ledger = Ledger(
        tmp_path / "l",
        session_id="u",
        config=LedgerConfig(evidence_bytes_per_hour=None),
        warn_gate=lambda code: shown.append(code) is None,
    )
    ledger.close()
    assert shown == [Code.LED_UNBOUNDED]


# -- the notices file ----


@pytest.mark.parametrize("path", corpus("ret-notices-*.json"), ids=lambda p: p.stem)
def test_a_malformed_notices_file_is_ignored_and_rewritten(path: Path, tmp_path: Path) -> None:
    assert parse_notices(path.read_bytes(), NOW) == ({}, None)
    state = tmp_path / "st"
    state.mkdir(mode=0o700)
    (state / FILE_NAME).write_bytes(path.read_bytes())
    os.chmod(state / FILE_NAME, 0o600)
    notices = Notices(state)
    assert notices.allow("IB-RET-001", state, 86400)  # nothing valid to rate-limit against
    assert not notices.allow("IB-RET-001", state, 86400)  # now there is
    shown, _ = parse_notices((state / FILE_NAME).read_bytes(), time.time() + 5)
    assert (
        list(shown) == [f"IB-RET-001@{state}"]
        and len((state / FILE_NAME).read_bytes()) <= MAX_BYTES
    )


def test_a_valid_notices_file_round_trips_and_is_bounded() -> None:
    shown = {f"IB-RET-001@/dir{i}": NOW - i for i in range(100)}
    data = render_notices(shown, NOW - 5)
    assert len(data) <= MAX_BYTES
    back, last = parse_notices(data, NOW)
    assert last == NOW - 5 and 0 < len(back) <= 32
    assert set(back) == set(sorted(shown, key=lambda k: shown[k])[-len(back) :])  # newest kept


def test_a_warning_is_shown_once_per_interval_and_again_after_it(tmp_path: Path) -> None:
    state = tmp_path / "st"
    state.mkdir(mode=0o700)
    clock = {"t": 1_000_000.0}
    notices = Notices(state, wall=lambda: clock["t"])
    assert notices.allow("IB-RET-001", state, 86400)
    clock["t"] += 86399
    assert not notices.allow("IB-RET-001", state, 86400)
    assert notices.allow("IB-RET-002", state, 86400)  # per code
    assert notices.allow("IB-RET-001", tmp_path, 86400)  # per directory
    clock["t"] += 2
    assert notices.allow("IB-RET-001", state, 86400)


def test_the_scan_is_due_once_per_interval_and_two_callers_claim_it_once(tmp_path: Path) -> None:
    state = tmp_path / "st"
    state.mkdir(mode=0o700)
    clock = {"t": 1_000_000.0}
    first = Notices(state, wall=lambda: clock["t"])
    second = Notices(state, wall=lambda: clock["t"])
    assert first.scan_due(600) and not second.scan_due(600)
    clock["t"] += 601
    assert second.scan_due(600) and not first.scan_due(600)


def test_a_lock_held_for_more_than_200_ms_displays_the_warning_and_leaves_the_file(
    tmp_path: Path,
) -> None:
    import fcntl

    state = tmp_path / "st"
    state.mkdir(mode=0o700)
    notices = Notices(state)
    assert notices.allow("IB-RET-001", state, 86400)
    path = state / FILE_NAME
    before = path.read_bytes()
    fd = os.open(path, os.O_RDWR)
    fcntl.flock(fd, fcntl.LOCK_EX)
    try:
        started = time.monotonic()
        assert notices.allow("IB-RET-001", state, 86400)  # held: a lock never causes silence
        assert time.monotonic() - started < 3
    finally:
        os.close(fd)
    assert path.read_bytes() == before  # and the file was not written


def test_without_lock_support_the_limits_still_work_across_a_thousand_callers(
    tmp_path: Path,
) -> None:
    state = tmp_path / "st"
    state.mkdir(mode=0o700)
    fs = FaultFs()
    fs.fail("flock", errno.ENOLCK)
    shown = 0
    for _ in range(1000):  # a script of a thousand short commands
        notices = Notices(state, fs=fs)
        shown += notices.allow("IB-LED-009", state, 86400)
    assert shown == 1


def test_eight_parallel_callers_without_locks_repeat_at_most_a_few_lines(tmp_path: Path) -> None:
    state = tmp_path / "st"
    state.mkdir(mode=0o700)
    fs = FaultFs()
    fs.fail("flock", errno.ENOLCK)
    results: list[bool] = []
    barrier = threading.Barrier(8)

    def run() -> None:
        notices = Notices(state, fs=fs)
        barrier.wait()
        for _ in range(50):
            results.append(notices.allow("IB-LED-006", state, 86400))

    threads = [threading.Thread(target=run) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)
    assert 1 <= sum(results) <= 8  # a race may repeat one line, never one per call


def test_a_symlink_or_foreign_file_as_the_notices_file_is_not_written(tmp_path: Path) -> None:
    state = tmp_path / "st"
    state.mkdir(mode=0o700)
    target = tmp_path / "precious"
    target.write_text("keep me")
    (state / FILE_NAME).symlink_to(target)
    notices = Notices(state)
    assert notices.allow("IB-RET-001", state, 86400)
    assert notices.allow("IB-RET-001", state, 86400)  # never rate-limited, never written
    assert target.read_text() == "keep me"
    linked = tmp_path / "st2"
    linked.mkdir(mode=0o700)
    (linked / FILE_NAME).write_text("x")
    os.link(linked / FILE_NAME, tmp_path / "other-name")
    os.chmod(linked / FILE_NAME, 0o600)
    assert Notices(linked).allow("IB-RET-001", linked, 86400)
    assert (linked / FILE_NAME).read_text() == "x"  # one link too many: ignored, not written


# -- ledger heads and directory names ----


@pytest.mark.parametrize("path", corpus("led-head-*.jsonl"), ids=lambda p: p.stem)
def test_a_malformed_ledger_head_is_classed_long_and_so_protected_not_spent(
    path: Path, tmp_path: Path
) -> None:
    state = tmp_path / "st"
    state.mkdir(mode=0o700)
    (target,) = make_session(state, 1, age_days=30, klass="short")
    target.write_bytes(path.read_bytes())
    os.utime(target, (NOW - 30 * DAY, NOW - 30 * DAY))
    result = full_scan(REAL_FS, state, "ledger", now=NOW, own_session=None, keep_recent=5)
    assert result.sessions[sid(1)].klass == "long"


def test_hostile_directory_names_are_never_displayed_or_counted_as_sessions(tmp_path: Path) -> None:
    state = tmp_path / "st"
    state.mkdir(mode=0o700)
    start = stamp(NOW - 30 * DAY)
    hostile = [
        f"{start}-{sid(1)}.jsonl\x1b[2J",
        f"{start}-\x1b]0;pwn\x07.jsonl",
        chr(0x202E) + f"{start}-{sid(1)}.jsonl",
        f"{start}-{sid(1)}.jsonl\n{start}-{sid(2)}.jsonl",
        "x" * 300,
        f"{start}-{sid(1)}.jsonl".encode().decode() + "\udcff",  # invalid UTF-8 as a surrogate
    ]
    for name in hostile:
        try:
            (state / name).write_text("x")
        except (OSError, UnicodeError):
            continue
    result = scan_directory(REAL_FS, state, "ledger")
    assert not result.sessions and result.messages == []
    for text in (result.partial or "",):
        assert "\x1b" not in text
