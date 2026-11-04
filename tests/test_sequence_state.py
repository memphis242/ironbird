"""Sequence counts shared by every ironbird process: the live table, the durable ceilings,
``ib send --sequence`` and ``ib seq``.

Two stores in one test stand for two processes: each opens its own descriptions of the files, so
``flock`` keeps them apart as it keeps processes apart. Real ``ib`` processes against a real
``ib respond`` check what reaches the wire. No assertion is made on the wording of a message,
only on its code. Each regression test names the review finding it reproduces (PR 40: B1-B4,
S1-S4, N1-N5).
"""

from __future__ import annotations

import asyncio
import errno
import fcntl
import hashlib
import json
import re
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from conftest import CORPUS, EXAMPLES
from ironbird import seqstate
from ironbird.ccdd.loader import load_ccdd
from ironbird.ccdd.model import MessageKind
from ironbird.codec import Codec
from ironbird.errors import Code, IronbirdError
from ironbird.header import load_profile
from ironbird.ledger import RECORD_TIERS
from ironbird.seqstate import (
    BLOCK,
    INVALID_NAME,
    LIVE_BYTES,
    LIVE_NAME,
    LOCK_NAME,
    MAX_BYTES,
    MAX_KEY_LEN,
    MAX_STREAMS,
    SLOT_BYTES,
    STATE_NAME,
    SequenceStore,
    key_of,
    node_stem,
    parse_key,
    parse_state,
    read_boot_id,
    render_state,
)
from support.faultfs import FaultFs

KEY = "fc/command/0x1890"
BOOT = bytes(range(16))  # the boot id the tests run in (the live corpus is written for it)
OTHER_BOOT = bytes(range(16, 32))


class _Reports:
    def __init__(self) -> None:
        self.errors: list[IronbirdError] = []
        self.events: list[tuple[str, dict[str, Any]]] = []

    def __call__(self, error: IronbirdError) -> None:
        self.errors.append(error)

    def record(self, event: str, fields: dict[str, Any]) -> None:
        self.events.append((event, fields))

    @property
    def codes(self) -> list[Code]:
        return [e.code for e in self.errors]

    def named(self, event: str) -> list[dict[str, Any]]:
        return [f for e, f in self.events if e == event]


def _store(
    directory: Path | None,
    reports: _Reports | None = None,
    *,
    boot: bytes = BOOT,
    fs: Any = None,
    wait: float = 5.0,
) -> SequenceStore:
    seen = reports if reports is not None else _Reports()
    extra = {"fs": fs} if fs is not None else {}
    return SequenceStore(
        directory,
        report=seen,
        record=seen.record,
        boot_id=lambda: boot,
        lock_wait_s=wait,
        **extra,
    )


async def _takes(store: SequenceStore, n: int, key: str = KEY) -> list[int]:
    out: list[int] = []
    for _ in range(n):
        lease = await store.lease(key)
        try:
            out.append(lease.count)
            lease.commit()
        finally:
            lease.release()
    return out


def _run(coro: Any) -> Any:
    return asyncio.run(coro)


def _marks(directory: Path) -> dict[str, int]:
    marks = parse_state((directory / STATE_NAME).read_bytes())
    assert marks is not None
    return marks


# -- exact counts across processes ---------------------------------------------------------


def test_counts_continue_exactly_across_processes_and_a_crash_loses_nothing(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        first = _store(tmp_path)
        assert await _takes(first, 3) == [0, 1, 2]
        # never closed: a crashed process; the live table keeps what it handed out
        second = _store(tmp_path)
        assert await _takes(second, 2) == [3, 4]
        await second.aclose()
        third = _store(tmp_path)
        assert await _takes(third, 1) == [5]
        await third.aclose()
        await first.aclose()

    _run(scenario())


def test_two_long_lived_senders_alternating_count_in_take_order(tmp_path: Path) -> None:
    """B3: two senders of one MID (a TUI and a script, two responders) interleaved on the wire
    as 0,64,1,65,...; now the series is exact in take order."""

    async def scenario() -> list[int]:
        one, two = _store(tmp_path), _store(tmp_path)
        seen: list[int] = []
        for _ in range(200):
            seen += await _takes(one, 1)
            seen += await _takes(two, 1)
        await one.aclose()
        await two.aclose()
        return seen

    assert _run(scenario()) == list(range(400))


def test_the_durable_ceiling_is_synced_ahead_before_a_count_is_used(tmp_path: Path) -> None:
    async def scenario() -> None:
        store = _store(tmp_path)
        assert await _takes(store, 1) == [0]
        assert _marks(tmp_path)[KEY] == BLOCK  # every count handed out is below it, on disk
        await _takes(store, BLOCK - 1)
        assert _marks(tmp_path)[KEY] == BLOCK  # the step is used up, not yet raised
        await _takes(store, 1)
        assert _marks(tmp_path)[KEY] == 2 * BLOCK
        await store.aclose()

    _run(scenario())


def test_a_take_within_the_ceiling_never_syncs(tmp_path: Path) -> None:
    """S1: the take itself is a lock, a pread and a pwrite; only a raised ceiling syncs."""
    fs = FaultFs()

    async def scenario() -> list[int]:
        store = _store(tmp_path, fs=fs)
        taken = await _takes(store, 10 * BLOCK)
        await store.aclose()
        return taken

    assert _run(scenario()) == list(range(10 * BLOCK))
    assert fs.count("fdatasync") == 10  # one per step of BLOCK counts, never per packet


def test_a_hung_sync_never_blocks_the_event_loop(tmp_path: Path) -> None:
    """S1: the sync of a raised ceiling (and up to a second of lock polling) ran on the loop
    that also reads the capture sockets; a hung disk hung the responder."""
    fs = FaultFs()

    async def scenario() -> None:
        store = _store(tmp_path, fs=fs)
        assert await _takes(store, BLOCK) == list(range(BLOCK))  # the next take must raise
        rule = fs.block("fdatasync", times=1)
        ticks = 0

        async def ticker() -> None:
            nonlocal ticks
            while True:
                ticks += 1
                await asyncio.sleep(0.005)

        ticking = asyncio.create_task(ticker())
        take = asyncio.create_task(store.lease(KEY))
        await asyncio.sleep(0.3)
        assert not take.done() and ticks >= 20  # the loop runs while the sync hangs
        assert rule.gate is not None
        rule.gate.set()
        lease = await take
        assert lease.count == BLOCK
        lease.commit()
        lease.release()
        ticking.cancel()
        await store.aclose()

    _run(scenario())


def test_a_power_loss_skips_at_most_one_step_and_never_reuses(tmp_path: Path) -> None:
    async def scenario() -> None:
        before = _store(tmp_path)
        used = await _takes(before, 10)
        reports = _Reports()
        after = _store(tmp_path, reports, boot=OTHER_BOOT)  # the page cache was lost
        nxt = (await _takes(after, 1))[0]
        assert max(used) < nxt <= max(used) + BLOCK
        assert reports.codes == [Code.SEQ_LIVE] and reports.named("sequence.rebuilt")
        await after.aclose()
        await before.aclose()

    _run(scenario())


def test_more_streams_than_the_old_limit_keep_every_count(tmp_path: Path) -> None:
    """B1: past 128 streams the oldest were evicted silently and restarted at 0, in one
    process (0,1,2,3,0,1,...) and across processes."""

    async def scenario() -> None:
        sender = _store(tmp_path)
        assert await _takes(sender, 5) == [0, 1, 2, 3, 4]
        others = [key_of(f"n{i % 4}", "command", 0x1800 + i) for i in range(300)]
        other = _store(tmp_path)
        for key in others:
            assert await _takes(other, 2, key) == [0, 1]
        assert await _takes(sender, 3) == [5, 6, 7]  # the same process carries on
        await sender.aclose()
        later = _store(tmp_path)
        assert await _takes(later, 1) == [8]  # and so does the next one
        assert await _takes(later, 1, others[0]) == [2]
        await later.aclose()
        await other.aclose()
        assert len(_marks(tmp_path)) == 301

    _run(scenario())


def test_a_full_table_refuses_a_new_stream_loudly_and_keeps_the_others(tmp_path: Path) -> None:
    """B1: the bound is explicit: a new stream is refused, coded and ledgered, never evicted."""
    tmp_path.mkdir(exist_ok=True)
    full = {key_of(f"n{i % 64}", "command", i): 7 for i in range(MAX_STREAMS)}
    (tmp_path / STATE_NAME).write_bytes(render_state(full))
    reports = _Reports()

    async def scenario() -> None:
        store = _store(tmp_path, reports)
        with pytest.raises(IronbirdError) as info:
            await store.lease(KEY)
        assert info.value.code is Code.SEQ_FULL
        assert reports.named("sequence.full")
        assert await _takes(store, 2, next(iter(full))) == [7, 8]  # a stored stream goes on
        await store.aclose()

    _run(scenario())
    assert len(_marks(tmp_path)) == MAX_STREAMS and KEY not in _marks(tmp_path)


def test_a_lock_held_too_long_fails_the_take_and_never_repeats_a_count(tmp_path: Path) -> None:
    """B2: a lock wait over the bound dropped the process to in-memory counting for good,
    which repeated counts other processes had; now the take fails, coded, and the next one
    carries on exactly."""

    async def scenario() -> None:
        store = _store(tmp_path, wait=0.3)
        assert await _takes(store, BLOCK) == list(range(BLOCK))  # the step is used up
        with open(tmp_path / LOCK_NAME, "rb") as holder:  # a stopped process holds it
            fcntl.flock(holder, fcntl.LOCK_EX)
            started = time.monotonic()
            with pytest.raises(IronbirdError) as info:
                await store.lease(KEY)
            assert info.value.code is Code.SEQ_TAKE
            assert time.monotonic() - started < 3
        assert store.persistent
        assert await _takes(store, 2) == [BLOCK, BLOCK + 1]
        with open(tmp_path / LIVE_NAME, "rb") as holder:
            fcntl.flock(holder, fcntl.LOCK_EX)
            with pytest.raises(IronbirdError) as info:
                await store.lease(KEY)
            assert info.value.code is Code.SEQ_TAKE
        assert await _takes(store, 1) == [BLOCK + 2]
        await store.aclose()

    _run(scenario())


def test_a_failure_after_the_store_is_open_is_an_error_not_memory(tmp_path: Path) -> None:
    """B2: only a directory unusable from the start counts in memory."""
    fs = FaultFs()
    reports = _Reports()

    async def scenario() -> None:
        store = _store(tmp_path, reports, fs=fs)
        assert await _takes(store, BLOCK) == list(range(BLOCK))
        rule = fs.fail("fdatasync", errno.EIO)
        with pytest.raises(IronbirdError) as info:
            await store.lease(KEY)
        assert info.value.code is Code.SEQ_TAKE
        rule.off = True
        assert store.persistent and await _takes(store, 1) == [BLOCK]
        await store.aclose()

    _run(scenario())
    assert Code.SEQ_UNSAVED not in reports.codes


def test_reset_while_a_sender_runs_takes_effect_at_once_and_nothing_rewinds(
    tmp_path: Path,
) -> None:
    """B4 and S2: a closing process gave back counts after a reset (ABA), and a running one
    went on with the old numbering, then jumped back."""

    async def scenario() -> list[int]:
        runner = _store(tmp_path)
        assert await _takes(runner, 10) == list(range(10))
        cli = _store(tmp_path)
        assert [r[:3] for r in await cli.forget("fc")] == [("fc", "command", 0x1890)]
        fresh = _store(tmp_path)
        seen = await _takes(fresh, 3) + await _takes(runner, 3)
        await runner.aclose()  # gives nothing back
        later = _store(tmp_path)
        seen += await _takes(later, 2)
        for store in (cli, fresh, later):
            await store.aclose()
        return seen

    assert _run(scenario()) == [0, 1, 2, 3, 4, 5, 6, 7]


def test_a_seat_from_another_process_is_seen_by_a_running_sender(tmp_path: Path) -> None:
    """S2: after `ib send --sequence 4` elsewhere, a running sender resent 5, 6, 7 later."""

    async def scenario() -> list[int]:
        runner = _store(tmp_path)
        assert await _takes(runner, 60) == list(range(60))
        other = _store(tmp_path)
        lease = await other.lease(KEY, seat=4)
        assert lease.count == 4
        lease.commit()
        lease.release()
        seen = await _takes(runner, 3) + await _takes(other, 1)
        await runner.aclose()
        await other.aclose()
        return seen

    assert _run(scenario()) == [5, 6, 7, 8]
    assert _marks(tmp_path)[KEY] == 4 + 1 + BLOCK  # a power loss now skips at most one step


def test_a_lease_released_without_commit_spends_nothing(tmp_path: Path) -> None:
    async def scenario() -> list[int]:
        store = _store(tmp_path)
        lease = await store.lease(KEY)
        assert lease.count == 0
        lease.release()
        seen = await _takes(store, 2)
        await store.aclose()
        return seen

    assert _run(scenario()) == [0, 1]


def test_span_send_holds_the_lock_until_release_and_serial_releases_at_commit(
    tmp_path: Path,
) -> None:
    """UDP holds the take's lock to the hand-off, so the wire order is the take order; serial
    (span_send off) lets other processes take once the count is committed."""

    async def scenario() -> None:
        held, other = _store(tmp_path), _store(tmp_path, wait=0.2)
        await _takes(held, 1)
        await _takes(other, 1)
        lease = await held.lease(KEY)
        lease.commit()
        with pytest.raises(IronbirdError) as info:
            await other.lease(KEY)  # still held: its packet is not on the wire yet
        assert info.value.code is Code.SEQ_TAKE
        lease.release()
        assert await _takes(other, 1) == [3]
        serial = await held.lease(KEY, span_send=False)
        serial.commit()
        assert await _takes(other, 1) == [5]  # no wait for the serial hand-off
        serial.release()
        await held.aclose()
        await other.aclose()

    _run(scenario())


def test_keeping_ahead_raises_ceilings_in_the_background(tmp_path: Path) -> None:
    reports = _Reports()

    async def scenario() -> list[int]:
        store = _store(tmp_path, reports)
        store.keep_ahead()
        taken: list[int] = []
        for _ in range(6 * BLOCK):
            taken += await _takes(store, 1)
            await asyncio.sleep(0)
            if len(taken) % 16 == 0:
                await asyncio.sleep(0.02)  # a responder's commands do not come back to back
        await store.aclose()
        return taken

    assert _run(scenario()) == list(range(6 * BLOCK))
    needed = [e for e in reports.named("sequence.extend") if e["why"] == "need"]
    assert len(needed) == 1  # the first take; every later step was raised ahead
    assert reports.named("sequence.extend")[-1]["why"] == "ahead"


_WORKER = textwrap.dedent(
    """
    import asyncio, sys
    from pathlib import Path
    from ironbird.seqstate import SequenceStore
    def refuse(error):
        raise SystemExit(f"unexpected warning: {error}")
    async def main():
        store = SequenceStore(Path(sys.argv[1]), report=refuse, boot_id=lambda: bytes(range(16)))
        taken = []
        for _ in range(int(sys.argv[2])):
            lease = await store.lease("fc/command/0x1890")
            taken.append(lease.count)
            lease.commit()
            lease.release()
        if sys.argv[3] == "close":
            await store.aclose()
        print(" ".join(map(str, taken)))
    asyncio.run(main())
    """
)


@pytest.mark.parametrize(("workers", "each"), [(8, 60), (48, 1)])
def test_parallel_processes_hand_out_every_count_exactly_once(
    tmp_path: Path, workers: int, each: int
) -> None:
    """B2 (r8): 64 and 128 parallel one-packet sends repeated counts; B3: by block, not by
    time. Now the union is exactly 0..N-1 and each process counts upward."""
    procs = [
        subprocess.Popen(
            [sys.executable, "-c", _WORKER, str(tmp_path), str(each), "close" if i % 2 else "-"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for i in range(workers)
    ]
    taken: list[int] = []
    for proc in procs:
        out, err = proc.communicate(timeout=120)
        assert proc.returncode == 0, err
        series = [int(n) for n in out.split()]
        assert series == sorted(series)
        taken += series
    assert sorted(taken) == list(range(workers * each))


# -- unusable directory --------------------------------------------------------------------


def test_a_directory_unusable_from_the_start_warns_once_and_counts_in_memory(
    tmp_path: Path,
) -> None:
    fs = FaultFs()
    fs.fail("open_at", errno.EROFS, when=lambda _fd, name, *_a: name == LIVE_NAME)
    reports = _Reports()

    async def scenario() -> list[int]:
        store = _store(tmp_path, reports, fs=fs)
        taken = await _takes(store, 200)
        lease = await store.lease(KEY, seat=5)
        lease.commit()
        lease.release()
        taken += await _takes(store, 1)
        await store.aclose()
        return taken

    assert _run(scenario()) == [*range(200), 6]
    assert reports.codes == [Code.SEQ_UNSAVED] and reports.codes[0].is_warning
    assert reports.named("sequence.memory_only")
    assert not (tmp_path / STATE_NAME).exists()


def test_no_file_locks_never_means_unlocked_counting(tmp_path: Path) -> None:
    fs = FaultFs()
    fs.fail("flock", errno.ENOLCK)
    reports = _Reports()

    async def scenario() -> None:
        store = _store(tmp_path, reports, fs=fs)
        assert await _takes(store, 1) == [0]
        assert not store.persistent
        await store.aclose()

    _run(scenario())
    assert reports.codes == [Code.SEQ_UNSAVED]


def test_no_state_directory_says_so_once(tmp_path: Path) -> None:
    """N4: with the ledger off the counts were in memory without a word."""
    reports = _Reports()

    async def scenario() -> None:
        store = _store(None, reports)
        assert await _takes(store, 3) == [0, 1, 2]
        await store.aclose()

    _run(scenario())
    assert reports.codes == [Code.SEQ_UNSAVED]


# -- the files are untrusted ---------------------------------------------------------------


@pytest.mark.parametrize("path", sorted(CORPUS.glob("seqstate-*.json")), ids=lambda p: p.stem)
def test_corpus_state_files_are_refused_by_the_parser(path: Path) -> None:
    assert parse_state(path.read_bytes()) is None


@pytest.mark.parametrize("path", sorted(CORPUS.glob("seqstate-*.json")), ids=lambda p: p.stem)
def test_a_corpus_state_file_is_moved_aside_with_a_warning(path: Path, tmp_path: Path) -> None:
    (tmp_path / STATE_NAME).write_bytes(path.read_bytes())
    reports = _Reports()

    async def scenario() -> list[int]:
        store = _store(tmp_path, reports)
        taken = await _takes(store, 3)
        await store.aclose()
        return taken

    assert _run(scenario()) == [0, 1, 2]  # nothing else knew the stream: it starts at 0
    assert reports.codes == [Code.SEQ_RESET]
    assert all("\n" not in e.user_line() for e in reports.errors)
    assert (tmp_path / INVALID_NAME).read_bytes() == path.read_bytes()  # the evidence is kept
    assert _marks(tmp_path)[KEY] == BLOCK


def test_an_invalid_durable_file_is_rebuilt_from_the_live_table(tmp_path: Path) -> None:
    """Within a boot, a damaged durable file loses nothing: the live table knows every stream."""
    reports = _Reports()

    async def scenario() -> None:
        store = _store(tmp_path, reports)
        assert await _takes(store, BLOCK + 3) == list(range(BLOCK + 3))
        assert await _takes(store, 1, "ground/command/0x1890") == [0]
        (tmp_path / STATE_NAME).write_text("garbage")
        assert await _takes(store, BLOCK) == list(range(BLOCK + 3, 2 * BLOCK + 3))
        await store.aclose()

    _run(scenario())
    assert reports.codes == [Code.SEQ_RESET]
    assert _marks(tmp_path)["ground/command/0x1890"] == BLOCK  # recovered, not dropped


def _live_corpus() -> list[Path]:
    return sorted(CORPUS.glob("seqlive-*.bin"))


@pytest.mark.parametrize("path", _live_corpus(), ids=lambda p: p.stem)
def test_a_corpus_live_table_is_rebuilt_from_the_durable_ceilings(
    path: Path, tmp_path: Path
) -> None:
    """A forged, damaged or stale live table (one of another boot) never hands out a count
    below the durable ceiling: the table is rebuilt and the stream carries on from it."""
    key = "fc/telemetry/0x0b00"  # the stream the corpus entries forge (slot 2)
    raw = path.read_bytes()
    if path.stem != "seqlive-short":
        raw = raw.ljust(LIVE_BYTES, b"\0")
    (tmp_path / LIVE_NAME).write_bytes(raw)
    (tmp_path / STATE_NAME).write_bytes(render_state({key: 128}))
    reports = _Reports()

    async def scenario() -> list[int]:
        store = _store(tmp_path, reports)
        taken = await _takes(store, 2, key)
        await store.aclose()
        return taken

    assert _run(scenario()) == [128, 129]
    assert reports.codes == [Code.SEQ_LIVE] and reports.named("sequence.rebuilt")
    assert (tmp_path / LIVE_NAME).stat().st_size == LIVE_BYTES


def test_a_live_table_removed_while_in_use_is_reopened_without_a_repeat(tmp_path: Path) -> None:
    reports = _Reports()

    async def scenario() -> list[int]:
        running = _store(tmp_path, reports)
        seen = await _takes(running, 5)
        (tmp_path / LIVE_NAME).unlink()  # someone cleaned the state directory
        newcomer = _store(tmp_path)
        seen += await _takes(newcomer, 2)
        seen += await _takes(running, 2)
        await running.aclose()
        await newcomer.aclose()
        return seen

    seen = _run(scenario())
    assert seen[:5] == [0, 1, 2, 3, 4] and len(set(seen)) == len(seen)
    assert seen == sorted(seen)
    assert Code.SEQ_LIVE in reports.codes


@pytest.mark.parametrize("planted", ["symlink", "directory"])
def test_a_planted_live_file_makes_the_directory_unusable_from_the_start(
    tmp_path: Path, planted: str
) -> None:
    target = tmp_path / "elsewhere"
    target.write_bytes(b"keep")
    if planted == "symlink":
        (tmp_path / LIVE_NAME).symlink_to(target)
    else:
        (tmp_path / LIVE_NAME).mkdir()
    reports = _Reports()

    async def scenario() -> None:
        store = _store(tmp_path, reports)
        assert await _takes(store, 1) == [0]
        await store.aclose()

    _run(scenario())
    assert reports.codes == [Code.SEQ_UNSAVED]
    assert target.read_bytes() == b"keep"


def test_a_forged_durable_symlink_is_not_followed_or_written_through(tmp_path: Path) -> None:
    target = tmp_path / "elsewhere"
    target.write_text('{"v":1,"next":{"fc/command/0x1890":7}}\n')
    (tmp_path / STATE_NAME).symlink_to(target)
    reports = _Reports()

    async def scenario() -> list[int]:
        store = _store(tmp_path, reports)
        taken = await _takes(store, 1)
        await store.aclose()
        return taken

    assert _run(scenario()) == [0]  # the link's content is not read
    assert Code.SEQ_RESET in reports.codes
    assert target.read_text() == '{"v":1,"next":{"fc/command/0x1890":7}}\n'
    assert (tmp_path / INVALID_NAME).is_symlink()  # moved aside, as it was


@settings(max_examples=200, deadline=1000, suppress_health_check=[HealthCheck.too_slow])
@given(st.binary(max_size=2000))
def test_parse_state_never_raises_and_what_it_accepts_round_trips(data: bytes) -> None:
    marks = parse_state(data)
    if marks is not None:
        assert parse_state(render_state(marks)) == marks


@settings(max_examples=300, deadline=1000)
@given(st.binary(min_size=SLOT_BYTES, max_size=SLOT_BYTES))
def test_a_live_slot_of_any_bytes_is_a_slot_or_refused(raw: bytes) -> None:
    try:
        slot = seqstate._unpack_slot(raw)
    except seqstate._Garbage:
        return
    assert slot.next <= slot.ceiling
    if slot.key:  # what it accepts, it writes back the same
        assert seqstate._pack_slot(slot.key, slot.next, slot.ceiling) == raw


@settings(max_examples=200, deadline=1000)
@given(
    st.sampled_from([KEY, "fc/telemetry/0x0b00", "ground/command/0x1891"]),
    st.integers(0, 2**63 - 1),
    st.integers(0, 2**63 - 1),
    st.integers(0, 88 * 8 - 1),
)
def test_the_loop_reads_its_own_slot_as_the_full_check_does(
    key: str, a: int, b: int, flipped: int
) -> None:
    """The take on the loop checks its stream's slot with a shortcut; it must accept exactly
    what the full check accepts for that key, a slot with one bit flipped included."""
    nxt, ceiling = min(a, b), max(a, b)
    raw = bytearray(seqstate._pack_slot(key, nxt, ceiling))
    assert seqstate._pack_own_slot(seqstate._padded(key), nxt, ceiling) == bytes(raw)
    for data in (
        bytes(raw),
        bytes(raw[: flipped // 8])
        + bytes([raw[flipped // 8] ^ (1 << (flipped % 8))])
        + bytes(raw[flipped // 8 + 1 :]),
    ):
        own = seqstate._own_slot(data, seqstate._padded(key))
        try:
            full = seqstate._unpack_slot(data)
        except seqstate._Garbage:
            assert own is None
            continue
        assert own == ((full.next, full.ceiling) if full.key == key else None)


def test_a_full_durable_file_of_the_longest_keys_fits_the_cap() -> None:
    marks = {
        key_of(f"{'n' * 28}{i:04d}", "telemetry", (1 << 64) - 1 - i): (1 << 63) - 1
        for i in range(MAX_STREAMS)
    }
    assert all(len(k) <= MAX_KEY_LEN for k in marks)
    raw = render_state(marks)
    assert len(raw) <= MAX_BYTES and parse_state(raw) == marks


def test_keys_have_one_spelling_and_odd_node_names_get_distinct_bounded_stems() -> None:
    a, b = key_of("x" * 300, "command", 5), key_of("x" * 299 + "y", "command", 5)
    assert a != b and len(a) <= MAX_KEY_LEN and len(b) <= MAX_KEY_LEN
    assert key_of("fc", "command", 5) != key_of("fc", "telemetry", 5)
    assert key_of("\x1b]0;x\x07", "command", 1) != key_of("\x1b]0;y\x07", "command", 1)
    for node in ("fc", "a-b_c", "x" * 300, "\U0001f680"):
        assert parse_key(key_of(node, "telemetry", 0x890)) == (node_stem(node), "telemetry", 0x890)
    for foreign in ("", "fc", "fc/command", "fc/other/0x1", "/command/0x1", "fc/command/0xzz"):
        assert parse_key(foreign) is None
    assert parse_key("fc/command/0x01890") is None  # r6: a second spelling of one stream
    assert parse_key("fc/command/0x1_890") is None


# -- ib seq at the store -------------------------------------------------------------------


def _seed(state: Path, boot: bytes = BOOT) -> None:
    """Stored counts for two nodes, as sends and a responder would leave them (in the boot a
    real ``ib`` sees, for the command-line tests)."""

    async def scenario() -> None:
        store = _store(state, boot=boot)
        for node, kind, mid in (
            ("fc", "command", 0x1890),
            ("fc", "command", 0x1891),
            ("fc", "telemetry", 0x0890),
            ("ground", "command", 0x1890),
        ):
            await _takes(store, 2, key_of(node, kind, mid))
        await store.aclose()

    state.mkdir(mode=0o700, exist_ok=True)
    _run(scenario())


def test_forget_selects_by_node_and_mid_and_everything_wipes_foreign_keys(tmp_path: Path) -> None:
    _seed(tmp_path)

    async def scenario() -> None:
        store = _store(tmp_path)
        assert await store.forget("fc", 0x1891) == [("fc", "command", 0x1891, 2)]
        assert len(await store.forget("fc")) == 2
        assert set(_marks(tmp_path)) == {key_of("ground", "command", 0x1890)}
        assert await store.forget() == [("ground", "command", 0x1890, 2)]
        assert _marks(tmp_path) == {}
        await store.aclose()

    _run(scenario())


def test_stored_reads_without_changing_any_file(tmp_path: Path) -> None:
    """S3: `ib seq show` rewrote an invalid file, destroying the evidence."""
    _seed(tmp_path)
    (tmp_path / STATE_NAME).write_text("not json")
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    reports = _Reports()

    async def scenario() -> list[Any]:
        store = _store(tmp_path, reports)
        rows = await store.stored()
        await store.aclose()
        return rows

    rows = _run(scenario())
    assert rows[0][:4] == ("fc", "command", 0x1890, 2)  # from the live table
    assert reports.codes == [Code.SEQ_RESET]
    assert {p.name: p.read_bytes() for p in tmp_path.iterdir()} == before


def test_stored_of_a_missing_directory_creates_nothing(tmp_path: Path) -> None:
    async def scenario() -> list[Any]:
        store = _store(tmp_path / "absent")
        rows = await store.stored()
        await store.aclose()
        return rows

    assert _run(scenario()) == []
    assert not (tmp_path / "absent").exists()


# -- the codec -----------------------------------------------------------------------------


def _codec() -> Codec:
    return Codec(
        load_ccdd(EXAMPLES / "ccdd" / "mock.json"),
        load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml"),
        load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml"),
        clock=lambda: 1_000_000.25,
    )


def test_a_count_is_reduced_to_the_width_of_the_sequence_field() -> None:
    codec = _codec()
    prepared = codec.prepare_command("MOCK_NOOP")
    assert prepared.sequenced
    assert [codec.frame(prepared, n).sequence for n in (16382, 16383, 16384, 16385)] == [
        16382,
        16383,
        0,
        1,
    ]


def test_set_sequence_is_validated_against_the_field_width() -> None:
    codec = _codec()
    for bad in (-1, 16384, 2**70):
        with pytest.raises(IronbirdError) as info:
            codec.check_sequence(MessageKind.COMMAND, bad)
        assert info.value.code is Code.ENC_ARG
    codec.check_sequence(MessageKind.COMMAND, 16383)


def test_set_sequence_is_refused_when_the_profile_has_no_sequence_field(tmp_path: Path) -> None:
    text = (EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml").read_text()
    assert 'name = "sequence"\nbits = 14\ncomputed = "sequence"' in text
    plain = tmp_path / "nosequence.toml"
    plain.write_text(
        text.replace(
            'name = "sequence"\nbits = 14\ncomputed = "sequence"',
            'name = "sequence"\nbits = 14\nvalue = 0',
        )
    )
    codec = Codec(
        load_ccdd(EXAMPLES / "ccdd" / "mock.json"),
        load_profile(plain),
        load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml"),
    )
    prepared = codec.prepare_command("MOCK_NOOP")
    assert not prepared.sequenced and codec.frame(prepared).sequence is None
    with pytest.raises(IronbirdError) as info:
        codec.check_sequence(MessageKind.COMMAND, 1)
    assert info.value.code is Code.ENC_ARG


def test_without_a_count_the_codec_counts_in_memory_from_zero() -> None:
    codec = _codec()
    assert [codec.encode_command("MOCK_NOOP").sequence for _ in range(2)] == [0, 1]
    assert codec.encode_command("MOCK_NOOP", sequence=9).sequence == 9
    assert codec.encode_command("MOCK_NOOP").sequence == 10


# -- real processes ------------------------------------------------------------------------


def _ib(config: Path, *args: str, timeout: float = 30) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "ironbird.cli.main", "-c", str(config), *args],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def _seq(done: subprocess.CompletedProcess[str]) -> int:
    assert done.returncode == 0, done.stderr
    return int(done.stdout.split(" seq ")[1].split(",")[0].split(")")[0].split()[0])


@pytest.fixture
def responder(mock_config_path: Path) -> Any:
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "ironbird.cli.main",
            "-c",
            str(mock_config_path),
            "respond",
            "--as",
            "fc",
            "--timeout",
            "90",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert proc.stderr is not None
    for _ in range(200):
        line = proc.stderr.readline()
        if "responding" in line:
            break
        assert line or proc.poll() is None, "respond exited"
    yield proc
    proc.kill()
    proc.communicate()


def _events(state: Path, name: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for path in sorted(state.glob("*.jsonl")):
        for line in path.read_text().splitlines():
            event = json.loads(line)
            if event.get("event") == name:
                out.append(event)
    return out


def _tx_sequences(state: Path) -> list[int]:
    return [e["sequence"] for e in _events(state, "tx.command")]


def test_two_consecutive_sends_count_zero_then_one(
    mock_config_path: Path, responder: Any, tmp_path: Path
) -> None:
    """Each `ib send` is its own process; the count used to restart at 0 in every one."""
    assert _seq(_ib(mock_config_path, "send", "fc", "MOCK_NOOP")) == 0
    assert _seq(_ib(mock_config_path, "send", "fc", "MOCK_NOOP")) == 1
    assert _seq(_ib(mock_config_path, "send", "fc", "MOCK_SET_MODE", "mode=SAFE", "pad=0")) == 2
    assert sorted(_tx_sequences(tmp_path / "state")) == [0, 1, 2]


def test_send_sequence_sets_one_packet_and_counts_on_from_it(
    mock_config_path: Path, responder: Any, tmp_path: Path
) -> None:
    assert _seq(_ib(mock_config_path, "send", "fc", "MOCK_NOOP", "--sequence", "100")) == 100
    assert _seq(_ib(mock_config_path, "send", "fc", "MOCK_NOOP")) == 101
    assert _seq(_ib(mock_config_path, "send", "fc", "MOCK_NOOP", "--sequence", "0x3fff")) == 16383
    assert _seq(_ib(mock_config_path, "send", "fc", "MOCK_NOOP")) == 0  # wrapped at 14 bits
    assert sorted(_tx_sequences(tmp_path / "state")) == [0, 100, 101, 16383]


@pytest.mark.parametrize("value", ["16384", "-1", "abc", "1.5", "\N{ARABIC-INDIC DIGIT ONE}"])
def test_send_sequence_refuses_what_does_not_fit_and_sends_nothing(
    mock_config_path: Path, responder: Any, tmp_path: Path, value: str
) -> None:
    done = _ib(mock_config_path, "send", "fc", "MOCK_NOOP", "--sequence", value)
    assert done.returncode == 2 and not done.stdout
    assert "error IB-" in done.stderr
    assert _tx_sequences(tmp_path / "state") == []
    assert _seq(_ib(mock_config_path, "send", "fc", "MOCK_NOOP")) == 0  # no count was taken


def test_concurrent_send_loops_reach_the_wire_in_count_order(
    mock_config_path: Path, responder: Any, tmp_path: Path
) -> None:
    """B3 (r5): two `ib send` loops on one MID jumped by 63 at every overlap and wrapped a
    14-bit field after ~680 sends. Now the responder receives 0, 1, 2, ... in arrival order."""
    loop = textwrap.dedent(
        """
        import subprocess, sys
        for _ in range(int(sys.argv[2])):
            done = subprocess.run([sys.executable, "-m", "ironbird.cli.main", "-c", sys.argv[1],
                                   "send", "fc", "MOCK_NOOP"], capture_output=True, text=True)
            if done.returncode:
                raise SystemExit(done.stderr)
        """
    )
    loops = [
        subprocess.Popen([sys.executable, "-c", loop, str(mock_config_path), "8"], text=True)
        for _ in range(2)
    ]
    for proc in loops:
        assert proc.wait(timeout=120) == 0
    state = tmp_path / "state"
    assert sorted(_tx_sequences(state)) == list(range(16))
    deadline = time.monotonic() + 10
    while len(_events(state, "rx.command")) < 8 and time.monotonic() < deadline:
        time.sleep(0.1)
    # one ledger writer, so file order is arrival order; rx.command is evidence, sampled
    arrived = [e["sequence"] for e in _events(state, "rx.command")]
    assert len(arrived) >= 8 and arrived == sorted(set(arrived)) and set(arrived) <= set(range(16))


def test_parallel_sends_get_consecutive_counts(
    mock_config_path: Path, responder: Any, tmp_path: Path
) -> None:
    procs = [
        subprocess.Popen(
            [
                sys.executable,
                "-m",
                "ironbird.cli.main",
                "-c",
                str(mock_config_path),
                "send",
                "fc",
                "MOCK_NOOP",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(8)
    ]
    counts: list[int] = []
    for proc in procs:
        out, err = proc.communicate(timeout=60)
        assert proc.returncode == 0, err
        counts.append(int(out.split(" seq ")[1].split(",")[0].split()[0]))
    assert sorted(counts) == list(range(8))
    assert sorted(_tx_sequences(tmp_path / "state")) == sorted(counts)


def test_responder_replies_count_on_across_restarts(mock_config_path: Path, tmp_path: Path) -> None:
    def replies() -> list[int]:
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "ironbird.cli.main",
                "-c",
                str(mock_config_path),
                "respond",
                "--as",
                "fc",
                "--timeout",
                "60",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        assert proc.stderr is not None
        while "responding" not in proc.stderr.readline():
            assert proc.poll() is None, "respond exited"
        for _ in range(3):
            assert _ib(mock_config_path, "send", "fc", "MOCK_NOOP").returncode == 0
        proc.terminate()
        proc.communicate(timeout=30)
        return [e["sequence"] for e in _events(tmp_path / "state", "tx.telemetry")]

    first = replies()
    assert first == [0, 1, 2]
    assert replies()[3:] == [3, 4, 5]


@pytest.mark.parametrize(
    "path",
    [p for p in sorted(CORPUS.glob("seqstate-*.json")) if p.stem != "seqstate-oversize"][:6],
    ids=lambda p: p.stem,
)
def test_a_damaged_state_file_warns_once_and_the_send_goes_out(
    mock_config_path: Path, responder: Any, tmp_path: Path, path: Path
) -> None:
    state = tmp_path / "state"
    state.mkdir(mode=0o700, exist_ok=True)
    (state / STATE_NAME).write_bytes(path.read_bytes())
    first = _ib(mock_config_path, "send", "fc", "MOCK_NOOP")
    assert _seq(first) == 0
    assert len([ln for ln in first.stderr.splitlines() if Code.SEQ_RESET.value in ln]) == 1
    second = _ib(mock_config_path, "send", "fc", "MOCK_NOOP")
    assert _seq(second) == 1 and Code.SEQ_RESET.value not in second.stderr  # repaired
    assert len(_events(state, "sequence.reset")) == 1
    assert (state / INVALID_NAME).read_bytes() == path.read_bytes()


def test_an_unusable_state_directory_warns_once_and_counts_in_memory(
    mock_config_path: Path, responder: Any, tmp_path: Path
) -> None:
    state = tmp_path / "state"
    state.mkdir(mode=0o700, exist_ok=True)
    (state / LOCK_NAME).mkdir()  # the lock cannot be opened, as in a read-only directory
    first = _ib(mock_config_path, "send", "fc", "MOCK_NOOP")
    assert _seq(first) == 0 and f"warning {Code.SEQ_UNSAVED.value}" in first.stderr
    assert first.stderr.count(Code.SEQ_UNSAVED.value) == 1
    second = _ib(mock_config_path, "send", "fc", "MOCK_NOOP")
    assert _seq(second) == 0  # restarts at 0 in every process, as warned
    assert not (state / STATE_NAME).exists()
    assert _events(state, "sequence.memory_only")


# -- ib seq show / ib seq reset ------------------------------------------------------------


def _stored_keys(state: Path) -> set[str]:
    return set(_marks(state))


def test_seq_reset_needs_to_be_told_what_to_reset(mock_config_path: Path, tmp_path: Path) -> None:
    state = tmp_path / "state"
    _seed(state, read_boot_id())
    before = _marks(state)
    for args in (
        ["seq", "reset"],
        ["seq", "reset", "--all", "fc"],
        ["seq", "reset", "--all", "fc", "1"],
    ):
        done = _ib(mock_config_path, *args)
        assert done.returncode == 2 and not done.stdout
    assert _marks(state) == before


def test_seq_reset_forms(mock_config_path: Path, responder: Any, tmp_path: Path) -> None:
    state = tmp_path / "state"
    _seed(state, read_boot_id())
    assert _seq(_ib(mock_config_path, "send", "fc", "MOCK_NOOP")) == 2  # counts on from the seed
    one = _ib(mock_config_path, "seq", "reset", "fc", "0x1891")
    assert one.returncode == 0 and _stored_keys(state) >= {key_of("fc", "command", 0x1890)}
    assert key_of("fc", "command", 0x1891) not in _stored_keys(state)
    named = _ib(mock_config_path, "seq", "reset", "fc", "MOCK_NOOP")  # a message name
    assert named.returncode == 0 and key_of("fc", "command", 0x1890) not in _stored_keys(state)
    assert _seq(_ib(mock_config_path, "send", "fc", "MOCK_NOOP")) == 0
    node = _ib(mock_config_path, "seq", "reset", "fc")
    assert node.returncode == 0
    assert not [k for k in _stored_keys(state) if k.startswith("fc/")]
    assert key_of("ground", "command", 0x1890) in _stored_keys(state)  # the other node stays
    everything = _ib(mock_config_path, "seq", "reset", "--all")
    assert everything.returncode == 0 and _stored_keys(state) == set()
    again = _ib(mock_config_path, "seq", "reset", "--all")  # nothing left: not an error
    assert again.returncode == 0
    assert _seq(_ib(mock_config_path, "send", "fc", "MOCK_NOOP")) == 0
    ledgered = [e for e in _events(state, "seq.reset") if e.get("kind") == "action"]
    assert len(ledgered) == 5
    assert any(r["removed"] and r["removed"][0]["kind"] == "command" for r in ledgered)


def test_seq_reset_refuses_an_unknown_node_or_mid(mock_config_path: Path, tmp_path: Path) -> None:
    state = tmp_path / "state"
    _seed(state, read_boot_id())
    before = _marks(state)
    unknown = _ib(mock_config_path, "seq", "reset", "nowhere")
    assert unknown.returncode == 2 and Code.CFG_REF.value in unknown.stderr
    bad_mid = _ib(mock_config_path, "seq", "reset", "fc", "NO_SUCH_MESSAGE")
    assert bad_mid.returncode == 2 and Code.CLI_USAGE.value in bad_mid.stderr
    huge = _ib(mock_config_path, "seq", "reset", "fc", "0x100000000")
    assert huge.returncode == 2 and Code.CLI_USAGE.value in huge.stderr
    assert _marks(state) == before


def test_seq_show_lists_the_stored_counts(mock_config_path: Path, tmp_path: Path) -> None:
    state = tmp_path / "state"
    state.mkdir(mode=0o700, exist_ok=True)
    assert "no stored" in _ib(mock_config_path, "seq", "show").stdout
    _seed(state, read_boot_id())
    everything = _ib(mock_config_path, "seq", "show")
    assert everything.returncode == 0 and len(everything.stdout.splitlines()) == 1 + 4
    only_fc = _ib(mock_config_path, "seq", "show", "fc")
    assert len(only_fc.stdout.splitlines()) == 1 + 3
    assert _ib(mock_config_path, "seq", "show", "nowhere").returncode == 2


def test_seq_show_gives_the_value_the_next_packet_carries(
    mock_config_path: Path, responder: Any, tmp_path: Path
) -> None:
    """N1: after --sequence 0x3fff, show said 16384 while the next packet carries 0."""
    assert _seq(_ib(mock_config_path, "send", "fc", "MOCK_NOOP", "--sequence", "0x3fff")) == 16383
    row = _ib(mock_config_path, "seq", "show", "fc").stdout.splitlines()[1].split()
    assert row[-1] == "0"
    verbose = _ib(mock_config_path, "seq", "show", "fc", "-v").stdout.splitlines()[1].split()
    assert "16384" in verbose  # the unreduced count, for whoever needs it


def test_seq_show_leaves_an_invalid_state_file_untouched(
    mock_config_path: Path, tmp_path: Path
) -> None:
    """S3 (r6): the read-only command rewrote the file it was asked to inspect."""
    state = tmp_path / "state"
    state.mkdir(mode=0o700, exist_ok=True)
    (state / STATE_NAME).write_bytes(b"not json")
    digest = hashlib.sha1((state / STATE_NAME).read_bytes()).hexdigest()  # noqa: S324
    done = _ib(mock_config_path, "seq", "show")
    assert done.returncode == 0 and Code.SEQ_RESET.value in done.stderr
    assert hashlib.sha1((state / STATE_NAME).read_bytes()).hexdigest() == digest  # noqa: S324
    assert not (state / INVALID_NAME).exists() and not (state / LIVE_NAME).exists()


def test_ledger_records_from_the_store_have_tiers() -> None:
    source = (Path(__file__).parents[1] / "src" / "ironbird" / "seqstate.py").read_text()
    named = set(re.findall(r'"(sequence\.[a-z_]+)"', source))
    assert named and named <= set(RECORD_TIERS)
