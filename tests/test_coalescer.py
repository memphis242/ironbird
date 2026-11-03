"""The notification coalescer with a hand-driven clock: repeats inside a window are counted not
re-shown, a closed window leaves one summary, per-code groups cap how many distinct keys can
speak, and the key table stays bounded however many unique keys arrive."""

from __future__ import annotations

from ironbird.coalesce import MAX_NEW_PER_GROUP, Coalescer
from ironbird.errors import Code
from ironbird.notice import Level, Notice


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def make(**kwargs: float | int) -> tuple[Coalescer, list[Notice], Clock]:
    clock = Clock()
    shown: list[Notice] = []
    return Coalescer(shown.append, clock=clock, **kwargs), shown, clock  # type: ignore[arg-type]


def test_a_flood_of_one_key_is_one_line_then_one_summary_when_the_window_closes() -> None:
    co, shown, clock = make(window_s=10.0)
    for _ in range(500):
        co.report(Code.DEC_CHECKSUM, Level.ERROR, "k", "bad checksum")
        clock.now += 0.01
    assert [n.count for n in shown] == [1]
    clock.now += 20
    co.report(Code.DEC_CHECKSUM, Level.ERROR, "k", "bad checksum")  # the late report closes it
    assert [n.count for n in shown][:2] == [1, 500]
    assert shown[1].window_s > 0
    assert len(shown) == 3  # ... and opens the next window with a fresh first line
    assert shown[2].count == 1


def test_tick_closes_expired_windows_without_a_new_report() -> None:
    co, shown, clock = make(window_s=5.0)
    co.report(Code.DEC_LENGTH, Level.ERROR, "a", "x")
    co.report(Code.DEC_LENGTH, Level.ERROR, "a", "x")
    co.tick()
    assert len(shown) == 1  # still open
    clock.now += 6
    co.tick()
    assert [n.count for n in shown] == [1, 2]
    co.tick()
    assert len(shown) == 2  # nothing is emitted twice


def test_a_key_that_never_repeated_leaves_no_summary() -> None:
    co, shown, clock = make(window_s=5.0)
    co.report(Code.DEC_LENGTH, Level.ERROR, "once", "x")
    clock.now += 60
    co.tick()
    co.flush()
    assert len(shown) == 1


def test_discard_drops_the_pending_summary_of_a_failure_that_was_raised_instead() -> None:
    co, shown, _ = make(window_s=5.0)
    co.report(Code.TX_RECV, Level.ERROR, "rx", "failed")
    co.report(Code.TX_RECV, Level.ERROR, "rx", "failed")
    co.discard("rx")
    co.discard("never-existed")
    co.flush()
    assert len(shown) == 1


def test_flush_emits_every_pending_summary_once() -> None:
    co, shown, _ = make()
    for key in ("a", "b"):
        co.report(Code.DEC_LENGTH, Level.ERROR, key, "x")
        co.report(Code.DEC_LENGTH, Level.ERROR, key, "x")
    co.flush()
    co.flush()
    assert sorted(n.count for n in shown) == [1, 1, 2, 2]


def test_the_key_table_is_bounded_and_evicts_the_oldest_with_its_summary() -> None:
    co, shown, _ = make(max_keys=4)
    for i in range(4):
        co.report(Code.DEC_UNKNOWN, Level.ERROR, f"k{i}", "x")
        co.report(Code.DEC_UNKNOWN, Level.ERROR, f"k{i}", "x")  # each has something to summarise
    shown.clear()
    co.report(Code.DEC_UNKNOWN, Level.ERROR, "newcomer", "x")
    assert [n.key for n in shown] == ["k0", "newcomer"]  # the oldest left, its count told
    assert len(co._entries) == 4
    for i in range(10_000):
        co.report(Code.DEC_UNKNOWN, Level.ERROR, f"flood{i}", "x")
    assert len(co._entries) == 4


def test_a_group_lets_only_a_few_distinct_keys_speak_per_window() -> None:
    co, shown, clock = make(window_s=10.0)
    for i in range(MAX_NEW_PER_GROUP + 20):
        co.report(Code.DEC_UNKNOWN, Level.ERROR, f"mid{i}", f"unknown {i}", group=Code.DEC_UNKNOWN)
    individual = [n for n in shown if n.key.startswith("mid")]
    assert len(individual) == MAX_NEW_PER_GROUP
    clock.now += 11
    co.report(Code.DEC_UNKNOWN, Level.ERROR, "mid-later", "x", group=Code.DEC_UNKNOWN)
    assert any(n.key == "mid-later" for n in shown), "a new window never opened for the group"
    co.flush()
    assert any(n.count > 1 and "other problems" in n.summary for n in shown)


def test_a_later_occurrence_keeps_the_pointer_to_its_ledger_record() -> None:
    co, shown, clock = make(window_s=5.0)
    co.report(Code.DEC_LENGTH, Level.ERROR, "k", "x", ledger_n=1)
    co.report(Code.DEC_LENGTH, Level.ERROR, "k", "x", ledger_n=7, ledger_part=2, ledger_file="f")
    co.report(Code.DEC_LENGTH, Level.ERROR, "k", "x")  # no record this time: pointer stays
    clock.now += 6
    co.tick()
    summary = shown[-1]
    assert (summary.count, summary.ledger_n, summary.ledger_part, summary.ledger_file) == (
        3,
        7,
        2,
        "f",
    )


def test_a_per_key_window_overrides_the_default() -> None:
    co, shown, clock = make(window_s=10.0)
    co.report(Code.LED_TIER, Level.WARNING, "slow", "x", window_s=600.0)
    co.report(Code.LED_TIER, Level.WARNING, "slow", "x")
    clock.now += 300
    co.tick()
    assert len(shown) == 1  # the 10 min window is still open
    clock.now += 400
    co.tick()
    assert [n.count for n in shown] == [1, 2]
