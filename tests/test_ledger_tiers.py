"""Stage 2d, layer 1: tiers, allowances, evidence sampling and the sizes they are derived from.

The arithmetic objects run on an injected clock, so simulated hours cost milliseconds. The
sizing guard measures record sizes through the real formatter, never from a constant.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from conftest import EXAMPLES
from ironbird.ccdd.loader import load_ccdd
from ironbird.ccdd.model import MessageKind
from ironbird.codec import Codec
from ironbird.config import LedgerConfig
from ironbird.errors import Code
from ironbird.events import extract_event
from ironbird.frame import Direction, Frame, FrameFlag, Stream
from ironbird.header import load_profile
from ironbird.ledger import Ledger
from ironbird.ledgertiers import (
    HOUR_S,
    Admit,
    EvidenceSampler,
    Tier,
    TierBudget,
    TokenBucket,
)
from ironbird.notice import Notice
from ironbird.session import event_record_fields
from ironbird.units import GIB, KIB, MIB
from ledgerutil import all_records
from support.faultfs import FakeClock

EVENT_BYTES_PER_HOUR = 160 * MIB

# -- the token bucket and the allowance ----


def test_token_bucket_refills_at_its_rate_up_to_the_burst() -> None:
    clock = FakeClock(0.0)
    bucket = TokenBucket(10.0, 600.0, clock)
    assert all(bucket.take() for _ in range(600)) and not bucket.take()
    clock.advance(1.0)
    assert sum(bucket.take() for _ in range(20)) == 10
    clock.advance(3600.0)
    assert sum(bucket.take() for _ in range(700)) == 600  # never more than the burst


def test_allowance_grows_with_run_time_after_a_one_hour_floor() -> None:
    budget = TierBudget(160 * MIB, start=0.0)
    assert budget.allowance(0.0) == 160 * MIB
    assert budget.allowance(1800.0) == 160 * MIB  # the first hour's worth is a floor
    assert budget.allowance(2 * HOUR_S) == 320 * MIB
    assert budget.allowance(12 * HOUR_S) == 12 * 160 * MIB


def test_unlimited_is_never_over() -> None:
    budget = TierBudget(None, start=0.0)
    assert budget.allowance(5.0) is None and not budget.update(10**15, 5.0)


def test_hysteresis_is_capped_at_a_minute_of_the_allowances_growth() -> None:
    budget = TierBudget(160 * MIB, start=0.0)
    # 1 % of a 12 h allowance is about 7 min of growth; the cap keeps it at 1 min.
    assert budget.hysteresis(12 * HOUR_S) == pytest.approx(160 * MIB / 60)
    assert budget.hysteresis(HOUR_S) == pytest.approx(min(1.6 * MIB, 160 * MIB / 60))
    small = TierBudget(MIB, start=0.0)  # the 1 MiB floor is capped by bytes_per_hour / 60
    assert small.hysteresis(HOUR_S) == pytest.approx(MIB / 60)


def test_over_the_allowance_resumes_only_with_hysteresis() -> None:
    budget = TierBudget(160 * MIB, start=0.0)
    assert not budget.update(160 * MIB - 1, 10.0)
    assert budget.update(160 * MIB, 10.0)
    assert budget.update(160 * MIB - 1, 10.0)  # no flapping: still counted only
    h = budget.hysteresis(10.0)
    assert not budget.update(int(160 * MIB - h), 10.0)


def test_a_tier_over_in_the_first_hour_stays_counted_until_the_hour_ends() -> None:
    budget = TierBudget(160 * MIB, start=0.0)
    written = 160 * MIB
    assert budget.update(written, 100.0)
    for t in (500.0, 1800.0, 3599.0):
        assert budget.update(written, t), "the floor is constant, the bytes on disk never shrink"
    # After the hour the allowance grows; resumption waits for allowance - h >= bytes, which is
    # about h / (bytes_per_hour / 3600) seconds - the cap keeps that near a minute.
    resumed = next(t for t in range(3600, 3800) if not budget.update(written, float(t)))
    assert 3600 < resumed <= 3600 + 62


# -- the 12 h event-rate arithmetic ----


EVENT_RECORD_BYTES = 947  # the full-text event record, measured below by the sizing guard


def _simulate(
    rate_of: Any, hours: float, *, bytes_per_hour: int = EVENT_BYTES_PER_HOUR
) -> dict[str, Any]:
    """Feed a rate profile (events/s as a function of the simulated second) to a budget."""
    budget = TierBudget(bytes_per_hour, start=0.0)
    written, episodes, first_over = 0, 0, None
    for t in range(int(hours * HOUR_S)):
        was = budget.over
        if not budget.update(written, float(t)):
            written += int(rate_of(t) * EVENT_RECORD_BYTES)
        if budget.over and not was:
            episodes += 1
            first_over = t if first_over is None else first_over
    return {"written": written, "episodes": episodes, "first_over": first_over}


def _test_profile(t: int) -> float:
    """Each hour: 45 min at about 1/s (idle seconds mixed in), 12 min at 30/s, 3 min at 100/s."""
    minute = (t % 3600) // 60
    if minute < 45:
        return 1.0 if t % 10 else 0.0
    return 30.0 if minute < 57 else 100.0


def test_the_expected_event_profile_never_reaches_the_allowance() -> None:
    result = _simulate(_test_profile, 12)
    assert result["episodes"] == 0
    assert result["written"] / (12 * MIB) < 40  # about 38 MiB/h, the documented 458 MiB in 12 h


def test_30_events_a_second_for_12_hours_never_reaches_the_allowance() -> None:
    assert _simulate(lambda _t: 30.0, 12)["episodes"] == 0


def test_a_100_per_second_burst_from_a_cold_start_then_30_a_second_stays_under() -> None:
    result = _simulate(lambda t: 100.0 if t < 15 * 60 else 30.0, 12)
    assert result["episodes"] == 0


def test_a_runaway_1000_per_second_reaches_the_allowance_at_the_predicted_time() -> None:
    result = _simulate(lambda _t: 1000.0, 3)
    predicted = EVENT_BYTES_PER_HOUR / (1000 * EVENT_RECORD_BYTES)  # about 177 s: "3 min"
    assert result["first_over"] == pytest.approx(predicted, rel=0.01)
    assert result["episodes"] > 1  # then episodes as the allowance grows
    assert result["written"] <= 3 * EVENT_BYTES_PER_HOUR  # bounded by the allowance at 3 h


# -- the real ledger, scaled ----


def _event_fields(index: int) -> dict[str, Any]:
    return {"node": "fc", "app": "SAMPLE_APP", "event_id": index % 7, "text": "x" * 100}


def test_the_real_ledger_follows_the_scaled_12_hour_profile_without_a_warning(
    tmp_path: Path,
) -> None:
    """The 12 h test profile at 1:64 (rates and allowance), real bytes on disk: never counted
    only, and the tier's bytes stay within the allowance at every simulated minute."""
    clock = FakeClock(0.0)
    scale = 64
    per_hour = EVENT_BYTES_PER_HOUR // scale
    config = LedgerConfig(event_bytes_per_hour=per_hour)
    shown: list[Notice] = []
    ledger = Ledger(
        tmp_path / "l", session_id="s", config=config, clock=clock, writer_poll_s=3600.0
    )
    ledger.set_notifier(shown.append)
    ledger.open()
    carry, index = 0.0, 0
    for second in range(12 * 3600):
        clock.now = float(second)
        carry += _test_profile(second) / scale
        while carry >= 1.0:
            carry -= 1.0
            ledger.record("event", "rx.event", **_event_fields(index))
            index += 1
        if second % 60 == 59:
            ledger.tick()
            ledger.flush()
            allowance = TierBudget(per_hour, 0.0).allowance(float(second))
            assert allowance is not None and ledger.written_bytes[Tier.EVENTS] <= allowance
    ledger.close()
    assert not [n for n in shown if n.code is Code.LED_TIER]
    assert ledger.counted[Tier.EVENTS] == (0, 0)


def test_a_runaway_event_storm_makes_episodes_with_gap_markers_and_few_notices(
    tmp_path: Path,
) -> None:
    clock = FakeClock(0.0)
    config = LedgerConfig(event_bytes_per_hour=MIB)  # the smallest allowance
    shown: list[Notice] = []
    ledger = Ledger(
        tmp_path / "l", session_id="s", config=config, clock=clock, writer_poll_s=3600.0
    )
    ledger.set_notifier(shown.append)
    ledger.open()
    for second in range(0, 3 * 3600, 5):
        clock.now = float(second)
        for i in range(200):
            ledger.record("event", "rx.event", **_event_fields(i))
        ledger.tick()
        ledger.flush()
    ledger.close()
    rows = all_records(ledger)
    over = [r for r in rows if r["event"] == "ledger.tier_over"]
    gaps = [r for r in rows if r["event"] == "ledger.tier_gap"]
    assert len(over) > 20 and len(gaps) >= len(over) - 1  # about one episode a minute after 1 h
    assert all(g["records"] > 0 and g["counted_tier"] == "events" for g in gaps)
    # every episode is ledgered; the display is one line per 10 minutes, later episodes fold into
    # the next line's count
    ledger.flush_notices()
    tier_notices = [n for n in shown if n.code is Code.LED_TIER]
    firsts = [n for n in tier_notices if n.count == 1]
    assert 1 <= len(firsts) <= 3 * 6 + 1 and len(firsts) < len(over)
    folded = sum(n.count - 1 for n in tier_notices if n.count > 1)
    assert len(firsts) + folded == len(over)
    allowance = TierBudget(MIB, 0.0).allowance(3 * 3600.0)
    assert allowance is not None and ledger.written_bytes[Tier.EVENTS] <= allowance
    assert ledger.counted[Tier.EVENTS][0] > 0


def test_unlimited_tiers_are_never_counted_only(tmp_path: Path) -> None:
    config = LedgerConfig(event_bytes_per_hour=None, evidence_bytes_per_hour=None)
    shown: list[Notice] = []
    ledger = Ledger(tmp_path / "l", session_id="s", config=config)
    ledger.set_notifier(shown.append)
    for i in range(3000):
        ledger.record("event", "rx.event", **_event_fields(i))
        ledger.record("packet", "rx.other", raw=b"\x00" * 100)
    ledger.close()
    assert ledger.counted[Tier.EVENTS] == (0, 0) and ledger.counted[Tier.EVIDENCE] == (0, 0)
    assert [n.code for n in shown] == [Code.LED_UNBOUNDED]  # evidence "unlimited" says so once


# -- evidence sampling ----


def _sampler(clock: FakeClock, rate: int = 256 * KIB, **kwargs: Any) -> EvidenceSampler:
    return EvidenceSampler(clock, rate, **kwargs)


def test_the_first_eight_are_kept_then_one_a_second_and_counts_are_exact() -> None:
    clock = FakeClock(0.0)
    sampler = _sampler(clock)
    verdicts = [sampler.admit("rx.malformed", "k", time_ns=i, seq=i, size=10) for i in range(100)]
    assert verdicts[:8] == [Admit.FULL] * 8
    assert verdicts[8:] == [Admit.SKIP] * 92  # no time passed: nothing more is kept
    clock.advance(1.0)
    assert sampler.admit("rx.malformed", "k", time_ns=100, seq=100, size=10) is Admit.FULL
    assert sampler.admit("rx.malformed", "k", time_ns=101, seq=101, size=10) is Admit.SKIP
    (summary,) = sampler.close_window()
    assert (summary.count, summary.kept, summary.skipped) == (102, 9, 93)
    assert (summary.bytes, summary.first_seq, summary.last_seq) == (1020, 0, 101)
    assert summary.first_ns == 0 and summary.last_ns == 101


def test_a_key_that_was_never_sampled_away_has_no_summary() -> None:
    sampler = _sampler(FakeClock(0.0))
    for i in range(8):
        sampler.admit("rx.malformed", "quiet", time_ns=i)
    assert sampler.close_window() == []


def test_the_1025th_key_folds_into_other_and_the_table_is_empty_after_the_window() -> None:
    clock = FakeClock(0.0)
    sampler = _sampler(clock)
    for i in range(1025):
        sampler.admit("rx.malformed", f"key{i}", time_ns=i)
    assert sampler.keys == 1025  # 1024 keys plus the one "other" entry
    for _ in range(20):
        sampler.admit("rx.malformed", "key-new", time_ns=0)  # folds into "other"
    (summary,) = sampler.close_window()
    assert summary.key == "other|rx.malformed"
    assert summary.count == 1 + 20 and summary.skipped == 13  # 8 kept in full, the rest counted
    assert sampler.keys == 0
    clock.advance(11.0)
    assert sampler.admit("rx.malformed", "key7", time_ns=0) is Admit.FULL


def test_the_raw_budget_drops_raw_bytes_not_records() -> None:
    clock = FakeClock(0.0)
    sampler = _sampler(clock, rate=1000)
    first = sampler.admit("rx.malformed", "a", time_ns=0, size=600, raw_len=600)
    second = sampler.admit("rx.malformed", "b", time_ns=0, size=600, raw_len=600)
    assert (first, second) == (Admit.FULL, Admit.NO_RAW)
    assert sampler.raw_dropped == 1
    clock.advance(1.0)
    assert sampler.admit("rx.malformed", "c", time_ns=0, size=600, raw_len=600) is Admit.FULL


def test_window_closing_is_driven_by_the_clock() -> None:
    clock = FakeClock(0.0)
    sampler = _sampler(clock)
    assert not sampler.window_due()
    clock.advance(10.0)
    assert sampler.window_due()
    sampler.close_window()
    assert not sampler.window_due()


def test_a_malformed_flood_through_the_ledger_is_sampled_and_summarised(tmp_path: Path) -> None:
    clock = FakeClock(0.0)
    ledger = Ledger(tmp_path / "l", session_id="s", clock=clock, writer_poll_s=3600.0)
    ledger.open()
    numbers = [
        ledger.record("packet", "rx.malformed", sample="IB-DEC-005:1:fc", raw=bytes(64), seq=i)
        for i in range(5000)
    ]
    assert sum(n is not None for n in numbers) == 8
    ledger.settle()
    ledger.close()
    rows = all_records(ledger)
    kept = [r for r in rows if r["event"] == "rx.malformed"]
    (summary,) = [r for r in rows if r["event"] == "rx.malformed.summary"]
    assert len(kept) == 8 and summary["count"] == 5000 and summary["skipped"] == 4992
    assert (summary["first_seq"], summary["last_seq"]) == (0, 4999)
    assert summary["bytes"] == 5000 * 64
    assert ledger.sampled_away == 4992


def test_a_spent_raw_budget_keeps_the_record_and_drops_its_bytes(tmp_path: Path) -> None:
    clock = FakeClock(0.0)
    config = LedgerConfig(evidence_raw_bytes_per_second=KIB)
    ledger = Ledger(
        tmp_path / "l", session_id="s", config=config, clock=clock, writer_poll_s=3600.0
    )
    ledger.open()
    for i in range(4):
        ledger.record("packet", "rx.malformed", sample=f"k{i}", raw=bytes(600), seq=i)
    ledger.close()
    rows = [r for r in all_records(ledger) if r["event"] == "rx.malformed"]
    assert len(rows) == 4
    assert [("raw" in r) for r in rows] == [True, False, False, False]
    assert all(r["raw_len"] == 600 for r in rows) and rows[1]["raw_omitted"] is True


# -- the sizes the allowances are derived from ----


def _long_event_record_size(
    *, message: str, app: str = "SAMPLE_APP", fill: int | None = None
) -> int:
    """Bytes of the ``rx.event`` line (newline included) for an ``EVS_LONG_EVENT`` of the example
    CCDD, through the real formatter. ``fill`` overwrites the text bytes with that byte value."""
    codec = Codec(
        load_ccdd(EXAMPLES / "ccdd" / "cfs.json"),
        load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml"),
        load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml"),
        clock=lambda: 1_000_000.25,
    )
    encoded = codec.encode_telemetry(
        "EVS_LONG_EVENT",
        {
            "AppName": app,
            "EventID": 3,
            "EventType": 2,
            "SpacecraftID": 66,
            "ProcessorID": 1,
            "Message": message,
        },
    )
    data = bytearray(encoded.data)
    if fill is not None:
        start = len(data) - (20 + 2 + 2 + 4 + 4 + 122 + 2)  # AppName .. Padding at the tail
        data[start : start + 20] = bytes([fill]) * 20
        data[start + 32 : start + 32 + 122] = bytes([fill]) * 122
    decoded = codec.decode(MessageKind.TELEMETRY, bytes(data))
    event = extract_event("fc", 1_700_000_000_000_000_000, decoded)  # type: ignore[arg-type]
    assert event is not None
    stream = Stream(0, "l", "fc", MessageKind.TELEMETRY, Direction.RX, "udp", "udp 0.0.0.0:1")
    frame = Frame(
        1,
        1_700_000_000_000_000_000,
        stream,
        "127.0.0.1:2234",
        encoded.mid,
        FrameFlag.HAS_MID,
        len(data),
        bytes(data),
    )
    probe = Ledger(Path("/nonexistent"), session_id="0123456789ab", enabled=False)
    entry = (
        1_700_000_000_000_000_000,
        123456,
        "event",
        "rx.event",
        event_record_fields(event, frame),
        Tier.EVENTS,
        0,
        None,
    )
    return len(probe._format(entry)) + 1  # type: ignore[arg-type]


def test_event_record_sizes_match_the_figures_the_allowance_was_sized_with() -> None:
    """The plan sizes the event allowance at 947 B per full-text event (856 B for a short text,
    about 1.67 KB when every text byte is 0xFF). If the record changes, these numbers - and the
    tables derived from them - must be re-derived."""
    short = _long_event_record_size(message="NOOP")
    full = _long_event_record_size(message="x" * 121)
    worst = _long_event_record_size(message="x" * 121, fill=0xFF)
    assert short == pytest.approx(856, rel=0.05)
    assert full == pytest.approx(947, rel=0.05)
    assert worst == pytest.approx(1670, rel=0.05)
    # the scenarios of the sizing table stay under the default allowance at the measured size
    per_hour = lambda rate: rate * 3600 * full  # noqa: E731
    assert per_hour(30) < EVENT_BYTES_PER_HOUR
    # 30/s of all-0xFF events reaches the allowance later than 50 min (the worst-case note)
    assert EVENT_BYTES_PER_HOUR / (30 * worst) > 50 * 60
    assert 12 * EVENT_BYTES_PER_HOUR < 2 * GIB  # 12 h of events at the allowance: 1.875 GiB
