"""Stage 2k: the stream framer.

The framer cuts a serial byte stream into packets without trusting any of it. These tests pin
its rules on hand-made streams, check its invariants on arbitrary bytes with Hypothesis (every
byte is accounted for, in order; the same stream gives the same frames however it is chunked;
packets separated by non-header garbage are all recovered; a lying length never costs a later
packet; hostile input stays within the work budget), and replay the checked-in corpus
``tests/corpus/malformed/ser-*.bin``.
"""

from __future__ import annotations

import math
import random
import tomllib
from collections.abc import Iterable, Sequence
from pathlib import Path

import pytest
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

import ser_corpus
import serialutil
from conftest import CORPUS, EXAMPLES
from ironbird import header as hdr
from ironbird.ccdd.model import MessageKind
from ironbird.errors import Code, DecodeError, IronbirdError
from ironbird.frame import FLAG_GARBAGE, FLAG_STALE
from ironbird.transport.framing import (
    CLAIM_MARGIN,
    EVAL_COST,
    NS_PER_MS,
    Emit,
    HeaderLengthFramer,
    WorkBudget,
    validate_stream_profile,
)

TLM = MessageKind.TELEMETRY
CAP = ser_corpus.CAP
CODEC = ser_corpus.codec()
TLM_PROFILE = CODEC.profile(TLM)
PEC_PROFILE = hdr.load_profile(EXAMPLES / "headers" / "ccsds_pec_tlm.toml")
PEC_BOUNDS = {0x100: (0, 40), 0x200: (4, 4), 0x300: (1, 200)}
IDLE_NS = 50 * NS_PER_MS
LONG_AFTER = 10**12  # a monotonic time far past any idle deadline
# Enough credit that a test never meets the budget unless it is testing it.
UNLIMITED = WorkBudget(per_byte=1 << 30, base=1 << 40, cap=1 << 41)

FUZZ = settings(
    max_examples=200,
    deadline=2000,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)


def tlm_framer(**kw: object) -> HeaderLengthFramer:
    options: dict[str, object] = {
        "max_packet_bytes": CAP,
        "payload_bounds": lambda mid: CODEC.payload_bounds(TLM, mid),
        "largest_payload": CODEC.largest_payload(TLM),
        "budget": UNLIMITED,
    }
    return HeaderLengthFramer(TLM_PROFILE, **(options | kw))  # type: ignore[arg-type]


def pec_framer(**kw: object) -> HeaderLengthFramer:
    options: dict[str, object] = {
        "max_packet_bytes": CAP,
        "payload_bounds": PEC_BOUNDS.get,
        "budget": UNLIMITED,
    }
    return HeaderLengthFramer(PEC_PROFILE, **(options | kw))  # type: ignore[arg-type]


def hk(counter: int = 1) -> bytes:
    return serialutil.hk(counter)


def pec(mid: int, payload: bytes) -> bytes:
    return hdr.pack(PEC_PROFILE, payload, mid=mid)


def feed(framer: HeaderLengthFramer, chunks: Iterable[bytes]) -> list[Emit]:
    out: list[Emit] = []
    for n, chunk in enumerate(chunks):
        out += framer.feed(chunk, 1_000 + n, n)
    return out


def finish(framer: HeaderLengthFramer, out: list[Emit]) -> list[Emit]:
    """The line goes quiet and stays quiet: everything pending is emitted."""
    return out + framer.flush(9_999)


def wire(emits: Iterable[Emit], sync: bytes = b"") -> bytes:
    """The bytes the emitted frames stand for: packets are preceded by their consumed sync."""
    return b"".join((sync + e.data) if e.flags == 0 else e.data for e in emits)


def packets(emits: Iterable[Emit]) -> list[bytes]:
    return [e.data for e in emits if e.flags == 0]


def split(data: bytes, sizes: Sequence[int]) -> list[bytes]:
    chunks, at = [], 0
    for size in sizes:
        if at >= len(data):
            break
        chunks.append(data[at : at + max(1, size)])
        at += max(1, size)
    if at < len(data):
        chunks.append(data[at:])
    return chunks


# -- the rules, on hand-made streams ---------------------------------------------------------


def test_a_profile_with_a_checksum_emits_each_packet_as_it_completes() -> None:
    framer = pec_framer()
    stream = pec(0x100, b"abc") + pec(0x200, b"wxyz")
    got = framer.feed(stream, 1, 0)
    assert packets(got) == [pec(0x100, b"abc"), pec(0x200, b"wxyz")]
    assert framer.deadline_ns() is None  # nothing is pending


def test_a_profile_without_a_checksum_waits_for_the_next_header_or_for_silence() -> None:
    framer = tlm_framer()
    first, second = hk(1), hk(2)
    assert framer.feed(first, 10, 0) == []  # sane, but nothing proves it yet
    assert framer.deadline_ns() == IDLE_NS
    assert packets(framer.feed(second[:16], 11, 1)) == [first]  # a sane header follows it
    assert framer.feed(second[16:], 12, 2) == []  # the second is whole, and unconfirmed
    assert framer.expire(2 + IDLE_NS - 1, 13) == []  # not yet idle long enough
    assert packets(framer.expire(2 + IDLE_NS, 14)) == [second]  # silence confirms it


def test_silence_confirms_the_last_packet_and_stamps_it_with_the_expiry_time() -> None:
    framer = tlm_framer()
    packet = hk(7)
    assert framer.feed(packet, 100, 5) == []
    assert framer.expire(5 + IDLE_NS - 1, 200) == []
    got = framer.expire(5 + IDLE_NS, 300)
    assert [(e.data, e.flags, e.time_ns) for e in got] == [(packet, 0, 300)]


def test_frames_are_stamped_with_the_time_of_the_read_that_completed_them() -> None:
    framer = pec_framer()
    packet = pec(0x100, b"abc")
    assert framer.feed(packet[:5], 111, 0) == []
    [done] = framer.feed(packet[5:], 222, 1)
    assert done.time_ns == 222


def test_garbage_is_emitted_before_the_packet_that_follows_it() -> None:
    framer = pec_framer()
    noise = b"\xff\xfe\xfd"
    got = framer.feed(noise + pec(0x100, b"abc"), 1, 0)
    assert [(e.flags, e.data) for e in got] == [(FLAG_GARBAGE, noise), (0, pec(0x100, b"abc"))]
    [episode] = framer.take_episodes()
    assert episode.skipped == 3 and episode.sample == noise
    assert framer.take_episodes() == []


def test_a_lying_length_above_the_cap_is_refused_before_it_can_swallow_anything() -> None:
    framer = pec_framer()
    lie = bytearray(pec(0x100, b"abc"))
    lie[4:6] = (CAP + 100).to_bytes(2, "big")  # claims more than the cap
    stream = bytes(lie) + pec(0x100, b"one") + pec(0x200, b"two!")
    got = finish(framer, framer.feed(stream, 1, 0))
    assert packets(got) == [pec(0x100, b"one"), pec(0x200, b"two!")]
    assert wire(got) == stream


def test_a_length_above_the_cap_does_not_hold_back_the_packets_behind_it() -> None:
    framer = pec_framer(payload_bounds=None)  # nothing but the cap can refuse the lie
    lie = bytearray(pec(0x100, b"abc"))
    lie[4:6] = (CAP + 100).to_bytes(2, "big")
    real = [pec(0x100, b"one"), pec(0x200, b"two!")]
    got = framer.feed(bytes(lie) + b"".join(real), 1, 0)  # no flush: the reads are the only input
    assert packets(got) == real


def test_a_false_header_whose_length_covers_real_packets_costs_none_of_them() -> None:
    framer = pec_framer()
    real = [pec(0x100, bytes([i]) * 9) for i in range(5)]
    liar = pec(0x300, b"x" * 60)[:8]  # a sane header claiming 6 + 60 + 2 bytes
    stream = liar + b"".join(real)
    assert hdr.check_word(PEC_PROFILE, stream[:68], max_size=CAP).__class__ is DecodeError
    got = finish(framer, framer.feed(stream, 1, 0))
    assert packets(got) == real
    assert wire(got) == stream


def test_a_partial_candidate_the_line_leaves_incomplete_is_a_stale_frame() -> None:
    framer = pec_framer()
    partial = pec(0x300, b"y" * 60)[:30]
    assert framer.feed(partial, 1, 0) == []
    got = framer.expire(IDLE_NS, 2)
    assert [(e.flags, e.data) for e in got] == [(FLAG_STALE, partial)]


def test_a_stale_candidate_is_searched_for_packets_it_would_have_covered() -> None:
    framer = pec_framer()
    liar = pec(0x300, b"x" * 100)[:6]
    real = pec(0x100, b"abcd")
    got = finish(framer, framer.feed(liar + real, 1, 0))
    assert [(e.flags, e.data) for e in got] == [(FLAG_STALE, liar), (0, real)]


def test_a_header_that_cannot_be_judged_before_the_link_ends_is_garbage_not_stale() -> None:
    framer = pec_framer()
    cut = pec(0x100, b"abc")[:3]
    assert [(e.flags, e.data) for e in finish(framer, framer.feed(cut, 1, 0))] == [
        (FLAG_GARBAGE, cut)
    ]


def test_the_sync_marker_is_consumed_and_what_precedes_it_is_garbage() -> None:
    sync = b"\x1a\xcf"
    framer = pec_framer(sync=sync)
    stream = b"\x00\x01" + sync + pec(0x100, b"abc") + sync + pec(0x200, b"wxyz")
    got = finish(framer, framer.feed(stream, 1, 0))
    assert [(e.flags, e.data) for e in got] == [
        (FLAG_GARBAGE, b"\x00\x01"),
        (0, pec(0x100, b"abc")),
        (0, pec(0x200, b"wxyz")),
    ]
    assert wire(got, sync) == stream


def test_a_sync_marker_split_across_reads_is_found() -> None:
    sync = b"\xde\xad\xbe\xef"
    stream = sync + pec(0x100, b"abc")
    for cut in range(1, len(sync) + 1):
        framer = pec_framer(sync=sync)
        got = finish(framer, feed(framer, [stream[:cut], stream[cut:]]))
        assert packets(got) == [pec(0x100, b"abc")], cut


def test_a_sync_marker_inside_garbage_that_is_not_followed_by_a_header_is_skipped() -> None:
    sync = b"\xaa"
    framer = pec_framer(sync=sync)
    stream = sync + b"\xff\xff\xff\xff\xff\xff\xff\xff" + sync + pec(0x100, b"abc")
    got = finish(framer, framer.feed(stream, 1, 0))
    assert packets(got) == [pec(0x100, b"abc")]
    assert wire(got, sync) == stream


def test_a_run_of_garbage_is_split_at_the_packet_cap() -> None:
    framer = pec_framer(max_packet_bytes=64)
    noise = b"\xfe" * 200
    got = finish(framer, framer.feed(noise, 1, 0))
    assert [len(e.data) for e in got] == [64, 64, 64, 8]
    assert all(e.flags == FLAG_GARBAGE for e in got)


def test_every_resync_run_is_one_episode_with_a_bounded_sample() -> None:
    framer = pec_framer()
    noise = b"\xfe" * 500
    finish(framer, framer.feed(noise + pec(0x100, b"abc") + b"\xfd" * 3, 1, 0))
    first, second = framer.take_episodes()
    assert (first.skipped, len(first.sample)) == (500, 32)
    assert (second.skipped, second.sample) == (3, b"\xfd" * 3)


def test_the_idle_deadline_follows_the_last_read_and_clears_when_nothing_is_pending() -> None:
    framer = pec_framer()
    assert framer.deadline_ns() is None
    framer.feed(b"\xfe", 1, 1000)
    assert framer.deadline_ns() == 1000 + IDLE_NS
    framer.feed(b"\xfe", 2, 5000)
    assert framer.deadline_ns() == 5000 + IDLE_NS
    framer.flush(3)
    assert framer.deadline_ns() is None


def test_an_empty_read_changes_nothing() -> None:
    framer = pec_framer()
    assert framer.feed(b"", 1, 0) == [] and framer.deadline_ns() is None


def test_a_header_the_ccdd_does_not_define_is_still_a_packet() -> None:
    packet = pec(0x7FF, b"zz")
    framer = pec_framer()
    assert packets(framer.feed(packet, 1, 0)) == [packet]  # 0x7FF is not in the bounds table
    assert framer.stats().mismatched == 1
    open_framer = pec_framer(payload_bounds=None)
    assert packets(open_framer.feed(packet, 1, 0)) == [packet]
    assert open_framer.stats().mismatched == 0


def test_a_payload_outside_the_range_of_its_mid_is_still_a_packet() -> None:
    bad = pec(0x200, b"toolong")  # MID 0x200 carries exactly 4 bytes
    framer = pec_framer()
    assert packets(finish(framer, framer.feed(bad, 1, 0))) == [bad]
    assert framer.stats().mismatched == 1 and framer.stats().garbage_bytes == 0


def grown() -> bytes:
    return ser_corpus.grown_hk(CODEC)


def unknown() -> bytes:
    return ser_corpus.unknown_mid(CODEC)


@pytest.mark.parametrize("sizes", [[10_000], [1], [7], [3, 11, 1]], ids=str)
def test_a_packet_the_ccdd_disagrees_with_costs_neither_it_nor_the_packets_around_it(
    sizes: list[int],
) -> None:
    """A struct that grew without the CCDD: the decode boundary reports it (IB-DEC-007), the
    framer must not turn it, and the packet before it, into garbage."""
    stream = hk(1) + grown() + hk(3) + hk(4)
    assert isinstance(hdr.check(TLM_PROFILE, grown()), type(None))  # the header and length are true
    framer = tlm_framer()
    got = finish(framer, feed(framer, split(stream, sizes)))
    assert [(e.flags, e.data) for e in got] == [(0, p) for p in (hk(1), grown(), hk(3), hk(4))]
    assert framer.stats().mismatched == 1 and framer.stats().garbage_bytes == 0


@pytest.mark.parametrize("sizes", [[10_000], [1]], ids=str)
def test_packets_the_ccdd_disagrees_with_may_follow_each_other(sizes: list[int]) -> None:
    """The header that confirms a packet need not be one the CCDD knows."""
    stream = grown() * 4
    framer = tlm_framer()
    got = finish(framer, feed(framer, split(stream, sizes)))
    assert packets(got) == [grown()] * 4 and framer.stats().mismatched == 4
    assert [e.flags for e in got] == [0] * 4
    stream = unknown() + grown() + unknown() + hk(2)
    framer = tlm_framer()
    got = finish(framer, feed(framer, split(stream, sizes)))
    assert packets(got) == [unknown(), grown(), unknown(), hk(2)]


def test_the_claim_limit_is_the_largest_ccdd_packet_with_its_margin() -> None:
    profile, largest = TLM_PROFILE, CODEC.largest_payload(TLM)
    limit = math.ceil((profile.min_bytes + largest) * CLAIM_MARGIN)
    assert limit < CAP, "the test needs a cap above the limit"

    def lie(total: int) -> bytes:  # an unknown MID claiming ``total`` bytes, then real packets
        return ser_corpus._data_header(CODEC, total, mid=0x8FE)

    framer = tlm_framer()
    got = finish(framer, framer.feed(lie(limit + 1) + hk(1) + hk(2), 1, 0))
    assert [(e.flags, e.data) for e in got] == [
        (FLAG_GARBAGE, lie(limit + 1)),
        (0, hk(1)),
        (0, hk(2)),
    ]
    # at the limit the claim is believed, and the packet is held until its bytes arrive
    framer = tlm_framer()
    assert (
        framer.feed(lie(limit), 1, 0) == [] and framer.stats().pending_bytes == profile.size_bytes
    )
    # a payload that grew by less than the margin is a packet the decode boundary reports
    framer = tlm_framer()
    assert packets(finish(framer, framer.feed(grown() + hk(2), 1, 0))) == [grown(), hk(2)]
    assert framer.stats().mismatched == 1
    # without the CCDD only the cap refuses a claim
    framer = tlm_framer(payload_bounds=None, largest_payload=None)
    assert framer.feed(lie(limit + 1), 1, 0) == []


@pytest.mark.parametrize("chunk", [1, 5, 4096], ids=lambda c: f"chunk{c}")
def test_a_forged_header_cannot_swallow_the_real_packets_its_claim_ends_on(chunk: int) -> None:
    """Review 2k (M1): an unknown MID claiming a length that ends on a sane header, with no
    checksum to refute it, used to take the 100 real packets in between."""
    real = [hk(i) for i in range(101)]
    body = b"".join(real[:100])
    cap = 4096
    forged = ser_corpus._data_header(CODEC, TLM_PROFILE.size_bytes + len(body), mid=0x8FE)
    stream = forged + body + real[100]
    framer = tlm_framer(max_packet_bytes=cap)
    got = finish(framer, feed(framer, split(stream, [chunk] * (len(stream) // chunk + 1))))
    assert packets(got) == real and wire(got) == stream


def test_swallow_loss_at_a_large_cap_stays_near_the_noise_baseline() -> None:
    """Checksum-less profile, 5% of packets followed by a noise burst, cap 32768: the loss is the
    damaged packets (about 5%), not the hundreds a false header could swallow (12% without the
    claim limit, over the whole run)."""
    lost = total = 0
    for seed in range(12):
        rng = random.Random(seed)
        real = [
            hk(rng.randrange(256))
            if rng.random() < 0.5
            else CODEC.encode_telemetry(
                "MOCK_DATA",
                {"Entries": [rng.randrange(100) for _ in range(rng.randint(0, 46))], "Text": "x"},
            ).data
            for _ in range(300)
        ]
        parts: list[bytes] = []
        for packet in real:
            parts.append(packet)
            if rng.random() < 0.05:
                parts.append(bytes(rng.randrange(256) for _ in range(rng.randint(1, 64))))
        stream = b"".join(parts)
        framer = tlm_framer(max_packet_bytes=32768)
        got = packets(finish(framer, feed(framer, split(stream, [64] * (len(stream) // 64 + 1)))))
        kept = set(got)
        lost += sum(1 for p in real if p not in kept)
        total += len(real)
    assert lost / total < 0.065, f"{lost} of {total} real packets lost"


def test_a_checksummed_packet_with_a_mid_the_ccdd_lacks_does_not_cost_its_neighbours() -> None:
    good = [pec(0x100, b"a" * 10) for _ in range(3)]
    other = pec(0x101, b"a" * 10)  # a valid checksum, a MID the CCDD does not define yet
    framer = pec_framer()
    got = framer.feed(good[0] + other + good[1] + good[2], 1, 0)  # no flush: only the reads
    assert [(e.flags, e.data) for e in got] == [(0, p) for p in (good[0], other, good[1], good[2])]


def test_a_lying_length_on_a_known_mid_is_still_refused_by_what_is_not_the_ccdd() -> None:
    """With no veto from the CCDD, a false header is refused by the checksum, or without one by
    nothing following it that looks like a header."""
    framer = tlm_framer()
    lie = ser_corpus._data_header(CODEC, TLM_PROFILE.min_bytes + 50)  # claims 50 payload bytes
    stream = lie + hk(1) + hk(2)  # the claimed span ends inside hk(2): no header there
    got = finish(framer, feed(framer, split(stream, [5] * 40)))
    assert packets(got) == [hk(1), hk(2)] and wire(got) == stream


def test_the_work_budget_turns_unexamined_bytes_into_garbage_instead_of_running_long() -> None:
    budget = WorkBudget(per_byte=1, base=64, cap=256)
    framer = pec_framer(budget=budget, payload_bounds=None)
    # Every offset starts a plausible 6-byte header claiming a long packet: costly to refuse.
    hostile = bytes([0x00, 0x01, 0xC0, 0x00, 0x03, 0xE8]) * 2000
    got = finish(framer, framer.feed(hostile, 1, 0))
    stats = framer.stats()
    assert wire(got) == hostile
    assert stats.work_exhausted > 0
    assert stats.work_spent <= stats.work_earned + CAP // 2 + 4 * EVAL_COST
    assert all(e.flags in (0, FLAG_GARBAGE, FLAG_STALE) for e in got)


def test_a_normal_stream_never_meets_the_default_budget() -> None:
    framer = HeaderLengthFramer(PEC_PROFILE, max_packet_bytes=CAP, payload_bounds=PEC_BOUNDS.get)
    stream = b"".join(pec(0x100, bytes(range(40))) for _ in range(500))
    noisy = b"".join(chunk + b"\xfe" * 20 for chunk in split(stream, [300] * 100))
    finish(framer, feed(framer, split(noisy, [1000] * 100)))
    assert framer.stats().work_exhausted == 0


def held_bound(profile: hdr.HeaderProfile, sync: bytes) -> int:
    """The most bytes held between two reads (the documented bound): a garbage run shorter than
    the cap, then a candidate of at most the cap that is still waiting for its bytes or for the
    header that confirms it, with the sync marker of each."""
    return 2 * CAP + 2 * len(sync) + profile.size_bytes


def _late_header_stream(profile: hdr.HeaderProfile, sync: bytes, byte: int) -> bytes:
    """``CAP - 1 - len(sync)`` bytes no header can begin in, then a header claiming a full cap:
    the framer holds the garbage and waits for the candidate to complete."""
    length = (CAP - profile.min_bytes).to_bytes(2, "big")  # the profiles keep it at offset 4
    head = bytearray(profile.size_bytes)
    head[4:6] = length
    if profile is TLM_PROFILE:
        head[0] = 0x08
    noise = bytes([byte]) * (CAP - 1 - len(sync))
    return noise + sync + bytes(head) + bytes([byte]) * CAP


@pytest.mark.parametrize("sync", [b"", b"\x1a\xcf\xfc\x1d"], ids=["nosync", "sync"])
@pytest.mark.parametrize("kind", ["pec", "tlm"])
@pytest.mark.parametrize("step", [1, 7, 100])
def test_pending_bytes_stay_within_two_caps_plus_a_header(
    kind: str, sync: bytes, step: int
) -> None:
    """The text of docs/plan.md bounds the held bytes: the garbage run in front of a candidate
    (under one cap) and the candidate (up to one cap), not the cap alone."""
    profile = PEC_PROFILE if kind == "pec" else TLM_PROFILE
    stream = _late_header_stream(profile, sync, 0xFF)
    framer = make(kind, sync=sync, budget=WorkBudget(), largest_payload=None)  # the cap alone
    held = 0
    for n in range(0, len(stream), step):
        framer.feed(stream[n : n + step], n, n)
        held = max(held, framer.stats().pending_bytes)
    bound = held_bound(profile, sync)
    assert held <= bound
    assert held > CAP + profile.size_bytes  # the hostile stream does hold more than one cap


def test_noise_without_headers_holds_at_most_the_cap() -> None:
    framer = pec_framer()
    held = 0
    for n in range(300):
        framer.feed(b"\xfe" * 100, n, n)
        held = max(held, framer.stats().pending_bytes)
    assert held <= CAP + 100


# -- a clean stream, read in any way, never exhausts the work budget ------------------------------

CLEAN_PROFILES = {
    "tlm": (TLM_PROFILE, 0x893),
    "tlm-time": (
        hdr.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm_cfe_time.toml"),
        0x893,
    ),
    "pec": (PEC_PROFILE, 0x100),
}
# Small enough that a stream whose scanning costs more than the bytes pay for runs it out within
# a few dozen packets; the default budget would take thousands.
STINGY = WorkBudget(base=2048, cap=4096)


def clean_stream(name: str, payload: int, count: int, sync: bytes) -> tuple[bytes, list[bytes]]:
    profile, mid = CLEAN_PROFILES[name]
    packet = hdr.pack(profile, bytes(payload), mid=mid, now=1.8e9)
    return (sync + packet) * count, [packet] * count


@pytest.mark.parametrize(
    "sync", [b"", b"\x1a\xcf\xfc\x1d", bytes(8)], ids=["nosync", "sync4", "sync8"]
)
@pytest.mark.parametrize("payload", [0, 1, 2, 3, 4, 8])
@pytest.mark.parametrize("name", sorted(CLEAN_PROFILES))
@pytest.mark.parametrize("step", [1, 2, 3])
def test_a_clean_stream_read_a_few_bytes_at_a_time_never_exhausts_the_work_budget(
    name: str, payload: int, sync: bytes, step: int
) -> None:
    profile = CLEAN_PROFILES[name][0]
    stream, expected = clean_stream(name, payload, 200, sync)
    framer = HeaderLengthFramer(profile, max_packet_bytes=CAP, sync=sync, budget=STINGY)
    out = finish(framer, feed(framer, split(stream, [step] * (len(stream) // step + 1))))
    stats = framer.stats()
    assert packets(out) == expected and stats.garbage_bytes == 0
    assert stats.work_exhausted == 0, f"{stats.work_spent} spent, {stats.work_earned} earned"


def test_a_complete_candidate_is_checked_once_however_many_reads_confirm_it() -> None:
    """A packet with no checksum waits for the header behind it: each byte of that header costs
    the confirmation, not another look at the packet."""
    stream, _ = clean_stream("tlm", 4, 2, b"")
    first = len(stream) // 2
    framer = tlm_framer(payload_bounds=None, budget=WorkBudget())
    framer.feed(stream[:first], 1, 0)
    assert framer.stats().packets == 0
    for n in range(first, len(stream) - 1):  # the second packet, but for its last byte
        before = framer.stats()
        framer.feed(stream[n : n + 1], 2 + n, n)
        if framer.stats().packets == before.packets:  # the byte that confirms it starts the next
            assert framer.stats().work_spent - before.work_spent <= EVAL_COST
    assert framer.stats().packets == 1


def test_only_a_profile_with_a_checksum_pays_per_byte_for_checking_a_candidate() -> None:
    plain = HeaderLengthFramer(TLM_PROFILE, max_packet_bytes=CAP, budget=WorkBudget())
    summed = HeaderLengthFramer(PEC_PROFILE, max_packet_bytes=CAP, budget=WorkBudget())
    big = CAP - 100
    plain.feed(hdr.pack(TLM_PROFILE, bytes(big - TLM_PROFILE.min_bytes), mid=0x893) * 2, 1, 0)
    summed.feed(hdr.pack(PEC_PROFILE, bytes(big - PEC_PROFILE.min_bytes), mid=0x100) * 2, 1, 0)
    assert summed.stats().work_spent >= big // 2
    assert plain.stats().work_spent < big // 8  # a header judgement or two


# -- profiles a stream cannot be framed with ------------------------------------------------


def _profile_without_a_length_field() -> hdr.HeaderProfile:
    text = (EXAMPLES / "headers" / "ccsds_pec_tlm.toml").read_text()
    data = tomllib.loads(text.replace('computed = "length"\nbias = -7', "value = 0"))
    return hdr.parse_profile(data, "no-length.toml")


def test_a_profile_without_a_length_field_cannot_frame_a_stream() -> None:
    profile = _profile_without_a_length_field()
    assert profile.length_window is None
    with pytest.raises(IronbirdError) as info:
        validate_stream_profile(profile, CAP, "link 'x'")
    assert info.value.code is Code.CFG_LINK and "length field" in info.value.summary
    with pytest.raises(IronbirdError):
        HeaderLengthFramer(profile, max_packet_bytes=CAP)


def test_a_cap_below_the_empty_packet_is_refused() -> None:
    with pytest.raises(IronbirdError) as info:
        validate_stream_profile(TLM_PROFILE, TLM_PROFILE.min_bytes - 1, "link 'x'")
    assert info.value.code is Code.CFG_LINK and "max_packet_bytes" in info.value.summary


# -- properties ------------------------------------------------------------------------------

_alphabet = st.sampled_from([0x00, 0x01, 0x08, 0x09, 0x18, 0x90, 0xC0, 0xFF, 0x1A, 0xCF])


@st.composite
def pec_packet(draw: st.DrawFn) -> bytes:
    mid = draw(st.sampled_from(sorted(PEC_BOUNDS)))
    low, high = PEC_BOUNDS[mid]
    size = draw(st.integers(low, min(high, 60)))
    return pec(mid, draw(st.binary(min_size=size, max_size=size)))


@st.composite
def tlm_packet(draw: st.DrawFn) -> bytes:
    kind = draw(st.sampled_from(["hk", "status", "data"]))
    if kind == "hk":
        return CODEC.encode_telemetry("MOCK_HK", {"cmd_counter": draw(st.integers(0, 255))}).data
    if kind == "status":
        return CODEC.encode_telemetry("MOCK_STATUS", {"temperature_c": 1.5}).data
    count = draw(st.integers(0, 6))
    return CODEC.encode_telemetry("MOCK_DATA", {"Entries": list(range(count)), "Text": "t"}).data


@st.composite
def messy_stream(draw: st.DrawFn) -> bytes:
    """Arbitrary bytes, header-like fragments and whole packets, mixed."""
    parts = draw(
        st.lists(
            st.one_of(
                st.binary(max_size=40),
                st.lists(_alphabet, max_size=24).map(bytes),
                pec_packet(),
                tlm_packet(),
                pec_packet().map(lambda p: p[: len(p) // 2]),
            ),
            max_size=14,
        )
    )
    return b"".join(parts)


SYNCS = st.sampled_from([b"", b"\x1a", b"\x1a\xcf\xfc\x1d"])
PROFILES = st.sampled_from(["pec", "tlm"])


def make(kind: str, **kw: object) -> HeaderLengthFramer:
    return pec_framer(**kw) if kind == "pec" else tlm_framer(**kw)


@FUZZ
@given(
    stream=messy_stream(),
    sizes=st.lists(st.integers(1, 40), max_size=30),
    idle_at=st.sets(st.integers(0, 30), max_size=4),
    kind=PROFILES,
    sync=SYNCS,
)
def test_every_byte_is_accounted_for_in_order_whatever_the_chunking_and_idle_gaps(
    stream: bytes, sizes: list[int], idle_at: set[int], kind: str, sync: bytes
) -> None:
    framer = make(kind, sync=sync)
    out: list[Emit] = []
    mono = 0
    for n, chunk in enumerate(split(stream, sizes)):
        mono += 1
        out += framer.feed(chunk, n, mono)
        if n in idle_at:
            mono += LONG_AFTER
            out += framer.expire(mono, n)
    out = finish(framer, out)
    assert wire(out, sync) == stream
    assert all(0 < len(e.data) <= CAP for e in out)
    assert all(e.flags in (0, FLAG_GARBAGE, FLAG_STALE) for e in out)
    assert framer.stats().pending_bytes == 0 and framer.deadline_ns() is None


@FUZZ
@given(
    stream=messy_stream(),
    first=st.lists(st.integers(1, 40), max_size=30),
    second=st.lists(st.integers(1, 7), max_size=30),
    kind=PROFILES,
    sync=SYNCS,
)
def test_the_same_stream_gives_the_same_frames_however_it_is_chunked(
    stream: bytes, first: list[int], second: list[int], kind: str, sync: bytes
) -> None:
    def frames(sizes: list[int]) -> list[tuple[int, bytes]]:
        framer = make(kind, sync=sync)
        out = finish(framer, feed(framer, split(stream, sizes)))
        return [(e.flags, e.data) for e in out]

    whole = frames([len(stream) + 1])
    assert frames(first) == whole
    assert frames(second) == whole


# A byte whose top five bits are not zero cannot begin a PEC header (the version, packet type
# and secondary-header flag are fixed at zero), so such garbage holds no header at any offset.
_no_pec_header = st.binary(min_size=1, max_size=60).map(
    lambda raw: bytes(b | 0x08 if b < 0x08 else b for b in raw)
)


@FUZZ
@given(
    items=st.lists(st.tuples(_no_pec_header, pec_packet()), min_size=1, max_size=8),
    tail=_no_pec_header,
    sizes=st.lists(st.integers(1, 50), max_size=20),
    sync=SYNCS,
)
def test_checksummed_packets_separated_by_garbage_are_all_recovered(
    items: list[tuple[bytes, bytes]], tail: bytes, sizes: list[int], sync: bytes
) -> None:
    assume(all(sync not in noise for noise, _ in items) and sync not in tail)
    stream = b"".join(noise + sync + packet for noise, packet in items) + tail
    framer = pec_framer(sync=sync)
    got = finish(framer, feed(framer, split(stream, sizes)))
    assert packets(got) == [packet for _, packet in items]
    assert wire(got, sync) == stream


@FUZZ
@given(
    items=st.lists(
        st.tuples(
            st.binary(max_size=30).map(lambda raw: bytes(b & 0xF7 | 0x80 for b in raw)),
            tlm_packet(),
        ),
        min_size=1,
        max_size=8,
    ),
    sizes=st.lists(st.integers(1, 50), max_size=20),
)
def test_packets_without_a_checksum_are_all_recovered_when_the_line_pauses_after_each(
    items: list[tuple[bytes, bytes]], sizes: list[int]
) -> None:
    framer = tlm_framer()
    out: list[Emit] = []
    mono = 0
    for noise, packet in items:
        for chunk in split(noise + packet, sizes):
            mono += 1
            out += framer.feed(chunk, mono, mono)
        mono += IDLE_NS
        out += framer.expire(mono, mono)  # the line is silent: the packet is confirmed
    assert packets(out) == [packet for _, packet in items]


@FUZZ
@given(
    real=st.lists(pec_packet(), min_size=1, max_size=6),
    kept=st.integers(6, 30),
    claimed=st.integers(0, 150),
    lead=_no_pec_header,
    sizes=st.lists(st.integers(1, 50), max_size=20),
)
def test_a_false_header_embedded_in_garbage_does_not_cost_a_later_packet(
    real: list[bytes], kept: int, claimed: int, lead: bytes, sizes: list[int]
) -> None:
    liar = pec(0x300, bytes(claimed + 1))  # sane header, claims 6 + claimed + 1 + 2 bytes
    false_head = liar[:kept]
    stream = lead + false_head + b"".join(real)
    # the lie must be a lie: the bytes it claims must not happen to be a valid packet
    span = stream[len(lead) : len(lead) + len(liar)]
    assume(len(span) < len(liar) or isinstance(hdr.check_word(PEC_PROFILE, span), DecodeError))
    framer = pec_framer()
    got = finish(framer, feed(framer, split(stream, sizes)))
    assert packets(got)[-len(real) :] == real
    assert wire(got) == stream


@FUZZ
@given(
    length=st.integers(0, 0xFFFF),
    real=st.lists(pec_packet(), min_size=1, max_size=5),
    mid=st.sampled_from(sorted(PEC_BOUNDS)),
)
def test_a_lying_length_never_swallows_more_than_the_cap_or_a_packet_behind_it(
    length: int, real: list[bytes], mid: int
) -> None:
    liar = bytearray(pec(mid, bytes(PEC_BOUNDS[mid][0])))
    liar[4:6] = length.to_bytes(2, "big")
    stream = bytes(liar) + b"".join(real)
    span = stream[: length + 7]
    assume(len(span) < length + 7 or isinstance(hdr.check_word(PEC_PROFILE, span), DecodeError))
    framer = pec_framer()
    got = finish(framer, framer.feed(stream, 1, 0))
    assert all(len(e.data) <= CAP for e in got)
    assert packets(got)[-len(real) :] == real


@FUZZ
@given(
    lengths=st.lists(st.integers(0, 0xFFFF), min_size=1, max_size=8),
    repeat=st.integers(1, 400),
    kind=PROFILES,
)
def test_hostile_input_is_processed_within_the_work_budget(
    lengths: list[int], repeat: int, kind: str
) -> None:
    profile = PEC_PROFILE if kind == "pec" else TLM_PROFILE
    heads = []
    for length in lengths:
        raw = bytearray(profile.size_bytes)
        raw[: len(raw)] = (0x0 if kind == "pec" else 0x08).to_bytes(1, "big") + bytes(len(raw) - 1)
        raw[4:6] = length.to_bytes(2, "big")
        heads.append(bytes(raw))
    stream = b"".join(heads) * repeat
    framer = (
        HeaderLengthFramer(PEC_PROFILE, max_packet_bytes=CAP)
        if kind == "pec"
        else HeaderLengthFramer(TLM_PROFILE, max_packet_bytes=CAP)
    )
    got: list[Emit] = []
    for n, chunk in enumerate(split(stream, [4096] * (len(stream) // 4096 + 1))):
        got += framer.feed(chunk, n, n)
    got = finish(framer, got)
    stats = framer.stats()
    assert wire(got) == stream
    assert stats.work_spent <= stats.work_earned + CAP // 2 + 4 * EVAL_COST


@st.composite
def any_pec_packet(draw: st.DrawFn) -> bytes:
    """A valid PEC packet with any MID and payload size, the CCDD's or not."""
    mid = draw(st.sampled_from([*sorted(PEC_BOUNDS), 0x101, 0x7FF]))
    return pec(mid, draw(st.binary(max_size=60)))


@st.composite
def any_tlm_packet(draw: st.DrawFn) -> bytes:
    """A well-formed telemetry packet that the CCDD may or may not agree with."""
    kind = draw(st.sampled_from(["known", "grown", "grown-more", "unknown"]))
    if kind == "known":
        return draw(tlm_packet())
    if kind == "unknown":
        mid = draw(st.sampled_from([0x8FF, 0xFFF, 0x801]))
        return ser_corpus.unknown_mid(CODEC, mid, draw(st.integers(0, 40)))
    return ser_corpus.grown_hk(CODEC, 2 if kind == "grown" else draw(st.integers(1, 200)))


def valid_packets(max_size: int) -> st.SearchStrategy[tuple[str, list[bytes]]]:
    """A profile name and valid packets of it (the CCDD's or not), as a stream carries them."""
    return st.sampled_from(["pec", "tlm"]).flatmap(
        lambda kind: st.tuples(
            st.just(kind),
            st.lists(
                any_pec_packet() if kind == "pec" else any_tlm_packet(),
                min_size=1,
                max_size=max_size,
            ),
        )
    )


def _valid(kind: str, items: list[bytes]) -> list[bytes]:
    profile = PEC_PROFILE if kind == "pec" else TLM_PROFILE
    assert all(hdr.check(profile, p) is None and len(p) <= CAP for p in items)
    return items


@FUZZ
@given(
    valid=valid_packets(12),
    sizes=st.lists(st.integers(1, 40), max_size=30),
    sync=SYNCS,
)
def test_well_formed_packets_are_all_framed_whether_or_not_the_ccdd_agrees_with_them(
    valid: tuple[str, list[bytes]], sizes: list[int], sync: bytes
) -> None:
    kind, items = valid
    stream = b"".join(sync + p for p in _valid(kind, items))
    framer = make(kind, sync=sync)
    got = finish(framer, feed(framer, split(stream, sizes)))
    assert [(e.flags, e.data) for e in got] == [(0, p) for p in items]


@FUZZ
@given(
    valid=valid_packets(40),
    sizes=st.lists(st.integers(1, 5), max_size=60),
    sync=SYNCS,
)
def test_a_stream_of_valid_packets_never_exhausts_the_work_budget_however_it_is_read(
    valid: tuple[str, list[bytes]], sizes: list[int], sync: bytes
) -> None:
    kind, items = valid
    stream = b"".join(sync + p for p in _valid(kind, items))
    framer = make(kind, sync=sync, budget=STINGY)
    chunks = split(stream, sizes) if sizes else split(stream, [1] * len(stream))
    got = finish(framer, feed(framer, chunks))
    assert packets(got) == items and framer.stats().garbage_bytes == 0
    assert framer.stats().work_exhausted == 0


# -- the checked-in corpus -------------------------------------------------------------------


def _corpus_files() -> list[Path]:
    files = sorted(CORPUS.glob("ser-*.bin"))
    assert files, "corpus has no ser-*.bin"
    return files


def test_the_corpus_files_are_what_the_builder_builds() -> None:
    built = {name: case.stream for name, case in ser_corpus.cases().items()}
    on_disk = {path.name: path.read_bytes() for path in _corpus_files()}
    assert on_disk == built, "run: python tests/ser_corpus.py"


@pytest.mark.parametrize("chunk", [0, 1, 7, 64], ids=lambda c: f"chunk{c}")
@pytest.mark.parametrize("path", _corpus_files(), ids=lambda p: p.stem)
def test_corpus_streams_are_accounted_for_and_framed_as_pinned(path: Path, chunk: int) -> None:
    case = ser_corpus.cases()[path.name]
    data = path.read_bytes()
    framer = tlm_framer(budget=WorkBudget())  # the default budget: hostile cases must fit it
    sizes = [chunk or len(data)] * (len(data) // (chunk or len(data)) + 1)
    got = finish(framer, feed(framer, split(data, sizes)))
    assert wire(got) == data
    assert all(0 < len(e.data) <= CAP for e in got)
    if case.expect is not None:
        assert tuple((e.flags, e.data) for e in got) == case.expect
    stats = framer.stats()
    assert stats.work_spent <= stats.work_earned + CAP // 2 + 4 * EVAL_COST
