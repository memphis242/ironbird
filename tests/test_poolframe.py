"""The wire format of the export pool (``ironbird.recording.poolframe``): what is written is read
back exactly, and nothing else is read at all.

A worker decodes untrusted recordings, so its replies are untrusted: every malformed or
unexpected message must be a ``FrameError`` (``IB-EXP-003``), never another exception, never an
allocation the message did not pay for, never a value the parent cannot apply.
"""

from __future__ import annotations

import array
import dataclasses
import os
import struct
from collections import Counter
from pathlib import Path
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from ironbird.ccdd.model import MessageKind
from ironbird.errors import Code, IronbirdError
from ironbird.frame import MAX_STREAM_ID, Direction, Stream
from ironbird.recording import pexport
from ironbird.recording import poolframe as pf
from ironbird.recording.export import ExportFormat, ExportResult
from ironbird.recording.player import Ending
from ironbird.recording.poolmsg import Block, EmbeddedDefinitions, Fault, Job, Outcome, Seed, Shape
from ironbird.recording.reader import Damage
from poolframe_corpus import CORPUS, JOB, PREFIX, SHAPE, cases, outcome, samples

SAMPLES = samples()


def refused(data: bytes) -> pf.FrameError:
    with pytest.raises(pf.FrameError) as caught:
        pf.decode(data)
    assert caught.value.code is Code.EXP_WORKER
    return caught.value


def accepted_or_refused(data: bytes) -> None:
    """What any bytes may do: decode to a message that is exactly what the encoder writes for it,
    or be refused with a ``FrameError``."""
    try:
        message = pf.decode(data)
    except pf.FrameError:
        return
    assert pf.encode(message) == data


# -- what is written is read back exactly -------------------------------------------------------


@pytest.mark.parametrize("name", sorted(SAMPLES))
def test_every_message_reads_back_as_written(name: str) -> None:
    message = SAMPLES[name]
    frame = pf.encode(message)
    assert pf.decode(frame) == message
    assert pf.encode(pf.decode(frame)) == frame  # canonical
    assert pf.decode(memoryview(frame)) == message
    assert frame[:4] == pf.MAGIC and frame[4] == pf.VERSION


def test_a_reply_that_tells_the_truth_is_read() -> None:
    reply = pf.read_reply(JOB, pf.encode(outcome()))
    assert reply == outcome() and reply.order is not None and list(reply.order) == [1, 0, 1]
    for name in ("outcome-crash", "outcome-moved", "outcome-failure"):
        message = SAMPLES[name]
        assert isinstance(message, Outcome)
        assert pf.read_reply(JOB, pf.encode(message)) == message


def test_the_operating_systems_bytes_travel_as_they_are() -> None:
    """A path and a filter that are not UTF-8 (a file named in another encoding, a ``--where``
    typed in one) reach the worker as the parent has them."""
    odd = os.fsdecode(b"/data/r\xffun.ibrec")
    shape = dataclasses.replace(SHAPE, where=os.fsdecode(b"name == '\xe9'"), messages=(odd,))
    job = dataclasses.replace(JOB, path=Path(odd), shape=shape)
    assert pf.decode(pf.encode(job)) == job


# -- nothing else is read --------------------------------------------------------------------


@pytest.mark.parametrize("name", sorted(SAMPLES))
def test_a_message_cut_anywhere_is_refused(name: str) -> None:
    frame = pf.encode(SAMPLES[name])
    body = frame[pf.HEADER.size :]
    for cut in range(len(frame)):
        refused(frame[:cut])
    for cut in range(len(body)):  # the header made to agree: the cut falls inside a field
        magic, version, kind, reserved, _ = pf.HEADER.unpack_from(frame)
        refused(pf.HEADER.pack(magic, version, kind, reserved, cut) + body[:cut])


@pytest.mark.parametrize("name", ["outcome", "job", "embedded", "config"])
def test_any_byte_changed_is_refused_or_read_exactly(name: str) -> None:
    frame = pf.encode(SAMPLES[name])
    for at in range(len(frame)):
        for value in {0x00, 0xFF, frame[at] ^ 0x01, frame[at] ^ 0x80}:
            accepted_or_refused(frame[:at] + bytes([value]) + frame[at + 1 :])


@pytest.mark.parametrize("name", ["outcome", "job"])
def test_a_length_that_lies_is_refused_before_it_is_used(name: str) -> None:
    """Every four bytes read as a length or a count, made huge or just past the end."""
    frame = pf.encode(SAMPLES[name])
    for at in range(pf.HEADER.size, len(frame) - 3):
        for value in (0xFFFFFFFF, len(frame) - at, 1 << 20):
            accepted_or_refused(frame[:at] + struct.pack("<I", value) + frame[at + 4 :])


def test_unknown_types_versions_and_magic_are_refused() -> None:
    frame = pf.encode(Fault("x"))
    known = {int(kind) for kind in pf.Kind}
    for kind in range(256):
        if kind not in known:
            assert "type" in refused(frame[:5] + bytes([kind]) + frame[6:]).reason
    for version in range(256):
        if version != pf.VERSION:
            assert "version" in refused(frame[:4] + bytes([version]) + frame[5:]).reason
    assert "magic" in refused(b"\0" + frame[1:]).reason


@pytest.mark.parametrize(
    "bad",
    [b"\xff", b"\xc0\xaf", b"\xed\xa0\x80", b"\xe2\x82", b"\xf4\x90\x80\x80", b"a\x80b"],
    ids=["ff", "overlong", "surrogate", "cut", "past-unicode", "continuation"],
)
def test_text_that_is_not_utf8_is_refused(bad: bytes) -> None:
    body = struct.pack("<I", len(bad)) + bad
    frame = pf.HEADER.pack(pf.MAGIC, pf.VERSION, pf.Kind.FAULT, 0, len(body)) + body
    assert "UTF-8" in refused(frame).reason


def test_a_message_longer_than_its_type_may_be_is_refused_unread() -> None:
    body = bytes(pf.MAX_JOB_BYTES - pf.HEADER.size + 1)
    frame = pf.HEADER.pack(pf.MAGIC, pf.VERSION, pf.Kind.JOB, 0, len(body)) + body
    assert "longer" in refused(frame).reason


def test_counts_over_their_caps_are_refused_before_anything_is_made() -> None:
    zeros = pf._OUTCOME.pack(7, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    for count in (pf.MAX_NOTES + 1, 0xFFFFFFFF):
        body = zeros + struct.pack("<I", count)
        frame = pf.HEADER.pack(pf.MAGIC, pf.VERSION, pf.Kind.OUTCOME, 0, len(body)) + body
        assert "more than" in refused(frame).reason


def test_a_message_of_many_small_items_is_refused_at_the_item_budget() -> None:
    """Small strings cost far more memory than their bytes: one message makes at most
    ``MAX_ITEMS`` of them, whatever each list's own cap allows."""
    columns = pf.MAX_ITEMS // pf.MAX_BLOCKS + 2
    block = Block(("m", ("",) * columns), False, 0, "", 0)
    many = Outcome(7, blocks=[dataclasses.replace(block, handle=h) for h in range(pf.MAX_BLOCKS)])
    assert "items" in refused(pf.encode(many)).reason


# -- what cannot be written is an error at the sender --------------------------------------------


@pytest.mark.parametrize(
    "message",
    [
        Fault("x" * (pf.MAX_DIAG_BYTES + 1)),
        Fault("\ud800"),  # text that is not Unicode
        Outcome(-1),
        Outcome(1 << 64),
        dataclasses.replace(JOB, event_cap=0),
        dataclasses.replace(JOB, text_cap=pf.MAX_TEXT_CAP + 1),
        dataclasses.replace(JOB, seed=Seed({3: JOB.seed.streams[1]}, {}, 0, 0)),
        dataclasses.replace(JOB, shape=dataclasses.replace(SHAPE, where="\ud800")),
        Outcome(7, definitions=[("stream", "not a stream")]),
        Outcome(7, damage=[Damage(0, 0, False, "x")]),  # no rows before it
        Outcome(7, failure=("IB-NOT-000", "x", "y")),
        Outcome(7, order=array.array("H", [1])),
        EmbeddedDefinitions(Path("x"), {}, "", (), {"x": object()}, 2048, ""),
    ],
    ids=lambda m: type(m).__name__,
)
def test_a_value_the_format_cannot_carry_is_refused_by_the_encoder(message: Any) -> None:
    with pytest.raises(pf.FrameError):
        pf.encode(message)


def test_error_text_is_made_fit_to_send() -> None:
    assert pf.diagnostic("plain") == "plain"
    assert pf.diagnostic("bad \udcff name") == "bad \\udcff name"
    long = "x" * pf.MAX_DIAG_BYTES + "\nValueError: the end"
    fitted = pf.diagnostic(long)
    assert len(fitted.encode()) <= pf.MAX_DIAG_BYTES and fitted.endswith("ValueError: the end")
    assert pf.decode(pf.encode(Fault(fitted))) == Fault(fitted)


# -- the corpus ------------------------------------------------------------------------------


def test_the_checked_in_corpus_is_what_is_built_here() -> None:
    built = cases()
    on_disk = {p.name: p.read_bytes() for p in CORPUS.glob(f"{PREFIX}*")}
    assert on_disk == built


@pytest.mark.parametrize("name", sorted(cases()))
def test_every_corpus_reply_is_refused(name: str) -> None:
    data = (CORPUS / name).read_bytes()
    with pytest.raises(pf.FrameError) as caught:
        pf.read_reply(JOB, data)
    assert caught.value.code is Code.EXP_WORKER


# -- properties ------------------------------------------------------------------------------

_U64 = st.integers(0, (1 << 64) - 1)
_I64 = st.integers(-(1 << 63), (1 << 63) - 1)
_U32 = st.integers(0, (1 << 32) - 1)
_U16 = st.integers(0, 0xFFFF)
_NAME = st.text(st.characters(codec="utf-8"), max_size=12)
_STREAM = st.builds(
    Stream,
    st.integers(0, MAX_STREAM_ID),
    _NAME,
    _NAME,
    st.sampled_from(MessageKind),
    st.sampled_from(Direction),
    _NAME,
    _NAME,
)
_DAMAGE = st.builds(
    Damage, _U64, _U64, st.booleans(), _NAME, st.none() | _U64, st.booleans(), st.booleans()
)
_COUNTS = st.builds(
    ExportResult,
    *([_U64] * 9),
    st.sampled_from(Ending),
    st.dictionaries(_NAME, _U64, max_size=3).map(Counter),
    st.dictionaries(_NAME, _U64, max_size=3),
)
_BLOCK = st.builds(
    Block,
    st.none() | st.tuples(_NAME, st.lists(_NAME, max_size=4).map(tuple)),
    st.booleans(),
    _U64,
    st.text(st.characters(codec="utf-8"), max_size=40),
    _U32,
)


@st.composite
def _outcomes(draw: st.DrawFn) -> Outcome:
    damage = draw(st.lists(st.tuples(_DAMAGE, _U64), max_size=3))
    definitions: list[tuple[Any, ...]] = draw(
        st.lists(
            st.tuples(st.just("stream"), _STREAM) | st.tuples(st.just("source"), _U16, _NAME),
            max_size=4,
        )
    )
    return Outcome(
        draw(_U64),
        start=draw(st.none() | _U64),
        end=draw(_U64),
        reached_stop=draw(st.booleans()),
        first_seq=draw(st.none() | _U64),
        last_seq=draw(_U64),
        definitions=definitions,
        used_streams=draw(st.lists(_U16, max_size=4)),
        used_sources=draw(st.lists(_U16, max_size=4)),
        work=draw(_I64),
        scanned=draw(_I64),
        trailer=draw(st.booleans()),
        partial=draw(st.booleans()),
        moved=draw(st.booleans()),
        damage=[d for d, _ in damage],
        damage_rows=[r for _, r in damage],
        failure=draw(st.none() | st.tuples(st.sampled_from([c.value for c in Code]), _NAME, _NAME)),
        crash=draw(st.none() | _NAME),
        frames_read=draw(_U64),
        frames_in_range=draw(_U64),
        gap_marks=draw(st.lists(st.tuples(_U64, _U64, _U64), max_size=3)),
        counts=draw(_COUNTS),
        blocks=draw(st.lists(_BLOCK, max_size=3)),
        categories=draw(st.none() | st.binary(max_size=16)),
        order=draw(st.none() | st.lists(_U32, max_size=6).map(lambda xs: array.array("I", xs))),
    )


_PATH = (
    st.binary(min_size=1, max_size=24)
    .filter(lambda b: b"\0" not in b)
    .map(lambda b: Path(os.fsdecode(b)))
)
_OS_TEXT = st.binary(max_size=16).map(os.fsdecode)


@st.composite
def _jobs(draw: st.DrawFn) -> Job:
    streams = draw(st.lists(_STREAM, max_size=3, unique_by=lambda s: s.id))
    shape = Shape(
        draw(st.sampled_from(ExportFormat)),
        draw(st.none() | _OS_TEXT),
        frozenset(draw(st.lists(_NAME, max_size=3))),
        draw(st.none() | st.lists(st.tuples(_OS_TEXT, _OS_TEXT), max_size=2).map(tuple)),
        tuple(draw(st.lists(_OS_TEXT, max_size=3))),
        *draw(st.tuples(*[st.booleans()] * 5)),
    )
    return Job(
        draw(_U64),
        draw(_PATH),
        draw(_U64),
        draw(st.none() | _U64),
        draw(st.booleans()),
        Seed(
            {s.id: s for s in streams},
            draw(st.dictionaries(_U16, _NAME, max_size=3)),
            draw(_U64),
            draw(_I64),
        ),
        draw(st.none() | _I64),
        draw(st.none() | _I64),
        shape,
        (draw(_U64), draw(_U64)),
        draw(st.integers(1, pf.MAX_TEXT_CAP)),
        draw(st.integers(1, pf.MAX_EVENT_CAP)),
    )


@settings(max_examples=300, deadline=None)
@given(st.one_of(_outcomes(), _jobs(), st.builds(Fault, _NAME)))
def test_any_value_reads_back_exactly_as_written(message: pf.Message) -> None:
    frame = pf.encode(message)
    assert pf.decode(frame) == message
    assert pf.encode(pf.decode(frame)) == frame


@settings(max_examples=300, deadline=None)
@given(_outcomes(), st.data())
def test_any_reply_is_applied_or_refused_never_crashes(reply: Outcome, data: st.DataObject) -> None:
    """``read_reply`` on any frame, a well-formed one with any values or a mutated one: an
    outcome it accepted, or a ``FrameError``; nothing else escapes."""
    frame = bytearray(pf.encode(reply))
    for _ in range(data.draw(st.integers(0, 3))):
        at = data.draw(st.integers(0, len(frame) - 1))
        frame[at] = data.draw(st.integers(0, 255))
    try:
        got = pf.read_reply(JOB, bytes(frame))
    except pf.FrameError:
        return
    pf.check_reply(JOB, got)


# -- the pool's two ends ---------------------------------------------------------------------


def _pool_with_a_fake_worker() -> tuple[pexport.WorkerPool, pexport.Ticket, Any]:
    """A pool whose one worker is the other end of a pipe this test holds, with ``JOB`` sent."""
    import multiprocessing

    definitions = SAMPLES["embedded"]
    assert isinstance(definitions, EmbeddedDefinitions)
    pool = pexport.WorkerPool(definitions, 1)
    ours, theirs = multiprocessing.Pipe()
    slot = pexport._Slot(None, ours)
    ticket = pexport.Ticket(JOB, b"")
    ticket.data = None
    slot.sent.append(ticket)
    pool._slots.append(slot)
    return pool, ticket, theirs


@pytest.mark.parametrize(
    "reply",
    [
        b"not a frame",
        pf.encode(Fault("the job could not be read")),
        pf.encode(JOB),
        pf.encode(dataclasses.replace(outcome(), index=8)),
    ],
    ids=["garbage", "fault", "a-job", "wrong-range"],
)
def test_a_reply_the_parent_cannot_read_ends_the_pool_like_a_worker_crash(reply: bytes) -> None:
    pool, ticket, theirs = _pool_with_a_fake_worker()
    try:
        theirs.send_bytes(reply)
        pool.poll(5.0)
        assert ticket.done() and pool.broken
        with pytest.raises(IronbirdError) as caught:
            ticket.result()
        assert caught.value.code is Code.EXP_WORKER
    finally:
        pool.close(abnormal=True)
        theirs.close()


def test_a_reply_longer_than_any_reply_is_refused_before_it_is_read() -> None:
    """Only the length is sent: the parent must not wait for (or make room for) the rest."""
    pool, ticket, theirs = _pool_with_a_fake_worker()
    try:
        os.write(theirs.fileno(), struct.pack("!i", pf.MAX_REPLY_BYTES + 1))
        pool.poll(5.0)
        assert ticket.done() and pool.broken
        with pytest.raises(IronbirdError) as caught:
            ticket.result()
        assert caught.value.code is Code.EXP_WORKER and "too long" in caught.value.summary
    finally:
        pool.close(abnormal=True)
        theirs.close()


def test_a_reply_that_tells_the_truth_reaches_its_ticket() -> None:
    pool, ticket, theirs = _pool_with_a_fake_worker()
    try:
        theirs.send_bytes(pf.encode(outcome()))
        pool.poll(5.0)
        assert ticket.result() == outcome() and not pool.broken
    finally:
        pool.close(abnormal=True)
        theirs.close()


@pytest.mark.parametrize(
    ("kind", "message"),
    [
        (pf.Kind.JOB, b"garbage"),
        (pf.Kind.JOB, pf.encode(SAMPLES["embedded"])),
        (pf.Kind.DEFINITIONS, pf.encode(JOB)),
    ],
    ids=["garbage", "definitions-for-a-job", "job-for-definitions"],
)
def test_a_worker_answers_what_it_cannot_read_with_a_fault(kind: pf.Kind, message: bytes) -> None:
    import multiprocessing

    parent, worker = multiprocessing.Pipe()
    try:
        parent.send_bytes(message)
        assert pexport._from_parent(worker, kind) is None
        answer = pf.decode(parent.recv_bytes())
        assert isinstance(answer, Fault) and answer.reason
    finally:
        parent.close()
        worker.close()


def test_a_reply_too_large_to_frame_becomes_a_crash_outcome() -> None:
    huge = Outcome(7, blocks=[Block(None, False, 1, "x" * (pf.MAX_REPLY_BYTES + 1) + "\n", 0)])
    reply = pf.decode(pexport._reply(huge))
    assert isinstance(reply, Outcome) and reply.index == 7 and reply.crash
    assert not reply.blocks


# -- a worker's crash is never a range without records ----------------------------------------


def test_a_worker_that_crashes_ends_the_export_instead_of_skipping_its_range(
    tmp_path: Path,
) -> None:
    """Real workers given definitions they cannot build answer every job with a crash outcome
    (which, like a range with no record, has no start): the export must end with
    ``IB-EXP-003``, not finish without those rows (found while replacing pickle: every range
    was skipped and the export reported success with no rows)."""
    from ironbird.recording.replay import Replay
    from poolutil import JOBS, clean_recording, run_export

    recording = clean_recording(tmp_path / "run.ibrec")
    replaying = Replay([recording])
    try:
        definitions = EmbeddedDefinitions.of(replaying, "")
    finally:
        replaying.close()
    unbuildable = dataclasses.replace(definitions, blobs={})
    with pexport.WorkerPool(definitions, JOBS) as pool:
        pool._greeting = pf.encode(unbuildable)  # what the workers build from
        pooled = run_export(recording, jobs=JOBS, pool=pool)
    assert run_export(recording).error is None
    assert pooled.error is not None and pooled.error[0] == Code.EXP_WORKER.value, pooled.error


def test_an_exact_range_answered_as_holding_no_record_is_refused() -> None:
    """Only a range read from a guess can begin past its end; an exact one begins where it was
    asked to, so a reply without a start (and without a crash or a move) would skip it."""
    exact = dataclasses.replace(JOB, exact=True)
    with pytest.raises(pf.FrameError):
        pf.check_reply(exact, Outcome(7))
    pf.check_reply(exact, Outcome(7, crash="x"))
    pf.check_reply(exact, Outcome(7, moved=True))
    pf.check_reply(JOB, Outcome(7))  # a guess: the walk decides
