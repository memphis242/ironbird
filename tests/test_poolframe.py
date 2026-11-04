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
from poolframe_corpus import CORPUS, JOB, LAYOUTS, PREFIX, ROWS, SHAPE, cases, outcome, samples

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
    reply = pf.read_reply(JOB, pf.encode(outcome()), LAYOUTS)
    assert reply == outcome() and reply.order is not None and list(reply.order) == [1, 0, 1]
    for name in ("outcome-crash", "outcome-moved", "outcome-failure"):
        message = SAMPLES[name]
        assert isinstance(message, Outcome)
        assert pf.read_reply(JOB, pf.encode(message), LAYOUTS) == message


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


def _many_small_items() -> Outcome:
    """An outcome of tables with so many columns that its items pass the budget (their bytes
    are far under the cap of a reply)."""
    columns = pf.MAX_ITEMS // pf.MAX_BLOCKS + 2
    block = Block(("m", ("",) * columns), False, 0, "", 0)
    return Outcome(7, blocks=[dataclasses.replace(block, handle=h) for h in range(pf.MAX_BLOCKS)])


def test_a_message_of_many_small_items_is_refused_at_the_item_budget() -> None:
    """Small strings cost far more memory than their bytes: one message makes at most
    ``MAX_ITEMS`` of them, whatever each list's own cap allows."""
    many = _many_small_items()
    out = pf._Out(pf.MAX_REPLY_BYTES)
    out.items = 1 << 62  # a sender that has no budget, to make the frame a receiver must refuse
    pf._put_outcome(out, many)
    assert "items" in refused(out.finish(pf.Kind.OUTCOME)).reason


def test_the_encoder_spends_the_item_budget_as_the_decoder_does() -> None:
    """A frame the encoder writes is never one the decoder refuses for its items (an honest
    worker with a very large definition set found its reply refused by the parent, not named
    at the sender)."""
    many = _many_small_items()
    with pytest.raises(pf.FrameError) as caught:
        pf.encode(many)
    assert "items" in caught.value.reason and "cannot be sent" in caught.value.summary
    assert "malformed" not in caught.value.summary
    # the largest outcome the budget allows is written, and read back
    columns = (pf.MAX_ITEMS // pf.MAX_BLOCKS - 3) // 2  # a block spends 2 a column, 2 more
    fits = Outcome(
        7,
        blocks=[
            Block(("m", ("",) * columns), False, 0, "", handle) for handle in range(pf.MAX_BLOCKS)
        ],
    )
    assert pf.decode(pf.encode(fits)) == fits


def test_a_result_over_the_item_budget_is_a_crash_outcome_that_says_why() -> None:
    reply = pf.decode(pexport._reply(_many_small_items()))
    assert isinstance(reply, Outcome) and reply.crash
    lines = reply.crash.strip().splitlines()
    assert "items" in lines[-1] and "IB-EXP-003" not in lines[-1] and "malformed" not in lines[-1]
    error = pexport._Runner._crashed(object(), reply.crash)  # type: ignore[arg-type]
    assert error.summary.count("IB-EXP-003") == 0 and "items" in error.summary


def test_a_worker_that_cannot_read_what_it_was_sent_says_so_once() -> None:
    import multiprocessing

    parent, worker = multiprocessing.Pipe()
    try:
        parent.send_bytes(b"garbage")
        assert pexport._from_parent(worker, pf.Kind.JOB) is None
        answer = pf.decode(parent.recv_bytes())
        assert isinstance(answer, Fault)
        with pytest.raises(pf.FrameError) as caught:
            pf.read_reply(JOB, pf.encode(answer), LAYOUTS)
        assert caught.value.summary.count("IB-EXP-003") == 0
        assert caught.value.summary.count("malformed") == 1
    finally:
        parent.close()
        worker.close()


# -- times ------------------------------------------------------------------------------------

_YEAR_1_NS = -62_135_596_800 * 10**9
_YEAR_9999_NS = 253_402_300_799 * 10**9


@pytest.mark.parametrize(
    ("start_ns", "end_ns"),
    [
        (_YEAR_1_NS, _YEAR_9999_NS),  # every instant --from/--to accept
        (None, 9223372036854775808),  # one past a signed 64-bit time (the year 2262)
        (-9223372036854775809, None),  # one before it (the year 1677)
        (0, 0),
        (-(1 << 127), (1 << 127) - 1),
    ],
)
def test_a_time_bound_of_any_year_travels(start_ns: int | None, end_ns: int | None) -> None:
    """A bound outside the range of a 64-bit nanosecond count is a valid ``--from``/``--to``
    (``--to 9999-12-31T23:59:59Z``), and a pooled export must take it as one process does."""
    job = dataclasses.replace(JOB, start_ns=start_ns, end_ns=end_ns)
    assert pf.decode(pf.encode(job)) == job


def test_a_time_bound_the_wire_cannot_hold_is_refused_at_the_sender() -> None:
    for bound in (1 << 127, -(1 << 127) - 1):
        with pytest.raises(pf.FrameError):
            pf.encode(dataclasses.replace(JOB, end_ns=bound))


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
        pf.read_reply(JOB, data, LAYOUTS)
    assert caught.value.code is Code.EXP_WORKER


# -- properties ------------------------------------------------------------------------------

_U64 = st.integers(0, (1 << 64) - 1)
_I64 = st.integers(-(1 << 63), (1 << 63) - 1)
_I128 = st.integers(-(1 << 127), (1 << 127) - 1)
_CODE = st.sampled_from([c.value for c in Code])
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
    st.dictionaries(_CODE, _U64, max_size=3).map(Counter),
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
        draw(st.none() | _I128),
        draw(st.none() | _I128),
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
        got = pf.read_reply(JOB, bytes(frame), LAYOUTS)
    except pf.FrameError:
        return
    pf.check_reply(JOB, got, LAYOUTS)


# -- the pool's two ends ---------------------------------------------------------------------


def _pool_with_a_fake_worker() -> tuple[pexport.WorkerPool, pexport.Ticket, Any]:
    """A pool whose one worker is the other end of a pipe this test holds, with ``JOB`` sent."""
    import multiprocessing

    definitions = SAMPLES["embedded"]
    assert isinstance(definitions, EmbeddedDefinitions)
    pool = pexport.WorkerPool(definitions, 1)
    ours, theirs = multiprocessing.Pipe()
    slot = pexport._Slot(None, ours)
    ticket = pexport.Ticket(JOB, b"", LAYOUTS)
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


def test_a_reply_of_a_table_no_definition_makes_ends_the_pool() -> None:
    """A table name from a worker reaches the file system in a directory export: a name that is
    not the key of a layout the codecs make is refused like any reply the parent cannot apply
    (it was accepted, and ``../../escaped`` was opened as a table: an ``AssertionError``, or with
    ``-O`` a file outside the export)."""
    evil = dataclasses.replace(
        outcome(),
        blocks=[
            Block(("../../escaped", ("seq", "CmdCounter")), False, 2, ROWS, 1),
            Block(None, True, 1, "5,6\n", 0),
        ],
    )
    pool, ticket, theirs = _pool_with_a_fake_worker()
    try:
        theirs.send_bytes(pf.encode(evil))
        pool.poll(5.0)
        assert ticket.done() and pool.broken
        with pytest.raises(IronbirdError) as caught:
            ticket.result()
        assert caught.value.code is Code.EXP_WORKER
    finally:
        pool.close(abnormal=True)
        theirs.close()


def test_a_table_is_accepted_only_if_a_codec_makes_it() -> None:
    assert LAYOUTS and pf.read_reply(JOB, pf.encode(outcome()), LAYOUTS)
    with pytest.raises(pf.FrameError):
        pf.read_reply(JOB, pf.encode(outcome()), frozenset())
    csv_only = dataclasses.replace(JOB, shape=dataclasses.replace(SHAPE, fmt=ExportFormat.TEXT))
    keyed = Outcome(7, blocks=[Block(("HK_TLM", ("seq", "CmdCounter")), False, 1, "1,2\n", 0)])
    with pytest.raises(pf.FrameError):  # text and JSON Lines have one stream, with no key
        pf.check_reply(csv_only, keyed, LAYOUTS)


def test_the_layouts_of_the_codecs_are_the_tables_an_export_makes(tmp_path: Path) -> None:
    from ironbird.recording.defs import embedded_codecs
    from ironbird.recording.replay import Replay
    from poolutil import clean_recording

    replaying = Replay([clean_recording(tmp_path / "run.ibrec")])
    try:
        codecs = embedded_codecs(
            replaying.parts[0],
            replaying.head,
            replaying.nodes,
            replaying.meta.data,
            replaying.max_packet_bytes,
        )
    finally:
        replaying.close()
    plain = pf.layouts_of(codecs, header_fields=False)
    wide = pf.layouts_of(codecs, header_fields=True)
    assert plain and wide and not plain & wide  # header columns change every table's key
    assert all(name and "/" not in name and columns for name, columns in plain | wide)


# -- a worker that stops part way through a reply is not waited for ---------------------------


def _polls_return(pool: pexport.WorkerPool, rounds: int, timeout: float) -> bool:
    """Whether ``rounds`` calls of ``pool.poll(timeout)`` all return within a few seconds (they
    ran in a thread so that a parent that blocks fails the test instead of hanging it)."""
    import threading

    def poll() -> None:
        for _ in range(rounds):
            pool.poll(timeout)

    thread = threading.Thread(target=poll, daemon=True)
    thread.start()
    thread.join(10.0)
    return not thread.is_alive()


def test_a_worker_that_stops_part_way_through_a_reply_does_not_hang_the_parent() -> None:
    """A worker that is alive and has sent the start of a reply (stopped, wedged or hostile) must
    cost the parent no more than the slice it waits in, so a signal is acted on between slices
    (``recv_bytes`` waited for the rest of the message, signals could not end it)."""
    pool, ticket, theirs = _pool_with_a_fake_worker()
    try:
        os.write(theirs.fileno(), struct.pack("!i", 100) + b"IBPF\x01\x03\x00\x00")
        assert _polls_return(pool, 3, 0.05), "poll waits for a reply that has not come"
        assert not ticket.done() and not pool.broken
        os.write(theirs.fileno(), bytes(92))  # the rest, which is not a frame
        pool.poll(5.0)
        assert ticket.done() and pool.broken
    finally:
        theirs.close()  # frees a parent that is stuck in the read, if the test failed
        pool.close(abnormal=True)


# -- the parent never waits on a worker that does not read, or stops, either ------------------


def _big_job(sources: int = 60000) -> Job:
    """A job whose frame is far larger than a socket's buffer (a seed that names many sources, as a
    recording with many UDP peers does)."""
    seed = Seed(
        JOB.seed.streams,
        {
            i: f"10.{i >> 16 & 255}.{i >> 8 & 255}.{i & 255}:{5000 + i % 1000}"
            for i in range(sources)
        },
        41,
        512,
    )
    job = dataclasses.replace(JOB, seed=seed)
    assert len(pf.encode(job)) > 1_000_000
    return job


def _runs_within(seconds: float, action: Any) -> bool:
    """Whether ``action()`` returns within ``seconds``; it runs in a thread, so that a parent that
    blocks fails the test instead of hanging it."""
    import threading

    thread = threading.Thread(target=action, daemon=True)
    thread.start()
    thread.join(seconds)
    return not thread.is_alive()


def test_a_big_job_sent_to_a_worker_that_does_not_read_never_hangs_the_parent() -> None:
    """The other direction of the stalled reply: a blocking send of a job over the socket buffer
    waited for a worker that was stopped or wedged, and no signal ended it."""
    pool, _, theirs = _pool_with_a_fake_worker()
    pool._slots[0].sent.clear()  # an idle worker: it is given the big job at once
    job = _big_job()
    try:

        def send_and_poll() -> None:
            pool.submit(job, LAYOUTS)
            for _ in range(5):
                pool.poll(pexport.WAIT_SLICE_S)

        assert _runs_within(10.0, send_and_poll), "the parent waits for a worker that does not read"
        assert pool._slots[0].outbox.unsent > 0 and not pool.broken
    finally:
        theirs.close()
        pool.close(abnormal=True)


def test_what_a_worker_does_not_read_at_once_reaches_it_when_it_does_in_order() -> None:
    """The greeting and the job travel the way ``send_bytes`` sends them: the worker's
    ``recv_bytes`` reads each whole, in order, however the pipe took them."""
    import threading

    definitions = SAMPLES["embedded"]
    pool, _, theirs = _pool_with_a_fake_worker()
    slot = pool._slots[0]
    slot.sent.clear()
    job = _big_job()
    greeting = pf.encode(definitions)
    slot.outbox.put(greeting)
    got: list[bytes] = []
    reader = threading.Thread(
        target=lambda: got.extend(theirs.recv_bytes() for _ in range(2)), daemon=True
    )
    try:
        pool.submit(job, LAYOUTS)
        reader.start()
        for _ in range(400):
            pool.poll(pexport.WAIT_SLICE_S)
            if not reader.is_alive():
                break
        reader.join(10.0)
        assert got == [greeting, pf.encode(job)]
        assert slot.outbox.unsent == 0 and not pool.broken
    finally:
        theirs.close()
        pool.close(abnormal=True)


def test_a_worker_that_does_not_read_is_a_stall_with_a_message_that_names_it() -> None:
    import time

    pool, _, theirs = _pool_with_a_fake_worker()
    pool.stall_s = 0.3
    slot = pool._slots[0]
    slot.sent.clear()
    try:
        ticket = pool.submit(_big_job(), LAYOUTS)
        started = time.monotonic()
        while not ticket.done() and time.monotonic() - started < 10.0:
            pool.poll(pexport.WAIT_SLICE_S)
        assert ticket.done() and pool.broken
        with pytest.raises(IronbirdError) as caught:
            ticket.result()
        assert caught.value.code is Code.EXP_WORKER
        assert "no progress" in caught.value.summary and "\n" not in caught.value.summary
        assert "[export] worker_stall" in (caught.value.details or "")
    finally:
        theirs.close()
        pool.close(abnormal=True)


def test_a_worker_that_holds_a_job_and_never_answers_is_a_stall() -> None:
    import time

    pool, ticket, theirs = _pool_with_a_fake_worker()
    pool.stall_s = 0.3
    try:
        started = time.monotonic()
        while not ticket.done() and time.monotonic() - started < 10.0:
            pool.poll(pexport.WAIT_SLICE_S)
        assert ticket.done() and pool.broken and 0.3 <= time.monotonic() - started < 5.0
        with pytest.raises(IronbirdError) as caught:
            ticket.result()
        assert caught.value.code is Code.EXP_WORKER and "no progress" in caught.value.summary
    finally:
        theirs.close()
        pool.close(abnormal=True)


def test_a_worker_that_keeps_moving_bytes_is_not_a_stall() -> None:
    import time

    pool, ticket, theirs = _pool_with_a_fake_worker()
    pool.stall_s = 0.5
    frame = pf.encode(outcome())
    wire = struct.pack("!i", len(frame)) + frame
    try:
        started = time.monotonic()
        at = 0
        while time.monotonic() - started < 1.5 and at < 12:  # three times the limit in all
            os.write(theirs.fileno(), wire[at : at + 1])
            at += 1
            pool.poll(0.1)
        assert not ticket.done() and not pool.broken
    finally:
        theirs.close()
        pool.close(abnormal=True)


def test_an_idle_worker_is_never_a_stall() -> None:
    import time

    pool, _, theirs = _pool_with_a_fake_worker()
    pool.stall_s = 0.2
    pool._slots[0].sent.clear()
    try:
        time.sleep(0.4)
        pool.poll(0.05)
        assert not pool.broken
    finally:
        theirs.close()
        pool.close(abnormal=True)


def test_one_wait_slice_is_shared_by_the_pipes_that_are_ready() -> None:
    """Each ready pipe is served for its share of one slice, so a poll cannot outlast a signal by
    the number of workers times the slice."""
    import multiprocessing

    pool, _, first = _pool_with_a_fake_worker()
    others = []
    for _ in range(3):
        ours, theirs = multiprocessing.Pipe()
        slot = pexport._Slot(None, ours)
        slot.sent.append(pexport.Ticket(JOB, b"", LAYOUTS))
        pool._slots.append(slot)
        others.append(theirs)
    budgets: list[float] = []
    pool._receive = lambda slot, budget: budgets.append(budget)  # type: ignore[method-assign]
    try:
        for end in (first, *others):
            end.send_bytes(b"x")
        pool.poll(5.0)
        assert len(budgets) == 4
        assert sum(budgets) <= pexport.WAIT_SLICE_S * 1.0001
    finally:
        for end in (first, *others):
            end.close()
        pool.close(abnormal=True)


def test_a_reply_is_held_only_up_to_what_its_job_can_have() -> None:
    """The length is checked against the job's own cap, not the cap of any reply: a worker that
    announces more than the job can honestly make is refused unread, and one that announces what it
    can make and stops is waited for without holding anything."""
    small = dataclasses.replace(JOB, text_cap=1000, event_cap=10, stop=JOB.guess + 4096)
    cap = pexport.reply_cap(small)
    assert cap < pf.MAX_REPLY_BYTES // 2
    for length, refused_at_once in ((cap + 1, True), (cap, False)):
        pool, ticket, theirs = _pool_with_a_fake_worker()
        pool._slots[0].sent[0] = pexport.Ticket(small, b"", LAYOUTS)
        ticket = pool._slots[0].sent[0]
        try:
            os.write(theirs.fileno(), struct.pack("!i", length))
            assert _polls_return(pool, 2, 0.05)
            assert ticket.done() is refused_at_once and pool.broken is refused_at_once
            assert pool._slots[0].inbox.held() <= 4
            if refused_at_once:
                with pytest.raises(IronbirdError) as caught:
                    ticket.result()
                assert "too long" in caught.value.summary
        finally:
            theirs.close()
            pool.close(abnormal=True)


def test_the_reply_cap_is_a_bound_on_what_an_honest_worker_makes() -> None:
    big = dataclasses.replace(JOB, text_cap=pf.MAX_TEXT_CAP, event_cap=pf.MAX_EVENT_CAP, stop=None)
    assert pexport.reply_cap(big) == pf.MAX_REPLY_BYTES
    sizes = [
        pexport.reply_cap(dataclasses.replace(JOB, text_cap=cap)) for cap in (1, 1000, 1 << 20)
    ]
    assert sizes == sorted(sizes) and sizes[0] > 0 and sizes[-1] < pf.MAX_REPLY_BYTES
    # rows past the cap of the widest allowed row, every character four bytes: still within it
    job = dataclasses.replace(JOB, text_cap=1000)
    wide = "\U0001f600" * (job.text_cap + pexport.REPLY_ROW_CHARS - 1) + "\n"
    worst = Outcome(job.index, blocks=[Block(None, False, 1, wide, 0)])
    assert len(pf.encode(worst)) <= pexport.reply_cap(job)


def test_a_table_is_a_layout_or_the_reply_is_refused() -> None:
    """A table name from a worker reaches the file system in a directory export (it was accepted:
    ``../../escaped`` was opened as a table). Every key that is not exactly a layout's is refused
    as the reply of a worker, by ``read_reply`` and through the pool."""
    keys = [
        ("../../escaped", ("seq", "CmdCounter")),
        ("HK_TLM", ("seq", "CmdCounter", "x")),
        ("HK_TLM\x00", ("seq", "CmdCounter")),
        ("hk_tlm", ("seq", "CmdCounter")),
    ]
    for key in keys:
        evil = dataclasses.replace(
            outcome(), blocks=[Block(key, False, 2, ROWS, 1), Block(None, True, 1, "5,6\n", 0)]
        )
        with pytest.raises(pf.FrameError, match="no definition makes"):
            pf.read_reply(JOB, pf.encode(evil), LAYOUTS)
        pool, ticket, theirs = _pool_with_a_fake_worker()
        try:
            theirs.send_bytes(pf.encode(evil))
            pool.poll(5.0)
            assert ticket.done() and pool.broken
            with pytest.raises(IronbirdError) as caught:
                ticket.result()
            assert caught.value.code is Code.EXP_WORKER
        finally:
            pool.close(abnormal=True)
            theirs.close()


def test_the_tables_an_answer_may_hold_are_always_given() -> None:
    import inspect

    for function in (pexport.WorkerPool.submit, pexport.Ticket.__init__):
        assert inspect.signature(function).parameters["layouts"].default is inspect.Parameter.empty


def test_a_worker_that_dies_part_way_through_a_reply_ends_the_pool() -> None:
    pool, ticket, theirs = _pool_with_a_fake_worker()
    try:
        os.write(theirs.fileno(), struct.pack("!i", 100) + b"IBPF")
        assert _polls_return(pool, 1, 0.05)
        assert not ticket.done()
        theirs.close()
        pool.poll(5.0)
        assert ticket.done() and pool.broken
        with pytest.raises(IronbirdError) as caught:
            ticket.result()
        assert caught.value.code is Code.EXP_WORKER and "died" in caught.value.summary
    finally:
        pool.close(abnormal=True)


def test_a_reply_that_arrives_in_small_pieces_is_put_together() -> None:
    pool, ticket, theirs = _pool_with_a_fake_worker()
    try:
        frame = pf.encode(outcome())
        wire = struct.pack("!i", len(frame)) + frame
        for at in range(0, len(wire), 7):
            os.write(theirs.fileno(), wire[at : at + 7])
            pool.poll(1.0)
        pool.poll(1.0)
        assert ticket.result() == outcome() and not pool.broken
    finally:
        pool.close(abnormal=True)
        theirs.close()


def test_a_reply_after_a_reply_is_read_in_its_turn() -> None:
    pool, first, theirs = _pool_with_a_fake_worker()
    second = pexport.Ticket(JOB, b"", LAYOUTS)
    pool._slots[0].sent.append(second)
    try:
        theirs.send_bytes(pf.encode(outcome()))
        theirs.send_bytes(pf.encode(outcome()))
        for _ in range(4):
            pool.poll(1.0)
        assert first.result() == outcome() and second.result() == outcome()
    finally:
        pool.close(abnormal=True)
        theirs.close()


def test_a_reply_that_is_empty_or_announces_a_negative_length_is_refused() -> None:
    for length, tail in ((0, b""), (-1, b"\0" * 8)):
        pool, ticket, theirs = _pool_with_a_fake_worker()
        try:
            os.write(theirs.fileno(), struct.pack("!i", length) + tail)
            pool.poll(5.0)
            assert ticket.done() and pool.broken
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
        pf.check_reply(exact, Outcome(7), LAYOUTS)
    pf.check_reply(exact, Outcome(7, crash="x"), LAYOUTS)
    pf.check_reply(exact, Outcome(7, moved=True), LAYOUTS)
    pf.check_reply(JOB, Outcome(7), LAYOUTS)  # a guess: the walk decides
