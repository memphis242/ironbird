"""Property tests of the filter language: arbitrary text never raises anything but a
``FilterError`` within a time bound, compiled filters equal a naive reference evaluator on random
frames and expressions, the candidate MID set never excludes a match, and a frame outside it is
never decoded."""

from __future__ import annotations

import contextlib
import time
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from filter_defs import (
    LINKS,
    SERIAL_STREAM,
    STREAMS,
    Reference,
    build_codecs,
    count_decodes,
    make_frame,
)
from ironbird.ccdd.model import MessageKind, TypeClass
from ironbird.codec import Codec
from ironbird.errors import Code
from ironbird.filter import MAX_FILTER_CHARS, FilterError, compile_filter, parse
from ironbird.frame import Frame

FUZZ = settings(max_examples=400, deadline=1000, suppress_health_check=[HealthCheck.too_slow])
PROPERTY = settings(max_examples=600, deadline=2000, suppress_health_check=[HealthCheck.too_slow])


@pytest.fixture(scope="module")
def codecs(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Codec]:
    return build_codecs(tmp_path_factory.mktemp("filter-fuzz"))


# -- arbitrary text ----------------------------------------------------------------------

ANY_CHAR = st.characters(blacklist_categories=())  # every code point, lone surrogates included
VOCABULARY = [
    "seq", "time", "len", "node", "link", "dir", "kind", "source", "mid", "truncated", "garbage",
    "category", "msg", "status", "fc", "htime", "hdr", "event", "hdr.sequence", "event.type",
    "TLM_A", "TLM_A.counter", "counter", "level", "mode", "name", "blob", "n1", "rx", "ok",
    "and", "or", "not", "in", "contains", "true", "false", "==", "!=", "<", "<=", ">", ">=", "=",
    "!", "(", ")", "[", "]", ",", ".", "-", "0", "1", "-1", "0x1f", "1.5", "-0.5", "99999999999",
    '"abc"', "'x'", '"', "'", "\\", "2026-10-29T12:00:00Z", "2026-10-29T12:00:00", "\x00", "\x1b",
    "\u00e9", "\u202e", " ", "\t", "\n",
]  # fmt: skip
SOUP = st.lists(st.sampled_from(VOCABULARY), max_size=60).flatmap(
    lambda tokens: st.sampled_from(["", " ", "  "]).map(lambda sep: sep.join(tokens))
)
NESTING = st.builds(
    lambda open_, count, body, close_: open_ * count + body + close_ * count,
    st.sampled_from(["(", "not ", "not (", "( not ", "(((", "[", "not not "]),
    st.integers(0, 3000),
    st.sampled_from(["", "seq == 1", "counter in [1]", "x"]),
    st.sampled_from([")", "", "]"]),
)
TEXTS = st.one_of(
    st.text(alphabet=ANY_CHAR, max_size=MAX_FILTER_CHARS + 50),
    SOUP,
    NESTING,
    st.binary(max_size=300).map(lambda b: b.decode("utf-8", errors="surrogateescape")),
)


def survives(codecs: dict[str, Codec], text: str) -> None:
    started = time.perf_counter()
    try:
        flt = compile_filter(text, codecs, links=LINKS)
    except FilterError as err:
        assert 1 <= err.column <= len(text) + 1, (err.column, len(text))
        assert err.exit_code == 2 and err.code.value.startswith("IB-FLT-")
        line = err.user_line()
        assert "\n" not in line and all(ch.isprintable() for ch in line)
        assert all(ch.isprintable() or ch == "\n" for ch in err.detail_text())
    else:
        # What compiled must run, whatever the frame.
        decode, _ = count_decodes(codecs)
        matcher = flt.bind(decode, lambda node: codecs[node])
        frame = make_frame(
            codecs["n1"], STREAMS[("n1", MessageKind.TELEMETRY)], b"\x08\x00" + b"\x00" * 30
        )
        assert matcher(frame) in (True, False)
    assert time.perf_counter() - started < 1.5


@FUZZ
@given(text=TEXTS)
def test_arbitrary_text_raises_only_filter_errors_and_quickly(
    codecs: dict[str, Codec], text: str
) -> None:
    survives(codecs, text)
    with contextlib.suppress(FilterError):
        parse(text)


@pytest.mark.parametrize(
    "text",
    [
        "(" * 5000,
        "not " * 1100,
        "(" * 32 + "seq == 1" + ")" * 32,
        "not " * 32 + "seq == 1",
        "a." * 2000,
        "seq in [" + "1," * 2000,
        "x == " + "9" * 4000,
        '"' * 4096,
        "seq == 1 " + "and seq == 1 " * 330,
        "seq == 1 " + "or not seq == 1 " * 250,
        "(" * 20 + "seq == 1 or " * 90 + "seq == 2" + ")" * 20,
        " " * 4096,
        "\x00" * 4096,
        "'" + "\\'" * 2000 + "'",
        "msg in [" + ", ".join(["TLM_A"] * 340) + "]",
    ],
)
def test_inputs_at_the_hard_limits_are_handled_in_bounded_time(
    codecs: dict[str, Codec], text: str
) -> None:
    survives(codecs, text)


# -- reference evaluator -----------------------------------------------------------------

OPS = ["==", "!=", "<", "<=", ">", ">="]
ops = st.sampled_from(OPS)
small = st.integers(-2, 300)
MIDS = [0x0800, 0x0801, 0x0802, 0x0810, 0x1800, 0x1801, 0x07F1, 1]

PREDICATES = st.one_of(
    st.builds("seq {} {}".format, ops, st.integers(0, 12)),
    st.builds("len {} {}".format, ops, st.integers(0, 60)),
    st.builds("mid {} {}".format, ops, st.sampled_from(MIDS)),
    st.builds(
        "mid in [{}]".format,
        st.lists(st.sampled_from(MIDS), min_size=1, max_size=3).map(
            lambda xs: ",".join(map(str, xs))
        ),
    ),
    st.builds("time {} 2026-10-{:02d}T00:00:00Z".format, ops, st.integers(28, 30)),
    st.sampled_from(
        [
            "node == n1",
            "node != n2",
            "node in [n1, n2]",
            "link == l2",
            "dir == rx",
            "dir != tx",
            "kind == telemetry",
            "kind in [command]",
            'source contains "127"',
            'source == "10.0.0.1:5"',
            "truncated == true",
            "garbage == true",
            "garbage != true",
            "category == event",
            "category != event",
            "category in [status, housekeeping]",
            "msg == TLM_A",
            "msg != TLM_B",
            "msg in [TLM_EV, CMD_Y, N2_CMD]",
            'msg contains "TLM"',
            "msg == CMD_X",
            "status == ok",
            "status != ok",
            "status == malformed",
            "status in [unknown, malformed]",
            "fc == 1",
            "fc >= 0",
            "fc != 0",
            "hdr.length > 20",
            "hdr.version == 0",
            "event.type >= error",
            "event.type == information",
            "event.type < warning",
            'event.app == "APP"',
            "event.id in [1, 2, 3]",
            'event.text contains "a"',
            "mode == SAFE",
            "mode != NOMINAL",
            "mode >= SAFE",
            "mode in [SAFE, SCIENCE]",
            "mode in [NOMINAL, SCIENCE, SAFE]",
            "mode != SCIENCE",
            "ratio == 0.1",
            "ratio in [0.1, 2.5]",
            "big > 254.5",
            "event.type > information",
            "event.type != error",
            "event.type in [debug, error]",
            'label contains "b"',
            'blob == "00112233"',
            'blob contains "11"',
            'counter == "hi"',
            'TLM_A.name == "abc"',
            "TLM_B.counter > 3",
            "other > 5",
            "level < -3",
            "ratio >= 0.5",
            "ratio != 1",
            "counter > 1.5",
            'name contains "a"',
            "TLM_EV.event_type == ERROR",
            "TLM_EV2.event_id < 2",
        ]
    ),
    st.builds("hdr.sequence {} {}".format, ops, st.integers(0, 3)),
    st.builds("counter {} {}".format, ops, small),
    st.builds("level {} {}".format, ops, st.integers(-10, 10)),
    st.builds("event_id {} {}".format, ops, st.integers(0, 5)),
)


def combine(children: st.SearchStrategy[str]) -> st.SearchStrategy[str]:
    return st.one_of(
        children.map("not {}".format),
        children.map("({})".format),
        st.lists(children, min_size=2, max_size=4).map(
            lambda xs: " and ".join(f"({x})" for x in xs)
        ),
        st.lists(children, min_size=2, max_size=4).map(
            lambda xs: " or ".join(f"({x})" for x in xs)
        ),
        st.lists(children, min_size=2, max_size=3).map(" and ".join),
    )


EXPRESSIONS = st.recursive(PREDICATES, combine, max_leaves=8)

CAP = 2048
SAMPLES: dict[TypeClass, Any] = {
    TypeClass.INT: st.sampled_from([0, 1, 2, 3, 4, 5, 9, 255]),
    TypeClass.FLOAT: st.sampled_from([0.0, 0.5, 1.0, -1.0, 2.5, 0.1]),
    TypeClass.CHAR: st.sampled_from(["", "abc", "APP", "hi", "a", "b"]),
}


@st.composite
def frames(draw: st.DrawFn, codecs: dict[str, Codec]) -> Frame:
    node = draw(st.sampled_from(["n1", "n2"]))
    codec = codecs[node]
    kind = draw(st.sampled_from(list(MessageKind)))
    message = draw(st.sampled_from([m for m in codec.model.messages if m.kind is kind]))
    values: dict[str, object] = {}
    for fld in message.fields:
        klass = fld.type.type_class
        if klass is TypeClass.BYTES:
            values[fld.name] = draw(
                st.sampled_from([bytes(fld.type.size), b"\x00\x11\x22\x33"[: fld.type.size]])
            )
        else:
            value = draw(SAMPLES[klass])
            if klass is TypeClass.INT:
                lo, hi = fld.type.bit_range
                value = max(lo, min(hi, value))
            values[fld.name] = value
    if kind is MessageKind.TELEMETRY:
        data = codec.encode_telemetry(message.name, values).data
    else:
        data = codec.encode_command(message.name, values).data
    mutation = draw(st.sampled_from(["none"] * 6 + ["flip", "chop", "extend", "mid", "random"]))
    if mutation == "flip" and data:
        i = draw(st.integers(0, len(data) - 1))
        data = data[:i] + bytes([data[i] ^ draw(st.integers(1, 255))]) + data[i + 1 :]
    elif mutation == "chop":
        data = data[: draw(st.integers(0, len(data)))]
    elif mutation == "extend":
        data += bytes(draw(st.integers(1, 8)))
    elif mutation == "mid" and len(data) >= 2:
        data = bytes([data[0], draw(st.sampled_from([0x00, 0x01, 0x10, 0x7F]))]) + data[2:]
    elif mutation == "random":
        data = draw(st.binary(max_size=60))
    stream = STREAMS[(node, kind)]
    cut = draw(st.sampled_from([None] * 8 + [50]))
    garbage = draw(st.sampled_from([False] * 12 + [True]))
    if garbage:
        stream, cut = SERIAL_STREAM, None
        node = "n1"
    elif cut:
        data = data.ljust(CAP, b"\0")[:CAP]
    return make_frame(
        codecs[stream.node],
        stream,
        data,
        seq=draw(st.integers(1, 12)),
        time_ns=draw(st.sampled_from([1_793_000_000_000_000_000, 1_793_300_000_000_000_000])),
        source=draw(st.sampled_from(["127.0.0.1:2234", "10.0.0.1:5", ""])),
        cut=cut,
        live=draw(st.booleans()),
        garbage=garbage,
    )


@PROPERTY
@given(expression=EXPRESSIONS, data=st.data())
def test_compiled_filters_equal_the_reference_and_respect_the_candidate_mids(
    codecs: dict[str, Codec], expression: str, data: st.DataObject
) -> None:
    try:
        flt = compile_filter(expression, codecs, links=LINKS)
    except FilterError:
        return  # not every random combination fits the CCDD; those are the compiler's refusals
    decode, seen = count_decodes(codecs)
    matcher = flt.bind(decode, lambda node: codecs[node])
    reference = Reference(codecs)
    offered = data.draw(st.lists(frames(codecs), min_size=1, max_size=8))
    for frame in offered:
        verdict = matcher(frame)
        assert verdict == reference.matches(expression, frame), (expression, frame)
        if verdict:
            assert flt.mids is None or frame.mid in flt.mids, (expression, frame.mid)
    if not flt.needs_decode:
        assert seen == [], expression
    if flt.mids is not None:
        assert all(f.mid in flt.mids for f in seen), expression  # never outside the candidates


@settings(
    max_examples=5,
    deadline=None,
    suppress_health_check=[
        HealthCheck.too_slow,
        HealthCheck.data_too_large,
        HealthCheck.filter_too_much,
    ],
)
@given(texts=st.lists(EXPRESSIONS, min_size=60, max_size=60))
def test_the_expression_pool_mostly_compiles(codecs: dict[str, Codec], texts: list[str]) -> None:
    """Guards the property test against passing on compile refusals alone."""
    refused = 0
    for text in texts:
        try:
            compile_filter(text, codecs, links=LINKS)
        except FilterError:
            refused += 1
    assert refused < len(texts) // 2, f"{refused} of {len(texts)} refused"


@pytest.mark.parametrize(
    "text", ["counter == 1", "msg == TLM_A", "mode == SAFE", "event.type >= error", "fc == 1"]
)
def test_pool_representatives_compile(codecs: dict[str, Codec], text: str) -> None:
    compile_filter(text, codecs, links=LINKS)


def test_codes_for_bad_text_are_filter_codes(codecs: dict[str, Codec]) -> None:
    for text in ["", "(", "seq ==", "nope == 1", "seq == x", "not " * 40 + "seq == 1"]:
        with pytest.raises(FilterError) as info:
            compile_filter(text, codecs)
        assert info.value.code in (Code.FLT_SYNTAX, Code.FLT_NAME, Code.FLT_TYPE, Code.FLT_LIMIT)
