"""The filter language (docs/plan.md S4): lexing, parsing, name and type checking, the candidate
MID set, cost ordering, and what each name means on a frame."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import pytest

from conftest import CORPUS, EXAMPLES
from filter_defs import (
    CCDD_N1,
    LINKS,
    SERIAL_STREAM,
    STREAMS,
    build_codecs,
    count_decodes,
    decode_frame,
    make_frame,
)
from ironbird.ccdd.loader import load_ccdd
from ironbird.ccdd.model import EventSeverity, MessageKind, meets_floor
from ironbird.codec import Codec
from ironbird.errors import Code, DecodeError
from ironbird.filter import (
    MAX_DEPTH,
    MAX_FILTER_CHARS,
    MAX_IN_ITEMS,
    MAX_TOKENS,
    Cost,
    Filter,
    FilterError,
    Matcher,
    compile_filter,
    parse,
)
from ironbird.filter.diag import SUGGEST_POOL_NAMES, display_width, suggest, visible
from ironbird.filter.lexer import Tok, tokenize
from ironbird.filter.parser import And, Not, Or, Predicate
from ironbird.frame import Frame, FrameFlag
from ironbird.header import load_profile

N1_TLM = STREAMS[("n1", MessageKind.TELEMETRY)]
N1_CMD = STREAMS[("n1", MessageKind.COMMAND)]
N2_TLM = STREAMS[("n2", MessageKind.TELEMETRY)]


@pytest.fixture(scope="module")
def codecs(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Codec]:
    return build_codecs(tmp_path_factory.mktemp("filter-ccdd"))


def compiled(codecs: dict[str, Codec], text: str) -> Filter:
    return compile_filter(text, codecs, links=LINKS)


def matcher_for(codecs: dict[str, Codec], text: str) -> tuple[Matcher, list[Frame]]:
    decode, seen = count_decodes(codecs)
    return compiled(codecs, text).bind(decode, lambda node: codecs[node]), seen


def error_of(codecs: dict[str, Codec], text: str) -> FilterError:
    with pytest.raises(FilterError) as info:
        compiled(codecs, text)
    return info.value


def hk(codecs: dict[str, Codec], **values: Any) -> Frame:
    data = codecs["n1"].encode_telemetry("TLM_A", values).data
    return make_frame(codecs["n1"], N1_TLM, data)


# -- lexer ---------------------------------------------------------------------------------


def kinds(text: str) -> list[Tok]:
    return [t.kind for t in tokenize(text)][:-1]


def test_negative_numbers_belong_to_the_literal() -> None:
    tokens = tokenize("x == -1 and y >= -0.5 and z in [-0x10, 2]")
    values = [t.value for t in tokens if t.kind in (Tok.INT, Tok.FLOAT, Tok.HEX)]
    assert values == [-1, -0.5, -16, 2]
    assert Tok.INT in kinds("x<-1")  # no spaces needed around the sign


@pytest.mark.parametrize(
    "text", ["x == 1.", "x == 12ab", "x == -", "x == 0x", "x == 1.5.2", "x == 1_0"]
)
def test_malformed_numbers_are_syntax_errors(text: str) -> None:
    with pytest.raises(FilterError) as info:
        parse(text)
    assert info.value.code is Code.FLT_SYNTAX


def test_time_literals_are_exact_and_need_a_zone() -> None:
    [*_, time_token, _end] = tokenize("time >= 2026-10-29T12:00:00.5Z")
    assert time_token.value == 1_793_275_200_500_000_000
    offset = tokenize("time == 2026-10-29T14:00:00+02:00")[2].value
    assert offset == 1_793_275_200_000_000_000
    assert tokenize("time == 2026-10-29T14:00:00+0200")[2].value == offset
    for text in (
        "time == 2026-10-29T12:00:00",  # naive local time
        "time == 2026-13-29T12:00:00Z",
        "time == 2026-10-29T25:00:00Z",
        "time == 2026-10-29T12:00:00+24:00",
        "time == 2026-10-29T12:00Z",
        "time == 9999-12-31T23:59:59-23:59",  # overflows datetime once shifted to UTC
        "time == 0001-01-01T00:00:00+23:59",
    ):
        with pytest.raises(FilterError) as info:
            tokenize(text)
        assert info.value.code is Code.FLT_SYNTAX, text
    with pytest.raises(FilterError):  # a date alone is not a time
        parse("time == 2026-10-29")


def test_strings_escapes_and_rejections() -> None:
    assert tokenize(r'x == "a\"b\\c"')[2].value == 'a"b\\c'
    assert tokenize("x == 'it\\'s'")[2].value == "it's"
    assert tokenize('x == "caf\u00e9 \u4e2d"')[2].value == "caf\u00e9 \u4e2d"  # non-ASCII inside
    for text in ['x == "open', 'x == "a\\n"', 'x == "a\tb"', 'x == "a\x1bb"', 'x == "a\u202eb"']:
        with pytest.raises(FilterError) as info:
            tokenize(text)
        assert info.value.code is Code.FLT_SYNTAX, text


@pytest.mark.parametrize(
    "text", ["caf\u00e9 == 1", "x \u2212 1", "x = 1", "x ! 1", "x == 1 \x00", "x == 1;", "x == $1"]
)
def test_stray_characters_are_syntax_errors(text: str) -> None:
    with pytest.raises(FilterError) as info:
        tokenize(text)
    assert info.value.code is Code.FLT_SYNTAX


# -- parser --------------------------------------------------------------------------------


def test_precedence_not_over_and_over_or_and_chains_are_flat() -> None:
    tree = parse("a == 1 or b == 2 and not c == 3 or d == 4 or e == 5")
    assert isinstance(tree, Or) and len(tree.children) == 4
    second = tree.children[1]
    assert isinstance(second, And) and isinstance(second.children[1], Not)
    # A parenthesised chain of the same kind merges into its parent, so nesting adds no depth.
    merged = parse("(a == 1 and b == 2) and (c == 3 and d == 4) and e == 5")
    assert isinstance(merged, And) and len(merged.children) == 5
    assert all(isinstance(c, Predicate) for c in merged.children)


def test_predicate_forms() -> None:
    p = parse("TLM_A.level in [1, -2, 0x3]")
    assert isinstance(p, Predicate)
    assert (
        p.name == ("TLM_A", "level") and p.op == "in" and [v.value for v in p.values] == [1, -2, 3]
    )
    q = parse('label contains "ab"')
    assert isinstance(q, Predicate) and q.op == "contains"
    assert isinstance(parse("hdr.and == 1"), Predicate)  # after a dot any word is a name


@pytest.mark.parametrize(
    ("text", "column"),
    [
        ("", 1),
        ("   ", 4),
        ("x ==", 5),
        ("x == 1 and", 11),
        ("(x == 1", 8),
        ("x == 1)", 7),
        ("x == 1 y == 2", 8),
        ("and == 1", 1),
        ("x in 1", 6),
        ("x in []", 7),
        ("x in [1,]", 9),
        ("x in [1 2]", 9),
        ("x contains 1", 12),
        ("x == and", 6),
        ("x . == 1", 5),
        ("not", 4),
        ("x < < 1", 5),
    ],
)
def test_syntax_errors_name_the_column(text: str, column: int) -> None:
    with pytest.raises(FilterError) as info:
        parse(text)
    err = info.value
    assert err.code is Code.FLT_SYNTAX and err.column == column, err.user_line()
    assert "\n" not in err.user_line()


# -- hard limits ---------------------------------------------------------------------------


def limit_error(text: str) -> FilterError:
    with pytest.raises(FilterError) as info:
        parse(text)
    assert info.value.code is Code.FLT_LIMIT
    return info.value


def test_depth_counts_every_not_and_every_parenthesis() -> None:
    ok = "not " * MAX_DEPTH + "x == 1"
    parse(ok)
    limit_error("not " * (MAX_DEPTH + 1) + "x == 1")
    parse("(" * MAX_DEPTH + "x == 1" + ")" * MAX_DEPTH)
    limit_error("(" * (MAX_DEPTH + 1) + "x == 1" + ")" * (MAX_DEPTH + 1))
    # A mix counts both; flat chains do not count at all.
    limit_error("not (" * 17 + "x == 1" + ")" * 17)
    parse(" and ".join(["x == 1"] * 250))
    parse(" or ".join(["x == 1"] * 250))


def test_length_token_in_list_and_number_limits() -> None:
    parse("x == 1" + " " * (MAX_FILTER_CHARS - 6))
    limit_error("x == 1" + " " * (MAX_FILTER_CHARS - 5))
    limit_error(" and ".join(["x == 1"] * 400))  # over 1024 tokens within 4096 characters
    assert MAX_TOKENS == 1024 and MAX_IN_ITEMS == 1024
    parse("x in [" + ",".join(["1"] * 500) + "]")
    limit_error("x in [" + ",".join(["1"] * 2000) + "]")
    # The documented item limit is the one in force: the items of a list are not counted as
    # tokens, so exactly MAX_IN_ITEMS fit and one more is refused for being one item too many.
    parse("x in [" + ",".join(["1"] * MAX_IN_ITEMS) + "]")
    over = limit_error("x in [" + ",".join(["1"] * (MAX_IN_ITEMS + 1)) + "]")
    assert "items" in over.summary and "tokens" not in over.summary
    assert over.column == len("x in [" + ",".join(["1"] * MAX_IN_ITEMS)) + 1
    # Tokens outside lists stay limited, and a list adds only its brackets to that count.
    parse("x in [1,2] and " * 200 + "y == 1")
    limit_error("x in [1,2] and " * 250 + "y == 1")
    parse("x == -" + "9" * 39)  # 40 characters, sign included
    limit_error("x == -" + "9" * 40)
    limit_error("x == 0x" + "f" * 39)
    limit_error("x == " + "9" * 20 + "." + "9" * 20)


def test_deepest_accepted_filter_compiles_and_evaluates_without_recursion_trouble(
    codecs: dict[str, Codec],
) -> None:
    text = "not " * 16 + "(" * 16 + "counter == 1" + ")" * 16
    flt = compiled(codecs, text)
    decode, _ = count_decodes(codecs)
    matcher = flt.bind(decode, lambda n: codecs[n])
    assert matcher(hk(codecs, counter=1)) is True  # 16 nots cancel
    assert matcher(hk(codecs, counter=2)) is False
    odd = compiled(codecs, "not " * 31 + "counter == 1")
    assert odd.bind(decode, lambda n: codecs[n])(hk(codecs, counter=1)) is False


# -- names ---------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text",
    [
        "seq >= 1",
        "time > 2026-01-01T00:00:00Z",
        "len == 22",
        "node == n1",
        "link in [l1, l2]",
        "dir == rx",
        "kind == telemetry",
        'source contains "127."',
        "mid in [0x800, 2049]",
        "truncated == false",
        "garbage != true",
        "category in [event, status]",
        "msg == TLM_A",
        "status != malformed",
        "fc == 1",
        "hdr.sequence > 0",
        "event.type >= error",
        'event.app == "X"',
        "event.id in [1, 2]",
        'event.text contains "oops"',
        "TLM_A.level < -5",
        "mode == SAFE",
        'label == "x"',
        'blob == "00ff00ff"',
        "ratio >= 0.5",
    ],
)
def test_every_documented_name_compiles(codecs: dict[str, Codec], text: str) -> None:
    compiled(codecs, text)


def test_unknown_names_get_a_column_and_close_matches(codecs: dict[str, Codec]) -> None:
    err = error_of(codecs, "msg == TLM_A and conter > 1")
    assert err.code is Code.FLT_NAME and err.column == 18
    assert err.suggestions[:1] == ("counter",) and "counter" in err.user_line()
    assert "^" in err.details and "did you mean" in err.details
    assert "TLM_A" in error_of(codecs, "TLM_C.counter == 1").suggestions
    assert error_of(codecs, "TLM_A.levl == 1").suggestions == ("level",)
    assert error_of(codecs, "hdr.lenght == 1").suggestions == ("length",)
    assert error_of(codecs, "event.typ >= error").code is Code.FLT_NAME
    assert error_of(codecs, "a.b.c == 1").code is Code.FLT_NAME


def test_literal_suggestions(codecs: dict[str, Codec]) -> None:
    err = error_of(codecs, "msg == TLM_AA")
    assert err.code is Code.FLT_TYPE and err.suggestions[0] == "TLM_A"
    assert error_of(codecs, "category == evnt").suggestions[:1] == ("event",)
    assert error_of(codecs, "node == n3").code is Code.FLT_TYPE
    assert error_of(codecs, "link == nope").code is Code.FLT_TYPE
    assert compile_filter("link == anything", codecs).mids is None  # unchecked without link names


def test_reserved_and_unavailable_names(codecs: dict[str, Codec]) -> None:
    assert error_of(codecs, "htime > 1").code is Code.FLT_TYPE  # the header time wants a time
    assert error_of(codecs, "hdr == 1").code is Code.FLT_NAME  # a namespace on its own
    assert error_of(codecs, "event == 1").code is Code.FLT_NAME


def collide(tmp_path: Path, mutate: Any) -> dict[str, Codec]:
    ccdd = json.loads(json.dumps(CCDD_N1))
    mutate(ccdd)
    path = tmp_path / "collide.json"
    path.write_text(json.dumps(ccdd))
    return {
        "n1": Codec(
            load_ccdd(path),
            load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml"),
            load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml"),
        )
    }


def test_reserved_names_always_win_and_a_ccdd_field_is_reached_by_its_message(
    tmp_path: Path,
) -> None:
    def add(ccdd: dict[str, Any]) -> None:
        ccdd["messages"][0]["fields"].append({"name": "mid", "type": "uint8"})
        ccdd["messages"][0]["fields"].append({"name": "hdr", "type": "uint8"})
        ccdd["messages"][0]["fields"].append({"name": "seq", "type": "uint8"})
        ccdd["messages"].append(
            {
                "name": "event",
                "kind": "telemetry",
                "mid": "0x0803",
                "fields": [{"name": "when", "type": "uint8"}],
            }
        )

    codecs = collide(tmp_path, add)
    data = codecs["n1"].encode_telemetry("TLM_A", {"mid": 1, "hdr": 2, "seq": 9}).data
    frame = make_frame(codecs["n1"], N1_TLM, data, seq=4)
    assert frame.mid == 0x0800

    def verdict(text: str) -> bool:
        return matcher_for(codecs, text)[0](frame)

    # The frame's own names mean the frame, whatever the CCDD calls its fields.
    assert verdict("mid == 0x800") and not verdict("mid == 1")
    assert verdict("seq == 4") and not verdict("seq == 9")
    assert compiled(codecs, "mid == 1").mids == frozenset({1})
    # The field is written with its message.
    assert verdict("TLM_A.mid == 1 and TLM_A.hdr == 2 and TLM_A.seq == 9")
    assert compiled(codecs, "TLM_A.mid == 1").mids == frozenset({0x0800})
    # A namespace is a namespace even where a message or field has its name.
    assert compiled(codecs, "event.id == 1").mids == frozenset({0x0802})
    assert compiled(codecs, "hdr.length > 1").mids is None
    err = error_of(codecs, "hdr == 1")
    assert err.code is Code.FLT_NAME and "namespace" in err.summary
    assert "TLM_A.hdr" in err.detail_text()  # the way to the field
    assert compiled(codecs, "when == 1").mids == frozenset({0x0803})  # bare field of `event`
    err = error_of(codecs, "event.when == 1")
    assert err.code is Code.FLT_NAME and "bare field" in err.detail_text()
    assert compiled(codecs, "seq > 1").mids is None


# -- types and operators -------------------------------------------------------------------


@pytest.mark.parametrize(
    "text",
    [
        "counter == 300",  # beyond every uint8 and uint16... TLM_B is 16 bit: see below
        "level == 40000",
        "mid == -1",
        "mid in [5000000000]",
        "hdr.version == 9",
        "counter == 1.5",  # an integer is never equal to a decimal
        'level == "x"',
        "name == 5",
        "name == bare",
        'name < "x"',
        "ratio == bare",
        'ratio contains "1"',
        "mode == NOPE",
        'mode == "SAFE" and counter == bare',
        'blob == "00"',
        'blob == "zz"',
        'blob contains "0011223344"',
        'seq == "5"',
        "time == 5",
        'time == 2026-10-29T12:00:00Z and seq contains "1"',
        "dir == sideways",
        "dir < rx",
        "kind > command",
        "status < ok",
        "truncated == maybe",
        "truncated < true",
        "category < event",
        "msg < TLM_A",
        "event.id == bare",
        "event.app == bare",
        "event.type == 2",
        "event.type == bogus",
        'event.type contains "e"',
        'fc contains "1"',
        "source == 5",
    ],
)
def test_literals_and_operators_that_do_not_fit_are_type_errors(
    codecs: dict[str, Codec], text: str
) -> None:
    if text == "counter == 300":  # TLM_B.counter is a uint16: 300 fits somewhere
        compiled(codecs, text)
        return
    err = error_of(codecs, text)
    assert err.code is Code.FLT_TYPE, (text, err.user_line())
    assert err.column >= 1 and "\n" not in err.user_line()


def test_a_literal_that_only_some_messages_can_take_selects_those(
    codecs: dict[str, Codec],
) -> None:
    # `counter` is a uint8 in TLM_A, a uint16 in TLM_B and text in n2's TLM_A: 300 only fits
    # TLM_B, and a number is not text.
    flt = compiled(codecs, "counter == 300")
    assert flt.mids == frozenset({0x0800, 0x0801, 0x1800})  # candidates are by field, not value
    matcher, _ = matcher_for(codecs, "counter == 300")
    b = make_frame(
        codecs["n1"],
        N1_TLM,
        codecs["n1"].encode_telemetry("TLM_B", {"counter": 300}).data,
    )
    assert matcher(b) and not matcher(hk(codecs, counter=255))
    n2_text = make_frame(
        codecs["n2"],
        N2_TLM,
        codecs["n2"].encode_telemetry("TLM_A", {"counter": "300"}).data,
    )
    assert not matcher(n2_text)
    # A text literal selects the text field only; a number-only label selects nothing else.
    text_matcher, _ = matcher_for(codecs, 'counter == "300"')
    assert text_matcher(n2_text) and not text_matcher(b)


def test_range_only_errors_when_no_message_can_ever_match(codecs: dict[str, Codec]) -> None:
    err = error_of(codecs, "level == 40000")
    assert "outside -32768..32767" in err.summary
    # TLM_A's uint8 can never be 300 but TLM_B's uint16 can: a TLM_A frame is simply "not 300".
    matcher, _ = matcher_for(codecs, "counter != 300 and msg == TLM_A")
    assert matcher(hk(codecs, counter=1))
    ordering, _ = matcher_for(codecs, "level < 40000")
    assert ordering(hk(codecs, level=-3))


# -- candidate MID set ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "mids"),
    [
        ("msg == TLM_A", {0x0800, 0x0801}),  # n1's TLM_A and n2's TLM_A
        ("msg == TLM_B", {0x0801}),
        ("msg in [TLM_B, CMD_X]", {0x0801, 0x1800}),
        ('msg contains "EV"', {0x0802, 0x0810}),
        ("msg != TLM_A", {0x0801, 0x0802, 0x0810, 0x1800, 0x1801}),
        ("mid == 0x800", {0x800}),
        ("mid in [1, 2, 2]", {1, 2}),
        ("mid != 5", None),
        ("mid > 5", None),
        ("category == event", {0x0802, 0x0810}),
        ("category != event", {0x0800, 0x0801}),
        ("event.type >= error", {0x0802, 0x0810}),
        ("fc == 1", {0x1800, 0x1801}),
        ("level < 0", {0x0800, 0x1801}),
        ("TLM_B.ratio > 1", {0x0801}),
        ("status == ok", None),
        ("seq > 1 and dir == rx", None),
        ("hdr.sequence > 0", None),
        ("msg == TLM_A and mid == 0x800", {0x0800}),  # and intersects
        ("msg == TLM_B or msg == TLM_EV", {0x0801, 0x0802}),  # or unites
        ("msg == TLM_B or seq > 5", None),  # an unbounded operand makes the union unbounded
        ("msg == TLM_B and seq > 5", {0x0801}),
        ("not msg == TLM_B", None),  # not makes the set unknown
    ],
)
def test_candidate_mid_set(codecs: dict[str, Codec], text: str, mids: set[int] | None) -> None:
    flt = compiled(codecs, text)
    assert flt.mids == (None if mids is None else frozenset(mids)), text


def test_needs_decode_and_cost_levels(codecs: dict[str, Codec]) -> None:
    for text, cost in [
        ("seq > 1", Cost.FRAME),
        ("msg == TLM_A", Cost.MODEL),
        ("category == event", Cost.MODEL),
        ("status == ok", Cost.CHECK),
        ("counter > 1", Cost.DECODE),
        ("hdr.length > 1", Cost.DECODE),
        ("seq > 1 and counter > 1", Cost.DECODE),
        ("not status == ok", Cost.CHECK),
    ]:
        flt = compiled(codecs, text)
        assert flt.cost is cost and flt.needs_decode == (cost is Cost.DECODE), text


# -- evaluation: what the names mean -------------------------------------------------------


def test_frame_and_model_names(codecs: dict[str, Codec]) -> None:
    frame = hk(codecs, counter=3)
    expect_true = [
        "seq == 1",
        f"len == {frame.orig_len}",
        "node == n1 and link == l1 and dir == rx and kind == telemetry",
        'source == "127.0.0.1:2234"',
        "mid == 0x800",
        "mid in [1, 0x800]",
        "truncated == false and garbage == false",
        "category == housekeeping and msg == TLM_A",
        'msg contains "LM_"',
        "time > 2026-01-01T00:00:00Z",
        "time < 2030-01-01T00:00:00+01:00",
        "status == ok",
        "not status == malformed",
    ]
    expect_false = [
        "seq > 1",
        "node == n2",
        "kind == command",
        "mid == 5",
        "mid < 5 and status == ok",
        "truncated == true",
        "category == event",
        "msg == TLM_B",
        "status in [unknown, malformed]",
        "time > 2030-01-01T00:00:00Z",
    ]
    for text in expect_true:
        assert matcher_for(codecs, text)[0](frame), text
    for text in expect_false:
        assert not matcher_for(codecs, text)[0](frame), text


def test_decode_level_names(codecs: dict[str, Codec]) -> None:
    frame = hk(codecs, counter=3, level=-7, ratio=0.5, name="abc", blob=b"\x00\x01\x02\x03", mode=2)
    for text in [
        "counter == 3 and counter in [1, 3]",
        "TLM_A.level < 0 and level >= -7",
        "ratio == 0.5 and ratio > 0 and ratio >= 0",
        'name == "abc" and name contains "b"',
        'blob == "00010203" and blob contains "0102"',
        "mode == NOMINAL and mode > SAFE and mode == 2",
        "hdr.sequence >= 0 and hdr.length > 0",
        "not TLM_B.counter == 3",
        "not TLM_B.counter == 3 and not event.id == 1",
    ]:
        assert matcher_for(codecs, text)[0](frame), text
    for text in [
        "counter == 4",
        "TLM_B.counter == 3",
        "fc == 1",
        "event.id == 1",
        'name contains "z"',
        'blob contains "ff"',
        "mode == SAFE",
    ]:
        assert not matcher_for(codecs, text)[0](frame), text


def test_events_and_commands(codecs: dict[str, Codec]) -> None:
    tlm = codecs["n1"]
    event = make_frame(
        tlm,
        N1_TLM,
        tlm.encode_telemetry(
            "TLM_EV", {"app": "SAMPLE", "event_id": 3, "event_type": 3, "text": "oops"}
        ).data,
        live=True,
    )
    for text in [
        'event.app == "SAMPLE" and event.id == 3 and event.text contains "oo"',
        "event.type == error and event.type >= warning and event.type < critical",
        "event.type in [error, CRITICAL]",
        'event.app != "OTHER"',
        "category == event",
    ]:
        assert matcher_for(codecs, text)[0](event), text
    assert not matcher_for(codecs, "event.type > error")[0](event)
    command = make_frame(
        tlm, N1_CMD, tlm.encode_command("CMD_Y", {"counter": 9, "mode": "SAFE"}).data
    )
    for text in ["fc == 1 and msg == CMD_Y", "counter == 9 and mode == SAFE", "kind == command"]:
        assert matcher_for(codecs, text)[0](command), text
    assert not matcher_for(codecs, "msg == CMD_X")[0](command)
    assert not matcher_for(codecs, "event.id == 3")[0](command)


def test_a_frame_is_judged_the_same_with_or_without_the_ingest_token(
    codecs: dict[str, Codec],
) -> None:
    data = codecs["n1"].encode_command("CMD_Y", {"counter": 9, "mode": 1}).data
    plain = make_frame(codecs["n1"], N1_CMD, data)
    live = make_frame(codecs["n1"], N1_CMD, data, live=True)
    assert plain.checked is None and live.checked is not None
    for text in ["msg == CMD_Y", "status == ok", "fc == 1", "counter == 9", "msg != CMD_X"]:
        assert matcher_for(codecs, text)[0](plain) == matcher_for(codecs, text)[0](live) is True


def test_malformed_and_unknown_frames(codecs: dict[str, Codec]) -> None:
    good = codecs["n1"].encode_telemetry("TLM_A").data
    short = make_frame(codecs["n1"], N1_TLM, good[:-3])  # payload size wrong, MID known
    unknown = bytearray(good)
    unknown[1] = 0x77  # a MID in no CCDD
    unknown_frame = make_frame(codecs["n1"], N1_TLM, bytes(unknown))
    cap = codecs["n1"].max_packet_bytes
    cut = make_frame(codecs["n1"], N1_TLM, good.ljust(cap, b"\0"), cut=1000)
    garbage = make_frame(codecs["n1"], SERIAL_STREAM, b"\xde\xad\xbe\xef", garbage=True, source="")
    assert short.mid == 0x0800 and unknown_frame.mid is not None and garbage.mid is None
    verdicts = {
        "status == malformed": (True, False, True, True),
        "status == unknown": (False, True, False, False),
        "msg == TLM_A": (True, False, True, False),  # by the MID the frame claims
        "counter >= 0": (False, False, False, False),  # decode-level: false when it cannot decode
        "not counter >= 0": (True, True, True, True),
        "truncated == true": (False, False, True, False),
        "garbage == true": (False, False, False, True),
        "len > 100": (False, False, True, False),  # len is the size on the wire
        "mid == 0x800": (True, False, True, False),
    }
    for text, expected in verdicts.items():
        matcher, _ = matcher_for(codecs, text)
        got = tuple(matcher(f) for f in (short, unknown_frame, cut, garbage))
        assert got == expected, text


def test_multi_node_frames_use_their_own_node_definitions(codecs: dict[str, Codec]) -> None:
    n1_b = make_frame(
        codecs["n1"], N1_TLM, codecs["n1"].encode_telemetry("TLM_B", {"counter": 7}).data
    )
    n2_a = make_frame(
        codecs["n2"], N2_TLM, codecs["n2"].encode_telemetry("TLM_A", {"counter": "hi"}).data
    )
    assert n1_b.mid == n2_a.mid == 0x0801  # same MID, different messages
    by_msg, _ = matcher_for(codecs, "msg == TLM_B")
    assert by_msg(n1_b) and not by_msg(n2_a)
    by_text, _ = matcher_for(codecs, 'counter == "hi"')
    assert by_text(n2_a) and not by_text(n1_b)
    by_node, _ = matcher_for(codecs, "node == n2 and msg == TLM_A")
    assert by_node(n2_a) and not by_node(n1_b)


# -- cost ordering and the decode guarantee ------------------------------------------------


def test_cheap_predicates_run_first_so_a_decode_is_saved(codecs: dict[str, Codec]) -> None:
    matcher, seen = matcher_for(codecs, "counter > 0 and seq > 100")  # written decode-first
    assert not matcher(hk(codecs, counter=5))
    assert seen == []  # seq > 100 decided it
    matcher, seen = matcher_for(codecs, "seq > 100 or counter > 0")
    assert matcher(hk(codecs, counter=5)) and len(seen) == 1
    matcher, seen = matcher_for(codecs, "seq == 1 or counter > 0")  # the cheap side wins
    assert matcher(hk(codecs, counter=0)) and seen == []


def test_a_frame_outside_the_candidate_mids_is_never_decoded(codecs: dict[str, Codec]) -> None:
    matcher, seen = matcher_for(codecs, 'msg == TLM_A and name == "abc"')
    other = make_frame(
        codecs["n1"], N1_TLM, codecs["n1"].encode_telemetry("TLM_EV").data, live=True
    )
    assert not matcher(other) and seen == []
    assert matcher(hk(codecs, name="abc")) and len(seen) == 1
    # Even where the root set is unbounded, a decode-level operand skips frames it cannot match.
    matcher, seen = matcher_for(codecs, 'status == malformed or name == "abc"')
    assert not matcher(other) and seen == []
    garbage = make_frame(codecs["n1"], SERIAL_STREAM, b"\x01\x02", garbage=True)
    assert matcher(garbage) and seen == []


def test_one_decode_serves_every_predicate_and_the_consumer(codecs: dict[str, Codec]) -> None:
    matcher, seen = matcher_for(
        codecs, "counter > 0 and level < 0 and hdr.length > 0 and ratio >= 0"
    )
    frame = hk(codecs, counter=2, level=-1)
    assert matcher(frame)
    again = matcher.decoded(frame)
    assert not hasattr(again, "code") and len(seen) == 1 and matcher.decodes == 1
    other = hk(codecs, counter=0)
    assert matcher.decoded(other) is not again  # a different frame decodes afresh
    assert len(seen) == 2


def test_a_frame_the_filter_did_not_decode_is_decoded_for_the_consumer(
    codecs: dict[str, Codec],
) -> None:
    matcher, seen = matcher_for(codecs, "seq == 1")
    frame = hk(codecs)
    assert matcher(frame) and seen == []
    assert decode_frame(codecs, frame) == matcher.decoded(frame) and len(seen) == 1


# -- errors --------------------------------------------------------------------------------


def test_errors_are_one_sanitized_line_with_the_expression_and_a_caret_below(
    codecs: dict[str, Codec],
) -> None:
    err = error_of(codecs, "msg == TLM_A\x1b[31m and x")
    assert err.code in (Code.FLT_SYNTAX, Code.FLT_NAME, Code.FLT_TYPE)
    assert "\x1b" not in err.user_line() and "\x1b" not in err.detail_text()
    assert "\n" not in err.user_line() and err.exit_code == 2
    long = "x == 1 and " * 3 + "nope == 1" + " and x == 1" * 3
    err = error_of(codecs, "msg == TLM_A and " * 1 + long)
    first, caret = err.details.splitlines()[:2]
    assert caret.strip() == "^" and len(first) >= len(caret)


def test_suggestions_are_bounded_whatever_the_pool_or_the_query() -> None:
    pool = [f"name_{i:06d}_" + "x" * 200 for i in range(100_000)]
    start = time.perf_counter()
    found = suggest("name_000003", pool)
    assert time.perf_counter() - start < 2.0
    assert len(found) <= 3 and all(len(f) <= 64 for f in found)
    assert suggest("q" * 100_000, pool) == () or len(suggest("q" * 100_000, pool)) <= 3
    # The pool is narrowed by similarity before it is cut, so a name that sorts late is found.
    assert SUGGEST_POOL_NAMES == 256
    late = [f"a{i:04d}" for i in range(300)] + ["zebra_crossing"]
    assert suggest("zebra_crossin", late) == ("zebra_crossing",)
    crowd = [f"hk_{i:05d}" for i in range(20_000)] + ["zz_counter"]  # many look-alikes sort first
    start = time.perf_counter()
    assert suggest("zz_countr", crowd) == ("zz_counter",)
    assert time.perf_counter() - start < 1.0
    twins = ["abcdefghijkl"[i % 12 :] + "abcdefghijkl"[: i % 12] for i in range(100_000)]
    start = time.perf_counter()
    assert len(suggest("abcdefghijkl", twins)) <= 3  # equal letters, all pass the cheap checks
    assert time.perf_counter() - start < 2.0
    assert suggest("zebra_crossin", ["zebra_crossing", "apple"]) == ("zebra_crossing",)
    assert suggest("", ["a"]) == ()


def test_a_filter_is_compiled_before_any_packet_is_read_and_is_reusable(
    codecs: dict[str, Codec],
) -> None:
    flt = compiled(codecs, "counter > 1")
    frames = [hk(codecs, counter=n) for n in (0, 5)]
    first = [flt.bind(count_decodes(codecs)[0], lambda n: codecs[n])(f) for f in frames]
    second = [flt.bind(count_decodes(codecs)[0], lambda n: codecs[n])(f) for f in frames]
    assert first == second == [False, True]  # a Filter holds no per-frame state


def test_corpus_filters_fail_with_a_filter_error_only(codecs: dict[str, Codec]) -> None:
    files = sorted(CORPUS.glob("flt-*.txt"))
    assert len(files) >= 8
    for path in files:
        text = path.read_bytes().decode("utf-8", errors="surrogateescape")
        expected = f"IB-FLT-{path.name.split('-')[1]}"  # flt-<number>-<what>.txt
        with pytest.raises(FilterError) as info:
            compile_filter(text, codecs, links=LINKS)
        assert info.value.code.value == expected, (path.name, info.value.user_line())
        assert "\n" not in info.value.user_line()


# -- garbage frames are malformed, whatever their bytes say ---------------------------------


def garbage_frame(codecs: dict[str, Codec]) -> Frame:
    """Serial bytes skipped while resynchronising that happen to spell a valid TLM_A packet."""
    data = (CORPUS / "frame-garbage-flagged-valid-looking-tlm.bin").read_bytes()
    assert not isinstance(codecs["n1"].decode(MessageKind.TELEMETRY, data), DecodeError)
    return make_frame(codecs["n1"], SERIAL_STREAM, data, garbage=True)


def test_a_garbage_frame_is_malformed_and_never_decoded_even_if_its_bytes_decode(
    codecs: dict[str, Codec],
) -> None:
    frame = garbage_frame(codecs)
    verdicts = {
        "status == malformed": True,
        "status == ok": False,
        "status == unknown": False,
        "counter == 5": False,
        "counter >= 0": False,
        "hdr.length > 0": False,
        "hdr.length >= 0 or garbage == true": True,  # only the frame flag can match
        "msg == TLM_A": False,  # a garbage frame has no MID, so no message
        "garbage == true": True,
        "not counter == 5": True,
    }
    for text, expected in verdicts.items():
        matcher, seen = matcher_for(codecs, text)
        assert matcher(frame) is expected, text
        assert seen == [], text  # the decoder is never asked for a garbage frame
        assert matcher.decodes == 0
    matcher, seen = matcher_for(codecs, "seq == 1")
    assert isinstance(matcher.resolved(frame), DecodeError)
    assert isinstance(matcher.decoded(frame), DecodeError) and seen == []
    assert matcher.decodes == 0


def test_the_frame_decoder_treats_a_garbage_frame_as_malformed() -> None:
    from types import SimpleNamespace

    from ironbird.engine import FrameDecoder

    stub = SimpleNamespace(rejected=[])
    stub.reject = lambda frame, error: stub.rejected.append(error)  # type: ignore[attr-defined]
    stub._codec_for = lambda node: pytest.fail("a garbage frame needs no codec")  # type: ignore[attr-defined]
    data = (CORPUS / "frame-garbage-flagged-valid-looking-tlm.bin").read_bytes()
    frame = Frame(1, 1, SERIAL_STREAM, "", None, FrameFlag.GARBAGE, len(data), data)
    quiet = FrameDecoder(stub._codec_for, stub.reject, report_checked=False)
    assert isinstance(quiet.decode(frame), DecodeError) and stub.rejected == []
    loud = FrameDecoder(stub._codec_for, stub.reject, report_checked=True)
    result = loud.decode(frame)
    assert isinstance(result, DecodeError) and stub.rejected == [result]


# -- `in` is an `or` of `==`, `!=` is `not ==` ------------------------------------------------


def tlm(codecs: dict[str, Codec], name: str, **values: Any) -> Frame:
    return make_frame(
        codecs["n1"], N1_TLM, codecs["n1"].encode_telemetry(name, values).data, live=True
    )


def verdicts_of(codecs: dict[str, Codec], text: str, frames: list[Frame]) -> list[bool]:
    matcher, _ = matcher_for(codecs, text)
    return [matcher(f) for f in frames]


def test_in_equals_an_or_of_equalities_for_labels_only_some_messages_have(
    codecs: dict[str, Codec],
) -> None:
    # TLM_A.mode and CMD_Y.mode take SAFE and NOMINAL; TLM_B.mode takes SAFE and SCIENCE.
    frames = [
        tlm(codecs, "TLM_A", mode=1),
        tlm(codecs, "TLM_A", mode=2),
        tlm(codecs, "TLM_B", mode=3),
        tlm(codecs, "TLM_B", mode=1),
        tlm(codecs, "TLM_EV"),
    ]
    expected = [True, False, True, True, False]
    assert verdicts_of(codecs, "mode in [SAFE, SCIENCE]", frames) == expected
    assert verdicts_of(codecs, "mode == SAFE or mode == SCIENCE", frames) == expected
    other = verdicts_of(codecs, "mode in [NOMINAL, SCIENCE]", frames)
    assert other == [False, True, True, False, False]
    assert verdicts_of(codecs, 'mode in [SAFE, "SCIENCE"]', frames) == expected
    both = compiled(codecs, "mode in [SAFE, SCIENCE]")
    assert both.mids == frozenset({0x0800, 0x0801, 0x1800})
    # A message is a candidate only when some item fits it.
    assert compiled(codecs, "mode in [SCIENCE, SCIENCE]").mids == frozenset({0x0801})
    # Items of other kinds: `counter` is a number in n1 and text in n2's TLM_A.
    n2_a = make_frame(
        codecs["n2"], N2_TLM, codecs["n2"].encode_telemetry("TLM_A", {"counter": "hi"}).data
    )
    mixed, _ = matcher_for(codecs, 'counter in [5, "hi"]')
    assert mixed(n2_a) and mixed(tlm(codecs, "TLM_A", counter=5))
    assert not mixed(tlm(codecs, "TLM_A", counter=6)) and not mixed(tlm(codecs, "TLM_B", counter=7))
    # An item no message can take is still a typo, as `== NOPE` would be.
    err = error_of(codecs, "mode in [SAFE, NOPE]")
    assert err.code is Code.FLT_TYPE and err.column == len("mode in [SAFE, ") + 1


def test_not_equal_is_the_negation_of_equal_for_a_value_the_field_cannot_take(
    codecs: dict[str, Codec],
) -> None:
    frames = [
        tlm(codecs, "TLM_A", mode=1),
        tlm(codecs, "TLM_A", mode=2),
        tlm(codecs, "TLM_B", mode=3),
        tlm(codecs, "TLM_B", mode=1),
    ]
    # SCIENCE is not a label of TLM_A.mode: there `mode == SCIENCE` is false, so `!=` is true.
    assert verdicts_of(codecs, "mode == SCIENCE", frames) == [False, False, True, False]
    assert verdicts_of(codecs, "mode != SCIENCE", frames) == [True, True, False, True]
    for label in ("SCIENCE", "NOMINAL", "SAFE"):  # every frame above has a `mode`
        assert verdicts_of(codecs, f"mode != {label}", frames) == verdicts_of(
            codecs, f"not mode == {label}", frames
        ), label
    # Text and numbers: a number is never the text "hi", a text never a number.
    assert verdicts_of(codecs, 'counter != "hi"', [tlm(codecs, "TLM_A", counter=5)]) == [True]
    n2_a = make_frame(
        codecs["n2"], N2_TLM, codecs["n2"].encode_telemetry("TLM_A", {"counter": "hi"}).data
    )
    n2_b = make_frame(
        codecs["n2"], N2_TLM, codecs["n2"].encode_telemetry("TLM_A", {"counter": "ho"}).data
    )
    matcher, _ = matcher_for(codecs, 'counter != "hi"')
    assert [matcher(n2_a), matcher(n2_b)] == [False, True]
    # A frame without the field still has nothing to compare: false, as documented.
    assert verdicts_of(codecs, "mode != SCIENCE", [tlm(codecs, "TLM_EV")]) == [False]


# -- float literals ---------------------------------------------------------------------------


def test_a_float32_field_is_compared_at_its_own_width(codecs: dict[str, Codec]) -> None:
    frame = tlm(codecs, "TLM_A", ratio=0.1)  # stored as 0.10000000149011612
    for text, expected in {
        "ratio == 0.1": True,
        "ratio != 0.1": False,
        "ratio <= 0.1": True,
        "ratio >= 0.1": True,
        "ratio < 0.1": False,
        "ratio > 0.1": False,
        "ratio in [0.2, 0.1]": True,
        "ratio > 0.09999999": True,
        "ratio == 0.100000001": True,  # the same float32
        "ratio == 0.1000001": False,  # the next ones up
        "TLM_A.ratio == 0.1": True,
    }.items():
        assert verdicts_of(codecs, text, [frame]) == [expected], text
    # A float64 field keeps its own width.
    wide = tlm(codecs, "TLM_B", ratio=0.1)
    assert verdicts_of(codecs, "TLM_B.ratio == 0.1", [wide]) == [True]
    assert verdicts_of(codecs, "TLM_B.ratio == 0.10000000149011612", [wide]) == [False]
    # Beyond the float32 range the literal is infinite; that must compile and compare, not raise.
    huge = tlm(codecs, "TLM_A", ratio=1.0)
    assert verdicts_of(codecs, "TLM_A.ratio < 1" + "0" * 39, [huge]) == [True]
    assert verdicts_of(codecs, "TLM_A.ratio > -1" + "0" * 38, [huge]) == [True]
    assert verdicts_of(codecs, "TLM_A.ratio == 1" + "0" * 39, [huge]) == [False]


def test_decimals_compare_exactly_with_integer_fields(codecs: dict[str, Codec]) -> None:
    top = 2**64 - 1
    frame = tlm(codecs, "TLM_B", big=top)
    for text, expected in {
        f"big > {top - 1}.5": True,
        f"big > {top}.5": False,
        f"big >= {top}.0": True,
        f"big > {top}.0": False,
        f"big < {top}.5": True,
        f"big <= {top - 1}.99999": False,
        f"big <= {top}.00000": True,
        f"big >= {top}.5": False,
        "big > -0.5": True,
    }.items():
        assert verdicts_of(codecs, text, [frame]) == [expected], text
    zero = tlm(codecs, "TLM_B", big=0)
    assert verdicts_of(codecs, "big < 0.1", [zero]) == [True]
    assert verdicts_of(codecs, "big > -0.1", [zero]) == [True]
    assert verdicts_of(codecs, "big >= 0.1", [zero]) == [False]
    # Frame-level integers too.
    matcher, _ = matcher_for(codecs, "seq > 0.5 and seq < 1.5 and len >= 35.5")
    assert matcher(tlm(codecs, "TLM_A")) is True


# -- event.type agrees with --min-type --------------------------------------------------------


def test_event_type_lower_bounds_agree_with_min_type_including_unranked_values(
    codecs: dict[str, Codec],
) -> None:
    # TLM_EV.event_type maps 1..4 to DEBUG, INFO, ERROR, CRITICAL; 7 maps to nothing.
    ranked: dict[int, EventSeverity | None] = {
        1: EventSeverity.DEBUG,
        2: EventSeverity.INFO,
        3: EventSeverity.ERROR,
        4: EventSeverity.CRITICAL,
        7: None,
    }
    frames = [tlm(codecs, "TLM_EV", event_type=value) for value in ranked]
    floors = {
        "debug": EventSeverity.DEBUG,
        "info": EventSeverity.INFO,
        "warning": EventSeverity.WARNING,
        "error": EventSeverity.ERROR,
        "critical": EventSeverity.CRITICAL,
    }
    for label, floor in floors.items():
        shown = [meets_floor(severity, floor) for severity in ranked.values()]  # --min-type
        assert verdicts_of(codecs, f"event.type >= {label}", frames) == shown, label
    # An event whose type has no severity is never hidden by a lower bound, and is not selected
    # by anything else; `!=` is `not ==`.
    assert verdicts_of(codecs, "event.type > warning", frames) == [False, False, True, True, True]
    assert verdicts_of(codecs, "event.type == error", frames) == [False, False, True, False, False]
    assert verdicts_of(codecs, "event.type != error", frames) == [True, True, False, True, True]
    assert verdicts_of(codecs, "event.type < critical", frames) == [True, True, True, False, False]
    assert verdicts_of(codecs, "event.type <= error", frames) == [True, True, True, False, False]
    assert verdicts_of(codecs, "event.type in [error, critical]", frames) == [
        False,
        False,
        True,
        True,
        False,
    ]


# -- a filter that can never match is refused -------------------------------------------------


@pytest.mark.parametrize(
    "text",
    [
        "msg == TLM_B and mid == 5",
        "mid == 1 and mid == 2",
        "msg == TLM_B and msg == TLM_EV",
        "category == event and TLM_B.counter > 1",
        "seq > 1 and (msg == TLM_B or msg == TLM_EV) and mid in [1, 2]",
        "not (msg == TLM_B and mid == 5)",  # what can never match inside a `not` is as wrong
    ],
)
def test_a_filter_whose_candidate_mids_are_empty_is_refused(
    codecs: dict[str, Codec], text: str
) -> None:
    err = error_of(codecs, text)
    assert err.code is Code.FLT_TYPE and "can never match" in err.summary
    assert 1 <= err.column <= len(text)


def test_overlapping_candidates_are_fine(codecs: dict[str, Codec]) -> None:
    assert compiled(codecs, "msg == TLM_A and mid == 0x800").mids == frozenset({0x800})
    assert compiled(codecs, "(msg == TLM_B or msg == TLM_EV) and mid in [0x801, 5]").mids == (
        frozenset({0x801})
    )


# -- error echo: display width and right-to-left text ------------------------------------------


def test_the_caret_is_aligned_by_display_width(codecs: dict[str, Codec]) -> None:
    prefix = 'source == "\u65e5\u672c\u8a9e" and '  # three double-width characters
    err = error_of(codecs, prefix + "nope == 1")
    shown, caret = err.details.splitlines()[:2]
    assert shown.index("nope") == 2 + len(prefix)
    assert caret.index("^") == 2 + len(prefix) + 3  # each wide character takes two columns
    assert display_width(prefix) == len(prefix) + 3
    # A combining mark takes none.
    combining = 'source == "e\u0301" and '
    err = error_of(codecs, combining + "nope == 1")
    assert err.details.splitlines()[1].index("^") == 2 + len(combining) - 1
    assert (
        display_width("a\u0301b") == 2 and display_width("\uff21") == 2 and display_width("") == 0
    )


def test_right_to_left_and_bidi_text_is_escaped_in_the_echo(codecs: dict[str, Codec]) -> None:
    hebrew, arabic, arabic_digits = "\u05e9\u05dc\u05d5\u05dd", "\u0627\u0644\u0633", "\u0663\u0664"
    for word in (hebrew, arabic, arabic_digits):
        prefix = f'source == "{word}" and '
        err = error_of(codecs, prefix + "nope == 1")
        text = err.detail_text() + err.user_line()
        assert word[0] not in text and all(ord(ch) < 0x590 for ch in text), word
        shown, caret = err.details.splitlines()[:2]
        assert shown.index("nope") == caret.index("^") == 2 + len(prefix)
    assert visible("a\u05d0b\u202ec\u200fd\u2066") == "a?b?c?d?"
    assert visible("caf\u00e9 \u65e5") == "caf\u00e9 \u65e5"


# -- node names and aliases --------------------------------------------------------------------


def test_node_accepts_a_nodes_alias_and_a_name_wins_over_an_alias(codecs: dict[str, Codec]) -> None:
    aliases = {"flight": "n1", "n2": "n1", "elsewhere": "n9"}
    flt = compile_filter("node == flight", codecs, aliases=aliases)
    matcher = flt.bind(count_decodes(codecs)[0], lambda n: codecs[n])
    n1_frame = hk(codecs)
    n2_data = codecs["n2"].encode_telemetry("TLM_A", {"counter": "hi"}).data
    n2_frame = make_frame(codecs["n2"], N2_TLM, n2_data)
    assert matcher(n1_frame) and not matcher(n2_frame)
    named = compile_filter("node == n2", codecs, aliases=aliases).bind(
        count_decodes(codecs)[0], lambda n: codecs[n]
    )
    assert named(n2_frame) and not named(n1_frame)  # `n2` is a node; the alias spelled n2 loses
    both = compile_filter("node in [flight, n2]", codecs, aliases=aliases).bind(
        count_decodes(codecs)[0], lambda n: codecs[n]
    )
    assert both(n1_frame) and both(n2_frame)
    other = compile_filter("node != flight", codecs, aliases=aliases).bind(
        count_decodes(codecs)[0], lambda n: codecs[n]
    )
    assert other(n2_frame) and not other(n1_frame)
    with pytest.raises(FilterError) as info:  # an alias of a node that is not in use
        compile_filter("node == elsewhere", codecs, aliases=aliases)
    assert info.value.code is Code.FLT_TYPE


@pytest.mark.parametrize(
    ("text", "nodes"),
    [
        ("node == n1", {"n1"}),
        ("node in [n1, n2]", {"n1", "n2"}),
        ("node != n1", {"n2"}),
        ("node == n1 and seq > 3", {"n1"}),
        ("node == n1 and node == n2", set()),
        ("node == n1 or node == n2", {"n1", "n2"}),
        ("(node == n1 or node == n2) and node != n1", {"n2"}),
        ("node == n1 or seq > 3", None),
        ("not node == n1", None),
        ("seq > 3", None),
        ('node contains "n"', None),
    ],
)
def test_the_candidate_nodes_of_a_filter(
    codecs: dict[str, Codec], text: str, nodes: set[str] | None
) -> None:
    found = compiled(codecs, text).nodes
    assert found == (None if nodes is None else frozenset(nodes))
