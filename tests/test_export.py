"""``ib export`` as a library: the cells, the tables, the formats, and what is exported equals what
``ib replay`` shows. Output-file rules and the command line are in test_export_cli.py; damaged and
hostile recordings in test_export_malformed.py."""

from __future__ import annotations

import csv
import itertools
import json
import math
from pathlib import Path
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from exportutil import (
    EVENT_MID,
    WIDE_MID,
    NullDestination,
    export_in_process,
    hk_and_status,
    inert,
    packet,
    payload_size,
    read_dicts,
    read_table,
    wide_codec,
    wide_frames,
    write_wide,
)
from ironbird.ccdd.model import FieldDef, TypeClass
from ironbird.codec import Codec
from ironbird.errors import Code, DecodeError, IronbirdError
from ironbird.frame import FrameFlag
from ironbird.ledger import format_ts
from ironbird.recording.export import (
    FIXED_COLUMNS,
    FORMULA_LEAD,
    FORMULA_PREFIX,
    MALFORMED_COLUMNS,
    ExportFormat,
    UtcFormatter,
    csv_line,
    safe_cell,
    stem_for,
)
from ironbird.recording.replay import TimeSpec
from ironbird.sanitize import clean
from recutil import RX, SERIAL, frame
from replayutil import (
    STEP_NS,
    T0,
    codec_status_frames,
    hk_frames,
    replay_in_process,
    write_recording,
)

FUZZ = settings(
    max_examples=60, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture]
)
INJECTIONS = ["=1+1", "+1", "-1", "@SUM(A1)", "=cmd|' /C calc'!A0", "\t=1", "\r=1", "-2+3"]


# -- cells ----


@pytest.mark.parametrize("lead", list("=+-@"))
def test_a_cell_that_leads_like_a_formula_is_prefixed(lead: str) -> None:
    assert safe_cell(f"{lead}1+1") == f"'{lead}1+1"
    assert safe_cell(lead) == f"'{lead}"


def test_tab_cr_and_lf_lead_cells_are_escaped_not_carried() -> None:
    for text in ("\t=1", "\r=1", "\n=1"):
        cell = safe_cell(text)
        assert cell[0] not in FORMULA_LEAD and "=1" in cell
        assert not any(ord(ch) < 32 for ch in cell)


def test_plain_text_is_untouched_and_a_leading_quote_is_not_doubled() -> None:
    for text in ("", "SAFE", "a=b", "x-y", "'=x", "50%", "3.5e3"):
        assert safe_cell(text) == text
    assert safe_cell("'=x") == "'=x"


def test_control_characters_and_escape_sequences_become_visible_escapes() -> None:
    cell = safe_cell("=a\nb\x1b[31mred\x00\x7f")
    assert cell == "'=a\\x0ab\\x1b[31mred\\x00\\x7f"
    assert safe_cell("bell\x07") == "bell\\x07"
    assert safe_cell(chr(0x202E) + "evil") == "\\u202eevil"  # a bidi override is not text


@given(st.text())
def test_a_string_cell_is_one_inert_text_that_no_spreadsheet_reads_as_a_formula(text: str) -> None:
    cell = safe_cell(text)
    assert clean(cell, 1 << 21) == cell  # nothing unsafe is left
    assert "\n" not in cell and "\r" not in cell and "\x1b" not in cell
    if cell and cell[0] in FORMULA_LEAD:
        raise AssertionError(f"{cell!r} leads like a formula")
    assert inert(cell), cell
    body = cell.removeprefix("'") if cell.startswith("'") and not text.startswith("'") else cell
    assert body == clean(text, 1 << 20)


SPACES = [
    " ",
    "  ",
    "\t",
    "\xa0",  # NBSP
    "\u3000",  # ideographic space
    "\u2003",  # em space
    "\u2028",  # line separator
    "\x85",  # next line
    "\x1f",
    " \xa0\u3000 ",
]
LEADS_UNDER_TEST = ["=", "+", "-", "@", "\uff1d", "\uff0b", "\uff0d", "\uff20"]


@pytest.mark.parametrize("lead", LEADS_UNDER_TEST)
@pytest.mark.parametrize("space", ["", *SPACES])
def test_a_formula_lead_behind_whitespace_a_spreadsheet_may_trim_is_prefixed(
    space: str, lead: str
) -> None:
    for text in (f"{space}{lead}1+1", f"{space}{lead}cmd|' /C calc'!A0"):
        cell = safe_cell(text)
        assert inert(cell), (text, cell)
        if space in ("", " ", "  "):  # survives escaping, so the lead is what the prefix guards
            assert cell == FORMULA_PREFIX + text


@pytest.mark.parametrize("lead", ["\uff1d", "\uff0b", "\uff0d", "\uff20"])
def test_fullwidth_formula_leads_are_prefixed(lead: str) -> None:
    assert safe_cell(f"{lead}1") == f"'{lead}1"


def test_whitespace_alone_or_before_ordinary_text_is_left_as_it_is() -> None:
    for text in (" ", "\u3000", " a=b", "\xa0x", "  50%", " 'x"):
        assert safe_cell(text) == clean(text, 1 << 20)


@given(
    st.lists(st.sampled_from([*SPACES, "\u00a0", "\u2009", "\u205f"]), max_size=4).map("".join),
    st.sampled_from([*LEADS_UNDER_TEST, "\t", "\r", "\n"]),
    st.text(),
)
def test_no_leading_whitespace_hides_a_formula_lead(space: str, lead: str, rest: str) -> None:
    cell = safe_cell(f"{space}{lead}{rest}")
    assert inert(cell), cell


@given(st.text(alphabet="".join(SPACES + LEADS_UNDER_TEST) + "a1", max_size=8))
def test_a_string_of_spaces_and_leads_is_always_inert(text: str) -> None:
    assert inert(safe_cell(text))


def test_hex_cells_carry_a_0x_prefix_so_a_spreadsheet_keeps_them_as_text(tmp_path: Path) -> None:
    """``00123456`` would lose its zeros and ``1e300000`` would become a number (infinity)."""
    codec = wide_codec()
    msg = codec.model.by_name["WIDE"]
    blob = msg.field("blob")
    assert blob is not None
    payloads = []
    for digits in ("00123456", "1e300000", "12345678", "deadbeef"):
        raw = bytearray(msg.payload_size)
        raw[blob.offset : blob.offset + 4] = bytes.fromhex(digits)
        payloads.append(bytes(raw))
    write_wide(tmp_path / "r.ibrec", wide_frames(codec, payloads))
    export_in_process(tmp_path / "r.ibrec", tmp_path / "out")
    rows = read_dicts(tmp_path / "out" / "WIDE.csv")
    assert [r["blob"] for r in rows] == ["0x00123456", "0x1e300000", "0x12345678", "0xdeadbeef"]
    assert all(not _parses_as_number(r["blob"]) for r in rows)
    text, _ = export_in_process(tmp_path / "r.ibrec", tmp_path / "t.txt", ExportFormat.TEXT)
    assert text.rows == 4  # text and jsonl are replay's lines, unchanged by the prefix


def _parses_as_number(cell: str) -> bool:
    try:
        float(cell)
    except ValueError:
        return False
    return True


@pytest.mark.parametrize(
    "reason", ["=cmd|' /C calc'!A0", " =1+1", "\uff1d1+1", "@SUM(1)", "-2+3", "+1\nx\x1b[31mred"]
)
def test_the_reason_of_a_malformed_row_exports_inert(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, reason: str
) -> None:
    """The reason is text a decoder produced from packet content, so it is a cell like any other."""
    monkeypatch.setattr(
        Codec, "decode", lambda self, kind, data: DecodeError(Code.DEC_PAYLOAD, 3, reason)
    )
    hk_and_status(tmp_path / "r.ibrec", 2, 0)
    export_in_process(tmp_path / "r.ibrec", tmp_path / "out")
    rows = read_dicts(tmp_path / "out" / "malformed.csv")
    assert len(rows) == 2
    for row in rows:
        assert inert(row["reason"]) and "\n" not in row["reason"] and "\x1b" not in row["reason"]
        assert row["reason"].lstrip("' ") != "" and all(inert(cell) for cell in row.values())
        if reason[0] in "=@-+":
            assert row["reason"].startswith("'" + reason[0]), row["reason"]


@given(st.lists(st.text(), min_size=1, max_size=6), st.integers(), st.floats(allow_nan=False))
def test_a_row_of_safe_cells_is_one_csv_line_that_parses_back(
    texts: list[str], number: int, real: float
) -> None:
    cells: list[object] = [*(safe_cell(t) for t in texts), number, real]
    line = csv_line(cells)
    assert line.endswith("\n") and line.count("\n") == 1
    (row,) = csv.reader([line])
    assert row[: len(texts)] == [safe_cell(t) for t in texts]
    assert int(row[-2]) == number and float(row[-1]) == real


@given(st.integers(min_value=0, max_value=2**64 - 1))
def test_the_cached_timestamp_formatter_equals_the_ledgers(time_ns: int) -> None:
    assert UtcFormatter()(time_ns) == format_ts(time_ns)


def test_the_formatter_follows_the_clock_across_seconds_and_back() -> None:
    utc = UtcFormatter()
    for ns in (0, 999_999_999, 1_000_000_000, 5, 1_700_000_000_123_456_789, 4):
        assert utc(ns) == format_ts(ns)


def test_a_table_stem_never_equals_a_message_name() -> None:
    assert stem_for("HK", 1) == "HK" and stem_for("HK", 2) == "HK.2"
    assert stem_for("malformed", 1) == "malformed.1"  # malformed.csv is the malformed table
    assert all("." not in name for name in ("HK", "MOCK_STATUS"))  # names have no dot (CCDD)


# -- tables: the round trip -----


def decode_all(codec: Any, frames: list[Any]) -> list[Any]:
    out = []
    for item in frames:
        decoded = codec.decode(item.stream.kind, item.data)
        assert not hasattr(decoded, "reason"), decoded
        out.append(decoded)
    return out


def check_cell(field: FieldDef, cell: str, value: object, *, raw_enums: bool = False) -> None:
    match field.type.type_class:
        case TypeClass.INT:
            label = None if raw_enums else field.enum_label(value)
            assert cell == (label if label is not None else str(value))
        case TypeClass.FLOAT:
            assert isinstance(value, float)
            if math.isnan(value):
                assert cell == "nan"
            elif math.isinf(value):
                assert cell == ("inf" if value > 0 else "'-inf")
            else:
                assert float(cell) == value and cell == repr(value)
        case TypeClass.CHAR:
            assert isinstance(value, str)
            text = clean(value, 1 << 20)
            assert cell in (text, "'" + text)
            assert cell == text or text[:1] in "=+-@\t\r\n"
        case TypeClass.BYTES:
            assert isinstance(value, bytes) and cell == "0x" + value.hex()


WIDE_COLUMNS = [
    *FIXED_COLUMNS,
    "i8",
    "u16",
    "i32",
    "u64",
    "i64",
    "f32",
    "f64",
    "mode",
    "name",
    "blob",
    "WIDE.seq",  # the payload field called seq clashes with the fixed column
]
_COUNTER = itertools.count()


@FUZZ
@given(data=st.data())
def test_every_field_type_round_trips_through_a_csv_table(
    data: st.DataObject, tmp_path: Path
) -> None:
    codec = wide_codec()
    size = payload_size(codec, "WIDE")
    payloads = data.draw(st.lists(st.binary(min_size=size, max_size=size), min_size=1, max_size=5))
    frames = wide_frames(codec, payloads)
    work = tmp_path / f"w{next(_COUNTER)}"
    work.mkdir()
    write_wide(work / "r.ibrec", frames)
    result, _ = export_in_process(work / "r.ibrec", work / "out")
    assert result.decoded == len(frames) and result.malformed == 0
    rows = read_dicts(work / "out" / "WIDE.csv")
    assert list(rows[0]) == WIDE_COLUMNS and len(rows) == len(frames)
    msg = codec.model.by_name["WIDE"]
    for source, row, wire in zip(frames, rows, decode_all(codec, frames), strict=True):
        assert row["rx_time_utc"] == format_ts(source.time_ns)
        assert int(row["rx_time_ns"]) == source.time_ns and int(row["seq"]) == source.seq
        assert (row["node"], row["link"], row["mid"]) == ("fc", "ground-fc", f"0x{WIDE_MID:04X}")
        names = ["i8", "u16", "i32", "u64", "i64", "f32", "f64", "mode", "name", "blob", "WIDE.seq"]
        for column, field, value in zip(names, msg.fields, wire.values, strict=True):
            check_cell(field, row[column], value)
    header, table = read_table(work / "out" / "malformed.csv")
    assert header == list(MALFORMED_COLUMNS) and table == []


def test_enum_labels_unknown_enum_values_and_raw_enums(tmp_path: Path) -> None:
    codec = wide_codec()
    size = payload_size(codec, "WIDE")
    mode_offset = codec.model.by_name["WIDE"].field("mode").offset  # type: ignore[union-attr]
    payloads = []
    for value in (0, 1, 2, 7):
        raw = bytearray(size)
        raw[mode_offset] = value
        payloads.append(bytes(raw))
    write_wide(tmp_path / "r.ibrec", wide_frames(codec, payloads))
    export_in_process(tmp_path / "r.ibrec", tmp_path / "labels")
    export_in_process(tmp_path / "r.ibrec", tmp_path / "raw", raw_enums=True)
    assert [r["mode"] for r in read_dicts(tmp_path / "labels" / "WIDE.csv")] == [
        "OFF",
        "ON",
        "AUTO",
        "7",
    ]
    assert [r["mode"] for r in read_dicts(tmp_path / "raw" / "WIDE.csv")] == ["0", "1", "2", "7"]


def test_non_finite_floats_are_text_cells_and_the_negative_infinity_is_neutralised(
    tmp_path: Path,
) -> None:
    import struct

    codec = wide_codec()
    msg = codec.model.by_name["WIDE"]
    f32, f64 = msg.field("f32"), msg.field("f64")
    assert f32 is not None and f64 is not None
    values = [
        (math.inf, -math.inf),
        (math.nan, 0.0),
        (-0.0, 1.5e300),
        (3.4028234663852886e38, 5e-324),
    ]
    payloads = []
    for a, b in values:
        raw = bytearray(msg.payload_size)
        struct.pack_into("<f", raw, f32.offset, a)
        struct.pack_into("<d", raw, f64.offset, b)
        payloads.append(bytes(raw))
    write_wide(tmp_path / "r.ibrec", wide_frames(codec, payloads))
    export_in_process(tmp_path / "r.ibrec", tmp_path / "out")
    rows = read_dicts(tmp_path / "out" / "WIDE.csv")
    assert [(r["f32"], r["f64"]) for r in rows] == [
        ("inf", "'-inf"),
        ("nan", "0.0"),
        ("-0.0", "1.5e+300"),
        ("3.4028234663852886e+38", "5e-324"),
    ]


def test_the_header_fields_come_after_the_fixed_columns_and_before_the_payload(
    tmp_path: Path,
) -> None:
    hk_and_status(tmp_path / "r.ibrec", 3, 2)
    export_in_process(tmp_path / "r.ibrec", tmp_path / "out", header_fields=True)
    export_in_process(tmp_path / "r.ibrec", tmp_path / "plain")
    with_header = read_dicts(tmp_path / "out" / "MOCK_HK.csv")
    plain = read_dicts(tmp_path / "plain" / "MOCK_HK.csv")
    columns = list(with_header[0])
    first_payload = columns.index("cmd_counter")
    assert columns[:6] == list(FIXED_COLUMNS) and first_payload > 6
    assert all(c.startswith("hdr.") for c in columns[6:first_payload])
    assert {"hdr.apid", "hdr.sequence", "hdr.length"} <= set(columns)
    assert [r["hdr.apid"] for r in with_header] == [str(0x890 & 0x7FF)] * 3
    assert list(plain[0]) == [c for c in columns if not c.startswith("hdr.")]


def test_one_message_makes_one_table_and_several_make_a_directory(tmp_path: Path) -> None:
    hk_and_status(tmp_path / "r.ibrec", 4, 3)
    single, _ = export_in_process(tmp_path / "r.ibrec", tmp_path / "one.csv", messages=["MOCK_HK"])
    assert single.files == 1 and single.rows == 4
    assert (tmp_path / "one.csv").is_file()
    assert len(read_dicts(tmp_path / "one.csv")) == 4
    both, _ = export_in_process(
        tmp_path / "r.ibrec", tmp_path / "two", messages=["MOCK_HK", "MOCK_STATUS"]
    )
    assert sorted(p.name for p in (tmp_path / "two").iterdir()) == [
        "MOCK_HK.csv",
        "MOCK_STATUS.csv",
        "malformed.csv",
    ]
    assert both.per_table == {"malformed": 0, "MOCK_HK": 4, "MOCK_STATUS": 3}
    everything, _ = export_in_process(tmp_path / "r.ibrec", tmp_path / "all")
    assert everything.rows == 7


def test_the_columns_are_the_ccdd_order_and_stable_across_runs(tmp_path: Path) -> None:
    hk_and_status(tmp_path / "r.ibrec", 2, 2)
    export_in_process(tmp_path / "r.ibrec", tmp_path / "a")
    export_in_process(tmp_path / "r.ibrec", tmp_path / "b")
    for name in ("MOCK_HK.csv", "MOCK_STATUS.csv"):
        assert (tmp_path / "a" / name).read_bytes() == (tmp_path / "b" / name).read_bytes()
    assert list(read_dicts(tmp_path / "a" / "MOCK_STATUS.csv")[0])[6:] == [
        "temperature_c",
        "bus_voltage_v",
        "uptime_s",
    ]


# -- naming collisions between tables ----


def test_a_message_called_malformed_and_names_that_differ_only_in_case_do_not_collide(
    tmp_path: Path,
) -> None:
    import asyncio
    import hashlib

    from exportutil import COMMAND_PROFILE, TELEMETRY_PROFILE
    from ironbird.ccdd.loader import load_ccdd_bytes
    from ironbird.codec import Codec
    from ironbird.header import load_profile
    from ironbird.recording.format import BlobKind
    from ironbird.recording.writer import BlobSpec, Recorder
    from recutil import FAST, MAX_PACKET, TX
    from replayutil import meta_data

    ccdd = {
        "schema": "ironbird-ccdd/0",
        "target": "t",
        "messages": [
            {
                "name": name,
                "kind": "telemetry",
                "mid": hex(0x900 + n),
                "category": "other",
                "description": "",
                "fields": [{"name": "x", "type": "uint8"}],
            }
            for n, name in enumerate(["malformed", "Foo", "foo"])
        ],
    }
    raw = json.dumps(ccdd).encode()
    codec = Codec(
        load_ccdd_bytes(raw, "t.json", "json"),
        load_profile(COMMAND_PROFILE),
        load_profile(TELEMETRY_PROFILE),
        max_packet_bytes=MAX_PACKET,
    )
    blobs = [
        BlobSpec(BlobKind.CCDD, "fc", "json", raw),
        BlobSpec(BlobKind.COMMAND_PROFILE, "fc", "toml", COMMAND_PROFILE.read_bytes()),
        BlobSpec(BlobKind.TELEMETRY_PROFILE, "fc", "toml", TELEMETRY_PROFILE.read_bytes()),
    ]
    meta = meta_data(
        nodes={
            "fc": {
                "adapter": "json",
                "ccdd_sha256": hashlib.sha256(raw).hexdigest(),
                "command_profile_sha256": hashlib.sha256(COMMAND_PROFILE.read_bytes()).hexdigest(),
                "telemetry_profile_sha256": hashlib.sha256(
                    TELEMETRY_PROFILE.read_bytes()
                ).hexdigest(),
            }
        }
    )
    recorder = Recorder.open(
        tmp_path / "r.ibrec", meta, blobs=blobs, streams=(RX, TX), options=FAST
    )
    for seq, name in enumerate(["malformed", "Foo", "foo", "foo"], start=1):
        data = packet(codec, name, bytes([seq]))
        recorder.append(frame(seq, stream=RX, data=data, mid=codec.model.by_name[name].mid))
    asyncio.run(recorder.close())
    result, _ = export_in_process(tmp_path / "r.ibrec", tmp_path / "out")
    names = sorted(p.name for p in (tmp_path / "out").iterdir())
    assert len({n.casefold() for n in names}) == len(names) == 4, names
    assert "malformed.csv" in names and "malformed.1.csv" in names and "Foo.csv" in names
    assert result.rows == 4 and sum(result.per_table.values()) == 4
    assert result.per_table["malformed"] == 0 and result.per_table["malformed.1"] == 1


# -- text and JSON Lines equal replay ----


def lines_of(path: Path) -> list[str]:
    return path.read_text().splitlines()


@pytest.fixture
def mixed(tmp_path: Path) -> Path:
    """HK and STATUS frames, an unknown MID, a short frame and a bad length: every kind."""
    frames = hk_frames(12) + codec_status_frames(6, 13)
    unknown = frame(19, stream=RX, data=b"\x0f\xf0" + hk_frames(1)[0].data[2:], mid=0x0FF0)
    short = frame(20, stream=RX, data=b"\x08\x90\xc0\x00", mid=None)
    bad_size = frame(21, stream=RX, data=hk_frames(1)[0].data + b"\x00", mid=0x890)
    write_recording(tmp_path / "mixed.ibrec", [*frames, unknown, short, bad_size])
    return tmp_path / "mixed.ibrec"


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"where": "cmd_counter >= 4"},
        {"where": "status != ok"},
        {"where": "msg == MOCK_STATUS"},
        {"start": TimeSpec(offset_ns=3 * STEP_NS), "end": TimeSpec(offset_ns=9 * STEP_NS)},
        {"where": "seq > 5 and seq < 15"},
    ],
    ids=["all", "field", "undecoded", "message", "range", "frame"],
)
def test_text_and_jsonl_are_what_replay_prints(
    mixed: Path, tmp_path: Path, kwargs: dict[str, Any]
) -> None:
    from ironbird.recording.player import OutputFormat

    for fmt, replay_fmt in (
        (ExportFormat.TEXT, OutputFormat.LINE),
        (ExportFormat.JSONL, OutputFormat.JSON),
    ):
        out = tmp_path / f"{fmt.value}.out"
        result, _ = export_in_process(mixed, out, fmt, **kwargs)
        expected = replay_in_process(mixed, fmt_=replay_fmt, **kwargs)
        assert lines_of(out) == expected.lines
        assert result.rows == len(expected.lines)
        assert result.malformed == expected.result.malformed
        assert result.unknown == expected.result.unknown


def test_csv_holds_the_frames_replay_shows_decoded_and_malformed_ones_separately(
    mixed: Path, tmp_path: Path
) -> None:
    result, _ = export_in_process(mixed, tmp_path / "out")
    shown = replay_in_process(mixed)
    assert result.rows == len(shown.lines) == 21
    hk = read_dicts(tmp_path / "out" / "MOCK_HK.csv")
    status = read_dicts(tmp_path / "out" / "MOCK_STATUS.csv")
    bad = read_dicts(tmp_path / "out" / "malformed.csv")
    assert (len(hk), len(status), len(bad)) == (12, 6, 3)
    assert [r["status"] for r in bad] == ["unknown", "malformed", "malformed"]
    assert [r["seq"] for r in bad] == ["19", "20", "21"]
    assert bad[0]["code"] == "IB-DEC-006" and bad[0]["mid"] == "0x0FF0" and bad[1]["mid"] == ""
    assert bad[2]["hex"].startswith("0x")
    assert bytes.fromhex(bad[2]["hex"].removeprefix("0x")) == hk_frames(1)[0].data + b"\x00"
    assert bad[2]["len"] == bad[2]["orig_len"] == str(len(hk_frames(1)[0].data) + 1)


def test_garbage_and_truncated_frames_go_to_the_malformed_table_with_their_flags(
    tmp_path: Path,
) -> None:
    good = hk_frames(1)[0]
    garbage = frame(
        2, stream=SERIAL, data=b"\xde\xad\xbe\xef", mid=None, flags=int(FrameFlag.GARBAGE)
    )
    cut = frame(
        3,
        stream=RX,
        data=good.data[:40] + bytes(2),
        mid=0x890,
        flags=int(FrameFlag.TRUNCATED),
        orig_len=3000,
    )
    write_recording(tmp_path / "r.ibrec", [good, garbage, cut], streams=(RX, SERIAL))
    result, _ = export_in_process(tmp_path / "r.ibrec", tmp_path / "out")
    bad = read_dicts(tmp_path / "out" / "malformed.csv")
    assert [r["status"] for r in bad] == ["garbage", "malformed"]
    assert bad[0]["flags"] == "garbage" and bad[1]["flags"] == "truncated"
    assert bad[1]["orig_len"] == "3000" and bad[1]["len"] == str(len(cut.data))
    assert (result.garbage, result.malformed, result.decoded) == (1, 1, 1)


# -- the selection ----


def test_message_selects_by_name_and_with_where_by_both(tmp_path: Path, mixed: Path) -> None:
    result, _ = export_in_process(
        mixed, tmp_path / "hk.csv", messages=["MOCK_HK"], where="cmd_counter > 6"
    )
    rows = read_dicts(tmp_path / "hk.csv")
    assert [r["cmd_counter"] for r in rows] == ["7", "8", "9", "10", "11", "12"]
    assert result.decoded == 6


def test_a_single_file_csv_drops_frames_that_did_not_decode_and_counts_them(
    tmp_path: Path, mixed: Path
) -> None:
    result, _ = export_in_process(mixed, tmp_path / "hk.csv", messages=["MOCK_HK"])
    assert result.not_written == 1  # the bad-size frame carries MOCK_HK's MID
    assert result.rows == 12 and len(read_dicts(tmp_path / "hk.csv")) == 12


def test_several_messages_keep_the_malformed_frames_of_their_mids(
    tmp_path: Path, mixed: Path
) -> None:
    result, _ = export_in_process(mixed, tmp_path / "d", messages=["MOCK_HK", "MOCK_STATUS"])
    bad = read_dicts(tmp_path / "d" / "malformed.csv")
    assert [r["seq"] for r in bad] == ["21"]  # the unknown MID and the short frame are not theirs
    assert result.not_written == 0


def test_an_unknown_message_name_is_a_usage_error_with_a_suggestion(
    tmp_path: Path, mixed: Path
) -> None:
    with pytest.raises(IronbirdError) as caught:
        export_in_process(mixed, tmp_path / "x.csv", messages=["MOCK_HKK"])
    assert caught.value.code is Code.CLI_USAGE and "MOCK_HK" in caught.value.summary
    assert not (tmp_path / "x.csv").exists()


def test_two_layouts_of_one_message_name_cannot_share_a_single_file(tmp_path: Path) -> None:
    # Two nodes embedding the same message name with different layouts.
    from ironbird.recording.export import Layouts

    codec = wide_codec()
    wide = codec.model.by_name["WIDE"]
    other = type(wide)(
        name="WIDE",
        kind=wide.kind,
        mid=wide.mid + 1,
        function_code=None,
        description="",
        fields=wide.fields[:3],
        payload_size=wide.payload_size,
        category=wide.category,
    )
    dest = NullDestination()
    layouts = Layouts(dest, header_fields=False, raw_enums=False, single=True, directory=False)
    layouts.plan_for(wide, codec)
    layouts.plan_for(wide, codec)  # the same message again is the same table
    with pytest.raises(IronbirdError) as caught:
        layouts.plan_for(other, codec)
    assert caught.value.code is Code.CLI_USAGE and "layout" in caught.value.summary
    free = Layouts(dest, header_fields=False, raw_enums=False, single=False, directory=False)
    first = free.plan_for(wide, codec)
    second = free.plan_for(other, codec)
    assert first.table.stem == "WIDE" and second.table.stem == "WIDE.2"


# -- untrusted strings, end to end ----


def event_payload(app: bytes, event_id: int, kind: int, text: bytes) -> bytes:
    import struct

    return app.ljust(8, b"\0")[:8] + struct.pack("<HH", event_id, kind) + text.ljust(32, b"\0")[:32]


def test_an_event_text_with_a_formula_a_newline_and_an_escape_exports_inert(
    tmp_path: Path,
) -> None:
    codec = wide_codec()
    text = b"=cmd|' /C calc'!A0\n\x1b[31mred\x07"
    payloads = [
        event_payload(b"=APP", 7, 3, text),
        event_payload(b"-x", 8, 2, b"+1"),
        event_payload(b"@y", 9, 1, b"\t=1"),
    ]
    write_wide(tmp_path / "r.ibrec", wide_frames(codec, payloads, name="EVT"))
    export_in_process(tmp_path / "r.ibrec", tmp_path / "e.csv", messages=["EVT"])
    raw = (tmp_path / "e.csv").read_bytes()
    assert b"\x1b" not in raw and b"\x07" not in raw and b"\t" not in raw
    assert raw.count(b"\n") == 4  # the header and one line per event, nothing the text added
    rows = read_dicts(tmp_path / "e.csv")
    assert rows[0]["text"] == "'=cmd|' /C calc'!A0\\x0a\\x1b[31mred\\x07"
    assert rows[0]["app"] == "'=APP" and rows[1]["app"] == "'-x" and rows[2]["app"] == "'@y"
    assert rows[1]["text"] == "'+1" and rows[2]["text"] == "\\x09=1"
    assert [r["event_type"] for r in rows] == ["ERROR", "INFORMATION", "DEBUG"]
    assert [r["event_id"] for r in rows] == ["7", "8", "9"]
    assert int(rows[0]["mid"], 16) == EVENT_MID


@FUZZ
@given(data=st.data())
def test_any_text_in_any_char_field_exports_one_inert_line(
    data: st.DataObject, tmp_path: Path
) -> None:
    codec = wide_codec()
    texts = data.draw(
        st.lists(
            st.one_of(st.binary(max_size=32), st.sampled_from(INJECTIONS).map(str.encode)),
            min_size=1,
            max_size=4,
        )
    )
    work = tmp_path / f"t{next(_COUNTER)}"
    work.mkdir()
    payloads = [event_payload(t[:8], 1, 2, t) for t in texts]
    write_wide(work / "r.ibrec", wide_frames(codec, payloads, name="EVT"))
    export_in_process(work / "r.ibrec", work / "e.csv", messages=["EVT"])
    raw = (work / "e.csv").read_bytes()
    assert raw.count(b"\n") == len(texts) + 1 and b"\r" not in raw
    assert not any(b < 32 and b != 10 for b in raw) and b"\x7f" not in raw
    for row in read_dicts(work / "e.csv"):
        for cell in row.values():
            assert inert(cell), cell


# -- the shape of the loop ----


def test_a_range_and_a_filter_that_select_nothing_still_write_a_valid_empty_table(
    tmp_path: Path, mixed: Path
) -> None:
    result, _ = export_in_process(mixed, tmp_path / "out", where="cmd_counter > 200")
    assert result.rows == 0
    assert sorted(p.name for p in (tmp_path / "out").iterdir()) == ["malformed.csv"]
    header, rows = read_table(tmp_path / "out" / "malformed.csv")
    assert header == list(MALFORMED_COLUMNS) and rows == []
    single, _ = export_in_process(
        mixed, tmp_path / "none.csv", messages=["MOCK_HK"], where="cmd_counter > 200"
    )
    assert single.rows == 0 and (tmp_path / "none.csv").read_bytes() == b""


def test_a_filter_that_needs_definitions_the_recording_lacks_is_not_silently_empty(
    tmp_path: Path,
) -> None:
    write_recording(tmp_path / "r.ibrec", hk_frames(4))
    with pytest.raises(IronbirdError) as caught:
        export_in_process(tmp_path / "r.ibrec", tmp_path / "out", where="nosuch == 1")
    assert caught.value.code.value.startswith("IB-FLT-")
    assert not (tmp_path / "out").exists()


def test_frames_of_a_node_without_definitions_are_malformed_rows_not_a_crash(
    tmp_path: Path,
) -> None:
    write_recording(tmp_path / "r.ibrec", hk_frames(3))
    result, _ = export_in_process(tmp_path / "r.ibrec", tmp_path / "out", codecs={})
    assert result.undecoded == 3 and result.decoded == 0
    rows = read_dicts(tmp_path / "out" / "malformed.csv")
    assert [r["status"] for r in rows] == ["undecoded"] * 3
    assert rows[0]["reason"] == "no definitions for the node" and rows[0]["code"] == ""
    filtered, _ = export_in_process(
        tmp_path / "r.ibrec", tmp_path / "f", codecs={}, where="seq > 1"
    )
    assert filtered.undecoded == 2  # a frame-level filter needs no definitions to be tested
    needing, _ = export_in_process(
        tmp_path / "r.ibrec", tmp_path / "g", codecs={}, where="status == ok"
    )
    assert needing.skipped_no_definitions == 3 and needing.rows == 0


def test_export_counts_agree_with_the_recording(tmp_path: Path) -> None:
    hk_and_status(tmp_path / "r.ibrec", 30, 20)
    result, replaying = export_in_process(tmp_path / "r.ibrec", tmp_path / "out")
    assert replaying.stats.frames_read == 50 == result.rows
    assert result.per_table == {"malformed": 0, "MOCK_HK": 30, "MOCK_STATUS": 20}
    assert [int(r["cmd_counter"]) for r in read_dicts(tmp_path / "out" / "MOCK_HK.csv")] == [
        n % 256 for n in range(1, 31)
    ]
    assert T0 > 0
    assert json.loads(json.dumps(result.per_table)) == result.per_table
