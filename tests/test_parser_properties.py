"""Quick property tests (PR CI) of the parsers the nightly atheris harnesses in ``tools/fuzz`` cover
at length: config TOML, header-profile TOML, the CCDD JSON loader and the codec built from it,
retention's on-disk inputs (notices file, ledger heads and reserves, directory names), the small
text grammars, the ledger's record formatting and the terminal/cell sanitizers.

Invariants: only a coded ``IronbirdError`` (or ``FilterError``) escapes, nothing hangs (the
Hypothesis deadline), bytes the code wrote read back as what was written, and output meant for a
terminal or a spreadsheet is inert. A crash a harness finds becomes a file under
``tests/corpus/malformed`` and a case in the corpus tests of the module it concerns.
"""

from __future__ import annotations

import json
import math
import os
import shutil
import tempfile
import time
import unicodedata
from pathlib import Path
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from conftest import EXAMPLES
from hosttiming import hang_deadline
from ironbird import header, render
from ironbird.ccdd.loader import load_ccdd_bytes
from ironbird.ccdd.model import FieldDef, MessageKind, TypeClass
from ironbird.codec import Codec, DecodedPacket
from ironbird.config import load_config
from ironbird.errors import Code, IronbirdError
from ironbird.events import extract_event
from ironbird.filter import FilterError
from ironbird.filter.lexer import parse_time
from ironbird.fsops import REAL_FS
from ironbird.ledger import Ledger
from ironbird.recording.export import FORMULA_LEAD, safe_cell
from ironbird.recording.replay import parse_time_spec
from ironbird.recording.session import parse_split
from ironbird.retention import full_scan, is_managed_name, parse_head
from ironbird.retnotices import FILE_NAME, MAX_BYTES, Notices, parse_notices, render_notices
from ironbird.sanitize import clean, clean_json, clean_terminal
from ironbird.units import compact_duration, parse_count, parse_duration, parse_size

FUZZ = settings(
    max_examples=200,
    # a parser that takes two seconds on a few KB is a hang for our purposes
    deadline=hang_deadline(2000),
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)
SLOW = settings(
    max_examples=60,
    deadline=hang_deadline(5000),
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)
CMD_PROFILE = header.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml")
TLM_PROFILE = header.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml")
MOCK_TOML = (EXAMPLES / "mock.toml").read_bytes()
MOCK_JSON = (EXAMPLES / "ccdd" / "mock.json").read_bytes()
PROFILES = [p.read_bytes() for p in sorted((EXAMPLES / "headers").glob("*.toml"))]
DAY = 86400.0
UNSAFE_CATEGORIES = frozenset({"Cc", "Cf", "Cs", "Co", "Cn", "Zl", "Zp"})


@st.composite
def mutated(draw: st.DrawFn, base: bytes) -> bytes:
    """``base`` with a few byte flips, insertions, deletions and a cut: the near-valid files a
    byte-level fuzzer spends most of its time on."""
    data = bytearray(base)
    for _ in range(draw(st.integers(1, 6))):
        if not data:
            break
        at = draw(st.integers(0, len(data) - 1))
        match draw(st.sampled_from(["flip", "set", "insert", "delete", "cut", "repeat"])):
            case "flip":
                data[at] ^= 1 << draw(st.integers(0, 7))
            case "set":
                data[at] = draw(st.integers(0, 255))
            case "insert":
                data[at:at] = draw(st.binary(min_size=1, max_size=8))
            case "delete":
                del data[at : at + draw(st.integers(1, 8))]
            case "cut":
                del data[at:]
            case _:  # repeat
                data[at:at] = data[at : at + draw(st.integers(1, 64))] * draw(st.integers(2, 40))
    return bytes(data)


def inert(text: str) -> bool:
    return all(
        c == " " or (c.isprintable() and unicodedata.category(c) not in UNSAFE_CATEGORIES)
        for c in text
    )


# -- config ----


def _load_config_bytes(data: bytes) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "ironbird.toml"
        path.write_bytes(data)
        try:
            load_config(path)
        except IronbirdError as err:
            assert err.code in {
                Code.CFG_FILE,
                Code.CFG_SYNTAX,
                Code.CFG_SCHEMA,
                Code.CFG_REF,
                Code.CFG_LINK,
            }, err
            assert "\n" not in err.user_line()


@FUZZ
@given(st.binary(max_size=3000))
def test_config_loader_on_arbitrary_bytes(data: bytes) -> None:
    _load_config_bytes(data)


@FUZZ
@given(mutated(MOCK_TOML))
def test_config_loader_on_byte_mutated_valid_config(data: bytes) -> None:
    _load_config_bytes(data)


# -- header profiles ----


def _use_profile(profile: header.HeaderProfile) -> None:
    for probe in (b"", b"\x00" * 8, b"\xff" * 8, bytes(range(200))):
        parsed = header.unpack(profile, probe, max_size=1 << 16)
        assert isinstance(parsed, header.Header) or parsed.code.value.startswith("IB-DEC-")
        mid = header.peek_mid(profile, probe)
        assert mid is None or isinstance(mid, int)
    try:
        packed = header.pack(profile, b"\x01\x02\x03", mid=0 if profile.mid_fields else None)
    except IronbirdError as err:
        assert err.code in {Code.HDR_PACK, Code.HDR_SCHEMA, Code.HDR_FILE, Code.ENC_FIT}
        return
    parsed = header.unpack(profile, packed, max_size=1 << 16)
    if isinstance(parsed, header.Header):
        assert header.peek_mid(profile, packed) == parsed.mid


def _load_profile_bytes(data: bytes) -> None:
    try:
        profile = header.load_profile_bytes(data, "fuzz.toml")
    except IronbirdError as err:
        assert err.code in {Code.HDR_FILE, Code.HDR_SCHEMA, Code.HDR_PACK}, err
        assert "\n" not in err.user_line()
        return
    _use_profile(profile)


@FUZZ
@given(st.binary(max_size=3000))
def test_header_profile_loader_on_arbitrary_bytes(data: bytes) -> None:
    _load_profile_bytes(data)


@FUZZ
@given(st.sampled_from(PROFILES).flatmap(mutated))
def test_header_profile_loader_on_byte_mutated_shipped_profiles(data: bytes) -> None:
    _load_profile_bytes(data)


def test_untrusted_profile_content_never_runs_the_python_escape_hatch() -> None:
    doc = (
        b'name = "x"\nkind = "telemetry"\nmid_fields = []\n\n[[fields]]\nname = "f"\nbits = 8\n'
        b"computed = \"python:__import__('os').system('true')\"\n"
    )
    with pytest.raises(IronbirdError) as info:
        header.load_profile_bytes(doc, "embedded.toml")
    assert info.value.code is Code.HDR_SCHEMA


# -- CCDD ----


def _zero(field: FieldDef) -> object:
    match field.type.type_class:
        case TypeClass.INT:
            return 0
        case TypeClass.FLOAT:
            return 0.0
        case TypeClass.CHAR | TypeClass.BYTES:
            return b""
        case TypeClass.ARRAY:
            return ()
    raise AssertionError("unreachable")


def _drive(data: bytes) -> None:
    """Load ``data`` as a CCDD file and, if it loads, encode every message and decode it back."""
    try:
        model = load_ccdd_bytes(data, "fuzz.json")
    except IronbirdError as err:
        assert err.code in {Code.CCD_FILE, Code.CCD_SCHEMA, Code.CCD_LAYOUT}, err
        assert "\n" not in err.user_line()
        return
    try:
        codec = Codec(model, CMD_PROFILE, TLM_PROFILE, clock=lambda: 1_000_000.25)
    except IronbirdError as err:
        assert err.code.value.startswith(("IB-CCD-", "IB-ENC-", "IB-HDR-")), err
        return
    for message in model.messages:
        try:
            if message.kind is MessageKind.COMMAND:
                packet = codec.encode_command(
                    message.name, {f.name: _zero(f) for f in message.fields}
                )
            else:
                packet = codec.encode_telemetry(message.name)
        except IronbirdError as err:
            assert err.code.value.startswith(("IB-CCD-", "IB-ENC-", "IB-HDR-")), err
            continue
        decoded = codec.decode(message.kind, packet.data)
        assert isinstance(decoded, DecodedPacket), decoded
        assert decoded.message.name == message.name
        if message.kind is MessageKind.TELEMETRY:
            extract_event("fc", 0, decoded)
            render.line("fc", 0, decoded)


@FUZZ
@given(st.binary(max_size=3000))
def test_ccdd_loader_and_codec_on_arbitrary_bytes(data: bytes) -> None:
    _drive(data)


@FUZZ
@given(mutated(MOCK_JSON))
def test_ccdd_loader_and_codec_on_byte_mutated_valid_file(data: bytes) -> None:
    _drive(data)


def test_the_shipped_ccdd_files_drive_the_codec_end_to_end() -> None:
    for path in sorted((EXAMPLES / "ccdd").glob("*.json")):
        _drive(path.read_bytes())


# -- retention's on-disk inputs ----


NOW = time.time()
finite_times = st.floats(min_value=0, max_value=NOW, allow_nan=False, allow_infinity=False)
notice_keys = st.text(
    alphabet=st.characters(min_codepoint=33, max_codepoint=126), min_size=1, max_size=60
)


@FUZZ
@given(st.binary(max_size=MAX_BYTES + 200))
def test_notices_parser_on_arbitrary_bytes(data: bytes) -> None:
    shown, last = parse_notices(data, NOW)
    assert len(shown) <= 32 and (last is None or math.isfinite(last))
    assert parse_notices(render_notices(shown, last), NOW) == (shown, last)


@FUZZ
@given(mutated(render_notices({"IB-RET-001@/x": NOW - 5}, NOW - 9)))
def test_notices_parser_on_byte_mutated_valid_file(data: bytes) -> None:
    shown, last = parse_notices(data, NOW)
    assert parse_notices(render_notices(shown, last), NOW) == (shown, last)


@FUZZ
@given(st.dictionaries(notice_keys, finite_times, max_size=80), st.none() | finite_times)
def test_notices_render_is_bounded_and_round_trips_what_it_keeps(
    shown: dict[str, float], last: float | None
) -> None:
    image = render_notices(shown, last)
    assert len(image) == MAX_BYTES
    back, back_last = parse_notices(image, NOW)
    assert back_last == last
    assert back.items() <= shown.items()
    assert len(back) == min(len(shown), 32)


@SLOW
@given(st.binary(max_size=4400))
def test_notices_transaction_on_arbitrary_file_leaves_a_bounded_file(data: bytes) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        state = Path(tmp)
        (state / FILE_NAME).write_bytes(data)
        os.chmod(state / FILE_NAME, 0o600)
        assert isinstance(Notices(state).allow("IB-RET-001", state, 86400.0), bool)
        assert isinstance(Notices(state).scan_due(3600.0), bool)
        assert len((state / FILE_NAME).read_bytes()) <= max(MAX_BYTES, len(data))


@FUZZ
@given(st.binary(max_size=5000))
def test_ledger_head_parser_answers_an_object_or_none(data: bytes) -> None:
    head = parse_head(data)
    assert head is None or isinstance(head, dict)


def _listing(directory: Path) -> dict[bytes, bytes]:
    return {
        name: (directory.joinpath(os.fsdecode(name))).read_bytes()
        for name in os.listdir(os.fsencode(directory))
    }


@SLOW
@given(
    head=st.binary(max_size=600),
    reserve=st.none() | st.binary(max_size=900),
    names=st.lists(st.binary(min_size=1, max_size=40), max_size=4),
)
def test_scanning_a_ledger_directory_of_hostile_files_never_spends_what_is_not_ours(
    head: bytes, reserve: bytes | None, names: list[bytes]
) -> None:
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(NOW - 30 * DAY))
    with tempfile.TemporaryDirectory() as tmp:
        state = Path(tmp) / "st"
        state.mkdir(mode=0o700)
        files: dict[bytes, bytes] = {f"{stamp}-000000000001.jsonl".encode(): head}
        if reserve is not None:
            files[f"{stamp}-000000000001.r0001.reserve".encode()] = reserve
        for raw in names:
            name = raw.replace(b"/", b"").replace(b"\0", b"")
            if name not in (b"", b".", b".."):
                files.setdefault(name, b"{}\n")
        for name, content in files.items():
            path = os.path.join(os.fsencode(state), name)
            with open(path, "wb") as handle:
                handle.write(content)
            os.utime(path, (NOW - 30 * DAY, NOW - 30 * DAY))
        foreign = {n for n in files if not is_managed_name(os.fsdecode(n))}
        before = _listing(state)
        for recover, check_only in ((False, False), (False, True)):
            full_scan(
                REAL_FS,
                state,
                "ledger",
                now=NOW,
                own_session=None,
                keep_recent=5,
                recover=recover,
                check_only=check_only,
            )
            assert _listing(state) == before, f"recover={recover} check_only={check_only}"
        full_scan(REAL_FS, state, "ledger", now=NOW, own_session=None, keep_recent=5)
        assert foreign <= set(os.listdir(os.fsencode(state)))
        shutil.rmtree(state, ignore_errors=True)


# -- text grammars ----


@FUZZ
@given(st.text(max_size=60))
def test_size_and_duration_grammars_answer_a_value_or_none(text: str) -> None:
    size = parse_size(text)
    assert size is None or 1 <= size < 1 << 53
    duration = parse_duration(text)
    if duration is not None:
        assert 1 <= duration < 1 << 53
        assert parse_duration(compact_duration(duration)) == duration
    count = parse_count(text)
    assert count is None or 0 <= count <= 1000


@FUZZ
@given(st.text(max_size=80))
def test_cli_text_options_end_in_a_value_or_a_one_line_coded_error(text: str) -> None:
    for call in (
        lambda: parse_split(text),
        lambda: parse_time_spec(text, "--from"),
    ):
        try:
            call()
        except IronbirdError as err:
            assert err.code is Code.CLI_USAGE and "\n" not in err.user_line()
    try:
        assert isinstance(parse_time(text), int)
    except FilterError as err:
        assert "\n" not in err.user_line()


# -- ledger records ----

JSONISH = st.recursive(
    st.none()
    | st.booleans()
    | st.integers()
    | st.floats()
    | st.text(max_size=30)
    | st.binary(max_size=30),
    lambda inner: (
        st.lists(inner, max_size=4) | st.dictionaries(st.text(max_size=8), inner, max_size=4)
    ),
    max_leaves=20,
)
RESERVED = {"ts", "session", "n", "kind", "event", "tier", "sample"}


@SLOW
@given(
    event=st.text(max_size=30),
    fields=st.dictionaries(
        st.text(min_size=1, max_size=10).filter(lambda k: k not in RESERVED), JSONISH, max_size=5
    ),
)
def test_a_ledger_record_is_one_ascii_json_line_whatever_its_fields(
    event: str, fields: dict[str, Any]
) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        ledger = Ledger(Path(tmp) / "led", session_id="prop")
        ledger.open()
        ledger.record("action", event, **fields)
        ledger.flush()
        raw = ledger.path.read_bytes()
        ledger.close()
    lines = raw.splitlines()
    assert raw.endswith(b"\n") and len(lines) >= 2  # the head, then the record
    for line in lines:
        record = json.loads(line.decode("ascii"))
        assert isinstance(record, dict) and {"ts", "session", "n", "kind", "event"} <= record.keys()


# -- sanitizers ----


@FUZZ
@given(st.text(max_size=80))
def test_sanitized_text_is_inert_bounded_and_encodable(text: str) -> None:
    for limit in (1, 16, 256):
        out = clean(text, limit)
        assert len(out) <= 10 * limit + 3 and inert(out.removesuffix("..."))
    for encoding in (None, "ascii", "utf-8", "latin-1"):
        out = clean_terminal(text, 256, encoding=encoding)
        out.encode(encoding or "ascii")
        assert inert(out)
    json.dumps({"k": clean_json(text)}, ensure_ascii=False).encode("utf-8")
    cell = safe_cell(text)
    assert inert(cell)
    assert (cell.lstrip()[:1] or "x") not in FORMULA_LEAD
