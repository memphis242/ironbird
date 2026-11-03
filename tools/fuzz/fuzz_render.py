"""Coverage-guided fuzzing (atheris) of what reaches the terminal, JSON output and exports.

Usage: python tools/fuzz/fuzz_render.py [-max_total_time=N]

The first input byte picks the target; the rest is its content:

* 0: text. ``clean``, ``clean_terminal`` (ASCII and UTF-8 streams), ``clean_json``, ``term`` and
  ``safe_cell`` answer text that is bounded, inert (no control, format, surrogate or unassigned
  character, so no escape sequence a terminal would act on), encodable in the stream it is for,
  and for a cell not formula-leading.
* 1: a telemetry packet. 2: a command packet. Decoded, it renders as a line, a JSON record (strict
  JSON: no NaN) and, for an event, an event line and record; not decoded, as an undecoded line,
  record, hex line and hex record. Every line is one inert line.

Seeds are the checked-in packets plus valid ones and ones that carry hostile text.
"""

from __future__ import annotations

import json
import unicodedata

import atheris
from fuzzcommon import CORPUS, EXAMPLES, run, scratch_dir

with atheris.instrument_imports():
    from ironbird import header, render
    from ironbird.ccdd.loader import load_ccdd
    from ironbird.ccdd.model import MessageKind
    from ironbird.codec import Codec, DecodedPacket
    from ironbird.errors import DecodeError
    from ironbird.events import extract_event
    from ironbird.frame import Direction, Frame, FrameFlag, Stream
    from ironbird.recording.export import FORMULA_LEAD, safe_cell
    from ironbird.sanitize import clean, clean_json, clean_terminal

SCRATCH = scratch_dir("render")
CMD, TLM = MessageKind.COMMAND, MessageKind.TELEMETRY
CODEC = Codec(
    load_ccdd(EXAMPLES / "ccdd" / "mock.json"),
    header.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml"),
    header.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml"),
    clock=lambda: 1_000_000.25,
)
STREAMS = {
    kind: Stream(1, "ground-fc", "fc", kind, Direction.RX, "udp", "udp 127.0.0.1:1")
    for kind in (CMD, TLM)
}
UNSAFE = frozenset({"Cc", "Cf", "Cs", "Co", "Cn", "Zl", "Zp"})
TIME_NS = 1_700_000_000_123_456_789


def inert(text: str) -> bool:
    """No character a terminal or a log viewer could act on (a plain space is fine)."""
    return all(
        c == " " or (c.isprintable() and unicodedata.category(c) not in UNSAFE) for c in text
    )


def text_target(data: bytes) -> None:
    text = data.decode("utf-8", errors="surrogateescape")
    for limit in (1, 16, 256):
        out = clean(text, limit)
        assert len(out) <= 10 * limit + 3  # an escape is at most ten characters
        assert inert(out.removesuffix("...")), out
    for encoding in (None, "ascii", "utf-8", "latin-1", "no-such-codec"):
        out = clean_terminal(text, 256, encoding=encoding)
        out.encode(encoding if encoding in ("utf-8", "latin-1") else "ascii")
        assert inert(out)
    assert inert(render.term(text))
    json.dumps({"k": clean_json(text)}, ensure_ascii=False).encode("utf-8")
    cell = safe_cell(text)
    assert inert(cell), cell
    assert (cell.lstrip()[:1] or "x") not in FORMULA_LEAD, cell


def check_line(line: str) -> None:
    assert "\n" not in line and "\r" not in line, line
    assert inert(line), line


def packet_target(kind: MessageKind, data: bytes) -> None:
    decoded = CODEC.decode(kind, data)
    if isinstance(decoded, DecodedPacket):
        check_line(render.line("fc", TIME_NS, decoded))
        json.dumps(render.record("fc", TIME_NS, decoded), allow_nan=False)
        if kind is TLM:
            event = extract_event("fc", TIME_NS, decoded)
            if event is not None:
                check_line(render.event_line(event))
                json.dumps(render.event_record(event), allow_nan=False)
        return
    assert isinstance(decoded, DecodeError)
    flags = FrameFlag.NONE
    mid = CODEC.peek_mid(kind, data)
    if mid is not None:
        flags = FrameFlag.HAS_MID
    frame = Frame(1, TIME_NS, STREAMS[kind], "127.0.0.1:1", mid, flags, len(data), data)
    check_line(render.undecoded_line(frame, decoded))
    check_line(render.hex_line(frame))
    json.dumps(render.undecoded_record(frame, decoded), allow_nan=False)
    json.dumps(render.hex_record(frame), allow_nan=False)


def one_input(data: bytes) -> None:
    if not data:
        return
    mode, content = data[0] % 3, data[1:]
    match mode:
        case 0:
            text_target(content)
        case 1:
            packet_target(TLM, content)
        case _:
            packet_target(CMD, content)


def main() -> None:
    hostile = {
        "app": "\x1b[2J\x07",
        "event_id": 7,
        "event_type": 3,
        "message": "=cmd|\x7f\nl2\x1b[31m",  # the encoder refuses NUL in text
    }
    valid = [
        b"\x00" + b"=1+1\x1b[31m\xe2\x80\xae\xff\xfe \t-x",
        b"\x01" + CODEC.encode_telemetry("MOCK_EVENT", hostile).data,
        b"\x01" + CODEC.encode_telemetry("MOCK_STATUS", {"temperature_c": float("nan")}).data,
        b"\x01" + CODEC.encode_telemetry("MOCK_HK", {"cmd_counter": 5, "label": "\x1b[2Jx"}).data,
        b"\x02" + CODEC.encode_command("MOCK_SET_LABEL", {"label": "abc"}).data,
    ]
    seeds = SCRATCH / "seeds"
    seeds.mkdir()
    index = 0
    for pattern, mode in (("*packet-cmd-*.bin", b"\x02"), ("*packet-tlm-*.bin", b"\x01")):
        for path in sorted(CORPUS.glob(pattern)):
            (seeds / f"seed{index}").write_bytes(mode + path.read_bytes())
            index += 1
    for i, blob in enumerate(valid):
        (seeds / f"valid{i}").write_bytes(blob)
    run(one_input, seeds, max_len=2048)


if __name__ == "__main__":
    main()
