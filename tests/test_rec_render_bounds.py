"""``ib rec info`` / ``ib rec check`` text for a recording that is large, odd and damaged:
whatever the file holds, the output stays bounded, free of control characters, and the JSON
form stays strict JSON.
"""

from __future__ import annotations

import dataclasses
import json
import struct
from pathlib import Path
from typing import Any

import pytest

from ironbird.ccdd.model import MessageKind
from ironbird.frame import Direction, GapReason, Stream
from ironbird.recording import format as fmt
from ironbird.recording import inspect as rec_inspect
from ironbird.recording.format import RecordType
from ironbird.recording.render import info_json, info_lines, info_record
from recutil import RX, Builder

NASTY = "\x1b[31mred\x1b]0;title\x07\x00"


def stream_n(n: int) -> Stream:
    return Stream(
        n, f"link{n}", "fc", MessageKind.TELEMETRY, Direction.RX, "udp", f"udp 127.0.0.1:{n}"
    )


def big_odd_recording(path: Path, *, damaged: bool) -> Path:
    builder = Builder()
    for n in range(40):  # more streams than the text shows
        builder.stream(stream_n(n))
    builder.source(0, "peer")
    seq = 0
    for i in range(200):  # more MIDs than the text shows; sequence holes no GAP explains
        seq += 5 if i % 7 == 0 else 1
        builder.frame(seq, stream=i % 40, mid=0x100 + i, time_ns=1_700_000_000_000_000_000 + i)
    for i in range(30):  # more gaps than the text shows, of every reason
        reason = list(GapReason)[i % len(GapReason)]
        builder.add(
            RecordType.GAP,
            fmt.encode_gap(fmt.Gap(i % 40, reason, 3 + i, 10 + i, 20 + i, 1, 2)),
        )
    for i in range(30):  # more marks than the text shows; one kind from the future
        kind = 200 if i == 3 else fmt.MarkKind.NOTE
        builder.add(RecordType.MARK, struct.pack("<QBxxx", 1000 + i, kind) + NASTY.encode())
    if damaged:
        builder.out += b"\xde\xad\xbe\xef" * 7  # an incomplete record at the end
    return builder.write(path)


def strict(text: str) -> Any:
    def refuse(name: str) -> Any:
        raise AssertionError(f"{name} in JSON output")

    return json.loads(text, parse_constant=refuse)


@pytest.mark.parametrize("damaged", [False, True], ids=["intact", "damaged-tail"])
def test_a_large_odd_recording_renders_bounded_clean_text(tmp_path: Path, damaged: bool) -> None:
    path = big_odd_recording(tmp_path / "big.ibrec", damaged=damaged)
    report = rec_inspect.scan(path)
    lines = info_lines(report)
    text = "\n".join(lines)
    assert 0 < len(lines) < 400
    assert all(ord(c) >= 0x20 or c == "\n" for c in text), "a control character reached the text"
    assert "\x1b" not in text
    assert any("more" in line for line in lines), "nothing was cut although the file is large"
    record = strict(info_json(report))
    assert isinstance(record, dict)
    assert strict(json.dumps(info_record(report), allow_nan=False)) == record


def test_a_recording_with_holes_the_gaps_do_not_explain_says_so_in_the_text(
    tmp_path: Path,
) -> None:
    path = big_odd_recording(tmp_path / "holes.ibrec", damaged=False)
    report = rec_inspect.scan(path)
    assert report.hole_frames > 0
    assert any("sequence holes" in line or "missing" in line for line in info_lines(report))


def test_a_single_damaged_stretch_is_summarised_whole_and_many_are_counted(
    tmp_path: Path,
) -> None:
    path = big_odd_recording(tmp_path / "d.ibrec", damaged=True)
    report = rec_inspect.scan(path)
    assert report.damage_total >= 1
    assert report.damage_total == 1 or "damaged stretches" in rec_inspect_summary(report)
    many = dataclasses.replace(report, damage_total=report.damage_total + 50)
    assert "damaged stretches" in rec_inspect_summary(many)


def rec_inspect_summary(report: Any) -> str:
    from ironbird.recording.render import damage_summary

    return damage_summary(report)


def test_an_undamaged_report_has_an_empty_damage_summary(tmp_path: Path) -> None:
    builder = Builder().standard(3)
    report = rec_inspect.scan(builder.write(tmp_path / "ok.ibrec"))
    assert rec_inspect_summary(report) == ""


def test_the_meta_text_keeps_containers_bounded(tmp_path: Path) -> None:
    builder = Builder()
    builder.out = bytearray(fmt.encode_preamble(builder.nonce))
    huge_list = list(range(1000))
    huge_dict = {f"k{i}": i for i in range(1000)}
    meta = {
        "schema": fmt.META_SCHEMA,
        "max_packet_bytes": 2048,
        "argv": huge_list,
        "nodes": huge_dict,
        "config": {
            "path": "c.toml",
            "deep": {"a": {"b": {"c": {"d": {"e": {"f": {"g": {"h": {"i": 1}}}}}}}}},
        },
    }
    builder.add(RecordType.META, json.dumps(meta).encode())
    builder.stream(RX)
    report = rec_inspect.scan(builder.write(tmp_path / "meta.ibrec"))
    record = info_record(report)
    assert len(record["meta"]["argv"]) <= 300
    assert len(record["meta"]["nodes"]) <= 300
    strict(info_json(report))
