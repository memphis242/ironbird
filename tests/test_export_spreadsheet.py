"""A spreadsheet application opening an export evaluates no cell.

Skipped when LibreOffice (``soffice``) is not installed. The file is imported headless with the
options that make an importer most eager - leading spaces trimmed and formulas evaluated - and
saved again as CSV: a cell that was evaluated comes back as its result, one that is inert comes
back as the text that went in."""

from __future__ import annotations

import csv
import shutil
import subprocess
from pathlib import Path

import pytest

from exportutil import export_in_process, read_dicts, wide_codec, wide_frames, write_wide
from ironbird.recording.export import csv_line, safe_cell
from test_export import event_payload

SOFFICE = shutil.which("soffice") or shutil.which("libreoffice")
pytestmark = pytest.mark.skipif(SOFFICE is None, reason="LibreOffice (soffice) is not installed")

#: CSV import options: ',' separator, '"' quote, UTF-8, first line 1, default formats, language
#: default, no quote-all, detect special numbers, -, -, remove (trim) spaces, all sheets,
#: evaluate formulas.
IMPORT = "Text - txt - csv (StarCalc):44,34,76,1,,0,false,true,false,false,true,,true"
EXPORT = "csv:Text - txt - csv (StarCalc):44,34,76,1,,0,false,false,false,false,false,0"

HOSTILE = [
    "=1+1",
    " =1+1",
    "   =2+2",
    "+3+3",
    " -4-4",
    "@SUM(1,2)",
    "\uff1d5+5",
    " \uff1d5+5",
    "\uff0b6+6",
    "\uff0d7",
    "\uff20SUM(1)",
    "\xa0=8+8",
    "\u3000=9+9",
    "\t=10+10",
    "\r=11+11",
    "\n=12+12",
    " \t =13+13",
    "=cmd|' /C calc'!A0",
    " =cmd|' /C calc'!A0",
    '=HYPERLINK("http://x","y")',
    "= 1+1",
]


def through_libreoffice(csv_file: Path, work: Path) -> list[list[str]]:
    assert SOFFICE is not None
    done = subprocess.run(
        [
            SOFFICE,
            "--headless",
            f"-env:UserInstallation={(work / 'profile').as_uri()}",
            f"--infilter={IMPORT}",
            "--convert-to",
            EXPORT,
            "--outdir",
            str(work / "out"),
            str(csv_file),
        ],
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    saved = work / "out" / csv_file.with_suffix(".csv").name
    if done.returncode != 0 or not saved.exists():
        pytest.skip(f"LibreOffice could not convert here: {done.stderr[-200:]}")
    with saved.open(newline="", encoding="utf-8") as handle:
        return list(csv.reader(handle))


def test_no_hostile_cell_is_evaluated_by_a_spreadsheet(tmp_path: Path) -> None:
    source = tmp_path / "cells.csv"
    source.write_text(
        "".join(csv_line([i, safe_cell(text)]) for i, text in enumerate(HOSTILE)),
        encoding="utf-8",
    )
    rows = through_libreoffice(source, tmp_path)
    assert len(rows) == len(HOSTILE)
    for (_index, cell), text in zip(rows, HOSTILE, strict=True):
        assert cell.strip() == safe_cell(text).strip(), f"{text!r} came back as {cell!r}"


def test_an_exported_recording_with_hostile_text_is_not_evaluated(tmp_path: Path) -> None:
    codec = wide_codec()
    payloads = [
        event_payload(b"=APP", i, 2, text)
        for i, text in enumerate(
            [b"=1+1", b" =2+2", b"+3", b"-4", b"@SUM(1)", b"\t=5", b" -cmd|x"], start=1
        )
    ]
    write_wide(tmp_path / "r.ibrec", wide_frames(codec, payloads, name="EVT"))
    export_in_process(tmp_path / "r.ibrec", tmp_path / "EVT.csv", messages=["EVT"])
    written = read_dicts(tmp_path / "EVT.csv")
    returned = through_libreoffice(tmp_path / "EVT.csv", tmp_path)
    header, body = returned[0], returned[1:]
    assert len(body) == len(written)
    for row, expected in zip(body, written, strict=True):
        got = dict(zip(header, row, strict=True))
        for column in ("app", "text"):
            assert got[column].strip() == expected[column].strip(), (column, got[column])


def test_hex_cells_stay_text_in_a_spreadsheet(tmp_path: Path) -> None:
    codec = wide_codec()
    msg = codec.model.by_name["WIDE"]
    blob = msg.field("blob")
    assert blob is not None
    payloads = []
    for digits in ("00123456", "1e300000", "12345678", "00000000", "0e000001"):
        raw = bytearray(msg.payload_size)
        raw[blob.offset : blob.offset + 4] = bytes.fromhex(digits)
        payloads.append(bytes(raw))
    write_wide(tmp_path / "r.ibrec", wide_frames(codec, payloads))
    export_in_process(tmp_path / "r.ibrec", tmp_path / "WIDE.csv", messages=["WIDE"])
    written = read_dicts(tmp_path / "WIDE.csv")
    returned = through_libreoffice(tmp_path / "WIDE.csv", tmp_path)
    header, body = returned[0], returned[1:]
    assert [dict(zip(header, row, strict=True))["blob"] for row in body] == [
        row["blob"] for row in written
    ]
