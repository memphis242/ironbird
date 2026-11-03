"""Coverage-guided fuzzing (atheris) of the export paths.

Usage: python tools/fuzz/fuzz_export.py [-max_total_time=N]

The first input byte picks one variant of the export; the rest is the recording.

Invariant: whatever bytes a recording holds, exporting it (CSV tables, text, JSONL; with and
without ``skip_damage``; with and without a filter and a message selection) ends in a normal end
or a coded ``IronbirdError``, never another exception, an assert or a hang. A normal end writes
at most one row per frame read; the rows written to disk are well-formed CSV whose cells no
spreadsheet would run as a formula; and no ``*.tmp`` file is left behind. Seeds are the
checked-in recordings (``rec-*``, ``rpl-*`` and the hostile-content ``exp-*``) plus a valid one with
definitions embedded.
"""

from __future__ import annotations

import sys

import atheris
from fuzzcommon import CORPUS, TESTS, reset_dir, run, scratch_dir, seed_corpus

with atheris.instrument_imports():
    from ironbird.errors import IronbirdError
    from ironbird.recording.export import ExportFormat

sys.path.insert(0, str(TESTS))
SCRATCH = scratch_dir("export")
TARGET = SCRATCH / "input.ibrec"
OUT = SCRATCH / "out"
WHERES = (None, "status == ok", "status != ok")


def dry(fmt: ExportFormat, *, skip: bool, where: str | None, messages: tuple[str, ...]) -> int:
    from exportutil import NullDestination, export_in_process  # noqa: PLC0415

    dest = NullDestination()
    result, replaying = export_in_process(
        TARGET, None, fmt, skip_damage=skip, where=where, messages=messages, destination=dest
    )
    headers = len(dest.members) if fmt is ExportFormat.CSV else 0
    assert dest.lines - headers == result.rows, (dest.lines, headers, result.rows)
    assert result.rows <= replaying.stats.frames_read
    return result.rows


def written(fmt: ExportFormat) -> None:
    from exportutil import export_in_process, inert, read_table  # noqa: PLC0415

    reset_dir(OUT)
    target = OUT / ("tables" if fmt is ExportFormat.CSV else f"out.{fmt.value}")
    export_in_process(TARGET, target, fmt, skip_damage=True, force=True)
    assert not list(OUT.rglob("*.tmp")), "a temporary file was left behind"
    if fmt is ExportFormat.CSV and target.is_dir():
        for table in target.iterdir():
            header, rows = read_table(table)
            for row in rows:
                assert len(row) == len(header), (table.name, header, row)
                assert all(inert(cell) for cell in row), row


_EXPORT_PREFIXES = ("IB-REC-", "IB-EXP-", "IB-CCD-", "IB-HDR-", "IB-FLT-", "IB-OUT-")


def one_input(data: bytes) -> None:
    """The first byte picks one variant (format, damage policy, filter, to disk or not): a
    whole matrix per input would cost a hundred times the coverage-guided search it buys."""
    if not data:
        return
    variant, content = data[0], data[1:]
    fmt = tuple(ExportFormat)[variant % 3]
    skip = bool(variant & 4)
    where = WHERES[(variant >> 3) % 3]
    TARGET.write_bytes(content)
    try:
        if variant & 0x80:
            written(fmt)
        else:
            dry(fmt, skip=skip, where=where, messages=())
    except IronbirdError as err:
        # Recording damage, a bad export target, or the definitions/filter a recording embeds.
        assert err.code.value.startswith(_EXPORT_PREFIXES), err.code
        assert "\n" not in err.user_line(), err.user_line()


def main() -> None:
    from test_replay_malformed import mixed_recording  # noqa: PLC0415

    valid = SCRATCH / "valid.ibrec"
    mixed_recording(valid)
    seeds = seed_corpus(
        SCRATCH,
        ["rec-*.ibrec", "rpl-*.ibrec", "exp-*.ibrec"],
        [valid.read_bytes()],
        root=CORPUS,
        prefixes=[bytes([0x04]), bytes([0x80]), bytes([0x0D]), bytes([0x86 | 0x10])],
    )
    run(one_input, seeds, max_len=16384)


if __name__ == "__main__":
    main()
