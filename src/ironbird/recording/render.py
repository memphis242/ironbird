"""Display text for ``ib rec info`` and ``ib rec check``. Every string from the file goes through
:func:`ironbird.render.term` (lines) or :func:`ironbird.render.jtext` (JSON)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Final

from ironbird.ledger import format_ts
from ironbird.recording import format as fmt
from ironbird.recording.inspect import Report
from ironbird.render import jtext, term

_MAX_MIDS_SHOWN: Final = 64
_MAX_ITEMS_SHOWN: Final = 20
_META_TEXT: Final = ("session", "version", "host", "user", "command", "acts_as", "created")


def size_text(count: int) -> str:
    value = float(count)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{count} B" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024
    raise AssertionError("unreachable")  # pragma: no cover


def _meta_text(meta: Mapping[str, Any], key: str) -> str:
    value = meta.get(key)
    return value if isinstance(value, str) else ""


def _duration_s(report: Report) -> float:
    return max(0, report.last_ns - report.first_ns) / 1e9 if report.frames else 0.0


def _status_text(report: Report) -> str:
    status = report.status
    detail = {
        "closed": "closed cleanly",
        "recording": "still being written (a recorder holds it)",
        "torn": "not closed cleanly: it ends in an incomplete record (readable up to there)",
        "unclosed": "not closed cleanly: it ends without a trailer",
        "damaged": "damaged mid-file (see below)",
        "inconsistent": "closed, but its summaries disagree with its records",
    }[status]
    return f"{status} - {detail}"


def _stream_line(stream_id: int, report: Report) -> str:
    s = report.streams[stream_id]
    return (
        f"  {stream_id:>3}  {term(s.link)}  {term(s.node)}  {s.kind.value} {s.direction.value}"
        f"  {term(s.endpoint)}"
    )


def _gap_text(gap: fmt.Gap) -> str:
    return (
        f"{gap.reason.name.lower()}: {gap.lost} frame(s) of stream {gap.stream}, "
        f"{format_ts(gap.first_ns)} to {format_ts(gap.last_ns)}, "
        f"seq {gap.first_seq}..{gap.last_seq}"
    )


def info_lines(report: Report) -> list[str]:
    """The ``ib rec info`` text."""
    meta = report.meta
    lines = [
        f"file:      {term(str(report.path), 400)} ({size_text(report.size)})",
        f"status:    {_status_text(report)}",
        f"format:    ibrec {report.version[0]}.{report.version[1]}, part "
        f"{meta.get('part', 1) if isinstance(meta.get('part', 1), int) else '?'}; "
        + (
            "summarised from the index"
            if report.source == "index"
            else "every record was read"
            + (f" (index: {term(report.index_note)})" if report.index != "ok" else "")
        ),
    ]
    by = [f"{key} {term(_meta_text(meta, key))}" for key in _META_TEXT if _meta_text(meta, key)]
    if by:
        lines.append("session:   " + ", ".join(by))
    config = meta.get("config")
    if isinstance(config, dict) and isinstance(config.get("path"), str):
        digest = config.get("sha256")
        lines.append(
            f"config:    {term(config['path'], 300)}"
            + (f" (sha256 {term(digest, 16)}...)" if isinstance(digest, str) else "")
        )
    if report.frames:
        lines.append(
            f"time:      {format_ts(report.first_ns)} to {format_ts(report.last_ns)} "
            f"({_duration_s(report):.3f} s)"
        )
    lines.append(
        f"frames:    {report.frames}"
        + (f" (seq {report.first_seq}..{report.last_seq})" if report.frames else "")
        + f", {report.chunks} chunk(s)"
        + (f", {report.frames_without_mid} without a MID" if report.frames_without_mid else "")
    )
    if report.streams:
        lines.append("streams:")
        lines += [_stream_line(i, report) for i in sorted(report.streams)]
    if report.mids:
        lines.append("MIDs:")
        shown = sorted(report.mids.items())[:_MAX_MIDS_SHOWN]
        lines += [f"  {mid:#010x}  {count}" for mid, count in shown]
        if len(report.mids) > len(shown):
            lines.append(f"  ... {len(report.mids) - len(shown)} more MIDs")
        if report.mids_dropped:
            lines.append("  (some frames are in no listed MID: a chunk's table overflowed)")
    if report.gap_total:
        lost = ", ".join(f"{why} {count}" for why, count in sorted(report.lost_by_reason.items()))
        lines.append(f"gaps:      {report.gap_total} ({lost})")
        lines += [f"  {_gap_text(g)}" for g in report.gaps[:_MAX_ITEMS_SHOWN]]
    else:
        lines.append("gaps:      none")
    if report.marks:
        lines.append("marks:")
        for mark in report.marks[:_MAX_ITEMS_SHOWN]:
            try:
                kind = fmt.MarkKind(mark.kind).name.lower()
            except ValueError:
                kind = f"kind {mark.kind}"
            lines.append(f"  {format_ts(mark.time_ns)}  {kind}  {term(mark.text)}")
    if report.blobs:
        lines.append("embedded definitions:")
        lines += [
            f"  {term(b.node)}  {b.kind}  {b.size} B  sha256 {b.sha256[:16]}..."
            for b in report.blobs
        ]
    if report.hole_frames:
        ranges = ", ".join(f"{a}..{b}" for a, b in report.holes[:_MAX_ITEMS_SHOWN])
        lines.append(
            f"missing:   {report.hole_frames} frame(s) in sequence holes no GAP explains: {ranges}"
        )
    for damage in report.damage:
        lines.append(f"damage:    {term(damage.to_error().user_line(), 600)}")
    for finding in report.findings:
        lines.append(f"finding:   {term(finding, 400)}")
    if report.findings_total > len(report.findings):
        lines.append(f"           ... {report.findings_total - len(report.findings)} more")
    return lines


def info_record(report: Report) -> dict[str, Any]:
    """The ``ib rec info --format json`` record."""
    meta = report.meta
    safe_meta = {
        key: jtext(value) if isinstance(value, str) else value
        for key, value in meta.items()
        if key
        in {
            *_META_TEXT,
            "pid",
            "max_packet_bytes",
            "part",
            "previous_last_seq",
            "argv",
            "nodes",
            "config",
        }
    }
    return {
        "file": jtext(str(report.path)),
        "size": report.size,
        "status": report.status,
        "writer_active": report.writer_active,
        "format": {"major": report.version[0], "minor": report.version[1]},
        "source": report.source,
        "meta": safe_meta,
        "frames": report.frames,
        "first_seq": report.first_seq,
        "last_seq": report.last_seq,
        "first_time": format_ts(report.first_ns) if report.frames else None,
        "last_time": format_ts(report.last_ns) if report.frames else None,
        "first_time_ns": report.first_ns if report.frames else None,
        "last_time_ns": report.last_ns if report.frames else None,
        "duration_s": _duration_s(report),
        "chunks": report.chunks,
        "frames_without_mid": report.frames_without_mid,
        "streams": [
            {
                "id": i,
                "link": jtext(s.link),
                "node": jtext(s.node),
                "kind": s.kind.value,
                "direction": s.direction.value,
                "transport": s.transport,
                "endpoint": jtext(s.endpoint),
            }
            for i, s in sorted(report.streams.items())
        ],
        "mids": [{"mid": mid, "count": count} for mid, count in sorted(report.mids.items())],
        "gaps": {
            "total": report.gap_total,
            "lost_by_reason": report.lost_by_reason,
            "items": [
                {
                    "stream": g.stream,
                    "reason": g.reason.name.lower(),
                    "lost": g.lost,
                    "first_time_ns": g.first_ns,
                    "last_time_ns": g.last_ns,
                    "first_seq": g.first_seq,
                    "last_seq": g.last_seq,
                }
                for g in report.gaps
            ],
        },
        "marks": [
            {"time_ns": m.time_ns, "kind": m.kind, "text": jtext(m.text)} for m in report.marks
        ],
        "definitions": [
            {
                "node": b.node,
                "kind": b.kind,
                "adapter": b.adapter,
                "sha256": b.sha256,
                "size": b.size,
            }
            for b in report.blobs
        ],
        "holes": [list(h) for h in report.holes],
        "missing_frames": report.hole_frames,
        "index": {"state": report.index, "note": jtext(report.index_note)},
        "damage": [
            {
                "offset": d.offset,
                "span": d.span,
                "torn": d.torn,
                "trailing": d.trailing,
                "reason": jtext(d.reason),
            }
            for d in report.damage
        ],
        "findings": [jtext(f) for f in report.findings],
    }
