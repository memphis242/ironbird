"""The checked-in corpus of recordings with bad embedded definitions
(``tests/corpus/malformed/rpl-*.ibrec``).

Each case is a well-formed recording (valid records, valid frames) whose BLOB definitions are what
replay must refuse with ``IB-REC-009`` rather than crash on, or a recording damaged in the head.
Built deterministically (fixed nonce): a test proves the checked-in files are exactly these, and a
new bug adds a case and regenerates: ``python tests/replay_corpus.py``.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import NamedTuple

from ironbird.errors import Code
from ironbird.recording import format as fmt
from ironbird.recording.format import BlobKind
from recutil import MAX_PACKET
from replayutil import crafted, definition_bytes, good_blobs, meta_data, nodes_table

CORPUS = Path(__file__).resolve().parent / "corpus" / "malformed"


class Expect(NamedTuple):
    code: Code | None  # the error `ib replay FILE` ends with (None: it plays)
    exit_code: int  # ... and its exit code
    hex_exit: int  # exit code of `ib replay FILE --format hex`
    reason: str = ""  # text the reason the loader gave must contain (the case is for that reason)


def refused(reason: str) -> Expect:
    return Expect(Code.REC_DEFS, 1, 0, reason)


def _blobs(**changes: tuple[BlobKind, str, str, bytes]) -> list[tuple[BlobKind, str, str, bytes]]:
    by_name = {"ccdd": 0, "command": 1, "telemetry": 2}
    out = good_blobs()
    for name, blob in changes.items():
        out[by_name[name]] = blob
    return out


def _meta_of(blobs: list[tuple[BlobKind, str, str, bytes]], max_packet: int) -> dict[str, object]:
    """META as a recorder writes it for ``blobs``: each node's digests are those of the content
    the file embeds (the first of a repeated definition), so a case reaches the loader it is for
    rather than the digest check."""
    names = {
        BlobKind.CCDD: "ccdd_sha256",
        BlobKind.COMMAND_PROFILE: "command_profile_sha256",
        BlobKind.TELEMETRY_PROFILE: "telemetry_profile_sha256",
    }
    table: dict[str, dict[str, str]] = {}
    for kind, node, adapter, content in blobs:
        entry = table.setdefault(node, {"adapter": adapter})
        entry.setdefault(names[kind], hashlib.sha256(content).hexdigest())
    return meta_data(nodes=table, max_packet_bytes=max_packet)


def _case(blobs: list[tuple[BlobKind, str, str, bytes]], **kwargs: object) -> bytes:
    if "meta" not in kwargs:
        max_packet = kwargs.get("max_packet", MAX_PACKET)
        assert isinstance(max_packet, int)
        kwargs["meta"] = _meta_of(blobs, max_packet)
    return bytes(crafted(blobs, **kwargs).out)  # type: ignore[arg-type]


def _tampered_meta(**nodes: str) -> dict[str, object]:
    table = nodes_table()
    for key, value in nodes.items():
        table["fc"][key] = value
    return meta_data(nodes=table)


def cases() -> dict[str, tuple[bytes, Expect]]:
    ccdd, command, telemetry = definition_bytes()
    out: dict[str, tuple[bytes, Expect]] = {}

    def add(name: str, data: bytes, expect: Expect) -> None:
        out[f"rpl-{name}.ibrec"] = (data, expect)

    c, n, t = BlobKind.CCDD, BlobKind.COMMAND_PROFILE, BlobKind.TELEMETRY_PROFILE
    add(
        "defs-none", _case([], meta=meta_data(nodes={})), refused("embeds no ccdd, command_profile")
    )
    add("defs-missing-profile", _case(good_blobs()[:2]), refused("embeds no telemetry_profile"))
    add(
        "defs-ccdd-not-json",
        _case(_blobs(ccdd=(c, "fc", "json", b"{not json"))),
        refused("invalid JSON (Expecting property name"),
    )
    add(
        "defs-ccdd-duplicate-keys",
        _case(
            _blobs(ccdd=(c, "fc", "json", ccdd.replace(b'"schema"', b'"schema": "x", "schema"', 1)))
        ),
        refused("duplicate key 'schema'"),
    )
    add(
        "defs-ccdd-deep-nesting",
        _case(_blobs(ccdd=(c, "fc", "json", b"[" * 100_000))),
        refused("maximum recursion depth"),
    )
    add(
        "defs-ccdd-wrong-schema",
        _case(_blobs(ccdd=(c, "fc", "json", b'{"schema":"other/9","target":"x","messages":[]}'))),
        refused("must be 'ironbird-ccdd/0'"),
    )
    add(
        "defs-ccdd-huge-integer",
        _case(
            _blobs(
                ccdd=(
                    c,
                    "fc",
                    "json",
                    b'{"schema":"ironbird-ccdd/0","target":"x","messages":[],"n":'
                    + b"9" * 5000
                    + b"}",
                )
            )
        ),
        refused("digits"),
    )
    add(
        "defs-ccdd-adapter-unknown",
        _case(_blobs(ccdd=(c, "fc", "xml", ccdd))),
        refused("unknown CCDD adapter 'xml'"),
    )
    add(
        "defs-ccdd-not-utf8",
        _case(_blobs(ccdd=(c, "fc", "json", b"\xff\xfe\x00{"))),
        refused("'utf-8' codec can't decode"),
    )
    add(
        "defs-profile-not-toml",
        _case(_blobs(command=(n, "fc", "toml", b"{{{ = ="))),
        refused("invalid TOML"),
    )
    add(
        "defs-profile-oversize",
        _case(_blobs(telemetry=(t, "fc", "toml", telemetry + b"\n# " + b"x" * 70_000 + b"\n"))),
        refused("larger than 64 KiB"),
    )
    escape = telemetry.replace(
        b'computed = "from_mid"', b"computed = \"python:__import__('os')\"", 1
    )
    assert escape != telemetry
    add(
        "defs-profile-python-escape",
        _case(_blobs(telemetry=(t, "fc", "toml", escape))),
        refused("'python:' escape hatch is not allowed"),
    )
    add(
        "defs-profiles-swapped",
        _case(_blobs(command=(n, "fc", "toml", telemetry), telemetry=(t, "fc", "toml", command))),
        refused("not a command header profile"),
    )
    add(
        "defs-profile-adapter-unknown",
        _case(_blobs(command=(n, "fc", "json", command))),
        refused("unknown header profile format"),
    )
    add(
        "defs-meta-digest-mismatch",
        _case(good_blobs(), meta=_tampered_meta(ccdd_sha256=hashlib.sha256(b"other").hexdigest())),
        refused("does not match the digest"),
    )
    add(
        "defs-meta-digest-not-text",
        _case(good_blobs(), meta=meta_data(nodes={"fc": {"ccdd_sha256": 7}, "x": [1]})),
        Expect(None, 0, 0),
    )
    add(
        "defs-conflicting-blobs",
        _case([*good_blobs(), (c, "fc", "json", ccdd.replace(b"MOCK_NOOP", b"MOCK_NOOP2", 1))]),
        refused("two different ccdd definitions"),
    )
    add(
        "defs-cap-too-small",
        _case(good_blobs(), max_packet=20, frames=0),
        refused("exceeds the 20-byte cap"),
    )
    many = [
        (kind, f"n{i}", a, b"{}")
        for i in range(300)
        for kind, a in ((c, "json"), (n, "toml"), (t, "toml"))
    ]
    add(
        "defs-too-many-nodes",
        _case([*good_blobs(), *many], meta=meta_data(nodes={})),
        refused("more than 256 nodes"),
    )

    # A BLOB whose own digest lies is damage in the head (the reader refuses the record): replay
    # stops there with IB-REC-006, before any stream is known.
    lie = fmt.encode_blob(c, "fc", "json", b"\x00" * 32, ccdd)
    add(
        "defs-blob-digest-lies",
        bytes(crafted([], raw_blobs=[lie]).out),
        Expect(Code.REC_DAMAGED, 5, 5, "BLOB content does not match its SHA-256"),
    )
    return out


def regenerate() -> None:
    for name, (data, _) in cases().items():
        (CORPUS / name).write_bytes(data)


if __name__ == "__main__":
    regenerate()
