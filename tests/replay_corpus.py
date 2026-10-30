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
from replayutil import crafted, definition_bytes, good_blobs, meta_data, nodes_table

CORPUS = Path(__file__).resolve().parent / "corpus" / "malformed"


class Expect(NamedTuple):
    code: Code | None  # the error `ib replay FILE` ends with (None: it plays)
    exit_code: int  # ... and its exit code
    hex_exit: int  # exit code of `ib replay FILE --format hex`


REFUSED = Expect(Code.REC_DEFS, 1, 0)


def _blobs(**changes: tuple[BlobKind, str, str, bytes]) -> list[tuple[BlobKind, str, str, bytes]]:
    by_name = {"ccdd": 0, "command": 1, "telemetry": 2}
    out = good_blobs()
    for name, blob in changes.items():
        out[by_name[name]] = blob
    return out


def _case(blobs: list[tuple[BlobKind, str, str, bytes]], **kwargs: object) -> bytes:
    return bytes(crafted(blobs, **kwargs).out)  # type: ignore[arg-type]


def _tampered_meta(**nodes: str) -> dict[str, object]:
    table = nodes_table()
    for key, value in nodes.items():
        table["fc"][key] = value
    return meta_data(nodes=table)


def cases() -> dict[str, tuple[bytes, Expect]]:
    ccdd, command, telemetry = definition_bytes()
    out: dict[str, tuple[bytes, Expect]] = {}

    def add(name: str, data: bytes, expect: Expect = REFUSED) -> None:
        out[f"rpl-{name}.ibrec"] = (data, expect)

    c, n, t = BlobKind.CCDD, BlobKind.COMMAND_PROFILE, BlobKind.TELEMETRY_PROFILE
    add("defs-none", _case([], meta=meta_data(nodes={})))
    add("defs-missing-profile", _case(good_blobs()[:2]))
    add("defs-ccdd-not-json", _case(_blobs(ccdd=(c, "fc", "json", b"{not json"))))
    add(
        "defs-ccdd-duplicate-keys",
        _case(
            _blobs(ccdd=(c, "fc", "json", ccdd.replace(b'"schema"', b'"schema": "x", "schema"', 1)))
        ),
    )
    add("defs-ccdd-deep-nesting", _case(_blobs(ccdd=(c, "fc", "json", b"[" * 100_000))))
    add(
        "defs-ccdd-wrong-schema",
        _case(_blobs(ccdd=(c, "fc", "json", b'{"schema":"other/9","target":"x","messages":[]}'))),
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
    )
    add("defs-ccdd-adapter-unknown", _case(_blobs(ccdd=(c, "fc", "xml", ccdd))))
    add("defs-ccdd-not-utf8", _case(_blobs(ccdd=(c, "fc", "json", b"\xff\xfe\x00{"))))
    add("defs-profile-not-toml", _case(_blobs(command=(n, "fc", "toml", b"{{{ = ="))))
    add(
        "defs-profile-oversize",
        _case(_blobs(telemetry=(t, "fc", "toml", telemetry + b"\n# " + b"x" * 70_000 + b"\n"))),
    )
    escape = telemetry.replace(
        b'computed = "from_mid"', b"computed = \"python:__import__('os')\"", 1
    )
    assert escape != telemetry
    add("defs-profile-python-escape", _case(_blobs(telemetry=(t, "fc", "toml", escape))))
    add(
        "defs-profiles-swapped",
        _case(_blobs(command=(n, "fc", "toml", telemetry), telemetry=(t, "fc", "toml", command))),
    )
    add("defs-profile-adapter-unknown", _case(_blobs(command=(n, "fc", "json", command))))
    add(
        "defs-meta-digest-mismatch",
        _case(good_blobs(), meta=_tampered_meta(ccdd_sha256=hashlib.sha256(b"other").hexdigest())),
    )
    add(
        "defs-meta-digest-not-text",
        _case(good_blobs(), meta=meta_data(nodes={"fc": {"ccdd_sha256": 7}, "x": [1]})),
        Expect(None, 0, 0),
    )
    add(
        "defs-conflicting-blobs",
        _case([*good_blobs(), (c, "fc", "json", ccdd.replace(b"MOCK_NOOP", b"MOCK_NOOP2", 1))]),
    )
    add("defs-cap-too-small", _case(good_blobs(), max_packet=20, frames=0))
    many = [
        (kind, f"n{i}", a, b"{}")
        for i in range(300)
        for kind, a in ((c, "json"), (n, "toml"), (t, "toml"))
    ]
    add("defs-too-many-nodes", _case([*good_blobs(), *many], meta=meta_data(nodes={})))

    # A BLOB whose own digest lies is damage in the head (the reader refuses the record): replay
    # stops there with IB-REC-006, before any stream is known.
    lie = fmt.encode_blob(c, "fc", "json", b"\x00" * 32, ccdd)
    add(
        "defs-blob-digest-lies",
        bytes(crafted([], raw_blobs=[lie]).out),
        Expect(Code.REC_DAMAGED, 5, 5),
    )
    return out


def regenerate() -> None:
    for name, (data, _) in cases().items():
        (CORPUS / name).write_bytes(data)


if __name__ == "__main__":
    regenerate()
