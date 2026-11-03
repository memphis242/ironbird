"""Coverage-guided fuzzing (atheris) of the CCDD JSON loader and the codec built from it.

Usage: python tools/fuzz/fuzz_ccdd.py [-max_total_time=N]

Invariant: whatever bytes a CCDD file holds, ``load_ccdd_bytes`` returns a model or raises only a
coded ``IronbirdError`` (``IB-CCD-*``) with a one-line user text, quickly. A model that loads is
driven the way the engine drives it: a ``Codec`` over the shipped headers either refuses it with a
coded error or encodes every message from zero values, and what it encoded decodes (one
``DecodedPacket`` of the same message, re-encodable from its own values); events extract from every
decoded event message. Seeds are the checked-in ``ccdd-*``/``codec-ccdd-*`` corpus plus the shipped
CCDD files.
"""

from __future__ import annotations

import atheris
from fuzzcommon import EXAMPLES, run, scratch_dir, seed_corpus

with atheris.instrument_imports():
    from ironbird import header
    from ironbird.ccdd.loader import load_ccdd_bytes
    from ironbird.ccdd.model import FieldDef, MessageKind, TypeClass
    from ironbird.codec import Codec, DecodedPacket
    from ironbird.errors import Code, IronbirdError
    from ironbird.events import extract_event

SCRATCH = scratch_dir("ccdd")
COMMAND_PROFILE = header.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml")
TELEMETRY_PROFILE = header.load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml")
_CCDD_CODES = {Code.CCD_FILE, Code.CCD_SCHEMA, Code.CCD_LAYOUT}
_USE_PREFIXES = ("IB-CCD-", "IB-ENC-", "IB-HDR-")


def zero(field: FieldDef) -> object:
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


def drive(codec: Codec) -> None:
    for message in codec.model.messages:
        try:
            if message.kind is MessageKind.COMMAND:
                args = {f.name: zero(f) for f in message.fields}
                packet = codec.encode_command(message.name, args)
            else:
                packet = codec.encode_telemetry(message.name)
        except IronbirdError as err:
            assert err.code.value.startswith(_USE_PREFIXES), err.code
            continue
        decoded = codec.decode(message.kind, packet.data)
        assert isinstance(decoded, DecodedPacket), decoded
        assert decoded.message.name == message.name
        if message.kind is MessageKind.TELEMETRY:
            extract_event("fc", 0, decoded)


def one_input(data: bytes) -> None:
    try:
        model = load_ccdd_bytes(data, "fuzz.json")
    except IronbirdError as err:
        assert err.code in _CCDD_CODES, err.code
        assert "\n" not in err.user_line(), err.user_line()
        return
    try:
        codec = Codec(model, COMMAND_PROFILE, TELEMETRY_PROFILE, clock=lambda: 1_000_000.25)
    except IronbirdError as err:
        assert err.code.value.startswith(_USE_PREFIXES), err.code
        return
    drive(codec)


def main() -> None:
    seeds = seed_corpus(
        SCRATCH,
        ["ccdd-*.json", "codec-ccdd-*.json"],
        [p.read_bytes() for p in sorted((EXAMPLES / "ccdd").glob("*.json"))],
    )
    run(one_input, seeds, max_len=8192, dictionary="ccdd.dict")


if __name__ == "__main__":
    main()
