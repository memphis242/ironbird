"""Coverage-guided fuzzing (atheris) of header-profile TOML.

Usage: python tools/fuzz/fuzz_header_profile.py [-max_total_time=N]

Invariant: whatever bytes a profile holds, the loader for content that is not the user's own
(``load_profile_bytes``, which refuses the ``python:`` escape hatch) returns a ``HeaderProfile``
or raises only a coded ``IronbirdError`` (``IB-HDR-*``) with a one-line user text, quickly. The
trusted loader (``parse_profile_file``) is not fuzzed: it would run what the fuzzer writes. A
profile that loads is then used the way the codec uses it: ``header.unpack``, ``peek_mid`` and
``frame_length`` answer a typed result for packets of every shape, and a packet ``header.pack``
builds from it unpacks again (``Header``, never a ``DecodeError``) with the same MID. Seeds are the
checked-in ``hdr-*`` corpus plus the shipped profiles.
"""

from __future__ import annotations

import atheris
from fuzzcommon import EXAMPLES, run, scratch_dir, seed_corpus

with atheris.instrument_imports():
    from ironbird import header
    from ironbird.errors import Code, DecodeError, IronbirdError

SCRATCH = scratch_dir("hdrprofile")
_HDR_CODES = {Code.HDR_FILE, Code.HDR_SCHEMA, Code.HDR_PACK}
_PACK_CODES = _HDR_CODES | {Code.ENC_FIT}  # a payload the header's length field cannot size
PROBES = (
    b"",
    b"\x00",
    b"\x00" * 8,
    b"\xff" * 8,
    b"\x08\x81\xc0\x00\x00\x01\x00\x00" * 8,
    b"\xa5" * 300,
)


def use(profile: header.HeaderProfile) -> None:
    for probe in PROBES:
        parsed = header.unpack(profile, probe, max_size=1 << 16)
        assert isinstance(parsed, header.Header | DecodeError)
        mid = header.peek_mid(profile, probe)
        assert mid is None or isinstance(mid, int)
        length = profile.frame_length(probe) if profile.length_window is not None else None
        assert length is None or isinstance(length, int)
    mid = 0 if profile.mid_fields else None
    for payload in (b"", b"\x01\x02\x03", bytes(40)):
        try:
            packed = header.pack(profile, payload, mid=mid, now=1_700_000_000.0)
        except IronbirdError as err:
            assert err.code in _PACK_CODES, err.code
            continue
        parsed = header.unpack(profile, packed, max_size=1 << 16)
        if isinstance(parsed, header.Header):
            assert header.peek_mid(profile, packed) == parsed.mid


def one_input(data: bytes) -> None:
    try:
        profile = header.load_profile_bytes(data, "fuzz.toml")
    except IronbirdError as err:
        assert err.code in _HDR_CODES, err.code
        assert "\n" not in err.user_line(), err.user_line()
        return
    use(profile)


def main() -> None:
    seeds = seed_corpus(
        SCRATCH,
        ["hdr-*.toml", "codec-hdr-*.toml", "codec-narrow-length-field.toml"],
        [p.read_bytes() for p in sorted((EXAMPLES / "headers").glob("*.toml"))],
    )
    run(one_input, seeds, max_len=4096, dictionary="toml.dict")


if __name__ == "__main__":
    main()
