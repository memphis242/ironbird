"""Output sanitization for strings that originate from untrusted data.

Telemetry strings, CCDD names and error text reach a terminal; control characters and
escape sequences must never be interpreted by it. Everything non-printable is replaced by
a visible escape, so the text stays informative but inert.
"""

from __future__ import annotations

import unicodedata

DEFAULT_MAX_LEN = 256

# Categories that are not safe to print raw: control, format (bidi overrides, zero-width),
# surrogate, private use, unassigned, and line/paragraph separators.
_UNSAFE_CATEGORIES = frozenset({"Cc", "Cf", "Cs", "Co", "Cn", "Zl", "Zp"})


def _escape_char(ch: str) -> str:
    code = ord(ch)
    if code <= 0xFF:
        return f"\\x{code:02x}"
    if code <= 0xFFFF:
        return f"\\u{code:04x}"
    return f"\\U{code:08x}"


def clean(text: str, max_len: int = DEFAULT_MAX_LEN) -> str:
    """Return ``text`` with unsafe characters escaped and length bounded.

    A literal backslash is kept as is; the escapes are for display, not for round-trip.
    """
    assert max_len > 0
    clipped = text[:max_len]
    out: list[str] = []
    for ch in clipped:
        if ch == " " or (ch.isprintable() and unicodedata.category(ch) not in _UNSAFE_CATEGORIES):
            out.append(ch)
        else:
            out.append(_escape_char(ch))
    if len(text) > max_len:
        out.append("...")
    return "".join(out)
