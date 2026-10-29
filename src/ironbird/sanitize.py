"""Output sanitization for strings that originate from untrusted data.

Telemetry strings, CCDD names and error text reach a terminal; control characters and
escape sequences must never be interpreted by it. Everything non-printable is replaced by
a visible escape, so the text stays informative but inert.

Which function to use:

* :func:`clean_terminal` - text printed to a terminal or a pipe whose encoding may be narrower
  than UTF-8. The result is encodable in ``encoding`` (ASCII when not given), so ``print`` never
  raises ``UnicodeEncodeError``. Pass ``sys.stdout.encoding`` to keep characters the stream can
  show (for example the degree sign on a UTF-8 terminal).
* :func:`clean` - bounded, Unicode-preserving display text for sinks known to be UTF-8 (the
  ledger file, error summaries that are re-sanitized by the caller).
* :func:`clean_json` - string values inside JSON output. Never truncates and never pre-escapes:
  ``json.dumps`` already escapes control characters inertly, so escaping them again would
  corrupt the data. Use it on every string that is placed in a JSON document.
"""

from __future__ import annotations

import codecs
import unicodedata

DEFAULT_MAX_LEN = 256

# Categories that are not safe to print raw: control, format (bidi overrides, zero-width),
# surrogate, private use, unassigned, and line/paragraph separators.
_UNSAFE_CATEGORIES = frozenset({"Cc", "Cf", "Cs", "Co", "Cn", "Zl", "Zp"})
# U+FFFD marks bytes that did not decode; showing it as an escape keeps the corruption visible
# and keeps it from being mistaken for text.
_REPLACEMENT_CHAR = "\ufffd"


def _escape_char(ch: str) -> str:
    code = ord(ch)
    if code <= 0xFF:
        return f"\\x{code:02x}"
    if code <= 0xFFFF:
        return f"\\u{code:04x}"
    return f"\\U{code:08x}"


def _is_safe(ch: str) -> bool:
    if ch == " ":
        return True
    return (
        ch != _REPLACEMENT_CHAR
        and ch.isprintable()
        and unicodedata.category(ch) not in _UNSAFE_CATEGORIES
    )


def clean(text: str, max_len: int = DEFAULT_MAX_LEN) -> str:
    """Return ``text`` with unsafe characters escaped and length bounded.

    A literal backslash is kept as is; the escapes are for display, not for round-trip.
    The result may contain non-ASCII; use :func:`clean_terminal` for a terminal.
    """
    assert max_len > 0
    clipped = text[:max_len]
    out = [ch if _is_safe(ch) else _escape_char(ch) for ch in clipped]
    if len(text) > max_len:
        out.append("...")
    return "".join(out)


def _encodable(encoding: str | None) -> str:
    """The codec name to use for ``encoding``; ASCII when unknown, so output stays safe."""
    if not encoding:
        return "ascii"
    try:
        return codecs.lookup(encoding).name
    except LookupError:
        return "ascii"


def clean_terminal(
    text: str, max_len: int = DEFAULT_MAX_LEN, *, encoding: str | None = None
) -> str:
    """:func:`clean`, then escape every character ``encoding`` cannot represent.

    ``encoding`` is the output stream's encoding (``sys.stdout.encoding``); unknown or missing
    means ASCII. The result is always encodable in that codec.
    """
    codec = _encodable(encoding)
    out: list[str] = []
    for ch in clean(text, max_len):
        try:
            ch.encode(codec)
        except UnicodeEncodeError:
            out.append(_escape_char(ch))
        else:
            out.append(ch)
    return "".join(out)


def clean_json(text: str) -> str:
    """``text`` made safe to hand to ``json.dumps``: whole, unescaped, always encodable.

    JSON escaping makes control characters and terminal escape sequences inert, so the value is
    passed through untouched except for lone surrogates (which no UTF-8 sink can encode); those
    become ``?``.
    """
    try:
        text.encode("utf-8")
    except UnicodeEncodeError:
        return text.encode("utf-8", "replace").decode("utf-8")
    return text
