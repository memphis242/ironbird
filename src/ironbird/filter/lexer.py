"""Filter lexer: text to tokens in one linear pass, with fixed token patterns only.

Every pattern here is anchored, has no nested quantifier and is applied at one position, so the
cost is linear in the text. The text and the token count are bounded before and during the scan.
Outside strings only printable ASCII is allowed; inside a string, any printable character.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import Enum, auto
from typing import Any, Final, NamedTuple

from ironbird.errors import Code
from ironbird.filter.diag import (
    MAX_FILTER_CHARS,
    MAX_NUMBER_CHARS,
    MAX_TOKENS,
    FilterError,
    excerpt,
)


class Tok(Enum):
    IDENT = auto()  # value: the name
    INT = auto()  # value: int
    HEX = auto()  # value: int
    FLOAT = auto()  # value: Decimal, exactly as written
    STRING = auto()  # value: str
    TIME = auto()  # value: int, nanoseconds since the Unix epoch
    OP = auto()  # value: one of == != < <= > >=
    LPAREN = auto()
    RPAREN = auto()
    LBRACKET = auto()
    RBRACKET = auto()
    COMMA = auto()
    DOT = auto()
    END = auto()


class Token(NamedTuple):
    kind: Tok
    value: Any
    pos: int  # 0-based character offset of the first character
    end: int  # offset just past the last character


KEYWORDS: Final = frozenset({"and", "or", "not", "in", "contains"})

_WHITESPACE: Final = " \t\r\n"
_IDENT_RE: Final = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_NUMBER_RE: Final = re.compile(r"-?(?:0[xX][0-9A-Fa-f]+|[0-9]+(?:\.[0-9]+)?)")
_TIME_START_RE: Final = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T")
_TIME_RE: Final = re.compile(
    r"(?P<y>[0-9]{4})-(?P<mo>[0-9]{2})-(?P<d>[0-9]{2})T"
    r"(?P<h>[0-9]{2}):(?P<mi>[0-9]{2}):(?P<s>[0-9]{2})(?:\.(?P<f>[0-9]{1,9}))?"
    r"(?P<z>Z|[+-][0-9]{2}(?::?[0-9]{2})?)?"
)
_PUNCTUATION: Final = {
    "(": Tok.LPAREN,
    ")": Tok.RPAREN,
    "[": Tok.LBRACKET,
    "]": Tok.RBRACKET,
    ",": Tok.COMMA,
    ".": Tok.DOT,
}
_TWO_CHAR_OPS: Final = frozenset({"==", "!=", "<=", ">="})
_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)
_NS: Final = 1_000_000_000
_ESCAPABLE: Final = frozenset({"\\", '"', "'"})

MAX_TIME_CHARS: Final = 64
TIME_FORMAT_HINT: Final = "use YYYY-MM-DDTHH:MM:SS[.fff]Z or an offset such as +02:00"


def tokenize(text: str) -> list[Token]:
    """The tokens of ``text`` followed by one ``END`` token.

    Raises :class:`FilterError` (``IB-FLT-001`` for a character or literal that is not valid,
    ``IB-FLT-004`` for text or numbers over the limits). At most :data:`MAX_TOKENS` tokens are
    allowed outside the brackets of a list; what is inside is bounded by the text length and,
    item by item, by the parser.
    """
    if len(text) > MAX_FILTER_CHARS:
        raise FilterError(
            Code.FLT_LIMIT,
            text,
            MAX_FILTER_CHARS,
            f"the filter is {len(text)} characters; the limit is {MAX_FILTER_CHARS}",
        )
    tokens: list[Token] = []
    size = len(text)
    i = 0
    counted = 0  # tokens outside an ``in`` list; the items of a list are limited by the parser
    in_list = False
    while i < size:
        ch = text[i]
        if ch in _WHITESPACE:
            i += 1
            continue
        if counted >= MAX_TOKENS:
            raise FilterError(
                Code.FLT_LIMIT, text, i, f"more than {MAX_TOKENS} tokens; simplify the filter"
            )
        token = _next_token(text, i)
        tokens.append(token)
        i = token.end
        if token.kind is Tok.RBRACKET:
            in_list = False
        if not in_list:
            counted += 1
        if token.kind is Tok.LBRACKET:
            in_list = True
    tokens.append(Token(Tok.END, None, size, size))
    return tokens


def _next_token(text: str, i: int) -> Token:
    ch = text[i]
    if ch.isascii() and (ch.isalpha() or ch == "_"):
        match = _IDENT_RE.match(text, i)
        assert match is not None
        return Token(Tok.IDENT, match.group(), i, match.end())
    if ch.isascii() and ch.isdigit():
        if _TIME_START_RE.match(text, i):
            return _time(text, i)
        return _number(text, i)
    if ch == "-":
        return _number(text, i)
    if ch in "\"'":
        return _string(text, i)
    punct = _PUNCTUATION.get(ch)
    if punct is not None:
        return Token(punct, ch, i, i + 1)
    if ch in "=!<>":
        two = text[i : i + 2]
        if two in _TWO_CHAR_OPS:
            return Token(Tok.OP, two, i, i + 2)
        if ch in "<>":
            return Token(Tok.OP, ch, i, i + 1)
        hint = "; write == to compare" if ch == "=" else "; write != for not equal, or not"
        raise FilterError(Code.FLT_SYNTAX, text, i, f"unexpected {ch!r}{hint}")
    if not ch.isascii():
        raise FilterError(
            Code.FLT_SYNTAX, text, i, "non-ASCII character outside a string; quote the text"
        )
    if not ch.isprintable():
        raise FilterError(Code.FLT_SYNTAX, text, i, "control character in the filter")
    raise FilterError(Code.FLT_SYNTAX, text, i, f"unexpected character {ch!r}")


def _number(text: str, i: int) -> Token:
    match = _NUMBER_RE.match(text, i)
    if match is None:  # a '-' with no digit after it
        raise FilterError(
            Code.FLT_SYNTAX,
            text,
            i,
            "a '-' must start a negative number written without a space, such as -1",
        )
    literal = match.group()
    end = match.end()
    if len(literal) > MAX_NUMBER_CHARS:
        raise FilterError(
            Code.FLT_LIMIT,
            text,
            i,
            f"number of {len(literal)} characters; the limit is {MAX_NUMBER_CHARS}",
        )
    follower = text[end : end + 1]
    if follower and follower.isascii() and (follower.isalnum() or follower in "_."):
        raise FilterError(Code.FLT_SYNTAX, text, i, f"malformed number near {excerpt(text, i)}")
    body = literal.removeprefix("-")
    if body[:2] in ("0x", "0X"):
        return Token(Tok.HEX, int(literal, 16), i, end)
    if "." in literal:
        return Token(Tok.FLOAT, Decimal(literal), i, end)
    return Token(Tok.INT, int(literal), i, end)


def _time(text: str, i: int) -> Token:
    match = _TIME_RE.match(text, i)
    if match is None:
        raise FilterError(
            Code.FLT_SYNTAX, text, i, f"malformed time near {excerpt(text, i)}; {TIME_FORMAT_HINT}"
        )
    end = match.end()
    follower = text[end : end + 1]
    if follower and follower.isascii() and (follower.isalnum() or follower in "_."):
        raise FilterError(
            Code.FLT_SYNTAX, text, i, f"malformed time near {excerpt(text, i)}; {TIME_FORMAT_HINT}"
        )
    zone = match.group("z")
    if zone is None:
        raise FilterError(
            Code.FLT_SYNTAX,
            text,
            i,
            f"time without a zone; local times are not accepted, {TIME_FORMAT_HINT}",
        )
    try:
        stamp = datetime(
            int(match.group("y")),
            int(match.group("mo")),
            int(match.group("d")),
            int(match.group("h")),
            int(match.group("mi")),
            int(match.group("s")),
            tzinfo=UTC,
        )
        delta = stamp - _zone_offset(zone) - _EPOCH
    except (ValueError, OverflowError) as exc:
        raise FilterError(
            Code.FLT_SYNTAX, text, i, f"not a valid time ({exc}) near {excerpt(text, i)}"
        ) from None
    fraction = (match.group("f") or "").ljust(9, "0")
    seconds = delta.days * 86_400 + delta.seconds
    return Token(Tok.TIME, seconds * _NS + int(fraction), i, end)


def parse_time(text: str) -> int:
    """Nanoseconds since the Unix epoch of ``text``, an ISO 8601 time with a zone (``Z`` or an
    offset) and seconds, as a filter's time literal is. Raises :class:`FilterError` for anything
    else; the caller names the option it came from."""
    if len(text) > MAX_TIME_CHARS or _TIME_START_RE.match(text) is None:
        raise FilterError(
            Code.FLT_SYNTAX, text[:MAX_TIME_CHARS], 0, f"not a time; {TIME_FORMAT_HINT}"
        )
    token = _time(text, 0)
    if token.end != len(text):
        raise FilterError(Code.FLT_SYNTAX, text, token.end, "unexpected text after the time")
    seconds: int = token.value
    return seconds


def _zone_offset(zone: str) -> timedelta:
    """The UTC offset of ``Z`` or ``+HH``, ``+HHMM``, ``+HH:MM``; ValueError when out of range."""
    if zone == "Z":
        return timedelta(0)
    digits = zone[1:].replace(":", "")
    hours, minutes = int(digits[:2]), int(digits[2:] or "0")
    if hours > 23 or minutes > 59:
        raise ValueError(f"offset {zone}")
    offset = timedelta(hours=hours, minutes=minutes)
    return offset if zone[0] == "+" else -offset


def _string(text: str, i: int) -> Token:
    quote = text[i]
    out: list[str] = []
    j = i + 1
    size = len(text)
    while j < size:
        ch = text[j]
        if ch == quote:
            return Token(Tok.STRING, "".join(out), i, j + 1)
        if ch == "\\":
            nxt = text[j + 1 : j + 2]
            if nxt not in _ESCAPABLE:
                raise FilterError(
                    Code.FLT_SYNTAX,
                    text,
                    j,
                    "unknown escape in a string; only \\\\, \\\" and \\' exist",
                )
            out.append(nxt)
            j += 2
            continue
        if not ch.isprintable():
            raise FilterError(
                Code.FLT_SYNTAX, text, j, "control or non-printable character in a string"
            )
        out.append(ch)
        j += 1
    raise FilterError(Code.FLT_SYNTAX, text, i, "unterminated string")
