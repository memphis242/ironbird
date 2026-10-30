"""Filter limits and filter errors.

A filter is user input (the command line, later a config file) and the names it resolves against
come from CCDD files, so every size is bounded here and every failure is a :class:`FilterError`
with a stable code, a column, and a short excerpt. Suggestions for a mistyped name are computed
over a bounded pool, so neither a hostile expression nor a hostile CCDD makes error reporting
expensive.
"""

from __future__ import annotations

import difflib
import heapq
from collections.abc import Iterable
from typing import Final

from ironbird.errors import Code, IronbirdError

MAX_FILTER_CHARS: Final = 4096
MAX_TOKENS: Final = 1024
MAX_DEPTH: Final = 32  # nested `not` and parentheses
MAX_IN_ITEMS: Final = 1024
MAX_NUMBER_CHARS: Final = 40  # sign included

MAX_SUGGESTIONS: Final = 3
SUGGEST_QUERY_CHARS: Final = 64  # the mistyped name is clipped to this before matching
SUGGEST_POOL_NAMES: Final = 256  # candidates compared: sorted, then cut
EXCERPT_CHARS: Final = 24  # source text quoted in a one-line message
DETAIL_WINDOW_CHARS: Final = 120  # source text shown around the column in the details

assert MAX_IN_ITEMS <= MAX_TOKENS  # an `in` list of the maximum size is within the token limit


def visible(text: str) -> str:
    """``text`` with every character that is not printable replaced by ``?``, one for one, so
    a caret line under it stays aligned and a hostile expression cannot drive the terminal."""
    return "".join(ch if ch.isprintable() else "?" for ch in text)


def excerpt(text: str, pos: int) -> str:
    """The source from ``pos`` on, as a short quoted piece for a one-line message."""
    piece = visible(text[pos : pos + EXCERPT_CHARS])
    if not piece:
        return "end of filter"
    return repr(piece + ("..." if len(text) > pos + EXCERPT_CHARS else ""))


def suggest(query: str, candidates: Iterable[str]) -> tuple[str, ...]:
    """Up to :data:`MAX_SUGGESTIONS` close matches of ``query`` among ``candidates``.

    The query is clipped to :data:`SUGGEST_QUERY_CHARS` and at most :data:`SUGGEST_POOL_NAMES`
    candidates (clipped the same way, de-duplicated, sorted, then cut) are compared, whatever
    the sizes of the inputs. Matching ignores case; the candidates are returned as spelled.
    """
    needle = query[:SUGGEST_QUERY_CHARS].lower()
    if not needle:
        return ()
    pool = heapq.nsmallest(SUGGEST_POOL_NAMES, {c[:SUGGEST_QUERY_CHARS] for c in candidates})
    by_lower: dict[str, str] = {}
    for name in pool:
        by_lower.setdefault(name.lower(), name)
    close = difflib.get_close_matches(needle, list(by_lower), n=MAX_SUGGESTIONS, cutoff=0.6)
    return tuple(by_lower[c] for c in close)


class FilterError(IronbirdError):
    """A filter that cannot be used: the code says why, ``column`` (1-based) where.

    The one-line summary names the column and what is wrong (with the best suggestion, if any);
    ``details`` repeat the expression around the column with a caret line and list the close
    matches, for ``--verbose`` and the ledger.
    """

    def __init__(
        self,
        code: Code,
        text: str,
        pos: int,
        message: str,
        *,
        suggestions: tuple[str, ...] = (),
        notes: str = "",
    ) -> None:
        assert code.value.startswith("IB-FLT-")
        pos = max(0, min(pos, len(text)))
        hint = f"; did you mean {suggestions[0]!r}?" if suggestions else ""
        super().__init__(
            code,
            f"filter column {pos + 1}: {message}{hint}",
            details=_details(text, pos, suggestions, notes),
        )
        self.column: int = pos + 1
        self.suggestions: tuple[str, ...] = suggestions


def _details(text: str, pos: int, suggestions: tuple[str, ...], notes: str) -> str:
    start = max(0, pos - DETAIL_WINDOW_CHARS // 2)
    end = min(len(text), start + DETAIL_WINDOW_CHARS)
    lead = "..." if start > 0 else ""
    trail = "..." if end < len(text) else ""
    shown = f"{lead}{visible(text[start:end])}{trail}"
    caret = " " * (len(lead) + pos - start) + "^"
    lines = [f"  {shown}", f"  {caret}"]
    if suggestions:
        lines.append("did you mean: " + ", ".join(suggestions))
    if notes:
        lines.extend(notes.splitlines())
    return "\n".join(lines)
