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
import unicodedata
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
SUGGEST_POOL_NAMES: Final = 256  # candidates compared by similarity, after the cheap narrowing
SUGGEST_CUTOFF: Final = 0.6  # the least similarity worth suggesting
EXCERPT_CHARS: Final = 24  # source text quoted in a one-line message
DETAIL_WINDOW_CHARS: Final = 120  # source text shown around the column in the details

# The items of an `in` list are not tokens against MAX_TOKENS (the lexer leaves them out), and a
# list of the maximum size fits the text limit: an item and its comma take two characters.
assert 2 * MAX_IN_ITEMS <= MAX_FILTER_CHARS


# Bidirectional classes that make a terminal reorder text: right-to-left letters, Arabic
# numbers and the explicit embedding, override and isolate controls.
_BIDI_UNSAFE: Final = frozenset(
    {"R", "AL", "AN", "RLE", "RLO", "LRE", "LRO", "PDF", "LRI", "RLI", "FSI", "PDI"}
)
_WIDE: Final = frozenset({"W", "F"})


def visible(text: str) -> str:
    """``text`` with every character that is not printable, and every right-to-left or bidi
    control character, replaced by ``?``, one for one: a caret line under it stays aligned and
    neither a hostile expression nor a name in another script can drive or reorder the
    terminal."""
    return "".join(
        ch if ch.isprintable() and unicodedata.bidirectional(ch) not in _BIDI_UNSAFE else "?"
        for ch in text
    )


def display_width(text: str) -> int:
    """Terminal columns ``text`` takes: 2 for each wide or full-width character, 0 for each
    combining mark, 1 otherwise. ``text`` is expected to be the output of :func:`visible`."""
    width = 0
    for ch in text:
        if unicodedata.combining(ch) or unicodedata.category(ch) in ("Mn", "Me"):
            continue
        width += 2 if unicodedata.east_asian_width(ch) in _WIDE else 1
    return width


def excerpt(text: str, pos: int) -> str:
    """The source from ``pos`` on, as a short quoted piece for a one-line message."""
    piece = visible(text[pos : pos + EXCERPT_CHARS])
    if not piece:
        return "end of filter"
    return repr(piece + ("..." if len(text) > pos + EXCERPT_CHARS else ""))


def suggest(query: str, candidates: Iterable[str]) -> tuple[str, ...]:
    """Up to :data:`MAX_SUGGESTIONS` close matches of ``query`` among ``candidates``.

    Every candidate is looked at once, clipped to :data:`SUGGEST_QUERY_CHARS`, de-duplicated and
    kept only if it passes the cheap similarity bounds (length, then letter counts) for the
    cutoff; of those, the :data:`SUGGEST_POOL_NAMES` with the best bound get the full
    comparison. A name far down an alphabetical list is thus as findable as the first one, and
    the work stays linear in the number of candidates whatever the sizes of the inputs.
    Matching ignores case; the candidates are returned as spelled.
    """
    needle = query[:SUGGEST_QUERY_CHARS].lower()
    if not needle:
        return ()
    matcher = difflib.SequenceMatcher()
    matcher.set_seq2(needle)
    seen: set[str] = set()
    bounded: list[tuple[float, str]] = []
    for candidate in candidates:
        name = candidate[:SUGGEST_QUERY_CHARS]
        if name in seen:
            continue
        seen.add(name)
        matcher.set_seq1(name.lower())
        if matcher.real_quick_ratio() < SUGGEST_CUTOFF or matcher.quick_ratio() < SUGGEST_CUTOFF:
            continue
        bounded.append((-matcher.quick_ratio(), name))
    pool = heapq.nsmallest(SUGGEST_POOL_NAMES, bounded)
    by_lower: dict[str, str] = {}
    for _, name in pool:
        by_lower.setdefault(name.lower(), name)
    close = difflib.get_close_matches(
        needle, list(by_lower), n=MAX_SUGGESTIONS, cutoff=SUGGEST_CUTOFF
    )
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
    window = visible(text[start:end])
    shown = f"{lead}{window}{trail}"
    caret = " " * (len(lead) + display_width(window[: pos - start])) + "^"
    lines = [f"  {shown}", f"  {caret}"]
    if suggestions:
        lines.append("did you mean: " + ", ".join(suggestions))
    if notes:
        lines.extend(notes.splitlines())
    return "\n".join(lines)
