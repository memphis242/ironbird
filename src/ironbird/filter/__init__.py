"""The filter language (docs/plan.md S4): ``compile_filter(text, codecs)`` gives a
:class:`Filter`; ``filter.bind(decode, codec_for)`` gives the :class:`Matcher` a consumer calls
on each frame. Live ``--where`` and replay share it.
"""

from ironbird.filter.compiler import NAMESPACES, RESERVED_NAMES, SCALAR_NAMES, compile_filter
from ironbird.filter.diag import (
    MAX_DEPTH,
    MAX_FILTER_CHARS,
    MAX_IN_ITEMS,
    MAX_NUMBER_CHARS,
    MAX_TOKENS,
    FilterError,
)
from ironbird.filter.matcher import Cost, Filter, Matcher
from ironbird.filter.parser import parse

__all__ = [
    "MAX_DEPTH",
    "MAX_FILTER_CHARS",
    "MAX_IN_ITEMS",
    "MAX_NUMBER_CHARS",
    "MAX_TOKENS",
    "NAMESPACES",
    "RESERVED_NAMES",
    "SCALAR_NAMES",
    "Cost",
    "Filter",
    "FilterError",
    "Matcher",
    "compile_filter",
    "parse",
]
