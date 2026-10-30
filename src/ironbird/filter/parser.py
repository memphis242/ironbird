"""Filter parser: tokens to a small syntax tree, by recursive descent.

Grammar (see docs/plan.md S4)::

    expr      = term { "or" term }
    term      = factor { "and" factor }
    factor    = "not" factor | "(" expr ")" | predicate
    predicate = name cmp literal | name "in" "[" literal { "," literal } "]"
              | name "contains" STRING

``or`` and ``and`` chains become one n-ary node each (a parenthesised chain of the same kind is
merged into its parent), parsed by loops, so only ``not`` and parentheses recurse, and each of
those is counted against :data:`~ironbird.filter.diag.MAX_DEPTH` before it recurses. The tree
holds names and literals as written; resolving them is the compiler's job.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final, NamedTuple

from ironbird.errors import Code
from ironbird.filter.diag import MAX_DEPTH, MAX_IN_ITEMS, FilterError
from ironbird.filter.lexer import KEYWORDS, Tok, Token, tokenize

ORDERING_OPS: Final = frozenset({"<", "<=", ">", ">="})
EQUALITY_OPS: Final = frozenset({"==", "!="})
COMPARISON_OPS: Final = ORDERING_OPS | EQUALITY_OPS

_LITERAL_KINDS: Final = frozenset({Tok.INT, Tok.HEX, Tok.FLOAT, Tok.STRING, Tok.IDENT, Tok.TIME})


class Literal(NamedTuple):
    kind: Tok  # INT, HEX, FLOAT, STRING, IDENT or TIME
    value: Any  # int, Decimal (FLOAT), str (IDENT and STRING), or int nanoseconds (TIME)
    pos: int
    end: int


@dataclass(frozen=True, slots=True)
class Predicate:
    name: tuple[str, ...]  # dotted name segments
    name_pos: tuple[int, ...]  # offset of each segment
    op: str  # == != < <= > >= in contains
    op_pos: int
    values: tuple[Literal, ...]  # one literal, except for ``in``
    pos: int  # offset of the first token


@dataclass(frozen=True, slots=True)
class Not:
    child: Expr
    pos: int


@dataclass(frozen=True, slots=True)
class And:
    children: tuple[Expr, ...]  # at least two, none of them an And
    pos: int


@dataclass(frozen=True, slots=True)
class Or:
    children: tuple[Expr, ...]  # at least two, none of them an Or
    pos: int


Expr = Predicate | Not | And | Or


def describe(token: Token, text: str) -> str:
    """The token as the user wrote it, for a message."""
    if token.kind is Tok.END:
        return "the end of the filter"
    return repr(text[token.pos : token.end][:32])


class _Parser:
    def __init__(self, text: str) -> None:
        self._text = text
        self._tokens = tokenize(text)
        self._i = 0

    # -- token helpers ---------------------------------------------------------------

    def _peek(self) -> Token:
        return self._tokens[self._i]

    def _advance(self) -> Token:
        token = self._tokens[self._i]
        assert token.kind is not Tok.END, "the END token is never consumed"
        self._i += 1
        return token

    def _is_word(self, word: str) -> bool:
        token = self._tokens[self._i]
        return token.kind is Tok.IDENT and token.value == word

    def _fail(self, token: Token, message: str) -> FilterError:
        return FilterError(Code.FLT_SYNTAX, self._text, token.pos, message)

    def _expect(self, kind: Tok, what: str) -> Token:
        token = self._peek()
        if token.kind is not kind:
            raise self._fail(token, f"expected {what}, found {describe(token, self._text)}")
        return self._advance()

    def _enter(self, token: Token, depth: int) -> int:
        if depth >= MAX_DEPTH:
            raise FilterError(
                Code.FLT_LIMIT,
                self._text,
                token.pos,
                f"nesting deeper than {MAX_DEPTH} levels of 'not' and parentheses",
            )
        return depth + 1

    # -- grammar ---------------------------------------------------------------------

    def parse(self) -> Expr:
        if self._peek().kind is Tok.END:
            raise self._fail(self._peek(), "the filter is empty")
        tree = self._expr(0)
        token = self._peek()
        if token.kind is not Tok.END:
            what = "closing parenthesis" if token.kind is Tok.RPAREN else "token"
            raise self._fail(token, f"unexpected {what} {describe(token, self._text)}")
        return tree

    def _expr(self, depth: int) -> Expr:
        first = self._term(depth)
        if not self._is_word("or"):
            return first
        children: list[Expr] = [first]
        pos = first.pos
        while self._is_word("or"):
            self._advance()
            children.append(self._term(depth))
        return Or(_flatten(children, Or), pos)

    def _term(self, depth: int) -> Expr:
        first = self._factor(depth)
        if not self._is_word("and"):
            return first
        children: list[Expr] = [first]
        pos = first.pos
        while self._is_word("and"):
            self._advance()
            children.append(self._factor(depth))
        return And(_flatten(children, And), pos)

    def _factor(self, depth: int) -> Expr:
        token = self._peek()
        if token.kind is Tok.IDENT and token.value == "not":
            inner = self._enter(token, depth)
            self._advance()
            return Not(self._factor(inner), token.pos)
        if token.kind is Tok.LPAREN:
            inner = self._enter(token, depth)
            self._advance()
            tree = self._expr(inner)
            self._expect(Tok.RPAREN, "')'")
            return tree
        return self._predicate()

    def _predicate(self) -> Predicate:
        first = self._peek()
        if first.kind is not Tok.IDENT or first.value in KEYWORDS:
            raise self._fail(first, f"expected a name, found {describe(first, self._text)}")
        self._advance()
        segments: list[str] = [first.value]
        positions: list[int] = [first.pos]
        while self._peek().kind is Tok.DOT:
            self._advance()
            part = self._expect(Tok.IDENT, "a name after '.'")
            segments.append(part.value)
            positions.append(part.pos)
        name = ".".join(segments)
        token = self._peek()
        if token.kind is Tok.OP:
            self._advance()
            values: tuple[Literal, ...] = (self._literal(),)
            return Predicate(
                tuple(segments), tuple(positions), token.value, token.pos, values, first.pos
            )
        if token.kind is Tok.IDENT and token.value == "in":
            self._advance()
            return Predicate(
                tuple(segments), tuple(positions), "in", token.pos, self._items(), first.pos
            )
        if token.kind is Tok.IDENT and token.value == "contains":
            self._advance()
            text = self._peek()
            if text.kind is not Tok.STRING:
                raise self._fail(
                    text,
                    f"'contains' needs a quoted string, found {describe(text, self._text)}",
                )
            self._advance()
            literal = Literal(Tok.STRING, text.value, text.pos, text.end)
            return Predicate(
                tuple(segments), tuple(positions), "contains", token.pos, (literal,), first.pos
            )
        raise self._fail(
            token,
            f"expected a comparison (==, !=, <, <=, >, >=, in, contains) after {name!r}, "
            f"found {describe(token, self._text)}",
        )

    def _literal(self) -> Literal:
        token = self._peek()
        if token.kind not in _LITERAL_KINDS:
            raise self._fail(token, f"expected a value, found {describe(token, self._text)}")
        if token.kind is Tok.IDENT and token.value in KEYWORDS:
            raise self._fail(token, f"{token.value!r} is a keyword; quote it to use it as a value")
        self._advance()
        return Literal(token.kind, token.value, token.pos, token.end)

    def _items(self) -> tuple[Literal, ...]:
        self._expect(Tok.LBRACKET, "'[' after 'in'")
        items: list[Literal] = [self._literal()]
        while self._peek().kind is Tok.COMMA:
            comma = self._advance()
            if len(items) >= MAX_IN_ITEMS:
                raise FilterError(
                    Code.FLT_LIMIT,
                    self._text,
                    comma.pos,
                    f"more than {MAX_IN_ITEMS} items in a list",
                )
            items.append(self._literal())
        self._expect(Tok.RBRACKET, "',' or ']'")
        return tuple(items)


def _flatten(children: list[Expr], kind: type[And] | type[Or]) -> tuple[Expr, ...]:
    """``children`` with any child of the same kind merged in, so chains stay n-ary and flat."""
    flat: list[Expr] = []
    for child in children:
        if isinstance(child, kind):
            flat.extend(child.children)
        else:
            flat.append(child)
    return tuple(flat)


def parse(text: str) -> Expr:
    """The syntax tree of ``text``; raises :class:`~ironbird.filter.diag.FilterError`.

    Never raises anything else for any ``str``: the text, the token count and the nesting are
    bounded, and what exhausts the Python stack in spite of that (a caller already deep in the
    stack) is reported as the nesting limit.
    """
    try:
        return _Parser(text).parse()
    except RecursionError:
        raise FilterError(
            Code.FLT_LIMIT, text, 0, f"nesting deeper than {MAX_DEPTH} levels"
        ) from None
