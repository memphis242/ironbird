"""Filter compiler: a syntax tree to a closure tree, checked against the CCDD it will run on.

Every name, operator and literal is resolved here, so a typo fails before a packet is read
(``IB-FLT-002`` for a name, ``IB-FLT-003`` for an operator or literal that does not fit it).
What runs per frame is a tree of closures over precomputed tables; no text is interpreted then.

Two optimisations never change a verdict:

* ``and`` and ``or`` run their cheapest operands first (frame fields, then a MID lookup, then
  the check, then a decode) and stop at the first that decides the result;
* the candidate MID set: every predicate that can only match some messages says which MIDs
  those are, ``and`` intersects, ``or`` unites, ``not`` makes the set unknown. The root rejects
  a frame outside the set before running anything, and each decode-level predicate checks its
  own messages' MIDs before it decodes, so a frame is decoded only when it can match.
"""

from __future__ import annotations

import operator
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from functools import partial
from typing import Any, Final, NamedTuple

from ironbird.ccdd.model import (
    SEVERITY_LABELS,
    Category,
    EventSeverity,
    FieldDef,
    Message,
    MessageKind,
    TypeClass,
)
from ironbird.codec import Codec
from ironbird.errors import Code, DecodeError
from ironbird.filter.diag import MAX_DEPTH, FilterError, suggest
from ironbird.filter.lexer import Tok
from ironbird.filter.matcher import Cost, Filter, Matcher, MatchFn
from ironbird.filter.parser import (
    COMPARISON_OPS,
    EQUALITY_OPS,
    ORDERING_OPS,
    And,
    Expr,
    Literal,
    Not,
    Or,
    Predicate,
    parse,
)
from ironbird.frame import MAX_MID, Direction, Frame

# -- names -------------------------------------------------------------------------------

SCALAR_NAMES: Final[Mapping[str, str]] = {
    "seq": "the frame sequence number",
    "time": "the frame receive time",
    "len": "the packet length on the wire",
    "node": "the frame's node",
    "link": "the frame's link",
    "dir": "the frame direction",
    "kind": "the stream kind (command or telemetry)",
    "source": "the frame's sender",
    "mid": "the frame's message ID",
    "truncated": "the truncated flag",
    "garbage": "the garbage flag",
    "category": "the telemetry category",
    "msg": "the message name",
    "status": "the frame's check status",
    "fc": "the command function code",
    "htime": "the header time",
}
NAMESPACES: Final[Mapping[str, str]] = {
    "hdr": "the header namespace (hdr.<field>)",
    "event": "the event namespace (event.app, event.id, event.type, event.text)",
}
RESERVED_NAMES: Final = frozenset(SCALAR_NAMES) | frozenset(NAMESPACES)
EVENT_PARTS: Final = ("app", "id", "type", "text")

_ALL_OPS: Final = COMPARISON_OPS | {"in"}
_NUMBER_OPS: Final = _ALL_OPS
_WORD_OPS: Final = EQUALITY_OPS | {"in", "contains"}
_SET_OPS: Final = EQUALITY_OPS | {"in"}

_FOUND: Final[Mapping[Tok, str]] = {
    Tok.INT: "the integer",
    Tok.HEX: "the integer",
    Tok.FLOAT: "the number",
    Tok.STRING: "the text",
    Tok.IDENT: "the word",
    Tok.TIME: "a time",
}
_STATUS: Final[Mapping[str, str]] = {"ok": "ok", "malformed": "malformed", "unknown": "unknown"}
_BOOLS: Final[Mapping[str, bool]] = {"true": True, "false": False}
_HEX_DIGITS: Final = frozenset("0123456789abcdefABCDEF")

Test = Callable[[Any], bool]


class _Reject(Exception):
    """A literal or operator that does not fit; becomes ``IB-FLT-003`` at the literal."""

    def __init__(self, reason: str, pool: Iterable[str] = ()) -> None:
        super().__init__(reason)
        self.reason = reason
        self.pool = tuple(pool)


class _Node(NamedTuple):
    cost: Cost
    mids: frozenset[int] | None
    fn: MatchFn


def _found(lit: Literal) -> str:
    shown = repr(lit.value[:32]) if isinstance(lit.value, str) else str(lit.value)
    if lit.kind is Tok.TIME:
        return "a time"
    return f"{_FOUND[lit.kind]} {shown}"


# -- literal conversion ------------------------------------------------------------------


def _int_literal(lit: Literal, op: str) -> Any:
    """An integer; a decimal number too, but only for ordering (``x > 1.5``): an integer is never
    equal to one."""
    if lit.kind in (Tok.INT, Tok.HEX):
        return lit.value
    if lit.kind is Tok.FLOAT and op in ORDERING_OPS:
        return lit.value
    raise _Reject(f"expected an integer, found {_found(lit)}")


def _out_of_range(op: str, values: Sequence[Any], lo: int, hi: int) -> str | None:
    """Why no value of ``lo..hi`` can equal any of ``values`` (``None``: some can, or ``op`` is
    an ordering, where any bound is meaningful)."""
    if op not in _SET_OPS or not values or any(lo <= v <= hi for v in values):
        return None
    shown = values[0] if len(values) == 1 else "any listed value"
    return f"{shown} is outside {lo}..{hi}"


def _float_literal(lit: Literal) -> float:
    if lit.kind in (Tok.INT, Tok.HEX, Tok.FLOAT):
        return float(lit.value)
    raise _Reject(f"expected a number, found {_found(lit)}")


def _text_literal(lit: Literal) -> str:
    if lit.kind is Tok.STRING:
        return lit.value
    raise _Reject(f"expected quoted text, found {_found(lit)}")


def _time_literal(lit: Literal) -> int:
    if lit.kind is Tok.TIME:
        return lit.value
    raise _Reject(f"expected a time such as 2026-10-29T12:00:00Z, found {_found(lit)}")


def _bool_literal(lit: Literal) -> bool:
    if lit.kind in (Tok.IDENT, Tok.STRING):
        got = _BOOLS.get(lit.value.lower())
        if got is not None:
            return got
    raise _Reject(f"expected true or false, found {_found(lit)}", _BOOLS)


def _word_literal(lit: Literal, table: Mapping[str, Any], *, fold: bool) -> Any:
    """A name (bare word or quoted text) looked up in ``table``."""
    if lit.kind not in (Tok.IDENT, Tok.STRING):
        raise _Reject(f"expected a name, found {_found(lit)}", table)
    key = lit.value.lower() if fold else lit.value
    if key not in table:
        raise _Reject(f"{lit.value[:40]!r} is not one of the known names", table)
    return table[key]


def _bytes_literal(lit: Literal, size: int, op: str) -> bytes:
    text = _text_literal(lit)
    if len(text) % 2 or not text or not _HEX_DIGITS.issuperset(text):
        raise _Reject('expected hexadecimal bytes in quotes, such as "0aff"')
    value = bytes.fromhex(text)
    if op == "contains":
        if len(value) > size:
            raise _Reject(f"{len(value)} bytes cannot occur in a {size}-byte field")
    elif len(value) != size:
        raise _Reject(f"the field is {size} bytes; the value has {len(value)}")
    return value


def _make_test(op: str, values: Sequence[Any]) -> Test:
    """A one-argument test of a value against the converted literal(s): ``value OP literal``."""
    match op:
        case "==":
            return partial(operator.eq, values[0])
        case "!=":
            return partial(operator.ne, values[0])
        case "<":
            return partial(operator.gt, values[0])
        case "<=":
            return partial(operator.ge, values[0])
        case ">":
            return partial(operator.lt, values[0])
        case ">=":
            return partial(operator.le, values[0])
        case "in":
            return frozenset(values).__contains__
        case "contains":
            needle = values[0]
            return lambda value: needle in value
        case _:
            raise AssertionError(f"unhandled operator {op}")  # pragma: no cover


# -- closures ----------------------------------------------------------------------------


def _all_of(fns: Sequence[MatchFn]) -> MatchFn:
    """The operands in order, stopping at the first that is false (short forms for 2 and 3)."""
    if len(fns) == 2:
        a, b = fns

        def two(frame: Frame, probe: Matcher) -> bool:
            return a(frame, probe) and b(frame, probe)

        return two
    if len(fns) == 3:
        a, b, c = fns

        def three(frame: Frame, probe: Matcher) -> bool:
            return a(frame, probe) and b(frame, probe) and c(frame, probe)

        return three
    rest = tuple(fns)

    def many(frame: Frame, probe: Matcher) -> bool:
        for fn in rest:  # noqa: SIM110 - a loop, not a generator: this is the per-frame path
            if not fn(frame, probe):
                return False
        return True

    return many


def _any_of(fns: Sequence[MatchFn]) -> MatchFn:
    """The operands in order, stopping at the first that is true (short forms for 2 and 3)."""
    if len(fns) == 2:
        a, b = fns

        def two(frame: Frame, probe: Matcher) -> bool:
            return a(frame, probe) or b(frame, probe)

        return two
    if len(fns) == 3:
        a, b, c = fns

        def three(frame: Frame, probe: Matcher) -> bool:
            return a(frame, probe) or b(frame, probe) or c(frame, probe)

        return three
    rest = tuple(fns)

    def many(frame: Frame, probe: Matcher) -> bool:
        for fn in rest:  # noqa: SIM110 - a loop, not a generator: this is the per-frame path
            if fn(frame, probe):
                return True
        return False

    return many


def _negated(inner: MatchFn) -> MatchFn:
    def negate(frame: Frame, probe: Matcher) -> bool:
        return not inner(frame, probe)

    return negate


def _frame_fn(get: Callable[[Frame], Any], test: Test, *, optional: bool = False) -> MatchFn:
    if optional:

        def maybe(frame: Frame, probe: Matcher) -> bool:
            value = get(frame)
            return value is not None and test(value)

        return maybe

    def plain(frame: Frame, probe: Matcher) -> bool:
        return test(get(frame))

    return plain


class _Entry(NamedTuple):
    slot: int  # position of the value in DecodedPacket.values
    test: Test


# node -> (MIDs of the messages in the table, message name -> entry)
_Table = dict[str, tuple[frozenset[int], dict[str, _Entry]]]


def _freeze(rows: Iterable[tuple[str, Message, _Entry]]) -> _Table:
    mids: dict[str, set[int]] = {}
    names: dict[str, dict[str, _Entry]] = {}
    for node, message, entry in rows:
        mids.setdefault(node, set()).add(message.mid)
        names.setdefault(node, {})[message.name] = entry
    return {node: (frozenset(mids[node]), names[node]) for node in names}


def _decode_fn(table: _Table) -> MatchFn:
    """Decode-level predicate over a table of messages; a frame not among them is not decoded."""

    def fn(frame: Frame, probe: Matcher) -> bool:
        row = table.get(frame.stream.node)
        if row is None or frame.mid not in row[0]:
            return False
        decoded = probe.decoded(frame)
        if isinstance(decoded, DecodeError):
            return False
        packet = decoded.packet
        entry = row[1].get(packet.message.name)
        return entry is not None and entry.test(packet.values[entry.slot])

    return fn


# -- the compiler ------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Located:
    """A message of one node with the index and definition of one of its fields."""

    node: str
    message: Message
    index: int
    field: FieldDef


class _Env:
    """What names mean for this run: the codecs of the nodes in use, indexed on first use."""

    def __init__(self, codecs: Mapping[str, Codec], links: Iterable[str] | None) -> None:
        self.codecs: dict[str, Codec] = dict(codecs)
        self.links: frozenset[str] | None = frozenset(links) if links is not None else None
        self.messages: list[tuple[str, Message]] = [
            (node, message)
            for node, codec in self.codecs.items()
            for message in codec.model.messages
        ]
        self._by_name: dict[str, list[tuple[str, Message]]] | None = None
        self._by_field: dict[str, list[_Located]] | None = None

    def named(self, name: str) -> list[tuple[str, Message]]:
        if self._by_name is None:
            index: dict[str, list[tuple[str, Message]]] = {}
            for node, message in self.messages:
                index.setdefault(message.name, []).append((node, message))
            self._by_name = index
        return self._by_name.get(name, [])

    def fields_named(self, name: str) -> list[_Located]:
        return self._field_index().get(name, [])

    def _field_index(self) -> dict[str, list[_Located]]:
        if self._by_field is None:
            index: dict[str, list[_Located]] = {}
            for node, message in self.messages:
                for i, fld in enumerate(message.fields):
                    index.setdefault(fld.name, []).append(_Located(node, message, i, fld))
            self._by_field = index
        return self._by_field

    def message_names(self) -> Iterable[str]:
        return {message.name for _, message in self.messages}

    def field_names(self) -> Iterable[str]:
        return self._field_index().keys()

    def header_fields(self) -> dict[str, int]:
        """Header field name to its widest mask over the profiles in use."""
        widest: dict[str, int] = {}
        for codec in self.codecs.values():
            for kind in MessageKind:
                for hf in codec.profile(kind).fields:
                    widest[hf.name] = max(widest.get(hf.name, 0), hf.mask)
        return widest


class _Compiler:
    def __init__(self, text: str, codecs: Mapping[str, Codec], links: Iterable[str] | None) -> None:
        self.text = text
        self.env = _Env(codecs, links)
        self._scalars: Mapping[str, Callable[[Predicate], _Node]] = {
            "seq": self._seq,
            "time": self._time,
            "len": self._len,
            "node": self._node,
            "link": self._link,
            "dir": self._dir,
            "kind": self._kind,
            "source": self._source,
            "mid": self._mid,
            "truncated": self._truncated,
            "garbage": self._garbage,
            "category": self._category,
            "msg": self._msg,
            "status": self._status,
            "fc": self._fc,
            "htime": self._htime,
        }
        assert self._scalars.keys() == SCALAR_NAMES.keys()

    # -- tree ------------------------------------------------------------------------

    def compile(self, tree: Expr) -> _Node:
        match tree:
            case Predicate():
                return self._predicate(tree)
            case Not(child=child):
                inner = self.compile(child)
                return _Node(inner.cost, None, _negated(inner.fn))
            case And(children=children):
                parts = sorted((self.compile(c) for c in children), key=lambda n: n.cost)
                mids: frozenset[int] | None = None
                for part in parts:
                    if part.mids is not None:
                        mids = part.mids if mids is None else mids & part.mids
                return _Node(parts[-1].cost, mids, _all_of([p.fn for p in parts]))
            case Or(children=children):
                parts = sorted((self.compile(c) for c in children), key=lambda n: n.cost)
                united: set[int] = set()
                for part in parts:
                    if part.mids is None:
                        return _Node(parts[-1].cost, None, _any_of([p.fn for p in parts]))
                    united |= part.mids
                return _Node(parts[-1].cost, frozenset(united), _any_of([p.fn for p in parts]))
        raise AssertionError(f"unhandled node {tree!r}")  # pragma: no cover

    # -- errors ----------------------------------------------------------------------

    def _name_error(
        self, pos: int, message: str, pool: Iterable[str], query: str = ""
    ) -> FilterError:
        found = suggest(query, pool) if query else ()
        return FilterError(Code.FLT_NAME, self.text, pos, message, suggestions=found)

    def _collision(self, pos: int, name: str, reserved: str, other: str, fix: str) -> FilterError:
        return FilterError(
            Code.FLT_NAME,
            self.text,
            pos,
            f"{name!r} is ambiguous: it is {reserved} and also {other}",
            notes=f"{fix}\nnothing is picked silently; no CCDD name can hide a reserved name",
        )

    def _wrong_op(self, p: Predicate, name: str, allowed: Iterable[str]) -> FilterError:
        return FilterError(
            Code.FLT_TYPE,
            self.text,
            p.op_pos,
            f"{p.op!r} is not defined for {name}; it takes {', '.join(sorted(allowed))}",
        )

    def _literal_error(self, lit: Literal, reject: _Reject, what: str) -> FilterError:
        found = suggest(str(lit.value), reject.pool) if lit.kind in (Tok.IDENT, Tok.STRING) else ()
        return FilterError(
            Code.FLT_TYPE, self.text, lit.pos, f"{what}: {reject.reason}", suggestions=found
        )

    def _convert(
        self, p: Predicate, what: str, ops: Iterable[str], convert: Callable[[Literal, str], Any]
    ) -> list[Any]:
        if p.op not in ops:
            raise self._wrong_op(p, what, ops)
        values: list[Any] = []
        for lit in p.values:
            try:
                values.append(convert(lit, p.op))
            except _Reject as reject:
                raise self._literal_error(lit, reject, what) from None
        return values

    # -- predicates ------------------------------------------------------------------

    def _predicate(self, p: Predicate) -> _Node:
        match p.name:
            case (name,):
                return self._bare(p, name)
            case ("hdr", field_name):
                return self._header(p, field_name)
            case ("event", part):
                return self._event(p, part)
            case (message_name, field_name):
                return self._qualified(p, message_name, field_name)
            case _:
                raise self._name_error(
                    p.name_pos[2],
                    f"unknown name {'.'.join(p.name)[:64]!r}: at most one '.' is allowed",
                    (),
                )

    def _bare(self, p: Predicate, name: str) -> _Node:
        located = self.env.fields_named(name)
        if name in RESERVED_NAMES:
            if located:
                shown = ", ".join(sorted({loc.message.name for loc in located})[:3])
                raise self._collision(
                    p.pos,
                    name,
                    (SCALAR_NAMES.get(name) or NAMESPACES[name]),
                    f"a field of {shown}",
                    f"write MSG.{name} for the field (MSG is one of {shown})",
                )
            if name in NAMESPACES:
                raise self._name_error(
                    p.pos, f"{name!r} is a namespace: write {name}.<name>", (), ""
                )
            return self._scalars[name](p)
        if not located:
            pool = [*RESERVED_NAMES, *self.env.field_names()]
            raise self._name_error(p.pos, f"unknown name {name[:64]!r}", pool, name)
        return self._fields(p, name, located)

    def _qualified(self, p: Predicate, message_name: str, field_name: str) -> _Node:
        messages = self.env.named(message_name)
        if message_name in RESERVED_NAMES:
            if messages:
                raise self._collision(
                    p.pos,
                    message_name,
                    (SCALAR_NAMES.get(message_name) or NAMESPACES[message_name]),
                    "the name of a message",
                    f"a message called {message_name!r} is reachable by its bare field names only",
                )
            raise self._name_error(p.pos, f"{message_name!r} has no fields to select", (), "")
        if not messages:
            pool = [*self.env.message_names(), *NAMESPACES]
            raise self._name_error(
                p.name_pos[0], f"unknown message {message_name[:64]!r}", pool, message_name
            )
        located = [
            _Located(node, message, i, fld)
            for node, message in messages
            for i, fld in enumerate(message.fields)
            if fld.name == field_name
        ]
        if not located:
            pool = [fld.name for _, message in messages for fld in message.fields]
            raise self._name_error(
                p.name_pos[1],
                f"message {message_name!r} has no field {field_name[:64]!r}",
                pool,
                field_name,
            )
        return self._fields(p, f"{message_name}.{field_name}", located)

    def _fields(self, p: Predicate, name: str, located: Sequence[_Located]) -> _Node:
        """A message field: every message that has it and can compare it with the literal(s).

        A message whose field cannot take the literal (wrong kind, a label its enum lacks, bytes
        of the wrong size) is left out; one that can, but whose range excludes it (``x == 300``
        on a uint8), is kept, where the comparison is simply never true. The filter is an error
        when no message qualifies, or none can ever match.
        """
        rows: list[tuple[str, Message, _Entry]] = []
        first_reject: tuple[_Located, _Reject, Literal | None] | None = None
        first_range: tuple[_Located, str] | None = None
        reachable = False
        for loc in located:
            try:
                test, problem = self._field_test(p, loc)
            except _FieldReject as bad:
                if first_reject is None:
                    first_reject = (loc, bad.reject, bad.literal)
                continue
            rows.append((loc.node, loc.message, _Entry(loc.index, test)))
            if problem is None:
                reachable = True
            elif first_range is None:
                first_range = (loc, problem)
        if not rows:
            assert first_reject is not None
            loc, reject, literal = first_reject
            where = f"{name} (a {loc.field.type.text} in {loc.message.name})"
            if literal is None:
                raise FilterError(Code.FLT_TYPE, self.text, p.op_pos, f"{where}: {reject.reason}")
            raise self._literal_error(literal, reject, where)
        if not reachable:
            assert first_range is not None
            loc, problem = first_range
            where = f"{name} (a {loc.field.type.text} in {loc.message.name})"
            raise FilterError(Code.FLT_TYPE, self.text, p.values[0].pos, f"{where}: {problem}")
        mids = frozenset(message.mid for _, message, _ in rows)
        return _Node(Cost.DECODE, mids, _decode_fn(_freeze(rows)))

    def _field_test(self, p: Predicate, loc: _Located) -> tuple[Test, str | None]:
        """The test of one message's field, and why it can never be true (``None``: it can)."""
        fld = loc.field
        op = p.op
        klass = fld.type.type_class
        allowed = {
            TypeClass.INT: _NUMBER_OPS,
            TypeClass.FLOAT: _NUMBER_OPS,
            TypeClass.CHAR: _WORD_OPS,
            TypeClass.BYTES: _WORD_OPS,
        }[klass]
        if op not in allowed:
            raise _FieldReject(
                _Reject(f"{op!r} is not defined here; it takes {', '.join(sorted(allowed))}"), None
            )
        values: list[Any] = []
        for lit in p.values:
            try:
                values.append(self._field_literal(fld, op, lit))
            except _Reject as reject:
                raise _FieldReject(reject, lit) from None
        problem = None
        if klass is TypeClass.INT:
            lo, hi = fld.type.bit_range
            problem = _out_of_range(op, values, lo, hi)
        return _make_test(op, values), problem

    @staticmethod
    def _field_literal(fld: FieldDef, op: str, lit: Literal) -> Any:
        match fld.type.type_class:
            case TypeClass.INT:
                if lit.kind in (Tok.IDENT, Tok.STRING) and fld.enum is not None:
                    if lit.value in fld.enum:
                        return fld.enum[lit.value]
                    raise _Reject(f"{lit.value[:40]!r} is not a label of {fld.name}", fld.enum)
                if lit.kind in (Tok.IDENT, Tok.STRING):
                    raise _Reject(f"{fld.name} has no enum; expected an integer")
                return _int_literal(lit, op)
            case TypeClass.FLOAT:
                return _float_literal(lit)
            case TypeClass.CHAR:
                return _text_literal(lit)
            case TypeClass.BYTES:
                return _bytes_literal(lit, fld.type.size, op)
        raise AssertionError("unreachable")  # pragma: no cover

    # -- frame level -----------------------------------------------------------------

    def _scalar(
        self,
        p: Predicate,
        name: str,
        ops: Iterable[str],
        convert: Callable[[Literal, str], Any],
        get: Callable[[Frame], Any],
        *,
        optional: bool = False,
    ) -> tuple[_Node, list[Any]]:
        values = self._convert(p, name, ops, convert)
        fn = _frame_fn(get, _make_test(p.op, values), optional=optional)
        return _Node(Cost.FRAME, None, fn), values

    def _seq(self, p: Predicate) -> _Node:
        return self._scalar(p, "seq", _NUMBER_OPS, _int_literal, operator.attrgetter("seq"))[0]

    def _time(self, p: Predicate) -> _Node:
        return self._scalar(
            p,
            "time",
            _NUMBER_OPS,
            lambda lit, op: _time_literal(lit),
            operator.attrgetter("time_ns"),
        )[0]

    def _len(self, p: Predicate) -> _Node:
        return self._scalar(
            p,
            "len",
            _NUMBER_OPS,
            _int_literal,
            operator.attrgetter("orig_len"),
        )[0]

    def _mid(self, p: Predicate) -> _Node:
        node, values = self._scalar(
            p, "mid", _NUMBER_OPS, _int_literal, operator.attrgetter("mid"), optional=True
        )
        self._require_in_range(p, "mid", values, 0, MAX_MID)
        mids = frozenset(values) if p.op in ("==", "in") else None
        return _Node(node.cost, mids, node.fn)

    def _require_in_range(
        self, p: Predicate, what: str, values: Sequence[Any], lo: int, hi: int
    ) -> None:
        problem = _out_of_range(p.op, values, lo, hi)
        if problem is not None:
            raise FilterError(Code.FLT_TYPE, self.text, p.values[0].pos, f"{what}: {problem}")

    def _node(self, p: Predicate) -> _Node:
        table = {name: name for name in self.env.codecs}
        return self._scalar(
            p,
            "node",
            _WORD_OPS,
            lambda lit, op: (
                _text_literal(lit) if op == "contains" else _word_literal(lit, table, fold=False)
            ),
            operator.attrgetter("stream.node"),
        )[0]

    def _link(self, p: Predicate) -> _Node:
        links = self.env.links

        def convert(lit: Literal, op: str) -> str:
            if links is None or op == "contains":
                if lit.kind not in (Tok.IDENT, Tok.STRING):
                    raise _Reject(f"expected a name, found {_found(lit)}")
                return lit.value
            return _word_literal(lit, {name: name for name in links}, fold=False)

        return self._scalar(p, "link", _WORD_OPS, convert, operator.attrgetter("stream.link"))[0]

    def _dir(self, p: Predicate) -> _Node:
        table = {d.value: d for d in Direction}
        return self._scalar(
            p,
            "dir",
            _SET_OPS,
            lambda lit, op: _word_literal(lit, table, fold=True),
            operator.attrgetter("stream.direction"),
        )[0]

    def _kind(self, p: Predicate) -> _Node:
        table = {k.value: k for k in MessageKind}
        return self._scalar(
            p,
            "kind",
            _SET_OPS,
            lambda lit, op: _word_literal(lit, table, fold=True),
            operator.attrgetter("stream.kind"),
        )[0]

    def _source(self, p: Predicate) -> _Node:
        return self._scalar(
            p,
            "source",
            _WORD_OPS,
            lambda lit, op: _text_literal(lit),
            operator.attrgetter("source"),
        )[0]

    def _truncated(self, p: Predicate) -> _Node:
        return self._flag(p, "truncated", 0x02)

    def _garbage(self, p: Predicate) -> _Node:
        return self._flag(p, "garbage", 0x04)

    def _flag(self, p: Predicate, name: str, bit: int) -> _Node:
        return self._scalar(
            p,
            name,
            EQUALITY_OPS,
            lambda lit, op: _bool_literal(lit),
            lambda frame: bool(int(frame.flags) & bit),
        )[0]

    # -- model level -----------------------------------------------------------------

    def _category(self, p: Predicate) -> _Node:
        table = {c.value: c for c in Category}
        wanted = self._convert(
            p, "category", _SET_OPS, lambda lit, op: _word_literal(lit, table, fold=True)
        )
        test = _make_test(p.op, wanted)
        rows: dict[str, set[int]] = {}
        for node, message in self.env.messages:
            if message.kind is MessageKind.TELEMETRY and test(message.category):
                rows.setdefault(node, set()).add(message.mid)
        by_node = {node: frozenset(mids) for node, mids in rows.items()}
        mids = frozenset().union(*by_node.values())
        if not mids:
            raise FilterError(
                Code.FLT_TYPE,
                self.text,
                p.values[0].pos,
                "category: no telemetry message of the selected nodes has it",
            )

        def fn(frame: Frame, probe: Matcher) -> bool:
            stream = frame.stream
            allowed = by_node.get(stream.node)
            return (
                allowed is not None
                and stream.kind is MessageKind.TELEMETRY
                and frame.mid in allowed
            )

        return _Node(Cost.MODEL, mids, fn)

    def _msg(self, p: Predicate) -> _Node:
        names = set(self.env.message_names())
        table = {name: name for name in names}
        values = self._convert(
            p,
            "msg",
            _WORD_OPS,
            lambda lit, op: (
                _text_literal(lit) if op == "contains" else _word_literal(lit, table, fold=False)
            ),
        )
        test = _make_test(p.op, values)
        telemetry: dict[str, set[int]] = {}
        commands: dict[str, set[str]] = {}
        mids: set[int] = set()
        for node, message in self.env.messages:
            if not test(message.name):
                continue
            mids.add(message.mid)
            if message.kind is MessageKind.TELEMETRY:
                telemetry.setdefault(node, set()).add(message.mid)
            else:
                commands.setdefault(node, set()).add(message.name)
        if not mids:
            lit = p.values[0]
            raise FilterError(
                Code.FLT_TYPE, self.text, lit.pos, "msg: no message of the selected nodes matches"
            )
        tlm = {node: frozenset(found) for node, found in telemetry.items()}
        cmd = {node: frozenset(found) for node, found in commands.items()}

        def fn(frame: Frame, probe: Matcher) -> bool:
            stream = frame.stream
            if stream.kind is MessageKind.TELEMETRY:
                allowed = tlm.get(stream.node)
                return allowed is not None and frame.mid in allowed
            named = cmd.get(stream.node)
            if named is None:
                return False
            resolved = probe.resolved(frame)
            return not isinstance(resolved, DecodeError) and resolved.message.name in named

        return _Node(Cost.MODEL, frozenset(mids), fn)

    # -- check level -----------------------------------------------------------------

    def _status(self, p: Predicate) -> _Node:
        values = self._convert(
            p, "status", _SET_OPS, lambda lit, op: _word_literal(lit, _STATUS, fold=True)
        )
        test = _make_test(p.op, values)

        def fn(frame: Frame, probe: Matcher) -> bool:
            resolved = probe.resolved(frame)
            if not isinstance(resolved, DecodeError):
                return test("ok")
            return test("unknown" if resolved.code is Code.DEC_UNKNOWN else "malformed")

        return _Node(Cost.CHECK, None, fn)

    # -- decode level ----------------------------------------------------------------

    def _fc(self, p: Predicate) -> _Node:
        values = self._convert(p, "fc", _NUMBER_OPS, _int_literal)
        test = _make_test(p.op, values)
        mids_by_node = {
            node: frozenset(m.mid for m in codec.model.commands())
            for node, codec in self.env.codecs.items()
        }
        all_mids = frozenset().union(*mids_by_node.values())
        if not all_mids:
            raise FilterError(
                Code.FLT_NAME,
                self.text,
                p.pos,
                "fc: no selected node defines commands, so no frame has a function code",
            )

        def fn(frame: Frame, probe: Matcher) -> bool:
            allowed = mids_by_node.get(frame.stream.node)
            if allowed is None or frame.mid not in allowed:
                return False
            decoded = probe.decoded(frame)
            if isinstance(decoded, DecodeError):
                return False
            code = decoded.packet.message.function_code
            return code is not None and test(code)

        return _Node(Cost.DECODE, all_mids, fn)

    def _htime(self, p: Predicate) -> _Node:
        raise self._name_error(
            p.pos, "htime (header time) is reserved and not available in this build", (), ""
        )

    def _header(self, p: Predicate, field_name: str) -> _Node:
        if self.env.named("hdr"):
            raise self._collision(
                p.pos,
                "hdr",
                NAMESPACES["hdr"],
                "the name of a message",
                "a message called 'hdr' is reachable by its bare field names only",
            )
        widest = self.env.header_fields()
        if field_name not in widest:
            raise self._name_error(
                p.name_pos[1],
                f"no header field {field_name[:64]!r} in the header profiles in use",
                widest,
                field_name,
            )
        limit = widest[field_name]
        values = self._convert(p, f"hdr.{field_name}", _NUMBER_OPS, _int_literal)
        self._require_in_range(p, f"hdr.{field_name}", values, 0, limit)
        test = _make_test(p.op, values)

        def fn(frame: Frame, probe: Matcher) -> bool:
            decoded = probe.decoded(frame)
            if isinstance(decoded, DecodeError):
                return False
            value = decoded.packet.header.values.get(field_name)
            return value is not None and test(value)

        return _Node(Cost.DECODE, None, fn)

    def _event(self, p: Predicate, part: str) -> _Node:
        if self.env.named("event"):
            raise self._collision(
                p.pos,
                "event",
                NAMESPACES["event"],
                "the name of a message",
                "a message called 'event' is reachable by its bare field names only",
            )
        if part not in EVENT_PARTS:
            raise self._name_error(p.name_pos[1], f"no event part {part[:64]!r}", EVENT_PARTS, part)
        what = f"event.{part}"
        severities = {name.lower(): sev for name, sev in SEVERITY_LABELS.items()}
        match part:
            case "id":
                values = self._convert(p, what, _NUMBER_OPS, _int_literal)
            case "type":
                values = self._convert(
                    p, what, _NUMBER_OPS, lambda lit, op: _word_literal(lit, severities, fold=True)
                )
            case _:
                values = self._convert(p, what, _WORD_OPS, lambda lit, op: _text_literal(lit))
        test = _make_test(p.op, values)
        rows: list[tuple[str, Message, _Entry]] = []
        for node, message in self.env.messages:
            spec = message.event
            if spec is None:
                continue
            field_name = {"app": spec.app, "id": spec.id, "type": spec.type, "text": spec.message}[
                part
            ]
            if field_name is None:
                continue
            index = next(i for i, f in enumerate(message.fields) if f.name == field_name)
            if part == "type":
                rows.append((node, message, _Entry(index, _severity_test(spec.severities, test))))
            else:
                rows.append((node, message, _Entry(index, test)))
        if not rows:
            raise FilterError(
                Code.FLT_NAME,
                self.text,
                p.pos,
                f"{what}: no event message of the selected nodes has it",
            )
        return _Node(Cost.DECODE, frozenset(m.mid for _, m, _ in rows), _decode_fn(_freeze(rows)))


def _severity_test(severities: Mapping[int, EventSeverity], test: Test) -> Test:
    def check(value: int) -> bool:
        severity = severities.get(value)
        return severity is not None and test(severity)

    return check


class _FieldReject(Exception):
    def __init__(self, reject: _Reject, literal: Literal | None) -> None:
        super().__init__(reject.reason)
        self.reject = reject
        self.literal = literal


def compile_filter(
    text: str, codecs: Mapping[str, Codec], *, links: Iterable[str] | None = None
) -> Filter:
    """Compile ``text`` against the codecs of the nodes a run uses (node name to codec).

    ``links`` are the link names ``link == ...`` may name (``None``: any). The returned
    :class:`Filter` is valid for frames decoded with these same codecs. Raises
    :class:`FilterError` (``IB-FLT-001..004``) and nothing else.
    """
    tree = parse(text)
    try:
        root = _Compiler(text, codecs, links).compile(tree)
    except RecursionError:
        raise FilterError(
            Code.FLT_LIMIT, text, 0, f"nesting deeper than {MAX_DEPTH} levels"
        ) from None
    return Filter(text, root.mids, root.cost is Cost.DECODE, _root(root), root.cost)


def _root(root: _Node) -> MatchFn:
    mids = root.mids
    if mids is None:
        return root.fn
    tree = root.fn

    def candidates_only(frame: Frame, probe: Matcher) -> bool:
        return frame.mid in mids and tree(frame, probe)

    return candidates_only
