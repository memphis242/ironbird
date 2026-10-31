"""Warning codes are reported, never raised: a raised error decides the exit code."""

from __future__ import annotations

import ast
from pathlib import Path

from ironbird.errors import WARNING_CODES, Code

SRC = Path(__file__).resolve().parent.parent / "src"
WARNING_NAMES: frozenset[str] = frozenset(code.name for code in WARNING_CODES)


def _raised_warning_codes(tree: ast.AST) -> list[int]:
    """Line numbers of ``raise IronbirdError(Code.<warning code>, ...)`` statements."""
    lines: list[int] = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Raise) and isinstance(node.exc, ast.Call)):
            continue
        call = node.exc
        func = call.func
        name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", "")
        if name != "IronbirdError" or not call.args:
            continue
        first = call.args[0]
        if isinstance(first, ast.Attribute) and first.attr in WARNING_NAMES:
            lines.append(node.lineno)
    return lines


def test_scanner_finds_a_raised_warning_code() -> None:
    assert WARNING_NAMES, "there is at least one warning code"
    name = sorted(WARNING_NAMES)[0]
    bad = ast.parse(f"def f():\n    raise IronbirdError(Code.{name}, 'x')\n")
    assert _raised_warning_codes(bad) == [2]
    ok = ast.parse(f"def f():\n    report(IronbirdError(Code.{name}, 'x'))\n")
    assert _raised_warning_codes(ok) == []
    assert Code.TX_SEND.name not in WARNING_NAMES


def test_no_warning_code_is_raised_in_src() -> None:
    found: list[str] = []
    for path in sorted(SRC.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        found.extend(f"{path.relative_to(SRC)}:{line}" for line in _raised_warning_codes(tree))
    assert not found, f"warning codes must be reported, not raised: {found}"
