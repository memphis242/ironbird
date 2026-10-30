"""Static analysis for the "no automatic deletion" guard: which calls in a source file remove,
rename, link or truncate files, or load ``ctypes``, with import aliases resolved
(``import os as o``, ``from os import unlink as u``) and the enclosing function named.

Known gaps, reviewed by hand instead (see the plan): dynamic access (``getattr(os, name)``, a
function passed in as a value), C extensions and third-party code (pyserial in 2k), and child
processes (``src/`` runs none that touch files).
"""

from __future__ import annotations

import ast
from dataclasses import dataclass

REMOVAL_FUNCS = {
    "os.unlink",
    "os.remove",
    "os.rmdir",
    "os.removedirs",
    "shutil.rmtree",
}
REMOVAL_ATTRS = {"unlink", "rmdir", "unlink_at", "unlink_path", "rmdir_at"}
RENAME_FUNCS = {"os.rename", "os.renames", "os.replace", "os.link", "shutil.move"}
RENAME_ATTRS = {"rename", "hardlink_to", "rename_at", "link_at"}
TRUNCATE_FUNCS = {"os.truncate", "os.ftruncate"}
TRUNCATE_ATTRS = {"truncate", "ftruncate", "write_text", "write_bytes"}
TEMPFILE_NAMES = {"tempfile.NamedTemporaryFile", "tempfile.TemporaryDirectory"}


@dataclass(frozen=True, slots=True)
class Finding:
    category: str  # "removal" | "rename" | "truncation" | "ctypes" | "tempfile"
    where: str  # enclosing qualified function name ("" at module level)
    what: str  # the call as written, resolved

    def key(self) -> tuple[str, str]:
        return self.category, self.where


class _Visitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self.aliases: dict[str, str] = {}
        self.stack: list[str] = []
        self.findings: list[Finding] = []

    # -- imports ----

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            self.aliases[alias.asname or alias.name.split(".")[0]] = (
                alias.name if alias.asname else alias.name.split(".")[0]
            )
            if alias.name.split(".")[0] == "ctypes":
                self.add("ctypes", f"import {alias.name}")

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        module = node.module or ""
        if module.split(".")[0] == "ctypes":
            self.add("ctypes", f"from {module} import ...")
        for alias in node.names:
            self.aliases[alias.asname or alias.name] = f"{module}.{alias.name}"

    # -- scopes ----

    def _scoped(self, node: ast.AST, name: str) -> None:
        self.stack.append(name)
        self.generic_visit(node)
        self.stack.pop()

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self._scoped(node, node.name)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._scoped(node, node.name)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._scoped(node, node.name)

    # -- the calls ----

    def add(self, category: str, what: str) -> None:
        self.findings.append(Finding(category, ".".join(self.stack), what))

    def dotted(self, node: ast.expr) -> str | None:
        """The dotted origin of a name or attribute chain, aliases resolved."""
        parts: list[str] = []
        while isinstance(node, ast.Attribute):
            parts.append(node.attr)
            node = node.value
        if isinstance(node, ast.Name):
            head = self.aliases.get(node.id, node.id)
            return ".".join([head, *reversed(parts)])
        return None

    def _classify(self, target: str | None, attr: str | None) -> str | None:
        """Which category a reference to ``target`` (resolved) or ``.attr`` belongs to."""
        if target in REMOVAL_FUNCS or attr in REMOVAL_ATTRS:
            return "removal"
        if target in RENAME_FUNCS or attr in RENAME_ATTRS:
            return "rename"
        if target in TRUNCATE_FUNCS or attr in TRUNCATE_ATTRS:
            return "truncation"
        if target in TEMPFILE_NAMES:
            return "tempfile"
        return None

    def visit_Attribute(self, node: ast.Attribute) -> None:
        # References count as well as calls: ``self._call(fs.ftruncate, ...)`` passes the
        # function as a value, and must not escape the guard.
        target = self.dotted(node)
        if node.attr == "O_TRUNC":
            self.add("truncation", "O_TRUNC")
        elif (
            target is not None
            and target.split(".")[0] == "ctypes"
            and isinstance(node.value, ast.Name)
        ):
            self.add("ctypes", target)
        else:
            category = self._classify(target, node.attr)
            if category is not None:
                self.add(category, target or node.attr)
        self.generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:
        origin = self.aliases.get(node.id, "")
        if origin.endswith(".O_TRUNC"):
            self.add("truncation", "O_TRUNC")
        elif origin.split(".")[0] == "ctypes":
            self.add("ctypes", origin)
        elif "." in origin:  # a name imported from a module: ``from os import unlink as u``
            category = self._classify(origin, None)
            if category is not None:
                self.add(category, origin)

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        target = self.dotted(func)
        attr = func.attr if isinstance(func, ast.Attribute) else None
        if (target in ("open", "io.open") or attr == "open") and self._write_mode(node):
            self.add("truncation", f"{target or attr}(..., 'w')")
        self.generic_visit(node)

    @staticmethod
    def _write_mode(call: ast.Call) -> bool:
        candidates: list[ast.expr] = list(call.args[1:2])
        candidates += [kw.value for kw in call.keywords if kw.arg == "mode"]
        return any(
            isinstance(c, ast.Constant) and isinstance(c.value, str) and "w" in c.value
            for c in candidates
        )


def analyze(source: str) -> list[Finding]:
    visitor = _Visitor()
    visitor.visit(ast.parse(source))
    return visitor.findings
