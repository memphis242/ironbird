"""Static analysis for the "no automatic deletion" guard: which calls in a source file remove,
rename, link, truncate or overwrite files, run other programs, or load ``ctypes``, with import
aliases resolved (``import os as o``, ``from os import unlink as u``) and the enclosing function
named.

Categories: ``removal``, ``rename`` (also link, copy-over, ``Path.replace``, rotating log
handlers), ``truncation`` (``open`` in a write mode or with a mode the analysis cannot read,
``O_TRUNC``, ``ftruncate``), ``overwrite`` (in-place modes such as ``r+``, ``O_RDWR`` or
``O_WRONLY`` without ``O_EXCL``, ``pwrite``), ``subprocess`` (another program may touch files),
``dynamic`` (an attribute of ``os``/``shutil``/``pathlib``/``io`` looked up by a computed name,
``vars()``, ``importlib``, ``__import__``), ``ctypes`` and ``tempfile``.

Known gaps, reviewed by hand instead (see the plan): a function passed in as a value from outside
the analysed file, C extensions and third-party code (pyserial in 2k). Nothing here replaces the
review; it makes the review's subject small and keeps it from growing unnoticed.
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
RENAME_FUNCS = {
    "os.rename",
    "os.renames",
    "os.replace",
    "os.link",
    "shutil.move",
    "shutil.copyfile",
    "shutil.copy",
    "shutil.copy2",
    "shutil.copymode",
    "shutil.copystat",
    "logging.handlers.RotatingFileHandler",
    "logging.handlers.TimedRotatingFileHandler",
}
RENAME_ATTRS = {
    "rename",
    "hardlink_to",
    "rename_at",
    "link_at",
    "RotatingFileHandler",
    "TimedRotatingFileHandler",
}
TRUNCATE_FUNCS = {"os.truncate", "os.ftruncate"}
TRUNCATE_ATTRS = {"truncate", "ftruncate", "write_text", "write_bytes"}
TEMPFILE_NAMES = {"tempfile.NamedTemporaryFile", "tempfile.TemporaryDirectory"}
#: Constructors whose second positional argument (or ``mode=``) is an ``open``-style mode.
OPEN_LIKE = {
    "open",
    "io.open",
    "io.FileIO",
    "gzip.open",
    "bz2.open",
    "lzma.open",
    "codecs.open",
    "tarfile.open",
    "zipfile.ZipFile",
    "logging.FileHandler",
    "logging.handlers.WatchedFileHandler",
}
SUBPROCESS_FUNCS = {
    "os.system",
    "os.popen",
    "os.execv",
    "os.execve",
    "os.execvp",
    "os.execl",
    "os.execlp",
    "os.spawnv",
    "os.spawnl",
    "os.posix_spawn",
    "os.fork",
    "os.forkpty",
}
DYNAMIC_MODULES = {"os", "shutil", "pathlib", "io", "ctypes", "tempfile"}
WRITE_FLAGS = {"O_RDWR", "O_WRONLY"}
PATH_CLASSES = {"pathlib.Path", "pathlib.PurePath", "pathlib.PosixPath", "Path"}


@dataclass(frozen=True, slots=True)
class Finding:
    category: str  # see the module docstring
    where: str  # enclosing qualified function name ("" at module level)
    what: str  # the call as written, resolved

    def key(self) -> tuple[str, str]:
        return self.category, self.where


class _Visitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self.aliases: dict[str, str] = {}
        self.stack: list[str] = []
        self.findings: list[Finding] = []
        self._flag_exprs: set[int] = set()  # ids of ``a | b`` nodes already judged as a whole

    # -- imports ----

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            self.aliases[alias.asname or alias.name.split(".")[0]] = (
                alias.name if alias.asname else alias.name.split(".")[0]
            )
            top = alias.name.split(".")[0]
            if top == "ctypes":
                self.add("ctypes", f"import {alias.name}")
            if top == "subprocess":
                self.add("subprocess", f"import {alias.name}")
            if alias.name == "importlib" or top == "importlib":
                self.add("dynamic", f"import {alias.name}")

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        module = node.module or ""
        top = module.split(".")[0]
        if top == "ctypes":
            self.add("ctypes", f"from {module} import ...")
        if top == "subprocess":
            self.add("subprocess", f"from {module} import ...")
        if top == "importlib":
            self.add("dynamic", f"from {module} import ...")
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

    # -- helpers ----

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
        if target in SUBPROCESS_FUNCS or (target or "").startswith("subprocess."):
            return "subprocess"
        if attr == "pwrite" or target == "os.pwrite":
            return "overwrite"
        return None

    # -- references ----

    def visit_Attribute(self, node: ast.Attribute) -> None:
        # References count as well as calls: ``self._call(fs.ftruncate, ...)`` passes the
        # function as a value, and must not escape the guard.
        target = self.dotted(node)
        if node.attr == "O_TRUNC":
            self.add("truncation", "O_TRUNC")
        elif node.attr in WRITE_FLAGS and target is not None and target.startswith("os."):
            self._judge_flags(node, {node.attr})
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
        elif origin.rpartition(".")[2] in WRITE_FLAGS and origin.startswith("os."):
            self._judge_flags(node, {origin.rpartition(".")[2]})
        elif origin.split(".")[0] == "ctypes":
            self.add("ctypes", origin)
        elif "." in origin:  # a name imported from a module: ``from os import unlink as u``
            category = self._classify(origin, None)
            if category is not None:
                self.add(category, origin)

    def visit_BinOp(self, node: ast.BinOp) -> None:
        # ``os.O_WRONLY | os.O_CREAT | os.O_EXCL`` is judged as one expression: a write flag is
        # fine next to ``O_EXCL`` (a new file) and an in-place overwrite otherwise.
        if isinstance(node.op, ast.BitOr) and id(node) not in self._flag_exprs:
            flags = self._flag_names(node)
            if flags & WRITE_FLAGS:
                self._flag_exprs.update(id(inner) for inner in ast.walk(node))
                if "O_EXCL" not in flags:
                    self.add("overwrite", "|".join(sorted(flags)))
        self.generic_visit(node)

    def _flag_names(self, node: ast.expr) -> set[str]:
        names: set[str] = set()
        for inner in ast.walk(node):
            if isinstance(inner, ast.Attribute) and inner.attr.startswith("O_"):
                names.add(inner.attr)
            elif isinstance(inner, ast.Name):
                origin = self.aliases.get(inner.id, "")
                if origin.startswith("os.O_"):
                    names.add(origin.rpartition(".")[2])
        return names

    def _judge_flags(self, node: ast.expr, flags: set[str]) -> None:
        if id(node) in self._flag_exprs:
            return
        if "O_EXCL" not in flags:
            self.add("overwrite", "|".join(sorted(flags)))

    # -- calls ----

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        target = self.dotted(func)
        attr = func.attr if isinstance(func, ast.Attribute) else None
        if target == "os.open":
            self._check_os_open(node)
        elif target in OPEN_LIKE:
            self._check_mode(node, target or "open", position=1, strict=True)
        elif attr == "open":  # ``Path.open(mode)`` and its kin: the mode comes first
            receiver = (
                self.dotted(func.value.func)
                if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Call)
                else None
            )
            self._check_mode(node, "open", position=0, strict=receiver in PATH_CLASSES)
        if attr == "replace" and len(node.args) == 1 and not node.keywords:
            # ``str.replace`` takes two arguments; one argument is ``Path.replace(target)``
            self.add("rename", "replace")
        if (attr == "copytree" or target == "shutil.copytree") and any(
            kw.arg == "dirs_exist_ok" for kw in node.keywords
        ):
            self.add("rename", "shutil.copytree(dirs_exist_ok=...)")
        self._check_dynamic(node, target)
        self.generic_visit(node)

    def _check_mode(self, call: ast.Call, label: str, *, position: int, strict: bool) -> None:
        """Judge the ``mode`` of an ``open``-like call: write modes truncate, ``+`` modes
        overwrite in place, a mode the analysis cannot read needs a human (``strict``: the call
        is known to open a file, so a computed mode is flagged; any other ``.open`` method is
        judged by its constant strings only)."""
        modes = [call.args[position]] if position < len(call.args) else []
        modes += [kw.value for kw in call.keywords if kw.arg == "mode"]
        for mode in modes:
            if isinstance(mode, ast.Constant) and isinstance(mode.value, str):
                if "w" in mode.value:
                    self.add("truncation", f"{label}(..., 'w')")
                elif "+" in mode.value:
                    self.add("overwrite", f"{label}(..., '{mode.value}')")
            elif strict and not isinstance(mode, ast.Constant):
                self.add("truncation", f"{label}(..., <mode not constant>)")

    def _check_os_open(self, call: ast.Call) -> None:
        """``os.open``: numeric flags hide ``O_TRUNC``; flags passed in as a value cannot be
        read here (the write flags are judged where the constant is built)."""
        flags = call.args[1] if len(call.args) > 1 else None
        for kw in call.keywords:
            if kw.arg == "flags":
                flags = kw.value
        if flags is None:
            return
        if any(isinstance(n, ast.Constant) and isinstance(n.value, int) for n in ast.walk(flags)):
            self.add("truncation", "os.open(..., <numeric flags>)")
        elif not self._flag_names(flags) and not isinstance(flags, ast.Constant):
            self.add("overwrite", "os.open(..., <flags not readable>)")

    def _check_dynamic(self, call: ast.Call, target: str | None) -> None:
        func = call.func
        if isinstance(func, ast.Name) and func.id in ("getattr", "vars", "__import__"):
            if func.id == "__import__":
                self.add("dynamic", "__import__")
            elif call.args:
                origin = self.dotted(call.args[0])
                module = (origin or "").split(".")[0]
                if func.id == "vars" and module in DYNAMIC_MODULES:
                    self.add("dynamic", f"vars({origin})")
                elif func.id == "getattr" and module in DYNAMIC_MODULES:
                    name = call.args[1] if len(call.args) > 1 else None
                    constant = isinstance(name, ast.Constant) and isinstance(name.value, str)
                    if not constant:
                        self.add("dynamic", f"getattr({origin}, <computed>)")
                    else:
                        assert isinstance(name, ast.Constant)
                        category = self._classify(f"{origin}.{name.value}", str(name.value))
                        if category is not None:
                            self.add(category, f"getattr({origin}, {name.value!r})")
        if target is not None and target.startswith("importlib."):
            self.add("dynamic", target)


def analyze(source: str) -> list[Finding]:
    visitor = _Visitor()
    visitor.visit(ast.parse(source))
    return visitor.findings
