"""Stage 2d, layer 4: the static guard behind "nothing is deleted automatically", and the check
that every ledger record the code base writes has a tier.

No code in ``src/`` removes, renames, links or truncates a file except where it is listed here,
and every listed place is one of: the confirmed prune's delete path, a process's own temporary
file on the failure path of the operation that created it, the second step of the non-replacing
rename, the completion of an interrupted rename in recovery, the line cut-back / reserve cut /
notices rewrite helpers, and the real implementation of each in the file system seam.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from conftest import REPO
from guard import Finding, analyze
from ironbird.ledger import DYNAMIC_RECORD_NAMES, RECORD_TIERS

SRC = REPO / "src" / "ironbird"

# (file relative to src/ironbird, category) -> the enclosing functions allowed to do it
ALLOWED: dict[tuple[str, str], set[str]] = {
    # the real file system: one system call per method
    ("fsops.py", "removal"): {
        "FileSystem.unlink_at",
        "FileSystem.unlink_path",
        "FileSystem.rmdir_at",
    },
    ("fsops.py", "rename"): {"FileSystem.link_at", "FileSystem.rename_at"},
    ("fsops.py", "truncation"): {"FileSystem.ftruncate"},
    ("fsops.py", "ctypes"): {
        "",
        "FileSystem.fstatfs_type",
        "FileSystem.renameat2_noreplace",
        "_libc",
    },
    # the confirmed prune
    ("prune.py", "removal"): {"_delete_session"},
    # a process's own temporary file on its failure path; the second step of rename_noreplace
    ("atomicfile.py", "removal"): {"_link", "_placeholder_rename", "TempOutput.__exit__"},
    ("atomicfile.py", "rename"): {"_link", "_placeholder_rename", "TempOutput.commit"},
    # recovery: completing an interrupted rename, cutting a reserve at its last record
    ("retention.py", "removal"): {"_recover_reserve"},
    ("retention.py", "truncation"): {"_commit_recovered"},
    # the ledger: cutting a part back to its last line, a reserve to its header or last record
    ("ledgerstore.py", "truncation"): {
        "LedgerStore._cut_back",
        "LedgerStore._reserve_blocks",
        "LedgerStore._retire",
        "LedgerStore._finish_files",
    },
    # the notices file is rewritten in place
    ("retnotices.py", "truncation"): {"Notices._transaction"},
    # the recorder removes the partial file it created itself when a start fails
    ("recording/writer.py", "removal"): {"_discard_partial"},
}


def allowed_for(path: str, category: str) -> set[str]:
    return ALLOWED.get((path, category), set())


def findings_by_file() -> dict[str, list[Finding]]:
    out: dict[str, list[Finding]] = {}
    for path in sorted(SRC.rglob("*.py")):
        found = analyze(path.read_text())
        if found:
            out[path.relative_to(SRC).as_posix()] = found
    return out


def test_every_removal_rename_link_and_truncation_in_src_is_on_the_allowlist() -> None:
    unexpected = [
        f"{path}: {f.category} in {f.where or '<module>'}: {f.what}"
        for path, findings in findings_by_file().items()
        for f in findings
        if f.where not in allowed_for(path, f.category)
    ]
    assert not unexpected, "\n".join(unexpected)


def test_nothing_in_src_uses_tempfile_deleting_helpers_or_o_trunc_or_write_text() -> None:
    for path, findings in findings_by_file().items():
        for f in findings:
            assert f.category != "tempfile", (path, f)
            assert f.what not in ("O_TRUNC", "write_text", "write_bytes"), (path, f)


def test_ctypes_is_used_only_by_the_file_system_seam() -> None:
    users = {
        path
        for path, findings in findings_by_file().items()
        if any(f.category == "ctypes" for f in findings)
    }
    assert users == {"fsops.py"}


def test_every_allowlist_entry_is_still_used() -> None:
    """A stale entry would silently widen what is allowed."""
    seen = {
        (path, f.category, f.where)
        for path, findings in findings_by_file().items()
        for f in findings
    }
    stale = [
        (path, category, where)
        for (path, category), wheres in ALLOWED.items()
        for where in wheres
        if (path, category, where) not in seen
    ]
    assert not stale, stale


# -- the analysis itself: mutation cases ----


@pytest.mark.parametrize(
    ("source", "category"),
    [
        ("import os\ndef f(p):\n    os.unlink(p)", "removal"),
        ("import os as o\ndef f(p):\n    o.unlink(p)", "removal"),
        ("from os import unlink as u\ndef f(p):\n    u(p)", "removal"),
        ("from os import remove\ndef f(p):\n    remove(p)", "removal"),
        ("import os\ndef f(p):\n    os.rmdir(p)", "removal"),
        ("import os\ndef f(p):\n    os.removedirs(p)", "removal"),
        ("import shutil\ndef f(p):\n    shutil.rmtree(p)", "removal"),
        ("import shutil as sh\ndef f(p):\n    sh.rmtree(p)", "removal"),
        ("from pathlib import Path\ndef f(p):\n    Path(p).unlink()", "removal"),
        ("def f(p):\n    p.rmdir()", "removal"),
        ("import os\ndef f(a, b):\n    os.rename(a, b)", "rename"),
        ("import os\ndef f(a, b):\n    os.replace(a, b)", "rename"),
        ("from os import replace as r\ndef f(a, b):\n    r(a, b)", "rename"),
        ("import os\ndef f(a, b):\n    os.link(a, b)", "rename"),
        ("import shutil\ndef f(a, b):\n    shutil.move(a, b)", "rename"),
        ("def f(p, q):\n    p.rename(q)", "rename"),
        ("def f(p, q):\n    p.hardlink_to(q)", "rename"),
        ("import os\ndef f(p):\n    os.truncate(p, 0)", "truncation"),
        ("import os\ndef f(fd):\n    os.ftruncate(fd, 0)", "truncation"),
        ("import os\ndef f(p):\n    os.open(p, os.O_WRONLY | os.O_TRUNC)", "truncation"),
        ("from os import O_TRUNC as T\ndef f():\n    return T", "truncation"),
        ("def f(p):\n    open(p, 'w')", "truncation"),
        ("def f(p):\n    p.open(mode='w+')", "truncation"),
        ("def f(p):\n    p.write_text('x')", "truncation"),
        ("def f(p):\n    p.write_bytes(b'x')", "truncation"),
        ("import ctypes\ndef f():\n    pass", "ctypes"),
        ("from ctypes import CDLL\ndef f():\n    pass", "ctypes"),
        ("import tempfile\ndef f():\n    tempfile.NamedTemporaryFile()", "tempfile"),
        ("import tempfile as t\ndef f():\n    t.TemporaryDirectory()", "tempfile"),
    ],
)
def test_the_guard_flags_every_removal_form_with_aliases_resolved(
    source: str, category: str
) -> None:
    assert category in {f.category for f in analyze(source)}, source


@pytest.mark.parametrize(
    "source",
    [
        "def f(items, x):\n    items.remove(x)",
        "def f(text):\n    return text.replace('a', 'b')",
        "def f(p):\n    return open(p)",
        "def f(p):\n    return open(p, 'rb')",
        "import os\ndef f(p):\n    os.stat(p)",
    ],
)
def test_the_guard_leaves_harmless_calls_alone(source: str) -> None:
    assert analyze(source) == []


def test_the_guard_reports_the_enclosing_function() -> None:
    found = analyze("import os\nclass C:\n    def m(self, p):\n        os.unlink(p)")
    assert found == [Finding("removal", "C.m", "os.unlink")]


# -- the tier of every record ----


def _record_names(tree: ast.AST) -> list[tuple[str, int]]:
    """Literal event names of ``.record("kind", "name", ...)`` calls (f-strings as patterns)."""
    names: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
            continue
        if node.func.attr == "record" and len(node.args) >= 2:
            arg = node.args[1]
        elif node.func.attr in ("record_tx",) and node.args:
            arg = node.args[0]
        else:
            continue
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            names.append((arg.value, node.lineno))
        elif isinstance(arg, ast.JoinedStr):
            pattern = "".join(
                str(v.value) if isinstance(v, ast.Constant) else "*" for v in arg.values
            )
            names.append((pattern, node.lineno))
        # a code's value (Code.X.value) names an error-coded record: tiered by code
    return names


def test_every_record_name_in_src_has_a_tier() -> None:
    missing: list[str] = []
    for path in sorted(SRC.rglob("*.py")):
        for name, line in _record_names(ast.parse(path.read_text())):
            known = name in RECORD_TIERS or name in DYNAMIC_RECORD_NAMES
            if not known:
                missing.append(f"{path.relative_to(SRC)}:{line}: {name!r}")
    assert not missing, "\n".join(missing)


def test_the_tier_table_has_no_stale_names() -> None:
    written: set[str] = set()
    for path in sorted(SRC.rglob("*.py")):
        text = path.read_text()
        for name in RECORD_TIERS:
            if f'"{name}"' in text:
                written.add(name)
    # names the ledger writes itself through other paths (the store, summaries, warnings)
    internal = {
        "ledger.open",
        "ledger.part",
        "ledger.reserve",
        "ledger.resumed",
        "ledger.stalled",
        "ledger.dropped",
        "ledger.summary",
        "ledger.tier_over",
        "ledger.tier_gap",
        "ledger.name_taken",
        "ledger.warning",
        "ledger.note",
        "ledger.recovered",
        "retention.scan",
        "retention.warning",
        "rx.malformed.summary",
        "rx.command.summary",
        "prune.plan",
        "prune.confirmed",
        "prune.declined",
        "prune.delete",
        "prune.gone",
        "prune.skip",
        "respond.replies",
        "recording.start",
        "recording.stop",
        "recording.part",
        "recording.failed",
    }
    stale = set(RECORD_TIERS) - written - internal
    assert not stale, sorted(stale)


def test_the_tiers_of_the_documented_examples() -> None:
    from ironbird.ledgertiers import Tier

    assert RECORD_TIERS["rx.event"] is Tier.EVENTS
    for name in ("rx.malformed", "rx.first", "rx.command"):
        assert RECORD_TIERS[name] is Tier.EVIDENCE
    for name in ("tx.command", "tx.telemetry", "cli.invoke", "ledger.open", "prune.delete"):
        assert RECORD_TIERS[name] is Tier.ESSENTIAL


def test_the_example_source_files_exist() -> None:
    assert Path(SRC / "fsops.py").is_file()
