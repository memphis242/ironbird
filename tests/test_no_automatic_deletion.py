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
    ("retention.py", "removal"): {"_complete_interrupted_rename"},
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
    # in-place writes (a write flag without O_EXCL, ``pwrite``): the file system seam itself, the
    # reserve written inside its allocated extent, the notices file, the final ledger part
    # appended to after close (O_APPEND), the descriptors 0-2 re-opened on /dev/null, and the
    # recording created with O_EXCL (the flags are built in a local the analysis cannot follow)
    ("fsops.py", "overwrite"): {
        "",
        "FileSystem.open_dir",
        "FileSystem.open_at",
        "FileSystem.pwrite",
    },
    ("ledgerstore.py", "overwrite"): {"LedgerStore._pwrite_all"},
    ("retnotices.py", "overwrite"): {"Notices._transaction"},
    ("ledger.py", "overwrite"): {"Ledger._append_final"},
    # a salvage's own leftover temporary file, picked up again and appended to (checked first)
    ("atomicfile.py", "overwrite"): {""},
    # attribute lookups by a computed name on the program's own record types
    ("config.py", "dynamic"): {"parse_config"},
    ("frame.py", "dynamic"): {"Frame._replace"},
    ("cli/main.py", "overwrite"): {"_fill_standard_descriptors"},
    ("recording/writer.py", "overwrite"): {"create_exclusive"},
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
        # review findings: forms that used to escape
        ("from pathlib import Path\ndef f(p, q):\n    Path(p).replace(q)", "rename"),
        ("def f(p, q):\n    p.replace(q)", "rename"),
        ("from pathlib import Path\ndef f(p):\n    Path(p).open('w')", "truncation"),
        ("from pathlib import Path\ndef f(p):\n    Path(p).open(mode='w')", "truncation"),
        ("def f(p):\n    m = 'w'\n    open(p, m)", "truncation"),
        ("def f(p, m):\n    open(p, mode=m)", "truncation"),
        ("from pathlib import Path\ndef f(p, m):\n    Path(p).open(m)", "truncation"),
        ("def f(p):\n    open(p, 'r+').write('x')", "overwrite"),
        ("def f(p):\n    open(p, 'rb+')", "overwrite"),
        ("import os\ndef f(p):\n    os.open(p, os.O_WRONLY | os.O_CREAT)", "overwrite"),
        ("import os\ndef f(p):\n    os.open(p, os.O_RDWR)", "overwrite"),
        ("from os import O_WRONLY as W\ndef f(p):\n    return W", "overwrite"),
        ("import os\ndef f(p, flags):\n    os.open(p, flags)", "overwrite"),
        ("import os\ndef f(p):\n    os.open(p, os.O_WRONLY | 512)", "truncation"),
        ("import shutil\ndef f(a, b):\n    shutil.copyfile(a, b)", "rename"),
        ("import shutil\ndef f(a, b):\n    shutil.copy2(a, b)", "rename"),
        ("import shutil\ndef f(a, b):\n    shutil.copytree(a, b, dirs_exist_ok=True)", "rename"),
        ("import io\ndef f(p):\n    io.FileIO(p, 'w')", "truncation"),
        ("import logging\ndef f(p):\n    logging.FileHandler(p, mode='w')", "truncation"),
        ("import zipfile\ndef f(p):\n    zipfile.ZipFile(p, 'w')", "truncation"),
        ("import gzip\ndef f(p):\n    gzip.open(p, 'wb')", "truncation"),
        (
            "from logging.handlers import RotatingFileHandler\n"
            "def f(p):\n    RotatingFileHandler(p, maxBytes=1, backupCount=1)",
            "rename",
        ),
        ("import subprocess\ndef f(p):\n    subprocess.run(['rm', p])", "subprocess"),
        ("from subprocess import run\ndef f(p):\n    run(['rm', p])", "subprocess"),
        ("import os\ndef f(p):\n    os.system('rm ' + p)", "subprocess"),
        ("import os\ndef f(p):\n    os.popen('rm ' + p)", "subprocess"),
        ("import os\ndef f(p):\n    getattr(os, 'remove')(p)", "removal"),
        ("import os\ndef f(p, n):\n    getattr(os, n)(p)", "dynamic"),
        ("import os\ndef f(p):\n    vars(os)['unlink'](p)", "dynamic"),
        ("import importlib\ndef f():\n    importlib.import_module('ctypes')", "dynamic"),
        ("def f():\n    __import__('ctypes')", "dynamic"),
        ("def f(fs, fd):\n    fs.pwrite(fd, b'x', 0)", "overwrite"),
        ("import os\ndef f(fd):\n    os.pwrite(fd, b'x', 0)", "overwrite"),
        # second review: forms that still escaped
        ("from os import *\ndef f(p):\n    unlink(p)", "dynamic"),
        ("import os\ndef f(p):\n    os.__dict__['unlink'](p)", "dynamic"),
        (
            "import asyncio\nasync def f(p):\n    await asyncio.create_subprocess_exec('rm', p)",
            "subprocess",
        ),
        (
            "import asyncio\nasync def f(p):\n"
            "    await asyncio.get_running_loop().subprocess_exec(None, 'rm', p)",
            "subprocess",
        ),
        ("import multiprocessing\ndef f():\n    multiprocessing.Process(target=f)", "subprocess"),
        ("from multiprocessing import Pool\ndef f():\n    Pool(2)", "subprocess"),
        ("import pty\ndef f(p):\n    pty.spawn(['rm', p])", "subprocess"),
        ("import os\ndef f(p):\n    os.posix_spawnp('rm', ['rm', p], {})", "subprocess"),
        ("import os\ndef f(p):\n    os.posix_spawn('/bin/rm', ['rm', p], {})", "subprocess"),
        ("import os\ndef f(p):\n    os.execvpe('rm', ['rm', p], {})", "subprocess"),
        ("import os\ndef f(p):\n    os.execl('/bin/rm', 'rm', p)", "subprocess"),
        ("import os\ndef f(p):\n    os.spawnvp(os.P_WAIT, 'rm', ['rm', p])", "subprocess"),
        ("import os\ndef f(a, b):\n    os.copy_file_range(a, b, 10, 0, 0)", "overwrite"),
        ("import os\ndef f(a, b):\n    os.sendfile(b, a, 0, 10)", "overwrite"),
        ("import mmap\ndef f(fd):\n    mmap.mmap(fd, 10)", "overwrite"),
        ("from mmap import mmap\ndef f(fd):\n    mmap(fd, 10)", "overwrite"),
        ("import builtins\ndef f(p):\n    builtins.open(p, 'w')", "truncation"),
        ("import os\ndef f(fd):\n    os.fdopen(fd, 'w')", "truncation"),
        ("import os\ndef f(fd):\n    os.fdopen(fd, 'r+')", "overwrite"),
        ("from pathlib import Path\ndef f(p):\n    getattr(Path(p), 'unlink')()", "removal"),
        ("def f(p):\n    getattr(p, 'rename')('x')", "rename"),
        ("def f(fs, name):\n    getattr(fs, name)(1)", "dynamic"),
        ("def f(p):\n    eval('__import__(\"os\").unlink(p)')", "dynamic"),
        ("def f(p):\n    exec('import os; os.unlink(p)')", "dynamic"),
        ("import sys\ndef f(p):\n    sys.modules['os'].remove(p)", "dynamic"),
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
        "def f(p):\n    return open(p, 'a')",
        "def f(p):\n    return open(p, 'x')",
        "from pathlib import Path\ndef f(p):\n    return Path(p).open('rb')",
        "def f(transport, receiver):\n    return transport.open(receiver)",
        "import os\ndef f(p):\n    return os.open(p, os.O_RDONLY | os.O_NOFOLLOW)",
        "import os\ndef f(p):\n    return os.open(p, os.O_WRONLY | os.O_CREAT | os.O_EXCL)",
        "import os\ndef f(p):\n    os.stat(p)",
        "def f(text):\n    return text.replace('a', 'b', 1)",
        "import os\ndef f(p):\n    return getattr(os, 'sep')",
        "def f(s):\n    return s.rename",  # a field called rename is not a call
        "def f(d):\n    return d.replace(tzinfo=None)",
        "def f(cfg):\n    return getattr(cfg, 'name')",
        "import os\ndef f(fd):\n    return os.fdopen(fd, 'rb')",
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
