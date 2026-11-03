"""Shared plumbing of the atheris harnesses in this directory.

It imports nothing from ``ironbird``: a harness imports the code under test inside
``atheris.instrument_imports()`` itself, so the coverage feedback sees it.

Every harness runs as ``python tools/fuzz/fuzz_<name>.py [libFuzzer options]``. The defaults here
make a hang and an unbounded allocation findings rather than a stuck CI job: ``-timeout`` ends an
input that runs too long (a ``timeout-*`` artifact) and ``-rss_limit_mb`` one that grows the
process too far (an ``oom-*`` artifact). Both can be overridden on the command line. With
``IRONBIRD_FUZZ_CORPUS`` set to a directory, each harness also keeps the inputs it finds
interesting there (one subdirectory per harness) and starts from them next time. Directory
arguments (not starting with ``-``) are passed on after the harness's own corpora, so
``-merge=1 OLD`` with ``IRONBIRD_FUZZ_CORPUS`` set to a fresh directory minimizes the corpus OLD
into it. With ``IRONBIRD_FUZZ_SCRATCH`` set, scratch directories are made under it instead of the
system temp directory.
"""

from __future__ import annotations

import atexit
import os
import shutil
import sys
import tempfile
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Final

REPO: Final = Path(__file__).resolve().parents[2]
EXAMPLES: Final = REPO / "examples"
CORPUS: Final = REPO / "tests" / "corpus" / "malformed"
TESTS: Final = REPO / "tests"
DICT_DIR: Final = Path(__file__).resolve().parent / "dict"

CORPUS_ENV: Final = "IRONBIRD_FUZZ_CORPUS"  # a directory that keeps inputs between runs
SCRATCH_ENV: Final = "IRONBIRD_FUZZ_SCRATCH"  # the base directory of scratch directories
DEFAULT_TIMEOUT_S: Final = 10  # one input taking this long is a hang
DEFAULT_RSS_MB: Final = 2048  # one input growing the process past this is unbounded memory


def scratch_dir(prefix: str) -> Path:
    """A fresh scratch directory under ``IRONBIRD_FUZZ_SCRATCH`` (else the system temp directory).

    It is removed at a Python-level exit; libFuzzer ends the process from C (``exit`` after
    ``-runs``, ``_exit`` on a finding), which skips that, so a caller that must not leak points
    ``IRONBIRD_FUZZ_SCRATCH`` at a directory it owns."""
    base = os.environ.get(SCRATCH_ENV) or None
    if base is not None:
        Path(base).mkdir(parents=True, exist_ok=True)
    path = Path(tempfile.mkdtemp(prefix=f"ironbird-fuzz-{prefix}-", dir=base))
    atexit.register(shutil.rmtree, path, True)
    return path


def seed_corpus(
    scratch: Path,
    globs: Iterable[str],
    valid: Iterable[bytes] = (),
    *,
    root: Path = CORPUS,
    prefixes: Iterable[bytes] = (b"",),
) -> Path:
    """A seeds directory under ``scratch``: the checked-in files matching ``globs`` under
    ``root``, then each of ``valid`` (a well-formed input the fuzzer mutates from), each once per
    entry of ``prefixes`` (selector bytes a harness reads before the content). The repo
    stays clean: libFuzzer writes its findings and new inputs only into this scratch copy."""
    seeds = scratch / "seeds"
    seeds.mkdir()
    heads = list(prefixes)
    blobs = [p.read_bytes() for pattern in globs for p in sorted(root.glob(pattern))]
    blobs += list(valid)
    for index, blob in enumerate(blobs):
        for which, head in enumerate(heads):
            (seeds / f"seed{index}-{which}").write_bytes(head + blob)
    assert any(seeds.iterdir()), f"no seeds for {list(globs)}"
    return seeds


def run(
    one_input: Callable[[bytes], None],
    seeds: Path,
    *,
    max_len: int = 4096,
    dictionary: str | None = None,
) -> None:
    """Hand ``one_input`` to libFuzzer with the project defaults (see the module docstring).

    ``dictionary`` names a file under ``tools/fuzz/dict`` of tokens the mutator splices in.
    ``max_len`` is raised to the largest seed: libFuzzer cuts every loaded input to ``-max_len``,
    and a cut seed (a truncated JSON document, a recording without its end) no longer parses."""
    import atheris  # noqa: PLC0415

    largest = max((p.stat().st_size for p in seeds.iterdir() if p.is_file()), default=0)
    max_len = max(max_len, largest)
    assert max_len >= largest
    options = [a for a in sys.argv[1:] if a.startswith("-")]
    extra = [a for a in sys.argv[1:] if not a.startswith("-")]
    names = {o.split("=", 1)[0] for o in options}
    defaults = {
        "-timeout": str(DEFAULT_TIMEOUT_S),
        "-rss_limit_mb": str(DEFAULT_RSS_MB),
        "-max_len": str(max_len),
        "-artifact_prefix": str(Path(tempfile.gettempdir()) / "ironbird-fuzz-crashes") + "/",
    }
    if dictionary is not None:
        defaults["-dict"] = str(DICT_DIR / dictionary)
    prefix = options_value(options, "-artifact_prefix", defaults["-artifact_prefix"])
    Path(prefix if prefix.endswith("/") else os.path.dirname(prefix) or ".").mkdir(
        parents=True, exist_ok=True
    )
    added = [f"{k}={v}" for k, v in defaults.items() if k not in names]
    corpora = [str(seeds)]
    kept = os.environ.get(CORPUS_ENV)
    if kept:  # libFuzzer writes new inputs to the first directory: the one that outlives the run
        keep = Path(kept) / Path(sys.argv[0]).stem
        keep.mkdir(parents=True, exist_ok=True)
        corpora.insert(0, str(keep))
    atheris.Setup([program(seeds.parent), *added, *options, *corpora, *extra], one_input)
    atheris.Fuzz()


def program(scratch: Path) -> str:
    """An executable that runs this harness under this interpreter. libFuzzer re-runs
    ``argv[0]`` as a command for ``-merge``, ``-fork`` and ``-minimize_crash``, and a harness
    script is neither executable nor tied to the interpreter (and venv) running it."""
    wrapper = scratch / "harness.sh"
    script = Path(sys.argv[0]).resolve()
    wrapper.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{script}" "$@"\n')
    wrapper.chmod(0o700)
    return str(wrapper)


def options_value(options: list[str], name: str, default: str) -> str:
    """The value of libFuzzer option ``name`` (``-name=value``) in ``options``, else ``default``."""
    for option in options:
        key, _, value = option.partition("=")
        if key == name:
            return value
    return default


def reset_dir(path: Path) -> Path:
    """``path`` as a new, empty directory."""
    shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True)
    return path
