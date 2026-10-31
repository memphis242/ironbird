"""A signal acted on as a held step of a write ends, while the step's raw failure leaves it, takes
that failure's place; the code that maps the failure to its error names the replaced one as that
same error, so the user is shown what they would have been shown without the signal. A failure
that would have been only a warning (a committed output whose directory could not be synced), or
that a recovery step handles itself, is not reported."""

from __future__ import annotations

import errno
import os
import signal
import threading
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from exportutil import export_in_process, hk_and_status
from ironbird.errors import Code, IronbirdError
from ironbird.recording import inspect as rec_inspect
from ironbird.recording.export import (
    DirDestination,
    ExportFormat,
    ExportResult,
    FileDestination,
    write_to_directory,
    write_to_file,
)
from ironbird.retention import full_scan
from ironbird.sigmask import Interrupted
from retutil import make_reserve, make_session, reserve_header, sid
from support.faultfs import FaultFs

pytestmark = pytest.mark.skipif(
    threading.current_thread() is not threading.main_thread(),
    reason="signal handlers can only be set in the main thread",
)


class _Raised(Interrupted):
    pass


@pytest.fixture(autouse=True)
def raising_hup() -> Iterator[None]:
    """SIGHUP runs a handler that raises :class:`_Raised`."""

    def handler(signum: int, _frame: object) -> None:
        raise _Raised(signum)

    previous = signal.signal(signal.SIGHUP, handler)
    try:
        yield
    finally:
        signal.signal(signal.SIGHUP, previous)


def _hup_then(failure: BaseException) -> Callable[..., Any]:
    """A file-system method that signals this thread and fails with ``failure``."""

    def call(*_args: Any) -> Any:
        signal.pthread_kill(threading.get_ident(), signal.SIGHUP)
        raise failure

    return call


def _eio() -> OSError:
    return OSError(errno.EIO, os.strerror(errno.EIO))


def _replaced(caught: pytest.ExceptionInfo[_Raised]) -> BaseException | None:
    return caught.value.replaced


@pytest.fixture
def rec(tmp_path: Path) -> Path:
    return hk_and_status(tmp_path / "in.ibrec", 6, 4)


def _file_run(rec: Path) -> Callable[[Any], ExportResult]:
    def run(dest: FileDestination) -> ExportResult:
        result, _ = export_in_process(rec, None, ExportFormat.TEXT, destination=dest)
        return result

    return run


def _dir_run(rec: Path) -> Callable[[Any], ExportResult]:
    def run(dest: DirDestination) -> ExportResult:
        result, _ = export_in_process(rec, None, ExportFormat.CSV, destination=dest)
        return result

    return run


# -- export ----


@pytest.mark.parametrize(
    ("method", "failure", "code"),
    [
        ("open_dir", FileNotFoundError(errno.ENOENT, "missing"), Code.EXP_WRITE),
        ("renameat2_noreplace", _eio(), Code.EXP_WRITE),
        ("renameat2_noreplace", FileExistsError(errno.EEXIST, "exists"), Code.EXP_EXISTS),
    ],
)
def test_a_signal_that_replaces_a_failed_file_step_names_the_export_error(
    tmp_path: Path, rec: Path, method: str, failure: OSError, code: Code
) -> None:
    fs = FaultFs()
    fs.override(method, _hup_then(failure))
    out = tmp_path / "out.txt"
    with pytest.raises(_Raised) as caught:
        write_to_file(out, force=False, run=_file_run(rec), fs=fs)
    replaced = _replaced(caught)
    assert isinstance(replaced, IronbirdError) and replaced.code is code, replaced
    assert isinstance(replaced.cause, OSError)
    assert not out.exists() and not [p for p in tmp_path.iterdir() if p.name.endswith(".tmp")]


@pytest.mark.parametrize(
    ("method", "failure", "code"),
    [
        ("open_dir", FileNotFoundError(errno.ENOENT, "missing"), Code.EXP_WRITE),
        ("open_at", OSError(errno.ENOSPC, "full"), Code.EXP_WRITE),
        ("renameat2_noreplace", FileExistsError(errno.EEXIST, "exists"), Code.EXP_EXISTS),
    ],
)
def test_a_signal_that_replaces_a_failed_directory_step_names_the_export_error(
    tmp_path: Path, rec: Path, method: str, failure: OSError, code: Code
) -> None:
    fs = FaultFs()
    real_open_at = fs.open_at

    def open_at_member(dir_fd: int, name: str, flags: int, mode: int = 0o600) -> int:
        if name.endswith(".csv") and flags & os.O_CREAT:  # the first table
            return int(_hup_then(failure)())
        return real_open_at(dir_fd, name, flags, mode)

    fs.override(method, open_at_member if method == "open_at" else _hup_then(failure))
    out = tmp_path / "out"
    with pytest.raises(_Raised) as caught:
        write_to_directory(out, run=_dir_run(rec), fs=fs)
    replaced = _replaced(caught)
    assert isinstance(replaced, IronbirdError) and replaced.code is code, replaced
    assert not out.exists() and not [p for p in tmp_path.iterdir() if p.name.endswith(".tmp")]


def test_a_signal_that_replaces_only_a_directory_sync_after_the_commit_names_nothing(
    tmp_path: Path, rec: Path
) -> None:
    """The output is committed and the sync failure is only a warning: nothing was replaced."""
    fs = FaultFs()
    fs.override("fsync", _hup_then(_eio()))
    out = tmp_path / "out.txt"
    with pytest.raises(_Raised) as caught:
        write_to_file(out, force=False, run=_file_run(rec), fs=fs)
    assert _replaced(caught) is None
    assert out.exists()


# -- rec repair ----


@pytest.mark.parametrize(
    "failure", [_eio(), FileExistsError(errno.EEXIST, "exists")], ids=["eio", "exists"]
)
def test_a_signal_that_replaces_a_failed_repair_step_names_the_repair_error(
    tmp_path: Path, rec: Path, failure: OSError
) -> None:
    fs = FaultFs()
    fs.override("renameat2_noreplace", _hup_then(failure))
    out = tmp_path / "fixed.ibrec"
    with pytest.raises(_Raised) as caught:
        rec_inspect.repair(rec, out, fs=fs)
    replaced = _replaced(caught)
    assert isinstance(replaced, IronbirdError) and replaced.code is Code.REC_CREATE, replaced
    assert not out.exists() and not [p for p in tmp_path.iterdir() if p.name.endswith(".tmp")]


# -- retention (the prune command runs it in the main thread) ----


def test_a_signal_that_replaces_a_failed_recovery_rename_names_no_error(tmp_path: Path) -> None:
    """A recovery step handles its own file-system failure (the session is protected, a message
    says so); a signal that took the failure's place leaves nothing to report."""
    state = tmp_path / "st"
    state.mkdir(mode=0o700)
    now = 1_700_000_000.0
    make_session(state, 1, age_days=3, now=now)
    body = (reserve_header(sid(1)) + '{"n": 1}\n').encode()
    reserve = make_reserve(state, 1, 1, body, now=now)
    fs = FaultFs()
    fs.override("renameat2_noreplace", _hup_then(FileExistsError(errno.EEXIST, "exists")))
    with pytest.raises(_Raised) as caught:
        full_scan(fs, state, "ledger", now=now, own_session=None, keep_recent=0)
    assert _replaced(caught) is None
    assert reserve.exists(), "nothing was renamed"
