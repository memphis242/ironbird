"""Command line front-end. Thin: parse arguments, call the session, print results.

Errors leave as one line ``error IB-XXX-NNN: summary`` on stderr (full detail with
``--verbose`` and always in the ledger) and a non-zero exit code.
"""

from __future__ import annotations

import asyncio
import contextlib
import errno
import inspect
import json
import math
import os
import resource
import signal
import sys
import time
import traceback
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any, Final

import typer

from ironbird import render
from ironbird.ccdd.model import EventSeverity, MessageKind
from ironbird.cli import prunecmd
from ironbird.cli.output import (
    CLOSE_WAIT_S,
    HANDLED_SIGNALS,
    BoundedWriter,
    Console,
    signals_held,
)
from ironbird.config import RecordingConfig, RetentionConfig, SystemConfig, find_config, load_config
from ironbird.errors import Code, IronbirdError
from ironbird.fsops import REAL_FS
from ironbird.ledger import ExitReport, Ledger, default_state_dir
from ironbird.recording import inspect as rec_inspect
from ironbird.recording.render import damage_summary, info_json, info_lines, size_text
from ironbird.recording.session import (
    PendingRecording,
    RecordRequest,
    default_recording_dir,
    parse_split,
)
from ironbird.recording.writer import Recorder
from ironbird.retention import LONG_COMMANDS
from ironbird.retnotices import Notices
from ironbird.retservice import Retention
from ironbird.session import Route, Session, Side

EXIT_TIMEOUT: Final = 3  # --count was given and not reached before --timeout
EXIT_OUTPUT_LOST: Final = 4  # the command completed but stdout lines were lost
EXIT_DAMAGED_INPUT: Final = 5  # output produced, but the input recording was damaged mid-file
EXIT_UNLOGGED: Final = 6  # a command or reply went out but its ledger record was never written
STATUS_INTERVAL_S: Final = 5.0  # ``ib record`` status line on stderr
MAX_TIMEOUT_S: Final = 365 * 24 * 3600.0
OUTPUT_POLL_S: Final = 0.5
DRAIN_NOTICE_S: Final = 2.0  # waiting this long for the reader of stdout earns one notice
SIGNAL_POLL_S: Final = 0.02
MIN_FREE_FDS: Final = 8  # event loop (3), UDP sockets, ledger, resolver threads, margin
_SIGNALS: Final = HANDLED_SIGNALS
# Keyword arguments for the ledger of this process (``fs``, ``clock``, ``join_timeout_s``, ...).
# Production leaves it empty; a test wrapper module assigns it before calling ``main()``.
LEDGER_OPTIONS: Final[dict[str, Any]] = {}

app = typer.Typer(
    name="ironbird",
    help="Integrated/system test tool for cFS vehicles: send commands, listen to telemetry.",
    no_args_is_help=True,
    add_completion=False,
    pretty_exceptions_enable=False,
)
rec_app = typer.Typer(
    name="rec",
    help="Inspect, check and repair recordings (.ibrec files).",
    no_args_is_help=True,
    add_completion=False,
    pretty_exceptions_enable=False,
)
app.add_typer(rec_app, name="rec")
ledger_app = typer.Typer(
    name="ledger",
    help="Manage the action ledger files (list and delete old sessions after confirmation).",
    no_args_is_help=True,
    add_completion=False,
    pretty_exceptions_enable=False,
)
app.add_typer(ledger_app, name="ledger")


class Format(StrEnum):
    LINE = "line"
    JSON = "json"


class MinType(StrEnum):
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

    @property
    def severity(self) -> EventSeverity:
        return EventSeverity[self.name]


@dataclass(frozen=True, slots=True)
class Globals:
    config: Path | None
    role: str | None
    verbose: bool


def term(text: str, max_len: int = 256, *, err: bool = False) -> str:
    """Terminal text for any config/CCDD/packet string; see :func:`ironbird.render.term`."""
    return render.term(text, max_len, err=err)


AsOption = Annotated[
    str | None, typer.Option("--as", help="Act as this node (default: acts_as in the config).")
]
VerboseOption = Annotated[
    bool, typer.Option("--verbose", "-v", help="Show full error detail and packet hex.")
]
LinkOption = Annotated[str | None, typer.Option("--link", help="Link name, if several match.")]
RecordOption = Annotated[
    str | None,
    typer.Option(
        "--record",
        help=(
            "Also record every frame of the captured links to this .ibrec file "
            "('auto': the default location and name)."
        ),
    ),
]
CountOption = Annotated[int | None, typer.Option("--count", min=1, help="Stop after N packets.")]


def _check_timeout(value: float | None) -> float | None:
    if value is not None and not (math.isfinite(value) and 0.01 <= value <= MAX_TIMEOUT_S):
        raise typer.BadParameter(f"must be a number of seconds from 0.01 to {MAX_TIMEOUT_S:.0f}")
    return value


TimeoutOption = Annotated[
    float | None,
    typer.Option(
        "--timeout",
        callback=_check_timeout,
        help=(
            "Stop after this many seconds (exit 3 if --count was not reached). Output already "
            "queued may keep draining after the timeout (see README)."
        ),
    ),
]


@app.callback()
def _root(
    ctx: typer.Context,
    config: Annotated[
        Path | None,
        typer.Option("--config", "-c", help="Path to ironbird.toml (or $IRONBIRD_CONFIG)."),
    ] = None,
    as_node: AsOption = None,
    verbose: VerboseOption = False,
) -> None:
    ctx.obj = Globals(config, as_node, verbose)


# -- signals ------------------------------------------------------------------------------


class _Terminated(BaseException):
    """Raised by the signal handler when no event loop is running to cancel instead."""

    def __init__(self, signum: int) -> None:
        super().__init__(signum)
        self.signum = signum


class _SignalState:
    """SIGINT/SIGTERM/SIGHUP end the command orderly, whatever it is doing.

    While the event loop runs, the handler cancels the main task, so ``finally`` blocks and
    ``async with`` exits unwind normally; before the command and while the loop is torn down it
    raises :class:`_Terminated`. From :meth:`detach` on (the command is over and only the
    shutdown remains) it just records the signal, so none can escape the shutdown unannounced:
    :func:`_finish` announces whatever was recorded and not yet :attr:`announced`. Only the
    first signal acts; repeats while shutting down are ignored.
    """

    def __init__(self) -> None:
        self.signum = 0
        self.announced = False  # the ledger and the user were told about ``signum``
        self._installed = False  # the handlers are ours (``main`` ran, not an in-process caller)
        self._raises = True
        self._task: asyncio.Task[Any] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    def install(self) -> None:
        self.signum, self.announced, self._raises = 0, False, True
        self._task = self._loop = None
        for sig in _SIGNALS:
            with contextlib.suppress(ValueError, OSError):  # not the main thread
                signal.signal(sig, self._handle)
                self._installed = True

    def restore_default(self) -> None:
        """Give the signals their default action back, once the shutdown is complete.

        Called with the signals still blocked: one that is pending then ends the process by that
        signal when the mask is lifted, instead of landing in a handler that has nothing left to
        record it in. A no-op where :meth:`install` did not run.
        """
        if not self._installed:
            return
        self._installed = False
        for sig in _SIGNALS:
            with contextlib.suppress(ValueError, OSError):
                signal.signal(sig, signal.SIG_DFL)

    def attach(self, task: asyncio.Task[Any], loop: asyncio.AbstractEventLoop) -> None:
        self._task, self._loop = task, loop

    def detach(self) -> None:
        self._raises = False
        self._task = self._loop = None

    def _handle(self, signum: int, _frame: object) -> None:
        if self.signum:
            return
        self.signum = signum
        task, loop = self._task, self._loop
        if task is not None and loop is not None and not task.done():
            loop.call_soon_threadsafe(task.cancel)
        elif self._raises:
            raise _Terminated(signum)


_STATE = _SignalState()


# -- plumbing --------------------------------------------------------------------------


@dataclass(slots=True)
class _Run:
    """What one command invocation shares between the loop and the code around it."""

    console: Console
    verbose: bool
    session: Session | None = None
    pipe_closed: asyncio.Event = field(default_factory=asyncio.Event)
    config: SystemConfig | None = None  # loaded before the ledger opens, so [ledger] applies
    config_error: IronbirdError | None = None
    config_missing: bool = False  # no configuration file at all (not an error for offline tools)
    retention: Retention | None = None


Body = Callable[[Session, SystemConfig, "_Run"], Awaitable[int]]
OfflineBody = Callable[[Ledger, Console, "_Run"], int]


def _print_error(console: Console, exc: IronbirdError, verbose: bool) -> None:
    console.err(term(exc.user_line(), 1000, err=True))
    if verbose:
        for line in exc.detail_text().splitlines():
            console.err(f"  {term(line, 2000, err=True)}")


def _report_output_drops(session: Session, console: Console) -> None:
    stdout, stderr = console.take_new_drops()
    for name, count in (("stdout", stdout), ("stderr", stderr)):
        if count:
            session.report(
                IronbirdError(
                    Code.OUT_DROPPED,
                    f"{name} is not draining: {count} line(s) dropped (still in the ledger)",
                ),
                context=name,
            )


async def _watch_output(session: Session, console: Console) -> None:
    while True:
        await asyncio.sleep(OUTPUT_POLL_S)
        _report_output_drops(session, console)


async def _main(glob: Globals, ledger: Ledger, body: Body, run: _Run) -> int:
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    assert task is not None
    _STATE.attach(task, loop)
    console = run.console
    pipe_closed = run.pipe_closed

    def reader_gone() -> None:  # called from the writer thread
        # The loop may be gone (the reader left during the final drain); the writer's
        # ``reader_gone`` flag is what the code after the loop looks at.
        with contextlib.suppress(RuntimeError):
            loop.call_soon_threadsafe(pipe_closed.set)

    console.stdout.set_on_closed(reader_gone)
    try:
        return await _main_body(glob, ledger, body, run)
    finally:
        console.stdout.set_on_closed(None)  # nothing may call into the loop once it is closing


async def _main_body(glob: Globals, ledger: Ledger, body: Body, run: _Run) -> int:
    console = run.console
    if run.config is None:
        exc = run.config_error or IronbirdError(Code.INTERNAL, "the configuration was not loaded")
        ledger.record_error(exc)
        _print_error(console, exc, run.verbose)
        return exc.exit_code
    config = run.config
    try:
        async with Session(
            config,
            role=glob.role,
            ledger=ledger,
            notify=lambda notice: console.err(
                term(notice.line(verbose=run.verbose), 1000, err=True)
            ),
        ) as session:
            run.session = session
            watcher = asyncio.ensure_future(_watch_output(session, console))
            try:
                return await body(session, config, run)
            except IronbirdError as exc:
                session.fatal(exc)
                _print_error(console, exc, run.verbose)
                return exc.exit_code
            finally:
                watcher.cancel()
                _report_output_drops(session, console)
    except IronbirdError as exc:  # role resolution failed before the session started
        ledger.record_error(exc)
        _print_error(console, exc, run.verbose)
        return exc.exit_code


def _interrupt_notice(name: str, signum: int, run: _Run) -> str:
    sig = signal.Signals(signum).name
    tx = run.session.last_tx if run.session is not None else None
    if name != "send":
        return f"interrupted ({sig})"
    if tx is None:
        return f"interrupted ({sig}): no command was built, so nothing was sent"
    if tx.went_out:
        return f"interrupted ({sig}): {tx.command} was sent; delivery was not confirmed"
    return f"interrupted ({sig}): {tx.command} was not sent"


def _recording_dir(config: SystemConfig | None) -> Path | None:
    """Where recordings are kept, or ``None`` when that cannot be resolved (no warning then)."""
    try:
        return default_recording_dir(config.recording if config is not None else RecordingConfig())
    except IronbirdError:
        return None


def _check_descriptors() -> None:
    """Refuse up front when too few file descriptors are left for the event loop and sockets.

    Failing inside the event loop's own setup would leave a half-built loop behind and a
    coroutine that was never awaited.
    """
    held: list[int] = []
    try:
        for _ in range(MIN_FREE_FDS):
            held.append(os.open(os.devnull, os.O_RDONLY))
    except OSError as exc:
        limit = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
        raise IronbirdError(
            Code.SYS_RESOURCE,
            f"too few file descriptors free to run (limit {limit}, {len(held)} free, need "
            f"{MIN_FREE_FDS}); raise the limit with 'ulimit -n' or close other files",
            cause=exc,
        ) from exc
    finally:
        for fd in held:
            os.close(fd)


def _record_signal(
    name: str, signum: int, run: _Run, ledger: Ledger, console: Console, *, direct: bool = False
) -> int:
    """Ledger and announce a signal that ended the command; returns the exit code 128+n.

    ``direct`` writes the notice to the process's stderr instead of the console, for when the
    console's stderr writer is already closed. The record is on disk before the notice is
    written, and the notice goes through a fresh :class:`BoundedWriter` closed after
    :data:`CLOSE_WAIT_S`: the signals are held here, so a stderr nobody reads must not be able
    to stall the exit.
    """
    _STATE.announced = True
    notice = _interrupt_notice(name, signum, run)
    tx = run.session.last_tx if run.session is not None else None
    ledger.record(
        "state",
        "cli.signal",
        command=name,
        signal=signal.Signals(signum).name,
        signum=int(signum),
        notice=notice,
        tx_command=tx.command if tx else None,
        tx_went_out=tx.went_out if tx else None,
    )
    if not direct:
        console.err(notice)
        return 128 + int(signum)
    ledger.flush()
    if sys.stderr is not None:  # closed with ``2>&-``: nobody to tell
        late = BoundedWriter(sys.stderr)
        late.write(term(notice, 1000, err=True))
        if not late.close(CLOSE_WAIT_S):
            ledger.record("state", "output.stderr_lost", lost=late.lost, notice=notice)
    return 128 + int(signum)


def _poll_signal(wait_s: float) -> int:
    """Take one held shutdown signal if it is (or becomes, within ``wait_s``) pending; else 0."""
    take = getattr(signal, "sigtimedwait", None)
    if take is None:  # pragma: no cover - not Linux
        time.sleep(wait_s)
        return 0
    info = take(_SIGNALS, wait_s)
    return int(info.si_signo) if info is not None else 0


def _drain(writer: BoundedWriter, on_slow: Callable[[], None] | None = None) -> int:
    """Wait with no deadline until ``writer`` has written every line it accepted.

    Runs with the shutdown signals held, so one that arrives is taken here and returned (the
    wait is over, the caller abandons what is left); 0 means everything was written.
    ``on_slow`` runs once if the wait lasts :data:`DRAIN_NOTICE_S`.
    """
    writer.begin_close()
    if not hasattr(signal, "sigtimedwait"):  # pragma: no cover - not Linux
        writer.wait(CLOSE_WAIT_S)
        return 0
    slow_at = time.monotonic() + DRAIN_NOTICE_S
    while not writer.wait(0.001):
        if on_slow is not None and time.monotonic() >= slow_at:
            on_slow()
            on_slow = None
        if signum := _poll_signal(SIGNAL_POLL_S):
            return signum
    return 0


def _stdout_loss(console: Console, name: str, run: _Run) -> IronbirdError | None:
    out = console.stdout
    if not out.lost:
        return None
    tx = run.session.last_tx if run.session is not None else None
    sent = f"; {tx.command} was sent" if name == "send" and tx is not None and tx.went_out else ""
    if out.unavailable:
        summary = f"stdout is closed: {out.lost} line(s) of output could not be shown{sent}"
    else:
        parts = [
            f"{count} {what}"
            for count, what in (
                (out.dropped_total, "dropped while it was not draining"),
                (out.abandoned_total, "abandoned at exit"),
                (out.undelivered_total, "undeliverable"),
            )
            if count
        ]
        cause = f" ({os.strerror(out.error.errno)})" if out.error and out.error.errno else ""
        summary = (
            f"stdout lost {out.lost} line(s) - {', '.join(parts)}{cause}; output is incomplete, "
            f"read it faster or write it to a file{sent}"
        )
    return IronbirdError(Code.OUT_LOST, summary)


def _apply_ledger_report(code: int, report: ExitReport) -> int:
    """Fold the ledger's account into the exit code.

    Replies withheld under ``on_full = "refuse_commands"`` are an error (1, above 3, 4, 5 and 6);
    a transmission whose record was never written or was possibly lost is 6, which wins over 4
    and 5 and yields to an error (1, 2), to 3 and to a signal.
    """
    if report.withheld and code in (0, EXIT_TIMEOUT, EXIT_OUTPUT_LOST, EXIT_DAMAGED_INPUT, 6):
        return 1
    if report.unlogged and code in (0, EXIT_OUTPUT_LOST, EXIT_DAMAGED_INPUT):
        return EXIT_UNLOGGED
    return code


def _report_ledger(report: ExitReport, run: _Run, console: Console, code: int) -> int:
    """Print the ledger's exit lines (and, with ``--verbose``, the unwritten transmission
    records as JSON lines, so their trace survives in the terminal or CI log)."""
    for notice in report.notices:
        console.err(term(notice.line(verbose=run.verbose), 1000, err=True))
    if run.verbose:
        for line in report.unwritten_tx_lines:
            console.err(term(line, 4000, err=True))
    return _apply_ledger_report(code, report)


def _finish(name: str, run: _Run, ledger: Ledger, console: Console, code: int) -> int:
    """Write out everything still queued, close the ledger and return the exit code.

    A normal end waits for the reader of stdout with no deadline, so a slow reader loses
    nothing; a signal (taken with ``sigtimedwait`` while the signals are held, so it can never
    run a handler mid-flush) cuts the wait short and the rest is abandoned and counted. After a
    signal already ended the run, the drain is bounded by :data:`CLOSE_WAIT_S`. A signal that
    was recorded but not yet announced (it arrived after the loop ended) is announced here.
    Anything lost
    on stdout is reported once, ledgered with its counts, and turns an exit code of 0 into
    :data:`EXIT_OUTPUT_LOST`.
    """
    out, err = console.stdout, console.stderr
    with signals_held():
        if run.retention is not None:
            run.retention.finish()  # a scan in flight reports before the account is made
        report = ledger.finish()  # the last write attempt, and the account of what is unwritten
        signum = _STATE.signum
        fresh = 0 if _STATE.announced else signum  # a signal that still has to be ledgered

        def waiting() -> None:
            queued = out.queued_lines
            summary = (
                f"stdout is not being read: {queued} line(s) still queued; "
                "waiting for the reader (send a signal to abandon them)"
            )
            # A wait, not a failure: nothing is lost unless a signal cuts it short. Ledgered and
            # on disk first, because the wait has no deadline and may end in SIGKILL.
            ledger.record("warning", Code.OUT_WAITING.value, summary=summary, stdout_queued=queued)
            ledger.flush()
            console.err(f"warning {Code.OUT_WAITING.value}: {summary}")

        if not signum and (got := _drain(out, waiting)):
            signum = fresh = _STATE.signum = got
        out.close()  # bounded when a signal ended things; abandons what is left
        if not signum and (got := _poll_signal(0)):
            signum = fresh = _STATE.signum = got
        if fresh:
            code = _record_signal(name, fresh, run, ledger, console)
        loss = _stdout_loss(console, name, run)
        if loss is not None:
            ledger.record_error(
                loss,
                stdout_lost=out.lost,
                stdout_dropped=out.dropped_total,
                stdout_abandoned=out.abandoned_total,
                stdout_undelivered=out.undelivered_total,
                stdout_unavailable=out.unavailable,
            )
            console.err(loss.user_line())
            if code == 0:
                code = EXIT_OUTPUT_LOST
        code = _report_ledger(report, run, console, code)
        if not signum and (got := _drain(err)):
            signum = fresh = _STATE.signum = got
            code = _record_signal(name, got, run, ledger, console)
        err.close()
        if out.reader_gone:
            ledger.record("state", "output.pipe_closed", stream="stdout")
        if err.lost or err.reader_gone:
            ledger.record(
                "state",
                "output.stderr_lost",
                lost=err.lost,
                unavailable=err.unavailable,
                reader_gone=err.reader_gone,
            )
        closing = ledger.close()
        ledger.set_notifier(None)
        if closing.unlogged > report.unlogged or closing.withheld > report.withheld:
            code = _apply_ledger_report(code, closing)  # a sync error at close; the exit says so
        if not signum and (got := _poll_signal(0)):  # arrived while the ledger was closing
            code = _record_signal(name, got, run, ledger, console, direct=True)
            ledger.close()
        _STATE.restore_default()  # a later signal ends the process by that signal, not silently
    return code


def _run(
    ctx: typer.Context,
    as_node: str | None,
    verbose: bool,
    body: Body | None,
    name: str,
    *,
    transmits: bool = False,
    offline: OfflineBody | None = None,
    scan: bool = True,
) -> None:
    """Open the ledger, run ``body`` in an event loop, flush everything, exit with its code.

    ``transmits`` commands (they put packets on a link) refuse to run without a ledger.
    ``offline`` runs instead of ``body``, synchronously and with no configuration or session: it
    is for commands that only read or write files (``ib rec ...``); a signal still ends it
    orderly.
    """
    base: Globals = ctx.obj
    glob = Globals(base.config, as_node or base.role, base.verbose or verbose)
    console = Console(sys.stdout, sys.stderr)
    run = _Run(console, glob.verbose)
    # The configuration is read before the ledger opens: its [ledger] table sets the allowances,
    # the reserve and the full-disk policy. A session command reports a bad one from its own
    # body (and ledgers it); the offline commands do without a configuration.
    try:
        config_path = find_config(glob.config)
    except IronbirdError as exc:
        run.config_error, run.config_missing = exc, True
    else:
        try:
            run.config = load_config(config_path)
        except IronbirdError as exc:
            run.config_error = exc
    retention_cfg = run.config.retention if run.config is not None else RetentionConfig()
    fs = LEDGER_OPTIONS.get("fs", REAL_FS)
    ledger = Ledger(Path("/nonexistent"), enabled=False)
    code = 1
    try:
        try:
            state_dir = default_state_dir()
            notices = Notices(state_dir, fs=fs)
            ledger = Ledger(
                state_dir,
                on_failure=lambda exc: _print_error(console, exc, glob.verbose),
                config=run.config.ledger if run.config is not None else None,
                session_class="long" if name in LONG_COMMANDS else "short",
                command=name,
                warn_gate=lambda code: notices.allow(
                    code.value, state_dir, retention_cfg.warn_interval_s
                ),
                **LEDGER_OPTIONS,
            )
            ledger.set_notifier(
                lambda notice: console.err(term(notice.line(verbose=glob.verbose), 1000, err=True))
            )
            ledger.open()
            if scan and ledger.enabled:
                recordings = _recording_dir(run.config)
                run.retention = Retention(
                    ledger,
                    retention_cfg,
                    state_dir,
                    recordings,
                    long_session=name in LONG_COMMANDS,
                    notices=notices,
                    fs=fs,
                )
                ledger.add_ticker(run.retention.poll)
                run.retention.start()
            if glob.verbose:  # the `[ledger n=N]` pointers on notices refer to this file
                console.err(f"ledger: {term(str(ledger.path), 1000, err=True)}")
        except IronbirdError as exc:
            _print_error(console, exc, glob.verbose)
            if transmits:
                console.err("nothing was sent: the action ledger is required")
                raise typer.Exit(exc.exit_code) from exc
            ledger.disable()
        ledger.record("action", "cli.invoke", command=name, argv=sys.argv[1:])
        main_coro: Coroutine[Any, Any, int] | None = None
        try:
            _check_descriptors()
            if offline is not None:
                code = offline(ledger, console, run)
            else:
                assert body is not None
                main_coro = _main(glob, ledger, body, run)
                code = asyncio.run(main_coro)
        except (_Terminated, asyncio.CancelledError) as stop:
            if not _STATE.signum:
                raise
            signum = stop.signum if isinstance(stop, _Terminated) else _STATE.signum
            code = _record_signal(name, signum, run, ledger, console)
        except IronbirdError as exc:
            ledger.record_error(exc)
            _print_error(console, exc, glob.verbose)
            code = exc.exit_code
        except Exception as exc:
            not_started = (
                main_coro is not None
                and inspect.getcoroutinestate(main_coro) == inspect.CORO_CREATED
            )
            if isinstance(exc, OSError) and not_started:  # the event loop could not be set up
                err: IronbirdError = IronbirdError(
                    Code.SYS_RESOURCE,
                    f"cannot start the event loop: {exc.strerror or exc}; "
                    "raise the limit with 'ulimit -n' or close other files",
                    cause=exc,
                )
            else:
                err = IronbirdError(
                    Code.INTERNAL,
                    f"unexpected {type(exc).__name__}: {exc}",
                    details=traceback.format_exc(),
                    cause=exc,
                )
            ledger.record_error(err)
            _print_error(console, err, glob.verbose)
            code = err.exit_code
        finally:
            if main_coro is not None:
                main_coro.close()  # no "never awaited" warning when the loop never started
    finally:
        _STATE.detach()  # from here a signal is only recorded; _finish announces it
        code = _finish(name, run, ledger, console, code)
    raise typer.Exit(code)


def _exit_for(reason: str, count: int | None, got: int) -> int:
    """0 normally; ``EXIT_TIMEOUT`` when ``--count`` was asked for and the timeout came first."""
    return EXIT_TIMEOUT if reason == "timeout" and count is not None and got < count else 0


async def _run_until(
    consumer: Coroutine[Any, Any, None], timeout: float | None, name: str, run: _Run
) -> str:
    """Run ``consumer`` until it finishes, the timeout passes, or the reader of stdout goes away.

    Returns ``"done"``, ``"timeout"`` or ``"pipe_closed"``. A signal cancels the main task, so it
    arrives here as ``CancelledError`` after the ending is ledgered as ``"signal"``.
    """
    assert run.session is not None
    session = run.session
    if timeout is not None and run.retention is not None:
        run.retention.set_deadline(timeout)  # the projection's horizon is what is left to run
    task = asyncio.ensure_future(consumer)
    pipe = asyncio.ensure_future(run.pipe_closed.wait())
    reason = "signal"
    try:
        await asyncio.wait({task, pipe}, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
        reason = "done" if task.done() else ("pipe_closed" if pipe.done() else "timeout")
    finally:
        session.ledger.record("state", f"{name}.ending", reason=reason)
        for pending in (task, pipe):
            if not pending.done():
                pending.cancel()
        results = await asyncio.gather(task, pipe, return_exceptions=True)
    outcome = results[0]
    if isinstance(outcome, BaseException) and not isinstance(outcome, asyncio.CancelledError):
        raise outcome
    return reason


def _parse_args(items: list[str] | None) -> dict[str, str]:
    args: dict[str, str] = {}
    for item in items or []:
        key, sep, value = item.partition("=")
        if not sep or not key:
            raise IronbirdError(Code.CLI_USAGE, f"argument {item[:40]!r} must look like name=value")
        if key in args:
            raise IronbirdError(Code.CLI_USAGE, f"argument {key[:40]!r} given more than once")
        args[key] = value
    return args


class _RecordingScope:
    """``--record`` for one command: a recording that lives for the ``async with`` block.

    Entering it stages the capture's frames; ``commit`` (called right after the capture has
    opened, so a wrong argument or a busy port leaves no file) creates the recording and hands
    the staged frames to it; leaving it closes the recording after the capture has closed, so
    the closing records are written on every way out (normal end, ``--duration``, a signal).
    Without a target it does nothing. ``exit_code`` folds a failed recorder into the command's
    exit code (the capture goes on; the command exits 1 at the end).
    """

    def __init__(
        self,
        session: Session,
        run: _Run,
        target: str | None,
        command: str,
        *,
        nodes: list[str],
        routes: list[Route] | None = None,
        split: tuple[int | None, float | None] = (None, None),
    ) -> None:
        self.recorder: Recorder | None = None
        self._run = run
        self._pending: PendingRecording | None = None
        if target is not None and nodes:
            request = RecordRequest(
                None if target == "auto" else Path(target), split[0], split[1], command
            )
            self._pending = PendingRecording(session, request, nodes=nodes, routes=routes or [])

    async def __aenter__(self) -> _RecordingScope:
        if self._pending is not None:
            self._pending.begin()
        return self

    async def commit(self) -> None:
        """Create the recording now (call once the capture is open). A no-op without a target."""
        if self._pending is None:
            return
        self.recorder = self._pending.commit()
        if self._run.retention is not None:
            self._run.retention.add_rate_source(self.recorder.rate_bytes_per_s)
        self._run.console.err(f"recording to {term(str(self.recorder.path), 1000, err=True)}")

    async def __aexit__(self, exc_type: object, exc: BaseException | None, tb: object) -> None:
        pending = self._pending
        if pending is None:
            return
        stats = await pending.finish(interrupted=isinstance(exc, asyncio.CancelledError))
        recorder = self.recorder
        if stats is None or recorder is None:
            return
        where = term(str(recorder.path), 1000, err=True)
        size = size_text(stats.file_bytes)
        if recorder.failed is not None:
            self._run.console.err(
                f"recording stopped after {stats.frames} frame(s), {stats.lost_frames} not "
                f"recorded ({size}) in {where}"
            )
        else:
            dropped = f", {stats.overrun_frames} dropped" if stats.overrun_frames else ""
            self._run.console.err(f"recorded {stats.frames} frame(s) ({size}{dropped}) to {where}")

    def exit_code(self, code: int) -> int:
        failed = self.recorder is not None and self.recorder.failed is not None
        return 1 if failed and code < 128 and code not in (1, 2) else code


# -- commands -----------------------------------------------------------------------------


@app.command()
def nodes(ctx: typer.Context, as_node: AsOption = None, verbose: VerboseOption = False) -> None:
    """List nodes, aliases, links, and which node we act as (also validates all files)."""

    async def body(session: Session, config: SystemConfig, run: _Run) -> int:
        out = run.console.out
        models = session.models()  # loads and validates every CCDD and header profile
        rows = [("NODE", "ALIAS", "MESSAGES", "CCDD / DESCRIPTION")]
        for name, node in config.nodes.items():
            mark = "*" if name == session.node.name else ""
            model = models.get(name)
            count = f"{len(model.commands())} cmd / {len(model.telemetry())} tlm" if model else "-"
            detail = str(node.ccdd) if node.ccdd else node.description or "-"
            rows.append((f"{term(name)}{mark}", term(node.alias), count, term(detail)))
        widths = [max(len(r[i]) for r in rows) for i in range(3)]
        for row in rows:
            out("  ".join(row[i].ljust(widths[i]) for i in range(3)) + "  " + row[3])
        out(f"acting as: {term(session.node.name)} ({term(session.node.alias)})   * = this node")
        if config.links:
            out("links:")
            for link in config.links:
                out(
                    f"  {term(link.name)}: {term(link.source)} -> {term(link.destination)} "
                    f"{term(link.transport)}  commands {term(str(link.command_endpoint))}  "
                    f"telemetry {term(str(link.telemetry_endpoint))}"
                )
        return 0

    _run(ctx, as_node, verbose, body, "nodes")


@app.command()
def find(
    ctx: typer.Context,
    query: Annotated[str, typer.Argument(help="Name, MID (0x...), or words from a description.")],
    limit: Annotated[int, typer.Option(min=1, help="Maximum results.")] = 30,
    as_node: AsOption = None,
    verbose: VerboseOption = False,
) -> None:
    """Find commands/telemetry: exact name or MID, then name substring, then description."""

    async def body(session: Session, config: SystemConfig, run: _Run) -> int:
        matches = session.find(query)
        session.ledger.record("action", "find", query=query, results=len(matches))
        for match in matches[:limit]:
            msg = match.message
            fc = f" fc={msg.function_code}" if msg.function_code is not None else ""
            desc = f"  {term(msg.description, 80)}" if msg.description else ""
            cat = f" cat={msg.category.value}" if msg.kind is MessageKind.TELEMETRY else ""
            run.console.out(
                f"{match.tier:<11} {term(match.node)}  {msg.kind.value:<9} {term(msg.name)}"
                f"  mid=0x{msg.mid:04X}{fc}{cat}{desc}"
            )
        if len(matches) > limit:
            run.console.err(f"{len(matches) - limit} more matches not shown (raise --limit)")
        if not matches:
            run.console.err(f"no match for '{term(query, 40, err=True)}'")
            return 1
        return 0

    _run(ctx, as_node, verbose, body, "find")


def _check_length_delta(delta: int, bits: int, profile: str) -> None:
    """A delta must change the field: not 0 modulo 2^bits, and within +/- (2^bits - 1)."""
    span = 1 << bits
    if delta % span == 0 or abs(delta) >= span:
        raise IronbirdError(
            Code.CLI_USAGE,
            f"--length-delta {delta} "
            f"{'changes nothing' if delta % span == 0 else 'is out of range'}"
            f" for the {bits}-bit length field of profile {profile}; "
            f"use a nonzero value between -{span - 1} and {span - 1}",
        )


@app.command()
def send(
    ctx: typer.Context,
    node: Annotated[str, typer.Argument(help="Target node name or alias.")],
    command: Annotated[str, typer.Argument(help="Command name from the target's CCDD.")],
    args: Annotated[list[str] | None, typer.Argument(help="Arguments as name=value.")] = None,
    link: LinkOption = None,
    bad_checksum: Annotated[
        bool, typer.Option("--bad-checksum", help="Fault injection: corrupt the checksum.")
    ] = False,
    length_delta: Annotated[
        int | None,
        typer.Option("--length-delta", help="Fault injection: add N to the length field."),
    ] = None,
    as_node: AsOption = None,
    verbose: VerboseOption = False,
) -> None:
    """Encode and send one command; print a one-line confirmation."""

    async def body(session: Session, config: SystemConfig, run: _Run) -> int:
        parsed = _parse_args(args)
        tamper: dict[str, int] = {}
        profile = session.codec_for(session.route(node, link_name=link).server.name).profile(
            MessageKind.COMMAND
        )
        if bad_checksum:
            if profile.checksum_field is None:
                raise IronbirdError(Code.CLI_USAGE, f"profile {profile.name} has no checksum field")
            tamper[profile.checksum_field.name] = 1
        if length_delta is not None:
            if profile.length_field is None:
                raise IronbirdError(Code.CLI_USAGE, f"profile {profile.name} has no length field")
            _check_length_delta(length_delta, profile.length_field.bits, profile.name)
            tamper[profile.length_field.name] = length_delta
        sent = await session.send_command(node, command, parsed, tamper=tamper, link=link)
        run.console.out(
            f"sent {term(sent.command)} to {term(sent.node)} via udp {term(sent.endpoint)} "
            f"(mid 0x{sent.mid:04X} fc {sent.function_code} seq {sent.sequence}, "
            f"{sent.size} bytes)"
        )
        if session_verbose(ctx, verbose):
            run.console.out(f"  hex: {sent.data.hex(' ')}")
        return 0

    _run(ctx, as_node, verbose, body, "send", transmits=True)


def session_verbose(ctx: typer.Context, local: bool) -> bool:
    base: Globals = ctx.obj
    return base.verbose or local


def _parse_mids(
    session: Session, node: str, items: list[str] | None, link: str | None
) -> frozenset[int] | None:
    """Resolve ``--mid`` values (number or message name) against the node's CCDD."""
    if not items:
        return None
    route = session.route(node, link_name=link)
    codec = session.codec_for(route.server.name)
    kind = MessageKind.TELEMETRY if route.side is Side.COMMANDER else MessageKind.COMMAND
    bits = codec.profile(kind).mid_bits
    mids: set[int] = set()
    for item in items:
        try:
            value = int(item, 0)
        except ValueError:
            msg = codec.model.by_name.get(item)
            if msg is None or msg.kind is not kind:
                raise IronbirdError(
                    Code.CLI_USAGE,
                    f"--mid {item[:40]!r} is neither a number nor a {kind.value} name of "
                    f"{route.server.name}",
                ) from None
            mids.add(msg.mid)
            continue
        if not 0 <= value < (1 << bits):
            raise IronbirdError(
                Code.CLI_USAGE, f"--mid {item[:40]!r} does not fit a {bits}-bit message ID"
            )
        if not codec.is_known_mid(kind, value):
            raise IronbirdError(
                Code.CLI_USAGE,
                f"--mid {item[:40]!r}: no {kind.value} with that MID in the CCDD of "
                f"{route.server.name} (see: ib find)",
            )
        mids.add(value)
    return frozenset(mids)


@app.command()
def listen(
    ctx: typer.Context,
    node: Annotated[str, typer.Argument(help="Peer node to listen to.")],
    mid: Annotated[
        list[str] | None, typer.Option("--mid", help="Only these MIDs (number or name).")
    ] = None,
    fmt: Annotated[Format, typer.Option("--format", help="Output format.")] = Format.LINE,
    count: CountOption = None,
    timeout: TimeoutOption = None,
    link: LinkOption = None,
    record: RecordOption = None,
    as_node: AsOption = None,
    verbose: VerboseOption = False,
) -> None:
    """Decode and print incoming packets (telemetry as ground; commands as a flight node)."""

    async def body(session: Session, config: SystemConfig, run: _Run) -> int:
        route = session.route(node, link_name=link) if record is not None else None
        async with _RecordingScope(
            session,
            run,
            record,
            "listen",
            nodes=[route.server.name] if route is not None else [],
            routes=[route] if route is not None else [],
        ) as scope:
            code = await listen_body(session, config, run, scope)
        return scope.exit_code(code)

    async def listen_body(
        session: Session, config: SystemConfig, run: _Run, scope: _RecordingScope
    ) -> int:
        io = run.console
        mids = _parse_mids(session, node, mid, link)
        listener = await session.listen(node, mids=mids, link=link)
        io.err(render.listen_banner(node, listener.local_address))
        shown = 0
        show_hex = session_verbose(ctx, verbose)

        async def consume() -> None:
            nonlocal shown
            async for rx in listener:
                node, time_ns = rx.frame.stream.node, rx.frame.time_ns
                if fmt is Format.JSON:
                    io.out(json.dumps(render.record(node, time_ns, rx.packet)))
                else:
                    io.out(render.line(node, time_ns, rx.packet))
                if show_hex:
                    io.err(f"  from {term(rx.frame.source, err=True)} size {rx.packet.size}")
                shown += 1
                if count is not None and shown >= count:
                    return

        try:
            await scope.commit()
            reason = await _run_until(consume(), timeout, "listen", run)
        finally:
            await listener.close()
        return _exit_for(reason, count, shown)

    _run(ctx, as_node, verbose, body, "listen")


@app.command()
def events(
    ctx: typer.Context,
    node: Annotated[
        list[str] | None,
        typer.Option("--node", help="Only this node's events; repeat for several (default: all)."),
    ] = None,
    min_type: Annotated[
        MinType | None,
        typer.Option("--min-type", case_sensitive=False, help="Hide events below this type."),
    ] = None,
    fmt: Annotated[Format, typer.Option("--format", help="Output format.")] = Format.LINE,
    count: Annotated[
        int | None, typer.Option("--count", min=1, help="Stop after N events.")
    ] = None,
    timeout: TimeoutOption = None,
    record: RecordOption = None,
    as_node: AsOption = None,
    verbose: VerboseOption = False,
) -> None:
    """Listen on every telemetry link of all (or selected) nodes and print only events."""

    async def body(session: Session, config: SystemConfig, run: _Run) -> int:
        routes = session.event_routes(node) if record is not None else []
        async with _RecordingScope(
            session,
            run,
            record,
            "events",
            nodes=sorted({r.server.name for r in routes}),
            routes=routes,
        ) as scope:
            code = await events_body(session, config, run, scope)
        return scope.exit_code(code)

    async def events_body(
        session: Session, config: SystemConfig, run: _Run, scope: _RecordingScope
    ) -> int:
        io = run.console
        stream = await session.events(
            node, min_severity=min_type.severity if min_type is not None else None
        )
        io.err(render.events_banner(stream.endpoints))
        shown = 0

        async def consume() -> None:
            nonlocal shown
            async for event in stream:
                if fmt is Format.JSON:
                    io.out(json.dumps(render.event_record(event)))
                else:
                    io.out(render.event_line(event))
                shown += 1
                if count is not None and shown >= count:
                    return

        try:
            await scope.commit()
            reason = await _run_until(consume(), timeout, "events", run)
        finally:
            await stream.close()
        return _exit_for(reason, count, shown)

    _run(ctx, as_node, verbose, body, "events")


@app.command()
def respond(
    ctx: typer.Context,
    peer: Annotated[str | None, typer.Option("--peer", help="Commanding node to answer.")] = None,
    count: CountOption = None,
    timeout: TimeoutOption = None,
    link: LinkOption = None,
    record: RecordOption = None,
    as_node: AsOption = None,
    verbose: VerboseOption = False,
) -> None:
    """Act as a flight-side node: decode commands, answer with configured telemetry."""

    async def body(session: Session, config: SystemConfig, run: _Run) -> int:
        route = session.respond_route(peer=peer, link=link) if record is not None else None
        async with _RecordingScope(
            session,
            run,
            record,
            "respond",
            nodes=[route.server.name] if route is not None else [],
            routes=[route] if route is not None else [],
        ) as scope:
            code = await respond_body(session, config, run, scope)
        return scope.exit_code(code)

    async def respond_body(
        session: Session, config: SystemConfig, run: _Run, scope: _RecordingScope
    ) -> int:
        io = run.console
        responder = await session.respond(peer=peer, link=link)
        io.err(render.respond_banner(session.node.name, responder.local_address))
        handled = 0

        async def consume() -> None:
            nonlocal handled
            async for event in responder:
                handled += 1
                if event.command is not None:
                    io.out(
                        render.respond_line(
                            session.node.name,
                            event.command.message.name,
                            event.command.header.sequence,
                            event.reply.message.name if event.reply else None,
                        )
                    )
                if count is not None and handled >= count:
                    return

        try:
            await scope.commit()
            reason = await _run_until(consume(), timeout, "respond", run)
        finally:
            await responder.close()
        return _exit_for(reason, count, handled)

    _run(ctx, as_node, verbose, body, "respond", transmits=True)


# -- record and the recording tools -------------------------------------------------------------


async def _status_loop(recorder: Recorder, console: Console) -> None:
    """One status line on stderr every :data:`STATUS_INTERVAL_S` (frames, bytes, rate, drops)."""
    last_time, last_frames = time.monotonic(), 0
    while True:
        await asyncio.sleep(STATUS_INTERVAL_S)
        stats, now = recorder.stats(), time.monotonic()
        rate = (stats.frames - last_frames) / max(now - last_time, 1e-9)
        last_time, last_frames = now, stats.frames
        dropped = stats.overrun_frames + stats.not_recorded_frames
        console.err(
            f"recording: {stats.frames} frames, {size_text(stats.file_bytes)}, "
            f"{rate:.0f} frames/s, {dropped} dropped"
        )


async def _close_when_failed(recorder: Recorder, capture: Any) -> None:
    """A recorder that stopped (a failed write) ends ``ib record`` at once."""
    assert recorder.failed_event is not None
    await recorder.failed_event.wait()
    await capture.close()


@app.command()
def record(
    ctx: typer.Context,
    nodes: Annotated[
        list[str] | None,
        typer.Argument(help="Record only the links to these nodes (default: all links)."),
    ] = None,
    link: LinkOption = None,
    output: Annotated[
        Path | None,
        typer.Option("-o", "--output", help="Recording file (default: the recording directory)."),
    ] = None,
    duration: Annotated[
        float | None,
        typer.Option(
            "--duration",
            callback=_check_timeout,
            help="Stop after this many seconds (a normal end, exit 0).",
        ),
    ] = None,
    count: Annotated[
        int | None, typer.Option("--count", min=1, help="Stop after at least N frames.")
    ] = None,
    split: Annotated[
        str | None,
        typer.Option("--split", help="Start a new part at this size (1GiB) or age (1h)."),
    ] = None,
    quiet: Annotated[bool, typer.Option("--quiet", help="No status line every 5 seconds.")] = False,
    as_node: AsOption = None,
    verbose: VerboseOption = False,
) -> None:
    """Capture all (or the selected) links of this node into a recording, decoding nothing."""

    async def body(session: Session, config: SystemConfig, run: _Run) -> int:
        io = run.console
        parts = parse_split(split) if split is not None else (None, None)
        routes = session.record_routes(nodes, link_name=link)
        scope = _RecordingScope(
            session,
            run,
            str(output) if output is not None else "auto",
            "record",
            nodes=sorted({r.server.name for r in routes}),
            routes=routes,
            split=parts,
        )
        async with scope:
            capture = await session.capture(routes)
            try:
                await scope.commit()
            except BaseException:
                await capture.close()
                raise
            recorder = scope.recorder
            assert recorder is not None
            helpers = [asyncio.ensure_future(_close_when_failed(recorder, capture))]
            if not quiet:
                helpers.append(asyncio.ensure_future(_status_loop(recorder, io)))
            try:
                where = ", ".join(term(s.endpoint, 200, err=True) for s in capture.streams)
                io.err(f"recording: listening on {where}")

                async def consume() -> None:
                    while True:
                        if not await capture.next_batch():
                            return
                        if count is not None and recorder.frames >= count:
                            return

                await _run_until(consume(), duration, "record", run)
            finally:
                for helper in helpers:
                    helper.cancel()
                await asyncio.gather(*helpers, return_exceptions=True)
                await capture.close()
        return scope.exit_code(0)

    _run(ctx, as_node, verbose, body, "record")


def _rec_failure(ledger: Ledger, console: Console, run: _Run, exc: IronbirdError) -> int:
    ledger.record_error(exc)
    _print_error(console, exc, run.verbose)
    return exc.exit_code


def _damage_lines(report: rec_inspect.Report, *, verbose: bool = False) -> list[str]:
    """The problems ``rec check`` reports: all the damage in one line (``verbose`` lists the
    stretches kept; the ledger has the same detail), then findings and unexplained holes."""
    lines: list[str] = []
    if report.damage_total:
        code = Code.REC_DAMAGED if report.mid_file_damage else Code.REC_TORN
        severity = "error" if report.mid_file_damage else "warning"
        lines.append(f"{severity} {code.value}: {damage_summary(report)}")
        if verbose and report.damage_total > 1:
            lines += [f"  {d.offset}+{d.span}: {d.reason}" for d in report.damage]
    for finding in report.findings:
        lines.append(f"error {Code.REC_DAMAGED.value}: {finding}")
    if len(report.holes) == 1:
        first, last = report.holes[0]
        lines.append(
            f"error {Code.REC_DAMAGED.value}: sequence {first}..{last} is missing and no GAP "
            "record explains it"
        )
    elif report.holes:
        first, last = report.holes[0]
        lines.append(
            f"error {Code.REC_DAMAGED.value}: {report.hole_frames} frame(s) are missing in "
            f"sequence holes no GAP record explains, the first {first}..{last}"
        )
    if report.trailer is None and not report.damage:
        lines.append(
            f"warning {Code.REC_TORN.value}: the recording ends without a trailer (not closed "
            "cleanly); every record before the end is readable"
        )
    return lines


@rec_app.command("info")
def rec_info(
    ctx: typer.Context,
    file: Annotated[Path, typer.Argument(help="The recording.")],
    fmt: Annotated[Format, typer.Option("--format", help="Output format.")] = Format.LINE,
    verbose: VerboseOption = False,
) -> None:
    """Summarise a recording: session, streams, per-MID counts, gaps, clean or torn."""

    def work(ledger: Ledger, console: Console, run: _Run) -> int:
        try:
            report = rec_inspect.info(file)
        except IronbirdError as exc:
            return _rec_failure(ledger, console, run, exc)
        ledger.record(
            "action",
            "rec.info",
            file=file,
            status=report.status,
            frames=report.frames,
            source=report.source,
        )
        if fmt is Format.JSON:
            console.out(info_json(report))
        else:
            for line in info_lines(report):
                console.out(line)
        if report.source == "scan" and report.index != "ok":
            note = term(report.index_note, 200, err=True)
            console.err(
                f"info {Code.REC_INDEX.value}: no usable index ({note}); "
                "every record was read instead (ib rec repair writes a fresh index)"
            )
        return 0

    _run(ctx, None, verbose, None, "rec.info", offline=work)


@rec_app.command("check")
def rec_check(
    ctx: typer.Context,
    file: Annotated[Path, typer.Argument(help="The recording.")],
    strict: Annotated[
        bool, typer.Option("--strict", help="A recording not closed cleanly also fails (exit 1).")
    ] = False,
    verbose: VerboseOption = False,
) -> None:
    """Read every record, check every CRC and rule; exit 1 if the file is damaged."""

    def work(ledger: Ledger, console: Console, run: _Run) -> int:
        try:
            report = rec_inspect.scan(file)
        except IronbirdError as exc:
            return _rec_failure(ledger, console, run, exc)
        unclosed = report.status in ("torn", "unclosed", "recording")
        bad = bool(report.mid_file_damage or report.findings_total or report.hole_frames)
        failed = bad or (strict and unclosed)
        ledger.record(
            "action",
            "rec.check",
            file=file,
            status=report.status,
            frames=report.frames,
            damage_total=report.damage_total,
            damage_bytes=report.damage_bytes,
            damage=[f"{d.offset}+{d.span}: {d.reason}" for d in report.damage],
            findings=report.findings_total,
            holes=report.hole_frames,
            ok=not failed,
        )
        console.out(
            f"{term(str(file), 400)}: {report.status}, {report.frames} frame(s) in "
            f"{report.chunks} chunk(s), {report.gap_total} gap record(s)"
        )
        for line in _damage_lines(report, verbose=run.verbose):
            console.err(term(line, 1000, err=True))
        if report.index != "ok" and report.trailer is not None:
            note = term(report.index_note, 200, err=True)
            console.err(f"info {Code.REC_INDEX.value}: the index is unusable ({note})")
        return 1 if failed else 0

    _run(ctx, None, verbose, None, "rec.check", offline=work)


@rec_app.command("repair")
def rec_repair(
    ctx: typer.Context,
    source: Annotated[Path, typer.Argument(metavar="IN", help="The damaged recording.")],
    output: Annotated[Path, typer.Argument(metavar="OUT", help="The repaired copy to create.")],
    force: Annotated[
        bool, typer.Option("--force", help="Replace OUT if it exists (never IN itself).")
    ] = False,
    verbose: VerboseOption = False,
) -> None:
    """Copy the valid prefix of IN plus a fresh index and trailer to a new file OUT."""

    def work(ledger: Ledger, console: Console, run: _Run) -> int:
        ledger.record("action", "rec.repair.start", source=source, output=output, force=force)
        try:
            result = rec_inspect.repair(source, output, force=force)
        except IronbirdError as exc:
            return _rec_failure(ledger, console, run, exc)
        ledger.record(
            "action",
            "rec.repair.done",
            source=source,
            output=output,
            frames=result.frames,
            bytes_kept=result.bytes_copied,
            bytes_dropped=result.dropped_bytes,
            damage=result.damage.reason if result.damage else None,
            frames_beyond_damage=result.frames_beyond,
        )
        console.out(
            f"repaired {term(str(source), 400)} -> {term(str(result.output), 400)}: "
            f"{result.frames} frame(s), {size_text(result.size)}; the original is unchanged"
        )
        damage = result.damage
        if damage is None:
            return 0
        error = damage.to_error()
        console.err(
            f"{'warning' if damage.torn else 'error'} {error.code.value}: "
            f"{term(error.summary, 600, err=True)}; {result.dropped_bytes} byte(s) were not copied"
        )
        if result.frames_beyond:
            console.err(
                f"warning {Code.REC_DAMAGED.value}: {result.frames_beyond} valid frame(s) after "
                "the damage are in the original only"
            )
        return 0 if damage.torn else EXIT_DAMAGED_INPUT

    _run(ctx, None, verbose, None, "rec.repair", offline=work)


# -- retention: prune ---------------------------------------------------------------------------

OlderThanOption = Annotated[
    str | None,
    typer.Option(
        "--older-than", help="Delete sessions older than this (default: retention.max_age)."
    ),
]
UntilUnderOption = Annotated[
    str | None,
    typer.Option(
        "--until-under",
        help="Also delete the oldest sessions until this directory is at or under this size.",
    ),
]
KeepOption = Annotated[
    str | None,
    typer.Option("--keep", help="Keep this many recent sessions (default: retention.keep_recent)."),
]
DryRunOption = Annotated[bool, typer.Option("--dry-run", help="List what would be deleted.")]
YesOption = Annotated[
    bool, typer.Option("--yes", help="Confirm the deletion without asking (needed in scripts).")
]


def _prune_command(
    ctx: typer.Context,
    kind: str,
    older_than: str | None,
    until_under: str | None,
    keep: str | None,
    dry_run: bool,
    yes: bool,
    fmt: Format,
    verbose: bool,
) -> None:
    def work(ledger: Ledger, console: Console, run: _Run) -> int:
        try:
            flags = prunecmd.parse_flags(
                older_than, until_under, keep, dry_run=dry_run, yes=yes, fmt_json=fmt is Format.JSON
            )
        except IronbirdError as exc:
            ledger.record_error(exc)
            _print_error(console, exc, run.verbose)
            return exc.exit_code
        return prunecmd.work(
            kind,
            flags,
            run.config,
            None if run.config_missing else run.config_error,
            ledger,
            console,
            verbose=run.verbose,
            fs=LEDGER_OPTIONS.get("fs", REAL_FS),
        )

    _run(ctx, None, verbose, None, f"{kind}.prune", offline=work, scan=False)


@ledger_app.command("prune")
def ledger_prune(
    ctx: typer.Context,
    older_than: OlderThanOption = None,
    until_under: UntilUnderOption = None,
    keep: KeepOption = None,
    dry_run: DryRunOption = False,
    yes: YesOption = False,
    fmt: Annotated[Format, typer.Option("--format", help="Output format.")] = Format.LINE,
    verbose: VerboseOption = False,
) -> None:
    """List old ledger sessions and delete them after confirmation (never one still in use)."""
    _prune_command(ctx, "ledger", older_than, until_under, keep, dry_run, yes, fmt, verbose)


@rec_app.command("prune")
def rec_prune(
    ctx: typer.Context,
    older_than: OlderThanOption = None,
    until_under: UntilUnderOption = None,
    keep: KeepOption = None,
    dry_run: DryRunOption = False,
    yes: YesOption = False,
    fmt: Annotated[Format, typer.Option("--format", help="Output format.")] = Format.LINE,
    verbose: VerboseOption = False,
) -> None:
    """List old recordings and delete them after confirmation (never one being written)."""
    _prune_command(ctx, "recording", older_than, until_under, keep, dry_run, yes, fmt, verbose)


# -- entry point ----------------------------------------------------------------------------


def _stderr_line(text: str) -> None:
    """Direct (unbuffered-queue) line on stderr for errors raised outside a command run."""
    if sys.stderr is None:  # closed with ``2>&-``; print(file=None) would use stdout
        return
    with contextlib.suppress(OSError, ValueError):
        print(term(text, 1000, err=True), file=sys.stderr, flush=True)


def _fill_standard_descriptors() -> None:
    """Point any closed descriptor 0-2 at ``/dev/null``.

    Otherwise the next file the process opens (the ledger, a socket) would take descriptor 1 or
    2 and receive whatever a library later writes to "standard output".
    """
    for fd in range(3):  # ascending: ``open`` returns the lowest free descriptor
        try:
            os.fstat(fd)
            continue
        except OSError as exc:
            if exc.errno != errno.EBADF:
                continue
        try:
            opened = os.open(os.devnull, os.O_RDWR)
        except OSError:
            return  # no descriptors left; _check_descriptors reports it
        if opened != fd:
            with contextlib.suppress(OSError):
                os.dup2(opened, fd)
            os.close(opened)


def main() -> int:
    """Console-script entry. Usage errors become coded one-liners (exit 2).

    Signals are handled from here on: during a command they unwind it orderly (ledger flushed,
    exit 128+n); before one starts they end the process the same way. Typer's bundled click
    moved between releases, so exceptions are recognised by shape (``format_message``) rather
    than by importing click classes.
    """
    _fill_standard_descriptors()
    for stream in (sys.stdout, sys.stderr):
        with contextlib.suppress(AttributeError, OSError, ValueError):
            stream.reconfigure(errors="backslashreplace")  # type: ignore[union-attr]
    _STATE.install()
    try:
        result: Any = app(standalone_mode=False)
    except _Terminated as stop:
        return 128 + stop.signum
    except Exception as exc:
        format_message = getattr(exc, "format_message", None)
        if callable(format_message):
            _stderr_line(f"error {Code.CLI_USAGE.value}: {format_message()}")
            return 2
        _stderr_line(f"error {Code.INTERNAL.value}: unexpected {type(exc).__name__}: {exc}")
        return 1
    return result if isinstance(result, int) else 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
