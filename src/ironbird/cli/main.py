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
from ironbird.cli.output import (
    CLOSE_WAIT_S,
    HANDLED_SIGNALS,
    BoundedWriter,
    Console,
    signals_held,
)
from ironbird.config import SystemConfig, find_config, load_config
from ironbird.errors import Code, IronbirdError
from ironbird.ledger import Ledger, default_state_dir
from ironbird.session import Session, Side

EXIT_TIMEOUT: Final = 3  # --count was given and not reached before --timeout
EXIT_OUTPUT_LOST: Final = 4  # the command completed but stdout lines were lost
MAX_TIMEOUT_S: Final = 365 * 24 * 3600.0
OUTPUT_POLL_S: Final = 0.5
DRAIN_NOTICE_S: Final = 2.0  # waiting this long for the reader of stdout earns one notice
SIGNAL_POLL_S: Final = 0.02
MIN_FREE_FDS: Final = 8  # event loop (3), UDP sockets, ledger, resolver threads, margin
_SIGNALS: Final = HANDLED_SIGNALS

app = typer.Typer(
    name="ironbird",
    help="Integrated/system test tool for cFS vehicles: send commands, listen to telemetry.",
    no_args_is_help=True,
    add_completion=False,
    pretty_exceptions_enable=False,
)


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


Body = Callable[[Session, SystemConfig, "_Run"], Awaitable[int]]


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
    try:
        config = load_config(find_config(glob.config))
    except IronbirdError as exc:
        ledger.record_error(exc)
        _print_error(console, exc, run.verbose)
        return exc.exit_code
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
        ledger.flush()
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
        ledger.close()
        if not signum and (got := _poll_signal(0)):  # arrived while the ledger was closing
            code = _record_signal(name, got, run, ledger, console, direct=True)
            ledger.close()
        _STATE.restore_default()  # a later signal ends the process by that signal, not silently
    return code


def _run(
    ctx: typer.Context,
    as_node: str | None,
    verbose: bool,
    body: Body,
    name: str,
    *,
    transmits: bool = False,
) -> None:
    """Open the ledger, run ``body`` in an event loop, flush everything, exit with its code.

    ``transmits`` commands (they put packets on a link) refuse to run without a ledger.
    """
    base: Globals = ctx.obj
    glob = Globals(base.config, as_node or base.role, base.verbose or verbose)
    console = Console(sys.stdout, sys.stderr)
    run = _Run(console, glob.verbose)
    ledger = Ledger(Path("/nonexistent"), enabled=False)
    code = 1
    try:
        try:
            ledger = Ledger(
                default_state_dir(),
                on_failure=lambda exc: _print_error(console, exc, glob.verbose),
            )
            ledger.open()
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
    as_node: AsOption = None,
    verbose: VerboseOption = False,
) -> None:
    """Decode and print incoming packets (telemetry as ground; commands as a flight node)."""

    async def body(session: Session, config: SystemConfig, run: _Run) -> int:
        io = run.console
        mids = _parse_mids(session, node, mid, link)
        listener = await session.listen(node, mids=mids, link=link)
        io.err(render.listen_banner(node, listener.local_address))
        shown = 0
        show_hex = session_verbose(ctx, verbose)

        async def consume() -> None:
            nonlocal shown
            async for rx in listener:
                if fmt is Format.JSON:
                    io.out(json.dumps(render.record(rx.node, rx.time_ns, rx.packet)))
                else:
                    io.out(render.line(rx.node, rx.time_ns, rx.packet))
                if show_hex:
                    io.err(f"  from {term(rx.source, err=True)} size {rx.packet.size}")
                shown += 1
                if count is not None and shown >= count:
                    return

        try:
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
    as_node: AsOption = None,
    verbose: VerboseOption = False,
) -> None:
    """Listen on every telemetry link of all (or selected) nodes and print only events."""

    async def body(session: Session, config: SystemConfig, run: _Run) -> int:
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
    as_node: AsOption = None,
    verbose: VerboseOption = False,
) -> None:
    """Act as a flight-side node: decode commands, answer with configured telemetry."""

    async def body(session: Session, config: SystemConfig, run: _Run) -> int:
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
            reason = await _run_until(consume(), timeout, "respond", run)
        finally:
            await responder.close()
        return _exit_for(reason, count, handled)

    _run(ctx, as_node, verbose, body, "respond", transmits=True)


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
