"""Command line front-end. Thin: parse arguments, call the session, print results.

Errors leave as one line ``error IB-XXX-NNN: summary`` on stderr (full detail with
``--verbose`` and always in the ledger) and a non-zero exit code.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import math
import signal
import sys
import traceback
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any, Final

import typer

from ironbird import render
from ironbird.ccdd.model import EventSeverity, MessageKind
from ironbird.cli.output import Console
from ironbird.config import SystemConfig, find_config, load_config
from ironbird.errors import Code, IronbirdError
from ironbird.ledger import Ledger, default_state_dir
from ironbird.session import Session, Side

EXIT_TIMEOUT: Final = 3  # --count was given and not reached before --timeout
MAX_TIMEOUT_S: Final = 365 * 24 * 3600.0
OUTPUT_POLL_S: Final = 0.5
_SIGNALS: Final = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)

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
        help="Stop after this many seconds (exit 3 if --count was not reached).",
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
    ``async with`` exits unwind normally; before and after the loop it raises
    :class:`_Terminated`. Only the first signal acts; repeats while shutting down are ignored.
    """

    def __init__(self) -> None:
        self.signum = 0
        self._task: asyncio.Task[Any] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    def install(self) -> None:
        for sig in _SIGNALS:
            with contextlib.suppress(ValueError, OSError):  # not the main thread
                signal.signal(sig, self._handle)

    def attach(self, task: asyncio.Task[Any], loop: asyncio.AbstractEventLoop) -> None:
        self._task, self._loop = task, loop

    def detach(self) -> None:
        self._task = self._loop = None

    def _handle(self, signum: int, _frame: object) -> None:
        if self.signum:
            return
        self.signum = signum
        task, loop = self._task, self._loop
        if task is not None and loop is not None and not task.done():
            loop.call_soon_threadsafe(task.cancel)
        else:
            raise _Terminated(signum)


_STATE = _SignalState()


@contextlib.contextmanager
def _signals_blocked() -> Any:
    """Hold signals back while the ledger and output are closed; they act afterwards."""
    blocked = hasattr(signal, "pthread_sigmask")
    if blocked:
        signal.pthread_sigmask(signal.SIG_BLOCK, _SIGNALS)
    try:
        yield
    finally:
        if blocked:
            signal.pthread_sigmask(signal.SIG_UNBLOCK, _SIGNALS)


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
        loop.call_soon_threadsafe(pipe_closed.set)

    console.stdout.set_on_closed(reader_gone)
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
            notify=lambda line: console.err(term(line, 1000, err=True)),
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
        except IronbirdError as exc:
            _print_error(console, exc, glob.verbose)
            if transmits:
                console.err("nothing was sent: the action ledger is required")
                raise typer.Exit(exc.exit_code) from exc
            ledger.disable()
        ledger.record("action", "cli.invoke", command=name, argv=sys.argv[1:])
        try:
            code = asyncio.run(_main(glob, ledger, body, run))
        except (_Terminated, asyncio.CancelledError) as stop:
            if not _STATE.signum:
                raise
            signum = stop.signum if isinstance(stop, _Terminated) else _STATE.signum
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
            console.err(notice)
            code = 128 + int(signum)
        except Exception as exc:
            err = IronbirdError(
                Code.INTERNAL,
                f"unexpected {type(exc).__name__}: {exc}",
                details=traceback.format_exc(),
                cause=exc,
            )
            ledger.record_error(err)
            _print_error(console, err, glob.verbose)
        if console.stdout.reader_gone:
            ledger.record("state", "output.pipe_closed", stream="stdout")
    finally:
        _STATE.detach()
        with _signals_blocked():
            ledger.close()
            console.close()
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
            run.console.err(f"no match for {term(query, 40, err=True)!r}")
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
    with contextlib.suppress(OSError, ValueError):
        print(term(text, 1000, err=True), file=sys.stderr, flush=True)


def main() -> int:
    """Console-script entry. Usage errors become coded one-liners (exit 2).

    Signals are handled from here on: during a command they unwind it orderly (ledger flushed,
    exit 128+n); before one starts they end the process the same way. Typer's bundled click
    moved between releases, so exceptions are recognised by shape (``format_message``) rather
    than by importing click classes.
    """
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
