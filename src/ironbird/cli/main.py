"""Command line front-end. Thin: parse arguments, call the session, print results.

Errors leave as one line ``error IB-XXX-NNN: summary`` on stderr (full detail with
``--verbose`` and always in the ledger) and a non-zero exit code.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import signal
import sys
import traceback
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any

import typer

from ironbird import render
from ironbird.ccdd.model import EventSeverity, MessageKind
from ironbird.config import SystemConfig, find_config, load_config
from ironbird.errors import Code, IronbirdError
from ironbird.ledger import Ledger, default_state_dir
from ironbird.sanitize import clean
from ironbird.session import Session

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


AsOption = Annotated[
    str | None, typer.Option("--as", help="Act as this node (default: acts_as in the config).")
]
VerboseOption = Annotated[
    bool, typer.Option("--verbose", "-v", help="Show full error detail and packet hex.")
]
LinkOption = Annotated[str | None, typer.Option("--link", help="Link name, if several match.")]
CountOption = Annotated[int | None, typer.Option("--count", min=1, help="Stop after N packets.")]
TimeoutOption = Annotated[
    float | None, typer.Option("--timeout", min=0.01, help="Stop after this many seconds.")
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


# -- plumbing --------------------------------------------------------------------------


def _out(text: str) -> None:
    print(text, flush=True)


def _err(text: str) -> None:
    print(text, file=sys.stderr, flush=True)


def _print_error(exc: IronbirdError, verbose: bool) -> None:
    _err(exc.user_line())
    if verbose:
        for line in exc.detail_text().splitlines():
            _err(f"  {clean(line, 2000)}")


Body = Callable[[Session, SystemConfig], Awaitable[int]]


async def _main(glob: Globals, ledger: Ledger, body: Body, verbose: bool) -> int:
    try:
        config = load_config(find_config(glob.config))
    except IronbirdError as exc:
        ledger.record_error(exc)
        _print_error(exc, verbose)
        return exc.exit_code
    try:
        async with Session(config, role=glob.role, ledger=ledger, notify=_err) as session:
            try:
                return await body(session, config)
            except IronbirdError as exc:
                session.fatal(exc)
                _print_error(exc, verbose)
                return exc.exit_code
    except IronbirdError as exc:  # role resolution failed before the session started
        ledger.record_error(exc)
        _print_error(exc, verbose)
        return exc.exit_code


def _run(ctx: typer.Context, as_node: str | None, verbose: bool, body: Body, name: str) -> None:
    base: Globals = ctx.obj
    glob = Globals(base.config, as_node or base.role, base.verbose or verbose)
    ledger = Ledger(
        default_state_dir(),
        on_failure=lambda exc: _print_error(exc, glob.verbose),
    )
    ledger.record("action", "cli.invoke", command=name, argv=sys.argv[1:])
    code = 1
    try:
        code = asyncio.run(_main(glob, ledger, body, glob.verbose))
    except Exception as exc:
        err = IronbirdError(
            Code.INTERNAL,
            f"unexpected {type(exc).__name__}: {exc}",
            details=traceback.format_exc(),
            cause=exc,
        )
        ledger.record_error(err)
        _print_error(err, glob.verbose)
    finally:
        ledger.close()
    raise typer.Exit(code)


async def _run_until(
    consumer: Coroutine[Any, Any, None], timeout: float | None, name: str, session: Session
) -> None:
    """Run ``consumer`` until it finishes, the timeout passes, or SIGINT/SIGTERM/SIGHUP."""
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    installed: list[signal.Signals] = []
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        with contextlib.suppress(NotImplementedError, RuntimeError):
            loop.add_signal_handler(sig, stop.set)
            installed.append(sig)
    task = asyncio.ensure_future(consumer)
    stopper = asyncio.ensure_future(stop.wait())
    try:
        await asyncio.wait({task, stopper}, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
        reason = "done" if task.done() else ("signal" if stop.is_set() else "timeout")
        session.ledger.record("state", f"{name}.ending", reason=reason)
    finally:
        for pending in (task, stopper):
            if not pending.done():
                pending.cancel()
        results = await asyncio.gather(task, stopper, return_exceptions=True)
        for sig in installed:
            loop.remove_signal_handler(sig)
    outcome = results[0]
    if isinstance(outcome, BaseException) and not isinstance(outcome, asyncio.CancelledError):
        raise outcome


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

    async def body(session: Session, config: SystemConfig) -> int:
        models = session.models()  # loads and validates every CCDD and header profile
        rows = [("NODE", "ALIAS", "MESSAGES", "CCDD / DESCRIPTION")]
        for name, node in config.nodes.items():
            mark = "*" if name == session.node.name else ""
            model = models.get(name)
            count = f"{len(model.commands())} cmd / {len(model.telemetry())} tlm" if model else "-"
            detail = str(node.ccdd) if node.ccdd else node.description or "-"
            rows.append((f"{name}{mark}", node.alias, count, clean(detail)))
        widths = [max(len(r[i]) for r in rows) for i in range(3)]
        for row in rows:
            _out("  ".join(row[i].ljust(widths[i]) for i in range(3)) + "  " + row[3])
        _out(f"acting as: {session.node.name} ({session.node.alias})   * = this node")
        if config.links:
            _out("links:")
            for link in config.links:
                _out(
                    f"  {link.name}: {link.source} -> {link.destination} {link.transport}"
                    f"  commands {link.command_endpoint}  telemetry {link.telemetry_endpoint}"
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

    async def body(session: Session, config: SystemConfig) -> int:
        matches = session.find(query)
        session.ledger.record("action", "find", query=query, results=len(matches))
        for match in matches[:limit]:
            msg = match.message
            fc = f" fc={msg.function_code}" if msg.function_code is not None else ""
            desc = f"  {clean(msg.description, 80)}" if msg.description else ""
            cat = f" cat={msg.category.value}" if msg.kind is MessageKind.TELEMETRY else ""
            _out(
                f"{match.tier:<11} {match.node}  {msg.kind.value:<9} {clean(msg.name)}"
                f"  mid=0x{msg.mid:04X}{fc}{cat}{desc}"
            )
        if len(matches) > limit:
            _err(f"{len(matches) - limit} more matches not shown (raise --limit)")
        if not matches:
            _err(f"no match for {clean(query, 40)!r}")
            return 1
        return 0

    _run(ctx, as_node, verbose, body, "find")


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
        int, typer.Option("--length-delta", help="Fault injection: add N to the length field.")
    ] = 0,
    as_node: AsOption = None,
    verbose: VerboseOption = False,
) -> None:
    """Encode and send one command; print a one-line confirmation."""

    async def body(session: Session, config: SystemConfig) -> int:
        parsed = _parse_args(args)
        tamper: dict[str, int] = {}
        profile = session.codec_for(session.route(node, link_name=link).server.name).profile(
            MessageKind.COMMAND
        )
        if bad_checksum:
            if profile.checksum_field is None:
                raise IronbirdError(Code.CLI_USAGE, f"profile {profile.name} has no checksum field")
            tamper[profile.checksum_field.name] = 1
        if length_delta:
            if profile.length_field is None:
                raise IronbirdError(Code.CLI_USAGE, f"profile {profile.name} has no length field")
            tamper[profile.length_field.name] = length_delta
        sent = await session.send_command(node, command, parsed, tamper=tamper, link=link)
        _out(
            f"sent {clean(sent.command)} to {sent.node} via udp {sent.endpoint} "
            f"(mid 0x{sent.mid:04X} fc {sent.function_code} seq {sent.sequence}, "
            f"{sent.size} bytes)"
        )
        if session_verbose(ctx, verbose):
            _out(f"  hex: {sent.data.hex(' ')}")
        return 0

    _run(ctx, as_node, verbose, body, "send")


def session_verbose(ctx: typer.Context, local: bool) -> bool:
    base: Globals = ctx.obj
    return base.verbose or local


def _parse_mids(session: Session, node: str, items: list[str] | None, link: str | None):
    if not items:
        return None
    route = session.route(node, link_name=link)
    model = session.codec_for(route.server.name).model
    mids: set[int] = set()
    for item in items:
        try:
            mids.add(int(item, 0))
            continue
        except ValueError:
            pass
        msg = model.by_name.get(item)
        if msg is None:
            raise IronbirdError(
                Code.CLI_USAGE, f"--mid {item[:40]!r} is neither a number nor a message name"
            )
        mids.add(msg.mid)
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

    async def body(session: Session, config: SystemConfig) -> int:
        mids = _parse_mids(session, node, mid, link)
        listener = await session.listen(node, mids=mids, link=link)
        _err(
            f"listening for {node} on udp {listener.local_address[0]}:{listener.local_address[1]}"
            if listener.local_address
            else f"listening for {node}"
        )
        shown = 0
        show_hex = session_verbose(ctx, verbose)

        async def consume() -> None:
            nonlocal shown
            async for rx in listener:
                if fmt is Format.JSON:
                    _out(json.dumps(render.record(rx.node, rx.time_ns, rx.packet)))
                else:
                    _out(render.line(rx.node, rx.time_ns, rx.packet))
                if show_hex:
                    _err(f"  from {rx.source} size {rx.packet.size}")
                shown += 1
                if count is not None and shown >= count:
                    return

        try:
            await _run_until(consume(), timeout, "listen", session)
        finally:
            await listener.close()
        return 0

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

    async def body(session: Session, config: SystemConfig) -> int:
        stream = await session.events(
            node, min_severity=min_type.severity if min_type is not None else None
        )
        where = ", ".join(
            f"{name} (udp {addr[0]}:{addr[1]})" if addr else name for name, addr in stream.endpoints
        )
        _err(f"listening for events from {where}")
        shown = 0

        async def consume() -> None:
            nonlocal shown
            async for event in stream:
                if fmt is Format.JSON:
                    _out(json.dumps(render.event_record(event)))
                else:
                    _out(render.event_line(event))
                shown += 1
                if count is not None and shown >= count:
                    return

        try:
            await _run_until(consume(), timeout, "events", session)
        finally:
            await stream.close()
        return 0

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

    async def body(session: Session, config: SystemConfig) -> int:
        responder = await session.respond(peer=peer, link=link)
        addr = responder.local_address
        _err(f"{session.node.name} responding on udp {addr[0]}:{addr[1]}" if addr else "responding")
        handled = 0

        async def consume() -> None:
            nonlocal handled
            async for event in responder:
                handled += 1
                if event.command is not None:
                    msg = event.command.message
                    reply = f" -> {event.reply.message.name}" if event.reply else ""
                    _out(
                        f"{session.node.name} rx {clean(msg.name)} "
                        f"seq={event.command.header.sequence}{reply}"
                    )
                if count is not None and handled >= count:
                    return

        try:
            await _run_until(consume(), timeout, "respond", session)
        finally:
            await responder.close()
        return 0

    _run(ctx, as_node, verbose, body, "respond")


# -- entry point ----------------------------------------------------------------------------


def main() -> int:
    """Console-script entry. Usage errors become coded one-liners (exit 2).

    Typer's bundled click moved between releases, so exceptions are recognised by shape
    (``format_message``) rather than by importing click classes.
    """
    try:
        result: Any = app(standalone_mode=False)
    except Exception as exc:
        if type(exc).__name__ == "Abort":
            _err("interrupted")
            return 130
        format_message = getattr(exc, "format_message", None)
        if callable(format_message):
            _err(f"error {Code.CLI_USAGE.value}: {clean(str(format_message()))}")
            return 2
        _err(f"error {Code.INTERNAL.value}: unexpected {type(exc).__name__}: {clean(str(exc))}")
        return 1
    return result if isinstance(result, int) else 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
