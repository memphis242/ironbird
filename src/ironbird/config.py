"""``ironbird.toml``: the system as a set of nodes joined by links.

Everything is validated up front; each problem is reported as ``file: key.path: message`` with
a stable code. Relative paths are resolved against the config file's directory.
"""

from __future__ import annotations

import os
import re
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final

from ironbird.errors import Code, IronbirdError
from ironbird.header import DEFAULT_MAX_PACKET_BYTES

MAX_CONFIG_BYTES: Final = 1024 * 1024
_NAME_RE: Final = re.compile(r"^[A-Za-z][A-Za-z0-9_\-]{0,31}$")
_IDENT_RE: Final = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
_HOST_RE: Final = re.compile(r"^[A-Za-z0-9._\-:\[\]]{1,253}$")
SUPPORTED_TRANSPORTS: Final = ("udp",)


@dataclass(frozen=True, slots=True)
class Endpoint:
    host: str
    port: int

    def __str__(self) -> str:
        return f"{self.host}:{self.port}"


@dataclass(frozen=True, slots=True)
class ResponseRule:
    """What a responding node answers to a command.

    ``fields`` are literal reply values, ``echo`` copies command arguments into reply
    fields (``reply_field = "command_arg"``), and the counters fill reply fields with the
    number of accepted / rejected commands so far.
    """

    on: str  # command name, or "*" for any command
    reply: str  # telemetry message name
    fields: Mapping[str, int | float | str]
    echo: Mapping[str, str]
    accepted_counter: str | None
    rejected_counter: str | None


@dataclass(frozen=True, slots=True)
class Node:
    name: str
    alias: str
    description: str
    ccdd: Path | None
    ccdd_adapter: str
    command_header: Path | None
    telemetry_header: Path | None
    responses: tuple[ResponseRule, ...]


@dataclass(frozen=True, slots=True)
class Link:
    """Directional UDP link. Commands flow ``source`` -> ``destination``; telemetry back.

    ``command_endpoint`` is where ``destination`` receives commands and
    ``telemetry_endpoint`` is where ``source`` receives telemetry. The ``destination`` node
    owns the CCDD and header profiles describing both directions.
    """

    name: str
    source: str
    destination: str
    transport: str
    command_endpoint: Endpoint
    telemetry_endpoint: Endpoint


@dataclass(frozen=True, slots=True)
class SystemConfig:
    path: Path
    acts_as: str
    max_packet_bytes: int
    nodes: Mapping[str, Node]
    links: tuple[Link, ...]

    def resolve(self, name_or_alias: str) -> Node:
        """Look a node up by name or alias."""
        node = self.nodes.get(name_or_alias)
        if node is not None:
            return node
        for candidate in self.nodes.values():
            if candidate.alias == name_or_alias:
                return candidate
        raise IronbirdError(
            Code.CFG_REF,
            f"unknown node {name_or_alias[:32]!r}; known: {', '.join(self.nodes)}",
            details=f"config {self.path}",
        )

    def links_between(self, a: str, b: str) -> tuple[Link, ...]:
        return tuple(
            link
            for link in self.links
            if {link.source, link.destination} == {a, b} and link.source != link.destination
        )

    def links_of(self, node: str) -> tuple[Link, ...]:
        return tuple(link for link in self.links if node in (link.source, link.destination))


class _Ctx:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.base = path.parent

    def fail(self, key_path: str, message: str, code: Code = Code.CFG_SCHEMA) -> IronbirdError:
        return IronbirdError(code, f"{self.path}: {key_path}: {message}")

    def keys(
        self, table: Mapping[str, Any], path: str, allowed: set[str], required: set[str]
    ) -> None:
        for key in table:
            if key not in allowed:
                raise self.fail(f"{path}.{key}" if path else key, "unknown key")
        for key in sorted(required - table.keys()):
            raise self.fail(f"{path}.{key}" if path else key, "required key is missing")

    def string(self, table: Mapping[str, Any], key: str, path: str, default: str = "") -> str:
        value = table.get(key, default)
        if not isinstance(value, str) or len(value) > 1024:
            raise self.fail(f"{path}.{key}", "must be a string of at most 1024 characters")
        return value

    def name(self, value: object, path: str) -> str:
        if not isinstance(value, str) or not _NAME_RE.match(value):
            raise self.fail(path, "must start with a letter; letters, digits, '-' and '_' only")
        return value

    def ident(self, value: object, path: str) -> str:
        if not isinstance(value, str) or not _IDENT_RE.match(value):
            raise self.fail(path, "must be a CCDD identifier (letters, digits, underscore)")
        return value

    def file(self, table: Mapping[str, Any], key: str, path: str) -> Path | None:
        if key not in table:
            return None
        text = self.string(table, key, path)
        if not text:
            raise self.fail(f"{path}.{key}", "must not be empty")
        candidate = Path(text)
        return candidate if candidate.is_absolute() else self.base / candidate

    def endpoint(self, value: object, path: str) -> Endpoint:
        if not isinstance(value, str) or ":" not in value:
            raise self.fail(path, 'must be a string "host:port"')
        host, _, port_text = value.rpartition(":")
        if not _HOST_RE.match(host) or not port_text.isascii() or not port_text.isdigit():
            raise self.fail(path, 'must be a string "host:port"')
        port = int(port_text)
        if not 1 <= port <= 65535:
            raise self.fail(path, f"port must be 1..65535, got {port}")
        return Endpoint(host.strip("[]"), port)


def _parse_response(ctx: _Ctx, raw: object, path: str) -> ResponseRule:
    if not isinstance(raw, dict):
        raise ctx.fail(path, "must be a table")
    ctx.keys(
        raw,
        path,
        {"on", "reply", "fields", "echo", "accepted_counter", "rejected_counter"},
        {"on", "reply"},
    )
    on = ctx.string(raw, "on", path)
    if on != "*":
        ctx.ident(on, f"{path}.on")
    reply = ctx.ident(raw["reply"], f"{path}.reply")
    fields: dict[str, int | float | str] = {}
    raw_fields = raw.get("fields", {})
    if not isinstance(raw_fields, dict):
        raise ctx.fail(f"{path}.fields", "must be a table")
    for key, value in raw_fields.items():
        if isinstance(value, bool) or not isinstance(value, int | float | str):
            raise ctx.fail(f"{path}.fields.{key}", "must be a number or string")
        fields[key] = value
    echo: dict[str, str] = {}
    raw_echo = raw.get("echo", {})
    if not isinstance(raw_echo, dict):
        raise ctx.fail(f"{path}.echo", "must be a table")
    for key, value in raw_echo.items():
        if not isinstance(value, str):
            raise ctx.fail(f"{path}.echo.{key}", "must be a command argument name")
        echo[key] = value
    counters: dict[str, str | None] = {}
    for key in ("accepted_counter", "rejected_counter"):
        counters[key] = ctx.ident(raw[key], f"{path}.{key}") if key in raw else None
    return ResponseRule(
        on=on,
        reply=reply,
        fields=MappingProxyType(fields),
        echo=MappingProxyType(echo),
        accepted_counter=counters["accepted_counter"],
        rejected_counter=counters["rejected_counter"],
    )


def _parse_node(ctx: _Ctx, name: str, raw: object) -> Node:
    path = f"nodes.{name}"
    if not isinstance(raw, dict):
        raise ctx.fail(path, "must be a table")
    ctx.keys(
        raw,
        path,
        {
            "alias",
            "description",
            "ccdd",
            "ccdd_adapter",
            "command_header",
            "telemetry_header",
            "responses",
        },
        set(),
    )
    alias = ctx.name(raw["alias"], f"{path}.alias") if "alias" in raw else name
    raw_responses = raw.get("responses", [])
    if not isinstance(raw_responses, list):
        raise ctx.fail(f"{path}.responses", "must be an array of tables")
    responses = tuple(
        _parse_response(ctx, r, f"{path}.responses[{i}]") for i, r in enumerate(raw_responses)
    )
    return Node(
        name=name,
        alias=alias,
        description=ctx.string(raw, "description", path),
        ccdd=ctx.file(raw, "ccdd", path),
        ccdd_adapter=ctx.string(raw, "ccdd_adapter", path, "json"),
        command_header=ctx.file(raw, "command_header", path),
        telemetry_header=ctx.file(raw, "telemetry_header", path),
        responses=responses,
    )


def parse_config(data: Mapping[str, Any], path: Path) -> SystemConfig:
    """Validate a parsed TOML mapping (paths resolve against ``path.parent``)."""
    ctx = _Ctx(path)
    ctx.keys(data, "", {"ironbird", "nodes", "links"}, {"nodes"})

    settings = data.get("ironbird", {})
    if not isinstance(settings, dict):
        raise ctx.fail("ironbird", "must be a table")
    ctx.keys(settings, "ironbird", {"acts_as", "max_packet_bytes"}, set())
    max_bytes = settings.get("max_packet_bytes", DEFAULT_MAX_PACKET_BYTES)
    if (
        isinstance(max_bytes, bool)
        or not isinstance(max_bytes, int)
        or not 64 <= max_bytes <= 65535
    ):
        raise ctx.fail("ironbird.max_packet_bytes", "must be an integer in 64..65535")

    raw_nodes = data["nodes"]
    if not isinstance(raw_nodes, dict) or not raw_nodes:
        raise ctx.fail("nodes", "must be a non-empty table of nodes")
    nodes: dict[str, Node] = {}
    claimed: dict[str, str] = {}
    for name, raw in raw_nodes.items():
        ctx.name(name, f"nodes.{name}")
        node = _parse_node(ctx, name, raw)
        for label in {node.name, node.alias}:
            if label in claimed:
                raise ctx.fail(
                    f"nodes.{name}", f"name/alias {label!r} already used by node {claimed[label]!r}"
                )
            claimed[label] = name
        nodes[name] = node

    acts_as_text = settings.get("acts_as")
    if acts_as_text is None:
        acts_as = next(iter(nodes))
    else:
        if not isinstance(acts_as_text, str) or acts_as_text not in claimed:
            raise ctx.fail(
                "ironbird.acts_as",
                f"must name a configured node or alias; known: {', '.join(nodes)}",
                Code.CFG_REF,
            )
        acts_as = claimed[acts_as_text]

    raw_links = data.get("links", [])
    if not isinstance(raw_links, list):
        raise ctx.fail("links", "must be an array of tables")
    links: list[Link] = []
    seen_names: set[str] = set()
    for i, raw in enumerate(raw_links):
        lpath = f"links[{i}]"
        if not isinstance(raw, dict):
            raise ctx.fail(lpath, "must be a table")
        ctx.keys(
            raw,
            lpath,
            {"name", "from", "to", "transport", "command_endpoint", "telemetry_endpoint"},
            {"from", "to", "command_endpoint", "telemetry_endpoint"},
        )
        ends: list[str] = []
        for key in ("from", "to"):
            value = raw[key]
            if not isinstance(value, str) or value not in claimed:
                raise ctx.fail(
                    f"{lpath}.{key}",
                    f"must name a configured node or alias; known: {', '.join(nodes)}",
                    Code.CFG_REF,
                )
            ends.append(claimed[value])
        if ends[0] == ends[1]:
            raise ctx.fail(lpath, "'from' and 'to' must be different nodes")
        transport = ctx.string(raw, "transport", lpath, "udp")
        if transport not in SUPPORTED_TRANSPORTS:
            raise ctx.fail(
                f"{lpath}.transport",
                f"unsupported transport {transport[:20]!r}; "
                f"supported: {', '.join(SUPPORTED_TRANSPORTS)}",
            )
        lname = ctx.string(raw, "name", lpath, f"{ends[0]}-{ends[1]}")
        if lname in seen_names:
            raise ctx.fail(f"{lpath}.name", f"duplicate link name {lname!r}")
        seen_names.add(lname)
        links.append(
            Link(
                name=lname,
                source=ends[0],
                destination=ends[1],
                transport=transport,
                command_endpoint=ctx.endpoint(raw["command_endpoint"], f"{lpath}.command_endpoint"),
                telemetry_endpoint=ctx.endpoint(
                    raw["telemetry_endpoint"], f"{lpath}.telemetry_endpoint"
                ),
            )
        )
        dest = nodes[ends[1]]
        for attr in ("ccdd", "command_header", "telemetry_header"):
            if getattr(dest, attr) is None:
                raise ctx.fail(
                    f"nodes.{dest.name}.{attr}",
                    f"required because link {lname!r} ends at node {dest.name!r}",
                    Code.CFG_REF,
                )
    return SystemConfig(
        path=path,
        acts_as=acts_as,
        max_packet_bytes=max_bytes,
        nodes=MappingProxyType(nodes),
        links=tuple(links),
    )


def find_config(explicit: Path | None, environ: Mapping[str, str] | None = None) -> Path:
    """Explicit path, ``$IRONBIRD_CONFIG``, ``./ironbird.toml``, then the XDG config dir."""
    env = os.environ if environ is None else environ
    if explicit is not None:
        return explicit
    from_env = env.get("IRONBIRD_CONFIG")
    if from_env:
        return Path(from_env)
    candidates = [Path("ironbird.toml")]
    xdg = env.get("XDG_CONFIG_HOME")
    base = Path(xdg) if xdg else Path.home() / ".config"
    candidates.append(base / "ironbird" / "ironbird.toml")
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise IronbirdError(
        Code.CFG_FILE,
        "no config found; pass --config PATH or set IRONBIRD_CONFIG",
        details="searched: " + ", ".join(str(c) for c in candidates),
    )


def load_config(path: Path) -> SystemConfig:
    try:
        with path.open("rb") as handle:
            raw = handle.read(MAX_CONFIG_BYTES + 1)
    except OSError as exc:
        raise IronbirdError(
            Code.CFG_FILE, f"{path}: cannot read config ({exc.strerror or exc})", cause=exc
        ) from exc
    if len(raw) > MAX_CONFIG_BYTES:
        raise IronbirdError(Code.CFG_FILE, f"{path}: config larger than 1 MiB")
    try:
        data = tomllib.loads(raw.decode("utf-8"))
    except (tomllib.TOMLDecodeError, UnicodeDecodeError) as exc:
        raise IronbirdError(Code.CFG_SYNTAX, f"{path}: invalid TOML ({exc})", cause=exc) from exc
    return parse_config(data, path)
