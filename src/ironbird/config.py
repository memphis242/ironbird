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
from ironbird.units import (
    DURATION_FORMS,
    GIB,
    KIB,
    MIB,
    SIZE_FORMS,
    TIB,
    parse_duration,
    parse_size,
)

MAX_CONFIG_BYTES: Final = 1024 * 1024
_NAME_RE: Final = re.compile(r"[A-Za-z][A-Za-z0-9_\-]{0,31}")
_IDENT_RE: Final = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,63}")
_HOST_RE: Final = re.compile(r"[A-Za-z0-9._\-:\[\]]{1,253}")
MAX_PORT_DIGITS: Final = 5
SUPPORTED_TRANSPORTS: Final = ("udp", "serial")
UDP_TRANSPORT: Final = "udp"
SERIAL_TRANSPORT: Final = "serial"

# The serial line settings a link may name, each with the values the transport can apply.
MIN_BAUD: Final = 50
MAX_BAUD: Final = 12_000_000
DATA_BITS: Final = (5, 6, 7, 8)
PARITIES: Final = ("none", "even", "odd", "mark", "space")
STOP_BITS: Final = (1.0, 1.5, 2.0)
FLOW_CONTROLS: Final = ("none", "rtscts", "xonxoff")
MAX_SYNC_BYTES: Final = 8
MIN_IDLE_MS: Final = 1
MAX_IDLE_MS: Final = 10_000
DEFAULT_IDLE_MS: Final = 50
FRAMING_KINDS: Final = ("header_length",)


def format_hostport(host: str, port: int) -> str:
    """``host:port``, with an IPv6 literal in brackets (``[::1]:5000``)."""
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


@dataclass(frozen=True, slots=True)
class Endpoint:
    host: str
    port: int

    def __str__(self) -> str:
        return format_hostport(self.host, self.port)


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
class UdpLinkSpec:
    """The UDP shape of a link: where each side receives.

    ``command_endpoint`` is where the link's destination receives commands and
    ``telemetry_endpoint`` is where its source receives telemetry.
    """

    command_endpoint: Endpoint
    telemetry_endpoint: Endpoint


@dataclass(frozen=True, slots=True)
class SerialLinkSpec:
    """The serial shape of a link: one device carries both directions."""

    device: str  # absolute path, free of control characters
    baud: int
    data_bits: int = 8
    parity: str = "none"
    stop_bits: float = 1.0
    flow: str = "none"

    def __post_init__(self) -> None:
        assert self.device.startswith("/") and MIN_BAUD <= self.baud <= MAX_BAUD
        assert self.data_bits in DATA_BITS and self.parity in PARITIES
        assert self.stop_bits in STOP_BITS and self.flow in FLOW_CONTROLS

    @property
    def bits_per_char(self) -> float:
        """Line bits one byte takes: start bit, data bits, parity bit if any, stop bits."""
        return 1 + self.data_bits + (0 if self.parity == "none" else 1) + self.stop_bits


@dataclass(frozen=True, slots=True)
class FramingSpec:
    """How a serial byte stream is cut into packets (``kind = "header_length"``: the header
    profile's length field)."""

    kind: str = "header_length"
    sync: bytes = b""  # a marker before every packet, consumed by the framer; empty: none
    max_idle_ms: int = DEFAULT_IDLE_MS
    require_known_mid: bool = True

    def __post_init__(self) -> None:
        assert self.kind in FRAMING_KINDS and len(self.sync) <= MAX_SYNC_BYTES
        assert MIN_IDLE_MS <= self.max_idle_ms <= MAX_IDLE_MS


@dataclass(frozen=True, slots=True)
class Link:
    """Directional link. Commands flow ``source`` -> ``destination``; telemetry back.

    The ``destination`` node owns the CCDD and header profiles describing both directions.
    Exactly one of ``udp`` and ``serial`` is set, the one ``transport`` names; a serial link
    also has its ``framing``.
    """

    name: str
    source: str
    destination: str
    transport: str
    udp: UdpLinkSpec | None = None
    serial: SerialLinkSpec | None = None
    framing: FramingSpec | None = None

    def __post_init__(self) -> None:
        assert (self.udp is None) != (self.serial is None), "exactly one transport spec is set"
        assert (self.transport == UDP_TRANSPORT) == (self.udp is not None), (
            "the spec matches the transport"
        )
        assert (self.serial is None) == (self.framing is None), "a serial link has its framing"

    @property
    def command_endpoint(self) -> Endpoint:
        """UDP links only: where ``destination`` receives commands."""
        assert self.udp is not None, f"link {self.name} is not a UDP link"
        return self.udp.command_endpoint

    @property
    def telemetry_endpoint(self) -> Endpoint:
        """UDP links only: where ``source`` receives telemetry."""
        assert self.udp is not None, f"link {self.name} is not a UDP link"
        return self.udp.telemetry_endpoint

    @property
    def command_target(self) -> str:
        """Where ``destination`` receives commands, as display text (the device for serial)."""
        if self.serial is not None:
            return self.serial.device
        return str(self.command_endpoint)

    @property
    def telemetry_target(self) -> str:
        """Where ``source`` receives telemetry, as display text (the device for serial)."""
        if self.serial is not None:
            return self.serial.device
        return str(self.telemetry_endpoint)


@dataclass(frozen=True, slots=True)
class RecordingConfig:
    """The ``[recording]`` table: where recordings go and how they are written."""

    dir: Path | None = None  # None: $IRONBIRD_RECORD_DIR, else the XDG data directory
    fsync: str = "chunk"  # "chunk" | "always" | "never"
    max_buffer_bytes: int = 256 * 1024 * 1024
    min_free_bytes: int = 1024 * 1024 * 1024
    embed_definitions: bool = True  # decision D1


UNLIMITED: Final = "unlimited"
ON_FULL_POLICIES: Final = ("continue", "refuse_commands")


@dataclass(frozen=True, slots=True)
class LedgerConfig:
    """The ``[ledger]`` table: tier allowances, the essential reserve, rotation, the full-disk
    policy. ``None`` for an allowance means ``"unlimited"``."""

    event_bytes_per_hour: int | None = 160 * MIB
    evidence_bytes_per_hour: int | None = 32 * MIB
    evidence_raw_bytes_per_second: int = 256 * KIB
    essential_reserve_bytes: int = 32 * MIB
    max_file_bytes: int = 32 * MIB
    on_full: str = "continue"


@dataclass(frozen=True, slots=True)
class RetentionConfig:
    """The ``[retention]`` table. ``None`` for ``max_age_s`` or ``max_total_bytes`` means
    ``"unlimited"`` (that warning is off). Retention only warns; ``ib ledger prune`` and
    ``ib rec prune`` are the only deletion."""

    max_age_s: int | None = 7 * 86400
    max_total_bytes: int | None = 8 * GIB
    keep_recent: int = 10
    warn_interval_s: int = 86400
    scan_interval_s: int = 600


@dataclass(frozen=True, slots=True)
class SystemConfig:
    path: Path
    acts_as: str
    max_packet_bytes: int
    nodes: Mapping[str, Node]
    links: tuple[Link, ...]
    recording: RecordingConfig = RecordingConfig()
    ledger: LedgerConfig = LedgerConfig()
    retention: RetentionConfig = RetentionConfig()

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
        if not isinstance(value, str) or not _NAME_RE.fullmatch(value):
            raise self.fail(path, "must start with a letter; letters, digits, '-' and '_' only")
        return value

    def ident(self, value: object, path: str) -> str:
        if not isinstance(value, str) or not _IDENT_RE.fullmatch(value):
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
        if (
            not _HOST_RE.fullmatch(host)
            or not port_text.isascii()
            or not port_text.isdigit()
            or len(port_text) > MAX_PORT_DIGITS
        ):
            raise self.fail(path, 'must be a string "host:port" with a port in 1..65535')
        port = int(port_text)
        if not 1 <= port <= 65535:
            raise self.fail(path, f"port must be 1..65535, got {port}")
        return Endpoint(host.strip("[]"), port)

    def device(self, value: object, path: str) -> str:
        if (
            not isinstance(value, str)
            or not value.startswith("/")
            or len(value) > 1024
            or any(ord(c) < 0x20 or ord(c) == 0x7F for c in value)
        ):
            raise self.fail(path, "must be an absolute device path without control characters")
        return value


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
        ctx.ident(key, f"{path}.fields.{key}")
        if isinstance(value, bool) or not isinstance(value, int | float | str):
            raise ctx.fail(f"{path}.fields.{key}", "must be a number or string")
        fields[key] = value
    echo: dict[str, str] = {}
    raw_echo = raw.get("echo", {})
    if not isinstance(raw_echo, dict):
        raise ctx.fail(f"{path}.echo", "must be a table")
    for key, value in raw_echo.items():
        ctx.ident(key, f"{path}.echo.{key}")
        echo[key] = ctx.ident(value, f"{path}.echo.{key}")
    if on == "*" and echo:
        raise ctx.fail(
            f"{path}.echo", "not allowed on a '*' rule; commands differ in arguments, name one"
        )
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
        ccdd_adapter=ctx.ident(
            ctx.string(raw, "ccdd_adapter", path, "json"), f"{path}.ccdd_adapter"
        ),
        command_header=ctx.file(raw, "command_header", path),
        telemetry_header=ctx.file(raw, "telemetry_header", path),
        responses=responses,
    )


MIN_BUFFER_BYTES: Final = 1024 * 1024
MAX_BUFFER_BYTES: Final = 64 * 1024 * 1024 * 1024
MAX_FREE_BYTES: Final = 1 << 50
FSYNC_POLICIES: Final = ("chunk", "always", "never")


def _config_int(
    ctx: _Ctx, table: Mapping[str, Any], key: str, path: str, low: int, high: int
) -> int:
    value = table[key]
    if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
        raise ctx.fail(f"{path}.{key}", f"must be an integer in {low}..{high}")
    return value


def _parse_recording(ctx: _Ctx, raw: object) -> RecordingConfig:
    if not isinstance(raw, dict):
        raise ctx.fail("recording", "must be a table")
    ctx.keys(
        raw,
        "recording",
        {"dir", "fsync", "max_buffer_bytes", "min_free_bytes", "embed_definitions"},
        set(),
    )
    defaults = RecordingConfig()
    fsync = ctx.string(raw, "fsync", "recording", defaults.fsync)
    if fsync not in FSYNC_POLICIES:
        raise ctx.fail("recording.fsync", f"must be one of {', '.join(FSYNC_POLICIES)}")
    embed = raw.get("embed_definitions", defaults.embed_definitions)
    if not isinstance(embed, bool):
        raise ctx.fail("recording.embed_definitions", "must be true or false")
    return RecordingConfig(
        dir=ctx.file(raw, "dir", "recording"),
        fsync=fsync,
        max_buffer_bytes=(
            _config_int(
                ctx, raw, "max_buffer_bytes", "recording", MIN_BUFFER_BYTES, MAX_BUFFER_BYTES
            )
            if "max_buffer_bytes" in raw
            else defaults.max_buffer_bytes
        ),
        min_free_bytes=(
            _config_int(ctx, raw, "min_free_bytes", "recording", 0, MAX_FREE_BYTES)
            if "min_free_bytes" in raw
            else defaults.min_free_bytes
        ),
        embed_definitions=embed,
    )


def _config_size(
    ctx: _Ctx,
    table: Mapping[str, Any],
    key: str,
    path: str,
    low: int,
    high: int,
    default: int | None,
    *,
    unlimited: bool = False,
) -> int | None:
    """A size: TOML integer (bytes) or a string in the size grammar; ``"unlimited"`` (exactly
    that spelling) only where ``unlimited`` allows it."""
    if key not in table:
        return default
    value = table[key]
    forms = SIZE_FORMS + (" or exactly 'unlimited'" if unlimited else "")
    if isinstance(value, bool):
        raise ctx.fail(f"{path}.{key}", f"must be {forms}")
    if isinstance(value, str) and unlimited and value == UNLIMITED:
        return None
    size = (
        value if isinstance(value, int) else parse_size(value) if isinstance(value, str) else None
    )
    if size is None:
        raise ctx.fail(f"{path}.{key}", f"must be {forms}")
    if not low <= size <= high:
        raise ctx.fail(f"{path}.{key}", f"must be from {low} to {high} bytes")
    return size


def _config_duration(
    ctx: _Ctx,
    table: Mapping[str, Any],
    key: str,
    path: str,
    low: int,
    high: int,
    default: int | None,
    *,
    unlimited: bool = False,
) -> int | None:
    if key not in table:
        return default
    value = table[key]
    forms = DURATION_FORMS + (" or exactly 'unlimited'" if unlimited else "")
    if isinstance(value, str) and unlimited and value == UNLIMITED:
        return None
    seconds = parse_duration(value) if isinstance(value, str) else None
    if seconds is None:
        raise ctx.fail(f"{path}.{key}", f"must be {forms}")
    if not low <= seconds <= high:
        raise ctx.fail(f"{path}.{key}", f"must be from {low} to {high} seconds")
    return seconds


def _parse_ledger(ctx: _Ctx, raw: object) -> LedgerConfig:
    if not isinstance(raw, dict):
        raise ctx.fail("ledger", "must be a table")
    ctx.keys(
        raw,
        "ledger",
        {
            "event_bytes_per_hour",
            "evidence_bytes_per_hour",
            "evidence_raw_bytes_per_second",
            "essential_reserve_bytes",
            "max_file_bytes",
            "on_full",
        },
        set(),
    )
    d = LedgerConfig()
    on_full = ctx.string(raw, "on_full", "ledger", d.on_full)
    if on_full not in ON_FULL_POLICIES:
        raise ctx.fail("ledger.on_full", f"must be one of {', '.join(ON_FULL_POLICIES)}")
    reserve = _config_size(
        ctx, raw, "essential_reserve_bytes", "ledger", MIB, GIB, d.essential_reserve_bytes
    )
    part = _config_size(ctx, raw, "max_file_bytes", "ledger", MIB, GIB, d.max_file_bytes)
    rate = _config_size(
        ctx,
        raw,
        "evidence_raw_bytes_per_second",
        "ledger",
        KIB,
        64 * MIB,
        d.evidence_raw_bytes_per_second,
    )
    assert reserve is not None and part is not None and rate is not None
    return LedgerConfig(
        event_bytes_per_hour=_config_size(
            ctx,
            raw,
            "event_bytes_per_hour",
            "ledger",
            MIB,
            TIB,
            d.event_bytes_per_hour,
            unlimited=True,
        ),
        evidence_bytes_per_hour=_config_size(
            ctx,
            raw,
            "evidence_bytes_per_hour",
            "ledger",
            MIB,
            TIB,
            d.evidence_bytes_per_hour,
            unlimited=True,
        ),
        evidence_raw_bytes_per_second=rate,
        essential_reserve_bytes=reserve,
        max_file_bytes=part,
        on_full=on_full,
    )


def _parse_retention(ctx: _Ctx, raw: object) -> RetentionConfig:
    if not isinstance(raw, dict):
        raise ctx.fail("retention", "must be a table")
    ctx.keys(
        raw,
        "retention",
        {"max_age", "max_total_bytes", "keep_recent", "warn_interval", "scan_interval"},
        set(),
    )
    d = RetentionConfig()
    warn = _config_duration(
        ctx, raw, "warn_interval", "retention", 60, 30 * 86400, d.warn_interval_s
    )
    scan = _config_duration(ctx, raw, "scan_interval", "retention", 60, 86400, d.scan_interval_s)
    assert warn is not None and scan is not None
    return RetentionConfig(
        max_age_s=_config_duration(
            ctx, raw, "max_age", "retention", 60, 100_000 * 86400, d.max_age_s, unlimited=True
        ),
        max_total_bytes=_config_size(
            ctx,
            raw,
            "max_total_bytes",
            "retention",
            MIB,
            1024 * TIB,
            d.max_total_bytes,
            unlimited=True,
        ),
        keep_recent=_config_int(ctx, raw, "keep_recent", "retention", 0, 1000)
        if "keep_recent" in raw
        else d.keep_recent,
        warn_interval_s=warn,
        scan_interval_s=scan,
    )


def _parse_serial(ctx: _Ctx, raw: object, path: str) -> SerialLinkSpec:
    if not isinstance(raw, dict):
        raise ctx.fail(path, "must be a table")
    ctx.keys(
        raw,
        path,
        {"device", "baud", "data_bits", "parity", "stop_bits", "flow"},
        {"device", "baud"},
    )
    defaults = SerialLinkSpec(device="/", baud=MIN_BAUD)

    def word(key: str, allowed: tuple[str, ...], default: str) -> str:
        value = raw.get(key, default)
        if not isinstance(value, str) or value not in allowed:
            raise ctx.fail(f"{path}.{key}", f"must be one of {', '.join(allowed)}")
        return value

    data_bits = raw.get("data_bits", defaults.data_bits)
    if isinstance(data_bits, bool) or not isinstance(data_bits, int) or data_bits not in DATA_BITS:
        raise ctx.fail(f"{path}.data_bits", f"must be one of {', '.join(map(str, DATA_BITS))}")
    stop_bits = raw.get("stop_bits", defaults.stop_bits)
    if (
        isinstance(stop_bits, bool)
        or not isinstance(stop_bits, int | float)
        or float(stop_bits) not in STOP_BITS
    ):
        raise ctx.fail(f"{path}.stop_bits", "must be one of 1, 1.5, 2")
    return SerialLinkSpec(
        device=ctx.device(raw["device"], f"{path}.device"),
        baud=_config_int(ctx, raw, "baud", path, MIN_BAUD, MAX_BAUD),
        data_bits=data_bits,
        parity=word("parity", PARITIES, defaults.parity),
        stop_bits=float(stop_bits),
        flow=word("flow", FLOW_CONTROLS, defaults.flow),
    )


def _parse_sync(ctx: _Ctx, value: object, path: str) -> bytes:
    """A sync marker: hex digits (an even number, at most ``MAX_SYNC_BYTES`` bytes), or empty."""
    digits = len(value) if isinstance(value, str) else 0
    if (
        not isinstance(value, str)
        or digits > 2 * MAX_SYNC_BYTES
        or digits % 2
        or not value.isascii()
        or not all(c in "0123456789abcdefABCDEF" for c in value)
    ):
        raise ctx.fail(
            path,
            f'must be hex digits for at most {MAX_SYNC_BYTES} bytes (e.g. "1ACFFC1D"), or empty',
        )
    return bytes.fromhex(value)


def _parse_framing(ctx: _Ctx, raw: object, path: str) -> FramingSpec:
    if not isinstance(raw, dict):
        raise ctx.fail(path, "must be a table")
    ctx.keys(raw, path, {"kind", "sync", "max_idle_ms", "require_known_mid"}, set())
    defaults = FramingSpec()
    kind = raw.get("kind", defaults.kind)
    if not isinstance(kind, str) or kind not in FRAMING_KINDS:
        raise ctx.fail(f"{path}.kind", f"must be one of {', '.join(FRAMING_KINDS)}")
    require = raw.get("require_known_mid", defaults.require_known_mid)
    if not isinstance(require, bool):
        raise ctx.fail(f"{path}.require_known_mid", "must be true or false")
    return FramingSpec(
        kind=kind,
        sync=_parse_sync(ctx, raw["sync"], f"{path}.sync") if "sync" in raw else defaults.sync,
        max_idle_ms=(
            _config_int(ctx, raw, "max_idle_ms", path, MIN_IDLE_MS, MAX_IDLE_MS)
            if "max_idle_ms" in raw
            else defaults.max_idle_ms
        ),
        require_known_mid=require,
    )


def _parse_link_transport(
    ctx: _Ctx, raw: dict[str, Any], lpath: str
) -> tuple[str, UdpLinkSpec | None, SerialLinkSpec | None, FramingSpec | None]:
    """The transport of one link table and its transport-specific part. Keys of the other
    transport are refused by name, not as unknown."""
    transport = ctx.string(raw, "transport", lpath, UDP_TRANSPORT)
    if transport not in SUPPORTED_TRANSPORTS:
        raise ctx.fail(
            f"{lpath}.transport",
            f"unsupported transport {transport[:20]!r}; "
            f"supported: {', '.join(SUPPORTED_TRANSPORTS)}",
        )
    common = {"name", "from", "to", "transport"}
    if transport == SERIAL_TRANSPORT:
        ctx.keys(raw, lpath, common | {"serial", "framing"}, {"from", "to", "serial"})
        return (
            transport,
            None,
            _parse_serial(ctx, raw["serial"], f"{lpath}.serial"),
            _parse_framing(ctx, raw.get("framing", {}), f"{lpath}.framing"),
        )
    ctx.keys(
        raw,
        lpath,
        common | {"command_endpoint", "telemetry_endpoint"},
        {"from", "to", "command_endpoint", "telemetry_endpoint"},
    )
    udp = UdpLinkSpec(
        command_endpoint=ctx.endpoint(raw["command_endpoint"], f"{lpath}.command_endpoint"),
        telemetry_endpoint=ctx.endpoint(raw["telemetry_endpoint"], f"{lpath}.telemetry_endpoint"),
    )
    return transport, udp, None, None


def parse_config(data: Mapping[str, Any], path: Path) -> SystemConfig:
    """Validate a parsed TOML mapping (paths resolve against ``path.parent``)."""
    ctx = _Ctx(path)
    ctx.keys(
        data, "", {"ironbird", "nodes", "links", "recording", "ledger", "retention"}, {"nodes"}
    )

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
    devices: dict[str, str] = {}
    for i, raw in enumerate(raw_links):
        lpath = f"links[{i}]"
        if not isinstance(raw, dict):
            raise ctx.fail(lpath, "must be a table")
        transport, udp, serial, framing = _parse_link_transport(ctx, raw, lpath)
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
        lname = ctx.name(raw["name"], f"{lpath}.name") if "name" in raw else f"{ends[0]}-{ends[1]}"
        if lname in seen_names:
            raise ctx.fail(f"{lpath}.name", f"duplicate link name {lname!r}")
        seen_names.add(lname)
        if serial is not None:
            owner = devices.setdefault(serial.device, lname)
            if owner != lname:
                raise ctx.fail(
                    f"{lpath}.serial.device",
                    f"{serial.device} is already the device of link {owner!r}; one device "
                    "carries one link",
                )
        links.append(
            Link(
                name=lname,
                source=ends[0],
                destination=ends[1],
                transport=transport,
                udp=udp,
                serial=serial,
                framing=framing,
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
        recording=_parse_recording(ctx, data["recording"])
        if "recording" in data
        else RecordingConfig(),
        ledger=_parse_ledger(ctx, data["ledger"]) if "ledger" in data else LedgerConfig(),
        retention=_parse_retention(ctx, data["retention"])
        if "retention" in data
        else RetentionConfig(),
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
    except (ValueError, RecursionError) as exc:
        # TOMLDecodeError and UnicodeDecodeError are ValueErrors; so is the int-digit limit.
        # RecursionError is deeply nested arrays/tables.
        raise IronbirdError(Code.CFG_SYNTAX, f"{path}: invalid TOML ({exc})", cause=exc) from exc
    return parse_config(data, path)
