"""Coded errors: the single vocabulary shared by every layer and front-end.

User-facing text is one line (``error IB-XXX-NNN: summary``); everything else (details,
cause chain, raw bytes) rides along for ``--verbose`` and the ledger.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Final

from ironbird.sanitize import clean


class Code(StrEnum):
    """Stable error codes. Never renumber or reuse a code."""

    # Configuration
    CFG_FILE = "IB-CFG-001"  # config file missing / unreadable / too large
    CFG_SYNTAX = "IB-CFG-002"  # config TOML syntax error
    CFG_SCHEMA = "IB-CFG-003"  # config schema violation (unknown key, wrong type, ...)
    CFG_REF = "IB-CFG-004"  # unknown node / alias / message referenced
    CFG_LINK = "IB-CFG-005"  # no usable link between nodes, or wrong role for the action
    # Header profiles
    HDR_FILE = "IB-HDR-001"  # profile unreadable or TOML syntax error
    HDR_SCHEMA = "IB-HDR-002"  # profile schema violation
    HDR_PACK = "IB-HDR-003"  # value cannot be packed into the header
    # CCDD
    CCD_FILE = "IB-CCD-001"  # CCDD file unreadable / syntax error
    CCD_SCHEMA = "IB-CCD-002"  # CCDD schema violation
    CCD_LAYOUT = "IB-CCD-003"  # CCDD layout violation (overlap, size, duplicates)
    # Encode
    ENC_UNKNOWN = "IB-ENC-001"  # unknown command / telemetry message
    ENC_ARG = "IB-ENC-002"  # bad argument value
    ENC_ARGSET = "IB-ENC-003"  # missing or unexpected arguments
    ENC_FIT = "IB-ENC-004"  # message does not fit the header profile or size cap
    # Decode (DecodeError instances carry these)
    DEC_SHORT = "IB-DEC-001"  # datagram shorter than the header
    DEC_OVERSIZE = "IB-DEC-002"  # datagram above the configured cap
    DEC_LENGTH = "IB-DEC-003"  # header length field disagrees with the datagram
    DEC_FIXED = "IB-DEC-004"  # fixed header field has the wrong value
    DEC_CHECKSUM = "IB-DEC-005"  # checksum mismatch
    DEC_UNKNOWN = "IB-DEC-006"  # MID / function code not in the CCDD
    DEC_PAYLOAD = "IB-DEC-007"  # payload size disagrees with the CCDD
    DEC_COUNT = "IB-DEC-008"  # variable-length count above the CCDD maximum
    DEC_OVERRUN = "IB-DEC-009"  # variable-length data runs past the end of the packet
    # Transport
    TX_OPEN = "IB-TX-001"  # cannot open / bind
    TX_SEND = "IB-TX-002"  # send failed or refused
    TX_RECV = "IB-TX-003"  # receive failed: a socket read errored (may end the capture)
    TX_OVERRUN = "IB-TX-004"  # datagrams dropped by the kernel socket buffer (or left unread)
    TX_RCVBUF = "IB-TX-005"  # kernel granted a smaller receive buffer than requested (warning)
    # Events
    EVT_NONE = "IB-EVT-001"  # no event telemetry is defined for the selected node(s)
    # Responder
    RSP_RULE = "IB-RSP-001"  # response rule failed
    # Ledger
    LED_WRITE = "IB-LED-001"  # ledger stalled: an essential record cannot be written (retrying)
    LED_DIR = "IB-LED-002"  # no usable ledger/state directory (HOME unset, path unusable)
    LED_TIER = "IB-LED-003"  # event or evidence tier over its allowance: counted, not written
    LED_UNLOGGED = (
        "IB-LED-004"  # a transmission whose ledger record was not (or may not be) written
    )
    LED_DISK = "IB-LED-005"  # disk or quota full: essential records go to the preallocated reserve
    LED_RESERVE = "IB-LED-006"  # the essential reserve is not available or not guaranteed
    LED_RESUMED = "IB-LED-007"  # the ledger writes again after a stall
    LED_UNBOUNDED = "IB-LED-008"  # evidence_bytes_per_hour = "unlimited"
    LED_NOLOCK = "IB-LED-009"  # file locks unavailable: liveness rests on freshness
    LED_ROTATE = "IB-LED-010"  # rotation blocked: part names taken or numbers exhausted
    # Retention
    RET_AGE = "IB-RET-001"  # sessions older than retention.max_age (review with prune)
    RET_TOTAL = "IB-RET-002"  # ledger + recordings past, or projected past, max_total_bytes
    RET_PRUNE = "IB-RET-003"  # a listed session was not deleted
    RET_SCAN = "IB-RET-004"  # retention scan partial, or a reserve that could not be recovered
    RET_TARGET = "IB-RET-005"  # --until-under cannot be reached: the rest is protected
    RET_UNSAFE = "IB-RET-006"  # directory writable by others without the sticky bit
    # Output
    OUT_DROPPED = "IB-OUT-001"  # output lines dropped because stdout/stderr is not draining
    OUT_LOST = "IB-OUT-002"  # final tally: output lines dropped, abandoned or never deliverable
    OUT_WAITING = "IB-OUT-003"  # ending, but still waiting for the reader of stdout to take lines
    # Recordings
    REC_CREATE = "IB-REC-001"  # cannot create the recording or a repaired copy
    REC_WRITE = "IB-REC-002"  # a recording write or sync failed; recording stopped
    REC_OVERRUN = "IB-REC-003"  # recorder buffer full; frames dropped (GAP recorded)
    REC_FORMAT = "IB-REC-004"  # not an ironbird recording (or unreadable)
    REC_VERSION = "IB-REC-005"  # unsupported version or unknown critical record type
    REC_DAMAGED = "IB-REC-006"  # damaged record mid-file
    REC_TORN = "IB-REC-007"  # recording ends in an incomplete record (not closed cleanly)
    REC_INDEX = "IB-REC-008"  # index missing or invalid; rebuilt by scanning
    REC_DEFS = "IB-REC-009"  # embedded definitions differ from the config, or cannot be loaded
    REC_SPACE = "IB-REC-010"  # low disk space for the recording
    REC_DIRSYNC = "IB-REC-011"  # the recording's directory cannot be synced (name not durable)
    REC_LEFTOVER = "IB-REC-012"  # a repaired copy is complete, but its temporary name is left over
    # Export
    EXP_WRITE = "IB-EXP-001"  # the export output could not be written
    EXP_EXISTS = "IB-EXP-002"  # the export output exists, is the input, or is a managed file
    # Engine
    ENG_LAG = "IB-ENG-001"  # a consumer fell behind: frames not shown to it (sinks still saw them)
    # System resources
    SYS_RESOURCE = "IB-SYS-001"  # the OS refused a resource needed to start (file descriptors)
    # Filter expressions (--where)
    FLT_SYNTAX = "IB-FLT-001"  # filter syntax error (column, excerpt)
    FLT_NAME = "IB-FLT-002"  # unknown name in a filter, or a reserved name misused
    FLT_TYPE = "IB-FLT-003"  # literal or operator does not fit the type; filter can never match
    FLT_LIMIT = "IB-FLT-004"  # filter too long, deep or wide
    # CLI / internal
    CLI_USAGE = "IB-CLI-001"  # command line usage error
    INTERNAL = "IB-INT-001"  # unexpected internal error (bug)

    @property
    def is_warning(self) -> bool:
        """True for a condition that does not stop the work: shown and ledgered as a warning."""
        return self in WARNING_CODES


#: Codes whose default report level is warning (``warning IB-XXX-NNN: ...``, ledger kind
#: ``warning``) because the operation continues. Every other code defaults to error. The default
#: is only a default: a reporter that passes an explicit level (``Session.report(level=...)``,
#: ``Ledger.record_error(level=...)``) sets the ledger kind and the displayed label, so a code
#: outside this set can be reported as a warning and a member can be reported as an error. A
#: warning code is never raised, so it never decides an exit code.
WARNING_CODES: Final[frozenset[Code]] = frozenset(
    {Code.TX_RCVBUF, Code.REC_DIRSYNC, Code.REC_LEFTOVER}
)


MAX_SUMMARY_LEN = 600
_USAGE_PREFIXES = ("IB-CFG-", "IB-HDR-", "IB-CCD-", "IB-CLI-", "IB-EVT-", "IB-FLT-")


class IronbirdError(Exception):
    """An error with a stable code, a one-line summary and optional full detail."""

    def __init__(
        self,
        code: Code,
        summary: str,
        details: str = "",
        cause: BaseException | None = None,
    ) -> None:
        # Summaries embed text from untrusted files and packets: control characters
        # (including newlines) become visible escapes so a summary is always one inert line.
        summary = clean(summary, MAX_SUMMARY_LEN)
        super().__init__(f"{code.value}: {summary}")
        assert "\n" not in summary
        self._code: Code = code
        self._summary: str = summary
        self.details: str = details
        self.cause: BaseException | None = cause
        if cause is not None:
            self.__cause__ = cause

    @property
    def code(self) -> Code:
        return self._code

    @property
    def summary(self) -> str:
        """One sanitized line, fixed at construction."""
        return self._summary

    def user_line(self) -> str:
        """The single line shown to the user (the summary is sanitized at construction)."""
        label = "warning" if self.code.is_warning else "error"
        return f"{label} {self.code.value}: {self.summary}"

    def detail_text(self) -> str:
        """Full detail for ``--verbose`` and the ledger: details plus the cause chain."""
        parts: list[str] = []
        if self.details:
            parts.append(self.details)
        cause: BaseException | None = self.cause
        depth = 0
        while cause is not None and depth < 8:
            parts.append(f"caused by {type(cause).__name__}: {cause}")
            cause = cause.__cause__
            depth += 1
        return "\n".join(parts)

    @property
    def exit_code(self) -> int:
        """2 for configuration/usage problems the user fixes in files, 1 otherwise."""
        return 2 if self.code.value.startswith(_USAGE_PREFIXES) else 1


@dataclass(frozen=True, slots=True)
class DecodeError:
    """Typed result of a failed parse: where and why, never an exception."""

    code: Code
    offset: int
    reason: str
    mid: int | None = None

    def __post_init__(self) -> None:
        assert self.code.value.startswith("IB-DEC-"), "DecodeError uses IB-DEC-* codes"
        assert self.offset >= 0

    def to_error(self, context: str = "") -> IronbirdError:
        prefix = f"{context}: " if context else ""
        mid_text = f"0x{self.mid:04X}" if self.mid is not None else ""
        # Many reasons already name the MID; say it once.
        mid = f" (MID {mid_text})" if mid_text and mid_text not in self.reason else ""
        return IronbirdError(
            self.code,
            f"{prefix}{self.reason}{mid}",
            details=f"offset {self.offset}",
        )
