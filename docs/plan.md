# ironbird - staged design, Stage 1 and Stage 2 plans

## Context

ironbird is a terminal tool (CLI + TUI) for integrated cFS vehicle testing, run over SSH
on a server wired to flight hardware (UDP, serial; 1553 later), with no extra privileges.
The repo currently holds only Claude Code config. We build in small stages, each one
usable on its own. This plan fixes the architecture that all stages share and spells
out Stage 1 in detail; later stages are outlined only, and will each get their own
plan when we reach them.

Decisions so far: Python 3.11 + Textual, plain pip/venv (via `pyenv`), no `uv`;
ironbird can act as any node of the system (usually ground); CCDD pipeline is "CSV -> JSON -> headers" but the
exact format is unknown, so ironbird reads a placeholder JSON behind an adapter;
Stage 1 is CLI send + receive.

## Architecture (all stages)

- **Python 3.11** (no 3.12+ syntax/stdlib), `pyenv virtualenv` + `pip install -e .[dev]`,
  setuptools `pyproject.toml`, `src/` layout, package `ironbird`, console scripts
  `ironbird` and `ib`. Dependencies kept few and stock (Typer, Textual, pyserial,
  openpyxl later); `tomllib` from the stdlib. CLI via Typer; TUI via Textual (Stage 3). Core is `asyncio`.
  Front-ends are thin: all behavior lives in the core, so CLI and TUI cannot drift.
- **Layers** (`src/ironbird/`):
  - `errors.py` - `IronbirdError(code, summary, details, cause)`; stable codes like
    `IB-CFG-001`, `IB-TX-003`. One-line summary for the user, details for the ledger.
  - `ledger.py` - action ledger: JSON Lines, one record per action/state change/error,
    UTC ISO 8601 with microseconds, session id. Default `$XDG_STATE_HOME/ironbird/`.
  - `config.py` - `ironbird.toml` describing the **system as a set of nodes** (ground,
    flight computer, payload, simulators...), each with an alias, its CCDD source,
    header profiles, and network/serial endpoints; plus **links** between nodes
    (UDP host:port pairs, serial device + baud). Validated up front with clear errors.
  - **Roles ("acting as")**: ironbird runs *as* one node (`--as ground` default, set
    in config). Its role decides direction: as ground it encodes commands and decodes
    telemetry; as a flight-side node it decodes incoming commands and emits telemetry
    (canned, scripted, or replayed). The codec is symmetric, so this costs no special
    code paths, and the same session API serves both roles.
  - `header.py` - **user-defined header profiles** (the answer to "custom headers"):
    a TOML file declaring header fields in order with bit widths, endianness, fixed
    values or computed values (`length`, `sequence`, `apid_from_mid`, `crc`), plus the
    CRC algorithm/span/placement. A generic bitfield codec packs/unpacks it. Ships with
    `ccsds_v1_cfs_cmd.toml` / `ccsds_v1_cfs_tlm.toml` (primary + cFS secondary header).
    Proprietary extensions are edits to a profile, not to code (and from Stage 4 a TUI
    header-profile editor that writes the same TOML); an escape hatch
    (`computed = "python:module.func"`) covers anything the declarative form cannot.
  - `ccdd/` - `model.py` (Message, Field, Enum; command vs telemetry; telemetry category
    and event mapping; fixed and variable-length fields), `loader.py` with a pluggable `Adapter` protocol and a
    placeholder JSON adapter. When your real JSON format is known, we write one adapter.
  - `codec.py` - encode command (header + args + CRC), decode telemetry into fields
    using `struct`-precompiled layouts per message (fast path).
  - `transport/` - `Transport` protocol (`open`, `send`, `receive` async iterator,
    `close`); `udp.py` now, `serial.py` (pyserial; user must already be in `dialout`),
    `mil1553.py` later.
  - `session.py` - wires target(s) + codec + ledger; the single API both front-ends use.
  - `cli/`, `tui/` - front-ends.
- **Recording format** (Stage 2, specified in "Stage 2" S3): append-only, CRC-checked
  records of *raw* frames (seq, ns timestamp, stream, source, MID, flags, length, bytes),
  decoded lazily on replay/export. Raw capture is the cheapest write path and never loses data to a
  decode bug. Exports (CSV, text, xlsx) decode from it.
- **Error UX**: one line, stable code, repeats coalesced (`x37 in 10 s`), rate-limited
  per code; full detail via `--verbose`/ledger (CLI) or a details pane (TUI).

## Telemetry categories and events

Not all telemetry is equally important, and cFS events are the most important: the
operator's first question after any action is "what did the vehicle say about it?".
Every telemetry message therefore has a **category**, declared in the CCDD
(`"category"`, default `other`), and the category's **priority** (1 = most important)
is the one ordering every view and alert path shares:

| Priority | Category (`category`) | Meaning |
|----------|-----------------------|---------|
| 1 | `event` | cFS events (EVS long/short event packets) and project equivalents |
| 2 | `command_response` | telemetry sent as the direct answer to a command |
| 3 | `housekeeping` | periodic HK packets |
| 4 | `status` | other periodic status telemetry |
| 5 | `commanded` | sent only in response to a request (diagnostics, dumps, registry reports) |
| 6 | `other` | anything else |

- **Model.** `ccdd.model.Category` is an ordered enum (declaration order = priority,
  `Category.priority`); commands have no category. The category is validated strictly:
  an unknown value, a category on a command, or `event` without a mapping is a coded
  CCDD error, not a silent default.
- **Event mapping.** An event message also carries
  `"event": {"app": <field>, "id": <field>, "type": <field>, "message": <field>}` naming
  the fields that hold the app name, event id, event type and text (`id` and `type`
  required; `app` and `message` optional, short-format events have no text). The
  `type` field has an enum whose labels map onto the normalised severities
  `DEBUG < INFO < WARNING < ERROR < CRITICAL` (case-insensitive, `INFORMATION` counts as
  `INFO`), so `--min-type` and severity colouring work for any project's wire format.
  Events render identically everywhere whatever the layout: `events.extract_event()` is
  the one place that reads the mapping.
- **Use.** Priority is the default sort, emphasis and filter-preset key in all views
  (packet views, replay, exports; "events only", "events + command responses",
  "housekeeping"), and orders alert prioritisation: an event outranks a decode warning
  about housekeeping. `ib listen` lines and JSON records carry the category.
- **Events page (TUI, Stage 3).** Besides appearing in the packet views like any packet,
  events get their own **Events** page: one row per event received from *all* cFS
  instances (all nodes), with node, time, app, event id, type/severity and message,
  newest last, severity-coloured, filterable by node / app / minimum type, with the raw
  packet one keypress away. It is fed by the same event extraction as the CLI.
- **CLI (Stage 1).** `ib events [--node X ...] [--min-type debug|info|warning|error|critical]
  [--format line|json]` listens on all telemetry links of all (or the selected) nodes at
  once and prints only events, one line each: time, node, app, type, id, sanitized
  message. The ledger records every received event in full (app, id, type, text, raw
  packet), including those `--min-type` hides: events are rare and valuable, so they are
  never sampled or truncated there.

## Execution model and performance

Budgets: design target 10k packets/s aggregate with zero capture loss; hard floor
1k packets/s; packets <= 1500 B, ~200 B average (2 MB/s at target, 200 KB/s at floor). TUI redraw at
15-20 Hz, keystroke latency under 50 ms.

**What this means.** At 2 MB/s, raw capture and recording are cheap; decoding every packet
costs roughly 5-20 us each in Python, i.e. 5-20% of a core at 10k/s, which is why the
UI decodes only what is visible. The
risk is not throughput but *interference*: a heavy redraw, a big filter, or an export
must never delay a socket read, and a stalled UI must never lose capture. So the design
isolates capture from everything else, and keeps it simple.

**The GIL.** Decode is pure-Python CPU work, so a thread pool does not decode in
parallel; it only contends with the UI thread. Threads are used only where they
release the GIL (blocking serial reads, file writes). Parallelism, where it pays, is
processes with batched transfer. The biggest win is still not doing work: decode
lazily, only what is looked at.

Processes:
1. **Engine** (owns the hardware): one `asyncio` loop, UDP and serial both driven by the
   loop's selector (Stage 2 2k; a reader thread only for a device that cannot be polled).
   Per packet it does only cheap work:
   `time.time_ns()` timestamp, peek MID at a fixed header offset, append the raw frame
   to the recording buffer (flushed by size or every ~250 ms on a writer thread - the
   one path that must never drop), and append to an outbound batch. It keeps
   `latest[mid]` and per-MID counts/rates. No full decode.
2. **UI** (Textual, its own loop): never touches sockets on the hot path.
   - **Data plane = shared-memory ring** (`multiprocessing.shared_memory`): one writer
     (engine), any number of readers (UI, CLI consumers). Fixed-size slots (1536 B
     payload + header: sequence, `time_ns`, MID, length, direction, node), sized for
     >= 30 s at 10k/s (~470 MB; configurable, default smaller). The writer never
     blocks; a reader that falls behind detects overrun via sequence numbers and
     reports it as one coalesced error, then resyncs. A per-slot sequence written
     last (seqlock pattern) makes torn reads detectable without locks.
   - **Control plane = Unix domain socket**: send command, pause, stop, filter and
     status messages; low rate, so simplicity wins.
   - On each ~15-20 Hz tick the UI reads new slots and decodes **only visible rows**,
     cached by (sequence, MID). MID filters run on the slot's MID field; field filters
     decode only candidates. **Hold** = UI stops advancing its view (it keeps reading
     into its scroll-back, engine continues); **pause** = engine stops recording;
     **stop** = engine closes the session.
3. **Decode workers** (`ProcessPoolExecutor`) only for bulk jobs: replay with field
   filters, exports, search over large recordings. Jobs are chunked by file offset;
   workers read the recording themselves, so only offsets go in and compact results
   come out. At these rates most recordings decode fast enough in one process, so the
   pool is created lazily and only for jobs over a size threshold.

**How processes are launched.** Everything is started by ironbird itself as the
invoking user: no daemons installed, no services, no root, no setuid.
- Start method is `spawn` everywhere (`multiprocessing.get_context("spawn")`): `fork`
  is unsafe with threads (serial reader, writers) and copies the UI state for nothing.
- `ib tui` (default, attached): the TUI starts the engine as a child process,
  connected by a `socketpair` (control) and a named shared-memory ring (data); the
engine unlinks the segment on exit, and stale segments from a crash are cleaned up
on next start by pidfile check. If the TUI exits or crashes, the engine sees EOF,
  flushes the recording and ledger, and exits; if the engine dies, the TUI shows a
  one-line coded error with details and offers restart. Children ignore SIGINT
  (the parent coordinates shutdown) and the parent handles SIGHUP (SSH drop) as an
  orderly shutdown.
- `ib engine start` (detached, for long captures over flaky SSH): the engine
  double-forks into the background (`os.setsid`, stdio to the log), listens on
  `$XDG_RUNTIME_DIR/ironbird/<session>.sock` (mode 0600, falls back to `~/.cache`),
  creates its ring under `/dev/shm` (user-owned, 0600), and writes a pidfile. `ib tui --attach [session]` and CLI commands
  (`ib send`, `ib listen`) connect to it; `ib engine ls|stop` manage it. Closing the
  SSH session no longer ends the capture, much like tmux. Plain `tmux`/`screen`
  around `ib tui` also works and needs nothing from us.
- Headless CLI without a running engine (`ib send`, `ib listen` in Stage 1): the engine
  runs in-process in the same `asyncio` loop; no child at all. Stage 1 builds the
  engine with the same message API it will later expose over the socket, so Stages 3+
  add hosting, not redesign.
- Worker pool: created on demand by whichever process runs a bulk job, same `spawn`
  context, torn down when the job finishes.

Decode fast path: per-message layouts compiled once at CCDD load into a
`struct.Struct` plus a small tuple of bitfield/enum post-processors; `unpack_from` on a
`memoryview` (no slicing copies); scaling/enum lookup only for displayed fields.

**Ledger = lowest priority.** Producers call `ledger.record(...)`, which only appends a
tuple to a `collections.deque` (no formatting, no I/O). An idle-priority writer drains
it: in each process, an `asyncio` task that wakes when the loop has been idle (no
pending packet batches or redraws) and formats/writes in small slices, so it can never
steal a frame. Bounded staleness so it cannot fall behind forever: forced drain at
>= 1 s age or >= 10k entries, and synchronous flush on error, shutdown, and fatal signals
(so the trace leading to a crash is on disk).

Benchmarks are part of each stage (`pytest-benchmark`): decode packets/s per message
shape, engine ingest packets/s on loopback, ledger record cost. Regressions are caught
like failing tests.

## Stage 0 - publish existing work

Attach `memphis242/ironbird`, add it as `origin` of `/home/user/ironbird`, and push the
three existing config commits (branch per CLAUDE.md; nothing merged to `main` without
your go-ahead). Commit this plan as `docs/plan.md`.
Add a short "untrusted input" rule to CLAUDE.md pointing at the practices below, so
every agent applies them.

## Stage 1 - CLI send + receive (this plan's implementation scope)

Deliverables:
1. Project scaffold: `pyproject.toml` (setuptools, ruff, pyright strict-ish, pytest + xdist),
   `src/ironbird/`, `tests/`, `examples/` (sample `ironbird.toml` with ground + flight computer nodes, header profiles,
   sample CCDD JSON with a no-arg command, an arg command, and telemetry packets of several
   categories including an event).
2. `errors.py`, `ledger.py`, `config.py`, `header.py`, `ccdd/`, `codec.py`,
   `transport/udp.py`, `session.py`.
3. CLI commands:
   - `ib nodes` - list configured nodes, aliases, links, and which node we act as.
   - Global `--as <node>` to pick the role for this invocation.
   - `ib find <query>` - tiered lookup (exact name/MID, then substring, then
     description match) - the basic tiers; semantic search comes later.
   - `ib send <node> <command> [arg=value ...]` - encode, send, ledger it, print a
     one-line confirmation with hex on `--verbose`.
   - `ib listen <node> [--mid ...] [--format line|json]` - decode and print
     incoming packets (telemetry as ground; commands when acting as a flight node);
     unknown MIDs reported once (coalesced), not per packet.
   - `ib events [--node X ...] [--min-type ...] [--format line|json]` - listen on all
     telemetry links of all (or selected) nodes concurrently and print only events (see
     "Telemetry categories and events"); `ib listen` lines carry the category.
   - `ib respond --as <node>` - act as a flight-side node: decode received commands
     and answer with configured telemetry (e.g. a command-accepted HK packet).
4. No separate simulator: ironbird acting as the vehicle node over loopback UDP is the
   test target, so everything is testable without hardware.

Out of Stage 1: TUI, recording, serial, variable-length payloads, scheduling/repeat.

## Untrusted input and malformed data

Every byte that enters ironbird is untrusted: network and serial packets, recording
files, and imported CCDD/config/header files. Python removes memory-corruption bugs,
but parsers still fail through crashes, hangs, resource exhaustion, wrong decodes, and
injection into whatever renders the output.

Practices (enforced in review and by tests):
- **Validate before you use.** Check every length before every unpack: datagram size
  vs. header length field vs. CCDD message size; variable-length fields checked
  against the bytes remaining *and* the CCDD maximum; reject above a global cap (MTU
  plus margin, configurable). No length taken from the wire ever sizes an allocation or
  a loop unchecked.
- **One decode boundary.** All parsing goes through `codec.decode()` and
  `header.unpack()`, which return a result or a typed `DecodeError` (code, offset,
  reason). Nothing else catches `struct.error`/`IndexError`, and no malformed packet
  can stop the engine: it is counted per MID/reason, kept raw in the recording,
  flagged in views, and reported as one coalesced line.
- **Preserve the evidence.** Malformed packets are recorded raw and shown as hex with
  the failing offset highlighted; they are never silently dropped or "repaired".
- **No code from data.** No `pickle`, `eval`, `exec`, or YAML object loading on
  anything read from files, the wire, or shared memory. The ring and worker pool move
  raw bytes and offsets only. The header-profile `python:` escape hatch is trusted
  local configuration and is documented as such.
- **Bounded resources.** Caps on per-MID caches, scroll-back, unknown-MID tables, and
  ledger queue; a flood of unique garbage MIDs cannot exhaust memory.
- **Output injection.** Strings from packets are sanitized before display: control
  characters and terminal escape sequences stripped/escaped, and Textual/Rich markup
  escaped, so telemetry cannot drive the user's terminal. CSV/xlsx exports neutralize
  formula-leading cells (`=`, `+`, `-`, `@`) in string fields.
- **Strict file parsing.** Config, CCDD, and header files are schema-validated with
  every error reported with file:line and a code; recordings carry a magic, version,
  and per-frame length, and a truncated or corrupt file plays back up to the damage
  and reports where it stopped.

Tests:
- Property-based tests (Hypothesis) on `header.unpack`, `codec.decode`, the CCDD
  loader, and the recording reader: arbitrary bytes, truncation at every offset,
  oversize, bit flips, length fields that lie in both directions, invalid enum values,
  variable-length counts beyond the buffer. Invariants: never an uncaught exception,
  never a hang, decode/encode round-trips for valid input.
- A checked-in corpus `tests/corpus/malformed/` of minimized bad inputs; every bug
  found adds one (the E2E regression rule).
- Coverage-guided fuzzing (`atheris`) of the decode boundary as a scheduled CI job.
- E2E: the mock flight node is sent malformed commands, and the ground side is sent
  malformed telemetry, at rate, asserting the engine keeps capturing and the errors
  coalesce.
- Terminal-injection test: a telemetry string with escape sequences and markup
  renders inert.

## Verification against real cFS (every stage)

A mock (ironbird acting as the vehicle node) serves quick development checks; the
acceptance gate for each stage is a real cFS instance.
- Build public NASA cFS (`nasa/cFS` bundle, native `pc-linux` / simulation target)
  out-of-tree (`../build/cfs` or scratch), as the normal user. It runs without root
  (OSAL permissive mode). Script it as `tools/cfs/build.sh` + `run.sh` and add a CI job
  that builds (cached) and runs it.
- Messaging path: commands over UDP to `ci_lab` (default port 1234); telemetry from
  `to_lab` (port 2234) after sending its enable-output command with our host address.
- CCDD for the test set: hand-authored placeholder CCDD JSON from the cFS headers for
  `sample_app`, `ci_lab`, `to_lab`, cFE `TIME` and cFE `ES`. Command/function codes and
  layouts are taken from the checked-out source, not memory.
- Header profiles: `ccsds_v1_cfs_cmd.toml` must reproduce cFE's command secondary
  header (function code + checksum) and its checksum algorithm exactly. The native
  build does not validate command checksums (`tools/cfs/NOTES.md`), so a successful
  command does not prove the checksum; unit tests against the algorithm cover it.
- Stage 1 acceptance cases:
  - `TO_LAB` enable output -> telemetry starts flowing; `ib listen` decodes cFE ES and
    TIME housekeeping fields to sane values.
  - `SAMPLE_APP` NOOP and reset counters -> command counters in its HK telemetry
    change as expected; an arg-bearing command (e.g. TIME set/add clock adjustment,
    or a `sample_app` command with a payload) is accepted and reflected in TIME HK/
    diag telemetry.
  - cFE TIME NOOP and HK/diag decode (clock state, MET, STCF).
  - Events: `SAMPLE_APP` NOOP -> `ib events` shows the `SAMPLE_APP` information event
    (id 3) with the right app, id, type and text and the ledger holds it in full;
    `--min-type error` shows a TIME invalid-STCF error event and hides the information
    event; events also appear in the `ib listen` stream with their category; EVS, SB and
    TBL housekeeping decode.
  - cFE ES write-system-log-to-file command (the on-demand syslog dump; exact
    command name/code confirmed from source): command accepted, the file appears in
    the cFS filesystem path, ES HK counters reflect it.
  - Deliberately malformed commands, ledgered with the exact bytes sent: a header length
    shorter than the command the app expects -> that app's (ES/TIME/SAMPLE_APP) command
    error counter increments; a header length larger than the datagram -> CI_LAB
    `IngestErrors` increments; a bad checksum is accepted by this build (pinned as a
    characterization test).
  - Automated as `tests/cfs/test_acceptance.py` (marker `cfs`, opt-in: `IRONBIRD_CFS=1`
    or `-m cfs`), driving the `ib` CLI against a cFS started by `tools/cfs/run.sh`.

## Stage 2 - recording, replay, filters, serial, variable-length payloads

Stage 2 makes captures durable and reviewable from the CLI: every frame that crosses a
link is recorded raw, replayed and exported through one filter language, over UDP or
serial, with variable-length payloads decoded. It also pays down the Stage 1 carry-over
list (outline item 2 below), mostly as the foundation the features need.

Out of Stage 2: TUI and its shared-memory ring (Stage 3; the frame encoding below is
designed so the ring can carry it), detached engine and control socket (Stage 3),
replay-to-wire (Stage 5 send modes), xlsx (Stage 6), arrays of structs in the CCDD.

**Delivery rule.** Each sub-step is one short-lived branch and PR off `main`, merged on its
own, with Stage 1 behaviour intact: the full Stage 1 suite and the real-cFS acceptance
suite stay green at every merge. Each sub-step lands its own error codes, corpus entries,
tests, README section and benchmark. Where the design below fixes a format or an
interface, later sub-steps build on it unchanged.

### Sub-step map

| Step | Title | Depends on | User-visible result |
|------|-------|-----------|---------------------|
| 2a | Structured notices, immutable shared data | - | `--verbose` notices point at their ledger record |
| 2b | Frame-yielding engine, transport factory | 2a | none (refactor); malformed frames reach consumers |
| 2c | Byte-bounded memory, receive errors | 2b | oversize floods cannot exhaust memory; `IB-TX-003`, `IB-ENG-001` |
| 2d | Ledger under floods: sampling, caps, rotation, retention | 2c | bounded ledger disk use; `[ledger]` config |
| 2e | Recording format and writer | 2b | `ib record`, `--record`, `ib rec info/check` |
| 2f | Filter language, live `--where` | 2b | `ib listen/events --where EXPR` |
| 2g | Replay | 2e, 2f | `ib replay`, `--follow`, `ib rec repair` |
| 2h | Export CSV / text | 2g | `ib export` |
| 2i | Header profile flexibility | 2b | per-field endianness, trailer CRC, msgid_v2, epochs, `--header` |
| 2j | Variable-length payloads | 2i (codec overlap), 2h | counted and tail arrays in CCDD, encode and decode |
| 2k | Serial transport | 2b, 2c, 2i | `transport = "serial"` links with resync and reconnect |
| 2l | Stage 2 acceptance, throughput gate, docs | all | end-to-end cFS scenario, 10k pkt/s gate |

Parallel lanes once 2b is merged: {2c -> 2d}, {2e}, {2f}, {2i}; then 2g, 2h, 2j, 2k.

### S1. Frame and stream (fixed in 2b, used everywhere)

The unit the engine produces, the recorder writes, replay reads, filters test and (in
Stage 3) the ring carries. Built only by the engine's ingest path and the recording
reader, after validation, so its invariants are asserted, not checked.

```python
class Direction(StrEnum):
    RX = "rx"
    TX = "tx"

class FrameFlag(IntFlag):
    NONE = 0
    HAS_MID = 0x01    # mid was peeked from the profile's MID window
    TRUNCATED = 0x02  # data cut at max_packet_bytes; orig_len is the size on the wire
    GARBAGE = 0x04    # serial: bytes skipped while resynchronising, not a packet
    STALE = 0x08      # serial: partial packet flushed by the inter-byte timeout

@dataclass(frozen=True, slots=True)
class Stream:          # one direction of one link, as seen by this process
    id: int            # u16, unique within a session and its recording
    link: str
    node: str          # node whose CCDD and profiles decode it (link.destination)
    kind: MessageKind  # what the bytes are: command or telemetry
    direction: Direction
    transport: str     # "udp" | "serial"
    endpoint: str      # sanitized display text: "udp 0.0.0.0:2234", "serial /dev/ttyUSB0"

@dataclass(frozen=True, slots=True)
class Frame:
    seq: int           # u64, session-wide, strictly increasing from 1, assigned at ingest
    time_ns: int       # CLOCK_REALTIME taken in the transport callback (receive or send)
    stream: Stream
    source: str        # interned peer text ("" for serial)
    mid: int | None    # peeked, used only for routing and filtering, never trusted
    flags: FrameFlag
    orig_len: int      # == len(data) unless TRUNCATED
    data: bytes        # len(data) <= max_packet_bytes
    # __post_init__ asserts: HAS_MID iff mid is not None; TRUNCATED iff len(data) < orig_len
```

- `seq` gives total order across links and makes loss visible: within a recording, seq
  is contiguous except where a GAP record says why.
- MIDs are limited to 32 bits: the codec rejects a profile whose `mid_bits` exceeds 32
  at load (`IB-HDR-002`), matching the CCDD's existing 32-bit MID cap.
- Decoded results pair with their frame: `Decoded(frame, packet)` replaces Stage 1's
  `Received`, so live and replay share one render and filter path.

### S2. Engine data flow (2b, bounded in 2c)

```
transport callback (loop thread)        engine ingest (cheap, per frame)              consumers
recvmsg_into / os.read + framer  --->   seq, Frame, peek MID, Codec.check()  --->  sinks: Recorder, stats
                                         malformed -> report (coalesced)     \-->  per-consumer queue (bytes-bounded)
                                                                                    -> filter -> decode -> render
```

- **Push ingest.** Transports call a receiver callback from the loop's reader callback;
  the engine builds the frame and hands it to *sinks* synchronously (recorder, stats)
  before any consumer queue. A slow consumer (a stalled stdout) can then never cost a
  recorded frame: its own bounded queue overflows instead, counted as `IB-ENG-001`
  ("N frames not shown; still recorded").
- **Validation at ingest, decode in the consumer.** `Codec.check(kind, data)` is the
  cheap part of the decode boundary: size bounds, MID known, header length field, fixed
  fields (one precompiled mask compare over the header word), payload size range, and
  the checksum if the profile has one. It reports malformed frames at ingest whatever
  the consumer filters, keeping Stage 1's semantics. `Codec.decode` calls `check` first,
  then unpacks fields; the consumer's decoder reports only what `check` did not
  (variable-length counts), so nothing is reported twice. Replay has no ingest and
  reports everything at decode.
- **Interfaces.**

```python
class Receiver(Protocol):
    def __call__(self, data: bytes, time_ns: int, source: str,
                 orig_len: int, flags: FrameFlag) -> None: ...

class Transport(Protocol):
    @property
    def description(self) -> str: ...
    async def open(self, receiver: Receiver | None) -> None: ...  # None: send-only
    async def send(self, data: bytes, *, confirm: bool = False) -> None: ...
    def stats(self) -> TransportStats: ...       # frozen snapshot
    async def close(self) -> None: ...

TRANSPORTS: Final[Mapping[str, TransportFactory]]  # MappingProxyType({"udp": ..., "serial": ...})
def open_transport(link: Link, end: LinkEnd, framing: Framing | None) -> Transport

class FrameSink(Protocol):
    def append(self, frame: Frame) -> None: ...  # synchronous, cheap, never raises

class Session:
    async def capture(self, routes: Sequence[Route], *, sinks: Sequence[FrameSink] = (),
                      queue_bytes: int = CONSUMER_QUEUE_BYTES) -> Capture: ...
    def decoder(self, *, report_checked: bool) -> FrameDecoder: ...

class Capture:   # async iterator of every frame: valid, malformed, unknown, garbage
    streams: tuple[Stream, ...]
    def __aiter__(self) -> AsyncIterator[Frame]: ...
    def stats(self) -> CaptureStats: ...
    async def close(self) -> None: ...
```

`transport/__init__.py` asserts at import that the keys of `TRANSPORTS` equal
`config.SUPPORTED_TRANSPORTS` (config cannot import the transports without a cycle), so
config validation and the factory cannot disagree. A transport that is open for a link is owned by the
session and shared by its sends and receives (a serial device can be opened once).

### S3. Recording file format `.ibrec` v1 (fixed in 2e)

All integers little-endian, explicit (`<` structs), independent of host. The file is a
16-byte preamble followed by records; everything after the preamble is CRC-checked.

**Preamble (16 bytes):** magic `89 49 42 52 0D 0A 1A 0A` (`\x89IBR\r\n\x1a\n`: high bit,
CR-LF and ^Z catch text-mode and 7-bit transfer damage), `u16 major = 1`, `u16 minor = 0`,
`u32 flags = 0`.

**Record envelope (12 bytes)**, record start 8-byte aligned, zero padding after the body:

| Offset | Type | Field |
|--------|------|-------|
| 0 | u32 | `body_len` (bytes after the envelope, padding excluded) |
| 4 | u8 | `type` |
| 5 | u8 | `flags` (0 in v1) |
| 6 | u16 | reserved (0) |
| 8 | u32 | `crc32` (`zlib.crc32`) over envelope bytes 0-7 and the body |

**Record types.** Types below 0x80 are *critical* (a reader that does not know one stops
with `IB-REC-005`); 0x80 and above are *skippable* extensions, so a minor version can add
them without breaking old readers.

| Type | Name | Body | Max body |
|------|------|------|----------|
| 0x01 | META | JSON (`ironbird-rec-meta/1`): created UTC, session id, ironbird version, host, user, pid, sanitized command line, acts-as node, config path + SHA-256, `max_packet_bytes`, per-node adapter and definition hashes, part number | 1 MiB |
| 0x02 | BLOB | embedded definition: `u8 kind` (1 CCDD, 2 command profile, 3 telemetry profile), node name, adapter name, SHA-256, content | 8 MiB + 1 KiB |
| 0x03 | STREAM | stream definition (S1 fields), before the stream's first frame | 4 KiB |
| 0x04 | SOURCE | `u16 id` + peer text, before first use; table capped at 4096, overflow uses id 0xFFFF "(other)" | 256 B |
| 0x05 | FRAME | frame (below) | 32 + `max_packet_bytes` |
| 0x06 | GAP | stream, reason (kernel drop, queue drop, recorder overrun, link down), count, first/last time, seq range | 64 B |
| 0x07 | CHUNK | chunk summary (below) | 2.2 KiB |
| 0x08 | MARK | session events (start, stop, signal, link up/down) and, later, user notes | 4 KiB |
| 0x09 | INDEX | `u32 n` + n x `u64` offsets of CHUNK records | 16 MiB |
| 0x0A | TRAILER | `u64 index_offset`, `u64 frame_count`, `u64 last_seq`; always the last record | 24 B |

**FRAME body (32-byte fixed part + data):**

| Offset | Type | Field |
|--------|------|-------|
| 0 | u64 | `seq` |
| 8 | u64 | `time_ns` |
| 16 | u16 | `stream` id |
| 18 | u16 | `source` id |
| 20 | u16 | `flags` (FrameFlag; unknown bits are corruption) |
| 22 | u16 | reserved (0) |
| 24 | u32 | `mid` (0 unless HAS_MID) |
| 28 | u32 | `orig_len` |
| 32 | bytes | `data`, `body_len - 32` bytes |

**CHUNK summary.** The writer closes a chunk every 1 MiB or 1 s, whichever comes first:
`u64 chunk_start` (offset of the chunk's first record), first/last seq, first/last
`time_ns`, `u32 frames`, `u32 bytes`, `u32 frames_without_mid`, `u16 flags` (bit 0:
MID table overflowed), `u16 n`, then n x (`u32 mid`, `u32 count`) sorted by MID, n <= 256.
Summaries let replay and export skip chunks by time range or MID set, give `ib rec info`
per-MID counts without reading frames, and are the work units for a decode worker pool.

**Writing and crash safety.**
- Create with `O_CREAT | O_EXCL`, mode 0600, directory 0700; never overwrite. Order on
  open: preamble, META, BLOBs (definitions, see decision D1), STREAMs, then `fsync` the
  file and its directory so the file survives a crash from the first frame on.
- `Recorder.append(frame)` (a sink) encodes into a `bytearray` with precompiled structs;
  at 256 KiB or every 250 ms the buffer goes to a writer thread (`os.write`, releases the
  GIL, handles partial writes). `fdatasync` once per chunk by default
  (`recording.fsync = "chunk" | "always" | "never"`): a power loss loses at most about
  1 s, a crash at most the unflushed 250 ms, a clean stop or signal nothing.
- Back-pressure is bounded: in-memory plus queued bytes are capped
  (`recording.max_buffer_bytes`, default 256 MiB, about 2 minutes at the 10k pkt/s
  target). Above it, frames are dropped and counted, and a GAP record (reason
  `recorder_overrun`, seq range) is written once there is room; `IB-REC-003` is an
  error-level notice, never coalesced away into silence.
- Transport drops (kernel, queue) never received a seq; the engine turns their counter
  deltas into GAP records on its 1 Hz tick, so a replay shows every loss and where.
- Close: final CHUNK, INDEX, TRAILER, `fdatasync`, close. On SIGINT/SIGTERM/SIGHUP the
  close runs in the cancellation path with a 5 s bound; a second signal abandons it,
  leaving a readable file without a trailer.
- Write failure (ENOSPC, EIO): `IB-REC-002`, recorder stops, capture goes on;
  `ib record` exits 1 at once, other commands with `--record` finish and exit 1. Free
  space: refuse to start below `recording.min_free_bytes` (default 1 GiB), check every
  10 s, warn (`IB-REC-010`) when the projected time to full drops under 10 minutes.
- Optional split (`--split 1GiB` / `--split 1h`): each part is a complete file
  (`<name>.p0002.ibrec`), its META naming the session, the part number and the previous
  part's last seq.

**Reading (untrusted input).**
- Plain buffered `os.pread` of 1 MiB blocks; no `mmap` (a file truncated or rewritten
  under the reader, e.g. by `--follow` or NFS, would raise SIGBUS).
- Per record, in order: envelope present (else torn), `body_len` within that type's
  maximum and minimum, body present (else torn), CRC (else damaged), type known or
  skippable, then type rules: FRAME stream and source ids already defined, data length
  <= META `max_packet_bytes` <= 65535, `orig_len >= len(data)`, only known flag bits,
  seq strictly increasing; BLOB SHA-256 matches its content; INDEX offsets increasing
  and inside the file. Every yielded frame passed its CRC.
- **Torn vs. damaged.** Damage reaching end of file with no valid record after it is a
  *torn tail* (the normal result of a crash): warning `IB-REC-007`, playback complete up
  to it. Damage followed by a valid record (found by scanning 8-byte aligned offsets) is
  *mid-file damage*: `IB-REC-006` with the offset and span; playback stops there unless
  `--skip-damage` resumes at the next valid record (lost frames show as a seq gap).
- **Index is a hint, never trusted.** A valid TRAILER leads to the INDEX; every CHUNK it
  points at is checked (envelope, CRC, type) when used. A missing or bad index falls
  back to a forward scan (`IB-REC-008`, info level), which `ib rec repair` makes
  permanent by copying the valid prefix plus a fresh index into a new file. The original
  is never modified (evidence).
- Embedded definitions load through the same strict loaders, from bytes, labelled
  `FILE#ccdd/<node>` in errors; header profiles from a recording are untrusted
  configuration and refuse the `python:` escape hatch.
- Strings from META, STREAM and SOURCE records are sanitized before display.

### S4. Filter language (fixed in 2f, used by live, replay, export)

A small expression language compiled to a closure tree. No `eval`, no regular
expressions; parsing and evaluation are linear in the input.

```
expr      = term { "or" term }
term      = factor { "and" factor }
factor    = "not" factor | "(" expr ")" | predicate
predicate = name cmp literal
          | name "in" "[" literal { "," literal } "]"
          | name "contains" STRING
cmp       = "==" | "!=" | "<" | "<=" | ">" | ">="
name      = IDENT { "." IDENT }
literal   = INT | HEX | FLOAT | STRING | IDENT | TIME
```

`TIME` is ISO 8601 UTC with `Z` or an offset (`2026-10-29T12:00:00.5Z`); naive local
times are rejected. `IDENT` literals are resolved by the left-hand name: message names,
category names, enum labels, node and link names, severities.

| Level | Names | Cost |
|-------|-------|------|
| frame | `seq`, `time`, `len`, `node`, `link`, `dir` (rx, tx), `kind` (command, telemetry), `source`, `mid`, `truncated`, `garbage` | none |
| model | `category`, `msg` (telemetry, by MID) | dict lookup |
| check | `status` (ok, malformed, unknown) | `Codec.check` |
| decode | `msg` (commands), `fc`, `hdr.<field>`, `htime` (header time, 2i), `<MSG>.<field>`, bare `<field>` (any message that has it), `event.app`, `event.id`, `event.type` (DEBUG..CRITICAL, ordered), `event.text` | full decode |

- **Semantics.** A predicate on something a frame does not have (a field of another
  message, a decode-level name on a malformed frame) is false; `not` inverts that, so
  `not X.f == 1` matches other messages. Reserved names win over bare field names; a
  field named `mid` must be qualified. Precedence: `not` > `and` > `or`.
- **Compilation** against the CCDD models in use (recording or config):
  `compile_filter(text, models) -> Filter(source, mids, needs_decode, match)`. `mids` is
  the candidate MID set derived from the tree (`and` intersects, `or` unites, `not` makes
  it unknown), applied to `frame.mid` before anything is decoded; `needs_decode` decides
  whether non-candidates are decoded at all. Names, types and literals are checked at
  compile time (`IB-FLT-002/003`), so a typo fails before any packet is read.
- **Limits** (`IB-FLT-004`): 4096 characters, 1024 tokens, depth 32, 1024 `in` items,
  numeric literals of at most 40 characters. Non-ASCII is allowed only inside strings.
- **Errors** are one line with the column and a short excerpt; the details carry the
  expression with a caret line and up to three close-match suggestions (`difflib`).
  `IB-FLT-*` codes exit 2 like other usage errors.
- `--mid` stays as sugar for `mid in [...]`; `--where` combines with it by `and`.

### S5. Notice (fixed in 2a)

```python
class Level(IntEnum):
    INFO = 1
    WARNING = 2
    ERROR = 3

@dataclass(frozen=True, slots=True)
class Notice:
    code: Code
    level: Level
    key: str               # coalescing key: code + MID + stream/context
    summary: str           # sanitized, one line, no count suffix
    count: int             # occurrences this notice stands for (1 = first)
    window_s: float        # span the count covers (0.0 for the first)
    ledger_n: int | None   # ledger record number of the latest occurrence
    def line(self) -> str: ...  # "error IB-DEC-005: ... (x37 in 10 s)"
```

The coalescer emits `Notice` values instead of strings; front-ends render them.
`Ledger.record` returns the record number so a notice can point at its full record
(`--verbose` appends `[ledger n=1234]`; the TUI details pane opens that record).

### S6. Ring compatibility (decided now, built in Stage 3)

The Stage 3 ring carries the S3 envelope plus FRAME body unchanged, with STREAM and
SOURCE records travelling the same way, so ring readers, recording readers and worker
pools share one decoder (`frame_codec.py`). Frames are self-delimiting, position
independent, CRC-checked and at most `12 + 32 + max_packet_bytes` bytes, so they fit
either a fixed-slot ring (slot = that maximum) or a variable-length byte ring. Which
shape is Stage 3's call (decision D9); Stage 2 only guarantees the encoding.

### 2a - Structured notices, immutable shared data

Deliverables
- `notice.py` (S5); `Coalescer` emits `Notice`; CLI renders `Notice.line()`, output text
  unchanged from Stage 1 at default verbosity.
- `Ledger.record` returns its record number.
- Immutability: `Header.values`, `checksum.REGISTRY`, codec profile and layout tables
  as `MappingProxyType`; `Session.stats` becomes private with
  `stats_snapshot() -> Mapping[int, MidStatSnapshot]` (frozen); public collections
  returned as tuples (`find()`); `Session.config`, `node`, `ledger` read-only properties.

Verification: unit tests that notices coalesce exactly as Stage 1 lines did (counts,
windows, group fold-in); a light test that shared tables reject mutation; Stage 1 E2E
and cFS acceptance unchanged.

### 2b - Frame-yielding engine and transport factory

Deliverables
- `frame.py` (S1), `engine.py` (ingest, `Capture`, sinks, consumer queues) split out of
  `session.py`; `Listener`, `EventStream` and `Responder` rebuilt as capture consumers
  that decode. Malformed and unknown-MID frames are yielded to consumers (and to sinks).
- `Transport` protocol with `open(receiver)`, `send(data, confirm=)`, `stats()` (S2);
  `TRANSPORTS` registry and `open_transport()`; UDP ported to push ingest.
- `Codec.check()` as the first half of `Codec.decode()`; the header gets a precompiled
  fixed-field mask/value pair so the check is one integer compare.
- Link config takes a transport-specific shape: UDP keeps `command_endpoint` /
  `telemetry_endpoint` at the link level (Stage 1 configs load unchanged); 2k adds a
  `serial = {...}` table. `Link` holds `udp: UdpLinkSpec | None` and
  `serial: SerialLinkSpec | None`, exactly one set (asserted after validation).
- TX frames: `send_command` and responder replies pass through ingest as `Direction.TX`
  frames, so a process that records also records what it sent.

Decisions
- Push ingest over a per-transport queue and pump: one fewer hop, and the recorder is
  ahead of every consumer by construction.
- Validation at ingest is header and size only; field unpacking stays in consumers.

Verification: Stage 1 suite unchanged in behaviour; new unit tests of ingest ordering
(sinks see every frame even when a consumer queue is full), seq monotonicity across two
links, malformed frames yielded with flags; ingest benchmark (frames/s on loopback) as a
baseline for the 10k gate; cFS acceptance unchanged.

### 2c - Byte-bounded memory and receive-side errors

Deliverables
- UDP receive into a `max_packet_bytes` buffer with `MSG_TRUNC`, so the kernel reports
  the true size: over-cap datagrams become TRUNCATED frames with `orig_len`, decoded as
  `IB-DEC-002` (oversize) and recorded truncated.
- Consumer queues bounded by bytes (default 32 MiB) and count; overflow drops the
  newest frame for that consumer only, reported as `IB-ENG-001` (coalesced).
- Ledger: bytes-like fields truncated when the record is *created* (`raw` plus
  `raw_len`), and the queue bounded by bytes (64 MiB) as well as by count.
- `IB-TX-003`: listener socket errors in the read callback (anything but EAGAIN/EINTR)
  are counted, ledgered and reported coalesced; a persistent error (EBADF) ends the
  capture with that error.

Untrusted input: a flood of 64 KiB datagrams with a stalled consumer must keep process
memory bounded. Verification: E2E flood test with a paused stdout asserting a bounded RSS
(measured from `/proc/self/status`) and exact drop accounting (`sent == recorded ==
shown + not_shown`); unit test of the `MSG_TRUNC` path on loopback; corpus entry for an
oversize datagram.

### 2d - Ledger under hostile or garbage floods

Deliverables
- **Evidence sampling** per key (code, MID, stream) and 10 s window: every occurrence
  counted; the first 8 kept in full with raw bytes, then one sample per second; the rest
  summarised by one `rx.malformed.summary` record at window close (count, bytes,
  first/last time, first/last seq). A global raw-evidence budget (default 256 KiB/s,
  token bucket) drops raw bytes, not records, beyond it. Key table bounded (1024, LRU;
  overflow folds into one "other" key). Malformed records carry the frame `seq`, so with
  a recording running the full evidence is one lookup away.
- Events stay unsampled (decision D3); actions, commands, state changes and errors are
  never sampled.
- **Two-tier size cap** per session: evidence tier (packet records, malformed samples)
  stops at `max_evidence_bytes` (`IB-LED-003`, warning, once); the essential tier
  continues to `max_session_bytes`. At the hard cap the ledger stops and, by default,
  commands are refused (`IB-LED-004`) because a command must not go out without a trace;
  capture and recording continue (decision D2 for the default).
- **Rotation**: at `max_file_bytes` the ledger continues in `<stamp>-<session>.pNNNN.jsonl`,
  each part opening with a `ledger.part` record naming the previous one.
- **Retention** at session start, in a worker thread: delete ledger files (our name
  pattern only, regular files only via `lstat`, never symlinks) beyond `retention_days`
  or `retention_total_bytes`, oldest first, skipping live sessions (each live ledger
  holds `flock(LOCK_SH)`; the pruner needs `LOCK_EX | LOCK_NB`). Deletions are
  ledgered; failures are `IB-LED-005` (warning).
- Config `[ledger]`: `max_session_bytes` (512 MiB), `max_evidence_bytes` (256 MiB),
  `max_file_bytes` (64 MiB), `evidence_rate` (256 KiB/s), `retention_days` (30),
  `retention_total_bytes` (2 GiB), `on_full = "refuse_commands" | "continue"`. Sizes are
  integers or strings matching `^[0-9]{1,12}(B|KiB|MiB|GiB|TiB)$`; anything else is
  `IB-CFG-003`.

Verification: unit tests of the sampler with an injected clock (counts exact, samples
where expected, summary per window); E2E garbage flood at 10k pkt/s for 30 s of unique
MIDs: ledger growth stays under the evidence budget, notices coalesce, every datagram is
counted; cap test (tiny caps) shows the tier switch and the command refusal; retention
test with a fake directory including a symlink, a foreign file and a live session.

### 2e - Recording format and writer

Deliverables
- `recording/format.py` (S3 structs and limits; module-level asserts on struct sizes),
  `recording/writer.py` (`Recorder` sink plus writer thread), `recording/reader.py`.
- `ib record [NODE ...] [--link L] [-o PATH] [--duration S] [--count N] [--split SIZE|TIME]
  [--quiet]`: capture-only on all (or the selected) links of the acting node, no decode
  beyond `check`; a status line on stderr every 5 s (frames, bytes, rate, drops).
- `--record PATH` on `listen`, `events` and `respond`.
- `ib rec info FILE [--format line|json]` (META, streams, duration, per-MID counts from
  chunk summaries, gaps, clean or torn) and `ib rec check FILE` (full CRC and rule pass,
  exit 1 on damage).
- Default location: `[recording] dir`, else `$IRONBIRD_RECORD_DIR`, else
  `$XDG_DATA_HOME/ironbird/recordings`, else `~/.local/share/ironbird/recordings`; name
  `<UTC stamp>-<session>.ibrec`; the path is printed and ledgered at start.
- Config `[recording]`: `dir`, `fsync`, `max_buffer_bytes`, `min_free_bytes`,
  `embed_definitions` (default true, D1).

Untrusted input: the reader is a decode boundary of its own and gets the full treatment:
Hypothesis round trip (arbitrary frame sequences written then read are equal),
truncation at every offset (yields a prefix, reports torn tail at a record boundary,
never raises), bit flips (never yields a frame that was not written), lying `body_len`
both ways, undefined stream ids, bad BLOB hashes, oversize META. Corpus
`tests/corpus/malformed/rec-*`; an `atheris` target for the reader.

Verification
- Unit: struct layouts, chunk closing by size and time, GAP on overrun (slow writer
  injected), fsync policy calls (mocked), signal-path close.
- Crash test: `kill -9` the recorder mid-capture; `ib rec info` reports a torn tail and
  every frame flushed before the kill is readable.
- Benchmark: writer throughput (MB/s, frames/s) and per-frame `append` cost.
- Real cFS: record 60 s after TO_LAB enable; `rec info` shows each HK MID at its
  SCH_LAB period (`tools/cfs/NOTES.md`: ES 4.0 s, EVS 4.2 s, ...) within one packet, seq
  contiguous, no gaps, clean close.

### 2f - Filter language and live `--where`

Deliverables: `filter/` (lexer, recursive-descent parser with a depth counter, compiler,
MID-set analysis) per S4; `--where` on `listen` and `events`; non-candidate frames are
never decoded.

Untrusted input: filter text is user input, CCDD names it resolves against are untrusted
files; both are bounded. Hypothesis: arbitrary text raises only `IB-FLT-*` within a time
bound; compiled filters agree with a naive reference evaluator on random frames and
expressions; MID-set analysis never excludes a frame the full evaluation would match.
Corpus `flt-*` (deep nesting, huge `in` list, unterminated string, control characters).
`atheris` target for the parser.

Verification: benchmark of filter evaluation (frame-level and decode-level, frames/s);
real cFS: `ib listen fc --where 'msg == SAMPLE_APP_HK and CommandCounter > 0'` after a
NOOP prints only that HK, and `ib events --where 'event.type >= error'` matches
`--min-type error`.

### 2g - Replay

Deliverables
- `ib replay FILE [--where EXPR] [--from T] [--to T] [--speed X] [--follow]
  [--format line|json|hex] [--defs recording|config] [--skip-damage] [--count N]`.
  `T` is ISO 8601 UTC or `+offset` from the first frame (`+90s`, `+1m30s`). `--speed 0`
  (default) runs as fast as possible; `1` paces by recorded `time_ns` deltas (negative
  deltas from clock steps clamp to zero).
- Chunk selection by time range and candidate MID set from the index or chunk
  summaries; frames decoded with the recording's embedded definitions by default
  (`IB-REC-009` info when they differ from the current config, error when an adapter is
  unknown).
- `--follow` tails a recording being written: at end of file it polls (100 ms) and
  treats a partial last record as pending, not torn; it ends at the TRAILER or a signal.
- `ib rec repair IN OUT`: valid prefix plus a fresh index and trailer into a new file.
- End-of-run summary on stderr: frames read, matched, malformed, gaps, damage.

Verification: E2E mock round trip (`ib respond` + `ib record`, then replay output equals
what `ib listen` printed live, by record not by text); damaged-file E2E (torn,
mid-file, bad index) with expected codes and exit codes; benchmark of replay frames/s
with and without field filters; real cFS: replay the 2e recording with
`--where 'event.app == "SAMPLE_APP" and event.id == 3'` yields exactly the K NOOP events
sent during the capture.

### 2h - Export CSV and text

Deliverables
- `ib export FILE --format csv|text|jsonl [-o PATH] [--message NAME ...] [--where EXPR]
  [--from T] [--to T] [--header-fields] [--raw-enums] [--force]`.
- CSV is per message (columns differ by message): with one `--message` a single file,
  otherwise a directory with `<message>.csv` per message plus `malformed.csv` (seq, time,
  stream, MID, code, offset, reason, hex), so malformed evidence is exported too.
  Columns: `rx_time_utc` (ISO 8601, microseconds), `rx_time_ns`, `seq`, `node`, `link`,
  `mid`, optional header fields (and `pkt_time_utc` once 2i adds epochs), then payload
  fields in CCDD order. Enum fields export labels (`--raw-enums` for numbers); floats
  use shortest round-trip `repr`; arrays (2j) are one cell `[1 2 3]`.
- `text` is the `listen` line format; `jsonl` is the `listen --format json` record.
- Output is written to `<path>.tmp` and renamed on success; existing output is refused
  without `--force` (`IB-EXP-002`); write errors are `IB-EXP-001` and remove the
  temporary file.
- Single process, streaming, constant memory. The decode worker pool stays unbuilt
  until a 2l benchmark shows a one-hour recording exports in over 60 s.

Untrusted input: string cells sanitized (control characters escaped) and formula-leading
cells (`=`, `+`, `-`, `@`, tab, CR) prefixed with `'`, string fields only (numbers are
typed); `csv` module quoting (RFC 4180). Test: a telemetry string starting with `=` and
holding a newline and an escape sequence exports inert.

Verification: unit tests per column type; real cFS: export the 2e recording's
`SAMPLE_APP_HK`: `CommandCounter` rises by exactly K across the NOOPs and returns to 0
after RESET_COUNTERS, and the row count equals the MID's count in `ib rec info`.

### 2i - Header profile flexibility

Deliverables (each a profile key, validated with `IB-HDR-002` and file context)
- Per-field `endian = "little"` on byte-aligned, whole-byte fields (little-endian
  bitfields are ambiguous and rejected).
- Checksum placement and span: a `[trailer]` section with the same field syntax (a CRC
  after the payload, common on serial links), and `span = "packet" | "header" |
  "payload" | { start = N, end = -M }` (negative from the packet end) for header or
  trailer checksums; the checksum field is zeroed if inside its span. Payload size
  becomes total minus header minus trailer everywhere (codec fit checks, length field).
  `crc16_ccitt` switches to `binascii.crc_hqx(data, 0xFFFF)` (same result, C speed),
  pinned by the existing check-value assert.
- Non-contiguous MID fields (cFE msgid_v2 style): `mid_fields` may list any fields in
  any position; the MID is their concatenation in list order. `peek_mid` uses a
  precompiled tuple of (byte offset, count, shift, mask, output shift) windows.
- Header values per message (CCDD `"header": {"field": N}`) and per send
  (`ib send ... --header field=N`); precedence send > message > profile default; names
  must be input fields of that profile and values must fit.
- Configurable time epoch: `[time] epoch = "1980-01-06T00:00:00Z"`, `leap_seconds = N`;
  `Header.time_ns` (UTC) on decode for filters (`htime`) and exports (`pkt_time_utc`).
- `HeaderProfile.length_window` and `frame_length(prefix) -> int | None` (None: need more
  bytes) for stream framing in 2k.
- Codec rejects `mid_bits > 32` (S1).

Verification: Hypothesis round trips for each new feature (pack then unpack equals, and
unpack of arbitrary bytes never raises); `frame_length` on every prefix length; corpus
`hdr-*` profiles (little-endian bitfield, span outside packet, MID window overlap).
Real cFS: Stage 1 checksum and command cases still pass; the telemetry header time of
TIME HK decodes with the epoch from the checked-out cFE mission config (read from source,
recorded in `tools/cfs/NOTES.md`). msgid_v2 and trailer CRCs have no cFS case in this
build (MsgId V1); unit tests only.

### 2j - Variable-length payloads

CCDD (placeholder schema `ironbird-ccdd/1`; `/0` still loads, decision D10):

```json
{"name": "Count",   "type": "uint16"},
{"name": "Entries", "type": "uint32[]", "count": "Count",     "max": 64},
{"name": "Text",    "type": "char[]",   "length": "remaining", "max": 122}
```

- Element types: any scalar, `char`, `bytes`. `count` names an element count,
  `length` a byte count; `"remaining"` takes the rest of the packet (from the header
  length) and is allowed only on the last field.
- Layout rules: all fixed fields first (explicit offsets only there), then up to 8
  variable fields packed in declaration order; a count/length field is an unsigned
  integer in the fixed prefix whose range can express `max`; `max` is required, `min`
  optional (default 0); `max * element size <= MAX_PAYLOAD_BYTES`. A message's
  `min_payload` / `max_payload` replace `payload_size`; the maximum must fit
  `max_packet_bytes` and the length field (load-time `IB-ENC-004`, as for fixed
  messages). An event mapping's `message` may name a `char[]` field.
- Decode (untrusted): fixed prefix as now; per variable field, count <= `max`
  (`IB-DEC-008`, offset of the count field), bytes needed <= bytes remaining
  (`IB-DEC-009`), no trailing bytes after the last field (`IB-DEC-007`). Elements via
  one precompiled single-element `Struct.iter_unpack` (no per-count struct cache).
  Values are tuples.
- Encode: `Entries=1,2,3` or `Entries=[1,2,3]`, hex for `bytes[]`; the count field is
  filled from the data when omitted and must agree when given, unless
  `--allow-count-mismatch` (fault injection, ledgered as such).
- Render: line format shows the first 16 elements and `...(+N)`; JSON full lists; CSV
  one cell (2h).

Verification: Hypothesis (counts beyond the buffer and beyond `max`, zero counts,
encode/decode round trip, truncation at every offset); corpus `ccdd-var-*` (count after
array, missing `max`, signed count field, `remaining` not last) and wire cases; mock E2E
with a variable-length command and reply through `ib respond`. Real cFS: stock cFS has no
variable-length packet as far as the Stage 1 survey shows (2j confirms it from source);
see decision D6.

### 2k - Serial transport

Config

```toml
[[links]]
name = "fc-uart"
from = "ground"
to = "fc"
transport = "serial"
serial = { device = "/dev/serial/by-id/usb-FTDI_...", baud = 115200, data_bits = 8,
           parity = "none", stop_bits = 1, flow = "none" }
framing = { kind = "header_length", sync = "", max_idle_ms = 50, require_known_mid = true }
```

Validated: absolute device path; baud 50..12000000; parity none/even/odd/mark/space;
stop bits 1, 1.5, 2; data bits 5..8; flow none/rtscts/xonxoff; `sync` hex of at most 8
bytes; `max_idle_ms` 1..10000. One device carries both directions of the link.

Deliverables
- `transport/serial.py`: pyserial configures the port (custom baud rates, raw mode,
  `exclusive=True` = `flock`), then the loop drives the fd with `add_reader`/`add_writer`
  and non-blocking `os.read`/`os.write`, like UDP (this replaces the reader thread the
  execution model mentions; a thread remains the fallback for a device that cannot be
  polled). TX goes through a bounded 64 KiB buffer; `send(confirm=True)` awaits
  `termios.tcdrain` in a thread with a timeout derived from bytes and baud.
- `transport/framing.py`: `Framer` protocol (`feed(chunk, time_ns)`, `expire(now_ns)`,
  `stats()`) and `HeaderLengthFramer`:
  1. Strip to `sync` if configured; bytes before it are garbage.
  2. With `length_window` bytes, `frame_length()`; candidate is sane when header size <=
     total <= `max_packet_bytes`, the fixed-field mask matches, and (by default) the MID
     is known to the CCDD.
  3. Not sane: one byte joins the current garbage run; rescan from the next byte.
  4. Sane and complete: verify the checksum if the profile has one; failure is treated
     as a false sync (step 3). Success emits the packet.
  5. Garbage runs are emitted as GARBAGE frames (split at `max_packet_bytes`) when a
     packet follows, at the cap, or on expiry; each resync episode is one coalesced
     `IB-TX-007` notice with a bounded byte sample in the ledger.
  6. A partial candidate idle for `max_idle_ms` is flushed as a STALE frame.
  Invariant: the concatenation of all emitted frames equals the input byte stream,
  byte for byte, for any chunking. The recording therefore holds the line exactly.
  A per-call work budget bounds rescanning so adversarial input cannot block the loop.
- Errors with remedies: missing device (`IB-TX-001`, "check the cable; list
  /dev/serial/by-id"), permission (`IB-TX-001`, "ask an admin to add your user to the
  device's group, usually dialout; ironbird never needs root"), locked by another
  process (`IB-TX-006`), EIO/ENXIO on unplug (`IB-TX-008`, link down, reconnect with
  backoff 0.5 s to 5 s, GAP record for the down time, link up/down as MARK records and
  ledger state changes).
- Timestamps are taken when the read that completed a frame returns; USB adapters add
  their latency timer (FTDI default 16 ms). Documented, not corrected.
- `pyserial` added to dependencies (already planned).

Untrusted input: Hypothesis on the framer (the concatenation invariant; chunking
independence without expiry; packets separated by non-sane garbage all recovered; a
lying length never swallows more than `max_packet_bytes`; adversarial input processed
within the work budget). Corpus `ser-*` streams. `atheris` target for the framer.

Verification
- Mock E2E through `tests/support/ptybridge.py`: two pty pairs and a relay that can
  inject garbage, drop or split bytes, pace at a baud rate, and "unplug" (close the
  masters). `ib respond --as fc` on one side, `ib listen` / `ib send` / `ib record` on the
  other; asserts decode continues after garbage, resync notices coalesce, the recording
  is byte-exact against the relay's log, and unplug/replug reconnects.
- Throughput through a pty (no baud limit): the 1k pkt/s floor with zero loss.
- Real cFS: `tools/cfs/serial_bridge.py` (stdlib only, its own CCSDS primary-header
  length framing as an independent oracle) joins a pty to CI_LAB/TO_LAB UDP;
  `examples/cfs-serial.toml`; the Stage 1 command and event acceptance cases pass over
  the serial link, and bridge-injected garbage yields resync notices without losing the
  packets around it.
- Real UART hardware: decision D7.

### 2l - Stage 2 acceptance, throughput gate, docs

- `tests/cfs/test_stage2_acceptance.py` (marker `cfs`), one scenario: start cFS;
  `ib record` 60 s; TO_LAB enable; K SAMPLE_APP NOOPs, then RESET_COUNTERS, then K more;
  TIME commands. Then: `ib rec info` clean, seq contiguous, per-MID counts match SCH_LAB
  periods; `ib replay --where` finds exactly 2K NOOP events; `ib export` of
  `SAMPLE_APP_HK` shows the counter rising by K, resetting, rising by K; row counts equal
  `rec info` counts. A second run is killed with SIGKILL mid-capture: torn tail
  reported, replay plays to it, `ib rec repair` output equals the readable prefix.
- Throughput gate (`tests/bench/`): a loopback blaster sends 200-byte valid telemetry at
  10k pkt/s for 60 s into `ib record` (subprocess): recorded frames equal sent, kernel
  and queue drops zero, recorder buffer peak and RSS bounded, CPU share reported. CI
  asserts the 1k pkt/s floor with zero loss on any runner; the 10k target is asserted on
  the development host and tracked as a benchmark (regressions over 20% fail).
- Benchmarks collected across 2b-2k: ingest frames/s, `Recorder.append` cost, writer
  MB/s, reader frames/s, filter evaluations/s, framer MB/s (clean and adversarial),
  variable-length decode/s, export rows/s.
- README: new commands, config tables, exit codes; this plan updated where the build
  differed.

### Error codes added in Stage 2

| Code | Name | Meaning | Step |
|------|------|---------|------|
| `IB-ENG-001` | `ENG_LAG` | a consumer fell behind; frames not shown (still captured and recorded) | 2c |
| `IB-TX-003` | `TX_RECV` | receive failed (existing code, now raised) | 2c |
| `IB-TX-006` | `TX_BUSY` | serial device locked by another process | 2k |
| `IB-TX-007` | `TX_RESYNC` | serial stream resynchronised, N bytes skipped | 2k |
| `IB-TX-008` | `TX_DOWN` | link lost, reconnecting | 2k |
| `IB-LED-003` | `LED_EVIDENCE` | raw-evidence cap reached; packet evidence now counted only | 2d |
| `IB-LED-004` | `LED_FULL` | session ledger cap reached; commands refused (policy) | 2d |
| `IB-LED-005` | `LED_PRUNE` | old ledger files could not be pruned | 2d |
| `IB-REC-001` | `REC_CREATE` | cannot create the recording (exists, permission, directory) | 2e |
| `IB-REC-002` | `REC_WRITE` | write or sync failed; recording stopped | 2e |
| `IB-REC-003` | `REC_OVERRUN` | recorder buffer full; frames dropped (GAP recorded) | 2e |
| `IB-REC-004` | `REC_FORMAT` | not an ironbird recording | 2e |
| `IB-REC-005` | `REC_VERSION` | unsupported version or unknown critical record type | 2e |
| `IB-REC-006` | `REC_DAMAGED` | damaged record mid-file (offset, span) | 2e |
| `IB-REC-007` | `REC_TORN` | recording ends in an incomplete record (not closed cleanly) | 2e |
| `IB-REC-008` | `REC_INDEX` | index missing or invalid; rebuilt by scanning | 2e |
| `IB-REC-009` | `REC_DEFS` | embedded definitions differ from config, or cannot be loaded | 2g |
| `IB-REC-010` | `REC_SPACE` | low disk space for the recording | 2e |
| `IB-FLT-001` | `FLT_SYNTAX` | filter syntax error (column, excerpt) | 2f |
| `IB-FLT-002` | `FLT_NAME` | unknown name in a filter | 2f |
| `IB-FLT-003` | `FLT_TYPE` | literal does not fit the name's type | 2f |
| `IB-FLT-004` | `FLT_LIMIT` | filter too long, deep or wide | 2f |
| `IB-EXP-001` | `EXP_WRITE` | cannot write export output | 2h |
| `IB-EXP-002` | `EXP_EXISTS` | export output exists; pass `--force` | 2h |
| `IB-DEC-008` | `DEC_COUNT` | variable-length count above the CCDD maximum | 2j |
| `IB-DEC-009` | `DEC_OVERRUN` | variable-length data runs past the packet end | 2j |

`IB-FLT-` joins the usage prefixes (exit 2). Proposed exit code 5 (decision D8): output
complete up to mid-file damage in an input recording.

### Carry-over traceability

| Stage 1 review item | Where |
|---------------------|-------|
| Raw frozen `Frame`, decode in the consumer, malformed frames yielded | S1, S2, 2b |
| Transport factory on `link.transport`, `send(confirm=)` | S2, 2b |
| Structured `Notice` | S5, 2a |
| Byte-bounded memory (edge cap, ledger truncation at creation, byte-bounded queues) | 2c |
| Coalesced `TX_RECV` | 2c |
| Header profiles: endianness, trailer/partial checksums, msgid_v2, per-message/per-send values, epoch, `frame_length()` | 2i |
| Immutable shared data | 2a, then every step |
| Ledger growth: sampling, caps, rotation, retention, send policy | 2d |
| Output and signals equivalence | Stage 3; Stage 2's new commands keep Stage 1's exit codes, bounded writer and signal path |

### Decisions for the user

- **D1 Embed definitions in recordings.** Recommended: yes, each recording carries its
  nodes' CCDD and header profiles (8 MiB cap each), and replay uses them by default
  (`--defs config` overrides), so a recording decodes correctly after the CCDD changes.
- **D2 Ledger full policy and caps.** Recommended: refuse commands at the session hard
  cap (`on_full = "refuse_commands"`), capture continues; caps 512 MiB per session (256
  MiB evidence), retention 30 days or 2 GiB per state directory.
- **D3 Events under floods.** The plan says events are never sampled; recommended to
  keep that, with the session cap as the only backstop.
- **D4 Commands from a separate `ib send`** are in the ledger (with raw bytes) but not in
  a Stage 2 recording; the Stage 3 engine fixes this. Recommended: accept the gap
  (alternative: replay and export interleave TX records from a ledger file).
- **D5 Replay to the wire** (retransmitting a recording) deferred to Stage 5 send modes,
  since it can transmit commands. Recommended: defer.
- **D6 Variable-length acceptance on real cFS.** Stock cFS appears to have no
  variable-length packet. Recommended: add a small `ib_test` cFS app under `tools/cfs`
  that emits a counted-array packet and can blast telemetry at 10k pkt/s, which also
  gives the throughput gate a real-cFS source. Alternative: mock only.
- **D7 Real serial hardware.** The pty bridge is the gate for now; which device and baud
  should a hardware acceptance run use, and when is it available?
- **D8 Exit code 5** for "output produced, but the input recording was damaged
  mid-file" (a torn tail alone is a warning with exit 0). Recommended: adopt.
- **D9 Ring shape (Stage 3).** A variable-length byte ring carrying S3 records needs
  about 7x less memory at the 200-byte average than fixed slots, and the plan's
  1536-byte slots are smaller than the 2048-byte default `max_packet_bytes`.
  Recommended: byte ring; decide at Stage 3 planning.
- **D10 CCDD placeholder schema** bumps to `ironbird-ccdd/1` for variable-length fields
  and per-message header values; `/0` keeps loading. Recommended: adopt.
- **D11 Arrays of structs** (e.g. EVS HK `AppData[16]`) stay out of Stage 2.
  Recommended: a follow-up after 2j, reusing its layout compiler.

## Later stages (outline; each gets its own plan)

2. Recording + replay with filters (CLI), serial transport, variable-length payloads,
   CSV/text export. Detailed plan: "Stage 2" above; every item below maps to a sub-step
   there ("Carry-over traceability").

   Carried over from Stage 1 review:
   - Engine yields raw frozen `Frame(seq, time_ns, direction, node, link, source, mid, data)`;
     decode happens in the consumer, and malformed frames are yielded too (recorded raw).
   - Transport factory keyed on `link.transport`, and a `Transport.send` signature that
     includes `confirm`.
   - Structured `Notice(code, key, summary, count, window_s, ledger_n)` replaces the `str`
     notify callback.
   - Byte-bounded memory: enforce `max_packet_bytes` at the transport edge, truncate raw
     data at record creation, bound the UDP queue by bytes.
   - Surface UDP receive-side socket errors as a coalesced `TX_RECV` (the effective
     `SO_RCVBUF` and kernel drops are already ledgered and reported in Stage 1).
   - Header profiles: per-field endianness, trailer and partial-span checksums,
     non-contiguous MID fields (msgid_v2), per-message and per-send header values,
     configurable time epoch, `frame_length()` for serial streams.
   - Immutable shared data: `MappingProxyType` for `Header.values`, registries and stats
     snapshots.
   - **Ledger growth under hostile or garbage floods** (a design decision, deliberately not
     implemented in Stage 1: the ledger keeps every malformed datagram in full, so a flood can
     write on the order of 1 GB in 10 s). Stage 2 decides and builds: raw-evidence sampling
     per key and window (every packet counted, the first N and a periodic sample kept raw,
     the rest reduced to a per-key count record); a ledger size cap per session and per state
     directory with a coded error when reached (and a defined "stop sending or keep going"
     policy for transmitting commands); and rotation plus retention (age and total size) of
     old ledger files.
   - Output and signals: the bounded stdout/stderr writer, the signal-to-cancel path and the
     exit codes (0 normal, 1 error, 2 usage/config, 3 `--count` not reached before `--timeout`,
     4 command succeeded but stdout lines were lost,
     128+n signal) are Stage 1 behaviour the TUI front-end must keep equivalent (a stalled
     terminal never stalls capture; SIGHUP is an orderly shutdown).
3. TUI: latest-per-packet view and time-scrolling view (sorted and emphasised by telemetry
   category priority, with category filter presets), a dedicated **Events** page showing
   only events from all nodes (node, time, app, event id, type/severity, message), hold /
   pause / stop, error status line + details pane, ledger viewer.
4. TUI command composer with tiered search and argument entry.
5. Send modes: scheduled at T, repeat N, every M ms/s until T or for duration.
6. xlsx export, optional semantic search (local sentence-transformer as an extra),
   1553 transport when hardware is available.

## Verification (Stage 1)

- `pyenv virtualenv 3.11 ironbird && pip install -e .[dev]`, then `pytest -n auto`: header profile pack/unpack round-trips (incl. CRC), codec
  encode/decode against hand-built byte fixtures, config validation errors, CCDD
  loader errors.
- Real cFS: all Stage 1 acceptance cases above pass.
- Malformed-input property tests and corpus pass; `atheris` smoke run on the decode boundary.
- E2E mock on localhost: `ib respond --as fc` in one shell; `ib send fc NOOP` and
  `ib send fc SET_MODE mode=SAFE` from another are decoded correctly by the fc side,
  which answers with telemetry that `ib listen fc` decodes; a bad arg / unknown command / closed port each produce one-line coded
  errors and full ledger entries.
- `ib events` against two mock flight nodes prints events from both (and honours
  `--min-type`); category/event-mapping validation errors are coded one-liners.
- Ledger file inspected for a complete, timestamped trace of the session.
