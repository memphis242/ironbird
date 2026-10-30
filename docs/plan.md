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
| 2d | Ledger under floods, disk full, retention | 2c | bounded ledger growth, disk-full reserve, exit code 6, retention warnings, `ib ledger prune`, `ib rec prune`; `[ledger]`, `[retention]` config |
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
    HAS_MID = 0x01  # mid was peeked from the profile's MID window
    TRUNCATED = 0x02  # data cut at max_packet_bytes; orig_len is the size on the wire
    GARBAGE = 0x04  # serial: bytes skipped while resynchronising, not a packet
    STALE = 0x08  # serial: partial packet flushed by the inter-byte timeout


@dataclass(frozen=True, slots=True)
class Stream:  # one direction of one link, as seen by this process
    id: int  # u16, unique within a session and its recording
    link: str
    node: str  # node whose CCDD and profiles decode it (link.destination)
    kind: MessageKind  # what the bytes are: command or telemetry
    direction: Direction
    transport: str  # "udp" | "serial"
    endpoint: str  # sanitized display text: "udp 0.0.0.0:2234", "serial /dev/ttyUSB0"


@dataclass(frozen=True, slots=True)
class Frame:
    seq: int  # u64, session-wide, strictly increasing from 1, assigned at ingest
    time_ns: int  # CLOCK_REALTIME taken in the transport callback (receive or send)
    stream: Stream
    source: str  # interned peer text ("" for serial)
    mid: int | None  # peeked, used only for routing and filtering, never trusted
    flags: FrameFlag
    orig_len: int  # == len(data) unless TRUNCATED
    data: bytes  # len(data) <= max_packet_bytes
    # __post_init__ asserts: HAS_MID iff mid is not None; TRUNCATED iff orig_len > len(data);
    # the flag-combination rules of S3 "Reading"; len(data) <= max_packet_bytes
```

- `seq` gives total order across links and makes loss visible. It is session-wide, so a
  recording that excludes a stream (`--link` subset, a stream with no recorder) is not
  contiguous: within a recording, seq is contiguous except where a GAP record says why,
  and the writer emits a GAP with reason `not_recorded` (stream, count, seq range) for
  frames of streams it was not given, so a hole is never unexplained.
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
24-byte preamble followed by records; everything after the preamble is CRC-checked.

**Preamble (24 bytes):** magic `89 49 42 52 0D 0A 1A 0A` (`\x89IBR\r\n\x1a\n`: high bit,
CR-LF and ^Z catch text-mode and 7-bit transfer damage), `u16 major = 1`, `u16 minor = 0`,
`u32 flags = 0`, `u64 nonce` (from `os.urandom` when the file is created; each split part
has its own).

**Why a nonce.** Frame data comes from the wire, so a packet can contain bytes that look
like a record, and a resync scan over a damaged file could lock onto it. The nonce
(chosen after the traffic it will hold is unknowable) seeds every CRC, so bytes forged
before the file existed cannot carry a valid CRC.

**Record envelope (12 bytes)**, record start 8-byte aligned, zero padding after the body:

| Offset | Type | Field |
|--------|------|-------|
| 0 | u32 | `body_len` (bytes after the envelope, padding excluded) |
| 4 | u8 | `type` |
| 5 | u8 | `flags` (0 in v1) |
| 6 | u16 | reserved (0) |
| 8 | u32 | `crc32`: `zlib.crc32(envelope[0:8] + body, nonce32)` (`nonce32` = low 32 bits of the preamble nonce as the CRC seed) |

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
| 0x06 | GAP | stream, reason (kernel drop, queue drop, recorder overrun, link down, not recorded), count, first/last time, seq range | 64 B |
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
`u64 chunk_start` (offset of the chunk's first record), first/last seq, **min/max**
`time_ns` over the chunk's frames (CLOCK_REALTIME can step backwards, so first/last are not
bounds; a step is also a MARK record), first/last `mono_ns` (CLOCK_MONOTONIC read with the
frame's `time_ns`, for pacing and for ordering across a clock step), `u32 frames`, `u32 bytes`, `u32 frames_without_mid`, `u16 flags` (bit 0:
MID table overflowed), `u16 n`, then n x (`u32 mid`, `u32 count`) sorted by MID, n <= 256.
Summaries let replay and export skip chunks by time range or MID set, give `ib rec info`
per-MID counts without reading frames, and are the work units for a decode worker pool.

**Writing and crash safety.**
- Create with `O_CREAT | O_EXCL`, mode 0600, directory 0700; never overwrite. The writer
  holds `flock(LOCK_EX)` on the file until it closes or dies (the kernel drops it), which
  lets `replay --follow` tell a live writer from a dead one (2g). Order on
  open: preamble, META, BLOBs (definitions, see decision D1), STREAMs, then `fsync` the
  file and its directory so the file survives a crash from the first frame on.
- `Recorder.append(frame)` (a sink) encodes into a `bytearray` with precompiled structs;
  at 256 KiB or every 250 ms the buffer goes to a writer thread (`os.write`, releases the
  GIL, handles partial writes). `fdatasync` once per chunk by default
  (`recording.fsync = "chunk" | "always" | "never"`): a power loss loses at most about
  1 s, a crash at most what was not yet handed to the disk (the 250 ms flush interval plus up
  to one timer tick, about 500 ms at the worst), a clean stop or signal nothing.
- Back-pressure is bounded: in-memory plus queued bytes are capped
  (`recording.max_buffer_bytes`, default 256 MiB, about 2 minutes at the 10k pkt/s
  target). Above it, frames are dropped and counted, and a GAP record (reason
  `recorder_overrun`, seq range) is written once there is room; `IB-REC-003` is an
  error-level notice, never coalesced away into silence.
- Transport drops (kernel, queue) never received a seq; the engine turns their counter
  deltas into GAP records on its 1 Hz tick, so a replay shows every loss and where.
- Close: final CHUNK, INDEX, TRAILER, `fdatasync`, close. On SIGINT/SIGTERM/SIGHUP the
  close runs in the cancellation path; it ends when the writer makes no progress for 5 s
  (a draining backlog is waited for), leaving a readable file without a trailer.
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
  <= META `max_packet_bytes` <= 65535, only known flag bits, seq strictly increasing,
  and the frame consistency rules below; BLOB SHA-256 matches its content; INDEX offsets increasing
  and inside the file. Every yielded frame passed its CRC.
- **Frame consistency (so `Frame` asserts are unreachable from a file).** `TRUNCATED` is set
  iff `orig_len > len(data)` (`orig_len < len(data)` is damage); `mid == 0` when `HAS_MID`
  is clear (the reader then yields `mid=None`); meaningless flag combinations are damage:
  `GARBAGE` with `STALE`, `GARBAGE` with `HAS_MID` or `TRUNCATED`, `GARBAGE` or `STALE` on a
  stream whose transport is not serial. Reserved fields and padding bytes must be zero.
- **Resync is conservative (untrusted).** After damage the scan to the next valid record
  accepts a candidate only if it passes every record rule above with the nonce-seeded CRC,
  its `seq` (FRAME) is greater than the last good `seq`, its padding is zero, **and** it is
  followed by a second valid record at the offset its length implies (two chained valid
  records). A lone plausible record never ends damage.
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
expr      = term { "or" term }          (parsed to one n-ary OR node)
term      = factor { "and" factor }     (parsed to one n-ary AND node)
factor    = "not" factor | "(" expr ")" | predicate
predicate = name cmp literal
          | name "in" "[" literal { "," literal } "]"
          | name "contains" STRING
cmp       = "==" | "!=" | "<" | "<=" | ">" | ">="
name      = IDENT { "." IDENT }
literal   = INT | HEX | FLOAT | STRING | IDENT | TIME
INT       = [ "-" ] digits              (the sign belongs to the literal; there is no unary
FLOAT     = [ "-" ] digits "." digits    minus, so `x == -1` and `x >= -0.5` work)
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

- **Names.** `hdr.` and `event.` are reserved namespaces, as are the frame, model, check and
  decode names in the table. Reserved names always win: a bare `mid`, `seq` or `status` is the
  frame's whatever a CCDD calls its fields, so a CCDD update can never change what a script
  means. A CCDD field with a reserved name is reached as `MSG.field` (`TLM_A.mid`); a message
  called like a reserved word has its fields reached by their bare names (`hdr.x` and
  `event.x` always mean the namespaces). A bare namespace (`hdr == 1`) is `IB-FLT-002`, with
  the `MSG.field` spelling of a CCDD field of that name in the details.
- **Semantics.** A predicate on something a frame does not have (a field of another
  message, a decode-level name on a malformed frame) is false; `not` inverts that, so
  `not X.f == 1` matches other messages. A `GARBAGE` frame (serial bytes skipped while
  resynchronising) is malformed whatever its bytes would decode to: `status` is `malformed`,
  it is never decoded and nothing that needs a decode matches it. `in [a, b]` is `== a or
  == b`: each item is judged against each field on its own, an item a field cannot hold
  (a label its enum lacks, a number for text) is simply never equal there, and a message
  is out only when no item fits it. `x != L` is `not (x == L)` for every message that has
  the field, including those whose field cannot hold `L` (`mode != SCIENCE` is true for a
  message whose `mode` has no `SCIENCE`); a message without the field is false, as above.
  A float literal is rounded to the width of the field it is compared with (a float32
  field holds float32 values, so `ratio == 0.1` is true for a stored 0.1f); a decimal
  compared with an integer field is exact (`big > 18446744073709551614.5`). Precedence:
  `not` > `and` > `or`.
- **Compilation** against the CCDD models in use (recording or config):
  `compile_filter(text, models) -> Filter(source, mids, needs_decode, match)`. `mids` is
  the candidate MID set derived from the tree (`and` intersects, `or` unites, `not` makes
  it unknown), applied to `frame.mid` before anything is decoded; `needs_decode` decides
  whether non-candidates are decoded at all. Names, types and literals are checked at
  compile time (`IB-FLT-002/003`), so a typo fails before any packet is read. A filter that
  can never match (an `and` whose operands share no MID) is `IB-FLT-003` too, as is one
  whose candidates share no MID with the run's `--mid` or with the MIDs of event messages.
- **Limits** (`IB-FLT-004`): 4096 characters, 1024 tokens outside the brackets of `in` lists,
  depth 32, 1024 `in` items (counted by the parser, not as tokens, so the limit is the one in
  force), numeric literals of at most 40 characters (sign included). Depth counts every nested `not`
  and every parenthesis; flat `and`/`or` chains are n-ary nodes evaluated iteratively, so
  they add no depth and cannot recurse deeply. Non-ASCII is allowed only inside strings.
- **Errors** are one line with the column and a short excerpt; the details carry the
  expression with a caret line and up to three close-match suggestions (`difflib`). The
  query is clipped to 64 characters; every candidate name is checked against the cheap
  similarity bounds (length, letter counts) and the 256 best by that bound are compared in
  full, so a hostile CCDD or expression cannot make suggestion matching expensive and a name
  that sorts late is as findable as one that sorts first. In the echo, characters that are
  not printable or that reorder text (right-to-left letters, Arabic digits, bidi controls)
  are shown as `?`, one for one, and the caret is placed by display width (wide characters
  take two columns, combining marks none). `IB-FLT-*` codes exit 2 like other usage errors.
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
    key: str  # coalescing key: code + MID + stream/context
    summary: str  # sanitized, one line, no count suffix
    count: int = 1  # occurrences this notice stands for (1 = first)
    window_s: float = 0.0  # span the count covers (0.0 for the first)
    ledger_n: int | None = None  # record number queued for the latest occurrence, if any

    def line(self, *, verbose: bool = False) -> str:
        """'error IB-DEC-005: ... (x37 in 10 s)'; verbose appends ' [ledger n=1234]'."""
```

The coalescer emits `Notice` values instead of strings; front-ends render them.
`Ledger.record` returns the record number so a notice can point at its full record
(`--verbose` appends `[ledger n=1234]` to every notice that has one and prints the ledger
file path once at start; the TUI details pane opens that record).
- `ledger_n` is the record number **queued** for the latest occurrence that had one. It is
  written unless the ledger is still stalled when the process ends (2d; reported as
  `IB-LED-001`) or the process is killed before the queue drains. `record` returns
  `None`, and the occurrence has no pointer, when the ledger is disabled (`IB-LED-002`),
  the queue was full and the record was refused (counted by `ledger.dropped`), evidence
  sampling (2d) skipped that occurrence, or the record's tier is counted only (over its
  allowance, writing into the reserve, or stalled; 2d). The coalescer keeps the last
  non-`None` pointer of a window.
- After rotation (2d) `n` stays session-wide, but finding it would mean scanning parts, so
  the pointer then names the part too: `Notice` gains `ledger_part: int = 1` and the
  verbose suffix becomes `[ledger n=1234 file=<stamp>-<session>.p0003]` (no `file=` for
  part 1). A record written into the active reserve names the part number the reserve
  takes when it retires; until then the file is `<stamp>-<session>.rNNNN.reserve`.

### S6. Ring compatibility (decided now, built in Stage 3)

The Stage 3 ring carries the S3 envelope plus FRAME body unchanged, with STREAM and
SOURCE records travelling the same way, so ring readers, recording readers and worker
pools share one decoder (`frame_codec.py`). Frames are self-delimiting, position
independent, CRC-checked and at most `12 + 32 + max_packet_bytes` bytes (`44 + max_packet_bytes`), so they fit
either a fixed-slot ring (slot = `round_up(44 + max_packet_bytes, 8)`) or a variable-length
byte ring. Which shape is Stage 3's call (decision D9); Stage 2 only guarantees the encoding.
A reader that attaches late has missed the STREAM and SOURCE records it needs to interpret
FRAMEs, so Stage 3 must give the ring either a side table of live STREAM/SOURCE definitions
that attachers read first, or periodic re-emission of them in the ring itself (a frame
whose ids are undefined is skipped and counted, never guessed).

### 2a - Structured notices, immutable shared data

Deliverables
- `notice.py` (S5); `Coalescer` emits `Notice`; CLI renders `Notice.line()`, output text
  unchanged from Stage 1 at default verbosity.
- `Ledger.record` returns its record number, or `None` when no record was queued
  (disabled, failed, or queue full: the new record is refused and counted, never an older
  one evicted; evidence-tier records, including wire-origin errors, have a capped slice of
  the queue so a flood is unlikely to crowd out essential records, but capture of an
  essential record is not guaranteed when the queue is full). `record_error` also returns `None` when its prompt flush failed.
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
links, malformed frames yielded with flags; ingest-only benchmark (frames/s on loopback) as a
baseline for the 10k gate; cFS acceptance unchanged.

As built (where the build differs from, or settles, the text above)
- `engine.py` holds `Engine` (ingest, sinks, per-link-end transports), `Capture`, `FrameDecoder`;
  `Session` wraps it (`capture()`, `decoder()`, `add_sink()`) and keeps `listen()`, `events()`,
  `respond()`, whose `Listener`, `EventStream` and `Responder` are capture consumers. `Received`
  is replaced by `frame.Decoded(frame, packet)`.
- A transport is one *link end* (`LinkEnd.SOURCE` receives telemetry and sends commands,
  `DESTINATION` the reverse), shared by that end's sends and receives; `open_transport(link,
  end, report)` builds it unopened (no `framing` argument until 2k). The UDP transport owns up to
  two sockets (a bound receive socket, a connected send socket opened by the first send so a
  listener does not need the command host to resolve). Beyond S2, the protocol has
  `local_address`, `facts()` (ledgered once after open, e.g. the receive buffer granted),
  `poll()` (1 Hz, off-loop counter refresh) and `send(..., handed_off=)`: the callback runs the
  moment the bytes leave the process, which `ib send` needs to tell "sent" from "not sent" while
  `confirm` still waits for an ICMP refusal. `Receiver` is a plain `Callable` alias.
- Sinks are session-wide: they see every frame the session ingests or sends from the capture's
  open to its close. Consumers receive RX frames only; TX frames (`send_command`, responder
  replies) reach sinks, so a recorder sees both directions.
- `Frame` cannot know `max_packet_bytes`, so it asserts the global 65535 cap; ingest enforces the
  configured cap by cutting longer data and setting TRUNCATED (the copy of the whole datagram
  before the cut is what 2c removes). A truncated frame is reported as `IB-DEC-002` at ingest.
- Consumer queues are bounded by frame count in 2b (`queue_frames`, default 16384, newest
  dropped); 2c adds the byte bound and `IB-ENG-001`. Until then an overflow keeps Stage 1's code
  `IB-TX-004`, worded as frames dropped from the consumer view. No recorder exists in 2b, so
  the notice says "still reached a sink" only for the frames a sink had received when they were
  dropped (counted per subscription), never "captured" unconditionally. A capture has one
  reader: a second reader that waits while the first is waiting raises `IB-INT-001` (the waiting
  reader is never the one refused). Ownership is not bound to the first reading task: on
  Python 3.11 `asyncio.wait_for(it.__anext__(), t)` runs each call in a new task, so a per-task
  owner would refuse that idiom; deferred to 2c with the byte bound if a real case appears.
- `Codec.classify`/`DatagramClass` are gone: `Codec.check` does their job and the rest of the
  boundary. Errors come in a fixed order: over the cap, shorter than the header, MID not in the
  CCDD, length field, fixed fields (`HeaderProfile.fixed_mask`/`fixed_value`, one compare),
  checksum, function code, payload size. `decode` accepts exactly what `check` accepts and
  returns the same error otherwise. `FrameDecoder(report_checked=False)` never re-reports a
  failure `check` already reported at ingest; 2j must report the errors only its decode can
  produce (variable-length counts) in both modes.
- The profile-load check that MIDs fit 32 bits is in `Codec` (`IB-HDR-002`, corpus
  `codec-hdr-mid-wider-than-32.toml`). `SerialLinkSpec` exists as a placeholder dataclass; the
  config still refuses `transport = "serial"` until 2k parses it.
- `Frame` and `Decoded` are `NamedTuple`s (`Frame` subclasses one to add the invariant asserts
  in a `__new__` that exists only when `__debug__`), `FrameFlag` stays an `IntFlag` for
  consumers while ingest works on plain-int `FLAG_*` constants and picks the `FrameFlag` from
  `FLAGS_BY_BITS`. A frame has a ninth field, `checked`: the `Codec.Resolved` token (message, header
  word, plus the codec, kind and `bytes` it was made from) of a frame that passed `Codec.check`
  at live ingest, `None` for a failed check and for any frame not built by live ingest.
  `FrameDecoder.decode` uses it to call `Codec.decode_resolved` (the second half of `decode`)
  instead of checking the packet again; a frame without it takes the whole `Codec.decode`.
  `decode_resolved` believes a token only when `token.codec is self`, the kind matches and
  `token.data is data` (a real check, also under `-O`), else it runs the whole `decode`; it never
  raises. `checked` is a cache, not identity: `Frame` equality, hashing, `copy` and pickling use
  the eight packet fields only (live frames are hashable and picklable and equal their
  replay-built twins), and `_replace`/`_make` rebuild through the constructor's invariant
  checks and drop the token. `Codec.resolve` is the one place the check
  runs, so the decode boundary stays single.
- The capture's queue owns every unread frame: iterating takes one frame as it is yielded, and
  `Capture.wait_frames()` hands the consumer the live deque to `popleft` from (`Listener` does
  so, so the receive-to-print path is one async generator, not a chain of them), so breaking out
  of an iteration and iterating again loses nothing. `Capture.next_batch()` (at most 512 frames)
  transfers what it returns to the caller.
  `BoundedWriter` writes whole chunks to a regular file or a terminal and `PIPE_BUF` slices only
  to pipes and sockets, where atomicity matters.
- Sinks are attached once however many holders ask (reference counted by identity), so one sink
  on two captures sees each frame once. A sink that raises is detached on its first failure and
  reported and ledgered once (`sink.detached`), never once per frame; its holders stay counted,
  so asking for it again re-attaches it and an old holder's release cannot detach it from a
  newer one.
- Datagrams still in a UDP receive buffer when its transport closes are read, discarded and
  counted (`TransportStats.unread_at_close`, ledger `rx.unread_at_close`); on a link end that
  was already losing frames the count is reported (`IB-TX-004`), and `Listener.close` reports the
  frames left unread in the queue, so a SIGTERM under overload accounts for every frame:
  `sent == shown + kernel drops + unread in the socket + unread in the queue`.
- Per-MID statistics count only frames that pass `Codec.check`.
- A link end's transport is opened, joined and closed under the end's lock: a capture subscribes
  inside the lock, the closer re-checks the subscribers inside it, and the end's state changes
  only after the close completes, so a close and an open of the same end cannot race for the
  port. A send that finds its transport closed under it is made again on the one that replaces
  it (`TransportClosed`, never reported as a link failure); a send after `Engine.close` is an
  `IB-TX-002` "not sent: the session is closing". `Engine.close` ends every capture's iteration. A transport open is bounded
  (`OPEN_TIMEOUT_S`, 5 s, `IB-TX-001`) and cut short by `Engine.close`, so a resolver that does
  not answer cannot hold the link end's lock; `_attach` re-checks `closed` under the lock, so a
  capture opened across a close raises (`IB-INT-001`, "the session is closed") and its transport
  is closed, never an iteration that does not end. A send made twice on a closed transport says
  whether the session is closing or another consumer closed the end twice.
- Ingest-only benchmarks (`tests/test_benchmarks.py`, names `*ingest_only*`): they time the
  receive side alone (a 200-byte valid telemetry packet from a separate sender process into a
  capture with a draining consumer that discards, 4-core dev host): one frame through ingest
  about 4 us; 73k packets/s saturated; 10k packets/s paced with zero loss. They say nothing
  about what a front-end sustains. The end-to-end guards are the `listen` ones:
  `test_listen_cli_at_the_target_rate_loses_no_output` (a separate `ib listen --format json`
  writing to a file, a separate sender at 10k packets/s of the large `MOCK_HK` packet: 30000 of
  30000 lines, no `IB-OUT-*`; the guard allows 5% host-side datagram loss) and
  `test_listen_path_costs_little_over_decode_and_render` (receive, queue, decode and JSON
  render at about 21 us per packet against 17.5 us for the bare decode and render; the guard is
  a 1.5x ratio so it holds on any host).

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

As built (where the build differs from, or settles, the text above)
- **Edge cap.** `UdpTransport` reads into a `bytearray(max_packet_bytes)` with
  `recvmsg_into(..., MSG_TRUNC)`; on Linux the returned size is the datagram's true length even
  when it is longer than the buffer, so the kernel copies at most `max_packet_bytes` and a longer
  datagram becomes one `TRUNCATED` frame with `orig_len` = the wire size (`IB-DEC-002`). The
  receiver is called with data already cut. The cut in `Engine._ingest` stays as a backstop for a
  transport that does not cut (the serial transport of 2k will cut at its own framer); the
  `MSG_TRUNC` size semantics are Linux's, the only supported platform. The connected send socket's
  drain (`_on_tx_readable`) reads into a 1-byte buffer, so a peer that sends to the ephemeral port
  of our send socket costs no copy. `open_transport(link, end, report, *, failed=None,
  max_packet_bytes=DEFAULT_MAX_PACKET_BYTES)` and `TransportFactory` carry the cap and the failure
  callback; `TransportStats.rx_errors` counts failed reads; `facts()` ledgers `max_packet_bytes`.
- **Consumer queue.** Bounded by frames (`queue_frames`, default 16384) and bytes (`queue_bytes`,
  `CONSUMER_QUEUE_BYTES` = 32 MiB, on `Engine.open_capture` and `Session.capture`). A frame is
  charged `len(data) + FRAME_OVERHEAD_BYTES` (320: the `Frame` tuple, the bytes header, the check
  token), so tiny frames cannot dodge the bound through overhead; `queue_bytes` must admit one
  largest frame (`max_packet_bytes + 320`, asserted). A frame the consumer pops off the live deque
  (`wait_frames`, iteration) stops counting when the queue next reconciles its charge list with
  `items`: when the bound would be passed and every 64 offers, so the figure is never below the
  truth and the hot path pays no per-pop work. `next_batch` charges its batch until the consumer
  asks again (`wait`), so a consumer sitting on batches cannot exceed the bound. `CaptureStats`
  gains `queued_bytes`, `peak_bytes` (an upper bound on what the queue ever held) and
  `not_shown_bytes`. The queue is closed with an optional error: once drained, every read raises it.
- **Worst-case memory per hop** (default `max_packet_bytes` 2048; at the 65535 maximum only the
  per-frame figures change): UDP read buffer 1 x `max_packet_bytes`; one consumer queue
  `queue_bytes` (32 MiB) plus one frame being handled; the ledger queue 64 MiB estimated
  (48 MiB of it evidence), each record at most `max_raw_bytes` of raw bytes. A flood of 64 KiB
  datagrams therefore costs at most 32 MiB x captures + 64 MiB + buffers, whatever the rate and
  however stalled the consumer or the disk. Measured (flood test, `max_packet_bytes` 65535, 4000 x
  60000 B, queue_bytes 4 MiB, consumer never reading): RSS grew 4.2 MiB; the same flood on `main`
  grew it 235 MiB with every frame unread.
- **IB-ENG-001 replaces IB-TX-004 for consumer lag**: the consumer-queue overflow notice and the
  "listening ended with N frame(s) received but not shown" notice are now `IB-ENG-001`
  ("consumer fell behind, N frame(s) not shown so far; K of them still reached a sink"; no
  recorder exists before 2e, so the notice does not say "recorded"). `IB-TX-004` is left for the
  kernel's drops and for datagrams unread in the socket at close. The ledger's `rx.queue_drops`
  record gains `queue_dropped_bytes` and `queue_peak_bytes`.
- **Ledger.** `Ledger.record` cuts every top-level bytes-like field to `max_raw_bytes` and turns
  `bytearray`/`memoryview` into `bytes` when the record is created (a reused buffer cannot change
  a queued record), adding `<field>_len` (`raw_len`) with the full length. The written text is
  unchanged (`hex ...(+NB)`); `_Cut` carries the head and the omitted count so it survives to the
  formatter. Each record is charged `ENTRY_OVERHEAD_BYTES` (512) plus its str and bytes-like field
  sizes against `max_queue_bytes` (64 MiB) and, for evidence, `max_evidence_queue_bytes` (48 MiB);
  a record over either bound is refused and counted like one over the count bounds (the count
  bounds stay). `Ledger.pending_bytes` exposes the figure. Only top-level fields are scanned
  (nested containers are not; no caller nests bytes), and strings are charged, not cut. The
  `rx.malformed` record gains `wire_len` (the size on the wire, which differs from `raw_len` only
  past the cap).
- **IB-TX-003.** A failed `recvmsg` on the receive socket (anything but EAGAIN/EINTR) was held in
  the transport's pending send error; it is now counted (`rx_errors`) and reported through the
  transport's `report` callback, coalesced per link end like any notice, with the running count in
  the text. It is permanent, and ends the transport (reader removed, nothing spins), when the errno
  is EBADF, ENOTSOCK or EINVAL, or when reads have been failing with no good read in between for
  `RECV_FAIL_WINDOW_S` (5 s, by the transport's monotonic clock) or for `RECV_FAIL_BACKSTOP`
  (100000) reads, whichever comes first. The clock decides, so a burst of any number of failures
  inside the window never ends a capture; the count is only a backstop, because a failing read
  leaves its datagram queued and the loop retries at once (the dev host managed about 69000
  failed reads in the 5 s with the ledger on, so there the window ends it; a host failing faster
  than 20000 reads/s reaches the backstop first). A good
  read ends the failing episode, and two failures a window or more apart are separate episodes
  (nothing arrived in between to prove either way, and an error that clears itself must not
  accumulate toward a fatal one). A permanent failure calls the engine's `failed` callback, which ledgers
  `rx.failed`, flushes the drop accounting and closes the queues of every capture fed from that
  link end with the error: each consumer reads what was received before the failure, then its
  read raises the `IB-TX-003` error, which the CLI prints and exits 1 on like any coded error. No
  separate notice is shown for a permanent failure (the raised error is the report, so the user is
  not told twice), and the coalescer window still open for the failures that led up to it is
  dropped (`Coalescer.discard`, reached through `Session.withdraw_notices`), so no
  "(xN in T s)" summary of them can print after the fatal line; the count is in the fatal error
  and every occurrence is in the ledger. A capture opened on a failed link end is refused with the same error until the
  last capture of the end closes and the transport is replaced. A closed queue is offered
  nothing, so a link end that failed does not make its siblings count lag.
- **The closing notice** ("listening ended with N frame(s) received but not shown") is given only
  when the consumer queue itself refused frames (`CaptureStats.not_shown`), so what is left
  unread was behind because of lag. Frames merely in flight at SIGTERM, or lost to the kernel
  (`IB-TX-004` says so), no longer produce an `IB-ENG-001` they do not deserve.
- **Not done.** Reader ownership by task (2b's deferral) stays deferred: no real case appeared.
  The ledger still writes a malformed 64 KiB packet as ~190 KB of JSON (hex plus separators);
  bounding what a flood writes is the evidence sampling of 2d.
- **Deferred (PR #6 review).**
  - A link end whose receive failed keeps its socket open (reader removed) until every capture
    on the end has closed, because the transport is replaced only then. A CLI run ends at once,
    so it is harmless; the TUI and library (a long-lived session) should close the socket at the
    failure so the port is free and no kernel buffer fills behind a dead reader. Stage 3 (TUI),
    with the first long-lived consumer.
  - Nested bytes in the `args` or `tamper` of a ledger record (and of any record with a
    container-valued field) are not cut, charged or copied at creation: only top-level fields are
    scanned. No caller nests bytes today; 2d's ledger bounds revisit this when evidence sampling
    lands (a recursive walk would cost the hot path, so it needs a schema for which fields can
    carry bytes).
  - Ingest-only saturation knee: about 4% lower at 25k packets/s than on `main` (the per-frame
    byte charge in the consumer queue), within the host's run-to-run noise; the 1.5x guard and
    the 30000-of-30000-lines check at 10k packets/s hold. Revisit if a profile of 2d shows it.
- **Tests.** `tests/test_bounded_memory.py`: the edge for sizes 0..65507 and the whole corpus
  (Hypothesis over sizes and caps, buffer reuse), receive errors (transient, each fatal errno,
  the failing window, the count backstop, episode reset on a good read or a long gap, the fatal
  line being the last word, through `Listener` and `EventStream`), the queue (model-based Hypothesis
  over put/pop/take, held batch, closed with error), the ledger (cut at creation, buffer
  isolation, wide and strided memoryviews, byte bounds, Hypothesis over record sizes) and the
  flood end to end with exact accounting (`sent == received + kernel_dropped`, `offered == shown +
  not_shown`). Corpus: `packet-tlm-oversize-by-one.bin` (cap + 1 bytes, replayed through the UDP
  edge and the decoder).
- **Cost** (4-core dev host, best of several runs). `_FrameQueue.put` 160 -> 380 ns per frame;
  `Ledger.record` of a 200-byte raw packet record 0.95 -> 1.65 us, of a one-field state record
  0.83 -> 1.05 us. The ingest-only, `listen` path and end-to-end guards are unchanged within the
  host's run-to-run noise (the ingest-only saturation rate is about 70k packets/s, the listen
  path about 1.1-1.3x the bare decode and render against a 1.5x guard, 30000 of 30000 lines at
  10k packets/s).

### 2d - Ledger under floods, disk full, retention

Deliverables
- **Evidence sampling** per key (code, MID, stream) and 10 s window: every occurrence
  counted; the first 8 kept in full with raw bytes, then one sample per second; the rest
  summarised by one `rx.malformed.summary` record at window close (count, bytes,
  first/last time, first/last seq). A global raw-evidence budget
  (`evidence_raw_bytes_per_second`, default 256 KiB/s, token bucket) drops raw bytes, not
  records, beyond it. The key table holds at most 1024 keys per window: a new key past
  that folds into one `other` key until the window closes, and the table is cleared at
  each window close. Nothing is evicted mid-window, so no key's count is split across
  two entries. Malformed records carry the frame `seq`, so with a recording running the
  full evidence is one lookup away.
- **Tiers, by who controls the rate.** Every record belongs to one tier:
  - *essential*: a rate bounded by the user's own actions or by ironbird's fixed timers:
    actions, commands the user sends (`tx.command`), responder replies within the reply
    rate (below), state changes ironbird makes, errors not caused by received data
    (config, transport open and send, I/O, internal), `ledger.*`, `retention.*` and
    `prune.*` records, periodic summaries. Never sampled, never subject to an allowance.
  - *evidence*: a rate the remote side or the environment drives: packet records, `rx.*`
    state records (`rx.first` per MID), malformed samples and summaries, a responder's
    received commands (`rx.command`, sampled per command name exactly like malformed
    records: the commanding peer sets their rate), wire-origin decode errors (`IB-DEC-*`
    from received bytes), errors a received command causes (`IB-RSP-*`), receive-side
    transport errors (`IB-TX-003`, `IB-TX-004`).
  - *events*: received cFS events, never sampled (decision D3), in a tier of their own.
  The tier is an explicit parameter of `record` and `record_error` (default by kind); a
  test checks every record name in `src/` against a tier table, so a new record cannot
  land in the wrong tier unnoticed.
- **Responder replies.** A reply is a transmission, but the peer's commands set its rate.
  Its `tx.telemetry` record is essential while within a fixed reply rate (token bucket,
  10 records/s, burst 600; not configurable). Beyond it the reply is still sent and is
  counted into one essential `respond.replies` record per 10 s window (count, bytes,
  counts per reply message, first and last time and sequence), so every reply has a
  trace, individual or counted, and a peer commanding at 10k/s grows the essential tier
  by at most about 14 MiB/h (10/s at about 400 B per reply record) plus the summaries.
  For `IB-LED-004` and exit 6 a counted reply is logged once its `respond.replies` record
  is written.
- **Allowances (decision D2).** The event and evidence tiers each have an allowance that
  grows with the session's run time: `allowance(t) = bytes_per_hour x max(1 h, t)`, `t` on
  the monotonic clock since session start, compared with the tier's bytes on disk. The
  first hour's worth is a floor that absorbs bursts at start, quiet seconds bank credit
  for later bursts, and only a rate sustained above `bytes_per_hour` reaches the cap.
  - Over its allowance a tier is *counted only*: `Ledger.record` returns `None` and the
    record is counted (records and raw-field bytes, per tier).
  - Recovery has hysteresis: writing resumes once the tier's bytes are at most
    `allowance - h`, `h = min(max(1 MiB, 1 % of allowance), bytes_per_hour / 60)`, so it
    does not flap per record and never waits longer than about 60 s of the allowance's
    growth (without the cap, 1 % of a 12 h event allowance is about 7 min). The bound
    holds once the allowance is growing, after the first hour: during the first-hour
    floor the allowance is constant and the tier's bytes on disk never shrink, so a tier
    that reached the floor stays counted only until the hour ends (and then up to about
    60 s more). That is intended: the floor is the budget for the hour. The first
    record written after resuming is preceded by a gap marker, `ledger.tier_gap` (tier,
    records and bytes counted, first and last time).
  - One `IB-LED-003` per episode per tier, displayed at most once per 10 min per tier
    (later episodes fold into the next line's count, e.g. `(x26 in 10 min)`); every
    episode is ledgered.
  - While the ledger writes into its reserve or is stalled (below), both tiers are counted
    only, whatever their allowance. `"unlimited"` removes a tier's allowance, never that
    rule.
- **Record size (measured).** An event record is the `rx.event` record that
  `EventStream._pump` queues, raw packet included as spaced hex. Method: encode a cFS
  long-format event (`EVS_LONG_EVENT` of `examples/ccdd/cfs.json`, 168 B on the wire with
  the cFS v1 telemetry header) and format its `rx.event` fields through `Ledger._format`,
  counting the line plus its newline: 856 B with a short message, 947 B with the full
  122-character text. Sizing below uses 947 B. Short-format events are smaller. A command
  record (`tx.command`) is 293 B for 8 command bytes, 462 B for 64 and 1039 B for 256;
  about 500 B is typical.
  - *Worst case.* Text fields decode as ASCII with `errors="replace"` and the ledger writes
    `ensure_ascii`, so every byte of `AppName` (20) and `Message` (122) that is 0x80 or
    above becomes `�` and every control byte `\u00XX`: 6 B per byte instead of 1. An
    event with every text byte 0xFF (raw bytes all 0xFF) is about 1.67 KB, 1.76 x the
    full-text record (an estimate through `Ledger._format` with representative envelope
    values; the sizing guard measures it exactly). Real cFS text is printable ASCII, so
    the sizing below keeps 947 B; the allowance bounds the disk whatever the record size
    (worst-case 12 h totals are unchanged), and only the time to reach it shortens: 30/s
    of all-0xFF events (about 172 MiB/h) reaches 160 MiB/h after about 56 min and is then
    counted only, with `IB-LED-003`. The margin is kept: raising the allowance for
    hostile text would only let a hostile source write more.
- **Sizing** (defaults; MiB = 2^20 B; events at 947 B). Events: 160 MiB/h (1920 MiB =
  1.875 GiB at 12 h). Evidence: 32 MiB/h (384 MiB at 12 h). The event allowance is sized
  so that 30/s sustained never reaches it and a 100/s burst of 15 min (up to 16 min)
  from a cold start, followed by 30/s sustained, stays under it: 154.4 of 160 MiB at the
  end of the first hour, after which 30/s (97.5 MiB/h) stays below the allowance's growth.

  | Event rate | Bytes/h | 12 h | Against 160 MiB/h |
  |------------|---------|------|-------------------|
  | 10/s (average) | 32.5 MiB | 390 MiB | never reached |
  | about 12/s (the test profile below) | 38.2 MiB | 458 MiB | never reached |
  | 30/s (typical active), sustained | 97.5 MiB | 1.14 GiB | never reached |
  | 100/s (burst) | 325 MiB | - | reached only if sustained 29.5 min from a cold start, 23 min after a first hour at 30/s, 46 min after a first hour at 10/s, about 4.6 h after six hours at 10/s |
  | 1000/s (runaway) | 3.2 GiB | bounded at 1.875 GiB | reached after 3 min; counted only beyond 160 MiB per hour of run time |

  Evidence is near zero in a healthy run; under a flood, sampling and the raw budget bound
  it to about 1 MB/s, so it reaches the first-hour floor within a minute and is then
  counted, with a trickle of samples as the allowance grows.

  12 h totals for one session, ledger plus recording. Telemetry belongs in recordings, not
  the ledger: at 100 pkt/s x 200 B a FRAME record is 248 B (12 B envelope, 32 B fixed
  part, 200 B data, 8-byte alignment), plus about 0.25 KiB of CHUNK per second: 86 MiB/h,
  1.01 GiB per 12 h. Essential records add little (10 000 commands are about 5 MiB).

  | Case | Events | Evidence | Recording | Total |
  |------|--------|----------|-----------|-------|
  | Expected (test profile, healthy wire) | 458 MiB | about 0 | 1.01 GiB | 1.46 GiB |
  | Active (30/s sustained) | 1.14 GiB | about 0 | 1.01 GiB | 2.15 GiB |
  | Worst case (both tiers at their allowance) | 1.875 GiB | 384 MiB | 1.01 GiB | 3.26 GiB |

  Against `retention.max_total_bytes` (8 GiB, decision D2): an expected 12 h run (1.46
  GiB) or an active one (2.15 GiB) fits several times over, so the warning appears only
  after several overnight runs. `IB-RET-002` is then shown during the run (the projection, once
  per process) and at the start of later sessions, at most once per `warn_interval` (24 h)
  until the user prunes or raises the cap.
- **Writer.** The ledger writes through a raw descriptor (`os.open` and `os.write`, no
  buffered text file) on one writer thread fed by the drain task; formatting stays on the
  loop at idle priority, so a slow or hung disk never blocks the loop or capture.
  - The writer is a daemon thread. Every file system call of the ledger runs on it: the
    open steps, writes, syncs, rotation, reserve allocation and retirement, and the
    heartbeat (`os.utime` on the active part's fd), so the heartbeat stops when the writer
    hangs and a hung writer cannot be mistaken for a live, idle one.
  - The writer loops on partial writes and tracks the offset of the last complete line, so
    it always knows exactly what reached the file.
  - `fdatasync` runs at most once per second when something was written, and at rotation
    and close, so a deferred error (NFS reports ENOSPC or EIO at sync or close, not at
    write) surfaces within about a second; the records written since the last good sync
    are then counted as possibly lost. A transmission record among them counts as
    unlogged (`IB-LED-004`, exit 6), whether the error came at a periodic sync, at
    rotation or at close: `ib send` syncs at close, so a deferred error on its one
    `tx.command` record gives exit 6, never a silent 0.
  - A write or sync still outstanding after 5 s is a stall, handled like a failed one
    (hung NFS server). The thread is never abandoned mid-line while the process runs: when
    the call returns, its result is handled like any other.
  - At exit the main thread joins the writer for at most 5 s (the last write attempt
    below). A writer still blocked then is left to die with the process (daemon); every
    record it had not confirmed counts as not written, so the exit line and exit 6 report
    it, and a hung disk cannot hold the process past the bound.
- **Opening, and what refuses a transmission (decision D-b).** The ledger opens at command
  start: resolve the state directory, `mkdir` (0700), create part 1 with
  `O_WRONLY | O_CREAT | O_EXCL | O_APPEND | O_NOFOLLOW | O_CLOEXEC` (0600; `EEXIST` draws
  a new session id, three tries), take the lock, then write the first record,
  `ledger.open` (class `long` or `short`, command, ironbird version, pid, host). The steps
  run on the writer thread and the command waits for them at most 5 s; past that the
  open counts as a stall (transient, below), so a hung `mkdir` on a dead NFS server
  cannot hang the command.
  - *The lock* is `flock(LOCK_EX | LOCK_NB)`, retried for up to 1 s (every 20 ms) on
    `EWOULDBLOCK`: on a file just created `O_EXCL` only a checker probing liveness (which
    releases at once) or a reader can hold it, and a blocking `LOCK_EX` would hang behind
    a reader. Still refused after 1 s, or `ENOLCK`, `EINVAL` or `EOPNOTSUPP` (no lock
    support: NFS without a lock daemon, some FUSE file systems), and the session runs
    unlocked: never refused for it, one `IB-LED-009` warning (displayed at most once per
    `warn_interval` per directory, always ledgered with the errno), and its liveness
    rests on freshness alone (heartbeat, below). The unsupported-lock part of the rule
    (never refuse, warn once, fall back to freshness) applies to every lock 2d takes:
    reserves, the notices file, and retention's liveness test and prune's per-session
    lock (prune then relies on freshness and its `fstat` checks, as under the NFS caveat).
    The notices file is read and rewritten without its lock, so the cross-process rate
    limits keep working (Rate limits and the notices file, below). Reserve recovery is the
    exception: it rewrites files, so without locks it is not done.
  - A failure in any other step is one of the two classes below.
  - *Configuration* (`IB-LED-002`): the location is wrong and only the user can fix it.
    `default_state_dir()` refusals (relative `IRONBIRD_STATE_DIR`; `HOME` empty, relative,
    or unset with no passwd entry), and `OSError` with `EACCES`, `EPERM`, `EROFS`,
    `ENOTDIR`, `ENOENT`, `ELOOP`, `ENAMETOOLONG`, `EISDIR`, `EEXIST` on a directory of the
    path, or any errno not listed as transient. Transmitting commands (`send`, `respond`)
    refuse to run (exit 1, nothing sent), as in Stage 1; read-only commands run without a
    ledger. The Stage 1 fail-closed tests (a relative `IRONBIRD_STATE_DIR`, a path through
    a regular file: `ENOTDIR`) are configuration cases and pass unchanged.
  - *Transient*: `ENOSPC`, `EDQUOT`, `EIO`, `EMFILE`, `ENFILE`, `ENOMEM`, `ESTALE`,
    `ETIMEDOUT`, or a stall over 5 s: the location is right, the disk or the system is
    not. The session starts with the ledger stalled and transmissions follow `on_full`.
    **This changes Stage 1's contract**: Stage 1 refuses to send on any failure to create
    the ledger (README: "`send` and `respond` refuse to run (`IB-LED-002`, nothing sent)
    when it cannot be created"), while 2d, under the default `on_full = "continue"`, sends
    with a warning when the failure is transient (the README change is under Docs).
  - Once `ledger.open` is written, every failure (any errno, including `EACCES` on a new
    part after a permission change) is transient: the location has proven usable. A stall
    retry after a transient open failure repeats every open step from `mkdir` (the
    directory may have been removed meanwhile), and at a retry every errno is transient:
    the command is already running, so there is nothing left to refuse.
- **Disk full and the essential reserve (decision D-d).** The same whether tiers are
  capped or unlimited.
  - *Allocation.* Long sessions (class `long`: `listen`, `events`, `respond`, `record`,
    `replay`; the Stage 3 engine) create `<stamp>-<session>.r0001.reserve`
    (`O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC`, 0600) right after part 1, take
    the lock on it (the opening rule) for the session's life before writing to it, write a
    `ledger.reserve` header record at offset 0 (the one line a reserve always holds), then
    `posix_fallocate` it to `essential_reserve_bytes` (32 MiB). A reserve is never opened
    with `O_APPEND`: on Linux `pwrite` on an `O_APPEND` descriptor ignores its offset and
    appends past the allocated extent, which needs new blocks. `EEXIST` takes the next
    free reserve number (the naming rule under Rotation). The file system check below runs
    first: outside the allowlist, no file is created.
  - *Verification.* The file system must be one where `posix_fallocate` really reserves
    blocks, decided before the reserve is created by `f_type` from `fstatfs` on the
    directory fd (through `ctypes`; `f_type` is the first member of `struct statfs` on
    64-bit Linux, asserted at import) against an allowlist: ext2/3/4 (`0xEF53`), XFS
    (`0x58465342`), tmpfs (`0x01021994`). Anything else (btrfs, ZFS, overlayfs, NFS, CIFS,
    FUSE, an unknown type) or a `fstatfs` failure is not guaranteed, and no reserve is
    created (`IB-LED-006`): a file that cannot be trusted to hold its blocks would only
    hide the problem and use space. An allowlist fails safe where a list of bad types
    would miss a new one. The check is one `fstatfs`, repeated once a minute. On an
    allowed file system, after the allocation `fstat().st_blocks x 512` must cover the
    size; a failed or short allocation is truncated back to the header (shrinking needs
    no space) and retried once a minute. Each of these cases gives one `IB-LED-006`
    warning (displayed at most once per `warn_interval` per directory across processes,
    always ledgered): the session runs, but a full disk stalls the ledger at once. Short
    commands (`send`, `nodes`, `find`, ...) hold no reserve.
  - *Use.* A ledger write failing with `ENOSPC` or `EDQUOT` (whatever filled the disk:
    recordings, other sessions, other programs) truncates the active part back to its last
    complete line (shrinking needs no space, so no line is left torn short of a crash) and
    makes the reserve the active part: a `ledger.part` record naming the previous part and
    the reason, then the interrupted essential records, each written with `pwrite` at the
    offset after the last complete record, inside the allocated extent, so no new blocks
    are needed. Event and evidence tiers become counted only; one `IB-LED-005` warning per
    episode, displayed at most once per 10 min (later episodes fold into its count), every
    episode ledgered. At about 500 B per command record, 32 MiB holds about 65 000
    commands.
  - *Used up.* A record that would cross the end of the reserve is not written; the
    ledger stalls (below).
  - *Retirement.* A reserve stops being the active part when space returns (allocating
    the next reserve, `.r0002.reserve`, succeeds: tried by each stall retry, and once a
    minute while writing into the reserve) and at close. Retiring: `ftruncate` to the end
    of its last record, `fdatasync`, then a non-replacing rename (below) to the session's
    next part name `<stamp>-<session>.pNNNN.jsonl`. `EEXIST` there takes the next free
    part number (the naming rule under Rotation); when no number is free, or the rename
    fails otherwise, the file keeps its reserve name (cut and synced, so it holds only
    complete records), the failure is ledgered, and the next scan after the session ends
    retries it as a recovery. Writing then continues in a new part. An unused reserve
    retires at close holding the session's closing records, so a long session ends with
    one small final part made from it.
  - *Non-replacing rename.* Every rename 2d makes (retirement, recovery) and the temporary
    commit of 2g and 2h go through one helper, `rename_noreplace(dir_fd, old, new, fd)`,
    so an existing file is never replaced:
    1. `fstatat(dir_fd, old)` without following symlinks must match `fstat(fd)` (device
       and inode) of the open, locked descriptor: the name still denotes the file this
       process checked.
    2. `renameat2(dir_fd, old, dir_fd, new, RENAME_NOREPLACE)` through `ctypes` (Linux
       3.15, glibc 2.28); `EEXIST` is a collision. It is *unavailable* when the `ctypes`
       lookup of the symbol fails (`AttributeError`, an older glibc) or the call returns
       `ENOSYS`, `EOPNOTSUPP` or `EINVAL` (the file system does not take the flag).
    3. Unavailable, and the file is a regular file: `link` old to new relative to the
       directory fd without following symlinks. `EEXIST` is a collision, except that the
       `EEXIST` of an NFSv3 retransmission (the first `link` went through, its reply was
       lost) counts as success when `fstatat(new)` matches `fd`. `EPERM`, `EOPNOTSUPP`,
       `ENOSYS` and `EXDEV` mean hard links are unavailable here (FAT, some FUSE and
       network file systems, protected links): go to step 4. After a `link`, check that
       `fstatat(new)` and `fstatat(old)` both match `fd`, then unlink old. A crash between
       the two steps leaves two names on one inode, and recovery completes it (below).
    4. Last fallback for regular files, and the only path for directories (which cannot be
       hard linked): create a placeholder at the final name, then rename onto it. A
       regular file's placeholder is created `O_CREAT | O_EXCL | O_NOFOLLOW` (0600), a
       directory's with `mkdir`; `EEXIST` is a collision. Then `rename(old, new)` over the
       placeholder (a directory replaces only an empty directory: `ENOTEMPTY` or `EEXIST`
       is a collision). Any failure after the placeholder exists removes the placeholder
       this process created, after `fstatat` matches its own open descriptor (or, for a
       directory, `fstatat` matches what `mkdir` left). A crash between the two steps
       leaves an empty placeholder at the final name: for a ledger part, a harmless empty
       part that later part numbers skip and prune removes with its session; for an
       export or repair output, an existing OUT that the next run refuses without
       `--force`.
    5. On a collision nothing is changed and the caller decides: retirement takes the next
       number, recovery leaves the file and reports `IB-RET-004`, export and repair report
       that the output exists.
    6. After a committed rename (any path) the helper `fsync`s the directory fd, so the
       new name survives a crash. An `fsync` failure does not undo the rename; it is
       reported like a sync error (retirement: ledgered and handled as a write error;
       export and repair: `IB-EXP-001` or `IB-REC-002`).
  - *After a crash.* A leftover reserve of a non-live session (liveness below) is
    recovered by the next scan, never deleted:
    1. The scan takes `LOCK_EX | LOCK_NB` on the session's ledger part 1 and holds it
       through the session's recovery. That proves no writer holds the session (a writer
       holds part 1 for its life) and serialises two scans recovering the same session. A
       session without part 1, or where locks are not supported (the opening rule), is not
       recovered: left untouched and reported (`IB-RET-004`). `EWOULDBLOCK` on part 1 (a
       writer, or another scan recovering the session, holds it) is not a failure: the
       session is skipped this scan, nothing is reported, and the next scan retries.
    2. Its reserves are handled in `rNNNN` order. That order is the order they were
       allocated, which is not always the order they were written (a reserve whose
       retirement rename failed keeps its name while later parts are written), so the
       authoritative part order is the `ledger.part` chain, not the number: each part's
       opening record names the part before it, and `ledger.recovered` names the old and
       new file. Each reserve is opened
       `O_RDWR | O_NOFOLLOW | O_NONBLOCK | O_NOCTTY | O_CLOEXEC` (never `O_APPEND`),
       checked by `fstat` (regular, ours, at most 1 GiB) and locked `LOCK_EX | LOCK_NB`
       (`EWOULDBLOCK` here skips the session for this scan, as for part 1); its first line
       must be a `ledger.reserve` record of that session (strict JSON, at most 4 KiB).
    3. A reserve that shares its device and inode with a part of the same session is an
       interrupted rename (the `link` path of the helper): its reserve name is unlinked
       after `fstatat` matches, completing the rename.
    4. The content after the header must have one of two shapes: complete lines followed
       by at most one partial line (no newline, no NUL) and then only NUL bytes to the end
       of the file (written into the reserve, then a crash), or complete lines ending in a
       newline with no NUL (retired but not renamed). Records never contain NUL
       (`ensure_ascii`). The file is cut after its last newline, synced, and renamed
       (non-replacing) to the session's next part name, one more than the highest part
       number of the session in the listing.
    5. A valid prefix followed by damage (a good `ledger.reserve` header and complete lines,
       then a non-NUL byte after the first NUL, a NUL inside a line, or two partial lines)
       is salvaged: the header's following complete lines, up to the last newline before
       the first byte that breaks the layout, are copied to the session's next part name
       (`O_EXCL`, synced, directory synced), and the damaged file is kept byte-identical
       under the name `<reserve name>.damaged` by the non-replacing rename. That name is
       outside the retention patterns, so it is never counted, recovered again or pruned,
       and the user decides what to do with it. A prefix with no record after the header
       is not salvaged (treated as below). `ledger.recovered` records the bytes kept and
       the bytes left in the `.damaged` file, and `IB-RET-004` is shown once:
       `warning IB-RET-004: reserve 20261030T120000Z-0123456789ab.r0001.reserve was damaged: 812 KiB saved as part p0004, the rest kept as r0001.reserve.damaged; inspect or delete that file`.
    6. Anything else (another session's header, no valid header, larger than 1 GiB, not
       ours, a collision, a salvage that fails) is left untouched and reported
       (`IB-RET-004`) with a message that names the file and the action:
       `warning IB-RET-004: reserve 20261030T120000Z-0123456789ab.r0001.reserve not recovered (header is another session's); inspect it and move it away, or keep it; its session is protected`.
       Its session is protected (never proposed, reason `unrecoverable reserve`) so prune
       cannot delete the rest of a session whose records are still in an unrecovered
       file.
    `ledger.recovered` (old name, new name, bytes kept, bytes cut) goes to the recovering
    process's ledger; nothing is displayed for a recovery, since there is nothing to act
    on.
- **Retention warns; nothing is deleted automatically.** No ironbird code path deletes a
  ledger part, a reserve or a recording on its own. The one removal without a prune
  confirmation is a process's own temporary file on the failure path of the operation that
  created it, for later steps that write through a temporary name (2g repair, 2h export):
  a file this process created with `O_EXCL` as `.<final name>.<16 hex>.tmp` (16 hex from
  `os.urandom(8)`, in the final name's directory) and never renamed, removed by directory
  fd and name only after `fstatat` (no symlink following) matches the still-open
  descriptor's device and inode. 2d builds the two helpers both steps use: the
  temporary-file failure helper and the temporary commit (`fdatasync`, then
  `rename_noreplace` to the final name, or `os.replace` only when the caller passed
  `--force` and the final name is an existing regular file of ours). A temporary file
  left by a crash stays: its leading dot keeps it outside the retention patterns, so it
  is neither counted nor pruned. 2d itself writes no temporary file: the notices file is
  rewritten in place.
- **Stall (decisions D-b, D-c).** When an essential record cannot be written (reserve used
  up; no reserve; a transient open failure; any write or sync error; a stall over 5 s) the
  ledger is *stalled*: essential records stay queued (bounded by the queue rule), event
  and evidence tiers are counted only, and a retry runs every 5 s. Stage 1's `_fail()`
  (ledger disabled for good, `IB-LED-001`) becomes stall entry; only `IB-LED-002` leaves a
  process without a ledger, and `record` keeps returning record numbers while stalled
  (queued, written if a retry succeeds).
  - *Stall entry, long sessions (with or without commands):* one line per episode,
    `warning IB-LED-001: ledger not writable (No space left on device): records queued, retrying every 5 s; free space in <dir> or set IRONBIRD_STATE_DIR`,
    displayed at most once per 10 min (later episodes fold into its count); every episode
    is ledgered once writing resumes.
  - *Resume:* a successful retry writes `ledger.resumed`: stall start and duration;
    `delayed`, queued records now written (with their original timestamps); `lost`,
    records refused by the full queue or counted only during the stall; `possibly_lost`,
    records written after the last good sync when the error was deferred; `unlogged_sent`,
    transmissions whose record was delayed or possibly lost. In a long session one info
    line says so, displayed at most once per 10 min (later resumes fold into its count):
    `IB-LED-007: ledger writing again after 42 s; 1203 delayed records written, 17 lost`.
  - *At exit while stalled:* one last synchronous write attempt (the writer join, 5 s
    bound). If it fails, one line,
    `warning IB-LED-001: ledger stalled at exit; 1203 records not written`, and with
    `--verbose` the unwritten transmission records are printed to stderr as JSON lines, so
    their trace survives in the terminal or CI log. When transmissions are among them, the
    `IB-LED-004` summary below replaces this line (it names the stall and the count).
  - *Short commands* (class `short`: `send`, `nodes`, `find`, prune, ...) display at most
    one stall line (`IB-LED-001/004/007`), at exit, after the last write attempt (the
    start-of-session warnings `IB-LED-006/008/009` and `IB-RET-*` are separate lines, each
    rate-limited by the notices file): nothing when everything was
    written by then (the stall and its resume are ledgered, `ledger.resumed` counting the
    delayed records; nothing is left to act on), else exactly one line that folds the
    stall into it: the `IB-LED-004` line for a transmitting command, else the
    `IB-LED-001` exit line. No separate stall-entry or resume line is displayed.
  - *Transmissions* (commands and responder replies) follow `on_full`:
    - `"continue"` (default, Stage 1 behaviour): the transmission goes out and its record
      is queued; if a retry succeeds before exit it is written late (delayed, not lost).
      In a long session, one line per transmission, coalesced by code only (not by command
      or MID), each naming the latest:
      `warning IB-LED-004: SET_MODE sent to fc but not logged (ledger stalled) (x12 in 10 s)`.
      `ib send` prints its one line at exit, and only when the record is still unwritten:
      `warning IB-LED-004: NOOP sent to fc but not logged (ledger not writable: No space left on device); free space in <dir> or set IRONBIRD_STATE_DIR`.
      When more than one transmission is still unwritten at exit (or was possibly lost to
      a deferred sync or close error), one summary line, also on the signal path (printed
      before the 128+n exit, which still takes precedence):
      `warning IB-LED-004: 37 commands sent but not logged (first NOOP to fc at 12:00:01.123Z, last SET_MODE to fc at 12:04:10.456Z)`.
      The process exits 6 when at least one transmission's record was never written or
      was possibly lost. Separate `ib send` processes do not coalesce with each other:
      each sends one command and prints at most its own single line.
    - `"refuse_commands"` (opt-in, stricter): transmissions are refused, nothing sent.
      `ib send` prints one line folding in the stall,
      `error IB-LED-004: NOOP to fc refused: ledger not writable (No space left on device); free space in <dir> or set IRONBIRD_STATE_DIR`,
      and exits 1. A long session coalesces its refusals the same way as the sent
      warnings. `respond` stays up, counts the replies it withheld, prints one line at
      exit (`error IB-LED-004: 12 replies withheld: ledger not writable`) and exits 1.
  - Capture and recording never stop for ledger reasons; the recorder has its own ENOSPC
    path (`IB-REC-002`).
- **Exit code 6 (decision D12):** a transmission went out but its ledger record was never
  written, or was written after the last good sync and then possibly lost to a deferred
  sync or close error (6 covers both: never written, or possibly lost). Same tier as 4:
  an error (1, 2), 3 or a signal (128+n) takes precedence; between 4 and 6, 6 wins (the
  `IB-OUT-002` line still reports the lost output); between 5 (decision D8, damaged input recording) and 6, 6 wins (no Stage 2
  command both replays and transmits, D5, so this only fixes the order for later
  stages). The precedence of 3 over 6 means `ib respond --count N --timeout T` that times
  out exits 3 even with unlogged replies; the `IB-LED-004` summary line is printed either
  way, so a script that needs both checks stderr or the ledger. `respond` withholding
  replies under `"refuse_commands"` exits 1 (an error), above 3, 4, 5 and 6. 2d adds the
  row to the README's exit table.
- **Queue.** The in-memory queue follows the same rule: when full, the new record is dropped
  (`Ledger.record` returns `None`, counted and summarised by `ledger.dropped`), and a
  queued essential record is never evicted. Each tier has its own capped slice: evidence
  (packets, wire-origin errors and the other evidence kinds, via the explicit tier
  parameter of `record` and `record_error`) and events each get a slice, and the two
  caps sum to less than the queue size, so neither a wire flood nor an event storm can
  take the other's room or the essential room (by count, and by bytes as 2c bounds the
  queue). This is a code change in 2d: today `_EVIDENCE_KINDS` puts events and packets in
  one shared evidence slice. When the queue is full a new record is refused and counted,
  essential or not: this bounds memory, it is not a guarantee that every essential record
  is captured.
- **Summary record.** `ledger.summary` every 60 s when any tier counted, sampled or dropped
  records in that minute, and once at close: per-tier records and bytes written, counted
  only, sampled away and dropped.
- **Rotation**: at `max_file_bytes` the ledger continues in `<stamp>-<session>.pNNNN.jsonl`
  (created `O_EXCL`), each part opening with a `ledger.part` record naming the previous
  one. Part numbers increase through the session; a reserve takes the next number when it
  retires.
  - *Naming rule* (parts, reserves and retirement renames): the number is the next one
    after the highest this session has used. On `EEXIST` (a foreign file holds the name)
    the writer moves to the next number, ledgers `ledger.name_taken` (name, device and
    inode of the file found) and tries again, at most 16 numbers per attempt. When all 16
    are taken the attempt fails, and what that means depends on what was being created:
    for a rotation, the ledger is still writable, so it keeps writing the current part
    past `max_file_bytes` (one coded warning, `IB-LED-010`, displayed at most once per
    10 min, always ledgered) instead of stalling, and retries the rotation once a minute,
    continuing from the next number; for a reserve, no reserve is created (`IB-LED-006`),
    retried once a minute likewise; for a retirement rename, the file keeps its reserve
    name (above). A taken name is never retried with the same number, so `EEXIST` cannot
    loop.
  - Part order is the `ledger.part` chain, not the number alone: a reserve whose
    retirement rename failed keeps its reserve name while later parts are written, and
    takes a higher number when it is finally renamed. Each part opens with a record
    naming the part before it (for a reserve not yet renamed, by its reserve name).
  - Numbers are written with at least 4 digits (`p0001`) and widen past 9999 (`p10000`),
    up to 9 digits; a name with a leading zero beyond the 4-digit padding (`p00001`) is not
    canonical and does not match. At the default 32 MiB per part 9999 parts are about
    312 GiB, at the 1 MiB minimum about 10 GiB, so widening is reachable; 999 999 999 is
    not (about 950 TiB at 1 MiB per part). Should it be reached anyway, the ledger keeps
    writing the current part with `IB-LED-010` (reason `part numbers exhausted`) rather
    than reuse a name.
- **Names and scope.** Retention covers files matching the anchored name patterns, from
  `^[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}` then `(\.pN)?\.jsonl` or `\.rN\.reserve` in the state
  directory, and `(\.pN)?\.ibrec` in the recording directory (2e), `N` being
  `(?:[0-9]{4}|[1-9][0-9]{4,8})` (the naming rule above), with a stamp that is a valid UTC
  time (else ignored, counted as `unparseable`). Files
  group into sessions by session id. A file counts by its name and location, however it
  was created; a recording written elsewhere or under another name (`-o`) is outside
  retention entirely (not counted, warned about, projected or pruned; only
  `recording.min_free_bytes` guards its disk), and the ledger records its path.
- **Directory hygiene.** Retention (scan, recovery, prune) opens each directory once
  (`O_RDONLY | O_DIRECTORY | O_CLOEXEC`) and does everything relative to that fd: listing
  (`os.scandir(fd)`), `stat` without following symlinks, opens, renames and unlinks.
  - Each file is opened `O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_NOCTTY | O_CLOEXEC` (a FIFO
    or device with a matching name cannot block or become a terminal; recovery opens
    reserves `O_RDWR` with the same other flags) and checked with `fstat`: a regular file
    with `st_uid == getuid()`, else ignored and counted as `foreign`.
  - Hard links: bytes are counted once per (device, inode); a file with `st_nlink > 1` is
    never proposed (someone linked it elsewhere on purpose, and unlinking one name would
    free nothing).
  - A directory writable by group or others without the sticky bit is scanned but never
    pruned and its reserves never recovered (another user could swap names between check
    and unlink or rename): `IB-RET-006` with the remedy (`chmod +t DIR` or
    `chmod go-w DIR`) when prune is asked to act there; a scan only ledgers it.
- **Liveness, not by lock alone.** A session is *live* if any of these holds:
  - *a lock the checker cannot take*: the checker tries `LOCK_EX | LOCK_NB` on each of the
    session's part 1 files (ledger `.jsonl`, recording `.ibrec`) and reserves, releasing
    at once. Writers take `LOCK_EX` (the opening rule: non-blocking, retried for up to
    1 s) on part 1 and on the reserve right after the `O_EXCL` create, before the first
    write, and hold it for the session's life. Readers (replay, export, `rec info`,
    `rec check`; 2e to 2h) take `LOCK_SH | LOCK_NB` on part 1 of what they read; when a
    writer holds it, the session is live and protected anyway, and a follower retries once
    the writer is gone. Where locks are not supported (`ENOLCK`, `EINVAL`, `EOPNOTSUPP`),
    or the probe fails with `EBADF`, the test proves nothing, and liveness rests on
    freshness and incompleteness. The checker never probes its own session's files: it
    holds them itself (a probe through a second descriptor would be refused by its own
    lock, or fail), so its own session id is live by definition and skipped.
  - *freshness*: the newest mtime among its files is within 10 min of now, or in the
    future. Writers heartbeat: the ledger and the recorder (2e) `os.utime` their active
    file every 60 s when nothing was written meanwhile, so an idle live session stays
    fresh, and a just-created file is fresh, which covers the moment between create and
    lock. The heartbeat runs on the writer thread, so a writer hung on its disk stops
    heartbeating. A backward clock step (or a file from a host with a clock ahead) leaves
    future mtimes: such a session is live (reason `future mtime`) until the clock passes
    them. A forward step makes sessions look older; live ones still hold their locks and
    heartbeat, so only `IB-RET-001` can come early.
  - *incomplete*: it has no part 1 (possibly a live session whose part 1 lock is not
    visible here). Such a group is never proposed, only counted.
  - NFS caveat: on NFS a lock can be local to one host (mount option `local_lock`) or lost
    across a server restart, and mtimes come from the server's clock, so liveness there
    rests on the freshness window: a session whose host has been suspended for longer
    than the window can be proposed by prune. The README says to keep the state and
    recording directories on a local filesystem, or to prune only from the host that
    writes them.
  - Stage 1 processes: a Stage 1 `ib` still running after the upgrade takes no lock and
    writes no heartbeat, so once idle for longer than the freshness window (a `listen` on
    a quiet link) its session can be proposed. The README says to end or restart Stage 1
    processes before the first prune after upgrading.
- **Session class and protection.** A session's class comes from the first record of its
  ledger part 1, `ledger.open` (a bounded head read: at most 4 KiB, a strict JSON object
  whose `session` equals the name's id). Stage 1 ledgers start with `cli.invoke` and are
  classed by its `command`; an unreadable or invalid head counts as `long`, so it is
  protected rather than spent. Recordings are long. A session is *protected* (never
  proposed, left out of `IB-RET-001`) when it is live (including `future mtime`),
  incomplete, hard-linked or holds an unrecoverable reserve, or:
  - it is among the `keep_recent` most recent long sessions of its directory, or among
    the `keep_recent` most recent of any class: a run of short `ib send` invocations
    cannot push the last overnight run out of the kept set, and a directory of short
    sessions only still keeps its newest;
  - it is a short session that started while a protected long session ran (between that
    session's start stamp and its newest mtime): the `ib send` commands of an overnight
    run (decision D4) are kept with it;
  - for ledgers: its recording (same session id) is present in the recording directory
    and not itself proposed. Pruning the recording first releases the ledger.
- **Scan.** One `retention.scan` record per scan (per directory: sessions, bytes,
  protected bytes by reason, oldest; counts of foreign, unparseable, incomplete and
  hard-linked files and of recovered and unrecoverable reserves). Leftover reserves are
  recovered as part of it.
  - *Who scans*: a long session at start and then every `scan_interval` (10 min) while it
    runs (the projection needs other sessions' growth); a short command only when the last
    scan by any process is older than `scan_interval` (its time is kept in the notices
    file, below), so a script of a thousand `ib send` runs scans about once per 10 min.
  - *Cost*: in a worker thread, at most 100 000 directory entries per directory; head reads
    newest first and only as far as the protection rules and the candidates need, at most
    one 4 KiB read per session. Beyond the bound, or with a
    directory unreadable, the figures are partial: `IB-RET-004`, displayed at most once per
    `warn_interval` per directory.
- *Age warning* `IB-RET-001`: non-protected sessions whose newest file's mtime is older
  than `max_age`, so the warning names only what prune would propose. One line per
  directory, e.g.
  `warning IB-RET-001: 23 ledger sessions older than 7d (180 MiB; 40 MiB more protected); review with 'ib ledger prune'`.
- *Total warning* `IB-RET-002`, on both directories' pattern files: (a) at session start if
  usage is already past `max_total_bytes`; (b) in a long-running session, every 10 s, when
  `usage + rate x horizon` passes it, `rate` being this process's ledger and recording
  bytes/s over the last 60 s and `horizon` the remaining `--duration`, else 1 h; usage is
  re-scanned every `scan_interval`. Shown once per process when the projection first
  crosses and once when usage itself crosses. The line names the protected bytes and
  suggests the prune for the directory holding the most selectable bytes, with a target
  for that directory that would bring the combined total under the cap, e.g.
  `warning IB-RET-002: ledger 0.4 GiB + recordings 1.4 GiB (0.6 GiB protected), projected past 8 GiB within 1 h; run 'ib rec prune --until-under 1GiB' or raise retention.max_total_bytes`.
- *Rate limits and the notices file*: every check is ledgered in full; display goes
  through the coalescer, and the start-of-session warnings (`IB-RET-001`, `IB-RET-002`
  past the total at start, `IB-RET-004`, `IB-LED-006`, `IB-LED-008`, `IB-LED-009`) are
  displayed at most
  once per `warn_interval` per code and directory across processes. The times live in
  `retention-notices.json` in the state directory, with the last scan time:
  - opened `O_RDWR | O_CREAT | O_NOFOLLOW | O_NONBLOCK | O_NOCTTY | O_CLOEXEC` (0600) and
    checked with `fstat` (regular, ours, one link; else ignored and not written);
  - `flock(LOCK_EX | LOCK_NB)` on it, retried for at most 200 ms on `EWOULDBLOCK`; on
    timeout the warning is displayed (a lock never causes silence) and the file is not
    written. Where locks are unsupported (`ENOLCK`, `EINVAL`, `EOPNOTSUPP`; the same
    file systems that give `IB-LED-009`), the file is read and rewritten without the
    lock: a race between two processes can at worst repeat one warning or one scan, while
    the cross-process rate limits (`IB-LED-009` itself, `IB-LED-006`, `IB-RET-*`, the scan
    interval) keep working, so a thousand `ib send` runs on such a file system still
    show `IB-LED-009` once per `warn_interval`. "Displayed, file not written" applies
    only to the 200 ms timeout;
  - untrusted on read: at most 4 KiB, strict schema, timestamps not in the future;
    anything else is ignored and rewritten;
  - rewritten in place under the lock (`pwrite` at 0, then `ftruncate`), so no temporary
    file exists; a rewrite torn by a crash reads as invalid and is rewritten, at worst
    repeating one warning.
- `max_age = "unlimited"` or `max_total_bytes = "unlimited"` turns that warning off.
- **Prune: deletion only on explicit confirmation.** `ib ledger prune` and `ib rec prune`
  share `retention.py` (2d builds both; 2e only feeds the recorder's byte rate):
  `ib ledger prune [--older-than AGE] [--until-under SIZE] [--keep N] [--dry-run] [--yes] [--format line|json]`.
  - *Needs a ledger*: a deletion must leave a trace, so with `IB-LED-002` (configuration)
    prune refuses to delete (exit 1, nothing deleted), as transmitting commands do;
    `--dry-run` still lists. A stalled ledger (transient, such as the full disk prune is
    run to fix) does not refuse: the `prune.*` records are queued, the deletions usually
    free the space to write them, and the short-command exit line reports any left
    unwritten.
  - *Flags* take the config grammars below (`AGE` a duration, `SIZE` a size, `N` matching
    `^(0|[1-9][0-9]{0,3})$` up to 1000), never `"unlimited"`; anything else is
    `IB-CLI-001` (exit 2).
  - *Proposes*, in this command's directory only: non-protected sessions older than AGE
    (default `max_age`), plus with `--until-under`, oldest first until this directory's
    total would be at or under SIZE (`--until-under` is per directory; the combined cap is
    only for the warning). `--keep N` replaces `keep_recent` in the protection rules.
    Never a protected session or a file outside the name patterns. Nothing selectable
    (e.g. `max_age = "unlimited"` and no flag): says so, exits 0. Prune runs a scan first
    (reserve recovery included), whatever `scan_interval` says.
  - *Lists* every session it would delete: start (UTC), session id, parts, size, age,
    reason (`age` or `size`), then the total and one line with the protected bytes by
    reason. Names come from the directory and are sanitized before display.
  - *Unreachable target*: when deleting everything selectable still leaves the directory
    over SIZE, `warning IB-RET-005: cannot get recordings under 1 GiB: 1.4 GiB protected (live 0.2, future mtime 0.1, keep_recent 0.8, incomplete or hard-linked 0.3)`;
    it still proposes what it can, and exits 1 after acting (0 with `--dry-run`). Every
    protection reason with bytes is named, `future mtime` (clock steps) and
    `unrecoverable reserve` included, so the user sees why the target is out of reach.
  - *Confirms*: interactive (stdin and stderr are terminals) asks
    `delete 14 sessions (310 MiB)? [y/N]`; only `y` or `yes` (any case) confirms;
    anything else or EOF declines (nothing deleted, exit 0). Non-interactive needs
    `--yes`; without it the list is printed, then `IB-CLI-001` (exit 2), nothing deleted.
    `--dry-run` lists and exits 0.
  - *Deletes exactly what was listed*: per session, take `LOCK_EX | LOCK_NB` on part 1,
    check every file still matches the listing (device, inode, size, mtime, one link,
    owner) with `fstat` relative to the directory fd and no symlink following, then
    `unlink` relative to that fd, higher parts first and part 1 last under the lock. A
    session that became live or changed, or a file that fails to unlink, is skipped and
    reported (`IB-RET-003`, coalesced; exit 1). `ENOENT` means already gone: reported in
    the totals, not an error.
  - *Ledgered*: `prune.plan` (the full list), `prune.confirmed` or `prune.declined`, one
    `prune.delete` per file, `prune.gone` and `prune.skip` with their reasons.
- **Docs.** README: exit code 6; `IB-LED-001` now means stalled, not disabled; `[ledger]`
  and `[retention]`; prune; the NFS caveat; the Stage 1 processes note (Liveness). The
  fail-closed sentence changes, since transient open failures no longer refuse. Stage 1:
  "`send` and `respond` refuse to run (`IB-LED-002`, nothing sent) when it cannot be
  created." 2d: "`send` and `respond` refuse to run (`IB-LED-002`, nothing sent) when the
  state directory is unusable: a relative path, no home, no permission, a read-only file
  system, a path through a file. When the directory is usable but the disk or the system
  fails (disk or quota full, I/O error, out of descriptors, a hung file server), they
  send and warn (`IB-LED-004`, exit 6 if the record is never written) under the default
  `on_full = "continue"`, or refuse (exit 1) under `on_full = "refuse_commands"`." The
  ledger paragraph's "unless the ledger fails (`IB-LED-001`)" becomes "unless the ledger
  is still stalled at exit (`IB-LED-001`)".
- Config `[ledger]`: `event_bytes_per_hour` (160 MiB), `evidence_bytes_per_hour` (32 MiB),
  `evidence_raw_bytes_per_second` (256 KiB), `essential_reserve_bytes` (32 MiB),
  `max_file_bytes` (32 MiB), `on_full = "continue" | "refuse_commands"` (default
  `"continue"`: transmissions are sent with a coded warning when the ledger cannot record
  them; `"refuse_commands"` refuses them instead).
  Config `[retention]`: `max_age` (`"7d"`), `max_total_bytes` (`"8GiB"`), `keep_recent`
  (10), `warn_interval` (`"24h"`), `scan_interval` (`"10min"`).
  - Sizes are TOML integers (bytes) or strings matching
    `^[1-9][0-9]{0,11}(B|KiB|MiB|GiB|TiB)$`; durations are strings matching
    `^[1-9][0-9]{0,5}(s|min|h|d)$`; `keep_recent` is a TOML integer. Booleans (`true` is
    an `int` in Python, so it is rejected explicitly), floats, other units (`"64MB"`),
    leading zeros, signs and whitespace are `IB-CFG-003`, naming the key and its accepted
    forms. The key names carry the rate unit; values never do.
  - The exact lowercase string `"unlimited"` is the one spelling of "no limit", accepted
    by exactly four keys: `event_bytes_per_hour`, `evidence_bytes_per_hour`, `max_age`
    and `max_total_bytes`. Zero, negatives, `"inf"`, `"none"`, `"Unlimited"` and any other
    spelling are `IB-CFG-003`. `evidence_bytes_per_hour = "unlimited"` lets a flood of
    malformed packets fill the disk, so loading it gives one `IB-LED-008` warning
    (displayed at most once per `warn_interval`, always ledgered).
  - Ranges: `event_bytes_per_hour` and `evidence_bytes_per_hour` 1 MiB to 1 TiB,
    `evidence_raw_bytes_per_second` 1 KiB to 64 MiB, `essential_reserve_bytes` and
    `max_file_bytes` 1 MiB to 1 GiB, `max_total_bytes` 1 MiB to 1024 TiB, `keep_recent` 0
    to 1000, `max_age` 1 min to 100 000 d, `warn_interval` 1 min to 30 d,
    `scan_interval` 1 min to 1 d.

Verification
- Test mechanics, shared by the cases below:
  - *Fault seam*: every file system call of the ledger and of retention (open, write,
    pwrite, fdatasync, fsync of a directory, ftruncate, flock, posix_fallocate, fstat,
    fstatfs, utime, mkdir, `rename_noreplace` and its steps (the `renameat2` lookup and
    call, link, placeholder create, rename), unlink, rmdir) goes through one injectable
    object
    that the writer and retention receive at construction; production passes the real
    one. In-process tests (the Session API, `CliRunner`) inject a scripted fake; subprocess
    tests load the fake through a test-only wrapper module under `tests/support/` that
    installs it before calling the CLI entry point. `src/` reads no environment variable
    or file to enable faults.
  - *Barrier*: a hung write or sync is a seam call blocked on a `threading.Event`, released
    by the test; the 5 s thresholds read an injected monotonic clock, so a stall is
    reached by advancing the clock, not by sleeping, and the blocked call returns when the
    test releases it.
  - *Retry*: a stall retry after a transient open failure repeats every step from `mkdir`;
    a test removes the state directory during the stall and checks the retry recreates it.
- Sampler unit tests with an injected clock (counts exact, samples where expected,
  summary per window); 1025 distinct keys in one window: the last one folds into `other`,
  counts stay exact, the table is empty after the window closes.
- Record sizes in every sizing test are measured from the real formatter, never a
  constant: event records are built by encoding `EVS_LONG_EVENT` with the example CCDD
  (short and full-length text, and text bytes all 0xFF) and formatting the `rx.event`
  fields through `Ledger._format`. A sizing guard asserts that the documented scenarios
  stay under the default allowance with the measured maximum of the ASCII cases, and
  fails if that maximum leaves 856-947 B, or the all-0xFF record leaves about 1.67 KB, by
  more than 5 %, so the tables and the worst-case figures above are re-derived whenever
  the record changes. It also asserts the all-0xFF case at 30/s reaches the allowance
  later than 50 min (the worst-case note above).
- 12 h event-rate arithmetic, time-compressed: the tier allowance as a pure object with an
  injected clock, fed a synthetic 12 h profile (each hour: 45 min at about 1/s with idle
  seconds, 12 min at 30/s, 3 min at 100/s; about 12/s average), 30/s for 12 h, and a run
  that opens with a 15 min burst at 100/s followed by 30/s sustained: never counted only.
  A runaway at 1000/s reaches the allowance at the predicted time within 1 %, then
  produces episodes with `ledger.tier_gap` markers and at most one displayed
  `IB-LED-003` per 10 min. Under 1 s, no I/O. The real `Ledger` with the same injected
  clock, rates and allowance scaled 1:64 and record bytes real (about 8 MB on disk): no
  `IB-LED-003`, on-disk tier bytes within the allowance at every simulated minute.
- E2E garbage flood at 10k pkt/s for 30 s of unique MIDs: ledger growth stays within the
  evidence allowance, notices coalesce, every datagram is counted; an event storm and a
  wire-error storm alone never refuse a command.
- Disk full through the writer's write seam (fault injection of ENOSPC and EDQUOT; no
  privileges needed), each case also with both tiers `"unlimited"`:
  - with a reserve: the active part is cut back to its last complete line (also after a
    partial write), the reserve becomes the active part with a `ledger.part` record, one
    `IB-LED-005`, essential records continue (the seam fails every write except
    `pwrite` into the reserve), every line parses;
  - reserve used up, default `on_full = "continue"`, in a long session: one `IB-LED-001`
    per episode, sends go out with coalesced `IB-LED-004` lines, and when the seam
    recovers one `IB-LED-007` and `ledger.resumed` counting the delayed records;
  - ledger stalled, `ib send` (short): it still reaches the wire (the mock responder
    counts it), its stderr holds exactly one stall line, the `IB-LED-004` line naming the
    stall's cause and remedy (no `IB-LED-001` line), and it exits 6; when the seam
    recovers before exit instead, the record is written late, `ledger.resumed` counts it
    as delayed, no ledger line is displayed, exit 0;
  - in one process (the Session API with an injected clock), 50 transmissions produce a
    bounded number of `IB-LED-004` lines, each naming the latest command, then one
    summary with the count and the first and last command; separate `ib send` processes
    print one line each and exit 6 each;
  - `on_full = "refuse_commands"`: one `IB-LED-004` line, `ib send` exits 1 with nothing
    on the wire (the responder counts zero); the seam recovers: sends accepted; `respond`
    under a stall withholds replies, prints one count line at exit and exits 1;
  - deferred ENOSPC or EIO at `fdatasync`: the records since the last good sync are
    counted as possibly lost; when a `tx.command` is among them (at a periodic sync, at
    rotation, and at close for `ib send`), `IB-LED-004` and exit 6; the same at `close`;
  - a write that blocks (barrier) past 5 s of injected clock: stall, the loop keeps
    running (the mock responder's telemetry is still counted) and `ib send` sends with
    the warning; a writer still blocked at exit: the process exits within the 5 s join
    bound, with the exit line and exit 6;
  - the open boundary, table-driven per errno at each open step (`mkdir`, create, lock,
    first write): configuration errnos refuse (exit 1, nothing on the wire), transient
    ones send and exit 6 when never written, an open step blocked past 5 s is transient;
    the Stage 1 fail-closed tests pass unchanged;
  - locks: `EWOULDBLOCK` on the lock for 0.5 s (a probing checker) is retried and the
    session locks; `EWOULDBLOCK` past 1 s and `ENOLCK`, `EINVAL`, `EOPNOTSUPP` run
    unlocked with one `IB-LED-009` within `warn_interval`, `ib send` still sends, and a
    scan of such a session judges liveness by freshness alone; `EBADF` from a liveness
    probe (seam) proves nothing and the scan never probes its own session id;
  - no-lock file system (seam: every `flock` fails `ENOLCK`, including on the notices
    file): 1000 `ib send` runs, sequential and 8 in parallel, show `IB-LED-009` once
    per `warn_interval` (and `IB-LED-006` likewise), scan once per `scan_interval`
    (a race may repeat one line, never one per run), all sends reach the wire; only a
    200 ms `EWOULDBLOCK` timeout on the notices file (a lock held by a subprocess)
    displays the warning and leaves the file unwritten;
  - hysteresis: a tier over its allowance during the first hour stays counted only until
    the hour ends, then resumes within about 60 s of the allowance's growth (injected
    clock);
  - a stalled `listen` with no command shows `IB-LED-001` once per episode and the exit
    line when still stalled at exit; a signal while transmissions are unwritten prints the
    `IB-LED-004` summary and exits 128+n;
  - `IB-LED-005` and `IB-LED-007` episodes repeated within 10 min are displayed once, with
    the later episodes folded into the count, and every episode is ledgered;
  - names taken: a foreign file at the next part name and at the next reserve name: the
    writer takes the next number and ledgers `ledger.name_taken`; 16 taken names in a
    row: for a part, no stall - the current part keeps growing past `max_file_bytes`
    with one `IB-LED-010`, and a retry once a minute rotates past them; `IB-LED-006` for
    a reserve, and the next retry continues past them; rotation from `p9999` writes
    `p10000`, which the scan matches and orders after `p9999`.
- Reserve allocation: an `f_type` outside the allowlist (btrfs, NFS, overlayfs, an unknown
  value) and a failing `fstatfs` from the seam: one `IB-LED-006` within `warn_interval`,
  no reserve file created (the seam sees no create and the directory listing is
  unchanged), the check retried once a minute. Short `st_blocks` (seam) and
  `posix_fallocate` failing with ENOSPC or EOPNOTSUPP on an allowed type: one
  `IB-LED-006` within `warn_interval`, the file cut back to its header, retried once a
  minute. The reserve's open flags (seam) hold
  `O_RDWR` and never `O_APPEND`, and a `pwrite` into a real reserve lands at its offset,
  leaving `st_size` and `st_blocks` unchanged.
- Crash recovery: `kill -9` a long session (subprocess) while writing into its reserve and
  while not: the next scan, once the freshness window has passed (injected clock), cuts
  the reserve at its last complete record and renames it to the next part; every line of
  the result parses; `ledger.recovered` is in the recovering ledger. Two reserves of one
  session are recovered in `rNNNN` order into consecutive parts. `rename_noreplace`,
  table-driven over the seam: `renameat2` available; unavailable by a missing symbol
  (`AttributeError`), `ENOSYS`, `EOPNOTSUPP` and `EINVAL`, so the link-then-unlink path
  runs; link unavailable by `EPERM`, `EOPNOTSUPP`, `ENOSYS` and `EXDEV`, so the
  placeholder path runs, for a file and for a directory (the only path for one); a
  collision in each path (`EEXIST` from `renameat2`, from `link`, from the placeholder
  create; `ENOTEMPTY` from the rename onto a placeholder) changes nothing, and a
  placeholder this process created is removed on a failure; an NFSv3 `link` retransmit
  (`EEXIST` with `fstatat(new)` matching `fd`) succeeds; the directory is `fsync`ed after
  each commit, and an `fsync` failure is reported without undoing the rename. A crash
  between link and unlink (seam) is completed by the next scan, leaving one name; a crash
  after the placeholder leaves an empty part that the next number skips.
  Two scans racing on one session (two subprocesses released by a barrier) recover it
  once, and `EWOULDBLOCK` on part 1 skips the session without `IB-RET-004` and the next
  scan recovers it. A valid prefix followed by damage (a non-NUL byte after the first
  NUL, a NUL inside a line, two partial lines) is salvaged: the prefix lines parse in the
  next part, the original is kept byte-identical as `.damaged` (outside the retention
  patterns), one `IB-RET-004` names both files, and a second scan does nothing. Left
  untouched and reported (`IB-RET-004`) with a message naming the file and the action,
  the session then protected: a foreign file already at the next part name (it stays
  byte-identical), a header with no records and damage after it, a session without part
  1, locks unsupported (seam).
  Left untouched: a live session's reserve (lock held by a subprocess), a fresh one
  (mtime now, no lock), a symlink, a foreign owner (through the `fstat` seam), a lookalike
  name, another session's header, a file over 1 GiB (sparse). Retirement in a live
  session with a foreign file at the next part name takes the following number.
- Liveness: lock held; no visible lock but a fresh mtime; a future mtime (reported as
  `future mtime` by `IB-RET-005`); an idle session kept fresh by its heartbeat (injected
  clock); a writer blocked on the barrier stops heartbeating; a group without part 1
  never proposed.
- Responder under a command flood (mock peer at 1k commands/s for 30 s): `rx.command`
  sampled per command name with exact counts in the summaries; reply records written
  individually up to the reply rate and counted in `respond.replies` beyond it (sum of
  individual and counted replies equals replies sent); the essential tier grows by at
  most the reply rate's bound.
- Queue slices: an event storm and a garbage flood together, with the drain paused, fill
  their own slices only; essential records are still accepted.
- No automatic deletion: state and recording directories with sessions past `max_age`
  and past `max_total_bytes`; start-up checks, a long session whose projection crosses,
  and three consecutive processes leave every file present and byte-identical (hashes),
  apart from reserve recovery (cut and renamed, or salvaged and renamed to `.damaged`,
  content otherwise identical);
  `IB-RET-001` and `IB-RET-002` are displayed once within `warn_interval` and ledgered
  every run. A static guard (AST scan of `src/`, import aliases resolved: `import os as
  o`, `from os import unlink as u`) allows:
  - file removal (`os.unlink`, `os.remove`, `os.rmdir`, `os.removedirs`, `Path.unlink`,
    `Path.rmdir`, `shutil.rmtree`) only in the prune delete path, the temporary-file
    failure helper, the second step of `rename_noreplace` (the `link` path's unlink and the
    placeholder cleanup's unlink or rmdir) and the interrupted-rename completion of
    recovery;
  - renames and links (`os.rename`, `os.renames`, `os.replace`, `os.link`, `Path.rename`,
    `Path.replace`, `Path.hardlink_to`, `shutil.move`) only in `rename_noreplace` and the
    temporary commit (the one `os.replace`, behind `--force`);
  - truncation (`os.truncate`, `os.ftruncate`, `O_TRUNC`, opens with a `"w"` mode,
    `Path.write_text`, `Path.write_bytes`) only in the line cut-back, reserve cut and
    notices rewrite helpers;
  - `ctypes` only in `rename_noreplace` (`renameat2`) and the `fstatfs` helper, and
    `tempfile` deleting helpers (`NamedTemporaryFile`, `TemporaryDirectory`) nowhere in
    `src/`.
  Its known gaps, reviewed by hand instead: dynamic access (`getattr(os, name)`, a
  function passed in as a value), C extensions and third-party code (pyserial in 2k),
  and child processes (`src/` runs none that touch files). A change that adds any of
  these to `src/` names it in its review.
- Prune with `IB-LED-002` (state directory through a regular file; `ib rec prune` with a
  usable recording directory): refuses, exit 1, nothing deleted; `--dry-run` lists.
  With the ledger stalled on ENOSPC (seam): deletes as confirmed and the `prune.*` records
  are written once space is freed.
- Prune: the list is exactly the expected set (age, `--until-under`, `--keep`, default
  `keep_recent` 10); protection: 30 short sends after one overnight long session keep the
  long session, a short session inside a protected long session's span is kept, a ledger
  whose recording is present is kept; never a live session (lock held by a subprocess), a
  symlink, a foreign file, a hard-linked file (counted once), a FIFO with a matching name
  (no blocking) or a lookalike (`.jsonl.bak`, uppercase hex, 13-digit id, an invalid
  stamp, a directory with a matching name); a group-writable directory without the sticky
  bit is refused with `IB-RET-006`; `--until-under` acts per directory and an unreachable
  target gives `IB-RET-005` with protected bytes and exit 1; interactive `y`, `N`, EOF
  and `yes ` through a pty; non-interactive without `--yes` exits 2 with nothing deleted;
  `--yes` deletes exactly the listed set; a file removed between listing and deletion is
  "already gone" (exit 0); a session that changes between listing and deletion (grown,
  replaced by a symlink, inode swapped, became live) is skipped with `IB-RET-003` and exit
  1; every step ledgered.
- Scan cost: short commands within `scan_interval` do not scan, and two processes racing
  on the notices file scan once; a 100 000-entry directory stays within the bound and
  `IB-RET-004` is displayed once within `warn_interval`.
- Config: `"unlimited"` accepted by exactly the four keys; `0`, `-1`, `true`, `1.5`,
  `1e6`, `"64MB"`, `"010MiB"`, `"inf"`, `"none"`, `"Unlimited"`, `" unlimited"`, `""`
  rejected with `IB-CFG-003`; range edges; unlimited tiers are never counted only (until
  the disk fills); unlimited age and total emit no warnings; unlimited evidence gives one
  `IB-LED-008` within `warn_interval`; prune flags with the same bad values exit 2.
- Untrusted input: corpus `ret-*` of directory names (control characters, escape
  sequences, overlong, invalid UTF-8) shown inert by prune and the warnings; malformed
  `retention-notices.json` (oversize, wrong types, future timestamps, torn) ignored and
  rewritten; `led-head-*` (malformed `ledger.open` heads: oversize line, not an object,
  other session) classed `long`; `led-reserve-*` (NUL mid-line, no newline, oversize first
  line, other session's header, a non-NUL byte after the NUL run, two partial lines)
  recovered when valid, salvaged when a valid prefix is followed by damage, else left
  untouched and its session protected; export `OUT` equal to the input, and `OUT` named
  like a ledger or recording file in the state or recording directory (through a
  symlinked directory, a relative path, a `..` path), refused even with `--force`.

Deferred from the 2d plan review (each revisited at the step named)
- Coalescing `IB-LED-004` across separate `ib send` processes: one line per process by
  design; the Stage 3 engine, one process sending many commands, coalesces them.
- State or recording directories on NFS and other network filesystems: best effort
  through the freshness window, with the README caveat; revisit when a deployment needs
  them.
- A guaranteed reserve on copy-on-write filesystems (btrfs, ZFS): warned (`IB-LED-006`),
  not solved; revisit with the Stage 3 engine.
- Finding a record by `n` across parts and reading ledgers back (`ib ledger show`):
  Stage 3, with the TUI details pane.
- Re-writing possibly lost records after a deferred sync error (keeping the lines since
  the last good sync to write them again): 2d counts them, and transmissions among them
  give `IB-LED-004` and exit 6; revisit with the Stage 3 engine if deferred errors show up
  in practice.
- A reserve guaranteed on file systems outside the `f_type` allowlist that do honour
  `posix_fallocate` (f2fs, others): added to the allowlist when a deployment needs one and
  a test shows `st_blocks` holds under a full disk.
- A temporary file of 2g or 2h left by a crash: kept, outside retention (leading dot),
  since removing it on a later run would be an automatic deletion; no cleanup command in
  Stage 2, revisit on request.
- Exit-code precedence of 3 over 6: kept (Stage 1 orders 3 above 4, and 6 shares 4's
  tier); the `IB-LED-004` line is printed either way. Revisit if scripts need 6 to win.

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
- Disk use (D2): recordings are never deleted automatically. Default-named recordings in
  the recording directory count toward `retention.max_total_bytes` and are removed only by
  `ib rec prune` (2d); the recorder feeds its byte rate into the `IB-RET-002` projection. A
  split recording keeps part 1 open and locked until the session ends, so retention sees
  the whole session as live. The recorder takes its lock before the first write and
  heartbeats its active file every 60 s while idle (2d liveness).

Untrusted input: the reader is a decode boundary of its own and gets the full treatment.
It validates before it builds a `Frame`, because `Frame` asserts its invariants rather than
checking them: `orig_len >= len(data)` and `TRUNCATED` iff `orig_len > len(data)`, and
`len(data)` against the `max_packet_bytes` the recording declares (the `oversize_error`
assertion in the codec fires on a lying `orig_len`), the flag-combination rules, and every
stream id against the stream table (`Engine._labels[frame.stream.id]` is indexed by it when
a frame is rejected). A record that fails any of these is damage (`IB-REC-*`), never a
`Frame`. Its frames carry no `checked` token (the field is `None`), so replay decodes through
the whole `Codec.decode`; the reader never copies a token from anywhere. Tests:
Hypothesis round trip (arbitrary frame sequences written then read are equal),
truncation at every offset (yields a prefix, reports torn tail at a record boundary,
never raises), bit flips (never yields a frame that was not written), lying `body_len`
both ways, undefined stream ids, bad BLOB hashes, oversize META, and forged records: a
frame whose data is a complete, well-formed record (even with a correct CRC for another
nonce) never ends damage and is never yielded as a record. Corpus
`tests/corpus/malformed/rec-*`; an `atheris` target for the reader.

Verification
- Unit: struct layouts, chunk closing by size and time, GAP on overrun (slow writer
  injected), fsync policy calls (mocked), signal-path close.
- Crash test: `kill -9` the recorder mid-capture; `ib rec info` reports a torn tail and
  every frame flushed before the kill is readable.
- Retention: with a fake clock, a recording whose rate crosses the projected total raises
  `IB-RET-002` once, at the predicted time; a split recording is skipped whole by
  `ib rec prune` while running.
- Benchmark: writer throughput (MB/s, frames/s) and per-frame `append` cost.
- Real cFS: record 60 s after TO_LAB enable; `rec info` shows each HK MID at its
  SCH_LAB period (`tools/cfs/NOTES.md`: ES 4.0 s, EVS 4.2 s, ...) within one packet, seq
  contiguous, no gaps, clean close.

As built (where the build differs from, or settles, the text above)
- Modules: `recording/format.py` (layouts, limits, pure record codecs), `reader.py`, `writer.py`,
  `inspect.py` (`scan` for `check`, `info`, `repair`), `render.py` (info text and JSON),
  `session.py` (location, embedded definitions, META, `recording_for`); `atomicfile.py` holds the
  non-replacing temporary commit (see below). The reader's checks live in `RecordingReader`;
  `recutil`/`rec_corpus` build crafted files for the tests, `tools/fuzz/fuzz_recording.py` is the
  `atheris` target (added to the nightly fuzz job; atheris is not installed on the dev host, so
  the target has not been run here).
- Layout details the plan left open. Chunk 0 starts at offset 24, so META, BLOBs and the declared
  STREAMs are the head of the first chunk. Non-frame records (STREAM, SOURCE, GAP, MARK) only appear
  at the start of a chunk: one needed while a chunk holds frames closes the chunk first, which is
  what lets `info` list gaps, marks and streams from chunk heads without reading frames. STREAM and
  SOURCE definitions are repeated every 60 s at a chunk start, so one damaged definition cannot make
  the rest of a stream unreadable (the reader requires every frame's stream and source to be
  defined, and a redefinition must be identical). Bodies: STREAM `u16 id, u8 kind (1 command,
  2 telemetry), u8 direction (1 rx, 2 tx)`, then u8-prefixed link, node and transport and a
  u16-prefixed endpoint; link and node follow the config name pattern, BLOB node and adapter the
  config identifier pattern (anything else is damage); BLOB `u8 kind, u8-prefixed node, u8-prefixed
  adapter, 32-byte SHA-256, content`; SOURCE `u16 id (below 4096) + UTF-8`, frames use 0xFFFF for
  "(other)"; GAP is a fixed 48-byte body `u16 stream, u8 reason, 0, u32 0, u64 lost, first_ns,
  last_ns, first_seq, last_seq`; MARK `u64 time_ns, u8 kind, 3 zero bytes, text`; CHUNK `bytes` is
  the file bytes of the chunk's FRAME records. TRAILER `index_offset` 0 means no index (more than
  2 M chunks); the trailer is the last record and bytes after it are damage (`trailing`).
- Stricter than the text: a TRUNCATED frame needs `orig_len` above both `len(data)` and META
  `max_packet_bytes` (the codec's `oversize_error` assertion), an unknown GAP reason or MARK
  reserved byte is damage, and the envelope `flags`/reserved bytes are checked before the CRC.
- Resync scans for the envelope shape with a regex over 1 MiB windows, at 8-byte aligned offsets,
  and validates a candidate plus its successor with the state (streams, sources, sequence) a
  real read would have. One work budget per pass (4 MiB plus 4 times the file size, charged for
  bytes read and CRC-checked and 64 per candidate looked at, never reset by a new damaged stretch)
  bounds hostile files; a record whose declared length runs past the end of the file is refused
  without reading it (a torn tail), and a scan that exhausts the budget ends the file as mid-file
  damage "the scan gave up", not as a torn tail.
  A reader takes `LOCK_SH | LOCK_NB` on the file; a writer's `LOCK_EX` shows as `writer_active`
  (`info` reports "recording" rather than torn).
- GAP reasons emitted: `kernel_drop` (the engine bracket: `first_seq`/`last_seq` are the engine's
  sequence numbers at the previous and the current 1 Hz poll, since a dropped datagram never had
  one; delivered to sinks that have a `gap` method, the new optional `GapSink` protocol),
  `recorder_overrun` and `not_recorded` (seq ranges of the frames not written, per stream).
  `queue_drop` and `link_down` are defined for 2k; a consumer queue overflow is not a recording gap
  because the sinks already have the frame.
- Writer: `Recorder.open` writes preamble, META, BLOBs and the declared streams synchronously and
  syncs file and directory before returning; the 250 ms flush, the 1 s chunk close, the end of an
  overrun (hysteresis at half the cap), the 10 s free-space projection (`IB-REC-010` warning under
  10 minutes) and the rate samples run on a loop timer; `fdatasync` and the 60 s heartbeat
  (`os.utime` of the active file) run on the writer thread, which holds the shutdown signals
  blocked. The close always syncs, even with `fsync = "never"`. A wall-clock step of 250 ms or more
  against the monotonic clock is a MARK. `Recorder.rate_bytes_per_s()` (bytes encoded over the last
  minute) is the hook for 2d's `IB-RET-002` projection; nothing consumes it yet. A split keeps part
  1 open and locked, and every part repeats META, BLOBs and definitions. A second signal does not
  abandon the close (the CLI ignores repeated signals); the close gives up only when the writer makes no
  progress for 5 s, writes the GAP for dropped frames and the trailer first when the disk is
  merely slow, and says how many frames were not written when it does give up.
- Commands: `ib record` (exit 0 on `--duration`, `--count` or a signal's 128+n; `--count` stops at
  the first batch past N, so it records at least N), `--record PATH|auto` on `listen`, `events`
  and `respond`, `ib rec info [--format line|json]`, `ib rec check [--strict]` (exit 1 on mid-file
  damage, a summary that disagrees with its records or an unexplained sequence hole; a torn tail
  is a warning unless `--strict`), `ib rec repair IN OUT [--force]` (exit 5 when valid frames
  after mid-file damage were left out; the synthesised closing chunk has no monotonic times; a clean
  recording repairs to identical bytes). `info` answers from the index and chunk summaries, so it
  does not notice a damaged frame; `check` does. The CLI gained an `offline` runner (no config,
  no session) for the `rec` commands, and `Session.record_routes`, `event_routes`, `respond_route`
  and `streams_for`.
- Config `[recording]`: `dir`, `fsync`, `max_buffer_bytes` (1 MiB to 64 GiB), `min_free_bytes`,
  `embed_definitions`; definition files above 8 MiB (64 KiB for profiles) refuse the recording
  with a hint to turn embedding off. META names every node's definition digests either way.
- `atomicfile.py` implements what 2d's plan assigns to 2d for the temporary commit, because
  `rec repair` needs it now: `TempOutput` (`.<name>.<16 hex>.tmp` created `O_EXCL`, removed on
  failure only after `fstatat` matches its descriptor) and `rename_noreplace` (`renameat2`
  `RENAME_NOREPLACE`, then `link` with the NFSv3 retransmission case, then a placeholder rename;
  directory sync after the commit; regular files only, the directory placeholder of the 2h export
  is not built). 2d should extend or adopt it rather than write a second one.
- Not done here: replay and `--follow` (2g), retention and prune (2d: the recording name matches
  its patterns, the heartbeat and lock exist, the rate hook is unwired), `IB-LED-009` for a file
  system without locks (the recorder runs unlocked and ledgers nothing), `IB-REC-009` (2g).
- PR #7 review fixes (reader and tools). The reader does no unbounded work on a hostile file: one work
  budget per pass (above), and a body that runs past the end of the file is never read. `Damage`
  objects kept are capped (1000; the count and byte total cover all), `rec check` and the ledger
  report damage as one coalesced line (`--verbose` lists the kept stretches). The reader no longer
  retains BLOB contents; `info` lists distinct definitions (set) with a capped output. Holes in the
  sequence are explained by a bounded merged set of GAP intervals over every GAP (so neither a
  false failure nor a false pass past the listing cap) and `lost_by_reason` sums all gaps. `rec
  repair` and `check` treat bytes after the valid prefix as damage: a stale INDEX, a TRAILER in
  mid-file, or a concatenated file; `repair` exits 5 and names how many bytes and valid frames
  were left only in the original. Stricter rules the mutation run showed untested are now pinned:
  nonzero padding, envelope flags, an INDEX that is not increasing or does not end at the trailer,
  a GAP naming an undefined stream, chunk time bounds and MID order, the trailer's last sequence, a
  MARK reserved byte, CHUNK `chunk_start` bounds. STREAM id 0xFFFF is reserved (it means "no stream"
  in a GAP). META nesting is capped at 16 levels (iterative check) and `info --format json` is
  strict JSON (no NaN or Infinity; nested META values pass through the same sanitizer). A complete
  final record that fails its CRC is reported as a damaged record, not an incomplete one. An empty
  split part (a recorder killed just after creating it) reports "the file is empty" with a hint that
  the earlier parts are complete; every non-first part's head is written and synced before the part
  takes frames, so the only residue of a crash is that empty or head-less last part.
- PR #7 review fixes (writer and commands). Statistics count frames when the write completed
  (`RecorderStats.frames`, `file_bytes`); the rest are `unwritten_frames` (encoded, never written),
  `after_stop_frames` and `overrun_frames`, and `lost_frames` sums them. A write failure says
  "recording stopped after N frame(s), M not recorded" in the notice, the CLI summary and the
  ledger, and words whether the capture goes on (`ib record` ends, `--record` commands continue).
  Close: no overall limit but "no writer progress for 5 s"; the overrun hysteresis does not fire
  during a close, so there is no second `IB-REC-003`. `Recorder.append`, `gap` and `mark` never
  raise: a failure stops the recorder through the internal-failure path (`IB-INT-001`). `--record`
  creates its file only after the arguments validated and the transport opened (frames are staged,
  bounded at 65536 events then counted as a `recorder_overrun` GAP, and handed over in one loop
  turn); a start that fails after the `O_EXCL` create removes its own partial file, or names it
  when it cannot. Datagrams unread at close become a `kernel_drop` GAP. A directory that cannot be
  synced (`EINVAL`, `ENOSYS`, `EOPNOTSUPP`) is tolerated with one `IB-REC-011` warning.
- Deferred (documented, not fixed): an `atheris` run of the recording target (not installed on
  the dev host); `IB-LED-009`; a crash in the microseconds between creating a non-first split part
  and writing its head can still leave that one part empty (reported with a hint, not repaired).
- Verification numbers (4-core dev host): `Recorder.append` of a 200-byte frame about 3-4 us; the
  writer sustains about 210-270k frames/s (52-66 MB/s) to a closed, synced file; the reader reads
  about 105-130k frames/s with every CRC and rule checked; `ib record` at 10k pkt/s of 200-byte
  packets for 3 s records 30000 of 30000 with no kernel drop, about 56 us of CPU per frame for the
  whole process, and the `ib listen` guards are unchanged. Tests: `test_recording.py` (writer,
  reader, engine sink), `test_recording_malformed.py` (truncation at every offset, every third
  bit flip, Hypothesis corruption and arbitrary records with valid CRCs, crafted frames, forged
  records, bounded resync, the 69-file corpus `tests/corpus/malformed/rec-*.ibrec` (each with an expected outcome asserted in
  `tests/rec_corpus.py`)),
  `test_recording_tools.py` (commit paths, repair), `test_recording_cli.py` (record, `--record`,
  SIGTERM and `kill -9`, `rec` tools, config) and benchmarks; `tests/cfs/test_acceptance.py` has
  a live-session recording case for CI (not run on the dev host: it needs the fixed cFS ports).

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

As built (where the build differs from, or settles, the text above)
- Package `ironbird.filter`: `lexer.py` (linear scan, anchored fixed patterns only), `parser.py`
  (recursive descent; only `not` and parentheses recurse, each counted before it does; `and`/`or`
  chains, also across parentheses of the same kind, are flat n-ary nodes), `compiler.py` (names,
  types, closure tree, candidate MIDs), `matcher.py` (`Filter`, `Matcher`, `Cost`), `diag.py`
  (limits, `FilterError`, suggestions). Public API: `compile_filter(text, codecs, *, links=None, aliases=None) ->
  Filter(source, mids, needs_decode, match, cost)`; `Filter.bind(decode, codec_for) -> Matcher`;
  `Matcher.__call__(frame) -> bool`, `Matcher.decoded(frame)` (the decode the filter made, or
  one on demand: made at most once per frame, so a consumer never decodes a matching frame
  twice), `Matcher.resolved(frame)`, `Matcher.decodes`. 2g builds its matcher the same way with its
  own decoder and the codecs of the recording's definitions; the filter is valid only for frames
  decoded with those same definitions. `Session.compile_filter(text, nodes)` and
  `Session.matcher(flt, decoder)` wrap it for live use.
- Deviation: `compile_filter` takes the nodes' **codecs** (node name to `Codec`), not bare CCDD
  models, because `hdr.<field>` names live in the header profiles. `links` (the config's link
  names) validates `link == ...`; `None` accepts any name. `aliases` (alias to node name) lets
  `node == flight` mean `node == fc`; an alias of a node not in use is unknown and a node name
  wins over an alias spelled the same. `Session.compile_filter` passes the config's aliases.
- Grammar as in S4, plus: strings are `"..."` or `'...'` (escapes `\\ \" \'` only; every
  character printable); keywords (`and or not in contains`) are lower case and cannot be bare
  literals (quote them); a bare word and a quoted string are interchangeable wherever a name is
  expected (message, category, enum label, node, link, severity, `dir`, `kind`, `status`,
  `true`/`false`); `event.app`, `event.text`, `source` and char fields take quoted text only;
  bytes fields take quoted hex (`==`, `!=`, `in`, `contains`); `HEX` is `[-]0x...`; `TIME` needs
  seconds (`...T12:00:00[.f]` and `Z`, `+HH`, `+HHMM` or `+HH:MM`), a zone is mandatory.
- Meaning of the names: `len` is `orig_len` (size on the wire); `msg` and `category` of a telemetry
  frame come from `frame.mid` alone (so a malformed frame of a known MID still has them); `msg`
  of a command frame costs a header check (`Codec.resolve`, or the ingest token), not a full
  decode, so `msg` is MODEL level; `status` is `unknown` when the check says `IB-DEC-006`,
  `malformed` for every other failure (truncated and garbage frames included), else `ok`;
  a `GARBAGE` frame is malformed by definition (`frame.garbage_error`, `IB-DEC-004`) in
  `Matcher.resolved`/`decoded` and in `FrameDecoder.decode`: it is never handed to the decoder
  and `hdr.*` and field names are not evaluated on it;
  `fc` is the decoded command's function code; `event.type` takes severity names
  (`debug info information warn warning error critical`, case-insensitive). An event whose type
  value maps to no severity is unranked: `>=` and `>` let it through (`ccdd.model.meets_floor`,
  the rule `--min-type` uses, so `event.type >= error` and `--min-type error` show the same
  events), `!=` is true as `not ==` is, and every other operator is false; `htime` is reserved
  and refused (`IB-FLT-002`) until 2i.
- Field predicates (`MSG.f`, bare `f`): a message is a candidate when it has the field. `in` is an
  `or` of `==`: every item is converted for every field on its own and the items a field cannot
  take (a label its enum lacks, wrong kind, bytes of the wrong size) are dropped for that
  message; a message is left out only when no item fits it. A single literal a field cannot take
  leaves the message out for `==`, the orderings and `contains`, and makes `!=` true for it
  (`x != L` is `not (x == L)` wherever the field exists; the message without the field stays
  false). An operator the type lacks leaves the message out. The filter is `IB-FLT-003` when no
  message takes the operator, when an item (or the single literal) fits no message at all (a
  typo), or when the integer is outside every qualifying field's range for `==`, `!=` or `in`
  (`counter == 300` on a uint8 is an error; next to a uint16 `counter` it selects that one, and
  `counter != 300` on the uint8 is plainly true). Ordering allows a decimal on an integer field
  (`x > 1.5`), equality does not. Decimals are lexed to `Decimal` and turned into the integer
  bound that orders every integer the same way (`x > 1.5` is `x > 1`, `x >= 1.5` is `x >= 2`), so
  no precision is lost at 2**64; a float field gets the literal as float, rounded to float32 for
  a `float32` field (through float64: a literal within 2**-53 relative of a float32 rounding
  boundary can land on the other side) and to the infinity it overflows to.
- Work is grouped, not repeated: the messages that have a field of a name are grouped by wire
  type and enum once, a predicate is judged once per group, and identical predicates are
  compiled once. A 2048-message CCDD with a 256-way `or` compiles in about 30 ms (3 s before).
- Reserved names win (S4): a bare reserved name is the frame's, `hdr.x` and `event.x` are the
  namespaces even if a message has that name, and a CCDD field with a reserved name is
  `MSG.field`. `MSG.x` for a message named like a reserved word is `IB-FLT-002`; a bare namespace
  (`hdr == 1`) is `IB-FLT-002` with the CCDD spelling in the details. There is no collision error,
  so a CCDD update cannot break a script.
- A filter that can never match is refused (`IB-FLT-003`): an `and` whose operands' candidate MIDs
  do not overlap (also inside a `not`), `Session.listen` when the filter's candidates share no MID
  with `--mid`, `Session.events` when they share none with the nodes' event MIDs.
- Evaluation: `and`/`or` children are sorted by cost (frame, model, check, decode; stable), the
  root rejects a frame whose MID is outside the candidate set before anything runs, and every
  decode-level predicate checks its own messages' MIDs (per node) before it asks for a decode, so
  a frame is decoded only when some message can match, also under an `or` with an unbounded
  operand. A predicate on `mid` itself sets candidates only for `==`/`in`. No candidate message
  at all for `msg`, `category`, `fc`, `event.*` is `IB-FLT-003`/`-002` (the filter could never match).
- Live: `Session.listen(..., where=)` and `Session.events(..., where=)` compile before anything is
  opened. `ib listen`/`ib events --where EXPR` (`--mid` and `--where` combine by and). `events`
  decodes every event MID frame for the ledger first and tests the filter on that frame after
  `--min-type`, so a hidden event is still ledgered in full. `listen` yields decoded packets only
  (as before): frames a `status` filter selects that do not decode are counted in the
  `listen.stop` ledger record (`where_undecodable`), not shown; replay/export (2g, 2h) show them.
  `listen.start` and `events.start` record the expression (`where`) only when there is one;
  `listen.start` adds the candidate count (`where_mid_count`) and the first 32 MIDs (`where_mids`),
  and `listen.stop` adds `where_undecodable` only for a filtered listener.
- With `--record` (2e) the filter only limits what is shown: the recorder is a sink that sees every
  frame of the link before any consumer queue, so a filtered `listen`/`events` still records every
  frame (malformed and filtered-out ones included). A bad `--where` is `IB-FLT-*` (exit 2) raised
  before the capture opens, so no recording file is created.
- `IB-FLT-*` is usage (exit 2). One-line message with column and (for names and literals) the best
  suggestion; `--verbose` and the ledger get the expression window, a caret line and up to three
  close names. Non-printable and right-to-left or bidi characters in the shown expression are `?`,
  one for one, and the caret sits at the display width of what precedes it.
  `RecursionError` cannot occur at the limits and is reported as `IB-FLT-004` if a caller is
  already deep in the stack. The `in` list limit (1024 items) is checked by the parser; the lexer
  does not count the tokens inside the brackets of a list against the 1024-token limit.
- Tests: `tests/test_filter.py` (lexer, parser, limits, names, types, mids, semantics, cost
  ordering, errors, corpus), `tests/test_filter_fuzz.py` (Hypothesis: arbitrary text, token soup and
  deep nesting raise only `FilterError` within a time bound; compiled filters equal a naive reference
  evaluator, `tests/filter_defs.py`, on random expressions and random valid, mutated, truncated,
  garbage and live-token frames over two nodes with clashing MIDs and names; the candidate set never
  excludes a match; no frame outside it is decoded), `tests/test_filter_diff.py` (seeded random
  expressions and frames against a second, independently written reference,
  `tests/filter_ref_independent.py`, written from the documented semantics alone), `tests/test_filter_live.py`
  (loopback sessions and CLI processes on ports 53000-53999), corpus `tests/corpus/malformed/flt-*.txt`
  and `frame-garbage-flagged-valid-looking-tlm.bin` (a valid packet flagged garbage),
  `tools/fuzz/fuzz_filter.py` (atheris), two `cfs` acceptance cases.
- Benchmarks (`tests/test_benchmarks.py`, 4-core dev host): per frame, frame-level
  `seq > 0 and node == fc and dir == rx and len >= 16` 1.4-1.6 us (about 650k frames/s); model-level
  (`msg == MOCK_HK and category == housekeeping`) 0.9-1.0 us (1.1M/s); check-level 0.5-0.8 us; decode-level
  (`msg == MOCK_HK and cmd_counter >= 0 and mode in [...]`) 7-11 us including the one decode (90-140k/s);
  a frame outside the candidate MIDs about 0.3 us. Whole listen path with a decode-level filter 22-25 us
  against 17-21 us for the bare decode and render (guard 1.8x; 1.5x without a filter). End to end
  `ib listen --where ...` at 10k pkt/s: 30000 of 30000 lines, no `IB-OUT-*`. Marginal cost of one
  more predicate in a chain of true ones: 0.13 us (frame), 0.3 us (model), 0.25 us (check), 0.17 us
  (decoded field) - guarded at 3 us and 6 us per predicate. Compile time of a 256-way `or`, a
  500-item `in` and a 256-way `and` over a 2048-message CCDD: 30-110 ms, guarded at 0.5 s.

### 2g - Replay

Deliverables
- Frames come from the 2e reader, which validates `orig_len`, the data length against the
  recording's `max_packet_bytes`, the flag rules and the stream ids before it builds one (see
  2e), so nothing replayed can trip `Frame`'s asserts, `oversize_error`'s assert or the
  `Engine._labels` index. Replay decodes through `FrameDecoder(report_checked=True)` and
  `Codec.decode` (no `checked` token), and its own reject path needs the stream table of the
  recording rather than the engine's `_labels`.
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
  A writer that died without a TRAILER would otherwise keep it waiting forever, so when a
  poll finds no new bytes it probes the writer's lock (`flock(LOCK_SH | LOCK_NB)`): if it
  is acquired, the writer is gone; the reader reads to the end once more, reports the
  partial record as a torn tail (`IB-REC-007`) and ends. Where the filesystem has no
  reliable `flock` (NFS), `--follow-idle S` (default off) ends after S seconds of no growth.
- `ib rec repair IN OUT [--force]`: valid prefix plus a fresh index and trailer into a new
  file, through the 2d temporary-file helpers as in 2h: an existing OUT is refused
  without `--force` (`IB-REC-001`), output goes to `.<OUT name>.<16 hex>.tmp` created
  `O_EXCL`, synced and committed by the non-replacing temporary commit (`--force`:
  `os.replace` of an existing regular file of ours); OUT equal to IN (same device and
  inode) is refused even with `--force`; a failure removes the temporary file.
- End-of-run summary on stderr: frames read, matched, malformed, gaps, damage.

Verification: E2E mock round trip (`ib respond` + `ib record`, then replay output equals
what `ib listen` printed live, by record not by text); damaged-file E2E (torn,
mid-file, bad index) with expected codes and exit codes; benchmark of replay frames/s
with and without field filters; real cFS: replay the 2e recording with
`--where 'event.app == "SAMPLE_APP" and event.id == 3'` yields exactly the K NOOP events
sent during the capture.

As built (where the build differs from, or settles, the text above)
- Modules: `recording/replay.py` (`Replay`: the frames of a file or split set, ranges, chunk
  selection, damage, `Follower`, `Pacer`, `parse_time_spec`), `recording/defs.py` (the head of a
  recording, codecs from embedded BLOBs or the configuration, comparison with the configuration),
  `recording/player.py` (`play`: pace, filter, decode, render; `compile_where`), the renderers in
  `render.py` (`undecoded_line/record`, `hex_line/record`), `ib replay` in `cli/main.py`. Shared
  pieces gained entry points: `ccdd.loader.load_ccdd_bytes` and `header.load_profile_bytes` (the
  on-disk loaders now read the file and call the same code), `filter.lexer.parse_time`,
  `RecordingReader.items(skip=, wait=)` and `skip_chunk`, `BoundedWriter.write_wait`, and
  `FrameDecoder(codec_for, reject, report_checked=)` (it no longer needs an `Engine`;
  `Engine.decoder()` builds the live one, so replay needs no `_labels`). `ib rec repair` was
  already done in 2e.
- Command: `ib replay FILE... [--where] [--from] [--to] [--speed] [--follow] [--follow-idle S]
  [--format line|json|hex] [--defs recording|config] [--skip-damage] [--count N]`. Runs offline
  (no session, no sockets, no ledger requirement beyond the usual); `-c PATH` is read only for
  `--defs config` and for the one-line comparison below. A decoded frame prints exactly as `listen`
  does (line and JSON), so replay output equals a live `listen` of the same traffic by record
  (the E2E test compares them without the receive times). A frame that does not decode prints
  `MALFORMED`, `UNKNOWN`, `GARBAGE` or `UNDECODED` with its code, the failing byte in brackets,
  `frame=` (the recording's seq; `seq=` is the packet's), its flags and its hex (256 bytes in
  lines, 1024 in JSON, which has `status`, `error`, `hex_truncated`); `--format hex` dumps every
  frame undecoded and, if the recording has no usable definitions, still works, with a `--where`
  made of frame-level names (a name it cannot check says `IB-REC-009` is why).
- Definitions: the BLOBs at the head are read with `head_records` (at most 4096 records, 64 MiB,
  256 nodes), each digest must equal the one META names (`IB-REC-009` if not; a META digest that
  is not text is ignored), a repeated definition that differs is a problem recorded in the head
  and refused when definitions are needed, never at open. Loading goes through the strict loaders
  with the recording's `max_packet_bytes`, and the `python:` header escape hatch is refused in
  `load_profile_bytes`. Every failure there (any loader, `Codec`, adapter, missing definition) is
  `IB-REC-009`, exit 1 (a bad file, not the user's configuration), naming `--defs config` and
  `--format hex`. `--defs config` uses the configuration's files with the recording's cap and its
  aliases (a node it lacks is `IB-REC-009`). With the default, when a configuration is found and a
  file's digest differs from the embedded one, one `info IB-REC-009` line lists `node:kind`s and the
  ledger records them; no configuration is not mentioned (ledger only).
- Ranges and chunk selection: `--from/--to` are ISO 8601 with a zone or `+<n>h|m|s|ms` offsets
  (decimals allowed, at most 100 years) from the first frame of the first part; half open on
  `time_ns`. With a closed file whose index checks out (`read_trailer`, `read_chunks`), a chunk is
  skipped when its summary shows no time in range, no frames, or (summary not overflowed) none of
  the filter's candidate MIDs; the reader jumps to the next chunk after learning the skipped chunk's
  stream and source definitions from its head and restarting the sequence check, because a chunk's
  frames may use definitions made in an earlier one. The index stays a hint: damage inside a
  skipped chunk is not seen, and a bad index is `IB-REC-008` (info) and a full read.
- Pace: `Pacer` anchors a schedule at the first frame and sleeps in 100 ms slices (so a closed output
  or a signal ends it at once); a backward step counts as zero, a late consumer is not waited for,
  `--speed` is 0 or 1e-6..1e9. Frames outside the filter still count for time. A forward clock step
  is not corrected (no `--max-gap`): a recorded hour of silence is an hour.
- Sets: one file that is part 1 plays `name.p0002.ibrec`, ... in order (found by name; the
  parts' META must give the same session and cap, part numbers ascending, else `IB-CLI-001`); a
  part that is missing (a later one exists, or the named parts skip a number) is `IB-REC-006`
  (error line, exit 5, the rest still plays). Several FILEs play as given. Mid-file damage without
  `--skip-damage` ends the whole replay; a torn tail ends only its part.
- Damage and exit codes: 0 normal (including `--count` reached, a closed pipe, a torn tail or a
  recording never closed: `warning IB-REC-007`; a file a recorder holds open is not torn, it says
  `--follow` keeps reading); 5 mid-file damage seen (`IB-REC-006`, at most 3 lines printed and 100
  ledgered, the stop record and the summary count all) or a missing part (not when `--count` ended
  the run first); 1 `IB-REC-004..009`; 2 usage and `IB-FLT-*`; 4 stdout closed or failing;
  128+n a signal. 6 (2d) is not produced here.
- Follow: `Follower` probes `flock(LOCK_SH | LOCK_NB)` on part 1 (the lock the writer holds for the
  whole session), `Replay` polls every 100 ms for growth, treats an incomplete last record or a bad
  record with no valid chain yet as pending (`items(wait=)`), reads once more when the probe says
  the writer is gone, and moves on to the next part when the file ends at its trailer and the writer
  is still alive. No lock support (probe `ENOLCK`/`EINVAL`/`EOPNOTSUPP`) without `--follow-idle` is
  `IB-CLI-001` before anything is read; `--follow-idle S` ends after S seconds without growth. A
  recorder that ran unlocked (`IB-LED-009`, 2d) looks dead to the probe.
- Output: replay writes with `write_wait`, so a reader that stops reading stops replay and nothing
  is dropped (`IB-OUT-001` cannot occur); a signal ends the wait, and whatever was queued is
  abandoned and counted by the existing `IB-OUT-002` path. Per frame costs the ledger nothing; the
  ledger gets `replay.start`, `replay.stop` (reason, counts, rejects by code, chunks skipped), the
  first 100 damage records and any definition note.
- Tests: `test_replay.py` (57: records equal `listen`'s, flagged frames, ranges, chunk skipping
  against a straight read, pacing with a fake clock, count, split sets, damage, definitions,
  filter compilation, follow with a partial record, a dead writer, idle and a lockless file system,
  `write_wait`), `test_replay_malformed.py` (31: the 20-file corpus `rpl-*.ibrec` built by
  `tests/replay_corpus.py` and run through the CLI, Hypothesis over mutated and arbitrary BLOB
  contents, over bit flips and cuts of a file with embedded definitions in every format, over any
  payload in any frame, and over ranges with clock steps equal to a frame-by-frame selection, plus
  a flip of every 97th bit and a cut every 37 bytes of the head), `test_replay_cli.py` (34 over
  loopback on ports 58000-58999: the `respond` + `listen --record` round trip against replay by
  record, nothing sent, exit codes, SIGTERM during pacing and while blocked, closed and slow
  readers, following a real `ib record` to a clean close and to `kill -9`), a `cfs` acceptance
  case (record a session with K NOOPs; `--where 'event.app == "SAMPLE_APP" and event.id == 3'`
  yields K, the last SAMPLE_APP_HK counts K; not run on the dev host), `tools/fuzz/fuzz_replay.py`
  (atheris, in the nightly fuzz job; atheris is not installed on the dev host, so the target ran
  only against a stub). A 100 s bit-flip hunt (10270 corrupted files, each in every format and with three filters)
  and 198610 mutated definition sets in 60 s found nothing beyond coded errors.
- Benchmarks (4-core dev host, 60k frames of 28 bytes, to a null output): replay as lines 21.8k
  frames/s (the bare reader + decode + render is 24.0k/s; 5 us decode, 22 us the shared
  `render.line`, 7 us read), as JSON 30.7k/s, as hex 48k/s; `ib replay` to a file 18.8k lines/s
  end to end; filtered: `seq > 59990` (no decode) 158k/s, `msg == MOCK_HK and cmd_counter > 250`
  96k/s with 40k frames read of 60k, `msg == MOCK_STATUS` 64k/s with 20k read; a 500-frame range of
  60k reads under 10 percent of it. Guards (`needs_a_quiet_host`): at least 15k frames/s, at most
  1.5x the bare path, a filter that selects little at 0.9x of the unfiltered time or better.
- Deferred: the index of a recording is read whole for a selection (memory grows with the chunk
  count, as `rec info`'s does); no `--max-gap` for pacing; `--mid` is not offered (use
  `--where 'mid in [...]'`); a skipped chunk's GAP records are not counted; the atheris target has
  not run on a real fuzzer.

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
- Output goes through the 2d temporary-file helpers: an existing OUT is refused up front
  without `--force` (`IB-EXP-002`); output is written to `.<final name>.<16 hex>.tmp`,
  created `O_EXCL` in OUT's directory, synced and committed by the temporary commit, a
  non-replacing rename, so an OUT that appeared meanwhile is refused too (`IB-EXP-002`)
  rather than overwritten; `--force` replaces an existing regular file of ours
  (`os.replace`). For the per-message directory the temporary is a directory of the same
  form, its files created `O_EXCL`; an existing OUT directory is refused even with
  `--force`, since replacing a directory would delete files ironbird did not write.
  Two refusals no `--force` overrides (`IB-EXP-002`): OUT is the input (same device and
  inode, checked after resolving symlinks and `..`), and OUT names a file retention
  manages, that is, its directory is the state or the recording directory (same device
  and inode as the directory) and its name matches a retention pattern, since pruning
  would later delete an export as if it were a ledger part or a recording. Write
  errors are `IB-EXP-001` and remove the temporary file (or the files this process
  created in the temporary directory, then the directory) through the failure helper.
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
see decision D6. The `ib_test` cFS app's variable-length packet (a counted array and a tail
string) is built here, so these cFS cases run against real FSW.

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
     is known to the CCDD **and the payload size (total minus header and trailer) lies in
     that MID's `[min_payload, max_payload]`**; a lying length is rejected here, before it
     can swallow the packets behind it.
  3. Not sane: one byte joins the current garbage run; rescan from the next byte.
  4. Sane and complete: verify the checksum if the profile has one; failure is treated
     as a false sync (step 3). Without a checksum, a sane header proves little, so the
     candidate is **confirmed before it is emitted** by either a sane header immediately
     after it (at offset `total`) or `max_idle_ms` of silence after it (the cost is that
     the last packet before a pause waits that long). Success emits the packet. Any
     rejection, including an unconfirmed candidate, **rescans from candidate+1**, never
     from candidate+total, so the bytes a false header covered are examined again.
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
independence without expiry; packets separated by non-sane garbage all recovered; **a
false header embedded in garbage, including one whose length covers the following real
packets, does not cost any later packet**; a
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
- `ib_test` throughput source (D6): the app built in 2j gains a command-enabled generator
  of up to 10k pkt/s of fixed-size telemetry (2j owns the app and its variable-length
  packet; 2l owns the generator).
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
| `IB-LED-001` | `LED_WRITE` | existing code, now *stalled* rather than disabled: an essential record cannot be written (write or sync error, reserve used up or none, a write hung over 5 s); records queued, retried every 5 s; in a long session one line per episode (at most once per 10 min), and one at exit if still stalled; a short command shows it only at exit | 2d |
| `IB-LED-002` | `LED_DIR` | existing code, contract narrowed from Stage 1: only a configuration-type open failure (unusable or relative state directory, no home, permission, read-only file system); transmissions and prune refused (exit 1, nothing sent or deleted), read-only commands run without a ledger. Transient open failures (disk or quota full, I/O error) no longer give it: they stall (`IB-LED-001`) and transmissions follow `on_full` | 2d |
| `IB-LED-003` | `LED_TIER` | event or evidence tier over its allowance; counted only until it is back under with hysteresis (`ledger.tier_gap` marks the gap); one per episode, displayed at most once per 10 min per tier | 2d |
| `IB-LED-004` | `LED_UNLOGGED` | a transmission while the ledger is stalled, or whose record was possibly lost to a deferred sync or close error: by default (`on_full = "continue"`) sent with a "sent but not logged" warning, coalesced by code and naming the latest command, plus a per-process summary at exit, also on the signal path (exit 6 if never written or possibly lost); a short command prints one line at exit that names the stall; with `"refuse_commands"` refused (exit 1, nothing sent) | 2d |
| `IB-LED-005` | `LED_DISK` | disk or quota full; essential records now written into the preallocated reserve, event and evidence tiers counted only; transmissions still logged; one per episode, displayed at most once per 10 min | 2d |
| `IB-LED-006` | `LED_RESERVE` | essential reserve not available or not guaranteed (allocation failed, short `st_blocks`, file system type outside the allowlist, which creates no reserve file, no free reserve name); a full disk stalls the ledger at once | 2d |
| `IB-LED-007` | `LED_RESUMED` | info: the ledger writes again after a stall (duration, delayed and lost records); long sessions only, displayed at most once per 10 min | 2d |
| `IB-LED-008` | `LED_UNBOUNDED` | `evidence_bytes_per_hour = "unlimited"`: a flood of malformed packets can fill the disk | 2d |
| `IB-LED-009` | `LED_NOLOCK` | file locks unavailable in the state or recording directory (`ENOLCK`, `EINVAL`, `EOPNOTSUPP`, or held past 1 s): the session runs unlocked and its liveness rests on freshness; displayed at most once per `warn_interval` per directory (the notices file is then used without its lock) | 2d |
| `IB-LED-010` | `LED_ROTATE` | rotation blocked: the next 16 part names are taken (or part numbers exhausted); the current part keeps growing past `max_file_bytes`, no stall; retried once a minute; displayed at most once per 10 min | 2d |
| `IB-RET-001` | `RET_AGE` | non-protected sessions older than `retention.max_age`; review with `ib ledger prune` / `ib rec prune` | 2d |
| `IB-RET-002` | `RET_TOTAL` | ledger + recordings past, or projected past, `retention.max_total_bytes` (protected bytes named); prune the named directory or raise the cap | 2d |
| `IB-RET-003` | `RET_PRUNE` | a listed session was not deleted (became live, changed since listing, unlink failed) | 2d |
| `IB-RET-004` | `RET_SCAN` | retention scan partial (directory unreadable, over 100 000 entries, a leftover reserve that could not be recovered: no valid header, other session's header, name collision, no part 1, locks unsupported; its session is then protected; or one salvaged only in part, the damaged original kept as `.damaged`; the message names the file and the action) | 2d |
| `IB-RET-005` | `RET_TARGET` | `--until-under` cannot be reached in this directory: the rest is protected (bytes by reason, `future mtime` and `unrecoverable reserve` included) | 2d |
| `IB-RET-006` | `RET_UNSAFE` | directory writable by group or others without the sticky bit; prune refused there | 2d |
| `IB-REC-001` | `REC_CREATE` | cannot create the recording or the `rec repair` output (exists, permission, directory) | 2e |
| `IB-REC-002` | `REC_WRITE` | write or sync failed; recording stopped | 2e |
| `IB-REC-003` | `REC_OVERRUN` | recorder buffer full; frames dropped (GAP recorded) | 2e |
| `IB-REC-004` | `REC_FORMAT` | not an ironbird recording | 2e |
| `IB-REC-005` | `REC_VERSION` | unsupported version or unknown critical record type | 2e |
| `IB-REC-006` | `REC_DAMAGED` | damaged record mid-file (offset, span) | 2e |
| `IB-REC-007` | `REC_TORN` | recording ends in an incomplete record (not closed cleanly) | 2e |
| `IB-REC-008` | `REC_INDEX` | index missing or invalid; rebuilt by scanning | 2e |
| `IB-REC-009` | `REC_DEFS` | embedded definitions differ from config, or cannot be loaded | 2g |
| `IB-REC-010` | `REC_SPACE` | low disk space for the recording | 2e |
| `IB-REC-011` | `REC_DIRSYNC` | the recording's directory cannot be synced; its name may not survive a power loss (warning) | 2e |
| `IB-FLT-001` | `FLT_SYNTAX` | filter syntax error (column, excerpt) | 2f |
| `IB-FLT-002` | `FLT_NAME` | unknown name in a filter, or a reserved name misused | 2f |
| `IB-FLT-003` | `FLT_TYPE` | literal does not fit the name's type, or the filter can never match | 2f |
| `IB-FLT-004` | `FLT_LIMIT` | filter too long, deep or wide | 2f |
| `IB-EXP-001` | `EXP_WRITE` | cannot write export output | 2h |
| `IB-EXP-002` | `EXP_EXISTS` | export output exists (pass `--force`), is the input, or is a file retention manages (`--force` does not apply to these two) | 2h |
| `IB-DEC-008` | `DEC_COUNT` | variable-length count above the CCDD maximum | 2j |
| `IB-DEC-009` | `DEC_OVERRUN` | variable-length data runs past the packet end | 2j |

`IB-FLT-` joins the usage prefixes (exit 2). Proposed exit code 5 (decision D8): output
complete up to mid-file damage in an input recording. Exit code 6 (decision D12, 2d): a
transmission went out but its ledger record was never written or was possibly lost (a
deferred sync or close error); same tier as 4, and 6 wins over 4 and over 5.

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
| Ledger growth: sampling, tier allowances, rotation, retention warnings and prune, send policy | 2d |
| Output and signals equivalence | Stage 3; Stage 2's new commands keep Stage 1's exit codes, bounded writer and signal path |

### CI notes (EL8 jobs) and deferrals

The EL8 container jobs (`.github/workflows/ci.yml`, `tools/ci/`) are guarded by
`tests/test_ci_el8.py` (parsed-workflow privilege checks with mutation cases, SIGPIPE static
check over scripts and workflow `run` blocks). Deferred, not done:

- **SHA-pin third-party actions and move to the v5 actions** (checkout, setup-python, cache,
  upload-artifact are on floating major tags v4/v5). Needs a pinned-SHA table and an update
  routine (Dependabot for `github-actions`).
- **Push + pull_request duplicate cFS builds.** Both events run `cfs`/`el8-cfs` for a PR
  branch, and the cache is per-ref, so the cFS build runs twice. Fix with `on.push.branches:
  [main]` plus `pull_request`, or a shared cache scoped to the base ref.
- **The root-only test never runs in CI.** The EL8 jobs are unprivileged on purpose and the
  Ubuntu jobs do not run the cFS root path (`run.sh` started as root dropping to
  `CFS_RUN_UID`, `clean.sh` as root). Needs a dedicated root job or a `sudo`-capable step.
- **Image digest.** The workflow cannot read the container's own digest from inside it; the
  digest is only in the runner's "Initialize containers" log. Pinning `image:` by digest
  (with an update routine) would make runs reproducible.
- **EL8 kernel behaviour** (rmem_max, `/proc/net/udp`, SO_RXQ_OVFL, SELinux, PAM limits) needs
  a real EL8 host or VM runner; containers cannot cover it (README "Supported platforms").
- **`--ulimit nofile=1024:524288` is unverified in Actions**: the full non-cFS suite passes
  under a 1024 soft limit locally, but no container runtime was available to run the EL8 jobs
  or the real-cFS tests with it.

### Decisions

Decided by the user (2026-10-30):
- **D1 Embed definitions in recordings: yes.** Each recording carries its nodes' CCDD and
  header profiles (8 MiB cap each); replay uses them by default (`--defs config`
  overrides), so a recording decodes correctly after the CCDD changes. A recording is therefore as
  sensitive as the definition files themselves (the README says so).
- **D2 Ledger budgets and retention: scale with run time, warn, never delete.** Expected
  load: events ~10/s average, ~30/s when active, bursts of 50-100/s, many seconds with
  none; most tests short, some 12 h overnight; telemetry goes to recordings, not the
  ledger.
  - Event and evidence tiers have per-hour allowances with a one-hour floor: events
    160 MiB/h (1.875 GiB at 12 h; D-a below), evidence 32 MiB/h (384 MiB at 12 h).
    Essential records are uncapped and keep a 32 MiB preallocated reserve for when the
    disk fills.
  - When an essential record cannot be written, commands are still sent by default
    (`on_full = "continue"`, Stage 1 behaviour), each with a coded "sent but not logged"
    warning that is coalesced and rate-limited, never silent. `on_full = "refuse_commands"`
    is the opt-in stricter policy (commands refused, nothing sent). Capture and recording
    never stop for ledger reasons.
  - Nothing is deleted automatically: no ledger part, reserve or recording. The one
    removal without a prune confirmation is a process's own never-renamed temporary file
    on the failure path of the operation that created it (defined narrowly in 2d; 2d
    itself creates none). Retention only warns: sessions past `max_age` (7 d), and ledger
    plus recordings past, or projected past, `max_total_bytes` (8 GiB). Deletion is only
    `ib ledger prune` / `ib rec prune` after an explicit confirmation, never of a
    protected session (live, among the `keep_recent` (10) most recent, and the other 2d
    protection rules).
  - Exactly four keys accept `"unlimited"` (the one spelling, strictly validated), for
    users who do not want to monitor: `event_bytes_per_hour`, `evidence_bytes_per_hour`,
    `max_age` and `max_total_bytes`.
- **D-a Event records keep their raw bytes; the event allowance rises instead.** Measured
  through the real formatter, an event record is 856-947 B (spaced hex of the 168 B
  packet included); 160 MiB/h keeps 30/s sustained and a 15 min burst at 100/s from a
  cold start under the allowance, at a worst case of about 1.9 GiB of events per 12 h.
- **D-b What refuses a transmission.** A configuration-type open failure (`IB-LED-002`:
  unusable or relative state directory, no home, permission) refuses to send, as in
  Stage 1; transient failures (disk or quota full with the reserve used up, I/O errors,
  stalls) send with a warning under the default `on_full = "continue"`, a change from
  Stage 1, which refuses on any failure to create the ledger (2d updates the README). The
  errno boundary is in 2d.
- **D-c Exit code 6** (recorded as D12 below) for "sent but not logged"; `IB-LED-004` is
  coalesced by code only and names the latest command, with a per-process summary at
  exit.
- **D-d The reserve holds data.** Essential records are written in place into the
  preallocated reserve file (`pwrite`), which then becomes a ledger part; after a crash
  the leftover is cut at its last complete record and kept as a ledger part, not deleted.
  Allocation is verified (`st_blocks`), with a coded warning when it fails or is not real.
- **D6 Variable-length and throughput on real cFS: add an `ib_test` cFS app** under
  `tools/cfs`, built with the pinned bundle, that emits a counted-array packet and can
  generate up to 10k pkt/s of telemetry. Used only for acceptance and the throughput gate.
  Owners: 2j builds the app with the variable-length packet, 2l adds the throughput source.
- **D7 Serial hardware: none yet.** The pty bridge (mock E2E and pty-to-cFS-UDP) is the
  gate; a real-UART run is added when hardware is available.

Adopted as recommended (revisit on request):
- **D3** Events are never sampled; they have a tier of their own with a per-hour allowance
  (160 MiB/h, one-hour floor; counted, not written, past it), so they cannot take the
  essential tier's space and a 12 h run at the expected rates never reaches it (2d).
- **D4** Commands from a separate `ib send` are in the ledger but not in a Stage 2
  recording; the Stage 3 engine closes the gap.
- **D5** Replay to the wire is deferred to Stage 5 send modes.
- **D8** Exit code 5 = output produced but the input recording was damaged mid-file; a
  torn tail alone is a warning with exit 0.
- **D9** Ring shape is decided at Stage 3 planning (byte ring preferred).
- **D10** CCDD placeholder schema bumps to `ironbird-ccdd/1`; `/0` keeps loading.
- **D11** Arrays of structs are a follow-up after 2j.
- **D12** Exit code 6 = a transmission went out but its ledger record was never written
  or was possibly lost to a deferred sync or close error (2d). Same tier as 4 (an error,
  3 or a signal takes precedence); 6 wins over 4 and over 5.

Decided by the user (2026-10-30, later):
- **`retention.max_total_bytes` default is 8 GiB.** An active 12 h run (about 2.15 GiB,
  worst case 3.26 GiB) fits several times over, so `IB-RET-002` warns after several
  overnight runs rather than after each one. Nothing is deleted automatically either way.

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
     the rest reduced to a per-key count record); per-session ledger budgets with a coded
     notice when reached (and a defined "stop sending or keep going" policy for transmitting
     commands); and rotation plus retention (age and total size) of old ledger files, which
     warns and leaves deletion to a confirmed `ib ledger prune` (D2).
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
