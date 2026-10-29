# ironbird - staged design and Stage 1 plan

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
  - `ccdd/` - `model.py` (Message, Field, Enum; command vs telemetry; fixed and
    variable-length fields), `loader.py` with a pluggable `Adapter` protocol and a
    placeholder JSON adapter. When your real JSON format is known, we write one adapter.
  - `codec.py` - encode command (header + args + CRC), decode telemetry into fields
    using `struct`-precompiled layouts per message (fast path).
  - `transport/` - `Transport` protocol (`open`, `send`, `receive` async iterator,
    `close`); `udp.py` now, `serial.py` (pyserial; user must already be in `dialout`),
    `mil1553.py` later.
  - `session.py` - wires target(s) + codec + ledger; the single API both front-ends use.
  - `cli/`, `tui/` - front-ends.
- **Recording format** (Stage 2): append-only binary frames of *raw* packets
  (`u64 ns timestamp | u8 direction | u16 target | u32 length | bytes`), decoded lazily
  on replay/export. Raw capture is the cheapest write path and never loses data to a
  decode bug. Exports (CSV, text, xlsx) decode from it.
- **Error UX**: one line, stable code, repeats coalesced (`x37 in 10 s`), rate-limited
  per code; full detail via `--verbose`/ledger (CLI) or a details pane (TUI).

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
1. **Engine** (owns the hardware): one `asyncio` loop, UDP via datagram protocols,
   serial via a reader thread feeding the loop. Per packet it does only cheap work:
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
   sample CCDD JSON with a no-arg command, an arg command, and two telemetry packets).
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
  `to_lab` (port 1235) after sending its enable-output command with our host address.
- CCDD for the test set: hand-authored placeholder CCDD JSON from the cFS headers for
  `sample_app`, `ci_lab`, `to_lab`, cFE `TIME` and cFE `ES`. Command/function codes and
  layouts are taken from the checked-out source, not memory.
- Header profiles: `ccsds_v1_cfs_cmd.toml` must reproduce cFE's command secondary
  header (function code + checksum) and its checksum algorithm exactly; cFS rejects
  bad checksums, so a successful command is the proof.
- Stage 1 acceptance cases:
  - `TO_LAB` enable output -> telemetry starts flowing; `ib listen` decodes cFE ES and
    TIME housekeeping fields to sane values.
  - `SAMPLE_APP` NOOP and reset counters -> command counters in its HK telemetry
    change as expected; an arg-bearing command (e.g. TIME set/add clock adjustment,
    or a `sample_app` command with a payload) is accepted and reflected in TIME HK/
    diag telemetry.
  - cFE TIME NOOP and HK/diag decode (clock state, MET, STCF).
  - cFE ES write-system-log-to-file command (the on-demand syslog dump; exact
    command name/code confirmed from source): command accepted, the file appears in
    the cFS filesystem path, ES HK counters reflect it.
  - Bad checksum / wrong length deliberately sent -> cFS error counter increments,
    and ironbird's ledger records the full exchange.

## Later stages (outline; each gets its own plan)

2. Recording + replay with filters (CLI), serial transport, variable-length payloads,
   CSV/text export.
3. TUI: latest-per-packet view and time-scrolling view, hold / pause / stop, error
   status line + details pane, ledger viewer.
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
- Ledger file inspected for a complete, timestamped trace of the session.
