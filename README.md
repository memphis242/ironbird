# ironbird

Terminal integrated/system test tool for cFS flight software vehicles. Stage 1: CLI send and
receive over UDP. See `docs/plan.md` for the design and `CLAUDE.md` for project rules.

## Setup

```bash
pyenv virtualenv 3.11 ironbird      # or: python3.11 -m venv <dir>
pyenv activate ironbird
pip install -e '.[dev]'
```

## Supported platforms

Target servers run RHEL 8 / Oracle Linux 8 (x86-64), as an ordinary unprivileged user.
ironbird needs the EL8 AppStream `python3.11` (`dnf install python3.11 python3.11-pip`, then
`python3.11 -m venv`); building the real-cFS target also needs `gcc make cmake git` and the
util-linux tools (`flock`, `setpriv`, `ipcrm`). No root, RT priorities or sysctl changes.

CI (`.github/workflows/ci.yml`) runs the lint, type check, tests and the real-cFS acceptance
tests twice: on Ubuntu (fast feedback, newest toolchain) and inside `oraclelinux:8`
containers (`redhat/ubi8` best effort) as a non-root user, using `tools/ci/el8-setup.sh`
to install the toolchain. What the EL8 containers match: glibc 2.28 (manylinux wheel
compatibility), gcc 8 under cFS's `-Werror`, the EL8 cmake and bash 4.4, the distro Python 3.11
and its pip/venv packaging, util-linux and coreutils versions, and running without privileges
(a non-root user with `no_new_privs`). pyright runs on the same pinned Node.js (the
`nodejs-wheel-binaries` wheel) in both families.

What they do not match: containers share the runner's Ubuntu kernel, so kernel-level behaviour
is not EL8's. That covers `net.core.rmem_max` and the UDP receive buffer the kernel grants,
`/proc/net/udp` columns, `SO_RXQ_OVFL` drop counts, `flock` and SysV IPC/mqueue limits, and
serial hardware. SELinux is not enforced in the containers (EL8 servers enforce it by default),
and `firewalld`, systemd and site-specific sysctls are absent. Resource limits differ too:
Docker's ulimits are not EL8's PAM defaults, so the EL8 jobs start the container with
`--ulimit nofile=1024:524288` to approximate the usual login limits, but other limits (for
example `nproc`, `memlock`, `rtprio`) are Docker's. Run the opt-in acceptance tests on a real
EL8 host before relying on kernel-dependent behaviour.

## Try it without hardware (ironbird as both ends)

```bash
ib -c examples/mock.toml nodes
ib -c examples/mock.toml respond --as fc          # shell 1: act as the flight computer
ib -c examples/mock.toml listen fc                # shell 2: decode telemetry
ib -c examples/mock.toml send fc MOCK_NOOP        # shell 3
ib -c examples/mock.toml send fc MOCK_SET_MODE mode=SAFE pad=0 --verbose
ib -c examples/mock.toml find mode
```

Events (shell 2 instead of `listen`; the mock reports whatever event it is asked to raise):

```bash
ib -c examples/mock.toml events --min-type info
ib -c examples/mock.toml send fc MOCK_RAISE_EVENT app=DEMO event_id=7 event_type=ERROR "message=bus voltage low"
```

`ib` is a short alias of `ironbird`. Global options: `--config/-c PATH` (or `$IRONBIRD_CONFIG`),
`--as NODE`, `--verbose/-v`. Errors are one line, `error IB-XXX-NNN: summary`, on stderr; add
`--verbose` for detail: it prints the ledger file path once at start and appends `[ledger n=N]`
to every notice that has a ledger record, `N` being the record number (field `n`) in that file
holding the full detail. Every action and error is recorded as JSON Lines in
`$XDG_STATE_HOME/ironbird/` (override with `IRONBIRD_STATE_DIR`). When the ledger queue is full
(by count, or by a 64 MiB estimate of its size; a flood faster than the disk) a new record is
refused and counted, and queued records are never evicted; one `ledger.dropped` record in the
ledger says how many. A packet's `raw` bytes are cut to `max_packet_bytes` when the record is
created and the full length is kept as `raw_len`. Packet and wire-error records
have a capped share of the queue so a flood of them is unlikely to crowd out actions and commands,
but capture is not guaranteed when the queue is full. A `[ledger n=N]` pointer names the record
queued for the latest occurrence; it is written unless the ledger fails (`IB-LED-001`) or the
process is killed first.

## Recording

`ib record [NODE ...] [--link L] [-o PATH] [--duration S] [--count N] [--split 1GiB|1h] [--quiet]`
captures every frame of all (or the selected) links of the acting node into an `.ibrec`
recording, decoding nothing beyond the ingest check (a status line goes to stderr every 5 s).
`--record PATH` on `listen`, `events` and `respond` records the same links while they run
(`--record auto` picks the default location and name); `respond` also records the replies it sends.
Commands sent by a separate `ib send` are in the ledger, not in the recording.

```bash
ib record -o /data/run1.ibrec --duration 600       # or: ib listen fc --record /data/run1.ibrec
ib rec info /data/run1.ibrec [--format json]       # session, streams, per-MID counts, gaps, clean/torn
ib rec check /data/run1.ibrec [--strict]           # every CRC and rule; exit 1 on damage
ib rec repair /data/run1.ibrec /data/run1.fixed.ibrec [--force]
```

- **Where.** `-o PATH` (an existing directory gets the default name inside it), else
  `[recording] dir`, `$IRONBIRD_RECORD_DIR`, `$XDG_DATA_HOME/ironbird/recordings`,
  `~/.local/share/ironbird/recordings`; the name is `<UTC stamp>-<session>.ibrec`. The path is
  printed and ledgered at start. A recording is created `O_EXCL` (mode 0600, directory 0700) and
  never overwritten or deleted by ironbird (`IB-REC-001` if it exists).
- **Durability.** The file survives a crash from the first frame on, `fdatasync` runs once per
  chunk (1 MiB or 1 s): a power loss loses about a second, `kill -9` the last 250 ms, a clean stop
  or a signal nothing. A write failure (`IB-REC-002`) stops the recorder and the capture goes on;
  the command exits 1 at the end (`ib record` at once). If the disk is slower than the traffic,
  buffered bytes are capped (`max_buffer_bytes`); frames past it are dropped, `IB-REC-003` says
  so, and a GAP record in the file says which. Frames the kernel dropped are GAP records too.
- **What is in it.** Raw frames of both directions (sequence, UTC ns time, stream, peer, MID,
  flags, bytes), chunk summaries (time and MID counts), a META record (session, host, user,
  command line, config hash) and, unless `embed_definitions = false`, the CCDD and header profiles
  of the nodes it decodes with. **A recording is as sensitive as those definition files and the
  traffic: share it accordingly.**
- **Damage.** A recording that ends in an incomplete record (a crash) is a *torn tail*: warning
  `IB-REC-007`, everything before it is readable. Damage followed by valid records is mid-file
  damage (`IB-REC-006`). `ib rec repair IN OUT` writes the valid prefix plus a fresh index and
  trailer to a new file and never modifies `IN`; it exits 5 if frames after mid-file damage were
  left out. `ib rec info` reads the trailer, index and chunk summaries, not frames (a missing or
  bad index falls back to a full read, `IB-REC-008`); `ib rec check` reads everything.
- **Config.** `[recording]`: `dir`, `fsync = "chunk" | "always" | "never"`,
  `max_buffer_bytes` (default 256 MiB), `min_free_bytes` (default 1 GiB: recording refuses to start
  below it, `IB-REC-010`), `embed_definitions` (default true). `ib record` warns (`IB-REC-010`)
  when the disk would fill within 10 minutes.
- **Splitting.** `--split 1GiB` or `--split 1h` starts `NAME.p0002.ibrec`, ... each a complete file
  naming the session, its part number and the previous part's last sequence number; part 1 stays
  open and locked until the session ends.

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | Normal end: done, `--count` reached, `--timeout` elapsed without `--count`, or the reader of stdout went away (`\| head -1`) |
| 1 | Runtime error (`error IB-...`), `find` with no match; `ib rec check` found damage; a recording failed to write (`IB-REC-002`) |
| 2 | Usage, configuration, CCDD or header-profile error |
| 3 | `listen`, `events` or `respond` had `--count N` and `--timeout` elapsed first |
| 5 | `ib rec repair` produced its output, but the input was damaged mid-file and valid frames after the damage are in the original only |
| 4 | The command otherwise succeeded but stdout lines were lost (`IB-OUT-002`): dropped while stdout was not draining, abandoned when a signal cut the final drain short, or stdout is closed (`>&-`). An error (1, 2), 3 or a signal (128+n) takes precedence. A `send` that exits 4 still went out |
| 128+n | Ended by signal n: 130 (Ctrl-C), 129 (SIGHUP), 143 (SIGTERM). The ledger is flushed first; an interrupted `send` says whether the command went out |

Output never blocks the tool while it runs: a stalled stdout (unread pipe, paused pager) drops
lines with one coalesced `IB-OUT-001` notice instead of freezing signals and `--timeout`; the
ledger keeps everything. When the command ends normally it then waits, with no deadline, for a
slow reader to take the lines already queued (one `IB-OUT-003` warning line, also in the ledger, says so
after 2 s; a signal abandons them), so `--timeout` bounds the run, not the output: lines may keep
draining after it. `ib listen --count N | slow-consumer` is lossless only while the backlog fits
the 512 KiB output queue; beyond that the overflow is dropped while stdout is not draining, and
the command exits 4 with the counts in the `IB-OUT-002` line. Whatever was lost is
summed up in one `IB-OUT-002` line, in the ledger, and in exit code 4. Datagrams the kernel drops because the socket buffer is full are reported as
`IB-TX-004`. Memory is bounded under a flood: a datagram longer than `max_packet_bytes` is cut by
the kernel's own copy (`IB-DEC-002` reports its size on the wire), and a front-end that cannot keep
up holds at most 32 MiB of received frames; those it was not shown are reported as `IB-ENG-001`
(a recorder sees them regardless). A socket read that fails is reported as `IB-TX-003`; one that
cannot recover ends the command with that error. If the kernel grants a smaller receive buffer than requested, `IB-TX-005` says to
raise `net.core.rmem_max` (`sysctl -w net.core.rmem_max=...`).

The ledger directory is an absolute `$IRONBIRD_STATE_DIR` (a relative one is refused), else an absolute `$XDG_STATE_HOME/ironbird`,
else `~/.local/state/ironbird`. `send` and `respond` refuse to run (`IB-LED-002`, nothing sent)
when it cannot be created.

## Events and telemetry categories

cFS events are the most important telemetry. `ib events [--node X ...] [--min-type error]
[--format line|json]` listens on every telemetry link of all (or the selected) nodes and
prints only events: time, node, app, type, id, message. They also show in `ib listen`. The
ledger records every received event in full.

Telemetry messages carry a `category` in the CCDD, in priority order: `event`,
`command_response`, `housekeeping`, `status`, `commanded`, `other` (default). An event message
also has an `event` mapping naming its app/id/type/message fields; see `docs/plan.md`
("Telemetry categories and events") and `examples/ccdd/cfs.json`.

## Filtering with --where

`ib listen NODE --where EXPR` and `ib events --where EXPR` show only the frames the expression
matches (`--mid` stays and combines with it by `and`). The expression is compiled against the
CCDD of the nodes in use before anything is opened, so a typo fails at once with a coded one-line
error (`IB-FLT-001` syntax, `-002` unknown or ambiguous name, `-003` literal or operator that does
not fit, `-004` over a limit; exit 2); `--verbose` adds the expression with a caret under the
column and up to three close names.

```bash
ib listen fc --where 'msg == MOCK_HK and cmd_counter > 0'
ib listen fc --where 'category in [event, command_response] and not mode == SAFE'
ib events --where 'event.type >= error and event.app == "SAMPLE_APP"'
```

Grammar: `expr = term {or term}`, `term = factor {and factor}`, `factor = not factor | (expr) |
name op literal`; `op` is `== != < <= > >= in [a, b] contains "text"`. Literals: integers (`-1`,
`0x1f`), decimals (`-0.5`), quoted text (`"..."` or `'...'`), bare words (message, category, enum,
node and severity names; quote one that is a keyword) and ISO 8601 times with `Z` or an offset
(`2026-10-29T12:00:00.5Z`; local times are refused).

| Names | Meaning |
|-------|---------|
| `seq time len node link dir kind source mid truncated garbage` | the frame itself (`len` is the size on the wire) |
| `category msg` | from the MID alone (`msg` of a command frame needs its header check) |
| `status` | `ok`, `malformed` or `unknown` (the ingest check) |
| `fc hdr.<field> event.app event.id event.type event.text` | decoded (`event.type` orders DEBUG < ... < CRITICAL) |
| `<MSG>.<field>`, bare `<field>` | a CCDD field (a bare field means any message that has it) |

A predicate on something the frame does not have is false (`not` inverts it). Reserved names always
win: a bare `mid` or `seq` is the frame's whatever a CCDD calls its fields, so a CCDD update never
changes what a script means; write `MSG.field` (`TLM_A.mid`) for a CCDD field with such a name.
`in [a, b]` is `== a or == b`, item by item per message (a label one message's enum lacks is simply
never equal there), and `x != L` is `not x == L` for every message that has `x`. A float literal is
rounded to the field's width (`ratio == 0.1` matches a stored float32 0.1) and a decimal compared
with an integer is exact. `node ==` accepts a node's alias. `event.type >= error` shows what
`--min-type error` shows, including events whose type has no severity. A filter that can never
match (`msg == A and msg == B`, or MIDs outside `--mid`) is refused with `IB-FLT-003`. Limits: 4096
characters, 1024 tokens outside `in` lists, 32 levels of `not` and parentheses, 1024 `in` items,
numbers of at most 40 characters. A frame whose MID cannot match is never decoded for the filter,
and neither is a serial garbage frame (it is malformed whatever its bytes say). Malformed frames
are reported at ingest as before and `listen` shows decoded packets only, so
`--where 'status == malformed'` selects them for replay and
export, not for `listen`.

## Configuration

- `examples/ironbird.toml` - ground plus a real cFS flight computer (CI_LAB 1234, TO_LAB 2234).
- `examples/mock.toml` - ironbird-vs-ironbird on distinct ports, with response rules.
- `examples/headers/*.toml` - header profiles (bit widths, fixed/computed fields, checksum).
- `examples/ccdd/*.json` - placeholder CCDD JSON (`ironbird-ccdd/0`).

## Development

```bash
pytest -n auto
ruff check . && ruff format --check .
pyright
```

With no `node` on PATH, pyright downloads the official Node.js build, which needs
`libatomic.so.1`: on RHEL 8 / Oracle Linux 8 run `dnf install libatomic` first.

Real-cFS acceptance tests (opt-in, serial, need `tools/cfs/build.sh` first):

```bash
IRONBIRD_CFS=1 pytest tests/cfs -n0      # or: pytest tests/cfs -m cfs -n0
```

A plain `pytest` skips them. They start and stop cFS themselves and need UDP 1234 and 2234
and `/dev/shm/osal:RAM` free (one instance per host).

Test corpus of malformed inputs: `tests/corpus/malformed/`. Add a file there for every parser bug.

Internals: the engine (`src/ironbird/engine.py`) turns whatever a transport receives into
immutable `Frame`s, numbers them, runs the cheap `Codec.check`, hands each frame to its sinks
(a recorder, later) and then to each consumer's own bounded queue, so a slow consumer never costs
a captured frame. Malformed and unknown frames are consumers' frames too; they are reported once,
coalesced, at ingest. `Session.capture()` gives the raw frames, `Session.decoder()` decodes them.
Throughput guards: `pytest tests/test_benchmarks.py -n0 -s` prints the loopback ingest-only rate
(receive side alone) and the end-to-end `ib listen` result at 10k packets/s.
