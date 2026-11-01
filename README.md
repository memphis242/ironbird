# ironbird

Terminal integrated/system test tool for cFS flight software vehicles. The CLI sends commands and
listens, records, replays, filters and exports telemetry over UDP and serial links; the TUI is a
later stage. See `docs/plan.md` for the design and `CLAUDE.md` for project rules.

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
queued for the latest occurrence; it is written unless the ledger is still stalled at exit
(`IB-LED-001`) or the process is killed first. After the ledger rotates into a second part the
pointer names the part too: `[ledger n=1234 file=<stamp>-<session>.p0003]`.

## Commands

`ib` is `ironbird`. Global options: `--config/-c PATH` (or `$IRONBIRD_CONFIG`), `--as NODE`,
`--verbose/-v`. `--help` on any command lists its options.

| Command | What it does | Section |
|---------|--------------|---------|
| `ib nodes` | List nodes, aliases and links, and check every definition file | below |
| `ib find TEXT` | Search commands and telemetry by name, MID or description | below |
| `ib send NODE CMD [name=value ...]` | Encode and send one command | below |
| `ib listen NODE [--where EXPR] [--record PATH]` | Decode and print incoming packets | Filtering |
| `ib events [--min-type T] [--where EXPR]` | Print only cFS events from every telemetry link | below |
| `ib respond --as NODE` | Act as a flight node: answer commands with configured telemetry | below |
| `ib record [NODE ...] [--duration S] [--count N] [--split S]` | Capture every frame into a recording | Recording |
| `ib rec info\|check FILE`, `ib rec repair IN OUT` | Summarise or verify a recording, or copy its valid prefix to a new file | Recording |
| `ib rec prune`, `ib ledger prune` | Delete old recordings or ledger sessions after confirmation | The ledger under load |
| `ib replay FILE... [--where EXPR] [--from T] [--to T] [--follow]` | Play a recording back as `listen` shows it | Replay |
| `ib export FILE... [-o OUT] [--format csv\|text\|jsonl] [--jobs N]` | Export a recording to tables, text or JSON Lines | Export |

## The ledger under load, a full disk, and retention

The ledger writes through one writer thread, so a slow or hung disk never blocks capture.

- **Tiers.** Records are *essential* (your actions, commands, state changes: never sampled or
  capped), *evidence* (what the wire drives: malformed packets, wire-error records, a
  responder's received commands) or *events* (received cFS events). Evidence is sampled per code,
  MID and stream in 10 s windows (the first eight kept in full, then one a second, the rest
  summarised in one `...summary` record; every occurrence is counted) and raw bytes are limited
  to 256 KiB/s. Events and evidence each have an hourly allowance that grows with the run time
  (`[ledger] event_bytes_per_hour` 160 MiB, `evidence_bytes_per_hour` 32 MiB, never less than one
  hour's worth): above it a tier is *counted only* (`IB-LED-003`, one line per 10 minutes,
  `ledger.tier_gap` marks the gap) and resumes when it is back under. Telemetry belongs in
  recordings, not the ledger.
- **Rotation.** At `max_file_bytes` (32 MiB) the ledger continues in `<stamp>-<session>.pNNNN.jsonl`,
  each part opening with a `ledger.part` record naming the previous one. If the next names are
  taken it keeps writing the current part (`IB-LED-010`).
- **Disk full.** Long commands (`listen`, `events`, `respond`, `record`) preallocate a 32 MiB
  essential reserve (`<stamp>-<session>.r0001.reserve`) on ext4, XFS or tmpfs. When a write fails
  with ENOSPC or EDQUOT the ledger continues in the reserve (`IB-LED-005`), events and evidence
  are counted only, and essential records, commands included, are still written. On any other
  file system no reserve is created (`IB-LED-006`) and a full disk stalls the ledger at once.
- **Stalled ledger.** When an essential record cannot be written (reserve used up, I/O error, a
  write that hangs for 5 s) the ledger *stalls*: records stay queued and it retries every 5 s
  (`IB-LED-001`; resuming is `IB-LED-007`). Commands are still sent by default
  (`[ledger] on_full = "continue"`): each is reported as `warning IB-LED-004: NOOP sent to fc but
  not logged (...)`, a short command like `ib send` prints one such line at exit, and the process
  exits **6** if a transmission's record was never written or may have been lost to a deferred
  sync error. `on_full = "refuse_commands"` refuses them instead (exit 1, nothing sent).
- **Locks.** Every session holds `flock` on its first part for its life, which is how other
  processes tell a live session from a dead one. Where locks are unsupported (some NFS and FUSE
  mounts) the session runs unlocked and says so (`IB-LED-009`); keep the state and recording
  directories on a local file system, or prune only from the host that writes them. Without a
  lock, a session is told apart from a dead one only by how recently its files changed (it
  touches its file every minute while idle): a clock stepped forward by more than ten minutes,
  or a host suspended for longer, can make a live unlocked session look idle, and a prune may
  then propose it. A clock stepped backward leaves future file times, which count as live until
  the clock catches up. Where this matters, do not prune while such a session runs.
- **Retention only warns.** ironbird never deletes a ledger part, a reserve or a recording on its
  own. `IB-RET-001` says that sessions older than `[retention] max_age` (7d) exist, `IB-RET-002`
  that ledgers plus recordings are past, or projected past, `max_total_bytes` (8GiB), `IB-RET-004`
  that a scan was partial or a reserve could not be recovered. Warnings are shown at most once per
  `warn_interval` (24h) per code and directory across processes. A crashed session's leftover
  reserve is cut at its last complete record and renamed to the next part (or, if damaged,
  salvaged and the original kept as `*.reserve.damaged`); it is never deleted. Long sessions and
  `prune` do this recovery; a short command only warns.
- **Pruning** is explicit: `ib ledger prune` and `ib rec prune [--older-than AGE] [--until-under SIZE]
  [--keep N] [--dry-run] [--yes] [--format line|json]` list exactly what they would delete and ask
  `delete 14 sessions (310 MiB)? [y/N]` (only `y` or `yes` confirms; without a terminal pass
  `--yes`). They never touch a live session, one among the `keep_recent` (10) most recent, a
  short session that ran inside a protected long one, an incomplete or hard-linked one, or a
  ledger whose recording is still present. Flags use the config grammars (sizes `64MiB`, ages `7d`).
  Stage 1 processes still running after an upgrade take no locks and write no heartbeat: end them
  before the first prune.

```toml
[ledger]    # all optional; sizes are bytes or "64MiB", durations "10min"
event_bytes_per_hour = "160MiB"        # or exactly "unlimited"
evidence_bytes_per_hour = "32MiB"      # "unlimited" lets a garbage flood fill the disk (IB-LED-008)
evidence_raw_bytes_per_second = "256KiB"
essential_reserve_bytes = "32MiB"
max_file_bytes = "32MiB"
on_full = "continue"                   # or "refuse_commands"

[retention]
max_age = "7d"                         # or "unlimited": no age warning
max_total_bytes = "8GiB"               # or "unlimited": no size warning
keep_recent = 10
warn_interval = "24h"
scan_interval = "10min"
```

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
  `max_buffer_bytes` (default 256 MiB, 1 MiB to 64 GiB), `min_free_bytes` (default 1 GiB, 0 to 1 PiB:
  recording refuses to start below it, `IB-REC-010`), `embed_definitions` (default true). Both sizes take
  bytes or a string such as `"64MiB"`, like the `[ledger]` and `[retention]` sizes. `ib record` warns (`IB-REC-010`)
  when the disk would fill within 10 minutes.
- **Splitting.** `--split 1GiB` or `--split 1h` starts `NAME.p0002.ibrec`, ... each a complete file
  naming the session, its part number and the previous part's last sequence number; part 1 stays
  open and locked until the session ends.

## Replay

`ib replay FILE... [--where EXPR] [--from T] [--to T] [--speed X] [--follow] [--follow-idle S]
[--format line|json|hex] [--defs recording|config] [--skip-damage] [--count N]` reads a recording
and prints what `ib listen` would have printed for it, plus what `listen` hides. It needs no
configuration and sends nothing anywhere (replay to the wire is a later stage).

```bash
ib replay /data/run1.ibrec                                    # every frame, as fast as possible
ib replay run1.ibrec --where 'msg == SAMPLE_APP_HK and CommandCounter > 0' --format json
ib replay run1.ibrec --from +90s --to +2m --speed 1           # that minute, at the recorded pace
ib replay run1.ibrec --where 'status != ok'                   # malformed, unknown and garbage frames
ib replay run1.ibrec --follow                                 # a recording still being written
```

- **Definitions.** A recording decodes with the CCDD and header profiles it embeds (decision D1), so
  it still decodes after the CCDD changes. They are loaded through the same strict loaders as files
  on disk (labelled `FILE#ccdd/<node>` in errors; the `python:` header escape hatch is refused) with
  the recording's own packet cap, and each must match the digest its META names. One that cannot be
  used, or that the recording does not carry (`embed_definitions = false`), is `IB-REC-009` (exit 1)
  with the way out: `--defs config` decodes with the current configuration's files (`-c PATH`),
  and `--format hex` needs no definitions at all. When a configuration is found and its files differ
  from the embedded ones, one `info IB-REC-009` line says so; the recording's still win.
- **What is shown.** Every frame of both directions, in order, inside the range and the filter. A
  decoded frame prints exactly as `listen` prints it (`--format line`) or as the same JSON record
  (`--format json`). A frame that does not decode is printed flagged instead of being dropped:
  `MALFORMED`, `UNKNOWN` (a MID or function code the CCDD lacks) or `GARBAGE` (serial bytes skipped
  while resynchronising), with the error code, the failing byte in brackets, the frame number
  (`frame=`, the recording's `seq`), its flags (`truncated`, `stale`) and its bytes in hex (the first
  256 bytes; JSON carries 1024 and a `status` key that decoded records lack). `--format hex` dumps
  every frame's bytes without decoding. `--where` is the `listen` filter language compiled against
  the recording's definitions, so `status == malformed` selects what `listen` cannot show.
- **Ranges.** `--from` and `--to` take ISO 8601 with `Z` or an offset (`2026-10-30T12:00:00Z`) or
  `+90s`, `+1m30s`, `+250ms` from the first frame; the range is half open (`from <= time < to`) on
  the frames' receive times. `--from` must be earlier than `--to` (`IB-CLI-001`, also when one is an
  offset and one a time, once the first frame is known). A `+offset` counts from the first frame
  actually read, after any damage that `--skip-damage` goes past. Where the file has a usable
  index, chunks outside the range or without a message the filter can match are skipped from their
  summaries: the summary line says how many were skipped unchecked, and damage inside a skipped
  chunk is not seen (its GAP records are still counted when they overlap the range). A missing or
  bad index falls back to reading every record (`info IB-REC-008`).
- **Pace.** `--speed 0` (default) runs as fast as possible; `--speed 1` follows the recorded time
  between frames, `--speed 2` twice as fast. Each backward step of the recorded clock between two
  frames counts as zero time (the frames after it keep their own spacing); a forward step is not
  shortened, so an hour of recorded silence is an hour.
- **Split recordings.** Part 1 of a split recording plays all its parts in order; several files
  play as given (same session, ascending part numbers, else `IB-CLI-001`). A part that is missing
  is `IB-REC-006` and exit 5 (not when `--count` ended the run first); the parts that exist after
  it still play.
- **Damage.** A torn tail, or a recording that was never closed, is a warning (`IB-REC-007`) after
  everything before it was replayed (exit 0); a recording a recorder still holds open is not called
  torn. Mid-file damage is `IB-REC-006` with its offset and span: replay stops there (exit 5), or
  with `--skip-damage` goes on at the next valid record (still exit 5; the lost frames show as a
  sequence gap). The end-of-run summary on stderr counts frames read, in range, shown, malformed,
  unknown and garbage, skipped chunks, GAP records (and the frames they say were lost) and damage.
- **Following.** `--follow` keeps reading a recording that is being written: at the end of the file
  it polls every 100 ms, treats a partly written last record as pending, and ends at the trailer
  (next part of a split recording first) or on a signal. A recorder that died would keep it waiting
  for ever, so when nothing new has arrived it probes the recorder's file lock
  (`flock(LOCK_SH | LOCK_NB)` on part 1, held until the session ends or the process dies): granted
  means the recorder is gone, the end of the file is read once more, and a partial record is a torn
  tail. Following a later part (`run.p0003.ibrec`) probes part 1's lock all the same, and a part that
  appears while following must belong to the same session. On a file system without usable locks
  (some NFS mounts) `--follow` needs `--follow-idle S`, which ends after S seconds without growth
  (the recorder may still be alive then, so the file is not called unclosed); a recorder that ran
  unlocked looks dead to the probe.
- **Output.** Replay blocks rather than drops: when the reader of stdout is slow, replay waits for
  room and loses no line, where `ib listen` has to keep up with the wire and drops lines with
  `IB-OUT-001` instead. A reader that goes away (`| head`) ends replay quietly (exit 0); stdout
  closed from the start (`>&-`) or failing is exit 4. "Shown" (in the summary and for `--count N`,
  which stops after N frames are shown) means accepted by the output queue; a reader that leaves
  at once may not have been handed the last of them. The ledger records the start (files, options) and the end (counts, how it
  ended); malformed frames are not ledgered one by one, the recording is their evidence.

## Export

`ib export FILE... [-o OUT] [--format csv|text|jsonl] [--message NAME ...] [--where EXPR]
[--from T] [--to T] [--header-fields] [--raw-enums] [--defs recording|config] [--skip-damage]
[--force] [--jobs N]` writes a recording to files other tools read. It decodes exactly as `ib replay` does
(same definitions, same `--where`, `--from`/`--to` and `--skip-damage`), needs no configuration
and sends nothing.

```bash
ib export run1.ibrec -o run1-csv                               # a directory: one CSV per message
ib export run1.ibrec --message SAMPLE_APP_HK -o hk.csv         # one message: one file
ib export run1.ibrec --message SAMPLE_APP_HK > hk.csv          # ... or to stdout
ib export run1.ibrec --where 'event.type >= error' --format jsonl -o errors.jsonl
ib export run1.ibrec --format text --from +90s --to +2m -o minute.txt
```

- **CSV tables.** The columns differ by message, so each message has its own table:
  `rx_time_utc` (ISO 8601, microseconds), `rx_time_ns`, `seq` (the recording's frame number),
  `node`, `link`, `mid` (hex), the header fields as `hdr.<name>` with `--header-fields`, then the
  payload fields in CCDD order (a payload field called like a fixed column is `<MESSAGE>.<field>`).
  Numbers are numbers (floats in the shortest form that round-trips, `inf`/`nan` as text);
  enum fields are their label (`--raw-enums`: the number; a value the enum lacks is its number);
  `char` fields are text; `bytes` fields are hex with a `0x` prefix (`0x00123456`; so is the `hex`
  of `malformed.csv`), which keeps a spreadsheet from reading `00123456` as the number 123456 or
  `1e300000` as a number at all. `text` and `jsonl` keep exactly what `ib replay` prints. Lines end
  in LF and cells are quoted as RFC 4180 says.
- **Spreadsheets reinterpret numbers.** A CSV carries no types, and Excel or LibreOffice guess: a
  number with more than 15 significant digits (a `uint64`, a nanosecond time such as `rx_time_ns`)
  is rounded to 15 and shown as `1.7E+18`, a text of digits keeps no leading zeros, and `1e30` is a
  number. The file is exact; the application's guess is not. When the values matter, import the
  columns as text (Excel: Data > From Text/CSV, set the column type to Text; LibreOffice: the
  import dialog's column type "Text"), or read the CSV with a tool that keeps types.
- **Which files.** One `--message` is a single table, written to `-o FILE` or to stdout. Several
  `--message`s, or none, make a directory (`-o DIR`, required) with `<MESSAGE>.csv` for each message
  that occurs and `malformed.csv` for every frame that did not decode (`seq`, time, `node`, `link`,
  `dir`, `mid`, `status`, `code`, `offset`, `reason`, `len`, `orig_len`, `flags`, and all of the
  frame's bytes as `hex`), so the evidence is exported with the data. A single-file CSV has no place
  for frames that did not decode: they are counted in a warning, never dropped silently. A message
  that two nodes define with different layouts gets `<MESSAGE>.2.csv` for the second; a message
  called `malformed` is `malformed.1.csv`. A single table (one `--message`, to a file or stdout)
  cannot hold two layouts: when the recording's nodes define that message differently the export is
  refused (`IB-CLI-001`) before any row is written, unless a `--where` that pins the node (`node ==
  fc`, `node in [...]`, `node != gc`) leaves one layout. `--format text` and `jsonl` are one file or stdout, with
  the lines and records `ib replay` prints (frames that did not decode flagged, as replay shows them).
- **Strings are untrusted.** Every string cell (names, enum labels, `char` text, hex, the reason of a
  malformed frame) has its control characters and escape sequences escaped, so a newline or an
  escape sequence from a packet cannot add a line or drive a terminal, and a cell that starts with
  `=`, `+`, `-`, `@` (or their fullwidth forms), a tab or a carriage return gets a leading `'`, so a
  spreadsheet does not read it as a formula; leading spaces do not hide the lead (importers trim
  them), so ` =1+1` is prefixed too. Numbers are never touched.
- **The output is written whole or not at all.** The data goes to `.<name>.<16 hex>.tmp` beside OUT
  (a directory of the same form for a CSV directory), is synced, and is renamed to OUT without
  replacing anything; a failure, a signal or a full disk removes the temporary file and leaves
  nothing. An existing OUT is refused (`IB-EXP-002`) unless `--force` replaces a regular file of
  yours (not one another process holds locked, not a directory or a symbolic link); a CSV directory
  is never replaced, `--force` or not. "Holds locked" means an advisory `flock` (what `ib record`
  and the other ironbird tools take): a program that writes the file without locking it, or locks it
  with POSIX `fcntl` locks, is not detected, so `--force` over a file something else is writing
  can still replace it. Two refusals no `--force` overrides: OUT is the recording
  being exported (same file, however the path is spelled, checked again just before the rename),
  and OUT is inside the state directory (which holds the ledger only) or named like a file `ib
  ledger prune` or `ib rec prune` manages (a ledger part, a reserve, a salvage temporary or a
  recording) in the recording directory, since a prune would later delete it. A write that fails is
  `IB-EXP-001`; a missing directory for OUT says so and that nothing was created. A killed export
  can leave `.<name>.<16 hex>.tmp` beside OUT: the next export of that name warns once and does not
  delete it. A directory export keeps at most as many tables open as the descriptor limit allows
  (`ulimit -n`; if the limit is still reached the error says to raise it).
- **Exit codes.** As for `replay`: 0 normal (a torn tail or a recording never closed is a warning),
  5 when the recording was damaged mid-file (the export ends there, or with `--skip-damage` goes
  past it) or a part of a split recording is missing; the output exists and holds everything read.
  1 for `IB-REC-004..009`, `IB-EXP-*`; 2 for usage and `IB-FLT-*`.
- **Cost and workers.** A recording of 8 MiB or more is decoded by a pool of worker processes, one per
  core by default (`--jobs N`, or `[export] jobs` in the configuration; `--jobs 1` decodes in the
  calling process, and so does a smaller recording, since starting the workers costs about a quarter
  of a second). The file is cut into byte ranges (`[export] range_bytes`, 4MiB); each worker reads
  its range itself through the same reader as every other command, decodes it exactly as the single
  process does and sends back only the text of the rows, which the export writes in file order, at
  most two ranges per worker in flight, so memory does not grow with the recording. **The output is
  the same bytes in every format and with every option, with one worker or many**, and a damaged
  recording ends at the same place with the same coded error (`IB-REC-006`/`007`, exit 5), because
  a range that does not hold up (damage, a definition that appeared late, a sequence number that
  goes back, a record boundary found in the wrong place) is read again, with the exact state, before
  it is used. A recording read through the index because `--from`/`--to` or `--where` rule chunks
  out, one still being written, and a followed one are exported in the calling process. A worker
  that dies (killed, out of memory) or cannot start is `IB-EXP-003` (the message says to run again
  or with `--jobs 1`; the cause is in the ledger), and Ctrl-C or SIGTERM ends the export (130/143),
  terminates the workers and leaves nothing: they ignore SIGINT and SIGHUP, which only the export
  acts on, and exit by themselves if the export is killed. `[export] pool_min_bytes` (8MiB; `0`
  for always) moves the size from which workers are used. The ledger records the start (options,
  workers), the pool (`export.pool`: ranges, ranges read again) and the end (counts per table, how it
  ended). Measured on the development host (4 vCPU): 62-field, 200-byte packets as CSV, 18,100 rows/s
  in one process and 51,000 to 65,000 rows/s with four workers (the host is shared; two runs).

## Throughput

Design target 10k packets/s (200-byte packets) with zero capture loss; hard floor 1k packets/s.
`tests/bench/` holds the gate: a separate loopback process sends valid telemetry at a fixed rate into an
`ib record` subprocess, and the run must record every datagram, in order, with no kernel or recorder-buffer
drop, nothing left unread by the `--count` loop (`queue_not_shown` of the `record.capture` ledger record counts
the frames that loop skipped; the recorder sees every frame regardless, so it is not recording loss), a bounded
buffer peak and RSS, and a reported CPU share. The blaster's catch-up after a stall is capped at 64 datagrams per
millisecond.

```bash
pytest tests/bench -n0 -s                                  # the 1k floor for 15 s: what CI runs
IRONBIRD_BENCH_10K=1 pytest tests/bench -n0 -s             # the 10k target for 60 s, on the dev host
IRONBIRD_BENCH_10K=1 IRONBIRD_BENCH_UPDATE=1 pytest tests/bench -n0 -k target   # new baseline
IRONBIRD_BENCH_EXPORT=1 pytest tests/bench/test_export_hour.py -n0 -s           # the export cost
IRONBIRD_BENCH_EXPORT=1 pytest tests/bench/test_export_pool_bench.py -n0 -s           # ... with workers
```

The 10k run is compared with `tests/bench/baseline.json` (CPU per frame, peak RSS); more than 20% above it
fails. Measured on the development host (4 vCPU Xeon 2.1 GHz): at 10k packets/s `ib record` uses about half
a core (48-51 us per frame), RSS stays near 32 MiB, the buffer peaks under 1.5 MiB of its 256 MiB, and
141 MiB are written per minute (8.3 GiB per hour). `ib export` runs at about 36,000 rows/s as CSV on the
75-byte housekeeping packet and 18,100 rows/s on the 200-byte, 62-field packet of the 10k target; with four
workers (the default on four cores) the 200-byte export runs at 51,000 to 65,000 rows/s (x3.1 to x3.6), so a
one-hour recording takes about 200-220 s at 1k packets/s in one process and 55-70 s with workers, and 33-36
minutes at 10k packets/s in one process and 9-12 minutes with workers. The ironbird side
never loses a datagram it was handed; the cFS side of a 10k packets/s source is limited by TO_LAB's
software-bus pipe (`tools/cfs/NOTES.md`, section 8).
