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
queued for the latest occurrence; it is written unless the ledger is still stalled at exit
(`IB-LED-001`) or the process is killed first. After the ledger rotates into a second part the
pointer names the part too: `[ledger n=1234 file=<stamp>-<session>.p0003]`.

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
  `max_buffer_bytes` (default 256 MiB), `min_free_bytes` (default 1 GiB: recording refuses to start
  below it, `IB-REC-010`), `embed_definitions` (default true). `ib record` warns (`IB-REC-010`)
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
[--force]` writes a recording to files other tools read. It decodes exactly as `ib replay` does
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
- **Cost.** One pass over the file in one process, with a few MiB of buffer whatever the size of the
  recording; about 40,000 rows per second as CSV on the development host. The ledger records the
  start (options) and the end (counts per table, how it ended).

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | Normal end: done, `--count` reached, `--timeout` elapsed without `--count`, or the reader of stdout went away (`\| head -1`) |
| 1 | Runtime error (`error IB-...`), `find` with no match; `ib rec check` found damage; a recording failed to write (`IB-REC-002`) |
| 2 | Usage, configuration, CCDD or header-profile error |
| 3 | `listen`, `events` or `respond` had `--count N` and `--timeout` elapsed first |
| 5 | The output was produced, but the input recording was damaged mid-file: `ib rec repair` left valid frames after the damage out of the copy; `ib replay` or `ib export` stopped at the damage (or, with `--skip-damage`, went past it) or a part of a split recording is missing |
| 6 | A command or reply went out but its ledger record was never written, or may have been lost to a deferred sync or close error (`IB-LED-004`). Same tier as 4 and wins over 4 and 5; an error (1, 2), 3 or a signal takes precedence. A `respond` that withheld replies under `on_full = "refuse_commands"` exits 1 |
| 4 | The command otherwise succeeded but stdout lines were lost (`IB-OUT-002`): dropped while stdout was not draining, abandoned when a signal cut the final drain short, or stdout is closed (`>&-`). An error (1, 2), 3, 5 or a signal (128+n) takes precedence. A `send` that exits 4 still went out |
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
cannot recover ends the command with that error. If the kernel grants a smaller receive buffer than requested, `IB-TX-005` is shown as a
warning (once per session; capture continues and exit codes are unchanged): ask an admin to
raise `net.core.rmem_max` (`sysctl -w net.core.rmem_max=...`).

The ledger directory is an absolute `$IRONBIRD_STATE_DIR` (a relative one is refused), else an absolute `$XDG_STATE_HOME/ironbird`,
else `~/.local/state/ironbird`. `send` and `respond` refuse to run (`IB-LED-002`, nothing sent)
when the state directory is unusable: a relative path, no home, no permission, a read-only file
system, a path through a file. When the directory is usable but the disk or the system fails
(disk or quota full, I/O error, out of descriptors, a hung file server), they send and warn
(`IB-LED-004`, exit 6 if the record is never written) under the default `on_full = "continue"`,
or refuse (exit 1) under `on_full = "refuse_commands"`.

## Serial links

A link with `transport = "serial"` carries both directions over one device (a UART, or a USB
adapter such as `/dev/serial/by-id/usb-FTDI_...`). You need access to the device (usually the
`dialout` group); ironbird never needs root. `examples/cfs-serial.toml` is a full example.

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

| Key | Values |
|-----|--------|
| `serial.device` | absolute path, required |
| `serial.baud` | 50 to 12000000, required |
| `serial.data_bits` | 5, 6, 7, 8 (default 8) |
| `serial.parity` | `none` (default), `even`, `odd`, `mark`, `space` |
| `serial.stop_bits` | 1 (default), 1.5, 2 (Linux sends 2 for 1.5) |
| `serial.flow` | `none` (default), `rtscts`, `xonxoff` |
| `framing.kind` | `header_length` (the only one): the header profile's length field cuts the stream |
| `framing.sync` | hex marker of up to 8 bytes that precedes every packet (consumed, not recorded); default none |
| `framing.max_idle_ms` | 1 to 10000 (default 50): silence after which a pending packet is settled |
| `framing.require_known_mid` | default `true`: a header is only believed if its MID is in the CCDD and the length fits that MID's payload range |

How the stream is cut. A byte stream has no packet boundaries, so a candidate header must look
right (length between the smallest and the largest packet, the fixed fields, a known MID with a
payload size its CCDD allows) and, with a checksum in the profile, the whole packet must pass it.
A profile without a checksum proves less, so a packet is also confirmed by a plausible header
right behind it or by `max_idle_ms` of silence: the last packet before a pause waits that long, and
a packet followed at once by line noise is not believed (use a header profile with a trailer
checksum, such as `examples/headers/ccsds_pec_tlm.toml`, where that matters). Bytes that are not
packets are kept, not dropped: they are recorded as `garbage` frames, a partial packet the line
leaves incomplete is a `stale` frame, and each run is reported as `IB-TX-007` (coalesced; the
ledger has the first bytes). A recording therefore holds the line byte for byte.

Frame times are taken when the read that completed a frame returns; a USB adapter adds its own
latency timer (an FTDI defaults to 16 ms). That is documented, not corrected.

One process owns the device at a time (`flock`): a second `ib` on it fails with `IB-TX-006`, so a
`listen` and a `send` take turns (a `respond` on the flight side answers on the same device).
Errors a user can act on: a missing device (`IB-TX-001`: check the cable, list
`/dev/serial/by-id`), no permission (`IB-TX-001`: ask an admin to add you to the device's group),
a device already in use (`IB-TX-006`). If the device goes away after it was opened (an unplugged
adapter, `EIO`/`ENXIO`), the link is down, not the command: `IB-TX-008` is shown once, the device is
reopened with a backoff of 0.5 s to 5 s, a recording gets `link_down` and `link_up` marks and one
`link_down` gap for the outage (its frame count is unknown, so the gap counts the interruption), and
`IB-TX-008` is shown once more as an info line when the link is back. A `send` while the link is down is
not sent (`IB-TX-008`). Closing the device may drop DTR on real hardware (the Linux default); a
device that resets on DTR needs that handled on its side.

## Events and telemetry categories

cFS events are the most important telemetry. `ib events [--node X ...] [--min-type error]
[--format line|json]` listens on every telemetry link of all (or the selected) nodes and
prints only events: time, node, app, type, id, message. They also show in `ib listen`. The
ledger records every received event in full.

Telemetry messages carry a `category` in the CCDD, in priority order: `event`,
`command_response`, `housekeeping`, `status`, `commanded`, `other` (default). An event message
also has an `event` mapping naming its app/id/type/message fields; see `docs/plan.md`
("Telemetry categories and events") and `examples/ccdd/cfs.json`.

## Variable-length payloads

A CCDD in schema `ironbird-ccdd/1` can end a message with fields whose size is on the wire:

```json
{"name": "Count",   "type": "uint16"},
{"name": "Entries", "type": "uint32[]", "count": "Count", "max": 64},
{"name": "Text",    "type": "char[]",   "length": "remaining", "max": 122}
```

The element type is a scalar, `char` or `bytes`. `count` names the fixed field that holds the number
of elements, `length` the one that holds the number of bytes, and `"remaining"` takes the rest of the
packet (last field only). `max` is required (`min` defaults to 0); the count field is an unsigned
integer in the fixed fields, which all come first, and must be able to hold `max`. At most 8 such
fields per message; the message's `min_payload`/`max_payload` follow from them.

A received packet is checked at ingest like any other: a payload outside the message's range is
`IB-DEC-007`, a count above `max` is `IB-DEC-008` (the offset is the count field), data that runs
past the end of the packet is `IB-DEC-009`, and bytes left after the last field are `IB-DEC-007`.
Values are tuples (`Entries`), text (`Text`, up to the first NUL) or bytes. `listen` shows the first 16
elements and `...(+N)`; `--format json` carries the whole list; a CSV cell holds `[1 2 3]`.

```bash
ib send fc MOCK_LOAD_DATA Entries=1,2,3 Text=hello     # Count is filled in; [1,2,3] works too
ib send fc MOCK_LOAD_DATA Count=9 Entries=1,2 Text=x --allow-count-mismatch   # fault injection
```

A `Count` that disagrees with the data is `IB-ENC-002` unless `--allow-count-mismatch` is given (a
usage error on a command with no count or length field); the ledger's `send.request` record carries
`allow_count_mismatch` and a `send.fault` record carries the `given` and `implied` value of each
count that went out wrong (nothing when they agree). A number array cannot be used in a
`--where` filter (`char[]` and `bytes[]` can).

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

## Header profiles

A header profile (`examples/headers/*.toml`) declares the header fields in wire order, as
big-endian bitfields packed MSB-first. What a profile can say beyond a field's width and value:

| Key | Where | Meaning |
|-----|-------|---------|
| `endian = "little"` | a field | stored least significant byte first; only for a field that starts on a byte boundary and fills whole bytes (a little-endian bitfield is ambiguous and refused) |
| `[[trailer.fields]]` | the profile | fields after the payload, fixed (`value = N`) or the one checksum; the payload is the packet minus header minus trailer |
| `span` | a checksum field | the bytes it covers: `"packet"` (default), `"header"`, `"payload"`, or `{ start = N, end = -M }` (byte N up to M bytes before the packet end); the checksum field is zeroed if it lies inside the span |
| `mid_fields` | the profile | any fields in any position; the MID is their values joined in list order, first listed most significant (cFE MsgId V2 spreads it over three places), at most 32 bits for a codec |
| `[time]` | the profile | `epoch = "1980-01-01T00:00:00Z"` (the instant the counter is zero; a zone is required, whole second, 1900..2200) and `leap_seconds = N` (counter = UTC - epoch + leap; a TAI counter has 37); applies to the `computed = "time_seconds"` and `"time_subseconds"` fields. Without `[time]` the counter is Unix seconds |

`crc16_ccitt` (CRC-16/CCITT-FALSE) is the CCSDS packet error control code; with the PEC as a trailer
it covers `{ start = 0, end = -2 }` (`examples/headers/ccsds_pec_tlm.toml`). At most one checksum per
profile. `ccsds_v1_cfs_tlm_cfe_time.toml` is `ccsds_v1_cfs_tlm.toml` with the cFE mission epoch
(1980-01-01, TAI, 37 leap seconds; `tools/cfs/NOTES.md`): `examples/ironbird.toml` (real cFS) uses
it, so `htime` is the right UTC instant. `ccsds_v1_cfs_tlm.toml` counts Unix seconds, which is what
the mock vehicle (`ib respond`, `examples/mock.toml`) stamps. `cfe_msgid_v2_{cmd,tlm}.toml` are the
cFE MsgId V2 headers, with the cFE defaults for the extended header (EDS version 1, little-endian
flag, system id 0x42 = the sample spacecraft id).

The decoded header time is `htime` in a filter (`--where 'htime >= 2026-10-29T12:00:00Z'`, UTC, with
the same time literals as `time`); it is false for a packet whose profile has no seconds field.
Trailer fields are `hdr.<name>` like header fields.

An input field of a profile (`default = N`) takes, highest priority first, `ib send NODE CMD --header field=N` (repeatable), the message's CCDD
`"header": {"field": N}`, and the profile default. Names must be input fields (the function code
field comes from the command); values must fit the field. A bad name or width is `IB-HDR-003` for
`--header` and `IB-ENC-004` when a CCDD is paired with the profile.

A profile is untrusted input like any other file: unknown keys, overlapping or repeated MID fields,
a span that cuts through its own checksum field or cannot fit any packet, a trailer that is not a
whole number of bytes, a contradictory byte order and a bad epoch are all `IB-HDR-002` with the
field path; a span that does not fit one particular packet is `IB-ENC-004` for a CCDD message and
`IB-DEC-005` for a received packet.

## Configuration

- `examples/ironbird.toml` - ground plus a real cFS flight computer (CI_LAB 1234, TO_LAB 2234), telemetry header time in the cFE epoch.
- `examples/mock.toml` - ironbird-vs-ironbird on distinct ports, with response rules, telemetry header time in Unix seconds.
- `examples/cfs-serial.toml` - ground plus a real cFS over a serial link, through `tools/cfs/serial_bridge.py` (a pty joined to CI_LAB / TO_LAB).
- `examples/headers/*.toml` - header profiles (bit widths, fixed/computed fields, checksum).
- `examples/ccdd/*.json` - placeholder CCDD JSON (`ironbird-ccdd/1`; `ironbird-ccdd/0` files still load).

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
