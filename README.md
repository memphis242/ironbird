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
and its pip/venv packaging, util-linux and coreutils versions, no SELinux enforcement in the
container, and running without privileges.

What they do not match: containers share the runner's Ubuntu kernel, so kernel-level behaviour
is not EL8's. That covers `net.core.rmem_max` and the UDP receive buffer the kernel grants,
`/proc/net/udp` columns, `SO_RXQ_OVFL` drop counts, `flock` and SysV IPC/mqueue limits, and
serial hardware. SELinux policy, `firewalld`, systemd and site-specific sysctls are absent. Run
the opt-in acceptance tests on a real EL8 host before relying on kernel-dependent behaviour.

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
(a flood faster than the disk) a new record is refused and counted, and queued records are never
evicted; one `ledger.dropped` record in the ledger says how many. Packet and wire-error records
have a capped share of the queue so a flood of them is unlikely to crowd out actions and commands,
but capture is not guaranteed when the queue is full. A `[ledger n=N]` pointer names the record
queued for the latest occurrence; it is written unless the ledger fails (`IB-LED-001`) or the
process is killed first.

Planned (Stage 2e): recordings embed the CCDD and header profiles they were captured with, so a
recording is exactly as sensitive as those definition files; share it accordingly.

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | Normal end: done, `--count` reached, `--timeout` elapsed without `--count`, or the reader of stdout went away (`\| head -1`) |
| 1 | Runtime error (`error IB-...`), `find` with no match |
| 2 | Usage, configuration, CCDD or header-profile error |
| 3 | `listen`, `events` or `respond` had `--count N` and `--timeout` elapsed first |
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
`IB-TX-004`; if the kernel grants a smaller receive buffer than requested, `IB-TX-005` says to
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

Real-cFS acceptance tests (opt-in, serial, need `tools/cfs/build.sh` first):

```bash
IRONBIRD_CFS=1 pytest tests/cfs -n0      # or: pytest tests/cfs -m cfs -n0
```

A plain `pytest` skips them. They start and stop cFS themselves and need UDP 1234 and 2234
and `/dev/shm/osal:RAM` free (one instance per host).

Test corpus of malformed inputs: `tests/corpus/malformed/`. Add a file there for every parser bug.
