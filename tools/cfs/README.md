# Real cFS verification target

Builds and runs NASA's public cFS bundle (native Linux simulation, `native_std`) as the
acceptance target for ironbird stages. Everything stays outside the source tree; no root.

    tools/cfs/build.sh          # clone (pinned) + build, ~1-2 min on 4 cores
    tools/cfs/run.sh            # foreground; Ctrl-C to stop
    tools/cfs/run.sh > cfs.log 2>&1 &   # background

Pinned version: bundle tag `v7.0.1` at commit `088b2fa828db9ff7e00733f1908e0eeb59f66ce3`
(`build.sh` refuses a checkout whose tag resolves elsewhere and writes the bundle plus every
submodule commit to `$CFS_BUILD_DIR/PINNED_VERSION`).

| Variable | Default | Used by |
|----------|---------|---------|
| `CFS_BUILD_DIR` | `/home/user/build/cfs` | both |
| `CFS_REF` / `CFS_COMMIT` | `v7.0.1` / its commit | build |
| `CFS_CONFIG` | `native_std` | both |
| `JOBS` | `nproc` | build |
| `CFS_APPS` | `sch_lab ci_lab to_lab sample_app` (`ALL` = stock startup script) | run |
| `CFS_RUN_UID` | `65534` (only used when started as root) | run |

Behind the agent proxy a failed clone is a proxy problem, see `/root/.ccr/README.md`.

`run.sh` copies `build-native_std/exe/cpu1` to `$CFS_BUILD_DIR/run/cpu1` on every start
(clean power-on, build tree untouched) and starts `core-cpu1` there. Success looks like:

    CI_LAB listening on UDP port: 1234
    ... TO_LAB 1: TO Lab Initialized ... Awaiting enable command.
    ... SAMPLE_APP 1: Sample App Initialized ...
    CFE_ES_Main: CFE_ES_Main entering OPERATIONAL state

## Talking to it

- Commands: raw cFS packets, one per UDP datagram, to `127.0.0.1:1234` (CI_LAB).
- Telemetry: send `TO_LAB_ENABLE_OUTPUT` (MID `0x1880`, FC 6, 16-byte NUL-padded IPv4 text,
  e.g. `127.0.0.1`), then receive datagrams on UDP `2234`. Housekeeping then arrives every
  4-6 s; a `*_SEND_HK` command requests it immediately.
- `ccdd/` stand-in for these messages: `examples/ccdd/cfs.json`.

`clean.sh` removes the SysV shared-memory segments the native PSP strands after every run
(keyed off the run directory's `.*keyfile`s; `run.sh` calls it before recreating the run
directory, otherwise a key collision eventually fails the start with `Cannot shmget ...
Invalid argument`). Run it after stopping cFS to leave nothing behind.

Automated acceptance: `IRONBIRD_CFS=1 pytest tests/cfs -n0` (`tests/cfs/test_acceptance.py`).

Host caveats: only one instance at a time per host (the cFE RAM disk is the fixed directory
`/dev/shm/osal:RAM`, and the CI_LAB/TO_LAB ports are fixed); a copy of that directory left
by a different user makes `/ram` file commands fail, and `run.sh` stops with a message if
it finds one.

Wire-format findings, MIDs, command codes and payload layouts with source citations, plus
behaviours that differ from what the plan assumed (telemetry port 2234, no flight-side
checksum verification, 16-byte telemetry header): `NOTES.md`.
