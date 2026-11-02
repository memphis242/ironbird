# Real cFS verification target

Builds and runs NASA's public cFS bundle (native Linux simulation, `native_std`) as the
acceptance target for ironbird stages. Everything stays outside the source tree; no root.

    tools/cfs/build.sh          # clone (pinned) + build, ~1-2 min on 4 cores
    tools/cfs/run.sh            # foreground; Ctrl-C to stop
    tools/cfs/run.sh > cfs.log 2>&1 &   # background

Pinned version: bundle tag `v7.0.1` at commit `088b2fa828db9ff7e00733f1908e0eeb59f66ce3`
(`build.sh` refuses a checkout whose tag resolves elsewhere. After a *successful* build it
writes the checked-out tag and commit plus every submodule commit to
`$CFS_BUILD_DIR/PINNED_VERSION`; a failed build leaves no such file.)

`run.sh` allows one instance per `$CFS_BUILD_DIR` (a `flock` on `run/.run.lock`), because it
recreates the run directory on every start. `clean.sh` removes only shared-memory segments that
belong to the invoking user, derive from the run directory's key files, and have nothing attached.

| Variable | Default | Used by |
|----------|---------|---------|
| `CFS_BUILD_DIR` | `/home/user/build/cfs` | both |
| `CFS_REF` / `CFS_COMMIT` | `v7.0.1` / its commit | build |
| `CFS_CONFIG` | `native_std` | both |
| `JOBS` | `nproc` | build |
| `CFS_APPS` | `sch_lab ci_lab to_lab sample_app` (the token `ALL` keeps the stock startup script; add `ib_test` to start ironbird's own app, e.g. `ALL ib_test`) | run |
| `CFS_RUN_UID` | `65534` (only used when started as root) | run |

EL8: CI also builds and runs this on `oraclelinux:8` (gcc 8.5, cmake and git from the EL8
AppStream, bash 4.4, util-linux 2.32) as a non-root user; `tools/ci/el8-setup.sh` is the package
list those hosts need (`flock`, `setpriv` and `ipcrm` come from util-linux).

`apps/ib_test` is ironbird's own cFS app: it gives the acceptance tests the variable-length packet that
stock cFS lacks (a counted array and a tail string, decision D6). `build.sh` copies each directory of
`tools/cfs/apps/` into the checkout's `apps/`, lists it once in `sample_defs/targets.cmake` and checks
that `cf/<app>.so` was built; `run.sh` adds `ib_test` to the startup script when `CFS_APPS` names it.
Its commands are `IB_TEST_NOOP`, `IB_TEST_RESET_COUNTERS`, `IB_TEST_SET_DATA` (counted array and tail
string, stored and published as `IB_TEST_VAR`), `IB_TEST_SEND_VAR` and `IB_TEST_SEND_HK`, and a throughput generator
(`IB_TEST_GEN_START Rate=1..10000 Count=N`, `IB_TEST_GEN_STOP`, `IB_TEST_GEN_SEND_STATUS`) that publishes
fixed-size 200-byte telemetry (`IB_TEST_GEN`, with `IB_TEST_GEN_STATUS` at the end of a run); telemetry MIDs
0x08E0 (HK) and 0x08E1 (VAR) are not in TO_LAB's stock table, so a test adds them with `TO_LAB_ADD_PKT`
(`BufLimit` 0..255, below TO_LAB's telemetry pipe depth, 1000 in this build); 0x08E2 (GEN) and 0x08E3
(GEN_STATUS) are in the subscription table this build installs (`sample_defs/tables/cpu1_to_lab_sub.c`) with a
`BufLimit` of the pipe depth, more than `TO_LAB_ADD_PKT` can carry.
The layouts are in `apps/ib_test/fsw/src/ib_test.h` and `examples/ccdd/cfs.json`.

`sample_defs/` holds ironbird's cFS configuration: `build.sh` copies each file (subdirectories too) into the
checkout's `sample_defs/` (it refuses the whole install if any name is a stock file, and removes an earlier run's
file that is no longer here, tracked in `$CFS_BUILD_DIR/IRONBIRD_SAMPLE_DEFS`), where cFS reads mission overrides.
`cpu1_osconfig.cmake` raises OSAL's maximum queue depth, `cpu1_to_lab_internal_cfg_values.h` sets TO_LAB's telemetry
pipe to that depth (1000), and `tables/cpu1_to_lab_sub.c` subscribes the generator's messages with a per-message limit
of that depth, so the 10k packets/s generator test can be strict; see `NOTES.md` section 8 for the numbers and for the
`fs.mqueue.msg_max` the host needs.

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
Invalid argument`). Run it after stopping cFS to leave nothing behind. It removes only the
unattached segments of the uid cFS ran as: the invoking uid, or `CFS_RUN_UID` (default 65534)
when invoked as root, which is who `run.sh` drops to. Uids are compared as numbers from
`/proc/sysvipc/shm`, never as the (truncated) names `ipcs` prints.

Automated acceptance: `IRONBIRD_CFS=1 pytest tests/cfs -n0` (`tests/cfs/test_acceptance.py`).

Host caveats: only one instance at a time per host (the cFE RAM disk is the fixed directory
`/dev/shm/osal:RAM`, and the CI_LAB/TO_LAB ports are fixed); a copy of that directory left
by a different user makes `/ram` file commands fail, and `run.sh` stops with a message if
it finds one.

Wire-format findings, MIDs, command codes and payload layouts with source citations, plus
behaviours that differ from what the plan assumed (telemetry port 2234, no flight-side
checksum verification, 16-byte telemetry header): `NOTES.md`.
