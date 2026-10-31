# cFS verification target: wire-format notes

Everything here was read from the checked-out source and then confirmed against a
running instance (UDP commands to CI_LAB, telemetry from TO_LAB). Nothing is from
memory. Paths are relative to the cFS checkout root
(`$CFS_BUILD_DIR/src`, default `/home/user/build/cfs/src`).

- Bundle: `nasa/cFS` tag `v7.0.1` (Draco), commit `088b2fa828db9ff7e00733f1908e0eeb59f66ce3`.
  Submodule commits are recorded by `build.sh` in `$CFS_BUILD_DIR/PINNED_VERSION`.
- Configuration built: `native_std` (`SIMULATION=native`, non-EDS, mission `sample`,
  spacecraft id `0x42`, `cpu1` processor id 1): `sample_defs/targets.cmake:77,132-133`,
  `target-configs.mk` (`PREP_OPTS_native_std`).
- Short file names: `msgdefs.h`, `msgstruct.h`, `extern_typedefs.h`, `msgids.h` mean the
  module's `config/default_<module>_<name>.h` (for example `msgdefs.h` in the TIME section is
  `cfe/modules/time/config/default_cfe_time_msgdefs.h`).
- Numbers below (sizes, offsets, MIDs, function codes) were also printed by a C probe
  compiled against the generated headers in `build-native_std/inc` and the app
  `config/` headers, so padding is as the compiler lays it out.

## 1. Build variant and header layout

This build uses the **primary-header-only, MsgId V1** variant, not the CCSDS
extended-header variant and not EDS:

- `cfe/modules/msg/CMakeLists.txt:30-53` selects `cfe_msg_initdefaulthdr_pri.c` +
  `cfe_msg_msgid_v1.c` when `MISSION_INCLUDE_CCSDSEXT_HEADER` is unset;
  `cfe/modules/msg/mission_build.cmake:11-16` selects `default_cfe_msg_hdr_pri.h`
  (the generated `build-native_std/inc/cfe_msg_hdr.h` includes exactly that file).
- Variants that exist in the source but are NOT in this build: extended header
  (`ccsds_hdr.h:73-84`, `default_cfe_msg_hdr_priext.h`), MsgId V2 (`cfe_msg_msgid_v2.c`),
  EDS (`apps/*/config/eds_*`). A real mission's profile may differ; ironbird's header
  profile must stay user-definable.

### CCSDS primary header (6 bytes, big-endian)

`cfe/modules/msg/fsw/inc/ccsds_hdr.h:51-68` (`CCSDS_PrimaryHeader_t`: three `uint8[2]`
words, "CCSDS headers must always be in network byte order", `ccsds_hdr.h:43`).
Masks: `cfe/modules/msg/fsw/src/cfe_msg_ccsdspri.c:30-41`. Big-endian accessors:
`cfe_msg_priv.h:47-68`.

| Bytes | Field | Mask / meaning |
|-------|-------|----------------|
| 0-1 | Stream ID word | `0xE000` version (0 here), `0x1000` type (1 = command, 0 = telemetry), `0x0800` secondary header present, `0x07FF` APID |
| 2-3 | Sequence word | `0xC000` segmentation flags (`3` = unsegmented, set by default: `cfe_msg_ccsdspri.c:49-59`), `0x3FFF` sequence count |
| 4-5 | Length | total packet length in bytes minus 7 (`CFE_MSG_SIZE_OFFSET`, `cfe_msg_ccsdspri.c:30,379,392-403`) |

The **MsgId is the whole 16-bit stream ID word** (`cfe_msg_msgid_v1.c:43`,
`(StreamId[0] << 8) + StreamId[1]`), so type and secondary-header bits are part of the
MID value (`CFE_SB_MsgId_Atom_t` is `uint32`, `cfe/modules/sb/config/default_cfe_sb_extern_typedefs.h:89`).
Consequences: command MIDs look like `0x18xx`, telemetry MIDs like `0x08xx`. There is no
separate APID to configure; the sample mission's "APID" is just the low 11 bits of the MID.

### Command packet = 8-byte header + payload

`cfe/modules/msg/option_inc/default_cfe_msg_hdr_pri.h:102-107` (`CFE_MSG_CommandHeader_t`
= `Msg` (6) + `Sec` (2)); secondary header `default_cfe_msg_sechdr.h:60-68`:

| Byte | Field |
|------|-------|
| 6 | `FunctionCode`; bits `0x7F` function code, `0x80` reserved (`cfe_msg_sechdr_fc.c:26,59`) |
| 7 | `Checksum` (8 bit) |

Payload starts at packet offset 8. The compile-time command initializer
`CFE_MSG_CMD_HDR_INIT` (`default_cfe_msg_hdr_pri.h:57-66`) sets sequence word to
`C0 00` (unsegmented, count 0).

### Telemetry packet = 16-byte header + payload

`default_cfe_msg_hdr_pri.h:113-118` (`CFE_MSG_TelemetryHeader_t` = `Msg` (6) + `Sec` (6)
+ `Spare[4]`); secondary header `default_cfe_msg_sechdr.h:73-76`:

| Bytes | Field |
|-------|-------|
| 6-9 | Seconds, **big-endian** uint32 (`cfe_msg_sechdr_time.c:60-63,103-104`) |
| 10-11 | Subseconds, **big-endian** uint16 = top 16 bits of the cFE 32-bit subseconds (LSB = 2^-32 s, so this field's LSB = 2^-16 s) (`cfe_msg_sechdr_time.c:64-65,102`) |
| 12-15 | `Spare[4]` - zero padding "to avoid compiler padding if payload requires 64 bit alignment" |

The time is `CFE_TIME_GetTime()`, stamped by `CFE_SB_TimeStampMsg` and again by the
origination action (`cfe_msg_integrity.c:41`). Payload starts
at packet offset **16, not 12**: this 4-byte spare is the one surprise versus the common
"6 + 6" expectation. Verified live: ES HK is 184 bytes = 16 + 168.

### Telemetry header time: epoch, scale and leap seconds

Read from the checked-out source (bundle v7.0.1, cFE `c5fb2b4`), not from memory. The native
build sets none of these in `sample_defs`, so the cFE defaults apply:

| Setting | Value | Where |
|---------|-------|-------|
| Epoch | 1980, day 1, 00:00:00.000000 = **1980-01-01T00:00:00** (not the GPS epoch 1980-01-06) | `CFE_MISSION_TIME_EPOCH_YEAR/DAY/HOUR/MINUTE/SECOND/MICROS` (`cfe/modules/time/fsw/inc/cfe_time_interface_cfg.h:186-202`, same in `cfe/cmake/sample_defs/example_mission_cfg.h:543-548`) |
| Epoch as Unix seconds | 315532800 | `CFE_MISSION_TIME_EPOCH_SECONDS` (`cfe_time_interface_cfg.h:205-206`); `CFE_TIME_Print` adds it to the seconds (`cfe_time_api.c:579`) |
| Time scale | TAI by default (`CFE_MISSION_TIME_CFG_DEFAULT_TAI true`) | `cfe_time_interface_cfg.h:53-54` |
| Leap seconds | 37 (`CFE_MISSION_TIME_DEF_LEAPS`); UTC = TAI - leap seconds (`CFE_TIME_GetUTC`) | `cfe_time_interface_cfg.h:162-163` |
| Initial MET / STCF | 1000 s / 1000000 s | `CFE_MISSION_TIME_DEF_MET_SECS`, `..._DEF_STCF_SECS` (`:150`, `:156`) |

The packet's time is `CFE_TIME_GetTime()` = MET + STCF, a TAI count of seconds since the epoch.
So a header seconds value S is the UTC instant `1980-01-01T00:00:00Z + S - 37 s`, and a fresh
`run.sh` (STCF 1000000, MET from 1000) should stamp packets with S near 1001000 = about
**1980-01-12T14:00Z**, in line with the simulated `1980-...` console lines (derived from the
defaults; the acceptance cases below check it against the live build). The profile
`examples/headers/ccsds_v1_cfs_tlm_cfe_time.toml` carries `[time] epoch = "1980-01-01T00:00:00Z"`
and `leap_seconds = 37`, and **`examples/ironbird.toml` (the real-cFS ground config) uses it**, so
`htime` is right without any edit. `ccsds_v1_cfs_tlm.toml` keeps the Unix epoch, which is what the
mock vehicle (`ib respond`) stamps its own telemetry with, and `examples/mock.toml` uses that one.
HK carries MET (`CurrentMET`), STCF (`AtToneSTCF`) and `AtToneLeapSeconds` of the same reference
as the stamp (`cfe_time_utils.c:391-397`), DIAG its own `CurrentTAI` and `CurrentUTC`
(`cfe_time_utils.c:444-447`). The `cfs` acceptance cases:
`test_listen_where_htime_selects_time_hk_in_the_cfe_epoch` (`--where` with a 1980 window) and
`test_header_time_equals_the_cfe_time_in_the_time_packets` (decoded `htime` of a TIME HK and a DIAG
equals `epoch + MET + STCF - leaps` / `epoch + CurrentUTC` of that same packet within 2 s).
If the mission configuration changes `EPOCH_*`, `CFG_DEFAULT_TAI` or `DEF_LEAPS`, edit the
`[time]` table to match; a UTC-scale mission uses `leap_seconds = 0`.

### MsgId V2 layout (not exercised by this build)

This build uses MsgId V1 (`cfe_msg_msgid_v1.c`). The V2 layout (`cfe_msg_msgid_v2.c:20-47`,
header `cfe/modules/msg/fsw/inc/ccsds_hdr.h:55-87`) builds the 16-bit MID from three places:
APID low 7 bits (primary header byte 1), the command/telemetry bit (primary header bit 3)
and the Subsystem low byte (extended header byte 7): `MID = subsystem << 8 | type << 7 | apid`.
`examples/headers/cfe_msgid_v2_{cmd,tlm}.toml` express it with
`mid_fields = ["subsystem_lo", "packet_type", "apid_lo"]`; unit tests only. Their input-field
defaults are the cFE ones (`CFE_MSG_SetDefaultCCSDSExt`, `cfe_msg_ccsdsext.c:41-54`): EDS version 1
(`CFE_PLATFORM_EDSVER`, `cfe_msg_defaults.h:56`), endian flag 1 = little (`CFE_PLATFORM_ENDIAN` is
`CCSDS_LITTLE_ENDIAN`, `default_cfe_core_private_internal_cfg.h:44`; the `#if` at `ccsdsext.c:45`
compares two names that no header in this checkout defines (both read as 0), so it is true either way), system id 0x42 (the spacecraft id
above, stored from `CFE_PSP_GetSpacecraftId()`, `ccsdsext.c:53`), playback 0.

### Byte order

- Primary header and telemetry time: **big-endian** (network order).
- Command function code/checksum: single bytes.
- **Payload structures: host (native) byte order**, plain C structs with natural
  alignment, no packing (`default_cfe_*_msgdefs.h`). On x86-64 and ARM Linux this is
  little-endian; the PSP reports "Host CPU Endian=LITTLE". cFE copies the struct bytes
  straight onto the wire. A big-endian flight CPU would emit big-endian payloads, so
  payload endianness is a per-target property (the CCDD `endian` field).
- `CFE_ES_MemOffset_t` is `uint64` (`cfe/modules/es/config/default_cfe_es_memaddress.h:50`),
  which forces 8-byte alignment in ES HK.

### Command checksum (exact algorithm)

`cfe/modules/msg/fsw/src/cfe_msg_sechdr_checksum.c`:

- `CFE_MSG_ComputeCheckSum` (35-50): `chksum = 0xFF; for each byte of the packet
  (length = header Length field + 7, i.e. the whole packet incl. primary header and
  the checksum byte): chksum ^= byte`.
- `CFE_MSG_GenerateChecksum` (58-90): zero `Sec.Checksum`, compute, store the result.
  Applies only to commands (`type == Cmd`, secondary header present, version 0), line 77.
- `CFE_MSG_ValidateChecksum` (98-125): packet is valid iff the XOR over the whole packet
  starting from `0xFF` equals `0`.

Equivalent ground implementation: set byte 7 to 0; `cksum = 0xFF ^ XOR(all packet bytes)`.

**The flight side never checks it in this build.** `CFE_MSG_ValidateChecksum` is called
only by SC (`apps/sc/fsw/src/sc_cmds.c:107,341`, not started in our config).
`CFE_MSG_VerificationAction` is a no-op that always accepts (`cfe_msg_integrity.c:60-76`),
and CI_LAB forwards ground packets with `CFE_SB_TransmitBuffer(buf, false)`
(`apps/ci_lab/fsw/src/ci_lab_app.c:267`) so `CFE_MSG_OriginationAction` (which would
recompute the checksum, `cfe_msg_integrity.c:30-53`, called at
`cfe/modules/sb/fsw/src/cfe_sb_priv.c:1112-1121`) is skipped. ES, TIME, SAMPLE_APP,
CI_LAB and TO_LAB contain no checksum references at all. Verified live: SAMPLE_APP and
ES accepted a NOOP with a deliberately wrong checksum and incremented their valid-command
counters. Plan.md's "cFS rejects bad checksums, so a successful command is the proof"
therefore does not hold for this target; correctness of ironbird's checksum can only be
verified against the algorithm above (or by enabling a validating app such as SC).

## 2. Message ID scheme

`MID = BASE | topic_id` (bitwise OR):
`cfe/modules/core_api/config/default_cfe_core_api_msgid_mapping.h:49,63,74`. Bases for
cpu1: `sample_defs/inc/global_core_api_base_msgid_values.h:30-37`
and `sample_defs/cpu1/cfe_core_api_base_msgid_values.h:32-33`:

| Kind | Base |
|------|------|
| cpu1 command | `0x1800` |
| cpu1 telemetry | `0x0800` |
| global command | `0x1860` (used by TIME `DATA_CMD`, `SEND_CMD` only) |
| cpu2 command/telemetry | `0x1900` / `0x0900` (not built into our `cpu1` instance) |

Topic IDs come from each module's `*_topicids.h`. Resulting MIDs (all confirmed by the
probe and on the wire):

| Message | Topic | MID |
|---------|-------|-----|
| ES command | 6 | `0x1806` |
| ES send-HK request | 8 | `0x1808` |
| ES HK telemetry | 0 | `0x0800` |
| TIME command | 5 | `0x1805` |
| TIME send-HK request | 13 | `0x180D` |
| TIME HK telemetry | 5 | `0x0805` |
| TIME diagnostic telemetry | 6 | `0x0806` |
| SAMPLE_APP command | 0x82 | `0x1882` |
| SAMPLE_APP send-HK request | 0x83 | `0x1883` |
| SAMPLE_APP HK telemetry | 0x83 | `0x0883` |
| CI_LAB command | 0x84 | `0x1884` |
| CI_LAB send-HK request | 0x85 | `0x1885` |
| CI_LAB HK telemetry | 0x84 | `0x0884` |
| TO_LAB command | 0x80 | `0x1880` |
| TO_LAB send-HK request | 0x81 | `0x1881` |
| TO_LAB HK telemetry | 0x80 | `0x0880` |
| TO_LAB data-types telemetry | 0x81 | `0x0881` |

Sources: ES `cfe/modules/es/fsw/inc/cfe_es_topicids.h:37-56`,
`cfe/modules/es/config/default_cfe_es_msgids.h:31-39`; TIME
`cfe/modules/time/fsw/inc/cfe_time_topicids.h:37-72`, `.../config/default_cfe_time_msgids.h:31-46`;
SAMPLE_APP `apps/sample_app/fsw/inc/sample_app_topicids.h:28-33`,
`apps/sample_app/config/default_sample_app_msgids.h:29-31`; CI_LAB
`apps/ci_lab/fsw/inc/ci_lab_topicids.h:28-35`, `.../config/default_ci_lab_msgids.h:29-32`;
TO_LAB `apps/to_lab/fsw/inc/to_lab_topicids.h:28-38`, `.../config/default_to_lab_msgids.h:29-32`.

## 3. Transport and how HK is requested

- **Uplink**: CI_LAB binds UDP `0.0.0.0:1234` (cpu1). Port =
  `CI_LAB_MISSION_BASE_UDP_PORT (1234, apps/ci_lab/fsw/inc/ci_lab_interface_cfg.h:50-51)
  + processor_id - 1` (`apps/ci_lab/fsw/src/ci_lab_app.c:168`). Startup log:
  `CI_LAB listening on UDP port: 1234`. One command per datagram, the raw cFS packet.
  Ingest (`apps/ci_lab/fsw/src/ci_lab_passthru_decode.c:76-110`): datagram shorter than 8
  bytes, or header length larger than the datagram, is dropped, bumps `IngestErrors` and
  raises an event; a datagram longer than the header length is accepted (trailing bytes
  ignored); max accepted datagram is 768 bytes (`ci_lab_internal_cfg.h:44-45`).
  Everything accepted is published on the software bus, so ground can inject *any* MID
  (including `*_SEND_HK` requests and telemetry MIDs).
- **Downlink**: TO_LAB sends from an ephemeral socket to `<dest_IP>:2234`. Port =
  `TO_LAB_MISSION_TLM_PORT (2234, apps/to_lab/fsw/inc/to_lab_interface_cfg.h:41)
  + processor_id - 1` (`apps/to_lab/fsw/src/to_lab_app.c:308`). **Plan.md says 1235; the
  source says 2234.** Each datagram is one raw cFS packet, length from the header
  (`apps/to_lab/fsw/src/to_lab_passthru_encode.c:41-52`). Nothing is sent until the
  enable-output command; `dest_IP` is the address cFS sends to (use `127.0.0.1` locally).
- **Which telemetry flows**: the installed subscription table is
  `sample_defs/tables/to_lab_sub.c:60-78` (not the all-commented default in
  `apps/to_lab/fsw/tables/to_lab_sub.c:34-52`): TO_LAB HK/data-types, CI_LAB HK, ES HK/app/
  memstats, EVS HK, SB HK, TBL HK, TIME HK and DIAG, SAMPLE_APP HK, and more (apps not
  started in our trimmed startup simply never publish). More MIDs can be added at run time
  with `TO_LAB_ADD_PKT`.
- **How HK is produced**: SCH_LAB publishes each module's `*_SEND_HK` command MID on a
  schedule, `sample_defs/tables/sch_lab_table.c:57-79` (`TickRate = 10`, entries in 0.1 s
  units): ES every 4.0 s, EVS 4.2, TIME 4.4, SB 4.6, TBL 4.8, CI_LAB 5.0, TO_LAB 5.2,
  SAMPLE_APP 5.4. A ground tool can also request HK on demand by sending the same
  `*_SEND_HK` MID through CI_LAB (8-byte command, function code 0); verified for ES,
  TIME, SAMPLE_APP, CI_LAB, TO_LAB. SAMPLE_APP copies its counters into the HK packet only
  when it handles that request (`apps/sample_app/fsw/src/sample_app_cmds.c:54-55`), so its
  HK always lags the command by up to one request.

## 4. Command and telemetry definitions

Function-code enums (only values 0-127 fit the header byte): ES
`cfe/modules/es/config/default_cfe_es_fcncode_values.h:37-62`, TIME
`cfe/modules/time/config/default_cfe_time_fcncode_values.h:37-54`, SAMPLE_APP
`apps/sample_app/config/default_sample_app_fcncode_values.h:37-42`, CI_LAB
`apps/ci_lab/config/default_ci_lab_fcncode_values.h:37-40`, TO_LAB
`apps/to_lab/config/default_to_lab_fcncode_values.h:37-45`.
"Payload offset" means bytes after the 8-byte command header or 16-byte telemetry header.

Length rule used by ES/TIME/SAMPLE_APP/CI_LAB: header-derived packet size must equal
`sizeof(<Cmd>_t)` exactly, otherwise an error event plus the app's error counter
(`cfe_es_dispatch.c:44-73`, `cfe_time_dispatch.c:37-66`, `sample_app_dispatch.c:39-70`,
`ci_lab_dispatch.c:35-64`). TO_LAB verifies no lengths at all (`to_lab_dispatch.c:38-83`).

### cFE ES (command MID `0x1806`)

| Name | FC | Payload |
|------|----|---------|
| NOOP | 0 | none (packet 8 bytes) |
| RESET_COUNTERS | 1 | none |
| CLEAR_SYS_LOG | 10 | none |
| WRITE_SYS_LOG | 11 | `char FileName[64]` at +0 (`default_cfe_es_msgdefs.h:58-62`, `CFE_MISSION_MAX_PATH_LEN` = 64 at `core_api/fsw/inc/cfe_core_api_interface_cfg.h:58`); packet 72 bytes |

`WRITE_SYS_LOG` (`cfe/modules/es/fsw/src/cfe_es_task.c:1452-1495`; dispatch
`cfe_es_dispatch.c:164-168`): the file name is parsed by `CFE_FS_ParseInputFileNameEx`;
an empty name selects the default `/ram/cfe_es_syslog.log`
(`cfe/modules/es/fsw/inc/cfe_es_internal_cfg.h:430`). On success `CommandCounter++`,
otherwise `CommandErrorCounter++`. Only the OSAL virtual mounts exist: `/cf` maps to
`<run dir>/cf` (`psp/fsw/pc-linux/src/cfe_psp_start.c:378`) and `/ram` is a OSAL volatile
disk kept in `/dev/shm/osal:RAM` (`osal/src/os/posix/src/os-impl-filesys.c:136,181`),
so `/ram/foo.dat` appears as `/dev/shm/osal:RAM/foo.dat` (verified, 2004 bytes). That
directory name is fixed per host: two cFS instances (or two users) collide, and a copy
left by another user makes `/ram` writes fail with "Permission denied" (`run.sh` checks).
Any other command code increments `CommandErrorCounter` (`cfe_es_dispatch.c:255-262`).
Send-HK request: MID `0x1808`, FC 0, 8 bytes (`cfe_es_dispatch.c:275-290`).

ES HK telemetry, MID `0x0800`, packet 184 bytes, payload 168
(`default_cfe_es_msgdefs.h:264-359`, struct in `default_cfe_es_msgstruct.h:272-276`). Payload
offsets (compiler-verified):

| Off | Type | Field |
|-----|------|-------|
| 0 | u8 | CommandCounter |
| 1 | u8 | CommandErrorCounter |
| 2 | u16 | CFECoreChecksum |
| 4,5,6,7 | u8 | CFEMajor/Minor/Revision/MissionRevision (live: 7,0,1,0) |
| 8-11 | u8 | OSAL Major/Minor/Revision/MissionRevision |
| 12-15 | u8 | PSP Major/Minor/Revision/MissionRevision |
| 16 | u64 | SysLogBytesUsed |
| 24 | u64 | SysLogSize |
| 32 | u32 | SysLogEntries |
| 36 | u32 | SysLogMode (0 overwrite, 1 discard: `cfe_es_extern_typedefs.h:42-47`) |
| 40 | u32 | ERLogIndex |
| 44 | u32 | ERLogEntries |
| 48 | u32 | RegisteredCoreApps |
| 52 | u32 | RegisteredExternalApps |
| 56 | u32 | RegisteredTasks |
| 60 | u32 | RegisteredLibs |
| 64 | u32 | ResetType (1 processor, 2 power-on: `psp/fsw/inc/cfe_psp_watchdog_api.h:68-69`) |
| 68 | u32 | ResetSubtype |
| 72 | u32 | ProcessorResets |
| 76 | u32 | MaxProcessorResets |
| 80 | u32 | BootSource |
| 84 | u32 | PerfState |
| 88 | u32 | PerfMode |
| 92 | u32 | PerfTriggerCount |
| 96 | u32[4] | PerfFilterMask (`CFE_MISSION_ES_PERF_MAX_IDS`=128, /32) |
| 112 | u32[4] | PerfTriggerMask |
| 128 | u32 | PerfDataStart |
| 132 | u32 | PerfDataEnd |
| 136 | u32 | PerfDataCount |
| 140 | u32 | PerfDataToWrite |
| 144 | u64 | HeapBytesFree (0 on this PSP) |
| 152 | u64 | HeapBlocksFree |
| 160 | u64 | HeapMaxBlockSize |

### cFE TIME (command MID `0x1805`)

Dispatch `cfe_time_dispatch.c:73-208`; handlers `cfe/modules/time/fsw/src/cfe_time_task.c`.
The default platform is a **time server** (`cfe_time_internal_cfg.h:53`, `CFG_SERVER true`,
`CFG_CLIENT false`), so the "server only" commands are accepted; without it they bump the
error counter (`cfe_time_task.c:964-973`).

| Name | FC | Payload (offsets after the command header) |
|------|----|---------------------------------------------|
| NOOP | 0 | none |
| RESET_COUNTERS | 1 | none |
| SEND_DIAGNOSTIC | 2 | none (publishes DIAG telemetry, `cfe_time_task.c:530`) |
| SET_SOURCE | 3 | `int16 TimeSource` +0 (1 internal, 2 external; needs `CFG_SOURCE`, off by default) |
| SET_STATE | 4 | `int16 ClockState` +0 (`msgdefs.h:62-69`; -1 invalid, 0 valid, 1 flywheel, `extern_typedefs.h:118-146`) |
| ADD_DELAY / SUB_DELAY | 5 / 6 | `u32 Seconds` +0, `u32 MicroSeconds` +4 (clients only) |
| SET_TIME | 7 | `u32 Seconds` +0, `u32 MicroSeconds` +4 (server; `cfe_time_task.c:824`) |
| SET_MET | 8 | same layout |
| SET_STCF | 9 | same layout; usec must be < 1000000 (`cfe_time_task.c:938-980`) |
| SET_LEAP_SECONDS | 10 | `int16 LeapSeconds` +0 (`msgdefs.h:54-57`); packet 10 bytes |
| ADD_ADJUST / SUB_ADJUST | 11 / 12 | `u32 Seconds` +0, `u32 MicroSeconds` +4 (`cfe_time_task.c:1034-1105`) |
| ADD/SUB_ONE_HZ_ADJUSTMENT | 13 / 14 | `u32 Seconds` +0, `u32 Subseconds` +4 |
| SET_SIGNAL | 15 | `int16 ToneSource` +0 (1 primary, 2 redundant) |

`TimeCmd_Payload` = `msgdefs.h:94-98`; packet 16 bytes. Send-HK request: MID `0x180D`
(topic 13), FC 0.

TIME HK, MID `0x0805`, packet 48 (`msgdefs.h:125-168`), payload offsets: 0 u8
CommandCounter, 1 u8 CommandErrorCounter, 2 u16 ClockStateFlags (bits: `msgdefs.h:35-48`,
live `0x33E0`), 4 i16 ClockStateAPI (-1 INVALID until time is set), 6 i16 LeapSeconds
(live 37), 8 MET {u32 sec, u32 subsec}, 16 STCF {u32,u32}, 24 AdjustmentFactor {u32,u32}.
`CFE_TIME_SysTime_t` = `{uint32 Seconds; uint32 Subseconds}` in payload (little-endian
here), subsecond LSB 2^-32 s (`extern_typedefs.h:40-45`). Live check: after SET_STCF
(1000, 500000 us) the HK showed STCF = (1000, 0x80000000).

TIME DIAG, MID `0x0806`, packet 216 (`msgdefs.h:175-319`), request = SEND_DIAGNOSTIC.
Payload offsets: 0 AtToneMET, 8 AtToneSTCF, 16 AtToneDelay, 24 AtToneLatch (each
{u32,u32}); 32 i16 AtToneLeapSeconds; 34 i16 ClockStateAPI; 36 TimeSinceTone, 44
CurrentLatch, 52 CurrentMET, 60 CurrentTAI, 68 CurrentUTC; 76 i16 ClockSetState, 78
ClockFlyState, 80 ClockSource, 82 ClockSignal, 84 ServerFlyState, 86 Forced2Fly; 88 u16
ClockStateFlags; 90 i16 OneTimeDirection, 92 OneHzDirection, 94 DelayDirection; 96
OneTimeAdjust, 104 OneHzAdjust, 112 ToneSignalLatch, 120 ToneDataLatch (each {u32,u32});
128-168 u32 counters (ToneMatchCounter, ToneMatchErrorCounter, ToneSignalCounter,
ToneDataCounter, ToneIntCounter, ToneIntErrorCounter, ToneTaskCounter, VersionCounter,
LocalIntCounter, LocalTaskCounter, VirtualMET); 172 u32 MinElapsed; 176 u32 MaxElapsed;
180 MaxLocalClock {u32,u32}; 188 u32 ToneOverLimit; 192 u32 ToneUnderLimit; 196 u32
DataStoreStatus. Note the 4-byte (not 8-byte) alignment: `TimeSinceTone` sits at 36.

### SAMPLE_APP (command MID `0x1882`)

Dispatch `apps/sample_app/fsw/src/sample_app_dispatch.c:77-123`; handlers
`sample_app_cmds.c`.

| Name | FC | Payload |
|------|----|---------|
| NOOP | 0 | none; `CommandCounter++` (`sample_app_cmds.c:79-89`) |
| RESET_COUNTERS | 1 | none; zeroes both counters (`:98-106`) |
| PROCESS | 2 | none; `CommandCounter++`, reads the example table, writes to the ES syslog (`:114-148`) |
| DISPLAY_PARAM | 3 | `u32 ValU32` +0, `i16 ValI16` +4, `char ValStr[10]` +6 (`sample_app_msgdefs.h:30-35`, string length `fsw/inc/sample_app_interface_cfg.h:44`); packet 24 bytes, payload 16, no internal padding; `CommandCounter++` and an info event echoing the values (`:155-166`) |

Unknown FC: an error event only, **no counter** (`sample_app_dispatch.c:116-121`).
Send-HK request: MID `0x1883`, FC 0. HK, MID `0x0883`, packet 18 bytes
(`sample_app_msgdefs.h:42-46`): +0 u8 CommandCounter, +1 u8 CommandErrorCounter.

### CI_LAB (command MID `0x1884`)

Dispatch `apps/ci_lab/fsw/src/ci_lab_dispatch.c:72-98`: NOOP (0) and RESET_COUNTERS (1),
no payload. **Unknown function codes are silently ignored** (`:95-96`: no counter, no
event). Send-HK request MID `0x1885`, FC 0; MID `0x1886` is the SCH_LAB-only
`READ_UPLINK` wake-up. HK, MID `0x0884`, packet 28 bytes (`ci_lab_msgdefs.h:33-41`):
+0 u8 CommandCounter, +1 u8 CommandErrorCounter, +2 u8 EnableChecksums (declared, never
written: always 0), +3 u8 SocketConnected, +4 u32 IngestPackets, +8 u32 IngestErrors.

### TO_LAB (command MID `0x1880`)

Dispatch `apps/to_lab/fsw/src/to_lab_dispatch.c:38-83` (no length verification; unknown FC
bumps `CommandErrorCounter`); handlers `to_lab_cmds.c`.

| Name | FC | Payload |
|------|----|---------|
| NOOP | 0 | none; `CommandCounter++` (`:69-84`) |
| RESET_STATUS | 1 | none; zeroes counters, does not count itself (`:91-99`) |
| ADD_PKT | 2 | `u32 Stream` +0 (MID), `u8 Priority` +4, `u8 Reliability` +5, `u8 BufLimit` +6, 1 pad byte +7; packet 16 (`to_lab_msgdefs.h:49-54`) |
| SEND_DATA_TYPES | 3 | none; publishes the data-types test packet |
| REMOVE_PKT | 4 | `u32 Stream` +0; packet 12 |
| REMOVE_ALL_PKT | 5 | none |
| OUTPUT_ENABLE | 6 | `char dest_IP[16]` +0, dotted IPv4 text, NUL-padded; packet 24 (`to_lab_msgdefs.h:61-64`, handler `to_lab_cmds.c:39-62`); starts the downlink socket on first use, later commands only switch the destination; `CommandCounter++` |

Send-HK request MID `0x1881`. HK, MID `0x0880`, packet 18 bytes
(`to_lab_msgdefs.h:31-35`): +0 u8 CommandCounter, +1 u8 CommandErrorCounter. Data-types
packet, MID `0x0881`, packet 80 bytes (`to_lab_msgdefs.h:37-47`), fixed content
(`to_lab_cmds.c:106-151`): u16 synch=0x6969 @0, u8 bl1=0 @2, u8 bl2=1 @3, i8 b1..b4
= 16,127,0x7F,0x45 @4-7, i16 w1=0x2468 @8, w2=0x7FFF @10, i32 dw1=0x12345678 @12,
dw2=0x87654321 @16, f32 f1=90.01 @20, f2=4.5e-6 @24, 4 pad bytes @28, f64 df1=99.9 @32,
df2=0.4444 @40, `char str[10]`="ABCDEFGHIJ" @48 (no NUL), 6 tail pad bytes @58. Useful as a
codec fixture for every scalar type.

## 5. Error behaviour relevant to ironbird's acceptance cases (verified live)

| Stimulus | Result |
|----------|--------|
| Wrong checksum | accepted, counted as a valid command (see section 1) |
| Consistent header length but not the size the app expects (e.g. NOOP with 2 extra payload bytes) | ES/TIME/SAMPLE_APP/CI_LAB: event `Invalid msg length: ID=.., CC=.., Len=.., Expected=..` and `CommandErrorCounter++`; TO_LAB: accepted |
| Header length larger than the datagram | dropped by CI_LAB, `IngestErrors++`, event `CI: cmd dropped - length mismatch` |
| Unknown function code | ES, TIME, TO_LAB: error counter and event; SAMPLE_APP: event only; CI_LAB: nothing |
| Valid MID nobody subscribes to | `CFE_SB 14: No subscribers for MsgId` event (SB error, not the app's) |

Events appear on the cFS console (`EVS Port1 ...` lines) and, as long-event telemetry
(MID `0x0808`), on the downlink (subscribed at `sample_defs/tables/to_lab_sub.c:73`).
Layouts are in section 7.

## 6. Platform notes found while bringing it up

- A non-root user is required for the intended mode. OSAL only truncates message-queue
  depth to `/proc/sys/fs/mqueue/msg_max` when `geteuid() != 0`
  (`osal/src/bsp/generic-linux/src/bsp_start.c:66-76`, used via
  `osal/src/os/posix/src/os-impl-queues.c:55-68,98-105`); permissive mode is enabled for
  native builds (`sample_defs/native_osconfig.cmake:39`). As root with a small `msg_max`
  (10 here), `core-cpu1` aborts creating the SB pipe (`OS_QueueCreate ... errno = 22`,
  `CFE_ES_TaskInit: Cannot Create SB Pipe`) and resets. `run.sh` therefore drops root to
  uid 65534 via `setpriv`.
- The stock startup script (`sample_defs/targets.cmake:89-101` global app list) starts LC, CF,
  DS, FM, HK, HS, MM, SC, MD, CS and SBN as well. With the reduced queue depth SBN floods
  its pipe (`CFE_SB 25: Pipe Overflow`) and events are squelched, so `run.sh` stages a
  startup script limited to `sch_lab ci_lab to_lab sample_app` (override with
  `CFS_APPS="ALL"`).
- Console lines carry a simulated `1980-...` epoch (cFE time before any time source is
  set), not wall time.

## 7. EVS events and the other core housekeeping MIDs

Topic IDs from `cfe/modules/{evs,sb,tbl}/fsw/inc/*_topicids.h`, offsets and sizes from the
C probe described at the top, MIDs confirmed on the wire. "Payload offset" is after the
16-byte telemetry header.

| MID | Message | Packet | Payload |
|-----|---------|--------|---------|
| `0x0801` | EVS housekeeping (`CFE_EVS_HousekeepingTlm_t`) | 160 | 144 |
| `0x0803` | SB housekeeping | 44 | 28 |
| `0x0804` | TBL housekeeping | 300 | 284 |
| `0x0808` | EVS **long event** | 172 | 156 |
| `0x0809` | EVS short event (not in the installed TO_LAB subscription table) | 48 | 32 |

Command side: EVS command `0x1801` (NOOP fc 0, RESET_COUNTERS fc 1; the NOOP emits event id 0),
EVS send-HK `0x1809`, SB send-HK `0x180B`, TBL send-HK `0x180C`, all 8 bytes, fc 0.

Long event payload (`default_cfe_evs_msgdefs.h`, `CFE_EVS_LongEventTlm_Payload_t`): +0
`char AppName[20]` (`CFE_MISSION_MAX_API_LEN`), +20 `u16 EventID`, +22 `u16 EventType`
(1 DEBUG, 2 INFORMATION, 3 ERROR, 4 CRITICAL: `default_cfe_evs_extern_typedefs.h:77-107`),
+24 `u32 SpacecraftID`, +28 `u32 ProcessorID`, +32 `char Message[122]`
(`CFE_MISSION_EVS_MAX_MESSAGE_LENGTH`), +154 `u8 Padding[2]`. The short event is the first 32
bytes only. Strings are NUL-padded; the message may contain newlines (ES NOOP's does).

Events seen live after enabling TO_LAB output (app / id / type): `TO_LAB 3 INFORMATION` (output
enabled), `SAMPLE_APP 3 INFORMATION` (`SAMPLE: NOOP command v..`), `CFE_ES 3 INFORMATION`
(plus a version event, id 92), `CFE_EVS 0 INFORMATION`, `CFE_TIME 36 ERROR` (`Invalid STCF`),
`SAMPLE_APP 8 INFORMATION` (DISPLAY_PARAM echo), `SAMPLE_APP 6 ERROR` (`Invalid Msg length`).
No periodic event was observed; with a quiet system the event stream is silent.

EVS HK payload: +0 u8 CommandCounter, +1 u8 CommandErrorCounter, +2 u8 MessageFormatMode (0 short,
1 long; live 1), +3 u8 MessageTruncCounter, +4 u8 UnregisteredAppCounter, +5 u8 OutputPort, +6 u8
LogFullFlag, +7 u8 LogMode, +8 u16 MessageSendCounter, +10 u16 LogOverflowCounter, +12 u8
LogEnabled, +13 u8[3] Padding, +16 `AppData[16]` of 8-byte records (u32 AppID, u16
AppMessageSentCounter, u8 AppEnableStatus, u8 AppMessageSquelchedCounter).
