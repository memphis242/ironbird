/*
 * ironbird's TO_LAB platform configuration values for the cpu1 target. cFS takes this file instead
 * of apps/to_lab/config/default_to_lab_internal_cfg_values.h because the cpu1_ prefix names it for
 * that target's generated to_lab_internal_cfg_values.h; it therefore sets every
 * TO_LAB_PLATFORM_CFGVAL item, and the ones left at TO_LAB's default point at that default.
 *
 * tools/cfs/build.sh copies this file into the checkout's sample_defs/.
 */
#ifndef IRONBIRD_TO_LAB_INTERNAL_CFG_VALUES_H
#define IRONBIRD_TO_LAB_INTERNAL_CFG_VALUES_H

#include "osconfig.h"

#define TO_LAB_PLATFORM_CFGVAL(x) IRONBIRD_TO_LAB_CFGVAL_##x

/*
 * Depth of TO_LAB's telemetry pipe: 100 ms of the 10k packets/s throughput test's stream
 * (tests/cfs/test_cfs_generator.py, which reads this line). It cannot exceed OS_QUEUE_MAX_DEPTH
 * (sample_defs/cpu1_osconfig.cmake). A subscription's BufLimit is bounded by it too: TO_LAB_ADD_PKT
 * needs a limit below it (and a uint8, so 255 at most), while the subscription table
 * (sample_defs/tables/cpu1_to_lab_sub.c) may use the depth itself, which it does for the generator.
 */
#define IRONBIRD_TO_LAB_CFGVAL_TLM_PIPE_DEPTH 1000

/*
 * Telemetry packets TO_LAB forwards per wakeup before it looks at its command pipe. TO_LAB's own
 * default is OS_QUEUE_MAX_DEPTH, which is 50 in stock cFS and would follow the deeper queues of
 * this build to 1000: with a 10k packets/s stream that is 100 ms between command-pipe checks. This
 * is the stock value, so the command pipe is checked about every 5 ms at that rate.
 */
#define IRONBIRD_TO_LAB_CFGVAL_MAX_TLM_PKTS 50

#define IRONBIRD_TO_LAB_CFGVAL_TLM_PIPE_TIMEOUT     DEFAULT_TO_LAB_PLATFORM_TLM_PIPE_TIMEOUT
#define IRONBIRD_TO_LAB_CFGVAL_CMD_PIPE_DEPTH       DEFAULT_TO_LAB_PLATFORM_CMD_PIPE_DEPTH
#define IRONBIRD_TO_LAB_CFGVAL_STARTUP_SYNC_TIMEOUT DEFAULT_TO_LAB_PLATFORM_STARTUP_SYNC_TIMEOUT

/* A negative array size is a compile error when the condition is false. */
typedef char IRONBIRD_TO_LAB_Assert_TlmPipeFitsOsal
    [(IRONBIRD_TO_LAB_CFGVAL_TLM_PIPE_DEPTH <= OS_QUEUE_MAX_DEPTH) ? 1 : -1];

#endif
