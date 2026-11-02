/*
 * ib_test: ironbird's cFS test application.
 *
 * Wire formats (all integers little-endian, as on the native Linux target):
 *
 *   SET_DATA command (fc 2) and VAR telemetry, after the cFE header:
 *       uint16  Count            number of Entries
 *       uint32  Entries[Count]   1..IB_TEST_MAX_ENTRIES of them (0 allowed)
 *       char    Text[]           the rest of the packet, 0..IB_TEST_MAX_TEXT bytes, no NUL
 *
 *   HK telemetry payload: uint8 CmdCounter, uint8 ErrCounter, uint16 Entries, uint16 TextLen,
 *   uint16 HkCounter (the HK packets sent so far, this one included; wraps at 65536). Each HK
 *   packet answers one SEND_HK request, so a reader that counts its requests can tell the reply
 *   to a request sent now from a late reply to an older one.
 *
 * Throughput generator (fixed-size telemetry):
 *
 *   GEN_START command (fc 4), after the cFE header: uint32 Rate (packets/s, 1..IB_TEST_GEN_MAX_RATE),
 *   uint32 Count (packets to send; 0 = until GEN_STOP). GEN_STOP (fc 5) and GEN_SEND_STATUS (fc 6)
 *   carry no payload. A run is refused while another is running.
 *
 *   GEN telemetry payload (IB_TEST_GEN_PACKET_BYTES in all): uint32 Serial (0 for the first packet
 *   of a run, then +1; the low 32 bits of the run's packet count, so a run of 2^32 packets or more
 *   wraps to 0 by design), uint32 Run (the run number, 1 for the first START after power-on), then
 *   IB_TEST_GEN_FILL_BYTES of Fill, where Fill[i] = (uint8)(Serial + i).
 *
 *   GEN_STATUS telemetry payload: uint32 Run, uint32 Rate, uint32 Target (the Count asked for),
 *   uint32 Sent (packets published in the run so far, modulo 2^32 like Serial), uint8 State,
 *   uint8 Spare, uint16 MaxBurst (the most packets published in one 1 ms tick; a large value says
 *   the task was descheduled).
 *   Published when a run ends (count reached or GEN_STOP) and on GEN_SEND_STATUS.
 *
 * The layouts are what ironbird's CCDD file examples/ccdd/cfs.json describes.
 */
#ifndef IB_TEST_H
#define IB_TEST_H

#include <stddef.h>

#include "cfe.h"
#include "cfe_core_api_base_msgids.h"

#define IB_TEST_MAX_ENTRIES 64
#define IB_TEST_MAX_TEXT    122
#define IB_TEST_VAR_MAX_PAYLOAD (2 + 4 * IB_TEST_MAX_ENTRIES + IB_TEST_MAX_TEXT)

/* Generator limits. */
#define IB_TEST_NS_PER_SEC        1000000000ULL
#define IB_TEST_GEN_MAX_RATE      10000
#define IB_TEST_GEN_FILL_BYTES    176
#define IB_TEST_GEN_PACKET_BYTES  200
#define IB_TEST_GEN_TICK_MS       1

/*
 * What one tick may publish. The packets go to TO_LAB's telemetry pipe, and its depth on the
 * native target is not the OS_QUEUE_MAX_DEPTH (50) cFE asks for: OSAL in permissive mode (every
 * native build) silently truncates each queue a non-root cFS creates to
 * /proc/sys/fs/mqueue/msg_max, which is 10 on a default Linux host (the cFS console says
 * "Maximum user msg queue depth = 10"). A tick's packets go out back to back, faster than TO_LAB
 * forwards them, so a tick that publishes more than the pipe has room for overflows it even while
 * TO_LAB runs.
 *
 * IB_TEST_GEN_MIN_PIPE_DEPTH is the shallowest pipe the generator is designed for, and
 * IB_TEST_GEN_PIPE_RESERVE the slots it leaves to the other telemetry TO_LAB forwards through the
 * same pipe (housekeeping, events). A tick publishes at most IB_TEST_GEN_CATCHUP_TICKS ticks' share
 * of the rate (IB_TEST_GEN_BURST_LIMIT), so a late generator catches up at that multiple of its
 * rate instead of in one burst, and never more than IB_TEST_GEN_MAX_BURST. At
 * IB_TEST_GEN_FLOOR_RATE (ironbird's 1k packets/s floor) the limit fits the pipe beside the
 * reserve, so the generator cannot overrun the pipe by itself; what remains is TO_LAB's own task
 * being off-CPU for longer than the pipe covers, which the software bus counts
 * (PipeOverflowErrorCounter).
 */
#define IB_TEST_GEN_MIN_PIPE_DEPTH 10
#define IB_TEST_GEN_PIPE_RESERVE   2
#define IB_TEST_GEN_CATCHUP_TICKS  2
#define IB_TEST_GEN_MAX_BURST      16
#define IB_TEST_GEN_FLOOR_RATE     1000
#define IB_TEST_GEN_TICK_SHARE(rate) ((((rate) * IB_TEST_GEN_TICK_MS) + 999) / 1000)
#define IB_TEST_GEN_BURST_LIMIT(rate)                                                    \
    ((IB_TEST_GEN_CATCHUP_TICKS * IB_TEST_GEN_TICK_SHARE(rate)) < IB_TEST_GEN_MAX_BURST     \
         ? (IB_TEST_GEN_CATCHUP_TICKS * IB_TEST_GEN_TICK_SHARE(rate))                      \
         : IB_TEST_GEN_MAX_BURST)
#define IB_TEST_GEN_IDLE_WAIT_MS  500
#define IB_TEST_GEN_TASK_NAME     "IB_TEST_GEN"
#define IB_TEST_GEN_TASK_PRIORITY 70
#define IB_TEST_GEN_TASK_STACK    16384

/* GEN_STATUS State values. */
#define IB_TEST_GEN_STATE_IDLE     0 /* no run since power-on */
#define IB_TEST_GEN_STATE_RUNNING  1
#define IB_TEST_GEN_STATE_FINISHED 2 /* the Count was reached */
#define IB_TEST_GEN_STATE_STOPPED  3 /* GEN_STOP ended the run */

/* Topic IDs no stock cFS app uses (they stop at 0xC9). */
#define IB_TEST_CMD_TOPICID     0xE0
#define IB_TEST_SEND_HK_TOPICID 0xE1
#define IB_TEST_HK_TOPICID      0xE0
#define IB_TEST_VAR_TOPICID     0xE1
#define IB_TEST_GEN_TOPICID     0xE2
#define IB_TEST_GEN_STATUS_TOPICID 0xE3

#define IB_TEST_CMD_MID     CFE_PLATFORM_CMD_TOPICID_TO_MIDV(IB_TEST_CMD_TOPICID)
#define IB_TEST_SEND_HK_MID CFE_PLATFORM_CMD_TOPICID_TO_MIDV(IB_TEST_SEND_HK_TOPICID)
#define IB_TEST_HK_TLM_MID  CFE_PLATFORM_TLM_TOPICID_TO_MIDV(IB_TEST_HK_TOPICID)
#define IB_TEST_VAR_TLM_MID CFE_PLATFORM_TLM_TOPICID_TO_MIDV(IB_TEST_VAR_TOPICID)
#define IB_TEST_GEN_TLM_MID CFE_PLATFORM_TLM_TOPICID_TO_MIDV(IB_TEST_GEN_TOPICID)
#define IB_TEST_GEN_STATUS_TLM_MID CFE_PLATFORM_TLM_TOPICID_TO_MIDV(IB_TEST_GEN_STATUS_TOPICID)

#define IB_TEST_NOOP_CC           0
#define IB_TEST_RESET_COUNTERS_CC 1
#define IB_TEST_SET_DATA_CC       2
#define IB_TEST_SEND_VAR_CC       3
#define IB_TEST_GEN_START_CC      4
#define IB_TEST_GEN_STOP_CC       5
#define IB_TEST_GEN_SEND_STATUS_CC 6

/* The command pipe holds IB_TEST_PIPE_DEPTH messages, and each subscription may have that many
 * queued: the software bus defaults to 4 per subscription, which a burst of commands overruns
 * while the pipe has room. */
#define IB_TEST_PIPE_DEPTH     32
#define IB_TEST_PIPE_MSG_LIMIT IB_TEST_PIPE_DEPTH
#define IB_TEST_PIPE_NAME  "IB_TEST_CMD_PIPE"

#define IB_TEST_INIT_EID     1
#define IB_TEST_NOOP_EID     2
#define IB_TEST_RESET_EID    3
#define IB_TEST_SET_DATA_EID 4
#define IB_TEST_LEN_ERR_EID  5
#define IB_TEST_CC_ERR_EID   6
#define IB_TEST_MID_ERR_EID  7
#define IB_TEST_PIPE_ERR_EID 8
#define IB_TEST_INIT_ERR_EID 9
#define IB_TEST_GEN_START_EID 10
#define IB_TEST_GEN_DONE_EID  11
#define IB_TEST_GEN_STOP_EID  12

typedef struct
{
    CFE_MSG_CommandHeader_t CommandHeader;
} IB_TEST_NoArgsCmd_t;

typedef struct
{
    uint8  CmdCounter;
    uint8  ErrCounter;
    uint16 Entries;
    uint16 TextLen;
    uint16 HkCounter;
} IB_TEST_HkPayload_t;

typedef struct
{
    CFE_MSG_TelemetryHeader_t TelemetryHeader;
    IB_TEST_HkPayload_t       Payload;
} IB_TEST_HkTlm_t;

/* The variable-length telemetry packet: the header, then Count, Entries and Text packed. */
typedef struct
{
    CFE_MSG_TelemetryHeader_t TelemetryHeader;
    uint8                     Payload[IB_TEST_VAR_MAX_PAYLOAD];
} IB_TEST_VarTlm_t;

typedef struct
{
    CFE_MSG_CommandHeader_t CommandHeader;
    uint32                  Rate;
    uint32                  Count;
} IB_TEST_GenStartCmd_t;

typedef struct
{
    uint32 Serial;
    uint32 Run;
    uint8  Fill[IB_TEST_GEN_FILL_BYTES];
} IB_TEST_GenPayload_t;

typedef struct
{
    CFE_MSG_TelemetryHeader_t TelemetryHeader;
    IB_TEST_GenPayload_t      Payload;
} IB_TEST_GenTlm_t;

typedef struct
{
    uint32 Run;
    uint32 Rate;
    uint32 Target;
    uint32 Sent;
    uint8  State;
    uint8  Spare;
    uint16 MaxBurst;
} IB_TEST_GenStatusPayload_t;

typedef struct
{
    CFE_MSG_TelemetryHeader_t  TelemetryHeader;
    IB_TEST_GenStatusPayload_t Payload;
} IB_TEST_GenStatusTlm_t;

/*
 * The generator's state. Every field is read and written with Lock held: the command task starts
 * and stops runs, the generator task publishes the packets and ends a run whose Count is reached.
 * The packets of a burst go out with Lock held, so a command sees the run between bursts only
 * and the Sent it reports is exactly what was published.
 */
typedef struct
{
    osal_id_t Lock;
    osal_id_t Wake; /* given by GEN_START so an idle task starts at once */
    CFE_ES_TaskId_t Task;

    uint32 Run;
    uint32 Rate;
    uint32 Target;
    uint64 Sent; /* packets published in the run; the packets carry its low 32 bits */
    uint8  State;
    uint16 MaxBurst;
    int64  StartNs; /* the PSP time at GEN_START */

    IB_TEST_GenTlm_t       Tlm;
    IB_TEST_GenStatusTlm_t StatusTlm;
} IB_TEST_Gen_t;

typedef struct
{
    uint8  CmdCounter;
    uint8  ErrCounter;
    uint16 Entries;
    uint16 TextLen;
    uint16 HkCounter;

    uint32 Data[IB_TEST_MAX_ENTRIES];
    char   Text[IB_TEST_MAX_TEXT];

    CFE_SB_PipeId_t CommandPipe;
    uint32          RunStatus;

    IB_TEST_HkTlm_t  HkTlm;
    IB_TEST_VarTlm_t VarTlm;

    IB_TEST_Gen_t Gen;
} IB_TEST_Data_t;

/*
 * Compile-time layout checks (the build is -std=c99 -pedantic, so no _Static_assert): a
 * negative array size is an error when the condition is false. The sizes are the ones the CCDD
 * file examples/ccdd/cfs.json and the header profiles in examples/headers rely on.
 */
#define IB_TEST_ASSERT_SIZE(name, cond) typedef char IB_TEST_Assert_##name[(cond) ? 1 : -1]

IB_TEST_ASSERT_SIZE(CmdHeader, sizeof(CFE_MSG_CommandHeader_t) == 8);
IB_TEST_ASSERT_SIZE(TlmHeader, sizeof(CFE_MSG_TelemetryHeader_t) == 16);
IB_TEST_ASSERT_SIZE(NoArgsCmd, sizeof(IB_TEST_NoArgsCmd_t) == 8);
IB_TEST_ASSERT_SIZE(HkPayload, sizeof(IB_TEST_HkPayload_t) == 1 + 1 + 2 + 2 + 2);
IB_TEST_ASSERT_SIZE(HkTlm, sizeof(IB_TEST_HkTlm_t) == 16 + 8);
IB_TEST_ASSERT_SIZE(VarMaxPayload, IB_TEST_VAR_MAX_PAYLOAD == 2 + 4 * 64 + 122);
IB_TEST_ASSERT_SIZE(VarTlm, sizeof(IB_TEST_VarTlm_t) == 16 + IB_TEST_VAR_MAX_PAYLOAD);

IB_TEST_ASSERT_SIZE(GenStartCmd, sizeof(IB_TEST_GenStartCmd_t) == 8 + 4 + 4);
IB_TEST_ASSERT_SIZE(GenStartRate, offsetof(IB_TEST_GenStartCmd_t, Rate) == 8);
IB_TEST_ASSERT_SIZE(GenStartCount, offsetof(IB_TEST_GenStartCmd_t, Count) == 12);
IB_TEST_ASSERT_SIZE(GenPayload, sizeof(IB_TEST_GenPayload_t) == 4 + 4 + IB_TEST_GEN_FILL_BYTES);
IB_TEST_ASSERT_SIZE(GenSerial, offsetof(IB_TEST_GenPayload_t, Serial) == 0);
IB_TEST_ASSERT_SIZE(GenRun, offsetof(IB_TEST_GenPayload_t, Run) == 4);
IB_TEST_ASSERT_SIZE(GenFill, offsetof(IB_TEST_GenPayload_t, Fill) == 8);
IB_TEST_ASSERT_SIZE(GenTlm, sizeof(IB_TEST_GenTlm_t) == IB_TEST_GEN_PACKET_BYTES);
IB_TEST_ASSERT_SIZE(GenStatusPayload, sizeof(IB_TEST_GenStatusPayload_t) == 4 + 4 + 4 + 4 + 1 + 1 + 2);
IB_TEST_ASSERT_SIZE(GenStatusTlm, sizeof(IB_TEST_GenStatusTlm_t) == 16 + 20);
IB_TEST_ASSERT_SIZE(PipeLimitIsDepth, IB_TEST_PIPE_MSG_LIMIT == IB_TEST_PIPE_DEPTH);
IB_TEST_ASSERT_SIZE(PipeDepthFitsU16, IB_TEST_PIPE_DEPTH >= 1 && IB_TEST_PIPE_DEPTH <= 0xFFFF);
IB_TEST_ASSERT_SIZE(PipeDepthFitsOsal, IB_TEST_PIPE_DEPTH <= OS_QUEUE_MAX_DEPTH);
IB_TEST_ASSERT_SIZE(GenBurstFitsPipe, IB_TEST_GEN_MAX_BURST < OS_QUEUE_MAX_DEPTH);
IB_TEST_ASSERT_SIZE(GenRateFitsTicks, IB_TEST_GEN_MAX_RATE <= 1000 * IB_TEST_GEN_MAX_BURST * IB_TEST_GEN_TICK_MS);

/* The pipe model: the shallowest pipe the generator is designed for is one cFE can ask for, and
 * leaves the generator room beside the reserve. */
IB_TEST_ASSERT_SIZE(GenMinPipeFitsOsal, IB_TEST_GEN_MIN_PIPE_DEPTH <= OS_QUEUE_MAX_DEPTH);
IB_TEST_ASSERT_SIZE(GenReserveLeavesRoom, IB_TEST_GEN_PIPE_RESERVE < IB_TEST_GEN_MIN_PIPE_DEPTH);
/* At the floor rate a tick never publishes more than that room, so a late generator does not
 * overflow the pipe by itself. */
IB_TEST_ASSERT_SIZE(GenFloorWithinRates,
                    IB_TEST_GEN_FLOOR_RATE >= 1 && IB_TEST_GEN_FLOOR_RATE <= IB_TEST_GEN_MAX_RATE);
IB_TEST_ASSERT_SIZE(GenFloorBurstFitsPipe, IB_TEST_GEN_BURST_LIMIT(IB_TEST_GEN_FLOOR_RATE) <=
                                               IB_TEST_GEN_MIN_PIPE_DEPTH - IB_TEST_GEN_PIPE_RESERVE);
/* A late generator catches up at every rate: a tick may publish more than its share (a real tick
 * is never shorter than IB_TEST_GEN_TICK_MS, so a limit of one share could never recover). */
IB_TEST_ASSERT_SIZE(GenCatchUpExists, IB_TEST_GEN_CATCHUP_TICKS >= 2);
IB_TEST_ASSERT_SIZE(GenMinRateCatchesUp, IB_TEST_GEN_BURST_LIMIT(1) > IB_TEST_GEN_TICK_SHARE(1));
IB_TEST_ASSERT_SIZE(GenMaxRateCatchesUp,
                    IB_TEST_GEN_BURST_LIMIT(IB_TEST_GEN_MAX_RATE) > IB_TEST_GEN_TICK_SHARE(IB_TEST_GEN_MAX_RATE));
/* MaxBurst (uint16) holds every limit, and the share arithmetic stays within uint32. */
IB_TEST_ASSERT_SIZE(GenBurstFitsU16, IB_TEST_GEN_MAX_BURST <= 0xFFFF);
IB_TEST_ASSERT_SIZE(GenShareFitsU32, IB_TEST_GEN_MAX_RATE <= (0xFFFFFFFFUL - 999) / IB_TEST_GEN_TICK_MS);

/* The schedule arithmetic is 64-bit for every elapsed time an int64 nanosecond count holds: the
 * product of the remainder of a second and the rate, and of whole seconds and the rate, fit. */
IB_TEST_ASSERT_SIZE(GenSentIs64, sizeof(((IB_TEST_Gen_t *)0)->Sent) == 8);
IB_TEST_ASSERT_SIZE(GenRemainderFits,
                    (IB_TEST_NS_PER_SEC - 1) <= (0xFFFFFFFFFFFFFFFFULL / IB_TEST_GEN_MAX_RATE));
IB_TEST_ASSERT_SIZE(GenWholeSecondsFit,
                    (9223372036854775807ULL / IB_TEST_NS_PER_SEC) <=
                        ((0xFFFFFFFFFFFFFFFFULL - 2 * IB_TEST_GEN_MAX_RATE) / IB_TEST_GEN_MAX_RATE));

void IB_TEST_Main(void);

#endif
