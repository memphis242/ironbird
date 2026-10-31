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
 *   uint16 Spare.
 *
 * The variable-length layout is what ironbird's CCDD file examples/ccdd/cfs.json describes.
 */
#ifndef IB_TEST_H
#define IB_TEST_H

#include "cfe.h"
#include "cfe_core_api_base_msgids.h"

#define IB_TEST_MAX_ENTRIES 64
#define IB_TEST_MAX_TEXT    122
#define IB_TEST_VAR_MAX_PAYLOAD (2 + 4 * IB_TEST_MAX_ENTRIES + IB_TEST_MAX_TEXT)

/* Topic IDs no stock cFS app uses (they stop at 0xC9). */
#define IB_TEST_CMD_TOPICID     0xE0
#define IB_TEST_SEND_HK_TOPICID 0xE1
#define IB_TEST_HK_TOPICID      0xE0
#define IB_TEST_VAR_TOPICID     0xE1

#define IB_TEST_CMD_MID     CFE_PLATFORM_CMD_TOPICID_TO_MIDV(IB_TEST_CMD_TOPICID)
#define IB_TEST_SEND_HK_MID CFE_PLATFORM_CMD_TOPICID_TO_MIDV(IB_TEST_SEND_HK_TOPICID)
#define IB_TEST_HK_TLM_MID  CFE_PLATFORM_TLM_TOPICID_TO_MIDV(IB_TEST_HK_TOPICID)
#define IB_TEST_VAR_TLM_MID CFE_PLATFORM_TLM_TOPICID_TO_MIDV(IB_TEST_VAR_TOPICID)

#define IB_TEST_NOOP_CC           0
#define IB_TEST_RESET_COUNTERS_CC 1
#define IB_TEST_SET_DATA_CC       2
#define IB_TEST_SEND_VAR_CC       3

#define IB_TEST_PIPE_DEPTH 32
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
    uint16 Spare;
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
    uint8  CmdCounter;
    uint8  ErrCounter;
    uint16 Entries;
    uint16 TextLen;

    uint32 Data[IB_TEST_MAX_ENTRIES];
    char   Text[IB_TEST_MAX_TEXT];

    CFE_SB_PipeId_t CommandPipe;
    uint32          RunStatus;

    IB_TEST_HkTlm_t  HkTlm;
    IB_TEST_VarTlm_t VarTlm;
} IB_TEST_Data_t;

void IB_TEST_Main(void);

#endif
