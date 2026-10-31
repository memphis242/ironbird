/*
 * ib_test: ironbird's cFS test application. See ib_test.h for the packet layouts.
 *
 * SET_DATA is the one command whose size varies; it is checked against the layout byte for
 * byte, so a count that disagrees with the packet size is rejected here exactly as flight
 * software would reject it.
 */
#include "ib_test.h"

#include <string.h>

IB_TEST_Data_t IB_TEST_Data;

static void IB_TEST_PublishHk(void)
{
    IB_TEST_Data.HkTlm.Payload.CmdCounter = IB_TEST_Data.CmdCounter;
    IB_TEST_Data.HkTlm.Payload.ErrCounter = IB_TEST_Data.ErrCounter;
    IB_TEST_Data.HkTlm.Payload.Entries    = IB_TEST_Data.Entries;
    IB_TEST_Data.HkTlm.Payload.TextLen    = IB_TEST_Data.TextLen;
    IB_TEST_Data.HkTlm.Payload.Spare      = 0;
    CFE_SB_TimeStampMsg(CFE_MSG_PTR(IB_TEST_Data.HkTlm.TelemetryHeader));
    CFE_SB_TransmitMsg(CFE_MSG_PTR(IB_TEST_Data.HkTlm.TelemetryHeader), true);
}

/* Pack the stored data as Count, Entries[Count], Text (no terminator), size the packet to it and
 * send it. */
static void IB_TEST_PublishVar(void)
{
    uint8   *out  = IB_TEST_Data.VarTlm.Payload;
    size_t   size = 0;
    uint16   count = IB_TEST_Data.Entries;

    memcpy(out, &count, sizeof(count));
    size += sizeof(count);
    memcpy(out + size, IB_TEST_Data.Data, (size_t)count * sizeof(uint32));
    size += (size_t)count * sizeof(uint32);
    memcpy(out + size, IB_TEST_Data.Text, IB_TEST_Data.TextLen);
    size += IB_TEST_Data.TextLen;

    CFE_MSG_SetSize(CFE_MSG_PTR(IB_TEST_Data.VarTlm.TelemetryHeader),
                    sizeof(IB_TEST_Data.VarTlm.TelemetryHeader) + size);
    CFE_SB_TimeStampMsg(CFE_MSG_PTR(IB_TEST_Data.VarTlm.TelemetryHeader));
    CFE_SB_TransmitMsg(CFE_MSG_PTR(IB_TEST_Data.VarTlm.TelemetryHeader), true);
}

static void IB_TEST_Reject(const char *what, CFE_MSG_FcnCode_t cc, size_t actual, size_t expected)
{
    IB_TEST_Data.ErrCounter++;
    CFE_EVS_SendEvent(IB_TEST_LEN_ERR_EID, CFE_EVS_EventType_ERROR,
                      "IB_TEST: %s rejected: CC = %u, size %u, expected %u", what, (unsigned int)cc,
                      (unsigned int)actual, (unsigned int)expected);
}

static bool IB_TEST_HasSize(const CFE_MSG_Message_t *msg, CFE_MSG_FcnCode_t cc, const char *what,
                            size_t expected)
{
    size_t actual = 0;

    CFE_MSG_GetSize(msg, &actual);
    if (actual != expected)
    {
        IB_TEST_Reject(what, cc, actual, expected);
        return false;
    }
    return true;
}

/* SET_DATA: header, uint16 Count, Count x uint32, then the tail string up to the end. */
static void IB_TEST_SetData(const CFE_SB_Buffer_t *buf, CFE_MSG_FcnCode_t cc)
{
    const size_t header = sizeof(CFE_MSG_CommandHeader_t);
    size_t       actual = 0;
    uint16       count  = 0;
    size_t       tail;
    const uint8 *in = (const uint8 *)buf;

    CFE_MSG_GetSize(&buf->Msg, &actual);
    if (actual < header + sizeof(count))
    {
        IB_TEST_Reject("SET_DATA", cc, actual, header + sizeof(count));
        return;
    }
    memcpy(&count, in + header, sizeof(count));
    if (count > IB_TEST_MAX_ENTRIES)
    {
        IB_TEST_Reject("SET_DATA count above maximum", cc, count, IB_TEST_MAX_ENTRIES);
        return;
    }
    if (actual < header + sizeof(count) + (size_t)count * sizeof(uint32))
    {
        IB_TEST_Reject("SET_DATA count beyond packet", cc, actual,
                       header + sizeof(count) + (size_t)count * sizeof(uint32));
        return;
    }
    tail = actual - header - sizeof(count) - (size_t)count * sizeof(uint32);
    if (tail > IB_TEST_MAX_TEXT)
    {
        IB_TEST_Reject("SET_DATA text above maximum", cc, tail, IB_TEST_MAX_TEXT);
        return;
    }

    memcpy(IB_TEST_Data.Data, in + header + sizeof(count), (size_t)count * sizeof(uint32));
    memcpy(IB_TEST_Data.Text, in + header + sizeof(count) + (size_t)count * sizeof(uint32), tail);
    IB_TEST_Data.Entries = count;
    IB_TEST_Data.TextLen = (uint16)tail;
    IB_TEST_Data.CmdCounter++;
    CFE_EVS_SendEvent(IB_TEST_SET_DATA_EID, CFE_EVS_EventType_INFORMATION,
                      "IB_TEST: SET_DATA stored %u entries and %u text bytes", (unsigned int)count,
                      (unsigned int)tail);
    IB_TEST_PublishVar();
}

static void IB_TEST_ProcessGroundCommand(const CFE_SB_Buffer_t *buf)
{
    CFE_MSG_FcnCode_t cc = 0;

    CFE_MSG_GetFcnCode(&buf->Msg, &cc);
    switch (cc)
    {
        case IB_TEST_NOOP_CC:
            if (IB_TEST_HasSize(&buf->Msg, cc, "NOOP", sizeof(IB_TEST_NoArgsCmd_t)))
            {
                IB_TEST_Data.CmdCounter++;
                CFE_EVS_SendEvent(IB_TEST_NOOP_EID, CFE_EVS_EventType_INFORMATION, "IB_TEST: NOOP");
            }
            break;
        case IB_TEST_RESET_COUNTERS_CC:
            if (IB_TEST_HasSize(&buf->Msg, cc, "RESET_COUNTERS", sizeof(IB_TEST_NoArgsCmd_t)))
            {
                IB_TEST_Data.CmdCounter = 0;
                IB_TEST_Data.ErrCounter = 0;
                CFE_EVS_SendEvent(IB_TEST_RESET_EID, CFE_EVS_EventType_INFORMATION,
                                  "IB_TEST: RESET_COUNTERS");
            }
            break;
        case IB_TEST_SET_DATA_CC:
            IB_TEST_SetData(buf, cc);
            break;
        case IB_TEST_SEND_VAR_CC:
            if (IB_TEST_HasSize(&buf->Msg, cc, "SEND_VAR", sizeof(IB_TEST_NoArgsCmd_t)))
            {
                IB_TEST_Data.CmdCounter++;
                IB_TEST_PublishVar();
            }
            break;
        default:
            IB_TEST_Data.ErrCounter++;
            CFE_EVS_SendEvent(IB_TEST_CC_ERR_EID, CFE_EVS_EventType_ERROR,
                              "IB_TEST: invalid command code %u", (unsigned int)cc);
            break;
    }
}

static void IB_TEST_TaskPipe(const CFE_SB_Buffer_t *buf)
{
    CFE_SB_MsgId_t id = CFE_SB_INVALID_MSG_ID;

    CFE_MSG_GetMsgId(&buf->Msg, &id);
    if (CFE_SB_MsgId_Equal(id, CFE_SB_ValueToMsgId(IB_TEST_SEND_HK_MID)))
    {
        IB_TEST_PublishHk();
    }
    else if (CFE_SB_MsgId_Equal(id, CFE_SB_ValueToMsgId(IB_TEST_CMD_MID)))
    {
        IB_TEST_ProcessGroundCommand(buf);
    }
    else
    {
        CFE_EVS_SendEvent(IB_TEST_MID_ERR_EID, CFE_EVS_EventType_ERROR,
                          "IB_TEST: invalid command packet, MID = 0x%x",
                          (unsigned int)CFE_SB_MsgIdToValue(id));
    }
}

static CFE_Status_t IB_TEST_Init(void)
{
    CFE_Status_t status;

    memset(&IB_TEST_Data, 0, sizeof(IB_TEST_Data));
    IB_TEST_Data.RunStatus = CFE_ES_RunStatus_APP_RUN;

    status = CFE_EVS_Register(NULL, 0, CFE_EVS_EventFilter_BINARY);
    if (status != CFE_SUCCESS)
    {
        CFE_ES_WriteToSysLog("IB_TEST: error registering events, RC = 0x%08lX\n", (unsigned long)status);
        return status;
    }

    CFE_MSG_Init(CFE_MSG_PTR(IB_TEST_Data.HkTlm.TelemetryHeader), CFE_SB_ValueToMsgId(IB_TEST_HK_TLM_MID),
                 sizeof(IB_TEST_Data.HkTlm));
    CFE_MSG_Init(CFE_MSG_PTR(IB_TEST_Data.VarTlm.TelemetryHeader),
                 CFE_SB_ValueToMsgId(IB_TEST_VAR_TLM_MID), sizeof(IB_TEST_Data.VarTlm));

    status = CFE_SB_CreatePipe(&IB_TEST_Data.CommandPipe, IB_TEST_PIPE_DEPTH, IB_TEST_PIPE_NAME);
    if (status == CFE_SUCCESS)
    {
        status = CFE_SB_Subscribe(CFE_SB_ValueToMsgId(IB_TEST_SEND_HK_MID), IB_TEST_Data.CommandPipe);
    }
    if (status == CFE_SUCCESS)
    {
        status = CFE_SB_Subscribe(CFE_SB_ValueToMsgId(IB_TEST_CMD_MID), IB_TEST_Data.CommandPipe);
    }
    if (status != CFE_SUCCESS)
    {
        CFE_EVS_SendEvent(IB_TEST_INIT_ERR_EID, CFE_EVS_EventType_ERROR,
                          "IB_TEST: initialization failed, RC = 0x%08lX", (unsigned long)status);
        return status;
    }

    CFE_EVS_SendEvent(IB_TEST_INIT_EID, CFE_EVS_EventType_INFORMATION, "IB_TEST initialized");
    return CFE_SUCCESS;
}

void IB_TEST_Main(void)
{
    CFE_SB_Buffer_t *buf = NULL;

    if (IB_TEST_Init() != CFE_SUCCESS)
    {
        IB_TEST_Data.RunStatus = CFE_ES_RunStatus_APP_ERROR;
    }

    while (CFE_ES_RunLoop(&IB_TEST_Data.RunStatus) == true)
    {
        if (CFE_SB_ReceiveBuffer(&buf, IB_TEST_Data.CommandPipe, CFE_SB_PEND_FOREVER) == CFE_SUCCESS)
        {
            IB_TEST_TaskPipe(buf);
        }
        else
        {
            CFE_EVS_SendEvent(IB_TEST_PIPE_ERR_EID, CFE_EVS_EventType_ERROR,
                              "IB_TEST: pipe read error, app will exit");
            IB_TEST_Data.RunStatus = CFE_ES_RunStatus_APP_ERROR;
        }
    }

    CFE_ES_ExitApp(IB_TEST_Data.RunStatus);
}
