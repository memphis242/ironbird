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
    IB_TEST_Data.HkCounter++;
    IB_TEST_Data.HkTlm.Payload.CmdCounter = IB_TEST_Data.CmdCounter;
    IB_TEST_Data.HkTlm.Payload.ErrCounter = IB_TEST_Data.ErrCounter;
    IB_TEST_Data.HkTlm.Payload.Entries    = IB_TEST_Data.Entries;
    IB_TEST_Data.HkTlm.Payload.TextLen    = IB_TEST_Data.TextLen;
    IB_TEST_Data.HkTlm.Payload.HkCounter  = IB_TEST_Data.HkCounter;
    CFE_SB_TimeStampMsg(CFE_MSG_PTR(IB_TEST_Data.HkTlm.TelemetryHeader));
    CFE_SB_TransmitMsg(CFE_MSG_PTR(IB_TEST_Data.HkTlm.TelemetryHeader), true);
}

/* Pack the stored data as Count, Entries[Count], Text (no terminator), size the packet to it and
 * send it. */
static void IB_TEST_PublishVar(void)
{
    uint8 *out   = IB_TEST_Data.VarTlm.Payload;
    size_t size  = 0;
    uint16 count = IB_TEST_Data.Entries;

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

/* -- throughput generator ------------------------------------------------------------------- */

/* The PSP's time source: the platform timebase, which on the Linux PSP is CLOCK_MONOTONIC. A
 * run's schedule must not move when the wall clock is stepped. */
static int64 IB_TEST_NowNs(void)
{
    OS_time_t now = OS_TIME_ZERO;

    CFE_PSP_GetTime(&now);
    return OS_TimeGetTotalNanoseconds(now);
}

/* Publish the status packet. The caller holds Gen.Lock. */
static void IB_TEST_GenPublishStatusLocked(void)
{
    IB_TEST_Gen_t *gen = &IB_TEST_Data.Gen;

    gen->StatusTlm.Payload.Run      = gen->Run;
    gen->StatusTlm.Payload.Rate     = gen->Rate;
    gen->StatusTlm.Payload.Target   = gen->Target;
    gen->StatusTlm.Payload.Sent     = (uint32)gen->Sent; /* wraps at 2^32 by design */
    gen->StatusTlm.Payload.State    = gen->State;
    gen->StatusTlm.Payload.Spare    = 0;
    gen->StatusTlm.Payload.MaxBurst = gen->MaxBurst;
    CFE_SB_TimeStampMsg(CFE_MSG_PTR(gen->StatusTlm.TelemetryHeader));
    CFE_SB_TransmitMsg(CFE_MSG_PTR(gen->StatusTlm.TelemetryHeader), true);
}

/* Publish one generator packet: the next Serial of the run, and the fill derived from it. Serial
 * is the low 32 bits of the run's packet count, so a run of 2^32 packets or more (Count = 0 at
 * 10k packets/s passes it after about 5 days) wraps to 0. The caller holds Gen.Lock. */
static void IB_TEST_GenPublishOneLocked(void)
{
    IB_TEST_Gen_t *gen    = &IB_TEST_Data.Gen;
    const uint32   serial = (uint32)gen->Sent;
    size_t         i;

    gen->Tlm.Payload.Serial = serial;
    gen->Tlm.Payload.Run    = gen->Run;
    for (i = 0; i < IB_TEST_GEN_FILL_BYTES; i++)
    {
        gen->Tlm.Payload.Fill[i] = (uint8)(serial + i);
    }
    CFE_SB_TimeStampMsg(CFE_MSG_PTR(gen->Tlm.TelemetryHeader));
    CFE_SB_TransmitMsg(CFE_MSG_PTR(gen->Tlm.TelemetryHeader), true);
    gen->Sent++;
}

/* The packets due after "elapsed_ns" of a run at "rate" packets/s: the first at time zero, then
 * one per 1/rate. Whole seconds and the remainder are multiplied separately, so no elapsed time
 * a signed 64-bit nanosecond count can hold overflows the 64-bit product. */
static uint64 IB_TEST_GenDue(int64 elapsed_ns, uint32 rate)
{
    uint64 due = 1;

    if (elapsed_ns > 0)
    {
        const uint64 whole     = (uint64)elapsed_ns / IB_TEST_NS_PER_SEC;
        const uint64 remainder = (uint64)elapsed_ns % IB_TEST_NS_PER_SEC;

        due += whole * rate + (remainder * rate) / IB_TEST_NS_PER_SEC;
    }
    return due;
}

/* One tick of a running generator: the packets due since the run began, at most one burst. The
 * schedule is the run's start time and its Rate, so a late tick catches up on the next ones
 * instead of shifting the rate. The caller holds Gen.Lock. */
static void IB_TEST_GenTickLocked(void)
{
    IB_TEST_Gen_t *gen   = &IB_TEST_Data.Gen;
    uint64         due   = IB_TEST_GenDue(IB_TEST_NowNs() - gen->StartNs, gen->Rate);
    uint32         burst = 0;

    if (gen->Target != 0 && due > gen->Target)
    {
        due = gen->Target;
    }
    while (gen->Sent < due && burst < IB_TEST_GEN_MAX_BURST)
    {
        IB_TEST_GenPublishOneLocked();
        burst++;
    }
    if (burst > gen->MaxBurst)
    {
        gen->MaxBurst = (uint16)burst;
    }
    if (gen->Target != 0 && gen->Sent >= gen->Target)
    {
        gen->State = IB_TEST_GEN_STATE_FINISHED;
        CFE_EVS_SendEvent(IB_TEST_GEN_DONE_EID, CFE_EVS_EventType_INFORMATION,
                          "IB_TEST: GEN run %u done, %u packets", (unsigned int)gen->Run,
                          (unsigned int)gen->Sent);
        IB_TEST_GenPublishStatusLocked();
    }
}

/* The generator task: ticks every IB_TEST_GEN_TICK_MS while a run is on, else waits for GEN_START. */
static void IB_TEST_GenTaskMain(void)
{
    IB_TEST_Gen_t *gen = &IB_TEST_Data.Gen;

    while (IB_TEST_Data.RunStatus == CFE_ES_RunStatus_APP_RUN)
    {
        bool running;

        OS_MutSemTake(gen->Lock);
        running = (gen->State == IB_TEST_GEN_STATE_RUNNING);
        if (running)
        {
            IB_TEST_GenTickLocked();
        }
        OS_MutSemGive(gen->Lock);

        if (running)
        {
            OS_TaskDelay(IB_TEST_GEN_TICK_MS);
        }
        else
        {
            OS_BinSemTimedWait(gen->Wake, IB_TEST_GEN_IDLE_WAIT_MS);
        }
    }
    CFE_ES_ExitChildTask();
}

static CFE_Status_t IB_TEST_GenInit(void)
{
    IB_TEST_Gen_t *gen = &IB_TEST_Data.Gen;
    int32          os  = OS_MutSemCreate(&gen->Lock, "IB_TEST_GEN_LOCK", 0);

    if (os == OS_SUCCESS)
    {
        os = OS_BinSemCreate(&gen->Wake, "IB_TEST_GEN_WAKE", 0, 0);
    }
    if (os != OS_SUCCESS)
    {
        CFE_ES_WriteToSysLog("IB_TEST: error creating the generator semaphores, RC = %d\n", (int)os);
        return CFE_STATUS_EXTERNAL_RESOURCE_FAIL;
    }
    gen->State = IB_TEST_GEN_STATE_IDLE;

    CFE_MSG_Init(CFE_MSG_PTR(gen->Tlm.TelemetryHeader), CFE_SB_ValueToMsgId(IB_TEST_GEN_TLM_MID),
                 sizeof(gen->Tlm));
    CFE_MSG_Init(CFE_MSG_PTR(gen->StatusTlm.TelemetryHeader),
                 CFE_SB_ValueToMsgId(IB_TEST_GEN_STATUS_TLM_MID), sizeof(gen->StatusTlm));

    return CFE_ES_CreateChildTask(&gen->Task, IB_TEST_GEN_TASK_NAME, IB_TEST_GenTaskMain,
                                  CFE_ES_TASK_STACK_ALLOCATE, IB_TEST_GEN_TASK_STACK,
                                  IB_TEST_GEN_TASK_PRIORITY, 0);
}

/* A command refused: "what" says which check failed, "got" is the offending value and "limit" the
 * value it was held to (the exact packet size, or the bound it had to stay within). */
static void IB_TEST_Reject(const char *what, CFE_MSG_FcnCode_t cc, size_t got, size_t limit)
{
    IB_TEST_Data.ErrCounter++;
    CFE_EVS_SendEvent(IB_TEST_LEN_ERR_EID, CFE_EVS_EventType_ERROR,
                      "IB_TEST: %s rejected: CC = %u, got %u, limit %u", what, (unsigned int)cc,
                      (unsigned int)got, (unsigned int)limit);
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
    size_t       tail   = 0;
    const uint8 *in = (const uint8 *)buf;

    CFE_MSG_GetSize(&buf->Msg, &actual);
    if (actual < header + sizeof(count))
    {
        IB_TEST_Reject("SET_DATA size below its fixed part", cc, actual, header + sizeof(count));
        return;
    }
    memcpy(&count, in + header, sizeof(count));
    if (count > IB_TEST_MAX_ENTRIES)
    {
        IB_TEST_Reject("SET_DATA count above its maximum", cc, count, IB_TEST_MAX_ENTRIES);
        return;
    }
    if (actual < header + sizeof(count) + (size_t)count * sizeof(uint32))
    {
        IB_TEST_Reject("SET_DATA size below what its count needs", cc, actual,
                       header + sizeof(count) + (size_t)count * sizeof(uint32));
        return;
    }
    tail = actual - header - sizeof(count) - (size_t)count * sizeof(uint32);
    if (tail > IB_TEST_MAX_TEXT)
    {
        IB_TEST_Reject("SET_DATA text length above its maximum", cc, tail, IB_TEST_MAX_TEXT);
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

/* GEN_START: Rate and Count, validated here; refused while a run is on. */
static void IB_TEST_GenStart(const CFE_SB_Buffer_t *buf, CFE_MSG_FcnCode_t cc)
{
    IB_TEST_Gen_t               *gen = &IB_TEST_Data.Gen;
    const IB_TEST_GenStartCmd_t *cmd = (const IB_TEST_GenStartCmd_t *)buf;

    if (!IB_TEST_HasSize(&buf->Msg, cc, "GEN_START size", sizeof(IB_TEST_GenStartCmd_t)))
    {
        return;
    }
    if (cmd->Rate < 1 || cmd->Rate > IB_TEST_GEN_MAX_RATE)
    {
        IB_TEST_Reject("GEN_START rate outside 1..maximum", cc, cmd->Rate, IB_TEST_GEN_MAX_RATE);
        return;
    }
    OS_MutSemTake(gen->Lock);
    if (gen->State == IB_TEST_GEN_STATE_RUNNING)
    {
        const uint32 run = gen->Run;

        OS_MutSemGive(gen->Lock);
        IB_TEST_Reject("GEN_START while a run is on", cc, run, 0);
        return;
    }
    gen->Run++;
    gen->Rate     = cmd->Rate;
    gen->Target   = cmd->Count;
    gen->Sent     = 0;
    gen->MaxBurst = 0;
    gen->StartNs  = IB_TEST_NowNs();
    gen->State    = IB_TEST_GEN_STATE_RUNNING;
    OS_MutSemGive(gen->Lock);
    OS_BinSemGive(gen->Wake);

    IB_TEST_Data.CmdCounter++;
    CFE_EVS_SendEvent(IB_TEST_GEN_START_EID, CFE_EVS_EventType_INFORMATION,
                      "IB_TEST: GEN_START rate %u count %u", (unsigned int)cmd->Rate,
                      (unsigned int)cmd->Count);
}

/* GEN_STOP: ends a run that is on and publishes its status; no run on is an error. */
static void IB_TEST_GenStop(const CFE_SB_Buffer_t *buf, CFE_MSG_FcnCode_t cc)
{
    IB_TEST_Gen_t *gen = &IB_TEST_Data.Gen;

    if (!IB_TEST_HasSize(&buf->Msg, cc, "GEN_STOP size", sizeof(IB_TEST_NoArgsCmd_t)))
    {
        return;
    }
    OS_MutSemTake(gen->Lock);
    if (gen->State != IB_TEST_GEN_STATE_RUNNING)
    {
        const uint32 state = gen->State;

        OS_MutSemGive(gen->Lock);
        IB_TEST_Reject("GEN_STOP with no run on", cc, state, IB_TEST_GEN_STATE_RUNNING);
        return;
    }
    gen->State = IB_TEST_GEN_STATE_STOPPED;
    CFE_EVS_SendEvent(IB_TEST_GEN_STOP_EID, CFE_EVS_EventType_INFORMATION,
                      "IB_TEST: GEN run %u stopped, %u packets", (unsigned int)gen->Run,
                      (unsigned int)gen->Sent);
    IB_TEST_GenPublishStatusLocked();
    OS_MutSemGive(gen->Lock);
    IB_TEST_Data.CmdCounter++;
}

static void IB_TEST_GenSendStatus(const CFE_SB_Buffer_t *buf, CFE_MSG_FcnCode_t cc)
{
    IB_TEST_Gen_t *gen = &IB_TEST_Data.Gen;

    if (!IB_TEST_HasSize(&buf->Msg, cc, "GEN_SEND_STATUS size", sizeof(IB_TEST_NoArgsCmd_t)))
    {
        return;
    }
    OS_MutSemTake(gen->Lock);
    IB_TEST_GenPublishStatusLocked();
    OS_MutSemGive(gen->Lock);
    IB_TEST_Data.CmdCounter++;
}

static void IB_TEST_ProcessGroundCommand(const CFE_SB_Buffer_t *buf)
{
    CFE_MSG_FcnCode_t cc = 0;

    CFE_MSG_GetFcnCode(&buf->Msg, &cc);
    switch (cc)
    {
        case IB_TEST_NOOP_CC:
            if (IB_TEST_HasSize(&buf->Msg, cc, "NOOP size", sizeof(IB_TEST_NoArgsCmd_t)))
            {
                IB_TEST_Data.CmdCounter++;
                CFE_EVS_SendEvent(IB_TEST_NOOP_EID, CFE_EVS_EventType_INFORMATION, "IB_TEST: NOOP");
            }
            break;
        case IB_TEST_RESET_COUNTERS_CC:
            if (IB_TEST_HasSize(&buf->Msg, cc, "RESET_COUNTERS size", sizeof(IB_TEST_NoArgsCmd_t)))
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
            if (IB_TEST_HasSize(&buf->Msg, cc, "SEND_VAR size", sizeof(IB_TEST_NoArgsCmd_t)))
            {
                IB_TEST_Data.CmdCounter++;
                IB_TEST_PublishVar();
            }
            break;
        case IB_TEST_GEN_START_CC:
            IB_TEST_GenStart(buf, cc);
            break;
        case IB_TEST_GEN_STOP_CC:
            IB_TEST_GenStop(buf, cc);
            break;
        case IB_TEST_GEN_SEND_STATUS_CC:
            IB_TEST_GenSendStatus(buf, cc);
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

    status = IB_TEST_GenInit();
    if (status != CFE_SUCCESS)
    {
        CFE_EVS_SendEvent(IB_TEST_INIT_ERR_EID, CFE_EVS_EventType_ERROR,
                          "IB_TEST: generator initialization failed, RC = 0x%08lX",
                          (unsigned long)status);
        return status;
    }

    status = CFE_SB_CreatePipe(&IB_TEST_Data.CommandPipe, IB_TEST_PIPE_DEPTH, IB_TEST_PIPE_NAME);
    if (status == CFE_SUCCESS)
    {
        status = CFE_SB_SubscribeEx(CFE_SB_ValueToMsgId(IB_TEST_SEND_HK_MID),
                                    IB_TEST_Data.CommandPipe, CFE_SB_DEFAULT_QOS,
                                    IB_TEST_PIPE_MSG_LIMIT);
    }
    if (status == CFE_SUCCESS)
    {
        status = CFE_SB_SubscribeEx(CFE_SB_ValueToMsgId(IB_TEST_CMD_MID), IB_TEST_Data.CommandPipe,
                                    CFE_SB_DEFAULT_QOS, IB_TEST_PIPE_MSG_LIMIT);
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
