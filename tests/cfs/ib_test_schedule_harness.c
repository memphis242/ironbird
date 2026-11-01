/*
 * Host-side harness for the ib_test generator's schedule (tests/cfs/test_generator_schedule.py).
 *
 * It includes ib_test.c itself and replaces the PSP clock and the software bus with fakes, so the
 * generator task's tick runs against a clock the harness sets. It prints "FAIL: ..." for every
 * broken expectation and "ALL OK" when there is none; the exit code is 0 only for ALL OK.
 */
#include "ib_test.c"

#include <stdio.h>
#include <stdlib.h>

static int64         fake_now_ns;
static unsigned long transmits;
static uint32        last_serial;
static int           serial_seen;
static int           serials_consecutive = 1;
static int           failures;

#define CHECK(cond, ...)                \
    do                                  \
    {                                   \
        if (!(cond))                    \
        {                               \
            failures++;                 \
            printf("FAIL: " __VA_ARGS__); \
            printf("\n");               \
        }                               \
    } while (0)

/* The generator schedules on the PSP clock: the wall clock must never be read. */
void CFE_PSP_GetTime(OS_time_t *LocalTime)
{
    *LocalTime = OS_TimeFromTotalNanoseconds(fake_now_ns);
}

int32 OS_GetLocalTime(OS_time_t *time_struct)
{
    (void)time_struct;
    printf("FAIL: the generator read the wall clock\n");
    exit(2);
}

CFE_Status_t CFE_SB_TransmitMsg(const CFE_MSG_Message_t *Msg, bool IncrementSequenceCount)
{
    (void)IncrementSequenceCount;
    if (Msg == CFE_MSG_PTR(IB_TEST_Data.Gen.Tlm.TelemetryHeader))
    {
        const uint32 serial = IB_TEST_Data.Gen.Tlm.Payload.Serial;

        if (serial_seen && serial != (uint32)(last_serial + 1))
        {
            serials_consecutive = 0;
        }
        last_serial = serial;
        serial_seen = 1;
    }
    transmits++;
    return CFE_SUCCESS;
}

void CFE_SB_TimeStampMsg(CFE_MSG_Message_t *MsgPtr)
{
    (void)MsgPtr;
}

CFE_Status_t CFE_EVS_SendEvent(uint16 EventID, uint16 EventType, const char *Spec, ...)
{
    (void)EventID;
    (void)EventType;
    (void)Spec;
    return CFE_SUCCESS;
}

static unsigned long Tick(void)
{
    const unsigned long before = transmits;

    IB_TEST_GenTickLocked();
    return transmits - before;
}

static void StartRun(uint32 rate, uint32 target, uint64 sent, int64 start_ns)
{
    IB_TEST_Gen_t *gen = &IB_TEST_Data.Gen;

    gen->Run      = 1;
    gen->Rate     = rate;
    gen->Target   = target;
    gen->Sent     = sent;
    gen->MaxBurst = 0;
    gen->StartNs  = start_ns;
    gen->State    = IB_TEST_GEN_STATE_RUNNING;
}

int main(void)
{
    IB_TEST_Gen_t *gen = &IB_TEST_Data.Gen;
    int            i;

    /* Across the 2^32 wrap of the packet count in a Count = 0 run at 10k/s: 10 packets per 1 ms
     * tick throughout, the serial wraps to 0, and Sent (64-bit) keeps counting. */
    StartRun(10000, 0, 0xFFFFFFF0ULL, 0);
    fake_now_ns = (int64)(0xFFFFFFF0ULL * 100000ULL); /* due == Sent + 1 */
    CHECK(Tick() == 1, "the packet due at the start");
    for (i = 0; i < 2000; i++)
    {
        unsigned long n;

        fake_now_ns += 1000000;
        n = Tick();
        CHECK(n == 10, "tick %d across the wrap published %lu, Sent=%llu", i, n,
              (unsigned long long)gen->Sent);
    }
    CHECK(gen->Sent == 0xFFFFFFF0ULL + 1 + 20000, "Sent = %llu", (unsigned long long)gen->Sent);
    CHECK(gen->Sent > 0xFFFFFFFFULL, "Sent did not pass 2^32");
    CHECK(serials_consecutive, "serials are not consecutive modulo 2^32");
    CHECK(last_serial == (uint32)(gen->Sent - 1), "last serial %u", (unsigned)last_serial);
    CHECK(gen->Tlm.Payload.Fill[0] == (uint8)last_serial, "fill follows the wrapped serial");
    IB_TEST_GenPublishStatusLocked();
    CHECK(gen->StatusTlm.Payload.Sent == (uint32)gen->Sent, "status Sent is the low 32 bits");

    /* The due count equals the exact value for elapsed times up to the largest int64. */
    {
        const int64  elapsed[] = {0,
                                  1,
                                  999999999LL,
                                  1000000000LL,
                                  1844674407370955LL,
                                  86400LL * 30 * 1000000000LL,
                                  86400LL * 365 * 100 * 1000000000LL,
                                  9223372036854775807LL};
        const uint32 rates[]   = {1, 7, 1000, 9999, 10000};
        size_t       a;
        size_t       b;

        for (a = 0; a < sizeof(elapsed) / sizeof(elapsed[0]); a++)
        {
            for (b = 0; b < sizeof(rates) / sizeof(rates[0]); b++)
            {
                const uint64 got = IB_TEST_GenDue(elapsed[a], rates[b]);
                /* 1 + floor(elapsed * rate / 1e9) in 128-bit arithmetic (a GNU extension) */
                const unsigned __int128 want =
                    1 + ((unsigned __int128)(uint64)elapsed[a] * rates[b]) / 1000000000ULL;

                CHECK((unsigned __int128)got == want, "due(%lld, %u) = %llu", (long long)elapsed[a],
                      (unsigned)rates[b], (unsigned long long)got);
            }
        }
    }

    /* Where elapsed * Rate once passed 2^64 (21.4 days at 10k/s), the rate still holds. */
    fake_now_ns = (int64)(1844674407370955ULL + 1000000ULL);
    StartRun(10000, 0, 0, 0);
    gen->Sent = IB_TEST_GenDue(fake_now_ns, gen->Rate) - 5;
    CHECK(Tick() == 5, "the catch-up after 21 days");
    for (i = 0; i < 100; i++)
    {
        unsigned long n;

        fake_now_ns += 1000000;
        n = Tick();
        CHECK(n == 10, "steady tick %d after 21 days published %lu", i, n);
    }

    /* A generator that was descheduled catches up at most IB_TEST_GEN_MAX_BURST per tick. */
    fake_now_ns += 50 * 1000000LL; /* 500 packets due */
    CHECK(Tick() == IB_TEST_GEN_MAX_BURST, "burst cap");
    CHECK(gen->MaxBurst == IB_TEST_GEN_MAX_BURST, "MaxBurst %u", (unsigned)gen->MaxBurst);

    /* A counted run publishes exactly Count packets and finishes. */
    StartRun(10000, 25, 0, fake_now_ns);
    for (i = 0; i < 10 && gen->State == IB_TEST_GEN_STATE_RUNNING; i++)
    {
        (void)Tick();
        fake_now_ns += 1000000;
    }
    CHECK(gen->State == IB_TEST_GEN_STATE_FINISHED && gen->Sent == 25, "counted run: state %u, Sent %llu",
          (unsigned)gen->State, (unsigned long long)gen->Sent);

    /* A clock reading before the run's start (it cannot with a monotonic clock) publishes
     * nothing extra. */
    StartRun(10000, 0, 1000, 100000000LL);
    fake_now_ns = 0;
    CHECK(Tick() == 0, "a clock before the start published packets");

    if (failures == 0)
    {
        printf("ALL OK\n");
    }
    return failures != 0;
}
