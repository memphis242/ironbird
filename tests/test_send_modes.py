"""``ib send`` modes (--at, --repeat, --every with --until/--for) and --raw-enum.

The schedule is tested on a simulated clock (exact, no sleeping); the command is tested end to
end against a UDP sink on loopback (counts and timing with tolerances meant for a loaded CI
host). Nothing here asserts on wording: codes, exit statuses, counts and ledger fields only.
Ports are in 58000-58999.
"""

from __future__ import annotations

import itertools
import json
import signal
import socket
import threading
import time
from collections.abc import Callable, Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from ironbird import sendmodes as sm
from ironbird.errors import Code, IronbirdError
from recutil import Proc, run_ib
from replayutil import child_env, write_config_58k

MS = 1_000_000
SEC = 1_000_000_000


# -- the schedule on a simulated clock --------------------------------------------------------


class FakeClock:
    """Time that moves only when something sleeps or a send takes time."""

    def __init__(self, wall_ns: int = 1_800_000_000 * SEC) -> None:
        self.mono = 5 * SEC
        self.offset = wall_ns - self.mono
        self.sleeps = 0

    def wall_ns(self) -> int:
        return self.mono + self.offset

    def mono_ns(self) -> int:
        return self.mono

    async def sleep(self, seconds: float) -> None:
        self.sleeps += seconds > 0.01
        self.mono += max(round(seconds * SEC), 1_000)  # a yield still takes a moment


def run_plan(
    plan: sm.SendPlan,
    clock: FakeClock,
    cost: int | Callable[[int], int] = 0,
    progress: sm.Progress | None = None,
) -> tuple[sm.Tally, list[int]]:
    """Run ``plan``; ``cost(n)`` is the nanoseconds the n-th send takes. Returns the tally and
    the monotonic instant each send began."""
    import asyncio

    tally = sm.Tally(planned=0)
    began: list[int] = []

    async def send() -> None:
        n = len(began)
        began.append(clock.mono)
        clock.mono += cost(n) if callable(cost) else cost

    asyncio.run(sm.execute(plan, clock, send, tally, progress))
    return tally, began


def plan_for(clock: FakeClock, **flags: Any) -> sm.SendPlan:
    spec = sm.parse_spec(
        flags.get("at"),
        flags.get("repeat"),
        flags.get("every"),
        flags.get("until"),
        flags.get("for_"),
    )
    return sm.resolve(spec, clock.wall_ns(), clock.wall_ns())


def test_periodic_sends_sit_on_an_absolute_grid_without_drift() -> None:
    clock = FakeClock()
    tally, began = run_plan(plan_for(clock, every="10ms", for_="1s"), clock, cost=100_000)
    assert tally.sent == 100 and tally.skipped == 0 and tally.planned == 100
    gaps = {b - a for a, b in itertools.pairwise(began)}
    assert gaps == {10 * MS}  # exactly the period, however long a send took
    assert tally.late_max_ns == 0


def test_window_is_half_open_and_repeat_caps_it() -> None:
    clock = FakeClock()
    tally, _ = run_plan(plan_for(clock, every="1s", for_="3s"), clock)
    assert tally.sent == 3  # at 0, 1 and 2 s, not at 3 s
    clock = FakeClock()
    tally, _ = run_plan(plan_for(clock, every="1s", for_="10s", repeat=4), clock)
    assert tally.sent == 4 and tally.planned == 4
    clock = FakeClock()
    tally, _ = run_plan(plan_for(clock, every="1s", repeat=3), clock)
    assert tally.sent == 3


def test_until_ends_at_an_absolute_time() -> None:
    clock = FakeClock()
    end = sm.iso_utc(clock.wall_ns() + 2 * SEC + 500 * MS)
    tally, _ = run_plan(plan_for(clock, every="1s", until=end), clock)
    assert tally.sent == 3  # 0, 1 and 2 s are before the end


def test_a_send_that_overruns_skips_slots_instead_of_catching_up() -> None:
    clock = FakeClock()
    # the 5th send (slot 4) takes 35 ms: slots 5 and 6 are lost, slot 7 is reached 5 ms late
    # (half a period) and is sent
    tally, began = run_plan(
        plan_for(clock, every="10ms", for_="200ms"), clock, cost=lambda n: 35 * MS if n == 4 else 0
    )
    assert tally.skipped == 2 and tally.sent == 20 - 2
    assert tally.overruns == 1
    gaps = [b - a for a, b in itertools.pairwise(began)]
    assert min(gaps) >= 5 * MS  # never closer than half a period: no burst to make up for it
    assert tally.late_max_ns <= 5 * MS


def test_skipped_slots_do_not_count_toward_repeat() -> None:
    clock = FakeClock()
    tally, _ = run_plan(
        plan_for(clock, every="10ms", repeat=10), clock, cost=lambda n: 35 * MS if n == 2 else 0
    )
    assert tally.sent == 10 and tally.skipped == 2


def test_a_stall_at_the_end_of_the_window_counts_only_slots_inside_it() -> None:
    clock = FakeClock()
    tally, _ = run_plan(
        plan_for(clock, every="10ms", for_="100ms"), clock, cost=lambda n: 500 * MS if n == 8 else 0
    )
    assert tally.sent == 9 and tally.skipped == 1  # only slot 9 was inside the window


def test_repeat_alone_is_a_burst_and_at_alone_is_one_packet() -> None:
    clock = FakeClock()
    tally, began = run_plan(plan_for(clock, repeat=5), clock, cost=1_000)
    assert tally.sent == 5 and tally.late_count == 0 and began == sorted(began)
    clock = FakeClock()
    tally, _ = run_plan(plan_for(clock, at="+3s"), clock)
    assert tally.sent == 1 and tally.planned == 1


def test_an_at_start_waits_for_the_wall_clock_then_runs_on_the_monotonic_one() -> None:
    clock = FakeClock()
    start_wall = clock.wall_ns() + 90 * SEC
    plan = plan_for(clock, at=sm.iso_utc(start_wall), every="1s", repeat=3)
    tally, began = run_plan(plan, clock)
    assert tally.sent == 3
    assert abs(clock.wall_ns() - start_wall - 2 * SEC) < 5 * MS  # 3 sends, the last at +2 s
    assert began[1] - began[0] == SEC and clock.sleeps < 20  # long waits sleep in few slices


def test_progress_is_reported_after_every_send() -> None:
    clock = FakeClock()
    seen: list[int] = []
    run_plan(plan_for(clock, every="10ms", repeat=4), clock, progress=lambda t: seen.append(t.sent))
    assert seen == [1, 2, 3, 4]


# -- the flags ---------------------------------------------------------------------------------


def usage_error(**flags: Any) -> IronbirdError:
    clock = FakeClock()
    with pytest.raises(IronbirdError) as info:
        plan_for(clock, **flags)
    assert info.value.code is Code.CLI_USAGE and info.value.exit_code == 2
    return info.value


@pytest.mark.parametrize(
    "flags",
    [
        {"every": "1s"},  # unbounded
        {"until": "+10s"},  # no --every
        {"for_": "10s"},  # no --every
        {"every": "1s", "until": "+10s", "for_": "10s"},  # two ends
        {"every": "1s", "repeat": 1},  # the period is never used
        {"every": "10s", "for_": "5s"},  # at most one packet
        {"every": "100us", "repeat": 5},  # finer than the loop can keep
        {"every": "5", "repeat": 5},  # a bare number has no unit
        {"every": "1x", "repeat": 5},
        {"every": "-1s", "repeat": 5},
        {"repeat": 0},
        {"repeat": sm.MAX_SENDS + 1},
        {"every": "1ms", "for_": "1000h"},  # more packets than the limit
        {"at": "yesterday"},
        {"at": "2000-01-01T00:00:00Z"},  # in the past
        {"at": "+0s"},
        {"at": "+10s", "every": "1s", "until": "+5s"},  # ends before it starts
        {"at": "+10s", "every": "10s", "until": "+15s"},  # one packet at most
        {"at": "+9999h"},  # a typo, not a plan
        {"at": "2026-13-45T25:00:00Z"},
    ],
)
def test_contradictory_or_meaningless_flags_are_coded_usage_errors(flags: dict[str, Any]) -> None:
    usage_error(**flags)


def test_times_without_a_zone_are_utc_and_the_plan_says_so() -> None:
    clock = FakeClock()
    future = clock.wall_ns() + 3600 * SEC
    bare = sm.iso_utc(future)[:-1]  # no Z
    plan = plan_for(clock, at=bare)
    assert plan.start_wall_ns == future // MS * MS and plan.assumed_utc
    assert sm.parse_time_arg(bare + "+02:00", "--at").assumed_utc is False
    zoned = sm.parse_time_arg(bare + "+02:00", "--at").absolute_ns
    assert zoned is not None and plan.start_wall_ns is not None
    assert zoned == plan.start_wall_ns - 7200 * SEC
    assert not plan_for(clock, at=sm.iso_utc(future)).assumed_utc
    assert plan_for(clock, at="+90s").start_wall_ns == clock.wall_ns() + 90 * SEC


def test_durations() -> None:
    assert sm.parse_duration("250ms", "--every") == 250 * MS
    assert sm.parse_duration("1m30s", "--every") == 90 * SEC
    assert sm.parse_duration("1.5s", "--every") == 1500 * MS


# -- ib send against a UDP sink ----------------------------------------------------------------


class Sink:
    """A UDP socket on the command port that timestamps what arrives."""

    def __init__(self, port: int) -> None:
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 << 20)
        self.sock.bind(("127.0.0.1", port))
        self.sock.settimeout(0.05)
        self.packets: list[tuple[int, bytes]] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                data, _ = self.sock.recvfrom(4096)
            except TimeoutError:
                continue
            except OSError:
                return
            self.packets.append((time.monotonic_ns(), data))

    @property
    def count(self) -> int:
        return len(self.packets)

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)
        self.sock.close()

    def sequences(self) -> list[int]:
        return [int.from_bytes(data[2:4], "big") & 0x3FFF for _, data in self.packets]


@contextmanager
def sink_for(port: int) -> Generator[Sink]:
    sink = Sink(port)
    try:
        yield sink
    finally:
        sink.close()


@pytest.fixture
def setup(tmp_path: Path) -> tuple[Path, int, dict[str, str]]:
    config, command_port, _ = write_config_58k(tmp_path)
    return config, command_port, child_env(tmp_path)


def ledger_events(tmp_path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for path in sorted((tmp_path / "state").glob("*.jsonl")):
        out += [json.loads(line) for line in path.read_text().splitlines()]
    return out


def ib_send(
    tmp_path: Path, config: Path, env: dict[str, str], *args: str
) -> tuple[Proc, list[str]]:
    argv = ["-c", str(config), "send", "fc", *args]
    return Proc(tmp_path, f"send{time.monotonic_ns()}", argv, env), argv


def consecutive(sequences: list[int]) -> bool:
    return all((b - a) % 16384 == 1 for a, b in itertools.pairwise(sequences))


def test_repeat_sends_n_packets_each_with_its_own_sequence(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    config, port, env = setup
    with sink_for(port) as sink:
        done = run_ib(
            tmp_path, ["-c", str(config), "send", "fc", "MOCK_NOOP", "--repeat", "7"], env
        )
        assert done.returncode == 0, done.stderr
        deadline = time.monotonic() + 5
        while sink.count < 7 and time.monotonic() < deadline:
            time.sleep(0.02)
        assert sink.count == 7
        assert consecutive(sink.sequences())
    events = ledger_events(tmp_path)
    assert [e["event"] for e in events].count("tx.command") == 7
    plan = next(e for e in events if e["event"] == "send.plan")
    outcome = next(e for e in events if e["event"] == "send.outcome")
    assert plan["planned"] == 7 and plan["mode"] == "repeat"
    assert outcome["outcome"] == "complete" and outcome["sent"] == 7


def test_every_for_keeps_the_period_and_reports_lateness(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    config, port, env = setup
    with sink_for(port) as sink:
        done = run_ib(
            tmp_path,
            ["-c", str(config), "send", "fc", "MOCK_NOOP", "--every", "50ms", "--for", "2s"],
            env,
        )
        assert done.returncode == 0, done.stderr
        time.sleep(0.2)
        count = sink.count
        times = [t for t, _ in sink.packets]
    assert 30 <= count <= 40  # 40 planned; a loaded host may skip some, never add
    gaps = sorted(b - a for a, b in itertools.pairwise(times))
    assert 40 * MS <= gaps[len(gaps) // 2] <= 60 * MS  # the median gap is the period
    outcome = next(e for e in ledger_events(tmp_path) if e["event"] == "send.outcome")
    assert outcome["sent"] == count and outcome["planned"] == 40
    assert outcome["sent"] + outcome["skipped"] <= 40
    assert 0 <= outcome["late_mean_ns"] <= outcome["late_max_ns"] < 25 * MS


def test_at_waits_for_the_start_and_sends_once(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    config, port, env = setup
    with sink_for(port) as sink:
        began = time.monotonic_ns()
        done = run_ib(tmp_path, ["-c", str(config), "send", "fc", "MOCK_NOOP", "--at", "+2s"], env)
        assert done.returncode == 0, done.stderr
        deadline = time.monotonic() + 5
        while sink.count < 1 and time.monotonic() < deadline:
            time.sleep(0.02)
        time.sleep(0.2)
        assert sink.count == 1
        assert sink.packets[0][0] - began >= 1_800 * MS  # not before the start time
    plan = next(e for e in ledger_events(tmp_path) if e["event"] == "send.plan")
    assert plan["mode"] == "at" and plan["start_utc"].endswith("Z")


def test_usage_errors_exit_2_and_send_nothing(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    config, port, env = setup
    bad_flags = [
        ["--every", "1s"],
        ["--until", "+5s"],
        ["--every", "1s", "--for", "5s", "--until", "+9s"],
        ["--at", "2001-01-01T00:00:00Z"],
        ["--repeat", "1", "--every", "1s"],
    ]
    with sink_for(port) as sink:
        for flags in bad_flags:
            done = run_ib(tmp_path, ["-c", str(config), "send", "fc", "MOCK_NOOP", *flags], env)
            assert done.returncode == 2, (flags, done.stderr)
            assert Code.CLI_USAGE.value in done.stderr
        time.sleep(0.2)
        assert sink.count == 0
    assert not [e for e in ledger_events(tmp_path) if e["event"] == "tx.command"]


def test_a_bad_argument_is_refused_before_a_scheduled_start(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    config, _, env = setup
    started = time.monotonic()
    done = run_ib(
        tmp_path,
        ["-c", str(config), "send", "fc", "MOCK_SET_MODE", "mode=NOPE", "pad=0", "--at", "+30s"],
        env,
    )
    assert done.returncode != 0 and Code.ENC_ARG.value in done.stderr
    assert time.monotonic() - started < 15  # it did not wait for the start to find out


@pytest.mark.parametrize("sig", [signal.SIGINT, signal.SIGTERM])
def test_a_signal_stops_the_series_between_sends_with_a_summary(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]], sig: signal.Signals
) -> None:
    config, port, env = setup
    with sink_for(port) as sink:
        proc, _ = ib_send(tmp_path, config, env, "MOCK_NOOP", "--every", "20ms", "--for", "60s")
        try:
            deadline = time.monotonic() + 20
            while sink.count < 5:
                assert proc.popen.poll() is None, proc.stderr
                assert time.monotonic() < deadline
                time.sleep(0.02)
            proc.popen.send_signal(sig)
            code = proc.finish(timeout=20)
        finally:
            proc.kill()
        assert code == 128 + int(sig), proc.stderr
        time.sleep(0.2)
        settled = sink.count
        time.sleep(0.3)
        assert sink.count == settled  # nothing after the process ended
    assert "Traceback" not in proc.stderr
    outcome = next(e for e in ledger_events(tmp_path) if e["event"] == "send.outcome")
    assert outcome["outcome"] == "interrupted" and abs(outcome["sent"] - settled) <= 1
    assert consecutive(sink.sequences())


def test_a_signal_while_waiting_for_the_start_sends_nothing(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    config, port, env = setup
    with sink_for(port) as sink:
        proc, _ = ib_send(tmp_path, config, env, "MOCK_NOOP", "--at", "+60s")
        try:
            proc.wait_for("starting at")
            proc.popen.send_signal(signal.SIGINT)
            code = proc.finish(timeout=20)
        finally:
            proc.kill()
        assert code == 130
        assert sink.count == 0
    outcome = next(e for e in ledger_events(tmp_path) if e["event"] == "send.outcome")
    assert outcome["outcome"] == "interrupted" and outcome["sent"] == 0


def test_a_transport_failure_mid_series_stops_it_with_the_coded_error(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    config, port, env = setup
    sink = Sink(port)
    proc, _ = ib_send(tmp_path, config, env, "MOCK_NOOP", "--every", "20ms", "--for", "60s")
    try:
        deadline = time.monotonic() + 20
        while sink.count < 5:
            assert proc.popen.poll() is None, proc.stderr
            assert time.monotonic() < deadline
            time.sleep(0.02)
        sink.close()  # the receiver goes away: the next sends are refused
        code = proc.finish(timeout=30)
    finally:
        proc.kill()
    assert code == 1, proc.stderr
    assert Code.TX_SEND.value in proc.stderr
    outcome = next(e for e in ledger_events(tmp_path) if e["event"] == "send.outcome")
    assert outcome["outcome"] == "failed" and outcome["error"] == Code.TX_SEND.value
    assert outcome["sent"] >= 5


def test_a_dead_receiver_is_found_on_the_first_packet_of_a_fast_series(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    config, _, env = setup  # nothing listens
    done = run_ib(
        tmp_path,
        ["-c", str(config), "send", "fc", "MOCK_NOOP", "--every", "5ms", "--for", "30s"],
        env,
        timeout=20,
    )
    assert done.returncode == 1 and Code.TX_SEND.value in done.stderr


# -- --raw-enum --------------------------------------------------------------------------------


def test_raw_enum_is_refused_without_the_flag_and_sent_with_it(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    config, port, env = setup
    base = ["-c", str(config), "send", "fc", "MOCK_SET_MODE", "mode=9", "pad=0"]
    with sink_for(port) as sink:
        refused = run_ib(tmp_path, base, env)
        assert refused.returncode == 2 and Code.ENC_ARG.value in refused.stderr
        assert "--raw-enum" in refused.stderr  # the way to send it deliberately
        time.sleep(0.1)
        assert sink.count == 0

        sent = run_ib(tmp_path, [*base, "--raw-enum"], env)
        assert sent.returncode == 0, sent.stderr
        series = run_ib(tmp_path, [*base, "--raw-enum", "--repeat", "3"], env)
        assert series.returncode == 0, series.stderr
        deadline = time.monotonic() + 5
        while sink.count < 4 and time.monotonic() < deadline:
            time.sleep(0.02)
        assert sink.count == 4
    events = ledger_events(tmp_path)
    faults = [e for e in events if e["event"] == "send.fault" and e["fault"] == "raw_enum"]
    assert faults and faults[0]["fields"] == [{"field": "mode", "value": 9}]
    plan = next(e for e in events if e["event"] == "send.plan")
    assert plan["request"]["raw_enums"] == [{"field": "mode", "value": 9}]


def test_raw_enum_with_nothing_to_allow_is_a_usage_error(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    config, _, env = setup
    done = run_ib(
        tmp_path,
        ["-c", str(config), "send", "fc", "MOCK_SET_MODE", "mode=SAFE", "pad=0", "--raw-enum"],
        env,
    )
    assert done.returncode == 2 and Code.CLI_USAGE.value in done.stderr


# -- performance -------------------------------------------------------------------------------


def test_one_millisecond_period_is_sustained_on_loopback(
    tmp_path: Path, setup: tuple[Path, int, dict[str, str]]
) -> None:
    """The target is --every 1ms with lateness well under the period; the floors here are loose
    because CI hosts are shared. The lateness figures come from the series' own ledger record."""
    config, port, env = setup
    with sink_for(port) as sink:
        done = run_ib(
            tmp_path,
            ["-c", str(config), "send", "fc", "MOCK_NOOP", "--every", "1ms", "--for", "3s"],
            env,
        )
        assert done.returncode == 0, done.stderr
        time.sleep(0.3)
        received = sink.count
    outcome = next(e for e in ledger_events(tmp_path) if e["event"] == "send.outcome")
    assert outcome["planned"] == 3000
    assert outcome["sent"] + outcome["skipped"] <= 3000
    assert outcome["sent"] >= 0.5 * 3000, outcome  # a shared host may preempt the spinning loop
    assert received >= 0.9 * outcome["sent"]  # loopback drops almost nothing
    assert outcome["late_mean_ns"] < 500_000, outcome
