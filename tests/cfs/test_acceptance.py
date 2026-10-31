"""Stage 1 acceptance against a real cFS instance, driven only through the ``ib`` CLI.

Opt-in (marker ``cfs``): skipped unless ``IRONBIRD_CFS=1`` is set or ``-m cfs`` is given,
and then skipped (or, with ``IRONBIRD_CFS=1``, failed) when the cFS build is missing. See
``tests/conftest.py`` for the gate and ``tools/cfs/`` for the build and run scripts.

One cFS instance is shared by every test in this module, so each test measures counter
*deltas* against a fresh baseline instead of assuming a power-on state. Telemetry is read
from one long-running ``ib listen`` process; commands go out as separate ``ib send``
processes. Housekeeping is requested on demand (``*_SEND_HK`` through CI_LAB) and every
check re-requests until the expected state shows up, because SCH_LAB also publishes the same
HK packets periodically and a periodic one can arrive between a command and our request.
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Generator, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from ironbird.ccdd.loader import load_ccdd
from ironbird.ccdd.model import MessageKind
from ironbird.codec import Codec, DecodedPacket
from ironbird.config import load_config
from ironbird.header import load_profile

pytestmark = pytest.mark.cfs

REPO = Path(__file__).resolve().parents[2]
CONFIG = REPO / "examples" / "ironbird.toml"
RUN_SH = REPO / "tools" / "cfs" / "run.sh"
CLEAN_SH = REPO / "tools" / "cfs" / "clean.sh"
RAM_DIR = Path("/dev/shm/osal:RAM")  # OSAL's backing directory for the cFE /ram volume

CI_LAB_PORT = 1234
TO_LAB_PORT = 2234
STARTUP_TIMEOUT_S = 60.0
SUBSEC = float(1 << 32)

Packet = dict[str, Any]


# -- helpers --------------------------------------------------------------------------------


def _udp_port_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        try:
            sock.bind(("0.0.0.0", port))
        except OSError:
            return False
        return True


def _wait_for_log(log: Path, needle: str, proc: subprocess.Popen[bytes], timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        text = log.read_text(errors="replace") if log.exists() else ""
        if needle in text:
            return
        if proc.poll() is not None:
            break
        time.sleep(0.1)
    tail = "\n".join((log.read_text(errors="replace") if log.exists() else "").splitlines()[-25:])
    raise AssertionError(f"cFS never printed {needle!r} (exit={proc.poll()}). Log tail:\n{tail}")


# CI uploads the pytest base directory as an artifact when this module fails, and
# actions/upload-artifact refuses the whole upload if any path holds one of these characters.
ARTIFACT_UNSAFE_CHARS = frozenset('":<>|*?\r\n')


@pytest.fixture(scope="module", autouse=True)
def _artifact_safe_paths(tmp_path_factory: pytest.TempPathFactory) -> Iterator[None]:
    """Fail if the tests leave a path CI could not upload, instead of losing the failure logs."""
    yield
    base = tmp_path_factory.getbasetemp()
    unsafe = [
        str(Path(root, name).relative_to(base))
        for root, dirs, files in os.walk(base)
        for name in (*dirs, *files)
        if not ARTIFACT_UNSAFE_CHARS.isdisjoint(name)
    ]
    assert not unsafe, f"paths actions/upload-artifact rejects: {unsafe[:5]}"


@pytest.fixture(scope="module")
def cfs(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Path]:
    """A running cFS (tools/cfs/run.sh); returns its console log. Stopped and cleaned after."""
    for port in (CI_LAB_PORT, TO_LAB_PORT):
        assert _udp_port_free(port), (
            f"UDP port {port} is already in use (another cFS instance running?); "
            "stop it before running the cfs tests"
        )
    ram_existed = RAM_DIR.exists()
    log = tmp_path_factory.mktemp("cfs") / "console.log"
    with log.open("wb") as sink:
        proc = subprocess.Popen(
            [str(RUN_SH)], stdout=sink, stderr=subprocess.STDOUT, start_new_session=True
        )
    try:
        # run.sh execs the cFS core, so proc is cFS itself (and SIGINT is its clean shutdown).
        _wait_for_log(log, "CI_LAB listening on UDP port: 1234", proc, STARTUP_TIMEOUT_S)
        _wait_for_log(log, "entering OPERATIONAL state", proc, STARTUP_TIMEOUT_S)
        _wait_for_log(log, "TO Lab subscribed to", proc, STARTUP_TIMEOUT_S)
        yield log
    finally:
        if proc.poll() is None:
            proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
        subprocess.run([str(CLEAN_SH)], check=False, timeout=30)  # the PSP's shm segments
        if not ram_existed:  # only remove what this run created
            shutil.rmtree(RAM_DIR, ignore_errors=True)


@dataclass
class Ground:
    """Drives the ``ib`` CLI: ``send`` per command, one long-running ``listen`` for telemetry."""

    env: dict[str, str]
    packets: list[tuple[float, Packet]] = field(default_factory=list)
    output_enabled: bool = False
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _proc: subprocess.Popen[str] | None = None
    _reader: threading.Thread | None = None
    _stderr_log: Path | None = None

    # -- lifecycle --

    def start_listener(self, stderr_log: Path) -> None:
        self._stderr_log = stderr_log
        with stderr_log.open("w") as err:
            self._proc = subprocess.Popen(
                [*self._ib(), "listen", "fc", "--format", "json"],
                stdout=subprocess.PIPE,
                stderr=err,
                text=True,
                env=self.env,
            )
        self._reader = threading.Thread(target=self._read, daemon=True)
        self._reader.start()
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if "listening" in stderr_log.read_text():
                return
            assert self._proc.poll() is None, f"ib listen died: {stderr_log.read_text()}"
            time.sleep(0.05)
        raise AssertionError("ib listen never reported that it is listening")

    def stop_listener(self) -> None:
        if self._proc is not None and self._proc.poll() is None:
            self._proc.send_signal(signal.SIGINT)
            try:
                self._proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self._proc.wait(timeout=5)
        if self._reader is not None:
            self._reader.join(timeout=5)

    @contextmanager
    def listener_paused(self) -> Generator[None, None, None]:
        """Stop ``ib listen`` (it owns UDP 2234) for the block, then start it again."""
        assert self._stderr_log is not None
        self.stop_listener()
        try:
            yield
        finally:
            self.start_listener(self._stderr_log)

    def _read(self) -> None:
        assert self._proc is not None and self._proc.stdout is not None
        for line in self._proc.stdout:
            record = json.loads(line)
            with self._lock:
                self.packets.append((time.monotonic(), record))

    # -- sending --

    @staticmethod
    def _ib() -> list[str]:
        return [sys.executable, "-m", "ironbird.cli.main", "-c", str(CONFIG)]

    def send(
        self, command: str, *args: str, flags: tuple[str, ...] = (), check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        """``ib send fc COMMAND [name=value...]``; uses this test's own ledger directory."""
        done = subprocess.run(
            [*self._ib(), "send", "fc", command, *args, *flags],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        if check:
            assert done.returncode == 0, f"ib send {command} failed: {done.stderr}"
            assert done.stdout.startswith("sent "), done.stdout
        return done

    # -- telemetry --

    def count(self) -> int:
        with self._lock:
            return len(self.packets)

    def since(self, index: int, name: str | None = None) -> list[Packet]:
        with self._lock:
            return [p for _, p in self.packets[index:] if name is None or p["name"] == name]

    def wait_for(
        self,
        name: str,
        predicate: Callable[[dict[str, Any]], bool],
        *,
        request: str | None = None,
        timeout: float = 20.0,
        start: int | None = None,
    ) -> Packet:
        """First ``name`` packet (after index ``start``) whose fields satisfy ``predicate``.

        With ``request`` (a ``*_SEND_HK``/``*_SEND_DIAGNOSTIC`` command), re-sends it about
        once a second until the packet shows up.
        """
        first = self.count() if start is None else start
        deadline = time.monotonic() + timeout
        last: list[Packet] = []
        next_request = 0.0
        while time.monotonic() < deadline:
            if request is not None and time.monotonic() >= next_request:
                self.send(request)
                next_request = time.monotonic() + 1.0
            last = self.since(first, name)
            for packet in last:
                if predicate(packet["fields"]):
                    return packet
            time.sleep(0.05)
        seen = [p["fields"] for p in last[-3:]]
        raise AssertionError(
            f"no {name} satisfying the predicate within {timeout} s; last seen: {seen}"
        )

    def fresh(self, name: str, request: str) -> dict[str, Any]:
        """Fields of the next ``name`` packet produced after we request it."""
        return self.wait_for(name, lambda _: True, request=request, timeout=10.0)["fields"]

    def enable_output(self) -> None:
        self.send("TO_LAB_ENABLE_OUTPUT", "dest_IP=127.0.0.1")
        self.output_enabled = True


def _delta(after: int, before: int) -> int:
    """Difference of two u8 counters, tolerant of wrap-around."""
    return (after - before) % 256


@pytest.fixture(scope="module")
def ground_session(cfs: Path, tmp_path_factory: pytest.TempPathFactory) -> Iterator[Ground]:
    """cFS up and an ``ib listen fc`` running, but TO_LAB output not yet enabled."""
    base = tmp_path_factory.mktemp("listener")
    env = {**os.environ, "IRONBIRD_STATE_DIR": str(base / "state")}
    env.pop("IRONBIRD_CONFIG", None)
    ground = Ground(env=env)
    ground.start_listener(base / "listen.stderr")
    try:
        yield ground
    finally:
        ground.stop_listener()


@pytest.fixture
def ground(ground_session: Ground) -> Ground:
    """Telemetry flowing: TO_LAB output enabled (idempotent), and a quiet start."""
    if not ground_session.output_enabled:
        ground_session.enable_output()
    return ground_session


def _ledger_events(state_dir: Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for path in sorted(state_dir.glob("*.jsonl")):
        events.extend(json.loads(line) for line in path.read_text().splitlines())
    return events


def _tx_commands(state_dir: Path, command: str) -> list[dict[str, Any]]:
    return [
        e
        for e in _ledger_events(state_dir)
        if e["event"] == "tx.command" and e["command"] == command
    ]


def _requests(state_dir: Path, command: str) -> list[dict[str, Any]]:
    return [
        e
        for e in _ledger_events(state_dir)
        if e["event"] == "send.request" and e["command"] == command
    ]


# -- case 1: TO_LAB enable output -> telemetry flows, ES and TIME HK decode sanely -------------


def test_to_lab_enable_output_starts_telemetry_and_hk_decodes_sanely(
    ground_session: Ground,
) -> None:
    g = ground_session
    # TO_LAB sends nothing before it is told where to send.
    time.sleep(1.0)
    if not g.output_enabled:
        assert g.count() == 0, "telemetry arrived before output was enabled"

    mark = g.count()
    g.enable_output()
    # Without any request, the periodic SCH_LAB housekeeping reaches us (4.0 s ES, 4.4 s TIME).
    es = g.wait_for("ES_HK", lambda _: True, start=mark, timeout=20)
    time_hk = g.wait_for("TIME_HK", lambda _: True, start=mark, timeout=20)

    f = es["fields"]
    assert (f["CFEMajorVersion"], f["CFEMinorVersion"], f["CFERevision"]) == (7, 0, 1)
    assert (f["OSALMajorVersion"], f["PSPMajorVersion"]) == (7, 7)
    assert f["SysLogBytesUsed"] <= f["SysLogSize"]
    assert f["SysLogEntries"] > 0
    assert f["RegisteredCoreApps"] >= 4
    assert 1 <= f["RegisteredExternalApps"] <= 16  # trimmed startup script runs a few apps
    assert f["RegisteredTasks"] >= f["RegisteredCoreApps"]
    assert es["labels"]["ResetType"] == "POWERON"  # fresh run.sh start
    assert f["ProcessorResets"] <= f["MaxProcessorResets"]
    assert es["mid"] == "0x0800" and es["kind"] == "telemetry"

    t = time_hk["fields"]
    assert 0 <= t["LeapSeconds"] <= 100
    assert t["MET_Subseconds"] < 2**32
    assert t["CommandErrorCounter"] == 0


def test_time_met_runs_at_wall_clock_rate(ground: Ground) -> None:
    first_index = ground.count()
    first = ground.wait_for("TIME_HK", lambda _: True, request="TIME_SEND_HK", start=first_index)
    t0 = _arrival(ground, first)
    time.sleep(2.0)
    second_index = ground.count()
    second = ground.wait_for("TIME_HK", lambda _: True, request="TIME_SEND_HK", start=second_index)
    t1 = _arrival(ground, second)

    def met(packet: Packet) -> float:
        fields = packet["fields"]
        return fields["MET_Seconds"] + fields["MET_Subseconds"] / SUBSEC

    elapsed_met, elapsed_wall = met(second) - met(first), t1 - t0
    assert elapsed_met > 1.5
    assert abs(elapsed_met - elapsed_wall) < 0.75, (elapsed_met, elapsed_wall)
    assert second["sequence"] != first["sequence"]


def _arrival(ground: Ground, packet: Packet) -> float:
    with ground._lock:
        for when, candidate in ground.packets:
            if candidate is packet:
                return when
    raise AssertionError("packet not in the capture")


def test_to_lab_data_types_packet_decodes_every_scalar_type(ground: Ground) -> None:
    """TO_LAB's fixed test packet is a ground-truth vector for the codec on all scalar types."""
    mark = ground.count()
    ground.send("TO_LAB_SEND_DATA_TYPES")
    packet = ground.wait_for("TO_LAB_DATA_TYPES", lambda _: True, start=mark, timeout=10)
    f = packet["fields"]
    assert (f["synch"], f["bl1"], f["bl2"]) == (0x6969, 0, 1)
    assert (f["b1"], f["b2"], f["b3"], f["b4"]) == (16, 127, 0x7F, 0x45)
    assert (f["w1"], f["w2"]) == (0x2468, 0x7FFF)
    assert (f["dw1"], f["dw2"]) == (0x12345678, 0x87654321 - (1 << 32))  # signed int32
    assert f["f1"] == pytest.approx(90.01, rel=1e-6)
    assert f["f2"] == pytest.approx(4.5e-6, rel=1e-6)
    assert f["df1"] == 99.9 and f["df2"] == 0.4444
    assert f["str"] == "ABCDEFGHIJ"  # 10 chars, no NUL terminator


# -- case 2: SAMPLE_APP NOOP and RESET_COUNTERS --------------------------------------------------


def test_sample_app_noop_and_reset_counters_reflected_in_hk(ground: Ground) -> None:
    base = ground.fresh("SAMPLE_APP_HK", "SAMPLE_APP_SEND_HK")

    for _ in range(3):
        ground.send("SAMPLE_APP_NOOP")
    expected = (base["CommandCounter"] + 3) % 256
    after_noops = ground.wait_for(
        "SAMPLE_APP_HK",
        lambda f: f["CommandCounter"] == expected,
        request="SAMPLE_APP_SEND_HK",
    )["fields"]
    assert _delta(after_noops["CommandCounter"], base["CommandCounter"]) == 3
    assert after_noops["CommandErrorCounter"] == base["CommandErrorCounter"]

    ground.send("SAMPLE_APP_RESET_COUNTERS")
    ground.wait_for(
        "SAMPLE_APP_HK",
        lambda f: f["CommandCounter"] == 0 and f["CommandErrorCounter"] == 0,
        request="SAMPLE_APP_SEND_HK",
    )


def test_sample_app_display_param_accepts_mixed_argument_types(ground: Ground) -> None:
    """uint32 + negative int16 + char[10] payload, accepted and counted as valid."""
    base = ground.fresh("SAMPLE_APP_HK", "SAMPLE_APP_SEND_HK")
    ground.send("SAMPLE_APP_DISPLAY_PARAM", "ValU32=4000000000", "ValI16=-5", "ValStr=hello")
    expected = (base["CommandCounter"] + 1) % 256
    after = ground.wait_for(
        "SAMPLE_APP_HK",
        lambda f: f["CommandCounter"] == expected,
        request="SAMPLE_APP_SEND_HK",
    )["fields"]
    assert after["CommandErrorCounter"] == base["CommandErrorCounter"]


# -- case 3: TIME NOOP, an argument command, HK and diagnostic decode ---------------------------


def test_time_noop_set_stcf_and_adjust_are_reflected_in_hk_and_diag(ground: Ground) -> None:
    base = ground.fresh("TIME_HK", "TIME_SEND_HK")

    ground.send("TIME_NOOP")
    expected = (base["CommandCounter"] + 1) % 256
    ground.wait_for("TIME_HK", lambda f: f["CommandCounter"] == expected, request="TIME_SEND_HK")

    # STCF = 1000.5 s: seconds and 2^-32 s subseconds on the wire, little-endian payload.
    ground.send("TIME_SET_STCF", "Seconds=1000", "MicroSeconds=500000")
    stcf = ground.wait_for(
        "TIME_HK",
        lambda f: f["STCF_Seconds"] == 1000,
        request="TIME_SEND_HK",
    )["fields"]
    assert stcf["STCF_Subseconds"] == 0x80000000
    assert stcf["CommandErrorCounter"] == base["CommandErrorCounter"]

    # A 2 s clock adjustment folds into STCF and shows as the one-time adjustment in DIAG.
    ground.send("TIME_ADD_ADJUST", "Seconds=2", "MicroSeconds=0")
    ground.wait_for(
        "TIME_HK",
        lambda f: f["STCF_Seconds"] == 1002 and f["STCF_Subseconds"] == 0x80000000,
        request="TIME_SEND_HK",
    )
    diag = ground.wait_for(
        "TIME_DIAG",
        lambda f: f["OneTimeAdjust_Seconds"] == 2,
        request="TIME_SEND_DIAGNOSTIC",
    )["fields"]
    assert diag["OneTimeAdjust_Subseconds"] == 0
    assert diag["AtToneSTCF_Seconds"] == 1002
    assert diag["CurrentMET_Seconds"] >= base["MET_Seconds"]


def test_time_rejects_out_of_range_microseconds(ground: Ground) -> None:
    """The FSW range check (usec < 1_000_000) counts an error; ib sends the value verbatim."""
    base = ground.fresh("TIME_HK", "TIME_SEND_HK")
    ground.send("TIME_SET_STCF", "Seconds=7", "MicroSeconds=1000000")
    expected = (base["CommandErrorCounter"] + 1) % 256
    after = ground.wait_for(
        "TIME_HK",
        lambda f: f["CommandErrorCounter"] == expected,
        request="TIME_SEND_HK",
    )["fields"]
    assert after["STCF_Seconds"] == base["STCF_Seconds"]  # the rejected value was not applied


# -- case 4: ES write-system-log-to-file ---------------------------------------------------------


def test_es_write_syslog_creates_file_and_counts_the_command(ground: Ground) -> None:
    name = f"ib_{os.getpid()}.log"  # OSAL file names are at most 19 characters
    target = RAM_DIR / name
    assert not target.exists()
    base = ground.fresh("ES_HK", "ES_SEND_HK")
    try:
        ground.send("ES_WRITE_SYS_LOG", f"FileName=/ram/{name}")
        expected = (base["CommandCounter"] + 1) % 256
        after = ground.wait_for(
            "ES_HK", lambda f: f["CommandCounter"] == expected, request="ES_SEND_HK"
        )["fields"]
        assert after["CommandErrorCounter"] == base["CommandErrorCounter"]
        assert target.is_file()
        data = target.read_bytes()
        assert len(data) > 0
    finally:
        target.unlink(missing_ok=True)


@pytest.mark.parametrize(
    "path",
    ["/no_such_volume/x.dat", "/ram/" + "n" * 30 + ".log", "/ram/" + "d" * 59],
    ids=["unknown-volume", "name-too-long-for-osal", "full-width-field-without-nul"],
)
def test_es_write_syslog_to_an_invalid_path_counts_an_error_and_writes_nothing(
    ground: Ground, path: str
) -> None:
    before_files = set(RAM_DIR.iterdir()) if RAM_DIR.exists() else set()
    base = ground.fresh("ES_HK", "ES_SEND_HK")
    ground.send("ES_WRITE_SYS_LOG", f"FileName={path}")
    expected = (base["CommandErrorCounter"] + 1) % 256
    after = ground.wait_for(
        "ES_HK", lambda f: f["CommandErrorCounter"] == expected, request="ES_SEND_HK"
    )["fields"]
    assert after["CommandCounter"] == base["CommandCounter"]
    assert (set(RAM_DIR.iterdir()) if RAM_DIR.exists() else set()) == before_files


# -- case 5: deliberately malformed commands -----------------------------------------------------

# (command, HK request, HK message): each FSW app verifies the command length it expects.
LENGTH_CHECKING_APPS = [
    ("ES_NOOP", "ES_SEND_HK", "ES_HK"),
    ("TIME_NOOP", "TIME_SEND_HK", "TIME_HK"),
    ("SAMPLE_APP_NOOP", "SAMPLE_APP_SEND_HK", "SAMPLE_APP_HK"),
]


@pytest.mark.parametrize(("command", "request_hk", "hk_name"), LENGTH_CHECKING_APPS)
def test_wrong_length_command_increments_the_apps_error_counter(
    ground: Ground, _isolated_state: Path, command: str, request_hk: str, hk_name: str
) -> None:
    """Header length one byte short of what the app expects: rejected and counted, not run."""
    base = ground.fresh(hk_name, request_hk)
    done = ground.send(command, flags=("--length-delta", "-1"))
    assert "(mid 0x" in done.stdout  # ib itself reports the send as normal

    expected = (base["CommandErrorCounter"] + 1) % 256
    after = ground.wait_for(
        hk_name, lambda f: f["CommandErrorCounter"] == expected, request=request_hk
    )["fields"]
    assert after["CommandCounter"] == base["CommandCounter"]  # the command did not execute

    # The ledger holds the request with its tamper and the exact bytes put on the wire.
    sent = _tx_commands(_isolated_state, command)
    assert len(sent) == 1 and sent[0]["result"] == "sent"
    raw = bytes.fromhex(sent[0]["raw"])
    assert len(raw) == 8
    assert int.from_bytes(raw[4:6], "big") + 7 == len(raw) - 1  # header claims one byte less
    requests = _requests(_isolated_state, command)
    assert len(requests) == 1 and requests[0]["tamper"] == {"length": -1}


def test_header_length_beyond_datagram_is_dropped_by_ci_lab_and_counted(
    ground: Ground, _isolated_state: Path
) -> None:
    base_ci = ground.fresh("CI_LAB_HK", "CI_LAB_SEND_HK")
    base_es = ground.fresh("ES_HK", "ES_SEND_HK")

    ground.send("ES_NOOP", flags=("--length-delta", "2"))
    expected = base_ci["IngestErrors"] + 1
    after = ground.wait_for(
        "CI_LAB_HK", lambda f: f["IngestErrors"] == expected, request="CI_LAB_SEND_HK"
    )["fields"]
    assert after["IngestPackets"] >= base_ci["IngestPackets"]
    es_after = ground.fresh("ES_HK", "ES_SEND_HK")
    assert es_after["CommandCounter"] == base_es["CommandCounter"]  # never reached ES
    assert es_after["CommandErrorCounter"] == base_es["CommandErrorCounter"]

    sent = _tx_commands(_isolated_state, "ES_NOOP")
    assert len(sent) == 1
    raw = bytes.fromhex(sent[0]["raw"])
    assert int.from_bytes(raw[4:6], "big") + 7 > len(raw)  # claims more bytes than were sent


def test_bad_checksum_is_accepted_by_this_cfs_build(ground: Ground, _isolated_state: Path) -> None:
    """This cFS build never validates command checksums (tools/cfs/NOTES.md, section 1).

    The corrupted packet is therefore executed and counted as valid. This pins the property
    that the corruption really reaches the wire and the target ignores it; a validating
    mission would reject it instead.
    """
    base = ground.fresh("ES_HK", "ES_SEND_HK")
    ground.send("ES_NOOP", flags=("--bad-checksum",))
    expected = (base["CommandCounter"] + 1) % 256
    after = ground.wait_for(
        "ES_HK", lambda f: f["CommandCounter"] == expected, request="ES_SEND_HK"
    )["fields"]
    assert after["CommandErrorCounter"] == base["CommandErrorCounter"]

    sent = _tx_commands(_isolated_state, "ES_NOOP")
    raw = bytes.fromhex(sent[0]["raw"])
    good_sum = 0xFF
    for byte in raw:
        good_sum ^= byte
    assert good_sum != 0  # XOR over the packet is no longer zero: the checksum is wrong
    requests = _requests(_isolated_state, "ES_NOOP")
    assert len(requests) == 1 and requests[0]["tamper"] == {"checksum": 1}


def test_unknown_command_and_bad_argument_never_reach_the_wire(
    ground: Ground, _isolated_state: Path
) -> None:
    """Encoder-side rejections are one-line coded errors and send nothing to cFS."""
    base_ci = ground.fresh("CI_LAB_HK", "CI_LAB_SEND_HK")
    unknown = ground.send("ES_NO_SUCH_COMMAND", check=False)
    assert unknown.returncode != 0 and unknown.stderr.startswith("error IB-")
    assert len(unknown.stderr.strip().splitlines()) == 1
    bad_arg = ground.send("TIME_SET_STCF", "Seconds=notanumber", "MicroSeconds=0", check=False)
    assert bad_arg.returncode != 0 and bad_arg.stderr.startswith("error IB-")
    assert not _tx_commands(_isolated_state, "TIME_SET_STCF")
    after_ci = ground.fresh("CI_LAB_HK", "CI_LAB_SEND_HK")
    # Only our own CI_LAB_SEND_HK request packets arrived in between.
    assert after_ci["IngestErrors"] == base_ci["IngestErrors"]


# -- events: the special telemetry --------------------------------------------------------------


@dataclass
class EventsProcess:
    """A running ``ib events`` collecting its output lines (JSON records with ``--format json``)."""

    proc: subprocess.Popen[str]
    lines: list[str] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _reader: threading.Thread | None = None

    def start_reading(self) -> None:
        self._reader = threading.Thread(target=self._read, daemon=True)
        self._reader.start()

    def _read(self) -> None:
        assert self.proc.stdout is not None
        for text in self.proc.stdout:
            with self._lock:
                self.lines.append(text.rstrip("\n"))

    def snapshot(self) -> list[str]:
        with self._lock:
            return list(self.lines)

    def wait_for(self, predicate: Callable[[str], bool], timeout: float = 20.0) -> str:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for text in self.snapshot():
                if predicate(text):
                    return text
            assert self.proc.poll() is None, f"ib events exited early: {self.snapshot()}"
            time.sleep(0.05)
        raise AssertionError(f"no matching event within {timeout} s; saw: {self.snapshot()}")

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGINT)
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)
        if self._reader is not None:
            self._reader.join(timeout=5)


@contextmanager
def _events(ground: Ground, state_dir: Path, *args: str) -> Generator[EventsProcess, None, None]:
    """``ib events`` (own ledger directory), started and listening; stopped on exit."""
    proc = subprocess.Popen(
        [*Ground._ib(), "events", "--timeout", "90", *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**ground.env, "IRONBIRD_STATE_DIR": str(state_dir)},
    )
    running = EventsProcess(proc)
    try:
        assert proc.stderr is not None
        # Notices (for example IB-TX-005 on a host with a small net.core.rmem_max) may precede
        # the banner.
        seen: list[str] = []
        for line in iter(proc.stderr.readline, ""):
            seen.append(line)
            if "listening for events from fc" in line:
                break
        else:
            raise AssertionError(f"no events banner; stderr so far: {seen}")
        running.start_reading()
        yield running
    finally:
        running.stop()


def test_events_command_shows_the_sample_app_noop_info_event(
    ground: Ground, tmp_path: Path
) -> None:
    state = tmp_path / "events-state"
    with ground.listener_paused(), _events(ground, state, "--format", "json") as events:
        ground.send("SAMPLE_APP_NOOP")
        record = json.loads(
            events.wait_for(lambda text: '"app": "SAMPLE_APP"' in text and '"id": 3,' in text)
        )
    assert record["node"] == "fc" and record["mid"] == "0x0808"
    assert (record["app"], record["id"], record["type"]) == ("SAMPLE_APP", 3, "INFORMATION")
    assert record["severity"] == "INFO" and record["type_value"] == 2
    assert record["message"].startswith("SAMPLE: NOOP command")
    # The ledger holds the received event in full, with the raw packet.
    logged = [e for e in _ledger_events(state) if e["event"] == "rx.event"]
    mine = [e for e in logged if e["app"] == "SAMPLE_APP" and e["event_id"] == 3]
    assert len(mine) == 1 and mine[0]["kind"] == "event" and mine[0]["raw"]
    assert mine[0]["text"] == record["message"]


def test_events_min_type_shows_only_the_error_but_the_ledger_keeps_the_info(
    ground: Ground, tmp_path: Path
) -> None:
    state = tmp_path / "events-state"
    with ground.listener_paused(), _events(ground, state, "--min-type", "error") as events:
        ground.send("SAMPLE_APP_NOOP")  # information event: below the floor
        ground.send("TIME_SET_STCF", "Seconds=7", "MicroSeconds=1000000")  # error event
        shown = events.wait_for(lambda text: "CFE_TIME" in text)
    assert " CFE_TIME ERROR id=36 Invalid STCF" in shown
    assert not [text for text in events.snapshot() if "SAMPLE_APP" in text]
    logged = {(e["app"], e["event_id"]) for e in _ledger_events(state) if e["event"] == "rx.event"}
    assert {("SAMPLE_APP", 3), ("CFE_TIME", 36)} <= logged


@contextmanager
def _listen_where(
    ground: Ground, state_dir: Path, *args: str
) -> Generator[EventsProcess, None, None]:
    """``ib listen fc --format json`` with extra arguments (own ledger directory), started and
    listening; stopped on exit. Output lines are collected like ``ib events`` lines."""
    proc = subprocess.Popen(
        [*Ground._ib(), "listen", "fc", "--format", "json", "--timeout", "90", *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**ground.env, "IRONBIRD_STATE_DIR": str(state_dir)},
    )
    running = EventsProcess(proc)
    try:
        assert proc.stderr is not None
        seen: list[str] = []
        for line in iter(proc.stderr.readline, ""):
            seen.append(line)
            if "listening for fc" in line:
                break
        else:
            raise AssertionError(f"no listen banner; stderr so far: {seen}")
        running.start_reading()
        yield running
    finally:
        running.stop()


def test_listen_where_selects_one_apps_housekeeping(ground: Ground, tmp_path: Path) -> None:
    """``--where 'msg == SAMPLE_APP_HK and CommandCounter > 0'`` prints only that HK, and only
    once a NOOP made the counter non-zero; the other apps' telemetry is filtered out.

    The counter is reset and seen at zero by the unfiltered listener *before* the filtered one
    starts: earlier tests leave it non-zero, and SCH_LAB's periodic SAMPLE_APP_HK (every 5.4 s)
    could otherwise be published between the filtered listener starting and the reset being
    executed, carrying the old counter through the filter legitimately. SB delivers one MID's
    packets in order, so once a zero-counter HK has arrived no older one is still in flight.
    """
    ground.send("SAMPLE_APP_RESET_COUNTERS")
    zeroed = ground.wait_for(
        "SAMPLE_APP_HK", lambda f: f["CommandCounter"] == 0, request="SAMPLE_APP_SEND_HK"
    )
    where = "msg == SAMPLE_APP_HK and CommandCounter > 0"
    with (
        ground.listener_paused(),
        _listen_where(ground, tmp_path / "state", "--where", where) as out,
    ):
        ground.send("SAMPLE_APP_SEND_HK")  # a zero-counter HK: must not appear
        time.sleep(1.5)
        assert out.snapshot() == []
        ground.send("SAMPLE_APP_NOOP")
        ground.send("SAMPLE_APP_SEND_HK")
        first = json.loads(out.wait_for(lambda text: '"SAMPLE_APP_HK"' in text))
        time.sleep(5.0)  # other apps publish housekeeping meanwhile (SCH_LAB periods)
        lines = [json.loads(text) for text in out.snapshot()]
    assert first["fields"]["CommandCounter"] == 1  # exactly the one NOOP since the reset
    # SB numbers each MID's packets (14-bit CCSDS sequence count): at least the requested
    # zero-counter HK was published between the one seen before the pause and the first one
    # shown, and the filter dropped it.
    assert (first["sequence"] - zeroed["sequence"]) % (1 << 14) >= 2
    assert lines and all(
        r["name"] == "SAMPLE_APP_HK" and r["fields"]["CommandCounter"] == 1 for r in lines
    )


def test_listen_where_htime_selects_time_hk_in_the_cfe_epoch(
    ground: Ground, tmp_path: Path
) -> None:
    """The shipped ground config reads the telemetry header time with the cFE mission epoch
    (1980-01-01, TAI, 37 leap seconds: tools/cfs/NOTES.md), so ``htime`` of TIME HK is a
    1980 date, which the Unix epoch (1970 plus a million seconds) cannot be."""
    where = "msg == TIME_HK and htime >= 1980-01-01T00:00:00Z and htime < 1981-01-01T00:00:00Z"
    with (
        ground.listener_paused(),  # one process owns UDP 2234
        _listen_where(ground, tmp_path / "state", "--where", where, "--count", "1") as out,
    ):
        deadline = time.monotonic() + 30
        while not out.snapshot() and time.monotonic() < deadline:
            ground.send("TIME_SEND_HK")
            time.sleep(1.0)
        lines = out.snapshot()
    assert lines, "no TIME_HK passed the htime filter"
    assert json.loads(lines[0])["name"] == "TIME_HK"


CFE_EPOCH = datetime(1980, 1, 1, tzinfo=UTC)  # CFE_MISSION_TIME_EPOCH_* (tools/cfs/NOTES.md)
HTIME_TOLERANCE_S = 2.0  # the packet is stamped a few ms after its fields are sampled


def _fc_codec() -> Codec:
    """The ``fc`` codec exactly as the shipped ground config builds it."""
    node = load_config(CONFIG).nodes["fc"]
    assert node.ccdd is not None
    assert node.command_header is not None and node.telemetry_header is not None
    return Codec(
        load_ccdd(node.ccdd, node.ccdd_adapter),
        load_profile(node.command_header),
        load_profile(node.telemetry_header),
    )


def _capture(ground: Ground, codec: Codec, requests: dict[str, str]) -> dict[str, DecodedPacket]:
    """Raw TO_LAB datagrams decoded in-process: one packet per message of ``requests``
    (message name -> the command that makes cFS send it), asked for about once a second."""
    found: dict[str, DecodedPacket] = {}
    with ground.listener_paused(), socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(("127.0.0.1", TO_LAB_PORT))
        sock.settimeout(0.2)
        deadline = time.monotonic() + 30
        next_request = 0.0
        while len(found) < len(requests) and time.monotonic() < deadline:
            if time.monotonic() >= next_request:
                for command in requests.values():
                    ground.send(command)
                next_request = time.monotonic() + 1.0
            try:
                data, _ = sock.recvfrom(4096)
            except TimeoutError:
                continue
            packet = codec.decode(MessageKind.TELEMETRY, data)
            if isinstance(packet, DecodedPacket) and packet.message.name in requests:
                found[packet.message.name] = packet
    assert found.keys() == requests.keys(), f"no packet of {requests.keys() - found.keys()}"
    return found


def _seconds(fields: dict[str, object], name: str) -> float:
    """Seconds plus 2^-32 s subseconds of the ``<name>_Seconds`` / ``<name>_Subseconds`` pair."""
    whole, sub = fields[f"{name}_Seconds"], fields[f"{name}_Subseconds"]
    assert isinstance(whole, int) and isinstance(sub, int)
    return whole + sub / SUBSEC


def _htime_error_s(packet: DecodedPacket, cfe_utc_s: float) -> float:
    """Decoded header time minus the UTC instant ``CFE_EPOCH + cfe_utc_s``, in seconds."""
    htime_ns = packet.header.time_ns
    assert htime_ns is not None, "the telemetry profile has no header time"
    expected_ns = round((CFE_EPOCH.timestamp() + cfe_utc_s) * 1e9)
    return (htime_ns - expected_ns) / 1e9


def test_header_time_equals_the_cfe_time_in_the_time_packets(ground: Ground) -> None:
    """The decoded ``htime`` of TIME HK and DIAG, with the shipped config, is the cFE time the
    same packet reports in its payload: HK carries MET, STCF and the leap seconds (time =
    MET + STCF, a TAI count since the epoch, UTC = time - leaps); DIAG carries cFE's own UTC.
    A wrong epoch (the GPS 1980-01-06 or Unix 1970) or zero leap seconds is days or 37 s off."""
    packets = _capture(
        ground,
        _fc_codec(),
        {"TIME_HK": "TIME_SEND_HK", "TIME_DIAG": "TIME_SEND_DIAGNOSTIC"},
    )

    hk = packets["TIME_HK"].as_dict()
    assert hk["LeapSeconds"] == 37  # CFE_MISSION_TIME_DEF_LEAPS, nothing in this suite changes it
    leaps = hk["LeapSeconds"]
    assert isinstance(leaps, int)
    hk_utc_s = _seconds(hk, "MET") + _seconds(hk, "STCF") - leaps
    assert abs(_htime_error_s(packets["TIME_HK"], hk_utc_s)) < HTIME_TOLERANCE_S

    diag = packets["TIME_DIAG"].as_dict()
    assert diag["AtToneLeapSeconds"] == 37
    tai_s = _seconds(diag, "CurrentTAI")
    assert abs(tai_s - 37 - _seconds(diag, "CurrentUTC")) < 1e-3  # cFE's own TAI -> UTC
    assert abs(_htime_error_s(packets["TIME_DIAG"], _seconds(diag, "CurrentUTC"))) < (
        HTIME_TOLERANCE_S
    )


def test_events_where_type_matches_min_type(ground: Ground, tmp_path: Path) -> None:
    """``--where 'event.type >= error'`` shows the same events as ``--min-type error``."""

    def error_events(label: str, *args: str) -> set[tuple[str, int]]:
        # A fixed label, not one derived from the arguments: CI uploads this tree as an
        # artifact on failure, and upload-artifact rejects paths containing " : < > | * ?.
        state = tmp_path / f"state-{label}"
        with ground.listener_paused(), _events(ground, state, "--format", "json", *args) as events:
            ground.send("SAMPLE_APP_NOOP")  # information event: below the floor
            ground.send("TIME_SET_STCF", "Seconds=7", "MicroSeconds=1000000")  # error event
            events.wait_for(lambda text: "CFE_TIME" in text)
            time.sleep(1.0)
            return {(r["app"], r["id"]) for r in map(json.loads, events.snapshot())}

    by_floor = error_events("min-type", "--min-type", "error")
    by_where = error_events("where", "--where", "event.type >= error")
    assert ("CFE_TIME", 36) in by_where and by_where == by_floor
    assert not [1 for app, _ in by_where if app == "SAMPLE_APP"]


def test_events_also_appear_in_the_packet_stream_with_their_category(ground: Ground) -> None:
    mark = ground.count()
    ground.send("ES_NOOP")
    packet = ground.wait_for(
        "EVS_LONG_EVENT", lambda f: f["AppName"] == "CFE_ES" and f["EventID"] == 3, start=mark
    )
    assert packet["category"] == "event" and packet["labels"]["EventType"] == "INFORMATION"
    assert ground.since(mark, "EVS_HK") == [] or all(
        p["category"] == "housekeeping" for p in ground.since(mark, "EVS_HK")
    )


def test_evs_housekeeping_decodes_and_counts_its_noop(ground: Ground) -> None:
    base = ground.fresh("EVS_HK", "EVS_SEND_HK")
    ground.send("EVS_NOOP")
    expected = (base["CommandCounter"] + 1) % 256
    after = ground.wait_for(
        "EVS_HK", lambda f: f["CommandCounter"] == expected, request="EVS_SEND_HK"
    )["fields"]
    assert after["CommandErrorCounter"] == base["CommandErrorCounter"]
    assert after["MessageSendCounter"] >= base["MessageSendCounter"]
    assert len(after["AppData"]) == 256  # 16 app records of 8 bytes, hex text
    sb = ground.fresh("SB_HK", "SB_SEND_HK")
    tbl = ground.fresh("TBL_HK", "TBL_SEND_HK")
    assert sb["MemInUse"] > 0 and tbl["NumTables"] >= 1


# -- stage 2e: record a live session, then check and summarise the recording ------------------

RECORD_SECONDS = 10.0
ES_HK_PERIOD_S = 4.0  # SCH_LAB publishes ES housekeeping every 4.0 s (tools/cfs/NOTES.md)


def test_a_live_session_records_to_a_clean_recording_with_the_periodic_housekeeping(
    ground: Ground, tmp_path: Path
) -> None:
    config = tmp_path / "ironbird.toml"
    examples = REPO / "examples"
    text = CONFIG.read_text().replace('"ccdd/', f'"{examples}/ccdd/')
    text = text.replace('"headers/', f'"{examples}/headers/')
    config.write_text(text + "\n[recording]\nmin_free_bytes = 0\n")
    out = tmp_path / "live.ibrec"
    env = {**ground.env, "IRONBIRD_CONFIG": str(config)}
    ib = [sys.executable, "-m", "ironbird.cli.main"]
    err_path = tmp_path / "record.stderr"
    with ground.listener_paused(), err_path.open("w") as err:  # one process owns UDP 2234
        recorder = subprocess.Popen([*ib, "record", "-o", str(out), "--quiet"], stderr=err, env=env)
        try:
            deadline = time.monotonic() + 15
            while "listening" not in err_path.read_text():
                assert recorder.poll() is None, err_path.read_text()
                assert time.monotonic() < deadline, "ib record never reported it was listening"
                time.sleep(0.05)
            started = time.monotonic()
            for _ in range(3):
                ground.send("SAMPLE_APP_NOOP")
            time.sleep(max(0.0, RECORD_SECONDS - (time.monotonic() - started)))
        finally:
            recorder.send_signal(signal.SIGINT)
            code = recorder.wait(timeout=20)
    assert code == 128 + signal.SIGINT, err_path.read_text()

    check = subprocess.run(
        [*ib, "rec", "check", str(out)], capture_output=True, text=True, env=env, check=False
    )
    assert check.returncode == 0, check.stderr
    info = subprocess.run(
        [*ib, "rec", "info", str(out), "--format", "json"],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert info.returncode == 0, info.stderr
    report = json.loads(info.stdout)
    assert report["status"] == "closed" and report["frames"] > 0
    assert report["gaps"]["total"] == 0 and report["missing_frames"] == 0  # seq contiguous
    by_mid = {m["mid"]: m["count"] for m in report["mids"]}
    span = report["duration_s"]
    es_hk = by_mid[0x0800]
    assert abs(es_hk - span / ES_HK_PERIOD_S) <= 2, (
        es_hk,
        span,
    )  # one every 4.0 s, within a packet
    assert [d["kind"] for d in report["definitions"]] == [
        "ccdd",
        "command_profile",
        "telemetry_profile",
    ]


# -- stage 2g: replay a recording of a live session -----------------------------------------

REPLAY_NOOPS = 3
RECORDED_TIMEOUT_S = 30.0
NOOP_EVENT = 'event.app == "SAMPLE_APP" and event.id == 3'  # SAMPLE_APP's NOOP information event


def _replayed(
    ib: list[str], recording: Path, env: dict[str, str], where: str
) -> list[dict[str, Any]]:
    """The records ``ib replay --format json --where WHERE`` shows for ``recording``.

    The recording may still be being written: replay then reads what the recorder has handed
    to the disk so far (its flush interval is 250 ms) and says the file is still open."""
    done = subprocess.run(
        [*ib, "replay", str(recording), "--format", "json", "--where", where],
        capture_output=True,
        text=True,
        env=env,
        check=False,
        timeout=60,
    )
    assert done.returncode == 0, done.stderr
    return [json.loads(line) for line in done.stdout.splitlines()]


def _await_recorded(
    ground: Ground,
    ib: list[str],
    recording: Path,
    env: dict[str, str],
    where: str,
    predicate: Callable[[list[dict[str, Any]]], bool],
    *,
    request: str,
) -> None:
    """Re-send ``request`` about once a second until the records of the live ``recording``
    selected by ``where`` satisfy ``predicate``.

    A recording session cannot watch ``ib listen`` (the recorder owns UDP 2234), so each step
    of the session waits on what the recording itself holds rather than on a fixed sleep: under
    load a requested packet can take longer than any sleep to arrive, and a recorder stopped
    before it arrives leaves it out of the recording."""
    deadline = time.monotonic() + RECORDED_TIMEOUT_S
    next_request = 0.0
    seen: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        if time.monotonic() >= next_request:
            ground.send(request)
            next_request = time.monotonic() + 1.0
        seen = _replayed(ib, recording, env, where)
        if predicate(seen):
            return
        time.sleep(0.2)
    shown = [(r["name"], r["fields"]) for r in seen[-6:]]
    raise AssertionError(
        f"the recording never held the expected records for {where!r} within "
        f"{RECORDED_TIMEOUT_S} s; last recorded: {shown}"
    )


def _zero_sample_app_counters(ground: Ground) -> None:
    """Reset SAMPLE_APP's counters and see the zero on the running listener.

    Earlier tests leave the counter non-zero, and SCH_LAB publishes SAMPLE_APP_HK on its own
    every 5.4 s: one published between a recorder starting and a reset sent after it being
    executed carries the old counter into the recording legitimately. SB delivers one MID's
    packets in order, so once a zero-counter HK has arrived no older one is still in flight,
    and every SAMPLE_APP_HK recorded afterwards counts from zero."""
    ground.send("SAMPLE_APP_RESET_COUNTERS")
    ground.wait_for(
        "SAMPLE_APP_HK", lambda f: f["CommandCounter"] == 0, request="SAMPLE_APP_SEND_HK"
    )


def _runs(values: list[int]) -> list[int]:
    """``values`` with consecutive repeats collapsed: ``[0, 0, 1, 2, 2, 0]`` -> ``[0, 1, 2, 0]``.

    SCH_LAB's periodic housekeeping repeats whatever the counter holds at the time, so what a
    command sequence fixes is the order of distinct values, not how often each is seen."""
    return [v for i, v in enumerate(values) if i == 0 or values[i - 1] != v]


def test_replaying_a_live_recording_selects_exactly_what_was_sent(
    ground: Ground, tmp_path: Path
) -> None:
    """Record a session in which SAMPLE_APP is reset and sent K NOOPs, then replay it: the filter
    ``event.app == "SAMPLE_APP" and event.id == 3`` yields exactly K events (the NOOP's
    information event), the last SAMPLE_APP housekeeping carries the K NOOPs in its counter, and
    the replay of everything shows as many frames as the recording holds."""
    config = tmp_path / "ironbird.toml"
    examples = REPO / "examples"
    text = CONFIG.read_text().replace('"ccdd/', f'"{examples}/ccdd/')
    text = text.replace('"headers/', f'"{examples}/headers/')
    config.write_text(text + "\n[recording]\nmin_free_bytes = 0\n")
    out = tmp_path / "replay-live.ibrec"
    env = {**ground.env, "IRONBIRD_CONFIG": str(config), "IRONBIRD_STATE_DIR": str(tmp_path / "s")}
    ib = [sys.executable, "-m", "ironbird.cli.main"]
    err_path = tmp_path / "record.stderr"

    def all_sent_is_recorded(records: list[dict[str, Any]]) -> bool:
        # The K-th NOOP event, then an HK counting it (SAMPLE_APP answers in pipe order).
        events = [i for i, r in enumerate(records) if r["category"] == "event"]
        return len(events) >= REPLAY_NOOPS and any(
            r["name"] == "SAMPLE_APP_HK" and r["fields"]["CommandCounter"] == REPLAY_NOOPS
            for r in records[events[-1] :]
        )

    _zero_sample_app_counters(ground)  # no recorded HK can count NOOPs from before the reset
    with ground.listener_paused(), err_path.open("w") as err:  # one process owns UDP 2234
        recorder = subprocess.Popen([*ib, "record", "-o", str(out), "--quiet"], stderr=err, env=env)
        try:
            deadline = time.monotonic() + 15
            while "listening" not in err_path.read_text():
                assert recorder.poll() is None, err_path.read_text()
                assert time.monotonic() < deadline, "ib record never reported it was listening"
                time.sleep(0.05)
            ground.send("SAMPLE_APP_RESET_COUNTERS")
            for _ in range(REPLAY_NOOPS):
                ground.send("SAMPLE_APP_NOOP")
            _await_recorded(
                ground,
                ib,
                out,
                env,
                f"msg == SAMPLE_APP_HK or ({NOOP_EVENT})",
                all_sent_is_recorded,
                request="SAMPLE_APP_SEND_HK",
            )
        finally:
            recorder.send_signal(signal.SIGINT)
            code = recorder.wait(timeout=20)
    assert code == 128 + signal.SIGINT, err_path.read_text()

    def replay(*args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [*ib, "replay", str(out), *args],
            capture_output=True,
            text=True,
            env=env,
            check=False,
            timeout=60,
        )

    events = replay("--format", "json", "--where", NOOP_EVENT)
    assert events.returncode == 0, events.stderr
    event_records = [json.loads(line) for line in events.stdout.splitlines()]
    assert len(event_records) == REPLAY_NOOPS, events.stdout
    assert all(r["category"] == "event" for r in event_records)

    housekeeping = replay("--format", "json", "--where", "msg == SAMPLE_APP_HK")
    assert housekeeping.returncode == 0, housekeeping.stderr
    counters = [
        json.loads(line)["fields"]["CommandCounter"] for line in housekeeping.stdout.splitlines()
    ]
    assert counters and counters[-1] == REPLAY_NOOPS, counters
    assert counters == sorted(counters), counters  # counted up from the reset, never back

    everything = replay("--format", "hex")
    info = subprocess.run(
        [*ib, "rec", "info", str(out), "--format", "json"],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert everything.returncode == 0 and info.returncode == 0, everything.stderr
    assert len(everything.stdout.splitlines()) == json.loads(info.stdout)["frames"] > 0
    undecoded = replay("--where", "status != ok")  # what a live listen would have hidden
    assert undecoded.returncode == 0, undecoded.stderr


# -- stage 2h: export a recording of a live session -----------------------------------------

EXPORT_NOOPS = 3


def test_exporting_a_live_recording_counts_the_noops_and_matches_rec_info(
    ground: Ground, tmp_path: Path
) -> None:
    """Record a session in which SAMPLE_APP is reset, sent K NOOPs (each followed by a housekeeping
    request) and reset again, then export it to CSV: in ``SAMPLE_APP_HK.csv`` the CommandCounter
    rises by exactly K across the NOOPs and is back to 0 after the second reset, and the table has
    as many rows as ``ib rec info`` counts for the MID."""
    import csv

    config = tmp_path / "ironbird.toml"
    examples = REPO / "examples"
    text = CONFIG.read_text().replace('"ccdd/', f'"{examples}/ccdd/')
    text = text.replace('"headers/', f'"{examples}/headers/')
    config.write_text(text + "\n[recording]\nmin_free_bytes = 0\n")
    out = tmp_path / "export-live.ibrec"
    env = {**ground.env, "IRONBIRD_CONFIG": str(config), "IRONBIRD_STATE_DIR": str(tmp_path / "s")}
    ib = [sys.executable, "-m", "ironbird.cli.main"]
    err_path = tmp_path / "record.stderr"
    expected = [*range(EXPORT_NOOPS + 1), 0]  # the distinct counter values, in order

    def recorded(*values: int) -> None:
        """Request housekeeping until the recording's SAMPLE_APP_HK counters are ``values``."""
        _await_recorded(
            ground,
            ib,
            out,
            env,
            "msg == SAMPLE_APP_HK",
            lambda hk: _runs([r["fields"]["CommandCounter"] for r in hk]) == list(values),
            request="SAMPLE_APP_SEND_HK",
        )

    _zero_sample_app_counters(ground)  # no recorded HK can count NOOPs from before the reset
    with ground.listener_paused(), err_path.open("w") as err:  # one process owns UDP 2234
        recorder = subprocess.Popen([*ib, "record", "-o", str(out), "--quiet"], stderr=err, env=env)
        try:
            deadline = time.monotonic() + 15
            while "listening" not in err_path.read_text():
                assert recorder.poll() is None, err_path.read_text()
                assert time.monotonic() < deadline, "ib record never reported it was listening"
                time.sleep(0.05)
            ground.send("SAMPLE_APP_RESET_COUNTERS")
            recorded(*expected[:1])
            for n in range(1, EXPORT_NOOPS + 1):
                ground.send("SAMPLE_APP_NOOP")
                recorded(*expected[: n + 1])  # this NOOP counted before the next is sent
            ground.send("SAMPLE_APP_RESET_COUNTERS")
            recorded(*expected)  # the zero after the reset is in before the recorder stops
        finally:
            recorder.send_signal(signal.SIGINT)
            code = recorder.wait(timeout=20)
    assert code == 128 + signal.SIGINT, err_path.read_text()

    tables = tmp_path / "tables"
    done = subprocess.run(
        [*ib, "export", str(out), "-o", str(tables)],
        capture_output=True,
        text=True,
        env=env,
        check=False,
        timeout=60,
    )
    assert done.returncode == 0, done.stderr
    with (tables / "SAMPLE_APP_HK.csv").open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    counters = [int(r["CommandCounter"]) for r in rows]
    # 0, up by one per NOOP to K, back to 0: nothing skipped, repeated out of order or left over
    assert _runs(counters) == expected, counters
    info = subprocess.run(
        [*ib, "rec", "info", str(out), "--format", "json"],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert info.returncode == 0, info.stderr
    by_mid = {m["mid"]: m["count"] for m in json.loads(info.stdout)["mids"]}
    assert len(rows) == by_mid[int(rows[0]["mid"], 16)] > 0
    # every decoded frame of the recording is in exactly one table
    total = sum(sum(1 for _ in csv.reader(p.open(newline=""))) - 1 for p in tables.iterdir())
    assert total == json.loads(info.stdout)["frames"]
