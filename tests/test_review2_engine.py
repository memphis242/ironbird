"""Regression tests for the second Stage 1 review: signals, blocked output, kernel drops,
ledger truth, exit codes, state directory handling, filtered listeners and the cFS tools.

Each test reproduces a reviewer-found failure through the real entry point where that is
feasible (subprocesses with real signals and pipes), otherwise through the session API.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import signal
import socket
import stat
import subprocess
import sys
import textwrap
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from conftest import REPO, free_udp_ports
from ironbird import render
from ironbird.ccdd.model import MessageKind
from ironbird.cli.main import app
from ironbird.config import Endpoint, load_config
from ironbird.errors import Code, DecodeError, IronbirdError
from ironbird.events import extract_event
from ironbird.ledger import Ledger, default_state_dir
from ironbird.session import Session
from ironbird.transport import udp
from ironbird.transport.udp import UdpTransport

EXIT_TIMEOUT = 3


# -- helpers --


@pytest.fixture
def state_dir(_isolated_state: Path) -> Path:
    return _isolated_state


def ledger_events(state: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for path in sorted(state.glob("*.jsonl")):
        out.extend(json.loads(line) for line in path.read_text().splitlines())
    return out


def events_named(state: Path, name: str) -> list[dict[str, Any]]:
    return [e for e in ledger_events(state) if e["event"] == name]


def cli_argv(config: Path, *args: str) -> list[str]:
    return [sys.executable, "-m", "ironbird.cli.main", "-c", str(config), *args]


def patched_argv(patch: str, config: Path, *args: str) -> list[str]:
    """``python -c`` wrapper: apply ``patch`` (source), then run the real entry point."""
    script = (
        "import asyncio, sys, time\n"
        + textwrap.dedent(patch)
        + "\nfrom ironbird.cli.main import main\nsys.exit(main())\n"
    )
    return [sys.executable, "-c", script, "-c", str(config), *args]


def wait_for_text(proc: subprocess.Popen[str], needle: str, timeout: float = 15.0) -> None:
    """Block until ``needle`` shows on the process's stderr pipe."""
    assert proc.stderr is not None
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        line = proc.stderr.readline()
        if needle in line:
            return
        if not line and proc.poll() is not None:
            break
    raise AssertionError(f"process never printed {needle!r}")


def wait_for_file_text(path: Path, needle: str, proc: subprocess.Popen[Any], timeout: float = 15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists() and needle in path.read_text():
            return
        if proc.poll() is not None:
            break
        time.sleep(0.05)
    raise AssertionError(f"{needle!r} never appeared in {path}")


def endpoints(config_path: Path) -> tuple[Endpoint, Endpoint]:
    link = load_config(config_path).links[0]
    return link.command_endpoint, link.telemetry_endpoint


def hk_wire(config_path: Path, label: str = "x") -> bytes:
    config = load_config(config_path)

    async def build() -> bytes:
        async with Session(
            config, role="ground", ledger=Ledger(config_path.parent / "l"), notify=lambda _: None
        ) as ground:
            return ground.codec_for("fc").encode_telemetry("MOCK_HK", {"label": label}).data

    return asyncio.run(build())


def kill_quietly(*procs: subprocess.Popen[Any]) -> None:
    for proc in procs:
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=5)


@pytest.fixture
def run_session(mock_config_path: Path, tmp_path: Path) -> Iterator[Any]:
    """Run ``scenario(ground, fc, ground_notes, fc_notes)`` in-process."""
    config = load_config(mock_config_path)

    def run(scenario: Any, *, tick: float | None = None) -> tuple[Ledger, list[str], list[str]]:
        gnotes: list[str] = []
        fnotes: list[str] = []
        gl = Ledger(tmp_path / "ledgers", session_id="g")
        fl = Ledger(tmp_path / "ledgers", session_id="f")

        async def main() -> None:
            async with (
                Session(
                    config, role="ground", ledger=gl, notify=lambda n: gnotes.append(n.line())
                ) as g,
                Session(
                    config, role="fc", ledger=fl, notify=lambda n: fnotes.append(n.line())
                ) as f,
            ):
                await asyncio.wait_for(scenario(g, f, gnotes, fnotes), timeout=30)

        asyncio.run(main())
        gl.flush()
        return gl, gnotes, fnotes

    yield run


def read_ledger(ledger: Ledger) -> list[dict[str, Any]]:
    ledger.flush()
    return [json.loads(line) for line in ledger.path.read_text().splitlines()]


# -- B1: signals outside the listen loops --

SLOW_OPEN = """
    from ironbird.transport.udp import UdpTransport
    async def slow_open(self, receiver):
        print("READY", file=sys.stderr, flush=True)
        await asyncio.sleep(60)
    UdpTransport.open = slow_open
"""
SLOW_START = """
    _orig_run = asyncio.run
    def slow_run(coro, *a, **k):
        print("READY", file=sys.stderr, flush=True)
        time.sleep(60)
    asyncio.run = slow_run
"""
SLOW_CONFIRM = """
    import ironbird.transport.udp as _udp
    _udp.CONFIRM_WAIT_S = 60
"""


@pytest.mark.parametrize(
    ("patch", "sig", "code"),
    [
        (SLOW_OPEN, signal.SIGTERM, 143),
        (SLOW_OPEN, signal.SIGHUP, 129),
        (SLOW_START, signal.SIGTERM, 143),
    ],
    ids=["term-during-send", "hup-during-send", "term-during-startup"],
)
def test_signals_outside_listen_flush_the_ledger_and_exit_128_plus_n(
    mock_config_path: Path, state_dir: Path, patch: str, sig: signal.Signals, code: int
) -> None:
    proc = subprocess.Popen(
        patched_argv(patch, mock_config_path, "send", "fc", "MOCK_NOOP"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        wait_for_text(proc, "READY")
        proc.send_signal(sig)
        proc.communicate(timeout=15)
    finally:
        kill_quietly(proc)
    assert proc.returncode == code
    assert events_named(state_dir, "cli.invoke"), "the ledger must reach the disk"
    signals = events_named(state_dir, "cli.signal")
    assert signals and signals[0]["signum"] == int(sig)


def test_ctrl_c_before_the_datagram_leaves_says_not_sent_and_ledgers_not_sent(
    mock_config_path: Path, state_dir: Path
) -> None:
    proc = subprocess.Popen(
        patched_argv(SLOW_OPEN, mock_config_path, "send", "fc", "MOCK_NOOP"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        wait_for_text(proc, "READY")
        proc.send_signal(signal.SIGINT)
        _, err = proc.communicate(timeout=15)
    finally:
        kill_quietly(proc)
    assert proc.returncode == 130
    assert "not sent" in err.lower()
    (tx,) = events_named(state_dir, "tx.command")
    assert tx["result"] in {"cancelled", "not_sent"} and tx["sent"] is False


def test_ctrl_c_after_the_datagram_left_says_sent_and_ledgers_sent(
    mock_config_path: Path, state_dir: Path
) -> None:
    command_ep, _ = endpoints(mock_config_path)
    receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receiver.bind((command_ep.host, command_ep.port))
    receiver.settimeout(10)
    proc = subprocess.Popen(
        patched_argv(SLOW_CONFIRM, mock_config_path, "send", "fc", "MOCK_NOOP"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        receiver.recvfrom(2048)  # the datagram is out; the process now waits for ICMP errors
        proc.send_signal(signal.SIGINT)
        _, err = proc.communicate(timeout=15)
    finally:
        receiver.close()
        kill_quietly(proc)
    assert proc.returncode == 130
    assert "was sent" in err.lower() and "not sent" not in err.lower()
    (tx,) = events_named(state_dir, "tx.command")
    assert tx["result"] == "sent" and tx["sent"] is True


# -- B4: tx.command tells the truth --


def test_tx_command_result_is_not_sent_when_cancelled_or_failed_before_sendto(
    run_session: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario(ground: Session, *_: Any) -> None:
        async def hang(self: UdpTransport, receiver: Any) -> None:
            await asyncio.sleep(60)

        async def boom(self: UdpTransport, receiver: Any) -> None:
            raise RuntimeError("open exploded")

        with monkeypatch.context() as patch:
            patch.setattr(UdpTransport, "open", hang)
            task = asyncio.ensure_future(ground.send_command("fc", "MOCK_NOOP"))
            await asyncio.sleep(0.1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        with monkeypatch.context() as patch:
            patch.setattr(UdpTransport, "open", boom)
            with pytest.raises(RuntimeError):
                await ground.send_command("fc", "MOCK_NOOP")

    ledger, _, _ = run_session(scenario)
    cancelled, exploded = [e for e in read_ledger(ledger) if e["event"] == "tx.command"]
    assert cancelled["result"] == "cancelled" and cancelled["sent"] is False
    assert exploded["result"] == "RuntimeError" and exploded["sent"] is False


def test_tx_command_result_is_sent_only_after_sendto(run_session: Any, mock_config_path: Path):
    command_ep, _ = endpoints(mock_config_path)
    receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receiver.bind((command_ep.host, command_ep.port))

    async def scenario(ground: Session, *_: Any) -> None:
        await ground.send_command("fc", "MOCK_NOOP")

    try:
        ledger, _, _ = run_session(scenario)
    finally:
        receiver.close()
    (tx,) = [e for e in read_ledger(ledger) if e["event"] == "tx.command"]
    assert tx["result"] == "sent" and tx["sent"] is True


# -- B2: blocked stdout never stops the loop --


def start_blocked_listen(config: Path, tmp_path: Path, *extra: str):
    """``ib listen`` whose stdout pipe nobody reads, flooded until the pipe is long full."""
    errfile = tmp_path / "stderr.txt"
    proc = subprocess.Popen(
        cli_argv(config, "listen", "fc", "--format", "json", *extra),
        stdout=subprocess.PIPE,  # never read
        stderr=errfile.open("w"),
        text=True,
    )
    wait_for_file_text(errfile, "listening", proc)
    _, telemetry = endpoints(config)
    wire = hk_wire(config, "flood")
    raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        for i in range(9000):
            raw.sendto(wire, (telemetry.host, telemetry.port))
            if i % 100 == 99:
                time.sleep(0.005)
    finally:
        raw.close()
    return proc, errfile


def test_blocked_stdout_does_not_stop_sigterm(mock_config_path: Path, tmp_path: Path, state_dir):
    proc, _ = start_blocked_listen(mock_config_path, tmp_path)
    try:
        time.sleep(0.5)
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=10)
    finally:
        kill_quietly(proc)
    assert proc.returncode == 143
    assert events_named(state_dir, "cli.signal")


def test_blocked_stdout_does_not_stop_timeout_and_dropped_lines_are_reported(
    mock_config_path: Path, tmp_path: Path, state_dir
) -> None:
    proc, errfile = start_blocked_listen(mock_config_path, tmp_path, "--timeout", "2")
    try:
        wait_for_file_text(errfile, Code.OUT_DROPPED.value, proc, timeout=20)
        deadline = time.monotonic() + 15
        while not events_named(state_dir, "listen.ending") and time.monotonic() < deadline:
            time.sleep(0.05)  # the timeout ends the run although stdout never drained
        assert events_named(state_dir, "listen.ending")[-1]["reason"] == "timeout"
        assert proc.poll() is None  # shutdown now waits for the reader instead of abandoning lines
        assert proc.stdout is not None
        proc.stdout.read()
        proc.wait(timeout=15)
    finally:
        kill_quietly(proc)
    assert proc.returncode == 4  # the command completed, but lines were lost (README: exit codes)
    assert any(e["event"] == Code.OUT_DROPPED.value for e in ledger_events(state_dir))


def test_non_utf8_terminal_does_not_crash_listen(mock_config_path: Path, tmp_path: Path) -> None:
    _, telemetry = endpoints(mock_config_path)
    wire = bytearray(hk_wire(mock_config_path, "abcdefgh"))
    wire[-8:] = b"\xc3\xa9\xff\x00\x00\x00\x00\x00"  # non-ASCII label bytes
    env = {**os.environ, "PYTHONIOENCODING": "ascii"}
    proc = subprocess.Popen(
        cli_argv(mock_config_path, "listen", "fc", "--count", "1", "--timeout", "20"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    try:
        wait_for_text(proc, "listening")
        raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        raw.sendto(bytes(wire), (telemetry.host, telemetry.port))
        raw.close()
        out, err = proc.communicate(timeout=15)
    finally:
        kill_quietly(proc)
    assert proc.returncode == 0, err
    assert "IB-INT-001" not in err
    assert "MOCK_HK" in out and out.isascii()


# -- follow-up: a closed pipe is a normal end -------------------------------------------------


def test_closed_stdout_pipe_ends_listen_normally(
    mock_config_path: Path, tmp_path: Path, state_dir: Path
) -> None:
    _, telemetry = endpoints(mock_config_path)
    wire = hk_wire(mock_config_path)
    proc = subprocess.Popen(
        cli_argv(mock_config_path, "listen", "fc", "--timeout", "30"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        wait_for_text(proc, "listening")
        assert proc.stdout is not None
        raw.sendto(wire, (telemetry.host, telemetry.port))
        assert "MOCK_HK" in proc.stdout.readline()
        proc.stdout.close()  # like `| head -1`
        deadline = time.monotonic() + 15
        while proc.poll() is None and time.monotonic() < deadline:
            raw.sendto(wire, (telemetry.host, telemetry.port))
            time.sleep(0.05)
    finally:
        raw.close()
        exited_by_itself = proc.poll() is not None
        kill_quietly(proc)
    err = proc.stderr.read() if proc.stderr else ""
    assert exited_by_itself, "listen must end once its stdout reader is gone"
    assert proc.returncode == 0, err
    assert "IB-INT-001" not in err and "Traceback" not in err
    endings = events_named(state_dir, "listen.ending")
    assert endings and endings[-1]["reason"] == "pipe_closed"


# -- B5: exit codes --


@pytest.mark.parametrize(
    "args",
    [
        ["listen", "fc"],
        ["events"],
        ["respond", "--as", "fc"],
    ],
    ids=["listen", "events", "respond"],
)
def test_timeout_before_count_has_a_distinct_exit_code(
    mock_config_path: Path, args: list[str]
) -> None:
    runner = CliRunner()
    base = ["-c", str(mock_config_path)]
    short = runner.invoke(app, [*base, *args, "--count", "2", "--timeout", "0.3"])
    assert short.exit_code == EXIT_TIMEOUT, short.output
    plain = runner.invoke(app, [*base, *args, "--timeout", "0.3"])
    assert plain.exit_code == 0, plain.output


@pytest.mark.parametrize(
    ("sig", "code"),
    [(signal.SIGTERM, 143), (signal.SIGINT, 130), (signal.SIGHUP, 129)],
)
def test_signal_ends_listen_with_128_plus_n(
    mock_config_path: Path, state_dir: Path, sig: signal.Signals, code: int
) -> None:
    proc = subprocess.Popen(
        cli_argv(mock_config_path, "listen", "fc", "--timeout", "60"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        wait_for_text(proc, "listening")
        proc.send_signal(sig)
        proc.communicate(timeout=15)
    finally:
        kill_quietly(proc)
    assert proc.returncode == code
    assert events_named(state_dir, "cli.signal")[0]["signum"] == int(sig)
    assert events_named(state_dir, "listen.ending")[-1]["reason"] == "signal"


# -- option validation --


@pytest.mark.parametrize("delta", ["0", "65536", "-65536", "131072", "99999999999999999999"])
def test_length_delta_that_changes_nothing_or_is_out_of_range_is_rejected(
    mock_config_path: Path, delta: str
) -> None:
    result = CliRunner().invoke(
        app, ["-c", str(mock_config_path), "send", "fc", "MOCK_NOOP", f"--length-delta={delta}"]
    )
    assert result.exit_code == 2 and Code.CLI_USAGE.value in result.output, result.output


def test_length_delta_in_range_is_sent(mock_config_path: Path) -> None:
    command_ep, _ = endpoints(mock_config_path)
    receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receiver.bind((command_ep.host, command_ep.port))
    try:
        for delta in ("1", "-1", "65535"):
            result = CliRunner().invoke(
                app,
                ["-c", str(mock_config_path), "send", "fc", "MOCK_NOOP", f"--length-delta={delta}"],
            )
            assert result.exit_code == 0, result.output
    finally:
        receiver.close()


@pytest.mark.parametrize("value", ["nan", "inf", "-inf", "1e400", "0", "-1"])
@pytest.mark.parametrize("command", ["listen", "events", "respond"])
def test_timeout_rejects_non_finite_and_non_positive(
    mock_config_path: Path, command: str, value: str
) -> None:
    extra = ["--as", "fc"] if command == "respond" else []
    argv = ["-c", str(mock_config_path), command, *(["fc"] if command == "listen" else [])]
    result = CliRunner().invoke(app, [*argv, *extra, f"--timeout={value}"])
    assert result.exit_code == 2, result.output


@pytest.mark.parametrize("mid", ["0x1234", "-1", "0x10000", "99999999999999999999", "MOCK_NOOP"])
def test_mid_filter_must_be_a_valid_mid_present_in_the_ccdd(
    mock_config_path: Path, mid: str
) -> None:
    result = CliRunner().invoke(
        app, ["-c", str(mock_config_path), "listen", "fc", "--mid", mid, "--timeout", "0.2"]
    )
    assert result.exit_code == 2 and Code.CLI_USAGE.value in result.output, result.output


def test_mid_filter_accepts_known_mids_and_names(mock_config_path: Path) -> None:
    for mid in ("0x0890", "MOCK_HK", "2193"):
        result = CliRunner().invoke(
            app,
            ["-c", str(mock_config_path), "listen", "fc", "--mid", mid, "--timeout", "0.2"],
        )
        assert result.exit_code == 0, (mid, result.output)


# -- transport errors blame the right thing --


def test_unresolvable_reply_host_blames_the_reply_endpoint(
    mock_config_path: Path, tmp_path: Path
) -> None:
    text = mock_config_path.read_text().replace(
        'telemetry_endpoint = "127.0.0.1:', 'telemetry_endpoint = "nonexistent.invalid:'
    )
    broken = tmp_path / "broken.toml"
    broken.write_text(text)
    result = CliRunner().invoke(app, ["-c", str(broken), "respond", "--as", "fc", "--timeout", "1"])
    assert result.exit_code == 1, result.output
    assert Code.TX_OPEN.value in result.output
    assert "nonexistent.invalid" in result.output  # the endpoint that failed, not the bind address
    assert "bind" not in result.output.lower()


def test_port_in_use_error_has_a_remedy(mock_config_path: Path) -> None:
    _, telemetry = endpoints(mock_config_path)
    squatter = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    squatter.bind((telemetry.host, telemetry.port))
    try:
        result = CliRunner().invoke(
            app, ["-c", str(mock_config_path), "listen", "fc", "--timeout", "0.3"]
        )
    finally:
        squatter.close()
    assert result.exit_code == 1 and Code.TX_OPEN.value in result.output
    assert "config" in result.output and "stop" in result.output.lower()


def test_decode_error_message_names_the_mid_once() -> None:
    error = DecodeError(Code.DEC_UNKNOWN, 0, "unknown telemetry MID 0x0803", 0x0803).to_error()
    assert error.summary.count("0x0803") == 1
    other = DecodeError(Code.DEC_SHORT, 0, "datagram is 3 bytes", 0x0803).to_error()
    assert "0x0803" in other.summary  # still named when the reason does not say it


# -- XDG and the ledger directory --


def test_relative_xdg_state_home_is_ignored() -> None:
    env = {"XDG_STATE_HOME": "relative/dir", "HOME": "/home/someone"}
    assert default_state_dir(env) == Path("/home/someone/.local/state/ironbird")
    absolute = {"XDG_STATE_HOME": "/var/state", "HOME": "/home/someone"}
    assert default_state_dir(absolute) == Path("/var/state/ironbird")


@pytest.mark.parametrize("home", ["", "relative/home"])
def test_empty_or_relative_home_is_refused_with_a_remedy(home: str) -> None:
    with pytest.raises(IronbirdError) as info:
        default_state_dir({"HOME": home})
    assert info.value.code is Code.LED_DIR
    assert "IRONBIRD_STATE_DIR" in info.value.summary


def test_send_fails_closed_when_the_ledger_directory_is_unusable(
    mock_config_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    blocker = tmp_path / "a-file"
    blocker.write_text("not a directory")
    monkeypatch.setenv("IRONBIRD_STATE_DIR", str(blocker / "state"))
    command_ep, _ = endpoints(mock_config_path)
    receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receiver.bind((command_ep.host, command_ep.port))
    receiver.setblocking(False)
    try:
        result = CliRunner().invoke(app, ["-c", str(mock_config_path), "send", "fc", "MOCK_NOOP"])
        assert result.exit_code == 1, result.output
        assert Code.LED_DIR.value in result.output
        assert "IRONBIRD_STATE_DIR" in result.output  # the remedy
        with pytest.raises(BlockingIOError):
            receiver.recvfrom(2048)  # nothing was sent
    finally:
        receiver.close()


def test_read_only_commands_still_run_without_a_ledger(
    mock_config_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    blocker = tmp_path / "a-file"
    blocker.write_text("not a directory")
    monkeypatch.setenv("IRONBIRD_STATE_DIR", str(blocker / "state"))
    result = CliRunner().invoke(app, ["-c", str(mock_config_path), "nodes"])
    assert result.exit_code == 0 and Code.LED_DIR.value in result.output


# -- B3: kernel drops and the receive buffer --


def test_transport_counts_datagrams_the_kernel_dropped() -> None:
    (port,) = free_udp_ports(1)
    total = 20_000

    async def main() -> tuple[int, UdpTransport]:
        received = 0

        def receiver(*_: Any) -> None:
            nonlocal received
            received += 1

        transport = UdpTransport(local=Endpoint("127.0.0.1", port))
        await transport.open(receiver)
        raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        for _ in range(total):  # no await: the loop cannot read while the socket buffer fills
            raw.sendto(bytes(1000), ("127.0.0.1", port))
        raw.close()
        while True:  # let the loop read what the buffer holds
            seen = received
            await asyncio.sleep(0.3)
            if received == seen:
                break
        transport.refresh_kernel_drops()
        stats = transport.stats()
        assert stats.rx_frames == received
        await transport.close()
        return received, transport

    received, transport = asyncio.run(main())
    assert transport.kernel_dropped > 0
    assert received + transport.kernel_dropped == total


def test_overflow_counter_rides_on_the_next_received_datagram() -> None:
    """SO_RXQ_OVFL (where the kernel has it) reports drops without reading /proc."""
    (port,) = free_udp_ports(1)

    async def main() -> tuple[bool, int]:
        transport = UdpTransport(local=Endpoint("127.0.0.1", port))
        await transport.open(lambda *_: None)
        raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        for _ in range(20_000):
            raw.sendto(bytes(1000), ("127.0.0.1", port))
        await asyncio.sleep(0.4)  # the loop reads the buffer; each datagram carries the counter
        raw.sendto(b"tail", ("127.0.0.1", port))
        await asyncio.sleep(0.2)
        raw.close()
        active, dropped = transport.overflow_counter_active, transport.kernel_dropped
        await transport.close()
        return active, dropped

    active, dropped = asyncio.run(main())
    if not active:
        pytest.skip("SO_RXQ_OVFL is not available on this kernel")
    assert dropped > 0


def test_session_reports_and_ledgers_kernel_drops(
    run_session: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("ironbird.session.TICK_S", 0.1)

    async def scenario(ground: Session, fc: Session, gnotes: list[str], _: list[str]) -> None:
        listener = await ground.listen("fc")  # opened, never read
        addr = listener.local_address
        assert addr is not None
        raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        for _i in range(20_000):
            raw.sendto(bytes(1000), addr)
        raw.close()
        await asyncio.sleep(0.6)
        await listener.close()
        assert any(Code.TX_OVERRUN.value in n and "kernel" in n for n in gnotes), gnotes

    ledger, _, _ = run_session(scenario)
    drops = [e for e in read_ledger(ledger) if e["event"] == "rx.kernel_drops"]
    assert drops and drops[-1]["total"] > 0


def test_effective_rcvbuf_is_ledgered_and_a_short_buffer_warns_once(
    run_session: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(udp, "RCVBUF_BYTES", 1 << 30)  # far above any rmem_max

    async def scenario(ground: Session, fc: Session, gnotes: list[str], _: list[str]) -> None:
        first = await ground.listen("fc")
        await first.close()
        second = await ground.listen("fc")
        await second.close()
        warned = [n for n in gnotes if Code.TX_RCVBUF.value in n]
        assert len(warned) == 1 and "rmem_max" in warned[0], gnotes

    ledger, _, _ = run_session(scenario)
    opened = [e for e in read_ledger(ledger) if e["event"] == "udp.open"]
    assert opened and opened[0]["rcvbuf_effective"] > 0
    assert opened[0]["rcvbuf_requested"] == 1 << 30


# -- filtered listeners still account for what they skip --


def test_filtered_listener_reports_skipped_malformed_and_unknown_packets(
    run_session: Any, mock_config_path: Path
) -> None:
    async def scenario(ground: Session, fc: Session, gnotes: list[str], _: list[str]) -> None:
        listener = await ground.listen("fc", mids=frozenset({0x0891}))  # MOCK_STATUS only
        addr = listener.local_address
        assert addr is not None
        hk = fc.codec_for("fc").encode_telemetry("MOCK_HK").data
        status = fc.codec_for("fc").encode_telemetry("MOCK_STATUS").data
        raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        received = aiter(listener)
        try:
            raw.sendto(hk + b"\x00\x00\x00", addr)  # known MID outside the filter, malformed
            raw.sendto(hk[:9], addr)  # truncated
            for mid in range(0x0900, 0x0900 + 1500):  # garbage MIDs, all unknown
                raw.sendto(mid.to_bytes(2, "big") + hk[2:], addr)
                if mid % 50 == 0:
                    await asyncio.sleep(0.002)
            raw.sendto(status, addr)
            rx = await asyncio.wait_for(received.__anext__(), timeout=10)
            assert rx.packet.message.name == "MOCK_STATUS"
        finally:
            raw.close()
            await listener.close()
        ground.flush_notices()
        assert any(Code.DEC_UNKNOWN.value in n for n in gnotes), gnotes
        assert any(re.search(r"IB-DEC-00[1357]", n) for n in gnotes), gnotes
        assert len(gnotes) < 20  # coalesced, not one line per packet
        assert len(ground.stats_snapshot()) < 8, "garbage MIDs must not fill the per-MID table"
        assert ground.malformed_total >= 1500

    ledger, _, _ = run_session(scenario)
    malformed = [e for e in read_ledger(ledger) if e["event"] == "rx.malformed"]
    assert len(malformed) >= 1500


# -- config/CCDD-sourced strings go through the one terminal-text helper -----------------------


@pytest.fixture
def marked(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(render, "term", lambda text, *a, **k: f"<<{text}>>")
    monkeypatch.setattr(render, "jtext", lambda text, *a, **k: f"<<{text}>>")


NODE_NAME = r"\bfc\b(?![= ]\d)"  # the node, not a function code ("fc=1", "fc 0")


def unmarked(text: str) -> str:
    return re.sub(r"<<.*?>>", "", text)


def test_nodes_find_and_send_sanitize_every_config_string(
    mock_config_path: Path, marked: None
) -> None:
    runner = CliRunner()
    base = ["-c", str(mock_config_path)]
    nodes = runner.invoke(app, [*base, "nodes"])
    assert nodes.exit_code == 0, nodes.output
    rest = unmarked(nodes.output)
    assert "flight" not in rest and "ground-fc" not in rest and not re.search(NODE_NAME, rest)
    found = runner.invoke(app, [*base, "find", "MOCK_SET_MODE"])
    assert found.exit_code == 0
    assert "MOCK_" not in unmarked(found.output) and not re.search(
        NODE_NAME, unmarked(found.output)
    )
    command_ep, _ = endpoints(mock_config_path)
    receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receiver.bind((command_ep.host, command_ep.port))
    try:
        sent = runner.invoke(app, [*base, "send", "fc", "MOCK_NOOP"])
    finally:
        receiver.close()
    assert sent.exit_code == 0, sent.output
    assert "MOCK_" not in unmarked(sent.output) and not re.search(NODE_NAME, unmarked(sent.output))


def test_render_sanitizes_packet_event_and_banner_strings(
    mock_config_path: Path, marked: None
) -> None:
    config = load_config(mock_config_path)

    async def build() -> tuple[Any, Any]:
        async with Session(
            config, role="ground", ledger=Ledger(mock_config_path.parent / "l"), notify=print
        ) as ground:
            fc = ground.codec_for("fc")
            hk = fc.decode(
                MessageKind.TELEMETRY,
                fc.encode_telemetry("MOCK_HK", {"mode": "SAFE", "label": "L"}).data,
            )
            ev = fc.decode(
                MessageKind.TELEMETRY,
                fc.encode_telemetry(
                    "MOCK_EVENT",
                    {"app": "DEMO", "event_id": 7, "event_type": "ERROR", "message": "m"},
                ).data,
            )
            return hk, ev

    hk, ev = asyncio.run(build())
    event = extract_event("fc", 0, ev)
    assert event is not None
    outputs = [
        render.line("fc", 0, hk),
        json.dumps(render.record("fc", 0, hk)),
        render.event_line(event),
        json.dumps(render.event_record(event)),
        render.listen_banner("fc", ("127.0.0.1", 1)),
        render.events_banner([("fc", ("127.0.0.1", 1))]),
        render.respond_banner("fc", ("127.0.0.1", 1)),
        render.respond_line("fc", "MOCK_NOOP", 3, "MOCK_HK"),
    ]
    for text in outputs:
        rest = unmarked(text)
        for word in ("MOCK_", "SAFE", "DEMO"):
            assert word not in rest, (word, text)
        assert not re.search(NODE_NAME, rest), text


# -- tools --

TOOLS = REPO / "tools" / "cfs"


def fake_cfs_tree(root: Path, body: str) -> Path:
    exe = root / "src" / "build-native_std" / "exe" / "cpu1"
    exe.mkdir(parents=True)
    core = exe / "core-cpu1"
    core.write_text(f"#!/usr/bin/env bash\n{body}\n")
    core.chmod(core.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    root.chmod(0o755)
    return root


@pytest.mark.skipif(shutil.which("flock") is None, reason="needs flock(1)")
@pytest.mark.skipif(Path("/dev/shm/osal:RAM").exists(), reason="a cFS RAM disk exists on this host")
def test_run_sh_refuses_a_second_instance_and_leaves_the_first_alone(tmp_path: Path) -> None:
    build = fake_cfs_tree(tmp_path / "build", "echo up > alive.txt\nexec sleep 60")
    env = {**os.environ, "CFS_BUILD_DIR": str(build), "CFS_APPS": "ALL"}
    env["TMPDIR"] = str(tmp_path)
    first = subprocess.Popen([str(TOOLS / "run.sh")], env=env, stderr=subprocess.PIPE, text=True)
    try:
        alive = build / "run" / "cpu1" / "alive.txt"
        deadline = time.monotonic() + 15
        while not alive.exists() and time.monotonic() < deadline:
            if first.poll() is not None:
                break
            time.sleep(0.05)
        assert alive.exists(), first.stderr.read() if first.stderr else ""
        second = subprocess.run(
            [str(TOOLS / "run.sh")],
            check=False,
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert second.returncode != 0
        assert "already running" in second.stderr
        assert alive.exists(), "the second run must not wipe the live instance's directory"
        assert first.poll() is None
    finally:
        first.terminate()
        first.wait(timeout=10)
        subprocess.run([str(TOOLS / "clean.sh"), str(build / "run" / "cpu1")], check=False)


def shm_row(key: int, shmid: int, nattch: int, uid: int) -> str:
    """One row of /proc/sysvipc/shm (decimal key)."""
    cols = [key, shmid, 600, 4096, 1, 1, nattch, uid, uid, uid, uid, 0, 0, 0, 0, 0]
    return " ".join(str(c) for c in cols)


def fake_ipc(tmp_path: Path, rows: list[str], uid: int) -> dict[str, str]:
    """Environment with a fixture shared-memory table of ``rows``, a stub ``ipcrm`` that logs its
    args, and an ``id`` that says the invoking user is the unprivileged ``uid``."""
    bin_dir = tmp_path / "stubs"
    bin_dir.mkdir()
    log = tmp_path / "ipcrm.log"
    table = tmp_path / "shm"
    header = "key shmid perms size cpid lpid nattch uid gid cuid cgid atime dtime ctime rss swap"
    table.write_text("\n".join([header, *rows]) + "\n")
    (bin_dir / "ipcrm").write_text(f'#!/usr/bin/env bash\necho "$@" >> {log}\n')
    (bin_dir / "id").write_text(
        f'#!/usr/bin/env bash\ncase "$1" in -u) echo {uid};; -un) echo tester;; *) exit 1;; esac\n'
    )
    for stub in bin_dir.iterdir():
        stub.chmod(0o755)
    return {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "CFS_SHM_TABLE": str(table),
    }


def test_clean_sh_removes_only_own_unattached_segments_of_its_run_dir(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    keyfile = run_dir / ".cdskeyfile"
    keyfile.write_text("")
    st = keyfile.stat()
    key = (0x52 << 24) | ((st.st_dev & 0xFF) << 16) | (st.st_ino & 0xFFFF)
    me = 1000
    rows = [
        shm_row(key, 101, 0, me),  # ours: removed
        shm_row(key, 102, 0, me + 1),  # other owner
        shm_row(key, 103, 2, me),  # still attached
        shm_row(key ^ 1, 104, 0, me),  # another key
    ]
    env = fake_ipc(tmp_path, rows, me)
    done = subprocess.run(
        [str(TOOLS / "clean.sh"), str(run_dir)],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert done.returncode == 0, done.stderr
    removed = (tmp_path / "ipcrm.log").read_text().split()
    assert removed == ["-m", "101"]


def init_repo(path: Path, tag: str) -> str:
    def git(*args: str) -> str:
        return subprocess.run(
            ["git", "-C", str(path), *args], capture_output=True, text=True, check=True
        ).stdout.strip()

    path.mkdir(parents=True)
    subprocess.run(["git", "init", "-q", str(path)], check=True)
    git("config", "user.email", "t@example.invalid")
    git("config", "user.name", "t")
    git("config", "commit.gpgsign", "false")
    (path / "f").write_text("x")
    git("add", "f")
    git("commit", "-q", "-m", "init")
    git("tag", tag)
    return git("rev-parse", "HEAD")


def stub_toolchain(tmp_path: Path, make_body: str) -> dict[str, str]:
    bin_dir = tmp_path / "toolbin"
    bin_dir.mkdir(parents=True)
    for tool in ("cmake", "gcc"):
        (bin_dir / tool).write_text("#!/usr/bin/env bash\nexit 0\n")
    (bin_dir / "make").write_text(f"#!/usr/bin/env bash\n{make_body}\n")
    for stub in bin_dir.iterdir():
        stub.chmod(0o755)
    return {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}"}


def test_build_sh_records_the_pinned_version_only_after_a_successful_build(tmp_path: Path) -> None:
    build = tmp_path / "build"
    commit = init_repo(build / "src", "v-actual")
    pinned = build / "PINNED_VERSION"
    base = {"CFS_BUILD_DIR": str(build), "CFS_REF": "v-requested", "CFS_COMMIT": "", "JOBS": "1"}

    failing = stub_toolchain(tmp_path / "fail", "exit 1")
    done = subprocess.run(
        [str(TOOLS / "build.sh")],
        env={**failing, **base},
        capture_output=True,
        text=True,
        check=False,
    )
    assert done.returncode != 0
    assert not pinned.exists(), "a failed build must not leave a PINNED_VERSION behind"

    exe = build / "src" / "build-native_std" / "exe" / "cpu1"
    ok = stub_toolchain(
        tmp_path / "ok",
        f'mkdir -p "{exe}"; printf "#!/bin/sh\\n" > "{exe}/core-cpu1"; chmod +x "{exe}/core-cpu1"',
    )
    done = subprocess.run(
        [str(TOOLS / "build.sh")], env={**ok, **base}, capture_output=True, text=True, check=False
    )
    assert done.returncode == 0, done.stderr
    text = pinned.read_text()
    assert f"bundle_commit={commit}" in text
    assert "bundle_ref=v-actual" in text, "the ref is what is checked out, not what was asked for"
