"""Stage 2k, end to end: ``ib`` processes talking over pseudo-terminal pairs.

Two ptys and a relay (``support/ptybridge.py``) stand in for a serial cable between the ground
node and the flight node. ``ib respond --as fc`` is one end of it, ``ib listen`` / ``ib send`` /
``ib record`` the other. Unplugging is closing a pty master; the device path is a symlink the
bridge repoints on a replug, as ``/dev/serial/by-id`` does. Every wait is for a condition (a line
on stderr, frames in the recording), never for a fixed time.
"""

from __future__ import annotations

import json
import os
import signal
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from ironbird.ccdd.model import MessageKind
from ironbird.errors import Code, DecodeError
from ironbird.frame import Direction, FrameFlag
from ironbird.recording import inspect as rec_inspect
from ironbird.recording.format import MarkKind
from ironbird.recording.reader import RecordingReader
from recutil import Proc, run_ib
from serialutil import CODEC, hk, serial_config
from support.ptybridge import PtyBridge


@pytest.fixture
def bridge(tmp_path: Path) -> Iterator[PtyBridge]:
    with PtyBridge(tmp_path) as cable:
        yield cable


@pytest.fixture
def configs(tmp_path: Path, bridge: PtyBridge) -> dict[str, Path]:
    return {
        "ground": serial_config(tmp_path, "ground.toml", bridge.a.path, "ground"),
        "fc": serial_config(tmp_path, "fc.toml", bridge.b.path, "fc"),
    }


@pytest.fixture
def env() -> dict[str, str]:
    return dict(os.environ)


def noop() -> bytes:
    return CODEC.encode_command("MOCK_NOOP").data


def wait_until(
    predicate: Callable[[], bool], what: str, *, timeout: float = 30.0, proc: Proc | None = None
) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if proc is not None:
            assert proc.popen.poll() is None, f"exited while waiting for {what}:\n{proc.stderr}"
        assert time.monotonic() < deadline, f"timed out waiting for {what}"
        time.sleep(0.02)


def frames_of(path: Path) -> list[Any]:
    """The frames of a recording that may still be written (none if it is not readable yet)."""
    try:
        with RecordingReader(path) as reader:
            return list(reader.frames())
    except Exception:
        return []


def stop(proc: Proc, sig: int = signal.SIGINT) -> int:
    if proc.popen.poll() is None:
        proc.popen.send_signal(sig)
    return proc.finish()


def ledger_events(tmp_path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for path in sorted((tmp_path / "state").glob("*.jsonl")):
        out += [json.loads(line) for line in path.read_text().splitlines()]
    return out


# -- commands over the line -------------------------------------------------------------------


def test_ib_respond_answers_a_command_that_ib_listen_decodes(
    tmp_path: Path, bridge: PtyBridge, configs: dict[str, Path], env: dict[str, str]
) -> None:
    responder = Proc(tmp_path, "respond", ["-c", str(configs["fc"]), "respond", "--as", "fc"], env)
    listener = Proc(tmp_path, "listen", ["-c", str(configs["ground"]), "listen", "fc"], env)
    try:
        responder.wait_for("responding")
        listener.wait_for("listening for")
        assert f"serial {bridge.a.path}" in listener.stderr
        bridge.inject("b", noop())  # the ground's command, as the line would carry it
        wait_until(lambda: "MOCK_HK" in listener.stdout, "the housekeeping reply", proc=listener)
        assert "cmd_counter=1" in listener.stdout
        assert "MOCK_NOOP" in responder.stdout
    finally:
        stop(listener)
        stop(responder)
    assert "IB-TX-" not in listener.stderr + responder.stderr


def test_ib_send_goes_out_over_the_line_and_the_responder_decodes_it(
    tmp_path: Path, bridge: PtyBridge, configs: dict[str, Path], env: dict[str, str]
) -> None:
    responder = Proc(tmp_path, "respond", ["-c", str(configs["fc"]), "respond", "--as", "fc"], env)
    try:
        responder.wait_for("responding")
        sent = run_ib(tmp_path, ["-c", str(configs["ground"]), "send", "fc", "MOCK_NOOP"], env)
        assert sent.returncode == 0, sent.stderr
        assert "via serial" in sent.stdout and str(bridge.a.path) in sent.stdout
        wait_until(lambda: "MOCK_NOOP" in responder.stdout, "the command", proc=responder)
        # the reply travels back over the line to the ground end, where no one listens now
        wait_until(lambda: len(bridge.log("a")) >= len(hk(1)), "the reply on the line")
    finally:
        stop(responder)
    on_the_line = bridge.log("b")  # what the sender wrote, byte for byte
    assert len(on_the_line) == len(noop()) == 8
    decoded = CODEC.decode(MessageKind.COMMAND, on_the_line)
    assert not isinstance(decoded, DecodeError) and decoded.message.name == "MOCK_NOOP"


def test_a_second_process_on_a_device_in_use_is_told_it_is_locked(
    tmp_path: Path, bridge: PtyBridge, configs: dict[str, Path], env: dict[str, str]
) -> None:
    listener = Proc(tmp_path, "listen", ["-c", str(configs["ground"]), "listen", "fc"], env)
    try:
        listener.wait_for("listening for")
        sent = run_ib(tmp_path, ["-c", str(configs["ground"]), "send", "fc", "MOCK_NOOP"], env)
    finally:
        stop(listener)
    assert sent.returncode == 1
    assert (
        f"error {Code.TX_BUSY.value}" in sent.stderr and "locked by another process" in sent.stderr
    )
    assert bridge.log("b") == b""  # nothing was sent


def test_a_missing_device_is_a_coded_error_with_a_remedy_and_nothing_is_retried(
    tmp_path: Path, bridge: PtyBridge, configs: dict[str, Path], env: dict[str, str]
) -> None:
    bridge.unplug("a")
    done = run_ib(tmp_path, ["-c", str(configs["ground"]), "listen", "fc"], env)
    assert done.returncode == 1
    assert f"error {Code.TX_OPEN.value}" in done.stderr
    assert "check the cable" in done.stderr and "/dev/serial/by-id" in done.stderr
    assert done.stderr.count(Code.TX_OPEN.value) == 1


def test_a_header_profile_without_a_length_field_is_a_coded_link_error(
    tmp_path: Path, bridge: PtyBridge, env: dict[str, str]
) -> None:
    from conftest import EXAMPLES

    text = (EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml").read_text()
    broken = tmp_path / "tlm-no-length.toml"
    broken.write_text(text.replace('computed = "length"', "value = 0").replace("bias = -7\n", ""))
    config = serial_config(tmp_path, "ground.toml", bridge.a.path, "ground")
    config.write_text(
        config.read_text().replace(f"{EXAMPLES}/headers/ccsds_v1_cfs_tlm.toml", str(broken))
    )
    done = run_ib(tmp_path, ["-c", str(config), "listen", "fc"], env)
    assert done.returncode == 2, done.stderr
    assert f"error {Code.CFG_LINK.value}" in done.stderr and "length field" in done.stderr


# -- recording over the line --------------------------------------------------------------------


def test_a_recording_over_the_line_holds_the_bytes_the_line_carried_and_resyncs_once_per_burst(
    tmp_path: Path, bridge: PtyBridge, configs: dict[str, Path], env: dict[str, str]
) -> None:
    out = tmp_path / "serial.ibrec"
    recorder = Proc(
        tmp_path, "rec", ["-c", str(configs["ground"]), "record", "-o", str(out), "--quiet"], env
    )
    bursts = 12
    try:
        recorder.wait_for("recording: listening on")
        bridge.split_into("a", 7)  # the line delivers in odd pieces
        for n in range(bursts):
            bridge.inject("a", hk(n))
            wait_until(lambda n=n: len(frames_of(out)) >= 2 * n + 1, f"packet {n}", proc=recorder)
            bridge.inject("a", bytes([0x00, 0x01, 0x02, n]) + b"noise")  # not a header
            wait_until(lambda n=n: len(frames_of(out)) >= 2 * n + 2, f"burst {n}", proc=recorder)
        bridge.inject("a", hk(99))
        wait_until(lambda: len(frames_of(out)) >= 2 * bursts + 1, "the last packet", proc=recorder)
    finally:
        code = stop(recorder)
    assert code in (0, 130), recorder.stderr
    frames = frames_of(out)
    rx = [f for f in frames if f.stream.direction is Direction.RX]
    # byte for byte what the relay delivered to the ground end
    assert b"".join(f.data for f in rx) == bridge.log("a")
    assert [f.data for f in rx if not f.flags & FrameFlag.GARBAGE][: bursts + 1] == [
        *(hk(n) for n in range(bursts)),
        hk(99),
    ]
    assert sum(1 for f in rx if f.flags & FrameFlag.GARBAGE) == bursts
    assert all(f.stream.transport == "serial" and f.source == "" for f in rx)
    lines = [ln for ln in recorder.stderr.splitlines() if Code.TX_RESYNC.value in ln]
    assert 1 <= len(lines) < bursts  # the bursts are coalesced, not announced one by one
    report = rec_inspect.info(out)
    assert report.status == "closed" and report.gap_total == 0
    events = ledger_events(tmp_path)
    assert any(e["event"] == "serial.open" and e["baud"] == 115200 for e in events)


def test_unplugging_and_replugging_the_device_records_a_link_down_gap(
    tmp_path: Path, bridge: PtyBridge, configs: dict[str, Path], env: dict[str, str]
) -> None:
    out = tmp_path / "unplug.ibrec"
    recorder = Proc(
        tmp_path, "rec", ["-c", str(configs["ground"]), "record", "-o", str(out), "--quiet"], env
    )
    try:
        recorder.wait_for("recording: listening on")
        bridge.inject("a", hk(1) + hk(2) + hk(3))
        wait_until(lambda: len(frames_of(out)) >= 3, "the packets before the unplug", proc=recorder)
        bridge.unplug("a")
        wait_until(lambda: Code.TX_DOWN.value in recorder.stderr, "the link down notice")
        assert "reconnecting" in recorder.stderr
        bridge.plug("a")
        wait_until(lambda: "link restored" in recorder.stderr, "the link restored notice")
        bridge.inject("a", hk(4) + hk(5))
        wait_until(lambda: len(frames_of(out)) >= 5, "the packets after the replug", proc=recorder)
    finally:
        stop(recorder)
    assert [f.data for f in frames_of(out)] == [hk(n) for n in range(1, 6)]
    report = rec_inspect.info(out)
    assert report.status == "closed"
    [gap] = report.gaps
    assert gap.reason.name == "LINK_DOWN" and gap.last_ns >= gap.first_ns and gap.lost == 1
    assert report.gap_total == 1 and report.lost_by_reason == {"link_down": 1}
    assert report.hole_frames == 0  # a link-down GAP leaves no sequence hole behind it
    kinds = [m.kind for m in report.marks]
    assert MarkKind.LINK_DOWN in kinds and MarkKind.LINK_UP in kinds
    assert kinds.index(MarkKind.LINK_DOWN) < kinds.index(MarkKind.LINK_UP)
    down = next(m for m in report.marks if m.kind == MarkKind.LINK_DOWN)
    assert down.time_ns == gap.first_ns
    events = ledger_events(tmp_path)
    states = [e["event"] for e in events if e["event"] in ("link.down", "link.up")]
    assert states == ["link.down", "link.up"]
    # visible in the tools that read the file
    info = run_ib(tmp_path, ["rec", "info", str(out)], env)
    assert "link_down" in info.stdout
    replay = run_ib(tmp_path, ["-c", str(configs["ground"]), "replay", str(out)], env)
    assert replay.returncode == 0 and "1 gap(s)" in replay.stderr
    export_dir = tmp_path / "exported"
    exported = run_ib(
        tmp_path, ["-c", str(configs["ground"]), "export", str(out), "-o", str(export_dir)], env
    )
    assert exported.returncode == 0, exported.stderr
    assert "1 gap(s)" in exported.stderr or "1 gap(s)" in exported.stdout


def test_listen_keeps_decoding_after_the_device_comes_back(
    tmp_path: Path, bridge: PtyBridge, configs: dict[str, Path], env: dict[str, str]
) -> None:
    listener = Proc(tmp_path, "listen", ["-c", str(configs["ground"]), "listen", "fc"], env)
    try:
        listener.wait_for("listening for")
        bridge.inject("a", hk(1))
        wait_until(lambda: "cmd_counter=1" in listener.stdout, "the first packet", proc=listener)
        bridge.unplug("a")
        wait_until(lambda: Code.TX_DOWN.value in listener.stderr, "the link down notice")
        bridge.plug("a")
        wait_until(lambda: "link restored" in listener.stderr, "the link restored notice")
        bridge.inject("a", hk(2))
        wait_until(lambda: "cmd_counter=2" in listener.stdout, "the packet after", proc=listener)
    finally:
        stop(listener)
    assert listener.stderr.count(f"error {Code.TX_DOWN.value}: ") >= 1
    down_notices = [ln for ln in listener.stderr.splitlines() if "link lost" in ln]
    assert len(down_notices) == 1  # one notice for the outage, however long the reconnect took


def test_an_udp_recording_never_gets_a_link_down_gap(
    tmp_path: Path, env: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    from recutil import send_datagrams, write_config

    path, command_port, telemetry_port = write_config(tmp_path)
    monkeypatch.setenv("IRONBIRD_CONFIG", str(path))
    out = tmp_path / "udp.ibrec"
    recorder = Proc(
        tmp_path, "rec", ["record", "-o", str(out), "--count", "3", "--quiet"], dict(os.environ)
    )
    try:
        recorder.wait_for("recording: listening on")
        send_datagrams(telemetry_port, [hk(n) for n in range(1, 4)], pause=0.01)
        assert recorder.finish() == 0, recorder.stderr
    finally:
        recorder.kill()
    report = rec_inspect.info(out)
    assert report.gap_total == 0 and not any(
        m.kind in (MarkKind.LINK_DOWN, MarkKind.LINK_UP) for m in report.marks
    )
    assert command_port != telemetry_port
