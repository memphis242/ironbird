"""The throughput gate's machinery: an ``ib record`` subprocess fed by the loopback blaster.

``run_gate`` records what a run did (counts, kernel and queue drops, the recorder's buffer peak,
its RSS and CPU); the assertions and the regression baseline are in ``test_throughput_gate.py``.
"""

from __future__ import annotations

import json
import os
import resource
import signal
import struct
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from conftest import EXAMPLES
from ironbird.config import load_config
from ironbird.ledger import Ledger
from ironbird.recording import inspect as rec_inspect
from ironbird.recording.reader import RecordingReader
from ironbird.session import Session
from recutil import Proc, high_ports

BLASTER = Path(__file__).with_name("blaster.py")
PACKET_BYTES = 200
HEADER_BYTES = 16  # the cFS telemetry header profile
SERIAL_OFFSET = HEADER_BYTES  # GATE_TLM.serial is the first payload field
GATE_MID = 0x0898
SETTLE_S = 30.0  # the most the recorder is given to drain after the blaster ends
LISTEN_TIMEOUT_S = 30.0
CLOCK_TICKS_PER_S = os.sysconf("SC_CLK_TCK")


@dataclass(frozen=True)
class GateResult:
    """What one run measured. Byte counts are bytes, times are seconds."""

    rate: int
    sent: int
    blast_s: float
    blast_max_lag_ms: float
    recorded: int  # telemetry frames of the gate message in the recording
    recorded_total: int  # every frame in the recording
    in_order: bool
    kernel_gap_lost: int  # datagrams the kernel dropped, as the recording's GAP records say
    udp_socket_drops: int  # the receiving socket's own drop counter (/proc/net/udp)
    queue_not_shown: int  # frames the capture's queue refused
    queue_peak_bytes: int
    overrun_frames: int  # frames the recorder's buffer refused
    gap_records: int
    peak_pending_bytes: int  # the recorder's buffer at its fullest
    max_buffer_bytes: int
    rss_before_kib: int
    rss_peak_kib: int
    cpu_blast_s: float  # recorder CPU between the first datagram and the blaster's end
    cpu_total_s: float  # the recorder's CPU over its whole life (start-up and close included)
    cpu_share: float  # cpu_blast_s / blast_s: cores' worth
    cpu_us_per_frame: float
    status: str
    file_bytes: int
    exit_code: int
    stderr_lines: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        out = asdict(self)
        out["stderr_lines"] = list(self.stderr_lines)
        return out


def gate_config(directory: Path) -> tuple[Path, int, bytes]:
    """A config with a 200-byte telemetry message ``GATE_TLM`` (a ``uint32 serial`` and a string)
    on a free loopback port. The retention budget is lifted: at 10k packets/s the recordings pass
    the default one within the hour, which is retention's warning to give, not the gate's to judge.
    Returns (the config, the telemetry port, one valid packet)."""
    ccdd = json.loads((EXAMPLES / "ccdd" / "mock.json").read_text())
    ccdd["messages"].append(
        {
            "name": "GATE_TLM",
            "kind": "telemetry",
            "mid": f"{GATE_MID:#06x}",
            "category": "housekeeping",
            "fields": [
                {"name": "serial", "type": "uint32"},
                {"name": "blob", "type": f"char[{PACKET_BYTES - HEADER_BYTES - 4}]"},
            ],
        }
    )
    ccdd_path = directory / "gate.json"
    ccdd_path.write_text(json.dumps(ccdd))
    command_port, telemetry_port = high_ports(2)
    path = directory / "gate.toml"
    path.write_text(
        f"""[ironbird]
acts_as = "ground"
[nodes.ground]
[nodes.fc]
ccdd = "{ccdd_path}"
command_header = "{EXAMPLES}/headers/ccsds_v1_cfs_cmd.toml"
telemetry_header = "{EXAMPLES}/headers/ccsds_v1_cfs_tlm.toml"
[[links]]
name = "ground-fc"
from = "ground"
to = "fc"
transport = "udp"
command_endpoint = "127.0.0.1:{command_port}"
telemetry_endpoint = "127.0.0.1:{telemetry_port}"
[recording]
min_free_bytes = 0
[retention]
max_total_bytes = "unlimited"
"""
    )
    config = load_config(path)
    ledger = Ledger(directory / "template-ledger", session_id="gate")
    session = Session(config, role="ground", ledger=ledger, notify=lambda _notice: None)
    wire = (
        session.codec_for("fc").encode_telemetry("GATE_TLM", {"serial": 0, "blob": "x" * 20}).data
    )
    assert len(wire) == PACKET_BYTES
    assert struct.unpack_from("<I", wire, SERIAL_OFFSET)[0] == 0
    return path, telemetry_port, wire


@dataclass(frozen=True)
class _ProcSample:
    cpu_s: float
    rss_kib: int
    peak_rss_kib: int


def _proc_sample(pid: int) -> _ProcSample | None:
    """CPU seconds, RSS and peak RSS of ``pid`` from /proc, or None once it is gone."""
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
        status = Path(f"/proc/{pid}/status").read_text()
    except OSError:
        return None
    after_name = stat[stat.rindex(")") + 2 :].split()  # field 3 (state) onwards
    ticks = int(after_name[11]) + int(after_name[12])  # utime + stime
    kib = {
        line.split(":")[0]: int(line.split()[1])
        for line in status.splitlines()
        if line.startswith(("VmRSS:", "VmHWM:"))
    }
    return _ProcSample(ticks / CLOCK_TICKS_PER_S, kib["VmRSS"], kib["VmHWM"])


def socket_drops(port: int) -> int:
    """The ``drops`` column of /proc/net/udp for the sockets bound to loopback ``port``."""
    total = 0
    suffix = f":{port:04X}"
    for table in ("/proc/net/udp", "/proc/net/udp6"):
        try:
            rows = Path(table).read_text(errors="replace").splitlines()[1:]
        except OSError:
            continue
        for row in rows:
            parts = row.split()
            if len(parts) > 12 and parts[1].endswith(suffix):
                total += int(parts[-1])
    return total


def _ledger_events(state: Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for path in sorted(state.glob("*.jsonl")):
        events.extend(json.loads(line) for line in path.read_text().splitlines() if line)
    return events


def _serials_in_order(path: Path) -> tuple[int, int, bool]:
    """(gate frames in the recording, every frame, whether the serials run 0, 1, 2, ... exactly)."""
    expected = 0
    gate_frames = 0
    total = 0
    in_order = True
    with RecordingReader(path) as reader:
        for item in reader.frames():
            total += 1
            if item.mid != GATE_MID or len(item.data) != PACKET_BYTES:
                in_order = False
                continue
            gate_frames += 1
            (serial,) = struct.unpack_from("<I", item.data, SERIAL_OFFSET)
            in_order = in_order and serial == expected
            expected = serial + 1
    return gate_frames, total, in_order


def run_gate(directory: Path, rate: int, seconds: int) -> GateResult:
    """Blast ``rate * seconds`` datagrams at ``rate`` per second into ``ib record`` and measure."""
    assert rate > 0 and seconds > 0
    count = rate * seconds
    config, port, wire = gate_config(directory)
    state = directory / "state"
    out = directory / "gate.ibrec"
    env = {
        **os.environ,
        "IRONBIRD_STATE_DIR": str(state),
        "IRONBIRD_RECORD_DIR": str(directory / "recordings"),
    }
    env.pop("IRONBIRD_CONFIG", None)
    recorder = Proc(
        directory,
        "record",
        ["-c", str(config), "record", "-o", str(out), "--quiet", "--count", str(count)],
        env,
    )
    try:
        recorder.wait_for("recording: listening on", LISTEN_TIMEOUT_S)
        pid = recorder.popen.pid
        before = _proc_sample(pid)
        assert before is not None
        blast = subprocess.run(
            [
                sys.executable,
                str(BLASTER),
                f"--port={port}",
                f"--rate={rate}",
                f"--count={count}",
                f"--template-hex={wire.hex()}",
                f"--serial-offset={SERIAL_OFFSET}",
            ],
            capture_output=True,
            text=True,
            check=True,
            timeout=seconds * 2 + 60,
        )
        blasted = json.loads(blast.stdout)
        # The blaster is reaped by now: the recorder, reaped later, is the only child added.
        children_before = resource.getrusage(resource.RUSAGE_CHILDREN)
        after = _proc_sample(pid)
        assert after is not None
        peak_rss = after.peak_rss_kib
        udp_drops = socket_drops(port)
        deadline = time.monotonic() + SETTLE_S
        while recorder.popen.poll() is None and time.monotonic() < deadline:
            latest = _proc_sample(pid)
            if latest is not None:
                peak_rss = max(peak_rss, latest.peak_rss_kib)
                udp_drops = max(udp_drops, socket_drops(port))
            time.sleep(0.1)
        if recorder.popen.poll() is None:  # frames were lost: stop it so the numbers show how many
            recorder.popen.send_signal(signal.SIGINT)
        code = recorder.finish(timeout=60)
    finally:
        recorder.kill()
    children_after = resource.getrusage(resource.RUSAGE_CHILDREN)
    cpu_total = (children_after.ru_utime - children_before.ru_utime) + (
        children_after.ru_stime - children_before.ru_stime
    )

    report = rec_inspect.scan(out)
    events = _ledger_events(state)
    stops = [e for e in events if e["event"] == "recording.stop"]
    starts = [e for e in events if e["event"] == "recording.start"]
    captures = [e for e in events if e["event"] == "record.capture"]
    assert len(stops) == len(starts) == len(captures) == 1, [e["event"] for e in events]
    recorded, total, in_order = _serials_in_order(out)
    cpu_blast = after.cpu_s - before.cpu_s
    blast_s = float(blasted["elapsed_s"])
    return GateResult(
        rate=rate,
        sent=int(blasted["sent"]),
        blast_s=blast_s,
        blast_max_lag_ms=float(blasted["max_lag_ms"]),
        recorded=recorded,
        recorded_total=total,
        in_order=in_order,
        kernel_gap_lost=report.lost_by_reason.get("kernel_drop", 0),
        udp_socket_drops=udp_drops,
        queue_not_shown=int(captures[0]["queue_not_shown"]),
        queue_peak_bytes=int(captures[0]["queue_peak_bytes"]),
        overrun_frames=int(stops[0]["overrun_frames"]),
        gap_records=report.gap_total,
        peak_pending_bytes=int(stops[0]["peak_pending_bytes"]),
        max_buffer_bytes=int(starts[0]["max_buffer_bytes"]),
        rss_before_kib=before.rss_kib,
        rss_peak_kib=peak_rss,
        cpu_blast_s=cpu_blast,
        cpu_total_s=cpu_total,
        cpu_share=cpu_blast / blast_s,
        cpu_us_per_frame=cpu_blast / max(recorded, 1) * 1e6,
        status=report.status,
        file_bytes=out.stat().st_size,
        exit_code=code,
        stderr_lines=tuple(recorder.stderr.splitlines()),
    )
