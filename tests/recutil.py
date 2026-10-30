"""Helpers shared by the recording tests: streams, frames, writing and crafting recordings."""

from __future__ import annotations

import asyncio
import os
import struct
import time
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path
from typing import Any

from ironbird.ccdd.model import MessageKind
from ironbird.frame import (
    FLAGS_BY_BITS,
    Direction,
    Frame,
    FrameFlag,
    Stream,
)
from ironbird.recording import format as fmt
from ironbird.recording.format import RecordType
from ironbird.recording.writer import Recorder, RecorderOptions

RX = Stream(0, "ground-fc", "fc", MessageKind.TELEMETRY, Direction.RX, "udp", "udp 127.0.0.1:2234")
TX = Stream(1, "ground-fc", "fc", MessageKind.COMMAND, Direction.TX, "udp", "udp 127.0.0.1:1234")
SERIAL = Stream(
    2, "uart", "fc", MessageKind.TELEMETRY, Direction.RX, "serial", "serial /dev/ttyUSB0"
)
MAX_PACKET = 2048
FAST = RecorderOptions(min_free_bytes=0)


def meta(**extra: Any) -> dict[str, Any]:
    return {
        "schema": fmt.META_SCHEMA,
        "session": "abcdef012345",
        "max_packet_bytes": MAX_PACKET,
        "nodes": {},
        **extra,
    }


def frame(
    seq: int,
    *,
    stream: Stream = RX,
    data: bytes | None = None,
    mid: int | None = 0x890,
    time_ns: int | None = None,
    source: str = "127.0.0.1:5000",
    flags: int = 0,
    orig_len: int | None = None,
) -> Frame:
    payload = bytes([seq % 251]) * 24 if data is None else data
    bits = flags | (int(FrameFlag.HAS_MID) if mid is not None else 0)
    return Frame(
        seq,
        1_700_000_000_000_000_000 + seq * 1000 if time_ns is None else time_ns,
        stream,
        source,
        mid,
        FLAGS_BY_BITS[bits],
        len(payload) if orig_len is None else orig_len,
        payload,
    )


def record(
    path: Path,
    frames: Iterable[Frame],
    *,
    options: RecorderOptions = FAST,
    streams: Sequence[Stream] = (RX, TX),
    accept: frozenset[int] | None = None,
    blobs: Sequence[Any] = (),
    marks: Callable[[Recorder], None] | None = None,
    **kwargs: Any,
) -> Recorder:
    """Write ``frames`` to ``path`` with a loop-less recorder and close it."""
    recorder = Recorder.open(
        path, meta(), blobs=blobs, streams=streams, accept=accept, options=options, **kwargs
    )
    for item in frames:
        recorder.append(item)
    if marks is not None:
        marks(recorder)
    asyncio.run(recorder.close())
    return recorder


def sample(path: Path, count: int = 40, **kwargs: Any) -> list[Frame]:
    frames = [frame(i, stream=RX if i % 4 else TX, mid=0x890 + i % 3) for i in range(1, count + 1)]
    record(path, frames, **kwargs)
    return frames


# -- crafting files byte by byte --


class Builder:
    """Builds a recording from raw records, so tests can write what the writer never would."""

    def __init__(self, nonce: int = 0x1122334455667788, *, max_packet: int = MAX_PACKET) -> None:
        self.nonce = nonce
        self.seed = fmt.seed_of(nonce)
        self.out = bytearray(fmt.encode_preamble(nonce))
        self.offsets: list[int] = []
        self.add(RecordType.META, fmt.encode_meta(meta(max_packet_bytes=max_packet)))

    def add(self, rtype: int, body: bytes) -> int:
        offset = len(self.out)
        self.offsets.append(offset)
        self.out += fmt.encode_record(rtype, body, self.seed)
        return offset

    def stream(self, stream: Stream = RX) -> int:
        return self.add(RecordType.STREAM, fmt.encode_stream(stream))

    def source(self, source_id: int = 0, text: str = "peer") -> int:
        return self.add(RecordType.SOURCE, fmt.encode_source(source_id, text))

    def frame_body(
        self,
        seq: int,
        *,
        data: bytes = b"x" * 20,
        stream: int = 0,
        source: int = 0,
        flags: int = 1,
        reserved: int = 0,
        mid: int = 0x890,
        orig_len: int | None = None,
        time_ns: int = 5,
    ) -> bytes:
        orig = len(data) if orig_len is None else orig_len
        return fmt.FRAME_FIXED.pack(seq, time_ns, stream, source, flags, reserved, mid, orig) + data

    def frame(self, seq: int, **kw: Any) -> int:
        return self.add(RecordType.FRAME, self.frame_body(seq, **kw))

    def raw(self, rtype: int, body: bytes, *, body_len: int | None = None) -> int:
        """A record whose envelope claims ``body_len`` (with a CRC that matches the claim)."""
        claimed = len(body) if body_len is None else body_len
        offset = len(self.out)
        import zlib

        head = fmt.ENVELOPE_HEAD.pack(claimed, rtype, 0, 0)
        crc = zlib.crc32(body, zlib.crc32(head, self.seed))
        self.out += fmt.ENVELOPE.pack(claimed, rtype, 0, 0, crc) + body
        self.out += bytes(-(fmt.ENVELOPE_SIZE + len(body)) & 7)
        return offset

    def standard(self, count: int = 6) -> Builder:
        self.stream(RX)
        self.stream(TX)
        self.source(0, "peer")
        for seq in range(1, count + 1):
            self.frame(seq, stream=seq % 2, source=0)
        return self

    def write(self, path: Path) -> Path:
        path.write_bytes(bytes(self.out))
        return path


def flip_bit(data: bytes, bit: int) -> bytes:
    out = bytearray(data)
    out[bit // 8] ^= 1 << (bit % 8)
    return bytes(out)


def u32(data: bytes, offset: int) -> int:
    return int(struct.unpack_from("<I", data, offset)[0])


def wait_until(predicate: Callable[[], bool], timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.02)


def is_writer_locked(path: Path) -> bool:
    import fcntl

    fd = os.open(path, os.O_RDONLY)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return True
        return False
    finally:
        os.close(fd)


# -- subprocess helpers for the CLI tests --

import socket  # noqa: E402
import subprocess  # noqa: E402
import sys  # noqa: E402

CLI_MAIN = "import sys; from ironbird.cli.main import main; sys.exit(main())"
EXAMPLES = Path(__file__).resolve().parent.parent / "examples"


def high_ports(count: int) -> list[int]:
    """Free UDP ports in 52000-52999 (mock tests keep clear of the fixed cFS ports)."""
    import random

    found: list[int] = []
    candidates = list(range(52000, 53000))
    random.shuffle(candidates)
    for port in candidates:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.bind(("127.0.0.1", port))
        except OSError:
            continue
        finally:
            sock.close()
        found.append(port)
        if len(found) == count:
            return found
    raise AssertionError("no free port in 52000-52999")


def write_config(directory: Path, extra: str = "") -> tuple[Path, int, int]:
    """examples/mock.toml with absolute paths, high ports and a recording table."""
    command_port, telemetry_port = high_ports(2)
    text = (EXAMPLES / "mock.toml").read_text()
    text = text.replace('"ccdd/', f'"{EXAMPLES}/ccdd/').replace(
        '"headers/', f'"{EXAMPLES}/headers/'
    )
    text = text.replace("127.0.0.1:41234", f"127.0.0.1:{command_port}")
    text = text.replace("127.0.0.1:41235", f"127.0.0.1:{telemetry_port}")
    if "[recording]" not in extra:
        extra = "[recording]\nmin_free_bytes = 0\n" + extra
    path = directory / "mock.toml"
    path.write_text(text + "\n" + extra)
    return path, command_port, telemetry_port


class Proc:
    """An ``ib`` subprocess with its output in files."""

    def __init__(self, directory: Path, name: str, args: list[str], env: dict[str, str]) -> None:
        self.out_path = directory / f"{name}.out"
        self.err_path = directory / f"{name}.err"
        self.out = self.out_path.open("wb")
        self.err = self.err_path.open("wb")
        self.popen = subprocess.Popen(
            [sys.executable, "-c", CLI_MAIN, *args], stdout=self.out, stderr=self.err, env=env
        )

    @property
    def stderr(self) -> str:
        return self.err_path.read_text(errors="replace")

    @property
    def stdout(self) -> str:
        return self.out_path.read_text(errors="replace")

    def wait_for(self, needle: str, timeout: float = 20.0) -> None:
        deadline = time.monotonic() + timeout
        while needle not in self.stderr:
            assert self.popen.poll() is None, f"exited early:\n{self.stderr}"
            assert time.monotonic() < deadline, f"never saw {needle!r}:\n{self.stderr}"
            time.sleep(0.03)

    def finish(self, timeout: float = 30.0) -> int:
        try:
            code = self.popen.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.popen.kill()
            self.popen.wait()
            raise
        finally:
            self.out.close()
            self.err.close()
        return code

    def kill(self) -> None:
        if self.popen.poll() is None:
            self.popen.kill()
            self.popen.wait()
        self.out.close()
        self.err.close()


def run_ib(
    directory: Path, args: list[str], env: dict[str, str], *, timeout: float = 60.0
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", CLI_MAIN, *args],
        capture_output=True,
        text=True,
        env=env,
        timeout=timeout,
        check=False,
        cwd=directory,
    )


def send_datagrams(port: int, datagrams: Sequence[bytes], *, pause: float = 0.0) -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        for data in datagrams:
            sock.sendto(data, ("127.0.0.1", port))
            if pause:
                time.sleep(pause)
    finally:
        sock.close()
