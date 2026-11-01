"""``main()``'s last-resort stderr line: bounded when stderr is a stalled terminal, a full pipe or
a full socket (the thread path and the threadless one), written whole to a healthy descriptor,
and encoded the same way whatever the stream's encoding is."""

from __future__ import annotations

import os
import pty
import re
import select
import socket
import subprocess
import sys
import textwrap
from collections.abc import Generator
from pathlib import Path

import pytest

from ironbird.cli.output import (
    CLOSE_WAIT_S,
    LINE_MARKER,
    WRITE_SLICE,
    BoundedWriter,
    encode_line,
    fit_line,
    line_codec,
)

SRC = str(Path(__file__).resolve().parent.parent / "src")
MARGIN_S = 1.5

CHILD = textwrap.dedent(
    """
    import sys, threading, time
    sys.path.insert(0, {src!r})
    mode, text = sys.argv[1], sys.argv[2]
    starts = []
    real_start = threading.Thread.start
    def start(self):
        starts.append(1)
        if mode == "threadless":
            raise RuntimeError("can't start new thread")
        real_start(self)
    threading.Thread.start = start
    if len(sys.argv) > 3:
        sys.stderr.reconfigure(encoding=sys.argv[3])
    import ironbird.cli.main as M
    began = time.monotonic()
    M._stderr_line(text)
    print(round(time.monotonic() - began, 3), len(starts), flush=True)
    """
).format(src=SRC)


def _child(
    mode: str, text: str, stderr: int, *extra: str, timeout: float = 20.0
) -> tuple[float, int]:
    """Run ``_stderr_line(text)`` in a child whose stderr is ``stderr``; returns the seconds it
    took and how many threads it tried to start."""
    done = subprocess.run(
        [sys.executable, "-c", CHILD, mode, text, *extra],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=stderr,
        timeout=timeout,
        check=True,
    )
    seconds, starts = done.stdout.split()
    return float(seconds), int(starts)


def _fill(path_or_fd: str | int) -> int:
    """Fill a pipe or terminal through a second, non-blocking description until it refuses;
    returns the bytes written. The descriptor the child inherits stays blocking."""
    target = path_or_fd if isinstance(path_or_fd, str) else f"/proc/self/fd/{path_or_fd}"
    second = os.open(target, os.O_WRONLY | os.O_NONBLOCK | os.O_NOCTTY)
    total = 0
    try:
        while True:
            try:
                total += os.write(second, b"y" * 1024)
            except BlockingIOError:
                return total
    finally:
        os.close(second)


@pytest.fixture
def tty() -> Generator[tuple[int, int]]:
    try:
        master, slave = pty.openpty()
    except OSError:
        pytest.skip("no pseudo-terminal available")
    yield master, slave
    os.close(master)
    os.close(slave)


MODES = ["thread", "threadless"]
LONG = "error IB-X: " + "\U0001f600" * 1000


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("drained", [0, 1, 700])
def test_a_stalled_terminal_does_not_hold_the_line_past_the_bound(
    tty: tuple[int, int], mode: str, drained: int
) -> None:
    master, slave = tty
    _fill(os.ttyname(slave))
    if drained:  # some room, but not for the whole line: the terminal still reports POLLOUT
        os.read(master, drained)
    seconds, starts = _child(mode, LONG, slave)
    assert starts == 1
    assert seconds <= CLOSE_WAIT_S + MARGIN_S


@pytest.mark.parametrize("mode", MODES)
def test_a_full_pipe_does_not_hold_the_line_past_the_bound(mode: str) -> None:
    read, write = os.pipe()
    try:
        _fill(write)
        seconds, _ = _child(mode, LONG, write)
        assert seconds <= CLOSE_WAIT_S + MARGIN_S
    finally:
        os.close(read)
        os.close(write)


@pytest.mark.parametrize("mode", MODES)
def test_a_full_socket_does_not_hold_the_line_past_the_bound(mode: str) -> None:
    left, right = socket.socketpair()
    try:
        left.setblocking(False)
        try:
            while True:
                left.send(b"y" * 4096)
        except BlockingIOError:
            pass
        left.setblocking(True)
        seconds, _ = _child(mode, LONG, left.fileno())
        assert seconds <= CLOSE_WAIT_S + MARGIN_S
    finally:
        left.close()
        right.close()


def test_a_pending_partial_line_does_not_hold_a_full_pipe() -> None:
    """Constructing a writer must not wait for room in a full pipe while text waits in the
    stream's buffer, and the writer's thread does not flush that text either. (The child itself
    cannot exit: that text can never be written.)"""
    code = textwrap.dedent(
        f"""
        import sys, time
        sys.path.insert(0, {SRC!r})
        from ironbird.cli.output import BoundedWriter
        sys.stdout.write("partial")
        began = time.monotonic()
        BoundedWriter(sys.stdout)
        print(round(time.monotonic() - began, 3), file=sys.stderr, flush=True)
        """
    )
    read, write = os.pipe()
    try:
        _fill(write)
        child = subprocess.Popen(
            [sys.executable, "-c", code],
            stdin=subprocess.DEVNULL,
            stdout=write,
            stderr=subprocess.PIPE,
            env={k: v for k, v in os.environ.items() if k != "PYTHONUNBUFFERED"},
        )
        try:
            assert child.stderr is not None
            ready, _, _ = select.select([child.stderr], [], [], 10)
            assert ready, "constructing the writer blocked"
            assert float(child.stderr.readline()) < 1.0
        finally:
            child.kill()
            child.wait()
    finally:
        os.close(read)
        os.close(write)


@pytest.mark.parametrize("mode", MODES)
def test_a_healthy_terminal_gets_the_whole_line(tty: tuple[int, int], mode: str) -> None:
    master, slave = tty
    _child(mode, "error IB-X: something failed", slave)
    ready, _, _ = select.select([master], [], [], 5)
    assert ready
    assert b"error IB-X: something failed" in os.read(master, 4096)


@pytest.mark.parametrize("mode", MODES)
def test_a_pipe_and_a_file_get_the_whole_line(tmp_path: Path, mode: str) -> None:
    read, write = os.pipe()
    try:
        _child(mode, "error IB-X: one", write)
        assert os.read(read, 4096) == b"error IB-X: one\n"
    finally:
        os.close(read)
        os.close(write)
    target = tmp_path / "err.txt"
    with target.open("wb") as handle:
        _child(mode, "error IB-X: two", handle.fileno())
    assert target.read_bytes() == b"error IB-X: two\n"
    with open(os.devnull, "wb") as null:
        _child(mode, "error IB-X: three", null.fileno())


# -- encoding ----------------------------------------------------------------------------

ENCODINGS = ["utf-8", "ascii", "latin-1", "utf-8-sig", "utf-16", "utf-32", "shift_jis", "cp037"]
TEXTS = [
    "error IB-X: " + "\U0001f600" * 1000,
    "error IB-X: " + "é" * 1000,
    "error IB-X: " + "中" * 1000,
    "error IB-X: short",
]


@pytest.mark.parametrize("encoding", ENCODINGS)
@pytest.mark.parametrize("text", TEXTS, ids=["emoji", "latin", "cjk", "short"])
def test_the_line_is_one_decodable_bounded_line_in_every_encoding(encoding: str, text: str) -> None:
    codec = line_codec(encoding)
    assert encode_line("x", codec)[-1:] == b"\n"
    fitted = fit_line(text, codec, WRITE_SLICE)
    data = encode_line(fitted, codec)
    assert len(data) <= WRITE_SLICE
    assert data.endswith(b"\n") and data.count(b"\n") == 1
    assert not data.startswith(tuple(b for b in (b"\xef\xbb\xbf", b"\xff\xfe", b"\xfe\xff")))
    decoded = data.decode(codec)  # whole: no half character or escape at the end
    assert decoded.endswith("\n")
    if fitted != text:
        assert decoded.endswith(LINE_MARKER + "\n")
    else:
        assert decoded == text.encode(codec, "backslashreplace").decode(codec) + "\n"


def test_a_cut_never_splits_an_escape() -> None:
    codec = line_codec("ascii")
    text = "error IB-X: " + "\U0001f600" * 1000
    data = encode_line(fit_line(text, codec, WRITE_SLICE), codec)
    assert re.fullmatch(rb"error IB-X: (\\U0001f600)+\.\.\.\n", data)


@pytest.mark.parametrize("encoding", ["utf-16", "utf-32", "cp037"])
def test_wide_or_foreign_newlines_fall_back_to_utf8(encoding: str) -> None:
    assert line_codec(encoding) == "utf-8"


@pytest.mark.parametrize("encoding", ["utf-8-sig", "latin-1", "shift_jis", "ascii"])
def test_ascii_newline_codecs_are_kept(encoding: str) -> None:
    assert line_codec(encoding) == encoding


def test_unknown_encoding_falls_back_to_utf8() -> None:
    assert line_codec("no-such-codec") == "utf-8"
    assert line_codec(None) == "utf-8"


@pytest.mark.parametrize("encoding", ["utf-16", "utf-8-sig", "ascii"])
def test_the_end_to_end_line_in_a_stream_encoding(encoding: str) -> None:
    read, write = os.pipe()
    try:
        _child("thread", LONG, write, encoding)
        data = os.read(read, 70000)
    finally:
        os.close(read)
        os.close(write)
    assert len(data) <= WRITE_SLICE
    assert data.endswith(b"\n")
    data.decode(line_codec(encoding))


def test_the_writer_uses_the_same_line_encoding() -> None:
    read, write = os.pipe()
    with os.fdopen(write, "w", encoding="utf-16", closefd=False) as stream:
        writer = BoundedWriter(stream)
        writer.write("one")
        writer.write("two")
        assert writer.close()
    try:
        assert os.read(read, 100) == b"one\ntwo\n"
    finally:
        os.close(read)
        os.close(write)
