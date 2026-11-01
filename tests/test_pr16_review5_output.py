"""Regression tests for the fifth round of PR #16 review findings on terminal output: the writer's
thread never flushes the stream (and so never holds its buffer lock on a full pipe), ``main``
flushes the standard streams before any console exists, a line's encoding strips the codec's own
mark and nothing else, a codec that cannot escape falls back to UTF-8, a line fits its bound in
every codec, and a refused reopen of a terminal drops the line instead of hanging."""

from __future__ import annotations

import contextlib
import inspect
import io
import os
import pty
import subprocess
import sys
import textwrap
import threading
from pathlib import Path

import pytest

from ironbird.cli.output import (
    LINE_MARKER,
    BoundedWriter,
    encode_line,
    fit_line,
    line_codec,
)

SRC = str(Path(__file__).resolve().parent.parent / "src")


# -- the writer never flushes the stream ----------------------------------------------------


class _RecordingText(io.TextIOWrapper):
    """A text stream that notes which threads flushed it."""

    flushed_by: list[str]

    def flush(self) -> None:
        self.flushed_by.append(threading.current_thread().name)
        super().flush()


def _pipe_stream(write_fd: int) -> _RecordingText:
    stream = _RecordingText(
        io.BufferedWriter(io.FileIO(write_fd, "w", closefd=False), 8192), encoding="utf-8"
    )
    stream.flushed_by = []
    return stream


def test_the_writer_has_no_flush_first_option() -> None:
    assert "flush_first" not in inspect.signature(BoundedWriter).parameters


def test_the_writer_thread_never_flushes_the_stream() -> None:
    read, write = os.pipe()
    try:
        stream = _pipe_stream(write)
        stream.write("earlier buffered text\n")
        writer = BoundedWriter(stream)
        writer.write("a line")
        assert writer.close()
        assert stream.flushed_by == []
        assert os.read(read, 100) == b"a line\n"  # only the writer's line reached the pipe
    finally:
        os.close(read)
        os.close(write)


def test_a_full_pipe_cannot_make_the_writer_hold_the_streams_buffer_lock() -> None:
    """With text waiting in the stream and the pipe full, an abandoned writer used to leave its
    thread blocked inside ``stream.flush()`` holding the buffer's lock, so any later use of the
    stream's buffer waited for it."""
    read, write = os.pipe()
    os.set_blocking(write, False)
    try:
        while True:
            os.write(write, b"x" * 65536)
    except BlockingIOError:
        pass
    os.set_blocking(write, True)
    stream = _pipe_stream(write)
    try:
        stream.write("earlier buffered text\n")
        writer = BoundedWriter(stream)
        writer.write("queued line")
        assert not writer.close(0.3)  # the pipe takes nothing: the drain is cut short
        done = threading.Event()

        def use_buffer() -> None:
            stream.buffer.write(b"y")  # takes the buffer's lock; buffers in memory
            done.set()

        user = threading.Thread(target=use_buffer, daemon=True)
        user.start()
        assert done.wait(2.0), "the buffer's lock is held by the writer's thread"
    finally:
        os.close(read)  # the abandoned write fails with EPIPE and the thread ends
        with contextlib.suppress(OSError, ValueError):
            stream.close()
        os.close(write)


FLUSH_CHILD = textwrap.dedent(
    """
    import io, sys, os
    sys.path.insert(0, {src!r})
    import ironbird.cli.main as M

    class Recording(io.StringIO):
        flushes = 0
        def flush(self):
            Recording.flushes += 1
            super().flush()

    sys.stdout, sys.stderr = Recording(), Recording()
    seen = []
    def fake_app(standalone_mode=False):
        seen.append(sys.stdout.flushes)  # flushes of this instance before the command ran
        return 0
    M.app = fake_app
    sys.stdout.write("text from before main")
    code = M.main()
    os.write(1, f"{{code}} {{seen[0]}}\\n".encode())
    """
).format(src=SRC)


def test_main_flushes_the_standard_streams_before_a_command_runs() -> None:
    done = subprocess.run(
        [sys.executable, "-c", FLUSH_CHILD],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        timeout=30,
        check=True,
    )
    code, flushes = done.stdout.split()
    assert code == b"0" and int(flushes) >= 1


# -- line encoding ---------------------------------------------------------------------------


def test_a_line_starting_with_a_byte_order_mark_character_keeps_it() -> None:
    assert encode_line("\ufeffbom", "utf-8") == "\ufeffbom\n".encode()
    assert encode_line("\ufeffbom", "utf-8-sig") == b"\xef\xbb\xbfbom\n"  # one mark, the text's
    assert encode_line("\ufeff", "latin-1") == b"\\ufeff\n"


@pytest.mark.parametrize("codec", ["utf-8", "utf-8-sig", "ascii", "latin-1", "shift_jis"])
def test_a_line_never_starts_with_the_codecs_own_mark(codec: str) -> None:
    mark = "".encode(codec)
    assert not mark or not encode_line("x", codec).startswith(mark)
    assert encode_line("x", codec) == b"x\n"


def test_a_codec_that_cannot_escape_falls_back_to_utf8() -> None:
    assert line_codec("idna") == "utf-8"
    assert encode_line("plain error line", line_codec("idna")) == b"plain error line\n"


CODECS = [
    "utf-8",
    "ascii",
    "latin-1",
    "utf-8-sig",
    "utf-16",
    "utf-16-le",
    "utf-32",
    "cp037",
    "iso2022_jp",
    "iso2022_jp_2",
    "hz",
    "shift_jis",
    "gb18030",
    "utf-7",
    "unicode_escape",
    "raw_unicode_escape",
    "idna",
    "punycode",
    "undefined",
    "rot13",
    "hex",
    "big5hkscs",
    "euc_jis_2004",
    "mbcs",
    "nonexistent",
    "",
    None,
    "UTF8",
    "cp65001",
    "palmos",
    "koi8_u",
]
TEXTS = [
    "plain ascii error line",
    "café 日本語 \U0001f600 á",
    "日" * 50 + "abc" + "\U0001f600" * 30,
    "back\\slash € \x80",
    "\ufeffbom start",
    "",
    "a" * 10,
]


@pytest.mark.parametrize("encoding", CODECS, ids=repr)
def test_every_stream_codec_gives_lines_that_fit_their_bound(encoding: str | None) -> None:
    codec = line_codec(encoding)
    smallest = len(encode_line(LINE_MARKER, codec))
    for text in TEXTS:
        full = encode_line(text, codec)
        assert full.endswith(b"\n") and full.count(b"\n") == text.count("\n") + 1, (text, codec)
        for limit in range(smallest, len(full) + 3):
            fitted = fit_line(text, codec, limit)
            assert len(encode_line(fitted, codec)) <= limit, (text, codec, limit)
            if fitted == text:
                continue
            assert fitted.endswith(LINE_MARKER) and text.startswith(fitted[:-3]), (text, codec)
            kept = len(fitted) - len(LINE_MARKER)
            if kept < len(text):  # maximal: one more character does not fit
                assert len(encode_line(text[: kept + 1] + LINE_MARKER, codec)) > limit, (
                    text,
                    codec,
                    limit,
                )


# -- a terminal that refuses the reopen ------------------------------------------------------

REFUSED_CHILD = textwrap.dedent(
    """
    import os, sys
    sys.path.insert(0, {src!r})
    from ironbird.cli import output as out
    os.setgroups([])
    os.setresgid(65534, 65534, 65534)
    os.setresuid(65534, 65534, 65534)
    import ctypes
    ctypes.CDLL(None).prctl(4, 1, 0, 0, 0)  # PR_SET_DUMPABLE: /proc/self/fd readable again
    ok = out.write_within(2, b"error IB-X: the line\\n", 0.5)
    os.write(1, b"returned %d\\n" % ok)
    """
).format(src=SRC)


@pytest.mark.skipif(os.geteuid() != 0, reason="needs root to give the terminal to another user")
def test_a_terminal_that_refuses_the_reopen_drops_the_line_and_returns() -> None:
    """The accepted gap (docs/plan.md): reachable only when no thread can start and the reopen
    of the terminal through ``/proc/self/fd`` is refused. The line is dropped, the call returns
    False, and the process is not held."""
    master, slave = pty.openpty()
    try:
        os.chown(os.ttyname(slave), 0, 5)
        os.chmod(os.ttyname(slave), 0o620)
        done = subprocess.run(
            [sys.executable, "-c", REFUSED_CHILD],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=slave,
            timeout=30,
            check=False,
        )
        assert done.returncode == 0, done.stdout
        assert done.stdout.strip() == b"returned 0"
    finally:
        os.close(master)
        os.close(slave)
