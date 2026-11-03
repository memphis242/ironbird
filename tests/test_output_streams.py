"""``BoundedWriter`` on streams that have no descriptor (in-memory capture, odd encodings) and on
streams that fail: lines are never lost silently, a gone reader is announced once, and a
failing stream is counted rather than raised into the producer."""

from __future__ import annotations

import errno
import io

import pytest

from ironbird.cli.output import BoundedWriter


class Latin1Out(io.StringIO):
    """A text stream that cannot encode non-Latin-1 text, like a terminal set to one."""

    encoding = "latin-1"

    def write(self, text: str, /) -> int:
        text.encode("latin-1")  # raises UnicodeEncodeError exactly as a real encoder would
        return super().write(text)


class Broken(io.StringIO):
    def __init__(self, error: BaseException) -> None:
        super().__init__()
        self.error = error

    def write(self, text: str, /) -> int:
        raise self.error


def test_lines_reach_a_stream_without_a_descriptor_in_order() -> None:
    out = io.StringIO()
    writer = BoundedWriter(out)
    for i in range(5):
        writer.write(f"line {i}")
    assert out.getvalue().splitlines() == [f"line {i}" for i in range(5)]
    assert writer.close()
    assert writer.lost == 0


def test_text_the_stream_cannot_encode_is_replaced_not_lost() -> None:
    out = Latin1Out()
    writer = BoundedWriter(out)
    writer.write("café ☃ done")
    [line] = out.getvalue().splitlines()
    assert line.startswith("caf") and line.endswith("done")
    assert not writer.reader_gone and writer.undelivered_total == 0


@pytest.mark.parametrize("error", [BrokenPipeError(), ValueError("I/O operation on closed file")])
def test_a_reader_that_left_is_announced_once_and_later_lines_are_not_written(
    error: BaseException,
) -> None:
    calls: list[int] = []
    writer = BoundedWriter(Broken(error), on_closed=lambda: calls.append(1))
    writer.write("first")
    writer.write("second")
    assert writer.reader_gone and calls == [1]


def test_a_stream_that_fails_for_another_reason_is_counted_and_recorded() -> None:
    error = OSError(errno.ENOSPC, "No space left on device")
    writer = BoundedWriter(Broken(error))
    writer.write("one")
    writer.write("two")
    assert writer.error is error and writer.reader_gone
    assert writer.undelivered_total >= 1


def test_a_callback_set_after_the_reader_left_runs_at_once() -> None:
    writer = BoundedWriter(Broken(BrokenPipeError()))
    writer.write("x")
    seen: list[int] = []
    writer.set_on_closed(lambda: seen.append(1))
    assert seen == [1]
    writer.set_on_closed(None)  # clearing it is harmless


def test_a_closed_standard_stream_counts_every_line_as_undelivered() -> None:
    writer = BoundedWriter(None)
    assert writer.unavailable
    writer.write("nobody is listening")
    writer.write("still nobody")
    assert writer.undelivered_total == 2
    assert writer.close()


def test_write_wait_on_a_descriptorless_stream_writes_directly() -> None:
    out = io.StringIO()
    writer = BoundedWriter(out)
    assert writer.write_wait("kept") is True
    assert out.getvalue() == "kept\n"
    gone = BoundedWriter(Broken(BrokenPipeError()))
    assert gone.write_wait("lost") is False
