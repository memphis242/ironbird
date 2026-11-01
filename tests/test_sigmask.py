"""The owed signal: a handler that calls ``deliver`` never raises; the main thread acts on the
signal at a ``checkpoint``, in the next slice of a wait, as the outermost ``signals_held`` scope
ends, or at ``Hold.begin``, and the exception names the one it replaced. A signal whose handler
runs where Python would swallow an exception (a weakref callback, an unraisable hook) is acted
on all the same."""

from __future__ import annotations

import os
import signal
import sys
import threading
import time
import weakref
from collections.abc import Iterator
from pathlib import Path

import pytest

from ironbird import fsops, sigmask
from ironbird.sigmask import (
    HANDLED_SIGNALS,
    Hold,
    Interrupted,
    checkpoint,
    deliver,
    naming_replaced,
    signals_held,
)

pytestmark = pytest.mark.skipif(
    threading.current_thread() is not threading.main_thread(),
    reason="signal handlers can only be set in the main thread",
)


@pytest.fixture
def delivering() -> Iterator[list[int]]:
    """SIGHUP runs a handler that records it and delivers it, as the CLI's does."""
    seen: list[int] = []

    def handler(signum: int, _frame: object) -> None:
        seen.append(signum)
        deliver(signum)

    previous = signal.signal(signal.SIGHUP, handler)
    try:
        yield seen
    finally:
        signal.signal(signal.SIGHUP, previous)
        sigmask.discard()


def _hup() -> None:
    signal.pthread_kill(threading.get_ident(), signal.SIGHUP)


def _hup_later(delay_s: float) -> threading.Timer:
    """SIGHUP to this (the main) thread from another one, ``delay_s`` from now."""
    main = threading.get_ident()
    timer = threading.Timer(delay_s, lambda: signal.pthread_kill(main, signal.SIGHUP))
    timer.start()
    return timer


def test_the_handler_never_raises_and_a_checkpoint_acts_on_the_signal(
    delivering: list[int],
) -> None:
    _hup()
    assert delivering == [signal.SIGHUP] and sigmask.owed() == signal.SIGHUP
    with pytest.raises(Interrupted) as caught:
        checkpoint()
    assert caught.value.signum == signal.SIGHUP and caught.value.replaced is None
    assert sigmask.owed() == 0
    checkpoint()  # acted on once


def test_a_signal_inside_the_scope_is_acted_on_as_the_outermost_scope_ends(
    delivering: list[int],
) -> None:
    steps: list[str] = []
    with pytest.raises(Interrupted), signals_held():
        _hup()
        checkpoint()  # held: nothing raised here
        with signals_held():  # nested: the inner end leaves it held
            pass
        steps.append("after")
    assert steps == ["after"] and delivering == [signal.SIGHUP] and sigmask.owed() == 0


def test_entering_a_held_scope_never_raises_so_a_cleanup_always_runs(
    delivering: list[int],
) -> None:
    _hup()
    ran = False
    with pytest.raises(Interrupted), signals_held():
        ran = True
    assert ran


def test_the_signal_names_the_failure_leaving_the_scope_and_nothing_else(
    delivering: list[int],
) -> None:
    failure = ValueError("the step failed")
    with pytest.raises(Interrupted) as caught, signals_held():
        _hup()
        raise failure
    assert caught.value.replaced is failure

    with pytest.raises(Interrupted) as caught, signals_held():
        _hup()
    assert caught.value.replaced is None

    with pytest.raises(Interrupted) as caught:  # handled on purpose when the signal is acted on
        try:
            raise failure
        except ValueError:
            _hup()
            sigmask.sleep(0)  # a retry's wait in its except block
    assert caught.value.replaced is None and caught.value.__context__ is failure


def test_a_cleanup_names_the_exception_it_runs_for(delivering: list[int]) -> None:
    failure = OSError("the commit failed")
    with pytest.raises(Interrupted) as caught, signals_held(unwinding=failure):
        _hup()
    assert caught.value.replaced is failure


def test_a_hold_holds_from_begin_to_the_end_of_its_block(delivering: list[int]) -> None:
    with pytest.raises(Interrupted), Hold():
        _hup()
        checkpoint()  # not begun: acted on at once
        raise AssertionError("not reached")

    steps: list[str] = []
    with pytest.raises(Interrupted) as caught, Hold() as hold:
        hold.begin()
        _hup()
        checkpoint()
        with signals_held():  # an inner scope that ends inside the hold does not lift it
            pass
        hold.begin()  # a no-op once begun
        steps.append("after")
        assert hold.begun
    assert steps == ["after"] and caught.value.replaced is None

    with Hold():  # never begun: nothing held, nothing raised
        pass


def test_a_signal_before_the_commit_point_is_acted_on_there_and_nothing_is_held(
    delivering: list[int],
) -> None:
    """``Hold.begin`` is the commit point: a signal owed by then ends the work before it."""
    published: list[str] = []
    with pytest.raises(Interrupted), Hold() as hold:
        _hup()  # arrives with no checkpoint between it and the commit
        hold.begin()
        published.append("output")
    assert published == [] and not hold.begun


def test_a_hold_names_the_failure_that_ends_its_block(delivering: list[int]) -> None:
    failure = RuntimeError("after the publish")
    with pytest.raises(Interrupted) as caught, Hold() as hold:
        hold.begin()
        _hup()
        raise failure
    assert caught.value.replaced is failure


def test_a_hold_is_never_begun_inside_an_inner_held_scope() -> None:
    with Hold() as hold, signals_held(), pytest.raises(AssertionError):
        hold.begin()  # the inner scope would end first and lift the hold
    assert not hold.begun and sigmask._DEPTH.open == 0


@pytest.mark.skipif(not __debug__, reason="the order is asserted")
def test_scopes_ended_out_of_order_restore_the_depth_before_the_assertion_fails() -> None:
    with pytest.raises(AssertionError, match="reverse order"), signals_held():
        sigmask._DEPTH.open += 2  # as if scopes opened inside this one were still open
    assert sigmask._DEPTH.open == 0


def test_a_signal_that_replaces_a_failure_is_named_as_the_error_its_mapper_makes_of_it(
    delivering: list[int],
) -> None:
    raw = OSError("raw")
    mapped = ValueError("mapped")

    def name(replaced: BaseException) -> BaseException | None:
        return mapped if replaced is raw else None

    with pytest.raises(Interrupted) as caught, naming_replaced(name), signals_held():
        _hup()
        raise raw
    assert caught.value.replaced is mapped

    with pytest.raises(Interrupted) as caught, naming_replaced(name), signals_held():
        _hup()
        raise ValueError("another")
    assert caught.value.replaced is None  # the mapper's answer: nothing to report


def test_naming_replaced_leaves_alone_what_replaced_nothing_and_ordinary_failures(
    delivering: list[int],
) -> None:
    def name(_replaced: BaseException) -> BaseException | None:
        raise AssertionError("asked about a signal that replaced nothing")

    with pytest.raises(Interrupted) as caught, naming_replaced(name), signals_held():
        _hup()
    assert caught.value.replaced is None
    with pytest.raises(KeyError), naming_replaced(name):
        raise KeyError("not a signal")


# -- waits -----------------------------------------------------------------------------------


def test_a_signal_ends_a_sleep_within_a_slice(delivering: list[int]) -> None:
    timer = _hup_later(0.05)
    started = time.monotonic()
    try:
        with pytest.raises(Interrupted) as caught:
            sigmask.sleep(60)
    finally:
        timer.cancel()
    assert caught.value.signum == signal.SIGHUP and time.monotonic() - started < 30
    assert sigmask.owed() == 0


def test_a_signal_that_lands_just_before_a_wait_begins_is_seen_as_its_slice_ends(
    delivering: list[int],
) -> None:
    """The handler ran before the wait's system call began, so that call is not interrupted
    (here: the signal is recorded from another thread while the poll is under way): the wait
    still ends, at the end of its slice, never later."""
    read_fd, write_fd = os.pipe()
    try:
        timer = threading.Timer(0.001, sigmask.deliver, (int(signal.SIGHUP),))
        timer.start()
        try:
            with pytest.raises(Interrupted):
                sigmask.wait_readable(read_fd)  # nothing is ever written
        finally:
            timer.join()
    finally:
        os.close(read_fd)
        os.close(write_fd)


def test_a_signal_ends_the_wait_on_a_fifo_nobody_writes(
    delivering: list[int], tmp_path: Path
) -> None:
    """A FIFO named as a configuration or definition file has no data until a writer comes; a
    signal ends that wait."""
    fifo = tmp_path / "config.toml"
    os.mkfifo(fifo)
    timer = _hup_later(0.05)
    try:
        with pytest.raises(Interrupted):
            fsops.read_capped(fifo, 1024)
    finally:
        timer.cancel()


def test_a_capped_read_of_a_fifo_waits_for_its_writer_and_reads_to_its_end(
    tmp_path: Path,
) -> None:
    fifo = tmp_path / "config.toml"
    os.mkfifo(fifo)

    def write_later() -> None:
        with fifo.open("wb") as writer:  # blocks until the reader has opened the FIFO
            writer.write(b"[a]\n")
            writer.flush()
            time.sleep(0.05)  # a writer that is not done at once
            writer.write(b"b = 1\n")

    thread = threading.Thread(target=write_later)
    thread.start()
    try:
        assert fsops.read_capped(fifo, 1024) == b"[a]\nb = 1\n"
    finally:
        thread.join(30)


def test_a_capped_read_gives_what_open_and_read_give(tmp_path: Path) -> None:
    path = tmp_path / "f"
    path.write_bytes(b"x" * 3000)
    assert fsops.read_capped(path, 5000) == b"x" * 3000
    assert fsops.read_capped(path, 2999) == b"x" * 3000  # one byte past the cap: too large
    assert fsops.read_capped(path, 10) == b"x" * 11
    with pytest.raises(IsADirectoryError):
        fsops.read_capped(tmp_path, 10)
    with pytest.raises(FileNotFoundError):
        fsops.read_capped(tmp_path / "missing", 10)


def test_an_owed_signal_is_acted_on_before_a_wait_starts(delivering: list[int]) -> None:
    _hup()
    started = time.monotonic()
    with pytest.raises(Interrupted):
        sigmask.sleep(60)
    assert time.monotonic() - started < sigmask.WAIT_SLICE_S


def test_inside_a_held_scope_a_wait_runs_to_its_end(delivering: list[int]) -> None:
    timer = _hup_later(0.01)
    try:
        with pytest.raises(Interrupted), signals_held():
            deadline = time.monotonic() + 30
            while not delivering and time.monotonic() < deadline:
                sigmask.sleep(0.01)  # held: the signal does not end it
            assert delivering, "the signal arrived"
    finally:
        timer.cancel()


# -- where Python swallows exceptions --------------------------------------------------------


class _Garbage:
    pass


def _signal_inside_a_weakref_callback() -> None:
    garbage = _Garbage()
    ref = weakref.ref(garbage, lambda _ref: _hup())
    del garbage  # the callback runs here, and the handler inside it
    assert ref() is None


def test_a_signal_handled_inside_a_weakref_callback_is_not_lost(
    delivering: list[int], capfd: pytest.CaptureFixture[str]
) -> None:
    _signal_inside_a_weakref_callback()
    assert delivering == [signal.SIGHUP] and sigmask.owed() == signal.SIGHUP
    with pytest.raises(Interrupted):
        checkpoint()
    assert "Exception ignored" not in capfd.readouterr().err


def test_a_signal_handled_inside_an_unraisable_hook_is_not_lost(
    delivering: list[int], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The handler runs while a Python-level ``sys.unraisablehook`` reports an unrelated
    unraisable exception (a library's hook, or pytest's): nothing is raised there to lose."""
    reported: list[type[BaseException] | None] = []

    def hook(unraisable: sys.UnraisableHookArgs) -> None:
        _hup()
        reported.append(unraisable.exc_type)

    monkeypatch.setattr(sys, "unraisablehook", hook)

    def fails(_ref: object) -> None:
        raise ValueError("unrelated")

    garbage = _Garbage()
    ref = weakref.ref(garbage, fails)
    del garbage
    assert ref() is None and reported == [ValueError]
    with pytest.raises(Interrupted):
        checkpoint()


# -- threads ---------------------------------------------------------------------------------


def test_another_thread_neither_raises_nor_takes_the_signal(delivering: list[int]) -> None:
    _hup()
    outcome: list[str] = []

    def other() -> None:
        checkpoint()
        with signals_held():
            pass
        outcome.append("ran")

    thread = threading.Thread(target=other)
    sigmask.start_thread(thread)
    thread.join(30)
    assert outcome == ["ran"] and sigmask.owed() == signal.SIGHUP
    with pytest.raises(Interrupted):
        checkpoint()


def _blocked() -> set[int]:
    return set(signal.pthread_sigmask(signal.SIG_BLOCK, ()))


def test_a_started_thread_blocks_the_signals_and_the_caller_keeps_its_mask() -> None:
    before = _blocked()
    seen: list[set[int]] = []
    thread = threading.Thread(target=lambda: seen.append(_blocked()))
    sigmask.start_thread(thread)
    thread.join(30)
    assert set(HANDLED_SIGNALS) <= seen[0] and _blocked() == before
