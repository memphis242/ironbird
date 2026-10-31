"""``signals_held``: a signal that arrives inside the scope runs its handler as the scope ends,
and the mask is what it was before, whatever the handler raises and wherever; the exception it
raises names the one it replaced. ``Hold``: held from ``begin`` to the end of the block."""

from __future__ import annotations

import signal
import threading
from collections.abc import Iterable, Iterator

import pytest

from ironbird import sigmask
from ironbird.sigmask import HANDLED_SIGNALS, Hold, Interrupted, naming_replaced, signals_held

pytestmark = pytest.mark.skipif(
    threading.current_thread() is not threading.main_thread(),
    reason="signal handlers can only be set in the main thread",
)


class _Raised(Interrupted):
    pass


def _mask() -> set[int]:
    return set(signal.pthread_sigmask(signal.SIG_BLOCK, ()))


@pytest.fixture
def raising_hup() -> Iterator[list[int]]:
    """SIGHUP runs a handler that records it and raises :class:`_Raised`."""
    seen: list[int] = []

    def handler(signum: int, _frame: object) -> None:
        seen.append(signum)
        raise _Raised(signum)

    previous = signal.signal(signal.SIGHUP, handler)
    try:
        yield seen
    finally:
        signal.signal(signal.SIGHUP, previous)


def test_a_signal_inside_the_scope_runs_its_handler_as_the_scope_ends(
    raising_hup: list[int],
) -> None:
    before = _mask()
    with pytest.raises(_Raised), signals_held():
        assert set(HANDLED_SIGNALS) <= _mask()
        signal.pthread_kill(threading.get_ident(), signal.SIGHUP)
        with signals_held():  # nested: the inner end restores a mask that still holds it
            pass
        assert raising_hup == [], "held: the handler has not run"
    assert raising_hup == [signal.SIGHUP] and _mask() == before


def test_a_handler_that_raises_while_the_block_is_made_leaves_the_mask_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``pthread_sigmask`` runs the handler of a signal caught just before the call once it has
    changed the mask, and its exception comes out of the call: the mask must be restored and
    the body must not run."""
    real = signal.pthread_sigmask

    def blocking_then_raising(how: int, mask: Iterable[int]) -> set[int]:
        wanted = set(mask)
        previous = real(how, wanted)
        if how == signal.SIG_BLOCK and wanted:
            raise _Raised(signal.SIGHUP)
        return previous

    before = _mask()
    monkeypatch.setattr(signal, "pthread_sigmask", blocking_then_raising)
    ran = False
    with pytest.raises(_Raised), signals_held():
        ran = True
    monkeypatch.undo()
    assert not ran and _mask() == before


def _hup() -> None:
    signal.pthread_kill(threading.get_ident(), signal.SIGHUP)


def test_the_signal_names_the_failure_leaving_the_scope_and_nothing_else(
    raising_hup: list[int],
) -> None:
    failure = ValueError("the step failed")
    with pytest.raises(_Raised) as caught, signals_held():
        _hup()
        raise failure
    assert caught.value.replaced is failure

    with pytest.raises(_Raised) as caught, signals_held():
        _hup()
    assert caught.value.replaced is None

    with pytest.raises(_Raised) as caught:  # handled on purpose when the signal arrives
        try:
            raise failure
        except ValueError:
            _hup()
    assert caught.value.replaced is None


def test_a_cleanup_names_the_exception_it_runs_for(raising_hup: list[int]) -> None:
    failure = OSError("the commit failed")
    with pytest.raises(_Raised) as caught, signals_held(unwinding=failure):
        _hup()
    assert caught.value.replaced is failure


def test_a_hold_holds_from_begin_to_the_end_of_its_block(raising_hup: list[int]) -> None:
    before = _mask()
    with pytest.raises(_Raised), Hold() as hold:
        _hup()  # not begun: the handler runs at once
    assert raising_hup == [signal.SIGHUP] and _mask() == before
    raising_hup.clear()

    steps: list[str] = []
    with pytest.raises(_Raised) as caught, Hold() as hold:
        hold.begin()
        _hup()
        with signals_held():  # an inner scope that ends inside the hold does not lift it
            pass
        hold.begin()  # a no-op once begun
        steps.append("after")
        assert raising_hup == [] and hold.begun
    assert steps == ["after"] and raising_hup == [signal.SIGHUP] and _mask() == before
    assert caught.value.replaced is None

    with Hold():  # never begun: nothing held, nothing raised
        assert _mask() == before


def test_a_hold_names_the_failure_that_ends_its_block(raising_hup: list[int]) -> None:
    failure = RuntimeError("after the publish")
    with pytest.raises(_Raised) as caught, Hold() as hold:
        hold.begin()
        _hup()
        raise failure
    assert caught.value.replaced is failure


def test_a_hold_is_never_begun_inside_an_inner_held_scope() -> None:
    before = _mask()
    with Hold() as hold, signals_held(), pytest.raises(AssertionError):
        hold.begin()  # the inner scope would end first and lift the hold
    assert not hold.begun and _mask() == before


@pytest.mark.skipif(not __debug__, reason="the order is asserted")
def test_scopes_ended_out_of_order_restore_the_mask_before_the_assertion_fails() -> None:
    before = _mask()
    with pytest.raises(AssertionError, match="reverse order"), signals_held():
        sigmask._DEPTH.open += 2  # as if scopes opened inside this one were still open
    assert _mask() == before, "restored first: a failed assertion does not leave signals blocked"
    assert sigmask._DEPTH.open == 0


def test_a_signal_that_replaces_a_failure_is_named_as_the_error_its_mapper_makes_of_it(
    raising_hup: list[int],
) -> None:
    raw = OSError("raw")
    mapped = ValueError("mapped")

    def name(replaced: BaseException) -> BaseException | None:
        return mapped if replaced is raw else None

    with pytest.raises(_Raised) as caught, naming_replaced(name), signals_held():
        _hup()
        raise raw
    assert caught.value.replaced is mapped

    with pytest.raises(_Raised) as caught, naming_replaced(name), signals_held():
        _hup()
        raise ValueError("another")
    assert caught.value.replaced is None  # the mapper's answer: nothing to report


def test_naming_replaced_leaves_alone_what_replaced_nothing_and_ordinary_failures(
    raising_hup: list[int],
) -> None:
    def name(_replaced: BaseException) -> BaseException | None:
        raise AssertionError("asked about a signal that replaced nothing")

    with pytest.raises(_Raised) as caught, naming_replaced(name), signals_held():
        _hup()
    assert caught.value.replaced is None
    with pytest.raises(KeyError), naming_replaced(name):
        raise KeyError("not a signal")
