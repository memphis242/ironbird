"""``signals_held``: a signal that arrives inside the scope runs its handler as the scope ends,
and the mask is what it was before, whatever the handler raises and wherever."""

from __future__ import annotations

import signal
import threading
from collections.abc import Iterable, Iterator

import pytest

from ironbird.sigmask import HANDLED_SIGNALS, signals_held

pytestmark = pytest.mark.skipif(
    threading.current_thread() is not threading.main_thread(),
    reason="signal handlers can only be set in the main thread",
)


class _Raised(BaseException):
    pass


def _mask() -> set[int]:
    return set(signal.pthread_sigmask(signal.SIG_BLOCK, ()))


@pytest.fixture
def raising_hup() -> Iterator[list[int]]:
    """SIGHUP runs a handler that records it and raises :class:`_Raised`."""
    seen: list[int] = []

    def handler(signum: int, _frame: object) -> None:
        seen.append(signum)
        raise _Raised

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
            raise _Raised
        return previous

    before = _mask()
    monkeypatch.setattr(signal, "pthread_sigmask", blocking_then_raising)
    ran = False
    with pytest.raises(_Raised), signals_held():
        ran = True
    monkeypatch.undo()
    assert not ran and _mask() == before
