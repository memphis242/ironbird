"""The termination signals, the scopes that hold them off, and what a held signal replaced.

The CLI handles SIGINT, SIGTERM and SIGHUP in the main thread; outside a running event loop its
handler raises an :class:`Interrupted`, so the exception can land between any two lines. Every
thread the tool starts inherits a mask that blocks these signals, so they only ever reach the
main thread, and a scope of :func:`signals_held` there turns a signal into a pending one whose
handler runs only as the scope ends. Two uses:

- a step that changes the file system and records the change (a temporary created and the name
  its removal goes by; a rename and the note that it happened) runs in one such scope, so no
  signal comes between the two;
- a :class:`Hold` begins at one point of a ``with`` block and lasts to its end, for work whose
  tail must not be cut short once it has begun (a published output, its record and its line).

A held signal that is acted on as a scope ends, while an exception is leaving that scope, takes
that exception's place: :attr:`Interrupted.replaced` names it, so the failure can still be
reported. That is the one way a signal replaces an error here; an exception that code was
handling on purpose when a signal arrived (a retry that waits in its ``except`` block) is not
named, because it was not leaving a held scope.
"""

from __future__ import annotations

import contextlib
import signal
import threading
from collections.abc import Generator, Iterable
from types import TracebackType
from typing import Final

HANDLED_SIGNALS: Final = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)


class Interrupted(BaseException):
    """What a handler of :data:`HANDLED_SIGNALS` raises to end the work.

    ``replaced``: the exception that was leaving the held scope at whose end this one was
    raised, which it replaced (``None``: it replaced nothing). Set by :func:`signals_held` only.
    """

    def __init__(self, signum: int) -> None:
        super().__init__(signum)
        self.signum = signum
        self.replaced: BaseException | None = None


class _Depth(threading.local):
    """How many :func:`signals_held` scopes are open in this thread."""

    open = 0


_DEPTH: Final = _Depth()


@contextlib.contextmanager
def signals_held(*, unwinding: BaseException | None = None) -> Generator[None]:
    """Block :data:`HANDLED_SIGNALS` in this thread; threads started inside inherit the mask.

    A blocked signal waits as pending (see ``signal.sigtimedwait``) instead of running a handler
    at an arbitrary line; its handler runs as the scope ends, so whatever the handler raises
    comes out of the ``with`` statement. The previous mask is restored on exit; scopes nest and
    end in the reverse order they began (asserted), since an inner end that came last would lift
    the outer hold.

    An :class:`Interrupted` raised as the scope ends records in ``replaced`` the exception that
    was leaving the scope: the one its body raised, else ``unwinding``, which a cleanup that runs
    for an exception (an ``__exit__``) passes, since that exception is on its way out too.

    ``pthread_sigmask`` runs the handlers of signals already caught before it returns, so the
    call that blocks can raise after it changed the mask: the mask is read first and the block
    made inside the ``try``, so the mask is restored whatever is raised, and the body does not
    run when the block raised.
    """
    if not hasattr(signal, "pthread_sigmask"):  # pragma: no cover - not POSIX
        yield
        return
    outer = _DEPTH.open
    replacing = unwinding
    previous = signal.pthread_sigmask(signal.SIG_BLOCK, ())  # reads the mask, changes nothing
    try:
        signal.pthread_sigmask(signal.SIG_BLOCK, HANDLED_SIGNALS)
        _DEPTH.open = outer + 1
        try:
            yield
        except BaseException as exc:
            replacing = exc
            raise
    finally:
        assert _DEPTH.open in (outer, outer + 1), "held scopes end in the reverse order they began"
        _DEPTH.open = outer
        _restore(previous, replacing)


def _restore(previous: Iterable[int], replacing: BaseException | None) -> None:
    """Restore the mask ``previous``; an :class:`Interrupted` that a pending handler raises on
    the way names ``replacing`` as the exception it replaced."""
    try:
        signal.pthread_sigmask(signal.SIG_SETMASK, previous)
    except Interrupted as raised:
        if raised.replaced is None and replacing is not raised:
            raised.replaced = replacing
        raise


class Hold:
    """Hold :data:`HANDLED_SIGNALS` from :meth:`begin` to the end of the ``with`` block.

    ``with Hold() as hold:`` runs as usual until ``hold.begin()``; from there a signal waits,
    and its handler runs as the block ends, after everything the block does past that point.
    A block that never begins holds nothing. ``begin`` is called where no held scope is open
    that was not open as the block began (asserted): such a scope would end first and lift this
    hold with it.
    """

    __slots__ = ("_depth", "_scope")

    def __init__(self) -> None:
        self._depth = -1  # held scopes open as the block began; -1: not entered
        self._scope: contextlib.AbstractContextManager[None] | None = None

    def __enter__(self) -> Hold:
        assert self._depth < 0, "a Hold is used once"
        self._depth = _DEPTH.open
        return self

    @property
    def begun(self) -> bool:
        return self._scope is not None

    def begin(self) -> None:
        """Hold the signals from here on (a no-op once begun). A signal already caught runs its
        handler here, before anything is held, and what it raises comes out of this call."""
        assert self._depth >= 0, "begin inside the with block"
        if self._scope is not None:
            return
        assert _DEPTH.open == self._depth, "a Hold begins outside every inner held scope"
        scope = signals_held()
        scope.__enter__()
        self._scope = scope

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        scope, self._scope = self._scope, None
        if scope is not None:
            scope.__exit__(exc_type, exc, tb)  # a held signal's handler runs here
