"""The termination signals, where the main thread acts on them, and what a signal replaced.

The CLI handles SIGINT, SIGTERM and SIGHUP in the main thread. Outside a running event loop its
handler never raises: it calls :func:`deliver`, which records the signal as *owed*, and the code
raises the owed :class:`Interrupted` itself, at defined points only:

- :func:`checkpoint`, called by every loop that can run long (each record a reader reads, each
  block a copy writes, each member a directory export syncs, each session a prune deletes);
- every wait, in slices of :data:`WAIT_SLICE_S` with a checkpoint between two (:func:`sleep`,
  :func:`wait_readable`, the wait for room on stdout, the drain at exit), so no wait of the
  main thread outlasts a signal by more than a slice, whenever the signal arrives: one that
  lands just before a wait begins, too late for the system call to be interrupted, is seen as
  the slice ends;
- the end of a :func:`signals_held` scope, and :meth:`Hold.begin` (the commit point of an
  output: a signal that came before it ends the work there, so nothing is published).

So the exception of a signal is never swallowed (Python only prints what a handler raises inside
a weakref callback, a ``__del__`` or an unraisable hook), and it never lands between two lines
that were not written to take it. Every thread the tool starts blocks these signals
(:func:`start_thread`), so they only ever reach the main thread.

A :func:`signals_held` scope defers the owed signal to its end: a step that changes the file
system and records the change (a temporary created and the name its removal goes by; a rename
and the note that it happened) runs in one, so no signal comes between the two. A
:class:`Hold` begins at one point of a ``with`` block and lasts to its end, for work whose tail
must not be cut short once it has begun (a published output, its record and its line).

A signal acted on as a held scope ends, while an exception is leaving that scope, takes that
exception's place: :attr:`Interrupted.replaced` names it, so the failure can still be reported.
That is the one way a signal replaces an error here; an exception that code was handling on
purpose when a signal was acted on (a retry that waits in its ``except`` block) is not named,
because it was not leaving a held scope. A step's raw failure that code one level up maps to its
own error is named as that error: that code wraps itself in :func:`naming_replaced`.
"""

from __future__ import annotations

import contextlib
import select
import signal
import threading
import time
from collections.abc import Callable, Generator
from types import TracebackType
from typing import Final, NoReturn

HANDLED_SIGNALS: Final = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
WAIT_SLICE_S: Final = 0.05  # no wait of the main thread outlasts a signal by more than this
WAIT_SLICE_MS: Final = int(WAIT_SLICE_S * 1000)


class Interrupted(BaseException):
    """The owed signal, raised where the main thread acts on it (see the module).

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


class _Owed:
    """The signal the main thread has yet to act on (0: none). Set by :func:`deliver` from the
    handler, taken by the main thread as it acts on it; both run in the main thread."""

    __slots__ = ("signum",)

    def __init__(self) -> None:
        self.signum = 0


_DEPTH: Final = _Depth()
_OWED: Final = _Owed()
_MAIN_IDENT: Final = threading.main_thread().ident


def deliver(signum: int) -> None:
    """Owe the main thread an :class:`Interrupted` for ``signum``; for a signal handler, which
    returns at once: the main thread acts on the signal at its next checkpoint (see the module).
    A signal already owed stays the one owed."""
    if not _OWED.signum:
        _OWED.signum = signum


def owed() -> int:
    """The signal owed and not yet acted on (0: none)."""
    return _OWED.signum


def discard() -> int:
    """Forget the owed signal and return it (0: none): from here it is the caller's to report."""
    signum, _OWED.signum = _OWED.signum, 0
    return signum


def _raise_owed(replaced: BaseException | None) -> NoReturn:
    signum, _OWED.signum = _OWED.signum, 0
    assert signum, "a signal is owed"
    stop = Interrupted(signum)
    stop.replaced = replaced
    raise stop


def checkpoint() -> None:
    """Act on the owed signal here: raise its :class:`Interrupted`, unless a held scope is open
    (then it is acted on as the outermost one ends) or this is not the main thread (which acts
    on every signal). Cheap enough for a loop that runs once per record."""
    if _OWED.signum and not _DEPTH.open and threading.get_ident() == _MAIN_IDENT:
        _raise_owed(None)


def sleep(seconds: float) -> None:
    """Sleep ``seconds``, in slices of :data:`WAIT_SLICE_S`: an owed signal is acted on before
    the first and after each (see :func:`checkpoint`)."""
    deadline = time.monotonic() + seconds
    while True:
        checkpoint()
        left = deadline - time.monotonic()
        if left <= 0:
            return
        time.sleep(min(left, WAIT_SLICE_S))


def wait_readable(fd: int) -> None:
    """Wait until ``fd`` has data to read, or its writer hung up (a terminal, a FIFO), in
    slices of :data:`WAIT_SLICE_S`: an owed signal is acted on before the first and after each.
    """
    poller = select.poll()
    poller.register(fd, select.POLLIN | select.POLLPRI)
    while True:
        checkpoint()
        if poller.poll(WAIT_SLICE_MS):
            return


@contextlib.contextmanager
def signals_held(*, unwinding: BaseException | None = None) -> Generator[None]:
    """Defer the owed signal to the end of the scope.

    No :func:`checkpoint` inside raises (and a wait inside runs to its end); as the outermost
    scope of the main thread ends, a signal owed by then is acted on, so its
    :class:`Interrupted` comes out of the ``with`` statement. Entering a scope never raises, so a
    cleanup that opens one always runs. Scopes nest and end in the reverse order they began
    (asserted), since an inner end that came last would lift the outer one.

    An :class:`Interrupted` raised as the scope ends records in ``replaced`` the exception that
    was leaving the scope: the one its body raised, else ``unwinding``, which a cleanup that runs
    for an exception (an ``__exit__``) passes, since that exception is on its way out too.
    """
    outer = _DEPTH.open
    _DEPTH.open = outer + 1
    replacing = unwinding
    try:
        yield
    except BaseException as exc:
        replacing = exc
        raise
    finally:
        ordered = _DEPTH.open == outer + 1
        _DEPTH.open = outer  # the depth first, so a failed assert leaves it right
        assert ordered, "held scopes end in the reverse order they began"
        if not outer and _OWED.signum and threading.get_ident() == _MAIN_IDENT:
            _raise_owed(replacing)


@contextlib.contextmanager
def masked() -> Generator[None]:
    """Block :data:`HANDLED_SIGNALS` in this thread for the block: a signal that arrives waits,
    and its handler runs as the block ends. A thread started inside inherits the block for good.
    """
    if not hasattr(signal, "pthread_sigmask"):  # pragma: no cover - not POSIX
        yield
        return
    previous = signal.pthread_sigmask(signal.SIG_BLOCK, HANDLED_SIGNALS)
    try:
        yield
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, previous)


def start_thread(thread: threading.Thread) -> None:
    """Start a helper thread with the shutdown signals blocked, so it never receives one: they are
    handled by the main thread, which must see them all."""
    with masked():
        thread.start()


@contextlib.contextmanager
def naming_replaced(
    name: Callable[[BaseException], BaseException | None],
) -> Generator[None]:
    """Let the code that maps a raw failure to its error name it for a signal too.

    A step that runs in a held scope raises its raw failure (an ``OSError``) out of the scope,
    and the code one level up maps it to the error the user is shown. A signal acted on as the
    scope ends takes the failure's place before that code sees it, so an :class:`Interrupted`
    leaving this block has its :attr:`~Interrupted.replaced` passed through ``name``, which
    returns the exception the failure would have become without the signal (``None``: it would
    not have been an error, or none to report). ``name`` returns an exception it does not
    know unchanged, and does not raise.
    """
    try:
        yield
    except Interrupted as stop:
        if stop.replaced is not None:
            stop.replaced = name(stop.replaced)
        raise


class Hold:
    """Hold the owed signal from :meth:`begin` to the end of the ``with`` block.

    ``with Hold() as hold:`` runs as usual until ``hold.begin()``, which is a commit point: a
    signal owed by then is acted on there, before anything is held. From there on a signal waits
    and is acted on as the block ends, after everything the block does past that point. A block
    that never begins holds nothing. ``begin`` is called where no held scope is open that was
    not open as the block began (asserted): such a scope would end first and lift this hold.
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
        """Hold the owed signal from here on (a no-op once begun). A signal owed already is
        acted on here, before anything is held: what it raises comes out of this call."""
        assert self._depth >= 0, "begin inside the with block"
        if self._scope is not None:
            return
        assert _DEPTH.open == self._depth, "a Hold begins outside every inner held scope"
        checkpoint()
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
            scope.__exit__(exc_type, exc, tb)  # an owed signal is acted on here
