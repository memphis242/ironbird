"""The termination signals and a scope that holds them off.

The CLI handles SIGINT, SIGTERM and SIGHUP in the main thread; outside a running event loop its
handler raises, so the exception can land between any two lines. Every thread the tool starts
inherits a mask that blocks these signals, so they only ever reach the main thread, and a scope
of :func:`signals_held` there turns a signal into a pending one that runs the handler only when
the scope ends. A step that makes something and records it for cleanup (a temporary file and the
name its removal goes by) does both inside one such scope, so no signal can come between them.
"""

from __future__ import annotations

import contextlib
import signal
from collections.abc import Generator
from typing import Final

HANDLED_SIGNALS: Final = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)


@contextlib.contextmanager
def signals_held() -> Generator[None]:
    """Block :data:`HANDLED_SIGNALS` in this thread; threads started inside inherit the mask.

    A blocked signal waits as pending (see ``signal.sigtimedwait``) instead of running a handler
    at an arbitrary line; its handler runs as the scope ends, so whatever the handler raises
    comes out of the ``with`` statement. The previous mask is restored on exit, so uses nest.

    ``pthread_sigmask`` runs the handlers of signals already caught before it returns, so the
    call that blocks can raise after it changed the mask: the mask is read first and the block
    made inside the ``try``, so the mask is restored whatever is raised, and the body does not
    run when the block raised.
    """
    if not hasattr(signal, "pthread_sigmask"):  # pragma: no cover - not POSIX
        yield
        return
    previous = signal.pthread_sigmask(signal.SIG_BLOCK, ())  # reads the mask, changes nothing
    try:
        signal.pthread_sigmask(signal.SIG_BLOCK, HANDLED_SIGNALS)
        yield
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, previous)
