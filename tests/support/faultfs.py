"""A scriptable file system for the ledger's fault tests.

:class:`FaultFs` is the real :class:`~ironbird.fsops.FileSystem` with a list of rules in front of
it: a rule makes the next calls of one method fail with an errno, blocks them on an event (a hung
disk), or lets a write land only in part. Every call is recorded. Nothing in ``src/`` knows it
exists: the code under test receives it through its ``fs`` argument, and subprocess tests install
it with :mod:`support.faulty_main`.
"""

from __future__ import annotations

import errno
import os
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from ironbird.fsops import REAL_FS, FileSystem

METHODS = tuple(
    name
    for name in dir(FileSystem)
    if not name.startswith("_") and callable(getattr(FileSystem, name))
)


@dataclass(slots=True)
class Rule:
    method: str
    err: int | None = None  # raise OSError(err) when it fires
    exc: BaseException | None = None  # or raise this
    skip: int = 0  # let this many matching calls through first
    times: int | None = None  # fire this many times (None: for ever)
    when: Callable[..., bool] | None = None  # predicate over the call's arguments
    gate: threading.Event | None = None  # block until set, then run the real call
    entered: threading.Event = field(default_factory=threading.Event)
    partial: int | None = None  # write: land only this many bytes, then fail the next write
    fired: int = 0
    off: bool = False

    def matches(self, name: str, args: tuple[Any, ...]) -> bool:
        if self.off or name != self.method:
            return False
        return self.when is None or bool(self.when(*args))


class FaultFs(FileSystem):
    def __init__(self, real: FileSystem = REAL_FS) -> None:
        super().__init__()
        self.real = real
        self.rules: list[Rule] = []
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self._lock = threading.Lock()
        for name in METHODS:
            setattr(self, name, self._wrap(name, getattr(real, name)))

    def _wrap(self, name: str, real: Callable[..., Any]) -> Callable[..., Any]:
        def call(*args: Any, **kwargs: Any) -> Any:
            with self._lock:
                self.calls.append((name, args))
                rule = self._fire(name, args)
            if rule is not None:
                if rule.gate is not None:
                    rule.entered.set()
                    rule.gate.wait(30)
                    return real(*args, **kwargs)
                if rule.partial is not None and name in ("write", "pwrite"):
                    data = args[1]
                    kept = bytes(data[: rule.partial])
                    rule.partial = None
                    done = real(args[0], kept, *args[2:]) if kept else 0
                    self.rules.append(Rule(name, err=rule.err or errno.ENOSPC, times=1))
                    return done
                if rule.exc is not None:
                    raise rule.exc
                if rule.err is not None:
                    raise OSError(rule.err, os.strerror(rule.err))
            return real(*args, **kwargs)

        return call

    def _fire(self, name: str, args: tuple[Any, ...]) -> Rule | None:
        for rule in self.rules:
            if not rule.matches(name, args):
                continue
            if rule.skip > 0:
                rule.skip -= 1
                continue
            if rule.times is not None and rule.fired >= rule.times:
                continue
            rule.fired += 1
            return rule
        return None

    # -- scripting -------------------------------------------------------------------------

    def fail(self, method: str, err: int, **kwargs: Any) -> Rule:
        rule = Rule(method, err=err, **kwargs)
        self.rules.append(rule)
        return rule

    def block(self, method: str, **kwargs: Any) -> Rule:
        """Calls of ``method`` wait on ``rule.gate`` (set by the test) before running."""
        rule = Rule(method, gate=threading.Event(), **kwargs)
        self.rules.append(rule)
        return rule

    def override(self, method: str, func: Callable[..., Any]) -> None:
        """Replace ``method`` by ``func`` (still recorded in :attr:`calls`)."""

        def call(*args: Any, **kwargs: Any) -> Any:
            with self._lock:
                self.calls.append((method, args))
            return func(*args, **kwargs)

        setattr(self, method, call)

    def clear(self) -> None:
        for rule in self.rules:
            rule.off = True

    def count(self, method: str) -> int:
        return sum(1 for name, _ in self.calls if name == method)

    def names(self) -> list[str]:
        return [name for name, _ in self.calls]


class FakeClock:
    """A monotonic clock the test moves by hand."""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds
