"""``retention-notices.json``: when each start-of-session warning was last shown, per code and
directory, and when retention last scanned - so a script of a thousand ``ib send`` runs shows a
warning once per ``warn_interval`` and scans once per ``scan_interval``.

The file lives in the state directory and is *untrusted* on read: opened ``O_NOFOLLOW |
O_NONBLOCK`` and checked with ``fstat`` (regular, ours, one link), at most 4 KiB, strict schema,
timestamps not in the future; anything else is ignored and rewritten. It is rewritten in place
(``pwrite`` at 0, then ``ftruncate``) under ``flock``, so no temporary file exists; a rewrite
torn by a crash reads as invalid and is rewritten, at worst repeating one warning.

A lock never causes silence: when it is held for more than 200 ms the warning is displayed and
the file is left unwritten. Where locks are unsupported (``ENOLCK``, ``EINVAL``,
``EOPNOTSUPP``) the file is read and rewritten without one - a race can at worst repeat one
warning or one scan.
"""

from __future__ import annotations

import errno
import fcntl
import json
import os
import stat
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, Final

from ironbird.fsops import (
    INSPECT_RW_FLAGS,
    REAL_FS,
    FileSystem,
    close_quietly,
)

FILE_NAME: Final = "retention-notices.json"
MAX_BYTES: Final = 4096
MAX_ENTRIES: Final = 32
MAX_KEY_LEN: Final = 200
LOCK_TRIES: Final = 20  # x 10 ms: the 200 ms bound
LOCK_STEP_S: Final = 0.01
SCHEMA: Final = 1
_NO_LOCK: Final = frozenset({errno.ENOLCK, errno.EINVAL, errno.EOPNOTSUPP, errno.EBADF})
_OPEN_FLAGS: Final = INSPECT_RW_FLAGS | os.O_CREAT


def _number(value: object, now: float) -> float | None:
    """A timestamp: a finite number that is not a bool and not in the future."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    if value != value or value in (float("inf"), float("-inf")):  # noqa: PLR0124 - NaN test
        return None
    if value < 0 or value > now + 1.0:
        return None
    return float(value)


def parse_notices(raw: bytes, now: float) -> tuple[dict[str, float], float | None]:
    """The state a file holds, or an empty one for anything that is not exactly the schema:
    ``{"v": 1, "last_scan": ts | null, "shown": {"<code>@<dir>": ts, ...}}``."""
    if not raw or len(raw) > MAX_BYTES:
        return {}, None
    try:
        data = json.loads(raw.decode("utf-8"), parse_constant=_refuse)
    except (ValueError, RecursionError):
        return {}, None
    if not isinstance(data, dict) or set(data) != {"v", "last_scan", "shown"}:
        return {}, None
    shown_raw = data["shown"]
    if data["v"] != SCHEMA or not isinstance(shown_raw, dict) or len(shown_raw) > MAX_ENTRIES:
        return {}, None
    shown: dict[str, float] = {}
    for key, value in shown_raw.items():
        stamp = _number(value, now)
        if not isinstance(key, str) or len(key) > MAX_KEY_LEN or stamp is None:
            return {}, None
        shown[key] = stamp
    last = data["last_scan"]
    if last is not None and _number(last, now) is None:
        return {}, None
    return shown, None if last is None else float(last)


def _refuse(token: str) -> object:
    raise ValueError(f"non-standard JSON constant {token}")


def render_notices(shown: Mapping[str, float], last_scan: float | None) -> bytes:
    """The file's content; the oldest entries go first when there are too many."""
    kept = dict(sorted(shown.items(), key=lambda item: item[1])[-MAX_ENTRIES:])
    text = json.dumps({"v": SCHEMA, "last_scan": last_scan, "shown": kept}, separators=(",", ":"))
    while len(text) + 1 > MAX_BYTES and kept:
        kept.pop(next(iter(kept)))
        text = json.dumps(
            {"v": SCHEMA, "last_scan": last_scan, "shown": kept}, separators=(",", ":")
        )
    return (text + "\n").encode("ascii")


class Notices:
    """Read-modify-write access to the notices file of one state directory."""

    def __init__(
        self,
        directory: Path,
        *,
        fs: FileSystem = REAL_FS,
        wall: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._directory = directory
        self._fs = fs
        self._wall = wall
        self._sleep = sleep
        self.unlocked = False  # the last access ran without a lock (unsupported here)

    # -- one transaction ------------------------------------------------------------------

    def _transaction(
        self,
        change: Callable[
            [dict[str, float], float | None], tuple[dict[str, float], float | None, Any]
        ],
    ) -> tuple[Any, bool]:
        """Run ``change(shown, last_scan) -> (shown, last_scan, result)``; returns ``(result,
        written)``. ``written`` is ``False`` when the file could not be used or its lock was
        held past the bound: the change still happened in memory and the caller acts on it."""
        fs = self._fs
        now = self._wall()
        try:
            dir_fd = fs.open_dir(self._directory)
        except OSError:
            shown, last, result = change({}, None)[0:3]
            return result, False
        fd = -1
        try:
            fd = fs.open_at(dir_fd, FILE_NAME, _OPEN_FLAGS, 0o600)
            st = fs.fstat(fd)
            if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() or st.st_nlink != 1:
                return change({}, None)[2], False
            if not self._lock(fd):
                return change({}, None)[2], False  # held: display, do not write
            raw = fs.pread(fd, MAX_BYTES + 1, 0)
            shown, last = parse_notices(raw, now)
            shown, last, result = change(shown, last)
            data = render_notices(shown, last)
            fs.pwrite(fd, data, 0)
            fs.ftruncate(fd, len(data))  # the rewrite helper: in place, no temporary file
            return result, True
        except OSError:
            return change({}, None)[2], False
        finally:
            close_quietly(fs, fd)
            close_quietly(fs, dir_fd)

    def _lock(self, fd: int) -> bool:
        self.unlocked = False
        for _ in range(LOCK_TRIES):
            try:
                self._fs.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return True
            except OSError as exc:
                if exc.errno in (errno.EWOULDBLOCK, errno.EAGAIN):
                    self._sleep(LOCK_STEP_S)
                    continue
                if exc.errno in _NO_LOCK:
                    self.unlocked = True
                    return True  # read and rewritten without the lock
                return False
        return False

    # -- the two questions -----------------------------------------------------------------

    def allow(self, code: str, directory: Path, interval_s: float) -> bool:
        """Whether ``code`` may be displayed for ``directory`` now (and note that it was).
        ``True`` also when the file cannot be used: a lock never causes silence."""
        key = f"{code}@{directory}"[:MAX_KEY_LEN]
        now = self._wall()

        def change(shown: dict[str, float], last: float | None) -> tuple[Any, ...]:
            before = shown.get(key)
            if before is not None and now - before < interval_s:
                return shown, last, False
            shown[key] = now
            return shown, last, True

        result, _written = self._transaction(change)
        return bool(result)

    def scan_due(self, interval_s: float) -> bool:
        """Whether the last scan by any process is older than ``interval_s`` - and, if so,
        claim it (note a scan as started now), so two racing processes scan once."""
        now = self._wall()

        def change(shown: dict[str, float], last: float | None) -> tuple[Any, ...]:
            if last is not None and now - last < interval_s:
                return shown, last, False
            return shown, now, True

        result, _written = self._transaction(change)
        return bool(result)
