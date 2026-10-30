"""Retention warnings: what a command tells the user about the disk it is filling.

A :class:`Retention` object runs the scans (in a worker thread, never on the event loop), turns
their results into ledger records and rate-limited warnings, and for a long-running session
re-scans every ``scan_interval`` and projects its own growth against ``max_total_bytes``.

Nothing here deletes anything: it warns (``IB-RET-001`` sessions older than ``max_age``,
``IB-RET-002`` ledger plus recordings past or projected past ``max_total_bytes``,
``IB-RET-004`` a partial scan or an unrecoverable reserve), and ``ib ledger prune`` /
``ib rec prune`` are the only deletion.

Every check is ledgered in full. The start-of-session warnings are displayed at most once per
``warn_interval`` per code and directory across processes (the notices file); the projection is
displayed once per process when it first crosses and once when usage itself crosses.
"""

from __future__ import annotations

import queue
import signal
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final

from ironbird.config import RetentionConfig
from ironbird.errors import Code
from ironbird.fsops import REAL_FS, FileSystem
from ironbird.ledger import Ledger
from ironbird.ledgerstore import SHUTDOWN_SIGNALS
from ironbird.retention import DirScan, full_scan, protected_bytes_by_reason, recording_blocks
from ironbird.retnotices import Notices
from ironbird.units import MIB, compact_duration, format_duration, format_size

PROJECTION_INTERVAL_S: Final = 10.0
RATE_WINDOW_S: Final = 60.0
DEFAULT_HORIZON_S: Final = 3600.0
JOIN_S: Final = 2.0


def size_argument(size: int) -> str:
    """``size`` as a ``--until-under`` argument in the size grammar (whole MiB, at least 1)."""
    mib = max(1, size // MIB)
    if mib % 1024 == 0:
        return f"{mib // 1024}GiB"
    return f"{mib}MiB"


@dataclass(slots=True)
class ScanResult:
    ledger: DirScan
    recordings: DirScan | None
    finished: float = 0.0


@dataclass(slots=True)
class _State:
    usage: int = 0
    usage_warned: bool = False
    projection_warned: bool = False
    last_scan: float = float("-inf")
    last_projection: float = float("-inf")
    own_bytes_at_scan: int = 0
    samples: deque[tuple[float, int]] = field(default_factory=deque)


class Retention:
    """The warnings and the periodic scans of one process."""

    def __init__(
        self,
        ledger: Ledger,
        config: RetentionConfig,
        state_dir: Path,
        recording_dir: Path | None,
        *,
        long_session: bool,
        notices: Notices,
        fs: FileSystem = REAL_FS,
        clock: Callable[[], float] = time.monotonic,
        wall: Callable[[], float] = time.time,
    ) -> None:
        self._ledger = ledger
        self._cfg = config
        self._state_dir = state_dir
        self._recording_dir = recording_dir
        self._long = long_session
        self._notices = notices
        self._fs = fs
        self._clock = clock
        self._wall = wall
        self._results: queue.SimpleQueue[ScanResult] = queue.SimpleQueue()
        self._thread: threading.Thread | None = None
        self._state = _State()
        self._deadline: float | None = None
        self._rate_sources: list[Callable[[], float]] = []
        self._last_ledger: DirScan | None = None
        self._last_recordings: DirScan | None = None
        self._pending = False

    # -- wiring -------------------------------------------------------------------------------

    def add_rate_source(self, bytes_per_second: Callable[[], float]) -> None:
        """A writer's current rate (the recorder's ``rate_bytes_per_s``) for the projection."""
        self._rate_sources.append(bytes_per_second)

    def set_deadline(self, seconds_from_now: float) -> None:
        """The command ends this many seconds from now (``--duration``): the projection's
        horizon. Without it the horizon is one hour."""
        self._deadline = self._clock() + seconds_from_now

    # -- scanning ----------------------------------------------------------------------------

    def start(self) -> None:
        """Begin the start-of-session scan: a long session always, a short command only when
        the last scan by any process is older than ``scan_interval``."""
        if self._long or self._notices.scan_due(self._cfg.scan_interval_s):
            self._begin_scan()

    def _begin_scan(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._state.last_scan = self._clock()
        self._pending = True
        previous = signal.pthread_sigmask(signal.SIG_BLOCK, SHUTDOWN_SIGNALS)
        try:
            self._thread = threading.Thread(target=self._scan, name="retention-scan", daemon=True)
            self._thread.start()
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous)

    def _scan(self) -> None:
        now = self._wall()
        own = self._ledger.session_id
        keep = self._cfg.keep_recent
        recordings: DirScan | None = None
        if self._recording_dir is not None:
            recordings = full_scan(
                self._fs,
                self._recording_dir,
                "recording",
                now=now,
                own_session=own,
                keep_recent=keep,
            )
        has_recording = recording_blocks(recordings, now, self._cfg.max_age_s)
        ledger_scan = full_scan(
            self._fs,
            self._state_dir,
            "ledger",
            now=now,
            own_session=own,
            keep_recent=keep,
            has_recording=has_recording,
        )
        self._results.put(ScanResult(ledger_scan, recordings, now))

    def poll(self) -> None:
        """Handle a finished scan, start the next when due, and check the projection. Called
        from the ledger's periodic upkeep on the event loop."""
        try:
            while True:
                self._handle(self._results.get_nowait())
        except queue.Empty:
            pass
        now = self._clock()
        if (
            self._long
            and not self._pending
            and now - self._state.last_scan >= self._cfg.scan_interval_s
        ):
            self._begin_scan()
        if self._long and now - self._state.last_projection >= PROJECTION_INTERVAL_S:
            self._state.last_projection = now
            self._project(now)

    def finish(self) -> None:
        """Wait (bounded) for a scan in flight and report its result before the command ends."""
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(JOIN_S)
        self.poll()

    # -- results ----------------------------------------------------------------------------------

    def _handle(self, result: ScanResult) -> None:
        self._pending = False
        self._last_ledger, self._last_recordings = result.ledger, result.recordings
        ledger = self._ledger
        usage = result.ledger.total_bytes() + (
            result.recordings.total_bytes() if result.recordings else 0
        )
        self._state.usage = usage
        self._state.own_bytes_at_scan = self._own_bytes()
        for scan in (result.ledger, result.recordings):
            if scan is None:
                continue
            self._record_scan(scan, result.finished)
            for recovery in scan.recoveries:
                ledger.record("state", "ledger.recovered", **recovery)
            if scan.partial:
                self._warn(Code.RET_SCAN, f"retention scan is partial: {scan.partial}", scan.path)
            for message in scan.messages:
                self._warn(Code.RET_SCAN, message, scan.path)
            if scan.unlocked:
                self._warn(
                    Code.RET_SCAN,
                    f"file locks are unavailable in {scan.path}: its reserves are not recovered",
                    scan.path,
                )
            self._check_age(scan, result.finished)
        self._check_total(result, usage)

    def _record_scan(self, scan: DirScan, now: float) -> None:
        sessions = list(scan.sessions.values())
        self._ledger.record(
            "state",
            "retention.scan",
            directory=str(scan.path),
            what=scan.kind,
            sessions=len(sessions),
            bytes=scan.total_bytes(),
            protected=protected_bytes_by_reason(scan),
            oldest=min((s.start for s in sessions), default=None),
            foreign=scan.foreign,
            unparseable=scan.unparseable,
            incomplete=sum(1 for s in sessions if not s.has_part1),
            hard_linked=sum(1 for s in sessions if s.hard_linked),
            recovered=scan.recovered,
            unrecoverable=len(scan.unrecoverable),
            entries=scan.entries,
            partial=scan.partial,
            unsafe=scan.unsafe,
        )

    def _warn(self, code: Code, summary: str, directory: Path) -> None:
        allowed = self._notices.allow(code.value, directory, self._cfg.warn_interval_s)
        self._ledger.warn(
            code,
            summary,
            event="retention.warning",
            display=allowed,
            directory=str(directory),
        )

    def _check_age(self, scan: DirScan, now: float) -> None:
        max_age = self._cfg.max_age_s
        if max_age is None:
            return
        old = [s for s in scan.sessions.values() if now - s.newest_mtime > max_age]
        selectable = [s for s in old if not s.protected]
        if not selectable:
            return
        protected = sum(s.size for s in old if s.protected)
        what = "ledger sessions" if scan.kind == "ledger" else "recordings"
        command = "ib ledger prune" if scan.kind == "ledger" else "ib rec prune"
        extra = f"; {format_size(protected)} more protected" if protected else ""
        self._warn(
            Code.RET_AGE,
            f"{len(selectable)} {what} older than {compact_duration(max_age)} "
            f"({format_size(sum(s.size for s in selectable))}{extra}); review with '{command}'",
            scan.path,
        )

    def _own_bytes(self) -> int:
        return sum(self._ledger.written_bytes.values())

    def _rate(self, now: float) -> float:
        """Bytes per second this process is adding: the ledger's over the last minute plus
        every recorder's."""
        samples = self._state.samples
        samples.append((now, self._own_bytes()))
        while len(samples) > 1 and now - samples[0][0] > RATE_WINDOW_S:
            samples.popleft()
        first = samples[0]
        span = now - first[0]
        ledger_rate = (samples[-1][1] - first[1]) / span if span > 0 else 0.0
        return ledger_rate + sum(source() for source in self._rate_sources)

    def _check_total(self, result: ScanResult, usage: int) -> None:
        cap = self._cfg.max_total_bytes
        if cap is None or usage <= cap or self._state.usage_warned:
            return
        self._state.usage_warned = True
        self._warn_total(result.ledger, result.recordings, usage, projected=False, horizon=0.0)

    def _project(self, now: float) -> None:
        """Every 10 s in a long session: usage plus this process's growth over the horizon
        against the cap. Shown once per process when the projection first crosses and once when
        usage itself crosses."""
        cap = self._cfg.max_total_bytes
        rate = self._rate(now)
        if cap is None or self._last_ledger is None:
            return
        horizon = (
            max(0.0, self._deadline - now) if self._deadline is not None else DEFAULT_HORIZON_S
        )
        grown = max(0, self._own_bytes() - self._state.own_bytes_at_scan)
        usage = int(self._state.usage + grown)
        if usage > cap:
            if not self._state.usage_warned:
                self._state.usage_warned = True
                self._warn_total(
                    self._last_ledger, self._last_recordings, usage, projected=False, horizon=0.0
                )
            return
        if usage + rate * horizon > cap and not self._state.projection_warned:
            self._state.projection_warned = True
            self._warn_total(
                self._last_ledger, self._last_recordings, usage, projected=True, horizon=horizon
            )

    def _warn_total(
        self,
        ledger: DirScan,
        recordings: DirScan | None,
        usage: int,
        *,
        projected: bool,
        horizon: float,
    ) -> None:
        cap = self._cfg.max_total_bytes
        assert cap is not None
        rec_bytes = recordings.total_bytes() if recordings else 0
        protected = sum(
            sum(protected_bytes_by_reason(scan).values())
            for scan in (ledger, recordings)
            if scan is not None
        )
        # the directory holding the most selectable bytes gets the suggested prune, with a
        # target that would bring the combined total under the cap
        choices = [("ledger", ledger, "ib ledger prune")]
        if recordings is not None:
            choices.append(("recordings", recordings, "ib rec prune"))

        def selectable(scan: DirScan) -> int:
            return sum(s.size for s in scan.sessions.values() if not s.protected)

        _, target_scan, command = max(choices, key=lambda c: selectable(c[1]))
        excess = max(0, usage - cap)
        target = max(MIB, target_scan.total_bytes() - excess - (cap // 20 if projected else 0))
        when = (
            f"projected past {format_size(cap)} within {format_duration(horizon)}"
            if projected
            else f"past {format_size(cap)}"
        )
        self._warn(
            Code.RET_TOTAL,
            f"ledger {format_size(ledger.total_bytes())} + recordings {format_size(rec_bytes)} "
            f"({format_size(protected)} protected), {when}; run '{command} --until-under "
            f"{size_argument(target)}' or raise retention.max_total_bytes",
            target_scan.path,
        )
