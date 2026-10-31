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
import threading
import time
import traceback
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final

from ironbird.config import RetentionConfig
from ironbird.errors import Code
from ironbird.fsops import REAL_FS, FileSystem
from ironbird.ledger import Ledger
from ironbird.retention import (
    DirScan,
    ScanControl,
    full_scan,
    protected_bytes_by_reason,
    recording_blocks,
)
from ironbird.retnotices import Notices
from ironbird.sigmask import start_thread
from ironbird.units import MIB, compact_duration, format_duration, format_size

PROJECTION_INTERVAL_S: Final = 10.0
RATE_WINDOW_S: Final = 60.0
DEFAULT_HORIZON_S: Final = 3600.0
JOIN_S: Final = 2.0  # a scan still running at exit gets this long to report
LEDGER_KEEP: Final = (
    0.5  # share of the exit budget a scan may not spend: the ledger's own last writes
)


def size_argument(size: int) -> str:
    """``size`` as a ``--until-under`` argument in the size grammar (whole MiB, at least 1)."""
    mib = max(1, size // MIB)
    if mib % 1024 == 0:
        return f"{mib // 1024}GiB"
    return f"{mib}MiB"


@dataclass(slots=True)
class ScanResult:
    """What a scan thread hands back: the scans, or why there are none (not due, or it failed)."""

    ledger: DirScan | None = None
    recordings: DirScan | None = None
    finished: float = 0.0
    not_due: bool = False
    failure: BaseException | None = None
    failure_text: str = ""


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
        self._control = ScanControl()

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
        the last scan by any process is older than ``scan_interval``. Nothing here touches the
        file system: the scan thread asks the notices file whether a scan is due."""
        self._begin_scan(ask_due=not self._long)

    def _begin_scan(self, *, ask_due: bool = False) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._state.last_scan = self._clock()
        self._pending = True
        self._thread = threading.Thread(
            target=self._scan, args=(ask_due,), name="retention-scan", daemon=True
        )
        start_thread(self._thread)

    def _scan(self, ask_due: bool) -> None:
        """The scan thread: nothing may escape it - a failure is handed back as a result."""
        try:
            if ask_due and not self._notices.scan_due(self._cfg.scan_interval_s):
                self._results.put(ScanResult(not_due=True))
                return
            self._results.put(self._scan_directories())
        except BaseException as exc:
            self._results.put(
                ScanResult(failure=exc, failure_text="".join(traceback.format_exception(exc)))
            )

    def _scan_directories(self) -> ScanResult:
        now = self._wall()
        own = self._ledger.session_id
        keep = self._cfg.keep_recent
        # A short command only looks: recovering a reserve rewrites files, and a command about
        # to exit must not leave that half done. Long sessions and prune recover.
        recover = self._long
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
            recover=recover,
            control=self._control,
            on_record=self._post_record,
        )
        return ScanResult(ledger_scan, recordings, now)

    def _post_record(self, event: str, fields: dict[str, Any]) -> None:
        """A recovery step ended (scan thread): the ledger takes the record from here, even when
        the process has stopped waiting for this scan."""
        self._ledger.post("state", event, **fields)

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
        """Wait (bounded) for a scan in flight and report its result before the command ends.

        No new recovery step begins once the exit has started, but one under way (a salvage
        copying a reserve) is given the exit's waiting budget to end rather than being cut off
        half done - all of it but the share the ledger needs for its own last writes and close
        (:data:`LEDGER_KEEP`); a scan that is only reading gets :data:`JOIN_S`. A step still
        running when the wait ends is abandoned: the ledger says so (``ledger.recovery_incomplete``
        with ``abandoned``), and the thread writes its own record if it finishes after all."""
        thread = self._thread
        if thread is not None and thread.is_alive():
            self._control.stop.set()
            budget = self._ledger.wait_budget
            if budget is not None:
                keep = budget.total * LEDGER_KEEP
                if (
                    not budget.join(thread, cap=JOIN_S, keep=keep)
                    and self._control.stepping.is_set()
                ):
                    budget.join(thread, keep=keep)
            else:
                thread.join(JOIN_S)
                if self._control.stepping.is_set():
                    thread.join(self._ledger.join_timeout_s * (1 - LEDGER_KEEP))
            self._note_abandoned(thread)
        self.poll()

    def _note_abandoned(self, thread: threading.Thread) -> None:
        step = self._control.step
        if thread.is_alive() and step is not None and self._control.stepping.is_set():
            self._ledger.record(
                "state",
                "ledger.recovery_incomplete",
                **step,
                abandoned=True,
                error="the process ended before the copy did; the next long session or prune "
                "finishes it from the .damaged file",
            )

    # -- results ----------------------------------------------------------------------------------

    def _handle(self, result: ScanResult) -> None:
        self._pending = False  # whatever came back, the next scan may run
        if result.not_due:
            return
        if result.failure is not None or result.ledger is None:
            self._report_failure(result)
            return
        self._last_ledger, self._last_recordings = result.ledger, result.recordings
        usage = result.ledger.total_bytes() + (
            result.recordings.total_bytes() if result.recordings else 0
        )
        self._state.usage = usage
        self._state.own_bytes_at_scan = self._own_bytes()
        for scan in (result.ledger, result.recordings):
            if scan is None:
                continue
            self._record_scan(scan, result.finished)
            # ledger.recovered / ledger.recovery_incomplete were handed to the ledger by the scan
            # thread as each step ended; nothing is left to collect here.
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
        self._check_total(result.ledger, result.recordings, usage)

    def _report_failure(self, result: ScanResult) -> None:
        """A scan that died of an unexpected error: one coalesced ``IB-RET-004`` line, the full
        traceback in the ledger. The next scan runs as usual."""
        exc = result.failure
        what = f"{type(exc).__name__}: {exc}" if exc is not None else "no result"
        self._warn(
            Code.RET_SCAN,
            f"retention scan failed ({what[:160]}); the figures are missing, see the ledger",
            self._state_dir,
            error=what,
            traceback=result.failure_text,
        )

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
            damaged_files=sum(len(s.damaged) for s in sessions),
            unreadable=len(scan.unreadable),
            entries=scan.entries,
            partial=scan.partial,
            unsafe=scan.unsafe,
        )

    def _warn(self, code: Code, summary: str, directory: Path, **fields: Any) -> None:
        """Ledger a warning and display it at most once per ``warn_interval`` per code and
        directory across processes. The notices file is asked on the ledger's worker thread, so
        neither the loop nor the main thread waits for it."""
        interval = self._cfg.warn_interval_s
        self._ledger.warn(
            code,
            summary,
            event="retention.warning",
            gate=lambda: self._notices.allow(code.value, directory, interval),
            directory=str(directory),
            **fields,
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

    def _check_total(self, ledger: DirScan, recordings: DirScan | None, usage: int) -> None:
        cap = self._cfg.max_total_bytes
        if cap is None or usage <= cap or self._state.usage_warned:
            return
        self._state.usage_warned = True
        self._warn_total(ledger, recordings, usage, projected=False, horizon=0.0)

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
