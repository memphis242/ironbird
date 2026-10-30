"""Stage 2d, layer 3: the retention service - when scans run and when it warns.

Scans run in a worker thread; ``poll`` (called from the ledger's periodic upkeep) hands the result
to the ledger. Clocks are injected, so intervals are crossed by moving them.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ironbird.config import RetentionConfig
from ironbird.errors import Code
from ironbird.ledger import Ledger
from ironbird.notice import Notice
from ironbird.retnotices import Notices
from ironbird.retservice import Retention, size_argument
from ironbird.units import GIB, KIB, MIB
from ledgerutil import session_records
from retutil import make_session
from support.faultfs import FakeClock, FaultFs


class Rig:
    def __init__(
        self,
        tmp_path: Path,
        config: RetentionConfig,
        *,
        long: bool = True,
        notices: Notices | None = None,
    ) -> None:
        self.state = tmp_path / "st"
        self.state.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.recordings = tmp_path / "rec"
        self.clock = FakeClock()
        self.fs = FaultFs()
        self.shown: list[Notice] = []
        self.ledger = Ledger(
            self.state, session_id="a" * 12, session_class="long" if long else "short"
        )
        self.ledger.set_notifier(self.shown.append)
        self.notices = notices or Notices(self.state)
        self.retention = Retention(
            self.ledger,
            config,
            self.state,
            self.recordings,
            long_session=long,
            notices=self.notices,
            fs=self.fs,
            clock=self.clock,
        )

    def settle(self) -> None:
        self.retention.finish()

    def codes(self) -> list[Code]:
        return [n.code for n in self.shown]


def test_the_total_warning_appears_at_start_when_usage_is_already_past_the_cap(
    tmp_path: Path,
) -> None:
    rig = Rig(tmp_path, RetentionConfig(max_total_bytes=MIB))
    for i in range(1, 6):
        make_session(rig.state, i, age_days=2, size=300_000, klass="short")
    rig.retention.start()
    rig.settle()
    (warning,) = [n for n in rig.shown if n.code is Code.RET_TOTAL]
    assert warning.summary.startswith("ledger ") and "past 1 MiB" in warning.summary
    assert "run 'ib ledger prune --until-under " in warning.summary
    rig.settle()
    assert rig.codes().count(Code.RET_TOTAL) == 1  # once per process
    rig.ledger.close()
    assert any(r["event"] == "retention.scan" for r in session_records(rig.state))


def test_the_projection_warns_once_when_growth_over_the_horizon_would_pass_the_cap(
    tmp_path: Path,
) -> None:
    rig = Rig(tmp_path, RetentionConfig(max_total_bytes=MIB))
    make_session(rig.state, 1, age_days=2, size=900 * KIB, klass="short")
    rig.retention.add_rate_source(lambda: 0.0)
    rig.retention.start()
    rig.settle()
    rig.clock.advance(11)
    rig.retention.poll()
    assert Code.RET_TOTAL not in rig.codes()  # a quiet process does not trip it
    rate = {"bps": 100 * KIB}  # a recorder writing 100 KiB/s
    rig.retention.add_rate_source(lambda: float(rate["bps"]))
    for _ in range(5):
        rig.clock.advance(11)
        rig.retention.poll()
    projected = [n for n in rig.shown if n.code is Code.RET_TOTAL]
    assert len(projected) == 1
    assert "projected past 1 MiB within 1 h" in projected[0].summary
    rig.ledger.close()


def test_the_horizon_is_what_is_left_of_the_duration(tmp_path: Path) -> None:
    rig = Rig(tmp_path, RetentionConfig(max_total_bytes=MIB))
    make_session(rig.state, 1, age_days=2, size=900 * KIB, klass="short")
    rig.retention.add_rate_source(lambda: 1 * KIB)  # 1 KiB/s
    rig.retention.set_deadline(60)  # a minute left: 60 KiB more, under the 100 KiB of room
    rig.retention.start()
    rig.settle()
    rig.clock.advance(11)
    rig.retention.poll()
    assert Code.RET_TOTAL not in rig.codes()
    rig.retention.set_deadline(300)  # five minutes: 300 KiB would pass the cap
    rig.clock.advance(11)
    rig.retention.poll()
    assert Code.RET_TOTAL in rig.codes()
    rig.ledger.close()


def test_a_long_session_rescans_every_scan_interval_and_not_before(tmp_path: Path) -> None:
    rig = Rig(tmp_path, RetentionConfig(scan_interval_s=600))
    make_session(rig.state, 1, age_days=2, klass="short")
    rig.retention.start()
    rig.settle()
    first = rig.fs.count("scandir")
    assert first >= 1
    rig.clock.advance(599)
    rig.retention.poll()
    rig.settle()
    assert rig.fs.count("scandir") == first
    rig.clock.advance(2)
    rig.retention.poll()
    rig.settle()
    assert rig.fs.count("scandir") > first
    rig.ledger.close()


def test_short_commands_scan_only_when_the_last_scan_by_any_process_is_old(
    tmp_path: Path,
) -> None:
    state = tmp_path / "st"
    state.mkdir(mode=0o700)
    notices = Notices(state)
    config = RetentionConfig(scan_interval_s=600)
    scans = []
    for index in range(3):
        rig = Rig(tmp_path / f"p{index}", config, long=False, notices=notices)
        rig.retention.start()
        rig.settle()
        scans.append(rig.fs.count("scandir"))
        rig.ledger.close()
    assert scans[0] >= 1 and scans[1:] == [0, 0]  # a script of short commands scans once


def test_a_failing_scan_is_reported_not_swallowed(tmp_path: Path) -> None:
    rig = Rig(tmp_path, RetentionConfig())
    make_session(rig.state, 1, age_days=2, klass="short")
    rig.fs.fail("scandir", 5)  # EIO
    rig.retention.start()
    rig.settle()
    (warning,) = [n for n in rig.shown if n.code is Code.RET_SCAN]
    assert "cannot be listed" in warning.summary
    rig.ledger.close()


@pytest.mark.parametrize(
    ("size", "text"),
    [(5 * MIB, "5MiB"), (GIB, "1GiB"), (3 * GIB + 512 * MIB, "3584MiB"), (10, "1MiB"), (0, "1MiB")],
)
def test_the_suggested_target_is_in_the_size_grammar(size: int, text: str) -> None:
    from ironbird.units import parse_size

    assert size_argument(size) == text and parse_size(text) is not None
