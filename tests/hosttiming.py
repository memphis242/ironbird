"""Marker shared by the host-timing guards (tests/test_benchmarks.py, tests/bench/)."""

from __future__ import annotations

import os

import pytest

# A guard whose threshold is a rate or a cost ratio measured on the host. Under xdist the other
# workers keep every core busy, so such a measurement says more about the scheduler than about
# the tool: a listener off-CPU for a few milliseconds overruns a socket buffer capped by a small
# net.core.rmem_max (212992 on GitHub runners), and a timed loop shares its core with another
# test. These run, and are enforced, in the serial runs of CI (``pytest tests/test_benchmarks.py
# -n0`` and ``pytest tests/bench -n0``), as pytest-benchmark's own ceilings are.
needs_a_quiet_host = pytest.mark.skipif(
    "PYTEST_XDIST_WORKER" in os.environ,
    reason="host-timing guard: enforced by the serial CI runs (pytest tests/bench -n0, "
    "pytest tests/test_benchmarks.py -n0)",
)
