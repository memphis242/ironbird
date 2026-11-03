#!/usr/bin/env bash
# Branch coverage of src/ironbird from the test suite.
#
#   tools/coverage.sh [pytest args...]        e.g. tools/coverage.sh -n auto -q
#
# Run it from the venv that has `pip install -e '.[dev]'`. The report is printed, and the run
# fails when total coverage is under [tool.coverage.report] fail_under in pyproject.toml.
# Data files go to $IRONBIRD_COV_DIR (default: a directory under $TMPDIR, never the source tree).
#
# Why this is not just `coverage run -m pytest`: several tests start `ib` as a subprocess with a
# scrubbed environment (env={"PATH": ...}), which drops COVERAGE_PROCESS_START, so the child never
# measures itself. This script installs a .pth file into the active venv for the duration of the
# run that starts coverage in every Python process (the pytest parent, the xdist workers, those
# children) from pyproject.toml, and removes it on exit.
#
# Tests that spawn a worker pool or wait on timers run slower under measurement; the host-timing
# benchmarks skip themselves under pytest-xdist and are not part of this run.
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
data="${IRONBIRD_COV_DIR:-${TMPDIR:-/tmp}/ironbird-coverage-$(id -un)}"
python="${PYTHON:-python3}"
purelib="$("$python" -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')"
pth="$purelib/zz_ironbird_coverage.pth"

mkdir -p "$data"
rm -f "$data"/.coverage*

cat > "$pth" <<PTH
import os; os.environ.setdefault("COVERAGE_PROCESS_START", "$repo/pyproject.toml"); os.environ.setdefault("COVERAGE_FILE", "$data/.coverage"); import coverage; coverage.process_startup()
PTH
trap 'rm -f "$pth"' EXIT

cd "$repo"
status=0
"$python" -m pytest -p no:cacheprovider --benchmark-disable "$@" || status=$?

rm -f "$pth"  # stop measuring before the report is built
COVERAGE_FILE="$data/.coverage" "$python" -m coverage combine --quiet "$data" || true
COVERAGE_FILE="$data/.coverage" "$python" -m coverage report || report=$?
exit $(( status != 0 ? status : ${report:-0} ))
