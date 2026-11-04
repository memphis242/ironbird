#!/usr/bin/env bash
# Branch coverage of src/ironbird from the test suite.
#
#   tools/coverage.sh [pytest args...]        e.g. tools/coverage.sh -n auto -q
#
# Run it from the venv that has `pip install -e '.[dev]'`. The report is printed, and the run
# fails when total coverage is under [tool.coverage.report] fail_under in pyproject.toml.
# Data files go to $IRONBIRD_COV_DIR (default: a fresh per-run mktemp directory, removed on exit;
# never the source tree).
#
# Why this is not just `coverage run -m pytest`: several tests start `ib` as a subprocess with a
# scrubbed environment (env={"PATH": ...}), which drops COVERAGE_PROCESS_START, so the child never
# measures itself. This script installs a per-run .pth hook into the active venv that starts
# coverage only in processes descended from this script (checked through /proc), so other
# sessions using the same venv are never measured. The hook is a single self-contained .pth line
# (as coverage's own a1_coverage.pth is), so no module is imported and no bytecode is written
# beside it. It is removed on exit; a hook left by a killed run is inert (its owner PID is gone)
# and is removed by the next run.
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python="${PYTHON:-python3}"

if [[ "$("$python" -c 'import sys; print(sys.prefix != sys.base_prefix)')" != "True" ]]; then
    echo "coverage.sh: error COV001: $python is not a virtualenv; activate the dev venv or set PYTHON" >&2
    exit 2
fi
purelib="$("$python" -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')"

# Remove hooks whose owning run is no longer alive.
for stale in "$purelib"/zz_ironbird_coverage_*.pth; do
    [[ -e "$stale" ]] || continue
    owner="${stale##*_}"; owner="${owner%.pth}"
    if ! kill -0 "$owner" 2>/dev/null; then
        echo "coverage.sh: removing stale hook from a killed run: $stale" >&2
        rm -f "$stale"
    fi
done

run_id="$$"
pth="$purelib/zz_ironbird_coverage_$run_id.pth"
own_data=""  # set to the data directory when this run created it
cleanup() {
    rm -f "$pth"
    if [[ -n "$own_data" ]]; then
        rm -rf "$own_data"
    fi
}
trap cleanup EXIT

if [[ -n "${IRONBIRD_COV_DIR:-}" ]]; then
    data="$IRONBIRD_COV_DIR"
    mkdir -p "$data"
    rm -f "$data"/.coverage*
else
    data="$(mktemp -d "${TMPDIR:-/tmp}/ironbird-coverage.XXXXXX")"
    own_data="$data"
fi

# Values reach Python through repr(), so any path characters are safe. site.py runs a .pth line
# that starts with "import" through exec(), so the whole hook is one line: the body is a string
# literal (its newlines escaped by repr) run with its own globals.
IB_REPO="$repo" IB_DATA="$data" IB_OWNER="$run_id" IB_OUT="$pth" "$python" - <<'PY'
import os
body = f'''import os
pid = os.getpid()
owner = {int(os.environ["IB_OWNER"])!r}
for _ in range(256):
    if pid == owner:
        os.environ.setdefault("COVERAGE_PROCESS_START", {os.path.join(os.environ["IB_REPO"], "pyproject.toml")!r})
        os.environ.setdefault("COVERAGE_FILE", {os.path.join(os.environ["IB_DATA"], ".coverage")!r})
        import coverage
        coverage.process_startup()
        break
    if pid <= 1:
        break
    try:
        with open("/proc/%d/stat" % pid, "rb") as f:
            stat = f.read()
        pid = int(stat[stat.rindex(b")") + 2:].split()[1])
    except (OSError, ValueError, IndexError):
        break
'''
line = "import sys; exec(" + repr(body) + ", {})\n"
assert "\n" not in line[:-1]
with open(os.environ["IB_OUT"], "w") as f:
    f.write(line)
PY

cd "$repo"
status=0
"$python" -m pytest -p no:cacheprovider --benchmark-disable "$@" || status=$?

rm -f "$pth"  # stop measuring before the report is built
report=0
if ! COVERAGE_FILE="$data/.coverage" "$python" -m coverage combine --quiet "$data"; then
    echo "coverage.sh: error COV002: coverage combine failed for $data; no report built" >&2
    exit $(( status != 0 ? status : 3 ))
fi
COVERAGE_FILE="$data/.coverage" "$python" -m coverage report || report=$?
exit $(( status != 0 ? status : report ))
