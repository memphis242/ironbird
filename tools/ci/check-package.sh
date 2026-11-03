#!/usr/bin/env bash
# Packaging check: build the sdist and wheel, validate their metadata, install the wheel into a
# clean venv (runtime dependencies only, pinned by constraints-dev.txt) and run the installed
# entry points. Needs `build` and `twine` (pip install '.[package]') on PATH's python.
#
#   tools/ci/check-package.sh [WORKDIR]
#
# WORKDIR (default: a fresh mktemp dir, removed on success) holds dist/, the sdist unpacked, and
# the clean venv. The checks, each a hard failure:
#   1. `python -m build` makes exactly one sdist and one wheel
#   2. `twine check --strict` accepts both (README renders, metadata is complete)
#   3. the wheel ships every module under src/ironbird, py.typed, and the entry points
#   4. the sdist ships LICENSE, README.md, pyproject.toml and the examples (the sample CCDD and
#      the header profiles under examples/headers: the code has no built-in profiles, a user
#      points the config at TOML files)
#   5. in the clean venv, away from the source tree: `ib --help`, `ironbird --help`,
#      ironbird imports from the venv, py.typed next to the installed package, and
#      `ib -c <sdist>/examples/mock.toml nodes` loads a config, CCDD JSON and header profiles
set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly CONSTRAINTS="$REPO_ROOT/constraints-dev.txt"

die() { echo "check-package.sh: error: $*" >&2; exit 1; }
step() { echo "== $*"; }

(( $# <= 1 )) || die "usage: check-package.sh [WORKDIR]"
python -c 'import build, twine' 2>/dev/null \
    || die "needs the build and twine packages: pip install '.[package]'"

cleanup=0
if (( $# == 1 )); then
    work="$1"
    mkdir -p "$work"
    [[ -z "$(ls -A "$work")" ]] || die "WORKDIR $work is not empty"
else
    work="$(mktemp -d)"
    cleanup=1
fi
dist="$work/dist"

# Built from a copy of the tracked and untracked-but-not-ignored files: the build writes build/
# and *.egg-info next to its sources, and the checkout must stay clean.
step "build sdist and wheel"
tree="$work/tree"
mkdir -p "$tree"
(cd "$REPO_ROOT" && git ls-files -z --cached --others --exclude-standard) \
    | tar -C "$REPO_ROOT" --null -T - -cf - | tar -C "$tree" -xf -
python -m build --sdist --wheel --outdir "$dist" "$tree"
shopt -s nullglob
sdists=("$dist"/*.tar.gz)
wheels=("$dist"/*.whl)
(( ${#sdists[@]} == 1 )) || die "expected one sdist in $dist, found ${#sdists[@]}"
(( ${#wheels[@]} == 1 )) || die "expected one wheel in $dist, found ${#wheels[@]}"
sdist="${sdists[0]}"
wheel="${wheels[0]}"

step "twine check --strict"
python -m twine check --strict "$sdist" "$wheel"

step "wheel contents"
wheel_files="$(python -I -c 'import sys, zipfile
print("\n".join(zipfile.ZipFile(sys.argv[1]).namelist()))' "$wheel")"
missing=0
while IFS= read -r source; do
    rel="${source#"$REPO_ROOT"/src/}"
    grep -qxF "$rel" <<<"$wheel_files" || { echo "wheel lacks $rel" >&2; missing=1; }
done < <(find "$REPO_ROOT/src/ironbird" -name '*.py' -o -name 'py.typed' | sort)
grep -qxF "ironbird/py.typed" <<<"$wheel_files" || { echo "wheel lacks ironbird/py.typed" >&2; missing=1; }
grep -q '^ironbird-.*\.dist-info/entry_points\.txt$' <<<"$wheel_files" \
    || { echo "wheel lacks entry_points.txt" >&2; missing=1; }
if grep -qE '^(tests|examples|docs|tools)/' <<<"$wheel_files"; then
    echo "wheel carries non-package directories:" >&2
    grep -E '^(tests|examples|docs|tools)/' <<<"$wheel_files" >&2
    missing=1
fi
(( missing == 0 )) || die "the wheel does not match src/ironbird"

step "sdist contents"
sdist_files="$(tar -tzf "$sdist")"
for need in LICENSE README.md pyproject.toml examples/mock.toml examples/ironbird.toml \
            examples/ccdd/mock.json examples/headers/ccsds_v1_cfs_tlm.toml; do
    grep -qE "^[^/]+/$need\$" <<<"$sdist_files" || die "the sdist lacks $need"
done
for header in "$REPO_ROOT"/examples/headers/*.toml; do
    grep -qE "^[^/]+/examples/headers/$(basename "$header")\$" <<<"$sdist_files" \
        || die "the sdist lacks examples/headers/$(basename "$header")"
done
unpacked="$work/sdist"
mkdir -p "$unpacked"
tar -xzf "$sdist" -C "$unpacked" --strip-components=1

step "install the wheel into a clean venv (runtime dependencies only)"
venv="$work/venv"
python -m venv "$venv"
"$venv/bin/python" -m pip install --quiet --disable-pip-version-check \
    -c "$CONSTRAINTS" "$wheel"
"$venv/bin/python" -m pip check

step "smoke-run the installed entry points (cwd outside the source tree)"
smoke="$work/smoke"
mkdir -p "$smoke"
cd "$smoke"
for entry in ib ironbird; do
    "$venv/bin/$entry" --help >/dev/null || die "'$entry --help' failed"
done
installed="$("$venv/bin/python" -I -c 'import ironbird, sys; print(ironbird.__file__)')"
case "$installed" in
    "$venv"/*) ;;
    *) die "ironbird imports from $installed, not the clean venv" ;;
esac
[[ -f "$(dirname "$installed")/py.typed" ]] || die "py.typed is not installed next to $installed"
"$venv/bin/ib" -c "$unpacked/examples/mock.toml" nodes >/dev/null \
    || die "'ib -c examples/mock.toml nodes' failed on the installed wheel"

step "ok"
cd "$REPO_ROOT"
if (( cleanup )); then rm -rf "$work"; fi
