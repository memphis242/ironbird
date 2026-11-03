#!/usr/bin/env bash
# Dependency-vulnerability audit with pip-audit (needs `pip install '.[audit]'` on PATH's python).
#
#   tools/ci/audit-deps.sh
#
# Audits two resolved sets, each frozen from a throwaway venv, so that neither the audit tool's
# own dependencies nor the venv bootstrap tools (pip, setuptools, wheel) are mixed in:
#   dev      '.[dev]' installed with constraints-dev.txt: the exact versions CI tests with
#   runtime  '.' installed without constraints: what `pip install ironbird` resolves today,
#            floating inside the ranges in pyproject.toml
# (pip, setuptools and wheel are the venv's bootstrap tools, not part of the closure; ironbird
# itself is not on an index.) Any known vulnerability, or any package the audit cannot check, fails the script (exit 1) and
# the advisories are printed with their fixed versions. Accepted advisories are listed, each
# with the reason, in tools/ci/audit-ignore.txt ("ID  # reason"); an entry without a reason is
# an error.
set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly CONSTRAINTS="$REPO_ROOT/constraints-dev.txt"
readonly IGNORE_FILE="$REPO_ROOT/tools/ci/audit-ignore.txt"

die() { echo "audit-deps.sh: error: $*" >&2; exit 1; }

(( $# == 0 )) || die "usage: audit-deps.sh"
command -v pip-audit >/dev/null || die "pip-audit not found: pip install '.[audit]'"

ignore_args=()
if [[ -f "$IGNORE_FILE" ]]; then
    while IFS= read -r line; do
        [[ "$line" =~ ^[[:space:]]*(#.*)?$ ]] && continue
        if [[ "$line" =~ ^([A-Za-z0-9-]+)[[:space:]]+#[[:space:]]*(.+)$ ]]; then
            ignore_args+=(--ignore-vuln "${BASH_REMATCH[1]}")
            echo "audit-deps.sh: ignoring ${BASH_REMATCH[1]}: ${BASH_REMATCH[2]}" >&2
        else
            die "$IGNORE_FILE: '$line' is not 'ID  # reason'"
        fi
    done < "$IGNORE_FILE"
fi

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# Copy first: installing from the checkout would write build/ and *.egg-info into it.
tree="$work/tree"
mkdir -p "$tree"
(cd "$REPO_ROOT" && git ls-files -z --cached --others --exclude-standard) \
    | tar -C "$REPO_ROOT" --null -T - -cf - | tar -C "$tree" -xf -

freeze() {  # freeze NAME PIP_ARGS...: install into a fresh venv, write $work/NAME.txt
    local name="$1"
    shift
    python -m venv "$work/venv-$name"
    "$work/venv-$name/bin/python" -m pip install --quiet --disable-pip-version-check "$@"
    "$work/venv-$name/bin/python" -m pip freeze --exclude-editable \
        --exclude pip --exclude setuptools --exclude wheel --exclude ironbird > "$work/$name.txt"
    [[ -s "$work/$name.txt" ]] || die "no packages resolved for the $name set"
    echo "== $name set: $(wc -l < "$work/$name.txt") packages"
}

freeze dev -c "$CONSTRAINTS" "$tree[dev]"
freeze runtime "$tree"

status=0
for name in dev runtime; do
    echo "== pip-audit: $name set"
    # --no-deps / --disable-pip: the freeze above is already the full closure, audited as is.
    pip-audit --strict --desc --no-deps --disable-pip --requirement "$work/$name.txt" \
        ${ignore_args[@]+"${ignore_args[@]}"} || status=1
done
(( status == 0 )) || die "pip-audit reported vulnerabilities or could not check a package (see above); upgrade, or accept an advisory in $IGNORE_FILE with a reason"
echo "== ok"
