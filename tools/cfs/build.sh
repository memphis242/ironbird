#!/usr/bin/env bash
# Build the public NASA cFS bundle (native Linux simulation target) as a normal
# user, out-of-tree, as ironbird's real-flight-software verification target.
#
# Environment overrides:
#   CFS_BUILD_DIR  root of the checkout + build (default /home/user/build/cfs)
#   CFS_REF        bundle tag to check out (default below)
#   CFS_COMMIT     expected commit of CFS_REF; the build fails if the tag does
#                  not resolve to it (default below; set to "" to skip the check
#                  when overriding CFS_REF)
#   CFS_CONFIG     cFS make configuration (default native_std)
#   JOBS           parallel build jobs (default: nproc)
set -euo pipefail

readonly DEFAULT_REF="v7.0.1"
readonly DEFAULT_COMMIT="088b2fa828db9ff7e00733f1908e0eeb59f66ce3"

CFS_BUILD_DIR="${CFS_BUILD_DIR:-/home/user/build/cfs}"
CFS_REF="${CFS_REF:-$DEFAULT_REF}"
if [[ "$CFS_REF" == "$DEFAULT_REF" ]]; then
    CFS_COMMIT="${CFS_COMMIT-$DEFAULT_COMMIT}"
else
    CFS_COMMIT="${CFS_COMMIT-}"
fi
CFS_CONFIG="${CFS_CONFIG:-native_std}"
JOBS="${JOBS:-$(nproc)}"
readonly CFS_BUILD_DIR CFS_REF CFS_COMMIT CFS_CONFIG JOBS
readonly SRC_DIR="$CFS_BUILD_DIR/src"
readonly CFS_URL="https://github.com/nasa/cFS"

die() { echo "build.sh: error: $*" >&2; exit 1; }

for tool in git make cmake gcc; do
    command -v "$tool" >/dev/null || die "required tool '$tool' not found on PATH"
done
[[ "$(id -u)" -ne 0 ]] || echo "build.sh: warning: running as root; cFS is intended to build and run as a normal user" >&2

mkdir -p "$CFS_BUILD_DIR"

if [[ ! -d "$SRC_DIR/.git" ]]; then
    echo "build.sh: cloning $CFS_URL @ $CFS_REF into $SRC_DIR"
    git clone --branch "$CFS_REF" --depth 1 --recurse-submodules \
        --shallow-submodules -j "$JOBS" "$CFS_URL" "$SRC_DIR" \
        || die "clone failed (network/proxy? see /root/.ccr/README.md when behind the agent proxy)"
else
    echo "build.sh: reusing existing checkout $SRC_DIR"
fi

actual_commit="$(git -C "$SRC_DIR" rev-parse HEAD)"
if [[ -n "$CFS_COMMIT" && "$actual_commit" != "$CFS_COMMIT" ]]; then
    die "checkout is at $actual_commit but $CFS_REF is pinned to $CFS_COMMIT; remove $SRC_DIR or fix CFS_REF/CFS_COMMIT"
fi

# A stale record must never describe a build that did not finish.
rm -f "$CFS_BUILD_DIR/PINNED_VERSION"

# What is actually checked out (not what was asked for): the exact tag if HEAD is on one.
actual_ref="$(git -C "$SRC_DIR" describe --tags --exact-match 2>/dev/null || true)"
[[ -n "$actual_ref" ]] || actual_ref="$(git -C "$SRC_DIR" rev-parse --abbrev-ref HEAD)"
if [[ "$actual_ref" != "$CFS_REF" ]]; then
    echo "build.sh: warning: $SRC_DIR is at $actual_ref, not the requested $CFS_REF" >&2
fi

cd "$SRC_DIR"
make "${CFS_CONFIG}.prep"
make -j"$JOBS" "${CFS_CONFIG}.install"

readonly EXE_DIR="$SRC_DIR/build-${CFS_CONFIG}/exe/cpu1"
[[ -x "$EXE_DIR/core-cpu1" ]] || die "build finished but $EXE_DIR/core-cpu1 is missing"

# Record exactly what was built (bundle commit + every submodule commit), only now that the
# build succeeded; write-then-rename so a reader never sees a half-written file.
{
    echo "bundle_url=$CFS_URL"
    echo "bundle_ref=$actual_ref"
    echo "bundle_commit=$actual_commit"
    echo "config=$CFS_CONFIG"
    echo "submodules:"
    git -C "$SRC_DIR" submodule status | sed 's/^/  /'
} > "$CFS_BUILD_DIR/PINNED_VERSION.tmp"
mv "$CFS_BUILD_DIR/PINNED_VERSION.tmp" "$CFS_BUILD_DIR/PINNED_VERSION"
echo "build.sh: recorded $CFS_BUILD_DIR/PINNED_VERSION"
echo "build.sh: OK - $EXE_DIR/core-cpu1 (cFS $actual_ref, $actual_commit)"
