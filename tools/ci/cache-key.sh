#!/usr/bin/env bash
# Print the inputs of the cFS build-cache key as `name=value` lines, ready to append to
# "$GITHUB_OUTPUT" (the CI user cannot write that file, so the workflow runs this as root):
#
#   commit=<40 hex>     the cFS commit tools/cfs/build.sh builds
#   toolchain=<16 hex>  with --toolchain (Enterprise Linux containers): a hash of the
#                       `rpm -q gcc glibc cmake` output, so a toolchain update in the base
#                       image does not restore a build made with the old one
#
# The human-readable toolchain versions go to stderr, i.e. into the job log.
set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly BUILD_SCRIPT="$REPO_ROOT/tools/cfs/build.sh"

die() { echo "cache-key.sh: error: $*" >&2; exit 1; }

want_toolchain=0
case "${1:-}" in
    "") ;;
    --toolchain) want_toolchain=1 ;;
    *) die "usage: cache-key.sh [--toolchain]" ;;
esac

commit="$(sed -n 's/^readonly DEFAULT_COMMIT="\(.*\)"$/\1/p' "$BUILD_SCRIPT")"
[[ "$commit" =~ ^[0-9a-f]{40}$ ]] \
    || die "no 40-hex DEFAULT_COMMIT in $BUILD_SCRIPT (got '$commit')"
echo "commit=$commit"

if (( want_toolchain )); then
    command -v rpm >/dev/null || die "--toolchain needs rpm (Enterprise Linux only)"
    versions="$(rpm -q gcc glibc cmake)" || die "gcc, glibc and cmake must all be installed"
    echo "$versions" >&2
    hash="$(printf '%s\n' "$versions" | sha256sum)"
    echo "toolchain=${hash:0:16}"
fi
