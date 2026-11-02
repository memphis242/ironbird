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

# ironbird's own apps (tools/cfs/apps/<name>, today ib_test) are copied into the checkout's apps/
# and added to the mission's applist, so the one pinned bundle builds them with everything else.
# Both steps are repeatable: the copy is replaced and the applist line is added once.
readonly APPS_SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/apps"
readonly TARGETS_CMAKE="$SRC_DIR/sample_defs/targets.cmake"
[[ -f "$TARGETS_CMAKE" ]] || die "$TARGETS_CMAKE not found; is $SRC_DIR a cFS bundle?"
for app_dir in "$APPS_SRC"/*/; do
    [[ -d "$app_dir" ]] || continue
    app="$(basename "$app_dir")"
    rm -rf "${SRC_DIR:?}/apps/$app"
    cp -a "$app_dir" "$SRC_DIR/apps/$app"
    if ! grep -q "^list(APPEND MISSION_GLOBAL_APPLIST $app) # ironbird\$" "$TARGETS_CMAKE"; then
        printf 'list(APPEND MISSION_GLOBAL_APPLIST %s) # ironbird\n' "$app" >> "$TARGETS_CMAKE"
    fi
    echo "build.sh: added ironbird app $app to the build"
done

# ironbird's cFS configuration (tools/cfs/sample_defs/**) goes into the checkout's sample_defs/,
# where cFS looks for mission overrides of its OSAL options, of an app's configuration headers and
# of its tables (an override replaces the app's own default file of that name, which stays
# untouched). A path the checkout already tracks is a stock cFS file and is never replaced. Every
# path (to install and to remove) is checked before anything is copied or deleted: it must have a
# plain name, and no component of its destination may be a symlink or lead out of sample_defs/.
# The installed paths are recorded in a manifest, so an override removed from
# tools/cfs/sample_defs is removed from the checkout on the next run and a rebuild matches a fresh
# build. The manifest first holds the old and the new paths together, so a run interrupted while
# copying is still cleaned up by the next one, and only after the copy it holds exactly the new set.
readonly DEFS_SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/sample_defs"
readonly DEFS_DST="$SRC_DIR/sample_defs"
readonly DEFS_MANIFEST="$CFS_BUILD_DIR/IRONBIRD_SAMPLE_DEFS"
[[ -d "$DEFS_DST" ]] || die "$DEFS_DST not found; is $SRC_DIR a cFS bundle?"
[[ ! -L "$DEFS_DST" ]] || die "$DEFS_DST is a symlink; refusing to install ironbird's cFS configuration through it"
readonly DEFS_DST_REAL="$(realpath -- "$DEFS_DST")"

# Reads stdin to $1 through a fresh temporary file renamed into place, so a reader never sees a
# half-written file and a symlink planted at a predictable name is never written through.
write_atomic() {
    local dest="$1" tmp
    tmp="$(mktemp -- "$dest.XXXXXX")" || die "cannot create a temporary file next to $dest"
    if ! cat > "$tmp" || ! chmod 644 "$tmp" || ! mv -f -- "$tmp" "$dest"; then
        rm -f -- "$tmp"
        die "cannot write $dest"
    fi
}

# Dies unless sample_defs/$1 is a plain relative path whose destination in the checkout cannot be
# redirected: no control characters or empty/dot components in the name, no symlink along the
# destination, an existing intermediate component is a directory and an existing final one a
# regular file, and the resolved path stays inside sample_defs/.
check_def_path() {
    local rel="$1" cur="$DEFS_DST" part resolved last i
    local -a parts
    case "$rel" in
        "" | /* | */ | *//* | ./* | */./* | */. | . | ../* | */../* | *..)
            die "sample_defs path is unsafe: $(printf '%q' "$rel")" ;;
        *[[:cntrl:]]*)
            die "sample_defs path contains a control character: $(printf '%q' "$rel")" ;;
    esac
    IFS=/ read -ra parts <<< "$rel"
    last=$(( ${#parts[@]} - 1 ))
    for i in "${!parts[@]}"; do
        part="${parts[$i]}"
        cur="$cur/$part"
        [[ ! -L "$cur" ]] || die "$cur is a symlink; refusing to touch sample_defs/$rel through it"
        if [[ -e "$cur" ]]; then
            if (( i < last )); then
                [[ -d "$cur" ]] || die "$cur is not a directory; cannot place sample_defs/$rel"
            else
                [[ -f "$cur" ]] || die "$cur is not a regular file; cannot place sample_defs/$rel"
            fi
        fi
    done
    resolved="$(realpath -m -- "$DEFS_DST/$rel")"
    case "$resolved" in
        "$DEFS_DST_REAL"/*) ;;
        *) die "sample_defs/$rel resolves to $resolved, outside $DEFS_DST_REAL" ;;
    esac
}

# The source tree is plain files: a symlink or special file there is an error, and hidden files
# (.git, editor lock files) and editor swap/backup files are not configuration and are skipped.
readonly -a DEFS_IGNORED=(\( -name '.*' -o -name '*~' -o -name '*.swp' -o -name '*.swo'
    -o -name '*.swx' -o -name '#*#' \))
odd_def="$(find "$DEFS_SRC" -mindepth 1 "${DEFS_IGNORED[@]}" -prune -o ! -type f ! -type d -print -quit)"
[[ -z "$odd_def" ]] || die "$odd_def is a symlink or special file; only regular files are installed from $DEFS_SRC"
defs=()
while IFS= read -r -d '' def_file; do
    defs+=("${def_file#"$DEFS_SRC"/}")
done < <(find "$DEFS_SRC" -mindepth 1 "${DEFS_IGNORED[@]}" -prune -o -type f -print0 | sort -z)
for def in "${defs[@]}"; do
    check_def_path "$def"
    if git -C "$SRC_DIR" ls-files --error-unmatch "sample_defs/$def" >/dev/null 2>&1; then
        die "sample_defs/$def is a stock cFS file; ironbird's override must use another name"
    fi
done

# What an earlier run installed and cFS does not track, and is no longer in tools/cfs/sample_defs.
old_defs=()
stale_defs=()
if [[ -f "$DEFS_MANIFEST" ]]; then
    while IFS= read -r old_def || [[ -n "$old_def" ]]; do
        [[ -n "$old_def" ]] || continue
        check_def_path "$old_def"
        old_defs+=("$old_def")
        if git -C "$SRC_DIR" ls-files --error-unmatch "sample_defs/$old_def" >/dev/null 2>&1; then
            continue
        fi
        if [[ ! -f "$DEFS_SRC/$old_def" && -e "$DEFS_DST/$old_def" ]]; then
            stale_defs+=("$old_def")
        fi
    done < "$DEFS_MANIFEST"
fi

for old_def in ${stale_defs[@]+"${stale_defs[@]}"}; do
    rm -f -- "${DEFS_DST:?}/$old_def"
    # and the directories it leaves empty (the stock ones are never empty)
    old_dir="$(dirname "$DEFS_DST/$old_def")"
    while [[ "$old_dir" != "$DEFS_DST" ]] && rmdir "$old_dir" 2>/dev/null; do
        old_dir="$(dirname "$old_dir")"
    done
    echo "build.sh: removed ironbird cFS configuration sample_defs/$old_def (no longer in tools/cfs/sample_defs)"
done

{ printf '%s\n' ${old_defs[@]+"${old_defs[@]}"} "${defs[@]}"; } | sort -u | write_atomic "$DEFS_MANIFEST"
for def in "${defs[@]}"; do
    mkdir -p "$(dirname "$DEFS_DST/$def")"
    cp -f -- "$DEFS_SRC/$def" "$DEFS_DST/$def"
    echo "build.sh: installed ironbird cFS configuration sample_defs/$def"
done
printf '%s\n' "${defs[@]}" | write_atomic "$DEFS_MANIFEST"

cd "$SRC_DIR"
make "${CFS_CONFIG}.prep"
make -j"$JOBS" "${CFS_CONFIG}.install"

readonly EXE_DIR="$SRC_DIR/build-${CFS_CONFIG}/exe/cpu1"
[[ -x "$EXE_DIR/core-cpu1" ]] || die "build finished but $EXE_DIR/core-cpu1 is missing"
for app_dir in "$APPS_SRC"/*/; do
    [[ -d "$app_dir" ]] || continue
    app="$(basename "$app_dir")"
    [[ -f "$EXE_DIR/cf/$app.so" ]] || die "build finished but $EXE_DIR/cf/$app.so is missing"
done

# Record exactly what was built (bundle commit + every submodule commit), only now that the
# build succeeded.
{
    echo "bundle_url=$CFS_URL"
    echo "bundle_ref=$actual_ref"
    echo "bundle_commit=$actual_commit"
    echo "config=$CFS_CONFIG"
    echo "submodules:"
    git -C "$SRC_DIR" submodule status | sed 's/^/  /'
} | write_atomic "$CFS_BUILD_DIR/PINNED_VERSION"
echo "build.sh: recorded $CFS_BUILD_DIR/PINNED_VERSION"
echo "build.sh: OK - $EXE_DIR/core-cpu1 (cFS $actual_ref, $actual_commit)"
