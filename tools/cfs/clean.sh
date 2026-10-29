#!/usr/bin/env bash
# Remove the SysV shared-memory segments the cFS native PSP leaves behind.
#
# The PSP keys three segments (CDS, reset area, user-reserved area) with
# ftok(".<name>keyfile", 'R') in its working directory, and never removes them on exit.
# run.sh recreates the run directory on every start, so each run gets new key files and
# strands the previous run's segments; once a key collides with a stranded segment of a
# different size the next start dies with "Cannot shmget ... Invalid argument". Run this
# against the previous run directory before it is deleted (run.sh does), and after a run
# to leave nothing behind.
#
# ftok keys keep only 8 bits of the device and 16 of the inode, so a key can collide with a
# segment that belongs to something else. A segment is therefore removed only when all of
# these hold:
#   - its key is the one derived from a key file in this run directory,
#   - it is owned by the invoking user (root's ipcrm would otherwise remove anyone's),
#   - nothing is attached to it (a live cFS instance keeps its segments attached).
# Segments that match the key but fail another test are reported and left alone.
#
# Environment overrides:
#   CFS_BUILD_DIR  root used by build.sh (default /home/user/build/cfs)
set -euo pipefail

CFS_BUILD_DIR="${CFS_BUILD_DIR:-/home/user/build/cfs}"
readonly RUN_DIR="${1:-$CFS_BUILD_DIR/run/cpu1}"
me="$(id -un)"
readonly me

command -v ipcs >/dev/null && command -v ipcrm >/dev/null || exit 0  # nothing to clean up with

segments="$(ipcs -m 2>/dev/null || true)"

for name in .cdskeyfile .resetkeyfile .reservedkeyfile; do
    keyfile="$RUN_DIR/$name"
    [[ -e "$keyfile" ]] || continue
    # glibc ftok(path, 'R'): 'R' << 24 | (st_dev & 0xff) << 16 | (st_ino & 0xffff)
    read -r dev ino < <(stat -c '%d %i' "$keyfile")
    key="$(printf '0x%08x' $(( (0x52 << 24) | ((dev & 0xff) << 16) | (ino & 0xffff) )))"
    # ipcs -m rows: key shmid owner perms bytes nattch [status]
    while read -r seg_key shmid owner _perms _bytes nattch _rest; do
        [[ "${seg_key,,}" == "$key" ]] || continue
        if [[ "$owner" != "$me" ]]; then
            echo "clean.sh: leaving segment $shmid (key $key): owned by $owner, not $me" >&2
        elif [[ "$nattch" != "0" ]]; then
            echo "clean.sh: leaving segment $shmid (key $key): $nattch process(es) attached" >&2
        else
            ipcrm -m "$shmid" 2>/dev/null || echo "clean.sh: could not remove segment $shmid" >&2
        fi
    done <<< "$segments"
done
