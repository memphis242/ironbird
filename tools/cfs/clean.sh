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
#   - it is owned by the uid cFS ran as (root's ipcrm would otherwise remove anyone's):
#     the invoking uid, or, when invoked as root, the CFS_RUN_UID that run.sh drops to. Uids
#     are compared as numbers from /proc/sysvipc/shm; user names (which ipcs truncates to 10
#     characters) are only used in messages,
#   - nothing is attached to it (a live cFS instance keeps its segments attached).
# Segments that match the key but fail another test are reported and left alone.
#
# Environment overrides:
#   CFS_BUILD_DIR  root used by build.sh (default /home/user/build/cfs)
#   CFS_RUN_UID    uid cFS runs as when run.sh is started as root (default 65534)
#   CFS_SHM_TABLE  the kernel's shared-memory table (default /proc/sysvipc/shm); tests point
#                  it at a fixture file
set -euo pipefail

CFS_BUILD_DIR="${CFS_BUILD_DIR:-/home/user/build/cfs}"
readonly RUN_DIR="${1:-$CFS_BUILD_DIR/run/cpu1}"
CFS_RUN_UID="${CFS_RUN_UID:-65534}"
# The uid whose segments are ours: whoever cFS ran as. run.sh started as root drops to
# CFS_RUN_UID, so a root-invoked clean.sh compares against that uid.
if [[ "$(id -u)" -eq 0 ]]; then
    me="$CFS_RUN_UID"
else
    me="$(id -u)"
fi
readonly me
if [[ ! "$me" =~ ^[0-9]+$ ]]; then
    echo "clean.sh: CFS_RUN_UID must be a number, got '$me'" >&2
    exit 2
fi

readonly SHM_TABLE="${CFS_SHM_TABLE:-/proc/sysvipc/shm}"
command -v ipcrm >/dev/null && [[ -r "$SHM_TABLE" ]] || exit 0  # nothing to clean up with

segments="$(cat "$SHM_TABLE" 2>/dev/null || true)"

for name in .cdskeyfile .resetkeyfile .reservedkeyfile; do
    keyfile="$RUN_DIR/$name"
    [[ -e "$keyfile" ]] || continue
    # glibc ftok(path, 'R'): 'R' << 24 | (st_dev & 0xff) << 16 | (st_ino & 0xffff)
    read -r dev ino < <(stat -c '%d %i' "$keyfile")
    key=$(( (0x52 << 24) | ((dev & 0xff) << 16) | (ino & 0xffff) ))
    # /proc/sysvipc/shm rows: key shmid perms size cpid lpid nattch uid gid cuid cgid ...
    # (the key is decimal; the header row and malformed rows fail the numeric test below)
    while read -r seg_key shmid _perms _size _cpid _lpid nattch owner _rest; do
        [[ "$seg_key" =~ ^-?[0-9]+$ && "$seg_key" -eq "$key" ]] || continue
        if [[ "$owner" != "$me" ]]; then
            echo "clean.sh: leaving segment $shmid (key $key): owned by uid $owner, not uid $me" >&2
        elif [[ "$nattch" != "0" ]]; then
            echo "clean.sh: leaving segment $shmid (key $key): $nattch process(es) attached" >&2
        else
            ipcrm -m "$shmid" 2>/dev/null || echo "clean.sh: could not remove segment $shmid" >&2
        fi
    done <<< "$segments"
done
