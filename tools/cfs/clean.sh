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
# Only segments whose key derives from this run directory's key files are touched, and
# ipcrm refuses segments owned by other users.
#
# Environment overrides:
#   CFS_BUILD_DIR  root used by build.sh (default /home/user/build/cfs)
set -euo pipefail

CFS_BUILD_DIR="${CFS_BUILD_DIR:-/home/user/build/cfs}"
readonly RUN_DIR="${1:-$CFS_BUILD_DIR/run/cpu1}"

for name in .cdskeyfile .resetkeyfile .reservedkeyfile; do
    keyfile="$RUN_DIR/$name"
    [[ -e "$keyfile" ]] || continue
    # glibc ftok(path, 'R'): 'R' << 24 | (st_dev & 0xff) << 16 | (st_ino & 0xffff)
    read -r dev ino < <(stat -c '%d %i' "$keyfile")
    key=$(( (0x52 << 24) | ((dev & 0xff) << 16) | (ino & 0xffff) ))
    ipcrm -M "$key" 2>/dev/null || true
done
