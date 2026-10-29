#!/usr/bin/env bash
# Run the cFS core (core-cpu1) in the foreground as a normal user.
#
# cFS is started from a fresh copy of the staged exe directory
# ($CFS_BUILD_DIR/run/cpu1) so every run is a clean power-on and the build tree
# is never written to. The native Linux PSP must run with that directory as its
# working directory (it finds cfe_es_startup.scr and the app shared objects
# relative to it); files commanded to /cf land in <run dir>/cf.
#
# Background use:  tools/cfs/run.sh > cfs.log 2>&1 &     (stop with SIGINT/SIGTERM)
#
# OSAL is built in permissive mode, so no root, RT priorities or raised mqueue
# limits are needed. The one exception is a root shell (e.g. a container): the
# OSAL queue-depth truncation only runs for non-root users, and core-cpu1 aborts
# at SB pipe creation when /proc/sys/fs/mqueue/msg_max is small. In that case
# this script drops to an unprivileged uid (CFS_RUN_UID, default 65534) with
# setpriv; it never touches system settings.
#
# The staged startup script is trimmed to the verification set (CFS_APPS); the
# stock script also starts LC, CF, DS, FM, HK, HS, MM, SC, MD, CS and SBN, whose
# event traffic adds noise (SBN overflows its pipe with the reduced mqueue depth).
#
# Environment overrides:
#   CFS_BUILD_DIR  root used by build.sh (default /home/user/build/cfs)
#   CFS_CONFIG     cFS make configuration (default native_std)
#   CFS_APPS       space-separated apps to start (default "sch_lab ci_lab to_lab
#                  sample_app"); set to "ALL" to keep the stock startup script
#   CFS_RUN_UID    uid to drop to when started as root (default 65534)
set -euo pipefail

CFS_BUILD_DIR="${CFS_BUILD_DIR:-/home/user/build/cfs}"
CFS_CONFIG="${CFS_CONFIG:-native_std}"
CFS_RUN_UID="${CFS_RUN_UID:-65534}"
CFS_APPS="${CFS_APPS:-sch_lab ci_lab to_lab sample_app}"
readonly EXE_DIR="$CFS_BUILD_DIR/src/build-${CFS_CONFIG}/exe/cpu1"
readonly RUN_DIR="$CFS_BUILD_DIR/run/cpu1"

die() { echo "run.sh: error: $*" >&2; exit 1; }

[[ -x "$EXE_DIR/core-cpu1" ]] || die "$EXE_DIR/core-cpu1 not found; run tools/cfs/build.sh first"

# OSAL backs the cFE RAM disk (/ram) with the fixed directory /dev/shm/osal:RAM,
# shared by every cFS instance on the host. A leftover owned by another user (for
# example from an earlier root run) makes /ram unwritable.
readonly RAM_DIR="/dev/shm/osal:RAM"
if [[ "$(id -u)" -eq 0 ]]; then run_uid="$CFS_RUN_UID"; else run_uid="$(id -u)"; fi
if [[ -e "$RAM_DIR" && "$(stat -c %u "$RAM_DIR")" != "$run_uid" ]]; then
    die "$RAM_DIR exists and is not owned by uid $run_uid; remove it if no other cFS instance is running"
fi

# One instance per build dir: the run directory is wiped below, which would pull the rug from
# under a live instance. The lock is held on fd 9 for as long as cFS runs (the exec below keeps
# the descriptor), and released by the kernel when the process ends, however it ends.
command -v flock >/dev/null || die "'flock' is unavailable (util-linux); cannot guard against a second instance"
mkdir -p "$CFS_BUILD_DIR/run"
exec 9>"$CFS_BUILD_DIR/run/.run.lock"
flock -n 9 || die "another run.sh is already running against $CFS_BUILD_DIR; stop it first (lock: $CFS_BUILD_DIR/run/.run.lock)"

# The previous run's shared-memory segments are keyed off the directory deleted next.
[[ -d "$RUN_DIR" ]] && "$(dirname "$0")/clean.sh" "$RUN_DIR"
rm -rf "$RUN_DIR"
mkdir -p "$RUN_DIR"
cp -a "$EXE_DIR/." "$RUN_DIR/"
cd "$RUN_DIR"

if [[ "$CFS_APPS" != "ALL" ]]; then
    # Keep libraries (minus the functional-test assert lib) and the listed apps.
    awk -F'[ ,]+' -v apps=" $CFS_APPS " '
        /^[[:space:]]*(!|#|$)/ { print; next }
        $1 == "CFE_LIB" { if ($2 != "cfe_assert") print; next }
        $1 == "CFE_APP" { if (index(apps, " " $2 " ")) print; next }
        { print }
    ' "$EXE_DIR/cf/cfe_es_startup.scr" > cf/cfe_es_startup.scr
fi

if [[ "$(id -u)" -eq 0 ]]; then
    command -v setpriv >/dev/null || die "running as root and 'setpriv' is unavailable; run as a normal user"
    echo "run.sh: running as root; dropping to uid $CFS_RUN_UID" >&2
    chown -R "$CFS_RUN_UID:$CFS_RUN_UID" "$RUN_DIR"
    exec setpriv --reuid="$CFS_RUN_UID" --regid="$CFS_RUN_UID" --clear-groups ./core-cpu1 "$@"
fi
# exec so signals reach cFS directly.
exec ./core-cpu1 "$@"
