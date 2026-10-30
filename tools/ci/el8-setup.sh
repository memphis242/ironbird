#!/usr/bin/env bash
# Prepare an Enterprise Linux 8 container (oraclelinux:8, redhat/ubi8) for ironbird CI.
#
# Run as root, once, inside the container. Installs everything the lint/type/test job and the
# real-cFS job need from the distribution's own repositories (no subscription: UBI and Oracle
# Linux repositories are public), then creates the unprivileged user that every later CI step
# runs as. ironbird and cFS must work without privileges, so CI never runs them as root.
#
# Python comes from the EL8 AppStream (python3.11, the interpreter the target servers get from
# `dnf install python3.11`), never from a downloaded build, so packaging (pip, venv, wheel
# tags for glibc 2.28) is what a user on RHEL 8 sees.
#
# Environment overrides:
#   CI_USER  unprivileged user to create (default ci)
#   CI_UID   its uid (default 1500)
set -euo pipefail

readonly CI_USER="${CI_USER:-ci}"
readonly CI_UID="${CI_UID:-1500}"

# Package -> why. The consistency test (tests/test_ci_el8.py) checks that every external tool
# tools/cfs/*.sh needs is provided by something in this list.
readonly PACKAGES=(
    python3.11        # interpreter
    python3.11-pip    # pip + ensurepip for `python3.11 -m venv`
    gcc               # cFS (gcc 8.5 on EL8; cFS builds with -Werror)
    make              # cFS
    cmake             # cFS (AppStream cmake)
    git               # actions/checkout and tools/cfs/build.sh (clone + submodules)
    tar               # actions/cache and checkout
    gzip              # actions/cache
    util-linux        # flock, setpriv, ipcrm (tools/cfs/run.sh, clean.sh), runuser
    shadow-utils      # useradd
    procps-ng         # ps, pgrep (process hygiene in tests)
    findutils         # find, xargs
    diffutils         # diff
    which             # which
    ca-certificates   # TLS for git clone and pip
    glibc-langpack-en # en_US.UTF-8, the locale users normally have
)

die() { echo "el8-setup.sh: error: $*" >&2; exit 1; }

[[ "$(id -u)" -eq 0 ]] || die "must run as root (it installs packages and creates the CI user)"
[[ "$CI_UID" =~ ^[1-9][0-9]*$ ]] || die "CI_UID must be a positive number, got '$CI_UID'"
command -v dnf >/dev/null || die "'dnf' not found; this script is for Enterprise Linux containers"

# shellcheck disable=SC1091
source /etc/os-release
[[ "${VERSION_ID%%.*}" == "8" ]] || die "expected Enterprise Linux 8, found $PRETTY_NAME"
echo "el8-setup.sh: $PRETTY_NAME"

dnf -y --setopt=install_weak_deps=False --nodocs install "${PACKAGES[@]}"
dnf clean all

if ! id "$CI_USER" >/dev/null 2>&1; then
    useradd --create-home --uid "$CI_UID" --shell /bin/bash "$CI_USER"
fi
[[ "$(id -u "$CI_USER")" -ne 0 ]] || die "'$CI_USER' resolves to uid 0"

# What the build actually runs on, recorded in the job log.
python3.11 --version
gcc --version | head -n 1
cmake --version | head -n 1
git --version
ldd --version | head -n 1
flock --version
setpriv --version
bash --version | head -n 1
[[ "$(python3.11 -c 'import sys; print(sys.version_info[:2] == (3, 11))')" == "True" ]] \
    || die "python3.11 is not Python 3.11"
echo "el8-setup.sh: OK - CI user '$CI_USER' (uid $(id -u "$CI_USER"))"
