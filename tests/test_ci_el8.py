"""The EL8 CI container (tools/ci/el8-setup.sh) must provide every external tool the cFS scripts
need, and the scripts must stay syntactically valid for the EL8 bash (4.4). These are static
checks; the container itself runs in CI (.github/workflows/ci.yml, jobs ``el8-*``)."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
SETUP = REPO / "tools" / "ci" / "el8-setup.sh"
CFS_SCRIPTS = sorted((REPO / "tools" / "cfs").glob("*.sh"))
WORKFLOW = REPO / ".github" / "workflows" / "ci.yml"

# External command -> the EL8 package that ships it.
PROVIDER = {
    "git": "git",
    "make": "make",
    "cmake": "cmake",
    "gcc": "gcc",
    "flock": "util-linux",
    "setpriv": "util-linux",
    "ipcrm": "util-linux",
    "stat": "coreutils",  # in the base image
    "awk": "gawk",  # in the base image
}
BASE_IMAGE = {"coreutils", "gawk"}


def _setup_packages() -> set[str]:
    body = SETUP.read_text()
    match = re.search(r"readonly PACKAGES=\(\n(.*?)\n\)", body, re.DOTALL)
    assert match is not None, "PACKAGES array not found in el8-setup.sh"
    names: set[str] = set()
    for line in match.group(1).splitlines():
        entry = line.split("#", 1)[0].strip()
        if entry:
            names.add(entry)
    return names


def _commands_used(script: Path) -> set[str]:
    """Tools a cFS script names as a requirement: ``command -v X`` and the build.sh tool loop."""
    text = script.read_text()
    used = set(re.findall(r"command -v (\w+)", text))
    loop = re.search(r"for tool in ([\w ]+); do", text)
    if loop is not None:
        used |= set(loop.group(1).split())
    return used


def test_el8_setup_installs_every_tool_the_cfs_scripts_require() -> None:
    packages = _setup_packages()
    missing: list[str] = []
    for script in CFS_SCRIPTS:
        for command in sorted(_commands_used(script)):
            package = PROVIDER.get(command)
            assert package is not None, f"{script.name} needs '{command}': add it to PROVIDER"
            if package not in packages and package not in BASE_IMAGE:
                missing.append(f"{script.name}: {command} (package {package})")
    assert not missing, f"tools/ci/el8-setup.sh does not install: {missing}"


def test_el8_setup_installs_distro_python_311_and_pip() -> None:
    packages = _setup_packages()
    assert {"python3.11", "python3.11-pip"} <= packages


def test_el8_setup_installs_libatomic_for_the_pyright_node() -> None:
    """pyright runs on the Node.js that nodeenv downloads; the official Node binaries link
    libatomic.so.1, which EL8 base images lack (the loader exits 127 before pyright starts)."""
    assert "libatomic" in _setup_packages()


# A pipeline stage that stops reading early (head, grep -q/-m, sed q) closes the pipe while the
# writer may still be writing; the writer dies of SIGPIPE and, under pipefail, the pipeline (and
# the `set -e` script) exits 141. Whether it happens depends on scheduling, so it is a flaky
# failure: `ldd --version | head -n 1` failed the EL8 setup step this way.
_EARLY_EXIT_READER = re.compile(r"\|\s*(head\b|grep\s+-\w*[qm]|sed\s+(-n\s+)?['\"]?\d+q)")


@pytest.mark.parametrize("script", [SETUP, *CFS_SCRIPTS], ids=lambda p: p.name)
def test_pipefail_scripts_do_not_pipe_into_early_exit_readers(script: Path) -> None:
    text = script.read_text()
    if "pipefail" not in text:
        pytest.skip("script does not set pipefail")
    offending = [
        f"{number}: {line.strip()}"
        for number, line in enumerate(text.splitlines(), start=1)
        if not line.lstrip().startswith("#") and _EARLY_EXIT_READER.search(line)
    ]
    assert not offending, f"{script.name}: SIGPIPE-prone pipelines under pipefail: {offending}"


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash not available")
@pytest.mark.parametrize("script", [SETUP, *CFS_SCRIPTS], ids=lambda p: p.name)
def test_shell_scripts_parse(script: Path) -> None:
    done = subprocess.run(
        ["bash", "-n", str(script)], capture_output=True, text=True, check=False, timeout=30
    )
    assert done.returncode == 0, done.stderr


def test_el8_setup_refuses_to_run_unprivileged() -> None:
    if os.getuid() == 0:
        pytest.skip("only meaningful for a non-root caller")
    done = subprocess.run([str(SETUP)], capture_output=True, text=True, check=False, timeout=30)
    assert done.returncode != 0
    assert "must run as root" in done.stderr


def test_el8_jobs_run_their_steps_unprivileged() -> None:
    """Every EL8 job drops to the unprivileged user by default; root steps opt in explicitly."""
    text = WORKFLOW.read_text()
    jobs = {
        m.group(1): m.group(0)
        for m in re.finditer(r"^  ([\w-]+):\n(?:(?!^  \S).*\n?)*", text, re.M)
    }
    for job in ("el8-checks", "el8-cfs"):
        block = jobs[job]
        assert "setpriv --reuid=ci" in block, f"{job} must default to the unprivileged shell"
        assert 'test "$(id -u)" -ne 0' in block, f"{job} must assert it runs the tests as non-root"
