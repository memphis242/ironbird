"""The EL8 CI container (tools/ci/el8-setup.sh) must provide every external tool the cFS scripts
need, and the scripts must stay syntactically valid for the EL8 bash (4.4). These are static
checks; the container itself runs in CI (.github/workflows/ci.yml, jobs ``el8-*``)."""

from __future__ import annotations

import importlib.util
import os
import re
import shutil
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO = Path(__file__).resolve().parent.parent
SETUP = REPO / "tools" / "ci" / "el8-setup.sh"
CACHE_KEY = REPO / "tools" / "ci" / "cache-key.sh"
CI_SCRIPTS = [SETUP, CACHE_KEY]
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


def test_el8_setup_installs_the_runtime_libraries_of_the_pyright_node() -> None:
    """The PyPI Node.js wheel links libstdc++ and libgcc; libatomic covers Node builds that
    link it (the loader exits 127 before pyright starts if a library is missing)."""
    assert {"libstdc++", "libatomic"} <= _setup_packages()


def test_pyright_runs_on_the_pinned_pypi_node() -> None:
    """Ubuntu and EL8 run pyright on the same Node.js (a manylinux_2_28 wheel, so glibc 2.28
    is enough), pinned exactly; the `nodejs` extra makes pyright prefer it over any node in PATH."""
    pyproject = (REPO / "pyproject.toml").read_text()
    assert re.search(r'"pyright\[nodejs\][^"]*"', pyproject), "dev extra must be pyright[nodejs]"
    constraints = (REPO / "constraints-dev.txt").read_text()
    assert re.search(r"^nodejs-wheel-binaries==\d+\.\d+\.\d+$", constraints, re.M)
    assert importlib.util.find_spec("nodejs_wheel") is not None, "dev install lacks the wheel"


def test_cache_key_script_prints_the_pinned_cfs_commit() -> None:
    done = subprocess.run([str(CACHE_KEY)], capture_output=True, text=True, check=False, timeout=30)
    assert done.returncode == 0, done.stderr
    assert re.fullmatch(r"commit=[0-9a-f]{40}\n", done.stdout), done.stdout


def test_cache_key_script_rejects_unknown_arguments() -> None:
    done = subprocess.run(
        [str(CACHE_KEY), "--bogus"], capture_output=True, text=True, check=False, timeout=30
    )
    assert done.returncode != 0
    assert "usage" in done.stderr


def test_unavailable_packages_exit_with_the_code_the_workflow_tolerates() -> None:
    """The best-effort image may skip its job only for el8-setup.sh's 'packages unavailable'
    exit code; the workflow and the script must agree on it."""
    code = re.search(r"^readonly EXIT_PACKAGES_UNAVAILABLE=(\d+)$", SETUP.read_text(), re.M)
    assert code is not None
    assert code.group(1) not in {"0", "1", "2"}
    assert f"-eq {code.group(1)} ]" in WORKFLOW.read_text()


# A pipeline stage that stops reading early closes the pipe while the writer may still be
# writing; the writer dies of SIGPIPE and, under pipefail, the pipeline (and the `set -e`
# script) exits 141. Whether it happens depends on scheduling, so it is a flaky failure:
# `ldd --version | head -n 1` failed the EL8 setup step this way. Early-exit readers:
#   head; grep -q/--quiet/--silent/-m/--max-count/-l; sed with a q command; awk with exit;
#   cmp -s/--quiet/--silent; diff -q/--brief; the `read` builtin as a whole stage.
_SINGLE_PIPE = re.compile(r"(?<!\|)\|(?!\|)&?")
_GREP_EARLY = re.compile(
    r"\s(-[A-Za-z]*[qml][A-Za-z0-9]*|--(quiet|silent|max-count|files-with-matches)\b)"
)
_SED_QUIT = re.compile(r"(?:^|[\s'\";{])(?:\d+|\$|/[^/]*/)?q\d*(?=$|[\s'\";}])")
_CMP_QUIET = re.compile(r"\s(-\w*s\w*|--(quiet|silent))\b")
_DIFF_QUIET = re.compile(r"\s(-\w*q\w*|--brief)\b")


def _early_exit_stage(stage: str) -> bool:
    """True if a pipeline stage (text after a single `|`) can stop reading before EOF."""
    words = stage.lstrip("({! \t").split(None, 1)
    if not words:
        return False
    command, rest = words[0], " " + (words[1] if len(words) > 1 else "")
    match command:
        case "head" | "read":
            return True
        case "grep" | "egrep" | "fgrep":
            return _GREP_EARLY.search(rest) is not None
        case "sed":
            return _SED_QUIT.search(rest) is not None
        case "awk" | "gawk":
            return re.search(r"\bexit\b", rest) is not None
        case "cmp":
            return _CMP_QUIET.search(rest) is not None
        case "diff":
            return _DIFF_QUIET.search(rest) is not None
        case _:
            return False


def _logical_lines(text: str) -> list[tuple[int, str]]:
    """Comment-free shell lines, with `\\` and trailing-`|` continuations joined."""
    lines: list[tuple[int, str]] = []
    pending = ""
    first = 0
    for number, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if line.startswith("#"):
            continue
        if not pending:
            first = number
        joined = (pending + " " + line).strip()
        if joined.endswith("\\"):
            pending = joined[:-1]
        elif re.search(r"(?<!\|)\|$", joined):
            pending = joined
        else:
            pending = ""
            lines.append((first, joined))
    if pending:
        lines.append((first, pending))
    return lines


def _sigpipe_offenders(text: str) -> list[str]:
    offenders: list[str] = []
    for number, line in _logical_lines(text):
        stages = _SINGLE_PIPE.split(line)[1:]
        if any(_early_exit_stage(stage) for stage in stages):
            offenders.append(f"{number}: {line}")
    return offenders


@pytest.mark.parametrize("script", [*CI_SCRIPTS, *CFS_SCRIPTS], ids=lambda p: p.name)
def test_pipefail_scripts_do_not_pipe_into_early_exit_readers(script: Path) -> None:
    text = script.read_text()
    if "pipefail" not in text:
        pytest.skip("script does not set pipefail")
    assert not _sigpipe_offenders(text), f"{script.name}: SIGPIPE-prone pipelines under pipefail"


def test_workflow_run_blocks_do_not_pipe_into_early_exit_readers() -> None:
    offenders: list[str] = []
    for name, job in _jobs(_load(WORKFLOW.read_text())).items():
        for step in _steps(job):
            run = step.get("run")
            if isinstance(run, str):
                offenders += [f"{name}/{step.get('name')}: {o}" for o in _sigpipe_offenders(run)]
    assert not offenders, offenders


@pytest.mark.parametrize(
    "line",
    [
        "ldd --version | head -n 1",
        "cmd | grep -q x",
        "cmd | grep -qE x",
        "cmd | grep -iq x",
        "cmd | grep --quiet x",
        "cmd | grep --silent x",
        "cmd | grep -m1 x",
        "cmd | grep -m 1 x",
        "cmd | grep --max-count=1 x",
        "cmd | grep -l x",
        "cmd | sed -n 1q",
        "cmd | sed -n '1q'",
        "cmd | sed q",
        "cmd | sed '/x/q'",
        "cmd | sed -n '/a/{p;q}'",
        "cmd | sed '5q;d'",
        "cmd | sed -e 'q1'",
        "cmd | awk '{print; exit}'",
        "cmd | awk 'NR==1{print;exit}'",
        "cmd | cmp -s - file",
        "cmd | cmp --quiet - file",
        "cmd | diff -q - file",
        "cmd | read -r first",
        "cmd |& head",
        "cmd |\n  head -n 1",
        "cmd \\\n  | head -n 1",
        "a | b || c | head",
        "x=$(cmd | head -n 1)",
        "cmd | (head -n 1)",
    ],
)
def test_sigpipe_check_flags_early_exit_readers(line: str) -> None:
    assert _sigpipe_offenders(line), line


@pytest.mark.parametrize(
    "line",
    [
        "x || head -n 1",
        "cmd | grep -c x",
        "cmd | grep -v q",
        "cmd | grep -E 'a|b'",
        "cmd | sed -n 's/q/x/p'",
        "cmd | sed -e 's/a/b/'",
        "cmd | awk '{print $1}'",
        "cmd | cmp - file",
        "cmd | while read -r x; do :; done",
        "# cmd | head -n 1",
        "head -n 1 file",
        'output="$(cmd)"',
    ],
)
def test_sigpipe_check_ignores_safe_lines(line: str) -> None:
    assert not _sigpipe_offenders(line), line


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash not available")
@pytest.mark.parametrize("script", [*CI_SCRIPTS, *CFS_SCRIPTS], ids=lambda p: p.name)
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


# --- The workflow itself ---------------------------------------------------------------------
# The EL8 jobs are only meaningful if every step really runs as the unprivileged user. That
# rests on three facts that a one-line YAML edit can silently undo, so they are parsed (not
# grepped) and checked: the default shell, the absence of per-step shell overrides, and the
# id check that starts every unprivileged step and fails loudly if privileges ever come back.

ID_CHECK = 'test "$(id -u)" -ne 0'
EL8_JOBS = ("el8-checks", "el8-cfs")
# Root steps (`shell: bash`) may only do bootstrap, ownership and key work; anything that
# runs the project's code or tools belongs to the unprivileged user.
_PROJECT_CODE = ("pytest", "tools/cfs/", "python -m pip", "pyright", "ruff", "-m venv")


def _load(text: str) -> dict[str, Any]:
    document = yaml.safe_load(text)
    assert isinstance(document, dict)
    return document


def _jobs(workflow: dict[str, Any]) -> dict[str, dict[str, Any]]:
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    return jobs


def _steps(job: dict[str, Any]) -> list[dict[str, Any]]:
    steps = job["steps"]
    assert isinstance(steps, list)
    return steps


def _workflow_violations(text: str) -> list[str]:
    """Everything wrong with the EL8 jobs' privilege handling; empty if the workflow is sound."""
    problems: list[str] = []
    jobs = _jobs(_load(text))
    for name in EL8_JOBS:
        job = jobs.get(name)
        if job is None:
            problems.append(f"{name}: job missing")
            continue
        defaults = job.get("defaults")
        shell = defaults.get("run", {}).get("shell") if isinstance(defaults, dict) else None
        if not isinstance(shell, str):
            problems.append(f"{name}: no defaults.run.shell")
        else:
            words = shell.split()
            if not shell.startswith("setpriv --reuid=ci"):
                problems.append(f"{name}: default shell does not start with setpriv --reuid=ci")
            if "--no-new-privs" not in words:
                problems.append(f"{name}: default shell lacks --no-new-privs")
            for word in words:
                if re.match(r"--(re|e|s)?[ug]id=", word) and word not in (
                    "--reuid=ci",
                    "--regid=ci",
                ):
                    problems.append(f"{name}: default shell sets '{word}'")
        gated = False
        for step in _steps(job):
            label = f"{name}/{step.get('name', step.get('uses'))}"
            run = step.get("run")
            if step.get("id") == "setup":
                gated = True
            else:
                condition = str(step.get("if", ""))
                if (
                    gated
                    and "always()" not in condition
                    and "steps.setup.outputs.ready" not in condition
                ):
                    problems.append(f"{label}: not gated on steps.setup.outputs.ready")
            if not isinstance(run, str):
                continue
            if "shell" in step:
                if step["shell"] != "bash":
                    problems.append(f"{label}: unexpected shell {step['shell']!r}")
                if any(token in run for token in _PROJECT_CODE):
                    problems.append(f"{label}: runs project code as root")
                continue
            first = run.strip().splitlines()[0].strip() if run.strip() else ""
            if first != ID_CHECK:
                problems.append(f"{label}: does not begin with the id check (begins {first!r})")
    cfs_env = jobs.get("el8-cfs", {}).get("env", {})
    if cfs_env.get("IRONBIRD_CFS") != "1":
        problems.append("el8-cfs: env IRONBIRD_CFS must be the string '1'")
    return problems


def test_el8_jobs_run_their_steps_unprivileged() -> None:
    assert _workflow_violations(WORKFLOW.read_text()) == []


def _defaults_block(text: str) -> str:
    match = re.search(r"    defaults:\n      run:\n        shell: >-\n(?:          .*\n)+", text)
    assert match is not None
    return match.group(0)


def _replace_once(old: str, new: str) -> Callable[[str], str]:
    def mutate(text: str) -> str:
        assert text.count(old) >= 1, f"mutation anchor not found: {old!r}"
        return text.replace(old, new, 1)

    return mutate


def _comment_out_defaults(text: str) -> str:
    block = _defaults_block(text)
    return text.replace(block, "".join(f"    # {line.strip()}\n" for line in block.splitlines()), 1)


_EL8_BENCH = (
    "      - name: Benchmarks (serial, so the performance ceilings are enforced)\n"
    "        if: steps.setup.outputs.ready == 'true'\n"
)
_EL8_LINT = "      - name: Lint\n        if: steps.setup.outputs.ready == 'true'\n        run: |\n"

MUTATIONS: dict[str, Callable[[str], str]] = {
    "benchmarks step overrides the shell": _replace_once(
        _EL8_BENCH, _EL8_BENCH + "        shell: bash\n"
    ),
    "defaults replaced by a comment mentioning setpriv": _comment_out_defaults,
    "IRONBIRD_CFS removed": _replace_once('\n      IRONBIRD_CFS: "1"\n', "\n"),
    "IRONBIRD_CFS set to 0": _replace_once(
        '\n      IRONBIRD_CFS: "1"\n', '\n      IRONBIRD_CFS: "0"\n'
    ),
    "id check followed by || true": _replace_once(
        f"          {ID_CHECK}\n", f"          {ID_CHECK} || true\n"
    ),
    "--reuid=0 appended": _replace_once("--no-new-privs\n", "--no-new-privs --reuid=0\n"),
    "--reuid=root appended": _replace_once("--no-new-privs\n", "--no-new-privs --reuid=root\n"),
    "--no-new-privs removed": _replace_once(" --no-new-privs\n", "\n"),
    "default shell no longer setpriv": _replace_once("setpriv --reuid=ci", "env --reuid=ci"),
    "lint step loses the id check": _replace_once(_EL8_LINT + f"          {ID_CHECK}\n", _EL8_LINT),
    "root shell on a pytest step": _replace_once(
        "        run: |\n          " + ID_CHECK + "\n          pytest -n auto\n",
        "        shell: bash\n        run: |\n          "
        + ID_CHECK
        + "\n          pytest -n auto\n",
    ),
    "step runs before the setup gate": _replace_once(
        "      - name: Create venv (EL8 python3.11)\n"
        "        if: steps.setup.outputs.ready == 'true'\n",
        "      - name: Create venv (EL8 python3.11)\n",
    ),
}


@pytest.mark.parametrize("mutation", sorted(MUTATIONS))
def test_workflow_check_catches_privilege_regressions(mutation: str) -> None:
    mutated = MUTATIONS[mutation](WORKFLOW.read_text())
    assert mutated != WORKFLOW.read_text()
    assert _workflow_violations(mutated), f"not caught: {mutation}"
