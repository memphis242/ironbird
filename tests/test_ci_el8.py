"""The EL8 CI container (tools/ci/el8-setup.sh) must provide every external tool the cFS scripts
need, and the scripts must stay syntactically valid for the EL8 bash (4.4). These are static
checks; the container itself runs in CI (.github/workflows/ci.yml, jobs ``el8-*``)."""

from __future__ import annotations

import importlib.util
import os
import re
import shlex
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
CHECK_PACKAGE = REPO / "tools" / "ci" / "check-package.sh"
AUDIT_DEPS = REPO / "tools" / "ci" / "audit-deps.sh"
CI_SCRIPTS = [SETUP, CACHE_KEY, CHECK_PACKAGE, AUDIT_DEPS]
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
EL8_JOBS = ("el8-checks", "el8-cfs", "el8-audit")
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


def _replace_last(old: str, new: str) -> Callable[[str], str]:
    def mutate(text: str) -> str:
        head, found, tail = text.rpartition(old)
        assert found, f"mutation anchor not found: {old!r}"
        return head + new + tail

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


# --- fs.mqueue.msg_max for the real-cFS jobs ---------------------------------------------------
# A non-root cFS has OSAL truncate every queue to fs.mqueue.msg_max (10 by default), so TO_LAB's
# telemetry pipe would be 10 deep instead of the depth it asks for and the generator tests
# (tests/cfs/test_cfs_generator.py, strict only at the full depth) would run in their lenient mode.
# Every job that runs real cFS must therefore raise it, a dropped or neutered line must not go
# unnoticed, and the tests must fail (not fall back to the lenient mode) when it did not take
# effect: IRONBIRD_CFS_REQUIRE_FULL_DEPTH=1 in both jobs.

TO_LAB_VALUES = REPO / "tools" / "cfs" / "sample_defs" / "cpu1_to_lab_internal_cfg_values.h"
REQUIRE_FULL_DEPTH_ENV = "IRONBIRD_CFS_REQUIRE_FULL_DEPTH"
CFS_ENV = "IRONBIRD_CFS"
_pipe_depth = re.search(
    r"^#define\s+IRONBIRD_TO_LAB_CFGVAL_TLM_PIPE_DEPTH\s+(\d+)\b", TO_LAB_VALUES.read_text(), re.M
)
assert _pipe_depth is not None, f"TO_LAB's telemetry pipe depth not defined in {TO_LAB_VALUES}"
MIN_MSG_MAX = int(_pipe_depth.group(1))  # the depth TO_LAB asks for
_MSG_MAX_SETTING = re.compile(r"fs\.mqueue\.msg_max=(\d+)")
# the step fails when the value read back from /proc is below the threshold (captured)
_MSG_MAX_READ_BACK = re.compile(
    r"^\s*test \"\$\(cat /proc/sys/fs/mqueue/msg_max\)\" -ge (\d+)\s*$", re.M
)
_SYSCTL_COMMAND = re.compile(r"^\s*sudo sysctl -w fs\.mqueue\.msg_max=(\d+)\s*$", re.M)
_MSG_MAX_ECHO = re.compile(
    r"^\s*echo \"fs\.mqueue\.msg_max = \$\(cat /proc/sys/fs/mqueue/msg_max\)\"\s*$", re.M
)
# `set +e`, `set +o errexit`, `set +eu`: anything that turns shell options off
_SHELL_OPTIONS_OFF = re.compile(r"^\s*set\s+\+", re.M)
# step or job keys that let a failing (or never-run) step go unnoticed
_BYPASS_KEYS = ("if", "continue-on-error", "shell")
EL8_TESTS_CONDITION = "steps.setup.outputs.ready == 'true'"


def _container_msg_max_values(options: str) -> tuple[list[int], bool]:
    """The ``fs.mqueue.msg_max`` values that ``docker create`` options set through ``--sysctl``
    (``--sysctl K=V``, ``--sysctl=K=V``, quoted or not) and whether every mention of msg_max in
    ``options`` was one of those."""
    try:
        words = shlex.split(options)
    except ValueError:
        return [], False
    values: list[int] = []
    for i, word in enumerate(words):
        if word == "--sysctl" and i + 1 < len(words):
            setting = words[i + 1]
        elif word.startswith("--sysctl="):
            setting = word[len("--sysctl=") :]
        else:
            continue
        match = _MSG_MAX_SETTING.fullmatch(setting)
        if match is not None:
            values.append(int(match.group(1)))
    return values, len(values) == options.count("msg_max")


def _runs_pytest(run: str) -> bool:
    return re.search(r"\bpytest\b", run) is not None


# The one way the real-cFS tests may be started: no selection (-k, --deselect, a narrower -m or
# path), no plugin or capture switches, no early stop, nothing before or after the command.
_CFS_TEST_COMMAND = ["pytest", "tests/cfs", "-m", "cfs", "-n0", "-v", "-rP"]
_BASETEMP_OPTION = re.compile(r"--basetemp=[\w$/.{}-]+")
# `env` options and assignments that the el8-cfs default shell may carry
_SHELL_ENV_NAMES = frozenset({"HOME", "USER", "LOGNAME", "PATH"})


def _test_command_violations(label: str, name: str, run: str) -> list[str]:
    lines = [line for _, line in _logical_lines(run)]
    problems: list[str] = []
    if lines[:-1] != ([ID_CHECK] if name == "el8-cfs" else []):
        problems.append(f"{label}: the step runs more than the cFS test command")
    lexer = shlex.shlex(lines[-1] if lines else "", posix=True, punctuation_chars=True)
    lexer.whitespace_split = True
    try:
        tokens = list(lexer)
    except ValueError:
        tokens = []
    expected = len(_CFS_TEST_COMMAND) + 1
    if not (
        len(tokens) == expected
        and tokens[: expected - 1] == _CFS_TEST_COMMAND
        and _BASETEMP_OPTION.fullmatch(tokens[-1])
    ):
        problems.append(
            f"{label}: the test command is not exactly '{' '.join(_CFS_TEST_COMMAND)} "
            f"--basetemp=DIR' (got {lines[-1] if lines else ''!r})"
        )
    return problems


def _shell_env_violations(shell: str) -> list[str]:
    """The el8-cfs default shell may set only HOME, USER, LOGNAME and PATH through ``env``; an
    option (``-i``, ``-u``, ``-S``, ``-C``) or any other name could clear or change the variables
    the tests are configured with."""
    words = shell.split()
    if words.count("env") != 1:
        return [
            f"el8-cfs: the default shell must use env exactly once (found {words.count('env')})"
        ]
    problems: list[str] = []
    for word in words[words.index("env") + 1 :]:
        if word == "bash":
            break
        name, equals, _ = word.partition("=")
        if not equals or name not in _SHELL_ENV_NAMES:
            problems.append(f"el8-cfs: the default shell's env has unexpected argument {word!r}")
    return problems


def _report_chars(command: str) -> str | None:
    """The characters of pytest's last ``-r`` option on ``command`` (the last one wins), or None.
    A short-option cluster such as ``-vrP`` ends in ``r`` plus its argument, which is the rest of
    the token or, when the token ends at ``r``, the next token."""
    tokens = command.split()
    chars: str | None = None
    for i, token in enumerate(tokens):
        if not token.startswith("-") or token.startswith("--"):
            continue
        position = token.find("r", 1)
        if position < 0:
            continue
        rest = token[position + 1 :]
        if not rest and i + 1 < len(tokens):
            rest = tokens[i + 1]
        chars = rest
    return chars


def _default_shell(container: dict[str, Any]) -> object:
    """``defaults.run.shell`` of a workflow or a job, or None."""
    defaults = container.get("defaults")
    run = defaults.get("run") if isinstance(defaults, dict) else None
    return run.get("shell") if isinstance(run, dict) else None


def _env_of(container: dict[str, Any]) -> dict[str, Any]:
    """``env`` of a workflow or a job, or an empty dict."""
    env = container.get("env")
    return env if isinstance(env, dict) else {}


def _cfs_job_violations(name: str, job: dict[str, Any], workflow: dict[str, Any]) -> list[str]:
    """What can skip, soften or hide the real-cFS test step of the job ``name``."""
    problems: list[str] = []
    workflow_env, job_env = _env_of(workflow), _env_of(job)
    if _default_shell(workflow) is not None:
        problems.append(f"{name}: the workflow sets defaults.run.shell")
    if "continue-on-error" in job:
        problems.append(f"{name}: the job sets continue-on-error")
    if job_env.get(REQUIRE_FULL_DEPTH_ENV) != "1":
        problems.append(f"{name}: env {REQUIRE_FULL_DEPTH_ENV} must be the string '1'")
    for scope, env in (("workflow", workflow_env), (f"{name} job", job_env)):
        for key in env:
            if str(key).startswith("PYTEST_"):
                problems.append(f"{scope} env sets {key}, which can change what pytest runs")
    shell = _default_shell(job)
    if name == "cfs" and shell is not None:
        problems.append("cfs: the job sets defaults.run.shell, which can lose errexit")
    if name == "el8-cfs" and (
        not isinstance(shell, str) or not any(re.fullmatch(r"-\w*e\w*", w) for w in shell.split())
    ):
        problems.append("el8-cfs: the default shell does not run with errexit (-e)")
    if name == "el8-cfs" and isinstance(shell, str):
        problems += _shell_env_violations(shell)
    for step in _steps(job):
        label = f"{name}/{step.get('name', step.get('uses'))}"
        step_env = step.get("env", {})
        if not isinstance(step_env, dict):
            step_env = {}
        for key in (REQUIRE_FULL_DEPTH_ENV, CFS_ENV):
            if step_env.get(key, "1") != "1":
                problems.append(f"{label}: the step overrides {key}")
        for key in step_env:
            if str(key).startswith("PYTEST_"):
                problems.append(f"{label}: the step env sets {key}")
        run = str(step.get("run", ""))
        if REQUIRE_FULL_DEPTH_ENV in run or "PYTEST_ADDOPTS" in run:
            problems.append(
                f"{label}: the script sets or unsets {REQUIRE_FULL_DEPTH_ENV} or PYTEST_ADDOPTS"
            )
        if _SHELL_OPTIONS_OFF.search(run):
            problems.append(f"{label}: the script turns shell options off (set +...)")
        for line in run.splitlines():
            if "msg_max" not in line and not re.search(r"\bsysctl\b", line):
                continue
            command = _SYSCTL_COMMAND.fullmatch(line.strip())
            if command is not None:
                if int(command.group(1)) < MIN_MSG_MAX:
                    problems.append(f"{label}: sysctl sets fs.mqueue.msg_max below {MIN_MSG_MAX}")
            elif not (_MSG_MAX_READ_BACK.fullmatch(line) or _MSG_MAX_ECHO.fullmatch(line)):
                problems.append(f"{label}: unrecognised use of msg_max: {line.strip()!r}")
        if not _runs_pytest(run):
            continue
        problems += _test_command_violations(label, name, run)
        for key in (REQUIRE_FULL_DEPTH_ENV, CFS_ENV):
            effective = step_env.get(key, job_env.get(key, workflow_env.get(key)))
            if effective != "1":
                problems.append(f"{label}: {key} is not the string '1' for the cFS test step")
        condition = step.get("if")
        if name == "cfs" and condition is not None:
            problems.append(f"{label}: the cFS test step has an if ({condition!r})")
        if name == "el8-cfs" and condition != EL8_TESTS_CONDITION:
            problems.append(f"{label}: the cFS test step's if is not {EL8_TESTS_CONDITION!r}")
        if "continue-on-error" in step:
            problems.append(f"{label}: the cFS test step sets continue-on-error")
        if "||" in run:
            problems.append(f"{label}: the cFS tests are followed by ||")
        chars = _report_chars(run)
        if chars is None or not ({"P", "A"} & set(chars)):
            problems.append(f"{label}: the last -r option hides the passed tests' mode line")
    return problems


def _msg_max_violations(text: str) -> list[str]:
    """Everything wrong with how the real-cFS jobs set ``fs.mqueue.msg_max``; empty if sound."""
    jobs = _jobs(_load(text))
    problems: list[str] = []
    # `run: ! cmd` parses to the same string as `run: cmd` but is not what a reader sees.
    tagged = sorted({str(e.tag) for e in yaml.parse(text) if getattr(e, "tag", None) is not None})
    if tagged:
        problems.append(f"the workflow uses explicit YAML tags {tagged}")

    container = jobs["el8-cfs"].get("container")
    options = container.get("options", "") if isinstance(container, dict) else ""
    values, understood = _container_msg_max_values(str(options))
    if not values or min(values) < MIN_MSG_MAX or not understood:
        problems.append(
            f"el8-cfs: container options lack --sysctl fs.mqueue.msg_max>={MIN_MSG_MAX} "
            f"(every value must, and every mention must be a plain --sysctl; found {values})"
        )

    workflow = _load(text)
    for name in ("cfs", "el8-cfs"):
        job = jobs[name]
        problems += _cfs_job_violations(name, job, workflow)
        tests = [s for s in _steps(job) if _runs_pytest(str(s.get("run", "")))]
        if len(tests) != 1:
            problems.append(f"{name}: expected one pytest step, found {len(tests)}")

    steps = _steps(jobs["cfs"])
    runs = [str(step.get("run", "")) for step in steps]
    raised = [
        i
        for i, run in enumerate(runs)
        if (m := _SYSCTL_COMMAND.search(run)) and int(m.group(1)) >= MIN_MSG_MAX
    ]
    ran = [i for i, run in enumerate(runs) if _runs_pytest(run)]
    if not raised:
        problems.append(f"cfs: no step runs sudo sysctl -w fs.mqueue.msg_max>={MIN_MSG_MAX}")
    elif not ran or raised[0] > ran[0]:
        problems.append("cfs: fs.mqueue.msg_max is not raised before the cFS tests run")
    else:
        step, run = steps[raised[0]], runs[raised[0]]
        for key in _BYPASS_KEYS:
            if key in step:
                problems.append(f"cfs: the sysctl step sets '{key}', which can hide its failure")
        sysctl, read_back = _SYSCTL_COMMAND.search(run), _MSG_MAX_READ_BACK.search(run)
        if read_back is None or sysctl is None or read_back.start() < sysctl.start():
            problems.append("cfs: the raised fs.mqueue.msg_max is not read back and checked")
        elif int(read_back.group(1)) < MIN_MSG_MAX:
            problems.append(
                f"cfs: the read-back accepts fs.mqueue.msg_max below {MIN_MSG_MAX} "
                f"(-ge {read_back.group(1)})"
            )
    return problems


def test_real_cfs_jobs_raise_the_message_queue_depth_limit() -> None:
    assert _msg_max_violations(WORKFLOW.read_text()) == []


_SYSCTL_STEP = "      - name: Raise fs.mqueue.msg_max so cFS keeps its 1000-deep pipes\n"
_SYSCTL_OPTIONS = "      options: --ulimit nofile=1024:524288 --sysctl fs.mqueue.msg_max=65536\n"
_READ_BACK = '          test "$(cat /proc/sys/fs/mqueue/msg_max)" -ge 1000\n'
_SYSCTL_COMMAND_LINE = "          sudo sysctl -w fs.mqueue.msg_max=65536\n"
_CFS_TESTS_STEP = "      - name: Real-cFS acceptance tests\n"
_EL8_TESTS_STEP = "      - name: Real-cFS acceptance tests as a non-root user\n"

_CFS_RUN = 'run: pytest tests/cfs -m cfs -n0 -v -rP --basetemp="$RUNNER_TEMP/pytest"'
_EL8_RUN = '          pytest tests/cfs -m cfs -n0 -v -rP --basetemp="$HOME/pytest-cfs"'

MSG_MAX_MUTATIONS: dict[str, Callable[[str], str]] = {
    "container sysctl removed": _replace_once(" --sysctl fs.mqueue.msg_max=65536", ""),
    "container sysctl too small": _replace_once(
        "--sysctl fs.mqueue.msg_max=65536", "--sysctl fs.mqueue.msg_max=10"
    ),
    "ubuntu sysctl removed": _replace_once(
        "          sudo sysctl -w fs.mqueue.msg_max=65536\n", ""
    ),
    "ubuntu sysctl too small": _replace_once(
        "sudo sysctl -w fs.mqueue.msg_max=65536", "sudo sysctl -w fs.mqueue.msg_max=10"
    ),
    "ubuntu read-back removed": _replace_once(_READ_BACK, ""),
    "read-back threshold -ge 1": _replace_once(_READ_BACK, _READ_BACK.replace("-ge 1000", "-ge 1")),
    "read-back threshold -ge 0": _replace_once(_READ_BACK, _READ_BACK.replace("-ge 1000", "-ge 0")),
    "read-back below the pipe depth": _replace_once(
        _READ_BACK, _READ_BACK.replace("-ge 1000", f"-ge {MIN_MSG_MAX - 1}")
    ),
    "read-back made optional": _replace_once(_READ_BACK, _READ_BACK.rstrip("\n") + " || true\n"),
    "read-back before the sysctl": _replace_once(
        "          sudo sysctl -w fs.mqueue.msg_max=65536\n" + _READ_BACK,
        _READ_BACK + "          sudo sysctl -w fs.mqueue.msg_max=65536\n",
    ),
    "sysctl step skipped with if: false": _replace_once(
        _SYSCTL_STEP, _SYSCTL_STEP + "        if: false\n"
    ),
    "sysctl step conditional": _replace_once(
        _SYSCTL_STEP, _SYSCTL_STEP + "        if: github.event_name == 'push'\n"
    ),
    "sysctl step continue-on-error": _replace_once(
        _SYSCTL_STEP, _SYSCTL_STEP + "        continue-on-error: true\n"
    ),
    "sysctl step shell without errexit": _replace_once(
        _SYSCTL_STEP, _SYSCTL_STEP + "        shell: bash {0}\n"
    ),
    "cfs job continue-on-error": _replace_once(
        "    name: real cFS acceptance\n",
        "    name: real cFS acceptance\n    continue-on-error: true\n",
    ),
    "cfs tests continue-on-error": _replace_once(
        "      - name: Real-cFS acceptance tests\n",
        "      - name: Real-cFS acceptance tests\n        continue-on-error: true\n",
    ),
    "cfs job requires no full depth": _replace_once(
        "      " + REQUIRE_FULL_DEPTH_ENV + ': "1"\n    steps:\n', "    steps:\n"
    ),
    "el8-cfs requires no full depth": _replace_once(
        '      IRONBIRD_CFS: "1"\n      ' + REQUIRE_FULL_DEPTH_ENV + ': "1"\n',
        '      IRONBIRD_CFS: "1"\n',
    ),
    "el8-cfs full depth set to 0": _replace_once(
        REQUIRE_FULL_DEPTH_ENV + ': "1"\n    defaults',
        REQUIRE_FULL_DEPTH_ENV + ': "0"\n    defaults',
    ),
    "cfs tests lose -rP": _replace_once(
        ' -v -rP --basetemp="$RUNNER_TEMP', ' -v --basetemp="$RUNNER_TEMP'
    ),
    "el8-cfs tests lose -rP": _replace_once(' -v -rP --basetemp="$HOME', ' -v --basetemp="$HOME'),
    "cfs tests step overrides the full depth": _replace_once(
        _CFS_TESTS_STEP + '        env:\n          IRONBIRD_CFS: "1"\n',
        _CFS_TESTS_STEP
        + '        env:\n          IRONBIRD_CFS: "1"\n          '
        + REQUIRE_FULL_DEPTH_ENV
        + ': "0"\n',
    ),
    "el8-cfs tests step overrides the full depth": _replace_once(
        _EL8_TESTS_STEP,
        _EL8_TESTS_STEP + "        env:\n          " + REQUIRE_FULL_DEPTH_ENV + ': "0"\n',
    ),
    "cfs tests set the full depth inline": _replace_once(
        "run: pytest tests/cfs", "run: " + REQUIRE_FULL_DEPTH_ENV + "=0 pytest tests/cfs"
    ),
    "el8-cfs tests set the full depth inline": _replace_once(
        "          pytest tests/cfs", "          " + REQUIRE_FULL_DEPTH_ENV + "=0 pytest tests/cfs"
    ),
    "cfs tests unset the full depth": _replace_once(
        "run: pytest tests/cfs", "run: env -u " + REQUIRE_FULL_DEPTH_ENV + " pytest tests/cfs"
    ),
    "second container sysctl too small": _replace_once(
        "--sysctl fs.mqueue.msg_max=65536\n",
        "--sysctl fs.mqueue.msg_max=65536 --sysctl fs.mqueue.msg_max=10\n",
    ),
    "first container sysctl too small": _replace_once(
        "--sysctl fs.mqueue.msg_max=65536\n",
        "--sysctl fs.mqueue.msg_max=10 --sysctl fs.mqueue.msg_max=65536\n",
    ),
    "sysctl step turns errexit off": _replace_once(
        _SYSCTL_COMMAND_LINE, "          set +e\n" + _SYSCTL_COMMAND_LINE
    ),
    "sysctl step turns errexit off by name": _replace_once(
        _SYSCTL_COMMAND_LINE, "          set +o errexit\n" + _SYSCTL_COMMAND_LINE
    ),
    "second sysctl lowers the value": _replace_once(
        _SYSCTL_COMMAND_LINE,
        _SYSCTL_COMMAND_LINE + "          sudo sysctl -w fs.mqueue.msg_max=10\n",
    ),
    "second sysctl lowers the value after the read-back": _replace_once(
        _READ_BACK, _READ_BACK + "          sudo sysctl -w fs.mqueue.msg_max=10\n"
    ),
    "value lowered through /proc": _replace_once(
        _READ_BACK, _READ_BACK + "          echo 10 > /proc/sys/fs/mqueue/msg_max\n"
    ),
    "value lowered in another step": _replace_once(
        "      - name: Real-cFS acceptance tests\n",
        "      - name: Lower it again\n        run: sudo sysctl -w fs.mqueue.msg_max=10\n"
        "      - name: Real-cFS acceptance tests\n",
    ),
    "cfs job default shell without errexit": _replace_once(
        "      " + REQUIRE_FULL_DEPTH_ENV + ': "1"\n    steps:\n',
        "      " + REQUIRE_FULL_DEPTH_ENV + ': "1"\n    defaults:\n      run:\n'
        "        shell: bash {0}\n    steps:\n",
    ),
    "workflow default shell": _replace_once(
        "\njobs:\n", "\ndefaults:\n  run:\n    shell: bash {0}\n\njobs:\n"
    ),
    "el8-cfs default shell loses errexit": _replace_last("-eo pipefail {0}", "-o pipefail {0}"),
    "later -r overrides -rP (cfs)": _replace_once(
        ' -v -rP --basetemp="$RUNNER_TEMP', ' -v -rP -rfE --basetemp="$RUNNER_TEMP'
    ),
    "later -r overrides -rP (el8-cfs)": _replace_once(
        ' -v -rP --basetemp="$HOME', ' -v -rP -r fE --basetemp="$HOME'
    ),
    "later -rN clears the report (cfs)": _replace_once(
        ' -v -rP --basetemp="$RUNNER_TEMP', ' -v -rP -rN --basetemp="$RUNNER_TEMP'
    ),
    "cfs tests step has an if": _replace_once(
        _CFS_TESTS_STEP, _CFS_TESTS_STEP + "        if: github.event_name == 'push'\n"
    ),
    "cfs tests step skipped with if: false": _replace_once(
        _CFS_TESTS_STEP, _CFS_TESTS_STEP + "        if: false\n"
    ),
    "el8-cfs tests step skipped": _replace_once(
        "        if: steps.setup.outputs.ready == 'true'\n        # One cFS instance",
        "        if: steps.setup.outputs.ready == 'true' && false\n        # One cFS instance",
    ),
    "cfs job sets PYTEST_ADDOPTS": _replace_once(
        "      " + REQUIRE_FULL_DEPTH_ENV + ': "1"\n    steps:\n',
        "      " + REQUIRE_FULL_DEPTH_ENV + ': "1"\n      PYTEST_ADDOPTS: -rN\n    steps:\n',
    ),
    "el8-cfs tests set PYTEST_ADDOPTS inline": _replace_once(
        "          pytest tests/cfs", "          PYTEST_ADDOPTS=-rN pytest tests/cfs"
    ),
    "cfs tests deselected with -k": _replace_once(_CFS_RUN, _CFS_RUN + ' -k "not ten_thousand"'),
    "el8-cfs tests deselected with -k": _replace_once(_EL8_RUN, _EL8_RUN + " -k generator"),
    "cfs tests --deselect": _replace_once(
        _CFS_RUN, _CFS_RUN + " --deselect tests/cfs/test_cfs_generator.py"
    ),
    "el8-cfs tests --deselect": _replace_once(
        _EL8_RUN, _EL8_RUN + " --deselect=tests/cfs/test_cfs_generator.py"
    ),
    "cfs tests --show-capture=no": _replace_once(_CFS_RUN, _CFS_RUN + " --show-capture=no"),
    "el8-cfs tests --show-capture=no": _replace_once(_EL8_RUN, _EL8_RUN + " --show-capture no"),
    "cfs tests narrow -m": _replace_once(
        '-m cfs -n0 -v -rP --basetemp="$RUNNER',
        "-m 'cfs and not slow' -n0 -v -rP --basetemp=\"$RUNNER",
    ),
    "el8-cfs tests narrow -m": _replace_once(_EL8_RUN, _EL8_RUN.replace("-m cfs", "-m 'not slow'")),
    "cfs tests run another path": _replace_once(
        _CFS_RUN, _CFS_RUN.replace("tests/cfs", "tests/cfs/test_cfs_smoke.py")
    ),
    "cfs tests run through python -m": _replace_once(
        _CFS_RUN, _CFS_RUN.replace("pytest tests", "python -m pytest tests")
    ),
    "cfs tests stop at the first failure": _replace_once(_CFS_RUN, _CFS_RUN + " -x"),
    "cfs tests disable a plugin": _replace_once(_CFS_RUN, _CFS_RUN + " -p no:terminal"),
    "cfs tests followed by ; true": _replace_once(_CFS_RUN, _CFS_RUN + "; true"),
    "cfs tests negated with a YAML tag": _replace_once(
        _CFS_RUN, _CFS_RUN.replace("run: ", "run: ! ")
    ),
    "el8-cfs tests behind env -i": _replace_once(
        _EL8_RUN, _EL8_RUN.replace("pytest", 'env -i PATH="$PATH" HOME="$HOME" pytest')
    ),
    "el8-cfs tests behind env -u": _replace_once(
        _EL8_RUN, _EL8_RUN.replace("pytest", "env -u IRONBIRD_CFS pytest")
    ),
    "el8-cfs default shell clears the environment": _replace_last(
        "env HOME=/home/ci USER=ci", "env -i HOME=/home/ci USER=ci"
    ),
    "el8-cfs default shell unsets IRONBIRD_CFS": _replace_last(
        "env HOME=/home/ci USER=ci", "env -u IRONBIRD_CFS HOME=/home/ci USER=ci"
    ),
    "el8-cfs default shell sets IRONBIRD_CFS": _replace_last(
        "env HOME=/home/ci USER=ci", "env IRONBIRD_CFS=0 HOME=/home/ci USER=ci"
    ),
    "quoted second container sysctl lowers it": _replace_once(
        "--sysctl fs.mqueue.msg_max=65536\n",
        '--sysctl fs.mqueue.msg_max=65536 --sysctl "fs.mqueue.msg_max=10"\n',
    ),
    "quoted only container sysctl too small": _replace_once(
        "--sysctl fs.mqueue.msg_max=65536\n", "--sysctl 'fs.mqueue.msg_max=10'\n"
    ),
    "=-form container sysctl lowers it": _replace_once(
        "--sysctl fs.mqueue.msg_max=65536\n",
        "--sysctl fs.mqueue.msg_max=65536 --sysctl=fs.mqueue.msg_max=10\n",
    ),
    "sysctl -p in the ubuntu job": _replace_once(
        _SYSCTL_COMMAND_LINE, _SYSCTL_COMMAND_LINE + "          sudo sysctl -p /etc/lower.conf\n"
    ),
    "sysctl --system in the ubuntu job": _replace_once(
        _SYSCTL_COMMAND_LINE, _SYSCTL_COMMAND_LINE + "          sudo sysctl --system\n"
    ),
    "sysctl in another ubuntu step": _replace_once(
        _CFS_TESTS_STEP, "      - name: Tune\n        run: sudo sysctl -p\n" + _CFS_TESTS_STEP
    ),
    "cfs tests step overrides IRONBIRD_CFS": _replace_once(
        _CFS_TESTS_STEP + '        env:\n          IRONBIRD_CFS: "1"\n',
        _CFS_TESTS_STEP + '        env:\n          IRONBIRD_CFS: "0"\n',
    ),
    "cfs tests step lacks IRONBIRD_CFS": _replace_once(
        _CFS_TESTS_STEP + '        env:\n          IRONBIRD_CFS: "1"\n', _CFS_TESTS_STEP
    ),
    "el8-cfs tests step overrides IRONBIRD_CFS": _replace_once(
        _EL8_TESTS_STEP, _EL8_TESTS_STEP + '        env:\n          IRONBIRD_CFS: "0"\n'
    ),
    "workflow env sets PYTEST_PLUGINS": _replace_once(
        "\njobs:\n", "\nenv:\n  PYTEST_PLUGINS: x\n\njobs:\n"
    ),
    "cfs tests followed by || true": _replace_once(
        '--basetemp="$RUNNER_TEMP/pytest"', '--basetemp="$RUNNER_TEMP/pytest" || true'
    ),
}


@pytest.mark.parametrize("mutation", sorted(MSG_MAX_MUTATIONS))
def test_msg_max_check_catches_regressions(mutation: str) -> None:
    mutated = MSG_MAX_MUTATIONS[mutation](WORKFLOW.read_text())
    assert mutated != WORKFLOW.read_text()
    assert _msg_max_violations(mutated), f"not caught: {mutation}"


@pytest.mark.parametrize(
    ("command", "chars"),
    [
        ("pytest tests/cfs -v -rP", "P"),
        ("pytest tests/cfs -vrP", "P"),
        ("pytest tests/cfs -r P -v", "P"),
        ("pytest tests/cfs -rfE -rP", "P"),
        ("pytest tests/cfs -rP -rfE", "fE"),
        ("pytest tests/cfs -rP -r N", "N"),
        ("pytest tests/cfs -v", None),
        ("pytest tests/cfs --basetemp=/x/rP", None),
    ],
)
def test_the_last_report_option_wins(command: str, chars: str | None) -> None:
    assert _report_chars(command) == chars
