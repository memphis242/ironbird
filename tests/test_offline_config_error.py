"""An offline command (it reads or writes files, with no session) does without a configuration it
cannot find, but never runs on defaults in place of one that is there and cannot be loaded: the
error is the coded one-line ``IB-CFG`` error, the exit status is the configuration's (2), the
ledger holds it in full, and the command does nothing."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from recutil import CLI_MAIN, write_config
from replayutil import child_env, hk_frames, write_recording

#: Tables that cannot be loaded: a TOML syntax error, a value of the wrong kind, an unknown key in
#: ``[export]``, an unknown key in ``[ledger]``.
BAD_TABLES = {
    "syntax": "[export]\nrange_bytes = 1MiB\n",
    "value": '[export]\nrange_bytes = "3 bananas"\n',
    "export-key": '[export]\nrnage_bytes = "1MiB"\n',
    "ledger-key": "[ledger]\nbogus = 1\n",
}


@pytest.fixture
def work(tmp_path: Path) -> tuple[Path, dict[str, str], Path]:
    for name in ("state", "recordings"):
        (tmp_path / name).mkdir(mode=0o700)
    recording = tmp_path / "r.ibrec"
    write_recording(recording, hk_frames(50))
    env = child_env(tmp_path)
    env["HOME"] = env["XDG_CONFIG_HOME"] = str(tmp_path / "home")  # no configuration to find
    return tmp_path, env, recording


def commands(directory: Path, recording: Path) -> dict[str, tuple[list[str], list[Path]]]:
    """Each offline command, with the files it would create."""
    out = directory / "out.txt"
    repaired = directory / "repaired.ibrec"
    return {
        "export": (["export", str(recording), "--format", "text", "-o", str(out)], [out]),
        "export-jobs": (
            ["export", str(recording), "--format", "text", "-o", str(out), "--jobs", "2"],
            [out],
        ),
        "replay": (["replay", str(recording), "--format", "line", "--count", "3"], []),
        "rec-info": (["rec", "info", str(recording)], []),
        "rec-check": (["rec", "check", str(recording)], []),
        "rec-repair": (["rec", "repair", str(recording), str(repaired)], [repaired]),
        "rec-prune": (["rec", "prune", "--dry-run"], []),
        "ledger-prune": (["ledger", "prune", "--dry-run"], []),
    }


COMMANDS = tuple(commands(Path("."), Path("r")))


def run(
    directory: Path, env: dict[str, str], config: Path | None, *args: str
) -> subprocess.CompletedProcess[str]:
    head = ["--config", str(config)] if config is not None else []
    return subprocess.run(
        [sys.executable, "-c", CLI_MAIN, *head, *args],
        capture_output=True,
        text=True,
        env=env,
        cwd=directory,
        timeout=120,
        check=False,
    )


def ledgered(directory: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in sorted((directory / "state").glob("*.jsonl")):
        records += [json.loads(line) for line in path.read_text().splitlines()]
    return records


@pytest.mark.parametrize("command", COMMANDS)
@pytest.mark.parametrize("table", list(BAD_TABLES))
def test_a_configuration_that_cannot_be_loaded_ends_an_offline_command_with_its_error(
    work: tuple[Path, dict[str, str], Path], command: str, table: str
) -> None:
    directory, env, recording = work
    config, _, _ = write_config(directory, BAD_TABLES[table])
    args, created = commands(directory, recording)[command]
    done = run(directory, env, config, *args)
    assert done.returncode == 2, done.stderr
    errors = [line for line in done.stderr.splitlines() if "IB-CFG-" in line]
    assert len(errors) == 1, done.stderr  # one line for the user, not a trace
    assert done.stdout == ""
    assert not any(path.exists() for path in created)
    coded = [r for r in ledgered(directory) if str(r.get("event", "")).startswith("IB-CFG-")]
    assert len(coded) == 1 and coded[0]["kind"] == "error", coded  # in the ledger, in full
    assert "IB-CFG-" in errors[0] and coded[0]["event"] in errors[0]


def test_the_details_of_the_configuration_error_are_one_step_away(
    work: tuple[Path, dict[str, str], Path],
) -> None:
    directory, env, recording = work
    config, _, _ = write_config(directory, BAD_TABLES["syntax"])
    plain = run(directory, env, config, "rec", "info", str(recording))
    verbose = run(directory, env, config, "rec", "info", str(recording), "--verbose")
    assert plain.returncode == verbose.returncode == 2
    assert len(verbose.stderr.splitlines()) > len(plain.stderr.splitlines())


def test_a_configuration_named_but_missing_is_an_error_too(
    work: tuple[Path, dict[str, str], Path],
) -> None:
    directory, env, recording = work
    done = run(directory, env, directory / "nowhere.toml", "rec", "info", str(recording))
    assert done.returncode == 2 and "IB-CFG-001" in done.stderr, done.stderr


@pytest.mark.parametrize("command", COMMANDS)
def test_no_configuration_at_all_is_not_an_error_for_an_offline_command(
    work: tuple[Path, dict[str, str], Path], command: str
) -> None:
    directory, env, recording = work
    args, _ = commands(directory, recording)[command]
    done = run(directory, env, None, *args)
    assert done.returncode == 0, done.stderr
    assert "IB-CFG-" not in done.stderr
