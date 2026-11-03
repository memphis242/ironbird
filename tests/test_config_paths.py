"""Config file discovery and the remaining schema defects: sections that are not tables, links
that repeat a name, oversized files, and links_of lookups."""

from __future__ import annotations

import copy
import tomllib
from pathlib import Path
from typing import Any

import pytest

from conftest import EXAMPLES
from ironbird.config import MAX_CONFIG_BYTES, find_config, load_config, parse_config
from ironbird.errors import Code, IronbirdError


def base() -> dict[str, Any]:
    return tomllib.loads((EXAMPLES / "mock.toml").read_text())


@pytest.mark.parametrize(
    ("edit", "needle"),
    [
        (lambda d: d.update(recording="nope"), "recording: must be a table"),
        (lambda d: d.update(ledger="nope"), "ledger: must be a table"),
        (lambda d: d.update(retention=[1]), "retention: must be a table"),
        (lambda d: d.update(export=3), "export: must be a table"),
        (lambda d: d["links"].append("nope"), "must be a table"),
        (lambda d: d["links"].append(copy.deepcopy(d["links"][0])), "duplicate link name"),
        (lambda d: d["nodes"]["fc"].update(responses={"on": "x"}), "must be an array of tables"),
    ],
)
def test_config_structure_defects_are_coded_schema_errors(edit: Any, needle: str) -> None:
    data = base()
    edit(data)
    with pytest.raises(IronbirdError) as info:
        parse_config(data, Path("sys.toml"))
    assert info.value.code is Code.CFG_SCHEMA
    assert needle in info.value.summary


def test_the_config_found_in_the_working_directory_wins_over_the_user_config_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    work = tmp_path / "work"
    work.mkdir()
    (work / "ironbird.toml").write_text("")
    xdg = tmp_path / "xdg"
    (xdg / "ironbird").mkdir(parents=True)
    (xdg / "ironbird" / "ironbird.toml").write_text("")
    monkeypatch.chdir(work)
    assert find_config(None, {"XDG_CONFIG_HOME": str(xdg)}) == Path("ironbird.toml")
    (work / "ironbird.toml").unlink()
    assert find_config(None, {"XDG_CONFIG_HOME": str(xdg)}) == xdg / "ironbird" / "ironbird.toml"


def test_an_unset_config_home_falls_back_to_dot_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    with pytest.raises(IronbirdError) as info:
        find_config(None, {})
    assert info.value.code is Code.CFG_FILE
    assert str(tmp_path / "home" / ".config" / "ironbird" / "ironbird.toml") in info.value.details


def test_a_config_over_the_size_cap_is_refused_before_it_is_parsed(tmp_path: Path) -> None:
    big = tmp_path / "big.toml"
    big.write_bytes(b"# " + b"x" * MAX_CONFIG_BYTES + b"\n")
    with pytest.raises(IronbirdError) as info:
        load_config(big)
    assert info.value.code is Code.CFG_FILE and "larger" in info.value.summary


def test_a_config_that_is_not_utf8_is_a_syntax_error_not_a_crash(tmp_path: Path) -> None:
    bad = tmp_path / "bin.toml"
    bad.write_bytes(b'[ironbird]\nacts_as = "\xff\xfe"\n')
    with pytest.raises(IronbirdError) as info:
        load_config(bad)
    assert info.value.code is Code.CFG_SYNTAX


def test_deeply_nested_toml_is_a_syntax_error_not_a_crash(tmp_path: Path) -> None:
    bad = tmp_path / "deep.toml"
    bad.write_text("a = " + "[" * 100_000 + "]" * 100_000 + "\n")
    with pytest.raises(IronbirdError) as info:
        load_config(bad)
    assert info.value.code is Code.CFG_SYNTAX


def test_a_config_path_that_is_a_directory_is_a_file_error(tmp_path: Path) -> None:
    with pytest.raises(IronbirdError) as info:
        load_config(tmp_path)
    assert info.value.code is Code.CFG_FILE


def test_links_of_names_only_the_links_touching_a_node() -> None:
    config = load_config(EXAMPLES / "mock.toml")
    assert [link.name for link in config.links_of("fc")] == ["ground-fc"]
    assert config.links_of("nobody") == ()
