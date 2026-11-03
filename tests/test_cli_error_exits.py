"""Command-line failures through the real ``ib`` app: each bad invocation ends with a stable
error code on stderr and the exit status that code class promises (2 for usage and
configuration problems the user fixes, 1 for the rest) - and never a traceback.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from typer.testing import CliRunner

from ironbird.cli.main import app
from ironbird.errors import Code

CODE_LINE = re.compile(r"^(?:error|warning) (IB-[A-Z]+-\d{3}): ", re.MULTILINE)


@pytest.fixture
def env(mock_config_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("IRONBIRD_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("IRONBIRD_RECORD_DIR", str(tmp_path / "recordings"))
    monkeypatch.delenv("IRONBIRD_CONFIG", raising=False)
    return mock_config_path


def invoke(config: Path, *args: str) -> tuple[int, list[str], str]:
    result = CliRunner().invoke(app, ["-c", str(config), *args])
    text = result.output
    assert "Traceback" not in text, text
    return result.exit_code, CODE_LINE.findall(text), text


CASES: list[tuple[list[str], Code]] = [
    (["send", "fc", "MOCK_NOOP", "a=1", "a=2"], Code.CLI_USAGE),
    (["send", "fc", "MOCK_NOOP", "novalue"], Code.CLI_USAGE),
    (["send", "fc", "MOCK_NOOP", "--length-delta", "99999999"], Code.CLI_USAGE),
    (["send", "fc", "MOCK_NOOP", "--allow-count-mismatch"], Code.CLI_USAGE),
    (["send", "fc", "MOCK_NOOP", "--header", "bogus"], Code.CLI_USAGE),
    (["send", "fc", "NOPE"], Code.ENC_UNKNOWN),
    (["send", "fc", "MOCK_SET_MODE", "mode=NOT_A_MODE", "pad=0"], Code.ENC_ARG),
    (["send", "fc", "MOCK_SET_MODE"], Code.ENC_ARGSET),
    (["send", "ghost", "MOCK_NOOP"], Code.CFG_REF),
    (["listen", "ghost"], Code.CFG_REF),
    (["listen", "fc", "--where", "nope ==="], Code.FLT_SYNTAX),
    (["listen", "fc", "--where", "no_such_column == 1"], Code.FLT_NAME),
    (["rec", "check", "/nonexistent.ibrec"], Code.REC_FORMAT),
    (["rec", "info", "/nonexistent.ibrec"], Code.REC_FORMAT),
]


@pytest.mark.parametrize(("args", "code"), CASES, ids=[" ".join(a) for a, _ in CASES])
def test_a_bad_invocation_ends_with_its_stable_code_and_exit_status(
    env: Path, args: list[str], code: Code
) -> None:
    status, codes, text = invoke(env, *args)
    assert codes[:1] == [code.value], text
    expected = (
        2
        if code.value.startswith(("IB-CLI-", "IB-CFG-", "IB-HDR-", "IB-CCD-", "IB-ENC-", "IB-FLT-"))
        else 1
    )
    assert status == expected, text


def test_a_missing_config_file_is_a_config_error_not_a_crash(env: Path, tmp_path: Path) -> None:
    status, codes, text = invoke(tmp_path / "absent.toml", "find", "x")
    assert (status, codes[:1]) == (2, [Code.CFG_FILE.value]), text


def test_a_config_with_a_syntax_error_names_the_file_and_exits_as_usage(
    env: Path, tmp_path: Path
) -> None:
    broken = tmp_path / "broken.toml"
    broken.write_text("[nodes\n")
    status, codes, text = invoke(broken, "find", "x")
    assert (status, codes[:1]) == (2, [Code.CFG_SYNTAX.value]), text
    assert "broken.toml" in text


def test_find_reports_a_miss_and_truncation_without_an_error_code(env: Path) -> None:
    status, codes, text = invoke(env, "find", "zzzz-matches-nothing")
    assert status == 1 and codes == []
    status, codes, text = invoke(env, "find", "MOCK", "--limit", "1")
    assert status == 0 and codes == [], text


# -- ib rec check on recordings with holes and damage --


def check_codes(config: Path, recording: Path, *extra: str) -> tuple[int, list[str], str]:
    return invoke(config, "rec", "check", str(recording), *extra)


def hole_recording(path: Path, *, holes: int) -> Path:
    from recutil import RX, Builder

    builder = Builder()
    builder.stream(RX)
    builder.source(0, "peer")
    seq = 0
    for i in range(10):
        seq += 6 if holes and i % (10 // holes) == 3 else 1  # a jump leaves frames missing
        builder.frame(seq, stream=0, source=0)
    return builder.write(path)


@pytest.mark.parametrize("holes", [1, 2])
def test_check_reports_sequence_holes_no_gap_explains_as_damage(
    env: Path, tmp_path: Path, holes: int
) -> None:
    status, codes, text = check_codes(env, hole_recording(tmp_path / "h.ibrec", holes=holes))
    assert status == 1 and Code.REC_DAMAGED.value in codes, text


def test_check_is_quiet_on_a_clean_recording_and_names_the_torn_tail_of_another(
    env: Path, tmp_path: Path
) -> None:
    from recutil import Builder

    clean = Builder().standard(5).write(tmp_path / "ok.ibrec")
    status, codes, text = check_codes(env, clean)
    assert status in (0, 1) and Code.REC_DAMAGED.value not in codes, text
    torn = Builder().standard(5)
    torn.out += b"\xde\xad\xbe\xef" * 9
    status, codes, text = check_codes(env, torn.write(tmp_path / "torn.ibrec"))
    assert Code.REC_TORN.value in codes, text  # a torn tail is a warning: the frames before it read
