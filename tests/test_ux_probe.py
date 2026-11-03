"""Regression tests for the user-probe findings: behaviours seen from the command line by someone
who has not read the source. Each runs the real CLI in a subprocess. They assert on exit codes,
files and the presence of facts in the output, never on exact wording."""

from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from typing import Any

import pytest

from exportutil import hk_and_status
from ironbird.coalesce import Coalescer
from ironbird.codec import Codec
from ironbird.errors import Code
from ironbird.notice import Level, Notice
from ledgerutil import session_records
from recutil import Proc, run_ib, send_datagrams, write_config


@pytest.fixture
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    path, command_port, telemetry_port = write_config(tmp_path)
    monkeypatch.setenv("IRONBIRD_CONFIG", str(path))
    monkeypatch.setenv("IRONBIRD_TEST_PORTS", f"{command_port},{telemetry_port}")
    return dict(os.environ)


def ports(env: dict[str, str]) -> tuple[int, int]:
    command, telemetry = env["IRONBIRD_TEST_PORTS"].split(",")
    return int(command), int(telemetry)


def ib(tmp_path: Path, env: dict[str, str], *args: str) -> subprocess.CompletedProcess[str]:
    return run_ib(tmp_path, list(args), env)


def ledger_rows(tmp_path: Path) -> list[dict[str, Any]]:
    return session_records(tmp_path / "state")


def sessions(tmp_path: Path) -> dict[str, list[dict[str, Any]]]:
    by_session: dict[str, list[dict[str, Any]]] = {}
    for row in ledger_rows(tmp_path):
        by_session.setdefault(row["session"], []).append(row)
    return by_session


# -- a group without a subcommand --------------------------------------------------------------


@pytest.mark.parametrize("group", ["ledger", "rec"])
def test_a_group_without_a_subcommand_shows_help_and_no_empty_error_line(
    tmp_path: Path, env: dict[str, str], group: str
) -> None:
    done = ib(tmp_path, env, group)
    assert done.returncode == 2
    assert "Usage" in done.stdout + done.stderr
    assert not re.search(r"error IB-CLI-001:\s*$", done.stderr, re.MULTILINE), done.stderr


# -- Ctrl-C before the command is up -----------------------------------------------------------


def test_an_interrupt_during_the_imports_ends_quietly_with_130(tmp_path: Path) -> None:
    """SIGINT delivered while the CLI's own modules are still being imported: no traceback."""
    code = textwrap.dedent(
        """
        import importlib.abc, os, signal, sys

        class Bomb(importlib.abc.MetaPathFinder):
            def find_spec(self, name, path, target=None):
                if name == "typer":
                    os.kill(os.getpid(), signal.SIGINT)
                return None

        sys.meta_path.insert(0, Bomb())
        from ironbird.cli.launch import main
        sys.exit(main())
        """
    )
    done = subprocess.run(
        [sys.executable, "-c", code, "nodes"],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
        env=dict(os.environ),
    )
    assert done.returncode == 130, done.stderr
    assert "Traceback" not in done.stderr and "KeyboardInterrupt" not in done.stderr


# -- export: where the output goes -------------------------------------------------------------


def test_export_dash_o_with_a_csv_name_for_several_tables_is_refused(
    tmp_path: Path, env: dict[str, str]
) -> None:
    recording = hk_and_status(tmp_path / "in.ibrec", 3, 2)
    done = ib(tmp_path, env, "export", str(recording), "-o", str(tmp_path / "out.csv"))
    assert done.returncode == 2, done.stderr
    assert "IB-CLI-001" in done.stderr
    assert not (tmp_path / "out.csv").exists()  # no directory named like a file


def test_export_without_options_prints_the_listen_lines_to_stdout(
    tmp_path: Path, env: dict[str, str]
) -> None:
    recording = hk_and_status(tmp_path / "in.ibrec", 3, 2)
    done = ib(tmp_path, env, "export", str(recording))
    assert done.returncode == 0, done.stderr
    assert "MOCK_HK" in done.stdout and "MOCK_STATUS" in done.stdout


def test_export_still_defaults_to_csv_for_one_message_and_for_a_directory(
    tmp_path: Path, env: dict[str, str]
) -> None:
    recording = hk_and_status(tmp_path / "in.ibrec", 3, 2)
    one = ib(tmp_path, env, "export", str(recording), "--message", "MOCK_HK")
    assert one.returncode == 0 and one.stdout.startswith("rx_time_utc"), one.stderr
    many = ib(tmp_path, env, "export", str(recording), "-o", str(tmp_path / "tables"))
    assert many.returncode == 0, many.stderr
    assert (tmp_path / "tables" / "MOCK_HK.csv").is_file()


# -- one vocabulary for event types; help that matches the options -----------------------------


@pytest.mark.parametrize("name", ["information", "INFORMATION", "info", "warn", "Critical"])
def test_events_min_type_takes_the_ccdd_spellings(
    tmp_path: Path, env: dict[str, str], name: str
) -> None:
    done = ib(tmp_path, env, "events", "--min-type", name, "--timeout", "0.3")
    assert done.returncode == 0, done.stderr


def test_events_min_type_refusal_lists_the_valid_names(tmp_path: Path, env: dict[str, str]) -> None:
    done = ib(tmp_path, env, "events", "--min-type", "loud")
    assert done.returncode == 2
    assert "information" in done.stderr and "critical" in done.stderr


def test_events_help_does_not_mention_an_option_it_lacks(
    tmp_path: Path, env: dict[str, str]
) -> None:
    helped = ib(tmp_path, env, "events", "--help").stdout
    assert "--mid" not in helped
    assert ib(tmp_path, env, "events", "--mid", "1").returncode == 2
    assert "--mid" in ib(tmp_path, env, "listen", "--help").stdout


def test_help_keeps_bracketed_config_table_names(tmp_path: Path, env: dict[str, str]) -> None:
    helped = ib(tmp_path, env, "export", "--help").stdout
    assert "[export]" in helped


def test_help_paragraphs_are_not_cut_at_the_source_line_breaks(
    tmp_path: Path, env: dict[str, str]
) -> None:
    narrow = dict(env, COLUMNS="200")
    helped = ib(tmp_path, narrow, "send", "--help").stdout
    assert "hex for bytes" in " ".join(helped.split())
    assert any("hex for bytes" in line for line in helped.splitlines()), helped


# -- a count that was not reached says so ------------------------------------------------------


def test_listen_count_not_reached_says_so_and_exits_3(tmp_path: Path, env: dict[str, str]) -> None:
    done = ib(tmp_path, env, "listen", "fc", "--count", "1", "--timeout", "0.5")
    assert done.returncode == 3
    assert "timeout" in done.stderr and "0 of 1" in done.stderr
    end = [r for r in ledger_rows(tmp_path) if r["event"] == "session.end"]
    assert end and end[-1]["exit"] == 3 and end[-1]["outcome"] == "count_not_reached"


# -- the ledger: outcome, order, and failed invocations ----------------------------------------


def test_every_ledger_session_ends_with_session_end_carrying_the_exit_code(
    tmp_path: Path, env: dict[str, str]
) -> None:
    assert ib(tmp_path, env, "nodes").returncode == 0
    assert ib(tmp_path, env, "send", "fc", "NO_SUCH_COMMAND").returncode == 2
    assert ib(tmp_path, env, "-c", str(tmp_path / "missing.toml"), "nodes").returncode == 2
    found = sessions(tmp_path)
    assert len(found) == 3
    codes = []
    for rows in found.values():
        ends = [r for r in rows if r["event"] == "session.end"]
        assert len(ends) == 1
        assert rows[-1] is ends[0], [r["event"] for r in rows]  # the last record of the session
        assert isinstance(ends[0]["exit"], int) and isinstance(ends[0]["outcome"], str)
        codes.append(ends[0]["exit"])
    assert sorted(codes) == [0, 2, 2]


def test_session_end_follows_the_signal_and_the_summaries(
    tmp_path: Path, env: dict[str, str], mock_codec: Codec
) -> None:
    _, telemetry = ports(env)
    listener = Proc(tmp_path, "listen", ["listen", "fc"], env)
    try:
        listener.wait_for("listening")
        send_datagrams(telemetry, [b"\x01"] * 30 + [b""], pause=0.002)
        time.sleep(0.4)
        listener.popen.send_signal(signal.SIGINT)
        assert listener.finish() == 130, listener.stderr
    finally:
        listener.kill()
    (rows,) = sessions(tmp_path).values()
    names = [r["event"] for r in rows if not r["event"].startswith("ledger.")]
    assert names[-1] == "session.end", names
    assert "cli.signal" in names and names.index("cli.signal") < len(names) - 1
    assert rows[-1]["event"] == "session.end" or rows[-1]["event"].startswith("ledger.")
    end = next(r for r in rows if r["event"] == "session.end")
    assert end["exit"] == 130 and end["outcome"] == "signal" and end["signal"] == "SIGINT"


@pytest.mark.parametrize(
    "args",
    [
        ["listen", "fc", "--where", "msg == "],
        ["listen", "fc", "--where", "no_such_column > 0"],
        ["listen", "fc", "--timeout", "0"],
        ["listen", "fc", "--no-such-option"],
        ["lisen", "fc"],
    ],
    ids=["syntax", "name", "timeout", "option", "command"],
)
def test_a_failed_invocation_is_in_the_ledger(
    tmp_path: Path, env: dict[str, str], args: list[str]
) -> None:
    done = ib(tmp_path, env, *args)
    assert done.returncode == 2, done.stderr
    (rows,) = sessions(tmp_path).values()
    events = [r["event"] for r in rows]
    assert "cli.invoke" in events
    assert any(r["kind"] == "error" and r["event"].startswith("IB-") for r in rows), events
    assert rows[-1]["event"] == "session.end" and rows[-1]["exit"] == 2
    assert not list((tmp_path / "state").glob("*.reserve"))  # a typo preallocates nothing


# -- --verbose shows packet hex ----------------------------------------------------------------


def test_verbose_listen_shows_the_hex_of_a_rejected_packet_and_of_a_good_one(
    tmp_path: Path, env: dict[str, str], mock_codec: Codec
) -> None:
    _, telemetry = ports(env)
    good = mock_codec.encode_telemetry("MOCK_HK", {"cmd_counter": 7}).data
    listener = Proc(tmp_path, "listen", ["-v", "listen", "fc", "--count", "1"], env)
    try:
        listener.wait_for("listening")
        send_datagrams(telemetry, [b"\xde\xad\xbe\xef"], pause=0.05)
        send_datagrams(telemetry, [good], pause=0.05)
        assert listener.finish() == 0, listener.stderr
    finally:
        listener.kill()
    assert "de ad be ef" in listener.stderr
    assert good.hex(" ") in listener.stderr


def test_a_coalesced_line_does_not_state_the_size_of_only_one_packet(
    tmp_path: Path, env: dict[str, str]
) -> None:
    _, telemetry = ports(env)
    listener = Proc(tmp_path, "listen", ["listen", "fc", "--timeout", "3"], env)
    try:
        listener.wait_for("listening")
        send_datagrams(telemetry, [b"\x01"] * 40 + [b""], pause=0.002)
        assert listener.finish() == 0, listener.stderr
    finally:
        listener.kill()
    grouped = [x for x in listener.stderr.splitlines() if "x41" in x]
    assert len(grouped) == 1, listener.stderr
    assert not re.search(r"\b[01] bytes?\b", grouped[0]), grouped[0]


# -- one exit-code rule ------------------------------------------------------------------------


@pytest.mark.parametrize(
    "args",
    [
        ["send", "fc", "NO_SUCH_COMMAND"],
        ["send", "nosuch", "MOCK_NOOP"],
        ["send", "fc", "MOCK_SET_MODE", "mode=NOPE", "pad=0"],
        ["send", "fc", "MOCK_SET_MODE", "pad=0", "pad=1", "mode=SAFE"],
        ["send", "fc", "MOCK_SET_MODE", "pad=0"],
        ["send", "fc", "MOCK_NOOP", "--header", "seq=5"],
        ["find", ""],
    ],
    ids=["command", "node", "enum", "repeat", "missing", "header", "empty-find"],
)
def test_a_wrong_command_line_is_exit_2(
    tmp_path: Path, env: dict[str, str], args: list[str]
) -> None:
    done = ib(tmp_path, env, *args)
    assert done.returncode == 2, done.stderr
    assert (tmp_path / "state").exists() and not list((tmp_path / "state").glob("*.reserve"))


def test_a_failure_while_running_is_still_exit_1(tmp_path: Path, env: dict[str, str]) -> None:
    done = ib(tmp_path, env, "send", "fc", "MOCK_NOOP")  # nothing is listening
    assert done.returncode == 1, done.stderr


def test_find_with_no_match_is_exit_1(tmp_path: Path, env: dict[str, str]) -> None:
    assert ib(tmp_path, env, "find", "zzzzzz_nothing").returncode == 1


# -- prune: what "live" means ------------------------------------------------------------------


def test_prune_explains_that_a_session_changed_in_the_last_minutes_is_protected(
    tmp_path: Path, env: dict[str, str]
) -> None:
    assert ib(tmp_path, env, "nodes").returncode == 0  # a finished session, just now
    done = ib(tmp_path, env, "ledger", "prune", "--older-than", "1s", "--keep", "0", "--dry-run")
    assert done.returncode == 0, done.stderr
    assert re.search(r"\b\d+ ?min", done.stdout + done.stderr), done.stdout


# -- fault injection is flagged and never wraps silently ---------------------------------------


def _hole(tmp_path: Path, env: dict[str, str]) -> Proc:
    return Proc(tmp_path, "rx", ["respond", "--as", "fc", "--timeout", "5"], env)


def test_a_fault_injected_send_says_so(tmp_path: Path, env: dict[str, str]) -> None:
    responder = _hole(tmp_path, env)
    try:
        responder.wait_for("responding")
        plain = ib(tmp_path, env, "send", "fc", "MOCK_NOOP")
        bad = ib(tmp_path, env, "send", "fc", "MOCK_NOOP", "--bad-checksum")
        longer = ib(tmp_path, env, "send", "fc", "MOCK_NOOP", "--length-delta", "2")
    finally:
        responder.kill()
    assert plain.returncode == bad.returncode == longer.returncode == 0, bad.stderr
    assert "fault" not in plain.stdout
    assert "fault" in bad.stdout and "checksum" in bad.stdout
    assert "fault" in longer.stdout and "+2" in longer.stdout


def test_a_length_delta_that_would_wrap_the_field_is_refused_and_nothing_is_sent(
    tmp_path: Path, env: dict[str, str]
) -> None:
    done = ib(tmp_path, env, "send", "fc", "MOCK_NOOP", "--length-delta", "-100")
    assert done.returncode == 2, done.stderr
    assert "IB-HDR-003" in done.stderr
    sent = [r for r in ledger_rows(tmp_path) if r["event"] == "tx.command"]
    assert not sent


# -- hints -------------------------------------------------------------------------------------


def test_an_unknown_command_suggests_the_closest_names(tmp_path: Path, env: dict[str, str]) -> None:
    done = ib(tmp_path, env, "send", "fc", "MOCK_NOOOP")
    assert done.returncode == 2
    assert "MOCK_NOOP" in done.stderr.split("MOCK_NOOOP", 1)[1]
    lower = ib(tmp_path, env, "send", "fc", "mock_noop")
    assert "MOCK_NOOP" in lower.stderr.split("mock_noop", 1)[1]


def test_respond_as_the_wrong_node_names_the_right_one(tmp_path: Path, env: dict[str, str]) -> None:
    done = ib(tmp_path, env, "respond")
    assert done.returncode == 2
    assert "--as fc" in done.stderr


def test_replay_of_a_name_without_the_extension_suggests_it(
    tmp_path: Path, env: dict[str, str]
) -> None:
    hk_and_status(tmp_path / "rec1.ibrec", 2, 1)
    done = ib(tmp_path, env, "replay", str(tmp_path / "rec1"))
    assert done.returncode == 1
    assert "rec1.ibrec" in done.stderr


def test_rec_info_writes_mids_as_everything_else_does(tmp_path: Path, env: dict[str, str]) -> None:
    recording = hk_and_status(tmp_path / "r.ibrec", 3, 0)
    done = ib(tmp_path, env, "rec", "info", str(recording))
    listing = ib(tmp_path, env, "replay", str(recording)).stdout
    assert re.search(r"^\s+0x0890\s+3$", done.stdout, re.MULTILINE), done.stdout
    assert "mid=0x0890" in listing


def test_the_port_in_use_error_points_at_listen_record(tmp_path: Path, env: dict[str, str]) -> None:
    holder = Proc(tmp_path, "a", ["listen", "fc", "--timeout", "10"], env)
    try:
        holder.wait_for("listening")
        done = ib(tmp_path, env, "record", "-o", str(tmp_path / "x.ibrec"), "--duration", "1")
    finally:
        holder.kill()
    assert done.returncode == 1 and "IB-TX-001" in done.stderr
    assert "listen --record" in done.stderr


def test_json_of_the_ledger_end_record_is_valid_json_lines(
    tmp_path: Path, env: dict[str, str]
) -> None:
    assert ib(tmp_path, env, "nodes").returncode == 0
    for path in (tmp_path / "state").glob("*.jsonl"):
        for line in path.read_text().splitlines():
            json.loads(line)


def test_a_failed_send_shows_the_packet_it_could_not_send_with_verbose(
    tmp_path: Path, env: dict[str, str]
) -> None:
    done = ib(tmp_path, env, "-v", "send", "fc", "MOCK_NOOP")  # nothing is listening
    assert done.returncode == 1, done.stderr
    assert re.search(r"hex: ([0-9a-f]{2} )+[0-9a-f]{2}", done.stderr), done.stderr


# -- the coalescer states only what the occurrences share ---------------------------------------


def _coalesced(summaries: list[str], raws: list[bytes] | None = None) -> list[Notice]:
    emitted: list[Notice] = []
    clock = [0.0]
    coalescer = Coalescer(emitted.append, window_s=10.0, clock=lambda: clock[0])
    for i, text in enumerate(summaries):
        coalescer.report(
            Code.DEC_SHORT,
            Level.ERROR,
            "key",
            text,
            raw=raws[i] if raws else b"",
            raw_len=len(raws[i]) if raws else None,
        )
    coalescer.flush()
    return emitted


def test_identical_repeats_keep_their_text() -> None:
    first, summary = _coalesced(["packet of 3 bytes"] * 4)
    assert summary.summary == first.summary == "packet of 3 bytes" and summary.count == 4


def test_differing_repeats_drop_the_words_that_differ() -> None:
    _, summary = _coalesced(["from fc: packet of 1 bytes", "from fc: packet of 0 bytes"] * 3)
    assert summary.summary == "from fc: packet of ... bytes" and summary.count == 6


def test_differing_repeats_of_another_length_keep_head_and_tail() -> None:
    _, summary = _coalesced(["bad frame at offset 4", "bad frame, header cut, at offset 4"])
    assert summary.summary.startswith("bad ") and summary.summary.endswith(" at offset 4")
    assert "..." in summary.summary


def test_a_summary_carries_the_latest_packet_for_verbose() -> None:
    _, summary = _coalesced(["x", "x", "x"], [b"\x01", b"\x02", b"\x03\x04"])
    assert summary.raw == b"\x03\x04"
    assert "last hex: 03 04" in summary.line(verbose=True)
    assert "hex" not in summary.line(verbose=False)
