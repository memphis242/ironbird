"""Stage 2j acceptance against a real cFS instance: variable-length packets.

Stock cFS has no variable-length packet, so this runs ironbird's own app (``ib_test``, built by
``tools/cfs/build.sh`` from ``tools/cfs/apps/ib_test``) next to the stock ones. Its SET_DATA
command and VAR telemetry carry a counted array and a tail string, the layout that
``examples/ccdd/cfs.json`` describes; the flight code checks every command's size against that
layout itself, so a packet ironbird encodes wrongly is rejected by real FSW, not by a test double.

Opt-in (marker ``cfs``), like the rest of this directory; see ``tests/conftest.py``. The fixtures
of ``test_acceptance`` are reused, with a ``cfs`` fixture that also starts ``ib_test``.
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
from collections.abc import Iterator
from pathlib import Path

import pytest
from test_acceptance import (
    CI_LAB_PORT,
    CLEAN_SH,
    RAM_DIR,
    RUN_SH,
    STARTUP_TIMEOUT_S,
    TO_LAB_PORT,
    Ground,
    _artifact_safe_paths,
    _delta,
    _udp_port_free,
    _wait_for_log,
    ground,
    ground_session,
)

pytestmark = pytest.mark.cfs

# Fixtures of the shared module; pytest finds them by name here, so they ask for this module's
# ``cfs`` below, not the one that starts only the stock apps.
SHARED_FIXTURES = (_artifact_safe_paths, ground, ground_session)

IB_TEST_HK_MID = 0x08E0
IB_TEST_VAR_MID = 0x08E1
MAX_ENTRIES = 64
MAX_TEXT = 122


@pytest.fixture(scope="module")
def cfs(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Path]:
    """A running cFS (tools/cfs/run.sh) with ``ib_test`` started; returns its console log."""
    for port in (CI_LAB_PORT, TO_LAB_PORT):
        assert _udp_port_free(port), (
            f"UDP port {port} is already in use (another cFS instance running?); "
            "stop it before running the cfs tests"
        )
    ram_existed = RAM_DIR.exists()
    log = tmp_path_factory.mktemp("cfs") / "console.log"
    apps = os.environ.get("CFS_APPS", "sch_lab ci_lab to_lab sample_app")
    env = {**os.environ, "CFS_APPS": f"{apps} ib_test"}
    with log.open("wb") as sink:
        proc = subprocess.Popen(
            [str(RUN_SH)], stdout=sink, stderr=subprocess.STDOUT, start_new_session=True, env=env
        )
    try:
        _wait_for_log(log, "CI_LAB listening on UDP port: 1234", proc, STARTUP_TIMEOUT_S)
        _wait_for_log(log, "IB_TEST initialized", proc, STARTUP_TIMEOUT_S)
        _wait_for_log(log, "entering OPERATIONAL state", proc, STARTUP_TIMEOUT_S)
        _wait_for_log(log, "TO Lab subscribed to", proc, STARTUP_TIMEOUT_S)
        yield log
    finally:
        if proc.poll() is None:
            proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
        subprocess.run([str(CLEAN_SH)], check=False, timeout=30)
        if not ram_existed:
            shutil.rmtree(RAM_DIR, ignore_errors=True)


@pytest.fixture(scope="module")
def ib_test_output(ground_session: Ground) -> Ground:
    """Telemetry flowing, with TO_LAB told to forward the two ``ib_test`` packets."""
    g = ground_session
    if not g.output_enabled:
        g.enable_output()
    for mid in (IB_TEST_HK_MID, IB_TEST_VAR_MID):
        g.send(
            "TO_LAB_ADD_PKT",
            f"Stream={mid:#x}",
            "QosPriority=0",
            "QosReliability=0",
            "BufLimit=8",
            "Padding=00",
        )
    return g


def _var(g: Ground, command: str, *args: str, flags: tuple[str, ...] = ()) -> dict[str, object]:
    """Fields of the IB_TEST_VAR packet that follows ``command`` (a fresh one, not an old one)."""
    start = g.count()
    g.send(command, *args, flags=flags)
    return g.wait_for("IB_TEST_VAR", lambda _: True, start=start, timeout=10.0)["fields"]


def _hk(g: Ground) -> dict[str, object]:
    return g.fresh("IB_TEST_HK", "IB_TEST_SEND_HK")


def test_set_data_is_stored_by_real_fsw_and_published_as_a_variable_length_packet(
    ib_test_output: Ground,
) -> None:
    g = ib_test_output
    before = _hk(g)
    fields = _var(g, "IB_TEST_SET_DATA", "Entries=1,2,3", "Text=hello from ironbird")
    assert fields == {"Count": 3, "Entries": [1, 2, 3], "Text": "hello from ironbird"}
    after = _hk(g)
    assert (after["entries"], after["text_len"]) == (3, len("hello from ironbird"))
    assert _delta(int(after["cmd_counter"]), int(before["cmd_counter"])) == 1  # type: ignore[call-overload]
    assert after["err_counter"] == before["err_counter"]


def test_the_smallest_and_the_largest_packets_are_accepted(ib_test_output: Ground) -> None:
    g = ib_test_output
    assert _var(g, "IB_TEST_SET_DATA", "Entries=[]", "Text=") == {
        "Count": 0,
        "Entries": [],
        "Text": "",
    }
    entries = list(range(1000, 1000 + MAX_ENTRIES))
    text = "".join(chr(ord("a") + i % 26) for i in range(MAX_TEXT))
    fields = _var(g, "IB_TEST_SET_DATA", "Entries=" + ",".join(map(str, entries)), f"Text={text}")
    assert fields == {"Count": MAX_ENTRIES, "Entries": entries, "Text": text}


def test_send_var_republishes_what_was_stored(ib_test_output: Ground) -> None:
    g = ib_test_output
    _var(g, "IB_TEST_SET_DATA", "Entries=4294967295,0,7", "Text=again")
    assert _var(g, "IB_TEST_SEND_VAR") == {
        "Count": 3,
        "Entries": [4294967295, 0, 7],
        "Text": "again",
    }


def test_real_fsw_rejects_a_count_that_disagrees_with_the_packet_size(
    ib_test_output: Ground, tmp_path: Path
) -> None:
    g = ib_test_output
    _var(g, "IB_TEST_SET_DATA", "Entries=5,6", "Text=kept")
    before = _hk(g)
    # Without the flag ironbird refuses to build the packet at all: nothing goes out.
    refused = g.send("IB_TEST_SET_DATA", "Count=9", "Entries=1,2", "Text=x", check=False)
    assert refused.returncode == 1 and "IB-ENC-002" in refused.stderr
    flags = ("--allow-count-mismatch",)
    # Count beyond the packet, then a count above the CCDD maximum: the app counts both as errors.
    for count in ("9", "200"):
        start = g.count()
        g.send("IB_TEST_SET_DATA", f"Count={count}", "Entries=1,2", "Text=x", flags=flags)
        after = g.fresh("IB_TEST_HK", "IB_TEST_SEND_HK")
        assert g.since(start, "IB_TEST_VAR") == []  # nothing was stored, so nothing was published
        assert after["entries"] == before["entries"] and after["text_len"] == before["text_len"]
    final = _hk(g)
    assert _delta(int(final["err_counter"]), int(before["err_counter"])) == 2  # type: ignore[call-overload]
    assert final["cmd_counter"] == before["cmd_counter"]
    # The stored data survived the rejected commands.
    assert _var(g, "IB_TEST_SEND_VAR") == {"Count": 2, "Entries": [5, 6], "Text": "kept"}


def test_the_ledger_marks_the_injected_fault(ib_test_output: Ground, tmp_path: Path) -> None:
    state = tmp_path / "state"
    env = {**os.environ, "IRONBIRD_STATE_DIR": str(state)}
    env.pop("IRONBIRD_CONFIG", None)
    done = subprocess.run(
        [
            *Ground._ib(),
            "send",
            "fc",
            "IB_TEST_SET_DATA",
            "Count=9",
            "Entries=1",
            "Text=y",
            "--allow-count-mismatch",
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
        env=env,
    )
    assert done.returncode == 0, done.stderr
    events = [
        json.loads(line)
        for path in state.rglob("*.jsonl")
        for line in path.read_text().splitlines()
    ]
    requests = [e for e in events if e["event"] == "send.request"]
    assert len(requests) == 1 and requests[0]["allow_count_mismatch"] is True
    sent = [e for e in events if e["event"] == "tx.command"]
    assert len(sent) == 1 and sent[0]["result"] == "sent"
    raw = bytes.fromhex(sent[0]["raw"])
    assert raw[8:10] == b"\x09\x00" and len(raw) == 8 + 2 + 4 + 1  # the count as given, one entry


def test_telemetry_with_a_variable_length_is_decoded_without_malformed_reports(
    ib_test_output: Ground,
) -> None:
    g = ib_test_output
    assert g._stderr_log is not None
    _var(g, "IB_TEST_SET_DATA", "Entries=1", "Text=ok")
    text = g._stderr_log.read_text()
    assert "IB-DEC-" not in text, text  # no IB-DEC-* report while the listener ran
