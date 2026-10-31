from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

from ironbird.ccdd.loader import load_ccdd
from ironbird.codec import Codec
from ironbird.errors import Code
from ironbird.header import load_profile

REPO = Path(__file__).resolve().parent.parent
EXAMPLES = REPO / "examples"
CORPUS = Path(__file__).resolve().parent / "corpus" / "malformed"

CFS_BUILD_DIR = Path(os.environ.get("CFS_BUILD_DIR", "/home/user/build/cfs"))
CFS_EXE = (
    CFS_BUILD_DIR
    / "src"
    / f"build-{os.environ.get('CFS_CONFIG', 'native_std')}"
    / "exe"
    / "cpu1"
    / "core-cpu1"
)


# Warnings that report a property of the host, not of the code under test: a receive buffer
# capped by a small net.core.rmem_max (IB-TX-005), a state directory on a file system outside
# the essential-reserve allowlist such as a container's overlayfs (IB-LED-006), and a file
# system without file locks (IB-LED-009). A test that counts the lines a command prints drops
# these first, so it holds on any host.
HOST_WARNINGS: frozenset[Code] = frozenset({Code.TX_RCVBUF, Code.LED_RESERVE, Code.LED_NOLOCK})
_HOST_WARNING_PREFIXES: tuple[str, ...] = tuple(f"warning {c.value}:" for c in HOST_WARNINGS)


def without_host_warnings(text: str) -> list[str]:
    """The non-blank lines of ``text`` other than the host warnings above."""
    return [
        line
        for line in text.splitlines()
        if line.strip() and not line.lstrip().startswith(_HOST_WARNING_PREFIXES)
    ]


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Gate for the real-cFS acceptance tests (marker ``cfs``, tests/cfs/).

    They run only when asked for: ``IRONBIRD_CFS=1`` in the environment, or ``-m cfs`` on the
    command line; otherwise a plain ``pytest`` skips them and stays fast. Once asked for, a
    missing cFS build is a skip with ``-m cfs`` alone and a hard failure with
    ``IRONBIRD_CFS=1`` (CI must not pass without testing anything). Build with
    ``tools/cfs/build.sh``.
    """
    forced = os.environ.get("IRONBIRD_CFS") == "1"
    selected = "cfs" in str(config.getoption("-m", default="") or "")
    for item in items:
        if item.get_closest_marker("cfs") is None:
            continue
        if not (forced or selected):
            reason = "real-cFS test is opt-in: set IRONBIRD_CFS=1 or use -m cfs"
            item.add_marker(pytest.mark.skip(reason=reason))
        elif not forced and not CFS_EXE.is_file():
            reason = f"cFS is not built ({CFS_EXE}); run tools/cfs/build.sh"
            item.add_marker(pytest.mark.skip(reason=reason))


def free_udp_ports(count: int) -> list[int]:
    """Distinct currently-free UDP ports on loopback (closed again before returning)."""
    socks: list[socket.socket] = []
    try:
        for _ in range(count):
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.bind(("127.0.0.1", 0))
            socks.append(sock)
        return [int(s.getsockname()[1]) for s in socks]
    finally:
        for sock in socks:
            sock.close()


_PROC_UDP_TABLES = ("/proc/net/udp", "/proc/net/udp6")


def udp_rx_queued_bytes(port: int) -> int | None:
    """Bytes queued unread in the receive buffers of the UDP sockets bound to local ``port``
    (the ``rx_queue`` column of ``/proc/net/udp``), or None where the tables cannot be read.

    A sender that waits for this to reach 0 before each chunk of datagrams cannot overrun the
    receiver's socket buffer, however slowly the receiving process is scheduled.
    """
    assert 0 < port < 0x10000
    suffix = f":{port:04X}"
    total: int | None = None
    for table in _PROC_UDP_TABLES:
        try:
            rows = Path(table).read_text(encoding="ascii", errors="replace").splitlines()[1:]
        except OSError:
            continue
        total = total or 0
        for row in rows:
            parts = row.split()
            if len(parts) > 4 and parts[1].endswith(suffix):
                total += int(parts[4].partition(":")[2], 16)
    return total


def _tree_status() -> str | None:
    """``git status`` of the repository (untracked files listed one by one); None if unknown."""
    git = shutil.which("git")
    if git is None or not (REPO / ".git").exists():
        return None
    done = subprocess.run(
        [git, "-C", str(REPO), "status", "--porcelain", "--untracked-files=all"],
        capture_output=True,
        text=True,
        check=False,
    )
    return done.stdout if done.returncode == 0 else None


@pytest.fixture(scope="session", autouse=True)
def _source_tree_unchanged() -> Iterator[None]:
    """Fail the run if the tests leave anything in the source tree (a stray ledger, recording or
    output file written to a relative path), judged by ``git status`` before and after."""
    before = _tree_status()
    yield
    after = _tree_status()
    if before is not None and after is not None and before != after:
        changed = sorted(set(before.splitlines()) ^ set(after.splitlines()))
        pytest.fail("the test run changed the source tree:\n" + "\n".join(changed), pytrace=False)


@pytest.fixture(autouse=True)
def _isolated_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Every test writes its ledger under tmp_path, never the user's state dir, and runs with
    tmp_path as its working directory, so a relative path can never write into the source tree."""
    monkeypatch.chdir(tmp_path)
    state = tmp_path / "state"
    monkeypatch.setenv("IRONBIRD_STATE_DIR", str(state))
    monkeypatch.setenv("IRONBIRD_RECORD_DIR", str(tmp_path / "recordings"))  # never the user's
    monkeypatch.delenv("IRONBIRD_CONFIG", raising=False)
    return state


@pytest.fixture(autouse=True)
def _fresh_cli_signal_state(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Every test sees the signal state as a fresh process has it, and leaves none behind.

    The states are module globals: a command run in-process (``CliRunner``) ends with
    ``_STATE.detach()``, after which the CLI's handler only records a signal instead of
    delivering it, and a test can leave a signal owed in :mod:`ironbird.sigmask`. Without a
    fresh state per test, a test that runs that handler, or that reaches a checkpoint, would
    behave according to whichever test ran before it in the same worker, which xdist decides.
    Not imported yet means not used yet, so its state is fresh already.
    """
    cli_main = sys.modules.get("ironbird.cli.main")
    if cli_main is not None:
        monkeypatch.setattr(cli_main, "_STATE", cli_main._SignalState())
    sigmask = sys.modules.get("ironbird.sigmask")
    if sigmask is not None:
        sigmask.discard()
    yield
    sigmask = sys.modules.get("ironbird.sigmask")
    if sigmask is not None:
        sigmask.discard()
        assert sigmask._DEPTH.open == 0, "a test left a signals_held scope open"


@pytest.fixture
def mock_codec() -> Codec:
    return Codec(
        load_ccdd(EXAMPLES / "ccdd" / "mock.json"),
        load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_cmd.toml"),
        load_profile(EXAMPLES / "headers" / "ccsds_v1_cfs_tlm.toml"),
        clock=lambda: 1_000_000.25,
    )


@pytest.fixture
def mock_config_path(tmp_path: Path) -> Path:
    """examples/mock.toml with absolute paths and free ports."""
    command_port, telemetry_port = free_udp_ports(2)
    text = (EXAMPLES / "mock.toml").read_text()
    text = text.replace('"ccdd/', f'"{EXAMPLES}/ccdd/').replace(
        '"headers/', f'"{EXAMPLES}/headers/'
    )
    text = text.replace("127.0.0.1:41234", f"127.0.0.1:{command_port}")
    text = text.replace("127.0.0.1:41235", f"127.0.0.1:{telemetry_port}")
    path = tmp_path / "mock.toml"
    path.write_text(text)
    return path
