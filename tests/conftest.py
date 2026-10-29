from __future__ import annotations

import os
import socket
from pathlib import Path

import pytest

from ironbird.ccdd.loader import load_ccdd
from ironbird.codec import Codec
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


@pytest.fixture(autouse=True)
def _isolated_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Every test writes its ledger under tmp_path, never the user's state dir."""
    state = tmp_path / "state"
    monkeypatch.setenv("IRONBIRD_STATE_DIR", str(state))
    monkeypatch.delenv("IRONBIRD_CONFIG", raising=False)
    return state


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
