from __future__ import annotations

import itertools
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


# Ports handed to tests come from a window below the kernel's ephemeral range (the kernel's
# ip_local_port_range, 32768-60999 by default), so the kernel never gives one to another socket.
# The window is split into one block per xdist worker, so no two workers share a port; within a
# block ports are handed out in turn, each probed free before it is returned.
_EPHEMERAL_RANGE_FILE = Path("/proc/sys/net/ipv4/ip_local_port_range")
_PORT_BASE_ENV = "IRONBIRD_TEST_PORT_BASE"
_PORT_END_ENV = "IRONBIRD_TEST_PORT_END"
_DEFAULT_PORT_WINDOW = (20000, 32768)  # half-open: [20000, 32768)
_PRIVILEGED_PORT_END = 1024
_PORT_SPACE_END = 0x10000
MAX_PORTS_PER_TEST = 4  # the most any one free_udp_ports call may ask for


class PortWindowError(Exception):
    """The test port window cannot be used on this host; the message says how to fix it."""


def _ephemeral_range() -> tuple[int, int] | None:
    """The kernel's ephemeral port range as ``(first, last)`` inclusive; None if unreadable."""
    try:
        first, last = _EPHEMERAL_RANGE_FILE.read_text(encoding="ascii").split()
    except (OSError, ValueError):
        return None
    if not (first.isdecimal() and last.isdecimal()):
        return None
    return int(first), int(last)


def _env_port(name: str) -> int | None:
    value = os.environ.get(name)
    if value is None:
        return None
    if not (value.isascii() and value.isdecimal()):
        raise PortWindowError(f"{name}={value!r} is not a decimal port number")
    return int(value)


def _port_window() -> tuple[int, int]:
    """The half-open window ``[base, end)`` of ports for the whole test run.

    The default is 20000-32767, or the window named by ``IRONBIRD_TEST_PORT_BASE`` and
    ``IRONBIRD_TEST_PORT_END`` (end exclusive). Either way it must not overlap the kernel's
    ephemeral range, where the kernel can hand a port to any other socket at any time.
    """
    base_env = _env_port(_PORT_BASE_ENV)
    end_env = _env_port(_PORT_END_ENV)
    if (base_env is None) != (end_env is None):
        raise PortWindowError(f"set both {_PORT_BASE_ENV} and {_PORT_END_ENV}, or neither")
    overridden = base_env is not None and end_env is not None
    if base_env is not None and end_env is not None:
        base, end = base_env, end_env
    else:
        base, end = _DEFAULT_PORT_WINDOW
    if not _PRIVILEGED_PORT_END <= base < end <= _PORT_SPACE_END:
        raise PortWindowError(
            f"the test port window [{base}, {end}) must satisfy "
            f"{_PRIVILEGED_PORT_END} <= base < end <= {_PORT_SPACE_END}"
        )
    ephemeral = _ephemeral_range()
    how = (
        f"set {_PORT_BASE_ENV} and {_PORT_END_ENV} to a window outside it (end exclusive), or "
        "raise the ephemeral range's start to 32768 (sysctl net.ipv4.ip_local_port_range)"
    )
    if ephemeral is None:
        if overridden:
            return base, end  # cannot be checked here; the caller vouches for the window
        raise PortWindowError(
            f"cannot read {_EPHEMERAL_RANGE_FILE}, so the default test port window "
            f"[{base}, {end}) cannot be checked against the ephemeral range: "
            f"set {_PORT_BASE_ENV} and {_PORT_END_ENV} to a window known to be outside it"
        )
    first, last = ephemeral
    if base <= last and first < end:
        raise PortWindowError(
            f"the test port window [{base}, {end}) overlaps the kernel's ephemeral port range "
            f"{first}-{last} ({_EPHEMERAL_RANGE_FILE}), so another socket could take a test's "
            f"port: {how}"
        )
    return base, end


def _split_window(base: int, end: int, workers: int, number: int) -> tuple[int, int]:
    """Worker ``number``'s half-open block ``[start, stop)`` of the window shared by ``workers``."""
    assert workers > 0 and 0 <= number < workers
    size = (end - base) // workers
    if size < MAX_PORTS_PER_TEST:
        raise PortWindowError(
            f"the test port window [{base}, {end}) gives each of {workers} workers only {size} "
            f"ports, fewer than the {MAX_PORTS_PER_TEST} one test can take: run fewer workers "
            f"(-n) or widen the window with {_PORT_BASE_ENV} and {_PORT_END_ENV}"
        )
    start = base + size * number
    return start, start + size


def _process_block(base: int, end: int) -> tuple[int, int]:
    """This process's block of the window.

    Every xdist worker has ``PYTEST_XDIST_WORKER_COUNT`` and ``PYTEST_XDIST_WORKER`` (gw<N>) set,
    and N picks the block; any other process uses the whole window.
    """
    count_env = os.environ.get("PYTEST_XDIST_WORKER_COUNT")
    if count_env is None:
        return base, end
    worker = os.environ.get("PYTEST_XDIST_WORKER", "")
    number = worker.removeprefix("gw")
    if not (count_env.isascii() and count_env.isdecimal() and int(count_env) > 0):
        raise PortWindowError(f"PYTEST_XDIST_WORKER_COUNT={count_env!r} is not a positive integer")
    if not (worker.startswith("gw") and number.isascii() and number.isdecimal()):
        raise PortWindowError(f"PYTEST_XDIST_WORKER={worker!r} is not of the form gw<N>")
    if int(number) >= int(count_env):
        raise PortWindowError(
            f"worker {worker} is outside the {count_env} workers of this run "
            "(a restarted worker takes a new number; fix the failure that made it exit)"
        )
    return _split_window(base, end, int(count_env), int(number))


def _resolve_block() -> tuple[int, int, PortWindowError | None]:
    """This process's block ``[start, stop)``, or a placeholder block and why there is none."""
    try:
        window = _port_window()
        return (*_process_block(*window), None)
    except PortWindowError as error:
        return 0, MAX_PORTS_PER_TEST, error


# Reported once, cleanly, by pytest_sessionstart (and by free_udp_ports outside pytest).
_block_start, _block_end, _PORT_ERROR = _resolve_block()
_block_size = _block_end - _block_start
assert _block_size >= MAX_PORTS_PER_TEST
# Start at a per-process offset so separate pytest sessions on one host rarely meet.
_block_ports = itertools.islice(
    itertools.cycle(range(_block_start, _block_end)),
    os.getpid() % _block_size,
    None,
)


def pytest_sessionstart(session: pytest.Session) -> None:
    """Stop the run before any test starts if the test port window cannot be used, or cannot be
    shared by the requested number of xdist workers (checked here so the run stops once, in the
    controller, rather than in every worker)."""
    error = _PORT_ERROR
    workers = getattr(session.config.option, "numprocesses", None)
    if error is None and isinstance(workers, int) and workers > 0:
        try:
            _split_window(*_port_window(), workers, 0)
        except PortWindowError as too_many:
            error = too_many
    if error is not None:
        pytest.exit(f"ironbird tests: {error}", returncode=4)


def _port_is_free(port: int) -> bool:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.bind(("127.0.0.1", port))
    except OSError:
        return False
    finally:
        sock.close()
    return True


def free_udp_ports(count: int) -> list[int]:
    """Distinct free UDP ports on loopback, from this process's block below the ephemeral range."""
    if _PORT_ERROR is not None:
        raise _PORT_ERROR
    assert 0 < count <= MAX_PORTS_PER_TEST
    ports: list[int] = []
    for port in itertools.islice(_block_ports, _block_size):
        if _port_is_free(port):
            ports.append(port)
            if len(ports) == count:
                return ports
    raise AssertionError(f"no {count} free UDP ports in [{_block_start}, {_block_end})")


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
