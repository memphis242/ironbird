"""The ``ib_test`` generator's schedule arithmetic, run on the host against a fake clock.

The generator task publishes the packets that are due since its run began. That count is
``1 + elapsed * rate`` and must neither overflow nor wrap in a long run: a ``Count = 0`` run at
10k packets/s passes 2^32 packets after about 5 days, and ``elapsed_ns * rate`` passes 2^64 after
about 21 days. The telemetry's ``Serial`` and ``Sent`` stay ``uint32`` and wrap by design; the
generator's own count is 64-bit. ``ib_test_schedule_harness.c`` includes ``ib_test.c`` with a fake
PSP clock and a fake software bus, so the real tick code runs across those boundaries in
milliseconds.

It needs the cFS headers of the build (``tools/cfs/build.sh``), so it is in the opt-in ``cfs``
group, but starts no cFS.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

from conftest import CFS_BUILD_DIR, REPO

pytestmark = pytest.mark.cfs

HARNESS = Path(__file__).with_name("ib_test_schedule_harness.c")
APP_SRC = REPO / "tools" / "cfs" / "apps" / "ib_test" / "fsw" / "src"
FLAGS = (
    CFS_BUILD_DIR
    / "src"
    / "build-native_std"
    / "native"
    / "default_cpu1"
    / "apps"
    / "ib_test"
    / "CMakeFiles"
    / "ib_test.dir"
    / "flags.make"
)


def _includes() -> list[str]:
    """The include directories cFS builds ``ib_test`` with."""
    assert FLAGS.is_file(), f"{FLAGS} not found: build cFS with tools/cfs/build.sh"
    match = re.search(r"^C_INCLUDES = (.*)$", FLAGS.read_text(), re.M)
    assert match is not None, f"no C_INCLUDES in {FLAGS}"
    return match.group(1).split()


@pytest.fixture(scope="module")
def harness(tmp_path_factory: pytest.TempPathFactory) -> Path:
    compiler = shutil.which("cc") or shutil.which("gcc")
    assert compiler is not None, "no C compiler"
    exe = tmp_path_factory.mktemp("schedule") / "harness"
    done = subprocess.run(
        [
            compiler,
            "-std=gnu99",
            "-Wall",
            "-Wno-unused-function",
            "-D_XOPEN_SOURCE=600",
            "-D_POSIX_OS_",
            "-D_LINUX_OS_",
            "-DSIMULATION=native",
            f"-I{APP_SRC}",
            *_includes(),
            str(HARNESS),
            "-o",
            str(exe),
            # the app's other functions call cFE, which the harness does not link
            "-Wl,--unresolved-symbols=ignore-all",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    assert done.returncode == 0, done.stderr
    return exe


def test_the_schedule_holds_across_the_32_bit_wrap_and_the_64_bit_product(harness: Path) -> None:
    done = subprocess.run([str(harness)], capture_output=True, text=True, check=False, timeout=60)
    assert done.returncode == 0 and done.stdout.strip() == "ALL OK", done.stdout
