"""The file system calls of the ledger, of retention and of the temporary-file helpers.

Every such call goes through one object, :class:`FileSystem`, which the writer, retention and
:mod:`ironbird.atomicfile` receive at construction. Production passes :data:`REAL_FS`; tests
pass a subclass that fails chosen calls (ENOSPC, EIO, ENOLCK, a call that blocks), which is how
disk-full and crash paths are exercised without privileges. Nothing in ``src/`` reads the
environment or a file to switch faults on.

The methods are thin: one system call each, relative to a directory descriptor where the name
is not a bare path, never following a symbolic link on a final component unless said so.
"""

from __future__ import annotations

import contextlib
import ctypes
import errno
import fcntl
import os
import sys
from pathlib import Path
from typing import Final

OPEN_DIR_FLAGS: Final = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC
# What a file we only inspect is opened with: a FIFO or device with a matching name must not
# block or become our terminal, and a symbolic link must not be followed.
INSPECT_FLAGS: Final = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_NOCTTY | os.O_CLOEXEC
INSPECT_RW_FLAGS: Final = os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_NOCTTY | os.O_CLOEXEC

AT_FDCWD: Final = -100
RENAME_NOREPLACE: Final = 1
# Linux file system magic numbers (statfs f_type) on which posix_fallocate really reserves blocks.
FS_EXT: Final = 0xEF53  # ext2, ext3, ext4
FS_XFS: Final = 0x58465342
FS_TMPFS: Final = 0x01021994
RESERVE_FS_ALLOWLIST: Final = frozenset({FS_EXT, FS_XFS, FS_TMPFS})

# ``f_type`` is the first member of ``struct statfs`` on 64-bit Linux, a signed long.
assert sys.platform.startswith("linux"), "ironbird supports Linux only"
assert ctypes.sizeof(ctypes.c_long) == 8, "the statfs layout below is the 64-bit Linux one"
_STATFS_BYTES: Final = 120  # sizeof(struct statfs) on x86-64 and aarch64 Linux


def _libc() -> ctypes.CDLL | None:
    try:
        return ctypes.CDLL(None, use_errno=True)
    except OSError:
        return None


_LIBC: Final = _libc()


class FileSystem:
    """The real file system. Subclass to inject faults; every method here is one call."""

    # -- directories --------------------------------------------------------------------

    def mkdir_p(self, path: Path, mode: int = 0o700) -> None:
        """Create ``path`` and missing parents (mode applies to the directories created)."""
        os.makedirs(path, mode=mode, exist_ok=True)

    def open_dir(self, path: Path) -> int:
        return os.open(path, OPEN_DIR_FLAGS)

    def scandir(self, dir_fd: int) -> list[os.DirEntry[str]]:
        """Entries of the directory ``dir_fd`` (the descriptor is not consumed)."""
        with os.scandir(dir_fd) as it:
            return list(it)

    def mkdir_at(self, dir_fd: int, name: str, mode: int = 0o700) -> None:
        os.mkdir(name, mode, dir_fd=dir_fd)

    def rmdir_at(self, dir_fd: int, name: str) -> None:
        os.rmdir(name, dir_fd=dir_fd)

    # -- opening and closing ------------------------------------------------------------

    def open_at(self, dir_fd: int, name: str, flags: int, mode: int = 0o600) -> int:
        return os.open(name, flags, mode, dir_fd=dir_fd)

    def close(self, fd: int) -> None:
        os.close(fd)

    # -- data ---------------------------------------------------------------------------

    def write(self, fd: int, data: bytes | memoryview) -> int:
        return os.write(fd, data)

    def pwrite(self, fd: int, data: bytes | memoryview, offset: int) -> int:
        return os.pwrite(fd, data, offset)

    def pread(self, fd: int, length: int, offset: int) -> bytes:
        return os.pread(fd, length, offset)

    def fdatasync(self, fd: int) -> None:
        os.fdatasync(fd)

    def fsync(self, fd: int) -> None:
        os.fsync(fd)

    def ftruncate(self, fd: int, length: int) -> None:
        os.ftruncate(fd, length)

    def posix_fallocate(self, fd: int, offset: int, length: int) -> None:
        os.posix_fallocate(fd, offset, length)

    def utime(self, fd: int) -> None:
        os.utime(fd, None)

    # -- locks and metadata -------------------------------------------------------------

    def flock(self, fd: int, operation: int) -> None:
        fcntl.flock(fd, operation)

    def fstat(self, fd: int) -> os.stat_result:
        return os.fstat(fd)

    def stat_at(self, dir_fd: int, name: str) -> os.stat_result:
        """``lstat`` of ``name`` in ``dir_fd``: a symbolic link is reported, not followed."""
        return os.stat(name, dir_fd=dir_fd, follow_symlinks=False)

    def fstatfs_type(self, fd: int) -> int:
        """``f_type`` of the file system holding ``fd`` (``OSError`` on failure)."""
        libc = _LIBC
        if libc is None or not hasattr(libc, "fstatfs"):
            raise OSError(errno.ENOSYS, "fstatfs is not available")
        buffer = ctypes.create_string_buffer(_STATFS_BYTES)
        if libc.fstatfs(ctypes.c_int(fd), buffer) != 0:
            err = ctypes.get_errno()
            raise OSError(err, os.strerror(err))
        return int(ctypes.c_long.from_buffer(buffer).value)

    # -- names --------------------------------------------------------------------------

    def renameat2_noreplace(self, dir_fd: int, old: str, new: str) -> bool | None:
        """``renameat2(RENAME_NOREPLACE)``: ``True`` renamed, ``None`` unavailable here (symbol
        missing, ``ENOSYS``, ``EOPNOTSUPP``, ``EINVAL``), ``FileExistsError`` if ``new`` exists."""
        libc = _LIBC
        func = getattr(libc, "renameat2", None) if libc is not None else None
        if func is None:
            return None
        result = func(
            ctypes.c_int(dir_fd),
            os.fsencode(old),
            ctypes.c_int(dir_fd),
            os.fsencode(new),
            ctypes.c_uint(RENAME_NOREPLACE),
        )
        if result == 0:
            return True
        err = ctypes.get_errno()
        if err == errno.EEXIST:
            raise FileExistsError(err, os.strerror(err), new)
        if err in (errno.ENOSYS, errno.EOPNOTSUPP, errno.EINVAL):
            return None
        raise OSError(err, os.strerror(err), new)

    def link_at(self, dir_fd: int, old: str, new: str) -> None:
        os.link(old, new, src_dir_fd=dir_fd, dst_dir_fd=dir_fd, follow_symlinks=False)

    def rename_at(self, dir_fd: int, old: str, new: str) -> None:
        os.rename(old, new, src_dir_fd=dir_fd, dst_dir_fd=dir_fd)

    def unlink_at(self, dir_fd: int, name: str) -> None:
        os.unlink(name, dir_fd=dir_fd)

    def unlink_path(self, path: Path) -> None:
        os.unlink(path)


REAL_FS: Final = FileSystem()


def same_file(a: os.stat_result, b: os.stat_result) -> bool:
    """Whether two ``stat`` results denote one inode."""
    return (a.st_dev, a.st_ino) == (b.st_dev, b.st_ino)


def try_stat_at(fs: FileSystem, dir_fd: int, name: str) -> os.stat_result | None:
    """``stat_at`` that answers ``None`` for any failure (the name is not a file we can see)."""
    try:
        return fs.stat_at(dir_fd, name)
    except OSError:
        return None


def close_quietly(fs: FileSystem, fd: int) -> None:
    if fd >= 0:
        with contextlib.suppress(OSError):
            fs.close(fd)


def write_all(fs: FileSystem, fd: int, data: bytes | memoryview) -> int:
    """``write`` until everything is written; returns the bytes that reached the file.

    On an error the exception carries the count in ``written`` so a caller can tell exactly
    how much landed (a partial write followed by ENOSPC is the usual case).
    """
    view = memoryview(data)
    done = 0
    try:
        while done < len(view):
            count = fs.write(fd, view[done:])
            if count <= 0:
                raise OSError(errno.EIO, "write made no progress")
            done += count
    except OSError as exc:
        exc.written = done  # type: ignore[attr-defined]
        raise
    return done
