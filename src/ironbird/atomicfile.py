"""Writing a file under a temporary name and committing it without replacing anything.

An output file (a repaired recording, later an export) is first written to
``.<final name>.<16 hex>.tmp`` in the final name's directory, created ``O_EXCL``, then synced and
committed by :func:`rename_noreplace`, so a final name that appeared meanwhile is never
overwritten. The only replacement is an explicit ``force`` over an existing regular file owned
by the caller. A failure removes the temporary file, and only that: by directory descriptor and
name, after checking that the name still denotes the descriptor this process holds open.

Nothing here deletes a file this process did not create in the same call.
"""

from __future__ import annotations

import contextlib
import ctypes
import errno
import os
import stat
from pathlib import Path
from typing import Final

_AT_FDCWD: Final = -100
_RENAME_NOREPLACE: Final = 1
_UNAVAILABLE: Final = frozenset({errno.ENOSYS, errno.EOPNOTSUPP, errno.EINVAL})
_NO_HARD_LINKS: Final = frozenset({errno.EPERM, errno.EOPNOTSUPP, errno.ENOSYS, errno.EXDEV})
_OPEN_DIR: Final = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC


class Collision(OSError):
    """The final name already exists; nothing was changed."""

    def __init__(self, name: str) -> None:
        super().__init__(errno.EEXIST, "file exists", name)


def _renameat2() -> object | None:
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        return libc.renameat2
    except (AttributeError, OSError):
        return None


_RENAMEAT2: Final = _renameat2()


def _same_file(a: os.stat_result, b: os.stat_result) -> bool:
    return (a.st_dev, a.st_ino) == (b.st_dev, b.st_ino)


def _stat_at(dir_fd: int, name: str) -> os.stat_result | None:
    try:
        return os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
    except OSError:
        return None


def _try_renameat2(dir_fd: int, old: str, new: str) -> bool | None:
    """``True`` renamed, ``None`` the call is unavailable here; a collision raises."""
    func = _RENAMEAT2
    if func is None:
        return None
    result = func(  # type: ignore[operator]
        ctypes.c_int(dir_fd),
        os.fsencode(old),
        ctypes.c_int(dir_fd),
        os.fsencode(new),
        ctypes.c_uint(_RENAME_NOREPLACE),
    )
    if result == 0:
        return True
    err = ctypes.get_errno()
    if err == errno.EEXIST:
        raise Collision(new)
    if err in _UNAVAILABLE:
        return None
    raise OSError(err, os.strerror(err), new)


def rename_noreplace(dir_fd: int, old: str, new: str, fd: int) -> None:
    """Rename ``old`` to ``new`` (both names in the directory ``dir_fd``) unless ``new`` exists.

    ``fd`` is the open descriptor of the file: ``old`` must still denote it. Raises
    :class:`Collision` when ``new`` exists and ``OSError`` for any other failure; after a
    committed rename the directory is synced (a failure of that sync is raised, but does not
    undo the rename).
    """
    held, named = os.fstat(fd), _stat_at(dir_fd, old)
    if named is None or not _same_file(held, named):
        raise OSError(errno.ENOENT, "the temporary file's name no longer denotes the file", old)
    if _try_renameat2(dir_fd, old, new) is None:
        _link_or_placeholder(dir_fd, old, new, fd, held)
    os.fsync(dir_fd)


def _link_or_placeholder(dir_fd: int, old: str, new: str, fd: int, held: os.stat_result) -> None:
    """The fallbacks when ``renameat2`` cannot be used: a hard link, then a placeholder."""
    if stat.S_ISREG(held.st_mode) and _link(dir_fd, old, new, held):
        return
    _placeholder_rename(dir_fd, old, new)


def _link(dir_fd: int, old: str, new: str, held: os.stat_result) -> bool:
    """Commit by hard link and unlink of the old name. ``False``: hard links are unavailable
    here. A name that is taken raises :class:`Collision`."""
    try:
        os.link(old, new, src_dir_fd=dir_fd, dst_dir_fd=dir_fd, follow_symlinks=False)
    except FileExistsError:
        current = _stat_at(dir_fd, new)
        if current is None or not _same_file(held, current):
            raise Collision(new) from None
        # An earlier link went through and its reply was lost (NFSv3): the name is ours.
    except OSError as exc:
        if exc.errno in _NO_HARD_LINKS:
            return False
        raise
    _unlink_pair(dir_fd, old, new, held)
    return True


def _unlink_pair(dir_fd: int, old: str, new: str, held: os.stat_result) -> None:
    now_new, now_old = _stat_at(dir_fd, new), _stat_at(dir_fd, old)
    if now_new is None or not _same_file(held, now_new):
        raise OSError(errno.EIO, "the committed name does not denote the file", new)
    if now_old is not None and _same_file(held, now_old):
        os.unlink(old, dir_fd=dir_fd)


def _placeholder_rename(dir_fd: int, old: str, new: str) -> None:
    """Create the final name exclusively, then rename over it (an empty regular file)."""
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
    try:
        placeholder = os.open(new, flags, 0o600, dir_fd=dir_fd)
    except FileExistsError:
        raise Collision(new) from None
    try:
        made = os.fstat(placeholder)
        try:
            os.rename(old, new, src_dir_fd=dir_fd, dst_dir_fd=dir_fd)
        except OSError:
            current = _stat_at(dir_fd, new)
            if current is not None and _same_file(made, current):
                with contextlib.suppress(OSError):
                    os.unlink(new, dir_fd=dir_fd)
            raise
    finally:
        os.close(placeholder)


class TempOutput:
    """A file being written under a temporary name, to be committed to ``final``.

    Use as ``with TempOutput(final, force=...) as out:`` write to ``out.fd``, then
    ``out.commit()``. Leaving the block without a commit removes the temporary file.
    """

    def __init__(self, final: Path, *, force: bool = False) -> None:
        self.final = final
        self.force = force
        self.fd = -1
        self.name = ""
        self._dir_fd = -1
        self._committed = False

    def __enter__(self) -> TempOutput:
        directory = self.final.parent
        self._dir_fd = os.open(directory, _OPEN_DIR)
        try:
            for _ in range(8):
                name = f".{self.final.name}.{os.urandom(8).hex()}.tmp"
                try:
                    self.fd = os.open(
                        name,
                        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                        0o600,
                        dir_fd=self._dir_fd,
                    )
                except FileExistsError:
                    continue
                self.name = name
                return self
            raise OSError(errno.EEXIST, "no free temporary name", str(directory))
        except BaseException:
            os.close(self._dir_fd)
            raise

    @property
    def path(self) -> Path:
        return self.final.parent / self.name

    def write(self, data: bytes | bytearray | memoryview) -> None:
        view = memoryview(data)
        while view:
            written = os.write(self.fd, view)
            if written <= 0:
                raise OSError(errno.EIO, "write made no progress", self.name)
            view = view[written:]

    def commit(self) -> None:
        """Sync the file and give it its final name. An existing name is refused
        (:class:`Collision`) unless ``force`` and it is an existing regular file, which is
        replaced."""
        os.fdatasync(self.fd)
        final = self.final.name
        if self.force:
            existing = _stat_at(self._dir_fd, final)
            if existing is not None and stat.S_ISREG(existing.st_mode):
                os.rename(self.name, final, src_dir_fd=self._dir_fd, dst_dir_fd=self._dir_fd)
                self._committed = True
                os.fsync(self._dir_fd)
                return
        rename_noreplace(self._dir_fd, self.name, final, self.fd)
        self._committed = True

    def __exit__(self, *exc_info: object) -> None:
        try:
            if not self._committed and self.name:
                held, named = os.fstat(self.fd), _stat_at(self._dir_fd, self.name)
                if named is not None and _same_file(held, named):
                    with contextlib.suppress(OSError):
                        os.unlink(self.name, dir_fd=self._dir_fd)
        finally:
            with contextlib.suppress(OSError):
                os.close(self.fd)
            with contextlib.suppress(OSError):
                os.close(self._dir_fd)
