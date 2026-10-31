"""Renaming and writing files without replacing or deleting anything that is not ours.

:func:`rename_noreplace` is the one rename of the ledger (a retired reserve), of reserve
recovery and of the temporary commit below: an existing file is never replaced. An output file
(a repaired recording, later an export) is first written to ``.<final name>.<16 hex>.tmp`` in
the final name's directory, created ``O_EXCL``, then synced and committed by
:func:`rename_noreplace`, so a final name that appeared meanwhile is never overwritten. The only
replacement is an explicit ``force`` over an existing regular file owned by the caller. A failure
removes the temporary file, and only that: by directory descriptor and name, after checking that
the name still denotes the descriptor this process holds open.

Nothing here deletes a file this process did not create in the same call. Every system call goes
through a :class:`~ironbird.fsops.FileSystem`, so tests can fail any step.
"""

from __future__ import annotations

import contextlib
import errno
import fcntl
import os
import stat
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Final

from ironbird.fsops import (
    INSPECT_FLAGS,
    REAL_FS,
    FileSystem,
    close_quietly,
    same_file,
    try_stat_at,
)

_NO_HARD_LINKS: Final = frozenset({errno.EPERM, errno.EOPNOTSUPP, errno.ENOSYS, errno.EXDEV})
_NAME_FLAGS: Final = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC


class Collision(OSError):
    """The final name already exists; nothing was changed."""

    def __init__(self, name: str) -> None:
        super().__init__(errno.EEXIST, "file exists", name)


class Locked(Collision):
    """The final name is an existing file that another process holds under ``flock``: a live
    writer's file, never replaced, ``--force`` or not."""

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.strerror = "file is in use (locked by another process)"


class SyncAfterCommit(OSError):
    """The rename was committed but syncing the directory failed: the new name may not survive
    a power loss. The rename is not undone."""


def rename_noreplace(dir_fd: int, old: str, new: str, fd: int, *, fs: FileSystem = REAL_FS) -> None:
    """Rename ``old`` to ``new`` (both names in the directory ``dir_fd``) unless ``new`` exists.

    ``fd`` is the open descriptor of the file (or directory) and ``old`` must still denote it.
    Raises :class:`Collision` when ``new`` exists, :class:`SyncAfterCommit` when only the
    directory sync after a committed rename failed, and ``OSError`` for any other failure, in
    which case nothing was changed (a placeholder this call made is removed again).
    """
    held, named = fs.fstat(fd), try_stat_at(fs, dir_fd, old)
    if named is None or not same_file(held, named):
        raise OSError(errno.ENOENT, "the name no longer denotes the file", old)
    try:
        renamed = fs.renameat2_noreplace(dir_fd, old, new)
    except FileExistsError:
        raise Collision(new) from None
    # renameat2 unavailable here: a hard link for a regular file, else the placeholder rename
    if renamed is None and not (stat.S_ISREG(held.st_mode) and _link(fs, dir_fd, old, new, held)):
        _placeholder_rename(fs, dir_fd, old, new, held)
    try:
        fs.fsync(dir_fd)
    except OSError as exc:
        raise SyncAfterCommit(exc.errno, exc.strerror or "directory sync failed", new) from exc


def _link(fs: FileSystem, dir_fd: int, old: str, new: str, held: os.stat_result) -> bool:
    """Commit by hard link, then unlink of the old name. ``False``: hard links are unavailable
    here. A name that is taken raises :class:`Collision`."""
    try:
        fs.link_at(dir_fd, old, new)
    except FileExistsError:
        current = try_stat_at(fs, dir_fd, new)
        if current is None or not same_file(held, current):
            raise Collision(new) from None
        # An earlier link went through and its reply was lost (NFSv3): the name is ours.
    except OSError as exc:
        if exc.errno in _NO_HARD_LINKS:
            return False
        raise
    now_new, now_old = try_stat_at(fs, dir_fd, new), try_stat_at(fs, dir_fd, old)
    if now_new is None or not same_file(held, now_new):
        raise OSError(errno.EIO, "the committed name does not denote the file", new)
    if now_old is not None and same_file(held, now_old):
        fs.unlink_at(dir_fd, old)  # the second step of the link rename: our own old name
    return True


def _placeholder_rename(
    fs: FileSystem, dir_fd: int, old: str, new: str, held: os.stat_result
) -> None:
    """Create the final name exclusively (an empty file, or a directory), then rename over it.

    A crash between the two steps leaves the empty placeholder at the final name, which a later
    run refuses as an existing name; any failure after the placeholder exists removes the
    placeholder this call created.
    """
    is_dir = stat.S_ISDIR(held.st_mode)
    placeholder_fd = -1
    try:
        if is_dir:
            fs.mkdir_at(dir_fd, new, 0o700)
        else:
            placeholder_fd = fs.open_at(dir_fd, new, _NAME_FLAGS, 0o600)
    except FileExistsError:
        raise Collision(new) from None
    try:
        made = fs.fstat(placeholder_fd) if placeholder_fd >= 0 else try_stat_at(fs, dir_fd, new)
        try:
            fs.rename_at(dir_fd, old, new)
        except OSError as exc:
            current = try_stat_at(fs, dir_fd, new)
            if made is not None and current is not None and same_file(made, current):
                with contextlib.suppress(OSError):
                    if is_dir:
                        fs.rmdir_at(dir_fd, new)  # the placeholder directory we made
                    else:
                        fs.unlink_at(dir_fd, new)  # the placeholder file we made
            if is_dir and exc.errno in (errno.ENOTEMPTY, errno.EEXIST):
                raise Collision(new) from exc
            raise
    finally:
        close_quietly(fs, placeholder_fd)


class TempOutput:
    """A file being written under a temporary name, to be committed to ``final``.

    Use as ``with TempOutput(final, force=...) as out:`` write to ``out.fd``, then
    ``out.commit()``. Leaving the block without a commit removes the temporary file.

    ``reuse`` names temporary files of an earlier, interrupted attempt in the same directory:
    the first that is a regular file of this user with one link and that ``accept(fd)`` approves
    (the caller checks its content) is continued instead of creating a new one - appended to
    through ``out.fd``, committed like any other. A reused file is not ours to delete: leaving
    the block without a commit keeps it for the next attempt.
    """

    def __init__(
        self,
        final: Path,
        *,
        force: bool = False,
        fs: FileSystem = REAL_FS,
        reuse: Sequence[str] = (),
        accept: Callable[[int], bool] | None = None,
    ) -> None:
        assert not reuse or accept is not None, "a reused temporary needs a judge of its content"
        self.final = final
        self.force = force
        self.fd = -1
        self.name = ""
        self._fs = fs
        self._dir_fd = -1
        self._committed = False
        self._reuse = tuple(reuse)
        self._accept = accept
        self._reused = False

    def __enter__(self) -> TempOutput:
        fs = self._fs
        directory = self.final.parent
        self._dir_fd = fs.open_dir(directory)
        try:
            for candidate in self._reuse:
                if self._adopt(candidate):
                    return self
            for _ in range(8):
                name = f".{self.final.name}.{os.urandom(8).hex()}.tmp"
                try:
                    self.fd = fs.open_at(self._dir_fd, name, _NAME_FLAGS, 0o600)
                except FileExistsError:
                    continue
                self.name = name
                return self
            raise OSError(errno.EEXIST, "no free temporary name", str(directory))
        except BaseException:
            close_quietly(fs, self._dir_fd)
            raise

    def _adopt(self, name: str) -> bool:
        """Take over ``name`` if it is a plain file of ours that the caller accepts. It is opened
        for reading (to check it) and appending, never truncated."""
        fs, accept = self._fs, self._accept
        assert accept is not None
        flags = os.O_RDWR | os.O_APPEND | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_NOCTTY | os.O_CLOEXEC
        try:
            fd = fs.open_at(self._dir_fd, name, flags)
        except OSError:
            return False
        try:
            st = fs.fstat(fd)
            usable = (
                stat.S_ISREG(st.st_mode)
                and st.st_uid == os.getuid()
                and st.st_nlink == 1
                and accept(fd)
            )
        except OSError:
            usable = False
        if not usable:
            close_quietly(fs, fd)
            return False
        self.fd, self.name, self._reused = fd, name, True
        return True

    @property
    def path(self) -> Path:
        return self.final.parent / self.name

    def write(self, data: bytes | bytearray | memoryview) -> None:
        view = memoryview(data)
        while view:
            written = self._fs.write(self.fd, view)
            if written <= 0:
                raise OSError(errno.EIO, "write made no progress", self.name)
            view = view[written:]

    def commit(self) -> None:
        """Sync the file and give it its final name. An existing name is refused
        (:class:`Collision`) unless ``force`` and it is an existing regular file owned by this
        user, which is replaced."""
        fs = self._fs
        fs.fdatasync(self.fd)
        final = self.final.name
        if self.force:
            existing = try_stat_at(fs, self._dir_fd, final)
            if (
                existing is not None
                and stat.S_ISREG(existing.st_mode)
                and existing.st_uid == os.getuid()
            ):
                if self._in_use(final):
                    raise Locked(final)
                fs.rename_at(self._dir_fd, self.name, final)  # --force: the one replacement
                self._committed = True
                try:
                    fs.fsync(self._dir_fd)
                except OSError as exc:
                    raise SyncAfterCommit(
                        exc.errno, exc.strerror or "directory sync failed", final
                    ) from exc
                return
        try:
            rename_noreplace(self._dir_fd, self.name, final, self.fd, fs=fs)
        except SyncAfterCommit:
            self._committed = True  # renamed; only the directory sync failed
            raise
        self._committed = True

    def _in_use(self, name: str) -> bool:
        """Whether another process holds ``name`` under ``flock`` (probed ``LOCK_EX | LOCK_NB``
        and released at once; a file that cannot be probed counts as in use)."""
        fs = self._fs
        try:
            probe = fs.open_at(self._dir_fd, name, INSPECT_FLAGS)
        except FileNotFoundError:
            return False
        except OSError:
            return True
        try:
            try:
                fs.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                # held by someone; any other error (locks unsupported) proves nothing
                return exc.errno in (errno.EWOULDBLOCK, errno.EAGAIN)
            with contextlib.suppress(OSError):
                fs.flock(probe, fcntl.LOCK_UN)
            return False
        finally:
            close_quietly(fs, probe)

    def __exit__(self, *exc_info: object) -> None:
        fs = self._fs
        try:
            if not self._committed and self.name and not self._reused:
                held, named = fs.fstat(self.fd), try_stat_at(fs, self._dir_fd, self.name)
                if named is not None and same_file(held, named):
                    with contextlib.suppress(OSError):
                        fs.unlink_at(self._dir_fd, self.name)  # our own uncommitted temporary
        finally:
            close_quietly(fs, self.fd)
            close_quietly(fs, self._dir_fd)
