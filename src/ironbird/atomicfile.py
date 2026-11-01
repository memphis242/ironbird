"""Renaming and writing files without replacing or deleting anything that is not ours.

:func:`rename_noreplace` is the one rename of the ledger (a retired reserve), of reserve
recovery and of the temporary commit below: an existing file is never replaced. An output file
(a repaired recording, an export) is first written to ``.<final name>.<16 hex>.tmp`` in
the final name's directory, created ``O_EXCL``, then synced and committed by
:func:`rename_noreplace`, so a final name that appeared meanwhile is never overwritten. The only
replacement is an explicit ``force`` over an existing regular file owned by the caller. A failure
removes the temporary file, and only that: by directory descriptor and name, after checking that
the name still denotes the descriptor this process holds open.

A commit has two phases. :meth:`TempOutput.prepare` (and :meth:`TempDirOutput.prepare`) syncs
the data, the members and the temporary directory; it changes no name, so a signal there simply
aborts and the cleanup removes the temporary. :meth:`~TempOutput.publish` is the commit point: a
signal owed by then is acted on as it begins (:func:`~ironbird.sigmask.checkpoint`, or
:meth:`~ironbird.sigmask.Hold.begin`), so nothing is published. It then gives the final name:
the rename (renameat2, link and unlink, or placeholder and rename), the directory sync and the
note that it happened (``committed``), in a held scope (:func:`~ironbird.sigmask.signals_held`),
so a signal that arrives meanwhile is acted on only once the output is durable and known to be
committed. A caller that must also finish its own record of the output before a signal acts
passes a :class:`~ironbird.sigmask.Hold`, which ``publish`` begins right before the rename and
the caller ends.

A signal cannot split any other step that changes the file system from the record of that
change either, because each runs held too: creating a temporary (file, directory or member) and
recording it for removal, and removing what was created. A caller that registers the cleanup
apart from a ``with`` statement (an :class:`contextlib.ExitStack`) holds them across the
registration too.

Nothing here deletes a file this process did not create in the same call. Every system call goes
through a :class:`~ironbird.fsops.FileSystem`, so tests can fail any step.
"""

from __future__ import annotations

import contextlib
import errno
import fcntl
import os
import stat
from collections import OrderedDict
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from types import TracebackType
from typing import Final, TypeVar

from ironbird.fsops import (
    INSPECT_FLAGS,
    OPEN_DIR_FLAGS,
    REAL_FS,
    FileSystem,
    close_quietly,
    same_file,
    try_stat_at,
)
from ironbird.sigmask import Hold, checkpoint, signals_held

_NO_HARD_LINKS: Final = frozenset({errno.EPERM, errno.EOPNOTSUPP, errno.ENOSYS, errno.EXDEV})
_NAME_FLAGS: Final = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
_OUT_OF_DESCRIPTORS: Final = frozenset({errno.EMFILE, errno.ENFILE})
_T = TypeVar("_T")


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
    """The rename was committed but a step after it failed: syncing the directory (the new name
    may not survive a power loss) or, for :class:`LeftoverAfterCommit`, removing the old name.
    The rename is not undone."""


class LeftoverAfterCommit(SyncAfterCommit):
    """A hard-link commit went through (the final name denotes the file) but the old, temporary
    name could not be removed: ``leftover`` names it, and it is not removed by this process any
    more. ``sync_error`` is the reason the directory sync that followed failed, if it did."""

    def __init__(self, errno_: int, strerror: str, leftover: str) -> None:
        super().__init__(errno_, strerror, leftover)
        self.leftover = leftover
        self.sync_error: str | None = None


def rename_noreplace(dir_fd: int, old: str, new: str, fd: int, *, fs: FileSystem = REAL_FS) -> None:
    """Rename ``old`` to ``new`` (both names in the directory ``dir_fd``) unless ``new`` exists.

    ``fd`` is the open descriptor of the file (or directory) and ``old`` must still denote it.
    Raises :class:`Collision` when ``new`` exists, :class:`SyncAfterCommit` when only the
    directory sync after a committed rename failed, :class:`LeftoverAfterCommit` when a hard-link
    commit could not remove the old name, and ``OSError`` for any other failure, in which case
    nothing was changed (a placeholder this call made is removed again).

    The rename and the directory sync run with the termination signals held, so a signal that
    lands in them is acted on only once the commit is durable.
    """
    held, named = fs.fstat(fd), try_stat_at(fs, dir_fd, old)
    if named is None or not same_file(held, named):
        raise OSError(errno.ENOENT, "the name no longer denotes the file", old)
    # One step a signal cannot split: the link without its unlink, or the placeholder without
    # the rename over it, would be left behind, and a committed name must get its directory
    # sync. A signal is acted on as the scope ends.
    with signals_held():
        leftover: LeftoverAfterCommit | None = None
        try:
            renamed = fs.renameat2_noreplace(dir_fd, old, new)
        except FileExistsError:
            raise Collision(new) from None
        # renameat2 unavailable here: a hard link for a regular file, else the placeholder rename
        try:
            if renamed is None and not (
                stat.S_ISREG(held.st_mode) and _link(fs, dir_fd, old, new, held)
            ):
                _placeholder_rename(fs, dir_fd, old, new, held)
        except LeftoverAfterCommit as exc:
            leftover = exc  # the name is committed all the same: sync it, then report
        try:
            fs.fsync(dir_fd)
        except OSError as exc:
            reason = exc.strerror or "directory sync failed"
            if leftover is None:
                raise SyncAfterCommit(exc.errno, reason, new) from exc
            leftover.sync_error = reason
        if leftover is not None:
            raise leftover


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
        try:
            fs.unlink_at(dir_fd, old)  # the second step of the link rename: our own old name
        except FileNotFoundError:
            pass  # a retransmitted unlink whose first reply was lost: the name is gone
        except OSError as exc:
            # The link went through: the final name is the export. Nothing is undone.
            raise LeftoverAfterCommit(
                exc.errno or errno.EIO, exc.strerror or "the old name was not removed", old
            ) from exc
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
        self._prepared = False
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
                # The file is created and recorded as ours in one step no signal can split; a
                # signal is acted on as the step ends, and the cleanup below removes the file.
                with signals_held():
                    try:
                        fd = fs.open_at(self._dir_fd, name, _NAME_FLAGS, 0o600)
                    except FileExistsError:
                        continue
                    self.fd, self.name = fd, name
                return self
            raise OSError(errno.EEXIST, "no free temporary name", str(directory))
        except BaseException as exc:
            try:
                with contextlib.suppress(OSError):  # the reason it failed is what is raised
                    self._discard(exc)
            finally:
                close_quietly(fs, self.fd)
                close_quietly(fs, self._dir_fd)
                self.fd = self._dir_fd = -1
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
        except BaseException:  # a signal acted on while the caller judged the content
            close_quietly(fs, fd)
            raise
        if not usable:
            close_quietly(fs, fd)
            return False
        self.fd, self.name, self._reused = fd, name, True
        return True

    @property
    def path(self) -> Path:
        return self.final.parent / self.name

    def write(self, data: bytes | bytearray | memoryview) -> None:
        assert not self._prepared, "nothing is written after prepare"
        view = memoryview(data)
        while view:
            written = self._fs.write(self.fd, view)
            if written <= 0:
                raise OSError(errno.EIO, "write made no progress", self.name)
            view = view[written:]

    @property
    def committed(self) -> bool:
        """Whether the file has its final name (a failed directory sync or a leftover old name
        after the rename counts: the rename is not undone)."""
        return self._committed

    def commit(self, *, hold: Hold | None = None) -> None:
        """:meth:`prepare`, then :meth:`publish`."""
        self.prepare()
        self.publish(hold=hold)

    def prepare(self) -> None:
        """Sync the file's data. Nothing is renamed, so this is interruptible: a signal here
        ends the work and leaving the block removes the temporary."""
        assert self.fd >= 0 and not self._committed
        self._fs.fdatasync(self.fd)
        self._prepared = True

    def publish(self, *, hold: Hold | None = None) -> None:
        """Give the prepared file its final name. An existing name is refused
        (:class:`Collision`) unless ``force`` and it is an existing regular file owned by this
        user, which is replaced.

        A signal owed as this begins is acted on first, so nothing is published. ``hold``
        (begun first) keeps a later signal held past this call, until the caller's ``with``
        block ends. In any case it is held until the directory is synced and :attr:`committed`
        set, so a finished commit is never reported as interrupted; it is acted on as this
        method ends (or the hold does).
        """
        assert self._prepared and not self._committed, "publish once, after prepare"
        if hold is not None:
            hold.begin()
        else:
            checkpoint()  # the commit point: a signal that came before it publishes nothing
        fs = self._fs
        final = self.final.name
        with signals_held():
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
                self._committed = True  # renamed; only a step after it failed
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

    def _discard(self, unwinding: BaseException | None) -> None:
        """Remove the temporary file this process created, unless it was committed, reused, or
        its name no longer denotes the file held open. ``unwinding``: the exception this
        cleanup runs for, which a signal acted on here replaces."""
        if self._committed or not self.name or self._reused:
            return
        assert self.fd >= 0 and self._dir_fd >= 0, "a named temporary is held open"
        fs = self._fs
        # a signal that lands in the removal does not cut it short
        with signals_held(unwinding=unwinding):
            held, named = fs.fstat(self.fd), try_stat_at(fs, self._dir_fd, self.name)
            if named is not None and same_file(held, named):
                with contextlib.suppress(OSError):
                    fs.unlink_at(self._dir_fd, self.name)  # our own uncommitted temporary

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        fs = self._fs
        try:
            self._discard(exc)
        finally:
            close_quietly(fs, self.fd)
            close_quietly(fs, self._dir_fd)


@dataclass(slots=True)
class _Member:
    name: str
    ident: tuple[int, int]  # (st_dev, st_ino) of the file this process created
    fd: int = -1  # -1: closed (evicted, or never needed again)


class TempDirOutput:
    """A directory of files being written under a temporary name, to be committed to ``final``.

    Use as ``with TempDirOutput(final) as out:``, ``member = out.create("a.csv")``,
    ``out.write(member, data)``, then ``out.commit()``. The directory is created as
    ``.<final name>.<16 hex>.tmp`` (mode 0700) beside ``final`` and every member inside it is
    created ``O_EXCL``; ``commit`` syncs the members and the directory and renames the directory
    to ``final`` without replacing anything, so a ``final`` that appeared meanwhile is refused
    (:class:`Collision`) and an existing directory is never replaced, ``--force`` or not.

    Only ``max_open`` members hold a descriptor at a time; a member whose descriptor was given up
    is appended to again through a descriptor that is checked to denote the file this process
    created. When the process runs out of descriptors anyway (``EMFILE``, ``ENFILE``) the least
    recently written member gives its descriptor up, ``max_open`` drops to the number still held,
    and the open is tried again; with none left to give up the error is raised as it is.
    Leaving the block without a commit removes the members this process created (by name, after
    checking that the name still denotes the file created) and then the directory with
    ``rmdir``; anything else inside is left alone, and the directory with it
    (:attr:`leftover` names it). A member that cannot be identified (``fstat`` failed on the
    descriptor just opened) is removed by name at once, before :meth:`create` raises: the call
    created that name exclusively, inside the directory this process made and holds open.
    """

    def __init__(self, final: Path, *, fs: FileSystem = REAL_FS, max_open: int = 64) -> None:
        assert max_open >= 1
        self.final = final
        self.name = ""
        self.leftover: str | None = None  # the temporary directory, when it could not be removed
        self._fs = fs
        self._max_open = max_open
        self._parent_fd = -1
        self._dir_fd = -1
        self._members: list[_Member] = []
        self._by_name: set[str] = set()
        self._open: OrderedDict[int, None] = OrderedDict()  # members with a descriptor, LRU first
        self._committed = False
        self._prepared = False

    def __enter__(self) -> TempDirOutput:
        fs = self._fs
        self._parent_fd = fs.open_dir(self.final.parent)
        try:
            for _ in range(8):
                name = f".{self.final.name}.{os.urandom(8).hex()}.tmp"
                # created and recorded as ours in one step no signal can split (see
                # TempOutput); the cleanup below removes the directory
                with signals_held():
                    try:
                        fs.mkdir_at(self._parent_fd, name, 0o700)
                    except FileExistsError:
                        continue
                    self.name = name
                break
            else:
                raise OSError(errno.EEXIST, "no free temporary name", str(self.final.parent))
            self._dir_fd = fs.open_at(self._parent_fd, name, OPEN_DIR_FLAGS | os.O_NOFOLLOW)
            named = try_stat_at(fs, self._parent_fd, name)
            if named is None or not same_file(fs.fstat(self._dir_fd), named):
                raise OSError(errno.EIO, "the temporary directory was replaced", name)
        except BaseException as exc:
            try:
                with contextlib.suppress(OSError):  # the reason it failed is what is raised
                    self._discard(exc)
            finally:
                close_quietly(fs, self._dir_fd)
                close_quietly(fs, self._parent_fd)
                self._dir_fd = self._parent_fd = -1
            raise
        return self

    @property
    def path(self) -> Path:
        return self.final.parent / self.name

    def create(self, name: str) -> int:
        """Create the member ``name`` (a bare file name) exclusively and return its handle."""
        assert name and "/" not in name and name not in (".", "..")
        assert name not in self._by_name, "a member is created once"
        assert not self._prepared, "nothing is created after prepare"
        fs = self._fs
        # created and recorded as a member in one step no signal can split, so leaving
        # the block removes it (see TempOutput)
        with signals_held():
            fd = self._with_room(lambda: fs.open_at(self._dir_fd, name, _NAME_FLAGS, 0o600))
            try:
                made = fs.fstat(fd)
            except OSError:
                close_quietly(fs, fd)
                # Not identified, so _discard could not tell it is ours later; but this call
                # created the name exclusively a moment ago, in the private directory this
                # process made and holds open. Left in place, it keeps the directory (leftover).
                with contextlib.suppress(OSError):
                    fs.unlink_at(self._dir_fd, name)
                raise
            member = _Member(name, (made.st_dev, made.st_ino), fd)
            self._members.append(member)
            self._by_name.add(name)
            handle = len(self._members) - 1
            self._open[handle] = None
        return handle

    def write(self, handle: int, data: bytes | bytearray | memoryview) -> None:
        assert not self._prepared, "nothing is written after prepare"
        member = self._members[handle]
        if member.fd < 0:
            self._reopen(handle)
        else:
            self._open.move_to_end(handle)
        view = memoryview(data)
        while view:
            written = self._fs.write(member.fd, view)
            if written <= 0:
                raise OSError(errno.EIO, "write made no progress", member.name)
            view = view[written:]

    def _give_up_oldest(self) -> None:
        oldest, _ = self._open.popitem(last=False)
        member = self._members[oldest]
        fd, member.fd = member.fd, -1  # forgotten first: never closed twice, whatever lands here
        close_quietly(self._fs, fd)

    def _with_room(self, opener: Callable[[], _T]) -> _T:
        """``opener()`` with fewer than ``max_open`` descriptors held; if the process is out of
        descriptors all the same, one more is given up and ``max_open`` follows, until it
        works or no member has a descriptor left to give up."""
        while True:
            while len(self._open) >= self._max_open:
                self._give_up_oldest()
            try:
                return opener()
            except OSError as exc:
                if exc.errno not in _OUT_OF_DESCRIPTORS or not self._open:
                    raise
                self._give_up_oldest()
                self._max_open = max(1, len(self._open))

    def _reopen(self, handle: int) -> None:
        fs, member = self._fs, self._members[handle]
        flags = (
            os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_NOCTTY | os.O_CLOEXEC
        )
        # append to a file this process made
        fd = self._with_room(lambda: fs.open_at(self._dir_fd, member.name, flags))
        try:
            now = fs.fstat(fd)
            if (now.st_dev, now.st_ino) != member.ident or not stat.S_ISREG(now.st_mode):
                raise OSError(
                    errno.EIO, "the member no longer denotes the file created", member.name
                )
        except OSError:
            close_quietly(fs, fd)
            raise
        member.fd = fd
        self._open[handle] = None

    @property
    def committed(self) -> bool:
        """Whether the directory has its final name (see :attr:`TempOutput.committed`)."""
        return self._committed

    def commit(self, *, hold: Hold | None = None) -> None:
        """:meth:`prepare`, then :meth:`publish`."""
        self.prepare()
        self.publish(hold=hold)

    def prepare(self) -> None:
        """Sync every member and the directory, and close the members. Nothing is renamed, so
        this is interruptible (see :meth:`TempOutput.prepare`)."""
        assert self.name and not self._committed
        fs = self._fs
        for member in self._members:
            checkpoint()  # a signal ends the syncs here: the cleanup removes the directory
            if member.fd >= 0:
                fs.fdatasync(member.fd)
                continue
            fd = self._with_room(partial(fs.open_at, self._dir_fd, member.name, INSPECT_FLAGS))
            try:
                now = fs.fstat(fd)
                if (now.st_dev, now.st_ino) != member.ident:
                    raise OSError(errno.EIO, "the member changed", member.name)
                fs.fdatasync(fd)
            finally:
                close_quietly(fs, fd)
        for member in self._members:
            fd, member.fd = member.fd, -1  # forgotten first, as in _give_up_oldest
            close_quietly(fs, fd)
        self._open.clear()
        fs.fsync(self._dir_fd)
        self._prepared = True

    def publish(self, *, hold: Hold | None = None) -> None:
        """Give the prepared directory its final name. An existing name is refused
        (:class:`Collision`), whatever it is. ``hold`` and the signals as in
        :meth:`TempOutput.publish`."""
        assert self._prepared and not self._committed, "publish once, after prepare"
        if hold is not None:
            hold.begin()
        else:
            checkpoint()  # the commit point: a signal that came before it publishes nothing
        with signals_held():
            try:
                rename_noreplace(
                    self._parent_fd, self.name, self.final.name, self._dir_fd, fs=self._fs
                )
            except SyncAfterCommit:
                self._committed = True  # renamed; only a step after it failed
                raise
            self._committed = True

    def _discard(self, unwinding: BaseException | None) -> None:
        """Remove what this process created: its members, then the (then empty) directory.
        ``unwinding`` as in :meth:`TempOutput._discard`."""
        fs = self._fs
        if not self.name:
            return
        # a signal that lands in the removal does not cut it short
        with signals_held(unwinding=unwinding):
            if self._dir_fd >= 0:
                named = try_stat_at(fs, self._parent_fd, self.name)
                if named is None or not same_file(fs.fstat(self._dir_fd), named):
                    return  # the name no longer denotes the directory made; not ours to remove
                for member in self._members:
                    found = try_stat_at(fs, self._dir_fd, member.name)
                    if found is not None and (found.st_dev, found.st_ino) == member.ident:
                        with contextlib.suppress(OSError):
                            fs.unlink_at(self._dir_fd, member.name)  # a file this process made
            try:
                fs.rmdir_at(self._parent_fd, self.name)  # fails, harmlessly, if anything is left
            except FileNotFoundError:
                pass
            except OSError:
                self.leftover = self.name

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        fs = self._fs
        try:
            if not self._committed:
                self._discard(exc)
        finally:
            for member in self._members:
                close_quietly(fs, member.fd)
            close_quietly(fs, self._dir_fd)
            close_quietly(fs, self._parent_fd)
