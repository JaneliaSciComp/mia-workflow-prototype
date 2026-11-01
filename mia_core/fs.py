"""Filesystem checks: which mount holds a path (network or host-local) and whether a secret file is private."""

from __future__ import annotations

import os
import stat
from pathlib import Path

NETWORK_FS = frozenset(
    {"nfs", "nfs4", "cifs", "smb3", "smbfs", "lustre", "gpfs", "beegfs", "ceph", "glusterfs", "fuse.sshfs", "autofs"}
)


def mount_type(path: Path, mounts: str | None = None) -> str:
    """The filesystem type of the mount holding `path`; "unknown" when no mount entry matches."""
    resolved = path.resolve()
    try:
        os.stat(resolved)  # triggers an automount, so the real mount shows up in the table
    except OSError:
        pass  # not created yet: the parent mount decides
    if mounts is None:
        mounts = Path("/proc/self/mounts").read_text()
    best_point, best_type = "", "unknown"
    for line in mounts.splitlines():
        fields = line.split(" ")
        if len(fields) < 3:
            continue
        mount_point = fields[1].replace("\\040", " ")
        matches = resolved == Path(mount_point) or resolved.is_relative_to(mount_point)
        # >=: when two entries share a point (autofs, then the real mount), the later one is the visible one
        if matches and len(mount_point) >= len(best_point):
            best_point, best_type = mount_point, fields[2]
    return best_type


def is_network_fs(path: Path, mounts: str | None = None) -> bool:
    """True when `path` lives on a network (or still-automounting) filesystem."""
    return mount_type(path, mounts) in NETWORK_FS


def require_local_dir(path: Path) -> None:
    """Create `path` and refuse it when it is on a network filesystem (locks and SQLite need local disk)."""
    path.mkdir(parents=True, exist_ok=True)
    if is_network_fs(path):
        raise RuntimeError(f"{path} is on {mount_type(path)}, a network filesystem; it must be host-local disk")


def require_secret_file(path: Path) -> None:
    """Refuse a secret file that is missing, not a regular file, a symlink, or not mode 600."""
    try:
        mode = os.lstat(path).st_mode
    except OSError as error:
        raise RuntimeError(
            f"{path} cannot be read ({error.strerror}); it must be a regular file with mode 600"
        ) from error
    if not stat.S_ISREG(mode) or stat.S_IMODE(mode) != 0o600:
        raise RuntimeError(f"{path} must be a regular file with mode 600 (chmod 600)")


def dir_bytes(path: Path) -> int:
    """Total size of the regular files under `path` (symlinks not followed); 0 when `path` is missing."""
    total = 0
    try:
        with os.scandir(path) as entries:
            for entry in entries:
                try:
                    if entry.is_dir(follow_symlinks=False):
                        total += dir_bytes(Path(entry.path))
                    elif entry.is_file(follow_symlinks=False):
                        total += entry.stat(follow_symlinks=False).st_size
                except FileNotFoundError:
                    pass  # removed while we walked
    except FileNotFoundError:
        pass
    return total
