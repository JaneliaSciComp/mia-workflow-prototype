import os
from pathlib import Path

import pytest

from mia_core import fs

MOUNTS = (
    "/dev/root / xfs rw 0 0\n"
    "auto /groups autofs rw 0 0\n"
    "auto /netfs autofs rw 0 0\n"
    "host:/vol /netfs/lab nfs rw 0 0\n"
    "/dev/sdb /mnt/my\\040disk ext4 rw 0 0\n"
    "systemd-1 /scratch autofs rw 0 0\n"
    "/dev/sdb /scratch xfs rw 0 0\n"
)


@pytest.mark.parametrize(
    "path, fstype",
    [
        ("/scratch/mia", "xfs"),
        ("/scratch", "xfs"),  # autofs then xfs at /scratch: the later entry is the visible one
        ("/netfs/lab/mia", "nfs"),
        ("/netfs/other", "autofs"),
        ("/groups/x/y", "autofs"),
        ("/mnt/my disk/a", "ext4"),
    ],
)
def test_mount_type_takes_the_longest_mount_point(path, fstype):
    assert fs.mount_type(Path(path), MOUNTS) == fstype


def test_mount_type_unknown_when_nothing_matches():
    assert fs.mount_type(Path("/scratch/mia"), "") == "unknown"
    assert not fs.is_network_fs(Path("/scratch/mia"), "")


@pytest.mark.parametrize("path, network", [("/scratch/mia", False), ("/netfs/lab/mia", True), ("/groups/x", True)])
def test_is_network_fs(path, network):
    assert fs.is_network_fs(Path(path), MOUNTS) is network


def test_mount_type_stats_the_path_first(tmp_path, monkeypatch):
    seen = []
    real = os.stat
    monkeypatch.setattr(fs.os, "stat", lambda p, *a, **k: (seen.append(Path(p)), real(p, *a, **k))[1])
    fs.mount_type(tmp_path, MOUNTS)
    assert tmp_path.resolve() in seen


def test_require_local_dir_creates_and_accepts_local(tmp_path, monkeypatch):
    monkeypatch.setattr(fs, "mount_type", lambda p, mounts=None: "xfs")
    fs.require_local_dir(tmp_path / "locks")
    assert (tmp_path / "locks").is_dir()


def test_require_local_dir_rejects_network(tmp_path, monkeypatch):
    monkeypatch.setattr(fs, "mount_type", lambda p, mounts=None: "nfs")
    with pytest.raises(RuntimeError, match="nfs, a network filesystem"):
        fs.require_local_dir(tmp_path / "locks")


@pytest.mark.parametrize("mode", [0o644, 0o640, 0o660, 0o700])
def test_require_secret_file_rejects_loose_modes(tmp_path, mode):
    tok = tmp_path / "tok"
    tok.write_text("x")
    tok.chmod(mode)
    with pytest.raises(RuntimeError, match="mode 600"):
        fs.require_secret_file(tok)


def test_require_secret_file_accepts_600_and_rejects_missing_or_symlink(tmp_path):
    tok = tmp_path / "tok"
    tok.write_text("x")
    tok.chmod(0o600)
    fs.require_secret_file(tok)
    with pytest.raises(RuntimeError):
        fs.require_secret_file(tmp_path / "missing")
    link = tmp_path / "link"
    link.symlink_to(tok)
    with pytest.raises(RuntimeError):
        fs.require_secret_file(link)
