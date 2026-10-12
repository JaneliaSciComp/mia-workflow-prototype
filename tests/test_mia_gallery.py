import hashlib

import pytest

from mia_data.gallery import gallery_files, publish_gallery


class FakeGH:
    repo = "o/data"

    def __init__(self, existing=()):
        self.existing, self.calls, self.puts = set(existing), [], []

    async def ensure_branch(self, branch):
        self.calls.append(("ensure_branch", branch))

    async def file_exists(self, path, ref):
        return path in self.existing

    async def upsert_file(self, path, content, branch, message):
        self.calls.append(("upsert_file", path, message))
        if await self.file_exists(path, branch):
            return False
        self.puts.append(path)
        self.existing.add(path)
        return True


def _tree(tmp_path, ids=("c1", "c2")):
    crops, prev = tmp_path / "crops", tmp_path / "preview"
    verified = []
    for c in ids:
        (crops / c).mkdir(parents=True), (prev / c).mkdir(parents=True)
        (crops / c / "raw.png").write_bytes(b"raw" + c.encode())
        (crops / c / "junk.txt").write_text("never published")
        (prev / c / "overlay.png").write_bytes(b"ov" + c.encode())
        verified.append({"crop_id": c, "files": {"raw.png": hashlib.sha256(b"raw" + c.encode()).hexdigest()}})
    (tmp_path / "b.yaml").write_text("record: x\n")
    (tmp_path / "v.json").write_text("{}")
    return crops, prev, verified


def test_gallery_files_only_verified(tmp_path):
    crops, prev, verified = _tree(tmp_path, ("c1", "c2", "c3"))
    files = gallery_files(crops, prev, verified[:2], tmp_path / "b.yaml", tmp_path / "v.json")
    assert sorted(files) == ["binding.yaml", "c1/overlay.png", "c1/raw.png", "c2/overlay.png", "c2/raw.png", "verify.json"]
    (crops / "c1" / "raw.png").write_bytes(b"swapped")
    with pytest.raises(ValueError):
        gallery_files(crops, prev, verified[:1], tmp_path / "b.yaml", tmp_path / "v.json")


async def test_publish_gallery(tmp_path):
    pre = "galleries/rec/i1-e2/abc"
    gh = FakeGH(existing={f"{pre}/c1/raw.png"})
    files = {"c1/raw.png": b"1", "c1/overlay.png": b"2", "binding.yaml": b"3"}
    urls = await publish_gallery(gh, "galleries", pre, files)
    assert [c for c in gh.calls if c[0] == "ensure_branch"] == [("ensure_branch", "galleries")]
    assert sorted(gh.puts) == [f"{pre}/binding.yaml", f"{pre}/c1/overlay.png"]
    assert all(c[2] == f"mia-data: {pre}" for c in gh.calls if c[0] == "upsert_file")
    assert urls["c1/raw.png"] == f"https://github.com/o/data/blob/galleries/{pre}/c1/raw.png?raw=true"
    assert set(urls) == set(files) and all(u.endswith("?raw=true") for u in urls.values())
