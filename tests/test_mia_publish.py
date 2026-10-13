import hashlib
import json
import os
import shutil
import threading
import time

import pytest

from mia_data.publish import Conflict, publish, reindex

AGE = 3600


def _src(tmp_path, ids=("c1", "c2"), snippet="tiff-zip-member", name="src"):
    src, verified = tmp_path / name, []
    for c in ids:
        d = src / c
        (d / "labels").mkdir(parents=True)
        files = {
            "raw.png": b"png" + c.encode(),
            "labels/k-x-instance.png": b"lab" + c.encode(),
            "crop.json": json.dumps(
                {"crop_id": c, "plane": {"z": 0, "c": 0}, "snippets": [{"name": snippet, "commit": "4c1d"}]}
            ).encode(),
        }
        for rel, data in files.items():
            (d / rel).write_bytes(data)
        verified.append({"crop_id": c, "files": {r: hashlib.sha256(v).hexdigest() for r, v in files.items()}})
    return src, verified


def test_publish_twice_is_idempotent(tmp_path):
    src, verified = _src(tmp_path)
    ds = tmp_path / "ds"
    assert publish(ds, src, verified, {}, tmp_max_age_s=AGE) == {"published": ["c1", "c2"], "already": [], "total": 2}
    assert publish(ds, src, verified, {}, tmp_max_age_s=AGE) == {"published": [], "already": ["c1", "c2"], "total": 2}
    assert not list(ds.glob(".tmp-*"))


def test_conflicting_crop_raises(tmp_path):
    src, verified = _src(tmp_path)
    ds = tmp_path / "ds"
    publish(ds, src, verified, {}, tmp_max_age_s=AGE)
    (ds / "c1" / "raw.png").write_bytes(b"different")
    with pytest.raises(Conflict):
        publish(ds, src, verified, {}, tmp_max_age_s=AGE)


def test_bad_crop_id_or_path_rejected(tmp_path):
    src, verified = _src(tmp_path)
    for bad in ({"crop_id": "../x", "files": {}}, {"crop_id": "c1", "files": {"../raw.png": "0"}}):
        with pytest.raises(ValueError):
            publish(tmp_path / "ds", src, [bad], {}, tmp_max_age_s=AGE)


def test_leftover_tmp_young_kept_old_removed(tmp_path):
    src, verified = _src(tmp_path)
    ds = tmp_path / "ds"
    young, old = ds / ".tmp-c1-aaaa", ds / ".tmp-c1-bbbb"
    for t in (young, old):
        shutil.copytree(src / "c1", t)  # a crash between copy and rename
    os.utime(old, (time.time() - 2 * AGE,) * 2)
    r = publish(ds, src, verified, {}, tmp_max_age_s=AGE)
    assert r["published"] == ["c1", "c2"]
    assert young.exists() and not old.exists()
    assert json.loads((ds / "manifest.json").read_text())["count"] == 2  # dot-dirs are not crops


def test_concurrent_publishers(tmp_path, monkeypatch):
    ids = [f"c{i}" for i in range(5)]
    srcs = [_src(tmp_path, ids=ids, name=f"src{n}") for n in range(2)]
    ds, results, errors = tmp_path / "ds", [], []
    barrier, rename = threading.Barrier(2), os.rename

    def lockstep(a, b):  # both attempts have copied the crop; both now rename it
        barrier.wait(timeout=10)
        rename(a, b)

    monkeypatch.setattr("mia_data.publish.os.rename", lockstep)

    def run(src, verified):
        try:
            results.append(publish(ds, src, verified, {}, tmp_max_age_s=AGE))
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    ts = [threading.Thread(target=run, args=s) for s in srcs]
    [t.start() for t in ts], [t.join() for t in ts]
    assert errors == []
    assert sorted(results[0]["published"] + results[1]["published"]) == ids  # each rename race has one winner
    assert sorted(results[0]["already"] + results[1]["already"]) == ids
    assert sorted(p.name for p in ds.iterdir()) == ids + ["manifest.json"]


def test_manifest_marks_withdrawn(tmp_path):
    ds = tmp_path / "ds"
    a, va = _src(tmp_path, ids=["c1"], name="a")
    b, vb = _src(tmp_path, ids=["c2"], snippet="bad-snippet", name="b")
    publish(ds, a, va, {}, tmp_max_age_s=AGE)
    publish(ds, b, vb, {"bad-snippet": "transposes yx"}, tmp_max_age_s=AGE)
    m = json.loads((ds / "manifest.json").read_text())
    assert m["count"] == 2 and [c["crop_id"] for c in m["crops"]] == ["c1", "c2"]
    assert (m["crops"][0]["withdrawn"], m["crops"][0]["withdrawn_reason"]) == (False, None)
    assert (m["crops"][1]["withdrawn"], m["crops"][1]["withdrawn_reason"]) == (True, "transposes yx")
    assert m["crops"][1]["plane"] == {"z": 0, "c": 0}
    reindex(ds, {})
    assert not any(c["withdrawn"] for c in json.loads((ds / "manifest.json").read_text())["crops"])


def test_manifest_entries_carry_file_checksums(tmp_path):
    src, verified = _src(tmp_path)
    ds = tmp_path / "ds"
    publish(ds, src, verified, {}, tmp_max_age_s=AGE)
    m = json.loads((ds / "manifest.json").read_text())
    assert [c["files"] for c in m["crops"]] == [v["files"] for v in verified]  # harness verify --existing reads these


def test_conflict_still_reindexes(tmp_path):
    src, verified = _src(tmp_path)
    ds = tmp_path / "ds"
    publish(ds, src, verified[:1], {}, tmp_max_age_s=AGE)
    (src / "c2" / "raw.png").write_bytes(b"changed after verify")
    (ds / "manifest.json").unlink()
    with pytest.raises(Conflict):
        publish(ds, src, verified, {}, tmp_max_age_s=AGE)
    assert json.loads((ds / "manifest.json").read_text())["count"] == 1


def test_symlinked_source_file_is_refused(tmp_path):
    src, verified = _src(tmp_path)
    real = tmp_path / "real.png"
    real.write_bytes((src / "c1" / "raw.png").read_bytes())
    (src / "c1" / "raw.png").unlink()
    (src / "c1" / "raw.png").symlink_to(real)
    with pytest.raises(Conflict, match="c1/raw.png"):
        publish(tmp_path / "ds", src, verified, {}, tmp_max_age_s=AGE)
