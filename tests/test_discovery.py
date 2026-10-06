from types import SimpleNamespace

from zarr_doctor import discovery
from zarr_doctor.state import State

V3 = "data/em-mouse-foo/crop-001_cortex.zarr"
V2 = "data/xray-fly-bar/crop-002_vnc.zarr"


def test_extract_v3(lmd_root):
    m = discovery.extract(lmd_root / V3)
    assert m["zarr_format"] == 3 and m["dtype"] == "uint8" and m["shape"] == [40, 40, 40]
    assert m["voxel_size"] == [8.0, 8.0, 8.0] and m["units"][0] == "nanometer" and m["scale_levels"] == 2
    assert {lb["name"]: lb["dtype"] for lb in m["labels"]} == {"auto_pred-mito-v2": "int32", "manual_gt-cell-dense": "uint64"}
    assert discovery.traits(m) == ["labels-uint64", "chunks-not-128"]  # zarr-python default codecs include zstd


def test_extract_v2(lmd_root):
    m = discovery.extract(lmd_root / V2)
    assert m["zarr_format"] == 2 and m["dtype"] == "uint16" and m["chunks"] == [20, 20, 20]
    assert set(discovery.traits(m)) >= {"zarr2", "no-voxel-size", "no-labels", "chunks-not-128"}


def test_non_volumes_skipped(lmd_root):
    assert discovery.extract(lmd_root / "data/em-mouse-foo/crop-004_empty.zarr") is None


def test_fingerprint_stable_and_sensitive(lmd_root):
    m = discovery.extract(lmd_root / V3)
    assert discovery.fingerprint(m) == discovery.fingerprint(discovery.extract(lmd_root / V3))
    assert discovery.fingerprint(m) != discovery.fingerprint({**m, "dtype": "uint16"})


def test_body_and_title(lmd_root):
    m = discovery.extract(lmd_root / V3)
    body = discovery.render_body(V3, m, "sha1:abc")
    assert discovery.MARKER_RE.search(body).groups() == (V3, "sha1:abc")
    assert "| raw dtype | uint8 |" in body and "40 × 40 × 40 (z, y, x)" in body
    assert "| label vocab complete | 1 of 2 |" in body
    assert discovery.title_for(V3) == "[em-mouse-foo] crop-001_cortex"
    assert discovery.title_for("data/weird/crop.zarr") == "weird/crop.zarr"
    assert discovery.labels_for(V3, m)[-2:] == ["modality:em", "organism:mouse"]


class FakeGH:
    def __init__(self, existing=None):
        self.issues, self.comments, self.existing = {}, [], existing or {}

    async def issue_markers(self):
        return self.existing

    async def create_issue(self, title, body, labels):
        n = 100 + len(self.issues)
        self.issues[n] = (title, body, labels)
        return {"number": n}

    async def update_issue(self, n, body):
        self.issues[n] = ("", body, [])

    async def comment(self, n, body):
        self.comments.append((n, body))


async def test_discover_idempotent(lmd_root, tmp_path):
    cfg = SimpleNamespace(lmd_root=lmd_root)
    state, gh, stages = State(tmp_path), FakeGH(), []

    async def set_stage(n, stage):
        stages.append((n, stage))

    s1 = await discovery.discover(cfg, gh=gh, state=state, set_stage=set_stage, max_new=2, out=lambda _: None)
    assert (s1.found, s1.created, s1.deferred, s1.skipped) == (3, 2, 1, 1)
    s2 = await discovery.discover(cfg, gh=gh, state=state, set_stage=set_stage, max_new=10, out=lambda _: None)
    assert (s2.existing, s2.created) == (2, 1)
    s3 = await discovery.discover(cfg, gh=gh, state=state, max_new=10, out=lambda _: None)
    assert (s3.existing, s3.created) == (3, 0)
    assert len(gh.issues) == 3 and [n for n, _ in stages] == [100, 101, 102] and stages[0] == (100, "discovered")


async def test_discover_dedupes_against_github_and_detects_change(lmd_root, tmp_path):
    cfg = SimpleNamespace(lmd_root=lmd_root)
    gh = FakeGH(existing={V3: (7, "sha1:old")})
    s = await discovery.discover(cfg, gh=gh, state=State(tmp_path), max_new=0, out=lambda _: None)
    assert s.created == 0 and s.existing == 1 and s.updated == 1
    assert gh.comments[0][0] == 7 and "Metadata changed" in gh.comments[0][1]


async def test_dry_run_writes_nothing(lmd_root, tmp_path):
    lines = []
    s = await discovery.discover(SimpleNamespace(lmd_root=lmd_root), state=State(tmp_path / "x", readonly=True),
                                 dry_run=True, out=lines.append)
    assert s.found == 3 and len(lines) == 3 and not (tmp_path / "x").exists()
