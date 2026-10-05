"""Activities against a fake GitHub + real local cluster run dirs (no network, no Temporal server)."""


from zarr_doctor import activities as A
from zarr_doctor.config import Config
from zarr_doctor.jobs.histogram import run as run_histogram
from zarr_doctor.models import FailureInput, JobOutcome, PublishInput
from zarr_doctor.state import State


class FakeGH:
    def __init__(self):
        self.comments, self.files, self.labels, self.removed = [], {}, [], []
        self.run_issues = []

    async def comment_once(self, n, body, marker):
        if any(marker in b for _, b in self.comments):
            return False
        self.comments.append((n, body))
        return True

    async def ensure_branch(self, b):
        return False

    async def upsert_file(self, path, content, branch, msg):
        if path in self.files:
            return False
        self.files[path] = content
        return True

    async def add_labels(self, n, labels):
        self.labels += labels

    async def remove_label(self, n, label):
        self.removed.append(label)

    async def list_issues(self, **params):
        return self.run_issues

    async def get_issue(self, n):
        return {"body": f"x\n<!-- zarr-doctor: path=data/x/c{n}.zarr fingerprint=sha1:1 -->"}


def _cfg(tmp_path):
    return Config(lmd_root=tmp_path, results_dir=tmp_path / "results", state_dir=tmp_path / "state",
                  github={"repo": "o/r"}, trigger={"mode": "label"})


class FakeCluster:
    def __init__(self, root):
        self.root = root

    def run_dir(self, issue, run_id):
        return self.root / str(issue) / run_id

    def log_tail(self, issue, run_id):
        return "Traceback...\nMemoryError"


async def test_publish_idempotent(lmd_root, tmp_path):
    cfg, gh = _cfg(tmp_path), FakeGH()
    cl = FakeCluster(cfg.results_dir)
    run_histogram(lmd_root / "data/em-mouse-foo/crop-001_cortex.zarr", cl.run_dir(5, "r1-x"), max_chunks=4)
    acts = A.Activities(cfg, gh, State(cfg.state_dir), cl)
    inp = PublishInput(5, "r1-x", "data/em-mouse-foo/crop-001_cortex.zarr", JobOutcome("99", "done", 0, "2 GB", 252.0))
    await acts.publish_results(inp)
    await acts.publish_results(inp)  # retry: no duplicate files or comment
    assert sorted(gh.files) == [f"results/5/r1-x/{n}" for n in ("histogram.json", "histogram.png", "stats.json")]
    assert len(gh.comments) == 1
    body = gh.comments[0][1]
    assert "Sampled **4 of 27 chunks (14.8%)**" in body
    assert "blob/zarr-doctor-results/results/5/r1-x/histogram.png?raw=true" in body
    assert "LSF job `99` · 4m 12s · peak memory 2 GB · exit 0" in body
    assert body.rstrip().endswith("<!-- zarr-doctor: run=r1-x result -->")


async def test_report_failure_label_mode(tmp_path):
    cfg, gh = _cfg(tmp_path), FakeGH()
    acts = A.Activities(cfg, gh, State(cfg.state_dir), FakeCluster(tmp_path))
    await acts.report_failure(FailureInput(5, "r2-x", "await_job", "LSF job 1 failed (exit 137)",
                                           JobOutcome("1", "failed", 137, None, 3482.0)))
    body = gh.comments[0][1]
    assert "**Stage:** await_job · **LSF job:** `1` · **exit:** 137 · **runtime:** 58m 02s" in body
    assert "MemoryError" in body and "<!-- zarr-doctor: run=r2-x failed -->" in body
    assert gh.labels == ["zd:failed"] and set(gh.removed) == {"zd:run", "zd:running", "zd:done"}


async def test_poll_signals_label_hits(tmp_path):
    cfg, gh = _cfg(tmp_path), FakeGH()
    gh.run_issues = [{"number": 3}, {"number": 4}]
    state = State(cfg.state_dir)
    state.upsert_volume("data/a/known.zarr", 3, "sha1:x")
    signalled = []

    class Client:
        async def start_workflow(self, fn, inp, **kw):
            signalled.append((inp.issue_number, inp.path, kw["id"], kw["start_signal"]))

    acts = A.Activities(cfg, gh, state, None, Client())
    assert await acts.poll_github() == [3, 4]
    assert signalled == [(3, "data/a/known.zarr", "zarr-doctor/issue-3", "run"),
                         (4, "data/x/c4.zarr", "zarr-doctor/issue-4", "run")]
    assert state.get("last_poll_at")


def test_ascii_histogram():
    out = A.ascii_histogram(list(range(33)), [1] * 16 + [3] * 16)
    lines = out.splitlines()
    assert len(lines) == 16 and lines[-1].endswith("#" * 40) and lines[0].count("#") == round(40 * 2 / 6)
