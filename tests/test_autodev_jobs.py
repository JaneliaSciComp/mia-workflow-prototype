from types import SimpleNamespace

import pytest
from test_autodev_agent import FakeCsub

from autodev.jobs import CsubJobs, JobSpec


def _jobs(tmp_path, fake):
    return CsubJobs("dep", tmp_path / "ids", client_factory=lambda: fake)


async def test_submit_once_per_key(tmp_path):
    fake = FakeCsub()
    spec = JobSpec("j", "true", str(tmp_path / "ws"), 10)
    assert await _jobs(tmp_path, fake).submit("k1", spec) == "777"
    assert (tmp_path / "ids" / "k1.id").read_text() == "777"
    fresh = CsubJobs("dep", tmp_path / "ids", client_factory=lambda: pytest.fail("client used"))
    assert await fresh.submit("k1", spec) == "777" and len(fake.submitted) == 1


async def test_reattach_by_cwd(tmp_path):
    ws = tmp_path / "ws"
    fake = FakeCsub(existing=[SimpleNamespace(job_id="555", cwd="/elsewhere"), SimpleNamespace(job_id="556", cwd=str(ws))])
    assert await _jobs(tmp_path, fake).submit("k1", JobSpec("j", "true", str(ws), 10)) == "556"
    assert fake.submitted == [] and (tmp_path / "ids" / "k1.id").read_text() == "556"


async def test_plain_job_spec(tmp_path):
    fake = FakeCsub()
    ws = str(tmp_path / "ws")
    await _jobs(tmp_path, fake).submit("k1", JobSpec("harness", "echo hi", ws, 30, cpus=2, mem_mb=8000,
                                                     allow_hosts=["pypi.org"], ro_mounts=["/data", "/tools"],
                                                     env={"A": "1"}))
    s = fake.submitted[0]
    assert s["agent"] is None and s["env"] == {"A": "1"} and s["session"] == "dep" and s["cwd"] == ws
    assert s["mounts"] == [{"path": ws, "mode": "rw"}, {"path": "/data", "mode": "ro"}, {"path": "/tools", "mode": "ro"}]
    assert (s["name"], s["command"], s["cpus"], s["mem_mb"], s["walltime_min"], s["allow_hosts"]) == \
        ("harness", "echo hi", 2, 8000, 30, ["pypi.org"])


async def test_rw_mounts_follow_the_workspace(tmp_path):
    fake = FakeCsub()
    ws = str(tmp_path / "ws")
    await _jobs(tmp_path, fake).submit("k1", JobSpec("j", "true", ws, 10, ro_mounts=["/ro"], rw_mounts=["/cache"]))
    assert fake.submitted[0]["mounts"] == [{"path": ws, "mode": "rw"}, {"path": "/cache", "mode": "rw"},
                                           {"path": "/ro", "mode": "ro"}]


@pytest.mark.parametrize("key", ["../x", "", "a/b", "x" * 121])
async def test_bad_key_rejected(tmp_path, key):
    with pytest.raises(ValueError):
        await _jobs(tmp_path, FakeCsub()).submit(key, JobSpec("j", "true", str(tmp_path), 10))
    assert not (tmp_path / "ids").exists()


async def test_wait_reads_exit_code_when_csub_forgot_the_job(tmp_path):
    (tmp_path / "exit_code").write_text("3\n")
    out = await _jobs(tmp_path, FakeCsub(states=["UNKNOWN"])).wait("777", str(tmp_path), lambda s: None)
    assert (out.state, out.exit_code) == ("EXIT", 3)


async def test_recorded_id_skips_building_the_spec(tmp_path, monkeypatch):
    from autodev_helpers import K, make_cfg

    from autodev.agent import AgentJobs
    from autodev.models import Workspace

    monkeypatch.delenv("UNSET_KEY", raising=False)
    j = AgentJobs(make_cfg(tmp_path, agent={"api_key_env": "UNSET_KEY"}), client_factory=lambda: pytest.fail("client used"))
    (j.cfg.paths.state_dir / "jobs").mkdir(parents=True)
    (j.cfg.paths.state_dir / "jobs" / f"issue-7-{K}-r1.id").write_text("444")
    assert (await j.submit(7, K, 1, Workspace(str(tmp_path), "a", "b"))).job_id == "444"
