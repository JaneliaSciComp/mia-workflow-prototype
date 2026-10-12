from mia_data.models import DatasetState
from mia_data.status import MARKER, render_crops_table, render_stage, render_status


def rows(n, cid="i1-e2-b1-{:03d}"):
    return [{"crop_id": cid.format(i), "raw_url": f"https://github.com/o/d/blob/g/p/{i}/raw.png?raw=true",
             "overlay_url": f"https://github.com/o/d/blob/g/p/{i}/overlay.png?raw=true",
             "plane": {"z": i, "c": 0}, "box": {"y": 10, "x": 20}} for i in range(n)]


def test_crops_table_small_plain_large_details():
    t4 = render_crops_table(rows(4))
    assert "<details>" not in t4 and t4.startswith("| raw | overlay | caption |")
    assert "| ![raw](https://github.com/o/d/blob/g/p/0/raw.png?raw=true) |" in t4
    assert "i1-e2-b1-000 · z=0 c=0 · (10, 20)" in t4
    t16 = render_crops_table(rows(16))
    assert t16.startswith("<details><summary>16 crops</summary>") and t16.rstrip().endswith("</details>")
    assert t16.count("![overlay]") == 16


def test_status_has_no_agent_markers(tmp_path):
    bad = "<!-- mia-data:status -->"
    p = tmp_path / "binding.yaml"
    p.write_text(f"reasons: ['{bad}', '```', 'ghp_{'a' * 36}']\n")
    st = DatasetState(issue=3, stage="review", binding=str(p), binding_hash="ab" * 32, pr=7)
    crops = render_crops_table(rows(2, cid=bad + "|{}"))
    out = render_status(st, {"crops": crops, "totals": f"2 published {bad}", "pr": "https://github.com/o/s/pull/7"})
    assert out.startswith(MARKER + "\n") and out.count("<!--") == 1
    assert "data:review" in out and "ab" * 32 in out and "[#7](https://github.com/o/s/pull/7)" in out
    assert "ghp_" not in out and "````yaml" in out  # longer fence than any backtick run in the binding
    assert "(none)" in render_status(DatasetState(issue=3), {})
    assert "<!--" not in render_stage("needs-human", bad)
