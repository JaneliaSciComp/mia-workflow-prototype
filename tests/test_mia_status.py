from mia_data.status import render_binding, render_crops_table, render_links, render_stage


def rows(n, cid="i1-e2-b1-{:03d}"):
    return [
        {
            "crop_id": cid.format(i),
            "raw_url": f"https://github.com/o/d/blob/g/p/{i}/raw.png?raw=true",
            "overlay_url": f"https://github.com/o/d/blob/g/p/{i}/overlay.png?raw=true",
            "plane": {"z": i, "c": 0},
            "box": {"y": 10, "x": 20},
        }
        for i in range(n)
    ]


def test_crops_table_small_plain_large_details():
    t4 = render_crops_table(rows(4))
    assert "<details>" not in t4 and t4.startswith("| raw | overlay | caption |")
    assert "| ![raw](https://github.com/o/d/blob/g/p/0/raw.png?raw=true) |" in t4
    assert "i1-e2-b1-000 · z=0 c=0 · (10, 20)" in t4
    t16 = render_crops_table(rows(16))
    assert t16.startswith("<details><summary>16 crops</summary>") and t16.rstrip().endswith("</details>")
    assert t16.count("![overlay]") == 16


def test_binding_block_fences_and_neutralizes(tmp_path):
    bad = "<!-- mia-data: stage x -->"
    p = tmp_path / "binding.yaml"
    p.write_text(f"reasons: ['{bad}', '```', 'ghp_{'a' * 36}']\n")
    out = render_binding(str(p), "ab" * 32)
    assert out.startswith("**Binding** (sha256 `" + "ab" * 32 + "`):\n````yaml\n") and out.rstrip().endswith("````")
    assert "<!--" not in out and "ghp_" not in out
    assert render_binding(str(tmp_path / "missing.yaml"), None) == "**Binding:** (none)\n"


def test_links_line_lists_only_given_urls():
    assert (
        render_links({"Workspace": None, "Crops": "https://fg.example/b/c d", "Gallery": "https://g/x"})
        == "**Folders:** [Crops](https://fg.example/b/c%20d) · [Gallery](https://g/x)\n"
    )
    assert render_links({"Workspace": None}) == ""
    assert "<!--" not in render_stage("needs-human", "<!-- mia-data:status -->")
