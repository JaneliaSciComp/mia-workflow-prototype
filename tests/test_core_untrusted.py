from mia_core.untrusted import loads, neutralize, read_untrusted, text, texts, write_atomic


def test_moved_helpers_behave_as_before(tmp_path):
    assert neutralize("a <!-- x --> ghp_abcdef", token=None) == "a &lt;!-- x --> [redacted]"
    assert loads("[1]") == [1] and loads("nope") is None and loads(None) is None
    assert text(None) == "" and text("x" * 10, 3) == "xxx" and texts("a") == ["a"] and texts(None) == []
    write_atomic(tmp_path / "d" / "f", "hi")
    assert (tmp_path / "d" / "f").read_text() == "hi" and not list((tmp_path / "d").glob(".*tmp"))
    (tmp_path / "job").mkdir()
    (tmp_path / "job" / "out.json").write_text("{}")
    assert read_untrusted(tmp_path / "job", "out.json", 100) == "{}"
    (tmp_path / "job" / "link").symlink_to(tmp_path / "job" / "out.json")
    assert read_untrusted(tmp_path / "job", "link", 100) is None


def test_old_import_sites_still_work():
    import mia_core.untrusted as core
    from autodev.activities import neutralize as a
    from autodev.jobs import read_untrusted as b
    from mia_data.activities import _loads
    from mia_data.activities import write_atomic as c

    assert a is core.neutralize and b is core.read_untrusted and c is core.write_atomic and _loads is core.loads
