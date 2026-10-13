import copy
from pathlib import Path

import pytest
import yaml

from mia_data.binding import check_binding, hosts, missing_hosts, record_urls
from mia_data.models import Pins
from tests.mia_helpers import make_cfg

REC = yaml.safe_load((Path(__file__).parent / "fixtures/mia/records/bbbc034-hipsc-nuclei-3d.yaml").read_text())
SHA = "37e18f6e" + "0" * 32
ZIP = "https://data.broadinstitute.org/bbbc/BBBC034/"
B = {
    "record": "bbbc034-hipsc-nuclei-3d",
    "record_commit": SHA,
    "snippets_commit": "4c1d",
    "raw": [
        {"snippet": "tiff-zip-member", "urls": [ZIP + "BBBC034_v1_dataset.zip::AICS_12_134_C=2.tif"], "params": {}}
    ],
    "labels": [
        {
            "snippet": "tiff-zip-member",
            "urls": [ZIP + "BBBC034_v1_DatasetGroundTruth.zip::ground_truth_segmented.tif"],
            "params": {},
            "pairs_with": 0,
            "label_key": "manual_gt-nucleus-instance",
            "segmentation_type": "instance",
        }
    ],
    "reasons": ["axes zyx"],
}
NAMES = {"tiff-zip-member"}


def check(b, rec=REC):
    return check_binding(b, rec, NAMES, pins=Pins("bbbc034-hipsc-nuclei-3d", SHA, "4c1d", "h", "e"))


def test_example_binding_passes():
    assert check(copy.deepcopy(B)) == []


@pytest.mark.parametrize(
    "url",
    [
        "https://evil.example.org/x.zip",
        "http://data.broadinstitute.org/x.zip",
        "ftp://data.broadinstitute.org/x.zip",
        "HTTPS://data.broadinstitute.org/x",
    ],
)
def test_bad_urls_rejected(url):
    b = copy.deepcopy(B)
    b["raw"][0]["urls"] = [url]
    assert len(check(b)) == 1


def test_host_compare_is_case_insensitive():
    b = copy.deepcopy(B)
    b["raw"][0]["urls"] = ["https://Data.BroadInstitute.org/bbbc/x.zip"]
    assert check(b) == []


def test_unknown_snippet_record_and_label_fields_rejected():
    b = copy.deepcopy(B)
    b["raw"][0]["snippet"] = "nope"
    b["record"], b["record_commit"] = "other", "f" * 40
    b["labels"][0].update(label_key="../raw", pairs_with=1)  # a path, not a file name
    errs = check(b)
    assert len(errs) == 5, errs
    assert check({"record": "x", "raw": []}) and check("not a mapping")


def test_catalog_label_names_accepted():
    for key in ("unknown-cell-labels", "manual_gt-nucleus-instance", "Manual-gt"):
        b = copy.deepcopy(B)
        b["labels"][0]["label_key"] = key
        assert check(b) == [], key


def test_s3_url_rewritten_and_accepted():
    rec = {"data": {"download_url": "s3://janelia-cosem/jrc_hela-2/"}}
    b = copy.deepcopy(B)
    b["raw"][0]["urls"] = ["s3://janelia-cosem/jrc_hela-2/jrc_hela-2.zarr/em/s0"]
    b["labels"] = []
    assert check(b, rec) == []
    assert b["raw"][0]["urls"] == ["https://janelia-cosem.s3.amazonaws.com/jrc_hela-2/jrc_hela-2.zarr/em/s0"]


def test_record_urls_include_sample_and_patterns():
    assert record_urls(REC)[0] == ZIP + "BBBC034_v1_dataset.zip"
    assert len(record_urls(REC)) == 1 + 2 + 2


def test_hosts_add_redirects_and_packages(tmp_path):
    cfg = make_cfg(
        tmp_path,
        network={"redirect_hosts": {"data.broadinstitute.org": ["cdn.broad.example"]}, "package_hosts": ["pypi.org"]},
    )
    urls = B["raw"][0]["urls"] + ["s3://bucket/key", "https://DATA.broadinstitute.org/y"]
    assert hosts(urls, cfg) == ["bucket.s3.amazonaws.com", "cdn.broad.example", "data.broadinstitute.org", "pypi.org"]
    with pytest.raises(ValueError):
        hosts(["https://raw.githubusercontent.com/o/r/x"], cfg)


def test_missing_hosts():
    pats = ["*.huggingface.co", "pypi.org"]
    assert missing_hosts(["cdn-lfs.huggingface.co", "pypi.org", "evil.org"], pats) == ["evil.org"]
