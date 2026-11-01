from pathlib import Path

import yaml

from mia_buffers.specmap import SPEC_SOURCES, SPEC_TYPES, facts, family_of

CATALOG = Path(__file__).parent / "fixtures" / "buffers" / "catalog"


def load(rel: str) -> tuple[str, dict]:
    path = f"datasets/{rel}"
    return path, yaml.safe_load((CATALOG / path).read_text())


def test_bcss_samples_axis_is_channels():
    f = facts(*load("2D/histology-WSI/github-bcss-breast-cancer-semantic-segmentation.yaml"))
    raw = f.largest_raw
    assert raw is not None
    assert (raw.extent_y, raw.extent_x, raw.channels, raw.planes) == (2467, 3394, 3, 1) and raw.shape_varies
    assert f.family == "lm" and f.modality_group == "histology-WSI" and f.download_method == "other"
    assert f.download_host == "drive.google.com" and f.n_items == 151 and f.license_spdx == "CC0-1.0"


def test_u937_t_axis_is_planes():
    f = facts(*load("2D/confocal/zenodo-20797339-u937-edu-h2ax-dapi-confocal.yaml"))
    raw = f.largest_raw
    assert raw is not None
    assert (raw.channels, raw.planes, raw.extent_y, raw.extent_x) == (3, 25, 2048, 2048)
    assert f.annotation_source == "unknown" and sorted(f.label_encodings) == ["binary", "instance-ids"]


def test_hemibrain_is_3d_with_z_extent():
    f = facts(*load("3D/FIB-SEM/janelia-hemibrain-v1-2-fib-sem-connectome.yaml"))
    assert f.dims == "3D" and f.largest_raw is not None and f.largest_raw.extent_z == 41394
    assert f.voxel_size_known("3D") and f.family == "em" and f.modality_group == "FIB-SEM"


def test_mitolab_voxel_size_unknown():
    f = facts(*load("2D/FIB-SEM/empiar-11037-cem-mitolab-mitochondria.yaml"))
    assert not f.voxel_size_known("2D") and f.largest_raw is not None
    assert f.largest_raw.voxels == 224 * 224 and f.label_encodings == ["instance-ids"]
    assert f.modalities == ["FIB-SEM", "TEM"] and f.modality_group == "FIB-SEM"


def test_record_without_technical():
    f = facts(*load("2D/TEM/synthetic-em-2d-dup.yaml"))
    assert f.raw_arrays == [] and f.largest_raw is None and f.license_spdx == "unknown"
    assert not f.annotations_present and f.doi == "10.1/dup"


def test_vocabulary_maps():
    assert family_of("confocal") == "lm" and family_of("made-up") == "other"
    assert SPEC_TYPES["point"] == "points" and SPEC_SOURCES["auto-predict"] == "automatic"
