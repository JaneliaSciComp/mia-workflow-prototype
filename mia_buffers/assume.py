"""Baked-in guesses for two facts the catalog often lacks: voxel size, and how to make a 1- or 3-channel sample.

Spec v0.5 §2 and §9.8: a static stand-in for the ingestion agent the lead described (infer from context, then check
against the pixels). The numbers are medians over the records that document them at catalog ab6a013d; they only
need to put a crop's box at a plausible physical scale, not to be exact.
"""

from __future__ import annotations

from mia_buffers.specmap import Facts

# (family, modality) -> (y and x pixel size in nm, z step in nm). Catalog medians, rounded.
VOXEL_NM: dict[tuple[str, str], tuple[float, float]] = {
    ("em", "FIB-SEM"): (8.0, 8.0),
    ("em", "SBF-SEM"): (10.0, 50.0),
    ("em", "ssSEM"): (6.0, 30.0),
    ("em", "ssTEM"): (4.0, 50.0),
    ("em", "TEM"): (2.0, 2.0),
    ("em", "cryo-ET"): (1.0, 1.0),
    ("em", "cryo-EM"): (0.2, 0.2),
    ("lm", "confocal"): (200.0, 330.0),
    ("lm", "spinning-disk"): (110.0, 330.0),
    ("lm", "widefield"): (225.0, 500.0),
    ("lm", "light-sheet"): (400.0, 2000.0),
    ("lm", "two-photon"): (300.0, 1000.0),
    ("lm", "fluorescence-other"): (250.0, 300.0),
    ("lm", "super-resolution"): (30.0, 125.0),
    ("lm", "brightfield"): (325.0, 1000.0),
    ("lm", "histology-WSI"): (250.0, 1000.0),
    ("lm", "phase-contrast"): (300.0, 500.0),
    ("lm", "DIC"): (420.0, 500.0),
    ("xray", "X-ray"): (2000.0, 2000.0),
    ("exm", "expansion"): (100.0, 350.0),
}
# Fallback when the modality is not in the table; "other" is the catalog's bucket for everything unclassified.
FAMILY_NM: dict[str, tuple[float, float]] = {
    "em": (5.0, 30.0),
    "lm": (300.0, 500.0),
    "xray": (2000.0, 2000.0),
    "exm": (100.0, 350.0),
    "other": (300.0, 900.0),
}
# Modalities whose 3- or 4-channel arrays are colour images (RGB or RGBA), not separate fluorescence channels.
RGB_MODALITIES = {"brightfield", "histology-WSI", "phase-contrast", "DIC"}


def voxel_size(f: Facts) -> tuple[dict[str, float | None], list[str]]:
    """The record's voxel size with every missing axis the spec needs filled from the table; names the assumed axes."""
    axes = ("x", "y", "z") if (f.dims or "").startswith("3D") else ("x", "y")
    yx, z = VOXEL_NM.get((f.family or "other", f.modality_group or ""), FAMILY_NM[f.family or "other"])
    merged: dict[str, float | None] = {axis: v for axis, v in f.voxel_size_nm.items() if v is not None}
    assumed: list[str] = []
    for axis in axes:
        if not isinstance(merged.get(axis), int | float):
            merged[axis] = z if axis == "z" else yx
            assumed.append(axis)
    return merged, assumed


def channel_recipes(channels: int, family: str | None, modality: str | None) -> dict[str, dict]:
    """How to derive a one-channel ("gray") and a three-channel ("rgb") sample from an array's stored channels."""
    if channels == 1:
        return {"gray": {"op": "channel", "index": 0}, "rgb": {"op": "replicate"}}
    if channels in (3, 4) and (modality in RGB_MODALITIES or family in (None, "other")):
        return {"gray": {"op": "luminance"}, "rgb": {"op": "rgb"}}
    return {"gray": {"op": "mean"}, "rgb": {"op": "additive"}}
