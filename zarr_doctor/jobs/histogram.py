"""Value histogram of one zarr volume's raw/<level>, over a deterministic chunk sample.

Runs on a compute node. No Temporal/GitHub dependencies; only numpy, zarr, matplotlib
(tensorstore if installed, as a fallback reader).

    python -m zarr_doctor.jobs.histogram --zarr /abs/crop.zarr --out DIR [--level s0] [--max-chunks 256] [--seed 0]
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
import warnings
from pathlib import Path

import numpy as np


def open_level(zarr_path: Path, level: str):
    """Return (array, reader, zarr_format). Array must support .shape/.dtype/.chunks and slicing."""
    arr_path = zarr_path / "raw" / level
    try:
        import zarr

        arr = zarr.open_array(store=str(arr_path), mode="r")
        return arr, "zarr-python", arr.metadata.zarr_format
    except Exception as zarr_err:
        try:
            import tensorstore as ts
        except ImportError:
            raise zarr_err from None
        driver = "zarr3" if (arr_path / "zarr.json").exists() else "zarr"
        arr = ts.open({"driver": driver, "kvstore": {"driver": "file", "path": str(arr_path)}}, read=True).result()
        chunks = tuple(arr.chunk_layout.read_chunk.shape)

        class _TS:  # tiny adapter so the rest of the code sees a zarr-like array
            shape, dtype = tuple(arr.shape), np.dtype(arr.dtype.numpy_dtype)

            def __getitem__(self, sel):
                return arr[sel].read().result()

        _TS.chunks = chunks
        return _TS(), "tensorstore", 3 if driver == "zarr3" else 2


def _dtype_name(dt) -> str:
    try:
        return np.dtype(dt).name  # ">u2" -> "uint16"
    except TypeError:
        return str(dt)  # e.g. bfloat16


def sample_chunks(grid: tuple[int, ...], max_chunks: int, seed: int) -> list[int]:
    """Flat chunk indices: all if few enough, else max_chunks evenly spaced with a seeded offset."""
    total = math.prod(grid)
    if total <= max_chunks:
        return list(range(total))
    stride = total / max_chunks
    offset = np.random.default_rng(seed).uniform(0, stride)
    return [int(offset + i * stride) for i in range(max_chunks)]


def iter_chunks(arr, indices: list[int]):
    grid = tuple(math.ceil(s / c) for s, c in zip(arr.shape, arr.chunks))
    for flat in indices:
        pos = np.unravel_index(flat, grid)
        sel = tuple(slice(p * c, min((p + 1) * c, s)) for p, c, s in zip(pos, arr.chunks, arr.shape))
        data = np.asarray(arr[sel])
        if data.dtype.kind == "V" or str(data.dtype) == "bfloat16":
            data = data.astype(np.float32)
        yield data.ravel()


def compute(arr, indices: list[int]) -> dict:
    """Two-pass histogram (one pass for uint8). Returns stats + edges/counts."""
    kind = np.dtype(arr.dtype).kind if str(arr.dtype) != "bfloat16" else "f"
    n = zeros = nonfinite = 0
    s1 = s2 = 0.0
    lo, hi = math.inf, -math.inf
    is_u8 = np.dtype(arr.dtype) == np.uint8 if kind != "f" else False
    u8_counts = np.zeros(256, np.int64)

    # Pass 1: moments, min/max, zeros (and exact counts for uint8).
    for x in iter_chunks(arr, indices):
        if kind == "f":
            finite = np.isfinite(x)
            nonfinite += int(x.size - finite.sum())
            x = x[finite]
        if x.size == 0:
            continue
        n += x.size
        zeros += int(np.count_nonzero(x == 0))
        xf = x.astype(np.float64)
        s1 += float(xf.sum())
        s2 += float((xf * xf).sum())
        lo, hi = min(lo, float(x.min())), max(hi, float(x.max()))
        if is_u8:
            u8_counts += np.bincount(x, minlength=256)
    if n == 0:
        raise ValueError("no finite voxels in sample")

    outliers_low = outliers_high = 0
    if is_u8:
        edges, counts = np.arange(257, dtype=np.float64), u8_counts
    elif kind in "iub":
        width = max(1, math.ceil((hi - lo + 1) / 1024))
        nbins = int((hi - lo) // width) + 1
        edges = lo + width * np.arange(nbins + 1, dtype=np.float64)
        counts = np.zeros(nbins, np.int64)
        for x in iter_chunks(arr, indices):
            counts += np.bincount(((x.astype(np.int64) - int(lo)) // width), minlength=nbins)[:nbins]
    else:
        # Floats: estimate p0.1/p99.9 from a fine pass-1.5 histogram over [min, max], then bin 512 inside.
        fine_edges = np.linspace(lo, hi if hi > lo else lo + 1, 4097)
        coarse = np.zeros(4096, np.int64)
        for x in iter_chunks(arr, indices):
            coarse += np.histogram(x[np.isfinite(x)], bins=fine_edges)[0]
        a, b = _percentile_from_hist(fine_edges, coarse, [0.1, 99.9])
        if b <= a:
            b = a + 1
        edges = np.linspace(a, b, 513)
        counts = np.zeros(512, np.int64)
        for x in iter_chunks(arr, indices):
            x = x[np.isfinite(x)]
            outliers_low += int(np.count_nonzero(x < a))
            outliers_high += int(np.count_nonzero(x > b))
            counts += np.histogram(x[(x >= a) & (x <= b)], bins=edges)[0]

    mean = s1 / n
    p01, p50, p99 = _percentile_from_hist(edges, counts, [1, 50, 99], below=outliers_low)
    if is_u8:  # exact integer percentiles for uint8
        p01, p50, p99 = (math.floor(v) for v in (p01, p50, p99))
    return {
        "voxels_counted": n,
        "min": lo, "max": hi, "mean": mean, "std": math.sqrt(max(0.0, s2 / n - mean * mean)),
        "p01": p01, "p50": p50, "p99": p99,
        "zero_fraction": zeros / n,
        "nonfinite": nonfinite, "outliers_low": outliers_low, "outliers_high": outliers_high,
        "edges": edges.tolist(), "counts": counts.tolist(),
    }


def _percentile_from_hist(edges, counts, qs, below: int = 0) -> list[float]:
    """Percentiles (0-100) from a histogram, interpolating within bins; `below` = mass left of edges[0]."""
    cum = np.concatenate([[below], below + np.cumsum(counts)]).astype(np.float64)
    total = cum[-1] if cum[-1] > 0 else 1
    out = []
    for q in qs:
        target = q / 100 * total
        i = int(np.clip(np.searchsorted(cum, target, side="left") - 1, 0, len(counts) - 1))
        span = cum[i + 1] - cum[i]
        frac = (target - cum[i]) / span if span > 0 else 0.0
        out.append(float(edges[i] + np.clip(frac, 0, 1) * (edges[i + 1] - edges[i])))
    return out


def render_png(path: Path, edges, counts, title: str, subtitle: str) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    edges, counts = np.asarray(edges), np.asarray(counts)
    fig, axes = plt.subplots(1, 2, figsize=(14, 7), dpi=100)
    for ax, log in zip(axes, (False, True)):
        ax.stairs(counts, edges, fill=True, color="#3b6ea5")
        ax.set_yscale("log" if log else "linear")
        ax.set_title("log count" if log else "count")
        ax.set_xlabel("value")
    fig.suptitle(f"{title}\n{subtitle}", fontsize=11)
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def rel_title(zarr_path: Path, level: str) -> str:
    parts = zarr_path.parts
    rel = "/".join(parts[parts.index("data"):]) if "data" in parts else "/".join(parts[-2:])
    return f"{rel} · raw/{level}"


def run(zarr_path: Path, out: Path, level: str = "s0", max_chunks: int = 256, seed: int = 0) -> dict:
    t0 = time.monotonic()
    arr, reader, zarr_format = open_level(zarr_path, level)
    grid = tuple(math.ceil(s / c) for s, c in zip(arr.shape, arr.chunks))
    indices = sample_chunks(grid, max_chunks, seed)
    h = compute(arr, indices)
    total = math.prod(grid)
    stats = {
        "zarr": str(zarr_path), "level": level, "zarr_format": zarr_format, "reader": reader,
        "shape": list(arr.shape), "dtype": _dtype_name(arr.dtype), "chunks": list(arr.chunks),
        "total_chunks": total, "chunks_read": len(indices), "sampled_fraction": len(indices) / total,
        **{k: v for k, v in h.items() if k not in ("edges", "counts")},
        "elapsed_s": round(time.monotonic() - t0, 3),
    }
    out.mkdir(parents=True, exist_ok=True)
    (out / "stats.json").write_text(json.dumps(stats, indent=2))
    (out / "histogram.json").write_text(json.dumps({"edges": h["edges"], "counts": h["counts"]}))
    subtitle = (f"{stats['dtype']} · sampled {stats['chunks_read']}/{total} chunks "
                f"({stats['sampled_fraction']:.1%}) · zeros {stats['zero_fraction']:.1%}")
    render_png(out / "histogram.png", h["edges"], h["counts"], rel_title(zarr_path, level), subtitle)
    return stats


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="zarr_doctor.jobs.histogram")
    p.add_argument("--zarr", required=True, type=Path)
    p.add_argument("--out", required=True, type=Path)
    p.add_argument("--level", default="s0")
    p.add_argument("--max-chunks", type=int, default=256)
    p.add_argument("--seed", type=int, default=0)
    a = p.parse_args(argv)
    # zarr's spec-compliance warnings about legacy metadata would bury real errors in lsf.err
    warnings.filterwarnings("ignore", module=r"zarr\..*")
    try:
        stats = run(a.zarr, a.out, a.level, a.max_chunks, a.seed)
    except Exception as e:  # one-line error for the failure comment's log tail
        print(f"histogram failed: {type(e).__name__}: {e}", file=sys.stderr)
        return 1
    print(json.dumps({k: stats[k] for k in ("dtype", "chunks_read", "total_chunks", "elapsed_s")}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
