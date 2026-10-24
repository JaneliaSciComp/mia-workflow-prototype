"""The consumer side: `BufferClient` speaks to buffer-api, `BufferDataset` is a MuViT dataset over a buffer.

Local mode (spec v0.5 §5): the dataset pulls crop descriptors from the service and tensorises them itself with the
mia-snippets harness (`mia_harness.tensorize`), so no pixel crosses the service. torch and muvit are optional: with
them the dataset subclasses `muvit.data.MuViTDataset` and returns tensors, without them it returns numpy arrays.
"""

from __future__ import annotations

import os
import socket
from collections import deque
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal

import httpx

try:
    import torch
except ImportError:  # the planner environment has no torch; samples stay numpy
    torch = None

try:
    from mia_harness.tensorize import tensorize
except ImportError:  # the harness is in the mia-snippets checkout's environment; local mode needs it
    tensorize = None

TENSORIZERS_ENV = "MIA_BUFFERS_TENSORIZERS"


class BufferEmpty(RuntimeError):
    """The service has nothing servable for this split yet (HTTP 204)."""


class BufferClient:
    """The §5 calls a consumer may make."""

    def __init__(self, url: str, buffer: str, *, timeout: float = 30.0):
        self.url, self.buffer = url.rstrip("/"), buffer
        self._http = httpx.Client(base_url=self.url, timeout=timeout)

    def _get(self, path: str, split: str, **params: Any) -> httpx.Response:
        try:
            response = self._http.get(f"/buffers/{self.buffer}/{path}", params=params)
        except httpx.TransportError as error:
            raise type(error)(f"{error} (buffer-api at {self.url})", request=error.request) from None
        if response.status_code == 204:
            raise BufferEmpty(f"buffer {self.buffer} has nothing servable for split {split}")
        response.raise_for_status()
        return response

    def status(self) -> dict:
        return self._get("status", "").json()

    def register(self, split: str, mode: str = "local", name: str | None = None) -> str:
        info = {
            "name": name or f"{socket.gethostname()}:{os.getpid()}",
            "host": socket.gethostname(),
            "pid": os.getpid(),
            "split": split,
            "mode": mode,
        }
        response = self._http.post(f"/buffers/{self.buffer}/consumers", json=info)
        response.raise_for_status()
        return str(response.json()["consumer_id"])

    def next(self, consumer: str, n: int) -> list[dict]:
        return list(self._get("next", "train", consumer=consumer, n=n).json()["descriptors"])

    def split(self, consumer: str, name: str, start: int, n: int) -> tuple[list[dict], int]:
        out = self._get(f"split/{name}", name, consumer=consumer, **{"from": start, "n": n}).json()
        return list(out["descriptors"]), int(out["size"])


class _Base:
    """Stand-in for muvit.data.MuViTDataset: same properties, same sanity check, no torch."""

    ndim: int
    levels: tuple[int, ...]
    n_channels: int

    def __len__(self) -> int:
        raise NotImplementedError

    def __getitem__(self, idx: int) -> dict:
        raise NotImplementedError

    @property
    def n_levels(self) -> int:
        return len(self.levels)

    def _sanity_check(self) -> None:
        if len(self) == 0:
            raise ValueError("Dataset is empty.")
        sample = self[0]
        img, bbox = sample["img"], sample["bbox"]
        if tuple(bbox.shape) != (self.n_levels, 2, self.ndim):
            raise ValueError(f"Expected bbox shape {(self.n_levels, 2, self.ndim)}, got {tuple(bbox.shape)}")
        if img.ndim != 4 or img.shape[0] != self.n_levels or img.shape[1] != self.n_channels:
            raise ValueError(f"Expected img (L={self.n_levels}, C={self.n_channels}, P, P), got {tuple(img.shape)}")


try:
    from muvit.data import MuViTDataset as Base
except ImportError:
    Base = _Base


class BufferDataset(Base):
    """Map-style dataset over one buffer and split; train streams fresh crops, val and test replay a frozen list."""

    ndim = 2

    def __init__(
        self,
        url: str,
        buffer: str,
        split: str = "train",
        n_samples: int | None = None,
        mode: Literal["local"] = "local",
        tensorizers: Path | str | None = None,
        prefetch: int = 64,
        timeout: float = 30.0,
    ):
        tensorizers = tensorizers or os.environ.get(TENSORIZERS_ENV)
        if tensorizers is None:
            raise ValueError(f"local mode needs `tensorizers` (the mia-snippets checkout) or ${TENSORIZERS_ENV}")
        if tensorize is None:
            raise ImportError("mia_harness is not importable; run under the mia-snippets harness environment")
        self.client = BufferClient(url, buffer, timeout=timeout)
        self.split, self.mode, self.prefetch = split, mode, max(1, prefetch)
        self.tensorizers = Path(tensorizers)
        self.config = self.client.status()["config"]
        self._levels = tuple(int(level) for level in self.config["levels"])
        self._consumer: tuple[int, str] | None = None  # (pid, consumer_id): DataLoader workers register anew
        self._queue: deque[dict] = deque()
        self._page: dict[int, dict] = {}
        self._stored_channels: int | None = None
        self._size = n_samples if split == "train" else self.client.split(self._consumer_id(), split, 0, 1)[1]
        if self._size is None:
            raise ValueError("n_samples is required for the train split (a virtual epoch)")
        super().__init__()
        if not hasattr(Base, "_sanity_check") or Base is _Base:
            self._sanity_check()  # muvit's metaclass runs it on construction; mirror that without muvit

    @property
    def levels(self) -> tuple[int, ...]:
        return self._levels

    @property
    def n_channels(self) -> int:
        policy = self.config["channels"]
        if policy in (1, 3):
            return int(policy)
        if self._stored_channels is None:  # policy "all": the plan made every volume share one count
            self._stored_channels = int(self._tensorize(self._descriptor(0))["img"].shape[1])
        return self._stored_channels

    def _consumer_id(self) -> str:
        pid = os.getpid()
        if self._consumer is None or self._consumer[0] != pid:
            self._consumer = (pid, self.client.register(self.split, self.mode))
            self._queue.clear()
            self._page.clear()
        return self._consumer[1]

    def __len__(self) -> int:
        return int(self._size or 0)

    def _descriptor(self, idx: int) -> dict:
        consumer = self._consumer_id()
        try:
            if self.split == "train":
                if not self._queue:
                    self._queue.extend(self.client.next(consumer, self.prefetch))
                return self._queue.popleft()
            if idx < 0 or idx >= len(self):
                raise IndexError(f"{idx} is outside the {self.split} list of {len(self)}")
            if idx not in self._page:
                start = idx - idx % self.prefetch
                descriptors, _ = self.client.split(consumer, self.split, start, self.prefetch)
                self._page = {start + i: d for i, d in enumerate(descriptors)}
            return self._page[idx]
        except BufferEmpty as error:
            raise RuntimeError(str(error)) from None

    def _tensorize(self, descriptor: dict) -> dict:
        params = {**self.config.get("tensorizer_params", {}), "channels": descriptor.get("channels")}
        assert tensorize is not None
        return dict(
            tensorize(Path(descriptor["path"]), descriptor, self.tensorizers, self.config["tensorizer"], params)
        )

    def __getitem__(self, idx: int) -> dict:
        sample = self._tensorize(self._descriptor(idx))
        meta = sample.pop("meta", {})
        convert: Callable[[Any], Any] = torch.from_numpy if torch is not None else (lambda array: array)
        out: dict = {key: convert(value) for key, value in sample.items()}
        out["meta"] = {
            k: v for k, v in meta.items() if isinstance(v, str | int | float | bool | list | dict | type(None))
        }
        return out
