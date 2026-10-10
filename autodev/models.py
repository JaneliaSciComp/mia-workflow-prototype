"""Dataclasses shared by workflows and activities (plain, so Temporal serializes them as JSON)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Feedback:
    id: str  # "review-<id>" | "comment-<id>"
    text: str
