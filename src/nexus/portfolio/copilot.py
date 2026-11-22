"""Bounded, read-only evidence retrieval without model calls or provider billing."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

SOURCES = (
    "docs/portfolio/CASE_STUDY.md",
    "docs/portfolio/SECURITY.md",
    "docs/portfolio/DEMO.md",
    "docs/portfolio/RELEASE.md",
)
STOP_WORDS = {"what", "when", "where", "does", "with", "this", "that", "have", "from", "which"}
MUTATION = re.compile(
    r"\b(approve|execute|delete|ignore|bypass|reveal|password|secret|token|override)\b", re.I
)


def answer_question(question: str, root: Path) -> dict[str, Any]:
    """Quote reviewed source paragraphs; refuse actions and abstain on missing evidence."""
    response: dict[str, Any] = {
        "mode": "deterministic-evidence",
        "read_only": True,
        "citations": [],
    }
    if MUTATION.search(question) and not re.search(
        r"\b(why|how|policy|policies|security)\b", question, re.I
    ):
        return {
            **response,
            "answer": "I cannot perform actions or disclose secrets. "
            "Use the role-controlled workflow. "
            "I can explain its documented policies.",
        }
    terms = set(re.findall(r"[a-z]{4,}", question.lower())) - STOP_WORDS
    ranked: list[tuple[int, str, str, str]] = []
    for relative in SOURCES:
        path = root / relative
        if not path.is_file():
            continue
        content = path.read_text(encoding="utf-8")
        title = content.splitlines()[0].lstrip("# ")
        for paragraph in re.split(r"\n\s*\n", content):
            if paragraph.startswith(("#", "```", "|", "![")):
                continue
            paragraph = " ".join(paragraph.split())
            words = set(re.findall(r"[a-z]{4,}", paragraph.lower()))
            score = len(terms & words)
            if score:
                ranked.append((score, relative, title, paragraph))
    ranked.sort(key=lambda item: (-item[0], item[1], item[3]))
    citations = [
        {"title": title, "path": path, "excerpt": paragraph[:900]}
        for _, path, title, paragraph in ranked[:3]
    ]
    if not citations:
        return {
            **response,
            "answer": "No supporting evidence found in the portfolio documentation. "
            "Try asking about forecasts, approval security, scenarios, or synthetic data.",
        }
    return {
        **response,
        "answer": "Retrieved project evidence (reference mode, not an LLM-generated answer):\n\n"
        + "\n\n".join(f"[{i}] {item['excerpt']}" for i, item in enumerate(citations, 1)),
        "citations": citations,
    }
