"""Versioned, permission-checked fictional evidence for a future local copilot.

This module retrieves typed claim references. Document text is always data: it is
never executed, treated as a policy instruction, or sent to a model here.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from nexus.contracts import ActorRole
from nexus.portfolio.auth import Actor

MAX_CORPUS_BYTES = 131_072
MAX_MATCHES = 8
IDENTIFIER = re.compile(r"[a-z][a-z0-9_-]{0,63}\Z")
CLAIM_KEY = re.compile(r"[a-z][a-z0-9_.-]{0,95}\Z")
CLAIM_VALUE = re.compile(r"[a-z0-9][a-z0-9_.-]{0,159}\Z")
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
HUMAN_ROLES = {ActorRole.PLANNER, ActorRole.APPROVER, ActorRole.AUDITOR}


class DocumentError(ValueError):
    """Invalid or ambiguous corpus; callers must fail closed."""


class DocumentAccessError(PermissionError):
    """The principal cannot retrieve evidence for this location."""


def passage_digest(claim_key: str, claim_value: str, text: str) -> str:
    """Hash the complete immutable claim payload, not only its prose."""
    content = json.dumps(
        {"claim_key": claim_key, "claim_value": claim_value, "text": text},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(content).hexdigest()


class Passage(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    passage_id: str = Field(min_length=1, max_length=64)
    claim_key: str = Field(min_length=1, max_length=96)
    claim_value: str = Field(min_length=1, max_length=160)
    text: str = Field(min_length=1, max_length=1200)
    sha256: str = Field(min_length=64, max_length=64)

    @model_validator(mode="after")
    def valid_identity_and_hash(self) -> Passage:
        if (
            IDENTIFIER.fullmatch(self.passage_id) is None
            or CLAIM_KEY.fullmatch(self.claim_key) is None
            or CLAIM_VALUE.fullmatch(self.claim_value) is None
            or SHA256.fullmatch(self.sha256) is None
            or self.sha256 != passage_digest(self.claim_key, self.claim_value, self.text)
        ):
            raise ValueError("Invalid passage identity or content hash")
        return self


class Document(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    document_id: str = Field(min_length=1, max_length=64)
    version: str = Field(pattern=r"^v[1-9][0-9]{0,3}$")
    kind: Literal["supplier_terms", "policy", "runbook"]
    subject_id: str | None = None  # Supplier ID for supplier terms; otherwise global to ACL.
    effective_from: str
    effective_until: str | None = None  # Exclusive.
    supersedes: str | None = None
    roles: tuple[Literal["planner", "approver", "auditor"], ...] = Field(min_length=1, max_length=3)
    locations: tuple[str, ...] = Field(min_length=1, max_length=16)
    passages: tuple[Passage, ...] = Field(min_length=1, max_length=16)

    @model_validator(mode="after")
    def valid_document(self) -> Document:
        try:
            start = date.fromisoformat(self.effective_from)
            end = date.fromisoformat(self.effective_until) if self.effective_until else None
        except ValueError as exc:
            raise ValueError("Invalid document effective date") from exc
        if (
            IDENTIFIER.fullmatch(self.document_id) is None
            or start.isoformat() != self.effective_from
            or (end is not None and end.isoformat() != self.effective_until)
            or (end is not None and end <= start)
            or (self.supersedes is not None and self.supersedes == self.version)
            or (self.kind == "supplier_terms" and self.subject_id is None)
            or (self.kind != "supplier_terms" and self.subject_id is not None)
            or (
                self.subject_id is not None
                and re.fullmatch(r"[A-Z][A-Z0-9_-]{0,63}", self.subject_id) is None
            )
            or len(set(self.roles)) != len(self.roles)
            or len(set(self.locations)) != len(self.locations)
            or any(
                re.fullmatch(r"[A-Z][A-Z0-9_-]{0,63}", location) is None
                for location in self.locations
            )
            or len({passage.passage_id for passage in self.passages}) != len(self.passages)
        ):
            raise ValueError("Invalid document identity, scope, interval or passages")
        return self


class CorpusFile(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    contract: Literal["nexus.local-copilot-documents.v1"]
    scope: Literal["fictional-development-only"]
    documents: tuple[Document, ...] = Field(min_length=1, max_length=32)


class DocumentCorpus(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    documents: tuple[Document, ...]
    corpus_sha256: str


class PassageReference(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    reference_id: str
    document_id: str
    version: str
    kind: Literal["supplier_terms", "policy", "runbook"]
    subject_id: str | None
    passage_id: str
    claim_key: str
    claim_value: str
    text: str
    sha256: str
    corpus_sha256: str


class ClaimEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    status: Literal["found", "missing", "conflict"]
    claim_key: str
    claim_value: str | None
    references: tuple[PassageReference, ...]


def load_corpus(path: Path) -> DocumentCorpus:
    """Load a bounded, hash-checked corpus from a trusted configured path."""
    with path.open("rb") as source:
        raw = source.read(MAX_CORPUS_BYTES + 1)
    if len(raw) > MAX_CORPUS_BYTES:
        raise DocumentError("Document corpus exceeds the byte limit")
    try:
        parsed = CorpusFile.model_validate_json(raw)
    except (ValueError, UnicodeError) as exc:
        raise DocumentError("Document corpus is invalid") from exc
    by_identity = {(doc.document_id, doc.version): doc for doc in parsed.documents}
    if len(by_identity) != len(parsed.documents):
        raise DocumentError("Duplicate document version")
    for doc in parsed.documents:
        if doc.supersedes is None:
            continue
        prior = by_identity.get((doc.document_id, doc.supersedes))
        if prior is None or date.fromisoformat(prior.effective_from) >= date.fromisoformat(
            doc.effective_from
        ):
            raise DocumentError("Invalid supersession chain")
    return DocumentCorpus(documents=parsed.documents, corpus_sha256=hashlib.sha256(raw).hexdigest())


def retrieve_claim(
    corpus: DocumentCorpus,
    *,
    actor: Actor,
    location_id: str,
    claim_key: str,
    as_of: date,
    subject_id: str | None = None,
) -> ClaimEvidence:
    """Return current authorized passages or explicit missing/conflicting evidence.

    The caller supplies a trusted authenticated actor and date. This function does
    not infer a role, location, claim or time from document prose.
    """
    if (
        not isinstance(actor, Actor)
        or actor.role not in HUMAN_ROLES
        or location_id not in actor.locations
    ):
        raise DocumentAccessError("Document access denied")
    if (
        not isinstance(claim_key, str)
        or CLAIM_KEY.fullmatch(claim_key) is None
        or not isinstance(as_of, date)
        or isinstance(as_of, datetime)
        or (
            subject_id is not None
            and (
                not isinstance(subject_id, str)
                or re.fullmatch(r"[A-Z][A-Z0-9_-]{0,63}", subject_id) is None
            )
        )
    ):
        raise ValueError("Invalid claim query")
    superseded = {
        (doc.document_id, doc.supersedes)
        for doc in corpus.documents
        if doc.supersedes is not None and date.fromisoformat(doc.effective_from) <= as_of
    }
    references: list[PassageReference] = []
    for doc in corpus.documents:
        if (
            (doc.document_id, doc.version) in superseded
            or date.fromisoformat(doc.effective_from) > as_of
            or (
                doc.effective_until is not None and as_of >= date.fromisoformat(doc.effective_until)
            )
            or actor.role.value not in doc.roles
            or location_id not in doc.locations
            or (doc.subject_id is not None and doc.subject_id != subject_id)
        ):
            continue
        for passage in doc.passages:
            if passage.claim_key == claim_key:
                references.append(
                    PassageReference(
                        reference_id=f"{doc.document_id}@{doc.version}#{passage.passage_id}",
                        document_id=doc.document_id,
                        version=doc.version,
                        kind=doc.kind,
                        subject_id=doc.subject_id,
                        passage_id=passage.passage_id,
                        claim_key=passage.claim_key,
                        claim_value=passage.claim_value,
                        text=passage.text,
                        sha256=passage.sha256,
                        corpus_sha256=corpus.corpus_sha256,
                    )
                )
                if len(references) > MAX_MATCHES:
                    raise DocumentError("Too many matching passages")
    references.sort(key=lambda ref: ref.reference_id)
    if not references:
        return ClaimEvidence(status="missing", claim_key=claim_key, claim_value=None, references=())
    values = {reference.claim_value for reference in references}
    return ClaimEvidence(
        status="found" if len(values) == 1 else "conflict",
        claim_key=claim_key,
        claim_value=next(iter(values)) if len(values) == 1 else None,
        references=tuple(references),
    )
