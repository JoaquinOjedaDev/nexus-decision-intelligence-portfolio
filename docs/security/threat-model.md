# R0 threat model

This is the foundational threat inventory, not a current penetration test or production security attestation. The [portfolio security guide](../portfolio/SECURITY.md) describes the implemented local demo controls and deployment limits.

**Method:** asset/trust-boundary review informed by STRIDE

**Scope:** source ingestion through mock execution and feedback

**Review trigger:** new integration, trust boundary, authoritative write, sensitive field, or AI tool

## Assets and security objectives

| Asset | Integrity | Confidentiality | Availability |
|---|---|---|---|
| Recommendation manifest and policy | Exact reviewed inputs and outputs | Restrict sensitive lineage | Rebuild/replay when needed |
| Approval and lifecycle ledger | Append-only, attributable transitions | Scoped audit access | Never bypass on outage |
| Inventory/demand/supplier facts | Point-in-time correctness and provenance | Minimize commercial data | Quarantine bad feeds |
| Execution command | One approved business effect | No secrets in payload/logs | Retry safely; fail closed |
| Identities, tokens, secrets | Authentic, least privilege | Never stored in data artifacts | Rotation and revocation |
| Model/AI artifacts | Versioned, evaluated, provenance-bound | Prevent training/context leakage | Deterministic fallback |

## Trust boundaries

1. External file/API/event source to ingestion.
2. Validated analytical data to decision kernel.
3. Decision kernel to human-facing control plane.
4. Human identity provider/browser to approval API.
5. Approved outbox to system of action.
6. Read-only evidence to the optional LLM adapter.
7. CI artifact/signing boundary to demo/release runtime.

## Threats and controls

| Threat | Example | Primary controls | R0 evidence expectation |
|---|---|---|---|
| Spoofing | Forged approver or source producer | OIDC/workload identity target; fixture separation; issuer/audience checks | Negative authorization tests |
| Tampering | Quantity changed after review | Canonical manifest hash; append-only ledger; exact-hash approval | Mutation and mismatch tests |
| Repudiation | Actor denies approving | Subject ID, role, timestamp, reason, correlation, immutable event | Rebuildable audit timeline |
| Information disclosure | Secrets/PII in logs or AI context | Schema allow-list, redaction, secret scanning, least privilege | Fixture/log scan; no datasets committed |
| Denial of service | Event flood or failed dependency | Bounds, timeouts, queues, rate limits, profile isolation; fail closed | Resource limits and failure tests |
| Elevation of privilege | Planner self-approves or worker changes state | Default deny; maker-checker; separate service roles | Policy matrix tests |
| Replay/duplication | Broker or ERP retry causes duplicate order | Stable IDs, idempotency keys, unique constraints, outbox | Duplicate-delivery test |
| Stale decision | Approval after material inventory change | Expiry, supersession, freshness check, replan event | Stale-approval test |
| Supply-chain compromise | Modified dependency/image/model | Lock files, hashes, SBOM/provenance target, protected promotion | CI verification evidence |
| Prompt injection | Source text asks AI to execute | Read-only allow-listed context/tools; no action credentials | Boundary/adversarial tests |

## Abuse cases that must fail closed

- A planner calls the approval endpoint directly for their own recommendation.
- An approver submits a valid signature for a different manifest hash.
- A duplicate or reordered event attempts to create a second inventory effect.
- A worker retries after losing the ERP response and creates a second business action.
- A data-quality failure, optimizer infeasibility, or approval outage attempts to continue execution.
- Source content or a user prompt asks the LLM to alter a quantity, role, constraint, or approval.

## Residual risk and deferrals

R0 uses local fixture identities and a mock ERP, so it cannot prove production identity-provider,
network, key-management, or downstream non-repudiation controls. Production data classification,
retention/deletion, backup recovery, dependency attestations, penetration testing, and incident
response are later gates. These gaps prohibit production use but do not prevent a local contract
proof.

## Review record

Threat status is tracked in [`../evidence/R0_GATE.md`](../evidence/R0_GATE.md). A resolved threat must
link a test, configuration, or recorded exercise; prose alone is not closure evidence.
