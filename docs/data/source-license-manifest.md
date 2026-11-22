# Data source and license manifest

**Rule:** no external dataset is committed, downloaded by bootstrap, or redistributed until access,
license, intended use, attribution, and redistribution terms are reviewed at the source and recorded
here. A public URL is not permission.

| ID | Source | Intended use | Access | Redistribution | Status |
|---|---|---|---|---|---|
| `synthetic-v1` | Deterministic NEXUS fixture generator | R0/R1 contracts, decisions, failures, and tests | Generated locally; repository-owned code | Fixtures and evidence must contain no copied external records | **APPROVED for R0/R1** |
| `synthetic-r2a` | `nexus-r2-supply-network` deterministic generator | R2A source integrity, bitemporal replay, corrections, quarantine, and dbt Gold tests | Generated locally from seed `20260718`; no external input | May be regenerated and shared under the repository license; must remain labeled synthetic | **APPROVED for R2A** |
| `synthetic-r2c` | `nexus-r2c-representative-supply-network` deterministic generator | Intermittency, cold-start, stockout-censoring, demand-availability, conservative lead-time-label, partition, and scale-mechanics tests | Generated locally from strict profile and seed `20260718`; no external input | May be regenerated and shared under the repository license; must remain labeled synthetic and must not be described as externally representative | **APPROVED for R2C** |
| `m5-forecasting` | M5 Forecasting competition data | Optional future demand-forecasting benchmark | Terms and account access must be reviewed manually at the original competition source | **Unverified**; do not commit or redistribute data, extracts, or derived row-level samples | **OPTIONAL — BLOCKED UNTIL REVIEWED** |
| `operational-feed` | Future ERP/WMS/TMS integration | Demo or production operational facts | Not selected | Not assessed | **NOT ONBOARDED** |

## M5 hold

M5 access and redistribution rights remain explicitly unverified until a reviewer opens the original
source, accepts any required terms themselves, records the exact source URL, terms or license
version, review date, allowed use, attribution, and redistribution decision, and obtains any required
project approval. NEXUS must not automate acceptance. No M5 dataset is included in this repository
or required for R0, R1, R2A, R2B, R2C, or R3B engineering mechanics.

M5 is one possible benchmark, not the architecture's data backbone and not a full-G2 dependency.
Any claim about external benchmark performance, real-world calibration, or production suitability
remains blocked until a lawful external source is onboarded and receives its own data card and
evidence gate.

Derived aggregates, model artifacts, screenshots, or example rows are not presumed redistributable
merely because the raw file is excluded. They require the same review.

## Onboarding checklist

For every proposed source, record:

1. Owner, authoritative URL or contract, retrieval method, and reviewer.
2. License or terms identifier and dated evidence; commercial and research restrictions.
3. Permitted environments, users, transformations, training, publication, and redistribution.
4. Data classification, sensitive fields, geographic, retention, and deletion obligations.
5. Schema, grain, time semantics, units, cadence, quality SLA, and provenance key.
6. Storage location and access controls; raw-data and derivative handling.
7. Approval outcome and expiry or review trigger.

## Repository controls

`data/` and generated artifacts are ignored except the placeholder. Repository hygiene tests fail when
the data directory contains another file, a local `.env` is present, or private-key file extensions
appear. Known-dataset filename and large-file guards expand as sources are onboarded. A developer
who has lawful local access must point NEXUS to an ignored path; they must not make that path part of
the clean-clone R0/R1/R2A/R2B/R2C flow.
