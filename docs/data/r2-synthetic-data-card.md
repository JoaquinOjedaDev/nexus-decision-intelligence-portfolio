# R2A synthetic supply-network data card

**Version:** 1.0.0

**Classification:** synthetic

**Release scope:** bounded R2A smoke profile

## Purpose

This dataset exists to teach and verify trustworthy data engineering: deterministic generation,
source integrity, typed validation, quarantine, bitemporal cutoffs, corrections, dbt data products,
and replay. It is not intended to estimate real demand, supplier risk, or business value.

## Provenance and license

All records are generated locally by repository-owned code in
`src/nexus/simulation/supply_network.py`. The generator does not read M5, ERP, WMS, TMS, personal,
customer, or proprietary data. The source manifest records `classification = synthetic`,
`generator = nexus-r2-supply-network`, schema version `1.0.0`, and the seed.

The approved-source entry is maintained in
[`source-license-manifest.md`](source-license-manifest.md). M5 remains blocked and is neither
downloaded nor required.

## Generation configuration

| Field | Value |
|---|---|
| Profile | `smoke` |
| Seed | `20260718` |
| Start date | `2026-06-01` |
| Duration | 28 days |
| Early cutoff | `2026-06-20T00:00:00Z` |
| Final cutoff | `2026-06-30T00:00:00Z` |
| SKUs | `SKU-001`, `SKU-002` |
| Locations | `DC-ARG-01`, `DC-ARG-02` |
| Suppliers | `SUP-001`, `SUP-002` |

Demand uses a small weekday pattern plus a seeded integer perturbation. Purchase orders follow a
fixed cadence. Four orders remain open at the final cutoff, and selected receipts include a
deterministic one-day supplier delay.

Supplier and product/location files are single immutable master snapshots with one row per business
key. They do not model slowly changing dimensions or historical master versions.

## Dataset inventory

| Source file | Grain | Rows | Main fields |
|---|---|---:|---|
| `suppliers.csv` | supplier | 2 | ID, name, active flag, `available_at` |
| `product_locations.csv` | SKU/location | 4 | primary supplier, active flag, `available_at` |
| `opening_inventory.csv` | SKU/location snapshot | 4 | on-hand, `snapshot_at`, `available_at` |
| `fulfillment_outcomes.csv` | day x SKU/location | 112 | demand, fulfilled, `available_at` |
| `purchase_orders.csv` | purchase-order line | 20 | ordered quantity and operational timestamps |
| `receipts.csv` | receipt | 16 | received quantity and operational timestamps |
| `inventory_events.jsonl` | event envelope/revision | 9 physical lines | adjustment payload and dual timestamps |

Five JSONL lines become validated adjustment revisions. Four lines are intentionally quarantined.

## Intentional quality cases

| Case | Expected disposition |
|---|---|
| Same event delivered twice | `duplicate_event_id` quarantine |
| Same semantic event under the same idempotency key | `duplicate_idempotency_key` quarantine |
| Adjustment for an unknown SKU/location | `unknown_product_location` quarantine |
| Truncated JSON envelope | `event_contract_invalid` quarantine |
| Correction revision received after the early cutoff | Validated, deferred early, effective later |
| Future event received after the final cutoff | Validated, deferred from the final snapshot |
| Original revision replaced by a visible correction | Retained as superseded history |
| Events physically out of time order | Deterministically reconciled during rebuild |

The quarantine cases are teaching fixtures, not estimates of real defect rates.

## Point-in-time outputs

The final cutoff creates five Gold products with 234 rows total:

- 112 demand-history rows;
- 112 fulfillment-outcome rows;
- 4 inventory-position rows;
- 4 open-order rows; and
- 2 supplier-performance rows.

Final controls are total demand `1112`, total on hand `178`, and total on order `240`. The final
inventory-position hash is
`0f43999ef57db66ab185cf813847c7b4b1fcd1e3734a3a583b961aa611bf25bc`.
Validated and Gold evidence records separate logical row-content and physical Arrow-schema hashes.

## Reproducibility

The same pinned generator and seed produce canonical source-manifest-content SHA-256
`03e32a3f197839134e44b8f6d384787a99ec22e2c088ee4edea8d8322814346b`.
The literal `source-manifest.json` bytes produce SHA-256
`aa25517d49cb6c36b76e9b6a218a83e7419359f5a7c40626412797ca13f8d768`.
Independent final dbt-backed rebuilds must produce the same content-derived semantic-manifest hash.

The source and output hashes are regression controls for the pinned implementation, not universal
identifiers for all future schema versions. An intentional contract or transformation change must
update the acceptance evidence rather than weakening the assertion.

## Limitations and bias

- Two SKUs, two locations, and two suppliers cannot represent a real network.
- The demand equation is hand-designed and too regular for model-performance claims.
- There are no prices, customers, geography, holidays, promotions, sensitive attributes, or
  external causal drivers.
- Lead-time behavior is simplistic and does not model capacity, partial shipments, cancellations,
  or correlated disruption.
- The fixture does not establish realistic stockout, service-level, or supplier-performance
  distributions.
- It must not be used to train or evaluate a production forecast, optimizer, or risk model.

## Responsible use

Use the data for software tests, architecture demonstrations, learning, and reproducibility checks.
Do not present synthetic metrics as company outcomes or external benchmark performance. Any future
external dataset requires its own source/license review and data card before onboarding.
