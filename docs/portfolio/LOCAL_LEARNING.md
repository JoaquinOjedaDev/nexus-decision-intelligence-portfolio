# Local learning and review

The connected learning experiment is separate from the frozen R3B result. It uses
actual local LightGBM training, not an API service or a hard-coded prediction.
It does not change the deployed planner or the historical scientific champion.

## Run it

From the repository root:

    uv sync --locked --group portfolio --group modeling --group dev
    uv run --group portfolio --group modeling nexus portfolio learning-fixture artifacts/learning-source
    uv run --group portfolio --group modeling nexus portfolio learn-runtime artifacts/learning-source --finalization-file artifacts/learning-source/outcome-finalization.json

The first command installs open-source packages; application execution requires
no provider key or paid service. The generator refuses a populated destination.
Learning creates a unique isolated evidence directory by default. The command
prints its retained receipt path. It never resets the interactive demo.

To also record the local proposal/candidate/offline shadow evidence and display
the result beside a fresh plan from exactly the same source:

    uv run --group portfolio --group modeling nexus portfolio learn-runtime artifacts/learning-source --finalization-file artifacts/learning-source/outcome-finalization.json --record-review --runtime-state artifacts/learning-demo
    uv run --group portfolio --group modeling nexus portfolio serve-runtime --state-dir artifacts/learning-demo --port 8767

Open System & audit to inspect Candidate evaluation. These commands bootstrap
explicit local demo planner/registrar identities, not a human approver. No model
is approved or deployed. Source changes mark the retained panel evidence stale.
Use a new state directory for changed scientific code; never overwrite old evidence.

## What is actually learned

The separate synthetic learning profile contains one year of four SKU/location
series. It passes through the same R2 source contracts and dbt Gold build.
The new generator/profile identifiers are explicit; the original 28-day source
fixture and historical R3B experiment are not rewritten.

Gold demand joins Gold fulfillment at the exact daily product/location grain.
Only fully fulfilled days with an explicit source-bound correction-closure
record enter learning. Arrival alone is not finality. This synthetic fixture
declares closure 72 hours after day completion; it is not a claim about a real
company's correction process. The five-second UI outcome is never a training label.

Point-in-time features use only records both available and finalized at each
forecast origin. Missing historical features follow the existing explicit
zero-with-history-count policy. Future or not-yet-finalized outcomes never enter
those features. Earlier matured outcomes may enter the history of later rolling
origins, including later origins within the calibration or evaluation windows.
The candidate's fitted parameters and calibration offsets remain fixed before
the final evaluation window.

The policy in configs/portfolio/runtime-learning.yaml fixes separate target
windows and maturity cutoffs. Origins are 14 days apart and contain complete
14-day windows. A missing/censored/unclosed day excludes its entire key-origin;
receipts list excluded windows and trailing dates. Results therefore describe
that complete, fully fulfilled subset, not all inventory demand.

Training must have at least 500 rows. Calibration requires at least 20 distinct
outcomes per horizon; the declared fixture supplies 24 for each of 14 horizons.
The final set has 24 product/location/origin blocks and 336 distinct targets.
The original monitoring floor of 500 mature outcomes remains separate.

## The current engineering result

The real fixture run fitted on 560 rows, calibrated on 336, and evaluated on 336.
Its WIS was approximately 0.378 versus 0.536 for the separately calibrated
seasonal comparator: about 29.5% lower on this synthetic subset. However, its
nominal 80% interval covered only 62.2% of outcomes, and its nominal 95% interval
covered 83.0%. Both must remain within five percentage points of nominal coverage.
The coverage guards reject
the candidate, despite the lower average error. No deployment or champion
promotion follows from this result.

This is an engineering benchmark, not confirmatory research or proof of customer
savings. An initial exploratory receipt exposed a missing coverage guard and is
superseded by the guarded run; no model was deployed from it. The first yearly
source also accumulated excess inventory because of an overbuying replenishment
rule. Recipe 1.1.0 uses a declared ex-ante mean-demand replenishment quantity;
the demand CSV is unchanged, inventory conservation passes, and no warehouse
constraint was relaxed. The older infeasible evidence remains retained. Final targets are
held out of fitting/calibration, but have been inspected during engineering.
Do not describe them as a never-viewed confirmatory holdout.

Native model strings, calibration offsets, source/closure/feature/split hashes,
code and dependency identities, paired WIS uncertainty, segment diagnostics and
reload parity are retained. No pickle is loaded. Altered model bytes, receipt
content or source bindings fail closed before registration or presentation.

## Governance boundaries

The local review bridge records measured historical baseline telemetry, a
training proposal, the actual candidate evaluation, and paired offline shadow
replay. It does not pretend that historical replay is a live deployment test.
A rejected, losing or insufficient candidate cannot be deployed even in the
sandbox. Independent approver-controlled sandbox switch/rollback behavior has
separate tests; synthetic evidence can never promote the scientific champion.

The operational planner still uses the fresh seasonal baseline. A local review
record is not an automatic drift-triggered model rollout, production monitoring
service, OIDC deployment, or closure of the original full G3/G5/G7 gates.
