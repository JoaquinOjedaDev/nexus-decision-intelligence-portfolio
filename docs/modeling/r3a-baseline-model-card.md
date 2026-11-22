# Model card: R3A seasonal-naive probabilistic baseline

## Model details

| Field | Value |
|---|---|
| Name | `seasonal_naive_empirical_intervals` |
| Version | `1.0.0` |
| Release role | R3A champion baseline and future candidate control |
| Owner | Data/ML publisher role |
| Decision authority | None; forecasts cannot approve or execute an action |
| Training data | Exact registered R2A `gold.r2_demand_history` snapshot |
| Output | Daily nonnegative quantiles 0.025, 0.10, 0.50, 0.90, and 0.975 |

## Intended use

This baseline is an explainable control for weekly replenishment research. It proves that NEXUS can
issue point-in-time forecasts with uncertainty, evaluate them under rolling origins, and bind them
to reproducible provenance. A later learned model must beat this control under the same frozen
protocol.

It is not approved for purchasing, staffing, financial reporting, autonomous execution, or a
production service-level promise.

## Method

The median forecast for each target day is demand from the same weekday seven days earlier. When
that observation is unavailable, the configured cold-start value is used. Uncertainty comes from
the empirical distribution of one-week seasonal residuals that were knowable at the issue time.
Each residual quantile is added to the median, predictions are clamped at zero, and crossings are
repaired into nondecreasing order.

No random fitting, external feature, promotion threshold, or stress-period result changes this
algorithm.

## Data and label policy

R3A uses latent `demand_qty`, not fulfilled sales, because the synthetic R2 contract explicitly
provides unconstrained demand. If a future source exposes sales only, stockout-censored records
must fail the target contract or pass through a separately reviewed latent-demand method.

An observation can influence a forecast only when its demand date is before the issue date and its
availability timestamp is no later than the issue time. Mature evaluation labels are joined after
forecast creation using a separate cutoff.

## Evaluation

The predeclared R3A smoke uses three weekly origins and a seven-day horizon. It reports WIS,
pinball loss, 80%/95% interval coverage, panel MASE, bias, hierarchy segments, and a deterministic
SKU/location/origin block-bootstrap interval. Panel MASE applies each series-origin's own seasonal
scale before equal-observation averaging.

The local frozen-stress result over 28 mature points is WIS `0.512589`, 80% coverage `0.75`, 95%
coverage `0.785714`, panel MASE `1.273954`, and bias `0.25`. The full 84-point smoke evaluation has
WIS `0.630613`; its aggregate MASE is intentionally unavailable because the first cold-start origin
has no valid seasonal scale. These results test implementation behavior on 112 synthetic rows;
they do not estimate real-world generalization.

## Limitations

- Four dense series over 28 days cannot validate intermittent, cold-start, promotion, or long-term
  behavior at portfolio scale.
- Empirical tails are coarse with so few residuals.
- Bottom-up sums of marginal quantiles are arithmetic-coherence evidence, not a calibrated joint
  distribution.
- The current master data are immutable snapshots, not point-in-time historical dimensions.
- Supplier lead-time uncertainty is absent from this model.
- The model can repeat a seasonal shock and cannot anticipate a genuinely new promotion or regime.

## Governance

The baseline remains champion until a candidate passes the frozen G3 comparison and a human
Data/ML publisher approves its immutable artifact bundle. Forecast failure must return a labeled
safe fallback; it must not silently reuse a stale forecast.
