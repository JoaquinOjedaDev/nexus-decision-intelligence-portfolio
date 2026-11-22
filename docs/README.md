# NEXUS documentation

Start with the [project README](../README.md) to run the local demo. The documents below explain what the system does, how to verify it, and where its claims stop.

## Portfolio review

1. [Five-minute demo](portfolio/DEMO.md): the browser walkthrough from recommendation to mock outcome.
2. [Case study](portfolio/CASE_STUDY.md): the business question and engineering decisions.
3. [Learning guide](portfolio/LEARNING.md): a tour of data, models, optimization, controls, and AI.
4. [Security boundaries](portfolio/SECURITY.md): why the demo must stay on loopback.
5. [Local acceptance](portfolio/VERIFICATION.md): dated receipts and their limits. Run `uv run --group portfolio nexus portfolio verify` on the current checkout for fresh local evidence.

## Deeper engineering

| Area | Start here |
| --- | --- |
| Data contracts and point-in-time joins | [Data lifecycle](architecture/data-lifecycle.md), [synthetic data card](data/r2-synthetic-data-card.md) |
| Forecast evaluation and champion choice | [Baseline model card](modeling/r3a-baseline-model-card.md), [local learning result](portfolio/LOCAL_LEARNING.md) |
| Constrained replenishment | [Decision formulation](decisioning/r4a-mathematical-formulation.md), [case study](portfolio/CASE_STUDY.md) |
| Approval and execution | [Portfolio security](portfolio/SECURITY.md), [foundational threat model](security/threat-model.md) |
| Optional connected runtime and local AI | [Connected runtime](portfolio/CONNECTED_RUNTIME.md), [local AI guide](portfolio/LOCAL_COPILOT_TOOLS.md) |
| Reproduction and CI | [CI tiers](runbooks/ci-tiers.md), [verification record](portfolio/VERIFICATION.md) |

The original private development repository retains dated planning, gate, and verification logs. This reviewer set carries the current local demonstration and its limits. The original distributed-service G2-G7 gates remain open.
