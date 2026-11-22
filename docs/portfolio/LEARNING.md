# Learning walkthrough: how to explain what you built

## 1. A table is not yet a trustworthy data product

Start at the source contracts and dbt models. A contract describes valid fields; a quality test checks meaning and relationships; point-in-time logic controls which information was available at a decision cutoff. Exercise: inject a duplicate or late event and inspect quarantine and resulting Gold rows. Explain why simply dropping bad rows loses useful evidence.

## 2. Prediction quality and business value are different

Read the seasonal baseline and LightGBM evaluation. Explain rolling temporal splits, leakage, uncertainty quantiles, and label maturity. Then inspect the champion decision: a more complex model did not earn promotion. Exercise: describe what additional evidence would justify switching the champion. Never train on the evaluation period to improve the displayed score.

## 3. Optimization turns predictions into feasible proposals

Read the planning contract, CP-SAT optimizer, and independent validator. The objective defines preferences; constraints define allowed actions. Exercise: reduce the budget and observe changed quantities or fail-closed behavior. A solver returning a value does not itself prove that the business constraints were encoded correctly.

## 4. A proposal is not an authorized action

Trace a recommendation fingerprint through submission, approval, expiry, execution, and audit. The fingerprint prevents silently changing the order after approval. Maker-checker separation prevents one identity from creating and approving its own action. Exercise: change a fingerprint in an API test and show the rejection.

The local JWT has a different job: it binds a browser session to the server's selected identity.
`iss` identifies the test issuer, `aud` the intended API, `sub` the actor, and `exp` the expiry.
The signature protects those claims from alteration; it does not encrypt them or prove who is
sitting at the keyboard. Server-side session records make logout/revocation effective and remain
the authority for permissions. Exercise: compare wrong-audience, expired-session, revoked-session
and cross-location refusals. A valid token still cannot bypass approval policy or CSRF checks.

## 5. Retries are normal

A network client may retry after losing a response. Unique execution records and idempotency keys let the system return the prior result. Exercise: issue two concurrent execution requests and check the number of mock effects. Explain the difference between local transactional guarantees and a distributed external side effect.

## 6. Evidence and observability are product features

Audit traces explain a decision after the fact; health and incidents help an operator intervene now. Exercise: trigger, resolve, restart, and inspect the local notification journal. Explain why this does not prove the behavior of an external email or paging service.

## 7. AI must have boundaries

The reference assistant retrieves evidence but has no action tools. Exercise: ask an unsupported question, an instruction to approve everything, and a valid forecasting question. Compare abstention, refusal, and citations. An optional installed local model can select governed read-only tools and cite fictional supplier and policy documents for a current recommendation. It cannot approve or execute, and the server checks its answer against current evidence. This bounded local evaluation is not a production AI acceptance gate.

## 8. Connected evidence and the remaining learning gate

The frozen demonstration keeps its historical forecast fixture separate from its
fresh dbt example. Connected runtime mode instead binds one Gold snapshot's
SKU/location keys, cutoff, fresh seasonal forecast and optimizer plan. Source
changes update provenance and invalidate approvals. This does not overwrite the
frozen learned-model experiment or change its `CHALLENGER_LOST` result.

Local measured monitoring and mature-label training now reject an ineligible
challenger. A prospective eligible model switch and rollback remain open; saving
an outcome row or fitting a model does not complete that gate. See [the scope
ledger](RELEASE.md).

## Interview questions to practice

- Why did the seasonal model stay champion?
- How do you prevent point-in-time leakage?
- What happens when optimization is infeasible?
- What prevents a duplicate purchase after a timeout?
- What does the audit hash chain prove, and what does it not prove?
- Which parts must change for multi-tenant production deployment?
- Which results are measured, simulated, historical, or not yet verified?

Use the code and acceptance evidence to answer. This project demonstrates thoughtful engineering; it does not substitute for years of production ownership or justify overstating seniority.
