# Optional local decision assistant

The default evidence assistant is deterministic retrieval with project-document citations. Starting the demo with `nexus portfolio serve --local-model` enables a separate, digest-pinned Ollama mode on loopback. The model must be installed locally; there is no automatic download, cloud fallback, or action permission. See the [README setup](../../README.md#optional-local-ai-explanation).

The installed model selects at most one governed tool: `get_forecast`, `get_inventory_risk`, `explain_recommendation`, `get_supplier_terms`, `compare_scenarios`, or `draft_purchase_order`. The last tool returns an **unsubmitted view** of an existing recommendation. It is not an execution command.

The server, rather than the model, binds the selected recommendation, authenticated role and location, current generation, fingerprint, quantities, and evidence. It checks session validity again after inference. Unavailable, stale, unsupported, or out-of-scope evidence produces a refusal or bounded failure. Neither assistant can approve or execute an order.

The [versioned fictional document corpus](../../fixtures/local_copilot/portfolio-demo-v1/README.md) provides synthetic supplier invoice terms and an independent-approval policy. Document claims include a reference and corpus digest; supplier terms shown in planning data are not silently treated as contractual payment terms.

The [frozen 24-case synthetic fixture](../../fixtures/local_copilot/portfolio-holdout-v4.json) and [opt-in installed-model test](../../tests/test_portfolio_local_copilot_holdout.py) cover tool selection, document citations, and refusals. The default test suite skips the real-model run. A locally observed holdout result is diagnostic evidence for this pinned model and synthetic corpus, not a general guarantee of grounding, prompt-injection resistance, or performance.

Full G6 and combined service acceptance still require broader held-out quality, authorization, injection, failure, latency, concurrency, and resource measurements. The local demo remains separate from any production purchasing system.
