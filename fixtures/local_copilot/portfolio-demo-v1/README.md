# Fictional portfolio demo documents

`documents.json` contains synthetic supplier invoice terms and a synthetic
independent-approval policy for the frozen `LOC-*` demo and connected `DC-ARG-*`
runtime. Its supplier IDs match the demo configs. These clauses are teaching
fixtures, not real contracts or source data. The server uses the current date,
the authenticated location and selected supplier to retrieve one authorized
claim; it never changes the clock to make a document appear current.

The separate `g6-documents-v1` corpus remains a development fixture for
supersession, conflict and injection tests. The portfolio demo corpus has SHA-256
`6a2cb875a2ebd41cdb199155d791174cad002c42f5a1e6b98f4c65919a9ae029`.
