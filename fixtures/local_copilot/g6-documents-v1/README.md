# Fictional development documents only

`documents.json` is a small deterministic development fixture for document parsing,
authorization, supersession, conflict and injection tests. Its 2027 effective dates,
`WH-NORTH`/`WH-SOUTH` locations and `SUP-001` supplier match the portable test seed.
They are not the current operational demo's `LOC-001` scope or current source dates.

Do not change the trusted `as_of` date to make this fixture appear current. A later,
separately versioned fictional demo corpus must use the actual demo's source
identity, locations, supplier bindings and effective dates, then receive its own
review and corpus hash. This fixture and its tests are development evidence, not
the sealed G6 holdout or actual-runtime acceptance.
