# ADR-008: Data contracts as YAML, compiled into the control plane

- **Status:** accepted
- **Date:** 2026-09-27

## Context

ADR-002 put configuration in the control plane. A data contract is
configuration, so the obvious move is another table — and it is the
wrong one.

A contract is an agreement between a producer and a consumer: these
columns, these types, nulls here and not there, refreshed this often,
this person answers the phone. That is a human-authored, reviewed
artefact. Putting it in a database means it changes by `UPDATE`, with no
diff, no review and no history a reviewer can read.

## Decision — YAML in git, compiled into `dq_rule`

Contracts live in `contracts/<system>/<object>.yml` and are validated by
Pydantic with `extra="forbid"`, so a typo fails loudly rather than
silently disabling a check.

`sync_contract` compiles a contract into `dq_rule` rows plus the `owner`
and `freshness_sla_minutes` fields on `source_object`. The engine still
reads only the control plane; the contract is the thing a human edits.
One generates the other, so they are not rival sources of truth.

Generated rules are marked in their expression. A sync deletes and
rewrites only marked rules, so deleting an expectation from the YAML
deletes the rule rather than leaving an orphan running forever, while
hand-written rules are left alone — the contract owns what it created
and nothing else.

## Decision — conformance is a gate, quality is a measurement

`check_conformance` compares an arriving table against the promised
columns and types. That is a breach of the agreement, not a data
problem, and it fails outright rather than producing a severity.

This is deliberately not step 26. Drift detection asks "did this source
change since we last saw it?"; conformance asks "does this source match
what we agreed?" A source can pass one and fail the other — a column
missing since before the contract was written drifts not at all and
breaches immediately.

Extra columns are not a breach. A contract states what must be present,
not what must be absent, and treating additions as failures would undo
step 26's additive-evolution policy.

## Consequences

- **A new runtime dependency: PyYAML.** It was already present
  transitively via `pre-commit`; relying on that was luck. It is now
  declared.
- `python -m lakehouse.contracts` validates without a database, so it
  runs in CI on every pull request touching a contract, for free.
- Contract and registered object can disagree — a contract for an
  unregistered object raises on sync. There is no check in the other
  direction yet: an object with no contract is not reported.
- Column types are Arrow type names as strings (`string`, `int64`). That
  couples contracts to the engine's type system, which is honest today
  and will need a mapping layer when step 33 adds Spark.
