# ADR-009: Data quality with three severity tiers

- **Status:** accepted
- **Date:** 2026-09-27

## Context

`dq_rule` and `dq_result` have existed since step 15, along with a
`Severity` enum of warn, quarantine and fail. This records the engine
that finally uses them, and why it behaves as it does.

## Decision — build it, do not adopt one

Great Expectations and Soda were the obvious candidates. Both are
heavier than this needs: they bring their own configuration format,
their own store, and their own opinion about where rules live — which
collides directly with the control plane being the single source of
configuration. The rule types here (not-null, unique, range, allowed
values, regex, freshness, row count) are a few dozen lines each against
Arrow.

**Consequence:** the honest version of this is that a production team
should probably adopt a library, and the README should say so rather
than pretend otherwise. The reason to hand-roll it here is that the
severity model and its integration with the audit tables *are* the
point of the project.

## Decision — three tiers, not two

- **warn** — record, load everything, carry on.
- **quarantine** — divert the failing rows, load the rest.
- **fail** — abort. Nothing is written.

A system offering only pass and fail gets configured entirely as
"ignore" within a quarter, because most quality problems genuinely
should not stop a pipeline, and the only way to express that is to turn
the check off. The middle tier is what keeps checks switched on.

## Decision — every rule is evaluated before any consequence

Even when a fail-severity rule has already been breached, the remaining
rules still run and still record results. Stopping at the first failure
hides the other nine, and the first failure is rarely the informative
one.

## Decision — an unevaluable rule is an error

A rule this engine cannot evaluate — `custom_sql`, a column not present
in the batch, an unparseable expression — raises `RuleEvaluationError`
rather than being skipped. A silently skipped rule is worse than no
rule, because somebody is relying on it.

## Decision — quality is enforced at Silver

Not Bronze. ADR-004 makes Bronze deliberately faithful to the source,
messiness included; that is what makes replay meaningful. Silver is the
first layer anyone should query, so it is the first layer that owes a
guarantee about its contents.

## Where nulls sit

A null passes every rule except `not_null`. A null is an absent value,
not a wrong one, and reporting it as both missing and out-of-range
buries the actual finding under a duplicate.

## Consequences

- **Quarantined rows are currently dropped, not persisted.** They are
  returned on `QualityOutcome.rejected` and counted in the task audit,
  but nothing writes them yet. That is step 29, and until it lands this
  is real data loss on any quarantine-severity rule.
- The task run status becomes `quarantined` rather than `succeeded` when
  rows were diverted, so the audit distinguishes a clean load from a
  lossy one.
- `unique` flags every copy of a duplicated value, not the copies after
  the first. Which row is the "right" one is a judgement the engine
  cannot make.
- Evaluation is per batch and in memory. Fine at this scale, and the
  first thing to revisit at step 33.
