# ADR-007: Schema drift detection and evolution policy

- **Status:** accepted
- **Date:** 2026-09-27

## Context

A source changes shape and nobody tells you. A column is added, renamed,
retyped or dropped upstream, and the pipeline either absorbs it silently
or fails somewhere far from the cause. This is the most common way a
working pipeline starts producing wrong answers, and unlike a crash it
does not announce itself.

`schema_version` has existed since step 15 and has been unused. This
records what it is for and what the platform does when a schema moves.

## Decision — two classes of drift, opposite treatment

**Additive drift evolves automatically.** A new column breaks nothing;
rows loaded before it simply lack it. Failing here would mean a pipeline
that stops every time an upstream team ships a feature, and a team that
learns to ignore the alerts — which is worse than no alerting, because
it is alerting you believe in until you stop.

**Breaking drift stops the load.** A removed or retyped column
invalidates assumptions downstream has already baked in. Loading anyway
produces a table that looks fine and joins wrong. The load raises
`SchemaDriftError` naming the object and every finding, and the task is
recorded as failed like any other error.

When a batch both adds and removes, the worst finding decides. There is
no partial acceptance.

Rejected alternatives:

1. **Fail on everything.** Safe and unusable, for the reason above.
2. **Evolve everything.** A dropped column becomes all-null and every
   downstream aggregate quietly changes meaning. This is the failure
   mode the whole ADR exists to prevent.
3. **Warn and continue on breaking changes.** A warning nobody reads is
   the same as option 2 with extra log volume.

## Where the check runs

At the point of arrival — immediately after the source read in all three
Bronze strategies (full, incremental, CDC). Checking later would mean
the bad shape is already persisted.

The first sighting of an object records version 1 and reports no drift.
There is nothing to compare against, and treating an unknown source as
broken would make onboarding impossible.

## What is tracked, and what is not

Tracked: column name, type, nullability. A column becoming non-nullable
is a real change with real consequences.

Not tracked: column order and Arrow metadata. Both churn without meaning
anything, and a detector that cries wolf gets muted. The schema hash is
computed over name-sorted columns so reordering alone is invisible.

## Consequences

- A refused batch leaves the recorded schema **unchanged**, so the next
  run re-reports the same drift rather than accepting it by attrition.
  Clearing it is a deliberate act.
- There is no automated path to accept a breaking change yet. Today that
  means editing `schema_version` by hand, which is honest but rough; a
  `make accept-schema --source=X` CLI belongs with the replay CLI at
  step 35.
- Renames are indistinguishable from a drop plus an add, so they present
  as breaking. That is the right default — the platform cannot know the
  two columns mean the same thing — but it will be the most common false
  alarm.
- Drift is recorded per object in the control plane, so "how has this
  source's shape changed over time" is a query rather than a git
  archaeology exercise. Step 38's dashboard should show it.
