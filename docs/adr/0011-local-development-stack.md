# ADR-011: A local stack that comes up with one command

- **Status:** accepted
- **Date:** 2026-09-27

## Context

Phase 1's exit criteria is that `git clone && make up && make seed &&
make test` works on a clean machine. Until now `make up` built a single
application container against a Postgres that nothing used, and the
lineage work at step 31 had nowhere to send events. This step makes the
criterion true.

## Decision — one Postgres, two databases

Marquez needs its own database. The obvious options were a second
Postgres container or a shared one.

Shared, with an init script creating the `marquez` database and role.
Two Postgres containers on a developer laptop is a gigabyte of RAM to
avoid writing four lines of SQL.

**Postgres 14, not 16.** Upstream Marquez is tested against 14, and
Postgres 15 changed the default grants on the `public` schema in a way
Marquez's migrations predate. Running a newer major version to look
current is the kind of decision that costs an afternoon later.

**The credentials are fixed and public.** `marquez.dev.yml` inside the
image hardcodes the database, user and password as `marquez` and only
makes host and port configurable, so there was no choice about those.
For the rest, a disposable local stack should never require a developer
to find a secret before the tests run. Step 37 resolves real
credentials by name.

## Decision — search is explicitly disabled

`SEARCH_ENABLED` defaults to **true** in Marquez's config, at which
point it tries to reach an OpenSearch host this stack does not run.
Setting it false is not a simplification, it is required.

## Decision — `make up` waits, and then verifies

`docker compose up -d --wait` blocks on the healthchecks, then
`python -m lakehouse.stack` checks each service is doing its job.

The two are not redundant. A container reports healthy before it is
useful: Postgres accepts TCP before it accepts queries, and Marquez
serves its admin port while migrations are still running. The checker
probes the API each service actually exposes, reports every failure
rather than the first, and exits non-zero — so it also works as a CI
gate.

## Decision — no object storage in this stack

The first version of this file ran MinIO. It could not be pulled:
MinIO removed `minio/minio` and `minio/mc` from Docker Hub between
11 and 14 September 2026, and the Hub API now returns object-not-found
for both. Mirrors are contested — quay.io serves the same manifests
according to some reports and refuses anonymous pulls according to
others — and the community forks are days old.

Rather than gamble on a registry, the service was removed, because it
had no consumer. `lake_root` is a local path and every writer takes a
`Path`; moving the lake to S3 means threading `storage_options` through
each writer, which is code this step does not touch. The bucket existed
only to be empty.

So the honest position is: the lake is on local disk, there is no
object storage, and the backend gets chosen at step 33 when something
actually reads from it — with whatever the registry situation is by
then.

The `s3_*` settings remain, defaulted to empty, so the settings surface
does not churn when that happens.

**The general lesson, which is the reusable part:** an unused
dependency is not free. It cost a broken `docker compose up` on a
clean machine, which is precisely the thing this step exists to
guarantee.

## Consequences

- Docker becomes a hard prerequisite for `make up`, though not for
  `make test` — the suite still runs with nothing up, and
  `OPENLINEAGE_ENABLED` defaults to false so CI never needs Marquez.
- Image tags are pinned rather than `latest`, except MinIO, whose
  release tags are date-stamped and churn weekly.
- `docker compose down -v` deletes the volumes, so the Marquez database
  is recreated on next start. Lineage history is disposable here; that
  would be wrong anywhere else.
