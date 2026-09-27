# ADR-017: PII classification as config, masking in Silver

- **Status:** accepted
- **Date:** 2026-09-27

## Context

The control plane has carried a `column_metadata` table and a
`Sensitivity` enum since ADR-002, and nothing has ever read either. A
platform that claims to be metadata-driven and then handles personal data
by not having any is not making a claim at all.

Three questions had to be answered: how a column gets classified, what
masking does to it, and where in the pipeline it happens. The third turns
out to matter most.

## Decision

### Classification is a proposal a human confirms

`python -m lakehouse.privacy` reads each Bronze table and prints what it
thinks every column is, matching on the column name first and then on a
sample of up to 1,000 values. Values can only *raise* a proposal — a
column called `notes` full of email addresses is PII whatever it was
named. `--apply` writes the proposals into `column_metadata`, and refuses
to overwrite any classification a person has already set to something
other than `none`.

Masking then reads only the table. Reclassifying a column is an UPDATE.

### Not Presidio

The plan called for Presidio. It was rejected:

- It pulls spaCy and a language model — hundreds of megabytes, a
  meaningful install on the Windows machine this project is developed on,
  for a check that runs against column names and a thousand sampled
  strings.
- It detects entities in free text. This warehouse has none: every column
  is a typed scalar, where a regex over a sample is not an approximation
  of the answer, it *is* the answer.
- Its confidence scores would invite exactly the behaviour this ADR
  refuses — masking a column because a model was 0.85 sure.

Presidio earns its weight when free text arrives. The scanner's rule
tables are the interface a Presidio backend would implement, so adding
one later is a new module, not a rewrite.

### Masking runs after quality and before every write

The ordering is the load-bearing decision:

1. **After the DQ engine.** Rules evaluate real values. A `regex` rule
   against a hashed email passes forever, which is worse than having no
   rule, because a green dashboard says the data is fine.
2. **Before the quarantine write.** Quarantine holds the rows that failed
   validation — the dirtiest, most manually inspected data in the lake.
   It is the easiest place to leak PII precisely because nobody thinks of
   it as a published table. It is masked with the same policy.
3. **Before SCD2.** Tombstones keep a deleted member's row forever
   (ADR-005). A value that reached history unmasked stays there, and a
   deletion request cannot reach it without rewriting history. Masking
   first means those versions were never unmasked.

### Hashing is keyed and deterministic

`hash` is `HMAC-SHA256(key, value)` truncated to 128 bits. Deterministic,
so a masked `customer_id` still joins to a masked `customer_id` in
another table and still deduplicates in Silver. Non-deterministic masking
breaks the star schema, and a platform where masking breaks the schema is
a platform where masking gets switched off.

Keyed, so the mask is not reversible by rainbow table — an unkeyed hash of
an email address is an email address.

`partial` keeps the last four characters, which is enough for a support
agent to confirm a record and not enough to identify anyone. `redact`
nulls the column and is the only strategy that works on non-string types;
the scanner proposes it automatically where hashing cannot apply.

Nulls stay null under every strategy. A masked null is still a null.

### `sensitive_pii` does not reach Gold

Columns classified `sensitive_pii` are dropped on the way into Gold
unless `allow_in_gold` is set on the row. Natural keys are exempt —
a dimension without its business key is not a dimension, and the value
has already been masked.

## Consequences

**Bronze keeps raw values.** Bronze is the faithful record of what the
source sent; masking it would make drift detection compare masked
schemas, and replay reproduce something that never happened. The
mitigation in a real deployment is access control on the Bronze prefix,
not a transform. This is the largest gap in this design and it is
deliberate.

**The key is not managed yet.** `MASKING_KEY` is read from settings and
defaults to a development key published in this repository, with a
warning logged whenever it is used. Step 37 resolves it by secret name.
Rotating the key rewrites every masked value, so rotation is a backfill,
not a config change — the same trade every deterministic-masking scheme
makes.

**Classifying after a load does not rewrite history.** An SCD2 table
whose columns are classified after it has already loaded keeps its
existing versions unmasked: the incoming rows carry the same source
timestamp, so SCD2 correctly refuses to open a version that is not newer,
and the masked values are skipped as late. Masking a table that is
already live is a replay (ADR-016), not a re-run. Classify at
registration time and the question never arises.

**No row-level security.** The plan asked for it on Gold. There is
nowhere to enforce it: Gold is Delta files on local disk, with no serving
engine holding a session identity. Column-level exclusion is what this
architecture can actually honour. RLS arrives with a catalog that has
users — Unity Catalog, Synapse, or a warehouse in front of the lake — and
claiming it before then would be decoration.

**Lineage does not mark masked columns.** The OpenLineage column facet
records the masked column as derived from itself, which is true but
uninformative. A transformation-type facet would say *how*; it is a small
follow-up.

**The seed generator now produces personal data.** `customers` gained
name, email, phone and a CPF-shaped document number, all synthetic and on
reserved `example.com` domains. Without them the feature would be
untestable and the demo would show masking applied to nothing.
