# ADR-014: A pluggable engine, with Spark as the second implementation

- **Status:** accepted
- **Date:** 2026-09-27

## Context

ADR-003 chose Arrow and delta-rs and deferred Spark. Deferring was
right: Spark costs a JVM, a cluster and seconds of startup to process
data that fits in memory. But "we could add Spark later" is only a real
claim if there is a seam to add it at, and until now there wasn't.

## Decision — one interface, chosen by configuration

`Engine` has one operation, `latest_per_key`, because that is the
pipeline's heaviest transform and the first thing that stops fitting in
memory. `ENGINE=arrow|spark` picks the implementation; Silver calls
`get_engine()` and knows nothing else.

PySpark is imported lazily inside the factory, so the default install
never touches it and never needs a JVM. That property is tested.

## Decision — correctness is the contract, and it is asserted

Two implementations are worth nothing if they disagree. Parity tests
compare both engines' output on the same input, including composite
keys and ties on the sequence column, and the benchmark refuses to
report timings if the results differ. A fast wrong answer is reported
as wrong.

## Decision — data crosses the boundary as Arrow

Spark 4 accepts a `pyarrow.Table` in `createDataFrame` and returns one
from `toArrow`. No pandas, no temporary files, no Hadoop filesystem.

The first version of this engine round-tripped through Parquet, on the
theory that files are how Spark really receives data. That was wrong
twice. It bought nothing for an in-memory signature — the table was
already in the driver's memory either side. And Spark's Parquet
*writer* goes through Hadoop's `FileOutputCommitter`, which calls
`winutils.exe` to set file permissions and fails outright on Windows
without `HADOOP_HOME` set.

The tests passed on Linux and failed on the developer's Windows
machine. Deleting the write removed a portability bug and a step, and
made the engine faster. Worth recording as the general shape of the
mistake: a design justified by how something *usually* works, rather
than by what this code actually needed.

## Measured

`order_items`, 2,030,000 rows, 7 columns, Linux x86_64, Python 3.12:

| engine | seconds | rows/sec |
|---|---:|---:|
| arrow | 3.13 | 647,850 |
| spark | 29.00 | 69,995 |

**Arrow is 9.3x faster at this size**, and both produce identical rows.

That number is the point, not an embarrassment. Spark's overhead here
is JVM startup and Arrow serialisation to the executors, both largely
fixed and both amortised as data grows. The crossover is where the input stops fitting in memory,
which at this schema is far above two million rows. Reaching for Spark
before then makes a pipeline slower and harder to run, and this table
is the evidence.

## The limitation, observed rather than predicted

On the developer's Windows machine at the default 1g driver heap, the
two-million-row benchmark failed with `TaskResultLost (result lost from
block manager)`. That message never mentions memory, which is why it is
worth writing down: it is the driver being unable to hold the collected
result, which is the limitation described below arriving on schedule.

`spark_driver_memory` (default 2g) is now set through
`SPARK_DRIVER_MEMORY` before the JVM starts — in local mode the builder's
`spark.driver.memory` is read too late to take effect — and
`spark.driver.maxResultSize` is raised to match. The benchmark reports a
Spark failure and still prints the Arrow number, rather than exiting on
a sixty-line traceback.

Raising the heap moves the ceiling. It does not remove it.

## The honest limitation

`latest_per_key` takes and returns an Arrow table, so the **result**
must still fit on one machine even though the computation did not. That
is enough to prove the seam and to benchmark, and not enough to process
more data than the driver can hold.

Removing that ceiling means the Spark engine reading and writing Delta
directly through `delta-spark`, so data never returns to the driver at
all. That is a larger change — Delta's Spark connector, JAR resolution,
and a second write path — and it is not done. Anyone reading this
should know the seam is real and the scaling is not yet.

## Consequences

- `pyspark` is an optional extra. The default install has no JVM
  dependency, and the Spark tests skip rather than fail where there is
  no JVM — the normal state of a machine running the default engine.
- Spark 4 requires Java 17 or later. The error message says so, because
  the failure otherwise surfaces from inside py4j.
- Only `latest_per_key` is pluggable. SCD2, the Gold builders and the
  DQ engine remain Arrow-only, and each would need its own
  implementation before a Spark-first deployment made sense.
