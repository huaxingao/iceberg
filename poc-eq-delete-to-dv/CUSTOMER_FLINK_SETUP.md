# Converting equality deletes to deletion vectors in your Flink pipeline

This guide is for customers running **Apache Flink** with **Apache Iceberg** who want to convert
the **equality deletes** their streaming/upsert jobs produce into **deletion vectors (DVs)** on
their table's `main` branch — so that downstream readers (including engines that read DVs but not
equality deletes) see a clean, delete-applied table.

The pattern: write your changes (inserts + equality deletes) to a **staging branch**, run the
`ConvertEqualityDeletes` maintenance task to convert them to DVs on `main`, and let readers read
`main`. Equality deletes never reach `main`.

---

## Prerequisites

- **Apache Flink 2.1.x.** (Iceberg also ships the task for Flink 1.20 and 2.0; use the runtime jar
  that matches your Flink minor version.)
- **An Iceberg table at format-version 3.** Deletion vectors are a v3 feature.
  ```sql
  -- new table
  CREATE TABLE db.t (...) TBLPROPERTIES ('format-version'='3');
  -- or upgrade an existing one
  ALTER TABLE db.t SET TBLPROPERTIES ('format-version'='3');
  ```
- **A catalog** Flink can write through (REST, Hive, Hadoop, Glue, …).

## Step 1 — Add the Iceberg Flink runtime jar

Place `iceberg-flink-runtime-2.1-<version>.jar` on your Flink cluster classpath — drop it in
Flink's `lib/` directory (restart the cluster) or pass it with `--jarfile` when submitting the
job. This jar contains both the `IcebergSink` writer and the `ConvertEqualityDeletes` maintenance
task.

## Step 2 — Write equality deletes to a staging branch

Keep your CDC/upsert writes off `main` by targeting a staging branch. The equality columns you
declare here are the table's key for upserts/deletes.

**DataStream (`IcebergSink`):**

```java
IcebergSink.forRowData(stream)
    .tableLoader(tableLoader)
    .equalityFieldColumns(List.of("id"))   // your upsert/delete key
    .upsert(true)
    .toBranch("staging")                   // write to the staging branch, not main
    .append();
```

**Flink SQL:**

```sql
-- one-time: create the branch
ALTER TABLE db.t CREATE BRANCH staging;

SET 'table.dynamic-table-options.enabled' = 'true';

-- upsert into the staging branch (produces equality deletes on `staging`)
INSERT INTO db.t /*+ OPTIONS('branch'='staging', 'upsert-enabled'='true') */
SELECT * FROM source;
```

At this point `staging` accumulates data files and **equality delete** files. `main` is untouched.

## Step 3 — Schedule the conversion task

Run `ConvertEqualityDeletes` as a long-running Flink job. Each scheduled cycle converts the
staging branch's equality deletes into DVs and commits them to `main`:

```java
import java.time.Duration;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.flink.maintenance.api.ConvertEqualityDeletes;
import org.apache.iceberg.flink.maintenance.api.TableMaintenance;

TableMaintenance.forTable(env, tableLoader, lockFactory)
    .uidSuffix("eq-delete-to-dv")
    .add(
        ConvertEqualityDeletes.builder()
            .scheduleOnInterval(Duration.ofMinutes(10))
            .stagingBranch("staging")
            .targetBranch(SnapshotRef.MAIN_BRANCH)
            .equalityFieldColumns(List.of("id"))   // MUST match Step 2's equalityFieldColumns
            .parallelism(2))
    .append();

env.executeAsync();
```

For production, use a durable `TriggerLockFactory` (e.g. `JdbcLockFactory`) so the task
coordinates safely with other maintenance jobs (like compaction).

## Step 4 — Read `main`

Readers query `main` as usual. After a conversion cycle, `main` contains the new data files plus
**deletion vectors** that apply the deletes — and **no equality deletes**. Deleted rows do not
appear; updated rows reflect their latest value. Readers that support v3 DVs (but not equality
deletes) now read the table correctly.

---

## Things to get right

- **Format v3 is mandatory.** The task rejects v2 tables with a clear error.
- **Equality columns must match.** The `equalityFieldColumns` in Step 3 must be identical to the
  writer's in Step 2, or deletes won't resolve correctly.
- **Staging vs main.** Point your writers at the staging branch and your readers at `main`. The
  task is the bridge; it commits converted results to `main` and leaves the staging branch's
  original equality deletes in place for auditability.
- **Scheduling.** `scheduleOnInterval` controls conversion latency — how soon a staging delete
  shows up as a DV on `main`. Tune to your freshness needs.
- **Monitoring.** The task emits a `TaskResult` per cycle through the maintenance framework; wire
  it into your job metrics to track success/failure and per-cycle work.

## Reference

- Task API: `org.apache.iceberg.flink.maintenance.api.ConvertEqualityDeletes` (Apache Iceberg,
  `flink/v2.1`).
- A runnable end-to-end proof of the staging→main conversion is in `TestEqualityDeleteToDVPoc.java`
  (this bundle).
