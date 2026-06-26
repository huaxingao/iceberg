# Invocation guide — `ConvertEqualityDeletes` (staging → main)

Operator-facing instructions for building the Iceberg Flink runtime jar and running the
equality-delete → deletion-vector (DV) conversion task. Hand this to whoever wires the task into
the Flink/Snowflake PoC.

---

## At a glance — what running this on your side requires

The jar ships the `ConvertEqualityDeletes` classes, but the jar alone does not run the
conversion. Before it works end-to-end you need all of the following:

| # | Item | Why |
|---|------|-----|
| 1 | **A Java job that submits the task** | `ConvertEqualityDeletes` is a **DataStream** task — there is **no SQL statement or `CALL` procedure** for it. A SQL-only harness (`sql-client.sh`) cannot trigger it. You must build the `TableMaintenance` + `ConvertEqualityDeletes` pipeline (see §3) and submit it as a Java job to the Flink JobManager. |
| 2 | **Table at format-version 3** | The task hard-fails on v2. |
| 3 | **`equalityFieldColumns` matching the writer** | Must equal the equality columns used to write the staging eq-delete files. |
| 4 | **Engine reads DVs on `main`** | The reader validating the result must support v3 deletion vectors (for the Snowflake consumer this is a feature-flag-gated capability). |

Item **1 is the one most likely to surprise a SQL-driven test harness** — plan for a small Java
entrypoint, not a SQL call.

---

## 1. Prerequisites

- **Apache Flink 2.1.x** runtime (the task lives in Iceberg's `flink/v2.1` module).
- **Iceberg table at format-version ≥ 3.** The task hard-fails on v2:
  `ConvertEqualityDeletes requires table format version >= 3 (DVs) …`. Upgrade with
  `ALTER TABLE … SET TBLPROPERTIES ('format-version'='3')` (or create the table at v3).
- **A staging branch** that holds the data files + **equality delete** files (this is what a
  Flink upsert/delete writer produces). The task reads it; it does not create it.
- **`equalityFieldColumns` that match the writer.** The columns you pass to the task must be the
  same equality columns the writer used to produce the staging eq-delete files. The partition
  source columns of an equality delete's spec must be a subset of these columns (Flink's
  `IcebergSink` already guarantees this).

## 2. Build the runtime jar

From an `apache/iceberg` checkout on `main` (Java 17):

```bash
./gradlew -DflinkVersions=2.1 :iceberg-flink:iceberg-flink-runtime-2.1:build
```

Artifact:

```
flink/v2.1/flink-runtime/build/libs/iceberg-flink-runtime-2.1-<version>.jar
```

Put this jar on the Flink job classpath (e.g. Flink `lib/`, or `--jarfile`). It is the
**deliverable** for the PoC.

## 3. Invoke the task

`ConvertEqualityDeletes` is a **DataStream maintenance task**, not a SQL statement. You drive it
through the Iceberg `TableMaintenance` framework:

```java
import java.time.Duration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.ConvertEqualityDeletes;
import org.apache.iceberg.flink.maintenance.api.TableMaintenance;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableIdentifier);

TableMaintenance.forTable(env, tableLoader)
    .uidSuffix("eq-delete-to-dv")
    .add(
        ConvertEqualityDeletes.builder()
            .scheduleOnInterval(Duration.ofMinutes(10)) // how often to run a conversion cycle
            .stagingBranch("staging")                   // branch holding eq-deletes
            .targetBranch(SnapshotRef.MAIN_BRANCH)      // where DVs get committed (default: main)
            .equalityFieldColumns(ImmutableList.of("id", "data")) // MUST match the writer
            .parallelism(2))
    .append();

env.executeAsync(); // long-running streaming job; runs a cycle per schedule trigger
```

Each trigger runs one conversion cycle: it reads the staging branch's equality deletes, builds a
primary-key index, resolves each equality delete to row positions, writes Puffin **DVs**, and
commits the data files + DVs to the **target branch**. The staging branch keeps its original
equality deletes (for transparency); main ends up with DVs and no equality deletes.

## 4. Parameter reference

| Builder method | Required | Meaning |
|---|---|---|
| `stagingBranch(String)` | **yes** | Branch holding the data + equality-delete files to convert. |
| `targetBranch(String)` | no (default `main`) | Branch the converted data files + DVs are committed to. |
| `equalityFieldColumns(List<String>)` | **yes** | Equality columns; must match the writer's. |
| `scheduleOnInterval(Duration)` | recommended | Run a conversion cycle every interval. |
| `parallelism(int)` | no | Parallelism of the reader/index/writer stages. |
| `uidSuffix(String)` | recommended | Stable operator UIDs for savepoint/restore. |

`stagingBranch` may equal `targetBranch` (convert in place on one branch) — the task handles that
case explicitly.

## 5. How to verify it worked

Check the **target branch head snapshot**:

- It contains **deletion vectors** — delete files where `ContentFileUtil.isDV(file)` is true.
- It contains **zero equality-delete files** — no delete file with
  `content() == FileContent.EQUALITY_DELETES`.
- Reading the branch returns the **delete-applied** rows (deleted rows are gone).

The runnable proof of exactly these checks is `TestEqualityDeleteToDVPoc.java` (in this bundle and
in the Iceberg tree). Run it with:

```bash
./gradlew -DflinkVersions=2.1 :iceberg-flink:iceberg-flink-2.1:test --tests "*TestEqualityDeleteToDVPoc*"
```
