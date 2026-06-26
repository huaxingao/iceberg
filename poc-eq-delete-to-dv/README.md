# Equality-delete → Deletion-vector PoC (OSS Apache Iceberg / Flink)

Proof that the `ConvertEqualityDeletes` Flink maintenance task converts **equality
deletes** written to a **staging branch** into **deletion vectors (DVs) on `main`**, leaving **no
equality deletes** behind — on stock OSS Apache Iceberg.

## Why this is the whole proof for the unmanaged path

An **unmanaged** Iceberg table is format-identical to an OSS Iceberg table at the spec level
(same data files, manifests, delete files, snapshots). Snowflake does not alter the on-disk
Iceberg representation for unmanaged tables. So if the conversion works on OSS Iceberg, it works
on an unmanaged table — **no Snowflake-specific test is needed for this claim.**

The conversion task and its tests are all merged on `apache/iceberg` main and backported to
`flink/v2.1`: #16831, #16844, #16858, #16874, #16889, #16948.

## Hard requirement

The table **must be format-version ≥ 3** — DVs are the v3 representation of positional deletes.
`ConvertEqualityDeletes` rejects v2 tables. `equalityFieldColumns` passed to the task **must
match** the equality columns the writer used for the staging eq-delete files.

## Contents

- **`TestEqualityDeleteToDVPoc.java`** — the runnable proof (a copy of the test that lives in the
  Iceberg tree at `flink/v2.1/flink/src/test/java/org/apache/iceberg/flink/maintenance/api/`).
- **`INVOCATION.md`** — how to build the jar and run the task (operator-facing).
- **`CUSTOMER_FLINK_SETUP.md`** — how a customer consumes this in their own Flink system.

## Build & run the proof

From an `apache/iceberg` checkout on `main` (Java 17):

```bash
# Run the PoC test
./gradlew -DflinkVersions=2.1 :iceberg-flink:iceberg-flink-2.1:test \
  --tests "*TestEqualityDeleteToDVPoc*"

# Build the deliverable runtime jar
./gradlew -DflinkVersions=2.1 :iceberg-flink:iceberg-flink-runtime-2.1:build
# -> flink/v2.1/flink-runtime/build/libs/iceberg-flink-runtime-2.1-*.jar
```

What the test asserts: before — `staging` has the equality delete, `main` has 0 DVs / 0 equality
deletes and reads all rows; after — `main` has 1 DV, **0 equality deletes**, reads the
delete-applied rows, and `staging` still retains its original equality delete.
