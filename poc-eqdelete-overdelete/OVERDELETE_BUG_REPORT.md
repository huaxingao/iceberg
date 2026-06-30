# Bug: `ConvertEqualityDeletes` over-deletes a re-inserted key on a separate staging branch

**Component:** Apache Iceberg — Flink maintenance task `ConvertEqualityDeletes` (`flink/v2.1`)
**Reproduced on:** apache/iceberg `main` @ `8c1ee9d`, 2026-06-30
**Severity:** Data loss / silent incorrect results on `main` after conversion
**Related PRs:** #16831, #16844, #16858, #16874, #16889, #16948, #16979

---

## Summary

When an upsert (single-key equality delete + re-insert of the same key) is committed on a
**separate staging branch** and then converted to deletion vectors onto `main` via
`ConvertEqualityDeletes`, the freshly re-inserted row is **also** deleted. The key disappears
from `main` entirely, even though the staging branch reads correctly.

This is an upsert workload, which is the **only** way a real Flink job produces single-key
equality deletes (CDC / `upsert-kafka` → `BaseDeltaTaskWriter.deleteKey()`). Flink SQL
`DELETE FROM` is *not* implemented by the Iceberg connector (`IcebergTableSink` declares only
`SupportsPartitioning, SupportsOverwrite`, not `SupportsRowLevelDelete`), so upsert is the
realistic customer path — and it is the path that triggers this bug.

---

## Symptom (reproduction)

Table with PK `transaction_id`, format-version 3.

**Start — `main`, 3 rows:**

| transaction_id | product    | price | file         |
|----------------|------------|-------|--------------|
| 1              | Laptop     | 999   | seed.parquet |
| 2              | Mouse      | 25    | seed.parquet |
| 4              | Headphones | 160   | seed.parquet |

**On `audit_branch` — upsert tx=4 price 160 → 30** (one row-delta commit: eq-delete on
`transaction_id = 4` + new row `(4, Headphones, 30)` in `new.parquet`).

**Branch read — correct, 3 rows:**

| transaction_id | price | live? |
|----------------|-------|-------|
| 1              | 999   | ✓     |
| 2              | 25    | ✓     |
| 4              | 30    | ✓ (updated) |

**After `ConvertEqualityDeletes(audit_branch → main)` with `equalityFieldColumns=["transaction_id"]` — BUG:**

| file         | row              | DV applied? | live?     |
|--------------|------------------|-------------|-----------|
| seed.parquet | tx=4, price 160  | ✓ delete    | ✗ (correct) |
| new.parquet  | tx=4, price 30   | ✓ delete    | ✗ **wrong** |

**`main` read: 2 rows (expected 3); tx=4 is gone.** Observed counters: `added-dvs=2`,
`added-position-deletes=2`, `dvCount=2`.

(Original PoC scale: 20 seed rows, id=4 price 159.99→30.0, scan returns 19 instead of 20.)

---

## Root cause — NOT "naive delete-by-key"

A natural reading of the symptom is *"the converter sees eq-delete key tx=4 and marks every
matching row deleted, ignoring sequence numbers."* **That is not what happens**, and stating it
that way will mislead the fix: a single conversion pass is **correct**.

There are two coupled defects; the damage comes from their interaction.

### Defect #1 (primary) — the planner re-converts the same staging snapshot forever

On the separate-branch path the planner never records that the upsert staging snapshot has been
converted, so it re-converts it on **every** scheduler cycle (observed ~739× in one run, yielding
the 2nd DV).

Chain of events:
- `EqualityConvertPlanner.ensureIndexCurrent()` early-returns whenever
  `lastMainSnapshotId == currentMainSnapshotId`
  (`EqualityConvertPlanner.java:285`).
- The planner's view of `main` (`table.snapshot("main")`) keeps returning the *seed* snapshot
  every cycle, so `discoverLastCommittedWork` runs exactly **once** — on cycle 1, before any
  conversion commit existed, when `main` had no committer marker → `lastCommittedStaging = null`.
- `lastStagingSnapshotId` therefore stays `null` forever, so
  `nextUnprocessedStagingSnapshot` falls back to `stopAt = findCommonAncestor(stagingHead, main)`
  (the fork point, `EqualityConvertPlanner.java:415`), and the upsert snapshot is *always*
  classified as "unprocessed."
- The planner never picks up its own `equality-convert-staging-snapshot` committer marker.

Cycle 1 in isolation produces exactly **1 DV** and the correct result. The over-delete is the
product of repeated re-conversion, not of a single pass.

### Defect #2 (secondary) — sequence gate is disabled on the separate-branch path

`EqualityConvertPKIndex.resolveDeletes()` applies the data-sequence gate
(*keep rows whose `dataSequenceNumber() >= deleteSeq`*) **only** when `stagingOnTargetBranch`:

```java
// EqualityConvertPKIndex.java:267
if (stagingOnTargetBranch && deleteSeq != null && pos.dataSequenceNumber() >= deleteSeq) {
  // skip — the row is newer than the delete, so it survives
}
```

On cycle 1 the re-inserted row (data seq 2) is not yet indexed as a position, so it survives
regardless. But once defect #1 re-resolves the delete on a later cycle, the now-indexed
re-inserted row (seq 2) is no longer protected — because the gate is off for the
separate-branch case — and gets a DV. That is the 2nd DV that deletes `(4, 30)`.

### Accurate one-liner

> The converter does respect sequence numbers on a single pass. The bug is that the planner
> re-converts the same staging snapshot indefinitely, and on those repeat passes the sequence
> gate that should protect the re-inserted row is disabled for the separate-branch case.

---

## Fix direction

1. **Primary — stop re-processing on separate branches.** Advance the planner's view of `main`
   and honor the `equality-convert-staging-snapshot` committer marker so a converted staging
   snapshot is not re-processed every cycle. (`ensureIndexCurrent` must observe the conversion
   commit on `main`, or `nextUnprocessedStagingSnapshot` must consult the marker rather than
   falling back to the fork point.)
2. **Secondary / backstop — sequence gate.** Consider applying the `dataSequenceNumber >= deleteSeq`
   gate on the separate-branch path too. *Caveat:* the unit test
   `deletesHigherSequenceWhenStagingNotOnTargetBranch` currently encodes the gate-OFF behavior,
   and the committer reassigns data sequence numbers, so enabling the gate globally needs care —
   it should be a defense-in-depth backstop, not the primary fix.

---

## Coverage gap that let this ship

The existing tests (`TestConvertEqualityDeletes`) fabricate equality-delete files via
`FileHelpers.writeDeleteFile(...)` using the **full row schema** (`eqDeleteSchema = table.schema()`),
then commit them with `table.newRowDelta().addDeletes(...)`. No real Flink ingest path produces
a full-row-keyed equality delete — both upsert mode and CDC retract streams key on the
identifier/PK columns only. The single-key (`transaction_id` only) equality delete that a real
upsert produces was never exercised, and the prior PoC (`TestEqualityDeleteToDVPoc`) never
re-inserted the deleted key. Recommend an upsert-driven E2E test that produces eq-deletes through
the real writer (`FlinkSink`/`IcebergSink` with `upsert-enabled=true`) rather than hand-built
delete files.

---

## Repro test

`flink/v2.1/flink/src/test/java/org/apache/iceberg/flink/maintenance/api/TestEqualityDeleteUpsertOverDeletePoc.java`
(copy in `/home/hgao/eqdelete-dv-poc/`). The single-key eq-delete via a projected `id`-only
schema is essential — keying on the full `(id, data)` schema hides the bug.

**Env note:** Flink MiniCluster tests fail with `UnknownHostException: <hostname>` until the
hostname is mapped: `sudo sh -c 'echo "127.0.0.1 $(hostname)" >> /etc/hosts'`.
