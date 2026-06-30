# For Max — `ConvertEqualityDeletes` over-delete on a separate staging branch

Full writeup + repro tables: `OVERDELETE_BUG_REPORT.md` (same dir). This is the condensed version.

**Repro on `main` @ `8c1ee9d`:** upsert a single key on a separate staging branch (eq-delete on
PK + re-insert same key, one row-delta commit), then `ConvertEqualityDeletes(staging → main)`.
Branch reads correctly; **`main` loses the key entirely** (re-inserted row also gets a DV).
Counters: `added-dvs=2`, `dvCount=2`. This is the upsert path — the only way a real Flink job
makes single-key eq-deletes (no `SupportsRowLevelDelete` in the connector, so SQL `DELETE` is out).

## Root cause — it's NOT naive delete-by-key

A single conversion pass is **correct** — it honors the data-seq gate, so the re-inserted row
(seq 2) survives the eq-delete (seq 1). The damage is two coupled defects:

1. **Primary — planner re-converts the same staging snapshot every cycle (~739×).**
   `ensureIndexCurrent()` early-returns while `lastMainSnapshotId == currentMainSnapshotId`
   (`EqualityConvertPlanner.java:285`). On the separate-branch path the planner's
   `table.snapshot("main")` keeps returning the seed snapshot, so `discoverLastCommittedWork`
   runs only on cycle 1 (no marker yet → `lastCommittedStaging=null`). `lastStagingSnapshotId`
   stays null, so `nextUnprocessedStagingSnapshot` falls back to `findCommonAncestor` (fork point,
   `:415`) and treats the upsert snapshot as unprocessed forever. The planner never picks up its
   own `equality-convert-staging-snapshot` committer marker.

2. **Secondary — seq gate is off on the separate-branch path.**
   `EqualityConvertPKIndex.resolveDeletes()` applies `dataSequenceNumber() >= deleteSeq` **only**
   when `stagingOnTargetBranch` (`EqualityConvertPKIndex.java:267`). So when defect #1 re-resolves
   the delete on a later cycle, the now-indexed re-inserted row (seq 2) is unprotected → 2nd DV →
   row deleted.

**One-liner:** the converter *does* respect sequence numbers on a single pass; the bug is that
the planner re-converts the same staging snapshot indefinitely, and on the repeat passes the seq
gate that should protect the re-inserted row is disabled for the separate-branch case.

## Fix direction

- **Primary:** stop re-processing on separate branches — advance the planner's view of `main` /
  honor the committer marker so a converted staging snapshot isn't re-picked every cycle.
- **Backstop:** consider applying the seq gate on the separate-branch path too — but
  `deletesHigherSequenceWhenStagingNotOnTargetBranch` encodes the current gate-off behavior and
  the committer reassigns data seq numbers, so this needs care; treat as defense-in-depth, not the
  primary fix.

## Coverage gap

Existing tests fabricate eq-deletes with `FileHelpers.writeDeleteFile(...)` using the **full row
schema**. No real ingest path (upsert or CDC retract) produces a full-row-keyed eq-delete — both
key on PK/identifier columns only. The single-key case was never exercised. Suggest an
upsert-driven E2E (`FlinkSink`/`IcebergSink` with `upsert-enabled=true`) instead of hand-built
delete files.

## Repro test

`flink/v2.1/.../maintenance/api/TestEqualityDeleteUpsertOverDeletePoc.java`. The projected
`id`-only eq-delete schema is essential — full `(id, data)` keying hides the bug.
(MiniCluster: map hostname in `/etc/hosts` or it throws `UnknownHostException`.)
