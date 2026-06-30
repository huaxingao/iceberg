/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.flink.maintenance.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.maintenance.operator.OperatorTestBase;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.ContentFileUtil;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reproduces the customer-reported "over-delete on single-key upsert" scenario for {@link
 * ConvertEqualityDeletes}.
 *
 * <p>Scenario (maps id->transaction_id, data->unit_price):
 *
 * <ol>
 *   <li>Seed {@code main} with 20 rows.
 *   <li>Fork an {@code audit_branch}.
 *   <li>On the branch only, UPSERT key id=4 in one commit: an equality delete keyed on id=4 ONLY
 *       (single equality column, like the reported transaction_id key) plus the new (4, "30.0")
 *       row. This is what a Flink upsert writer emits.
 *   <li>Run {@code ConvertEqualityDeletes(audit_branch -> main)} with equalityFieldColumns=["id"].
 * </ol>
 *
 * <p>Correct merge-on-read semantics: the eq delete (seq N) removes the OLD id=4 row (seq &lt; N)
 * but NOT the re-inserted id=4 row written in the same commit (seq N). So main should end with 20
 * rows, id=4 -&gt; "30.0", and exactly ONE deletion vector. The reported bug is 2 DVs (the new row
 * is over-deleted) leaving 19 rows.
 */
class TestEqualityDeleteUpsertOverDeletePoc extends OperatorTestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestEqualityDeleteUpsertOverDeletePoc.class);
  private static final String AUDIT_BRANCH = "audit_branch";

  @TempDir private Path tempDir;
  private StreamExecutionEnvironment env;

  @BeforeEach
  public void beforeEach() {
    this.env = StreamExecutionEnvironment.getExecutionEnvironment();
  }

  @Test
  void singleKeyUpsertOnBranchMustNotOverDelete() throws Exception {
    Table table = createTableWithDelete(3); // v3 (DVs), write.upsert.enabled=true, PK = id

    // --- Seed main with 20 rows: id=1..20, "unit_price" in data. id=4 starts at "159.99". ---
    List<Record> seed = Lists.newArrayList();
    for (int id = 1; id <= 20; id++) {
      seed.add(SimpleDataUtil.createRecord(id, id == 4 ? "159.99" : ("p" + id)));
    }
    insert(table, seed);
    table.refresh();

    // --- Fork the audit branch from main. ---
    table.manageSnapshots().createBranch(AUDIT_BRANCH).commit();
    table.refresh();

    // --- Upsert id=4 (159.99 -> 30.0) on the branch in ONE commit: eq-delete(id=4) + new row. ---
    DataFile newRow =
        new GenericAppenderHelper(table, FileFormat.PARQUET, tempDir)
            .writeFile(Lists.newArrayList(SimpleDataUtil.createRecord(4, "30.0")));
    DeleteFile idOnlyDelete = writeIdOnlyEqualityDelete(table, 4);
    table.newRowDelta().addRows(newRow).addDeletes(idOnlyDelete).toBranch(AUDIT_BRANCH).commit();
    table.refresh();

    // Branch sanity: 1 eq delete file, the new row present, key 4 -> 30.0, still 20 logical rows.
    assertThat(equalityDeleteCount(table, AUDIT_BRANCH))
        .as("branch holds exactly one equality delete")
        .isEqualTo(1);
    assertThat(rowCount(table, AUDIT_BRANCH)).as("branch still has 20 logical rows").isEqualTo(20);
    assertThat(priceOf(table, AUDIT_BRANCH, 4)).as("branch id=4 price").isEqualTo("30.0");

    // main untouched so far.
    assertThat(dvCount(table, SnapshotRef.MAIN_BRANCH)).as("main starts with no DVs").isZero();
    assertThat(rowCount(table, SnapshotRef.MAIN_BRANCH)).as("main starts with 20 rows").isEqualTo(20);

    // --- Run the conversion audit_branch -> main, single equality column "id". ---
    TableMaintenance.forTable(env, tableLoader(), LOCK_FACTORY)
        .uidSuffix("UpsertOverDeletePoc")
        .rateLimit(Duration.ofMillis(50))
        .lockCheckDelay(Duration.ofMillis(50))
        .add(
            ConvertEqualityDeletes.builder()
                .scheduleOnInterval(Duration.ofMillis(100))
                .stagingBranch(AUDIT_BRANCH)
                .targetBranch(SnapshotRef.MAIN_BRANCH)
                .equalityFieldColumns(ImmutableList.of("id"))
                .parallelism(2))
        .append();

    JobClient jobClient = null;
    try {
      jobClient = env.executeAsync();

      // Wait until the conversion commits at least one DV to main.
      Awaitility.await("a DV is committed to main")
          .atMost(Duration.ofMinutes(5))
          .pollInterval(Duration.ofMillis(200))
          .untilAsserted(() -> assertThat(dvCount(table, SnapshotRef.MAIN_BRANCH)).isGreaterThan(0));

      // Give a beat for any (erroneous) second DV to also land, then snapshot the state.
      table.refresh();
      Snapshot mainHead = table.snapshot(SnapshotRef.MAIN_BRANCH);
      long dvs = dvCount(table, SnapshotRef.MAIN_BRANCH);
      long rows = rowCount(table, SnapshotRef.MAIN_BRANCH);
      LOG.info("=== POC RESULT === main summary: {}", mainHead.summary());
      LOG.info("=== POC RESULT === main dvCount={} rowCount={}", dvs, rows);
      LOG.info("=== POC RESULT === main id=4 price={}", priceOf(table, SnapshotRef.MAIN_BRANCH, 4));

      // The proof. Correct behavior is exactly one DV and 20 surviving rows with id=4 -> 30.0.
      assertThat(dvCount(table, SnapshotRef.MAIN_BRANCH))
          .as("main must have exactly ONE deletion vector (old id=4 only), not two")
          .isEqualTo(1);
      assertThat(rowCount(table, SnapshotRef.MAIN_BRANCH))
          .as("main must keep all 20 logical rows after conversion")
          .isEqualTo(20);
      assertThat(priceOf(table, SnapshotRef.MAIN_BRANCH, 4))
          .as("id=4 must reflect the upserted price 30.0 on main")
          .isEqualTo("30.0");
      assertThat(equalityDeleteCount(table, SnapshotRef.MAIN_BRANCH))
          .as("main must have zero equality deletes after conversion")
          .isZero();
    } finally {
      closeJobClient(jobClient);
    }
  }

  /** Equality delete keyed on the single column "id" (mirrors a transaction_id-only upsert key). */
  private DeleteFile writeIdOnlyEqualityDelete(Table table, int id) throws IOException {
    File file = File.createTempFile("junit", null, tempDir.toFile());
    assertThat(file.delete()).isTrue();
    Schema idSchema = table.schema().select("id");
    GenericRecord delete = GenericRecord.create(idSchema);
    delete.setField("id", id);
    return FileHelpers.writeDeleteFile(
        table,
        Files.localOutput(file),
        new PartitionData(PartitionSpec.unpartitioned().partitionType()),
        Lists.newArrayList(delete),
        idSchema);
  }

  private static long rowCount(Table table, String ref) throws IOException {
    table.refresh();
    Snapshot snapshot = table.snapshot(ref);
    if (snapshot == null) {
      return 0;
    }

    long count = 0;
    try (CloseableIterable<Record> rows =
        IcebergGenerics.read(table).useSnapshot(snapshot.snapshotId()).build()) {
      for (Record ignored : rows) {
        count++;
      }
    }
    return count;
  }

  private static String priceOf(Table table, String ref, int id) throws IOException {
    table.refresh();
    Snapshot snapshot = table.snapshot(ref);
    if (snapshot == null) {
      return null;
    }

    try (CloseableIterable<Record> rows =
        IcebergGenerics.read(table).useSnapshot(snapshot.snapshotId()).build()) {
      for (Record r : rows) {
        if (id == (Integer) r.getField("id")) {
          return (String) r.getField("data");
        }
      }
    }
    return null;
  }

  private static long dvCount(Table table, String ref) throws IOException {
    return countDeleteFiles(table, ref, ContentFileUtil::isDV);
  }

  private static long equalityDeleteCount(Table table, String ref) throws IOException {
    return countDeleteFiles(table, ref, file -> file.content() == FileContent.EQUALITY_DELETES);
  }

  private static long countDeleteFiles(
      Table table, String ref, java.util.function.Predicate<DeleteFile> predicate)
      throws IOException {
    table.refresh();
    Snapshot snapshot = table.snapshot(ref);
    if (snapshot == null) {
      return 0;
    }

    long count = 0;
    for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
        for (DeleteFile file : reader) {
          if (predicate.test(file)) {
            count++;
          }
        }
      }
    }

    return count;
  }
}
