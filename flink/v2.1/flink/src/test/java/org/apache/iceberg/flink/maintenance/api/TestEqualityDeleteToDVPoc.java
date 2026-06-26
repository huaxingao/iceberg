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

import static org.apache.iceberg.flink.SimpleDataUtil.createRecord;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.maintenance.operator.OperatorTestBase;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.ContentFileUtil;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * PoC: prove that the {@link ConvertEqualityDeletes} Flink maintenance task converts
 * equality deletes written to a <b>staging branch</b> into <b>deletion vectors (DVs) on {@code
 * main}</b>, leaving <b>no equality deletes</b> behind, on stock OSS Apache Iceberg.
 *
 * <p>This is the unmanaged-path proof for the customer PoC. An unmanaged Iceberg table is
 * format-identical to OSS Iceberg at the spec level, so a proof against a plain OSS table (this
 * test) carries over directly to the unmanaged case — no Snowflake-specific test is required for
 * this claim. The Snowflake side (refresh {@code main}, read the DVs, validate queries) is owned
 * separately.
 *
 * <p>The flow mirrors the upstream {@code TestConvertEqualityDeletesE2E} (known-good API usage)
 * but asserts the scope claims explicitly:
 *
 * <ol>
 *   <li><b>Before:</b> {@code staging} holds equality delete file(s); {@code main} has 0 DVs and 0
 *       equality deletes and still reads every row.
 *   <li><b>Run</b> {@code ConvertEqualityDeletes} (staging → main) through the maintenance
 *       framework.
 *   <li><b>After:</b> {@code main} has a DV, <b>zero</b> equality deletes, and reads back the
 *       delete-applied rows; {@code staging} still retains its original equality deletes for
 *       transparency.
 * </ol>
 */
class TestEqualityDeleteToDVPoc extends OperatorTestBase {
  private static final String STAGING_BRANCH = "staging";

  @TempDir private Path tempDir;
  private StreamExecutionEnvironment env;

  @BeforeEach
  public void beforeEach() {
    this.env = StreamExecutionEnvironment.getExecutionEnvironment();
  }

  @Test
  void equalityDeletesOnStagingBecomeDeletionVectorsOnMain() throws Exception {
    // --- Setup: a format-v3 table (DVs require v3). createTableWithDelete(3) sets
    // format-version=3 and write.upsert.enabled=true, schema (id INT, data STRING). ---
    Table table = createTableWithDelete(3);

    // Seed main with three rows: (1,a), (2,b), (3,c). main is append-only, no deletes yet.
    insert(table, 1, "a");
    insert(table, 2, "b");
    insert(table, 3, "c");
    table.refresh();

    // Fork the staging branch from main's current snapshot.
    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    // Write an EQUALITY delete for (1,a) and commit it to staging only (delete-only snapshot).
    // This is what a Flink upsert/delete writer produces on the staging branch.
    DeleteFile equalityDelete = writeEqualityDelete(table, 1, "a");
    table.newRowDelta().addDeletes(equalityDelete).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    // --- Before: prove the starting state. ---
    // staging carries the equality delete...
    assertThat(equalityDeleteCount(table, STAGING_BRANCH))
        .as("staging branch should hold the equality delete")
        .isGreaterThan(0);
    // ...and main is still untouched: no DVs, no equality deletes, all 3 rows visible.
    assertThat(dvCount(table, SnapshotRef.MAIN_BRANCH)).as("main starts with no DVs").isZero();
    assertThat(equalityDeleteCount(table, SnapshotRef.MAIN_BRANCH))
        .as("main starts with no equality deletes")
        .isZero();
    SimpleDataUtil.assertTableRecords(
        table, ImmutableList.of(createRecord(1, "a"), createRecord(2, "b"), createRecord(3, "c")));

    // --- Run the conversion: staging -> main, through the maintenance framework. ---
    // equalityFieldColumns MUST match the writer's equality columns (here the full PK: id, data).
    TableMaintenance.forTable(env, tableLoader(), LOCK_FACTORY)
        .uidSuffix("EqDeleteToDVPoc")
        .rateLimit(Duration.ofMillis(50))
        .lockCheckDelay(Duration.ofMillis(50))
        .add(
            ConvertEqualityDeletes.builder()
                .scheduleOnInterval(Duration.ofMillis(100))
                .stagingBranch(STAGING_BRANCH)
                .targetBranch(SnapshotRef.MAIN_BRANCH)
                .equalityFieldColumns(ImmutableList.of("id", "data"))
                .parallelism(2))
        .append();

    JobClient jobClient = null;
    try {
      jobClient = env.executeAsync();

      // --- After: the proof. main gets exactly one DV from the converted equality delete. ---
      Awaitility.await("a DV is committed to main")
          .atMost(Duration.ofMinutes(5))
          .pollInterval(Duration.ofMillis(200))
          .untilAsserted(() -> assertThat(dvCount(table, SnapshotRef.MAIN_BRANCH)).isEqualTo(1));

      table.refresh();

      // Headline claim: NO equality deletes remain on main (it converted to a DV, not copied).
      assertThat(equalityDeleteCount(table, SnapshotRef.MAIN_BRANCH))
          .as("main must have zero equality deletes after conversion")
          .isZero();

      // The delete is actually applied on main: row (1,a) is gone, (2,b) and (3,c) remain.
      SimpleDataUtil.assertTableRecords(
          table, ImmutableList.of(createRecord(2, "b"), createRecord(3, "c")));

      // Transparency: staging keeps its original equality delete (the task does not rewrite it).
      assertThat(equalityDeleteCount(table, STAGING_BRANCH))
          .as("staging retains its original equality delete")
          .isGreaterThan(0);
    } finally {
      closeJobClient(jobClient);
    }
  }

  /** Write an equality delete file for a single (id, data) row, keyed on the full schema. */
  private DeleteFile writeEqualityDelete(Table table, Integer id, String data) throws IOException {
    File file = File.createTempFile("junit", null, tempDir.toFile());
    assertThat(file.delete()).isTrue();
    return FileHelpers.writeDeleteFile(
        table,
        Files.localOutput(file),
        new PartitionData(PartitionSpec.unpartitioned().partitionType()),
        Lists.newArrayList(SimpleDataUtil.createRecord(id, data)),
        table.schema());
  }

  /** Count Puffin deletion-vector files on the given branch head. */
  private static long dvCount(Table table, String ref) throws IOException {
    return countDeleteFiles(table, ref, ContentFileUtil::isDV);
  }

  /** Count classic equality-delete files on the given branch head. */
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
