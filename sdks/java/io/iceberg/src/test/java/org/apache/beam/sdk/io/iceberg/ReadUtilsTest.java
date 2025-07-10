/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.iceberg;

import static org.apache.beam.sdk.io.iceberg.IcebergIO.ReadRows.StartingStrategy.EARLIEST;
import static org.apache.beam.sdk.io.iceberg.IcebergIO.ReadRows.StartingStrategy.LATEST;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.ParquetReader;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

/** Test class for {@link ReadUtils}. */
public class ReadUtilsTest {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");
  @Rule public TestName testName = new TestName();

  @Test
  public void testCreateReader() throws IOException {
    TableIdentifier tableId = TableIdentifier.of("default", testName.getMethodName());
    Table simpleTable = warehouse.createTable(tableId, TestFixtures.SCHEMA);

    Map<String, List<Record>> data =
        ImmutableMap.<String, List<Record>>builder()
            .put("files1s1.parquet", TestFixtures.FILE1SNAPSHOT1)
            .put("file2s1.parquet", TestFixtures.FILE2SNAPSHOT1)
            .put("file3s1.parquet", TestFixtures.FILE3SNAPSHOT1)
            .build();

    for (Map.Entry<String, List<Record>> entry : data.entrySet()) {
      simpleTable
          .newFastAppend()
          .appendFile(
              warehouse.writeRecords(entry.getKey(), simpleTable.schema(), entry.getValue()))
          .commit();
    }

    int numFiles = 0;
    try (CloseableIterable<CombinedScanTask> iterable = simpleTable.newScan().planTasks()) {
      for (CombinedScanTask combinedScanTask : iterable) {
        for (FileScanTask fileScanTask : combinedScanTask.tasks()) {
          String fileName = Iterables.getLast(Splitter.on("/").split(fileScanTask.file().path()));
          List<Record> recordsRead = new ArrayList<>();
          try (ParquetReader<Record> reader =
              ReadUtils.createReader(fileScanTask, simpleTable, simpleTable.schema())) {
            reader.forEach(recordsRead::add);
          }

          assertEquals(data.get(fileName), recordsRead);
          numFiles++;
        }
      }
    }
    assertEquals(data.size(), numFiles);
  }

  @Test
  public void testSnapshotsBetween() throws IOException {
    TableIdentifier tableId = TableIdentifier.of("default", testName.getMethodName());
    Table simpleTable = warehouse.createTable(tableId, TestFixtures.SCHEMA);

    Map<String, List<Record>> data =
        ImmutableMap.<String, List<Record>>builder()
            .put("files1s1.parquet", TestFixtures.FILE1SNAPSHOT1)
            .put("file2s2.parquet", TestFixtures.FILE2SNAPSHOT2)
            .put("file3s3.parquet", TestFixtures.FILE3SNAPSHOT3)
            .build();

    for (Map.Entry<String, List<Record>> entry : data.entrySet()) {
      simpleTable
          .newFastAppend()
          .appendFile(
              warehouse.writeRecords(entry.getKey(), simpleTable.schema(), entry.getValue()))
          .commit();
    }

    List<Snapshot> originalSnapshots = Lists.newArrayList(simpleTable.snapshots());
    List<SnapshotInfo> snapshotsBetween =
        ReadUtils.snapshotsBetween(
            simpleTable, tableId.toString(), null, simpleTable.currentSnapshot().snapshotId());

    assertEquals("size", originalSnapshots.size(), snapshotsBetween.size());
    assertEquals(
        "snapshot id out of order",
        originalSnapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toList()),
        snapshotsBetween.stream().map(SnapshotInfo::getSnapshotId).collect(Collectors.toList()));
    assertEquals(
        "sequence number out of order",
        originalSnapshots.stream().map(Snapshot::sequenceNumber).collect(Collectors.toList()),
        snapshotsBetween.stream()
            .map(SnapshotInfo::getSequenceNumber)
            .collect(Collectors.toList()));
  }

  @Test
  public void testResolveFromSnapshotExclusive() throws IOException {
    TableIdentifier tableId = TableIdentifier.of("default", testName.getMethodName());
    Table simpleTable = warehouse.createTable(tableId, TestFixtures.SCHEMA);

    // creates 4 snapshots
    warehouse.commitData(simpleTable);
    List<Snapshot> snapshots =
        Lists.newArrayList(simpleTable.snapshots()).stream()
            .sorted(Comparator.comparingLong(Snapshot::sequenceNumber))
            .collect(Collectors.toList());
    ;
    Snapshot earliest = snapshots.get(0);
    Snapshot third = snapshots.get(2);
    Snapshot latest = snapshots.get(3);

    IcebergScanConfig baseScanConfig =
        IcebergScanConfig.builder()
            .setCatalogConfig(IcebergCatalogConfig.builder().build())
            .setScanType(IcebergScanConfig.ScanType.TABLE)
            .setTableIdentifier(tableId)
            .setSchema(IcebergUtils.icebergSchemaToBeamSchema(simpleTable.schema()))
            .build();
    IcebergScanConfig streamingScanConfig = baseScanConfig.toBuilder().setStreaming(true).build();

    // Note that the incremental append scan is configured with an _exclusive_ starting snapshot,
    // so we always return the _parent_ of the target snapshot.
    List<TestCase> scanConfigCases =
        Arrays.asList(
            // batch
            TestCase.of(baseScanConfig, earliest.parentId(), "default batch read"),
            TestCase.of(
                baseScanConfig.toBuilder().setFromSnapshotInclusive(third.snapshotId()).build(),
                third.parentId(),
                "batch with from_snapshot"),
            TestCase.of(
                baseScanConfig.toBuilder().setFromTimestamp(third.timestampMillis()).build(),
                third.parentId(),
                "batch with from_timestamp"),
            TestCase.of(
                baseScanConfig.toBuilder().setStartingStrategy(EARLIEST).build(),
                earliest.parentId(),
                "batch with starting_strategy=earliest"),
            TestCase.of(
                baseScanConfig.toBuilder().setStartingStrategy(LATEST).build(),
                latest.parentId(),
                "batch with starting_strategy=latest"),
            // streaming
            TestCase.of(streamingScanConfig, latest.parentId(), "default streaming read"),
            TestCase.of(
                streamingScanConfig
                    .toBuilder()
                    .setFromSnapshotInclusive(third.snapshotId())
                    .build(),
                third.parentId(),
                "streaming with from_snapshot"),
            TestCase.of(
                streamingScanConfig.toBuilder().setFromTimestamp(third.timestampMillis()).build(),
                third.parentId(),
                "streaming with from_timestamp"),
            TestCase.of(
                streamingScanConfig.toBuilder().setStartingStrategy(EARLIEST).build(),
                earliest.parentId(),
                "streaming with starting_strategy=earliest"),
            TestCase.of(
                streamingScanConfig.toBuilder().setStartingStrategy(LATEST).build(),
                latest.parentId(),
                "streaming with starting_strategy=latest"));

    List<String> errors = new ArrayList<>();
    for (TestCase testCase : scanConfigCases) {
      @Nullable
      Long snapshotId = ReadUtils.getFromSnapshotExclusive(simpleTable, testCase.scanConfig);
      if (!Objects.equals(testCase.expectedSnapshotId, snapshotId)) {
        errors.add(
            String.format(
                "\t%s: expected %s but got %s",
                testCase.description, testCase.expectedSnapshotId, snapshotId));
      }
    }
    if (!errors.isEmpty()) {
      List<Long> snapshotIds =
          snapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toList());
      throw new RuntimeException(
          "Errors for table with snapshots "
              + snapshotIds
              + ". Test cases that failed:\n"
              + String.join("\n", errors));
    }
  }

  static class TestCase {
    IcebergScanConfig scanConfig;
    @Nullable Long expectedSnapshotId;
    String description;

    TestCase(IcebergScanConfig scanConfig, @Nullable Long expectedSnapshotId, String description) {
      this.scanConfig = scanConfig;
      this.expectedSnapshotId = expectedSnapshotId;
      this.description = description;
    }

    static TestCase of(
        IcebergScanConfig scanConfig, @Nullable Long expectedSnapshotId, String description) {
      return new TestCase(scanConfig, expectedSnapshotId, description);
    }
  }
}
