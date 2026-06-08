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
package org.apache.beam.sdk.io.iceberg.cdc;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.IcebergIO.ReadRows.StartingStrategy;
import org.apache.beam.sdk.io.iceberg.IcebergScanConfig;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.io.iceberg.TestDataWarehouse;
import org.apache.beam.sdk.io.iceberg.TestFixtures;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.values.OutputBuilder;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link WatchForSnapshotsSdf}. */
@RunWith(JUnit4.class)
public class WatchForSnapshotsSdfTest {
  private static final Schema CDC_SCHEMA =
      new Schema(
          ImmutableList.of(
              required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get())),
          ImmutableSet.of(1));

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");
  @Rule public TestName testName = new TestName();

  @Test
  public void earliestStreamingRestrictionEmitsSnapshotsInSequenceOrder() throws Exception {
    TableIdentifier tableId = tableId();
    Table table = warehouse.createTable(tableId, CDC_SCHEMA, null, tableProperties());
    commitAppend(table, "s1.parquet", records("one", 1L));
    commitAppend(table, "s2.parquet", records("two", 2L));
    commitAppend(table, "s3.parquet", records("three", 3L));
    List<Snapshot> snapshots = Lists.newArrayList(table.snapshots());

    WatchForSnapshotsSdf sdf =
        new WatchForSnapshotsSdf(
            scanConfigBuilder(table, tableId)
                .setStreaming(true)
                .setStartingStrategy(StartingStrategy.EARLIEST)
                .setPollInterval(Duration.millis(1L))
                .build());

    OffsetRange restriction = sdf.initialRestriction();
    assertEquals(1L, restriction.getFrom());
    assertEquals(Long.MAX_VALUE, restriction.getTo());

    CapturingOutputReceiver out = new CapturingOutputReceiver();
    ManualWatermarkEstimator<Instant> watermark =
        sdf.newWatermarkEstimator(sdf.initialWatermarkState());
    DoFn.ProcessContinuation continuation =
        sdf.process(sdf.newTracker(restriction), watermark, out);

    Long[] expectedSnapshotIds = snapshots.stream().map(Snapshot::snapshotId).toArray(Long[]::new);
    List<Long> actualSnapshotIds =
        out.values.stream().map(TimestampedValue::getValue).collect(Collectors.toList());
    assertTrue(continuation.shouldResume());
    assertEquals(Duration.millis(1L), continuation.resumeDelay());
    assertThat(actualSnapshotIds, contains(expectedSnapshotIds));
    assertEquals(
        Instant.ofEpochMilli(snapshots.get(snapshots.size() - 1).timestampMillis()),
        watermark.currentWatermark());
  }

  @Test
  public void boundedSnapshotRangeUsesInclusiveLowerAndExclusiveUpperSequence() throws Exception {
    TableIdentifier tableId = tableId();
    Table table = warehouse.createTable(tableId, CDC_SCHEMA, null, tableProperties());
    commitAppend(table, "s1.parquet", records("one", 1L));
    commitAppend(table, "s2.parquet", records("two", 2L));
    commitAppend(table, "s3.parquet", records("three", 3L));
    commitAppend(table, "s4.parquet", records("four", 4L));
    List<Snapshot> snapshots = Lists.newArrayList(table.snapshots());
    Snapshot second = snapshots.get(1);
    Snapshot third = snapshots.get(2);

    WatchForSnapshotsSdf sdf =
        new WatchForSnapshotsSdf(
            scanConfigBuilder(table, tableId)
                .setStreaming(true)
                .setFromSnapshotInclusive(second.snapshotId())
                .setToSnapshot(third.snapshotId()) // this is inclusive
                .setPollInterval(Duration.standardSeconds(30L))
                .build());

    OffsetRange restriction = sdf.initialRestriction();
    assertEquals(second.sequenceNumber(), restriction.getFrom());
    assertEquals(third.sequenceNumber() + 1, restriction.getTo());

    CapturingOutputReceiver out = new CapturingOutputReceiver();
    DoFn.ProcessContinuation continuation =
        sdf.process(
            sdf.newTracker(restriction),
            sdf.newWatermarkEstimator(sdf.initialWatermarkState()),
            out);

    List<Long> outputSnapshotIds =
        out.values.stream().map(TimestampedValue::getValue).collect(Collectors.toList());

    assertFalse(continuation.shouldResume());
    assertThat(outputSnapshotIds, contains(second.snapshotId(), third.snapshotId()));
    assertEquals(2, out.values.size());
    assertEquals(Instant.ofEpochMilli(second.timestampMillis()), out.values.get(0).getTimestamp());
    assertEquals(Instant.ofEpochMilli(third.timestampMillis()), out.values.get(1).getTimestamp());
  }

  @Test
  public void streamingDefaultStartsAtLatestSnapshotAndEarliestStartsAtFirst() throws Exception {
    TableIdentifier tableId = tableId();
    Table table = warehouse.createTable(tableId, CDC_SCHEMA, null, tableProperties());
    commitAppend(table, "s1.parquet", records("one", 1L));
    commitAppend(table, "s2.parquet", records("two", 2L));
    Snapshot latest = table.currentSnapshot();

    WatchForSnapshotsSdf defaultSdf =
        new WatchForSnapshotsSdf(
            scanConfigBuilder(table, tableId)
                .setStreaming(true)
                .setPollInterval(Duration.standardSeconds(1L))
                .build());
    WatchForSnapshotsSdf earliestSdf =
        new WatchForSnapshotsSdf(
            scanConfigBuilder(table, tableId)
                .setStreaming(true)
                .setStartingStrategy(StartingStrategy.EARLIEST)
                .setPollInterval(Duration.standardSeconds(1L))
                .build());

    assertEquals(latest.sequenceNumber(), defaultSdf.initialRestriction().getFrom());
    assertEquals(1L, earliestSdf.initialRestriction().getFrom());
  }

  @Test
  public void emptyTableReturnsResumeAndAdvancesIdleWatermark() {
    TableIdentifier tableId = tableId();
    Table table = warehouse.createTable(tableId, CDC_SCHEMA, null, tableProperties());
    WatchForSnapshotsSdf sdf =
        new WatchForSnapshotsSdf(
            scanConfigBuilder(table, tableId)
                .setStreaming(true)
                .setMaxSnapshotDiscoveryDelay(Duration.ZERO)
                .setPollInterval(Duration.millis(25L))
                .build());
    ManualWatermarkEstimator<Instant> watermark =
        sdf.newWatermarkEstimator(sdf.initialWatermarkState());
    Instant beforeProcess = Instant.now();
    CapturingOutputReceiver out = new CapturingOutputReceiver();

    DoFn.ProcessContinuation continuation =
        sdf.process(sdf.newTracker(sdf.initialRestriction()), watermark, out);

    assertTrue(continuation.shouldResume());
    assertEquals(Duration.millis(25L), continuation.resumeDelay());
    assertThat(out.values, empty());
    assertThat(watermark.currentWatermark(), greaterThan(beforeProcess.minus(Duration.millis(1L))));
    assertThat(watermark.currentWatermark(), lessThanOrEqualTo(Instant.now()));
  }

  private TableIdentifier tableId() {
    return TableIdentifier.of("default", testName.getMethodName());
  }

  private IcebergScanConfig.Builder scanConfigBuilder(Table table, TableIdentifier tableId) {
    return IcebergScanConfig.builder()
        .setCatalogConfig(
            IcebergCatalogConfig.builder()
                .setCatalogName("name")
                .setCatalogProperties(
                    ImmutableMap.of("type", "hadoop", "warehouse", warehouse.location))
                .build())
        .setTableIdentifier(tableId)
        .setSchema(IcebergUtils.icebergSchemaToBeamSchema(table.schema()))
        .setUseCdc(true);
  }

  private static Map<String, String> tableProperties() {
    return ImmutableMap.of(TableProperties.FORMAT_VERSION, "2");
  }

  private void commitAppend(Table table, String fileName, List<Record> records) throws IOException {
    DataFile file =
        warehouse.writeRecords(testName.getMethodName() + "-" + fileName, table.schema(), records);
    table.newFastAppend().appendFile(file).commit();
    table.refresh();
  }

  private static List<Record> records(String data, long... ids) {
    ImmutableList.Builder<Record> records = ImmutableList.builder();
    for (long id : ids) {
      records.add(TestFixtures.createRecord(CDC_SCHEMA, ImmutableMap.of("id", id, "data", data)));
    }
    return records.build();
  }

  private static final class CapturingOutputReceiver implements DoFn.OutputReceiver<Long> {
    private final ImmutableList.Builder<TimestampedValue<Long>> builder = ImmutableList.builder();
    private List<TimestampedValue<Long>> values = ImmutableList.of();

    @Override
    public OutputBuilder<Long> builder(Long value) {
      throw new UnsupportedOperationException("Use outputWithTimestamp in this test receiver.");
    }

    @Override
    public void outputWithTimestamp(Long value, Instant timestamp) {
      builder.add(TimestampedValue.of(value, timestamp));
      values = builder.build();
    }
  }
}
