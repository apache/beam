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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.IcebergScanConfig;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.io.iceberg.TestDataWarehouse;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.joda.time.Instant;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for {@link LocalResolveDoFn}. */
@RunWith(JUnit4.class)
public class LocalResolveDoFnTest {
  private static final org.apache.iceberg.Schema CDC_SCHEMA =
      new org.apache.iceberg.Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "visible", Types.StringType.get()),
              Types.NestedField.optional(3, "hidden", Types.StringType.get())),
          ImmutableSet.of(1));

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");
  @Rule public TestName testName = new TestName();

  @Test
  public void copyOnWriteRewriteOfIdenticalRowsIsDropped() throws Exception {
    TableIdentifier tableId = tableId();
    Table table = warehouse.createTable(tableId, CDC_SCHEMA, null, tableProperties());
    IcebergScanConfig scanConfig = scanConfig(table, tableId);
    DataFile oldFile =
        warehouse.writeRecords(
            testName.getMethodName() + "-old.parquet",
            table.schema(),
            ImmutableList.of(record(1L, "shown", "same-hidden")));
    DataFile newFile =
        warehouse.writeRecords(
            testName.getMethodName() + "-new.parquet",
            table.schema(),
            ImmutableList.of(record(1L, "shown", "same-hidden")));

    List<ValueInSingleWindow<Row>> output =
        process(
            scanConfig,
            descriptor(tableId, 1L, 1L),
            ImmutableList.of(
                task(SerializableChangelogTask.Type.DELETED_FILE, oldFile, table, 300L),
                task(SerializableChangelogTask.Type.ADDED_ROWS, newFile, table, 300L)),
            new Instant(0L));

    assertThat(output, empty());
  }

  @Test
  public void hiddenOnlyUpdateIsResolvedBeforeProjection() throws Exception {
    TableIdentifier tableId = tableId();
    Table table = warehouse.createTable(tableId, CDC_SCHEMA, null, tableProperties());
    IcebergScanConfig scanConfig = scanConfig(table, tableId);
    DataFile oldFile =
        warehouse.writeRecords(
            testName.getMethodName() + "-old.parquet",
            table.schema(),
            ImmutableList.of(record(1L, "shown", "old-hidden")));
    DataFile newFile =
        warehouse.writeRecords(
            testName.getMethodName() + "-new.parquet",
            table.schema(),
            ImmutableList.of(record(1L, "shown", "new-hidden")));
    Instant timestamp = new Instant(1234L);

    List<ValueInSingleWindow<Row>> output =
        process(
            scanConfig,
            descriptor(tableId, 1L, 1L),
            ImmutableList.of(
                task(SerializableChangelogTask.Type.DELETED_FILE, oldFile, table, 301L),
                task(SerializableChangelogTask.Type.ADDED_ROWS, newFile, table, 301L)),
            timestamp);

    assertThat(
        output.stream().map(LocalResolveDoFnTest::kindAndProjectedRow).collect(Collectors.toList()),
        contains("UPDATE_BEFORE:1:shown:2", "UPDATE_AFTER:1:shown:2"));
    assertEquals(
        ImmutableList.of(timestamp, timestamp),
        output.stream().map(ValueInSingleWindow::getTimestamp).collect(Collectors.toList()));
  }

  private List<ValueInSingleWindow<Row>> process(
      IcebergScanConfig scanConfig,
      ChangelogDescriptor descriptor,
      List<SerializableChangelogTask> tasks,
      Instant timestamp)
      throws Exception {
    try (DoFnTester<KV<ChangelogDescriptor, List<SerializableChangelogTask>>, Row> tester =
        DoFnTester.of(new LocalResolveDoFn(scanConfig))) {
      tester.processTimestampedElement(TimestampedValue.of(KV.of(descriptor, tasks), timestamp));
      return tester.getMutableOutput(tester.getMainOutputTag());
    }
  }

  private TableIdentifier tableId() {
    return TableIdentifier.of("default", testName.getMethodName());
  }

  private IcebergScanConfig scanConfig(Table table, TableIdentifier tableId) {
    return IcebergScanConfig.builder()
        .setCatalogConfig(
            IcebergCatalogConfig.builder()
                .setCatalogName("name")
                .setCatalogProperties(
                    ImmutableMap.of("type", "hadoop", "warehouse", warehouse.location))
                .build())
        .setTableIdentifier(tableId)
        .setSchema(IcebergUtils.icebergSchemaToBeamSchema(table.schema()))
        .setKeepFields(ImmutableList.of("id", "visible"))
        .setUseCdc(true)
        .build();
  }

  private static ChangelogDescriptor descriptor(
      TableIdentifier tableId, long lowerInclusive, long upperInclusive) {
    org.apache.beam.sdk.schemas.Schema pkSchema =
        org.apache.beam.sdk.schemas.Schema.builder().addInt64Field("id").build();
    return ChangelogDescriptor.builder()
        .setTableIdentifierString(tableId.toString())
        .setSnapshotSequenceNumber(1)
        .setCommitSnapshotId(1)
        .setOverlapLower(Row.withSchema(pkSchema).addValue(lowerInclusive).build())
        .setOverlapUpper(Row.withSchema(pkSchema).addValue(upperInclusive).build())
        .build();
  }

  private static Record record(long id, String visible, String hidden) {
    GenericRecord record = GenericRecord.create(CDC_SCHEMA);
    record.setField("id", id);
    record.setField("visible", visible);
    record.setField("hidden", hidden);
    return record;
  }

  private static SerializableChangelogTask task(
      SerializableChangelogTask.Type type, DataFile dataFile, Table table, long snapshotId) {
    return SerializableChangelogTask.builder()
        .setType(type)
        .setDataFile(dataFile, table.spec().partitionToPath(dataFile.partition()), true)
        .setAddedDeletes(ImmutableList.of())
        .setExistingDeletes(ImmutableList.of())
        .setSpecId(table.spec().specId())
        .setOperation(
            type == SerializableChangelogTask.Type.ADDED_ROWS
                ? ChangelogOperation.INSERT
                : ChangelogOperation.DELETE)
        .setOrdinal(0)
        .setCommitSnapshotId(snapshotId)
        .setStart(0L)
        .setLength(dataFile.fileSizeInBytes())
        .setJsonExpression(ExpressionParser.toJson(Expressions.alwaysTrue()))
        .build();
  }

  private static Map<String, String> tableProperties() {
    return ImmutableMap.of(TableProperties.FORMAT_VERSION, "2");
  }

  private static String kindAndProjectedRow(ValueInSingleWindow<Row> value) {
    ValueKind kind = value.getValueKind();
    Row row = value.getValue();
    return kind.name()
        + ":"
        + row.getInt64("id")
        + ":"
        + row.getString("visible")
        + ":"
        + row.getSchema().getFieldCount();
  }
}
