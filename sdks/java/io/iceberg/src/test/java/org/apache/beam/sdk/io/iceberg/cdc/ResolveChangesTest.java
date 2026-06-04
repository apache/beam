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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.IcebergScanConfig;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.io.iceberg.TestDataWarehouse;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.joda.time.Instant;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link ResolveChanges}. */
@RunWith(JUnit4.class)
public class ResolveChangesTest {
  private static final org.apache.iceberg.Schema SIMPLE_ICEBERG_SCHEMA =
      new org.apache.iceberg.Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.IntegerType.get()),
              Types.NestedField.optional(2, "data", Types.StringType.get())),
          ImmutableSet.of(1));
  private static final Schema SIMPLE_BEAM_SCHEMA =
      IcebergUtils.icebergSchemaToBeamSchema(SIMPLE_ICEBERG_SCHEMA);
  private static final Schema PK_SCHEMA = Schema.builder().addInt32Field("id").build();

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");
  @Rule public final TestName testName = new TestName();

  @Test
  public void fullRowDuplicateDeleteInsertEmitsNothing() throws Exception {
    Row duplicate = simpleRow(1, "duplicate");

    List<ValueInSingleWindow<Row>> output =
        process(
            SIMPLE_ICEBERG_SCHEMA,
            pkRow(1),
            Collections.singletonList(duplicate),
            Collections.singletonList(duplicate),
            new Instant(0L));

    assertThat(output, empty());
  }

  @Test
  public void updatePairAndExtraRowsPreserveKindsAndTimestamp() throws Exception {
    Instant timestamp = new Instant(123L);
    Row before = simpleRow(1, "old");
    Row after = simpleRow(1, "new");
    Row extraDelete = simpleRow(1, "deleted-only");

    List<ValueInSingleWindow<Row>> output =
        process(
            SIMPLE_ICEBERG_SCHEMA,
            pkRow(1),
            Arrays.asList(before, extraDelete),
            Collections.singletonList(after),
            timestamp);

    assertThat(
        output.stream().map(ResolveChangesTest::kindAndData).collect(Collectors.toList()),
        contains("UPDATE_BEFORE:old", "UPDATE_AFTER:new", "DELETE:deleted-only"));
    assertEquals(
        Collections.nCopies(3, timestamp),
        output.stream().map(ValueInSingleWindow::getTimestamp).collect(Collectors.toList()));
  }

  @Test
  public void duplicateDetectionUsesDeepEqualityForNestedValues() throws Exception {
    org.apache.iceberg.Schema icebergSchema =
        new org.apache.iceberg.Schema(
            ImmutableList.of(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(
                    2,
                    "nested",
                    Types.StructType.of(
                        Types.NestedField.optional(3, "name", Types.StringType.get()))),
                Types.NestedField.optional(
                    4, "items", Types.ListType.ofOptional(5, Types.StringType.get())),
                Types.NestedField.optional(
                    6,
                    "attrs",
                    Types.MapType.ofOptional(
                        7, 8, Types.StringType.get(), Types.IntegerType.get())),
                Types.NestedField.optional(9, "payload", Types.BinaryType.get()),
                Types.NestedField.optional(10, "nullable", Types.StringType.get())),
            ImmutableSet.of(1));
    Schema beamSchema = IcebergUtils.icebergSchemaToBeamSchema(icebergSchema);
    Schema nestedSchema = beamSchema.getField("nested").getType().getRowSchema();
    Row delete =
        Row.withSchema(beamSchema)
            .addValues(
                1,
                Row.withSchema(nestedSchema).addValue("same").build(),
                ImmutableList.of("a", "b"),
                ImmutableMap.of("x", 1),
                new byte[] {1, 2, 3},
                null)
            .build();
    Row insert =
        Row.withSchema(beamSchema)
            .addValues(
                1,
                Row.withSchema(nestedSchema).addValue("same").build(),
                ImmutableList.of("a", "b"),
                ImmutableMap.of("x", 1),
                new byte[] {1, 2, 3},
                null)
            .build();

    List<ValueInSingleWindow<Row>> output =
        process(
            icebergSchema,
            pkRow(1),
            Collections.singletonList(delete),
            Collections.singletonList(insert),
            new Instant(0L));

    assertThat(output, empty());
  }

  private List<ValueInSingleWindow<Row>> process(
      org.apache.iceberg.Schema icebergSchema,
      Row pk,
      List<Row> deletes,
      List<Row> inserts,
      Instant timestamp)
      throws Exception {
    CoGbkResult result =
        CoGbkResult.of(ResolveChanges.DELETES, deletes).and(ResolveChanges.INSERTS, inserts);
    try (DoFnTester<KV<CdcRowDescriptor, CoGbkResult>, Row> tester =
        DoFnTester.of(new ResolveChanges(scanConfig(icebergSchema)))) {
      tester.processTimestampedElement(
          TimestampedValue.of(
              KV.of(
                  CdcRowDescriptor.builder()
                      .setCommitSnapshotId(123)
                      .setSnapshotSequenceNumber(456)
                      .setPrimaryKey(pk)
                      .build(),
                  result),
              timestamp));
      return tester.getMutableOutput(tester.getMainOutputTag());
    }
  }

  private IcebergScanConfig scanConfig(org.apache.iceberg.Schema icebergSchema) {
    TableIdentifier tableId = TableIdentifier.of("default", testName.getMethodName());
    IcebergCatalogConfig catalogConfig =
        IcebergCatalogConfig.builder()
            .setCatalogProperties(
                ImmutableMap.of("type", "hadoop", "warehouse", warehouse.location))
            .build();
    catalogConfig.catalog().createTable(tableId, icebergSchema);
    return IcebergScanConfig.builder()
        .setCatalogConfig(catalogConfig)
        .setTableIdentifier(tableId)
        .setSchema(IcebergUtils.icebergSchemaToBeamSchema(icebergSchema))
        .setUseCdc(true)
        .build();
  }

  private static Row simpleRow(int id, String data) {
    return Row.withSchema(SIMPLE_BEAM_SCHEMA).addValues(id, data).build();
  }

  private static Row pkRow(int id) {
    return Row.withSchema(PK_SCHEMA).addValue(id).build();
  }

  private static String kindAndData(ValueInSingleWindow<Row> value) {
    ValueKind kind = value.getValueKind();
    return kind.name() + ":" + value.getValue().getString("data");
  }
}
