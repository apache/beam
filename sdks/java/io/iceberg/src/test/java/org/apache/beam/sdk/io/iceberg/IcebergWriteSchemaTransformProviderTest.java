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

import static org.apache.beam.sdk.io.iceberg.IcebergWriteSchemaTransformProvider.Configuration;
import static org.apache.beam.sdk.io.iceberg.IcebergWriteSchemaTransformProvider.INPUT_TAG;
import static org.apache.beam.sdk.io.iceberg.IcebergWriteSchemaTransformProvider.SNAPSHOTS_TAG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.RowFilter;
import org.apache.beam.sdk.util.RowStringInterpolator;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.yaml.snakeyaml.Yaml;

@RunWith(JUnit4.class)
public class IcebergWriteSchemaTransformProviderTest {

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule
  public transient TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  @Rule public transient TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void testBuildTransformWithRow() {
    Map<String, String> properties = new HashMap<>();
    properties.put("type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP);
    properties.put("warehouse", "test_location");

    Row transformConfigRow =
        Row.withSchema(new IcebergWriteSchemaTransformProvider().configurationSchema())
            .withFieldValue("table", "test_table_identifier")
            .withFieldValue("catalog_name", "test-name")
            .withFieldValue("catalog_properties", properties)
            .build();

    new IcebergWriteSchemaTransformProvider().from(transformConfigRow);
  }

  @Test
  public void testSimpleAppend() {
    String identifier = "default.table_" + Long.toString(UUID.randomUUID().hashCode(), 16);

    Map<String, String> properties = new HashMap<>();
    properties.put("type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP);
    properties.put("warehouse", warehouse.location);

    Configuration config =
        Configuration.builder()
            .setTable(identifier)
            .setCatalogName("name")
            .setCatalogProperties(properties)
            .build();

    PCollectionRowTuple input =
        PCollectionRowTuple.of(
            INPUT_TAG,
            testPipeline
                .apply(
                    "Records To Add", Create.of(TestFixtures.asRows(TestFixtures.FILE1SNAPSHOT1)))
                .setRowSchema(IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.SCHEMA)));

    PCollection<Row> result =
        input
            .apply("Append To Table", new IcebergWriteSchemaTransformProvider().from(config))
            .get(SNAPSHOTS_TAG);

    PAssert.that(result)
        .satisfies(new VerifyOutputs(Collections.singletonList(identifier), "append"));

    testPipeline.run().waitUntilFinish();

    TableIdentifier tableId = TableIdentifier.parse(identifier);
    Table table = warehouse.loadTable(tableId);

    List<Record> writtenRecords = ImmutableList.copyOf(IcebergGenerics.read(table).build());

    assertThat(writtenRecords, Matchers.containsInAnyOrder(TestFixtures.FILE1SNAPSHOT1.toArray()));
  }

  @Test
  public void testWriteUsingManagedTransform() {
    String identifier = "default.table_" + Long.toString(UUID.randomUUID().hashCode(), 16);

    String yamlConfig =
        String.format(
            "table: %s\n"
                + "catalog_name: test-name\n"
                + "catalog_properties: \n"
                + "  type: %s\n"
                + "  warehouse: %s",
            identifier, CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP, warehouse.location);
    Map<String, Object> configMap = new Yaml().load(yamlConfig);

    PCollection<Row> inputRows =
        testPipeline
            .apply("Records To Add", Create.of(TestFixtures.asRows(TestFixtures.FILE1SNAPSHOT1)))
            .setRowSchema(IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.SCHEMA));
    PCollection<Row> result =
        inputRows.apply(Managed.write(Managed.ICEBERG).withConfig(configMap)).get(SNAPSHOTS_TAG);

    PAssert.that(result)
        .satisfies(new VerifyOutputs(Collections.singletonList(identifier), "append"));

    testPipeline.run().waitUntilFinish();

    Table table = warehouse.loadTable(TableIdentifier.parse(identifier));
    List<Record> writtenRecords = ImmutableList.copyOf(IcebergGenerics.read(table).build());
    assertThat(writtenRecords, Matchers.containsInAnyOrder(TestFixtures.FILE1SNAPSHOT1.toArray()));
  }

  /**
   * @param operation if null, just perform a normal dynamic destination write test; otherwise,
   *     performs a simple filter on the record before writing. Valid options are "keep", "drop",
   *     and "only"
   */
  private void writeToDynamicDestinationsAndFilter(@Nullable String operation, boolean streaming) {
    String salt = Long.toString(UUID.randomUUID().hashCode(), 16);

    Schema nestedSchema =
        Schema.builder().addNullableStringField("str").addInt64Field("long").build();
    Schema beamSchema =
        Schema.builder()
            .addNullableInt32Field("id")
            .addStringField("name")
            .addFloatField("cost")
            .addRowField("nested", nestedSchema)
            .build();

    String destinationTemplate = "default.table_{id}_{name}_";
    // for streaming, test substitution works for windowing
    if (streaming) {
      destinationTemplate += "{$DD}_";
    }
    destinationTemplate += salt;

    Map<String, Object> writeConfig =
        new HashMap<>(
            ImmutableMap.<String, Object>builder()
                .put("table", destinationTemplate)
                .put("catalog_name", "test-name")
                .put(
                    "catalog_properties",
                    ImmutableMap.<String, String>builder()
                        .put("type", "hadoop")
                        .put("warehouse", warehouse.location)
                        .build())
                .build());

    if (streaming) {
      writeConfig.put("triggering_frequency_seconds", 100);
    }

    // (drop) we drop these fields from our iceberg table, so we drop them from our input rows
    // (keep) we want to include only these fields in our iceberg table, so we keep them and drop
    // everything else
    // (only) we unnest and write this single record field.
    List<String> filteredFields = Arrays.asList("nested", "id");
    RowFilter filter = new RowFilter(beamSchema);
    if (operation != null) {
      switch (operation) {
        case "drop":
          filter = filter.drop(filteredFields);
          writeConfig.put(operation, filteredFields);
          break;
        case "keep":
          filter = filter.keep(filteredFields);
          writeConfig.put(operation, filteredFields);
          break;
        case "only":
          filter = filter.only(filteredFields.get(0));
          writeConfig.put(operation, filteredFields.get(0));
          break;
        default:
          throw new UnsupportedOperationException("Unknown operation: " + operation);
      }
    }

    List<Row> rows =
        Arrays.asList(
            Row.withSchema(beamSchema)
                .addValues(0, "a", 1.23f, Row.withSchema(nestedSchema).addValues("x", 1L).build())
                .build(),
            Row.withSchema(beamSchema)
                .addValues(1, "b", 4.56f, Row.withSchema(nestedSchema).addValues("y", 2L).build())
                .build(),
            Row.withSchema(beamSchema)
                .addValues(2, "c", 7.89f, Row.withSchema(nestedSchema).addValues("z", 3L).build())
                .build());

    // use interpolator to fetch destinations identifiers. create iceberg tables beforehand
    RowStringInterpolator interpolator = new RowStringInterpolator(destinationTemplate, beamSchema);
    Instant first = new Instant(0);
    Instant second = first.plus(Duration.standardDays(1));
    Instant third = second.plus(Duration.standardDays(1));
    String identifier0 =
        interpolator.interpolate(
            ValueInSingleWindow.of(rows.get(0), first, GlobalWindow.INSTANCE, PaneInfo.NO_FIRING));
    String identifier1 =
        interpolator.interpolate(
            ValueInSingleWindow.of(rows.get(1), second, GlobalWindow.INSTANCE, PaneInfo.NO_FIRING));
    String identifier2 =
        interpolator.interpolate(
            ValueInSingleWindow.of(rows.get(2), third, GlobalWindow.INSTANCE, PaneInfo.NO_FIRING));

    org.apache.iceberg.Schema icebergSchema =
        IcebergUtils.beamSchemaToIcebergSchema(filter.outputSchema());

    TestStream<Row> stream =
        TestStream.create(beamSchema)
            .advanceWatermarkTo(first)
            .addElements(rows.get(0))
            .advanceProcessingTime(Duration.standardDays(1))
            .advanceWatermarkTo(second)
            .addElements(rows.get(1))
            .advanceProcessingTime(Duration.standardDays(1))
            .advanceWatermarkTo(third)
            .addElements(rows.get(2))
            .advanceProcessingTime(Duration.standardDays(1))
            .advanceWatermarkToInfinity();

    PCollection<Row> inputRows;
    if (streaming) {
      inputRows =
          testPipeline
              .apply(stream)
              .apply(
                  Window.<Row>into(FixedWindows.of(Duration.standardMinutes(5)))
                      .accumulatingFiredPanes());
    } else {
      inputRows = testPipeline.apply(Create.of(rows).withRowSchema(beamSchema));
    }

    PCollection<Row> result =
        inputRows
            .apply("Write records", Managed.write(Managed.ICEBERG).withConfig(writeConfig))
            .getSinglePCollection();

    PAssert.that(result)
        .satisfies(
            new VerifyOutputs(Arrays.asList(identifier0, identifier1, identifier2), "append"));

    testPipeline.run().waitUntilFinish();

    Table table0 = warehouse.loadTable(TableIdentifier.parse(identifier0));
    Table table1 = warehouse.loadTable(TableIdentifier.parse(identifier1));
    Table table2 = warehouse.loadTable(TableIdentifier.parse(identifier2));
    List<Record> table0Records = ImmutableList.copyOf(IcebergGenerics.read(table0).build());
    List<Record> table1Records = ImmutableList.copyOf(IcebergGenerics.read(table1).build());
    List<Record> table2Records = ImmutableList.copyOf(IcebergGenerics.read(table2).build());

    assertThat(
        table0Records,
        Matchers.contains(
            IcebergUtils.beamRowToIcebergRecord(icebergSchema, filter.filter(rows.get(0)))));
    assertThat(
        table1Records,
        Matchers.contains(
            IcebergUtils.beamRowToIcebergRecord(icebergSchema, filter.filter(rows.get(1)))));
    assertThat(
        table2Records,
        Matchers.contains(
            IcebergUtils.beamRowToIcebergRecord(icebergSchema, filter.filter(rows.get(2)))));
  }

  @Test
  public void testWriteToDynamicDestinations() {
    writeToDynamicDestinationsAndFilter(null, false);
  }

  @Test
  public void testWriteToDynamicDestinationsAndDropFields() {
    writeToDynamicDestinationsAndFilter("drop", false);
  }

  @Test
  public void testWriteToDynamicDestinationsAndKeepFields() {
    writeToDynamicDestinationsAndFilter("keep", false);
  }

  @Test
  public void testWriteToDynamicDestinationsAndWriteOnlyRecord() {
    writeToDynamicDestinationsAndFilter("only", false);
  }

  @Test
  public void testStreamToDynamicDestinationsAndKeepFields() {
    writeToDynamicDestinationsAndFilter("keep", true);
  }

  private static class VerifyOutputs implements SerializableFunction<Iterable<Row>, Void> {
    private final List<String> tableIds;
    private final String operation;

    public VerifyOutputs(List<String> identifier, String operation) {
      this.tableIds = identifier;
      this.operation = operation;
    }

    @Override
    public Void apply(Iterable<Row> input) {
      Row row = input.iterator().next();

      assertThat(tableIds, Matchers.hasItem(row.getString("table")));
      assertEquals(operation, row.getString("operation"));
      return null;
    }
  }
}
