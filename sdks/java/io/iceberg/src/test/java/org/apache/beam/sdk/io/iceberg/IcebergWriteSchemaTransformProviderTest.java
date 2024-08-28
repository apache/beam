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

import static org.apache.beam.sdk.io.iceberg.IcebergWriteSchemaTransformProvider.INPUT_TAG;
import static org.apache.beam.sdk.io.iceberg.IcebergWriteSchemaTransformProvider.OUTPUT_TAG;
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
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.RowFilter;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.hamcrest.Matchers;
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

    TableIdentifier tableId = TableIdentifier.parse(identifier);

    // Create a table and add records to it.
    Table table = warehouse.createTable(tableId, TestFixtures.SCHEMA);

    Map<String, String> properties = new HashMap<>();
    properties.put("type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP);
    properties.put("warehouse", warehouse.location);

    SchemaTransformConfiguration config =
        SchemaTransformConfiguration.builder()
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
            .get(OUTPUT_TAG);

    PAssert.that(result)
        .satisfies(new VerifyOutputs(Collections.singletonList(identifier), "append"));

    testPipeline.run().waitUntilFinish();

    List<Record> writtenRecords = ImmutableList.copyOf(IcebergGenerics.read(table).build());

    assertThat(writtenRecords, Matchers.containsInAnyOrder(TestFixtures.FILE1SNAPSHOT1.toArray()));
  }

  @Test
  public void testWriteUsingManagedTransform() {
    String identifier = "default.table_" + Long.toString(UUID.randomUUID().hashCode(), 16);
    Table table = warehouse.createTable(TableIdentifier.parse(identifier), TestFixtures.SCHEMA);

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
        inputRows.apply(Managed.write(Managed.ICEBERG).withConfig(configMap)).get(OUTPUT_TAG);

    PAssert.that(result)
        .satisfies(new VerifyOutputs(Collections.singletonList(identifier), "append"));

    testPipeline.run().waitUntilFinish();

    List<Record> writtenRecords = ImmutableList.copyOf(IcebergGenerics.read(table).build());
    assertThat(writtenRecords, Matchers.containsInAnyOrder(TestFixtures.FILE1SNAPSHOT1.toArray()));
  }

  @Test
  public void testWriteToDynamicDestinationsUsingManagedTransform() {
    String salt = Long.toString(UUID.randomUUID().hashCode(), 16);

    String identifier0 = "default.table_0_" + salt;
    String identifier1 = "default.table_1_" + salt;
    String identifier2 = "default.table_2_" + salt;
    Table table0 = warehouse.createTable(TableIdentifier.parse(identifier0), TestFixtures.SCHEMA);
    Table table1 = warehouse.createTable(TableIdentifier.parse(identifier1), TestFixtures.SCHEMA);
    Table table2 = warehouse.createTable(TableIdentifier.parse(identifier2), TestFixtures.SCHEMA);

    String identifierTemplate = "default.table_{id}_" + salt;

    Map<String, Object> writeConfig =
        ImmutableMap.<String, Object>builder()
            .put("table", identifierTemplate)
            .put("catalog_name", "test-name")
            .put(
                "catalog_properties",
                ImmutableMap.<String, String>builder()
                    .put("type", "hadoop")
                    .put("warehouse", warehouse.location)
                    .build())
            .build();

    PCollection<Row> inputRows =
        testPipeline
            .apply("Records To Add", Create.of(TestFixtures.asRows(TestFixtures.FILE1SNAPSHOT1)))
            .setRowSchema(IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.SCHEMA));
    PCollection<Row> result =
        inputRows
            .apply(Managed.write(Managed.ICEBERG).withConfig(writeConfig))
            .getSinglePCollection();

    PAssert.that(result)
        .satisfies(
            new VerifyOutputs(Arrays.asList(identifier0, identifier1, identifier2), "append"));

    testPipeline.run().waitUntilFinish();

    List<Record> table0Records = ImmutableList.copyOf(IcebergGenerics.read(table0).build());
    List<Record> table1Records = ImmutableList.copyOf(IcebergGenerics.read(table1).build());
    List<Record> table2Records = ImmutableList.copyOf(IcebergGenerics.read(table2).build());

    assertThat(table0Records, Matchers.contains(TestFixtures.FILE1SNAPSHOT1.get(0)));
    assertThat(table1Records, Matchers.contains(TestFixtures.FILE1SNAPSHOT1.get(1)));
    assertThat(table2Records, Matchers.contains(TestFixtures.FILE1SNAPSHOT1.get(2)));
  }

  /** @param drop if true, will test dropping fields. if false, will test keeping fields */
  private void writeToDynamicDestinationsAndFilter(boolean drop) {
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

    // (drop) we drop these fields from our iceberg table, so we drop them from our input rows
    // (keep) we want to include only these fields in our iceberg table, so we keep them and drop
    // everything else
    List<String> filteredFields = Arrays.asList("id", "nested.long");
    RowFilter filter = new RowFilter(beamSchema);
    filter = drop ? filter.dropping(filteredFields) : filter.keeping(filteredFields);
    org.apache.iceberg.Schema icebergSchema =
        IcebergUtils.beamSchemaToIcebergSchema(filter.outputSchema());

    String identifierTemplate = "default.table_{id}_{nested.str}_" + salt;
    String identifier0 = "default.table_0_x_" + salt;
    String identifier1 = "default.table_1_y_" + salt;
    String identifier2 = "default.table_2_z_" + salt;
    Table table0 = warehouse.createTable(TableIdentifier.parse(identifier0), icebergSchema);
    Table table1 = warehouse.createTable(TableIdentifier.parse(identifier1), icebergSchema);
    Table table2 = warehouse.createTable(TableIdentifier.parse(identifier2), icebergSchema);

    Map<String, Object> writeConfig =
        ImmutableMap.<String, Object>builder()
            .put("table", identifierTemplate)
            .put(drop ? "drop" : "keep", filteredFields)
            .put("catalog_name", "test-name")
            .put(
                "catalog_properties",
                ImmutableMap.<String, String>builder()
                    .put("type", "hadoop")
                    .put("warehouse", warehouse.location)
                    .build())
            .build();

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

    PCollection<Row> inputRows =
        testPipeline.apply("Records To Add", Create.of(rows)).setRowSchema(beamSchema);
    PCollection<Row> result =
        inputRows
            .apply(Managed.write(Managed.ICEBERG).withConfig(writeConfig))
            .getSinglePCollection();

    PAssert.that(result)
        .satisfies(
            new VerifyOutputs(Arrays.asList(identifier0, identifier1, identifier2), "append"));

    testPipeline.run().waitUntilFinish();

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
  public void testWriteToDynamicDestinationsAndDropFields() {
    writeToDynamicDestinationsAndFilter(true);
  }

  @Test
  public void testWriteToDynamicDestinationsAndKeepFields() {
    writeToDynamicDestinationsAndFilter(false);
  }

  private static class VerifyOutputs implements SerializableFunction<Iterable<Row>, Void> {
    private final List<String> tableIds;
    private final String operation;

    public VerifyOutputs(List<String> identifiers, String operation) {
      this.tableIds = identifiers;
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
