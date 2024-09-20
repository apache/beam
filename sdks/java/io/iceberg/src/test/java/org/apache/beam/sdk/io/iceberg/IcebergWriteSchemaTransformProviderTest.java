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
import static org.apache.beam.sdk.io.iceberg.IcebergWriteSchemaTransformProvider.OUTPUT_TAG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
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
            .get(OUTPUT_TAG);

    PAssert.that(result).satisfies(new VerifyOutputs(identifier, "append"));

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

    PAssert.that(result).satisfies(new VerifyOutputs(identifier, "append"));

    testPipeline.run().waitUntilFinish();

    List<Record> writtenRecords = ImmutableList.copyOf(IcebergGenerics.read(table).build());
    assertThat(writtenRecords, Matchers.containsInAnyOrder(TestFixtures.FILE1SNAPSHOT1.toArray()));
  }

  private static class VerifyOutputs implements SerializableFunction<Iterable<Row>, Void> {
    private final String tableId;
    private final String operation;

    public VerifyOutputs(String identifier, String operation) {
      this.tableId = identifier;
      this.operation = operation;
    }

    @Override
    public Void apply(Iterable<Row> input) {
      Row row = input.iterator().next();

      assertEquals(tableId, row.getString("table"));
      assertEquals(operation, row.getString("operation"));
      return null;
    }
  }
}
