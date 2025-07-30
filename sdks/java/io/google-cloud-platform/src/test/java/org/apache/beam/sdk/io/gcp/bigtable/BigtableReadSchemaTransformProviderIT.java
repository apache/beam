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
package org.apache.beam.sdk.io.gcp.bigtable;

import static org.apache.beam.sdk.io.gcp.bigtable.BigtableReadSchemaTransformProvider.CELL_SCHEMA;
import static org.apache.beam.sdk.io.gcp.bigtable.BigtableReadSchemaTransformProvider.FLATTENED_ROW_SCHEMA;
import static org.apache.beam.sdk.io.gcp.bigtable.BigtableReadSchemaTransformProvider.ROW_SCHEMA;
import static org.junit.Assert.assertThrows;

import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableReadSchemaTransformProvider.BigtableReadSchemaTransformConfiguration;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class BigtableReadSchemaTransformProviderIT {
  private static final Logger LOG =
      LoggerFactory.getLogger(BigtableReadSchemaTransformProviderIT.class);

  @Rule public final transient TestPipeline p = TestPipeline.create();

  private static final String COLUMN_FAMILY_NAME_1 = "test_cf_1";
  private static final String COLUMN_FAMILY_NAME_2 = "test_cf_2";
  private BigtableTableAdminClient tableAdminClient;
  private BigtableDataClient dataClient;
  private String tableId;
  private String projectId;
  private String instanceId;

  @Test
  public void testInvalidConfigs() {
    // Properties cannot be empty (project, instance, and table)
    List<BigtableReadSchemaTransformConfiguration.Builder> invalidConfigs =
        Arrays.asList(
            BigtableReadSchemaTransformConfiguration.builder()
                .setProjectId("project")
                .setInstanceId("instance")
                .setTableId(""),
            BigtableReadSchemaTransformConfiguration.builder()
                .setProjectId("")
                .setInstanceId("instance")
                .setTableId("table"),
            BigtableReadSchemaTransformConfiguration.builder()
                .setProjectId("project")
                .setInstanceId("")
                .setTableId("table"));

    for (BigtableReadSchemaTransformConfiguration.Builder config : invalidConfigs) {
      assertThrows(
          IllegalArgumentException.class,
          () -> {
            config.build().validate();
          });
    }
  }

  @Before
  public void setup() throws Exception {
    BigtableTestOptions options =
        TestPipeline.testingPipelineOptions().as(BigtableTestOptions.class);
    projectId = options.as(GcpOptions.class).getProject();
    instanceId = options.getInstanceId();

    BigtableDataSettings settings =
        BigtableDataSettings.newBuilder().setProjectId(projectId).setInstanceId(instanceId).build();
    // Creates a bigtable data client.
    dataClient = BigtableDataClient.create(settings);

    BigtableTableAdminSettings adminSettings =
        BigtableTableAdminSettings.newBuilder()
            .setProjectId(projectId)
            .setInstanceId(instanceId)
            .build();
    tableAdminClient = BigtableTableAdminClient.create(adminSettings);

    tableId = String.format("BTReadSchemaTransformIT-%tF-%<tH-%<tM-%<tS-%<tL", new Date());
    if (!tableAdminClient.exists(tableId)) {
      CreateTableRequest createTableRequest =
          CreateTableRequest.of(tableId)
              .addFamily(COLUMN_FAMILY_NAME_1)
              .addFamily(COLUMN_FAMILY_NAME_2);
      tableAdminClient.createTable(createTableRequest);
    }
  }

  @After
  public void tearDown() {
    try {
      tableAdminClient.deleteTable(tableId);
      LOG.info("Table {} deleted successfully.", tableId);
    } catch (NotFoundException e) {
      LOG.warn("Failed to delete a non-existent table [{}]: \n{}", tableId, e.getMessage());
    }
    dataClient.close();
    tableAdminClient.close();
  }

  @Test
  public void testRead() {
    int numRows = 20;
    List<Row> expectedRows = new ArrayList<>();
    for (int i = 1; i <= numRows; i++) {
      String key = "key" + i;
      byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
      String valueA = "value a" + i;
      byte[] valueABytes = valueA.getBytes(StandardCharsets.UTF_8);
      String valueB = "value b" + i;
      byte[] valueBBytes = valueB.getBytes(StandardCharsets.UTF_8);
      String valueC = "value c" + i;
      byte[] valueCBytes = valueC.getBytes(StandardCharsets.UTF_8);
      String valueD = "value d" + i;
      byte[] valueDBytes = valueD.getBytes(StandardCharsets.UTF_8);
      long timestamp = 1000L * i;

      RowMutation rowMutation =
          RowMutation.create(tableId, key)
              .setCell(COLUMN_FAMILY_NAME_1, "a", timestamp, valueA)
              .setCell(COLUMN_FAMILY_NAME_1, "b", timestamp, valueB)
              .setCell(COLUMN_FAMILY_NAME_2, "c", timestamp, valueC)
              .setCell(COLUMN_FAMILY_NAME_2, "d", timestamp, valueD);
      dataClient.mutateRow(rowMutation);

      // Set up expected Beam Row
      Map<String, List<Row>> columns1 = new HashMap<>();
      columns1.put(
          "a",
          Arrays.asList(
              Row.withSchema(CELL_SCHEMA)
                  .withFieldValue("value", valueABytes)
                  .withFieldValue("timestamp_micros", timestamp)
                  .build()));
      columns1.put(
          "b",
          Arrays.asList(
              Row.withSchema(CELL_SCHEMA)
                  .withFieldValue("value", valueBBytes)
                  .withFieldValue("timestamp_micros", timestamp)
                  .build()));

      Map<String, List<Row>> columns2 = new HashMap<>();
      columns2.put(
          "c",
          Arrays.asList(
              Row.withSchema(CELL_SCHEMA)
                  .withFieldValue("value", valueCBytes)
                  .withFieldValue("timestamp_micros", timestamp)
                  .build()));
      columns2.put(
          "d",
          Arrays.asList(
              Row.withSchema(CELL_SCHEMA)
                  .withFieldValue("value", valueDBytes)
                  .withFieldValue("timestamp_micros", timestamp)
                  .build()));

      Map<String, Map<String, List<Row>>> families = new HashMap<>();
      families.put(COLUMN_FAMILY_NAME_1, columns1);
      families.put(COLUMN_FAMILY_NAME_2, columns2);

      Row expectedRow =
          Row.withSchema(ROW_SCHEMA)
              .withFieldValue("key", keyBytes)
              .withFieldValue("column_families", families)
              .build();

      expectedRows.add(expectedRow);
    }
    LOG.info("Finished writing {} rows to table {}", numRows, tableId);

    BigtableReadSchemaTransformConfiguration config =
        BigtableReadSchemaTransformConfiguration.builder()
            .setTableId(tableId)
            .setInstanceId(instanceId)
            .setProjectId(projectId)
            .setFlatten(false)
            .build();

    SchemaTransform transform = new BigtableReadSchemaTransformProvider().from(config);

    PCollection<Row> rows = PCollectionRowTuple.empty(p).apply(transform).get("output");

    PAssert.that(rows).containsInAnyOrder(expectedRows);
    p.run().waitUntilFinish();
  }

  @Test
  public void testReadFlatten() {
    int numRows = 20;
    List<Row> expectedRows = new ArrayList<>();
    for (int i = 1; i <= numRows; i++) {
      String key = "key" + i;
      byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
      String valueA = "value a" + i;
      byte[] valueABytes = valueA.getBytes(StandardCharsets.UTF_8);
      String valueB = "value b" + i;
      byte[] valueBBytes = valueB.getBytes(StandardCharsets.UTF_8);
      String valueC = "value c" + i;
      byte[] valueCBytes = valueC.getBytes(StandardCharsets.UTF_8);
      String valueD = "value d" + i;
      byte[] valueDBytes = valueD.getBytes(StandardCharsets.UTF_8);
      long timestamp = 1000L * i;

      // Write a row with four distinct columns to Bigtable
      RowMutation rowMutation =
          RowMutation.create(tableId, key)
              .setCell(COLUMN_FAMILY_NAME_1, "a", timestamp, valueA)
              .setCell(COLUMN_FAMILY_NAME_1, "b", timestamp, valueB)
              .setCell(COLUMN_FAMILY_NAME_2, "c", timestamp, valueC)
              .setCell(COLUMN_FAMILY_NAME_2, "d", timestamp, valueD);
      dataClient.mutateRow(rowMutation);

      // For each Bigtable row, we expect four flattened Beam Rows as output.
      // Each Row corresponds to one column.
      expectedRows.add(
          Row.withSchema(FLATTENED_ROW_SCHEMA)
              .withFieldValue("key", keyBytes)
              .withFieldValue("family_name", COLUMN_FAMILY_NAME_1)
              .withFieldValue("column_qualifier", "a".getBytes(StandardCharsets.UTF_8))
              .withFieldValue(
                  "cells",
                  Arrays.asList(
                      Row.withSchema(CELL_SCHEMA)
                          .withFieldValue("value", valueABytes)
                          .withFieldValue("timestamp_micros", timestamp)
                          .build()))
              .build());

      expectedRows.add(
          Row.withSchema(FLATTENED_ROW_SCHEMA)
              .withFieldValue("key", keyBytes)
              .withFieldValue("family_name", COLUMN_FAMILY_NAME_1)
              .withFieldValue("column_qualifier", "b".getBytes(StandardCharsets.UTF_8))
              .withFieldValue(
                  "cells",
                  Arrays.asList(
                      Row.withSchema(CELL_SCHEMA)
                          .withFieldValue("value", valueBBytes)
                          .withFieldValue("timestamp_micros", timestamp)
                          .build()))
              .build());

      expectedRows.add(
          Row.withSchema(FLATTENED_ROW_SCHEMA)
              .withFieldValue("key", keyBytes)
              .withFieldValue("family_name", COLUMN_FAMILY_NAME_2)
              .withFieldValue("column_qualifier", "c".getBytes(StandardCharsets.UTF_8))
              .withFieldValue(
                  "cells",
                  Arrays.asList(
                      Row.withSchema(CELL_SCHEMA)
                          .withFieldValue("value", valueCBytes)
                          .withFieldValue("timestamp_micros", timestamp)
                          .build()))
              .build());

      expectedRows.add(
          Row.withSchema(FLATTENED_ROW_SCHEMA)
              .withFieldValue("key", keyBytes)
              .withFieldValue("family_name", COLUMN_FAMILY_NAME_2)
              .withFieldValue("column_qualifier", "d".getBytes(StandardCharsets.UTF_8))
              .withFieldValue(
                  "cells",
                  Arrays.asList(
                      Row.withSchema(CELL_SCHEMA)
                          .withFieldValue("value", valueDBytes)
                          .withFieldValue("timestamp_micros", timestamp)
                          .build()))
              .build());
    }
    LOG.info("Finished writing {} rows to table {} with Flatten state true", numRows, tableId);

    // Configure the transform to use flatten mode (the default).
    BigtableReadSchemaTransformConfiguration config =
        BigtableReadSchemaTransformConfiguration.builder()
            .setTableId(tableId)
            .setInstanceId(instanceId)
            .setProjectId(projectId)
            .setFlatten(true)
            .build();

    SchemaTransform transform = new BigtableReadSchemaTransformProvider().from(config);

    PCollection<Row> rows = PCollectionRowTuple.empty(p).apply(transform).get("output");

    // Assert that the actual rows match the expected flattened rows.
    PAssert.that(rows).containsInAnyOrder(expectedRows);
    p.run().waitUntilFinish();
  }
}
