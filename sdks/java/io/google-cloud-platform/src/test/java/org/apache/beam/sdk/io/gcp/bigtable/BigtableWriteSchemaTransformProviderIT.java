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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableWriteSchemaTransformProvider.BigtableWriteSchemaTransformConfiguration;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Longs;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BigtableWriteSchemaTransformProviderIT {
  @Rule public final transient TestPipeline p = TestPipeline.create();

  private static final String COLUMN_FAMILY_NAME_1 = "test_cf_1";
  private static final String COLUMN_FAMILY_NAME_2 = "test_cf_2";
  private BigtableTableAdminClient tableAdminClient;
  private BigtableDataClient dataClient;
  private String tableId = String.format("BigtableWriteIT-%tF-%<tH-%<tM-%<tS-%<tL", new Date());
  private String projectId;
  private String instanceId;
  private PTransform<PCollectionRowTuple, PCollectionRowTuple> writeTransform;
  private static final Schema SCHEMA =
      Schema.builder()
          .addByteArrayField("key")
          .addArrayField(
              "mutations", Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.BYTES))
          .build();

  @Test
  public void testInvalidConfigs() {
    System.out.println(writeTransform.getName());
    // Properties cannot be empty (project, instance, and table)
    List<BigtableWriteSchemaTransformConfiguration.Builder> invalidConfigs =
        Arrays.asList(
            BigtableWriteSchemaTransformConfiguration.builder()
                .setProjectId("project")
                .setInstanceId("instance")
                .setTableId(""),
            BigtableWriteSchemaTransformConfiguration.builder()
                .setProjectId("")
                .setInstanceId("instance")
                .setTableId("table"),
            BigtableWriteSchemaTransformConfiguration.builder()
                .setProjectId("project")
                .setInstanceId("")
                .setTableId("table"));

    for (BigtableWriteSchemaTransformConfiguration.Builder config : invalidConfigs) {
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

    // set up the table with some pre-written rows to test our mutations on.
    // each test is independent of the others
    if (!tableAdminClient.exists(tableId)) {
      CreateTableRequest createTableRequest =
          CreateTableRequest.of(tableId)
              .addFamily(COLUMN_FAMILY_NAME_1)
              .addFamily(COLUMN_FAMILY_NAME_2);
      tableAdminClient.createTable(createTableRequest);
    }

    BigtableWriteSchemaTransformConfiguration config =
        BigtableWriteSchemaTransformConfiguration.builder()
            .setProjectId(projectId)
            .setInstanceId(instanceId)
            .setTableId(tableId)
            .build();
    writeTransform = new BigtableWriteSchemaTransformProvider().from(config);
  }

  @After
  public void tearDown() {
    try {
      tableAdminClient.deleteTable(tableId);
      System.out.printf("Table %s deleted successfully%n", tableId);
    } catch (NotFoundException e) {
      System.err.println("Failed to delete a non-existent table: " + e.getMessage());
    }
    dataClient.close();
    tableAdminClient.close();
  }

  @Test
  public void testSetMutationsExistingColumn() {
    RowMutation rowMutation =
        RowMutation.create(tableId, "key-1")
            .setCell(COLUMN_FAMILY_NAME_1, "col_a", "val-1-a")
            .setCell(COLUMN_FAMILY_NAME_2, "col_c", "val-1-c");
    dataClient.mutateRow(rowMutation);

    List<Map<String, byte[]>> mutations = new ArrayList<>();
    // mutation to set cell in an existing column
    mutations.add(
        ImmutableMap.of(
            "type", "SetCell".getBytes(StandardCharsets.UTF_8),
            "value", "new-val-1-a".getBytes(StandardCharsets.UTF_8),
            "column_qualifier", "col_a".getBytes(StandardCharsets.UTF_8),
            "family_name", COLUMN_FAMILY_NAME_1.getBytes(StandardCharsets.UTF_8)));
    mutations.add(
        ImmutableMap.of(
            "type", "SetCell".getBytes(StandardCharsets.UTF_8),
            "value", "new-val-1-c".getBytes(StandardCharsets.UTF_8),
            "column_qualifier", "col_c".getBytes(StandardCharsets.UTF_8),
            "family_name", COLUMN_FAMILY_NAME_2.getBytes(StandardCharsets.UTF_8)));
    Row mutationRow =
        Row.withSchema(SCHEMA)
            .withFieldValue("key", "key-1".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("mutations", mutations)
            .build();

    PCollectionRowTuple.of("input", p.apply(Create.of(Arrays.asList(mutationRow))))
        .apply(writeTransform);
    p.run().waitUntilFinish();

    // get rows from table
    List<com.google.cloud.bigtable.data.v2.models.Row> rows =
        dataClient.readRows(Query.create(tableId)).stream().collect(Collectors.toList());
    // we should still have only one row with the same key
    assertEquals(1, rows.size());
    assertEquals("key-1", rows.get(0).getKey().toStringUtf8());

    // check that we now have two cells in each column we added to and that
    // the last cell in each column has the updated value
    com.google.cloud.bigtable.data.v2.models.Row row = rows.get(0);
    List<RowCell> cellsColA =
        row.getCells(COLUMN_FAMILY_NAME_1, "col_a").stream()
            .sorted(RowCell.compareByNative())
            .collect(Collectors.toList());
    List<RowCell> cellsColC =
        row.getCells(COLUMN_FAMILY_NAME_2, "col_c").stream()
            .sorted(RowCell.compareByNative())
            .collect(Collectors.toList());
    assertEquals(2, cellsColA.size());
    assertEquals(2, cellsColC.size());
    System.out.println(cellsColA);
    System.out.println(cellsColC);
    assertEquals("new-val-1-a", cellsColA.get(1).getValue().toStringUtf8());
    assertEquals("new-val-1-c", cellsColC.get(1).getValue().toStringUtf8());
  }

  @Test
  public void testSetMutationNewColumn() {
    RowMutation rowMutation =
        RowMutation.create(tableId, "key-1").setCell(COLUMN_FAMILY_NAME_1, "col_a", "val-1-a");
    dataClient.mutateRow(rowMutation);

    List<Map<String, byte[]>> mutations = new ArrayList<>();
    // mutation to set cell in a new column
    mutations.add(
        ImmutableMap.of(
            "type", "SetCell".getBytes(StandardCharsets.UTF_8),
            "value", "new-val-1".getBytes(StandardCharsets.UTF_8),
            "column_qualifier", "new_col".getBytes(StandardCharsets.UTF_8),
            "family_name", COLUMN_FAMILY_NAME_1.getBytes(StandardCharsets.UTF_8)));
    Row mutationRow =
        Row.withSchema(SCHEMA)
            .withFieldValue("key", "key-1".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("mutations", mutations)
            .build();

    PCollectionRowTuple.of("input", p.apply(Create.of(Arrays.asList(mutationRow))))
        .apply(writeTransform);
    p.run().waitUntilFinish();

    // get rows from table
    List<com.google.cloud.bigtable.data.v2.models.Row> rows =
        dataClient.readRows(Query.create(tableId)).stream().collect(Collectors.toList());

    // we should still have only one row with the same key
    assertEquals(1, rows.size());
    assertEquals("key-1", rows.get(0).getKey().toStringUtf8());
    // check the new column exists with only one cell.
    // also check cell value is correct
    com.google.cloud.bigtable.data.v2.models.Row row = rows.get(0);
    List<RowCell> cellsNewCol = row.getCells(COLUMN_FAMILY_NAME_1, "new_col");
    assertEquals(1, cellsNewCol.size());
    assertEquals("new-val-1", cellsNewCol.get(0).getValue().toStringUtf8());
  }

  @Test
  public void testDeleteCellsFromColumn() {
    RowMutation rowMutation =
        RowMutation.create(tableId, "key-1")
            .setCell(COLUMN_FAMILY_NAME_1, "col_a", "val-1-a")
            .setCell(COLUMN_FAMILY_NAME_1, "col_b", "val-1-b");
    dataClient.mutateRow(rowMutation);
    // write two cells in col_a. both should get deleted
    rowMutation =
        RowMutation.create(tableId, "key-1").setCell(COLUMN_FAMILY_NAME_1, "col_a", "new-val-1-a");
    dataClient.mutateRow(rowMutation);

    List<Map<String, byte[]>> mutations = new ArrayList<>();
    // mutation to delete cells from a column
    mutations.add(
        ImmutableMap.of(
            "type", "DeleteFromColumn".getBytes(StandardCharsets.UTF_8),
            "column_qualifier", "col_a".getBytes(StandardCharsets.UTF_8),
            "family_name", COLUMN_FAMILY_NAME_1.getBytes(StandardCharsets.UTF_8)));
    Row mutationRow =
        Row.withSchema(SCHEMA)
            .withFieldValue("key", "key-1".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("mutations", mutations)
            .build();

    PCollectionRowTuple.of("input", p.apply(Create.of(Arrays.asList(mutationRow))))
        .apply(writeTransform);
    p.run().waitUntilFinish();

    // get rows from table
    List<com.google.cloud.bigtable.data.v2.models.Row> rows =
        dataClient.readRows(Query.create(tableId)).stream().collect(Collectors.toList());

    // we should still have one row with the same key
    assertEquals(1, rows.size());
    assertEquals("key-1", rows.get(0).getKey().toStringUtf8());
    // get cells from this column family. we started with three cells and deleted two from one
    // column.
    // we should end up with one cell in the column we didn't touch.
    // check that the remaining cell is indeed from col_b
    com.google.cloud.bigtable.data.v2.models.Row row = rows.get(0);
    List<RowCell> cells = row.getCells(COLUMN_FAMILY_NAME_1);
    assertEquals(1, cells.size());
    assertEquals("col_b", cells.get(0).getQualifier().toStringUtf8());
  }

  @Test
  public void testDeleteCellsFromColumnWithTimestampRange() {
    // write two cells in one column with different timestamps.
    RowMutation rowMutation =
        RowMutation.create(tableId, "key-1")
            .setCell(COLUMN_FAMILY_NAME_1, "col", 100_000_000, "val");
    dataClient.mutateRow(rowMutation);
    rowMutation =
        RowMutation.create(tableId, "key-1")
            .setCell(COLUMN_FAMILY_NAME_1, "col", 200_000_000, "new-val");
    dataClient.mutateRow(rowMutation);

    List<Map<String, byte[]>> mutations = new ArrayList<>();
    // mutation to delete cells from a column within a timestamp range
    mutations.add(
        ImmutableMap.of(
            "type", "DeleteFromColumn".getBytes(StandardCharsets.UTF_8),
            "column_qualifier", "col".getBytes(StandardCharsets.UTF_8),
            "family_name", COLUMN_FAMILY_NAME_1.getBytes(StandardCharsets.UTF_8),
            "start_timestamp_micros", Longs.toByteArray(99_999_999),
            "end_timestamp_micros", Longs.toByteArray(100_000_001)));
    Row mutationRow =
        Row.withSchema(SCHEMA)
            .withFieldValue("key", "key-1".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("mutations", mutations)
            .build();

    PCollectionRowTuple.of("input", p.apply(Create.of(Arrays.asList(mutationRow))))
        .apply(writeTransform);
    p.run().waitUntilFinish();

    // get rows from table
    List<com.google.cloud.bigtable.data.v2.models.Row> rows =
        dataClient.readRows(Query.create(tableId)).stream().collect(Collectors.toList());

    // we should still have one row with the same key
    assertEquals(1, rows.size());
    assertEquals("key-1", rows.get(0).getKey().toStringUtf8());
    // we had two cells in col_a and deleted the older one. we should be left with the newer cell.
    // check cell has correct value and timestamp
    com.google.cloud.bigtable.data.v2.models.Row row = rows.get(0);
    List<RowCell> cells = row.getCells(COLUMN_FAMILY_NAME_1, "col");
    assertEquals(1, cells.size());
    assertEquals("new-val", cells.get(0).getValue().toStringUtf8());
    assertEquals(200_000_000, cells.get(0).getTimestamp());
  }

  @Test
  public void testDeleteColumnFamily() {
    RowMutation rowMutation =
        RowMutation.create(tableId, "key-1")
            .setCell(COLUMN_FAMILY_NAME_1, "col_a", "val")
            .setCell(COLUMN_FAMILY_NAME_2, "col_b", "val");
    dataClient.mutateRow(rowMutation);

    List<Map<String, byte[]>> mutations = new ArrayList<>();
    // mutation to delete a whole column family
    mutations.add(
        ImmutableMap.of(
            "type", "DeleteFromFamily".getBytes(StandardCharsets.UTF_8),
            "family_name", COLUMN_FAMILY_NAME_1.getBytes(StandardCharsets.UTF_8)));
    Row mutationRow =
        Row.withSchema(SCHEMA)
            .withFieldValue("key", "key-1".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("mutations", mutations)
            .build();

    PCollectionRowTuple.of("input", p.apply(Create.of(Arrays.asList(mutationRow))))
        .apply(writeTransform);
    p.run().waitUntilFinish();

    // get rows from table
    List<com.google.cloud.bigtable.data.v2.models.Row> rows =
        dataClient.readRows(Query.create(tableId)).stream().collect(Collectors.toList());

    // we should still have one row with the same key
    assertEquals(1, rows.size());
    assertEquals("key-1", rows.get(0).getKey().toStringUtf8());
    // we had one cell in each of two column families. we deleted a column family, so should end up
    // with
    // one cell in the column family we didn't touch.
    com.google.cloud.bigtable.data.v2.models.Row row = rows.get(0);
    List<RowCell> cells = row.getCells();
    assertEquals(1, cells.size());
    assertEquals(COLUMN_FAMILY_NAME_2, cells.get(0).getFamily());
  }

  @Test
  public void testDeleteRow() {
    RowMutation rowMutation =
        RowMutation.create(tableId, "key-1").setCell(COLUMN_FAMILY_NAME_1, "col", "val-1");
    dataClient.mutateRow(rowMutation);
    rowMutation =
        RowMutation.create(tableId, "key-2").setCell(COLUMN_FAMILY_NAME_1, "col", "val-2");
    dataClient.mutateRow(rowMutation);

    List<Map<String, byte[]>> mutations = new ArrayList<>();
    // mutation to delete a whole row
    mutations.add(ImmutableMap.of("type", "DeleteFromRow".getBytes(StandardCharsets.UTF_8)));
    Row mutationRow =
        Row.withSchema(SCHEMA)
            .withFieldValue("key", "key-1".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("mutations", mutations)
            .build();

    PCollectionRowTuple.of("input", p.apply(Create.of(Arrays.asList(mutationRow))))
        .apply(writeTransform);
    p.run().waitUntilFinish();

    // get rows from table
    List<com.google.cloud.bigtable.data.v2.models.Row> rows =
        dataClient.readRows(Query.create(tableId)).stream().collect(Collectors.toList());

    // we created two rows then deleted one, so should end up with the row we didn't touch
    assertEquals(1, rows.size());
    assertEquals("key-2", rows.get(0).getKey().toStringUtf8());
  }
}
