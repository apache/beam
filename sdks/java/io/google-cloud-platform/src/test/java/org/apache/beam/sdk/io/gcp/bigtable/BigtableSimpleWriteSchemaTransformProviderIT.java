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
import static org.junit.Assert.assertTrue;

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
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableWriteSchemaTransformProvider.BigtableWriteSchemaTransformConfiguration;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType; // Import FieldType
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BigtableSimpleWriteSchemaTransformProviderIT {
  @Rule public final transient TestPipeline p = TestPipeline.create();

  private static final String COLUMN_FAMILY_NAME_1 = "test_cf_1";
  private static final String COLUMN_FAMILY_NAME_2 = "test_cf_2";
  private BigtableTableAdminClient tableAdminClient;
  private BigtableDataClient dataClient;
  private String tableId = String.format("BigtableWriteIT-%tF-%<tH-%<tM-%<tS-%<tL", new Date());
  private String projectId;
  private String instanceId;
  private PTransform<PCollectionRowTuple, PCollectionRowTuple> writeTransform;

  @Test
  public void testInvalidConfigs() {
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
            .setCell(COLUMN_FAMILY_NAME_1, "col_a", 1000, "val-1-a")
            .setCell(COLUMN_FAMILY_NAME_2, "col_c", 1000, "val-1-c");
    dataClient.mutateRow(rowMutation);
    Schema testSchema =
        Schema.builder()
            .addByteArrayField("key")
            .addStringField("type")
            .addByteArrayField("value")
            .addByteArrayField("column_qualifier")
            .addStringField("family_name")
            .addField("timestamp_micros", FieldType.INT64) // Changed to INT64
            .build();

    Row mutationRow1 =
        Row.withSchema(testSchema)
            .withFieldValue("key", "key-1".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("type", "SetCell")
            .withFieldValue("value", "new-val-1-a".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("column_qualifier", "col_a".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("family_name", COLUMN_FAMILY_NAME_1)
            .withFieldValue("timestamp_micros", 2000L)
            .build();
    Row mutationRow2 =
        Row.withSchema(testSchema)
            .withFieldValue("key", "key-1".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("type", "SetCell")
            .withFieldValue("value", "new-val-1-c".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("column_qualifier", "col_c".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("family_name", COLUMN_FAMILY_NAME_2)
            .withFieldValue("timestamp_micros", 2000L)
            .build();

    PCollection<Row> inputPCollection =
        p.apply(Create.of(Arrays.asList(mutationRow1, mutationRow2)));
    inputPCollection.setRowSchema(testSchema);

    PCollectionRowTuple.of("input", inputPCollection) // Use the schema-set PCollection
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
    // Bigtable keeps cell history ordered by descending timestamp
    assertEquals("new-val-1-a", cellsColA.get(0).getValue().toStringUtf8());
    assertEquals("new-val-1-c", cellsColC.get(0).getValue().toStringUtf8());
    assertEquals("val-1-a", cellsColA.get(1).getValue().toStringUtf8());
    assertEquals("val-1-c", cellsColC.get(1).getValue().toStringUtf8());
  }

  @Test
  public void testSetMutationNewColumn() {
    RowMutation rowMutation =
        RowMutation.create(tableId, "key-1").setCell(COLUMN_FAMILY_NAME_1, "col_a", "val-1-a");
    dataClient.mutateRow(rowMutation);
    Schema testSchema =
        Schema.builder()
            .addByteArrayField("key")
            .addStringField("type")
            .addByteArrayField("value")
            .addByteArrayField("column_qualifier")
            .addStringField("family_name")
            .addField("timestamp_micros", FieldType.INT64)
            .build();
    Row mutationRow =
        Row.withSchema(testSchema)
            .withFieldValue("key", "key-1".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("type", "SetCell")
            .withFieldValue("value", "new-val-1".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("column_qualifier", "new_col".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("family_name", COLUMN_FAMILY_NAME_1)
            .withFieldValue("timestamp_micros", 999_000L)
            .build();

    PCollection<Row> inputPCollection = p.apply(Create.of(Arrays.asList(mutationRow)));
    inputPCollection.setRowSchema(testSchema);

    PCollectionRowTuple.of("input", inputPCollection) // Use the schema-set PCollection
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
    Schema testSchema =
        Schema.builder()
            .addByteArrayField("key")
            .addStringField("type")
            .addByteArrayField("column_qualifier")
            .addStringField("family_name")
            .build();
    Row mutationRow =
        Row.withSchema(testSchema)
            .withFieldValue("key", "key-1".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("type", "DeleteFromColumn")
            .withFieldValue("column_qualifier", "col_a".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("family_name", COLUMN_FAMILY_NAME_1)
            .build();

    PCollection<Row> inputPCollection = p.apply(Create.of(Arrays.asList(mutationRow)));
    inputPCollection.setRowSchema(testSchema);

    PCollectionRowTuple.of("input", inputPCollection) // Use the schema-set PCollection
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
    Schema testSchema =
        Schema.builder()
            .addByteArrayField("key")
            .addStringField("type")
            .addByteArrayField("column_qualifier")
            .addStringField("family_name")
            .addField("start_timestamp_micros", FieldType.INT64)
            .addField("end_timestamp_micros", FieldType.INT64)
            .build();
    Row mutationRow =
        Row.withSchema(testSchema)
            .withFieldValue("key", "key-1".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("type", "DeleteFromColumn")
            .withFieldValue("column_qualifier", "col".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("family_name", COLUMN_FAMILY_NAME_1)
            .withFieldValue("start_timestamp_micros", 99_990_000L)
            .withFieldValue("end_timestamp_micros", 100_000_000L)
            .build();

    PCollection<Row> inputPCollection = p.apply(Create.of(Arrays.asList(mutationRow)));
    inputPCollection.setRowSchema(testSchema);

    PCollectionRowTuple.of("input", inputPCollection) // Use the schema-set PCollection
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
    assertEquals(2, cells.size());
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
    Schema testSchema =
        Schema.builder()
            .addByteArrayField("key")
            .addStringField("type")
            .addStringField("family_name")
            .build();
    Row mutationRow =
        Row.withSchema(testSchema)
            .withFieldValue("key", "key-1".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("type", "DeleteFromFamily")
            .withFieldValue("family_name", COLUMN_FAMILY_NAME_1)
            .build();

    PCollection<Row> inputPCollection = p.apply(Create.of(Arrays.asList(mutationRow)));
    inputPCollection.setRowSchema(testSchema);

    PCollectionRowTuple.of("input", inputPCollection) // Use the schema-set PCollection
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
    Schema testSchema = Schema.builder().addByteArrayField("key").addStringField("type").build();
    Row mutationRow =
        Row.withSchema(testSchema)
            .withFieldValue("key", "key-1".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("type", "DeleteFromRow")
            .build();

    PCollection<Row> inputPCollection = p.apply(Create.of(Arrays.asList(mutationRow)));
    inputPCollection.setRowSchema(testSchema);

    PCollectionRowTuple.of("input", inputPCollection) // Use the schema-set PCollection
        .apply(writeTransform);
    p.run().waitUntilFinish();

    // get rows from table
    List<com.google.cloud.bigtable.data.v2.models.Row> rows =
        dataClient.readRows(Query.create(tableId)).stream().collect(Collectors.toList());

    // we created two rows then deleted one, so should end up with the row we didn't touch
    assertEquals(1, rows.size());
    assertEquals("key-2", rows.get(0).getKey().toStringUtf8());
  }

  @Test
  public void testAllMutations() {

    // --- Initial Setup: Populate the table with diverse data ---
    dataClient.mutateRow(
        RowMutation.create(tableId, "row-setcell")
            .setCell(COLUMN_FAMILY_NAME_1, "col_initial_1", "initial_val_1")
            .setCell(COLUMN_FAMILY_NAME_2, "col_initial_2", "initial_val_2"));

    dataClient.mutateRow(
        RowMutation.create(tableId, "row-delete-col")
            .setCell(COLUMN_FAMILY_NAME_1, "col_to_delete_A", 1000, "val_to_delete_A_old")
            .setCell(COLUMN_FAMILY_NAME_1, "col_to_delete_A", 2000, "val_to_delete_A_new")
            .setCell(COLUMN_FAMILY_NAME_1, "col_to_keep_B", "val_to_keep_B"));

    dataClient.mutateRow(
        RowMutation.create(tableId, "row-delete-col-ts")
            .setCell(COLUMN_FAMILY_NAME_1, "ts_col", 1000, "ts_val_old")
            .setCell(COLUMN_FAMILY_NAME_1, "ts_col", 2000, "ts_val_new")
            .setCell(COLUMN_FAMILY_NAME_2, "ts_col_other_cf", "ts_val_other_cf"));

    dataClient.mutateRow(
        RowMutation.create(tableId, "row-delete-family")
            .setCell(COLUMN_FAMILY_NAME_1, "col_to_delete_family", "val_delete_family")
            .setCell(COLUMN_FAMILY_NAME_2, "col_to_keep_family", "val_keep_family"));

    dataClient.mutateRow(
        RowMutation.create(tableId, "row-delete-row")
            .setCell(COLUMN_FAMILY_NAME_1, "col", "val_delete_row"));

    dataClient.mutateRow(
        RowMutation.create(tableId, "row-final-check")
            .setCell(COLUMN_FAMILY_NAME_1, "col_final_1", "val_final_1"));

    // --- Define Schema for various mutation types ---

    Schema uberSchema =
        Schema.builder()
            .addByteArrayField("key") // Key is always present and non-null
            .addStringField(
                "type") // Type is always present and non-null (e.g., "SetCell", "DeleteFromRow")
            // All other fields are conditional based on the 'type' of mutation, so they must be
            // nullable.
            .addNullableField("value", FieldType.BYTES) // Used by SetCell
            .addNullableField(
                "column_qualifier", FieldType.BYTES) // Used by SetCell, DeleteFromColumn
            .addNullableField(
                "family_name",
                FieldType.STRING) // Used by SetCell, DeleteFromColumn, DeleteFromFamily
            .addNullableField("timestamp_micros", FieldType.INT64) // Optional for SetCell
            .addNullableField(
                "start_timestamp_micros", FieldType.INT64) // Used by DeleteFromColumn with range
            .addNullableField(
                "end_timestamp_micros", FieldType.INT64) // Used by DeleteFromColumn with range
            .build();

    // --- Create a list of mutation Rows ---
    List<Row> mutations = new ArrayList<>();

    // 1. SetCell (Update an existing cell, add a new cell)
    // Update "row-setcell", col_initial_1
    mutations.add(
        Row.withSchema(uberSchema)
            .withFieldValue("key", "row-setcell".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("type", "SetCell")
            .withFieldValue("value", "updated_val_1".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("column_qualifier", "col_initial_1".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("family_name", COLUMN_FAMILY_NAME_1)
            .withFieldValue("timestamp_micros", 3000L)
            .build());
    // Add new cell to "row-setcell"
    mutations.add(
        Row.withSchema(uberSchema)
            .withFieldValue("key", "row-setcell".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("type", "SetCell")
            .withFieldValue("value", "new_col_val".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("column_qualifier", "new_col_A".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("family_name", COLUMN_FAMILY_NAME_1)
            .withFieldValue("timestamp_micros", 4000L)
            .build());

    // 2. DeleteFromColumn
    // Delete "col_to_delete_A" from "row-delete-col"
    mutations.add(
        Row.withSchema(uberSchema)
            .withFieldValue("key", "row-delete-col".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("type", "DeleteFromColumn")
            .withFieldValue("column_qualifier", "col_to_delete_A".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("family_name", COLUMN_FAMILY_NAME_1)
            .build());

    // 3. DeleteFromColumn with Timestamp Range
    // Delete "ts_col" with timestamp 1000 from "row-delete-col-ts"
    mutations.add(
        Row.withSchema(uberSchema)
            .withFieldValue("key", "row-delete-col-ts".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("type", "DeleteFromColumn")
            .withFieldValue("column_qualifier", "ts_col".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("family_name", COLUMN_FAMILY_NAME_1)
            .withFieldValue("start_timestamp_micros", 999L) // Inclusive
            .withFieldValue("end_timestamp_micros", 1001L) // Exclusive
            .build());

    // 4. DeleteFromFamily
    // Delete COLUMN_FAMILY_NAME_1 from "row-delete-family"
    mutations.add(
        Row.withSchema(uberSchema)
            .withFieldValue("key", "row-delete-family".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("type", "DeleteFromFamily")
            .withFieldValue("family_name", COLUMN_FAMILY_NAME_1)
            .build());

    // 5. DeleteFromRow
    // Delete "row-delete-row"
    mutations.add(
        Row.withSchema(uberSchema)
            .withFieldValue("key", "row-delete-row".getBytes(StandardCharsets.UTF_8))
            .withFieldValue("type", "DeleteFromRow")
            .build());

    // --- Apply the mutations --
    PCollection<Row> inputPCollection = p.apply(Create.of(mutations));
    inputPCollection.setRowSchema(uberSchema); // Set the comprehensive schema for the PCollection

    PCollectionRowTuple.of("input", inputPCollection) // Use the schema-set PCollection
        .apply(writeTransform);
    p.run().waitUntilFinish();

    // --- Assertions: Verify the final state of the table ---

    List<com.google.cloud.bigtable.data.v2.models.Row> rows =
        dataClient.readRows(Query.create(tableId)).stream().collect(Collectors.toList());

    assertEquals(5, rows.size()); // Expecting 'row-setcell', 'row-delete-col', 'row-delete-col-ts',
    // 'row-final-check'

    // Verify "row-setcell"
    com.google.cloud.bigtable.data.v2.models.Row rowSetCell =
        rows.stream()
            .filter(r -> r.getKey().toStringUtf8().equals("row-setcell"))
            .findFirst()
            .orElse(null);
    assertEquals("row-setcell", rowSetCell.getKey().toStringUtf8());
    List<RowCell> cellsSetCellCol1 =
        rowSetCell.getCells(COLUMN_FAMILY_NAME_1, "col_initial_1").stream()
            .sorted(RowCell.compareByNative())
            .collect(Collectors.toList());
    assertEquals(2, cellsSetCellCol1.size()); // Original + updated
    assertEquals(
        "initial_val_1", cellsSetCellCol1.get(0).getValue().toStringUtf8()); // Newest value
    assertEquals(
        "updated_val_1", cellsSetCellCol1.get(1).getValue().toStringUtf8()); // Oldest value
    List<RowCell> cellsSetCellNewCol = rowSetCell.getCells(COLUMN_FAMILY_NAME_1, "new_col_A");
    assertEquals(1, cellsSetCellNewCol.size());
    assertEquals("new_col_val", cellsSetCellNewCol.get(0).getValue().toStringUtf8());
    List<RowCell> cellsSetCellCol2 = rowSetCell.getCells(COLUMN_FAMILY_NAME_2, "col_initial_2");
    assertEquals(1, cellsSetCellCol2.size());
    assertEquals("initial_val_2", cellsSetCellCol2.get(0).getValue().toStringUtf8());

    // Verify "row-delete-col"
    com.google.cloud.bigtable.data.v2.models.Row rowDeleteCol =
        rows.stream()
            .filter(r -> r.getKey().toStringUtf8().equals("row-delete-col"))
            .findFirst()
            .orElse(null);
    assertEquals("row-delete-col", rowDeleteCol.getKey().toStringUtf8());
    List<RowCell> cellsColToDeleteA =
        rowDeleteCol.getCells(COLUMN_FAMILY_NAME_1, "col_to_delete_A");
    assertTrue(cellsColToDeleteA.isEmpty()); // Should be deleted
    List<RowCell> cellsColToKeepB = rowDeleteCol.getCells(COLUMN_FAMILY_NAME_1, "col_to_keep_B");
    assertEquals(1, cellsColToKeepB.size());
    assertEquals("val_to_keep_B", cellsColToKeepB.get(0).getValue().toStringUtf8());

    // Verify "row-delete-col-ts"
    com.google.cloud.bigtable.data.v2.models.Row rowDeleteColTs =
        rows.stream()
            .filter(r -> r.getKey().toStringUtf8().equals("row-delete-col-ts"))
            .findFirst()
            .orElse(null);
    assertEquals("row-delete-col-ts", rowDeleteColTs.getKey().toStringUtf8());
    List<RowCell> cellsTsCol = rowDeleteColTs.getCells(COLUMN_FAMILY_NAME_1, "ts_col");
    assertEquals(1, cellsTsCol.size()); // Only the 2000 timestamp cell should remain
    assertEquals("ts_val_new", cellsTsCol.get(0).getValue().toStringUtf8());
    assertEquals(2000, cellsTsCol.get(0).getTimestamp());
    List<RowCell> cellsTsColOtherCf =
        rowDeleteColTs.getCells(COLUMN_FAMILY_NAME_2, "ts_col_other_cf");
    assertEquals(1, cellsTsColOtherCf.size());
    assertEquals("ts_val_other_cf", cellsTsColOtherCf.get(0).getValue().toStringUtf8());

    // Verify "row-delete-family"
    com.google.cloud.bigtable.data.v2.models.Row rowDeleteFamily =
        rows.stream()
            .filter(r -> r.getKey().toStringUtf8().equals("row-delete-family"))
            .findFirst()
            .orElse(null);
    assertEquals("row-delete-family", rowDeleteFamily.getKey().toStringUtf8());
    List<RowCell> cellsCf1 = rowDeleteFamily.getCells(COLUMN_FAMILY_NAME_1);
    assertTrue(cellsCf1.isEmpty()); // COLUMN_FAMILY_NAME_1 should be empty
    List<RowCell> cellsCf2 = rowDeleteFamily.getCells(COLUMN_FAMILY_NAME_2);
    assertEquals(1, cellsCf2.size());
    assertEquals("val_keep_family", cellsCf2.get(0).getValue().toStringUtf8());

    // Verify "row-delete-row" is gone
    assertTrue(rows.stream().noneMatch(r -> r.getKey().toStringUtf8().equals("row-delete-row")));

    // Verify "row-final-check" still exists
    com.google.cloud.bigtable.data.v2.models.Row rowFinalCheck =
        rows.stream()
            .filter(r -> r.getKey().toStringUtf8().equals("row-final-check"))
            .findFirst()
            .orElse(null);
    assertEquals("row-final-check", rowFinalCheck.getKey().toStringUtf8());
    List<RowCell> cellsFinalCheck = rowFinalCheck.getCells(COLUMN_FAMILY_NAME_1, "col_final_1");
    assertEquals(1, cellsFinalCheck.size());
    assertEquals("val_final_1", cellsFinalCheck.get(0).getValue().toStringUtf8());
  }
}
