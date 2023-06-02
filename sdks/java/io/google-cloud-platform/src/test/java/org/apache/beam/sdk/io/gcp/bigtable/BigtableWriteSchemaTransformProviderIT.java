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

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.bigtable.v2.Mutation;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import jdk.internal.joptsimple.internal.Strings;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableWriteSchemaTransformProvider.BigtableWriteSchemaTransformConfiguration;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import javax.annotation.Nullable;

@RunWith(JUnit4.class)
public class BigtableWriteSchemaTransformProviderIT {
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
        List<BigtableWriteSchemaTransformConfiguration.Builder> invalidConfigs =
                Arrays.asList(
                        BigtableWriteSchemaTransformConfiguration.builder()
                                .setProject("project")
                                .setInstance("instance")
                                .setTable(""),
                        BigtableWriteSchemaTransformConfiguration.builder()
                                .setProject("")
                                .setInstance("instance")
                                .setTable("table"),
                        BigtableWriteSchemaTransformConfiguration.builder()
                                .setProject("project")
                                .setInstance("")
                                .setTable("table"));

        for (BigtableWriteSchemaTransformConfiguration.Builder config : invalidConfigs) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> {
                        config.build();
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

    public List<Row> writeToTable(int numRows) throws Exception {
        // Checks if table exists, creates table if does not exist.
        if (!tableAdminClient.exists(tableId)) {
            CreateTableRequest createTableRequest =
                    CreateTableRequest.of(tableId)
                            .addFamily(COLUMN_FAMILY_NAME_1)
                            .addFamily(COLUMN_FAMILY_NAME_2);
            tableAdminClient.createTable(createTableRequest);
        }

        List<Row> expectedRows = new ArrayList<>();

        try {
            for (int i = 1; i <= numRows; i++) {
                String key = "key" + i;
                String valueA = "value a" + i;
                String valueB = "value b" + i;
                String valueC = "value c" + i;
                String valueD = "value d" + i;
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
                                        .withFieldValue("value", valueA)
                                        .withFieldValue("timestamp", timestamp)
                                        .withFieldValue("labels", Collections.emptyList())
                                        .build()));
                columns1.put(
                        "b",
                        Arrays.asList(
                                Row.withSchema(CELL_SCHEMA)
                                        .withFieldValue("value", valueB)
                                        .withFieldValue("timestamp", timestamp)
                                        .withFieldValue("labels", Collections.emptyList())
                                        .build()));

                Map<String, List<Row>> columns2 = new HashMap<>();
                columns2.put(
                        "c",
                        Arrays.asList(
                                Row.withSchema(CELL_SCHEMA)
                                        .withFieldValue("value", valueC)
                                        .withFieldValue("timestamp", timestamp)
                                        .withFieldValue("labels", Collections.emptyList())
                                        .build()));
                columns2.put(
                        "d",
                        Arrays.asList(
                                Row.withSchema(CELL_SCHEMA)
                                        .withFieldValue("value", valueD)
                                        .withFieldValue("timestamp", timestamp)
                                        .withFieldValue("labels", Collections.emptyList())
                                        .build()));

                Map<String, Map<String, List<Row>>> families = new HashMap<>();
                families.put(COLUMN_FAMILY_NAME_1, columns1);
                families.put(COLUMN_FAMILY_NAME_2, columns2);

                Row expectedRow =
                        Row.withSchema(ROW_SCHEMA)
                                .withFieldValue("key", key)
                                .withFieldValue("families", families)
                                .build();

                expectedRows.add(expectedRow);
            }
        } catch (NotFoundException e) {
            throw new RuntimeException("Failed to write to table", e);
        }
        return expectedRows;
    }

    @Test
    public void testRead() throws Exception {
        tableId = "BigtableWriteSchemaTransformIT";
        List<Row> expectedRows = writeToTable(10);

        BigtableWriteSchemaTransformConfiguration config =
                BigtableWriteSchemaTransformConfiguration.builder()
                        .setTable(tableId)
                        .setInstance(instanceId)
                        .setProject(projectId)
                        .build();
        SchemaTransform transform = new BigtableWriteSchemaTransformProvider().from(config);

        PCollection<Row> rows =
                PCollectionRowTuple.empty(p).apply(transform.buildTransform()).get("output");
        PAssert.that(rows).containsInAnyOrder(expectedRows);
        p.run().waitUntilFinish();
    }

    public Row generateBeamRowMutations(byte[] key,
                                        byte[] value,
                                        @Nullable Map<String, List<String>> columnFamiliesAndColumnsToSet,
                                        @Nullable String columnToDelete,
                                        @Nullable String columnFamilyToDelete,
                                        boolean deleteRow) {
        Schema schema = Schema.builder()
                .addByteArrayField("key")
                .addArrayField("mutations", Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.BYTES))
                .build();

        List<Map<String, byte[]>> mutations = new ArrayList<>();
        // populate columns in column families
        if (columnFamiliesAndColumnsToSet != null) {
            for (Map.Entry<String, List<String>> entry : columnFamiliesAndColumnsToSet.entrySet()) {
                String columnFamily = entry.getKey();
                for (String column : entry.getValue()) {
                    mutations.add(ImmutableMap.of(
                        "type", "SetCell".getBytes(StandardCharsets.UTF_8),
                        "value", value,
                        "column_qualifier", column.getBytes(StandardCharsets.UTF_8),
                        "family_name", columnFamily.getBytes(StandardCharsets.UTF_8),
                        "timestamp_micros", Longs.toByteArray(10_000L)));
                }
            }
        }
        if(columnToDelete != null) {
            assert columnFamilyToDelete != null;
            mutations.add(ImmutableMap.of(
                    "type", "DeleteFromColumn".getBytes(StandardCharsets.UTF_8),
                    "column_qualifier", columnToDelete.getBytes(StandardCharsets.UTF_8),
                    "family_name", columnFamilyToDelete.getBytes(StandardCharsets.UTF_8)));
        }
        if(columnToDelete == null && columnFamilyToDelete != null) {
            mutations.add(ImmutableMap.of(
                    "type", "DeleteFromFamily".getBytes(StandardCharsets.UTF_8),
                    "family_name", columnFamilyToDelete.getBytes(StandardCharsets.UTF_8)));
        }
        if (deleteRow) {
            mutations.add(ImmutableMap.of(
                    "type", "DeleteFromRow".getBytes(StandardCharsets.UTF_8)));
        }

        return Row.withSchema(schema)
                .withFieldValue("key", key)
                .withFieldValue("mutations", mutations)
                .build();

    }

    public List<Row> generateRows(int numRows) {
        List<Row> rows = new ArrayList<>(numRows);

        for (int i = 0; i < numRows; i++) {
            byte[] key = String.format("key-%s", i).getBytes(StandardCharsets.UTF_8);
            byte[] value = String.format("value-%s", i).getBytes(StandardCharsets.UTF_8);
            Map<String, List<String>> columnsToPopulate = ImmutableMap.of(
                    COLUMN_FAMILY_NAME_1, Arrays.asList("col_a", "col_b"),
                    COLUMN_FAMILY_NAME_2, Arrays.asList("col_c", "col_d"));
            String deleteRowsInThisColumn = "col_a";
            String deleteColumnInThisFamily = COLUMN_FAMILY_NAME_1;
                    // time range?
        }
    }

    @Test
    public void testWrite() throws Exception {
        // generate data

        //write

        //then
        Table table = tableAdminClient.getTable(tableId);
        assertThat(table.getColumnFamilies(), Matchers.hasSize(2));
        assertThat(table.getColumnFamilies().stream().map(colFam -> colFam.getId()).collect(Collectors.toList()),
                Matchers.containsInAnyOrder(COLUMN_FAMILY_NAME_1, COLUMN_FAMILY_NAME_2));


        ServerStream<com.google.cloud.bigtable.data.v2.models.Row> bigtableRows = dataClient.readRows(Query.create(tableId));

        List<KV<ByteString, List<RowCell>>> data = bigtableRows.stream().map(row -> KV.of(row.getKey(), row.getCells(COLUMN_FAMILY_NAME_1))).collect(Collectors.toList());
    }
}
