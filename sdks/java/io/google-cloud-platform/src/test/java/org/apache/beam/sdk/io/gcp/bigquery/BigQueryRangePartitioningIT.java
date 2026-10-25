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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.apache.beam.sdk.io.gcp.testing.BigtableUtils.TEMP_DATASET_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.RangePartitioning;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test that writes to integer-range partitioned tables in BigQuery. */
@RunWith(JUnit4.class)
public class BigQueryRangePartitioningIT {
  private static final int NUM_ROWS = 100;

  private static String project;
  private static String bigQueryLocation;
  private static final BigqueryClient BQ_CLIENT = new BigqueryClient("BigQueryRangePartitioningIT");
  private static final String DATASET_NAME =
      TEMP_DATASET_PREFIX
          + "range_partitioning_"
          + System.currentTimeMillis()
          + "_"
          + new SecureRandom().nextInt(32);
  private static final RangePartitioning RANGE_PARTITIONING =
      new RangePartitioning()
          .setField("id")
          .setRange(new RangePartitioning.Range().setStart(0L).setEnd(100L).setInterval(10L));
  private static final Clustering CLUSTERING = new Clustering().setFields(Arrays.asList("name"));
  private static final TableSchema SCHEMA =
      new TableSchema()
          .setFields(
              Arrays.asList(
                  new TableFieldSchema().setName("id").setType("INTEGER"),
                  new TableFieldSchema().setName("name").setType("STRING")));

  private TestPipelineOptions options;

  @BeforeClass
  public static void setupTestEnvironment() throws Exception {
    project = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
    bigQueryLocation =
        TestPipeline.testingPipelineOptions().as(TestBigQueryOptions.class).getBigQueryLocation();
    BQ_CLIENT.createNewDataset(project, DATASET_NAME, null, bigQueryLocation);
  }

  @Before
  public void setUp() {
    options = TestPipeline.testingPipelineOptions().as(TestPipelineOptions.class);
    options.setTempLocation(
        FileSystems.matchNewDirectory(options.getTempRoot(), "temp-it").toString());
  }

  @AfterClass
  public static void cleanup() {
    BQ_CLIENT.deleteDataset(project, DATASET_NAME);
  }

  private static List<TableRow> rows() {
    List<TableRow> rows = new ArrayList<>();
    for (int i = 0; i < NUM_ROWS; i++) {
      rows.add(new TableRow().set("id", i).set("name", "name_" + (i % 7)));
    }
    return rows;
  }

  private Table getTable(String tableName) throws Exception {
    return BQ_CLIENT.getTableResource(project, DATASET_NAME, tableName);
  }

  private static void assertRangePartitioning(Table table) {
    RangePartitioning actual = table.getRangePartitioning();
    assertNotNull("Table is not range partitioned", actual);
    assertEquals(RANGE_PARTITIONING.getField(), actual.getField());
    assertEquals(RANGE_PARTITIONING.getRange().getStart(), actual.getRange().getStart());
    assertEquals(RANGE_PARTITIONING.getRange().getEnd(), actual.getRange().getEnd());
    assertEquals(RANGE_PARTITIONING.getRange().getInterval(), actual.getRange().getInterval());
  }

  private void assertRowCount(String tableName) throws Exception {
    TableRow response =
        Iterables.getOnlyElement(
            BQ_CLIENT.queryUnflattened(
                String.format("SELECT COUNT(*) FROM `%s.%s.%s`", project, DATASET_NAME, tableName),
                project,
                true,
                true,
                bigQueryLocation));
    assertEquals(NUM_ROWS, Integer.parseInt((String) response.get("f0_")));
  }

  @Test
  public void testE2EBigQueryRangePartitioning() throws Exception {
    String tableName = "range_partitioned_" + System.currentTimeMillis();

    Pipeline p = Pipeline.create(options);
    p.apply(Create.<TableRow>of(rows()))
        .apply(
            BigQueryIO.writeTableRows()
                .to(String.format("%s.%s", DATASET_NAME, tableName))
                .withRangePartitioning(RANGE_PARTITIONING)
                .withSchema(SCHEMA)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
    p.run().waitUntilFinish();

    assertRangePartitioning(getTable(tableName));
    assertRowCount(tableName);
  }

  @Test
  public void testE2EBigQueryRangePartitioningTableFunction() throws Exception {
    String tableName = "range_partitioned_table_function_" + System.currentTimeMillis();
    String destination = String.format("%s.%s", DATASET_NAME, tableName);

    Pipeline p = Pipeline.create(options);
    p.apply(Create.<TableRow>of(rows()))
        .apply(
            BigQueryIO.writeTableRows()
                .to(
                    (ValueInSingleWindow<TableRow> vsw) ->
                        new TableDestination(
                            destination, null, null, CLUSTERING, RANGE_PARTITIONING))
                .withClustering()
                .withRangePartitioning()
                .withSchema(SCHEMA)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
    p.run().waitUntilFinish();

    Table table = getTable(tableName);
    assertRangePartitioning(table);
    assertEquals(CLUSTERING.getFields(), table.getClustering().getFields());
    assertRowCount(tableName);
  }

  @Test
  public void testE2EBigQueryRangePartitioningStorageWriteApi() throws Exception {
    String tableName = "range_partitioned_storage_api_" + System.currentTimeMillis();

    Pipeline p = Pipeline.create(options);
    p.apply(Create.<TableRow>of(rows()))
        .apply(
            BigQueryIO.writeTableRows()
                .to(String.format("%s.%s", DATASET_NAME, tableName))
                .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                .withRangePartitioning(RANGE_PARTITIONING)
                .withSchema(SCHEMA)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
    p.run().waitUntilFinish();

    assertRangePartitioning(getTable(tableName));
    assertRowCount(tableName);
  }

  @Test
  public void testE2EBigQueryTempTablesIntoExistingRangePartitionedTable() throws Exception {
    String tableName = "range_partitioned_existing_" + System.currentTimeMillis();
    BQ_CLIENT.createNewTable(
        project,
        DATASET_NAME,
        new Table()
            .setTableReference(
                new TableReference()
                    .setProjectId(project)
                    .setDatasetId(DATASET_NAME)
                    .setTableId(tableName))
            .setSchema(SCHEMA)
            .setRangePartitioning(RANGE_PARTITIONING));

    // The small partition limits make FILE_LOADS go through temp tables and copy jobs. BigQuery
    // rejects the copy unless the temp tables have the same range partitioning as the existing
    // table, and no withRangePartitioning() is set here.
    Pipeline p = Pipeline.create(options);
    p.apply(Create.<TableRow>of(rows()))
        .apply(
            BigQueryIO.writeTableRows()
                .to(String.format("%s.%s", DATASET_NAME, tableName))
                .withMaxBytesPerPartition(1)
                .withMaxFilesPerPartition(1)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
    p.run().waitUntilFinish();

    assertRangePartitioning(getTable(tableName));
    assertRowCount(tableName);
  }
}
