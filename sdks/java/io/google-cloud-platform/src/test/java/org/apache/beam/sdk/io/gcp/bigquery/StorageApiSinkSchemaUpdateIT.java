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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects.firstNonNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class StorageApiSinkSchemaUpdateIT {
  @Parameterized.Parameters
  public static Iterable<Object[]> data() {
    return ImmutableList.of(
        new Object[] {false, false},
        new Object[] {false, true},
        new Object[] {true, false},
        new Object[] {true, true});
  }

  @Parameterized.Parameter(0)
  public boolean useInputSchema;

  @Parameterized.Parameter(1)
  public boolean changeTableSchema;

  @Rule public TestName testName = new TestName();

  private static final Logger LOG = LoggerFactory.getLogger(StorageApiSinkSchemaUpdateIT.class);

  private static final BigqueryClient BQ_CLIENT =
      new BigqueryClient("StorageApiSinkSchemaChangeIT");
  private static final String PROJECT =
      TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
  private static final String BIG_QUERY_DATASET_ID =
      "storage_api_sink_schema_change_" + System.nanoTime();

  private static final String[] FIELDS = {
    "BOOL",
    "BOOLEAN",
    "BYTES",
    "INT64",
    "INTEGER",
    "FLOAT",
    "FLOAT64",
    "NUMERIC",
    "STRING",
    "DATE",
    "TIMESTAMP"
  };

  private static final int TOTAL_N = 35;

  private final Random randomGenerator = new Random();

  @BeforeClass
  public static void setUpTestEnvironment() throws IOException, InterruptedException {
    // Create one BQ dataset for all test cases.
    LOG.info("Creating dataset {}.", BIG_QUERY_DATASET_ID);
    BQ_CLIENT.createNewDataset(PROJECT, BIG_QUERY_DATASET_ID);
  }

  @AfterClass
  public static void cleanUp() {
    LOG.info("Cleaning up dataset {} and tables.", BIG_QUERY_DATASET_ID);
    BQ_CLIENT.deleteDataset(PROJECT, BIG_QUERY_DATASET_ID);
  }

  private String createTable(TableSchema tableSchema) throws IOException, InterruptedException {
    String tableId = Iterables.get(Splitter.on('[').split(testName.getMethodName()), 0);
    if (useInputSchema) {
      tableId += "WithInputSchema";
    }
    if (changeTableSchema) {
      tableId += "OnSchemaChange";
    }
    BQ_CLIENT.deleteTable(PROJECT, BIG_QUERY_DATASET_ID, tableId);
    BQ_CLIENT.createNewTable(
        PROJECT,
        BIG_QUERY_DATASET_ID,
        new Table()
            .setSchema(tableSchema)
            .setTableReference(
                new TableReference()
                    .setTableId(tableId)
                    .setDatasetId(BIG_QUERY_DATASET_ID)
                    .setProjectId(PROJECT)));
    return tableId;
  }

  static class UpdateSchemaDoFn extends DoFn<KV<Integer, TableRow>, TableRow> {

    private final String projectId;
    private final String datasetId;
    private final String tableId;
    // represent as String because TableSchema is not serializable
    private final String newSchema;

    private transient BigqueryClient bqClient;

    private static final String ROW_COUNTER = "rowCounter";

    @StateId(ROW_COUNTER)
    @SuppressWarnings("unused")
    private final StateSpec<@org.jetbrains.annotations.NotNull ValueState<Integer>> counter;

    public UpdateSchemaDoFn(
        String projectId, String datasetId, String tableId, TableSchema newSchema) {
      this.projectId = projectId;
      this.datasetId = datasetId;
      this.tableId = tableId;
      this.newSchema = BigQueryHelpers.toJsonString(newSchema);
      this.bqClient = null;
      this.counter = StateSpecs.value();
    }

    @Setup
    public void setup() {
      bqClient = new BigqueryClient("StorageApiSinkSchemaChangeIT");
    }

    @ProcessElement
    public void processElement(ProcessContext c, @StateId(ROW_COUNTER) ValueState<Integer> counter)
        throws Exception {
      int current = firstNonNull(counter.read(), 0);
      Thread.sleep(1000);
      // We update schema early on to leave a healthy amount of time for StreamWriter to recognize
      // it.
      if (current == 2) {
        bqClient.updateTableSchema(
            projectId,
            datasetId,
            tableId,
            BigQueryHelpers.fromJsonString(newSchema, TableSchema.class));
      }

      counter.write(++current);
      c.output(c.element().getValue());
    }
  }

  static class GenerateRowFunc implements SerializableFunction<Long, TableRow> {
    private final List<String> fieldNames;

    public GenerateRowFunc(List<String> fieldNames) {
      this.fieldNames = fieldNames;
    }

    @Override
    public TableRow apply(Long rowId) {
      TableRow row = new TableRow();
      row.set("id", rowId);

      for (String name : fieldNames) {
        String type = Iterables.get(Splitter.on('_').split(name), 0);
        switch (type) {
          case "BOOL":
          case "BOOLEAN":
            if (rowId % 2 == 0) {
              row.set(name, false);
            } else {
              row.set(name, true);
            }
            break;
          case "BYTES":
            row.set(name, String.format("test_blob_%s", rowId).getBytes(StandardCharsets.UTF_8));
            break;
          case "INT64":
          case "INTEGER":
            row.set(name, rowId + 10);
            break;
          case "FLOAT":
          case "FLOAT64":
            row.set(name, 0.5 + rowId);
            break;
          case "NUMERIC":
            row.set(name, rowId + 0.12345);
            break;
          case "DATE":
            row.set(name, "2022-01-01");
            break;
          case "TIMESTAMP":
            row.set(name, "2022-01-01T10:10:10.012Z");
            break;
          case "STRING":
            row.set(name, "test_string" + rowId);
            break;
          default:
            row.set(name, "unknown" + rowId);
            break;
        }
      }
      return row;
    }
  }

  private static TableSchema makeTableSchemaFromTypes(
      List<String> fieldNames, Set<String> nullableFieldNames) {
    ImmutableList.Builder<TableFieldSchema> builder = ImmutableList.<TableFieldSchema>builder();

    // Add an id field for verification of correctness
    builder.add(new TableFieldSchema().setType("INTEGER").setName("id").setMode("REQUIRED"));

    // the name is prefix with type_.
    for (String name : fieldNames) {
      String type = Iterables.get(Splitter.on('_').split(name), 0);
      String mode = "REQUIRED";
      if (nullableFieldNames != null && nullableFieldNames.contains(name)) {
        mode = "NULLABLE";
      }
      builder.add(new TableFieldSchema().setType(type).setName(name).setMode(mode));
    }

    return new TableSchema().setFields(builder.build());
  }

  private void runStreamingPipelineWithSchemaChange(
      Write.Method method, boolean useAutoSchemaUpdate, boolean useIgnoreUnknownValues)
      throws Exception {
    Pipeline p = Pipeline.create(TestPipeline.testingPipelineOptions());
    p.getOptions().as(BigQueryOptions.class).setStorageApiAppendThresholdBytes(0);
    p.getOptions().as(DirectOptions.class).setTargetParallelism(1);

    List<String> fieldNamesOrigin = new ArrayList<String>(Arrays.asList(FIELDS));

    // Shuffle the fields in the write schema to do fuzz testing on field order
    List<String> fieldNamesShuffled = new ArrayList<String>(fieldNamesOrigin);
    Collections.shuffle(fieldNamesShuffled, randomGenerator);

    // The updated schema includes all fields in the original schema plus a random new field
    List<String> fieldNamesWithExtra = new ArrayList<String>(fieldNamesOrigin);
    String extraField =
        fieldNamesOrigin.get(randomGenerator.nextInt(fieldNamesOrigin.size())) + "_EXTRA";
    fieldNamesWithExtra.add(extraField);

    TableSchema bqTableSchema = makeTableSchemaFromTypes(fieldNamesOrigin, null);
    TableSchema inputSchema = makeTableSchemaFromTypes(fieldNamesShuffled, null);
    TableSchema updatedSchema =
        makeTableSchemaFromTypes(fieldNamesWithExtra, ImmutableSet.of(extraField));

    String tableId = createTable(bqTableSchema);
    String tableSpec = PROJECT + ":" + BIG_QUERY_DATASET_ID + "." + tableId;

    TestStream.Builder<TableRow> testStream =
        TestStream.create(TableRowJsonCoder.of()).advanceWatermarkTo(new Instant(0));

    // Generate rows with original schema
    int numOriginalRows = 30;
    GenerateRowFunc originalSchemaFunc = new GenerateRowFunc(fieldNamesOrigin);
    for (long i = 0; i < numOriginalRows; i++) {
      testStream = testStream.addElements(originalSchemaFunc.apply(i));
      testStream = testStream.advanceProcessingTime(Duration.standardSeconds(5));
    }

    // Generate rows with updated schema
    // These rows should only reach the table if ignoreUnknownValues is set,
    // and the extra field should be present only when autoSchemaUpdate is set
    GenerateRowFunc updatedSchemaFunc = new GenerateRowFunc(fieldNamesWithExtra);
    for (long i = numOriginalRows; i < TOTAL_N; i++) {
      testStream = testStream.addElements(updatedSchemaFunc.apply(i));
      testStream = testStream.advanceProcessingTime(Duration.standardSeconds(5));
    }

    // build write transform
    Write<TableRow> write =
        BigQueryIO.writeTableRows()
            .to(tableSpec)
            .withAutoSchemaUpdate(useAutoSchemaUpdate)
            .withMethod(method)
            .withCreateDisposition(CreateDisposition.CREATE_NEVER)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND);
    if (method == Write.Method.STORAGE_WRITE_API) {
      write = write.withTriggeringFrequency(Duration.standardSeconds(1));
    }
    if (useInputSchema) {
      write = write.withSchema(inputSchema);
    }
    if (useIgnoreUnknownValues) {
      write = write.ignoreUnknownValues();
    }

    // build pipeline
    PCollection<TableRow> rows = p.apply("Generate rows", testStream.advanceWatermarkToInfinity());
    if (changeTableSchema) {
      rows =
          rows
              // UpdateSchemaDoFn uses state, so need to have a KV input
              .apply("Add a dummy key", WithKeys.of(1))
              .apply(
                  "Update Schema",
                  ParDo.of(
                      new UpdateSchemaDoFn(PROJECT, BIG_QUERY_DATASET_ID, tableId, updatedSchema)));
    }
    WriteResult result = rows.apply("Stream to BigQuery", write);
    if (useIgnoreUnknownValues) {
      // We ignore the extra fields, so no rows should have been sent to DLQ
      PAssert.that(result.getFailedStorageApiInserts()).empty();
    } else {
      // When we don't set ignoreUnknownValues, the rows with extra fields should be sent to DLQ.
      PAssert.that(result.getFailedStorageApiInserts())
          .satisfies(new VerifyPCollectionSize(TOTAL_N - numOriginalRows, extraField));
    }
    p.run().waitUntilFinish();

    // Check row completeness, non-duplication, and that schema update works as intended.
    int expectedCount = useIgnoreUnknownValues ? TOTAL_N : numOriginalRows;
    boolean checkNoDuplication = (method == Write.Method.STORAGE_WRITE_API) ? true : false;
    checkRowCompleteness(tableSpec, expectedCount, checkNoDuplication);
    if (useIgnoreUnknownValues) {
      checkRowsWithUpdatedSchema(tableSpec, extraField, numOriginalRows, useAutoSchemaUpdate);
    }
  }

  private static class VerifyPCollectionSize
      implements SerializableFunction<Iterable<BigQueryStorageApiInsertError>, Void> {
    int expectedSize;
    String extraField;

    VerifyPCollectionSize(int expectedSize, String extraField) {
      this.expectedSize = expectedSize;
      this.extraField = extraField;
    }

    @Override
    public Void apply(Iterable<BigQueryStorageApiInsertError> input) {
      List<BigQueryStorageApiInsertError> itemList = new ArrayList<>();
      for (BigQueryStorageApiInsertError err : input) {
        itemList.add(err);
        // Check the error message is due to schema mismatch from the extra field.
        assertTrue(err.getErrorMessage().contains("SchemaTooNarrowException"));
        assertTrue(err.getErrorMessage().contains(extraField));
      }
      // Check we have the expected number of rows in DLQ.
      // Should be equal to number of rows with extra fields.
      assertEquals(expectedSize, itemList.size());
      return null;
    }
  }

  // Check the expected number of rows reached the table.
  // If appropriate (using STORAGE_WRITE_API), check no duplication happened.
  private static void checkRowCompleteness(
      String tableSpec, int expectedCount, boolean checkNoDuplication)
      throws IOException, InterruptedException {
    TableRow queryResponse =
        Iterables.getOnlyElement(
            BQ_CLIENT.queryUnflattened(
                String.format("SELECT COUNT(DISTINCT(id)), COUNT(id) FROM [%s]", tableSpec),
                PROJECT,
                true,
                false));

    int distinctCount = Integer.parseInt((String) queryResponse.get("f0_"));
    int totalCount = Integer.parseInt((String) queryResponse.get("f1_"));

    LOG.info("total distinct count = {}, total count = {}", distinctCount, totalCount);

    assertEquals(expectedCount, distinctCount);
    if (checkNoDuplication) {
      assertEquals(distinctCount, totalCount);
    }
  }

  // Performs checks on the table's rows under different conditions.
  // Note: these should only be performed when ignoreUnknownValues is set.
  public void checkRowsWithUpdatedSchema(
      String tableSpec, String extraField, int numOriginalRows, boolean useAutoSchemaUpdate)
      throws IOException, InterruptedException {
    List<TableRow> actualRows =
        BQ_CLIENT.queryUnflattened(
            String.format("SELECT * FROM [%s]", tableSpec), PROJECT, true, false);

    for (TableRow row : actualRows) {
      // Rows written to the table should not have the extra field if
      // 1. The original row didn't have it in the first place
      // 2. We didn't set autoSchemaUpdate (the extra field would just be dropped)
      // 3. We didn't change the table's schema (again, the extra field would be dropped)
      if (Integer.parseInt((String) row.get("id")) < numOriginalRows
          || !useAutoSchemaUpdate
          || !changeTableSchema) {
        assertTrue(row.get(extraField) == null);
      } else {
        assertTrue(row.get(extraField) != null);
      }
    }
  }

  @Test
  public void testExactlyOnce() throws Exception {
    runStreamingPipelineWithSchemaChange(
        Write.Method.STORAGE_WRITE_API,
        /** autoSchemaUpdate */
        false,
        /** ignoreUnknownvalues */
        false);
  }

  @Test
  public void testExactlyOnceWithIgnoreUnknownValues() throws Exception {
    runStreamingPipelineWithSchemaChange(Write.Method.STORAGE_WRITE_API, false, true);
  }

  @Test
  public void testExactlyOnceWithAutoSchemaUpdate() throws Exception {
    runStreamingPipelineWithSchemaChange(Write.Method.STORAGE_WRITE_API, true, true);
  }

  @Test
  public void testAtLeastOnce() throws Exception {
    runStreamingPipelineWithSchemaChange(Write.Method.STORAGE_API_AT_LEAST_ONCE, false, false);
  }

  @Test
  public void testAtLeastOnceWithIgnoreUnknownValues() throws Exception {
    runStreamingPipelineWithSchemaChange(Write.Method.STORAGE_API_AT_LEAST_ONCE, false, true);
  }

  @Test
  public void testAtLeastOnceWithAutoSchemaUpdate() throws Exception {
    runStreamingPipelineWithSchemaChange(Write.Method.STORAGE_API_AT_LEAST_ONCE, true, true);
  }
}
