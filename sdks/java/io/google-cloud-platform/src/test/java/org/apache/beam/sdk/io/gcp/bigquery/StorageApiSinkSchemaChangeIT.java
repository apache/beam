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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects.firstNonNull;
import static org.junit.Assert.assertThrows;
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
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class StorageApiSinkSchemaChangeIT {
  @Parameterized.Parameters
  public static Iterable<Object[]> data() {
    return ImmutableList.of(new Object[] {true}, new Object[] {false});
  }

  @Parameterized.Parameter(0)
  public boolean useWriteSchema;

  private static final Logger LOG = LoggerFactory.getLogger(StorageApiSinkSchemaChangeIT.class);

  private static final BigqueryClient BQ_CLIENT =
      new BigqueryClient("StorageApiSinkSchemaChangeIT");
  private static final String PROJECT =
      TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
  private static final String BIG_QUERY_DATASET_ID =
      "storage_api_sink_schema_change" + System.nanoTime();

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

  private static final int MAX_N = 50;

  private static final long RANDOM_SEED = 1;

  @Rule public transient ExpectedLogs loggedBigQueryIO = ExpectedLogs.none(BigQueryIO.class);

  @BeforeClass
  public static void setUpTestEnvironment() throws IOException, InterruptedException {
    // Create one BQ dataset for all test cases.
    BQ_CLIENT.createNewDataset(PROJECT, BIG_QUERY_DATASET_ID);
  }

  @AfterClass
  public static void cleanup() {
    LOG.info("Start to clean up tables and datasets.");
    BQ_CLIENT.deleteDataset(PROJECT, BIG_QUERY_DATASET_ID);
  }

  private static String createTable(TableSchema tableSchema)
      throws IOException, InterruptedException {
    String tableId = "table" + System.nanoTime();
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

  private static boolean checkRowCompleteness(String tableSpec)
      throws IOException, InterruptedException {
    TableRow queryResponse =
        Iterables.getOnlyElement(
            BQ_CLIENT.queryUnflattened(
                String.format("SELECT COUNT(DISTINCT(id)), MIN(id), MAX(id) FROM %s", tableSpec),
                PROJECT,
                true,
                true));

    int distinctCount = Integer.parseInt((String) queryResponse.get("f0_"));
    int rangeMin = Integer.parseInt((String) queryResponse.get("f1_"));
    int rangeMax = Integer.parseInt((String) queryResponse.get("f2_"));

    LOG.info("total distinct count = {}, min = {}, max = {}", distinctCount, rangeMin, rangeMax);

    return (rangeMax - rangeMin + 1) == distinctCount && distinctCount == MAX_N;
  }

  private static boolean checkRowDuplication(String tableSpec)
      throws IOException, InterruptedException {
    TableRow queryResponse =
        Iterables.getOnlyElement(
            BQ_CLIENT.queryUnflattened(
                String.format("SELECT COUNT(DISTINCT(id)), COUNT(id) FROM %s", tableSpec),
                PROJECT,
                true,
                true));

    int distinctCount = Integer.parseInt((String) queryResponse.get("f0_"));
    int totalCount = Integer.parseInt((String) queryResponse.get("f1_"));

    LOG.info("total distinct count = {}, total count = {}", distinctCount, totalCount);

    return distinctCount == totalCount;
  }

  static class UpdateSchemaDoFn extends DoFn<KV<Integer, Long>, Long> {

    private final String projectId;
    private final String datasetId;
    private final String tableId;

    private final String schemaString;

    private transient BigqueryClient bqClient;

    private static final String MY_COUNTER = "myCounter";

    @StateId(MY_COUNTER)
    @SuppressWarnings("unused")
    private final StateSpec<@org.jetbrains.annotations.NotNull ValueState<Integer>> counter;

    public UpdateSchemaDoFn(
        String projectId, String datasetId, String tableId, TableSchema schema) {
      this.projectId = projectId;
      this.datasetId = datasetId;
      this.tableId = tableId;
      this.schemaString = BigQueryHelpers.toJsonString(schema);
      this.bqClient = null;
      this.counter = StateSpecs.value();
    }

    private int getRowCount(String tableSpec) throws IOException, InterruptedException {
      TableRow queryResponse =
          Iterables.getOnlyElement(
              bqClient.queryUnflattened(
                  String.format("SELECT COUNT(*) FROM %s", tableSpec), PROJECT, true, true));
      return Integer.parseInt((String) queryResponse.get("f0_"));
    }

    @Setup
    public void setup() {
      bqClient = new BigqueryClient("StorageApiSinkSchemaChangeIT_UpdateSchema");
    }

    @Teardown
    public void tearDown() {
      return;
    }

    @ProcessElement
    public void processElement(ProcessContext c, @StateId(MY_COUNTER) ValueState<Integer> counter)
        throws InterruptedException {
      int current = firstNonNull(counter.read(), 0);
      if (current == 0) {
        int rowCount = 0;
        try {
          rowCount = this.getRowCount(this.projectId + "." + this.datasetId + "." + this.tableId);
        } catch (Exception e) {
          LOG.error(e.toString());
        }
        LOG.info("checking # of rows in BQ: {}", rowCount);
        if (rowCount > 0) {
          bqClient.updateTableSchema(
              this.projectId,
              this.datasetId,
              this.tableId,
              BigQueryHelpers.fromJsonString(this.schemaString, TableSchema.class));

          Thread.sleep(5000);
          counter.write(1);
        }
      }

      c.output(Objects.requireNonNull(c.element()).getValue());
    }
  }

  static class GenerateRowFunc implements SerializableFunction<Long, TableRow> {

    private final List<String> fieldNames;

    public GenerateRowFunc(List<String> fieldNames) {
      this.fieldNames = fieldNames;
    }

    @Override
    public TableRow apply(Long rowId) {
      LOG.info("Generating row #{}", rowId);
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
            row.set(name, "test_blob".getBytes(StandardCharsets.UTF_8));
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

  private static void runStreamingPipelineWithSchemaChange(
      Write.Method method,
      boolean useWriteSchema,
      boolean useAutoSchemaUpdate,
      int triggeringFreq,
      boolean useAutoSharding,
      int numShards,
      boolean useIgnoreUnknownValues)
      throws IOException, InterruptedException {
    PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
    BigQueryOptions bqOptions = pipelineOptions.as(BigQueryOptions.class);
    bqOptions.setStorageApiAppendThresholdRecordCount(1);

    Pipeline p = Pipeline.create(pipelineOptions);

    List<String> fieldNamesOrigin = new ArrayList<String>(Arrays.asList(FIELDS));

    // Shuffle the fields in the write schema to do fuzz testing on field order
    List<String> fieldNamesShuffled = new ArrayList<String>(fieldNamesOrigin);
    Collections.shuffle(fieldNamesShuffled, new Random(RANDOM_SEED));

    // The updated schema includes all fields in the original schema plus a random new field
    List<String> fieldNamesWithExtra = new ArrayList<String>(fieldNamesOrigin);
    Random r = new Random(RANDOM_SEED);
    String extraField = fieldNamesOrigin.get(r.nextInt(fieldNamesOrigin.size())) + "_EXTRA";
    fieldNamesWithExtra.add(extraField);

    TableSchema bqTableSchema = makeTableSchemaFromTypes(fieldNamesOrigin, null);
    LOG.info("original table schema: {}", BigQueryHelpers.toJsonString(bqTableSchema));

    TableSchema bqWriteSchema = makeTableSchemaFromTypes(fieldNamesShuffled, null);
    LOG.info("write schema: {}", BigQueryHelpers.toJsonString(bqWriteSchema));

    TableSchema bqTableSchemaUpdated =
        makeTableSchemaFromTypes(fieldNamesWithExtra, ImmutableSet.of(extraField));
    LOG.info("updated table schema: {}", BigQueryHelpers.toJsonString(bqTableSchemaUpdated));

    String tableId = createTable(bqTableSchema);

    String tableSpec = PROJECT + "." + BIG_QUERY_DATASET_ID + "." + tableId;

    TestStream.Builder<Long> testStream =
        TestStream.create(VarLongCoder.of()).advanceWatermarkTo(new Instant(0));
    // These rows contain unknown fields, which should be dropped.
    for (long i = 0; i < 5; i++) {
      testStream = testStream.addElements(i);
      testStream = testStream.advanceProcessingTime(Duration.standardSeconds(10));
    }
    // Expire the timer, which should update the schema.
    testStream = testStream.advanceProcessingTime(Duration.standardSeconds(100));
    // Add one element to trigger discovery of new schema.
    for (long i = 5; i < MAX_N; i++) {
      testStream = testStream.addElements(i);
    }

    PCollection<Long> source = p.apply("Generate numbers", testStream.advanceWatermarkToInfinity());

    PCollection<Long> input =
        source
            .apply("Add a dummy key", WithKeys.of(1))
            .apply(
                "Update Schema",
                ParDo.of(
                    new UpdateSchemaDoFn(
                        PROJECT, BIG_QUERY_DATASET_ID, tableId, bqTableSchemaUpdated)));

    Write<Long> write =
        BigQueryIO.<Long>write()
            .to(tableSpec)
            .withAutoSchemaUpdate(useAutoSchemaUpdate)
            .withSchema(bqWriteSchema)
            .withFormatFunction(new GenerateRowFunc(fieldNamesOrigin))
            .withMethod(method)
            .withCreateDisposition(CreateDisposition.CREATE_NEVER)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND);

    if (useWriteSchema) {
      write = write.withSchema(bqWriteSchema);
    }

    if (useAutoSharding) {
      write = write.withAutoSharding();
    }

    if (useIgnoreUnknownValues) {
      write = write.ignoreUnknownValues();
    }

    if (triggeringFreq > 0) {
      write = write.withTriggeringFrequency(Duration.standardSeconds(triggeringFreq));
    }

    if (numShards > 0) {
      write = write.withNumStorageWriteApiStreams(numShards);
    }

    input.apply("Stream to BigQuery", write);

    LOG.info("Start to run the pipeline ...");
    p.run().waitUntilFinish();

    assertTrue(checkRowCompleteness(tableSpec));

    if (method == Write.Method.STORAGE_WRITE_API) {
      assertTrue(checkRowDuplication(tableSpec));
    }
  }

  @Test
  public void testWriteExactlyOnceOnSchemaChange() throws IOException, InterruptedException {
    runStreamingPipelineWithSchemaChange(
        Write.Method.STORAGE_WRITE_API, useWriteSchema, false, 1, true, 0, false);
  }

  @Test
  public void testWriteExactlyOnceOnSchemaChangeWithAutoSchemaUpdate()
      throws IOException, InterruptedException {
    runStreamingPipelineWithSchemaChange(
        Write.Method.STORAGE_WRITE_API, useWriteSchema, true, 1, true, 0, true);
  }

  @Test
  public void testWriteAtLeastOnceOnSchemaChange() throws IOException, InterruptedException {
    runStreamingPipelineWithSchemaChange(
        Write.Method.STORAGE_API_AT_LEAST_ONCE, useWriteSchema, false, 0, false, 0, false);
  }

  @Test
  public void testWriteAtLeastOnceOnSchemaChangeWithAutoSchemaUpdate()
      throws IOException, InterruptedException {
    runStreamingPipelineWithSchemaChange(
        Write.Method.STORAGE_API_AT_LEAST_ONCE, useWriteSchema, true, 0, false, 0, true);
  }

  @Test
  public void testExceptionOnWriteExactlyOnceWithZeroTriggeringFreq()
      throws IOException, InterruptedException {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            runStreamingPipelineWithSchemaChange(
                Write.Method.STORAGE_WRITE_API, useWriteSchema, false, 0, true, 0, false));
  }

  @Test
  public void testExceptionOnWriteExactlyOnceWithBothNumShardsAndAutoSharding()
      throws IOException, InterruptedException {
    runStreamingPipelineWithSchemaChange(
        Write.Method.STORAGE_WRITE_API, useWriteSchema, false, 1, true, 1, false);
    loggedBigQueryIO.verifyWarn("The setting of numStorageWriteApiStreams is ignored.");
  }

  @Test
  public void testExceptionOnAutoSchemaUpdateWithoutIgnoreUnknownValues()
      throws IOException, InterruptedException {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            runStreamingPipelineWithSchemaChange(
                Write.Method.STORAGE_WRITE_API, useWriteSchema, true, 1, true, 0, false));
  }

  @Test
  public void testExceptionOnWriteAtLeastOnceWithTriggeringFreq()
      throws IOException, InterruptedException {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            runStreamingPipelineWithSchemaChange(
                Write.Method.STORAGE_API_AT_LEAST_ONCE, useWriteSchema, false, 1, true, 0, false));
  }

  @Test
  public void testExceptionOnWriteAtLeastOnceWithAutoSharding()
      throws IOException, InterruptedException {
    runStreamingPipelineWithSchemaChange(
        Write.Method.STORAGE_API_AT_LEAST_ONCE, useWriteSchema, false, 0, true, 0, false);
    loggedBigQueryIO.verifyWarn("The setting of auto-sharding is ignored.");
  }

  @Test
  public void testExceptionOnWriteAtLeastOnceWithNumShards()
      throws IOException, InterruptedException {
    runStreamingPipelineWithSchemaChange(
        Write.Method.STORAGE_API_AT_LEAST_ONCE, useWriteSchema, false, 0, false, 1, false);
    loggedBigQueryIO.verifyWarn("The setting of numStorageWriteApiStreams is ignored.");
  }
}
