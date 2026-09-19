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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.PeriodicImpulse;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runners.Parameterized;
import org.junit.runners.model.RunnerScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class StorageApiSinkSchemaUpdateITBase {
  private final boolean useInputSchema;
  private final boolean changeTableSchema;
  private final String bigQueryDatasetId;

  StorageApiSinkSchemaUpdateITBase(
      boolean useInputSchema, boolean changeTableSchema, String bigQueryDatasetId) {
    this.useInputSchema = useInputSchema;
    this.changeTableSchema = changeTableSchema;
    this.bigQueryDatasetId = bigQueryDatasetId;
  }

  /**
   * Runs the parameter sets in parallel.
   *
   * <p>Every test writes to its own table (the table name is derived from the test method name and
   * the parameter values) and the class holds no mutable static state, so the parameter sets are
   * independent. These tests spend nearly all of their time waiting on BigQuery rather than using
   * CPU, so running the parameter sets concurrently cuts the wall-clock time of the class roughly
   * by the parallelism factor. Note that Gradle's {@code maxParallelForks} only parallelizes across
   * test classes, so it does not help a {@link Parameterized} class on its own.
   */
  public static class ParallelParameterized extends Parameterized {
    private static final int MAX_PARALLEL_PARAMETERS = 4;

    public ParallelParameterized(Class<?> klass) throws Throwable {
      super(klass);
      setScheduler(
          new RunnerScheduler() {
            private final ExecutorService executor =
                Executors.newFixedThreadPool(MAX_PARALLEL_PARAMETERS);

            @Override
            public void schedule(Runnable childStatement) {
              // execute() rather than submit(): the child statement already records its own
              // failures with JUnit, and submit() would swallow them into an unchecked Future.
              executor.execute(childStatement);
            }

            @Override
            public void finished() {
              executor.shutdown();
              try {
                executor.awaitTermination(2, TimeUnit.HOURS);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
              }
            }
          });
    }
  }

  @Rule public TestName testName = new TestName();

  private static final Logger LOG = LoggerFactory.getLogger(StorageApiSinkSchemaUpdateITBase.class);

  private static final BigqueryClient BQ_CLIENT =
      new BigqueryClient("StorageApiSinkSchemaChangeIT");
  private static final String PROJECT =
      TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
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

  // ************ NOTE ************
  // The test may fail if Storage API Streams take longer than expected to recognize
  // an updated schema. If that happens consistently, just increase these two numbers
  // to give it more time.
  // Total number of rows written to the sink
  private static final int TOTAL_N = 70;
  // Number of rows with the original schema
  private static final int ORIGINAL_N = 60;
  // for dynamic destination test
  private static final int NUM_DESTINATIONS = 3;
  private static final int TOTAL_NUM_STREAMS = 6;
  // wait up to 60 seconds
  private static final int SCHEMA_PROPAGATION_TIMEOUT_MS = 60000;
  // interval between checks
  private static final int SCHEMA_PROPAGATION_CHECK_INTERVAL_MS = 5000;
  // Rather than pacing every element (which dominated the runtime of these tests), rows are
  // emitted as fast as possible and wall-clock delays are inserted only where they are actually
  // required. (a) Before the table schema is changed, so that the sink has already built its
  // message converter / append client from the original schema.
  private static final int PRE_SCHEMA_UPDATE_SETTLE_MS = 10000;
  // (b) BigQuery only reports an updated table schema on an append *response*, so the sink must
  // keep appending after the schema change for the new schema to ever reach it. The rows between
  // the schema update and the first new-column row are spread across this window to provide that
  // traffic. This is a wall-clock budget, independent of TOTAL_N.
  private static final int SCHEMA_RECOGNITION_WINDOW_MS = 60000;
  // trigger for updating the schema when the row counter reaches this value
  private static final int SCHEMA_UPDATE_TRIGGER = 2;
  // Long wait (in seconds) for Storage API streams to recognize the new schema.
  private static final int LONG_WAIT_SECONDS = 5;

  private final Random randomGenerator = new Random();

  // used when test suite specifies a particular GCP location for BigQuery operations
  private static String bigQueryLocation;

  static void setUpTestEnvironment(String bigQueryDatasetId)
      throws IOException, InterruptedException {
    // Create one BQ dataset for all test cases.
    bigQueryLocation =
        TestPipeline.testingPipelineOptions().as(TestBigQueryOptions.class).getBigQueryLocation();
    BQ_CLIENT.createNewDataset(PROJECT, bigQueryDatasetId, null, bigQueryLocation);
  }

  static void cleanUp(String bigQueryDatasetId) {
    LOG.info("Cleaning up dataset {} and tables.", bigQueryDatasetId);
    BQ_CLIENT.deleteDataset(PROJECT, bigQueryDatasetId);
  }

  private String createTable(TableSchema tableSchema) throws IOException, InterruptedException {
    return createTable(tableSchema, "");
  }

  private String createTable(TableSchema tableSchema, String suffix)
      throws IOException, InterruptedException {
    String tableId = Iterables.get(Splitter.on('[').split(testName.getMethodName()), 0);
    if (useInputSchema) {
      tableId += "WithInputSchema";
    }
    if (changeTableSchema) {
      tableId += "OnSchemaChange";
    }
    tableId += suffix;

    BQ_CLIENT.deleteTable(PROJECT, bigQueryDatasetId, tableId);
    BQ_CLIENT.createNewTable(
        PROJECT,
        bigQueryDatasetId,
        new Table()
            .setSchema(tableSchema)
            .setTableReference(
                new TableReference()
                    .setTableId(tableId)
                    .setDatasetId(bigQueryDatasetId)
                    .setProjectId(PROJECT)));
    return tableId;
  }

  static class UpdateSchemaDoFn extends DoFn<KV<Integer, TableRow>, TableRow> {

    private final String projectId;
    private final String datasetId;
    // represent as String because TableSchema is not serializable
    private final Map<String, String> newSchemas;

    private transient BigqueryClient bqClient;

    private static final String ROW_COUNTER = "rowCounter";

    @StateId(ROW_COUNTER)
    @SuppressWarnings("unused")
    private final StateSpec<ValueState<Integer>> counter;

    private final boolean waitForPropagation;
    // Whether to insert the wall-clock delays around the schema change. Only needed by the test
    // variants whose expectations depend on the sink observing (or not yet observing) the new
    // table schema.
    private final boolean insertSchemaChangeDelays;

    public UpdateSchemaDoFn(
        String projectId,
        String datasetId,
        Map<String, TableSchema> newSchemas,
        boolean waitForPropagation,
        boolean insertSchemaChangeDelays) {
      this.projectId = projectId;
      this.datasetId = datasetId;
      Map<String, String> serializableSchemas = new HashMap<>();
      for (Map.Entry<String, TableSchema> entry : newSchemas.entrySet()) {
        serializableSchemas.put(entry.getKey(), BigQueryHelpers.toJsonString(entry.getValue()));
      }
      this.newSchemas = serializableSchemas;
      this.bqClient = null;
      this.counter = StateSpecs.value();
      this.waitForPropagation = waitForPropagation;
      this.insertSchemaChangeDelays = insertSchemaChangeDelays;
    }

    @Setup
    public void setup() {
      bqClient = new BigqueryClient("StorageApiSinkSchemaChangeIT");
    }

    @ProcessElement
    public void processElement(ProcessContext c, @StateId(ROW_COUNTER) ValueState<Integer> counter)
        throws Exception {
      int current = firstNonNull(counter.read(), 0);
      // We update schema early on to leave a healthy amount of time for the StreamWriter to
      // recognize it,
      // ensuring that subsequent writers are created with the updated schema.
      if (current == SCHEMA_UPDATE_TRIGGER) {
        if (insertSchemaChangeDelays) {
          // Give the rows emitted so far time to reach BigQuery, so that the sink's message
          // converter and append client are built from the *original* table schema.
          Thread.sleep(PRE_SCHEMA_UPDATE_SETTLE_MS);
        }
        for (Map.Entry<String, String> entry : newSchemas.entrySet()) {
          bqClient.updateTableSchema(
              projectId,
              datasetId,
              entry.getKey(),
              BigQueryHelpers.fromJsonString(entry.getValue(), TableSchema.class));
        }

        if (waitForPropagation) {
          // check that schema update propagated fully
          long startTime = System.currentTimeMillis();
          long timeoutMillis = SCHEMA_PROPAGATION_TIMEOUT_MS;
          boolean schemaPropagated = false;
          while (System.currentTimeMillis() - startTime < timeoutMillis) {
            schemaPropagated = true;
            for (Map.Entry<String, String> entry : newSchemas.entrySet()) {
              TableSchema currentSchema =
                  bqClient.getTableResource(projectId, datasetId, entry.getKey()).getSchema();
              TableSchema expectedSchema =
                  BigQueryHelpers.fromJsonString(entry.getValue(), TableSchema.class);
              if (currentSchema.getFields().size() != expectedSchema.getFields().size()) {
                schemaPropagated = false;
                break;
              }
            }
            if (schemaPropagated) {
              break;
            }
            Thread.sleep(SCHEMA_PROPAGATION_CHECK_INTERVAL_MS);
          }
          if (!schemaPropagated) {
            LOG.warn("Schema update did not propagate fully within the timeout.");
          } else {
            LOG.info(
                "Schema update propagated fully within the timeout - {}.",
                System.currentTimeMillis() - startTime);
          }
        }
      }

      // Spread the rows between the schema update and the first new-column row across the
      // recognition window. These appends are what cause BigQuery to eventually return the
      // updated schema on an append response; rows outside the window are emitted immediately.
      if (insertSchemaChangeDelays) {
        Object rowId = c.element().getValue().get("id");
        if (rowId instanceof Number) {
          long id = ((Number) rowId).longValue();
          if (id > SCHEMA_UPDATE_TRIGGER && id < ORIGINAL_N) {
            Thread.sleep(SCHEMA_RECOGNITION_WINDOW_MS / (ORIGINAL_N - SCHEMA_UPDATE_TRIGGER - 1));
          }
        }
      }

      counter.write(++current);
      c.output(c.element().getValue());
    }
  }

  static class GenerateRowFunc implements SerializableFunction<Long, TableRow> {
    private final List<String> fieldNames;
    private final List<String> fieldNamesWithExtra;

    public GenerateRowFunc(List<String> fieldNames, List<String> fieldNamesWithExtra) {
      this.fieldNames = fieldNames;
      this.fieldNamesWithExtra = fieldNamesWithExtra;
    }

    @Override
    public TableRow apply(Long rowId) {
      TableRow row = new TableRow();
      row.set("id", rowId);

      List<String> fields = rowId < ORIGINAL_N ? fieldNames : fieldNamesWithExtra;

      for (String name : fields) {
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
      Write.Method method,
      boolean useAutoSchemaUpdate,
      boolean consistentAutoUpdate,
      boolean useIgnoreUnknownValues)
      throws Exception {
    Pipeline p = Pipeline.create(TestPipeline.testingPipelineOptions());
    // Set threshold bytes to 0 so that the stream attempts to fetch an updated schema after each
    // append
    p.getOptions().as(BigQueryOptions.class).setStorageApiAppendThresholdBytes(0);
    // Limit parallelism so that all streams recognize the new schema in an expected short amount
    // of time (before we start writing rows with updated schema)
    p.getOptions().as(BigQueryOptions.class).setNumStorageWriteApiStreams(TOTAL_NUM_STREAMS);
    p.getOptions().as(StreamingOptions.class).setStreaming(true);

    // Need to manually enable streaming engine for legacy dataflow runner
    ExperimentalOptions.addExperiment(
        p.getOptions().as(ExperimentalOptions.class), GcpOptions.STREAMING_ENGINE_EXPERIMENT);
    // Only run the most relevant test case on Dataflow
    if (p.getOptions().getRunner().getName().contains("DataflowRunner")) {
      assumeTrue(
          "Skipping in favor of more relevant test case and to avoid timing issues",
          consistentAutoUpdate || (!changeTableSchema && useInputSchema && useAutoSchemaUpdate));
    }
    if (consistentAutoUpdate) {
      assumeTrue(changeTableSchema);
      assumeFalse(useAutoSchemaUpdate);
    }

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
    String tableSpec = PROJECT + ":" + bigQueryDatasetId + "." + tableId;

    // build write transform
    Write<TableRow> write =
        BigQueryIO.writeTableRows()
            .to(tableSpec)
            .withMethod(method)
            .withCreateDisposition(CreateDisposition.CREATE_NEVER)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND);
    // These two are mutually exclusive: withAutoSchemaUpdateConsistent also sets the
    // autoSchemaUpdate flag, so calling both would clobber withAutoSchemaUpdate().
    if (consistentAutoUpdate) {
      write = write.withAutoSchemaUpdateConsistent(true, Duration.standardMinutes(5));
    } else {
      write = write.withAutoSchemaUpdate(useAutoSchemaUpdate);
    }
    if (useInputSchema) {
      write = write.withSchema(inputSchema);
    }
    if (useIgnoreUnknownValues) {
      write = write.ignoreUnknownValues();
    }
    // We give a healthy waiting period between each element to give Storage API streams a chance to
    // recognize the new schema. Apply on relevant tests.
    boolean waitLonger =
        changeTableSchema && (useAutoSchemaUpdate || !useInputSchema) && !consistentAutoUpdate;
    if (method == Write.Method.STORAGE_WRITE_API) {

      write =
          write.withTriggeringFrequency(
              waitLonger ? Duration.standardSeconds(LONG_WAIT_SECONDS) : Duration.millis(10));
    }

    // set up and build pipeline.
    // Rows are emitted as fast as possible; any wall-clock delay that the test needs is inserted
    // by UpdateSchemaDoFn around the schema change itself (see insertSchemaChangeDelays).
    Instant start = new Instant(0);
    Duration interval = Duration.millis(1);
    Duration stop = Duration.millis(TOTAL_N - 1);
    Function<Instant, Long> getIdFromInstant =
        (Function<Instant, Long> & Serializable) (Instant instant) -> instant.getMillis();

    // Generates rows with original schema up for row IDs under ORIGINAL_N
    // Then generates rows with updated schema for the rest
    // Rows with updated schema should only reach the table if ignoreUnknownValues is set,
    // and the extra field should be present only when autoSchemaUpdate is set
    GenerateRowFunc generateRowFunc = new GenerateRowFunc(fieldNamesOrigin, fieldNamesWithExtra);
    PCollection<Instant> instants =
        p.apply(
            "Generate Instants",
            PeriodicImpulse.create()
                .startAt(start)
                .stopAt(start.plus(stop))
                .withInterval(interval)
                .catchUpToNow(false));
    PCollection<TableRow> rows =
        instants.apply(
            "Create TableRows",
            MapElements.into(TypeDescriptor.of(TableRow.class))
                .via(instant -> generateRowFunc.apply(getIdFromInstant.apply(instant))));

    if (changeTableSchema) {
      rows =
          rows
              // UpdateSchemaDoFn uses state, so need to have a KV input
              .apply("Add a dummy key", WithKeys.of(1))
              .apply(
                  "Update Schema",
                  ParDo.of(
                      new UpdateSchemaDoFn(
                          PROJECT,
                          bigQueryDatasetId,
                          ImmutableMap.of(tableId, updatedSchema),
                          !consistentAutoUpdate,
                          // The *Consistent variants need the settle delay too: without it the
                          // sink's converter may be built AFTER the table schema changed, so the
                          // test passes without ever exercising schema-change detection.
                          waitLonger || consistentAutoUpdate)));
    }
    WriteResult result = rows.apply("Stream to BigQuery", write);
    if (useIgnoreUnknownValues || consistentAutoUpdate) {
      // We ignore the extra fields, so no rows should have been sent to DLQ
      PAssert.that("Check DLQ is empty", result.getFailedStorageApiInserts()).empty();
    } else {
      // When we don't set ignoreUnknownValues, the rows with extra fields should be sent to DLQ.
      PAssert.that(
              String.format("Check DLQ has %s schema errors", TOTAL_N - ORIGINAL_N),
              result.getFailedStorageApiInserts())
          .satisfies(new VerifyPCollectionSize(TOTAL_N - ORIGINAL_N, extraField));
    }
    p.run().waitUntilFinish();

    // Check row completeness, non-duplication, and that schema update works as intended.
    int expectedCount = (useIgnoreUnknownValues || consistentAutoUpdate) ? TOTAL_N : ORIGINAL_N;
    boolean checkNoDuplication = (method == Write.Method.STORAGE_WRITE_API);
    verifyTable(
        tableSpec,
        expectedCount,
        checkNoDuplication,
        // The extra-field checks should only be performed when ignoreUnknownValues is set.
        (useIgnoreUnknownValues || consistentAutoUpdate) ? extraField : null,
        useAutoSchemaUpdate || consistentAutoUpdate);
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
      String expectedError = "SchemaTooNarrowException";
      for (BigQueryStorageApiInsertError err : input) {
        itemList.add(err);
        // Check the error message is due to schema mismatch from the extra field.
        assertTrue(
            String.format(
                "Didn't find expected [%s] error in failed message: %s", expectedError, err),
            err.getErrorMessage().contains(expectedError));
        assertTrue(
            String.format(
                "Didn't find expected [%s] schema field in failed message: %s", expectedError, err),
            err.getErrorMessage().contains(extraField));
      }
      // Check we have the expected number of rows in DLQ.
      // Should be equal to number of rows with extra fields.
      LOG.info("Found {} failed rows in DLQ", itemList.size());
      assertEquals(expectedSize, itemList.size());
      return null;
    }
  }

  // Fetches the table's rows once and runs every verification against that single snapshot:
  //   - the expected number of rows reached the table (and, if using STORAGE_WRITE_API, that no
  //     duplication happened), and
  //   - if extraField is non-null, that the extra field is present exactly on the rows that are
  //     expected to have it.
  // These used to be two separate queries, but every BigqueryClient#queryUnflattened call creates
  // and then deletes a temporary dataset, which costs several seconds.
  private void verifyTable(
      String tableSpec,
      int expectedCount,
      boolean checkNoDuplication,
      @Nullable String extraField,
      boolean useAutoSchemaUpdate)
      throws IOException, InterruptedException {
    List<TableRow> actualRows =
        BQ_CLIENT.queryUnflattened(
            String.format("SELECT * FROM [%s]", tableSpec), PROJECT, true, false, bigQueryLocation);

    Set<String> distinctIds = new HashSet<>();
    for (TableRow row : actualRows) {
      distinctIds.add((String) row.get("id"));
    }
    int distinctCount = distinctIds.size();
    int totalCount = actualRows.size();

    LOG.info("total distinct count = {}, total count = {}", distinctCount, totalCount);

    assertEquals(expectedCount, distinctCount);
    if (checkNoDuplication) {
      assertEquals(distinctCount, totalCount);
    }

    if (extraField != null) {
      checkRowsWithUpdatedSchema(actualRows, extraField, useAutoSchemaUpdate);
    }
  }

  // Performs checks on the table's rows under different conditions.
  // Note: these should only be performed when ignoreUnknownValues is set.
  private void checkRowsWithUpdatedSchema(
      List<TableRow> actualRows, String extraField, boolean useAutoSchemaUpdate) {
    for (TableRow row : actualRows) {
      // Rows written to the table should not have the extra field if
      // 1. The row has original schema
      // 2. We didn't set autoSchemaUpdate (the extra field would just be dropped)
      // 3. We didn't change the table's schema (again, the extra field would be dropped)
      if (Integer.parseInt((String) row.get("id")) < ORIGINAL_N
          || !useAutoSchemaUpdate
          || !changeTableSchema) {
        assertNull(
            String.format("Expected row to NOT have field %s:\n%s", extraField, row),
            row.get(extraField));
      } else {
        assertNotNull(
            String.format("Expected row to have field %s:\n%s", extraField, row),
            row.get(extraField));
      }
    }
  }

  @Test
  public void testExactlyOnce() throws Exception {
    runStreamingPipelineWithSchemaChange(
        Write.Method.STORAGE_WRITE_API,
        /** autoSchemaUpdate */
        false,
        false,
        /** ignoreUnknownvalues */
        false);
  }

  @Test
  public void testExactlyOnceWithIgnoreUnknownValues() throws Exception {
    runStreamingPipelineWithSchemaChange(Write.Method.STORAGE_WRITE_API, false, false, true);
  }

  @Test
  public void testExactlyOnceWithAutoSchemaUpdate() throws Exception {
    runStreamingPipelineWithSchemaChange(Write.Method.STORAGE_WRITE_API, true, false, true);
  }

  @Test
  public void testExactlyOnceWithAutoSchemaUpdateConsistent() throws Exception {
    runStreamingPipelineWithSchemaChange(Write.Method.STORAGE_WRITE_API, false, true, true);
  }

  @Test
  public void testAtLeastOnce() throws Exception {
    runStreamingPipelineWithSchemaChange(
        Write.Method.STORAGE_API_AT_LEAST_ONCE, false, false, false);
  }

  @Test
  public void testAtLeastOnceWithIgnoreUnknownValues() throws Exception {
    runStreamingPipelineWithSchemaChange(
        Write.Method.STORAGE_API_AT_LEAST_ONCE, false, false, true);
  }

  @Test
  public void testAtLeastOnceWithAutoSchemaUpdate() throws Exception {
    runStreamingPipelineWithSchemaChange(Write.Method.STORAGE_API_AT_LEAST_ONCE, true, false, true);
  }

  @Test
  public void testAtLeastOnceWithAutoSchemaUpdateConsistent() throws Exception {
    runStreamingPipelineWithSchemaChange(Write.Method.STORAGE_API_AT_LEAST_ONCE, false, true, true);
  }

  public void runDynamicDestinationsWithAutoSchemaUpdate(boolean useAtLeastOnce) throws Exception {
    Pipeline p = Pipeline.create(TestPipeline.testingPipelineOptions());
    // 0 threshold so that the stream tries fetching an updated schema after each append
    p.getOptions().as(BigQueryOptions.class).setStorageApiAppendThresholdBytes(0);
    p.getOptions().as(BigQueryOptions.class).setStorageApiMismatchRetryTimeMilliSec(20);
    // Total streams per destination
    p.getOptions()
        .as(BigQueryOptions.class)
        .setNumStorageWriteApiStreams(TOTAL_NUM_STREAMS / NUM_DESTINATIONS);
    // Need to manually enable streaming engine for legacy dataflow runner
    ExperimentalOptions.addExperiment(
        p.getOptions().as(ExperimentalOptions.class), GcpOptions.STREAMING_ENGINE_EXPERIMENT);
    // Skipping dynamic destinations tests on Dataflow because of timing issues
    // These tests are more stable on the DirectRunner, where timing is less variable
    assumeFalse(
        "Skipping dynamic destinations tests on Dataflow because of timing issues",
        p.getOptions().getRunner().getName().contains("DataflowRunner"));

    List<String> fieldNamesOrigin = new ArrayList<String>(Arrays.asList(FIELDS));

    // Shuffle the fields in the write schema to do fuzz testing on field order
    List<String> fieldNamesShuffled = new ArrayList<String>(fieldNamesOrigin);
    Collections.shuffle(fieldNamesShuffled, randomGenerator);
    TableSchema bqTableSchema = makeTableSchemaFromTypes(fieldNamesOrigin, null);
    TableSchema inputSchema = makeTableSchemaFromTypes(fieldNamesShuffled, null);

    Map<Long, String> destinations = new HashMap<>(NUM_DESTINATIONS);
    Map<String, TableSchema> updatedSchemas = new HashMap<>(NUM_DESTINATIONS);
    Map<String, String> extraFields = new HashMap<>(NUM_DESTINATIONS);
    Map<Long, GenerateRowFunc> rowFuncs = new HashMap<>(NUM_DESTINATIONS);
    for (int i = 0; i < NUM_DESTINATIONS; i++) {
      // The updated schema includes all fields in the original schema plus a random new field
      List<String> fieldNamesWithExtra = new ArrayList<String>(fieldNamesOrigin);
      String extraField =
          fieldNamesOrigin.get(randomGenerator.nextInt(fieldNamesOrigin.size())) + "_EXTRA";
      fieldNamesWithExtra.add(extraField);
      TableSchema updatedSchema =
          makeTableSchemaFromTypes(fieldNamesWithExtra, ImmutableSet.of(extraField));
      GenerateRowFunc generateRowFunc = new GenerateRowFunc(fieldNamesOrigin, fieldNamesWithExtra);

      String tableId = createTable(bqTableSchema, "_dynamic_" + i);
      String tableSpec = PROJECT + ":" + bigQueryDatasetId + "." + tableId;

      rowFuncs.put((long) i, generateRowFunc);
      destinations.put((long) i, tableSpec);
      updatedSchemas.put(tableId, updatedSchema);
      extraFields.put(tableSpec, extraField);
    }

    // build write transform
    Write<TableRow> write =
        BigQueryIO.writeTableRows()
            .to(
                row -> {
                  long l = (int) row.getValue().get("id") % NUM_DESTINATIONS;
                  String destination = destinations.get(l);
                  return new TableDestination(destination, null);
                })
            .withAutoSchemaUpdate(true)
            .ignoreUnknownValues()
            .withMethod(Write.Method.STORAGE_API_AT_LEAST_ONCE)
            .withCreateDisposition(CreateDisposition.CREATE_NEVER)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND);
    if (useInputSchema) {
      write = write.withSchema(inputSchema);
    }
    if (!useAtLeastOnce) {
      write =
          write
              .withMethod(Write.Method.STORAGE_WRITE_API)
              .withTriggeringFrequency(
                  Duration.standardSeconds(changeTableSchema ? LONG_WAIT_SECONDS : 1));
    }

    int numRows = TOTAL_N;
    // set up and build pipeline
    Instant start = new Instant(0);
    // We give a healthy waiting period between each element to give Storage API streams a chance to
    // recognize the new schema. Apply on relevant tests.
    Duration interval = Duration.millis(1);
    Duration stop = Duration.millis(numRows - 1);
    Function<Instant, Long> getIdFromInstant =
        (Function<Instant, Long> & Serializable) Instant::getMillis;

    // Generates rows with original schema up for row IDs under ORIGINAL_N
    // Then generates rows with updated schema for the rest
    // Rows with updated schema should only reach the table if ignoreUnknownValues is set,
    // and the extra field should be present only when autoSchemaUpdate is set
    PCollection<Instant> instants =
        p.apply(
            "Generate Instants",
            PeriodicImpulse.create()
                .startAt(start)
                .stopAt(start.plus(stop))
                .withInterval(interval)
                .catchUpToNow(false));
    PCollection<TableRow> rows =
        instants.apply(
            "Create TableRows",
            MapElements.into(TypeDescriptor.of(TableRow.class))
                .via(
                    instant -> {
                      long rowId = getIdFromInstant.apply(instant);
                      long dest = rowId % NUM_DESTINATIONS;
                      return rowFuncs.get(dest).apply(rowId);
                    }));
    if (changeTableSchema) {
      rows =
          rows
              // UpdateSchemaDoFn uses state, so need to have a KV input
              .apply("Add a dummy key", WithKeys.of(1))
              .apply(
                  "Update Schema",
                  ParDo.of(
                      new UpdateSchemaDoFn(
                          PROJECT, bigQueryDatasetId, updatedSchemas, true, true)));
    }

    WriteResult result = rows.apply("Stream to BigQuery", write);
    // We ignore the extra fields, so no rows should have been sent to DLQ
    PAssert.that("Check DLQ is empty", result.getFailedStorageApiInserts()).empty();
    p.run().waitUntilFinish();

    Map<String, Integer> expectedCounts = new HashMap<>(NUM_DESTINATIONS);
    for (int i = 0; i < numRows; i++) {
      long mod = i % NUM_DESTINATIONS;
      String destination = destinations.get(mod);
      expectedCounts.merge(destination, 1, Integer::sum);
    }

    for (Map.Entry<String, Integer> expectedCount : expectedCounts.entrySet()) {
      String dest = expectedCount.getKey();
      verifyTable(dest, expectedCount.getValue(), true, extraFields.get(dest), true);
    }
  }

  @Test
  public void testExactlyOnceDynamicDestinationsWithAutoSchemaUpdate() throws Exception {
    runDynamicDestinationsWithAutoSchemaUpdate(false);
  }

  @Test
  public void testAtLeastOnceDynamicDestinationsWithAutoSchemaUpdate() throws Exception {
    runDynamicDestinationsWithAutoSchemaUpdate(true);
  }
}
