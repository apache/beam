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
package org.apache.beam.it.gcp.bigquery;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.toTableReference;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.toTableSpec;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.google.api.core.ApiFuture;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.auto.value.AutoValue;
import com.google.cloud.bigquery.storage.v1.FlushRowsResponse;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.gcp.IOLoadTestBase;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServicesImpl;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.PeriodicImpulse;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Load test for the Storage Write API sink
 *
 * <p>This test is set up to first write rows using batch FILE_LOADS mode to a "source of truth"
 * table. Afterwards, it will write the same rows in streaming mode with Storage API to a second
 * table. Then it will query between these two tables to check that they are identical. There is
 * also the option of providing an existing table with the expected data, in which case the test
 * will skip the first step.
 *
 * <p>The throughput, length of test (in minutes), and data shape can be changed via pipeline
 * options. See the cases in `getOptions()` for examples.
 *
 * <p>This also includes the option of testing the sink's retry resilience by setting the
 * `crashIntervalSeconds` System property. This intentionally fails the worker or work item
 * periodically and expects the sink to recover appropriately. Note: Metrics are not published when
 * this is used.
 */
public class BigQueryStreamingLT extends IOLoadTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryStreamingLT.class);

  private static final BigqueryClient BQ_CLIENT = new BigqueryClient("BigQueryStreamingLT");
  private static final String BIG_QUERY_DATASET_ID =
      "storage_api_sink_load_test_" + System.nanoTime();

  private TestConfiguration config;
  private Integer crashIntervalSeconds;

  @Rule public final transient TestPipeline fileLoadsPipeline = TestPipeline.create();
  @Rule public final transient TestPipeline storageApiPipeline = TestPipeline.create();

  @BeforeClass
  public static void setUpTestClass() throws IOException, InterruptedException {
    PipelineOptionsFactory.register(TestPipelineOptions.class);
    BQ_CLIENT.createNewDataset(project, BIG_QUERY_DATASET_ID);
  }

  @Before
  public void setUpTest() {
    String testConfig =
        TestProperties.getProperty("configuration", "small", TestProperties.Type.PROPERTY);
    config = TEST_CONFIGS.get(testConfig);
    if (config == null) {
      throw new IllegalArgumentException(
          String.format(
              "Unknown test configuration: [%s]. Known configs: %s",
              testConfig, TEST_CONFIGS.keySet()));
    }
    // tempLocation needs to be set for file loads
    if (!Strings.isNullOrEmpty(tempBucketName)) {
      String tempLocation = String.format("gs://%s/temp/", tempBucketName);
      fileLoadsPipeline.getOptions().as(TestPipelineOptions.class).setTempRoot(tempLocation);
      fileLoadsPipeline.getOptions().setTempLocation(tempLocation);
    }

    // Set expected table if the property is provided,
    @Nullable
    String expectedTable =
        TestProperties.getProperty("expectedTable", "", TestProperties.Type.PROPERTY);
    if (!Strings.isNullOrEmpty(expectedTable)) {
      config.toBuilder().setExpectedTable(expectedTable).build();
    }

    crashIntervalSeconds =
        Integer.parseInt(
            TestProperties.getProperty("crashIntervalSeconds", "-1", TestProperties.Type.PROPERTY));
  }

  @AfterClass
  public static void cleanup() {
    BQ_CLIENT.deleteDataset(project, BIG_QUERY_DATASET_ID);
  }

  private static final Map<String, TestConfiguration> TEST_CONFIGS =
      ImmutableMap.of(
          "local", // 300K rows, >3 MB, 1K rows/s, >10KB/s
              TestConfiguration.of(5, 5, 2, 1_000, "DirectRunner", null),
          "small", // 600K rows, >30 MB, 1K rows/s, >50KB/s
              TestConfiguration.of(10, 10, 5, 1_000, "DataflowRunner", null),
          "medium", // 6M rows, >1.2 GB, 5K rows/s, >1MB/s
              TestConfiguration.of(20, 20, 10, 5_000, "DataflowRunner", null),
          "large", // 18M rows, >18 GB, 10K rows/s, >10MB/s
              TestConfiguration.of(30, 50, 20, 10_000, "DataflowRunner", null));

  /** Options for Bigquery IO Streaming load test. */
  @AutoValue
  abstract static class TestConfiguration {
    /** Rows will be generated for this many minutes. */
    abstract Integer getMinutes();

    /** Data shape: The byte-size for each field. */
    abstract Integer getByteSizePerField();

    /** Data shape: The number of fields per row. */
    abstract Integer getNumFields();

    /**
     * Rate of generated elements sent to the sink. Will run with a minimum of 1k rows per second.
     */
    abstract Integer getRowsPerSecond();

    abstract String getRunner();

    /**
     * The expected table to check against for correctness. If unset, the test will run a batch
     * FILE_LOADS job and use the resulting table as a source of truth.
     */
    abstract @Nullable String getExpectedTable();

    static TestConfiguration of(
        int numMin,
        int byteSizePerField,
        int numFields,
        int rowsPerSecond,
        String runner,
        @Nullable String expectedTable) {
      return new AutoValue_BigQueryStreamingLT_TestConfiguration.Builder()
          .setMinutes(numMin)
          .setByteSizePerField(byteSizePerField)
          .setNumFields(numFields)
          .setRowsPerSecond(rowsPerSecond)
          .setRunner(runner)
          .setExpectedTable(expectedTable)
          .build();
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setMinutes(int numMin);

      abstract Builder setByteSizePerField(int byteSizePerField);

      abstract Builder setNumFields(int numFields);

      abstract Builder setRowsPerSecond(int rowsPerSecond);

      abstract Builder setRunner(String runner);

      abstract Builder setExpectedTable(@Nullable String expectedTable);

      abstract TestConfiguration build();
    }

    abstract Builder toBuilder();
  }

  @Test
  public void testExactlyOnceStreaming() throws IOException, InterruptedException {
    runTest(BigQueryIO.Write.Method.STORAGE_WRITE_API);
  }

  @Test
  @Ignore
  public void testAtLeastOnceStreaming() throws IOException, InterruptedException {
    runTest(BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE);
  }

  public void runTest(BigQueryIO.Write.Method writeMethod)
      throws IOException, InterruptedException {
    long millis = Duration.standardMinutes(config.getMinutes()).getMillis();
    int rowsPerSecond = Math.max(config.getRowsPerSecond(), 1000);

    // The PeriodicImpulse source will generate an element every this many millis:
    int fireInterval = 1;
    // Each element from PeriodicImpulse will fan out to this many elements
    // (applicable when a high row-per-second rate is set)
    long multiplier = rowsPerSecond / 1000;
    long totalRows = multiplier * millis / fireInterval;
    // If we run with DataflowRunner and have not specified a positive crash duration for the sink,
    // this signifies a performance test, and so we publish metrics to a BigQuery dataset
    boolean publishMetrics =
        config.getRunner().equalsIgnoreCase(DataflowRunner.class.getSimpleName())
            && crashIntervalSeconds <= 0;

    String expectedTable = config.getExpectedTable();
    GenerateTableRow genRow =
        new GenerateTableRow(config.getNumFields(), config.getByteSizePerField());
    TableSchema schema = generateTableSchema(config.getNumFields());
    if (Strings.isNullOrEmpty(expectedTable)) {
      String fileLoadsDescription =
          String.format("fileloads-%s-records", withScaleSymbol(totalRows));
      expectedTable =
          String.format("%s.%s.%s", project, BIG_QUERY_DATASET_ID, fileLoadsDescription);
      LOG.info(
          "No expected table was set. Will run a batch job to load {} rows to {}."
              + " This will be used as the source of truth.",
          totalRows,
          expectedTable);

      fileLoadsPipeline
          .apply(GenerateSequence.from(0).to(totalRows))
          .apply(
              "Write to source of truth",
              BigQueryIO.<Long>write()
                  .to(expectedTable)
                  .withFormatFunction(genRow)
                  .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
                  .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                  .withSchema(schema));

      // If running on Dataflow, launch pipeline via launcher utils
      if (publishMetrics) {
        PipelineLauncher.LaunchConfig options =
            PipelineLauncher.LaunchConfig.builder("test-" + fileLoadsDescription)
                .setSdk(PipelineLauncher.Sdk.JAVA)
                .setPipeline(fileLoadsPipeline)
                .addParameter("runner", config.getRunner())
                .addParameter(
                    "maxNumWorkers",
                    TestProperties.getProperty("maxNumWorkers", "10", TestProperties.Type.PROPERTY))
                .build();

        // Don't use PipelineOperator because we don't want to wait on this batch job
        // The streaming job will run in parallel and it will take longer anyways; this job will
        // finish by then.
        pipelineLauncher.launch(project, region, options);
      } else {
        fileLoadsPipeline.run();
      }
    }

    String atLeastOnce =
        writeMethod == BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE ? "-atleastonce" : "";
    String storageApiDescription =
        String.format(
            "storageapi%s-load-%sqps-%smin-%stotal",
            atLeastOnce,
            withScaleSymbol(rowsPerSecond),
            config.getMinutes(),
            withScaleSymbol(totalRows));
    String destTable =
        String.format("%s.%s.%s", project, BIG_QUERY_DATASET_ID, storageApiDescription);
    LOG.info(
        "Preparing a source generating at a rate of {} rows per second for a period of {} minutes."
            + " This results in a total of {} rows written to {}.",
        rowsPerSecond,
        config.getMinutes(),
        totalRows,
        destTable);

    PCollection<Long> source =
        storageApiPipeline
            .apply(
                PeriodicImpulse.create()
                    .stopAfter(Duration.millis(millis - 1))
                    .withInterval(Duration.millis(fireInterval)))
            .apply(
                "Extract row IDs",
                MapElements.into(TypeDescriptors.longs())
                    .via(instant -> instant.getMillis() % totalRows));
    if (multiplier > 1) {
      source =
          source
              .apply(
                  String.format("One input to %s outputs", multiplier),
                  ParDo.of(new MultiplierDoFn(multiplier)))
              .apply("Reshuffle fanout", Reshuffle.viaRandomKey());
    }

    BigQueryIO.Write<Long> storageWriteTransform =
        BigQueryIO.<Long>write()
            .to(destTable)
            .withFormatFunction(genRow)
            .withMethod(writeMethod)
            .withTriggeringFrequency(Duration.standardSeconds(1))
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withSchema(schema);

    // If a crash interval is specified, use our crashing service implementation
    if (crashIntervalSeconds > 0) {
      LOG.info(
          "A crash interval of {} seconds has been set. The Storage API sink will periodically crash.",
          crashIntervalSeconds);
      storageWriteTransform =
          storageWriteTransform.withTestServices(
              new CrashingBigQueryServices(crashIntervalSeconds));
    }
    source.apply(storageWriteTransform);

    // If we're publishing metrics, launch pipeline via Dataflow launcher utils and export metrics
    if (publishMetrics) {
      // Set up dataflow job
      PipelineLauncher.LaunchConfig storageApiOptions =
          PipelineLauncher.LaunchConfig.builder("test-" + storageApiDescription)
              .setSdk(PipelineLauncher.Sdk.JAVA)
              .setPipeline(storageApiPipeline)
              .addParameter("runner", config.getRunner())
              .addParameter("streaming", "true")
              .addParameter("experiments", GcpOptions.STREAMING_ENGINE_EXPERIMENT)
              .addParameter(
                  "maxNumWorkers",
                  TestProperties.getProperty("maxNumWorkers", "10", TestProperties.Type.PROPERTY))
              .build();
      // Launch job
      PipelineLauncher.LaunchInfo storageApiInfo =
          pipelineLauncher.launch(project, region, storageApiOptions);
      // Wait until the streaming pipeline is finished and drained, get the result.
      PipelineOperator.Result storageApiResult =
          pipelineOperator.waitUntilDoneAndFinish(
              PipelineOperator.Config.builder()
                  .setJobId(storageApiInfo.jobId())
                  .setProject(project)
                  .setRegion(region)
                  .setTimeoutAfter(java.time.Duration.ofMinutes(config.getMinutes() * 2L))
                  .setCheckAfter(java.time.Duration.ofSeconds(config.getMinutes() * 60 / 20))
                  .build());
      // Check the initial launch didn't fail
      assertNotEquals(PipelineOperator.Result.LAUNCH_FAILED, storageApiResult);
      // Check that the pipeline succeeded
      assertEquals(
          PipelineLauncher.JobState.DONE,
          pipelineLauncher.getJobStatus(project, region, storageApiInfo.jobId()));

      // Export metrics
      MetricsConfiguration metricsConfig =
          MetricsConfiguration.builder()
              .setInputPCollection(
                  (multiplier > 1) ? "Extract row IDs.out0" : "Reshuffle fanout.out0")
              .build();
      try {
        exportMetricsToBigQuery(storageApiInfo, getMetrics(storageApiInfo, metricsConfig));
      } catch (Exception e) {
        // Just log the error. Don't re-throw because we have accuracy checks that are more
        // important below
        LOG.error("Encountered an error while exporting metrics to BigQuery:\n{}", e);
      }
    }
    // If we're not publishing metrics, just run the pipeline normally
    else {
      storageApiPipeline.run().waitUntilFinish();
    }

    LOG.info(
        "Write pipeline finished writing to {}. Will now perform accuracy checks against the rows in {}.",
        destTable,
        expectedTable);
    // Filter our structs and arrays because they are not supported when querying with `EXCEPT
    // DISTINCT`
    String columnNames =
        schema.getFields().stream()
            .map(TableFieldSchema::getName)
            .filter(fieldName -> fieldName.startsWith(FIELD_PREFIX))
            .collect(Collectors.joining(", "));
    checkCorrectness(columnNames, destTable, expectedTable);
    // check non-duplication for STORAGE_WRITE_API
    if (writeMethod == BigQueryIO.Write.Method.STORAGE_WRITE_API) {
      checkNonDuplication(destTable, expectedTable, totalRows);
    }
  }

  // A BigQueryServices class that is almost identical to BigQueryServicesImpl, except that
  // it returns a dataset service implementation that periodically crashes on flush()
  private static class CrashingBigQueryServices extends BigQueryServicesImpl {
    public final Integer crashIntervalSeconds;

    public CrashingBigQueryServices(Integer crashIntervalSeconds) {
      this.crashIntervalSeconds = crashIntervalSeconds;
    }

    @Override
    public WriteStreamService getWriteStreamService(BigQueryOptions options) {
      return new CrashingWriteStreamService(options);
    }

    private class CrashingWriteStreamService extends BigQueryServicesImpl.WriteStreamServiceImpl {
      private Instant lastCrash;

      public CrashingWriteStreamService(BigQueryOptions bqOptions) {
        super(bqOptions);
      }

      // We choose flush() to host the crash logic because it's called frequently during
      // the span of a Storage Write API pipeline
      @Override
      public ApiFuture<FlushRowsResponse> flush(String streamName, long flushOffset)
          throws IOException, InterruptedException {
        maybeCrash();
        return super.flush(streamName, flushOffset);
      }

      // When specified, crash when the interval is met by:
      // throwing an exception (failed work item) or
      // performing a System exit (worker failure)
      private void maybeCrash() {
        if (crashIntervalSeconds != -1) {
          Instant last = lastCrash;
          if (last == null) {
            lastCrash = Instant.now();
          } else if (Instant.now().isAfter(last.plusSeconds(crashIntervalSeconds))) {
            lastCrash = Instant.now();

            // Only crash 30% of the time (this is arbitrary)
            if (ThreadLocalRandom.current().nextInt(100) < 30) {
              // Half the time throw an exception (which fails this specific work item)
              // Other half crash the entire worker, which fails all work items on this worker
              if (ThreadLocalRandom.current().nextBoolean()) {
                throw new RuntimeException(
                    "Throwing a random exception! This is for testing retry resilience.");
              } else {
                LOG.error("Crashing this worker! This is for testing retry resilience.");
                System.exit(0);
              }
            }
          }
        }
      }
    }
  }

  public void checkCorrectness(String columnNames, String destTable, String expectedTable)
      throws IOException, InterruptedException {
    // Need table spec to be in the format `myproject.mydataset.mytable` to include in BQ queries.
    destTable = toTableSpec(toTableReference(destTable));
    expectedTable = toTableSpec(toTableReference(expectedTable));

    String checkCorrectnessQuery =
        String.format(
            "WITH \n"
                + "storage_api_table AS (SELECT %s FROM `%s`), \n"
                + "expected_table AS (SELECT %s FROM `%s`), \n"
                + "rows_mismatched AS (SELECT * FROM expected_table EXCEPT DISTINCT SELECT * FROM storage_api_table) \n"
                + "SELECT COUNT(*) FROM rows_mismatched",
            columnNames, destTable, columnNames, expectedTable);

    LOG.info("Executing query to check correctness:\n{}", checkCorrectnessQuery);

    TableRow queryResponse =
        Iterables.getOnlyElement(
            BQ_CLIENT.queryUnflattened(checkCorrectnessQuery, "google.com:clouddfe", true, true));
    long result = Long.parseLong((String) queryResponse.get("f0_"));

    LOG.info("Number of mismatched rows: {}", result);
    assertEquals(
        String.format("Saw %s rows that are missing from %s.", result, destTable), 0, result);
  }

  public void checkNonDuplication(String destTable, String expectedTable, long totalRows)
      throws IOException, InterruptedException {
    String checkDuplicationQuery =
        String.format(
            "SELECT \n"
                + "(SELECT COUNT(*) FROM  `%s`) AS actualCount,\n"
                + "(SELECT COUNT(*) FROM  `%s`) AS expectedCount",
            destTable, expectedTable);

    LOG.info("Executing query to check non-duplication:\n{}", checkDuplicationQuery);

    TableRow queryResponse =
        Iterables.getOnlyElement(
            BQ_CLIENT.queryUnflattened(checkDuplicationQuery, "google.com:clouddfe", true, true));
    long actualCount = Long.parseLong((String) queryResponse.get("actualCount"));
    long expectedCount = Long.parseLong((String) queryResponse.get("expectedCount"));
    assertEquals(
        "Comparing actual table count and expected table count.", expectedCount, actualCount);
    assertEquals(
        "Comparing actual table count and calculated expected count.", totalRows, actualCount);
  }

  // From a value, get the appropriate shortened name that includes the scale
  // For example, from 12,345,678 return 12M
  public String withScaleSymbol(long value) {
    List<String> scales = Arrays.asList("", "K", "M", "B", "T", "Q");
    int scaleIndex = 0;
    while (value / 1000 > 0) {
      scaleIndex++;
      value /= 1000;
    }

    return String.format("%s%s", value, scales.get(scaleIndex));
  }

  public static class MultiplierDoFn extends DoFn<Long, Long> {
    private long multiplier;

    MultiplierDoFn(long multiplier) {
      this.multiplier = multiplier;
    }

    @ProcessElement
    public void processElement(@Element Long element, OutputReceiver<Long> outputReceiver) {
      for (int i = 0; i < multiplier; i++) {
        outputReceiver.output(element);
      }
    }
  }

  static final String FIELD_PREFIX = "byte_field_";
  static final String RECORD_FIELD_PREFIX = "record_" + FIELD_PREFIX;
  static final String NESTED_FIELD_PREFIX = "nested_" + FIELD_PREFIX;
  static final String REPEATED_FIELD_PREFIX = "repeated_" + FIELD_PREFIX;

  public static TableSchema generateTableSchema(int numFields) {
    List<TableFieldSchema> fields = new ArrayList<>(numFields);
    fields.add(new TableFieldSchema().setType("INTEGER").setName("id"));
    int j = 1;
    for (int i = 1; i <= numFields; i++) {
      TableFieldSchema fieldSchema = new TableFieldSchema();
      // Every 4th field will be a struct, every 5th field will be an array
      if (j == 4) {
        fieldSchema
            .setType("RECORD")
            .setName(RECORD_FIELD_PREFIX + i)
            .setFields(
                Arrays.asList(
                    new TableFieldSchema().setType("BYTES").setName(NESTED_FIELD_PREFIX + 1),
                    new TableFieldSchema().setType("BYTES").setName(NESTED_FIELD_PREFIX + 2)));
      } else if (j == 5) {
        fieldSchema.setType("BYTES").setMode("REPEATED").setName(REPEATED_FIELD_PREFIX + i);
        j = 0;
      } else {
        fieldSchema.setType("BYTES").setName(FIELD_PREFIX + i);
      }
      j++;
      fields.add(fieldSchema);
    }
    return new TableSchema().setFields(fields);
  }

  static class GenerateTableRow implements SerializableFunction<Long, TableRow> {
    private final int numFields;
    private final int sizePerField;

    public GenerateTableRow(int numFields, int sizePerField) {
      assert numFields >= 0;
      this.numFields = numFields;
      this.sizePerField = sizePerField;
    }

    @Override
    public TableRow apply(Long rowId) {
      TableRow row = new TableRow();
      row.set("id", rowId);
      byte[] payload = getPayload(sizePerField, rowId).array();
      int j = 1;
      for (int i = 1; i <= numFields; i++) {
        // TODO: we can also make the struct and array sizes variable
        if (j == 4) {
          row.set(
              RECORD_FIELD_PREFIX + i,
              new TableRow()
                  .set(NESTED_FIELD_PREFIX + 1, Arrays.copyOfRange(payload, 0, sizePerField / 2))
                  .set(
                      NESTED_FIELD_PREFIX + 2,
                      Arrays.copyOfRange(payload, sizePerField / 2, sizePerField)));
        } else if (j == 5) {
          row.set(
              REPEATED_FIELD_PREFIX + i,
              Arrays.asList(
                  Arrays.copyOfRange(payload, 0, sizePerField / 3),
                  Arrays.copyOfRange(payload, sizePerField / 3, sizePerField * 2 / 3),
                  Arrays.copyOfRange(payload, sizePerField * 2 / 3, sizePerField)));
          j = 0;
        } else {
          row.set(FIELD_PREFIX + i, payload);
        }
        j++;
      }
      return row;
    }

    private @Nullable ByteBuffer getPayload(int payloadSize, long rowId) {
      if (payloadSize <= 0) {
        return null;
      }
      byte[] payload = new byte[payloadSize];
      Random localRandom = ThreadLocal.withInitial(() -> new Random(rowId)).get();
      localRandom.setSeed(rowId);
      localRandom.nextBytes(payload);

      return ByteBuffer.wrap(payload);
    }
  }
}
