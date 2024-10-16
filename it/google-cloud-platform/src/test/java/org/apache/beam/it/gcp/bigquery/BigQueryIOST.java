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

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.IOStressTestBase;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.synthetic.SyntheticSourceOptions;
import org.apache.beam.sdk.io.synthetic.SyntheticUnboundedSource;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.testutils.publishing.InfluxDBSettings;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/**
 * BigQueryIO stress tests. The test is designed to assess the performance of BigQueryIO under
 * various conditions.
 *
 * <p>Usage: <br>
 * - To run medium-scale stress tests: {@code gradle
 * :it:google-cloud-platform:BigQueryStressTestMedium} <br>
 * - To run large-scale stress tests: {@code gradle
 * :it:google-cloud-platform:BigQueryStressTestLarge}
 */
public final class BigQueryIOST extends IOStressTestBase {

  private static final String READ_ELEMENT_METRIC_NAME = "read_count";
  private static final String STORAGE_WRITE_API_METHOD = "STORAGE_WRITE_API";
  private static final String STORAGE_API_AT_LEAST_ONCE_METHOD = "STORAGE_API_AT_LEAST_ONCE";
  private static final int STORAGE_API_AT_LEAST_ONCE_MAX_ALLOWED_DIFFERENCE = 10_000;

  private static BigQueryResourceManager resourceManager;
  private static String tableName;
  private static String tableQualifier;
  private static InfluxDBSettings influxDBSettings;

  private Configuration configuration;
  private String tempLocation;
  private String testConfigName;
  private TableSchema schema;

  @Rule public TestPipeline writePipeline = TestPipeline.create();

  @BeforeClass
  public static void beforeClass() {
    resourceManager =
        BigQueryResourceManager.builder("io-bigquery-lt", project, CREDENTIALS).build();
    resourceManager.createDataset(region);
  }

  @Before
  public void setup() {
    // generate a random table name
    tableName =
        "io-bq-source-table-"
            + DateTimeFormatter.ofPattern("MMddHHmmssSSS")
                .withZone(ZoneId.of("UTC"))
                .format(java.time.Instant.now())
            + UUID.randomUUID().toString().substring(0, 10);
    tableQualifier = String.format("%s:%s.%s", project, resourceManager.getDatasetId(), tableName);

    // parse configuration
    testConfigName =
        TestProperties.getProperty("configuration", "medium", TestProperties.Type.PROPERTY);
    configuration = TEST_CONFIGS_PRESET.get(testConfigName);
    if (configuration == null) {
      try {
        configuration = Configuration.fromJsonString(testConfigName, Configuration.class);
      } catch (IOException e) {
        throw new IllegalArgumentException(
            String.format(
                "Unknown test configuration: [%s]. Pass to a valid configuration json, or use"
                    + " config presets: %s",
                testConfigName, TEST_CONFIGS_PRESET.keySet()));
      }
    }

    // prepare schema
    List<TableFieldSchema> fields = new ArrayList<>(configuration.numColumns);
    for (int idx = 0; idx < configuration.numColumns; ++idx) {
      fields.add(new TableFieldSchema().setName("data_" + idx).setType("BYTES"));
    }
    schema = new TableSchema().setFields(fields);

    // tempLocation needs to be set for bigquery IO writes
    if (!Strings.isNullOrEmpty(tempBucketName)) {
      tempLocation = String.format("gs://%s/temp/", tempBucketName);
      writePipeline.getOptions().as(TestPipelineOptions.class).setTempRoot(tempLocation);
      writePipeline.getOptions().setTempLocation(tempLocation);
    }
    if (configuration.exportMetricsToInfluxDB) {
      configuration.influxHost =
          TestProperties.getProperty("influxHost", "", TestProperties.Type.PROPERTY);
      configuration.influxDatabase =
          TestProperties.getProperty("influxDatabase", "", TestProperties.Type.PROPERTY);
      configuration.influxMeasurement =
          TestProperties.getProperty("influxMeasurement", "", TestProperties.Type.PROPERTY);
    }
  }

  @AfterClass
  public static void tearDownClass() {
    ResourceManagerUtils.cleanResources(resourceManager);
  }

  private static final Map<String, Configuration> TEST_CONFIGS_PRESET;

  static {
    try {
      TEST_CONFIGS_PRESET =
          ImmutableMap.of(
              "medium",
              Configuration.fromJsonString(
                  "{\"numColumns\":5,\"rowsPerSecond\":25000,\"minutes\":30,\"numRecords\":2500000,\"valueSizeBytes\":1000,\"pipelineTimeout\":60,\"runner\":\"DataflowRunner\"}",
                  Configuration.class),
              "large",
              Configuration.fromJsonString(
                  "{\"numColumns\":10,\"rowsPerSecond\":50000,\"minutes\":60,\"numRecords\":10000000,\"valueSizeBytes\":1000,\"pipelineTimeout\":120,\"runner\":\"DataflowRunner\"}",
                  Configuration.class));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testAvroStorageAPIWrite() throws IOException {
    configuration.writeFormat = WriteFormat.AVRO.name();
    configuration.writeMethod = STORAGE_WRITE_API_METHOD;
    runTest();
  }

  @Test
  public void testJsonStorageAPIWrite() throws IOException {
    configuration.writeFormat = WriteFormat.JSON.name();
    configuration.writeMethod = STORAGE_WRITE_API_METHOD;
    runTest();
  }

  @Test
  public void testAvroStorageAPIAtLeastOnce() throws IOException {
    configuration.writeFormat = WriteFormat.AVRO.name();
    configuration.writeMethod = STORAGE_API_AT_LEAST_ONCE_METHOD;
    runTest();
  }

  @Test
  public void testJsonStorageAPIAtLeastOnce() throws IOException {
    configuration.writeFormat = WriteFormat.JSON.name();
    configuration.writeMethod = STORAGE_API_AT_LEAST_ONCE_METHOD;
    runTest();
  }

  /**
   * Runs a stress test for BigQueryIO based on the specified configuration parameters. The method
   * initializes the stress test by determining the WriteFormat, configuring the BigQueryIO. Write
   * instance accordingly, and then executing data generation and read/write operations on BigQuery.
   */
  private void runTest() throws IOException {
    if (configuration.exportMetricsToInfluxDB) {
      influxDBSettings =
          InfluxDBSettings.builder()
              .withHost(configuration.influxHost)
              .withDatabase(configuration.influxDatabase)
              .withMeasurement(
                  configuration.influxMeasurement
                      + "_"
                      + testConfigName
                      + "_"
                      + configuration.writeFormat
                      + "_"
                      + configuration.writeMethod)
              .get();
    }
    WriteFormat writeFormat = WriteFormat.valueOf(configuration.writeFormat);
    BigQueryIO.Write<byte[]> writeIO = null;
    switch (writeFormat) {
      case AVRO:
        writeIO =
            BigQueryIO.<byte[]>write()
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withSuccessfulInsertsPropagation(false)
                .withoutValidation()
                .optimizedWrites()
                .withAvroFormatFunction(
                    new AvroFormatFn(
                        configuration.numColumns,
                        !(STORAGE_WRITE_API_METHOD.equalsIgnoreCase(configuration.writeMethod)
                            || STORAGE_API_AT_LEAST_ONCE_METHOD.equalsIgnoreCase(
                                configuration.writeMethod))));
        break;
      case JSON:
        writeIO =
            BigQueryIO.<byte[]>write()
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withSuccessfulInsertsPropagation(false)
                .withoutValidation()
                .optimizedWrites()
                .withFormatFunction(new JsonFormatFn(configuration.numColumns));
        break;
    }
    if (configuration.writeMethod.equals(STORAGE_WRITE_API_METHOD)) {
      writeIO = writeIO.withTriggeringFrequency(org.joda.time.Duration.standardSeconds(60));
    }
    generateDataAndWrite(writeIO);
  }

  /**
   * The method creates a pipeline to simulate data generation and write operations to BigQuery
   * table, based on the specified configuration parameters. The stress test involves varying the
   * load dynamically over time, with options to use configurable parameters.
   */
  private void generateDataAndWrite(BigQueryIO.Write<byte[]> writeIO) throws IOException {
    BigQueryIO.Write.Method method = BigQueryIO.Write.Method.valueOf(configuration.writeMethod);
    writePipeline.getOptions().as(StreamingOptions.class).setStreaming(true);

    // Each element from PeriodicImpulse will fan out to this many elements:
    int startMultiplier =
        Math.max(configuration.rowsPerSecond, DEFAULT_ROWS_PER_SECOND) / DEFAULT_ROWS_PER_SECOND;
    List<LoadPeriod> loadPeriods =
        getLoadPeriods(configuration.minutes, DEFAULT_LOAD_INCREASE_ARRAY);

    PCollection<KV<byte[], byte[]>> source =
        writePipeline.apply(Read.from(new SyntheticUnboundedSource(configuration)));
    if (startMultiplier > 1) {
      source =
          source
              .apply(
                  "One input to multiple outputs",
                  ParDo.of(new MultiplierDoFn<>(startMultiplier, loadPeriods)))
              .apply("Reshuffle fanout", Reshuffle.of());
    }
    source
        .apply("Extract values", Values.create())
        .apply("Counting element", ParDo.of(new CountingFn<>(READ_ELEMENT_METRIC_NAME)))
        .apply(
            "Write to BQ",
            writeIO
                .to(tableQualifier)
                .withMethod(method)
                .withSchema(schema)
                .withCustomGcsTempLocation(ValueProvider.StaticValueProvider.of(tempLocation)));

    String experiments =
        configuration.writeMethod.equals(STORAGE_API_AT_LEAST_ONCE_METHOD)
            ? GcpOptions.STREAMING_ENGINE_EXPERIMENT + ",streaming_mode_at_least_once"
            : GcpOptions.STREAMING_ENGINE_EXPERIMENT;

    PipelineLauncher.LaunchConfig options =
        PipelineLauncher.LaunchConfig.builder("write-bigquery")
            .setSdk(PipelineLauncher.Sdk.JAVA)
            .setPipeline(writePipeline)
            .addParameter("runner", configuration.runner)
            .addParameter(
                "autoscalingAlgorithm",
                DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.THROUGHPUT_BASED
                    .toString())
            .addParameter("numWorkers", String.valueOf(configuration.numWorkers))
            .addParameter("maxNumWorkers", String.valueOf(configuration.maxNumWorkers))
            .addParameter("experiments", experiments)
            .build();

    PipelineLauncher.LaunchInfo launchInfo = pipelineLauncher.launch(project, region, options);
    PipelineOperator.Result result =
        pipelineOperator.waitUntilDone(
            createConfig(launchInfo, Duration.ofMinutes(configuration.pipelineTimeout)));

    // Fail the test if pipeline failed.
    assertNotEquals(PipelineOperator.Result.LAUNCH_FAILED, result);

    // check metrics
    double numRecords =
        pipelineLauncher.getMetric(
            project,
            region,
            launchInfo.jobId(),
            getBeamMetricsName(PipelineMetricsType.COUNTER, READ_ELEMENT_METRIC_NAME));
    Long rowCount = resourceManager.getRowCount(tableName);

    // Depending on writing method there might be duplicates on different sides (read or write).
    if (configuration.writeMethod.equals(STORAGE_API_AT_LEAST_ONCE_METHOD)) {
      assertTrue(
          String.format(
              "Row difference (%d) exceeds the limit of %d. Rows: %d, Expected: %d",
              (long) numRecords - rowCount,
              STORAGE_API_AT_LEAST_ONCE_MAX_ALLOWED_DIFFERENCE,
              rowCount,
              (long) numRecords),
          (long) numRecords - rowCount <= STORAGE_API_AT_LEAST_ONCE_MAX_ALLOWED_DIFFERENCE);
    } else {
      assertTrue(
          String.format(
              "Number of rows in the table (%d) is greater than the expected number (%d).",
              rowCount, (long) numRecords),
          numRecords >= rowCount);
    }

    // export metrics
    MetricsConfiguration metricsConfig =
        MetricsConfiguration.builder()
            .setInputPCollection("Reshuffle fanout/ExpandIterable.out0")
            .setOutputPCollection("Counting element.out0")
            .build();
    exportMetrics(
        launchInfo, metricsConfig, configuration.exportMetricsToInfluxDB, influxDBSettings);
  }

  abstract static class FormatFn<InputT, OutputT> implements SerializableFunction<InputT, OutputT> {
    protected final int numColumns;

    public FormatFn(int numColumns) {
      this.numColumns = numColumns;
    }
  }

  /** Avro format function that transforms AvroWriteRequest<byte[]> into a GenericRecord. */
  private static class AvroFormatFn extends FormatFn<AvroWriteRequest<byte[]>, GenericRecord> {

    protected final boolean isWrapBytes;

    public AvroFormatFn(int numColumns, boolean isWrapBytes) {
      super(numColumns);
      this.isWrapBytes = isWrapBytes;
    }

    // TODO(https://github.com/apache/beam/issues/26408) eliminate this method once the Beam issue
    // resolved
    private Object maybeWrapBytes(byte[] input) {
      if (isWrapBytes) {
        return ByteBuffer.wrap(input);
      } else {
        return input;
      }
    }

    @Override
    public GenericRecord apply(AvroWriteRequest<byte[]> writeRequest) {
      byte[] data = Objects.requireNonNull(writeRequest.getElement());
      GenericRecord record = new GenericData.Record(writeRequest.getSchema());
      if (numColumns == 1) {
        // only one column, just wrap incoming bytes
        record.put("data_0", maybeWrapBytes(data));
      } else {
        // otherwise, distribute bytes
        int bytePerCol = data.length / numColumns;
        int curIdx = 0;
        for (int idx = 0; idx < numColumns - 1; ++idx) {
          record.put(
              "data_" + idx, maybeWrapBytes(Arrays.copyOfRange(data, curIdx, curIdx + bytePerCol)));
          curIdx += bytePerCol;
        }
        record.put(
            "data_" + (numColumns - 1),
            maybeWrapBytes(Arrays.copyOfRange(data, curIdx, data.length)));
      }
      return record;
    }
  }

  /** JSON format function that transforms byte[] into a TableRow. */
  private static class JsonFormatFn extends FormatFn<byte[], TableRow> {

    public JsonFormatFn(int numColumns) {
      super(numColumns);
    }

    @Override
    public TableRow apply(byte[] input) {
      TableRow tableRow = new TableRow();
      Base64.Encoder encoder = Base64.getEncoder();
      if (numColumns == 1) {
        // only one column, just wrap incoming bytes
        tableRow.set("data_0", encoder.encodeToString(input));
      } else {
        // otherwise, distribute bytes
        int bytePerCol = input.length / numColumns;
        int curIdx = 0;
        for (int idx = 0; idx < numColumns - 1; ++idx) {
          tableRow.set(
              "data_" + idx,
              encoder.encodeToString(Arrays.copyOfRange(input, curIdx, curIdx + bytePerCol)));
          curIdx += bytePerCol;
        }
        tableRow.set(
            "data_" + (numColumns - 1),
            encoder.encodeToString(Arrays.copyOfRange(input, curIdx, input.length)));
      }
      return tableRow;
    }
  }

  private enum WriteFormat {
    AVRO,
    JSON
  }

  /** Options for Bigquery IO stress test. */
  static class Configuration extends SyntheticSourceOptions {

    /**
     * Number of columns of each record. The column size is equally distributed as
     * valueSizeBytes/numColumns.
     */
    @JsonProperty public int numColumns = 1;

    /** Pipeline timeout in minutes. Must be a positive value. */
    @JsonProperty public int pipelineTimeout = 20;

    /** Runner specified to run the pipeline. */
    @JsonProperty public String runner = "DirectRunner";

    /** Number of workers for the pipeline. */
    @JsonProperty public int numWorkers = 20;

    /** Maximum number of workers for the pipeline. */
    @JsonProperty public int maxNumWorkers = 100;

    /** BigQuery write method: DEFAULT/FILE_LOADS/STREAMING_INSERTS/STORAGE_WRITE_API. */
    @JsonProperty public String writeMethod = "DEFAULT";

    /** BigQuery write format: AVRO/JSON. */
    @JsonProperty public String writeFormat = WriteFormat.AVRO.name();

    /**
     * Rate of generated elements sent to the source table. Will run with a minimum of 1k rows per
     * second.
     */
    @JsonProperty public int rowsPerSecond = DEFAULT_ROWS_PER_SECOND;

    /** Rows will be generated for this many minutes. */
    @JsonProperty public int minutes = 15;

    /**
     * Determines the destination for exporting metrics. If set to true, metrics will be exported to
     * InfluxDB and displayed using Grafana. If set to false, metrics will be exported to BigQuery
     * and displayed with Looker Studio.
     */
    @JsonProperty public boolean exportMetricsToInfluxDB = true;

    /** InfluxDB measurement to publish results to. * */
    @JsonProperty public String influxMeasurement = BigQueryIOST.class.getName();

    /** InfluxDB host to publish metrics. * */
    @JsonProperty public String influxHost;

    /** InfluxDB database to publish metrics. * */
    @JsonProperty public String influxDatabase;
  }
}
