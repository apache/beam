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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.Timestamp;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.time.Duration;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
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
import org.apache.beam.it.gcp.IOLoadTestBase;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.synthetic.SyntheticSourceOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.testutils.NamedTestResult;
import org.apache.beam.sdk.testutils.metrics.IOITMetrics;
import org.apache.beam.sdk.testutils.publishing.InfluxDBSettings;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.PeriodicImpulse;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Longs;
import org.joda.time.Instant;
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
public final class BigQueryIOST extends IOLoadTestBase {

  private static final String READ_ELEMENT_METRIC_NAME = "read_count";
  private static final String TEST_ID = UUID.randomUUID().toString();
  private static final String TEST_TIMESTAMP = Timestamp.now().toString();
  private static final int DEFAULT_ROWS_PER_SECOND = 1000;

  /**
   * The load will initiate at 1x, progressively increase to 2x and 4x, then decrease to 2x and
   * eventually return to 1x.
   */
  private static final int[] DEFAULT_LOAD_INCREASE_ARRAY = {1, 2, 2, 4, 2, 1};

  private static BigQueryResourceManager resourceManager;
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
    // generate a random table names
    String sourceTableName =
        "io-bq-source-table-"
            + DateTimeFormatter.ofPattern("MMddHHmmssSSS")
                .withZone(ZoneId.of("UTC"))
                .format(java.time.Instant.now())
            + UUID.randomUUID().toString().substring(0, 10);
    tableQualifier =
        String.format("%s:%s.%s", project, resourceManager.getDatasetId(), sourceTableName);

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
                  "{\"numColumns\":10,\"rowsPerSecond\":25000,\"minutes\":30,\"numRecords\":90000000,\"valueSizeBytes\":1000,\"pipelineTimeout\":60,\"runner\":\"DataflowRunner\"}",
                  Configuration.class),
              "large",
              Configuration.fromJsonString(
                  "{\"numColumns\":20,\"rowsPerSecond\":25000,\"minutes\":240,\"numRecords\":720000000,\"valueSizeBytes\":10000,\"pipelineTimeout\":300,\"runner\":\"DataflowRunner\"}",
                  Configuration.class));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testJsonStreamingWriteThenRead() throws IOException {
    configuration.writeFormat = "JSON";
    configuration.writeMethod = "STREAMING_INSERTS";
    runTest();
  }

  @Test
  public void testAvroStorageAPIWrite() throws IOException {
    configuration.writeFormat = "AVRO";
    configuration.writeMethod = "STORAGE_WRITE_API";
    runTest();
  }

  @Test
  public void testJsonStorageAPIWrite() throws IOException {
    configuration.writeFormat = "JSON";
    configuration.writeMethod = "STORAGE_WRITE_API";
    runTest();
  }

  @Test
  public void testAvroStorageAPIWriteAtLeastOnce() throws IOException {
    configuration.writeFormat = "AVRO";
    configuration.writeMethod = "STORAGE_API_AT_LEAST_ONCE";
    runTest();
  }

  @Test
  public void testJsonStorageAPIWriteAtLeastOnce() throws IOException {
    configuration.writeFormat = "JSON";
    configuration.writeMethod = "STORAGE_API_AT_LEAST_ONCE";
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
                .withTriggeringFrequency(org.joda.time.Duration.standardSeconds(30))
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withAvroFormatFunction(
                    new AvroFormatFn(
                        configuration.numColumns,
                        !("STORAGE_WRITE_API".equalsIgnoreCase(configuration.writeMethod))));
        break;
      case JSON:
        writeIO =
            BigQueryIO.<byte[]>write()
                .withTriggeringFrequency(org.joda.time.Duration.standardSeconds(30))
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withSuccessfulInsertsPropagation(false)
                .withFormatFunction(new JsonFormatFn(configuration.numColumns));
        break;
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

    // The PeriodicImpulse source will generate an element every this many millis:
    int fireInterval = 1;
    // Each element from PeriodicImpulse will fan out to this many elements:
    int startMultiplier =
        Math.max(configuration.rowsPerSecond, DEFAULT_ROWS_PER_SECOND) / DEFAULT_ROWS_PER_SECOND;
    long stopAfterMillis =
        org.joda.time.Duration.standardMinutes(configuration.minutes).getMillis();
    long totalRows = startMultiplier * stopAfterMillis / fireInterval;
    List<LoadPeriod> loadPeriods =
        getLoadPeriods(configuration.minutes, DEFAULT_LOAD_INCREASE_ARRAY);

    PCollection<byte[]> source =
        writePipeline
            .apply(
                PeriodicImpulse.create()
                    .stopAfter(org.joda.time.Duration.millis(stopAfterMillis - 1))
                    .withInterval(org.joda.time.Duration.millis(fireInterval)))
            .apply(
                "Extract row IDs",
                MapElements.into(TypeDescriptor.of(byte[].class))
                    .via(instant -> Longs.toByteArray(instant.getMillis() % totalRows)));
    if (startMultiplier > 1) {
      source =
          source
              .apply(
                  "One input to multiple outputs",
                  ParDo.of(new MultiplierDoFn(startMultiplier, loadPeriods)))
              .apply("Reshuffle fanout", Reshuffle.viaRandomKey())
              .apply("Counting element", ParDo.of(new CountingFn<>(READ_ELEMENT_METRIC_NAME)));
    }
    source.apply(
        "Write to BQ",
        writeIO
            .to(tableQualifier)
            .withMethod(method)
            .withSchema(schema)
            .withCustomGcsTempLocation(ValueProvider.StaticValueProvider.of(tempLocation)));

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
            .addParameter("experiments", GcpOptions.STREAMING_ENGINE_EXPERIMENT)
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
    Long rowCount = resourceManager.getRowCount(tableQualifier);
    assertEquals(rowCount, numRecords, 0.5);

    // export metrics
    MetricsConfiguration metricsConfig =
        MetricsConfiguration.builder()
            .setInputPCollection("Reshuffle fanout/Values/Values/Map.out0")
            .setInputPCollectionV2("Reshuffle fanout/Values/Values/Map/ParMultiDo(Anonymous).out0")
            .setOutputPCollection("Counting element.out0")
            .setOutputPCollectionV2("Counting element/ParMultiDo(Counting).out0")
            .build();
    try {
      Map<String, Double> metrics = getMetrics(launchInfo, metricsConfig);
      if (configuration.exportMetricsToInfluxDB) {
        Collection<NamedTestResult> namedTestResults = new ArrayList<>();
        for (Map.Entry<String, Double> entry : metrics.entrySet()) {
          NamedTestResult metricResult =
              NamedTestResult.create(TEST_ID, TEST_TIMESTAMP, entry.getKey(), entry.getValue());
          namedTestResults.add(metricResult);
        }
        IOITMetrics.publishToInflux(TEST_ID, TEST_TIMESTAMP, namedTestResults, influxDBSettings);
      } else {
        exportMetricsToBigQuery(launchInfo, metrics);
      }
    } catch (ParseException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Custom Apache Beam DoFn designed for use in stress testing scenarios. It introduces a dynamic
   * load increase over time, multiplying the input elements based on the elapsed time since the
   * start of processing. This class aims to simulate various load levels during stress testing.
   */
  private static class MultiplierDoFn extends DoFn<byte[], byte[]> {
    private final int startMultiplier;
    private final long startTimesMillis;
    private final List<LoadPeriod> loadPeriods;

    MultiplierDoFn(int startMultiplier, List<LoadPeriod> loadPeriods) {
      this.startMultiplier = startMultiplier;
      this.startTimesMillis = Instant.now().getMillis();
      this.loadPeriods = loadPeriods;
    }

    @ProcessElement
    public void processElement(
        @Element byte[] element,
        OutputReceiver<byte[]> outputReceiver,
        @DoFn.Timestamp Instant timestamp) {

      int multiplier = this.startMultiplier;
      long elapsedTimeMillis = timestamp.getMillis() - startTimesMillis;

      for (LoadPeriod loadPeriod : loadPeriods) {
        if (elapsedTimeMillis >= loadPeriod.getPeriodStartMillis()
            && elapsedTimeMillis < loadPeriod.getPeriodEndMillis()) {
          multiplier *= loadPeriod.getLoadIncreaseMultiplier();
          break;
        }
      }
      for (int i = 0; i < multiplier; i++) {
        outputReceiver.output(element);
      }
    }
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

  /**
   * Generates and returns a list of LoadPeriod instances representing periods of load increase
   * based on the specified load increase array and total duration in minutes.
   *
   * @param minutesTotal The total duration in minutes for which the load periods are generated.
   * @return A list of LoadPeriod instances defining periods of load increase.
   */
  private List<LoadPeriod> getLoadPeriods(int minutesTotal, int[] loadIncreaseArray) {

    List<LoadPeriod> loadPeriods = new ArrayList<>();
    long periodDurationMillis =
        Duration.ofMinutes(minutesTotal / loadIncreaseArray.length).toMillis();
    long startTimeMillis = 0;

    for (int loadIncreaseMultiplier : loadIncreaseArray) {
      long endTimeMillis = startTimeMillis + periodDurationMillis;
      loadPeriods.add(new LoadPeriod(loadIncreaseMultiplier, startTimeMillis, endTimeMillis));

      startTimeMillis = endTimeMillis;
    }
    return loadPeriods;
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
    @JsonProperty public String writeFormat = "AVRO";

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
    @JsonProperty public boolean exportMetricsToInfluxDB = false;

    /** InfluxDB measurement to publish results to. * */
    @JsonProperty public String influxMeasurement = BigQueryIOST.class.getName();

    /** InfluxDB host to publish metrics. * */
    @JsonProperty public String influxHost;

    /** InfluxDB database to publish metrics. * */
    @JsonProperty public String influxDatabase;
  }

  /**
   * Represents a period of time with associated load increase properties for stress testing
   * scenarios.
   */
  private static class LoadPeriod implements Serializable {
    private final int loadIncreaseMultiplier;
    private final long periodStartMillis;
    private final long periodEndMillis;

    public LoadPeriod(int loadIncreaseMultiplier, long periodStartMillis, long periodEndMin) {
      this.loadIncreaseMultiplier = loadIncreaseMultiplier;
      this.periodStartMillis = periodStartMillis;
      this.periodEndMillis = periodEndMin;
    }

    public int getLoadIncreaseMultiplier() {
      return loadIncreaseMultiplier;
    }

    public long getPeriodStartMillis() {
      return periodStartMillis;
    }

    public long getPeriodEndMillis() {
      return periodEndMillis;
    }
  }
}
