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
package org.apache.beam.it.gcp.spanner;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Mutation;
import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.time.Duration;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.IOStressTestBase;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.synthetic.SyntheticSourceOptions;
import org.apache.beam.sdk.io.synthetic.SyntheticUnboundedSource;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testutils.publishing.InfluxDBSettings;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * SpannerIO stress test. The test is designed to assess the performance of SpannerIO under various
 * conditions.
 *
 * <p>Usage: <br>
 * - To run medium-scale stress tests: {@code gradle
 * :it:google-cloud-platform:SpannerStressTestMedium} <br>
 * - To run large-scale stress tests: {@code gradle
 * :it:google-cloud-platform:SpannerStressTestLarge}
 */
public class SpannerIOST extends IOStressTestBase {

  private static final String READ_ELEMENT_METRIC_NAME = "read_count";
  private static final String WRITE_ELEMENT_METRIC_NAME = "write_count";
  private static final int INSTANCE_NODE_COUNT = 30;

  private String tableName;
  private String testConfigName;
  private Configuration configuration;
  private InfluxDBSettings influxDBSettings;
  private SpannerResourceManager resourceManager;

  @Rule public TestPipeline writePipeline = TestPipeline.create();
  @Rule public TestPipeline readPipeline = TestPipeline.create();

  @Before
  public void setup() throws IOException {
    // generate a random table name
    tableName =
        "io_spanner_"
            + DateTimeFormatter.ofPattern("MMddHHmmssSSS")
                .withZone(ZoneId.of("UTC"))
                .format(java.time.Instant.now())
            + UUID.randomUUID().toString().replace("-", "").substring(0, 10);

    resourceManager =
        SpannerResourceManager.builder(testName, project, region, INSTANCE_NODE_COUNT).build();

    // parse configuration
    testConfigName =
        TestProperties.getProperty("configuration", "local", TestProperties.Type.PROPERTY);
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
    String createTable =
        createTableStatement(
            tableName, configuration.numColumns, (int) configuration.valueSizeBytes);
    // Create table
    resourceManager.executeDdlStatement(createTable);

    if (configuration.exportMetricsToInfluxDB) {
      configuration.influxHost =
          TestProperties.getProperty("influxHost", "", TestProperties.Type.PROPERTY);
      configuration.influxDatabase =
          TestProperties.getProperty("influxDatabase", "", TestProperties.Type.PROPERTY);
      configuration.influxMeasurement =
          TestProperties.getProperty("influxMeasurement", "", TestProperties.Type.PROPERTY);
    }
  }

  @After
  public void teardown() {
    ResourceManagerUtils.cleanResources(resourceManager);
  }

  private static final Map<String, Configuration> TEST_CONFIGS_PRESET;

  static {
    try {
      TEST_CONFIGS_PRESET =
          ImmutableMap.of(
              "medium",
              Configuration.fromJsonString(
                  "{\"numRecords\":1000000,\"rowsPerSecond\":25000,\"minutes\":10,\"valueSizeBytes\":1000,\"pipelineTimeout\":60,\"runner\":\"DataflowRunner\"}",
                  Configuration.class),
              "large",
              Configuration.fromJsonString(
                  "{\"numRecords\":20000000,\"rowsPerSecond\":50000,\"minutes\":60,\"valueSizeBytes\":1000,\"pipelineTimeout\":180,\"runner\":\"DataflowRunner\"}",
                  Configuration.class));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Run stress test with configurations specified by TestProperties. */
  @Test
  public void runTest() throws IOException, ParseException, InterruptedException {
    if (configuration.exportMetricsToInfluxDB) {
      influxDBSettings =
          InfluxDBSettings.builder()
              .withHost(configuration.influxHost)
              .withDatabase(configuration.influxDatabase)
              .withMeasurement(configuration.influxMeasurement + "_" + testConfigName)
              .get();
    }

    PipelineLauncher.LaunchInfo writeInfo = generateDataAndWrite();
    PipelineOperator.Result writeResult =
        pipelineOperator.waitUntilDone(
            createConfig(writeInfo, Duration.ofMinutes(configuration.pipelineTimeout)));
    assertNotEquals(PipelineOperator.Result.LAUNCH_FAILED, writeResult);

    PipelineLauncher.LaunchInfo readInfo = readData();
    PipelineOperator.Result readResult =
        pipelineOperator.waitUntilDone(
            createConfig(readInfo, Duration.ofMinutes(configuration.pipelineTimeout)));
    assertNotEquals(PipelineOperator.Result.LAUNCH_FAILED, readResult);

    try {
      double writeNumRecords =
          pipelineLauncher.getMetric(
              project,
              region,
              writeInfo.jobId(),
              getBeamMetricsName(PipelineMetricsType.COUNTER, WRITE_ELEMENT_METRIC_NAME));

      double readNumRecords =
          pipelineLauncher.getMetric(
              project,
              region,
              readInfo.jobId(),
              getBeamMetricsName(PipelineMetricsType.COUNTER, READ_ELEMENT_METRIC_NAME));

      // Assert that writeNumRecords equals or greater than readNumRecords since there might be
      // duplicates when testing big amount of data
      assertTrue(writeNumRecords >= readNumRecords);
    } finally {
      // clean up write streaming pipeline
      if (pipelineLauncher.getJobStatus(project, region, writeInfo.jobId())
          == PipelineLauncher.JobState.RUNNING) {
        pipelineLauncher.cancelJob(project, region, writeInfo.jobId());
      }
    }

    // export metrics
    MetricsConfiguration writeMetricsConfig =
        MetricsConfiguration.builder()
            .setInputPCollection("Map records.out0")
            .setInputPCollectionV2("Map records/ParMultiDo(GenerateMutations).out0")
            .setOutputPCollection("Counting element.out0")
            .setOutputPCollectionV2("Counting element/ParMultiDo(Counting).out0")
            .build();

    MetricsConfiguration readMetricsConfig =
        MetricsConfiguration.builder()
            .setOutputPCollection("Counting element.out0")
            .setOutputPCollectionV2("Counting element/ParMultiDo(Counting).out0")
            .build();

    exportMetrics(
        writeInfo, writeMetricsConfig, configuration.exportMetricsToInfluxDB, influxDBSettings);
    exportMetrics(
        readInfo, readMetricsConfig, configuration.exportMetricsToInfluxDB, influxDBSettings);
  }

  /**
   * The method creates a pipeline to simulate data generation and write operations to Spanner,
   * based on the specified configuration parameters. The stress test involves varying the load
   * dynamically over time, with options to use configurable parameters.
   */
  private PipelineLauncher.LaunchInfo generateDataAndWrite() throws IOException {
    int startMultiplier =
        Math.max(configuration.rowsPerSecond, DEFAULT_ROWS_PER_SECOND) / DEFAULT_ROWS_PER_SECOND;
    List<IOStressTestBase.LoadPeriod> loadPeriods =
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
        .apply(
            "Map records to Spanner format",
            ParDo.of(
                new GenerateMutations(
                    tableName, configuration.numColumns, (int) configuration.valueSizeBytes)))
        .apply("Counting element", ParDo.of(new CountingFn<>(WRITE_ELEMENT_METRIC_NAME)))
        .apply(
            "Write to Spanner",
            SpannerIO.write()
                .withProjectId(project)
                .withInstanceId(resourceManager.getInstanceId())
                .withDatabaseId(resourceManager.getDatabaseId())
                .withCommitDeadline(org.joda.time.Duration.standardMinutes(2)));

    PipelineLauncher.LaunchConfig options =
        PipelineLauncher.LaunchConfig.builder("write-spanner")
            .setSdk(PipelineLauncher.Sdk.JAVA)
            .setPipeline(writePipeline)
            .addParameter("runner", configuration.runner)
            .addParameter(
                "autoscalingAlgorithm",
                DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.THROUGHPUT_BASED
                    .toString())
            .addParameter("numWorkers", String.valueOf(configuration.numWorkers))
            .addParameter("maxNumWorkers", String.valueOf(configuration.maxNumWorkers))
            .addParameter("streaming", "true")
            .build();

    return pipelineLauncher.launch(project, region, options);
  }

  private PipelineLauncher.LaunchInfo readData() throws IOException {
    SpannerIO.Read readTrabsfirn =
        SpannerIO.read()
            .withProjectId(project)
            .withInstanceId(resourceManager.getInstanceId())
            .withDatabaseId(resourceManager.getDatabaseId())
            .withQuery(String.format("SELECT * FROM %s", tableName));

    readPipeline
        .apply("Read from Spanner", readTrabsfirn)
        .apply("Counting element", ParDo.of(new CountingFn<>(READ_ELEMENT_METRIC_NAME)));

    PipelineLauncher.LaunchConfig options =
        PipelineLauncher.LaunchConfig.builder("read-spanner")
            .setSdk(PipelineLauncher.Sdk.JAVA)
            .setPipeline(readPipeline)
            .addParameter("runner", configuration.runner)
            .build();

    return pipelineLauncher.launch(project, region, options);
  }

  /** Options for SpannerIO load test. */
  static class Configuration extends SyntheticSourceOptions {

    /**
     * Number of columns (besides the primary key) of each record. The column size is equally
     * distributed as valueSizeBytes/numColumns.
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
    @JsonProperty public String influxMeasurement = SpannerIOST.class.getName();

    /** InfluxDB host to publish metrics. * */
    @JsonProperty public String influxHost;

    /** InfluxDB database to publish metrics. * */
    @JsonProperty public String influxDatabase;
  }

  /**
   * Generate a create table sql statement with 1 integer column (Id) and additional numBytesCol
   * columns.
   */
  static String createTableStatement(String tableId, int numBytesCol, int valueSizeBytes) {
    int sizePerCol = valueSizeBytes / numBytesCol;
    StringBuilder statement = new StringBuilder();
    statement.append(String.format("CREATE TABLE %s (Id STRING(MAX)", tableId));
    for (int col = 0; col < numBytesCol; ++col) {
      statement.append(String.format(",\n COL%d BYTES(%d)", col + 1, sizePerCol));
    }
    statement.append(") PRIMARY KEY(Id)");
    return statement.toString();
  }

  /** Maps KV<byte[], byte[]> number to the Spanner format record. */
  private static class GenerateMutations extends DoFn<KV<byte[], byte[]>, Mutation>
      implements Serializable {
    private final String table;
    private final int numBytesCol;
    private final int sizePerCol;

    public GenerateMutations(String tableId, int numBytesCol, int valueSizeBytes) {
      checkArgument(valueSizeBytes >= numBytesCol);
      this.table = tableId;
      this.numBytesCol = numBytesCol;
      this.sizePerCol = valueSizeBytes / numBytesCol;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(table);
      String key = "key-" + UUID.randomUUID() + UUID.randomUUID() + Instant.now().getMillis();
      builder.set("Id").to(key);
      Random random = new Random();
      byte[] value = new byte[sizePerCol];
      for (int col = 0; col < numBytesCol; ++col) {
        String name = String.format("COL%d", col + 1);
        random.nextBytes(value);
        builder.set(name).to(ByteArray.copyFrom(value));
      }
      Mutation mutation = builder.build();
      c.output(mutation);
    }
  }
}
