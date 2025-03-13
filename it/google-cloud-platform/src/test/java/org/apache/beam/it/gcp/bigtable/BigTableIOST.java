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
package org.apache.beam.it.gcp.bigtable;

import static org.apache.beam.it.gcp.bigtable.BigtableResourceManagerUtils.generateTableId;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.bigtable.v2.Mutation;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.IOStressTestBase;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.synthetic.SyntheticSourceOptions;
import org.apache.beam.sdk.io.synthetic.SyntheticUnboundedSource;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.testutils.publishing.InfluxDBSettings;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * BigTableIO stress test. The test is designed to assess the performance of BigTableIO under
 * various conditions.
 *
 * <p>Usage: <br>
 * - To run medium-scale stress tests: {@code gradle
 * :it:google-cloud-platform:BigTableStressTestMedium} - To run large-scale stress tests: {@code
 * gradle :it:google-cloud-platform:BigTableStressTestLarge}
 */
public final class BigTableIOST extends IOStressTestBase {

  private static final String WRITE_ELEMENT_METRIC_NAME = "write_count";
  private static final String READ_ELEMENT_METRIC_NAME = "read_count";
  private static final String COLUMN_FAMILY_NAME = "cf";
  private static final long TABLE_MAX_AGE_MINUTES = 800L;

  private BigtableResourceManager resourceManager;
  private InfluxDBSettings influxDBSettings;
  private Configuration configuration;
  private String testConfigName;
  private String tableId;

  @Rule public TestPipeline writePipeline = TestPipeline.create();
  @Rule public TestPipeline readPipeline = TestPipeline.create();

  @Before
  public void setup() throws IOException {
    resourceManager =
        BigtableResourceManager.builder(testName, project, CREDENTIALS_PROVIDER).build();

    // create table
    tableId = generateTableId(testName);
    resourceManager.createTable(
        tableId,
        ImmutableList.of(COLUMN_FAMILY_NAME),
        org.threeten.bp.Duration.ofMinutes(TABLE_MAX_AGE_MINUTES));

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

    // tempLocation needs to be set for DataflowRunner
    if (!Strings.isNullOrEmpty(tempBucketName)) {
      String tempLocation = String.format("gs://%s/temp/", tempBucketName);
      writePipeline.getOptions().as(TestPipelineOptions.class).setTempRoot(tempLocation);
      writePipeline.getOptions().setTempLocation(tempLocation);
      readPipeline.getOptions().as(TestPipelineOptions.class).setTempRoot(tempLocation);
      readPipeline.getOptions().setTempLocation(tempLocation);
    }
    // Use streaming pipeline to write records
    writePipeline.getOptions().as(StreamingOptions.class).setStreaming(true);

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
                  "{\"rowsPerSecond\":10000,\"minutes\":40,\"pipelineTimeout\":80,\"numRecords\":1000000,\"valueSizeBytes\":100,\"runner\":\"DataflowRunner\"}",
                  Configuration.class),
              "large",
              Configuration.fromJsonString(
                  "{\"rowsPerSecond\":50000,\"minutes\":60,\"pipelineTimeout\":120,\"numRecords\":10000000,\"valueSizeBytes\":1000,\"runner\":\"DataflowRunner\"}",
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

      // Assert that readNumRecords equals or greater than writeNumRecords since there might be
      // duplicates when testing big amount of data
      assertTrue(readNumRecords >= writeNumRecords);
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
   * The method creates a pipeline to simulate data generation and write operations to BigTable,
   * based on the specified configuration parameters. The stress test involves varying the load
   * dynamically over time, with options to use configurable parameters.
   */
  private PipelineLauncher.LaunchInfo generateDataAndWrite() throws IOException {
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
              .apply("Counting element", ParDo.of(new CountingFn<>(WRITE_ELEMENT_METRIC_NAME)));
    }
    source
        .apply(
            "Map records to BigTable format",
            ParDo.of(new MapToBigTableFormat((int) configuration.valueSizeBytes)))
        .apply(
            "Write to BigTable",
            BigtableIO.write()
                .withProjectId(project)
                .withInstanceId(resourceManager.getInstanceId())
                .withTableId(tableId));

    PipelineLauncher.LaunchConfig options =
        PipelineLauncher.LaunchConfig.builder("write-bigtable")
            .setSdk(PipelineLauncher.Sdk.JAVA)
            .setPipeline(writePipeline)
            .addParameter("runner", configuration.runner)
            .addParameter(
                "autoscalingAlgorithm",
                DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.THROUGHPUT_BASED
                    .toString())
            .addParameter("numWorkers", String.valueOf(configuration.numWorkers))
            .addParameter("maxNumWorkers", String.valueOf(configuration.maxNumWorkers))
            .addParameter("experiments", "use_runner_v2")
            .build();

    return pipelineLauncher.launch(project, region, options);
  }

  /** The method reads data from BigTable in batch mode. */
  private PipelineLauncher.LaunchInfo readData() throws IOException {
    BigtableIO.Read readIO =
        BigtableIO.read()
            .withoutValidation()
            .withProjectId(project)
            .withInstanceId(resourceManager.getInstanceId())
            .withTableId(tableId);

    readPipeline
        .apply("Read from BigTable", readIO)
        .apply("Counting element", ParDo.of(new CountingFn<>(READ_ELEMENT_METRIC_NAME)));

    PipelineLauncher.LaunchConfig options =
        PipelineLauncher.LaunchConfig.builder("read-bigtable")
            .setSdk(PipelineLauncher.Sdk.JAVA)
            .setPipeline(readPipeline)
            .addParameter("runner", configuration.runner)
            .addParameter(
                "autoscalingAlgorithm",
                DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.THROUGHPUT_BASED
                    .toString())
            .addParameter("numWorkers", String.valueOf(configuration.numWorkers))
            .addParameter("maxNumWorkers", String.valueOf(configuration.maxNumWorkers))
            .build();

    return pipelineLauncher.launch(project, region, options);
  }

  /** Options for BigTableIO stress test. */
  static class Configuration extends SyntheticSourceOptions {
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
    @JsonProperty public String influxMeasurement = BigTableIOST.class.getName();

    /** InfluxDB host to publish metrics. * */
    @JsonProperty public String influxHost;

    /** InfluxDB database to publish metrics. * */
    @JsonProperty public String influxDatabase;
  }

  /** Maps Instant to the BigTable format record. */
  private static class MapToBigTableFormat
      extends DoFn<KV<byte[], byte[]>, KV<ByteString, Iterable<Mutation>>> implements Serializable {

    private final int valueSizeBytes;

    public MapToBigTableFormat(int valueSizeBytes) {
      this.valueSizeBytes = valueSizeBytes;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      ByteBuffer byteBuffer = ByteBuffer.wrap(Objects.requireNonNull(c.element()).getValue());
      int index = byteBuffer.getInt();

      ByteString key =
          ByteString.copyFromUtf8(
              String.format(
                  "key%s",
                  index
                      + "-"
                      + UUID.randomUUID()
                      + "-"
                      + UUID.randomUUID()
                      + "-"
                      + org.joda.time.Instant.now().getMillis()));
      Random random = new Random(index);
      byte[] valBytes = new byte[this.valueSizeBytes];
      random.nextBytes(valBytes);
      ByteString value = ByteString.copyFrom(valBytes);

      Iterable<Mutation> mutations =
          ImmutableList.of(
              Mutation.newBuilder()
                  .setSetCell(
                      Mutation.SetCell.newBuilder()
                          .setValue(value)
                          .setTimestampMicros(java.time.Instant.now().toEpochMilli() * 1000L)
                          .setFamilyName(COLUMN_FAMILY_NAME))
                  .build());
      c.output(KV.of(key, mutations));
    }
  }
}
