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
package org.apache.beam.it.kafka;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.gcp.IOStressTestBase;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.synthetic.SyntheticSourceOptions;
import org.apache.beam.sdk.io.synthetic.SyntheticUnboundedSource;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.testutils.publishing.InfluxDBSettings;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * KafkaIO stress tests. The test is designed to assess the performance of KafkaIO under various
 * conditions. To run the test, a live remote Kafka broker is required. You can deploy Kafka within
 * a Kubernetes cluster following the example described here: {@link
 * .github/workflows/beam_PerformanceTests_Kafka_IO.yml} If you choose to use Kubernetes, it's
 * important to remember that each pod should have a minimum of 10GB memory allocated. Additionally,
 * when running the test, it's necessary to pass the addresses of Kafka bootstrap servers as an
 * argument.
 *
 * <p>Usage: <br>
 * - To run medium-scale stress tests: {@code gradle :it:kafka:KafkaStressTestMedium
 * -DbootstrapServers="0.0.0.0:32400,1.1.1.1:32400"} <br>
 * - To run large-scale stress tests: {@code gradle :it:kafka:KafkaStressTestLarge
 * -DbootstrapServers="0.0.0.0:32400,1.1.1.1:32400"}
 */
public final class KafkaIOST extends IOStressTestBase {
  private static final String WRITE_ELEMENT_METRIC_NAME = "write_count";
  private static final String READ_ELEMENT_METRIC_NAME = "read_count";
  private static InfluxDBSettings influxDBSettings;
  private Configuration configuration;
  private AdminClient adminClient;
  private String testConfigName;
  private String tempLocation;
  private String kafkaTopic;

  @Rule public TestPipeline writePipeline = TestPipeline.create();
  @Rule public TestPipeline readPipeline = TestPipeline.create();

  @Before
  public void setup() {
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
    configuration.bootstrapServers =
        TestProperties.getProperty("bootstrapServers", null, TestProperties.Type.PROPERTY);
    String useDataflowRunnerV2FromProps =
        TestProperties.getProperty("useDataflowRunnerV2", "true", TestProperties.Type.PROPERTY);
    if (!useDataflowRunnerV2FromProps.isEmpty()) {
      configuration.useDataflowRunnerV2 = Boolean.parseBoolean(useDataflowRunnerV2FromProps);
    }

    adminClient =
        AdminClient.create(ImmutableMap.of("bootstrap.servers", configuration.bootstrapServers));
    kafkaTopic =
        "io-kafka-"
            + DateTimeFormatter.ofPattern("MMddHHmmssSSS")
                .withZone(ZoneId.of("UTC"))
                .format(java.time.Instant.now())
            + UUID.randomUUID().toString().substring(0, 10);
    adminClient.createTopics(Collections.singletonList(new NewTopic(kafkaTopic, 1, (short) 3)));

    // tempLocation needs to be set for DataflowRunner
    if (!Strings.isNullOrEmpty(tempBucketName)) {
      tempLocation = String.format("gs://%s/temp/", tempBucketName);
      writePipeline.getOptions().as(TestPipelineOptions.class).setTempRoot(tempLocation);
      writePipeline.getOptions().setTempLocation(tempLocation);
      readPipeline.getOptions().as(TestPipelineOptions.class).setTempRoot(tempLocation);
      readPipeline.getOptions().setTempLocation(tempLocation);
    }
    // Use streaming pipeline to write and read records
    writePipeline.getOptions().as(StreamingOptions.class).setStreaming(true);
    readPipeline.getOptions().as(StreamingOptions.class).setStreaming(true);

    if (configuration.exportMetricsToInfluxDB) {
      configuration.influxHost =
          TestProperties.getProperty("influxHost", "", TestProperties.Type.PROPERTY);
      configuration.influxDatabase =
          TestProperties.getProperty("influxDatabase", "", TestProperties.Type.PROPERTY);
      configuration.influxMeasurement =
          TestProperties.getProperty("influxMeasurement", "", TestProperties.Type.PROPERTY);
    }
  }

  private static final Map<String, Configuration> TEST_CONFIGS_PRESET;

  static {
    try {
      TEST_CONFIGS_PRESET =
          ImmutableMap.of(
              "medium",
              Configuration.fromJsonString(
                  "{\"rowsPerSecond\":10000,\"numRecords\":1000000,\"valueSizeBytes\":1000,\"minutes\":20,\"pipelineTimeout\":60,\"runner\":\"DataflowRunner\"}",
                  Configuration.class),
              "large",
              Configuration.fromJsonString(
                  "{\"rowsPerSecond\":50000,\"numRecords\":5000000,\"valueSizeBytes\":1000,\"minutes\":60,\"pipelineTimeout\":240,\"runner\":\"DataflowRunner\"}",
                  Configuration.class));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Run stress test with configurations specified by TestProperties. */
  @Test
  public void testWriteAndRead() throws IOException, ParseException, InterruptedException {
    if (configuration.exportMetricsToInfluxDB) {
      influxDBSettings =
          InfluxDBSettings.builder()
              .withHost(configuration.influxHost)
              .withDatabase(configuration.influxDatabase)
              .withMeasurement(configuration.influxMeasurement + "_" + testConfigName)
              .get();
    }

    PipelineLauncher.LaunchInfo writeInfo = generateDataAndWrite();
    PipelineLauncher.LaunchInfo readInfo = readData();

    try {
      PipelineOperator.Result readResult =
          pipelineOperator.waitUntilDone(
              createConfig(readInfo, Duration.ofMinutes(configuration.pipelineTimeout)));
      assertNotEquals(PipelineOperator.Result.LAUNCH_FAILED, readResult);

      // Delete topic after test run
      adminClient.deleteTopics(Collections.singleton(kafkaTopic));

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
      // clean up pipelines
      if (pipelineLauncher.getJobStatus(project, region, writeInfo.jobId())
          == PipelineLauncher.JobState.RUNNING) {
        pipelineLauncher.cancelJob(project, region, writeInfo.jobId());
      }
      if (pipelineLauncher.getJobStatus(project, region, readInfo.jobId())
          == PipelineLauncher.JobState.RUNNING) {
        pipelineLauncher.cancelJob(project, region, readInfo.jobId());
      }
    }

    // export metrics
    MetricsConfiguration writeMetricsConfig =
        MetricsConfiguration.builder()
            .setInputPCollection("Reshuffle fanout/Values/Values/Map.out0")
            .setInputPCollectionV2("Reshuffle fanout/Values/Values/Map/ParMultiDo(Anonymous).out0")
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
   * The method creates a pipeline to simulate data generation and write operations to Kafka topic,
   * based on the specified configuration parameters. The stress test involves varying the load
   * dynamically over time, with options to use configurable parameters.
   */
  private PipelineLauncher.LaunchInfo generateDataAndWrite() throws IOException {
    int startMultiplier =
        Math.max(configuration.rowsPerSecond, DEFAULT_ROWS_PER_SECOND) / DEFAULT_ROWS_PER_SECOND;
    List<LoadPeriod> loadPeriods =
        getLoadPeriods(configuration.minutes, DEFAULT_LOAD_INCREASE_ARRAY);

    PCollection<byte[]> source =
        writePipeline
            .apply(Read.from(new SyntheticUnboundedSource(configuration)))
            .apply(
                "Extract values",
                MapElements.into(TypeDescriptor.of(byte[].class))
                    .via(kv -> Objects.requireNonNull(kv).getValue()));

    if (startMultiplier > 1) {
      source =
          source
              .apply(
                  "One input to multiple outputs",
                  ParDo.of(new MultiplierDoFn<>(startMultiplier, loadPeriods)))
              .apply("Counting element", ParDo.of(new CountingFn<>(WRITE_ELEMENT_METRIC_NAME)));
    }
    source.apply(
        "Write to Kafka",
        KafkaIO.<byte[], byte[]>write()
            .withBootstrapServers(configuration.bootstrapServers)
            .withTopic(kafkaTopic)
            .withValueSerializer(ByteArraySerializer.class)
            .withProducerConfigUpdates(
                ImmutableMap.of(
                    ProducerConfig.RETRIES_CONFIG, 10,
                    ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 600000,
                    ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 5000))
            .values());

    PipelineLauncher.LaunchConfig options =
        PipelineLauncher.LaunchConfig.builder("write-kafka")
            .setSdk(PipelineLauncher.Sdk.JAVA)
            .setPipeline(writePipeline)
            .addParameter("runner", configuration.runner)
            .addParameter(
                "autoscalingAlgorithm",
                DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.THROUGHPUT_BASED
                    .toString())
            .addParameter("numWorkers", String.valueOf(configuration.numWorkers))
            .addParameter("maxNumWorkers", String.valueOf(configuration.maxNumWorkers))
            .addParameter("experiments", configuration.useDataflowRunnerV2 ? "use_runner_v2" : "")
            .build();

    return pipelineLauncher.launch(project, region, options);
  }

  /** The method reads data from Kafka topic in streaming mode. */
  private PipelineLauncher.LaunchInfo readData() throws IOException {
    KafkaIO.Read<byte[], byte[]> readFromKafka =
        KafkaIO.readBytes()
            .withBootstrapServers(configuration.bootstrapServers)
            .withTopic(kafkaTopic)
            .withConsumerConfigUpdates(ImmutableMap.of("auto.offset.reset", "earliest"));

    readPipeline
        .apply("Read from Kafka", readFromKafka)
        .apply("Counting element", ParDo.of(new CountingFn<>(READ_ELEMENT_METRIC_NAME)));

    PipelineLauncher.LaunchConfig options =
        PipelineLauncher.LaunchConfig.builder("read-kafka")
            .setSdk(PipelineLauncher.Sdk.JAVA)
            .setPipeline(readPipeline)
            .addParameter("numWorkers", String.valueOf(configuration.numWorkers))
            .addParameter("runner", configuration.runner)
            .addParameter("experiments", configuration.useDataflowRunnerV2 ? "use_runner_v2" : "")
            .build();

    return pipelineLauncher.launch(project, region, options);
  }

  /** Options for Kafka IO stress test. */
  static class Configuration extends SyntheticSourceOptions {
    /** Pipeline timeout in minutes. Must be a positive value. */
    @JsonProperty public int pipelineTimeout = 20;

    /** Runner specified to run the pipeline. */
    @JsonProperty public String runner = "DirectRunner";

    /**
     * Determines whether to use Dataflow runner v2. If set to true, it uses SDF mode for reading
     * from Kafka. Otherwise, Unbounded mode will be used.
     */
    @JsonProperty public boolean useDataflowRunnerV2 = false;

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

    /** Kafka bootstrap server addresses. */
    @JsonProperty public String bootstrapServers;

    /**
     * Determines the destination for exporting metrics. If set to true, metrics will be exported to
     * InfluxDB and displayed using Grafana. If set to false, metrics will be exported to BigQuery
     * and displayed with Looker Studio.
     */
    @JsonProperty public boolean exportMetricsToInfluxDB = true;

    /** InfluxDB measurement to publish results to. * */
    @JsonProperty public String influxMeasurement = KafkaIOST.class.getName();

    /** InfluxDB host to publish metrics. * */
    @JsonProperty public String influxHost;

    /** InfluxDB database to publish metrics. * */
    @JsonProperty public String influxDatabase;
  }
}
