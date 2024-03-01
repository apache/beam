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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.gcp.IOLoadTestBase;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.synthetic.SyntheticSourceOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.testutils.NamedTestResult;
import org.apache.beam.sdk.testutils.metrics.IOITMetrics;
import org.apache.beam.sdk.testutils.publishing.InfluxDBSettings;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Longs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import com.google.cloud.Timestamp;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.time.Duration;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * KafkaIO stress tests. The test is designed to assess the performance of KafkaIO under
 * various conditions.
 * To run the test, a live remote Kafka broker is required.
 * You can deploy Kafka within a Kubernetes cluster following the example described here:
 * {@link .github/workflows/beam_PerformanceTests_Kafka_IO.yml}
 * If you choose to use Kubernetes, it's important to remember
 * that each pod should have a minimum of 10GB memory allocated.
 * Additionally, when running the test, it's necessary to pass the addresses of Kafka bootstrap servers as an argument.
 *
 * <p>Usage: <br>
 * - To run medium-scale stress tests: {@code gradle
 * :it:kafka:KafkaStressTestMedium -DbootstrapServers="0.0.0.0:32400,1.1.1.1:32400"} <br>
 * - To run large-scale stress tests: {@code gradle
 * :it:kafka:KafkaStressTestLarge -DbootstrapServers="0.0.0.0:32400,1.1.1.1:32400"}
 */
public final class KafkaIOST extends IOLoadTestBase {
    /**
     * The load will initiate at 1x, progressively increase to 2x and 4x, then decrease to 2x and
     * eventually return to 1x.
     */
    private static final int[] DEFAULT_LOAD_INCREASE_ARRAY = {1, 2, 2, 4, 2, 1};
    private static InfluxDBSettings influxDBSettings;
    private static final String WRITE_ELEMENT_METRIC_NAME = "write_count";
    private static final String READ_ELEMENT_METRIC_NAME = "read_count";
    private static final int DEFAULT_ROWS_PER_SECOND = 1000;
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

        adminClient = AdminClient.create(ImmutableMap.of("bootstrap.servers", configuration.bootstrapServers));
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
    }

    private static final Map<String, Configuration> TEST_CONFIGS_PRESET;

    static {
        try {
            TEST_CONFIGS_PRESET =
                    ImmutableMap.of(
                            "medium",
                            Configuration.fromJsonString(
                                    "{\"rowsPerSecond\":25000,\"minutes\":30,\"pipelineTimeout\":60,\"runner\":\"DataflowRunner\"}",
                                    Configuration.class),
                            "large",
                            Configuration.fromJsonString(
                                    "{\"rowsPerSecond\":25000,\"minutes\":130,\"pipelineTimeout\":300,\"runner\":\"DataflowRunner\"}",
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
            // streaming read pipeline does not end itself
            assertEquals(
                    PipelineLauncher.JobState.RUNNING,
                    pipelineLauncher.getJobStatus(project, region, readInfo.jobId()));

            // Delete topic after test run
            adminClient.deleteTopics(Collections.singleton(kafkaTopic));

            double writeNumRecords = pipelineLauncher.getMetric(
                    project,
                    region,
                    writeInfo.jobId(),
                    getBeamMetricsName(PipelineMetricsType.COUNTER, WRITE_ELEMENT_METRIC_NAME));
            double readNumRecords = pipelineLauncher.getMetric(
                    project,
                    region,
                    readInfo.jobId(),
                    getBeamMetricsName(PipelineMetricsType.COUNTER, READ_ELEMENT_METRIC_NAME));
            assertEquals(writeNumRecords, readNumRecords, 0);
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

        exportMetrics(writeInfo, writeMetricsConfig);
        exportMetrics(readInfo, readMetricsConfig);
    }

    /**
     * The method creates a pipeline to simulate data generation and write operations to Kafka
     * topic, based on the specified configuration parameters. The stress test involves varying the
     * load dynamically over time, with options to use configurable parameters.
     */
    private PipelineLauncher.LaunchInfo generateDataAndWrite() throws IOException {
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
                                "Extract values",
                                MapElements.into(TypeDescriptor.of(byte[].class))
                                        .via(instant -> Longs.toByteArray(instant.getMillis() % totalRows)));
        if (startMultiplier > 1) {
            source =
                    source
                            .apply(
                                    "One input to multiple outputs",
                                    ParDo.of(new MultiplierDoFn(startMultiplier, loadPeriods)))
                            .apply("Reshuffle fanout", Reshuffle.viaRandomKey())
                            .apply("Counting element", ParDo.of(new CountingFn<>(WRITE_ELEMENT_METRIC_NAME)));
        }
        source
                .apply(
                        "Write to Kafka",
                        KafkaIO.<byte[], byte[]>write()
                                .withBootstrapServers(configuration.bootstrapServers)
                                .withTopic(kafkaTopic)
                                .withValueSerializer(ByteArraySerializer.class)
                                .withProducerConfigUpdates(ImmutableMap.of(
                                        ProducerConfig.RETRIES_CONFIG, 10,
                                        ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 600000,
                                        ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 5000
                                ))
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
                        .addParameter("experiments", "use_runner_v2")
                        .build();

        return pipelineLauncher.launch(project, region, options);
    }

    /** The method reads data from Kafka topic in streaming mode. */
    private PipelineLauncher.LaunchInfo readData() throws IOException {
        KafkaIO.Read<byte[], byte[]> readFromKafka =
                KafkaIO.readBytes()
                        .withBootstrapServers(configuration.bootstrapServers)
                        .withTopic(kafkaTopic)
                        .withConsumerConfigUpdates(ImmutableMap.of(
                                "auto.offset.reset", "earliest"
                        ));

        readPipeline
                .apply("Read from unbounded Kafka", readFromKafka)
                .apply("Counting element", ParDo.of(new CountingFn<>(READ_ELEMENT_METRIC_NAME)));

        PipelineLauncher.LaunchConfig options =
                PipelineLauncher.LaunchConfig.builder("read-kafka")
                        .setSdk(PipelineLauncher.Sdk.JAVA)
                        .setPipeline(readPipeline)
                        .addParameter("numWorkers", String.valueOf(configuration.numWorkers))
                        .addParameter("runner", configuration.runner)
                        .addParameter("experiments", "use_runner_v2")
                        .build();

        return pipelineLauncher.launch(project, region, options);
    }

    private void exportMetrics(
            PipelineLauncher.LaunchInfo launchInfo, MetricsConfiguration metricsConfig)
            throws IOException, ParseException, InterruptedException {

        Map<String, Double> metrics = getMetrics(launchInfo, metricsConfig);
        String testId = UUID.randomUUID().toString();
        String testTimestamp = Timestamp.now().toString();

        if (configuration.exportMetricsToInfluxDB) {
            Collection<NamedTestResult> namedTestResults = new ArrayList<>();
            for (Map.Entry<String, Double> entry : metrics.entrySet()) {
                NamedTestResult metricResult =
                        NamedTestResult.create(testId, testTimestamp, entry.getKey(), entry.getValue());
                namedTestResults.add(metricResult);
            }
            IOITMetrics.publishToInflux(testId, testTimestamp, namedTestResults, influxDBSettings);
        } else {
            exportMetricsToBigQuery(launchInfo, metrics);
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

    /** Options for Kafka IO stress test. */
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

        /** Kafka bootstrap server addresses. */
        @JsonProperty public String bootstrapServers;

        /**
         * Determines the destination for exporting metrics. If set to true, metrics will be exported to
         * InfluxDB and displayed using Grafana. If set to false, metrics will be exported to BigQuery
         * and displayed with Looker Studio.
         */
        @JsonProperty public boolean exportMetricsToInfluxDB = false;

        /** InfluxDB measurement to publish results to. * */
        @JsonProperty public String influxMeasurement = KafkaIOST.class.getName();

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