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
package org.apache.beam.sdk.io.sparkreceiver;

import static org.apache.beam.sdk.io.synthetic.SyntheticOptions.fromJsonString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.common.IOITHelper;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.io.synthetic.SyntheticSourceOptions;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testutils.NamedTestResult;
import org.apache.beam.sdk.testutils.metrics.IOITMetrics;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;
import org.apache.beam.sdk.testutils.metrics.TimeMonitor;
import org.apache.beam.sdk.testutils.publishing.InfluxDBSettings;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * IO Integration test for {@link org.apache.beam.sdk.io.sparkreceiver.SparkReceiverIO}.
 *
 * <p>{@see https://beam.apache.org/documentation/io/testing/#i-o-transform-integration-tests} for
 * more details.
 *
 * <p>NOTE: This test sets retention policy of the messages so that all messages are retained in the
 * topic so that we could read them back after writing.
 */
@RunWith(JUnit4.class)
public class SparkReceiverIOIT {

  private static final Logger LOG = LoggerFactory.getLogger(SparkReceiverIOIT.class);

  private static final String READ_TIME_METRIC_NAME = "read_time";

  private static final String RUN_TIME_METRIC_NAME = "run_time";

  private static final String READ_ELEMENT_METRIC_NAME = "spark_read_element_count";

  private static final String NAMESPACE = SparkReceiverIOIT.class.getName();

  private static final String TEST_ID = UUID.randomUUID().toString();

  private static final String TIMESTAMP = Instant.now().toString();

  private static final String TEST_MESSAGE_PREFIX = "Test ";

  private static Options options;

  private static SyntheticSourceOptions sourceOptions;

  private static GenericContainer<?> rabbitMqContainer;

  private static InfluxDBSettings settings;

  @Rule public TestPipeline readPipeline = TestPipeline.create();

  @BeforeClass
  public static void setup() throws IOException {
    options = IOITHelper.readIOTestPipelineOptions(Options.class);
    sourceOptions = fromJsonString(options.getSourceOptions(), SyntheticSourceOptions.class);
    if (options.isWithTestcontainers()) {
      setupRabbitMqContainer();
    } else {
      settings =
          InfluxDBSettings.builder()
              .withHost(options.getInfluxHost())
              .withDatabase(options.getInfluxDatabase())
              .withMeasurement(options.getInfluxMeasurement())
              .get();
    }
    clearRabbitMQ();
  }

  @AfterClass
  public static void afterClass() {
    if (rabbitMqContainer != null) {
      rabbitMqContainer.stop();
    }

    clearRabbitMQ();
  }

  private static void setupRabbitMqContainer() {
    rabbitMqContainer =
        new RabbitMQContainer(
                DockerImageName.parse("rabbitmq").withTag(options.getRabbitMqContainerVersion()))
            .withExposedPorts(5672, 15672);
    rabbitMqContainer.start();
    options.setRabbitMqBootstrapServerAddress(
        getBootstrapServers(
            rabbitMqContainer.getHost(), rabbitMqContainer.getMappedPort(5672).toString()));
  }

  private static String getBootstrapServers(String host, String port) {
    return String.format("amqp://guest:guest@%s:%s", host, port);
  }

  /** Pipeline options specific for this test. */
  public interface Options extends IOTestPipelineOptions, StreamingOptions {

    @Description("Options for synthetic source.")
    @Validation.Required
    @Default.String("{\"numRecords\": \"500\",\"keySizeBytes\": \"1\",\"valueSizeBytes\": \"90\"}")
    String getSourceOptions();

    void setSourceOptions(String sourceOptions);

    @Description("RabbitMQ bootstrap server address")
    @Default.String("amqp://guest:guest@localhost:5672")
    String getRabbitMqBootstrapServerAddress();

    void setRabbitMqBootstrapServerAddress(String address);

    @Description("RabbitMQ stream")
    @Default.String("rabbitMqTestStream")
    String getStreamName();

    void setStreamName(String streamName);

    @Description("Whether to use testcontainers")
    @Default.Boolean(false)
    Boolean isWithTestcontainers();

    void setWithTestcontainers(Boolean withTestcontainers);

    @Description("RabbitMQ container version. Use when useTestcontainers is true")
    @Nullable
    @Default.String("3.9-alpine")
    String getRabbitMqContainerVersion();

    void setRabbitMqContainerVersion(String rabbitMqContainerVersion);

    @Description("Time to wait for the events to be processed by the read pipeline (in seconds)")
    @Default.Integer(50)
    @Validation.Required
    Integer getReadTimeout();

    void setReadTimeout(Integer readTimeout);
  }

  private void writeToRabbitMq(final List<String> messages)
      throws URISyntaxException, NoSuchAlgorithmException, KeyManagementException, IOException,
          TimeoutException {

    final ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setUri(options.getRabbitMqBootstrapServerAddress());
    Map<String, Object> arguments = new HashMap<>();
    arguments.put("x-queue-type", "stream");

    try (Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel()) {
      channel.queueDeclare(options.getStreamName(), true, false, false, arguments);

      messages.forEach(
          message -> {
            try {
              channel.basicPublish(
                  "",
                  options.getStreamName(),
                  MessageProperties.PERSISTENT_TEXT_PLAIN,
                  message.getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          });
    }
  }

  private SparkReceiverIO.Read<String> readFromRabbitMqWithOffset() {
    final ReceiverBuilder<String, RabbitMqReceiverWithOffset> receiverBuilder =
        new ReceiverBuilder<>(RabbitMqReceiverWithOffset.class)
            .withConstructorArgs(
                options.getRabbitMqBootstrapServerAddress(),
                options.getStreamName(),
                sourceOptions.numRecords);

    return SparkReceiverIO.<String>read()
        .withGetOffsetFn(
            rabbitMqMessage ->
                Long.valueOf(rabbitMqMessage.substring(TEST_MESSAGE_PREFIX.length())))
        .withSparkReceiverBuilder(receiverBuilder);
  }

  /**
   * Since streams in RabbitMQ are durable by definition, we have to clean them up after test
   * execution. The simplest way is to delete the whole stream after test execution.
   */
  private static void clearRabbitMQ() {
    final ConnectionFactory connectionFactory = new ConnectionFactory();

    try {
      connectionFactory.setUri(options.getRabbitMqBootstrapServerAddress());
      try (Connection connection = connectionFactory.newConnection();
          Channel channel = connection.createChannel()) {
        channel.queueDelete(options.getStreamName());
      }
    } catch (URISyntaxException
        | NoSuchAlgorithmException
        | KeyManagementException
        | IOException
        | TimeoutException e) {
      LOG.error("Error during RabbitMQ clean up", e);
    }
  }

  /** Function for counting processed pipeline elements. */
  private static class CountingFn extends DoFn<String, Void> {

    private final Counter elementCounter;

    CountingFn(String namespace, String name) {
      elementCounter = Metrics.counter(namespace, name);
    }

    @ProcessElement
    public void processElement() {
      elementCounter.inc(1L);
    }
  }

  private void cancelIfTimeout(PipelineResult readResult, PipelineResult.State readState)
      throws IOException {
    if (readState == null) {
      readResult.cancel();
    }
  }

  private long readElementMetric(PipelineResult result) {
    MetricsReader metricsReader = new MetricsReader(result, SparkReceiverIOIT.NAMESPACE);
    return metricsReader.getCounterMetric(SparkReceiverIOIT.READ_ELEMENT_METRIC_NAME);
  }

  private Set<NamedTestResult> readMetrics(PipelineResult readResult) {
    BiFunction<MetricsReader, String, NamedTestResult> supplier =
        (reader, metricName) -> {
          long start = reader.getStartTimeMetric(metricName);
          long end = reader.getEndTimeMetric(metricName);
          return NamedTestResult.create(TEST_ID, TIMESTAMP, metricName, (end - start) / 1e3);
        };

    NamedTestResult readTime =
        supplier.apply(new MetricsReader(readResult, NAMESPACE), READ_TIME_METRIC_NAME);
    NamedTestResult runTime =
        NamedTestResult.create(TEST_ID, TIMESTAMP, RUN_TIME_METRIC_NAME, readTime.getValue());

    return ImmutableSet.of(readTime, runTime);
  }

  @Test
  public void testSparkReceiverIOReadsInStreamingWithOffset() throws IOException {

    final List<String> messages =
        LongStream.range(0, sourceOptions.numRecords)
            .mapToObj(number -> TEST_MESSAGE_PREFIX + number)
            .collect(Collectors.toList());

    try {
      writeToRabbitMq(messages);
    } catch (Exception e) {
      LOG.error("Can not write to rabbit {}", e.getMessage());
      fail();
    }
    LOG.info(sourceOptions.numRecords + " records were successfully written to RabbitMQ");

    // Use streaming pipeline to read RabbitMQ records.
    readPipeline.getOptions().as(Options.class).setStreaming(true);
    readPipeline
        .apply("Read from unbounded RabbitMq", readFromRabbitMqWithOffset())
        .setCoder(StringUtf8Coder.of())
        .apply("Measure read time", ParDo.of(new TimeMonitor<>(NAMESPACE, READ_TIME_METRIC_NAME)))
        .apply("Counting element", ParDo.of(new CountingFn(NAMESPACE, READ_ELEMENT_METRIC_NAME)));

    final PipelineResult readResult = readPipeline.run();
    final PipelineResult.State readState =
        readResult.waitUntilFinish(Duration.standardSeconds(options.getReadTimeout()));

    cancelIfTimeout(readResult, readState);

    assertEquals(sourceOptions.numRecords, readElementMetric(readResult));

    if (!options.isWithTestcontainers()) {
      Set<NamedTestResult> metrics = readMetrics(readResult);
      IOITMetrics.publishToInflux(TEST_ID, TIMESTAMP, metrics, settings);
    }
  }
}
