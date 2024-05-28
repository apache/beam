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
package org.apache.beam.sdk.io.jms;

import static org.apache.beam.sdk.io.jms.CommonJms.PASSWORD;
import static org.apache.beam.sdk.io.jms.CommonJms.QUEUE;
import static org.apache.beam.sdk.io.jms.CommonJms.USERNAME;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.common.IOITHelper;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testutils.NamedTestResult;
import org.apache.beam.sdk.testutils.metrics.IOITMetrics;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;
import org.apache.beam.sdk.testutils.metrics.TimeMonitor;
import org.apache.beam.sdk.testutils.publishing.InfluxDBSettings;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * A performance test of {@link JmsIO} on a Jms Broker.
 *
 * <p>Usage:
 *
 * <pre>
 *  ./gradlew integrationTest -p sdks/java/io/jms -DintegrationTestPipelineOptions='[ \
 *    "--jmsBrokerHost=amqp://host", \
 *    "--jmsBrokerPort=5672", \
 *    "--localJmsBrokerEnabled=false", \
 *    "--readTimeout=10" \
 *    ]' \
 *  --tests org.apache.beam.sdk.io.jms.JmsIOIT \
 *  -DintegrationTestRunner=direct
 * </pre>
 *
 * The default values for each parameter are: - jmsBrokerHost: amqp://localhost - jmsBrokerPort:
 * 5672 - localJmsBrokerEnabled: true - readTimeout: 30
 */
@RunWith(Parameterized.class)
public class JmsIOIT implements Serializable {
  private static final String NAMESPACE = JmsIOIT.class.getName();
  private static final String READ_TIME_METRIC = "read_time";
  private static final String WRITE_TIME_METRIC = "write_time";
  private static final String READ_ELEMENT_METRIC_NAME = "jms_read_element_count";

  /** JmsIO options. */
  public interface JmsIOITOptions extends IOTestPipelineOptions, StreamingOptions {
    @Description("Host name for Jms Broker. By default, 'amqp://localhost'")
    @Default.String("amqp://localhost")
    String getJmsBrokerHost();

    void setJmsBrokerHost(String host);

    @Description("Port for Jms Broker")
    @Default.Integer(5672)
    Integer getJmsBrokerPort();

    void setJmsBrokerPort(Integer port);

    @Description("Enabling Jms Broker locally")
    @Default.Boolean(true)
    boolean isLocalJmsBrokerEnabled();

    void setLocalJmsBrokerEnabled(boolean isEnabled);

    @Description("JMS Read Timeout in seconds")
    @Default.Integer(30)
    Integer getReadTimeout();

    void setReadTimeout(Integer timeout);

    @Description(
        "Use ConnectionFactory provider function instead of pre-instantiated ConnectionFactory object")
    @Default.Boolean(false)
    boolean getUseConnectionFactoryProviderFn();

    void setUseConnectionFactoryProviderFn(boolean value);
  }

  private static final JmsIOITOptions OPTIONS =
      IOITHelper.readIOTestPipelineOptions(JmsIOITOptions.class);

  private static final InfluxDBSettings settings =
      InfluxDBSettings.builder()
          .withHost(OPTIONS.getInfluxHost())
          .withDatabase(OPTIONS.getInfluxDatabase())
          .withMeasurement(OPTIONS.getInfluxMeasurement())
          .get();

  @Rule public transient TestPipeline pipelineWrite = TestPipeline.create();
  @Rule public transient TestPipeline pipelineRead = TestPipeline.create();

  @Parameterized.Parameters(name = "with client class {3}")
  public static Collection<Object[]> connectionFactories() {
    return ImmutableList.of(
        new Object[] {
          "vm://localhost", 5672, "jms.sendAcksAsync=false", ActiveMQConnectionFactory.class
        });
    // TODO(https://github.com/apache/beam/issues/26175) Test failure on direct runner due to
    //  JmsIO read on amqp slow on CI (passed locally)
    // new Object[] {
    //   "amqp://localhost", 5672, "jms.forceAsyncAcks=false", JmsConnectionFactory.class
    // });
  }

  private final CommonJms commonJms;
  private ConnectionFactory connectionFactory;
  private Class<? extends ConnectionFactory> connectionFactoryClass;

  public JmsIOIT(
      String brokerUrl,
      Integer brokerPort,
      String forceAsyncAcksParam,
      Class<? extends ConnectionFactory> connectionFactoryClass) {
    this.commonJms =
        new CommonJms(
            OPTIONS.isLocalJmsBrokerEnabled() ? brokerUrl : OPTIONS.getJmsBrokerHost(),
            OPTIONS.isLocalJmsBrokerEnabled() ? brokerPort : OPTIONS.getJmsBrokerPort(),
            forceAsyncAcksParam,
            connectionFactoryClass);
  }

  @Before
  public void setup() throws Exception {
    if (OPTIONS.isLocalJmsBrokerEnabled()) {
      this.commonJms.startBroker();
      connectionFactory = this.commonJms.createConnectionFactory();
      connectionFactoryClass = this.commonJms.getConnectionFactoryClass();
      // use a small number of record for local integration test
      OPTIONS.setNumberOfRecords(10000);
    }
  }

  @After
  public void tearDown() throws Exception {
    if (OPTIONS.isLocalJmsBrokerEnabled()) {
      this.commonJms.stopBroker();
      connectionFactory = null;
      connectionFactoryClass = null;
    }
  }

  @Test
  public void testPublishingThenReadingAll() throws IOException, JMSException {
    PipelineResult writeResult = publishingMessages();
    PipelineResult.State writeState = writeResult.waitUntilFinish();
    assertNotEquals(PipelineResult.State.FAILED, writeState);

    PipelineResult readResult = readMessages();
    PipelineResult.State readState =
        readResult.waitUntilFinish(Duration.standardSeconds(OPTIONS.getReadTimeout()));
    // A workaround to stop the pipeline for waiting for too long
    cancelIfTimeouted(readResult, readState);
    assertNotEquals(PipelineResult.State.FAILED, readState);

    MetricsReader metricsReader = new MetricsReader(readResult, NAMESPACE);
    long actualRecords = metricsReader.getCounterMetric(READ_ELEMENT_METRIC_NAME);

    // TODO(yathu) resolve pending messages with direct runner then we can simply assert
    //   actual-records == total-records.
    //   Due to direct runner only finalize checkpoint at very end, there are open consumers (may
    //   with buffer) and O(open_consumer) message won't get delivered to other session.
    int unackRecords = countRemain(QUEUE);
    assertTrue(
        String.format("Too many unacknowledged messages: %d", unackRecords),
        unackRecords < OPTIONS.getNumberOfRecords() * 0.002);

    // acknowledged records
    int ackRecords = OPTIONS.getNumberOfRecords() - unackRecords;
    assertTrue(
        String.format(
            "actual number of records %d smaller than expected: %d.", actualRecords, ackRecords),
        ackRecords <= actualRecords);
    collectAndPublishMetrics(writeResult, readResult);
  }

  private void cancelIfTimeouted(PipelineResult readResult, PipelineResult.State readState)
      throws IOException {
    if (readState == null) {
      readResult.cancel();
    }
  }

  private PipelineResult readMessages() {
    pipelineRead.getOptions().as(JmsIOITOptions.class).setStreaming(true);
    pipelineRead.getOptions().as(JmsIOITOptions.class).setBlockOnRun(false);
    JmsIO.Read<String> jmsIORead = JmsIO.readMessage();
    if (pipelineRead.getOptions().as(JmsIOITOptions.class).getUseConnectionFactoryProviderFn()) {
      jmsIORead =
          jmsIORead.withConnectionFactoryProviderFn(
              CommonJms.toSerializableFunction(commonJms::createConnectionFactory));
    } else {
      jmsIORead = jmsIORead.withConnectionFactory(connectionFactory);
    }
    pipelineRead
        .apply(
            "Read Messages",
            jmsIORead
                .withQueue(QUEUE)
                .withUsername(USERNAME)
                .withPassword(PASSWORD)
                .withCoder(SerializableCoder.of(String.class))
                .withMessageMapper(getJmsMessageMapper()))
        .apply(ParDo.of(new TimeMonitor<>(NAMESPACE, READ_TIME_METRIC)))
        .apply("Counting element", ParDo.of(new CountingFn(NAMESPACE, READ_ELEMENT_METRIC_NAME)));
    return pipelineRead.run();
  }

  private PipelineResult publishingMessages() {

    JmsIO.Write<String> jmsIOWrite = JmsIO.write();
    if (pipelineWrite.getOptions().as(JmsIOITOptions.class).getUseConnectionFactoryProviderFn()) {
      jmsIOWrite =
          jmsIOWrite.withConnectionFactoryProviderFn(
              CommonJms.toSerializableFunction(commonJms::createConnectionFactory));
    } else {
      jmsIOWrite = jmsIOWrite.withConnectionFactory(connectionFactory);
    }

    pipelineWrite
        .apply("Generate Sequence Data", GenerateSequence.from(0).to(OPTIONS.getNumberOfRecords()))
        .apply("Convert to String", ParDo.of(new ToString()))
        .apply("Collect write time", ParDo.of(new TimeMonitor<>(NAMESPACE, WRITE_TIME_METRIC)))
        .apply(
            "Publish to Jms Broker",
            jmsIOWrite
                .withQueue(QUEUE)
                .withUsername(USERNAME)
                .withPassword(PASSWORD)
                .withValueMapper(new TextMessageMapper()));
    return pipelineWrite.run();
  }

  private void collectAndPublishMetrics(PipelineResult writeResult, PipelineResult readResult) {
    String uuid = UUID.randomUUID().toString();
    String timestamp = Instant.now().toString();

    Set<Function<MetricsReader, NamedTestResult>> readSuppliers =
        getMetricsSuppliers(uuid, timestamp, READ_TIME_METRIC);
    Set<Function<MetricsReader, NamedTestResult>> writeSuppliers =
        getMetricsSuppliers(uuid, timestamp, WRITE_TIME_METRIC);

    IOITMetrics readMetrics =
        new IOITMetrics(readSuppliers, readResult, NAMESPACE, uuid, timestamp);
    IOITMetrics writeMetrics =
        new IOITMetrics(writeSuppliers, writeResult, NAMESPACE, uuid, timestamp);

    readMetrics.publishToInflux(settings);
    writeMetrics.publishToInflux(settings);
  }

  private Set<Function<MetricsReader, NamedTestResult>> getMetricsSuppliers(
      String uuid, String timestamp, String metric) {
    Set<Function<MetricsReader, NamedTestResult>> suppliers = new HashSet<>();
    suppliers.add(getTimeMetric(uuid, timestamp, metric));
    return suppliers;
  }

  private Function<MetricsReader, NamedTestResult> getTimeMetric(
      final String uuid, final String timestamp, final String metricName) {
    return reader -> {
      long startTime = reader.getStartTimeMetric(metricName);
      long endTime = reader.getEndTimeMetric(metricName);
      return NamedTestResult.create(uuid, timestamp, metricName, (endTime - startTime) / 1e3);
    };
  }

  private int countRemain(String queue) throws JMSException {
    Connection connection = connectionFactory.createConnection(USERNAME, PASSWORD);
    connection.start();
    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    QueueBrowser browser = session.createBrowser(session.createQueue(queue));
    Enumeration<Message> messages = browser.getEnumeration();
    int count = 0;
    while (messages.hasMoreElements()) {
      messages.nextElement();
      count++;
    }
    return count;
  }

  static class ToString extends DoFn<Long, String> {
    @ProcessElement
    public void processElement(@Element Long element, OutputReceiver<String> outputReceiver) {
      outputReceiver.output(String.format("Message %d", element));
    }
  }

  private JmsIO.MessageMapper<String> getJmsMessageMapper() {
    return rawMessage ->
        connectionFactoryClass == JmsConnectionFactory.class
            ? ((TextMessage) rawMessage).getText()
            : ((ActiveMQTextMessage) rawMessage).getText();
  }

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
}
