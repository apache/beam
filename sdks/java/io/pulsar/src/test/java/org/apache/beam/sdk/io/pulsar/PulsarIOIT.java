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
package org.apache.beam.sdk.io.pulsar;

import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;

@RunWith(JUnit4.class)
public class PulsarIOIT {
  @Rule public Timeout globalTimeout = Timeout.seconds(60);
  protected static PulsarContainer pulsarContainer;
  protected static PulsarClient client;

  private long endExpectedTime = 0;
  private long startTime = 0;

  private static final Logger LOG = LoggerFactory.getLogger(PulsarIOIT.class);

  @Rule public final transient TestPipeline testPipeline = TestPipeline.create();

  public List<Message<byte[]>> receiveMessages(String topic) throws PulsarClientException {
    if (client == null) {
      initClient();
    }
    List<Message<byte[]>> messages = new ArrayList<>();
    try (Consumer<byte[]> consumer =
        client.newConsumer().topic(topic).subscriptionName("receiveMockMessageFn").subscribe()) {
      consumer.seek(MessageId.earliest);
      LOG.warn("started receiveMessages");
      while (!consumer.hasReachedEndOfTopic()) {
        Message<byte[]> msg = consumer.receive(5, TimeUnit.SECONDS);
        if (msg == null) {
          LOG.warn("null message");
          break;
        }
        messages.add(msg);
        consumer.acknowledge(msg);
      }
    }
    messages.sort(Comparator.comparing(s -> new String(s.getValue(), StandardCharsets.UTF_8)));
    return messages;
  }

  public List<PulsarMessage> produceMessages(String topic) throws PulsarClientException {
    client = initClient();
    Producer<byte[]> producer = client.newProducer().topic(topic).create();
    Consumer<byte[]> consumer =
        client.newConsumer().topic(topic).subscriptionName("produceMockMessageFn").subscribe();
    int numElements = 101;
    List<PulsarMessage> inputs = new ArrayList<>();
    for (int i = 0; i < numElements; i++) {
      String msg = ("PULSAR_TEST_READFROMSIMPLETOPIC_" + i);
      producer.send(msg.getBytes(StandardCharsets.UTF_8));
      Message<byte[]> message = consumer.receive(5, TimeUnit.SECONDS);
      if (i == 100) {
        endExpectedTime = message.getPublishTime();
      } else {
        inputs.add(PulsarMessage.create(message));
        if (i == 0) {
          startTime = message.getPublishTime();
        }
      }
    }
    consumer.close();
    producer.close();
    client.close();
    return inputs;
  }

  private static PulsarClient initClient() throws PulsarClientException {
    return PulsarClient.builder().serviceUrl(pulsarContainer.getPulsarBrokerUrl()).build();
  }

  private static void setupPulsarContainer() {
    pulsarContainer = new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:2.11.4"));
    pulsarContainer.withCommand("bin/pulsar", "standalone");
    try {
      pulsarContainer.start();
    } catch (IllegalStateException unused) {
      pulsarContainer = new PulsarContainerLocalProxy();
    }
  }

  static class PulsarContainerLocalProxy extends PulsarContainer {
    @Override
    public String getPulsarBrokerUrl() {
      return "pulsar://localhost:6650";
    }

    @Override
    public String getHttpServiceUrl() {
      return "http://localhost:8080";
    }
  }

  @BeforeClass
  public static void setup() throws PulsarClientException {
    setupPulsarContainer();
    client = initClient();
  }

  @AfterClass
  public static void afterClass() {
    if (pulsarContainer != null && pulsarContainer.isRunning()) {
      pulsarContainer.stop();
    }
  }

  @Test
  public void testReadFromSimpleTopic() throws PulsarClientException {
    String topic = "PULSARIOIT_READ" + RandomStringUtils.randomAlphanumeric(4);
    List<PulsarMessage> inputsMock = produceMessages(topic);
    PulsarIO.Read<PulsarMessage> reader =
        PulsarIO.read()
            .withClientUrl(pulsarContainer.getPulsarBrokerUrl())
            .withAdminUrl(pulsarContainer.getHttpServiceUrl())
            .withTopic(topic)
            .withStartTimestamp(startTime)
            .withEndTimestamp(endExpectedTime)
            .withPublishTime();
    testPipeline.apply(reader).apply(ParDo.of(new PulsarRecordsMetric()));

    PipelineResult pipelineResult = testPipeline.run();
    MetricQueryResults metrics =
        pipelineResult
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(PulsarIOIT.class.getName(), "PulsarRecordsCounter"))
                    .build());
    long recordsCount = 0;
    for (MetricResult<Long> metric : metrics.getCounters()) {
      if (metric
          .getName()
          .toString()
          .equals("org.apache.beam.sdk.io.pulsar.PulsarIOIT:PulsarRecordsCounter")) {
        recordsCount = metric.getAttempted();
        break;
      }
    }
    assertEquals(inputsMock.size(), (int) recordsCount);
  }

  @Test
  public void testWriteToTopic() throws PulsarClientException {
    String topic = "PULSARIOIT_WRITE_" + RandomStringUtils.randomAlphanumeric(4);
    PulsarIO.Write writer =
        PulsarIO.write().withClientUrl(pulsarContainer.getPulsarBrokerUrl()).withTopic(topic);
    int numberOfMessages = 10;
    List<byte[]> messages = new ArrayList<>();
    for (int i = 0; i < numberOfMessages; i++) {
      messages.add(("PULSAR_WRITER_TEST_" + i).getBytes(StandardCharsets.UTF_8));
    }
    testPipeline.apply(Create.of(messages)).apply(writer);

    testPipeline.run();

    List<Message<byte[]>> receiveMsgs = receiveMessages(topic);
    assertEquals(numberOfMessages, receiveMsgs.size());
    for (int i = 0; i < numberOfMessages; i++) {
      assertEquals(
          new String(receiveMsgs.get(i).getValue(), StandardCharsets.UTF_8),
          "PULSAR_WRITER_TEST_" + i);
    }
  }

  public static class PulsarRecordsMetric extends DoFn<PulsarMessage, PulsarMessage> {
    private final Counter counter =
        Metrics.counter(PulsarIOIT.class.getName(), "PulsarRecordsCounter");

    @ProcessElement
    public void processElement(ProcessContext context) {
      counter.inc();
      context.output(context.element());
    }
  }
}
