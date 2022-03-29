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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;

@RunWith(JUnit4.class)
public class PulsarIOTest {

  private static final String TOPIC = "PULSAR_IO_TEST";
  protected static PulsarContainer pulsarContainer;
  protected static PulsarClient client;

  private long endExpectedTime = 0;
  private long startTime = 0;

  private static final Logger LOG = LoggerFactory.getLogger(PulsarIOTest.class);

  @Rule public final transient TestPipeline testPipeline = TestPipeline.create();

  public List<Message<byte[]>> receiveMessages() throws PulsarClientException {
    if (client == null) {
      initClient();
    }
    List<Message<byte[]>> messages = new ArrayList<>();
    Consumer<byte[]> consumer =
        client.newConsumer().topic(TOPIC).subscriptionName("receiveMockMessageFn").subscribe();
    while (consumer.hasReachedEndOfTopic()) {
      Message<byte[]> msg = consumer.receive();
      messages.add(msg);
      try {
        consumer.acknowledge(msg);
      } catch (Exception e) {
        consumer.negativeAcknowledge(msg);
      }
    }
    return messages;
  }

  public List<PulsarMessage> produceMessages() throws PulsarClientException {
    client = initClient();
    Producer<byte[]> producer = client.newProducer().topic(TOPIC).create();
    Consumer<byte[]> consumer =
        client.newConsumer().topic(TOPIC).subscriptionName("produceMockMessageFn").subscribe();
    int numElements = 101;
    List<PulsarMessage> inputs = new ArrayList<>();
    for (int i = 0; i < numElements; i++) {
      String msg = ("PULSAR_TEST_READFROMSIMPLETOPIC_" + i);
      producer.send(msg.getBytes(StandardCharsets.UTF_8));
      CompletableFuture<Message<byte[]>> future = consumer.receiveAsync();
      Message<byte[]> message = null;
      try {
        message = future.get(5, TimeUnit.SECONDS);
        if (i >= 100) {
          endExpectedTime = message.getPublishTime();
        } else {
          inputs.add(new PulsarMessage(message.getTopicName(), message.getPublishTime(), message));
          if (i == 0) {
            startTime = message.getPublishTime();
          }
        }
      } catch (InterruptedException e) {
        LOG.error(e.getMessage());
      } catch (ExecutionException e) {
        LOG.error(e.getMessage());
      } catch (TimeoutException e) {
        LOG.error(e.getMessage());
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
    pulsarContainer = new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:2.9.0"));
    pulsarContainer.withCommand("bin/pulsar", "standalone");
    pulsarContainer.start();
  }

  @BeforeClass
  public static void setup() throws PulsarClientException {
    setupPulsarContainer();
    client = initClient();
  }

  @AfterClass
  public static void afterClass() {
    if (pulsarContainer != null) {
      pulsarContainer.stop();
    }
  }

  @Test
  @SuppressWarnings({"rawtypes"})
  public void testPulsarFunctionality() throws Exception {
    try (Consumer consumer =
            client.newConsumer().topic(TOPIC).subscriptionName("PulsarIO_IT").subscribe();
        Producer<byte[]> producer = client.newProducer().topic(TOPIC).create(); ) {
      String messageTxt = "testing pulsar functionality";
      producer.send(messageTxt.getBytes(StandardCharsets.UTF_8));
      CompletableFuture<Message> future = consumer.receiveAsync();
      Message message = future.get(5, TimeUnit.SECONDS);
      assertEquals(messageTxt, new String(message.getData(), StandardCharsets.UTF_8));
      client.close();
    }
  }

  @Test
  public void testReadFromSimpleTopic() {
    try {
      List<PulsarMessage> inputsMock = produceMessages();
      PulsarIO.Read reader =
          PulsarIO.read()
              .withClientUrl(pulsarContainer.getPulsarBrokerUrl())
              .withAdminUrl(pulsarContainer.getHttpServiceUrl())
              .withTopic(TOPIC)
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
                          MetricNameFilter.named(
                              PulsarIOTest.class.getName(), "PulsarRecordsCounter"))
                      .build());
      long recordsCount = 0;
      for (MetricResult<Long> metric : metrics.getCounters()) {
        if (metric
            .getName()
            .toString()
            .equals("org.apache.beam.sdk.io.pulsar.PulsarIOTest:PulsarRecordsCounter")) {
          recordsCount = metric.getAttempted();
          break;
        }
      }
      assertEquals(inputsMock.size(), (int) recordsCount);

    } catch (PulsarClientException e) {
      LOG.error(e.getMessage());
    }
  }

  @Test
  public void testWriteFromTopic() {
    try {
      PulsarIO.Write writer =
          PulsarIO.write().withClientUrl(pulsarContainer.getPulsarBrokerUrl()).withTopic(TOPIC);
      int numberOfMessages = 100;
      List<byte[]> messages = new ArrayList<>();
      for (int i = 0; i < numberOfMessages; i++) {
        messages.add(("PULSAR_WRITER_TEST_" + i).getBytes(StandardCharsets.UTF_8));
      }
      testPipeline.apply(Create.of(messages)).apply(writer);

      testPipeline.run();

      List<Message<byte[]>> receiveMsgs = receiveMessages();
      assertEquals(numberOfMessages, receiveMessages().size());
      for (int i = 0; i < numberOfMessages; i++) {
        assertTrue(
            new String(receiveMsgs.get(i).getValue(), StandardCharsets.UTF_8)
                .equals("PULSAR_WRITER_TEST_" + i));
      }
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
  }

  public static class PulsarRecordsMetric extends DoFn<PulsarMessage, PulsarMessage> {
    private final Counter counter =
        Metrics.counter(PulsarIOTest.class.getName(), "PulsarRecordsCounter");

    @ProcessElement
    public void processElement(ProcessContext context) {
      counter.inc();
      context.output(context.element());
    }
  }
}
