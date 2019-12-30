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
package org.apache.beam.sdk.io.rabbitmq;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Consumer;
import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.common.NetworkTestHelper;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.qpid.server.SystemLauncher;
import org.apache.qpid.server.model.SystemConfig;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test of {@link RabbitMqIO}. */
@RunWith(JUnit4.class)
public class RabbitMqIOTest implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(RabbitMqIOTest.class);

  private static final int ONE_MINUTE_MS = 60 * 1000;

  private static int port;
  private static String defaultPort;
  private static String uri;
  private static ConnectionHandler connectionHandler;

  @ClassRule public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule public transient TestPipeline p = TestPipeline.create();

  private static transient SystemLauncher launcher;

  @BeforeClass
  public static void beforeClass() throws Exception {
    port = NetworkTestHelper.getAvailableLocalPort();

    defaultPort = System.getProperty("qpid.amqp_port");
    System.setProperty("qpid.amqp_port", "" + port);

    System.setProperty("derby.stream.error.field", "MyApp.DEV_NULL");

    // see https://stackoverflow.com/a/49234754/796064 for qpid setup
    launcher = new SystemLauncher();

    Map<String, Object> attributes = new HashMap<>();
    URL initialConfig = RabbitMqIOTest.class.getResource("rabbitmq-io-test-config.json");
    attributes.put("type", "Memory");
    attributes.put("initialConfigurationLocation", initialConfig.toExternalForm());
    attributes.put(SystemConfig.DEFAULT_QPID_WORK_DIR, temporaryFolder.newFolder().toString());

    launcher.startup(attributes);

    uri = "amqp://guest:guest@localhost:" + port;

    connectionHandler = new ConnectionHandler(uri);
  }

  @AfterClass
  public static void afterClass() {
    if (defaultPort != null) {
      System.setProperty("qpid.amqp_port", defaultPort);
    } else {
      System.clearProperty("qpid.amqp_port");
    }
    launcher.shutdown();
    try {
      connectionHandler.close();
    } catch (IOException e) {
      /* ignored */
    }
  }

  @Test(timeout = ONE_MINUTE_MS)
  public void testReadFromExistingQueue() throws Exception {
    UUID testId = UUID.randomUUID();

    final int maxNumRecords = 10;
    final String queueName = "READ";
    RabbitMqIO.Read spec =
        RabbitMqIO.read()
            .withUri(uri)
            .withReadParadigm(ReadParadigm.ExistingQueue.of(queueName))
            .withMaxNumRecords(maxNumRecords);

    PCollection<RabbitMqMessage> raw = p.apply(spec);

    PCollection<String> output =
        raw.apply(
            MapElements.into(TypeDescriptors.strings()).via(RabbitMqTestUtils::messageToString));

    final List<RabbitMqMessage> messages =
        RabbitMqTestUtils.generateRecords(maxNumRecords).stream()
            .map(msg -> msg.toBuilder().setRoutingKey(queueName).build())
            .collect(Collectors.toList());
    Set<String> expected =
        messages.stream().map(RabbitMqTestUtils::messageToString).collect(Collectors.toSet());

    PAssert.that(output).containsInAnyOrder(expected);

    try {
      RabbitMqTestUtils.createQueue(testId, connectionHandler, spec.readParadigm().queueName());
      Thread publisher =
          RabbitMqTestUtils.publishMessagesThread(
              connectionHandler, testId, spec, messages, Duration.millis(250));
      publisher.start();
      p.run();
      publisher.join();
    } finally {
      connectionHandler.closeChannel(testId);
    }
  }

  /**
   * Helper for running tests against an exchange.
   *
   * <p>This function will automatically specify (and overwrite) the uri and numRecords values of
   * the Read definition.
   */
  private void doNewQueueTest(
      RabbitMqIO.Read read,
      int expectedRecords,
      List<RabbitMqMessage> toPublishMatches,
      List<RabbitMqMessage> toPublishIgnored)
      throws Exception {
    final UUID testId = UUID.randomUUID();

    ReadParadigm paradigm = read.readParadigm();
    if (!(paradigm instanceof ReadParadigm.NewQueue)) {
      throw new IllegalArgumentException(
          "ExchangeTest only supported for ReadParadigm.NewQueue. '" + paradigm + "' given.");
    }
    ReadParadigm.NewQueue newQueue = (ReadParadigm.NewQueue) paradigm;

    PCollection<RabbitMqMessage> raw =
        p.apply(read.withUri(uri).withMaxNumRecords(expectedRecords));

    PCollection<String> result =
        raw.apply(
            MapElements.into(TypeDescriptors.strings())
                .via((RabbitMqMessage message) -> RabbitMqTestUtils.bodyToString(message.body())));

    final List<String> expectedBodies =
        toPublishMatches.stream()
            .map(RabbitMqMessage::body)
            .map(RabbitMqTestUtils::bodyToString)
            .collect(Collectors.toList());

    PAssert.that(result).containsInAnyOrder(expectedBodies);

    final Duration initialDelay = Duration.standardSeconds(5);
    List<RabbitMqMessage> toPublish = new ArrayList<>();
    toPublish.addAll(toPublishMatches);
    toPublish.addAll(toPublishIgnored);
    final List<RabbitMqMessage> finalToPublish = Collections.unmodifiableList(toPublish);

    try {
      if (!newQueue.isDefaultExchange()) {
        connectionHandler.useChannel(testId, channel -> channel.exchangeDeclare(newQueue.getExchange(), newQueue.getExchangeType()));
      }
      Thread publisher =
          RabbitMqTestUtils.publishMessagesThread(
              connectionHandler, testId, read, finalToPublish, initialDelay);
      publisher.start();
      p.run();
      publisher.join();
    } finally {
      if (!newQueue.isDefaultExchange()) {
        boolean ifUnused = false;
        connectionHandler.useChannel(
            testId,
            channel -> {
              try {
                channel.exchangeDeleteNoWait(newQueue.getExchange(), ifUnused);
              } catch (IOException e) {
                /* muted */
              }
              return null;
            });
      }
      connectionHandler.closeChannel(testId);
    }
  }

  @Test(timeout = ONE_MINUTE_MS)
  public void testReadFanoutExchange() throws Exception {
    final int expectedRecords = 10;
    final String uniqueId = RabbitMqTestUtils.mkUniqueSuffix();
    doNewQueueTest(
        RabbitMqIO.read()
            .withUri(uri)
            .withReadParadigm(
                ReadParadigm.NewQueue.fanoutExchange("exchange" + uniqueId, "queue" + uniqueId)),
        expectedRecords,
        RabbitMqTestUtils.generateRecords(expectedRecords),
        Collections.emptyList());
  }

  @Test(timeout = ONE_MINUTE_MS)
  public void testReadWildcardTopicExchange() throws Exception {
    final int expectedRecords = 10;
    final String uniqueId = RabbitMqTestUtils.mkUniqueSuffix();

    final List<RabbitMqMessage> expected = RabbitMqTestUtils.generateRecords(expectedRecords);
    doNewQueueTest(
        RabbitMqIO.read()
            .withUri(uri)
            .withReadParadigm(
                ReadParadigm.NewQueue.topicExchange(
                    "exchange" + uniqueId, "queue" + uniqueId, "#")),
        expectedRecords,
        expected,
        Collections.emptyList());
  }

  @Test(timeout = ONE_MINUTE_MS)
  public void testReadSpecificTopics() throws Exception {
    final int expectedRecords = 10;
    final String uniqueId = RabbitMqTestUtils.mkUniqueSuffix();

    final List<RabbitMqMessage> toPublishMatches =
        RabbitMqTestUtils.generateRecords(expectedRecords).stream()
            .map(
                msg ->
                    msg.toBuilder()
                        .setRoutingKey("prd." + RabbitMqTestUtils.mkUniqueSuffix())
                        .build())
            .collect(Collectors.toList());
    final List<RabbitMqMessage> toPublishIgnored =
        RabbitMqTestUtils.generateRecords(expectedRecords).stream()
            .map(
                msg ->
                    msg.toBuilder()
                        .setRoutingKey("dev." + RabbitMqTestUtils.mkUniqueSuffix())
                        .build())
            .collect(Collectors.toList());

    doNewQueueTest(
        RabbitMqIO.read()
            .withUri(uri)
            .withReadParadigm(
                ReadParadigm.NewQueue.topicExchange(
                    "exchange" + uniqueId, "queue" + uniqueId, "prd.*")),
        expectedRecords,
        toPublishMatches,
        toPublishIgnored);
  }

  @Test(timeout = ONE_MINUTE_MS)
  public void testRecordIdDeduplicationHonored() throws Exception {
    final int expectedRecords = 1;
    final String uniqueId = RabbitMqTestUtils.mkUniqueSuffix();

    final RabbitMqMessage msg =
        RabbitMqTestUtils.generateRecord(1).toBuilder().setMessageId(uniqueId).build();

    doNewQueueTest(
        RabbitMqIO.read()
            .withUri(uri)
            .withReadParadigm(
                ReadParadigm.NewQueue.fanoutExchange("exchange" + uniqueId, "queue" + uniqueId))
            .withRecordIdPolicy(RecordIdPolicy.messageId()),
        expectedRecords,
        Collections.singletonList(msg),
        Collections.singletonList(msg));
  }

  @Test(expected = Pipeline.PipelineExecutionException.class)
  public void testUseRecordIdFailsWhenIdsMissing() throws Exception {
    final int expectedRecords = 1;
    final String uniqueId = RabbitMqTestUtils.mkUniqueSuffix();

    final RabbitMqMessage msg =
        RabbitMqTestUtils.generateRecord(1).toBuilder().setMessageId(null).build();

    doNewQueueTest(
        RabbitMqIO.read()
            .withUri(uri)
            .withReadParadigm(
                ReadParadigm.NewQueue.fanoutExchange("exchange" + uniqueId, "queue" + uniqueId))
            .withRecordIdPolicy(RecordIdPolicy.messageId()),
        expectedRecords,
        Collections.singletonList(msg),
        Collections.emptyList());
  }

  @Test
  public void testWrite() throws Exception {
    final UUID testId = UUID.randomUUID();

    final String suffix = RabbitMqTestUtils.mkUniqueSuffix();
    final String queueName = "queue" + suffix;
    final String exchangeName = "exchange" + suffix;

    final int maxNumRecords = 1000;
    List<RabbitMqMessage> data =
        RabbitMqTestUtils.generateRecords(maxNumRecords).stream()
            .map(
                msg ->
                    msg.toBuilder()
                        .setRoutingKey("test." + RabbitMqTestUtils.mkUniqueSuffix())
                        .build())
            .collect(Collectors.toList());
    p.apply(Create.of(data)).apply(RabbitMqIO.write().withUri(uri).withExchange(exchangeName));

    final List<String> received = new ArrayList<>();
    try {
      connectionHandler.useChannel(
          testId,
          channel -> {
            channel.exchangeDeclare(exchangeName, BuiltinExchangeType.TOPIC);
            channel.queueDeclare(queueName, true, false, false, null);
            return channel.queueBind(queueName, exchangeName, "test.*");
          });

      p.run();

      connectionHandler.useChannel(testId, channel -> {
        Consumer consumer = new RabbitMqTestUtils.TestConsumer(channel, received);
        channel.basicConsume(queueName, /* auto-ack */ true, consumer);
        while (received.size() < maxNumRecords) {
          try {
            Thread.sleep(250);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
        return null;
      });

      assertEquals(maxNumRecords, received.size());
      for (int i = 0; i < maxNumRecords; i++) {
        assertTrue(received.contains("Test " + i));
      }

    } finally {
      connectionHandler.closeChannel(testId);
    }
  }
}
