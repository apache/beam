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
import static org.junit.Assert.fail;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Method;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.Serializable;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.common.NetworkTestHelper;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.apache.qpid.server.SystemLauncher;
import org.apache.qpid.server.model.SystemConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test of {@link RabbitMqIO}. */
@RunWith(JUnit4.class)
public class RabbitMqIOTest implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(RabbitMqIOTest.class);

  private static int port;
  private static String defaultPort;

  @ClassRule public static TemporaryFolder temporaryFolder = new TemporaryFolder();
  @ClassRule public static Timeout classTimeout = new Timeout(5, TimeUnit.MINUTES);

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
  }

  @AfterClass
  public static void afterClass() {
    launcher.shutdown();
    if (defaultPort != null) {
      System.setProperty("qpid.amqp_port", defaultPort);
    } else {
      System.clearProperty("qpid.amqp_port");
    }
  }

  @Test
  public void testReadQueue() throws Exception {
    final int maxNumRecords = 10;
    PCollection<RabbitMqMessage> raw =
        p.apply(
            RabbitMqIO.read()
                .withUri("amqp://guest:guest@localhost:" + port)
                .withQueue("READ")
                .withMaxNumRecords(maxNumRecords));
    PCollection<String> output =
        raw.apply(
            MapElements.into(TypeDescriptors.strings())
                .via(
                    (RabbitMqMessage message) ->
                        RabbitMqTestUtils.recordToString(message.getBody())));

    List<String> records =
        RabbitMqTestUtils.generateRecords(maxNumRecords).stream()
            .map(RabbitMqTestUtils::recordToString)
            .collect(Collectors.toList());
    PAssert.that(output).containsInAnyOrder(records);

    ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setUri("amqp://guest:guest@localhost:" + port);
    Connection connection = null;
    Channel channel = null;
    try {
      connection = connectionFactory.newConnection();
      channel = connection.createChannel();
      channel.queueDeclare("READ", false, false, false, null);
      for (String record : records) {
        channel.basicPublish("", "READ", null, record.getBytes(StandardCharsets.UTF_8));
      }

      p.run();
    } finally {
      if (channel != null) {
        channel.close();
      }
      if (connection != null) {
        connection.close();
      }
    }
  }

  /**
   * Helper for running tests against an exchange.
   *
   * <p>This function will automatically specify (and overwrite) the uri and numRecords values of
   * the Read definition.
   */
  private void doExchangeTest(ExchangeTestPlan testPlan, boolean simulateIncompatibleExchange)
      throws Exception {
    final byte[] terminalRecord = new byte[0];
    final String uri = "amqp://guest:guest@localhost:" + port;
    RabbitMqIO.Read read = testPlan.getRead();
    PCollection<RabbitMqMessage> raw =
        p.apply(read.withUri(uri).withMaxNumRecords(testPlan.getNumRecords()));

    PCollection<String> result =
        raw.apply(
            MapElements.into(TypeDescriptors.strings())
                .via(
                    (RabbitMqMessage message) ->
                        RabbitMqTestUtils.recordToString(message.getBody())));

    List<String> expected = testPlan.expectedResults();

    PAssert.that(result).containsInAnyOrder(expected);

    // on UUID fallback: tests appear to execute concurrently in jenkins, so
    // exchanges and queues between tests must be distinct
    String exchange =
        Optional.ofNullable(read.exchange()).orElseGet(() -> UUID.randomUUID().toString());
    String exchangeType = Optional.ofNullable(read.exchangeType()).orElse("fanout");
    if (simulateIncompatibleExchange) {
      // Rabbit will fail when attempting to declare an existing exchange that
      // has different properties (e.g. declaring a non-durable exchange if
      // an existing one is durable). QPid does not exhibit this behavior. To
      // simulate the error condition where RabbitMqIO attempts to declare an
      // incompatible exchange, we instead declare an exchange with the same
      // name but of a different type. Both Rabbit and QPid will fail this.
      if ("fanout".equalsIgnoreCase(exchangeType)) {
        exchangeType = "direct";
      } else {
        exchangeType = "fanout";
      }
    }
    final String finalExchangeType = exchangeType;
    final CountDownLatch waitForExchangeToBeDeclared = new CountDownLatch(1);
    final BlockingQueue<byte[]> recordsToPublish = new LinkedBlockingQueue<>();
    recordsToPublish.addAll(RabbitMqTestUtils.generateRecords(testPlan.getNumRecordsToPublish()));
    Thread publisher =
        new Thread(
            () -> {
              Connection connection = null;
              Channel channel = null;
              try {
                ConnectionFactory connectionFactory = new ConnectionFactory();
                connectionFactory.setAutomaticRecoveryEnabled(false);
                connectionFactory.setUri(uri);
                connection = connectionFactory.newConnection();
                channel = connection.createChannel();
                channel.exchangeDeclare(exchange, finalExchangeType);
                // We are relying on the pipeline to declare the queue and messages that are
                // published without a queue being declared are "unroutable". Since there is a race
                // between when the pipeline declares and when we can start publishing, we add a
                // handler to republish messages that are returned to us.
                channel.addReturnListener(
                    (replyCode, replyText, exchange1, routingKey, properties, body) -> {
                      try {
                        recordsToPublish.put(body);
                      } catch (Exception e) {
                        throw new RuntimeException(e);
                      }
                    });
                waitForExchangeToBeDeclared.countDown();
                while (true) {
                  byte[] record = recordsToPublish.take();
                  if (record == terminalRecord) {
                    return;
                  }
                  channel.basicPublish(
                      exchange,
                      testPlan.publishRoutingKeyGen().get(),
                      true, // ensure that messages are returned to sender
                      testPlan.getPublishProperties(),
                      record);
                }

              } catch (Exception e) {
                throw new RuntimeException(e);
              } finally {
                if (channel != null) {
                  // channel may have already been closed automatically due to protocol failure
                  try {
                    channel.close();
                  } catch (Exception e) {
                    /* ignored */
                  }
                }
                if (connection != null) {
                  // connection may have already been closed automatically due to protocol failure
                  try {
                    connection.close();
                  } catch (Exception e) {
                    /* ignored */
                  }
                }
              }
            });
    publisher.start();
    waitForExchangeToBeDeclared.await();
    p.run();
    recordsToPublish.put(terminalRecord);
    publisher.join();
  }

  private void doExchangeTest(ExchangeTestPlan testPlan) throws Exception {
    doExchangeTest(testPlan, false);
  }

  @Test
  public void testReadDeclaredFanoutExchange() throws Exception {
    doExchangeTest(
        new ExchangeTestPlan(
            RabbitMqIO.read().withExchange("DeclaredFanoutExchange", "fanout", "ignored"), 10));
  }

  @Test
  public void testReadDeclaredTopicExchangeWithQueueDeclare() throws Exception {
    doExchangeTest(
        new ExchangeTestPlan(
            RabbitMqIO.read()
                .withExchange("DeclaredTopicExchangeWithQueueDeclare", "topic", "#")
                .withQueue("declared-queue")
                .withQueueDeclare(true),
            10));
  }

  @Test
  public void testReadDeclaredTopicExchange() throws Exception {
    final int numRecords = 10;
    RabbitMqIO.Read read =
        RabbitMqIO.read().withExchange("DeclaredTopicExchange", "topic", "user.create.#");

    final Supplier<String> publishRoutingKeyGen =
        new Supplier<String>() {
          private AtomicInteger counter = new AtomicInteger(0);

          @Override
          public String get() {
            int count = counter.getAndIncrement();
            if (count % 2 == 0) {
              return "user.create." + count;
            }
            return "user.delete." + count;
          }
        };

    ExchangeTestPlan plan =
        new ExchangeTestPlan(read, numRecords / 2, numRecords) {
          @Override
          public Supplier<String> publishRoutingKeyGen() {
            return publishRoutingKeyGen;
          }

          @Override
          public List<String> expectedResults() {
            return IntStream.range(0, numRecords)
                .filter(i -> i % 2 == 0)
                .mapToObj(RabbitMqTestUtils::generateRecord)
                .map(RabbitMqTestUtils::recordToString)
                .collect(Collectors.toList());
          }
        };

    doExchangeTest(plan);
  }

  @Test
  public void testDeclareIncompatibleExchangeFails() throws Exception {
    RabbitMqIO.Read read =
        RabbitMqIO.read().withExchange("IncompatibleExchange", "direct", "unused");
    try {
      doExchangeTest(new ExchangeTestPlan(read, 1), true);
      fail("Expected to have failed to declare an incompatible exchange");
    } catch (Exception e) {
      Throwable cause = Throwables.getRootCause(e);
      if (cause instanceof ShutdownSignalException) {
        ShutdownSignalException sse = (ShutdownSignalException) cause;
        Method reason = sse.getReason();
        if (reason instanceof com.rabbitmq.client.AMQP.Connection.Close) {
          com.rabbitmq.client.AMQP.Connection.Close close =
              (com.rabbitmq.client.AMQP.Connection.Close) reason;
          assertEquals("Expected failure is 530: not-allowed", 530, close.getReplyCode());
        } else {
          fail(
              "Unexpected ShutdownSignalException reason. Expected Connection.Close. Got: "
                  + reason);
        }
      } else {
        fail("Expected to fail with ShutdownSignalException. Instead failed with " + cause);
      }
    }
  }

  @Test
  public void testUseCorrelationIdSucceedsWhenIdsPresent() throws Exception {
    int messageCount = 1;
    AMQP.BasicProperties publishProps =
        new AMQP.BasicProperties().builder().correlationId("123").build();
    doExchangeTest(
        new ExchangeTestPlan(
            RabbitMqIO.read()
                .withExchange("CorrelationIdSuccess", "fanout")
                .withUseCorrelationId(true),
            messageCount,
            messageCount,
            publishProps));
  }

  @Test(expected = Pipeline.PipelineExecutionException.class)
  public void testUseCorrelationIdFailsWhenIdsMissing() throws Exception {
    int messageCount = 1;
    AMQP.BasicProperties publishProps = null;
    doExchangeTest(
        new ExchangeTestPlan(
            RabbitMqIO.read()
                .withExchange("CorrelationIdFailure", "fanout")
                .withUseCorrelationId(true),
            messageCount,
            messageCount,
            publishProps));
  }

  @Test(expected = Pipeline.PipelineExecutionException.class)
  public void testQueueDeclareWithoutQueueNameFails() throws Exception {
    RabbitMqIO.Read read = RabbitMqIO.read().withQueueDeclare(true);
    doExchangeTest(new ExchangeTestPlan(read, 1));
  }

  @Test
  public void testWriteQueue() throws Exception {
    final int maxNumRecords = 1000;
    List<RabbitMqMessage> data =
        RabbitMqTestUtils.generateRecords(maxNumRecords).stream()
            .map(RabbitMqMessage::new)
            .collect(Collectors.toList());
    p.apply(Create.of(data))
        .apply(
            RabbitMqIO.write().withUri("amqp://guest:guest@localhost:" + port).withQueue("TEST"));

    ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setUri("amqp://guest:guest@localhost:" + port);
    Connection connection = null;
    Channel channel = null;
    try {
      connection = connectionFactory.newConnection();
      channel = connection.createChannel();
      channel.queueDeclare("TEST", true, false, false, null);
      RabbitMqTestUtils.TestConsumer consumer = new RabbitMqTestUtils.TestConsumer(channel);
      channel.basicConsume("TEST", true, consumer);

      p.run();

      while (consumer.getReceived().size() < maxNumRecords) {
        Thread.sleep(500);
      }

      assertEquals(maxNumRecords, consumer.getReceived().size());
      for (int i = 0; i < maxNumRecords; i++) {
        assertTrue(consumer.getReceived().contains("Test " + i));
      }
    } finally {
      if (channel != null) {
        channel.close();
      }
      if (connection != null) {
        connection.close();
      }
    }
  }

  @Test
  public void testWriteExchange() throws Exception {
    final int maxNumRecords = 1000;
    List<RabbitMqMessage> data =
        RabbitMqTestUtils.generateRecords(maxNumRecords).stream()
            .map(RabbitMqMessage::new)
            .collect(Collectors.toList());
    p.apply(Create.of(data))
        .apply(
            RabbitMqIO.write()
                .withUri("amqp://guest:guest@localhost:" + port)
                .withExchange("WRITE", "fanout"));

    ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setUri("amqp://guest:guest@localhost:" + port);
    Connection connection = null;
    Channel channel = null;
    try {
      connection = connectionFactory.newConnection();
      channel = connection.createChannel();
      channel.exchangeDeclare("WRITE", "fanout");
      String queueName = channel.queueDeclare().getQueue();
      channel.queueBind(queueName, "WRITE", "");
      RabbitMqTestUtils.TestConsumer consumer = new RabbitMqTestUtils.TestConsumer(channel);
      channel.basicConsume(queueName, true, consumer);

      p.run();

      while (consumer.getReceived().size() < maxNumRecords) {
        Thread.sleep(500);
      }

      assertEquals(maxNumRecords, consumer.getReceived().size());
      for (int i = 0; i < maxNumRecords; i++) {
        assertTrue(consumer.getReceived().contains("Test " + i));
      }
    } finally {
      if (channel != null) {
        channel.close();
      }
      if (connection != null) {
        connection.close();
      }
    }
  }
}
