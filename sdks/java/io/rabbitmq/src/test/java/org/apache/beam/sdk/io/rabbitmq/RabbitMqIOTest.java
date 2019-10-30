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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.io.common.NetworkTestHelper;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.qpid.server.Broker;
import org.apache.qpid.server.BrokerOptions;
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
  @ClassRule public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule public transient TestPipeline p = TestPipeline.create();

  private static transient Broker broker;

  @BeforeClass
  public static void beforeClass() throws Exception {
    port = NetworkTestHelper.getAvailableLocalPort();

    System.setProperty("derby.stream.error.field", "MyApp.DEV_NULL");
    broker = new Broker();
    BrokerOptions options = new BrokerOptions();
    options.setConfigProperty(BrokerOptions.QPID_AMQP_PORT, String.valueOf(port));
    options.setConfigProperty(BrokerOptions.QPID_WORK_DIR, temporaryFolder.newFolder().toString());
    options.setConfigProperty(BrokerOptions.QPID_HOME_DIR, "src/test/qpid");
    broker.startup(options);
  }

  @AfterClass
  public static void afterClass() {
    broker.shutdown();
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
  public void doExchangeTest(ExchangeTestPlan testPlan) throws Exception {
    String uri = "amqp://guest:guest@localhost:" + port;
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

    String exchange =
        Optional.ofNullable(read.exchange()).orElseGet(() -> UUID.randomUUID().toString());
    String exchangeType = Optional.ofNullable(read.exchangeType()).orElse("fanout");

    ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setUri(uri);
    Connection connection = null;
    Channel channel = null;

    try {
      connection = connectionFactory.newConnection();
      channel = connection.createChannel();
      channel.exchangeDeclare(exchange, exchangeType);
      Channel finalChannel = channel;
      Thread publisher =
          new Thread(
              () -> {
                try {
                  Thread.sleep(5000);
                } catch (Exception e) {
                  LOG.error(e.getMessage(), e);
                }
                for (int i = 0; i < testPlan.getNumRecordsToPublish(); i++) {
                  try {
                    finalChannel.basicPublish(
                        exchange,
                        testPlan.publishRoutingKeyGen().get(),
                        null,
                        RabbitMqTestUtils.generateRecord(i));
                  } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                  }
                }
              });
      publisher.start();
      p.run();
      publisher.join();
    } finally {
      if (channel != null) {
        channel.close();
      }
      if (connection != null) {
        connection.close();
      }
    }
  }

  @Test(timeout = ONE_MINUTE_MS)
  public void testReadDeclaredFanoutExchange() throws Exception {
    doExchangeTest(
        new ExchangeTestPlan(RabbitMqIO.read().withExchange("READEXCHANGE", "fanout", null), 10));
  }

  @Test(timeout = ONE_MINUTE_MS)
  public void testReadDeclaredTopicExchange() throws Exception {
    final int numRecords = 10;
    RabbitMqIO.Read read = RabbitMqIO.read().withExchange("READTOPIC", "topic", "user.create.#");

    final Supplier<String> publishRoutingKeyGen = new Supplier<String>() {
      private AtomicInteger counter = new AtomicInteger(0);

      @Override
      public String get() {
        int count = counter.getAndIncrement();
        if (count % 2 == 0) return "user.create." + count;
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
  public void testWriteQueue() throws Exception {
    final int maxNumRecords = 1000;
    List<RabbitMqMessage> data =
        RabbitMqTestUtils.generateRecords(maxNumRecords).stream()
            .map(RabbitMqMessage::new)
            .collect(Collectors.toList());
    p.apply(Create.of(data))
        .apply(
            RabbitMqIO.write().withUri("amqp://guest:guest@localhost:" + port).withQueue("TEST"));

    final List<String> received = new ArrayList<>();
    ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setUri("amqp://guest:guest@localhost:" + port);
    Connection connection = null;
    Channel channel = null;
    try {
      connection = connectionFactory.newConnection();
      channel = connection.createChannel();
      channel.queueDeclare("TEST", true, false, false, null);
      Consumer consumer = new RabbitMqTestUtils.TestConsumer(channel, received);
      channel.basicConsume("TEST", true, consumer);

      p.run();

      while (received.size() < maxNumRecords) {
        Thread.sleep(500);
      }

      assertEquals(maxNumRecords, received.size());
      for (int i = 0; i < maxNumRecords; i++) {
        assertTrue(received.contains("Test " + i));
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

    final List<String> received = new ArrayList<>();
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
      Consumer consumer = new RabbitMqTestUtils.TestConsumer(channel, received);
      channel.basicConsume(queueName, true, consumer);

      p.run();

      while (received.size() < maxNumRecords) {
        Thread.sleep(500);
      }

      assertEquals(maxNumRecords, received.size());
      for (int i = 0; i < maxNumRecords; i++) {
        assertTrue(received.contains("Test " + i));
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
