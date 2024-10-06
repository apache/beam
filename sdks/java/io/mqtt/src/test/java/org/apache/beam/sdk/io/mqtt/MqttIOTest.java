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
package org.apache.beam.sdk.io.mqtt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.Connection;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.common.NetworkTestHelper;
import org.apache.beam.sdk.io.mqtt.MqttIO.Read;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests of {@link MqttIO}. */
@RunWith(JUnit4.class)
public class MqttIOTest {

  private static final Logger LOG = LoggerFactory.getLogger(MqttIOTest.class);

  private BrokerService brokerService;

  private int port;

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Before
  public void startBroker() throws Exception {
    port = NetworkTestHelper.getAvailableLocalPort();
    LOG.info("Starting ActiveMQ brokerService on {}", port);
    brokerService = new BrokerService();
    brokerService.setDeleteAllMessagesOnStartup(true);
    // use memory persistence for the test: it's faster and don't pollute test folder with KahaDB
    brokerService.setPersistent(false);
    brokerService.addConnector("mqtt://localhost:" + port);
    brokerService.start();
    brokerService.waitUntilStarted();
  }

  @Test(timeout = 60 * 1000)
  @Ignore("https://github.com/apache/beam/issues/18723 Test timeout failure.")
  public void testReadNoClientId() throws Exception {
    final String topicName = "READ_TOPIC_NO_CLIENT_ID";
    Read<byte[]> mqttReader =
        MqttIO.read()
            .withConnectionConfiguration(
                MqttIO.ConnectionConfiguration.create("tcp://localhost:" + port, topicName))
            .withMaxNumRecords(10);
    PCollection<byte[]> output = pipeline.apply(mqttReader);
    PAssert.that(output)
        .containsInAnyOrder(
            "This is test 0".getBytes(StandardCharsets.UTF_8),
            "This is test 1".getBytes(StandardCharsets.UTF_8),
            "This is test 2".getBytes(StandardCharsets.UTF_8),
            "This is test 3".getBytes(StandardCharsets.UTF_8),
            "This is test 4".getBytes(StandardCharsets.UTF_8),
            "This is test 5".getBytes(StandardCharsets.UTF_8),
            "This is test 6".getBytes(StandardCharsets.UTF_8),
            "This is test 7".getBytes(StandardCharsets.UTF_8),
            "This is test 8".getBytes(StandardCharsets.UTF_8),
            "This is test 9".getBytes(StandardCharsets.UTF_8));

    // produce messages on the brokerService in another thread
    // This thread prevents to block the pipeline waiting for new messages
    MQTT client = new MQTT();
    client.setHost("tcp://localhost:" + port);
    final BlockingConnection publishConnection = client.blockingConnection();
    publishConnection.connect();
    Thread publisherThread =
        new Thread(
            () -> {
              try {
                LOG.info(
                    "Waiting pipeline connected to the MQTT broker before sending "
                        + "messages ...");
                boolean pipelineConnected = false;
                while (!pipelineConnected) {
                  Thread.sleep(1000);
                  for (Connection connection : brokerService.getBroker().getClients()) {
                    if (!connection.getConnectionId().isEmpty()) {
                      pipelineConnected = true;
                    }
                  }
                }
                for (int i = 0; i < 10; i++) {
                  publishConnection.publish(
                      topicName,
                      ("This is test " + i).getBytes(StandardCharsets.UTF_8),
                      QoS.EXACTLY_ONCE,
                      false);
                }
              } catch (Exception e) {
                // nothing to do
              }
            });
    publisherThread.start();
    pipeline.run();

    publishConnection.disconnect();
    publisherThread.join();
  }

  @Test(timeout = 40 * 1000)
  public void testRead() throws Exception {
    PCollection<byte[]> output =
        pipeline.apply(
            MqttIO.read()
                .withConnectionConfiguration(
                    MqttIO.ConnectionConfiguration.create("tcp://localhost:" + port, "READ_TOPIC")
                        .withClientId("READ_PIPELINE"))
                .withMaxReadTime(Duration.standardSeconds(5)));
    PAssert.that(output)
        .containsInAnyOrder(
            "This is test 0".getBytes(StandardCharsets.UTF_8),
            "This is test 1".getBytes(StandardCharsets.UTF_8),
            "This is test 2".getBytes(StandardCharsets.UTF_8),
            "This is test 3".getBytes(StandardCharsets.UTF_8),
            "This is test 4".getBytes(StandardCharsets.UTF_8),
            "This is test 5".getBytes(StandardCharsets.UTF_8),
            "This is test 6".getBytes(StandardCharsets.UTF_8),
            "This is test 7".getBytes(StandardCharsets.UTF_8),
            "This is test 8".getBytes(StandardCharsets.UTF_8),
            "This is test 9".getBytes(StandardCharsets.UTF_8));

    // produce messages on the brokerService in another thread
    // This thread prevents to block the pipeline waiting for new messages
    MQTT client = new MQTT();
    client.setHost("tcp://localhost:" + port);
    final BlockingConnection publishConnection = client.blockingConnection();
    publishConnection.connect();
    Thread publisherThread =
        new Thread(
            () -> {
              try {
                LOG.info(
                    "Waiting pipeline connected to the MQTT broker before sending "
                        + "messages ...");
                boolean pipelineConnected = false;
                while (!pipelineConnected) {
                  for (Connection connection : brokerService.getBroker().getClients()) {
                    if (connection.getConnectionId().startsWith("READ_PIPELINE")) {
                      pipelineConnected = true;
                    }
                  }
                  Thread.sleep(1000);
                }
                for (int i = 0; i < 10; i++) {
                  publishConnection.publish(
                      "READ_TOPIC",
                      ("This is test " + i).getBytes(StandardCharsets.UTF_8),
                      QoS.EXACTLY_ONCE,
                      false);
                }
              } catch (Exception e) {
                // nothing to do
              }
            });
    publisherThread.start();
    pipeline.run();

    publisherThread.join();
    publishConnection.disconnect();
  }

  @Test(timeout = 60 * 1000)
  public void testReadWithMetadata() throws Exception {
    final String wildcardTopic = "topic/#";
    final String topic1 = "topic/1";
    final String topic2 = "topic/2";

    final PTransform<PBegin, PCollection<MqttRecord>> mqttReaderWithMetadata =
        MqttIO.readWithMetadata()
            .withConnectionConfiguration(
                MqttIO.ConnectionConfiguration.create("tcp://localhost:" + port, wildcardTopic))
            .withMaxNumRecords(10);

    final PCollection<MqttRecord> output = pipeline.apply(mqttReaderWithMetadata);
    PAssert.that(output)
        .containsInAnyOrder(
            new MqttRecord(topic1, "This is test 0".getBytes(StandardCharsets.UTF_8)),
            new MqttRecord(topic1, "This is test 1".getBytes(StandardCharsets.UTF_8)),
            new MqttRecord(topic1, "This is test 2".getBytes(StandardCharsets.UTF_8)),
            new MqttRecord(topic1, "This is test 3".getBytes(StandardCharsets.UTF_8)),
            new MqttRecord(topic1, "This is test 4".getBytes(StandardCharsets.UTF_8)),
            new MqttRecord(topic2, "This is test 5".getBytes(StandardCharsets.UTF_8)),
            new MqttRecord(topic2, "This is test 6".getBytes(StandardCharsets.UTF_8)),
            new MqttRecord(topic2, "This is test 7".getBytes(StandardCharsets.UTF_8)),
            new MqttRecord(topic2, "This is test 8".getBytes(StandardCharsets.UTF_8)),
            new MqttRecord(topic2, "This is test 9".getBytes(StandardCharsets.UTF_8)));

    // produce messages on the brokerService in another thread
    // This thread prevents to block the pipeline waiting for new messages
    MQTT client = new MQTT();
    client.setHost("tcp://localhost:" + port);
    final BlockingConnection publishConnection = client.blockingConnection();
    publishConnection.connect();
    Thread publisherThread =
        new Thread(
            () -> {
              try {
                LOG.info(
                    "Waiting pipeline connected to the MQTT broker before sending "
                        + "messages ...");
                boolean pipelineConnected = false;
                while (!pipelineConnected) {
                  Thread.sleep(1000);
                  for (Connection connection : brokerService.getBroker().getClients()) {
                    if (!connection.getConnectionId().isEmpty()) {
                      pipelineConnected = true;
                    }
                  }
                }
                for (int i = 0; i < 5; i++) {
                  publishConnection.publish(
                      topic1,
                      ("This is test " + i).getBytes(StandardCharsets.UTF_8),
                      QoS.EXACTLY_ONCE,
                      false);
                }
                for (int i = 5; i < 10; i++) {
                  publishConnection.publish(
                      topic2,
                      ("This is test " + i).getBytes(StandardCharsets.UTF_8),
                      QoS.EXACTLY_ONCE,
                      false);
                }

              } catch (Exception e) {
                // nothing to do
              }
            });

    publisherThread.start();
    pipeline.run();

    publishConnection.disconnect();
    publisherThread.join();
  }

  /** Test for BEAM-3282: this test should not timeout. */
  @Test(timeout = 30 * 1000)
  public void testReceiveWithTimeoutAndNoData() throws Exception {
    pipeline.apply(
        MqttIO.read()
            .withConnectionConfiguration(
                MqttIO.ConnectionConfiguration.create("tcp://localhost:" + port, "READ_TOPIC")
                    .withClientId("READ_PIPELINE"))
            .withMaxReadTime(Duration.standardSeconds(2)));

    // should stop before the test timeout
    pipeline.run();
  }

  @Test
  public void testWrite() throws Exception {
    final int numberOfTestMessages = 200;
    MQTT client = new MQTT();
    client.setHost("tcp://localhost:" + port);
    final BlockingConnection connection = client.blockingConnection();
    connection.connect();
    connection.subscribe(new Topic[] {new Topic(Buffer.utf8("WRITE_TOPIC"), QoS.EXACTLY_ONCE)});

    final Set<String> messages = new ConcurrentSkipListSet<>();

    Thread subscriber =
        new Thread(
            () -> {
              try {
                for (int i = 0; i < numberOfTestMessages; i++) {
                  Message message = connection.receive();
                  messages.add(new String(message.getPayload(), StandardCharsets.UTF_8));
                  message.ack();
                }
              } catch (Exception e) {
                LOG.error("Can't receive message", e);
              }
            });
    subscriber.start();

    ArrayList<byte[]> data = new ArrayList<>();
    for (int i = 0; i < numberOfTestMessages; i++) {
      data.add(("Test " + i).getBytes(StandardCharsets.UTF_8));
    }
    pipeline
        .apply(Create.of(data))
        .apply(
            MqttIO.write()
                .withConnectionConfiguration(
                    MqttIO.ConnectionConfiguration.create(
                        "tcp://localhost:" + port, "WRITE_TOPIC")));
    pipeline.run();
    subscriber.join();

    connection.disconnect();

    assertEquals(numberOfTestMessages, messages.size());
    for (int i = 0; i < numberOfTestMessages; i++) {
      assertTrue(messages.contains("Test " + i));
    }
  }

  @Test
  public void testDynamicWrite() throws Exception {
    final int numberOfTopic1Count = 100;
    final int numberOfTopic2Count = 100;
    final int numberOfTestMessages = numberOfTopic1Count + numberOfTopic2Count;

    MQTT client = new MQTT();
    client.setHost("tcp://localhost:" + port);
    final BlockingConnection connection = client.blockingConnection();
    connection.connect();
    final String writeTopic1 = "WRITE_TOPIC_1";
    final String writeTopic2 = "WRITE_TOPIC_2";
    connection.subscribe(
        new Topic[] {
          new Topic(Buffer.utf8(writeTopic1), QoS.EXACTLY_ONCE),
          new Topic(Buffer.utf8(writeTopic2), QoS.EXACTLY_ONCE)
        });

    final Map<String, List<String>> messageMap = new ConcurrentSkipListMap<>();
    final Thread subscriber =
        new Thread(
            () -> {
              try {
                for (int i = 0; i < numberOfTestMessages; i++) {
                  Message message = connection.receive();
                  List<String> messages = messageMap.get(message.getTopic());
                  if (messages == null) {
                    messages = new ArrayList<>();
                  }
                  messages.add(new String(message.getPayload(), StandardCharsets.UTF_8));
                  messageMap.put(message.getTopic(), messages);
                  message.ack();
                }
              } catch (Exception e) {
                LOG.error("Can't receive message", e);
              }
            });

    subscriber.start();

    ArrayList<KV<String, byte[]>> data = new ArrayList<>();
    for (int i = 0; i < numberOfTopic1Count; i++) {
      data.add(KV.of(writeTopic1, ("Test" + i).getBytes(StandardCharsets.UTF_8)));
    }

    for (int i = 0; i < numberOfTopic2Count; i++) {
      data.add(KV.of(writeTopic2, ("Test" + i).getBytes(StandardCharsets.UTF_8)));
    }

    pipeline
        .apply(Create.of(data))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), ByteArrayCoder.of()))
        .apply(
            MqttIO.<KV<String, byte[]>>dynamicWrite()
                .withConnectionConfiguration(
                    MqttIO.ConnectionConfiguration.create("tcp://localhost:" + port)
                        .withClientId("READ_PIPELINE"))
                .withTopicFn(input -> input.getKey())
                .withPayloadFn(input -> input.getValue()));

    pipeline.run();
    subscriber.join();

    connection.disconnect();

    assertEquals(
        numberOfTestMessages, messageMap.values().stream().mapToLong(Collection::size).sum());

    assertEquals(2, messageMap.keySet().size());
    assertTrue(messageMap.containsKey(writeTopic1));
    assertTrue(messageMap.containsKey(writeTopic2));
    for (Map.Entry<String, List<String>> entry : messageMap.entrySet()) {
      final List<String> messages = entry.getValue();
      messages.forEach(message -> assertTrue(message.contains("Test")));
      if (entry.getKey().equals(writeTopic1)) {
        assertEquals(numberOfTopic1Count, messages.size());
      } else {
        assertEquals(numberOfTopic2Count, messages.size());
      }
    }
  }

  @Test
  public void testReadHaveNoConnectionConfiguration() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> MqttIO.read().expand(PBegin.in(pipeline)));

    assertEquals("connectionConfiguration can not be null", exception.getMessage());
  }

  @Test
  public void testReadHaveNoTopic() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                MqttIO.read()
                    .withConnectionConfiguration(MqttIO.ConnectionConfiguration.create("serverUri"))
                    .expand(PBegin.in(pipeline)));

    assertEquals("topic can not be null", exception.getMessage());

    pipeline.run();
  }

  @Test
  public void testWriteHaveNoConnectionConfiguration() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> MqttIO.write().expand(pipeline.apply(Create.of(new byte[] {}))));

    assertEquals("connectionConfiguration can not be null", exception.getMessage());

    pipeline.run();
  }

  @Test
  public void testWriteHaveNoTopic() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                MqttIO.write()
                    .withConnectionConfiguration(MqttIO.ConnectionConfiguration.create("serverUri"))
                    .expand(pipeline.apply(Create.of(new byte[] {}))));

    assertEquals("topic can not be null", exception.getMessage());

    pipeline.run();
  }

  @Test
  public void testDynamicWriteHaveNoConnectionConfiguration() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> MqttIO.dynamicWrite().expand(pipeline.apply(Create.of(new byte[] {}))));

    assertEquals("connectionConfiguration can not be null", exception.getMessage());

    pipeline.run();
  }

  @Test
  public void testDynamicWriteHaveNoTopicFn() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                MqttIO.dynamicWrite()
                    .withConnectionConfiguration(MqttIO.ConnectionConfiguration.create("serverUri"))
                    .expand(pipeline.apply(Create.of(new byte[] {}))));

    assertEquals("topicFn can not be null", exception.getMessage());

    pipeline.run();
  }

  @Test
  public void testDynamicWriteHaveNoPayloadFn() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                MqttIO.dynamicWrite()
                    .withConnectionConfiguration(MqttIO.ConnectionConfiguration.create("serverUri"))
                    .withTopicFn(input -> "topic")
                    .expand(pipeline.apply(Create.of(new byte[] {}))));

    assertEquals("payloadFn can not be null", exception.getMessage());

    pipeline.run();
  }

  @Test
  public void testDynamicWriteHaveStaticTopic() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                MqttIO.dynamicWrite()
                    .withConnectionConfiguration(
                        MqttIO.ConnectionConfiguration.create("serverUri", "topic"))
                    .expand(pipeline.apply(Create.of(new byte[] {}))));

    assertEquals("DynamicWrite can not have static topic", exception.getMessage());

    pipeline.run();
  }

  @Test
  public void testWriteWithTopicFn() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> MqttIO.write().withTopicFn(e -> "some topic"));

    assertEquals("withTopicFn can not use in non-dynamic write", exception.getMessage());
  }

  @Test
  public void testWriteWithPayloadFn() {
    final IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> MqttIO.write().withPayloadFn(e -> new byte[] {}));

    assertEquals("withPayloadFn can not use in non-dynamic write", exception.getMessage());
  }

  @Test
  public void testReadObject() throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(bos);
    MqttIO.MqttCheckpointMark cp1 = new MqttIO.MqttCheckpointMark(UUID.randomUUID().toString());
    out.writeObject(cp1);

    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    ObjectInputStream in = new ObjectInputStream(bis);
    MqttIO.MqttCheckpointMark cp2 = (MqttIO.MqttCheckpointMark) in.readObject();

    // there should be no bytes left in the stream
    assertEquals(0, in.available());
    // the number of messages of the decoded checkpoint should be zero
    assertEquals(0, cp2.messages.size());
    assertEquals(cp1.clientId, cp2.clientId);
    assertEquals(cp1.oldestMessageTimestamp, cp2.oldestMessageTimestamp);
  }

  @After
  public void stopBroker() throws Exception {
    if (brokerService != null) {
      brokerService.stop();
      brokerService.waitUntilStopped();
      brokerService = null;
    }
  }
}
