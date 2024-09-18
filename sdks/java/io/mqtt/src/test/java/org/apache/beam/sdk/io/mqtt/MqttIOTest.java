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
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.Connection;
import org.apache.beam.sdk.io.common.NetworkTestHelper;
import org.apache.beam.sdk.io.mqtt.MqttIO.Read;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
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
    Read mqttReader =
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
