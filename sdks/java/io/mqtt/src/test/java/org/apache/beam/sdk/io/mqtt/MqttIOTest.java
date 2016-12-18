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

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.Connection;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.Future;
import org.fusesource.mqtt.client.FutureConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests of {@link MqttIO}.
 */
@RunWith(JUnit4.class)
public class MqttIOTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(MqttIOTest.class);

  private static transient BrokerService brokerService;

  private static int port;

  @Before
  public void startBroker() throws Exception {
    LOGGER.info("Finding free network port");
    ServerSocket socket = new ServerSocket(0);
    port = socket.getLocalPort();
    socket.close();

    LOGGER.info("Starting ActiveMQ brokerService on {}", port);
    brokerService = new BrokerService();
    brokerService.setDeleteAllMessagesOnStartup(true);
    brokerService.setPersistent(false);
    brokerService.addConnector("mqtt://localhost:" + port);
    brokerService.start();
    brokerService.waitUntilStarted();
  }

  @Test(timeout = 60 * 1000)
  @Category(NeedsRunner.class)
  public void testRead() throws Exception {
    final Pipeline pipeline = TestPipeline.create();

    PCollection<byte[]> output = pipeline.apply(
        MqttIO.read()
            .withConnectionConfiguration(
                MqttIO.ConnectionConfiguration.create(
                    "tcp://localhost:" + port,
                    "READ_TOPIC",
                    "READ_PIPELINE"))
          .withMaxNumRecords(10));
    PAssert.that(output).containsInAnyOrder(
        "This is test 0".getBytes(),
        "This is test 1".getBytes(),
        "This is test 2".getBytes(),
        "This is test 3".getBytes(),
        "This is test 4".getBytes(),
        "This is test 5".getBytes(),
        "This is test 6".getBytes(),
        "This is test 7".getBytes(),
        "This is test 8".getBytes(),
        "This is test 9".getBytes()
    );

    // produce messages on the brokerService in another thread
    // This thread prevents to block the pipeline waiting for new messages
    MQTT client = new MQTT();
    client.setHost("tcp://localhost:" + port);
    final BlockingConnection publishConnection = client.blockingConnection();
    publishConnection.connect();
    Thread publisherThread = new Thread() {
      public void run() {
        try {
          LOGGER.info("Waiting pipeline connected to the MQTT broker before sending "
              + "messages ...");
          boolean pipelineConnected = false;
          while (!pipelineConnected) {
            Thread.sleep(1000);
            for (Connection connection : brokerService.getBroker().getClients()) {
              if (connection.getConnectionId().startsWith("READ_PIPELINE")) {
                pipelineConnected = true;
              }
            }
          }
          for (int i = 0; i < 10; i++) {
            publishConnection.publish("READ_TOPIC", ("This is test " + i).getBytes(),
                QoS.AT_LEAST_ONCE, false);
          }
        } catch (Exception e) {
          // nothing to do
        }
      }
    };
    publisherThread.start();
    pipeline.run();

    publishConnection.disconnect();
    publisherThread.join();
  }

  @Test
  public void testWrite() throws Exception {
    MQTT client = new MQTT();
    client.setHost("tcp://localhost:" + port);
    FutureConnection connection = client.futureConnection();
    Future<Void> f1 = connection.connect();
    Future<Message> receive = connection.receive();
    connection.subscribe(new Topic[]{new Topic(Buffer.utf8("WRITE_TOPIC"), QoS.EXACTLY_ONCE)});

    Pipeline pipeline = TestPipeline.create();

    ArrayList<byte[]> data = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      data.add(("Test " + i).getBytes());
    }
    pipeline.apply(Create.of(data))
        .apply(MqttIO.write()
            .withConnectionConfiguration(
                MqttIO.ConnectionConfiguration.create(
                    "tcp://localhost:" + port,
                    "WRITE_TOPIC")));
    pipeline.run();

    Set<String> messages = new HashSet<>();

    for (int i = 0; i < 200; i++) {
      Message message = receive.await();
      messages.add(new String(message.getPayload()));
      message.ack();
      receive = connection.receive();
    }

    Future<Void> f4 = connection.disconnect();
    f4.await();

    assertEquals(200, messages.size());
    for (int i = 0; i < 200; i++) {
      assertTrue(messages.contains("Test " + i));
    }
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
