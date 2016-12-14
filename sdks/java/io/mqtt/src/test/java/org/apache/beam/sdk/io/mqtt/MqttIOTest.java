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

import java.io.File;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.URI;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.Connection;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
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
public class MqttIOTest implements Serializable {

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
    brokerService.setPersistent(true);
    brokerService.setDataDirectory("target/activemq-data/");
    KahaDBStore kahaDBStore = new KahaDBStore();
    kahaDBStore.setDirectory(new File("target/activemq-data/"));
    brokerService.setPersistenceAdapter(kahaDBStore);
    brokerService.setAdvisorySupport(false);
    brokerService.setUseJmx(true);
    brokerService.getManagementContext().setCreateConnector(false);
    brokerService.setSchedulerSupport(true);
    brokerService.setPopulateJMSXUserID(true);
    TransportConnector mqttConnector = new TransportConnector();
    mqttConnector.setName("mqtt");
    mqttConnector.setUri(new URI("mqtt://localhost:" + port));
    mqttConnector.setAllowLinkStealing(true);
    brokerService.addConnector(mqttConnector);
    brokerService.start();
    brokerService.waitUntilStarted();
  }

  @Test(timeout = 120 * 1000)
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
    Thread thread = new Thread() {
      public void run() {
        try {
          MqttClient client = new MqttClient("tcp://localhost:" + port,
              MqttClient.generateClientId(), new MqttDefaultFilePersistence("target/paho/"));
          client.connect();
          LOGGER.info("Waiting pipeline connected to the MQTT broker before sending "
              + "messages ...");
          boolean pipelineConnected = false;
          while (!pipelineConnected) {
            Thread.sleep(50);
            for (Connection connection : brokerService.getBroker().getClients()) {
              if (connection.getConnectionId().equals("READ_PIPELINE")) {
                pipelineConnected = true;
              }
            }
          }
          for (int i = 0; i < 10; i++) {
            MqttMessage message = new MqttMessage();
            message.setQos(0);
            message.setPayload(("This is test " + i).getBytes());
            client.publish("READ_TOPIC", message);
          }
          client.disconnect();
          client.close();
        } catch (Exception e) {
          // nothing to do
        }
      }
    };
    thread.start();
    pipeline.run();
    thread.join();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWrite() throws Exception {
    final CountDownLatch latch = new CountDownLatch(200);

    MqttClient client = new MqttClient("tcp://localhost:" + port, MqttClient.generateClientId(),
        new MqttDefaultFilePersistence("target/paho/"));
    client.connect();
    client.subscribe("WRITE_TOPIC");
    client.setCallback(new MqttCallback() {
      @Override
      public void connectionLost(Throwable throwable) {
        LOGGER.warn("Connection Lost", throwable);
        throwable.printStackTrace();
      }

      @Override
      public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
        latch.countDown();
      }

      @Override
      public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
        // nothing to do
      }
    });

    Pipeline pipeline = TestPipeline.create();

    ArrayList<byte[]> data = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      data.add("Test".getBytes());
    }
    pipeline.apply(Create.of(data))
        .apply(MqttIO.write()
            .withConnectionConfiguration(
                MqttIO.ConnectionConfiguration.create(
                    "tcp://localhost:" + port,
                    "WRITE_TOPIC", "TEST")));
    pipeline.run();

    latch.await(30, TimeUnit.SECONDS);
    assertEquals(0, latch.getCount());

    client.disconnect();
    client.close();
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
