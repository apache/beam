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

import java.io.Serializable;
import java.net.ServerSocket;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.memory.MemoryPersistenceAdapter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
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

  private static transient BrokerService broker;

  private static int port;

  @BeforeClass
  public static void startBroker() throws Exception {
    LOGGER.info("Finding free network port");
    ServerSocket socket = new ServerSocket(0);
    port = socket.getLocalPort();
    socket.close();

    LOGGER.info("Starting ActiveMQ broker on {}", port);
    broker = new BrokerService();
    broker.setUseJmx(false);
    broker.setPersistenceAdapter(new MemoryPersistenceAdapter());
    broker.addConnector(new URI("mqtt://localhost:" + port));
    broker.start();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testRead() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    PCollection<byte[]> output = pipeline.apply(
        MqttIO.read()
            .withConnectionConfiguration(
                MqttIO.ConnectionConfiguration.create(
                    "tcp://localhost:" + port,
                    "BEAM_PIPELINE",
                    "READ_TOPIC"))
          .withMaxNumRecords(10));

    PAssert.thatSingleton(output.apply("Count", Count.<byte[]>globally()))
        .isEqualTo(10L);
    PAssert.that(output).satisfies(new SerializableFunction<Iterable<byte[]>, Void>() {
      @Override
      public Void apply(Iterable<byte[]> input) {
        for (byte[] element : input) {
          String inputString = new String(element);
          Assert.assertTrue(inputString.startsWith("This is test "));
          int count = Integer.parseInt(inputString.substring("This is test ".length()));
          Assert.assertTrue(count < 10);
          Assert.assertTrue(count >= 0);
        }
        return null;
      }
    });

    // produce messages on the broker in another thread
    // This thread prevents to block the pipeline waiting for new messages
    Thread thread = new Thread() {
      public void run() {
        try {
          // gives time to the pipeline to start
          Thread.sleep(2000);
        } catch (Exception e) {
          // nothing to do
        }
        try {
          MqttClient client = new MqttClient("tcp://localhost:" + port, "publisher");
          client.connect();
          for (int i = 0; i < 10; i++) {
            MqttMessage message = new MqttMessage();
            message.setQos(1);
            message.setRetained(true);
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
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWrite() throws Exception {
    List<MqttMessage> messages = new ArrayList<>();
    MqttClient client = receive(messages);

    Pipeline pipeline = TestPipeline.create();

    ArrayList<byte[]> data = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      data.add("Test".getBytes());
    }
    // we use QoS 2 here to be sure the subscriber completely receive all messages before
    // shutting down the MQTT broker.
    // Quality of Service 2 indicates that a message should be delivered once. The message will
    // be persisted to disk, and will be subject to a two-phase ack.
    pipeline.apply(Create.of(data))
        .apply(MqttIO.write()
            .withConnectionConfiguration(
                MqttIO.ConnectionConfiguration.create(
                    "tcp://localhost:" + port,
                    "BEAM_PIPELINE",
                    "WRITE_TOPIC"))
            .withQoS(2));
    pipeline.run();

    Assert.assertEquals(100, messages.size());

    client.disconnect();
    client.close();
  }

  private MqttClient receive(final List<MqttMessage> messages) throws MqttException {
    MqttClient client = new MqttClient("tcp://localhost:" + port, "receiver");
    MqttCallback callback = new MqttCallback() {
      @Override
      public void connectionLost(Throwable cause) {
        cause.printStackTrace();
      }

      @Override
      public void messageArrived(String topic, MqttMessage message) throws Exception {
          messages.add(message);
      }

      @Override
      public void deliveryComplete(IMqttDeliveryToken token) {
        // nothing to do
      }
    };
    client.connect();
    client.subscribe("WRITE_TOPIC");
    client.setCallback(callback);
    return client;
  }

  @AfterClass
  public static void stopBroker() throws Exception {
    if (broker != null) {
      broker.stop();
    }
  }

}
