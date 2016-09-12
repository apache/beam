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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests of {@link MqttIO}.
 */
@RunWith(JUnit4.class)
public class MqttIOTest implements Serializable {

  private transient BrokerService broker;

  @Before
  public void startBroker() throws Exception {
    broker = new BrokerService();
    broker.setUseJmx(false);
    broker.setPersistenceAdapter(new MemoryPersistenceAdapter());
    broker.addConnector(new URI("mqtt://localhost:11883"));
    broker.start();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testRead() throws Exception {
    // produce message on the broker
    MqttClient client = new MqttClient("tcp://localhost:11883", "test");
    client.connect();
    for (int i = 0; i < 10; i++) {
      MqttMessage message = new MqttMessage();
      message.setQos(2);
      message.setRetained(true);
      message.setPayload("This is a test".getBytes());
      client.publish("BEAM", message);
    }
    client.disconnect();
    client.close();

    Pipeline pipeline = TestPipeline.create();

    PCollection<byte[]> output = pipeline.apply(
        MqttIO.read()
          .withClientId("BEAM_PIPELINE")
          .withServerUri("tcp://localhost:11883")
          .withTopic("BEAM")
          .withMaxNumRecords(10));

    PAssert.thatSingleton(output.apply("Count", Count.<byte[]>globally()))
        .isEqualTo(10L);
    PAssert.that(output).satisfies(new SerializableFunction<Iterable<byte[]>, Void>() {
      @Override
      public Void apply(Iterable<byte[]> input) {
        for (byte[] element : input) {
          String inputString = new String(element);
          Assert.assertEquals("This is a test", inputString);
        }
        return null;
      }
    });
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
    pipeline.apply(Create.of(data))
        .apply(MqttIO.write().withClientId("BEAM_PIPELINE")
            .withServerUri("tcp://localhost:11883").withTopic("BEAM_TOPIC")
            .withRetained(true).withQoS(2));
    pipeline.run();

    Assert.assertEquals(100, messages.size());

    client.disconnect();
    client.close();
  }

  private MqttClient receive(final List<MqttMessage> messages) throws MqttException {
    MqttClient client = new MqttClient("tcp://localhost:11883", "test");
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
        System.out.println("Delivery complete");
      }
    };
    client.connect();
    client.subscribe("BEAM_TOPIC");
    client.setCallback(callback);
    return client;
  }

  @After
  public void stopBroker() throws Exception {
    if (broker != null) {
      broker.stop();
    }
  }

}
