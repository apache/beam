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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.activemq.broker.BrokerService;
import org.apache.beam.sdk.io.common.NetworkTestHelper;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqttSchemaTransformProviderTest {
  private static final Logger LOG = LoggerFactory.getLogger(MqttSchemaTransformProviderTest.class);

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

  @Test(timeout = 30 * 1000)
  public void testReceiveWithTimeoutAndNoData() {
    MqttReadSchemaTransformProvider.ReadConfiguration readConfiguration =
        MqttReadSchemaTransformProvider.ReadConfiguration.builder()
            .setConnectionConfiguration(
                MqttIO.ConnectionConfiguration.create("tcp://localhost:" + port, "READ_TOPIC")
                    .withClientId("READ_PIPELINE"))
            .setMaxReadTimeSeconds(2L)
            .build();

    PCollectionRowTuple.empty(pipeline)
        .apply(new MqttReadSchemaTransformProvider().from(readConfiguration));

    // should stop before the test timeout
    pipeline.run().waitUntilFinish();
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
                  System.out.println(
                      "message: " + new String(message.getPayload(), StandardCharsets.UTF_8));
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

    MqttWriteSchemaTransformProvider.WriteConfiguration writeConfiguration =
        MqttWriteSchemaTransformProvider.WriteConfiguration.builder()
            .setConnectionConfiguration(
                MqttIO.ConnectionConfiguration.create("tcp://localhost:" + port, "WRITE_TOPIC"))
            .build();
    Schema dataSchema = Schema.builder().addByteArrayField("bytes").build();

    PCollection<Row> inputRows =
        pipeline
            .apply(Create.of(data))
            .apply(
                MapElements.into(TypeDescriptors.rows())
                    .via(d -> Row.withSchema(dataSchema).addValue(d).build()))
            .setRowSchema(dataSchema);
    PCollectionRowTuple.of("input", inputRows)
        .apply(new MqttWriteSchemaTransformProvider().from(writeConfiguration));
    pipeline.run();
    subscriber.join();

    connection.disconnect();

    assertEquals(numberOfTestMessages, messages.size());
    for (int i = 0; i < numberOfTestMessages; i++) {
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
