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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.activemq.broker.BrokerService;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.common.NetworkTestHelper;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
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

  /** Collects the bytes field of every output row into a shared queue (DirectRunner is in-JVM). */
  private static final ConcurrentLinkedQueue<String> STREAMING_RECEIVED =
      new ConcurrentLinkedQueue<>();

  private static class CollectBytesFn extends DoFn<Row, Void> {
    @ProcessElement
    public void processElement(@Element Row row) {
      byte[] bytes = row.getBytes("bytes");
      if (bytes != null) {
        STREAMING_RECEIVED.add(new String(bytes, StandardCharsets.UTF_8));
      }
    }
  }

  /**
   * Reads in streaming mode: when neither {@code maxNumRecords} nor {@code maxReadTimeSeconds} is
   * set the SchemaTransform produces an unbounded PCollection. Verifies that messages published
   * after the reader subscribes flow through continuously, then cancels the running pipeline.
   */
  @Test(timeout = 90 * 1000)
  public void testReadUnboundedStreaming() throws Exception {
    STREAMING_RECEIVED.clear();
    final String topicName = "STREAM_READ_TOPIC";

    // No bound -> unbounded (streaming) read.
    MqttReadSchemaTransformProvider.ReadConfiguration readConfiguration =
        MqttReadSchemaTransformProvider.ReadConfiguration.builder()
            .setConnectionConfiguration(
                MqttIO.ConnectionConfiguration.create("tcp://localhost:" + port, topicName)
                    .withClientId("STREAM_READ_PIPELINE"))
            .build();

    // Use a local pipeline so run() does not block (the read never terminates on its own).
    PipelineOptions options =
        PipelineOptionsFactory.fromArgs("--blockOnRun=false").withoutStrictParsing().create();
    Pipeline p = Pipeline.create(options);
    PCollectionRowTuple.empty(p)
        .apply(new MqttReadSchemaTransformProvider().from(readConfiguration))
        .get("output")
        .apply(ParDo.of(new CollectBytesFn()));

    // Publish a steady stream of messages until the reader has consumed enough.
    final boolean[] keepPublishing = {true};
    MQTT client = new MQTT();
    client.setHost("tcp://localhost:" + port);
    final BlockingConnection publishConnection = client.blockingConnection();
    publishConnection.connect();
    Thread publisher =
        new Thread(
            () -> {
              int i = 0;
              try {
                while (keepPublishing[0]) {
                  publishConnection.publish(
                      topicName,
                      ("stream-" + i).getBytes(StandardCharsets.UTF_8),
                      QoS.AT_LEAST_ONCE,
                      false);
                  i++;
                  Thread.sleep(200);
                }
              } catch (Exception e) {
                // ignore: connection closed on teardown
              }
            });

    PipelineResult result = p.run();
    publisher.start();

    // Wait until the unbounded read delivers a meaningful number of records.
    int expected = 10;
    long deadline = System.currentTimeMillis() + 60 * 1000;
    while (STREAMING_RECEIVED.size() < expected && System.currentTimeMillis() < deadline) {
      Thread.sleep(500);
    }

    keepPublishing[0] = false;
    publisher.join();
    publishConnection.disconnect();
    result.cancel();

    assertTrue(
        "expected at least " + expected + " streamed records, got " + STREAMING_RECEIVED.size(),
        STREAMING_RECEIVED.size() >= expected);
    for (String received : STREAMING_RECEIVED) {
      assertTrue("unexpected payload: " + received, received.startsWith("stream-"));
    }
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
                  LOG.info("message: {}", new String(message.getPayload(), StandardCharsets.UTF_8));
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
