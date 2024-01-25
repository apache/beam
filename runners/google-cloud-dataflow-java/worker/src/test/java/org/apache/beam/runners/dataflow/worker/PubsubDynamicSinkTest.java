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
package org.apache.beam.runners.dataflow.worker;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Sink;
import org.apache.beam.runners.dataflow.worker.windmill.Pubsub;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Unit tests for {@link PubsubSink}. */
@RunWith(JUnit4.class)
public class PubsubDynamicSinkTest {
  @Mock StreamingModeExecutionContext mockContext;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testWriteDynamicDestinations() throws Exception {
    Windmill.WorkItemCommitRequest.Builder outputBuilder =
        Windmill.WorkItemCommitRequest.newBuilder()
            .setKey(ByteString.copyFromUtf8("key"))
            .setWorkToken(0);

    when(mockContext.getOutputBuilder()).thenReturn(outputBuilder);

    Map<String, Object> spec = new HashMap<>();
    spec.put(PropertyNames.OBJECT_TYPE_NAME, "PubsubDynamicSink");
    spec.put(PropertyNames.PUBSUB_TIMESTAMP_ATTRIBUTE, "ts");
    spec.put(PropertyNames.PUBSUB_ID_ATTRIBUTE, "id");

    CloudObject cloudSinkSpec = CloudObject.fromSpec(spec);
    PubsubDynamicSink sink =
        (PubsubDynamicSink)
            SinkRegistry.defaultRegistry()
                .create(
                    cloudSinkSpec,
                    WindowedValue.getFullCoder(VoidCoder.of(), IntervalWindow.getCoder()),
                    null,
                    mockContext,
                    null)
                .getUnderlyingSink();

    Sink.SinkWriter<WindowedValue<PubsubMessage>> writer = sink.writer();

    List<Windmill.Message> expectedMessages1 = Lists.newArrayList();
    List<Windmill.Message> expectedMessages2 = Lists.newArrayList();
    List<Windmill.Message> expectedMessages3 = Lists.newArrayList();

    for (int i = 0; i < 10; ++i) {
      int baseTimestamp = i * 10;
      byte[] payload1 = String.format("value_%d_%d", i, 1).getBytes(StandardCharsets.UTF_8);
      byte[] payload2 = String.format("value_%d_%d", i, 2).getBytes(StandardCharsets.UTF_8);
      byte[] payload3 = String.format("value_%d_%d", i, 3).getBytes(StandardCharsets.UTF_8);

      expectedMessages1.add(
          Windmill.Message.newBuilder()
              .setTimestamp(baseTimestamp * 1000)
              .setData(
                  Pubsub.PubsubMessage.newBuilder()
                      .setData(ByteString.copyFrom(payload1))
                      .build()
                      .toByteString())
              .build());
      expectedMessages2.add(
          Windmill.Message.newBuilder()
              .setTimestamp((baseTimestamp + 1) * 1000)
              .setData(
                  Pubsub.PubsubMessage.newBuilder()
                      .setData(ByteString.copyFrom(payload2))
                      .build()
                      .toByteString())
              .build());
      expectedMessages3.add(
          Windmill.Message.newBuilder()
              .setTimestamp((baseTimestamp + 2) * 1000)
              .setData(
                  Pubsub.PubsubMessage.newBuilder()
                      .setData(ByteString.copyFrom(payload3))
                      .build()
                      .toByteString())
              .build());
      writer.add(
          WindowedValue.timestampedValueInGlobalWindow(
              new PubsubMessage(payload1, null).withTopic("topic1"), new Instant(baseTimestamp)));
      writer.add(
          WindowedValue.timestampedValueInGlobalWindow(
              new PubsubMessage(payload2, null).withTopic("topic2"),
              new Instant(baseTimestamp + 1)));
      writer.add(
          WindowedValue.timestampedValueInGlobalWindow(
              new PubsubMessage(payload3, null).withTopic("topic3"),
              new Instant(baseTimestamp + 2)));
    }
    writer.close();

    Windmill.WorkItemCommitRequest expectedCommit =
        Windmill.WorkItemCommitRequest.newBuilder()
            .setKey(ByteString.copyFromUtf8("key"))
            .setWorkToken(0)
            .addPubsubMessages(
                Windmill.PubSubMessageBundle.newBuilder()
                    .setTopic("topic1")
                    .setTimestampLabel("ts")
                    .setIdLabel("id")
                    .setWithAttributes(true)
                    .addAllMessages(expectedMessages1))
            .addPubsubMessages(
                Windmill.PubSubMessageBundle.newBuilder()
                    .setTopic("topic2")
                    .setTimestampLabel("ts")
                    .setIdLabel("id")
                    .setWithAttributes(true)
                    .addAllMessages(expectedMessages2))
            .addPubsubMessages(
                Windmill.PubSubMessageBundle.newBuilder()
                    .setTopic("topic3")
                    .setTimestampLabel("ts")
                    .setIdLabel("id")
                    .setWithAttributes(true)
                    .addAllMessages(expectedMessages3))
            .build();
    assertEquals(expectedCommit, outputBuilder.build());
  }
}
