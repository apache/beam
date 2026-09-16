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
import static org.mockito.Mockito.verify;
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
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
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
                    WindowedValues.getFullCoder(VoidCoder.of(), IntervalWindow.getCoder()),
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
          WindowedValues.timestampedValueInGlobalWindow(
              new PubsubMessage(payload1, null).withTopic("topic1"), new Instant(baseTimestamp)));
      writer.add(
          WindowedValues.timestampedValueInGlobalWindow(
              new PubsubMessage(payload2, null).withTopic("topic2"),
              new Instant(baseTimestamp + 1)));
      writer.add(
          WindowedValues.timestampedValueInGlobalWindow(
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

  @Test
  public void testSingleKey_finishKeyDoesNotFlush_closeAttachesToKey() throws Exception {
    when(mockContext.multiKeyBundleEnabled()).thenReturn(false);

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
                    WindowedValues.getFullCoder(VoidCoder.of(), IntervalWindow.getCoder()),
                    null,
                    mockContext,
                    null)
                .getUnderlyingSink();

    Sink.SinkWriter<WindowedValue<PubsubMessage>> writer = sink.writer();
    byte[] payload0 = "msg0".getBytes(StandardCharsets.UTF_8);
    byte[] payload1 = "msg1".getBytes(StandardCharsets.UTF_8);

    writer.add(
        WindowedValues.timestampedValueInGlobalWindow(
            new PubsubMessage(payload0, null).withTopic("topic1"), new Instant(0)));

    // In single-key mode, finishKey does not flush
    writer.finishKey("key");
    assertEquals(0, outputBuilder.getPubsubMessagesCount());

    // close flushes all outputs into the key's outputBuilder
    writer.add(
        WindowedValues.timestampedValueInGlobalWindow(
            new PubsubMessage(payload1, null).withTopic("topic2"), new Instant(1000)));
    writer.close();

    assertEquals(2, outputBuilder.getPubsubMessagesCount());
    Map<String, Windmill.PubSubMessageBundle> bundlesByTopic = new HashMap<>();
    for (Windmill.PubSubMessageBundle bundle : outputBuilder.getPubsubMessagesList()) {
      bundlesByTopic.put(bundle.getTopic(), bundle);
    }
    assertEquals(1, bundlesByTopic.get("topic1").getMessagesCount());
    assertEquals(1, bundlesByTopic.get("topic2").getMessagesCount());
    Pubsub.PubsubMessage pubsubMsg0 =
        Pubsub.PubsubMessage.parseFrom(bundlesByTopic.get("topic1").getMessages(0).getData());
    assertEquals(ByteString.copyFrom(payload0), pubsubMsg0.getData());
    Pubsub.PubsubMessage pubsubMsg1 =
        Pubsub.PubsubMessage.parseFrom(bundlesByTopic.get("topic2").getMessages(0).getData());
    assertEquals(ByteString.copyFrom(payload1), pubsubMsg1.getData());
  }

  @Test
  public void testMultiKey_flushesAllTopicsToBundleLevelAtClose() throws Exception {
    when(mockContext.multiKeyBundleEnabled()).thenReturn(true);

    Windmill.WorkItemCommitRequest.Builder outputBuilderKey1 =
        Windmill.WorkItemCommitRequest.newBuilder()
            .setKey(ByteString.copyFromUtf8("key1"))
            .setWorkToken(1);
    Windmill.WorkItemCommitRequest.Builder outputBuilderKey2 =
        Windmill.WorkItemCommitRequest.newBuilder()
            .setKey(ByteString.copyFromUtf8("key2"))
            .setWorkToken(2);
    when(mockContext.getOutputBuilder()).thenReturn(outputBuilderKey1);

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
                    WindowedValues.getFullCoder(VoidCoder.of(), IntervalWindow.getCoder()),
                    null,
                    mockContext,
                    null)
                .getUnderlyingSink();

    Sink.SinkWriter<WindowedValue<PubsubMessage>> writer = sink.writer();

    // 1. Process Key 1 messages (to topicA and topicB)
    writer.add(
        WindowedValues.timestampedValueInGlobalWindow(
            new PubsubMessage("k1-msgA".getBytes(StandardCharsets.UTF_8), null).withTopic("topicA"),
            new Instant(0)));
    writer.add(
        WindowedValues.timestampedValueInGlobalWindow(
            new PubsubMessage("k1-msgB".getBytes(StandardCharsets.UTF_8), null).withTopic("topicB"),
            new Instant(10)));
    writer.finishKey("key1");

    // In multi-key mode, finishKey does not flush to key-level commit
    assertEquals(0, outputBuilderKey1.getPubsubMessagesCount());

    // 2. Process Key 2 messages (to topicB and topicC)
    when(mockContext.getOutputBuilder()).thenReturn(outputBuilderKey2);
    writer.add(
        WindowedValues.timestampedValueInGlobalWindow(
            new PubsubMessage("k2-msgB".getBytes(StandardCharsets.UTF_8), null).withTopic("topicB"),
            new Instant(100)));
    writer.add(
        WindowedValues.timestampedValueInGlobalWindow(
            new PubsubMessage("k2-msgC".getBytes(StandardCharsets.UTF_8), null).withTopic("topicC"),
            new Instant(110)));
    writer.finishKey("key2");

    // In multi-key mode, finishKey does not flush to key-level commit
    assertEquals(0, outputBuilderKey2.getPubsubMessagesCount());

    // 3. Process finishBundle messages (to topicC) and close
    writer.add(
        WindowedValues.timestampedValueInGlobalWindow(
            new PubsubMessage("bundle-tC".getBytes(StandardCharsets.UTF_8), null)
                .withTopic("topicC"),
            new Instant(200)));
    writer.close();

    // Verify Bundle-level flush expectations: all messages grouped by topic at bundle level
    ArgumentCaptor<Windmill.PubSubMessageBundle> captor =
        ArgumentCaptor.forClass(Windmill.PubSubMessageBundle.class);
    verify(mockContext, org.mockito.Mockito.times(3)).addBundlePubsubMessages(captor.capture());
    Map<String, Integer> actualBundleTopicCounts = new HashMap<>();
    for (Windmill.PubSubMessageBundle b : captor.getAllValues()) {
      actualBundleTopicCounts.put(b.getTopic(), b.getMessagesCount());
    }
    assertEquals(Map.of("topicA", 1, "topicB", 2, "topicC", 2), actualBundleTopicCounts);
  }

  @Test
  public void testMultiKey_emptyBundleFlushesNothing() throws Exception {
    when(mockContext.multiKeyBundleEnabled()).thenReturn(true);

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
                    WindowedValues.getFullCoder(VoidCoder.of(), IntervalWindow.getCoder()),
                    null,
                    mockContext,
                    null)
                .getUnderlyingSink();

    Sink.SinkWriter<WindowedValue<PubsubMessage>> writer = sink.writer();
    writer.finishKey("key1");
    writer.close();

    verify(mockContext, org.mockito.Mockito.never())
        .addBundlePubsubMessages(org.mockito.ArgumentMatchers.any());
  }

  @Test
  public void testAbort() throws Exception {
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
                    WindowedValues.getFullCoder(VoidCoder.of(), IntervalWindow.getCoder()),
                    null,
                    mockContext,
                    null)
                .getUnderlyingSink();

    Sink.SinkWriter<WindowedValue<PubsubMessage>> writer = sink.writer();

    // Buffer and abort
    writer.add(
        WindowedValues.timestampedValueInGlobalWindow(
            new PubsubMessage("aborted".getBytes(StandardCharsets.UTF_8), null).withTopic("topic1"),
            new Instant(0)));
    writer.abort();
    assertEquals(0, outputBuilder.getPubsubMessagesCount());
  }
}
