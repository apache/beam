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
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Sink;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
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
public class PubsubSinkTest {
  @Mock StreamingModeExecutionContext mockContext;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
  }

  private void testWriteWith(String formatFn) throws Exception {
    Windmill.WorkItemCommitRequest.Builder outputBuilder =
        Windmill.WorkItemCommitRequest.newBuilder()
            .setKey(ByteString.copyFromUtf8("key"))
            .setWorkToken(0);

    when(mockContext.getOutputBuilder()).thenReturn(outputBuilder);

    Map<String, Object> spec = new HashMap<>();
    spec.put(PropertyNames.OBJECT_TYPE_NAME, "");
    spec.put(PropertyNames.PUBSUB_TOPIC, "topic");
    spec.put(PropertyNames.PUBSUB_TIMESTAMP_ATTRIBUTE, "ts");
    spec.put(PropertyNames.PUBSUB_ID_ATTRIBUTE, "id");
    if (formatFn != null) {
      spec.put(PropertyNames.PUBSUB_SERIALIZED_ATTRIBUTES_FN, formatFn);
    }
    CloudObject cloudSinkSpec = CloudObject.fromSpec(spec);
    PubsubSink.Factory factory = new PubsubSink.Factory();
    PubsubSink<String> sink =
        (PubsubSink<String>)
            factory.create(
                cloudSinkSpec,
                WindowedValues.getFullCoder(StringUtf8Coder.of(), IntervalWindow.getCoder()),
                null,
                mockContext,
                null);

    Sink.SinkWriter<WindowedValue<String>> writer = sink.writer();

    assertEquals(
        2, writer.add(WindowedValues.timestampedValueInGlobalWindow("e0", new Instant(0))));
    assertEquals(
        2, writer.add(WindowedValues.timestampedValueInGlobalWindow("e1", new Instant(1))));
    assertEquals(
        2, writer.add(WindowedValues.timestampedValueInGlobalWindow("e2", new Instant(2))));
    writer.close();

    assertEquals(
        Windmill.WorkItemCommitRequest.newBuilder()
            .setKey(ByteString.copyFromUtf8("key"))
            .setWorkToken(0)
            .addPubsubMessages(
                Windmill.PubSubMessageBundle.newBuilder()
                    .setTopic("topic")
                    .setTimestampLabel("ts")
                    .setIdLabel("id")
                    .addMessages(
                        Windmill.Message.newBuilder()
                            .setTimestamp(0)
                            .setData(ByteString.copyFromUtf8("e0")))
                    .addMessages(
                        Windmill.Message.newBuilder()
                            .setTimestamp(1000)
                            .setData(ByteString.copyFromUtf8("e1")))
                    .addMessages(
                        Windmill.Message.newBuilder()
                            .setTimestamp(2000)
                            .setData(ByteString.copyFromUtf8("e2")))
                    .setWithAttributes(formatFn != null))
            .build(),
        outputBuilder.build());
  }

  @Test
  public void testBasic() throws Exception {
    testWriteWith(null /* No formatFn */);
  }

  @Test
  public void testEmptyParseFn() throws Exception {
    testWriteWith("");
  }

  private static class ErrorCoder extends Coder<String> {
    @Override
    public void encode(String value, OutputStream outStream) throws CoderException, IOException {
      outStream.write(1);
      throw new CoderException("encode error");
    }

    @Override
    public String decode(InputStream inStream) throws IOException {
      throw new CoderException("decode error");
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return null;
    }

    @Override
    public void verifyDeterministic() {}
  }

  // Regression test that the PubsubSink properly resets internal state on encoding exceptions to
  // prevent precondition failures on further output.
  @Test
  public void testExceptionAfterEncoding() throws Exception {
    Map<String, Object> spec = new HashMap<>();
    spec.put(PropertyNames.OBJECT_TYPE_NAME, "");
    spec.put(PropertyNames.PUBSUB_TOPIC, "topic");
    spec.put(PropertyNames.PUBSUB_TIMESTAMP_ATTRIBUTE, "ts");
    spec.put(PropertyNames.PUBSUB_ID_ATTRIBUTE, "id");
    CloudObject cloudSinkSpec = CloudObject.fromSpec(spec);
    PubsubSink.Factory factory = new PubsubSink.Factory();
    PubsubSink<String> sink =
        (PubsubSink<String>)
            factory.create(
                cloudSinkSpec,
                WindowedValues.getFullCoder(new ErrorCoder(), IntervalWindow.getCoder()),
                null,
                mockContext,
                null);

    Sink.SinkWriter<WindowedValue<String>> writer = sink.writer();
    assertThrows(
        "encode error",
        CoderException.class,
        () -> writer.add(WindowedValues.timestampedValueInGlobalWindow("e0", new Instant(0))));
    assertThrows(
        "encode error",
        CoderException.class,
        () -> writer.add(WindowedValues.timestampedValueInGlobalWindow("e0", new Instant(0))));
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
    spec.put(PropertyNames.OBJECT_TYPE_NAME, "");
    spec.put(PropertyNames.PUBSUB_TOPIC, "topic");
    spec.put(PropertyNames.PUBSUB_TIMESTAMP_ATTRIBUTE, "ts");
    spec.put(PropertyNames.PUBSUB_ID_ATTRIBUTE, "id");
    CloudObject cloudSinkSpec = CloudObject.fromSpec(spec);
    PubsubSink.Factory factory = new PubsubSink.Factory();
    PubsubSink<String> sink =
        (PubsubSink<String>)
            factory.create(
                cloudSinkSpec,
                WindowedValues.getFullCoder(StringUtf8Coder.of(), IntervalWindow.getCoder()),
                null,
                mockContext,
                null);

    Sink.SinkWriter<WindowedValue<String>> writer = sink.writer();
    writer.add(WindowedValues.timestampedValueInGlobalWindow("e0", new Instant(0)));

    // In single key mode, finishKey should not flush
    writer.finishKey("key");
    assertEquals(0, outputBuilder.getPubsubMessagesCount());

    // close should flush and attach to the key's outputBuilder
    writer.add(WindowedValues.timestampedValueInGlobalWindow("e1", new Instant(1000)));
    writer.close();

    assertEquals(1, outputBuilder.getPubsubMessagesCount());
    Windmill.PubSubMessageBundle bundle = outputBuilder.getPubsubMessages(0);
    assertEquals("topic", bundle.getTopic());
    assertEquals(2, bundle.getMessagesCount());
    assertEquals("e0", bundle.getMessages(0).getData().toStringUtf8());
    assertEquals("e1", bundle.getMessages(1).getData().toStringUtf8());
  }

  private static class MultiKeyTestCase {
    final String testName;
    final List<String> key1Messages;
    final List<String> key2Messages;
    final List<String> finishBundleMessages;
    final int expectedKey1FlushedMessages;
    final int expectedKey2FlushedMessages;
    final int expectedBundleFlushedMessages;

    MultiKeyTestCase(
        String testName,
        List<String> key1Messages,
        List<String> key2Messages,
        List<String> finishBundleMessages,
        int expectedKey1FlushedMessages,
        int expectedKey2FlushedMessages,
        int expectedBundleFlushedMessages) {
      this.testName = testName;
      this.key1Messages = key1Messages;
      this.key2Messages = key2Messages;
      this.finishBundleMessages = finishBundleMessages;
      this.expectedKey1FlushedMessages = expectedKey1FlushedMessages;
      this.expectedKey2FlushedMessages = expectedKey2FlushedMessages;
      this.expectedBundleFlushedMessages = expectedBundleFlushedMessages;
    }
  }

  private void runMultiKeyTest(MultiKeyTestCase testCase) throws Exception {
    MockitoAnnotations.initMocks(this);
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
    spec.put(PropertyNames.OBJECT_TYPE_NAME, "");
    spec.put(PropertyNames.PUBSUB_TOPIC, "topic");
    spec.put(PropertyNames.PUBSUB_TIMESTAMP_ATTRIBUTE, "ts");
    spec.put(PropertyNames.PUBSUB_ID_ATTRIBUTE, "id");
    CloudObject cloudSinkSpec = CloudObject.fromSpec(spec);
    PubsubSink.Factory factory = new PubsubSink.Factory();
    PubsubSink<String> sink =
        (PubsubSink<String>)
            factory.create(
                cloudSinkSpec,
                WindowedValues.getFullCoder(StringUtf8Coder.of(), IntervalWindow.getCoder()),
                null,
                mockContext,
                null);

    Sink.SinkWriter<WindowedValue<String>> writer = sink.writer();

    // 1. Process Key 1 messages
    for (String msg : testCase.key1Messages) {
      writer.add(WindowedValues.timestampedValueInGlobalWindow(msg, new Instant(0)));
    }
    writer.finishKey("key1");

    // Verify Key 1 flush expectations
    if (testCase.expectedKey1FlushedMessages > 0) {
      assertEquals(testCase.testName, 1, outputBuilderKey1.getPubsubMessagesCount());
      assertEquals(
          testCase.testName,
          testCase.expectedKey1FlushedMessages,
          outputBuilderKey1.getPubsubMessages(0).getMessagesCount());
    } else {
      assertEquals(testCase.testName, 0, outputBuilderKey1.getPubsubMessagesCount());
    }

    // 2. Process Key 2 messages
    when(mockContext.getOutputBuilder()).thenReturn(outputBuilderKey2);
    for (String msg : testCase.key2Messages) {
      writer.add(WindowedValues.timestampedValueInGlobalWindow(msg, new Instant(100)));
    }
    writer.finishKey("key2");

    // Verify Key 2 flush expectations
    if (testCase.expectedKey2FlushedMessages > 0) {
      assertEquals(testCase.testName, 1, outputBuilderKey2.getPubsubMessagesCount());
      assertEquals(
          testCase.testName,
          testCase.expectedKey2FlushedMessages,
          outputBuilderKey2.getPubsubMessages(0).getMessagesCount());
    } else {
      assertEquals(testCase.testName, 0, outputBuilderKey2.getPubsubMessagesCount());
    }

    // 3. Process finishBundle messages and close
    for (String msg : testCase.finishBundleMessages) {
      writer.add(WindowedValues.timestampedValueInGlobalWindow(msg, new Instant(200)));
    }
    writer.close();

    // Verify Bundle-level flush expectations
    if (testCase.expectedBundleFlushedMessages > 0) {
      ArgumentCaptor<Windmill.PubSubMessageBundle> captor =
          ArgumentCaptor.forClass(Windmill.PubSubMessageBundle.class);
      verify(mockContext).addBundlePubsubMessages(captor.capture());
      Windmill.PubSubMessageBundle bundleLevel = captor.getValue();
      assertEquals(testCase.testName, "topic", bundleLevel.getTopic());
      assertEquals(
          testCase.testName,
          testCase.expectedBundleFlushedMessages,
          bundleLevel.getMessagesCount());
    } else {
      verify(mockContext, org.mockito.Mockito.never())
          .addBundlePubsubMessages(org.mockito.ArgumentMatchers.any());
    }
  }

  @Test
  public void testMultiKey_parameterizedCombinations() throws Exception {
    String largePayload = "a".repeat(1024 * 1024); // 1MB exact
    String p100K = "b".repeat(100 * 1024); // 100KB
    String p200K = "c".repeat(200 * 1024); // 200KB
    String p600K_1 = "d".repeat(600 * 1024); // 600KB
    String p600K_2 = "e".repeat(600 * 1024); // 600KB
    String boundaryBelow1MB = "f".repeat(1024 * 1024 - 1); // 1MB - 1 byte

    List<MultiKeyTestCase> testCases =
        List.of(
            // 1. Key 1 Large (>= 1MB), Key 2 Small (< 1MB)
            new MultiKeyTestCase(
                "Key1 Large, Key2 Small",
                /* key1Messages= */ List.of(largePayload),
                /* key2Messages= */ List.of("key2-small"),
                /* finishBundleMessages= */ List.of("bundle-msg"),
                /* expectedKey1FlushedMessages= */ 1,
                /* expectedKey2FlushedMessages= */ 0,
                /* expectedBundleFlushedMessages= */ 2),
            // 2. Key 1 Small (< 1MB), Key 2 Large (>= 1MB) -> accumulated >= 1MB flushes both to
            // Key 2
            new MultiKeyTestCase(
                "Key1 Small, Key2 Large",
                /* key1Messages= */ List.of("key1-small"),
                /* key2Messages= */ List.of(largePayload),
                /* finishBundleMessages= */ List.of(),
                /* expectedKey1FlushedMessages= */ 0,
                /* expectedKey2FlushedMessages= */ 2,
                /* expectedBundleFlushedMessages= */ 0),
            // 3. Both Small, sum < 1MB -> flushes to bundle level at close
            new MultiKeyTestCase(
                "Both Small, sum < 1MB",
                /* key1Messages= */ List.of(p100K),
                /* key2Messages= */ List.of(p200K),
                /* finishBundleMessages= */ List.of("bundle-tail"),
                /* expectedKey1FlushedMessages= */ 0,
                /* expectedKey2FlushedMessages= */ 0,
                /* expectedBundleFlushedMessages= */ 3),
            // 4. Both Small, sum >= 1MB (600KB + 600KB = 1.2MB) -> flushes both to Key 2
            new MultiKeyTestCase(
                "Both Small, sum >= 1MB",
                /* key1Messages= */ List.of(p600K_1),
                /* key2Messages= */ List.of(p600K_2),
                /* finishBundleMessages= */ List.of(),
                /* expectedKey1FlushedMessages= */ 0,
                /* expectedKey2FlushedMessages= */ 2,
                /* expectedBundleFlushedMessages= */ 0),
            // 5. Both Large (>= 1MB each) -> each flushes independently to its own key
            new MultiKeyTestCase(
                "Both Large, each >= 1MB",
                /* key1Messages= */ List.of(largePayload),
                /* key2Messages= */ List.of(largePayload),
                /* finishBundleMessages= */ List.of(),
                /* expectedKey1FlushedMessages= */ 1,
                /* expectedKey2FlushedMessages= */ 1,
                /* expectedBundleFlushedMessages= */ 0),
            // 6. Exact 1MB Boundary: 1MB - 1 byte does not flush; second key adds element pushing
            // >= 1MB
            new MultiKeyTestCase(
                "1MB boundary: below threshold on Key 1, crossed on Key 2",
                /* key1Messages= */ List.of(boundaryBelow1MB),
                /* key2Messages= */ List.of("x"),
                /* finishBundleMessages= */ List.of(),
                /* expectedKey1FlushedMessages= */ 0,
                /* expectedKey2FlushedMessages= */ 2,
                /* expectedBundleFlushedMessages= */ 0));

    for (MultiKeyTestCase testCase : testCases) {
      runMultiKeyTest(testCase);
    }
  }

  @Test
  public void testAbort() throws Exception {
    Windmill.WorkItemCommitRequest.Builder outputBuilder =
        Windmill.WorkItemCommitRequest.newBuilder()
            .setKey(ByteString.copyFromUtf8("key"))
            .setWorkToken(0);
    when(mockContext.getOutputBuilder()).thenReturn(outputBuilder);

    Map<String, Object> spec = new HashMap<>();
    spec.put(PropertyNames.OBJECT_TYPE_NAME, "");
    spec.put(PropertyNames.PUBSUB_TOPIC, "topic");
    spec.put(PropertyNames.PUBSUB_TIMESTAMP_ATTRIBUTE, "ts");
    spec.put(PropertyNames.PUBSUB_ID_ATTRIBUTE, "id");
    CloudObject cloudSinkSpec = CloudObject.fromSpec(spec);
    PubsubSink.Factory factory = new PubsubSink.Factory();
    PubsubSink<String> sink =
        (PubsubSink<String>)
            factory.create(
                cloudSinkSpec,
                WindowedValues.getFullCoder(StringUtf8Coder.of(), IntervalWindow.getCoder()),
                null,
                mockContext,
                null);

    Sink.SinkWriter<WindowedValue<String>> writer = sink.writer();

    // Buffer message and abort
    writer.add(WindowedValues.timestampedValueInGlobalWindow("msg-aborted", new Instant(0)));
    writer.abort();
    assertEquals(0, outputBuilder.getPubsubMessagesCount());
  }
}
