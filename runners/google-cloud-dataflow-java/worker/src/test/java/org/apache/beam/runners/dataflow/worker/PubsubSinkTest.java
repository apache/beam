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
}
