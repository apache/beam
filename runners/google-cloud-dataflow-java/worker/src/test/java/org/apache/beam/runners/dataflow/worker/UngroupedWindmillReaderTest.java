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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow.IntervalWindowCoder;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.PaneInfoCoder;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.values.CausedByDrain;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link UngroupedWindmillReader}. */
@RunWith(JUnit4.class)
public class UngroupedWindmillReaderTest {
  private StreamingModeExecutionContext mockContext;

  @Before
  public void setUp() {
    mockContext = mock(StreamingModeExecutionContext.class);
  }

  private static ByteString encodeMetadata(List<IntervalWindow> windows) throws IOException {
    ByteStringOutputStream stream = new ByteStringOutputStream();
    PaneInfoCoder.INSTANCE.encode(PaneInfo.NO_FIRING, stream);
    ListCoder.of(IntervalWindowCoder.of()).encode(windows, stream);
    return stream.toByteString();
  }

  @Test
  public void testReadWithEmptyMetadataForImpulse() throws Exception {
    when(mockContext.getWorkItem())
        .thenReturn(
            Windmill.WorkItem.newBuilder()
                .setKey(ByteString.copyFromUtf8("key"))
                .setWorkToken(0)
                .addMessageBundles(
                    Windmill.InputMessageBundle.newBuilder()
                        .setSourceComputationId("in_memory")
                        .addMessages(
                            Windmill.Message.newBuilder()
                                .setTimestamp(0)
                                .setData(ByteString.EMPTY)
                                .setMetadata(ByteString.EMPTY)))
                .build());

    Map<String, Object> spec = new HashMap<>();
    spec.put(PropertyNames.OBJECT_TYPE_NAME, "UngroupedWindmillReader");
    CloudObject cloudSourceSpec = CloudObject.fromSpec(spec);
    UngroupedWindmillReader.Factory factory = new UngroupedWindmillReader.Factory();
    @SuppressWarnings("unchecked")
    UngroupedWindmillReader<byte[]> reader =
        (UngroupedWindmillReader<byte[]>)
            factory.create(
                cloudSourceSpec,
                WindowedValues.getFullCoder(ByteArrayCoder.of(), GlobalWindow.Coder.INSTANCE),
                null,
                mockContext,
                null);

    NativeReader.NativeReaderIterator<WindowedValue<byte[]>> iter = reader.iterator();
    assertTrue(iter.start());
    WindowedValue<byte[]> current = iter.getCurrent();
    assertArrayEquals(new byte[0], current.getValue());
    assertEquals(new Instant(0), current.getTimestamp());
    assertEquals(
        ImmutableList.of(GlobalWindow.INSTANCE), ImmutableList.copyOf(current.getWindows()));
    assertEquals(PaneInfo.NO_FIRING, current.getPaneInfo());
    assertFalse(iter.advance());
  }

  @Test
  public void testReadWithMetadata() throws Exception {
    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(10000));
    when(mockContext.getWorkItem())
        .thenReturn(
            Windmill.WorkItem.newBuilder()
                .setKey(ByteString.copyFromUtf8("key"))
                .setWorkToken(0)
                .addMessageBundles(
                    Windmill.InputMessageBundle.newBuilder()
                        .setSourceComputationId("stream")
                        .addMessages(
                            Windmill.Message.newBuilder()
                                .setTimestamp(1000)
                                .setData(ByteString.copyFromUtf8("hello"))
                                .setMetadata(encodeMetadata(ImmutableList.of(window)))))
                .build());

    Map<String, Object> spec = new HashMap<>();
    spec.put(PropertyNames.OBJECT_TYPE_NAME, "UngroupedWindmillReader");
    CloudObject cloudSourceSpec = CloudObject.fromSpec(spec);
    UngroupedWindmillReader.Factory factory = new UngroupedWindmillReader.Factory();
    @SuppressWarnings("unchecked")
    UngroupedWindmillReader<String> reader =
        (UngroupedWindmillReader<String>)
            factory.create(
                cloudSourceSpec,
                WindowedValues.getFullCoder(StringUtf8Coder.of(), IntervalWindow.getCoder()),
                null,
                mockContext,
                null);

    NativeReader.NativeReaderIterator<WindowedValue<String>> iter = reader.iterator();
    assertTrue(iter.start());
    assertEquals(
        WindowedValues.of("hello", new Instant(1), window, PaneInfo.NO_FIRING),
        iter.getCurrent());
    assertFalse(iter.advance());
  }

  @Test
  public void testReadWithAdditionalMetadata() throws Exception {
    WindowedValues.WindowedValueCoder.setMetadataSupported();
    try {
      IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(10000));
      WindowedValues.FullWindowedValueCoder<String> windowedValueCoder =
          WindowedValues.getFullCoder(StringUtf8Coder.of(), IntervalWindow.getCoder());
      ByteString metadata =
          WindmillSink.encodeMetadata(
              windowedValueCoder.getWindowsCoder(),
              ImmutableList.of(window),
              PaneInfo.NO_FIRING,
              BeamFnApi.Elements.ElementMetadata.newBuilder()
                  .setDrain(BeamFnApi.Elements.DrainMode.Enum.DRAINING)
                  .setValueKind(BeamFnApi.Elements.ValueKind.Enum.DELETE)
                  .build());
      when(mockContext.getWorkItem())
          .thenReturn(
              Windmill.WorkItem.newBuilder()
                  .setKey(ByteString.copyFromUtf8("key"))
                  .setWorkToken(0)
                  .addMessageBundles(
                      Windmill.InputMessageBundle.newBuilder()
                          .setSourceComputationId("stream")
                          .addMessages(
                              Windmill.Message.newBuilder()
                                  .setTimestamp(1000)
                                  .setData(ByteString.copyFromUtf8("hello"))
                                  .setMetadata(metadata)))
                  .build());

      Map<String, Object> spec = new HashMap<>();
      spec.put(PropertyNames.OBJECT_TYPE_NAME, "UngroupedWindmillReader");
      CloudObject cloudSourceSpec = CloudObject.fromSpec(spec);
      UngroupedWindmillReader.Factory factory = new UngroupedWindmillReader.Factory();
      @SuppressWarnings("unchecked")
      UngroupedWindmillReader<String> reader =
          (UngroupedWindmillReader<String>)
              factory.create(cloudSourceSpec, windowedValueCoder, null, mockContext, null);

      NativeReader.NativeReaderIterator<WindowedValue<String>> iter = reader.iterator();
      assertTrue(iter.start());
      WindowedValue<String> current = iter.getCurrent();
      assertEquals("hello", current.getValue());
      assertEquals(new Instant(1), current.getTimestamp());
      assertEquals(ImmutableList.of(window), ImmutableList.copyOf(current.getWindows()));
      assertEquals(PaneInfo.NO_FIRING.withElementMetadata(true), current.getPaneInfo());
      assertEquals(CausedByDrain.CAUSED_BY_DRAIN, current.causedByDrain());
      assertEquals(ValueKind.DELETE, current.getValueKind());
      assertFalse(iter.advance());
    } finally {
      WindowedValues.WindowedValueCoder.setMetadataNotSupported();
    }
  }
}
