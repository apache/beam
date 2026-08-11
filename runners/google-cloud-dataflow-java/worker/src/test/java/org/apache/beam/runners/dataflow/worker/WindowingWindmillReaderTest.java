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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.dataflow.worker.streaming.Watermarks;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.FakeGetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillTagEncodingV1;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow.IntervalWindowCoder;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.PaneInfoCoder;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues.FullWindowedValueCoder;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class WindowingWindmillReaderTest {
  private StreamingModeExecutionContext mockContext;
  private WindowingWindmillReader<String, Long> reader;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    mockContext = mock(StreamingModeExecutionContext.class);
    when(mockContext.workIsFailed()).thenReturn(false);
    when(mockContext.getWindmillTagEncoding()).thenReturn(WindmillTagEncodingV1.instance());
    when(mockContext.getDrainMode()).thenReturn(false);

    Coder<String> keyCoder = StringUtf8Coder.of();
    Coder<Long> valueCoder = VarLongCoder.of();
    KvCoder<String, Long> kvCoder = KvCoder.of(keyCoder, valueCoder);
    WindmillKeyedWorkItem.FakeKeyedWorkItemCoder<String, Long> keyedWorkItemCoder =
        (WindmillKeyedWorkItem.FakeKeyedWorkItemCoder<String, Long>)
            WindmillKeyedWorkItem.FakeKeyedWorkItemCoder.of(kvCoder);
    FullWindowedValueCoder<KeyedWorkItem<String, Long>> coder =
        FullWindowedValueCoder.of(keyedWorkItemCoder, IntervalWindowCoder.of());

    reader =
        WindowingWindmillReader.create(
            coder, mockContext, ValueProvider.StaticValueProvider.of(false));
  }

  private static Work createMockWork(Windmill.WorkItem workItem) {
    return Work.create(
        workItem,
        workItem.getSerializedSize(),
        Watermarks.builder().setInputDataWatermark(new Instant(1000)).build(),
        Work.createProcessingContext(
            "computationId", new FakeGetDataClient(), ignored -> {}, mock(HeartbeatSender.class)),
        false,
        Instant::now,
        ImmutableList.of());
  }

  private static ByteString encodeMetadata(List<IntervalWindow> windows) throws IOException {
    ByteStringOutputStream stream = new ByteStringOutputStream();
    PaneInfoCoder.INSTANCE.encode(PaneInfo.NO_FIRING, stream);
    ListCoder.of(IntervalWindowCoder.of()).encode(windows, stream);
    return stream.toByteString();
  }

  private static ByteString encodeValue(long value) throws IOException {
    ByteStringOutputStream stream = new ByteStringOutputStream();
    VarLongCoder.of().encode(value, stream);
    return stream.toByteString();
  }

  @Test
  public void testSingleNonEmptyKey() throws IOException {
    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(1000));
    Windmill.WorkItem workItem =
        Windmill.WorkItem.newBuilder()
            .setKey(ByteString.copyFromUtf8("key1"))
            .setWorkToken(100L)
            .addMessageBundles(
                Windmill.InputMessageBundle.newBuilder()
                    .setSourceComputationId("foo")
                    .addMessages(
                        Windmill.Message.newBuilder()
                            .setTimestamp(1000)
                            .setData(encodeValue(42L))
                            .setMetadata(encodeMetadata(ImmutableList.of(window)))
                            .build())
                    .build())
            .build();
    Work work = createMockWork(workItem);

    when(mockContext.getKey()).thenReturn("key1");
    when(mockContext.getWorkItem()).thenReturn(workItem);
    when(mockContext.getWork()).thenReturn(work);
    when(mockContext.advance()).thenReturn(false);

    try (NativeReader.NativeReaderIterator<WindowedValue<KeyedWorkItem<String, Long>>> iter =
        reader.iterator()) {
      assertTrue(iter.start());
      WindowedValue<KeyedWorkItem<String, Long>> current = iter.getCurrent();
      assertEquals("key1", current.getValue().key());
      assertFalse(Iterables.isEmpty(current.getValue().elementsIterable()));
      WindowedValue<Long> elem = Iterables.getOnlyElement(current.getValue().elementsIterable());
      assertEquals(42L, elem.getValue().longValue());

      assertFalse(iter.advance());
      verify(mockContext).finishKey();
    }
  }

  @Test
  public void testSingleEmptyKey() throws IOException {
    Windmill.WorkItem workItem =
        Windmill.WorkItem.newBuilder()
            .setKey(ByteString.copyFromUtf8("key1"))
            .setWorkToken(100L)
            .build(); // No message bundles or timers
    Work work = createMockWork(workItem);

    when(mockContext.getKey()).thenReturn("key1");
    when(mockContext.getWorkItem()).thenReturn(workItem);
    when(mockContext.getWork()).thenReturn(work);
    when(mockContext.advance()).thenReturn(false);

    try (NativeReader.NativeReaderIterator<WindowedValue<KeyedWorkItem<String, Long>>> iter =
        reader.iterator()) {
      assertFalse(
          iter.start()); // Should skip the empty key and return false because advance returns false
      verify(mockContext).finishKey();
    }
  }

  @Test
  public void testMultipleKeys_withEmptyAndNonEmpty() throws IOException {
    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(1000));
    // Key 1: Empty
    Windmill.WorkItem workItem1 =
        Windmill.WorkItem.newBuilder()
            .setKey(ByteString.copyFromUtf8("key1"))
            .setWorkToken(100L)
            .build();
    Work work1 = createMockWork(workItem1);

    // Key 2: Non-empty
    Windmill.WorkItem workItem2 =
        Windmill.WorkItem.newBuilder()
            .setKey(ByteString.copyFromUtf8("key2"))
            .setWorkToken(200L)
            .addMessageBundles(
                Windmill.InputMessageBundle.newBuilder()
                    .setSourceComputationId("foo")
                    .addMessages(
                        Windmill.Message.newBuilder()
                            .setTimestamp(2000)
                            .setData(encodeValue(84L))
                            .setMetadata(encodeMetadata(ImmutableList.of(window)))
                            .build())
                    .build())
            .build();
    Work work2 = createMockWork(workItem2);

    // Key 3: Empty
    Windmill.WorkItem workItem3 =
        Windmill.WorkItem.newBuilder()
            .setKey(ByteString.copyFromUtf8("key3"))
            .setWorkToken(300L)
            .build();
    Work work3 = createMockWork(workItem3);

    // Initial state
    when(mockContext.getKey()).thenReturn("key1");
    when(mockContext.getWorkItem()).thenReturn(workItem1);
    when(mockContext.getWork()).thenReturn(work1);

    // Mock transition behaviour of context.advance()
    when(mockContext.advance())
        .thenAnswer(
            new org.mockito.stubbing.Answer<Boolean>() {
              private int count = 0;

              @Override
              public Boolean answer(org.mockito.invocation.InvocationOnMock invocation) {
                if (count == 0) {
                  count++;
                  when(mockContext.getKey()).thenReturn("key2");
                  when(mockContext.getWorkItem()).thenReturn(workItem2);
                  when(mockContext.getWork()).thenReturn(work2);
                  return true;
                } else if (count == 1) {
                  count++;
                  when(mockContext.getKey()).thenReturn("key3");
                  when(mockContext.getWorkItem()).thenReturn(workItem3);
                  when(mockContext.getWork()).thenReturn(work3);
                  return true;
                }
                return false;
              }
            });

    try (NativeReader.NativeReaderIterator<WindowedValue<KeyedWorkItem<String, Long>>> iter =
        reader.iterator()) {
      // Key 1 is empty, so start() calls advance() which calls finishKey(1) and advance() to Key 2.
      // Key 2 is non-empty, so start() returns true yielding Key 2.
      assertTrue(iter.start());
      assertEquals("key2", iter.getCurrent().getValue().key());
      WindowedValue<Long> elem =
          Iterables.getOnlyElement(iter.getCurrent().getValue().elementsIterable());
      assertEquals(84L, elem.getValue().longValue());

      // Next advance() calls finishKey(2), calls advance() to Key 3.
      // Key 3 is empty, so it loops, calls finishKey(3), calls advance() which returns false.
      // So iter.advance() should return false.
      assertFalse(iter.advance());

      verify(mockContext, times(3))
          .finishKey(); // finishKey should have been called on key1, key2, key3
    }
  }

  @Test
  public void testWorkItemCancelled() throws IOException {
    when(mockContext.workIsFailed()).thenReturn(true);
    Windmill.WorkItem workItem =
        Windmill.WorkItem.newBuilder().setKey(ByteString.EMPTY).setWorkToken(0L).build();
    when(mockContext.getWorkItem()).thenReturn(workItem);

    try (NativeReader.NativeReaderIterator<WindowedValue<KeyedWorkItem<String, Long>>> iter =
        reader.iterator()) {
      iter.start();
      fail("Expected WorkItemCancelledException");
    } catch (WorkItemCancelledException e) {
      // Expected
    }
  }
}
