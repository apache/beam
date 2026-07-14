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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link WindmillReaderIteratorBase}. */
@RunWith(JUnit4.class)
public class WindmillReaderIteratorBaseTest {
  private static class TestWindmillReaderIterator extends WindmillReaderIteratorBase<Long> {
    protected TestWindmillReaderIterator(
        StreamingModeExecutionContext context, ValueProvider<Boolean> skipUndecodableElements) {
      super(context, skipUndecodableElements);
    }

    @Override
    protected WindowedValue<Long> decodeMessage(Windmill.Message message) throws CoderException {
      if (message.getTimestamp() < 0) {
        throw new CoderException("Injected decoding error to test skipping.");
      }
      return WindowedValues.valueInGlobalWindow(message.getTimestamp());
    }
  }

  @Test
  public void testBasic() throws IOException {
    testForMessageBundleCounts();
    testForMessageBundleCounts(0);
    testForMessageBundleCounts(0, 0);
    testForMessageBundleCounts(1);
    testForMessageBundleCounts(2);
    testForMessageBundleCounts(1, 1);
    testForMessageBundleCounts(0, 1);
    testForMessageBundleCounts(1, 0);
    testForMessageBundleCounts(0, 0, 1, 3, 0, 1, 0, 0, 0, 1);
    testForMessageBundleCounts(0, 0, 1, 3, 0, 1, 0, 0, 0, 0);
  }

  @Test
  public void testSkipErrors() throws IOException {
    testForMessageBundleCounts(true);
    testForMessageBundleCounts(true, 0);
    testForMessageBundleCounts(true, 0, 0);
    testForMessageBundleCounts(true, 1);
    testForMessageBundleCounts(true, 2);
    testForMessageBundleCounts(true, 1, 1);
    testForMessageBundleCounts(true, 0, 1);
    testForMessageBundleCounts(true, 1, 0);
    testForMessageBundleCounts(true, 0, 0, 1, 3, 0, 1, 0, 0, 0, 1);
    testForMessageBundleCounts(true, 0, 0, 1, 3, 0, 1, 0, 0, 0, 0);
  }

  @Test
  public void testWorkItemCancelledException() throws IOException {
    StreamingModeExecutionContext mockContext = mock(StreamingModeExecutionContext.class);
    when(mockContext.workIsFailed()).thenReturn(true);
    Windmill.WorkItem workItem =
        Windmill.WorkItem.newBuilder().setKey(ByteString.EMPTY).setWorkToken(0L).build();
    when(mockContext.getWorkItem()).thenReturn(workItem);

    try (TestWindmillReaderIterator iter =
        new TestWindmillReaderIterator(mockContext, ValueProvider.StaticValueProvider.of(false))) {
      iter.start();
      fail("Expected WorkItemCancelledException");
    } catch (WorkItemCancelledException e) {
      // Expected
    }
  }

  @Test
  public void testFinishKeyCalled() throws Exception {
    StreamingModeExecutionContext mockContext = mock(StreamingModeExecutionContext.class);
    when(mockContext.workIsFailed()).thenReturn(false);
    Windmill.WorkItem workItem =
        Windmill.WorkItem.newBuilder()
            .setKey(ByteString.EMPTY)
            .setWorkToken(0L)
            .addMessageBundles(
                Windmill.InputMessageBundle.newBuilder()
                    .setSourceComputationId("foo")
                    .addMessages(
                        Windmill.Message.newBuilder()
                            .setTimestamp(0)
                            .setData(ByteString.EMPTY)
                            .build())
                    .build())
            .build();
    when(mockContext.getWorkItem()).thenReturn(workItem);
    when(mockContext.advance()).thenReturn(false);

    try (TestWindmillReaderIterator iter =
        new TestWindmillReaderIterator(mockContext, ValueProvider.StaticValueProvider.of(false))) {
      assertTrue(iter.start());
      assertFalse(iter.advance()); // This should trigger finishKey
      verify(mockContext).finishKey();
    }
  }

  @Test
  public void testAdvanceKeyChaining() throws Exception {
    StreamingModeExecutionContext mockContext = mock(StreamingModeExecutionContext.class);
    when(mockContext.workIsFailed()).thenReturn(false);

    // Work item A (1 message)
    Windmill.WorkItem workItemA =
        Windmill.WorkItem.newBuilder()
            .setKey(ByteString.copyFromUtf8("keyA"))
            .setWorkToken(100L)
            .addMessageBundles(
                Windmill.InputMessageBundle.newBuilder()
                    .setSourceComputationId("foo")
                    .addMessages(
                        Windmill.Message.newBuilder()
                            .setTimestamp(1000)
                            .setData(ByteString.EMPTY)
                            .build())
                    .build())
            .build();
    when(mockContext.getWorkItem()).thenReturn(workItemA);

    // Work item B (1 message)
    Windmill.WorkItem workItemB =
        Windmill.WorkItem.newBuilder()
            .setKey(ByteString.copyFromUtf8("keyB"))
            .setWorkToken(200L)
            .addMessageBundles(
                Windmill.InputMessageBundle.newBuilder()
                    .setSourceComputationId("foo")
                    .addMessages(
                        Windmill.Message.newBuilder()
                            .setTimestamp(2000)
                            .setData(ByteString.EMPTY)
                            .build())
                    .build())
            .build();

    // Set up context.advance() to mock transition
    when(mockContext.advance())
        .thenAnswer(
            new org.mockito.stubbing.Answer<Boolean>() {
              private int count = 0;

              @Override
              public Boolean answer(org.mockito.invocation.InvocationOnMock invocation) {
                if (count == 0) {
                  count++;
                  when(mockContext.getWorkItem()).thenReturn(workItemB);
                  return true;
                }
                return false;
              }
            });

    try (TestWindmillReaderIterator iter =
        new TestWindmillReaderIterator(mockContext, ValueProvider.StaticValueProvider.of(false))) {
      assertTrue(iter.start());
      assertEquals(1000L, iter.getCurrent().getValue().longValue());

      // Advance should trigger context.advance(), transition to workItemB, and decode message from
      // workItemB (timestamp 2000)
      assertTrue(iter.advance());
      assertEquals(2000L, iter.getCurrent().getValue().longValue());

      // Next advance should exhaust it and return false
      assertFalse(iter.advance());
    }
  }

  private void testForMessageBundleCounts(int... messageBundleCounts) throws IOException {
    testForMessageBundleCounts(false, messageBundleCounts);
  }

  private void testForMessageBundleCounts(boolean skipErrors, int... messageBundleCounts)
      throws IOException {
    List<Windmill.InputMessageBundle> bundles = new ArrayList<>();
    long numTotalMessages = 0;
    for (int count : messageBundleCounts) {
      Windmill.InputMessageBundle.Builder bundle =
          Windmill.InputMessageBundle.newBuilder().setSourceComputationId("foo");
      for (int i = 0; i < count; ++i) {
        bundle.addMessages(
            Windmill.Message.newBuilder()
                .setTimestamp(numTotalMessages++)
                .setData(ByteString.EMPTY)
                .build());
      }
      if (skipErrors && ThreadLocalRandom.current().nextBoolean()) {
        bundle.addMessages(
            Windmill.Message.newBuilder().setTimestamp(-10).setData(ByteString.EMPTY).build());
      }
      bundles.add(bundle.build());
    }
    Windmill.WorkItem workItem =
        Windmill.WorkItem.newBuilder()
            .setKey(ByteString.EMPTY)
            .setWorkToken(0L)
            .addAllMessageBundles(bundles)
            .build();

    StreamingModeExecutionContext mockContext = mock(StreamingModeExecutionContext.class);
    when(mockContext.getWorkItem()).thenReturn(workItem);

    try (TestWindmillReaderIterator iter =
        new TestWindmillReaderIterator(
            mockContext, ValueProvider.StaticValueProvider.of(skipErrors))) {
      List<Long> actual =
          ReaderTestUtils.windowedValuesToValues(
              ReaderUtils.readRemainingFromIterator(iter, false));
      assertFalse(iter.advance());
      List<Long> expected = new ArrayList<>();
      for (int i = 0; i < numTotalMessages; ++i) {
        expected.add((long) i);
      }
      assertEquals(Arrays.toString(messageBundleCounts) + skipErrors, expected, actual);
    }
  }

  private static Work createMockWork(Windmill.WorkItem workItem) {
    return Work.create(
        workItem,
        workItem.getSerializedSize(),
        org.apache.beam.runners.dataflow.worker.streaming.Watermarks.builder()
            .setInputDataWatermark(new org.joda.time.Instant(1000))
            .build(),
        Work.createProcessingContext(
            "computationId",
            mock(
                org.apache.beam.runners.dataflow.worker.windmill.client.getdata.GetDataClient
                    .class),
            ignored -> {},
            mock(
                org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender
                    .class)),
        false,
        org.joda.time.Instant::now);
  }
}
