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
package org.apache.beam.runners.spark.translation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.sdk.fn.test.TestExecutors;
import org.apache.beam.sdk.fn.test.TestExecutors.TestExecutorService;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;
import scala.Tuple2;

/** * Test suite for {@link SparkInputDataProcessor}. */
@RunWith(JUnit4.class)
public class SparkInputDataProcessorTest {

  @Rule public TestExecutorService executor = TestExecutors.from(Executors.newCachedThreadPool());

  @Test
  @SuppressWarnings("rawtypes")
  public void testBoundedProcessWorksWithEmptyInput() {

    SparkInputDataProcessor<String, String, Tuple2<TupleTag<?>, WindowedValue<?>>> processor =
        SparkInputDataProcessor.createBounded();
    AtomicInteger writeCount = new AtomicInteger();
    SparkProcessContext<String, String, String> ctx =
        setUpCtx(processor.getOutputManager(), 1, writeCount);

    Iterator<WindowedValue<String>> input = Collections.emptyIterator();
    Iterator result = processor.createOutputIterator(input, ctx);

    assertFalse(result.hasNext());
    assertEquals(0, Iterators.size(result));
    assertEquals(0, writeCount.get());
  }

  @Test
  @SuppressWarnings("rawtypes")
  public void testUnboundedProcessWorksWithEmptyInput() {

    SparkInputDataProcessor<String, String, Tuple2<TupleTag<?>, WindowedValue<?>>> processor =
        SparkInputDataProcessor.createUnbounded();
    AtomicInteger writeCount = new AtomicInteger();
    SparkProcessContext<String, String, String> ctx =
        setUpCtx(processor.getOutputManager(), 1, writeCount);

    Iterator<WindowedValue<String>> input = Collections.emptyIterator();
    Iterator result = processor.createOutputIterator(input, ctx);

    assertFalse(result.hasNext());
    assertEquals(0, Iterators.size(result));
    assertEquals(0, writeCount.get());
  }

  @Test
  @SuppressWarnings("rawtypes")
  public void testBoundedProcessBlocksOnMaxInputsUntilTheyAreConsumed() throws Exception {

    SparkInputDataProcessor<String, String, Tuple2<TupleTag<?>, WindowedValue<?>>> processor =
        SparkInputDataProcessor.createBounded();
    AtomicInteger writeCount = new AtomicInteger();
    int desiredWriteCount = 1000;
    SparkProcessContext<String, String, String> ctx =
        setUpCtx(processor.getOutputManager(), desiredWriteCount, writeCount);

    Iterator<WindowedValue<String>> input =
        Lists.newArrayList(WindowedValue.valueInGlobalWindow("tick")).iterator();
    Iterator result = processor.createOutputIterator(input, ctx);

    CountDownLatch maxReached = new CountDownLatch(1);
    // max queue capacity plus one poll from the queue by iterator
    int expectedMaxWrites = 500 + 1;
    Future<?> unused =
        executor.submit(
            () -> {
              while (writeCount.get() != expectedMaxWrites) {
                try {
                  Thread.sleep(100);
                } catch (InterruptedException e) {
                  Thread.interrupted();
                }
              }
              maxReached.countDown();
            });

    // this will trigger input processing via doFn
    assertTrue(result.hasNext());

    // wait until we reach expected max
    if (!maxReached.await(10, TimeUnit.SECONDS)) {
      throw new IllegalStateException("Did not reach expected max writes cap while not consuming");
    }

    // after tiny while, we shall see still the same max writes => are blocked untill consumed
    assertEquals(expectedMaxWrites, writeCount.get());
    assertEquals(desiredWriteCount, Iterators.size(result));
    assertEquals(desiredWriteCount, writeCount.get());
  }

  @Test
  @SuppressWarnings("rawtypes")
  public void testUnboundedProcessWritesAllInputsAndNotBlock() {

    SparkInputDataProcessor<String, String, Tuple2<TupleTag<?>, WindowedValue<?>>> processor =
        SparkInputDataProcessor.createUnbounded();
    AtomicInteger writeCount = new AtomicInteger();
    int desiredWriteCount = 1000;
    SparkProcessContext<String, String, String> ctx =
        setUpCtx(processor.getOutputManager(), desiredWriteCount, writeCount);

    Iterator<WindowedValue<String>> input =
        Lists.newArrayList(WindowedValue.valueInGlobalWindow("tick")).iterator();
    Iterator result = processor.createOutputIterator(input, ctx);

    // this will trigger input processing via doFn
    assertTrue(result.hasNext());

    // check for writes first before fully consuming results
    assertEquals(desiredWriteCount, writeCount.get());
    assertEquals(desiredWriteCount, Iterators.size(result));
  }

  @Test
  @SuppressWarnings("rawtypes")
  public void testBoundedProcessLifecycle() {

    SparkInputDataProcessor<String, String, Tuple2<TupleTag<?>, WindowedValue<?>>> processor =
        SparkInputDataProcessor.createBounded();
    AtomicInteger writeCount = new AtomicInteger();
    int desiredWriteCount = 1000;
    SparkProcessContext<String, String, String> ctx =
        setUpCtx(processor.getOutputManager(), desiredWriteCount, writeCount);

    WindowedValue<String> value = WindowedValue.valueInGlobalWindow("tick");
    Iterator<WindowedValue<String>> input = Lists.newArrayList(value).iterator();
    Iterator result = processor.createOutputIterator(input, ctx);

    assertEquals(desiredWriteCount, Iterators.size(result));

    DoFnRunner<String, String> doFnRunner = ctx.getDoFnRunner();

    Mockito.verify(doFnRunner).startBundle();
    Mockito.verify(doFnRunner).processElement(value);
    Mockito.verify(doFnRunner).finishBundle();
    Mockito.verifyNoMoreInteractions(doFnRunner);
  }

  @Test
  @SuppressWarnings("rawtypes")
  public void testUnboundedProcessLifecycle() {

    SparkInputDataProcessor<String, String, Tuple2<TupleTag<?>, WindowedValue<?>>> processor =
        SparkInputDataProcessor.createUnbounded();
    AtomicInteger writeCount = new AtomicInteger();
    int desiredWriteCount = 1000;
    SparkProcessContext<String, String, String> ctx =
        setUpCtx(processor.getOutputManager(), desiredWriteCount, writeCount);

    WindowedValue<String> value = WindowedValue.valueInGlobalWindow("tick");
    Iterator<WindowedValue<String>> input = Lists.newArrayList(value).iterator();
    Iterator result = processor.createOutputIterator(input, ctx);

    assertEquals(desiredWriteCount, Iterators.size(result));

    DoFnRunner<String, String> doFnRunner = ctx.getDoFnRunner();

    Mockito.verify(doFnRunner).startBundle();
    Mockito.verify(doFnRunner).processElement(value);
    Mockito.verify(doFnRunner).finishBundle();
    Mockito.verifyNoMoreInteractions(doFnRunner);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private SparkProcessContext<String, String, String> setUpCtx(
      DoFnRunners.OutputManager output, int desiredCount, AtomicInteger producedCount) {
    SparkProcessContext ctx = Mockito.mock(SparkProcessContext.class);
    TestDoFnRunner runner = new TestDoFnRunner(output, desiredCount, producedCount);

    DoFnRunner<String, String> doFnRunnerMock = Mockito.spy(runner);
    DoFn<String, String> doFnMock = Mockito.spy(runner.getFn());
    Mockito.when(ctx.getDoFnRunner()).thenReturn(doFnRunnerMock);
    Mockito.when(ctx.getDoFn()).thenReturn(doFnMock);
    Mockito.when(ctx.getTimerDataIterator()).thenReturn(Collections.emptyIterator());
    return ctx;
  }

  private static class TestDoFnRunner implements DoFnRunner<String, String> {

    private final DoFnRunners.OutputManager output;
    private final AtomicInteger producedCount;
    private final int desiredCount;
    private final TestDoFn fn = new TestDoFn();

    TestDoFnRunner(
        DoFnRunners.OutputManager output, int desiredCount, AtomicInteger producedCount) {
      this.output = output;
      this.producedCount = producedCount;
      this.desiredCount = desiredCount;
    }

    @Override
    public void startBundle() {}

    @Override
    public void processElement(WindowedValue<String> elem) {
      fn.processElement(elem.getValue());
    }

    @Override
    public <KeyT> void onTimer(
        String timerId,
        String timerFamilyId,
        KeyT key,
        BoundedWindow window,
        Instant timestamp,
        Instant outputTimestamp,
        TimeDomain timeDomain) {}

    @Override
    public void finishBundle() {}

    @Override
    public <KeyT> void onWindowExpiration(BoundedWindow window, Instant timestamp, KeyT key) {}

    @Override
    public DoFn<String, String> getFn() {
      return fn;
    }

    class TestDoFn extends DoFn<String, String> {

      @DoFn.ProcessElement
      public void processElement(@Element String value) {
        for (int i = 0; i < desiredCount; i++) {
          output.output(new TupleTag<>("key"), WindowedValue.valueInGlobalWindow(value + "_" + i));
          producedCount.incrementAndGet();
        }
      }
    }
  }
}
