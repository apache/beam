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
package org.apache.beam.sdk.transforms;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.util.PCollectionViews;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;

import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

/**
 * Tests for {@link DoFnTester}.
 */
@RunWith(JUnit4.class)
public class DoFnTesterTest {

  @Test
  public void processElement() throws Exception {
    CounterDoFn counterDoFn = new CounterDoFn();
    DoFnTester<Long, String> tester = DoFnTester.of(counterDoFn);

    tester.processElement(1L);

    List<String> take = tester.takeOutputElements();

    assertThat(take, hasItems("1"));

    // Following takeOutputElements(), neither takeOutputElements()
    // nor peekOutputElements() return anything.
    assertTrue(tester.takeOutputElements().isEmpty());
    assertTrue(tester.peekOutputElements().isEmpty());

    // processElement() caused startBundle() to be called, but finishBundle() was never called.
    CounterDoFn deserializedDoFn = (CounterDoFn) tester.fn;
    assertTrue(deserializedDoFn.wasStartBundleCalled());
    assertFalse(deserializedDoFn.wasFinishBundleCalled());
  }

  @Test
  public void processElementsWithPeeks() throws Exception {
    CounterDoFn counterDoFn = new CounterDoFn();
    DoFnTester<Long, String> tester = DoFnTester.of(counterDoFn);

    // Explicitly call startBundle().
    tester.startBundle();

    // verify startBundle() was called but not finishBundle().
    CounterDoFn deserializedDoFn = (CounterDoFn) tester.fn;
    assertTrue(deserializedDoFn.wasStartBundleCalled());
    assertFalse(deserializedDoFn.wasFinishBundleCalled());

    // process a couple of elements.
    tester.processElement(1L);
    tester.processElement(2L);

    // peek the first 2 outputs.
    List<String> peek = tester.peekOutputElements();
    assertThat(peek, hasItems("1", "2"));

    // process a couple more.
    tester.processElement(3L);
    tester.processElement(4L);

    // peek all the outputs so far.
    peek = tester.peekOutputElements();
    assertThat(peek, hasItems("1", "2", "3", "4"));
    // take the outputs.
    List<String> take = tester.takeOutputElements();
    assertThat(take, hasItems("1", "2", "3", "4"));

    // Following takeOutputElements(), neither takeOutputElements()
    // nor peekOutputElements() return anything.
    assertTrue(tester.peekOutputElements().isEmpty());
    assertTrue(tester.takeOutputElements().isEmpty());

    // verify finishBundle() hasn't been called yet.
    assertTrue(deserializedDoFn.wasStartBundleCalled());
    assertFalse(deserializedDoFn.wasFinishBundleCalled());

    // process a couple more.
    tester.processElement(5L);
    tester.processElement(6L);

    // peek and take now have only the 2 last outputs.
    peek = tester.peekOutputElements();
    assertThat(peek, hasItems("5", "6"));
    take = tester.takeOutputElements();
    assertThat(take, hasItems("5", "6"));

    tester.finishBundle();

    // verify finishBundle() was called.
    assertTrue(deserializedDoFn.wasStartBundleCalled());
    assertTrue(deserializedDoFn.wasFinishBundleCalled());
  }

  @Test
  public void processBatch() throws Exception {
    CounterDoFn counterDoFn = new CounterDoFn();
    DoFnTester<Long, String> tester = DoFnTester.of(counterDoFn);

    // processBundle() returns all the output like takeOutputElements().
    List<String> take = tester.processBundle(1L, 2L, 3L, 4L);

    assertThat(take, hasItems("1", "2", "3", "4"));

    // peek now returns nothing.
    assertTrue(tester.peekOutputElements().isEmpty());

    // verify startBundle() and finishBundle() were both called.
    CounterDoFn deserializedDoFn = (CounterDoFn) tester.fn;
    assertTrue(deserializedDoFn.wasStartBundleCalled());
    assertTrue(deserializedDoFn.wasFinishBundleCalled());
  }

  @Test
  public void processElementWithTimestamp() throws Exception {
    CounterDoFn counterDoFn = new CounterDoFn();
    DoFnTester<Long, String> tester = DoFnTester.of(counterDoFn);

    tester.processElement(1L);
    tester.processElement(2L);

    List<TimestampedValue<String>> peek = tester.peekOutputElementsWithTimestamp();
    TimestampedValue<String> one = TimestampedValue.of("1", new Instant(1000L));
    TimestampedValue<String> two = TimestampedValue.of("2", new Instant(2000L));
    assertThat(peek, hasItems(one, two));

    tester.processElement(3L);
    tester.processElement(4L);

    TimestampedValue<String> three = TimestampedValue.of("3", new Instant(3000L));
    TimestampedValue<String> four = TimestampedValue.of("4", new Instant(4000L));
    peek = tester.peekOutputElementsWithTimestamp();
    assertThat(peek, hasItems(one, two, three, four));
    List<TimestampedValue<String>> take = tester.takeOutputElementsWithTimestamp();
    assertThat(take, hasItems(one, two, three, four));

    // Following takeOutputElementsWithTimestamp(), neither takeOutputElementsWithTimestamp()
    // nor peekOutputElementsWithTimestamp() return anything.
    assertTrue(tester.takeOutputElementsWithTimestamp().isEmpty());
    assertTrue(tester.peekOutputElementsWithTimestamp().isEmpty());

    // peekOutputElements() and takeOutputElements() also return nothing.
    assertTrue(tester.peekOutputElements().isEmpty());
    assertTrue(tester.takeOutputElements().isEmpty());
  }

  @Test
  public void getAggregatorValuesShouldGetValueOfCounter() throws Exception {
    CounterDoFn counterDoFn = new CounterDoFn();
    DoFnTester<Long, String> tester = DoFnTester.of(counterDoFn);
    tester.processBundle(1L, 2L, 4L, 8L);

    Long aggregatorVal = tester.getAggregatorValue(counterDoFn.agg);

    assertThat(aggregatorVal, equalTo(15L));
  }

  @Test
  public void getAggregatorValuesWithEmptyCounterShouldSucceed() throws Exception {
    CounterDoFn counterDoFn = new CounterDoFn();
    DoFnTester<Long, String> tester = DoFnTester.of(counterDoFn);
    tester.processBundle();
    Long aggregatorVal = tester.getAggregatorValue(counterDoFn.agg);
    // empty bundle
    assertThat(aggregatorVal, equalTo(0L));
  }

  @Test
  public void getAggregatorValuesInStartFinishBundleShouldGetValues() throws Exception {
    CounterDoFn fn = new CounterDoFn(1L, 2L);
    DoFnTester<Long, String> tester = DoFnTester.of(fn);
    tester.processBundle(0L, 0L);

    Long aggValue = tester.getAggregatorValue(fn.agg);
    assertThat(aggValue, equalTo(1L + 2L));
  }

  @Test
  public void peekValuesInWindow() throws Exception {
    CounterDoFn fn = new CounterDoFn(1L, 2L);
    DoFnTester<Long, String> tester = DoFnTester.of(fn);

    tester.startBundle();
    tester.processElement(1L);
    tester.processElement(2L);
    tester.finishBundle();

    assertThat(
        tester.peekOutputElementsInWindow(GlobalWindow.INSTANCE),
        containsInAnyOrder(
            TimestampedValue.of("1", new Instant(1000L)),
            TimestampedValue.of("2", new Instant(2000L))));
    assertThat(
        tester.peekOutputElementsInWindow(new IntervalWindow(new Instant(0L), new Instant(10L))),
        Matchers.<TimestampedValue<String>>emptyIterable());
  }

  @Test
  public void fnWithSideInputDefault() throws Exception {
    final PCollectionView<Integer> value =
        PCollectionViews.singletonView(
            TestPipeline.create(), WindowingStrategy.globalDefault(), true, 0, VarIntCoder.of());
    DoFn<Integer, Integer> fn = new SideInputDoFn(value);

    DoFnTester<Integer, Integer> tester = DoFnTester.of(fn);

    tester.processElement(1);
    tester.processElement(2);
    tester.processElement(4);
    tester.processElement(8);
    assertThat(tester.peekOutputElements(), containsInAnyOrder(0, 0, 0, 0));
  }

  @Test
  public void fnWithSideInputExplicit() throws Exception {
    final PCollectionView<Integer> value =
        PCollectionViews.singletonView(
            TestPipeline.create(), WindowingStrategy.globalDefault(), true, 0, VarIntCoder.of());
    DoFn<Integer, Integer> fn = new SideInputDoFn(value);

    DoFnTester<Integer, Integer> tester = DoFnTester.of(fn);
    tester.setSideInput(value, GlobalWindow.INSTANCE, -2);
    tester.processElement(16);
    tester.processElement(32);
    tester.processElement(64);
    tester.processElement(128);
    tester.finishBundle();

    assertThat(tester.peekOutputElements(), containsInAnyOrder(-2, -2, -2, -2));
  }

  private static class SideInputDoFn extends DoFn<Integer, Integer> {
    private final PCollectionView<Integer> value;

    private SideInputDoFn(PCollectionView<Integer> value) {
      this.value = value;
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      c.output(c.sideInput(value));
    }
  }

  /**
   * A DoFn that adds values to an aggregator and converts input to String in processElement.
   */
  private static class CounterDoFn extends DoFn<Long, String> {
    Aggregator<Long, Long> agg = createAggregator("ctr", new Sum.SumLongFn());
    private final long startBundleVal;
    private final long finishBundleVal;
    private boolean startBundleCalled;
    private boolean finishBundleCalled;

    public CounterDoFn() {
      this(0L, 0L);
    }

    public CounterDoFn(long start, long finish) {
      this.startBundleVal = start;
      this.finishBundleVal = finish;
    }

    @Override
    public void startBundle(Context c) {
      agg.addValue(startBundleVal);
      startBundleCalled = true;
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      agg.addValue(c.element());
      Instant instant = new Instant(1000L * c.element());
      c.outputWithTimestamp(c.element().toString(), instant);
    }

    @Override
    public void finishBundle(Context c) {
      agg.addValue(finishBundleVal);
      finishBundleCalled = true;
    }

    boolean wasStartBundleCalled() {
      return startBundleCalled;
    }

    boolean wasFinishBundleCalled() {
      return finishBundleCalled;
    }
  }
}
