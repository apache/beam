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

import static com.google.common.base.Preconditions.checkState;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link DoFnTester}.
 */
@RunWith(JUnit4.class)
public class DoFnTesterTest {

  @Rule public final TestPipeline p = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void processElement() throws Exception {
    for (DoFnTester.CloningBehavior cloning : DoFnTester.CloningBehavior.values()) {
      try (DoFnTester<Long, String> tester = DoFnTester.of(new CounterDoFn())) {
        tester.setCloningBehavior(cloning);
        tester.processElement(1L);

        List<String> take = tester.takeOutputElements();

        assertThat(take, hasItems("1"));

        // Following takeOutputElements(), neither takeOutputElements()
        // nor peekOutputElements() return anything.
        assertTrue(tester.takeOutputElements().isEmpty());
        assertTrue(tester.peekOutputElements().isEmpty());
      }
    }
  }

  @Test
  public void processElementsWithPeeks() throws Exception {
    for (DoFnTester.CloningBehavior cloning : DoFnTester.CloningBehavior.values()) {
      try (DoFnTester<Long, String> tester = DoFnTester.of(new CounterDoFn())) {
        tester.setCloningBehavior(cloning);
        // Explicitly call startBundle().
        tester.startBundle();

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

        // process a couple more.
        tester.processElement(5L);
        tester.processElement(6L);

        // peek and take now have only the 2 last outputs.
        peek = tester.peekOutputElements();
        assertThat(peek, hasItems("5", "6"));
        take = tester.takeOutputElements();
        assertThat(take, hasItems("5", "6"));

        tester.finishBundle();
      }
    }
  }

  @Test
  public void processBundle() throws Exception {
    for (DoFnTester.CloningBehavior cloning : DoFnTester.CloningBehavior.values()) {
      try (DoFnTester<Long, String> tester = DoFnTester.of(new CounterDoFn())) {
        tester.setCloningBehavior(cloning);
        // processBundle() returns all the output like takeOutputElements().
        assertThat(tester.processBundle(1L, 2L, 3L, 4L), hasItems("1", "2", "3", "4"));

        // peek now returns nothing.
        assertTrue(tester.peekOutputElements().isEmpty());
      }
    }
  }

  @Test
  public void processMultipleBundles() throws Exception {
    for (DoFnTester.CloningBehavior cloning : DoFnTester.CloningBehavior.values()) {
      try (DoFnTester<Long, String> tester = DoFnTester.of(new CounterDoFn())) {
        tester.setCloningBehavior(cloning);
        // processBundle() returns all the output like takeOutputElements().
        assertThat(tester.processBundle(1L, 2L, 3L, 4L), hasItems("1", "2", "3", "4"));
        assertThat(tester.processBundle(5L, 6L, 7L), hasItems("5", "6", "7"));
        assertThat(tester.processBundle(8L, 9L), hasItems("8", "9"));

        // peek now returns nothing.
        assertTrue(tester.peekOutputElements().isEmpty());
      }
    }
  }

  @Test
  public void doNotClone() throws Exception {
    final AtomicInteger numSetupCalls = new AtomicInteger();
    final AtomicInteger numTeardownCalls = new AtomicInteger();
    DoFn<Long, String> fn =
        new DoFn<Long, String>() {
          @ProcessElement
          public void process(ProcessContext context) {}

          @Setup
          public void setup() {
            numSetupCalls.addAndGet(1);
          }

          @Teardown
          public void teardown() {
            numTeardownCalls.addAndGet(1);
          }
        };

    try (DoFnTester<Long, String> tester = DoFnTester.of(fn)) {
      tester.setCloningBehavior(DoFnTester.CloningBehavior.DO_NOT_CLONE);

      tester.processBundle(1L, 2L, 3L);
      tester.processBundle(4L, 5L);
      tester.processBundle(6L);
    }
    assertEquals(1, numSetupCalls.get());
    assertEquals(1, numTeardownCalls.get());
  }

  private static class CountBundleCallsFn extends DoFn<Long, String> {
    private int numStartBundleCalls = 0;
    private int numFinishBundleCalls = 0;

    @ProcessElement
    public void process(ProcessContext context) {
      context.output(numStartBundleCalls + "/" + numFinishBundleCalls);
    }

    @StartBundle
    public void startBundle() {
      ++numStartBundleCalls;
    }

    @FinishBundle
    public void finishBundle() {
      ++numFinishBundleCalls;
    }
  }

  @Test
  public void cloneOnce() throws Exception {
    try (DoFnTester<Long, String> tester = DoFnTester.of(new CountBundleCallsFn())) {
      tester.setCloningBehavior(DoFnTester.CloningBehavior.CLONE_ONCE);

      assertThat(tester.processBundle(1L, 2L, 3L), contains("1/0", "1/0", "1/0"));
      assertThat(tester.processBundle(4L, 5L), contains("2/1", "2/1"));
      assertThat(tester.processBundle(6L), contains("3/2"));
    }
  }

  @Test
  public void clonePerBundle() throws Exception {
    try (DoFnTester<Long, String> tester = DoFnTester.of(new CountBundleCallsFn())) {
      tester.setCloningBehavior(DoFnTester.CloningBehavior.CLONE_PER_BUNDLE);

      assertThat(tester.processBundle(1L, 2L, 3L), contains("1/0", "1/0", "1/0"));
      assertThat(tester.processBundle(4L, 5L), contains("1/0", "1/0"));
      assertThat(tester.processBundle(6L), contains("1/0"));
    }
  }

  @Test
  public void processTimestampedElement() throws Exception {
    try (DoFnTester<Long, TimestampedValue<Long>> tester = DoFnTester.of(new ReifyTimestamps())) {
      TimestampedValue<Long> input = TimestampedValue.of(1L, new Instant(100));
      tester.processTimestampedElement(input);
      assertThat(tester.takeOutputElements(), contains(input));
    }
  }

  static class ReifyTimestamps extends DoFn<Long, TimestampedValue<Long>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(TimestampedValue.of(c.element(), c.timestamp()));
    }
  }

  @Test
  public void processElementWithOutputTimestamp() throws Exception {
    try (DoFnTester<Long, String> tester = DoFnTester.of(new CounterDoFn())) {
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
  }

  @Test
  public void peekValuesInWindow() throws Exception {
    try (DoFnTester<Long, String> tester = DoFnTester.of(new CounterDoFn())) {
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
          Matchers.emptyIterable());
    }
  }

  @Test
  public void fnWithSideInputDefault() throws Exception {
    PCollection<Integer> pCollection = p.apply(Create.empty(VarIntCoder.of()));
    final PCollectionView<Integer> value = pCollection.apply(
        View.<Integer>asSingleton().withDefaultValue(0));

    try (DoFnTester<Integer, Integer> tester = DoFnTester.of(new SideInputDoFn(value))) {
      tester.processElement(1);
      tester.processElement(2);
      tester.processElement(4);
      tester.processElement(8);
      assertThat(tester.peekOutputElements(), containsInAnyOrder(0, 0, 0, 0));
    }
  }

  @Test
  public void fnWithSideInputExplicit() throws Exception {
    PCollection<Integer> pCollection = p.apply(Create.of(-2));
    final PCollectionView<Integer> value = pCollection.apply(
        View.<Integer>asSingleton().withDefaultValue(0));

    try (DoFnTester<Integer, Integer> tester = DoFnTester.of(new SideInputDoFn(value))) {
      tester.setSideInput(value, GlobalWindow.INSTANCE, -2);
      tester.processElement(16);
      tester.processElement(32);
      tester.processElement(64);
      tester.processElement(128);
      tester.finishBundle();

      assertThat(tester.peekOutputElements(), containsInAnyOrder(-2, -2, -2, -2));
    }
  }

  @Test
  public void testSupportsWindowParameter() throws Exception {
    Instant now = Instant.now();
    try (DoFnTester<Integer, KV<Integer, BoundedWindow>> tester =
        DoFnTester.of(new DoFnWithWindowParameter())) {
      BoundedWindow firstWindow = new IntervalWindow(now, now.plus(Duration.standardMinutes(1)));
      tester.processWindowedElement(1, now, firstWindow);
      tester.processWindowedElement(2, now, firstWindow);
      BoundedWindow secondWindow = new IntervalWindow(now, now.plus(Duration.standardMinutes(4)));
      tester.processWindowedElement(3, now, secondWindow);
      tester.finishBundle();

      assertThat(
          tester.peekOutputElementsInWindow(firstWindow),
          containsInAnyOrder(
              TimestampedValue.of(KV.of(1, firstWindow), now),
              TimestampedValue.of(KV.of(2, firstWindow), now)));
      assertThat(
          tester.peekOutputElementsInWindow(secondWindow),
          containsInAnyOrder(
              TimestampedValue.of(KV.of(3, secondWindow), now)));
    }
  }

  private static class DoFnWithWindowParameter extends DoFn<Integer, KV<Integer, BoundedWindow>> {
    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) {
      c.output(KV.of(c.element(), window));
    }
  }

  @Test
  public void testSupportsFinishBundleOutput() throws Exception {
    for (DoFnTester.CloningBehavior cloning : DoFnTester.CloningBehavior.values()) {
      try (DoFnTester<Integer, Integer> tester = DoFnTester.of(new BundleCounterDoFn())) {
        tester.setCloningBehavior(cloning);

        assertThat(tester.processBundle(1, 2, 3, 4), contains(4));
        assertThat(tester.processBundle(5, 6, 7), contains(3));
        assertThat(tester.processBundle(8, 9), contains(2));
      }
    }
  }

  private static class BundleCounterDoFn extends DoFn<Integer, Integer> {
    private int elements;

    @StartBundle
    public void startBundle() {
      elements = 0;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      elements++;
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) {
      c.output(elements, Instant.now(), GlobalWindow.INSTANCE);
    }
  }

  private static class SideInputDoFn extends DoFn<Integer, Integer> {
    private final PCollectionView<Integer> value;

    private SideInputDoFn(PCollectionView<Integer> value) {
      this.value = value;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      c.output(c.sideInput(value));
    }
  }

  /**
   * A {@link DoFn} that adds values to a user metric and converts input to String in
   * {@link DoFn.ProcessElement @ProcessElement}.
   */
  private static class CounterDoFn extends DoFn<Long, String> {
    Counter agg = Metrics.counter(CounterDoFn.class, "ctr");
    Counter startBundleCalls = Metrics.counter(CounterDoFn.class, "startBundleCalls");
    Counter finishBundleCalls = Metrics.counter(CounterDoFn.class, "finishBundleCalls");

    private enum LifecycleState {
      UNINITIALIZED,
      SET_UP,
      INSIDE_BUNDLE,
      TORN_DOWN
    }

    private LifecycleState state = LifecycleState.UNINITIALIZED;

    @Setup
    public void setup() {
      checkState(state == LifecycleState.UNINITIALIZED, "Wrong state: %s", state);
      state = LifecycleState.SET_UP;
    }

    @StartBundle
    public void startBundle() {
      checkState(state == LifecycleState.SET_UP, "Wrong state: %s", state);
      state = LifecycleState.INSIDE_BUNDLE;
      startBundleCalls.inc();
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      checkState(state == LifecycleState.INSIDE_BUNDLE, "Wrong state: %s", state);
      agg.inc(c.element());
      Instant instant = new Instant(1000L * c.element());
      c.outputWithTimestamp(c.element().toString(), instant);
    }

    @FinishBundle
    public void finishBundle() {
      checkState(state == LifecycleState.INSIDE_BUNDLE, "Wrong state: %s", state);
      state = LifecycleState.SET_UP;
      finishBundleCalls.inc();
    }

    @Teardown
    public void teardown() {
      checkState(state == LifecycleState.SET_UP, "Wrong state: %s", state);
      state = LifecycleState.TORN_DOWN;
    }
  }
}
