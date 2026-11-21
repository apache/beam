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
package org.apache.beam.runners.dataflow.worker.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.runners.dataflow.worker.util.GroupAlsoByWindowProperties.GroupAlsoByWindowDoFnFactory;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.util.common.Reiterable;
import org.apache.beam.sdk.util.common.Reiterator;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for {@link BatchGroupAlsoByWindowViaIteratorsFn}.
 *
 * <p>Note the absence of tests for sessions, as merging window functions are not supported.
 */
@RunWith(JUnit4.class)
public class GroupAlsoByWindowViaIteratorsDoFnTest {

  @Rule public final transient ExpectedException thrown = ExpectedException.none();

  private class GABWViaIteratorsDoFnFactory<K, InputT>
      implements GroupAlsoByWindowDoFnFactory<K, InputT, Iterable<InputT>> {
    @Override
    public <W extends BoundedWindow>
        BatchGroupAlsoByWindowFn<K, InputT, Iterable<InputT>> forStrategy(
            WindowingStrategy<?, W> windowingStrategy,
            StateInternalsFactory<K> stateInternalsFactory) {
      return new BatchGroupAlsoByWindowViaIteratorsFn<K, InputT, W>(windowingStrategy);
    }
  }

  @Test
  public void testEmptyInputEmptyOutput() throws Exception {
    GroupAlsoByWindowProperties.emptyInputEmptyOutput(new GABWViaIteratorsDoFnFactory<>());
  }

  @Test
  public void testGroupsElementsIntoFixedWindows() throws Exception {
    GroupAlsoByWindowProperties.groupsElementsIntoFixedWindows(
        new GABWViaIteratorsDoFnFactory<String, String>());
  }

  @Test
  public void testGroupsElementsIntoSlidingWindows() throws Exception {
    GroupAlsoByWindowProperties.groupsElementsIntoSlidingWindowsWithMinTimestamp(
        new GABWViaIteratorsDoFnFactory<String, String>());
  }

  @Test
  public void testGroupsIntoOverlappingNonmergingWindows() throws Exception {
    GroupAlsoByWindowProperties.groupsIntoOverlappingNonmergingWindows(
        new GABWViaIteratorsDoFnFactory<String, String>());
  }

  @Test
  public void testGroupsElementsIntoFixedWindowsWithEndOfWindowTimestamp() throws Exception {
    GroupAlsoByWindowProperties.groupsElementsIntoFixedWindowsWithEndOfWindowTimestamp(
        new GABWViaIteratorsDoFnFactory<String, String>());
  }

  @Test
  public void testLatestTimestampNotSupported() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("TimestampCombiner");
    thrown.expectMessage("not support");

    GroupAlsoByWindowProperties.groupsElementsIntoFixedWindowsWithLatestTimestamp(
        new GABWViaIteratorsDoFnFactory<String, String>());
  }

  @Test
  public void testMergingNotSupported() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("merging");
    thrown.expectMessage("not support");

    GroupAlsoByWindowProperties.groupsElementsInMergedSessions(
        new GABWViaIteratorsDoFnFactory<String, String>());
  }

  /** Subclass of {@link IntervalWindow} overriding {@code maxTimestamp()}. */
  private static final class CustomMaxTimestampIntervalWindow extends IntervalWindow {
    private final Instant customMaxTs;

    CustomMaxTimestampIntervalWindow(Instant start, Instant end, Instant customMaxTs) {
      super(start, end);
      this.customMaxTs = customMaxTs;
    }

    @Override
    public Instant maxTimestamp() {
      return customMaxTs;
    }
  }

  private static <T> Reiterable<T> asReiterable(final List<T> list) {
    return () -> new ListReiterator<>(list, 0);
  }

  private static final class ListReiterator<T> implements Reiterator<T> {
    private final List<T> list;
    private int idx;

    ListReiterator(List<T> list, int idx) {
      this.list = list;
      this.idx = idx;
    }

    @Override
    public boolean hasNext() {
      return idx < list.size();
    }

    @Override
    public T next() {
      if (idx >= list.size()) {
        throw new NoSuchElementException();
      }
      return list.get(idx++);
    }

    @Override
    public Reiterator<T> copy() {
      return new ListReiterator<>(list, idx);
    }
  }

  @Test
  public void testEagerConsumptionFixedWindowsAndReiteration() throws Exception {
    IntervalWindow w2 = new IntervalWindow(new Instant(10), new Instant(20));
    IntervalWindow w3 = new IntervalWindow(new Instant(20), new Instant(30));

    // Use distinct IntervalWindow instances with identical bounds (mirrors IntervalWindowCoder).
    List<WindowedValue<String>> inputs =
        Arrays.asList(
            WindowedValues.of(
                "a1",
                new Instant(0),
                new IntervalWindow(new Instant(0), new Instant(10)),
                PaneInfo.NO_FIRING),
            WindowedValues.of(
                null,
                new Instant(9), // exact w1.maxTimestamp()
                new IntervalWindow(new Instant(0), new Instant(10)),
                PaneInfo.NO_FIRING),
            WindowedValues.of("b1", new Instant(10), w2, PaneInfo.NO_FIRING),
            WindowedValues.of("c1", new Instant(25), w3, PaneInfo.NO_FIRING));

    WindowingStrategy<?, IntervalWindow> strategy =
        WindowingStrategy.of(FixedWindows.of(Duration.millis(10)));
    BatchGroupAlsoByWindowViaIteratorsFn<String, String, IntervalWindow> fn =
        new BatchGroupAlsoByWindowViaIteratorsFn<>(strategy);

    List<List<String>> eagerResults = new ArrayList<>();
    List<Iterable<String>> savedIterables = new ArrayList<>();
    fn.processElement(
        KV.of("k", asReiterable(inputs)),
        null,
        null,
        null,
        wv -> {
          Iterable<String> iterable = wv.getValue().getValue();
          savedIterables.add(iterable);
          Reiterator<String> it = ((Reiterable<String>) iterable).iterator();
          // Verify hasNext() idempotence and copy() before & after exhaustion.
          assertTrue(it.hasNext());
          assertTrue(it.hasNext());
          List<String> drained = new ArrayList<>();
          while (it.hasNext()) {
            drained.add(it.next());
          }
          Reiterator<String> exhaustedCopy = it.copy();
          assertFalse(exhaustedCopy.hasNext());
          try {
            exhaustedCopy.next();
            fail("Expected NoSuchElementException");
          } catch (NoSuchElementException expected) {
          }
          eagerResults.add(drained);
        });

    assertEquals(3, eagerResults.size());
    assertEquals(Arrays.asList("a1", null), eagerResults.get(0));
    assertEquals(Collections.singletonList("b1"), eagerResults.get(1));
    assertEquals(Collections.singletonList("c1"), eagerResults.get(2));

    // Saved reiterables must still yield exact window elements on a second pass after
    // processElement.
    List<String> secondPass0 = new ArrayList<>();
    savedIterables.get(0).forEach(secondPass0::add);
    assertEquals(Arrays.asList("a1", null), secondPass0);
    List<String> secondPass1 = new ArrayList<>();
    savedIterables.get(1).forEach(secondPass1::add);
    assertEquals(Collections.singletonList("b1"), secondPass1);
    List<String> secondPass2 = new ArrayList<>();
    savedIterables.get(2).forEach(secondPass2::add);
    assertEquals(Collections.singletonList("c1"), secondPass2);
  }

  @Test
  public void testEagerPartialConsumptionAndOverlappingWindowsDetachOnce() throws Exception {
    IntervalWindow w1 = new IntervalWindow(new Instant(0), new Instant(20));
    IntervalWindow w2 = new IntervalWindow(new Instant(10), new Instant(30));
    List<WindowedValue<String>> inputs =
        Arrays.asList(
            WindowedValues.of("v1", new Instant(5), w1, PaneInfo.NO_FIRING),
            WindowedValues.of("v2", new Instant(12), Arrays.asList(w1, w2), PaneInfo.NO_FIRING),
            WindowedValues.of("v3", new Instant(15), Arrays.asList(w1, w2), PaneInfo.NO_FIRING),
            WindowedValues.of("v4", new Instant(25), w2, PaneInfo.NO_FIRING));

    BatchGroupAlsoByWindowViaIteratorsFn<String, String, IntervalWindow> fn =
        new BatchGroupAlsoByWindowViaIteratorsFn<>(
            WindowingStrategy.of(
                SlidingWindows.of(Duration.millis(20)).every(Duration.millis(10))));

    // Eagerly read 1 element via next() (without calling hasNext() first!), copy mid-window, then
    // finish both the iterator and copy AFTER processElement returns.
    List<Reiterator<String>> partialIters = new ArrayList<>();
    List<Reiterator<String>> copies = new ArrayList<>();
    List<String> firstElems = new ArrayList<>();
    fn.processElement(
        KV.of("k", asReiterable(inputs)),
        null,
        null,
        null,
        wv -> {
          Reiterator<String> it = ((Reiterable<String>) wv.getValue().getValue()).iterator();
          firstElems.add(it.next());
          copies.add(it.copy());
          partialIters.add(it);
        });

    assertEquals(Arrays.asList("v1", "v2"), firstElems);
    List<String> w1Rest = new ArrayList<>();
    partialIters.get(0).forEachRemaining(w1Rest::add);
    assertEquals(Arrays.asList("v2", "v3"), w1Rest);

    List<String> w1CopyRest = new ArrayList<>();
    copies.get(0).forEachRemaining(w1CopyRest::add);
    assertEquals(Arrays.asList("v2", "v3"), w1CopyRest);

    List<String> w2Rest = new ArrayList<>();
    partialIters.get(1).forEachRemaining(w2Rest::add);
    assertEquals(Arrays.asList("v3", "v4"), w2Rest);

    List<String> w2CopyRest = new ArrayList<>();
    copies.get(1).forEachRemaining(w2CopyRest::add);
    assertEquals(Arrays.asList("v3", "v4"), w2CopyRest);
  }

  @Test
  public void testCustomIntervalWindowSubclassMaxTimestampRespected() throws Exception {
    // Subclass overrides maxTimestamp() to 20 while IntervalWindow.end() is 10 (end-1 = 9).
    CustomMaxTimestampIntervalWindow customWindow =
        new CustomMaxTimestampIntervalWindow(new Instant(0), new Instant(10), new Instant(20));
    IntervalWindow w2 = new IntervalWindow(new Instant(5), new Instant(25));

    List<WindowedValue<String>> singlePathInputs =
        Arrays.asList(
            WindowedValues.of("v1", new Instant(1), customWindow, PaneInfo.NO_FIRING),
            WindowedValues.of("v2", new Instant(12), customWindow, PaneInfo.NO_FIRING));

    BatchGroupAlsoByWindowViaIteratorsFn<String, String, IntervalWindow> fn =
        new BatchGroupAlsoByWindowViaIteratorsFn<>(
            WindowingStrategy.of(FixedWindows.of(Duration.millis(10))));

    List<List<String>> emittedSingle = new ArrayList<>();
    fn.processElement(
        KV.of("k", asReiterable(singlePathInputs)),
        null,
        null,
        null,
        wv -> {
          List<String> vals = new ArrayList<>();
          wv.getValue().getValue().forEach(vals::add);
          emittedSingle.add(vals);
        });
    assertEquals(1, emittedSingle.size());
    assertEquals(Arrays.asList("v1", "v2"), emittedSingle.get(0));

    // Also test promotion from singleActiveWindow into ActiveWindows.
    List<WindowedValue<String>> promotedInputs =
        Arrays.asList(
            WindowedValues.of("v1", new Instant(1), customWindow, PaneInfo.NO_FIRING),
            WindowedValues.of("v2", new Instant(6), w2, PaneInfo.NO_FIRING),
            WindowedValues.of("v3", new Instant(12), customWindow, PaneInfo.NO_FIRING));
    List<List<String>> emittedPromoted = new ArrayList<>();
    fn.processElement(
        KV.of("k", asReiterable(promotedInputs)),
        null,
        null,
        null,
        wv -> {
          List<String> vals = new ArrayList<>();
          wv.getValue().getValue().forEach(vals::add);
          emittedPromoted.add(vals);
        });
    assertEquals(2, emittedPromoted.size());
    assertEquals(Arrays.asList("v1", "v3"), emittedPromoted.get(0));
    assertEquals(Collections.singletonList("v2"), emittedPromoted.get(1));
  }

  @Test
  public void testNoDuplicateEmissionWhenSameWindowFollowsPastMaxTimestamp() throws Exception {
    IntervalWindow w = new IntervalWindow(new Instant(0), new Instant(10));
    List<WindowedValue<String>> inputs =
        Arrays.asList(
            WindowedValues.of("v1", new Instant(1), w, PaneInfo.NO_FIRING),
            WindowedValues.of("v2", new Instant(15), w, PaneInfo.NO_FIRING));

    BatchGroupAlsoByWindowViaIteratorsFn<String, String, IntervalWindow> fn =
        new BatchGroupAlsoByWindowViaIteratorsFn<>(
            WindowingStrategy.of(FixedWindows.of(Duration.millis(10))));

    List<WindowedValue<KV<String, Iterable<String>>>> emitted = new ArrayList<>();
    fn.processElement(KV.of("k", asReiterable(inputs)), null, null, null, emitted::add);
    assertEquals(1, emitted.size());
  }

  @Test
  public void testIntervalWindowMinValueEndThrowsArithmeticException() throws Exception {
    IntervalWindow minWin =
        new IntervalWindow(new Instant(Long.MIN_VALUE), new Instant(Long.MIN_VALUE));
    List<WindowedValue<String>> inputs =
        Collections.singletonList(
            WindowedValues.of("v1", new Instant(0), minWin, PaneInfo.NO_FIRING));
    BatchGroupAlsoByWindowViaIteratorsFn<String, String, IntervalWindow> fn =
        new BatchGroupAlsoByWindowViaIteratorsFn<>(
            WindowingStrategy.of(FixedWindows.of(Duration.millis(10))));
    thrown.expect(ArithmeticException.class);
    fn.processElement(KV.of("k", inputs), null, null, null, wv -> {});
  }

  @Test
  public void testValueInEmptyWindowsSkipped() throws Exception {
    List<WindowedValue<String>> inputs =
        Collections.singletonList(new ValueInEmptyWindows<>("ignored"));
    BatchGroupAlsoByWindowViaIteratorsFn<String, String, GlobalWindow> fn =
        new BatchGroupAlsoByWindowViaIteratorsFn<>(WindowingStrategy.globalDefault());
    List<Object> emitted = new ArrayList<>();
    fn.processElement(KV.of("k", asReiterable(inputs)), null, null, null, emitted::add);
    assertTrue(emitted.isEmpty());
  }

  @Test
  public void testCopyCountBoundedForSlidingAndFixedWindows() throws Exception {
    final AtomicInteger slidingCopies = new AtomicInteger();
    // 20 timestamps in SlidingWindows(size=40, period=10) so each element sits in 4 windows.
    List<WindowedValue<String>> slidingInputs = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      long ts = 30L + i * 2L;
      List<IntervalWindow> wins = new ArrayList<>();
      for (long start = ((ts / 10L) * 10L) - 30L; start <= ts; start += 10L) {
        if (ts >= start && ts < start + 40L) {
          wins.add(new IntervalWindow(new Instant(start), new Instant(start + 40L)));
        }
      }
      slidingInputs.add(WindowedValues.of("v" + i, new Instant(ts), wins, PaneInfo.NO_FIRING));
    }
    BatchGroupAlsoByWindowViaIteratorsFn<String, String, IntervalWindow> slidingFn =
        new BatchGroupAlsoByWindowViaIteratorsFn<>(
            WindowingStrategy.of(
                SlidingWindows.of(Duration.millis(40)).every(Duration.millis(10))));
    int[] windowCount = new int[1];
    slidingFn.processElement(
        KV.of("k", asCountingReiterable(slidingInputs, slidingCopies)),
        null,
        null,
        null,
        wv -> {
          windowCount[0]++;
          for (String s : wv.getValue().getValue()) {
            assertNotNull(s);
          }
        });
    // Per window: 1 copy in WindowReiterable.createIterator() + 1 detach copy in
    // WindowReiterator.skipToValidElement() = 2 * W copies (O(W), NOT O(W * N)).
    assertEquals(2 * windowCount[0], slidingCopies.get());

    // Non-overlapping FixedWindows eagerly drained: exact 1 copy per window (from
    // WindowReiterable.createIterator()) and 0 redundant parent copies at window boundaries.
    final AtomicInteger fixedCopies = new AtomicInteger();
    List<WindowedValue<String>> fixedInputs = new ArrayList<>();
    for (int w = 0; w < 5; w++) {
      IntervalWindow win = new IntervalWindow(new Instant(w * 10L), new Instant((w + 1) * 10L));
      for (int e = 0; e < 4; e++) {
        fixedInputs.add(
            WindowedValues.of(
                "w" + w + "_" + e, new Instant(w * 10L + e), win, PaneInfo.NO_FIRING));
      }
    }
    BatchGroupAlsoByWindowViaIteratorsFn<String, String, IntervalWindow> fixedFn =
        new BatchGroupAlsoByWindowViaIteratorsFn<>(
            WindowingStrategy.of(FixedWindows.of(Duration.millis(10))));
    int[] fixedWindowsSeen = new int[1];
    fixedFn.processElement(
        KV.of("k", asCountingReiterable(fixedInputs, fixedCopies)),
        null,
        null,
        null,
        wv -> {
          fixedWindowsSeen[0]++;
          for (String s : wv.getValue().getValue()) {
            assertNotNull(s);
          }
        });
    assertEquals(5, fixedWindowsSeen[0]);
    assertEquals(5, fixedCopies.get());
  }

  @Test
  public void testFixedWindowPartialDrainWithoutTrailingHasNextDetachesParent() throws Exception {
    IntervalWindow w0 = new IntervalWindow(new Instant(0), new Instant(10));
    IntervalWindow w1 = new IntervalWindow(new Instant(10), new Instant(20));
    List<WindowedValue<String>> inputs =
        Arrays.asList(
            WindowedValues.of("a", new Instant(1), w0, PaneInfo.NO_FIRING),
            WindowedValues.of("b", new Instant(11), w1, PaneInfo.NO_FIRING),
            WindowedValues.of("c", new Instant(12), w1, PaneInfo.NO_FIRING));
    BatchGroupAlsoByWindowViaIteratorsFn<String, String, IntervalWindow> fn =
        new BatchGroupAlsoByWindowViaIteratorsFn<>(
            WindowingStrategy.of(FixedWindows.of(Duration.millis(10))));
    List<Reiterator<String>> saved = new ArrayList<>();
    List<String> w1Vals = new ArrayList<>();
    fn.processElement(
        KV.of("k", asReiterable(inputs)),
        null,
        null,
        null,
        wv -> {
          Reiterator<String> it = ((Reiterable<String>) wv.getValue().getValue()).iterator();
          if (saved.isEmpty()) {
            // Pull only element of w0 WITHOUT calling trailing hasNext().
            assertEquals("a", it.next());
            saved.add(it);
          } else {
            it.forEachRemaining(w1Vals::add);
          }
        });
    assertEquals(Arrays.asList("b", "c"), w1Vals);
    // Saved w0 iterator must still observe end-of-window cleanly without disturbing w1.
    assertFalse(saved.get(0).hasNext());
    assertFalse(saved.get(0).copy().hasNext());
  }

  private static <T> Reiterable<T> asCountingReiterable(List<T> list, AtomicInteger copyCount) {
    return () -> new CountingListReiterator<>(list, 0, copyCount);
  }

  private static final class CountingListReiterator<T> implements Reiterator<T> {
    private final List<T> list;
    private int index;
    private final AtomicInteger copyCount;

    CountingListReiterator(List<T> list, int index, AtomicInteger copyCount) {
      this.list = list;
      this.index = index;
      this.copyCount = copyCount;
    }

    @Override
    public boolean hasNext() {
      return index < list.size();
    }

    @Override
    public T next() {
      return list.get(index++);
    }

    @Override
    public Reiterator<T> copy() {
      copyCount.incrementAndGet();
      return new CountingListReiterator<>(list, index, copyCount);
    }
  }
}
