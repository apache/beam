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
package org.apache.beam.sdk.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.util.BundleLifter.BundleLiftDoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BundleLifterTest {

  private static final TupleTag<Integer> INTEGER_SMALL = new TupleTag<Integer>() {};
  private static final TupleTag<Integer> INTEGER_LARGE = new TupleTag<Integer>() {};
  private static final TupleTag<String> STRING_SMALL = new TupleTag<String>() {};
  private static final TupleTag<String> STRING_LARGE = new TupleTag<String>() {};

  @Test
  public void testSmallBundle() throws Exception {
    DoFnTester<Integer, Void> tester =
        DoFnTester.of(new BundleLiftDoFn<>(INTEGER_SMALL, INTEGER_LARGE, 3, x -> 1));

    tester.startBundle();
    tester.processElement(1);
    tester.processElement(2);
    tester.finishBundle();

    assertThat(tester.peekOutputElements(INTEGER_SMALL), containsInAnyOrder(1, 2));
    assertThat(tester.peekOutputElements(INTEGER_LARGE), empty());
  }

  @Test
  public void testLargeBundle() throws Exception {
    DoFnTester<Integer, Void> tester =
        DoFnTester.of(new BundleLiftDoFn<>(INTEGER_SMALL, INTEGER_LARGE, 3, x -> 1));

    tester.startBundle();
    tester.processElement(1);
    tester.processElement(2);
    tester.processElement(3);
    tester.finishBundle();

    assertThat(tester.peekOutputElements(INTEGER_SMALL), empty());
    assertThat(tester.peekOutputElements(INTEGER_LARGE), containsInAnyOrder(1, 2, 3));
  }

  @Test
  public void testSmallBundleWithSizer() throws Exception {
    DoFnTester<String, Void> tester =
        DoFnTester.of(new BundleLiftDoFn<>(STRING_SMALL, STRING_LARGE, 10, e -> e.length()));

    tester.startBundle();
    tester.processElement("123");
    tester.processElement("456");
    tester.processElement("789");
    tester.finishBundle();

    assertThat(tester.peekOutputElements(STRING_SMALL), containsInAnyOrder("123", "456", "789"));
    assertThat(tester.peekOutputElements(STRING_LARGE), empty());
  }

  @Test
  public void testLargeBundleWithSizer() throws Exception {
    DoFnTester<String, Void> tester =
        DoFnTester.of(new BundleLiftDoFn<>(STRING_SMALL, STRING_LARGE, 10, e -> e.length()));

    tester.startBundle();
    tester.processElement("123");
    tester.processElement("456");
    tester.processElement("789");
    tester.processElement("0");
    tester.finishBundle();

    assertThat(tester.peekOutputElements(STRING_SMALL), empty());
    assertThat(
        tester.peekOutputElements(STRING_LARGE), containsInAnyOrder("123", "456", "789", "0"));
  }

  @Test
  public void testStreamsElementsThroughOnceThresholdIsReached() throws Exception {
    DoFnTester<Integer, Void> tester =
        DoFnTester.of(new BundleLiftDoFn<>(INTEGER_SMALL, INTEGER_LARGE, 2, x -> 1));

    tester.startBundle();
    tester.processElement(1);
    assertThat(tester.peekOutputElements(INTEGER_LARGE), empty());
    tester.processElement(2);
    // The buffered elements are flushed as soon as the threshold is reached...
    assertThat(tester.peekOutputElements(INTEGER_LARGE), contains(1, 2));
    tester.processElement(3);
    // ...and later elements are output directly, before the bundle finishes.
    assertThat(tester.peekOutputElements(INTEGER_LARGE), contains(1, 2, 3));
    tester.finishBundle();

    assertThat(tester.peekOutputElements(INTEGER_SMALL), empty());
    assertThat(tester.peekOutputElements(INTEGER_LARGE), contains(1, 2, 3));
  }

  @Test
  public void testKeyedThresholdIsEvaluatedPerKey() throws Exception {
    DoFnTester<Integer, Void> tester =
        DoFnTester.of(new BundleLiftDoFn<>(INTEGER_SMALL, INTEGER_LARGE, 3, x -> 1, x -> x % 2));

    tester.startBundle();
    for (int i = 0; i < 5; i++) {
      tester.processElement(i);
    }
    tester.finishBundle();

    // Even key has 3 elements and reaches the threshold; odd key has 2 and does not.
    assertThat(tester.peekOutputElements(INTEGER_LARGE), containsInAnyOrder(0, 2, 4));
    assertThat(tester.peekOutputElements(INTEGER_SMALL), containsInAnyOrder(1, 3));
  }

  @Test
  public void testKeyedThresholdIsEvaluatedPerWindow() throws Exception {
    IntervalWindow w1 = new IntervalWindow(new Instant(0), new Instant(10));
    IntervalWindow w2 = new IntervalWindow(new Instant(10), new Instant(20));
    DoFnTester<Integer, Void> tester =
        DoFnTester.of(new BundleLiftDoFn<>(INTEGER_SMALL, INTEGER_LARGE, 3, x -> 1, x -> "key"));

    tester.startBundle();
    tester.processWindowedElement(1, new Instant(1), w1);
    tester.processWindowedElement(2, new Instant(2), w1);
    tester.processWindowedElement(3, new Instant(11), w2);
    tester.processWindowedElement(4, new Instant(12), w2);
    tester.finishBundle();

    // 4 elements share the key, but each window only has 2.
    assertThat(tester.peekOutputElements(INTEGER_LARGE), empty());
    assertThat(tester.peekOutputElements(INTEGER_SMALL), containsInAnyOrder(1, 2, 3, 4));
  }

  @Test
  public void testUnkeyedThresholdIsEvaluatedPerBundle() throws Exception {
    IntervalWindow w1 = new IntervalWindow(new Instant(0), new Instant(10));
    IntervalWindow w2 = new IntervalWindow(new Instant(10), new Instant(20));
    DoFnTester<Integer, Void> tester =
        DoFnTester.of(new BundleLiftDoFn<>(INTEGER_SMALL, INTEGER_LARGE, 3, x -> 1));

    tester.startBundle();
    tester.processWindowedElement(1, new Instant(1), w1);
    tester.processWindowedElement(2, new Instant(2), w1);
    tester.processWindowedElement(3, new Instant(11), w2);
    tester.processWindowedElement(4, new Instant(12), w2);
    tester.finishBundle();

    // Without a key function the whole bundle counts toward the threshold.
    assertThat(tester.peekOutputElements(INTEGER_LARGE), containsInAnyOrder(1, 2, 3, 4));
    assertThat(tester.peekOutputElements(INTEGER_SMALL), empty());
  }

  @Test
  public void testNullKeysShareTheirOwnKey() throws Exception {
    DoFnTester<Integer, Void> tester =
        DoFnTester.of(
            new BundleLiftDoFn<>(
                INTEGER_SMALL, INTEGER_LARGE, 2, x -> 1, x -> x < 0 ? null : "key"));

    tester.startBundle();
    tester.processElement(-1);
    tester.processElement(5);
    tester.processElement(-2);
    tester.finishBundle();

    // The two null-key elements share a key and reach the threshold; "key" has only one element.
    assertThat(tester.peekOutputElements(INTEGER_LARGE), containsInAnyOrder(-1, -2));
    assertThat(tester.peekOutputElements(INTEGER_SMALL), contains(5));
  }

  @Test
  public void testMemoryCapEvictsSmallestKey() throws Exception {
    // Threshold 4 caps total buffered bytes at 8.
    DoFnTester<Integer, Void> tester =
        DoFnTester.of(new BundleLiftDoFn<>(INTEGER_SMALL, INTEGER_LARGE, 4, x -> 1, x -> x));

    tester.startBundle();
    for (int i = 0; i < 9; i++) {
      tester.processElement(i);
    }
    // The 9th distinct key pushes the total (9) over the cap (8), so the smallest keys are evicted
    // in one pass until at most the threshold (4) remains buffered: 5 keys go out early.
    assertThat(tester.peekOutputElements(INTEGER_SMALL), hasSize(5));
    tester.finishBundle();

    assertThat(
        tester.peekOutputElements(INTEGER_SMALL), containsInAnyOrder(0, 1, 2, 3, 4, 5, 6, 7, 8));
    assertThat(tester.peekOutputElements(INTEGER_LARGE), empty());
  }

  @Test
  public void testUntrackedKeysGoToSmallOutputAfterEviction() throws Exception {
    // Threshold 4 caps total buffered bytes at 8.
    DoFnTester<Integer, Void> tester =
        DoFnTester.of(new BundleLiftDoFn<>(INTEGER_SMALL, INTEGER_LARGE, 4, x -> 1, x -> x));

    tester.startBundle();
    for (int i = 0; i < 9; i++) {
      tester.processElement(i);
    }
    // Keys 0-4 were evicted; keys 5-8 are still buffered.
    assertThat(tester.peekOutputElements(INTEGER_SMALL), containsInAnyOrder(0, 1, 2, 3, 4));

    // A new key and an evicted key are no longer tracked, so they go straight to the small output.
    tester.processElement(100);
    tester.processElement(0);
    assertThat(tester.peekOutputElements(INTEGER_SMALL), containsInAnyOrder(0, 1, 2, 3, 4, 100, 0));

    // A key that is still buffered can still reach the threshold.
    tester.processElement(5);
    tester.processElement(5);
    tester.processElement(5);
    assertThat(tester.peekOutputElements(INTEGER_LARGE), contains(5, 5, 5, 5));
    tester.finishBundle();

    assertThat(
        tester.peekOutputElements(INTEGER_SMALL),
        containsInAnyOrder(0, 1, 2, 3, 4, 100, 0, 6, 7, 8));
  }

  @Test
  public void testCustomMaxBufferedBytes() throws Exception {
    // Threshold 4 with a cap of 4: the 5th distinct key exceeds the cap, and keys are evicted
    // until at most half of it (2) remains buffered.
    DoFnTester<Integer, Void> tester =
        DoFnTester.of(new BundleLiftDoFn<>(INTEGER_SMALL, INTEGER_LARGE, 4, 4, x -> 1, x -> x));

    tester.startBundle();
    for (int i = 0; i < 5; i++) {
      tester.processElement(i);
    }
    assertThat(tester.peekOutputElements(INTEGER_SMALL), containsInAnyOrder(0, 1, 2));
    tester.finishBundle();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMaxBufferedBytesBelowThresholdIsRejected() {
    BundleLifter.of(INTEGER_SMALL, INTEGER_LARGE, 10L).withMaxBufferedBytes(5L);
  }

  @Test
  public void testWithMaxBufferedBytes() {
    BundleLifter<Integer> lifter =
        BundleLifter.of(INTEGER_SMALL, INTEGER_LARGE, 10L).withMaxBufferedBytes(15L);
    assertEquals(15L, lifter.maxBufferedBytes);
    assertEquals(20L, BundleLifter.of(INTEGER_SMALL, INTEGER_LARGE, 10L).maxBufferedBytes);
  }

  @Test
  public void testPreservesEachElementsWindowAndTimestamp() throws Exception {
    IntervalWindow w1 = new IntervalWindow(new Instant(0), new Instant(10));
    IntervalWindow w2 = new IntervalWindow(new Instant(10), new Instant(20));

    DoFnTester<Integer, Void> large =
        DoFnTester.of(new BundleLiftDoFn<>(INTEGER_SMALL, INTEGER_LARGE, 2, x -> 1));
    large.startBundle();
    large.processWindowedElement(1, new Instant(5), w1);
    large.processWindowedElement(2, new Instant(15), w2);
    large.finishBundle();
    assertThat(
        describe(large.getMutableOutput(INTEGER_LARGE)),
        contains("1@" + new Instant(5) + " in " + w1, "2@" + new Instant(15) + " in " + w2));

    DoFnTester<Integer, Void> small =
        DoFnTester.of(new BundleLiftDoFn<>(INTEGER_SMALL, INTEGER_LARGE, 3, x -> 1));
    small.startBundle();
    small.processWindowedElement(1, new Instant(5), w1);
    small.processWindowedElement(2, new Instant(15), w2);
    small.finishBundle();
    assertThat(
        describe(small.getMutableOutput(INTEGER_SMALL)),
        contains("1@" + new Instant(5) + " in " + w1, "2@" + new Instant(15) + " in " + w2));
  }

  private static List<String> describe(List<ValueInSingleWindow<Integer>> outputs) {
    List<String> described = new ArrayList<>();
    for (ValueInSingleWindow<Integer> output : outputs) {
      described.add(output.getValue() + "@" + output.getTimestamp() + " in " + output.getWindow());
    }
    return described;
  }

  @Test
  public void testCoderBasedSizing() {
    BundleLifter<String> lifter =
        BundleLifter.of(STRING_SMALL, STRING_LARGE, 10L, StringUtf8Coder.of());
    assertThat(lifter.elementSizer.apply("hello"), greaterThanOrEqualTo(5));
  }
}
