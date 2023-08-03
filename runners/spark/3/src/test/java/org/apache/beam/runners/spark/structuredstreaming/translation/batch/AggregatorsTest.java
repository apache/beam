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
package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers.encoderFor;
import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers.windowedValueEncoder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.joda.time.Duration.standardMinutes;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Streams;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.util.MutablePair;
import org.hamcrest.Matcher;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

@RunWith(Enclosed.class)
public class AggregatorsTest {

  // just something easy readable
  private static final Instant NOW = Instant.parse("2000-01-01T00:00Z");

  /** Tests for NonMergingWindowedAggregator in {@link Aggregators}. */
  public static class NonMergingWindowedAggregatorTest {

    private SlidingWindows sliding =
        SlidingWindows.of(standardMinutes(15)).every(standardMinutes(5));

    private Aggregator<
            WindowedValue<Integer>,
            Map<IntervalWindow, MutablePair<Instant, Integer>>,
            Collection<WindowedValue<Integer>>>
        agg = windowedAgg(sliding);

    @Test
    public void testReduce() {
      Map<IntervalWindow, MutablePair<Instant, Integer>> acc;

      acc = agg.reduce(agg.zero(), windowedValue(1, at(10)));
      assertThat(
          acc,
          equalsToMap(
              KV.of(intervalWindow(0, 15), pair(at(10), 1)),
              KV.of(intervalWindow(5, 20), pair(at(10), 1)),
              KV.of(intervalWindow(10, 25), pair(at(10), 1))));

      acc = agg.reduce(acc, windowedValue(2, at(16)));
      assertThat(
          acc,
          equalsToMap(
              KV.of(intervalWindow(0, 15), pair(at(10), 1)),
              KV.of(intervalWindow(5, 20), pair(at(16), 3)),
              KV.of(intervalWindow(10, 25), pair(at(16), 3)),
              KV.of(intervalWindow(15, 30), pair(at(16), 2))));
    }

    @Test
    public void testMerge() {
      Map<IntervalWindow, MutablePair<Instant, Integer>> acc;

      assertThat(agg.merge(agg.zero(), agg.zero()), equalTo(agg.zero()));

      acc = mapOf(KV.of(intervalWindow(0, 15), pair(at(0), 1)));

      assertThat(agg.merge(acc, agg.zero()), equalTo(acc));
      assertThat(agg.merge(agg.zero(), acc), equalTo(acc));

      acc = agg.merge(acc, acc);
      assertThat(acc, equalsToMap(KV.of(intervalWindow(0, 15), pair(at(0), 1 + 1))));

      acc = agg.merge(acc, mapOf(KV.of(intervalWindow(5, 20), pair(at(5), 3))));
      assertThat(
          acc,
          equalsToMap(
              KV.of(intervalWindow(0, 15), pair(at(0), 1 + 1)),
              KV.of(intervalWindow(5, 20), pair(at(5), 3))));

      acc = agg.merge(mapOf(KV.of(intervalWindow(10, 25), pair(at(10), 4))), acc);
      assertThat(
          acc,
          equalsToMap(
              KV.of(intervalWindow(0, 15), pair(at(0), 1 + 1)),
              KV.of(intervalWindow(5, 20), pair(at(5), 3)),
              KV.of(intervalWindow(10, 25), pair(at(10), 4))));
    }

    private WindowedValue<Integer> windowedValue(Integer value, Instant ts) {
      return WindowedValue.of(value, ts, sliding.assignWindows(ts), PaneInfo.NO_FIRING);
    }
  }

  /**
   * Shared implementation of tests for SessionsAggregator and MergingWindowedAggregator in {@link
   * Aggregators}.
   */
  public abstract static class AbstractSessionsTest<
      AccT extends Map<IntervalWindow, MutablePair<Instant, Integer>>> {

    static final Duration SESSIONS_GAP = standardMinutes(15);

    final Aggregator<WindowedValue<Integer>, AccT, Collection<WindowedValue<Integer>>> agg;

    AbstractSessionsTest(WindowFn<?, ?> windowFn) {
      agg = windowedAgg(windowFn);
    }

    abstract AccT accOf(KV<IntervalWindow, MutablePair<Instant, Integer>>... entries);

    @Test
    public void testReduce() {
      AccT acc;

      acc = agg.reduce(agg.zero(), sessionValue(10, at(0)));
      assertThat(acc, equalsToMap(KV.of(sessionWindow(0), pair(at(0), 10))));

      // 2nd session after 1st
      acc = agg.reduce(acc, sessionValue(7, at(20)));
      assertThat(
          acc,
          equalsToMap(
              KV.of(sessionWindow(0), pair(at(0), 10)), KV.of(sessionWindow(20), pair(at(20), 7))));

      // merge into 2nd session
      acc = agg.reduce(acc, sessionValue(6, at(18)));
      assertThat(
          acc,
          equalsToMap(
              KV.of(sessionWindow(0), pair(at(0), 10)),
              KV.of(sessionWindow(18, 35), pair(at(20), 7 + 6))));

      // merge into 2nd session
      acc = agg.reduce(acc, sessionValue(5, at(21)));
      assertThat(
          acc,
          equalsToMap(
              KV.of(sessionWindow(0), pair(at(0), 10)),
              KV.of(sessionWindow(18, 36), pair(at(21), 7 + 6 + 5))));

      // 3rd session after 2nd
      acc = agg.reduce(acc, sessionValue(2, NOW.plus(standardMinutes(45))));
      assertThat(
          acc,
          equalsToMap(
              KV.of(sessionWindow(0), pair(at(0), 10)),
              KV.of(sessionWindow(18, 36), pair(at(21), 7 + 6 + 5)),
              KV.of(sessionWindow(45), pair(at(45), 2))));

      // merge with 1st and 2nd
      acc = agg.reduce(acc, sessionValue(1, at(10)));
      assertThat(
          acc,
          equalsToMap(
              KV.of(sessionWindow(0, 36), pair(at(21), 10 + 7 + 6 + 5 + 1)),
              KV.of(sessionWindow(45), pair(at(45), 2))));
    }

    @Test
    public void testMerge() {
      AccT acc;

      assertThat(agg.merge(agg.zero(), agg.zero()), equalTo(agg.zero()));

      acc = accOf(KV.of(sessionWindow(0), pair(at(0), 1)));

      assertThat(agg.merge(acc, agg.zero()), equalTo(acc));
      assertThat(agg.merge(agg.zero(), acc), equalTo(acc));

      acc = agg.merge(acc, acc);
      assertThat(acc, equalsToMap(KV.of(sessionWindow(0), pair(at(0), 1 + 1))));

      acc = agg.merge(acc, accOf(KV.of(sessionWindow(20), pair(at(20), 2))));
      assertThat(
          acc,
          equalsToMap(
              KV.of(sessionWindow(0), pair(at(0), 1 + 1)),
              KV.of(sessionWindow(20), pair(at(20), 2))));

      acc = agg.merge(accOf(KV.of(sessionWindow(40), pair(at(40), 3))), acc);
      assertThat(
          acc,
          equalsToMap(
              KV.of(sessionWindow(0), pair(at(0), 1 + 1)),
              KV.of(sessionWindow(20), pair(at(20), 2)),
              KV.of(sessionWindow(40), pair(at(40), 3))));

      acc = agg.merge(acc, accOf(KV.of(sessionWindow(10), pair(at(10), 4))));
      assertThat(
          acc,
          equalsToMap(
              KV.of(sessionWindow(0, 35), pair(at(20), 1 + 1 + 2 + 4)),
              KV.of(sessionWindow(40), pair(at(40), 3))));

      acc = agg.merge(accOf(KV.of(sessionWindow(5, 45), pair(at(30), 5))), acc);
      assertThat(
          acc, equalsToMap(KV.of(sessionWindow(0, 55), pair(at(40), 1 + 1 + 2 + 4 + 3 + 5))));
    }

    private WindowedValue<Integer> sessionValue(Integer value, Instant ts) {
      return WindowedValue.of(value, ts, new IntervalWindow(ts, SESSIONS_GAP), PaneInfo.NO_FIRING);
    }

    private IntervalWindow sessionWindow(int fromMinutes) {
      return new IntervalWindow(at(fromMinutes), SESSIONS_GAP);
    }

    private static IntervalWindow sessionWindow(int fromMinutes, int toMinutes) {
      return intervalWindow(fromMinutes, toMinutes);
    }
  }

  /** Tests for specialized SessionsAggregator in {@link Aggregators}. */
  public static class SessionsAggregatorTest
      extends AbstractSessionsTest<TreeMap<IntervalWindow, MutablePair<Instant, Integer>>> {

    public SessionsAggregatorTest() {
      super(Sessions.withGapDuration(SESSIONS_GAP));
    }

    @Override
    TreeMap<IntervalWindow, MutablePair<Instant, Integer>> accOf(
        KV<IntervalWindow, MutablePair<Instant, Integer>>... entries) {
      return new TreeMap<>(mapOf(entries));
    }
  }

  /** Tests for MergingWindowedAggregator in {@link Aggregators}. */
  public static class MergingWindowedAggregatorTest
      extends AbstractSessionsTest<Map<IntervalWindow, MutablePair<Instant, Integer>>> {

    public MergingWindowedAggregatorTest() {
      super(new CustomSessions<>());
    }

    @Override
    Map<IntervalWindow, MutablePair<Instant, Integer>> accOf(
        KV<IntervalWindow, MutablePair<Instant, Integer>>... entries) {
      return mapOf(entries);
    }

    /** Wrapper around {@link Sessions} to test the MergingWindowedAggregator. */
    private static class CustomSessions<T> extends WindowFn<T, IntervalWindow> {
      private final Sessions sessions = Sessions.withGapDuration(SESSIONS_GAP);

      @Override
      public Collection<IntervalWindow> assignWindows(WindowFn<T, IntervalWindow>.AssignContext c) {
        return sessions.assignWindows((WindowFn.AssignContext) c);
      }

      @Override
      public void mergeWindows(WindowFn<T, IntervalWindow>.MergeContext c) throws Exception {
        sessions.mergeWindows((WindowFn<Object, IntervalWindow>.MergeContext) c);
      }

      @Override
      public boolean isCompatible(WindowFn<?, ?> other) {
        return sessions.isCompatible(other);
      }

      @Override
      public Coder<IntervalWindow> windowCoder() {
        return sessions.windowCoder();
      }

      @Override
      public WindowMappingFn<IntervalWindow> getDefaultWindowMappingFn() {
        return sessions.getDefaultWindowMappingFn();
      }
    }
  }

  private static IntervalWindow intervalWindow(int fromMinutes, int toMinutes) {
    return new IntervalWindow(at(fromMinutes), at(toMinutes));
  }

  private static Instant at(int minutes) {
    return NOW.plus(standardMinutes(minutes));
  }

  private static Matcher<Map<IntervalWindow, MutablePair<Instant, Integer>>> equalsToMap(
      KV<IntervalWindow, MutablePair<Instant, Integer>>... entries) {
    return equalTo(mapOf(entries));
  }

  private static Map<IntervalWindow, MutablePair<Instant, Integer>> mapOf(
      KV<IntervalWindow, MutablePair<Instant, Integer>>... entries) {
    return Arrays.asList(entries).stream().collect(Collectors.toMap(KV::getKey, KV::getValue));
  }

  private static MutablePair<Instant, Integer> pair(Instant ts, int value) {
    return new MutablePair<>(ts, value);
  }

  private static <AccT>
      Aggregator<WindowedValue<Integer>, AccT, Collection<WindowedValue<Integer>>> windowedAgg(
          WindowFn<?, ?> windowFn) {
    Encoder<Integer> intEnc = EncoderHelpers.encoderOf(Integer.class);
    Encoder<BoundedWindow> windowEnc = encoderFor((Coder) IntervalWindow.getCoder());
    Encoder<WindowedValue<Integer>> outputEnc = windowedValueEncoder(intEnc, windowEnc);

    WindowingStrategy<?, ?> windowing =
        WindowingStrategy.of(windowFn).withTimestampCombiner(TimestampCombiner.LATEST);

    Aggregator<WindowedValue<Integer>, ?, Collection<WindowedValue<Integer>>> agg =
        Aggregators.windowedValue(
            new SimpleSum(), WindowedValue::getValue, windowing, windowEnc, intEnc, outputEnc);
    return (Aggregator) agg;
  }

  private static class SimpleSum extends Combine.CombineFn<Integer, Integer, Integer> {

    @Override
    public Integer createAccumulator() {
      return 0;
    }

    @Override
    public Integer addInput(Integer acc, Integer input) {
      return acc + input;
    }

    @Override
    public Integer mergeAccumulators(Iterable<Integer> accs) {
      return Streams.stream(accs.iterator()).reduce((a, b) -> a + b).orElseGet(() -> 0);
    }

    @Override
    public Integer extractOutput(Integer acc) {
      return acc;
    }
  }
}
