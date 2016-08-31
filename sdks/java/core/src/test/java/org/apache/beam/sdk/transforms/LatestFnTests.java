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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.TimestampedValue;

import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for {@link Latest.LatestFn}.
 * */
@RunWith(JUnit4.class)
public class LatestFnTests {
  @Rule
  public final ExpectedException thrown = ExpectedException.none();

  private final Latest.LatestFn<Long> fn = new Latest.LatestFn<>();
  private final Instant baseTimestamp = Instant.now();

  @Test
  public void testDefaultValue() {
    assertThat(fn.defaultValue(), nullValue());
  }

  @Test
  public void testCreateAccumulator() {
    assertEquals(TimestampedValue.<Long>atMinimumTimestamp(null), fn.createAccumulator());
  }

  @Test
  public void testAddInputInitialAdd() {
    TimestampedValue<Long> input = timestamped(baseTimestamp);
    assertEquals(input, fn.addInput(fn.createAccumulator(), input));
  }

  @Test
  public void testAddInputMinTimestamp() {
    TimestampedValue<Long> input = timestamped(BoundedWindow.TIMESTAMP_MIN_VALUE);
    assertEquals(input, fn.addInput(fn.createAccumulator(), input));
  }

  @Test
  public void testAddInputEarlierValue() {
    TimestampedValue<Long> accum = timestamped(baseTimestamp);
    TimestampedValue<Long> input = timestamped(baseTimestamp.minus(10));
    assertEquals(accum, fn.addInput(accum, input));
  }

  @Test
  public void testAddInputLaterValue() {
    TimestampedValue<Long> accum = timestamped(baseTimestamp);
    TimestampedValue<Long> input = timestamped(baseTimestamp.plus(10));

    assertEquals(input, fn.addInput(accum, input));
  }

  @Test
  public void testAddInputSameTimestamp() {
    TimestampedValue<Long> accum = timestamped(baseTimestamp);
    TimestampedValue<Long> input = timestamped(baseTimestamp.plus(10));

    assertThat("Latest for values with the same timestamp is chosen arbitrarily",
        fn.addInput(accum, input), isOneOf(accum, input));
  }

  @Test
  public void testAddInputNullAccumulator() {
    TimestampedValue<Long> input = timestamped(baseTimestamp);
    thrown.expect(NullPointerException.class);
    fn.addInput(null, input);
  }

  @Test
  public void testAddInputNullInput() {
    TimestampedValue<Long> accum = timestamped(baseTimestamp);
    thrown.expect(NullPointerException.class);
    fn.addInput(accum, null);
  }

  @Test
  public void testAddInputNullValue() {
    TimestampedValue<Long> accum = timestamped(baseTimestamp);
    TimestampedValue<Long> input = TimestampedValue.of(null, baseTimestamp.plus(10));

    assertEquals("Null values are allowed", input, fn.addInput(accum, input));
  }

  @Test
  public void testMergeAccumulatorsMultipleValues() {
    TimestampedValue<Long> latest = timestamped(baseTimestamp.plus(100));
    Iterable<TimestampedValue<Long>> accums = Lists.newArrayList(
        timestamped(baseTimestamp),
        latest,
        timestamped(baseTimestamp.minus(10))
    );

    assertEquals(latest, fn.mergeAccumulators(accums));
  }

  @Test
  public void testMergeAccumulatorsSingleValue() {
    TimestampedValue<Long> accum = timestamped(baseTimestamp);
    assertEquals(accum, fn.mergeAccumulators(Lists.newArrayList(accum)));
  }

  @Test
  public void testMergeAccumulatorsEmptyIterable() {
    ArrayList<TimestampedValue<Long>> emptyAccums = Lists.newArrayList();
    assertEquals(TimestampedValue.atMinimumTimestamp(null), fn.mergeAccumulators(emptyAccums));
  }

  @Test
  public void testMergeAccumulatorsDefaultAccumulator() {
    TimestampedValue<Long> accum = timestamped(baseTimestamp);
    TimestampedValue<Long> defaultAccum = fn.createAccumulator();
    assertEquals(accum, fn.mergeAccumulators(Lists.newArrayList(accum, defaultAccum)));
  }

  @Test
  public void testMergeAccumulatorsAllDefaultAccumulators() {
    TimestampedValue<Long> defaultAccum = fn.createAccumulator();
    assertEquals(defaultAccum, fn.mergeAccumulators(
        Lists.newArrayList(defaultAccum, defaultAccum)));
  }

  @Test
  public void testMergeAccumulatorsNullIterable() {
    thrown.expect(NullPointerException.class);
    fn.mergeAccumulators(null);
  }

  @Test
  public void testExtractOutput() {
    TimestampedValue<Long> accum = timestamped(baseTimestamp);
    assertEquals(accum.getValue(), fn.extractOutput(accum));
  }

  @Test
  public void testExtractOutputDefaultAggregator() {
    TimestampedValue<Long> accum = fn.createAccumulator();
    assertThat(fn.extractOutput(accum), nullValue());
  }

  @Test
  public void testExtractOutputNullValue() {
    TimestampedValue<Long> accum = TimestampedValue.of(null, baseTimestamp);
    assertEquals(null, fn.extractOutput(accum));
  }

  @Test
  public void testAggregator() throws Exception {
    final TimestampedValue<Long> first = timestamped(baseTimestamp);
    final TimestampedValue<Long> latest = timestamped(baseTimestamp.plus(100));
    final TimestampedValue<Long> oldest = timestamped(baseTimestamp.minus(10));

    LatestAggregatorsFn<Long> doFn = new LatestAggregatorsFn<>(oldest.getValue());
    DoFnTester<Long, Long> harness = DoFnTester.of(doFn);
    for (TimestampedValue<Long> element : Arrays.asList(first, latest, oldest)) {
      harness.processTimestampedElement(element);
    }

    assertEquals(latest.getValue(), harness.getAggregatorValue(doFn.allValuesAgg));
    assertEquals(oldest.getValue(), harness.getAggregatorValue(doFn.specialValueAgg));
    assertThat(harness.getAggregatorValue(doFn.noValuesAgg), nullValue());
  }

  @Test
  public void testDefaultCoderHandlesNull() throws CannotProvideCoderException {
    Latest.LatestFn<Long> fn = new Latest.LatestFn<>();

    CoderRegistry registry = new CoderRegistry();
    TimestampedValue.TimestampedValueCoder<Long> inputCoder =
        TimestampedValue.TimestampedValueCoder.of(VarLongCoder.of());

    assertThat("Default output coder should handle null values",
        fn.getDefaultOutputCoder(registry, inputCoder), instanceOf(NullableCoder.class));
    assertThat("Default accumulator coder should handle null values",
        fn.getAccumulatorCoder(registry, inputCoder), instanceOf(NullableCoder.class));
  }

  static class LatestAggregatorsFn<T> extends DoFn<T, T> {
    private final T specialValue;
    LatestAggregatorsFn(T specialValue) {
      this.specialValue = specialValue;
    }

    Aggregator<TimestampedValue<T>, T> allValuesAgg =
        createAggregator("allValues", new Latest.LatestFn<T>());

    Aggregator<TimestampedValue<T>, T> specialValueAgg =
        createAggregator("oneValue", new Latest.LatestFn<T>());

    Aggregator<TimestampedValue<T>, T> noValuesAgg =
        createAggregator("noValues", new Latest.LatestFn<T>());

    @ProcessElement
    public void processElement(ProcessContext c) {
      TimestampedValue<T> val = TimestampedValue.of(c.element(), c.timestamp());
      allValuesAgg.addValue(val);
      if (Objects.equals(c.element(), specialValue)) {
        specialValueAgg.addValue(val);
      }
    }
  }

  private static final AtomicLong uniqueLong = new AtomicLong();
  /** Helper method to easily create a timestamped value. */
  private static TimestampedValue<Long> timestamped(Instant timestamp) {
    return TimestampedValue.of(uniqueLong.incrementAndGet(), timestamp);
  }
}
