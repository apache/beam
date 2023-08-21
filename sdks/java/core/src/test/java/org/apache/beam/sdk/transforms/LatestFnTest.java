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

import java.util.ArrayList;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link Latest.LatestFn}. */
@RunWith(JUnit4.class)
public class LatestFnTest {
  private static final Instant INSTANT = new Instant(100);
  private static final long VALUE = 100 * INSTANT.getMillis();

  private static final TimestampedValue<Long> TV = TimestampedValue.of(VALUE, INSTANT);
  private static final TimestampedValue<Long> TV_MINUS_TEN =
      TimestampedValue.of(VALUE - 10, INSTANT.minus(Duration.millis(10)));
  private static final TimestampedValue<Long> TV_PLUS_TEN =
      TimestampedValue.of(VALUE + 10, INSTANT.plus(Duration.millis(10)));

  @Rule public final ExpectedException thrown = ExpectedException.none();

  private final Latest.LatestFn<Long> fn = new Latest.LatestFn<>();
  private final Instant baseTimestamp = Instant.now();

  @Test
  public void testDefaultValue() {
    assertThat(fn.defaultValue(), nullValue());
  }

  @Test
  public void testCreateAccumulator() {
    assertEquals(TimestampedValue.atMinimumTimestamp(null), fn.createAccumulator());
  }

  @Test
  public void testAddInputInitialAdd() {
    TimestampedValue<Long> input = TV;
    assertEquals(input, fn.addInput(fn.createAccumulator(), input));
  }

  @Test
  public void testAddInputMinTimestamp() {
    TimestampedValue<Long> input = TimestampedValue.atMinimumTimestamp(1234L);
    assertEquals(input, fn.addInput(fn.createAccumulator(), input));
  }

  @Test
  public void testAddInputEarlierValue() {
    assertEquals(TV, fn.addInput(TV, TV_MINUS_TEN));
  }

  @Test
  public void testAddInputLaterValue() {
    assertEquals(TV_PLUS_TEN, fn.addInput(TV, TV_PLUS_TEN));
  }

  @Test
  public void testAddInputSameTimestamp() {
    TimestampedValue<Long> accum = TimestampedValue.of(100L, INSTANT);
    TimestampedValue<Long> input = TimestampedValue.of(200L, INSTANT);

    assertThat(
        "Latest for values with the same timestamp is chosen arbitrarily",
        fn.addInput(accum, input),
        isOneOf(accum, input));
  }

  @Test
  public void testAddInputNullAccumulator() {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("accumulator");
    fn.addInput(null, TV);
  }

  @Test
  public void testAddInputNullInput() {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("input");
    fn.addInput(TV, null);
  }

  @Test
  public void testAddInputNullValue() {
    TimestampedValue<Long> input = TimestampedValue.of(null, INSTANT.plus(Duration.millis(10)));
    assertEquals("Null values are allowed", input, fn.addInput(TV, input));
  }

  @Test
  public void testMergeAccumulatorsMultipleValues() {
    Iterable<TimestampedValue<Long>> accums = Lists.newArrayList(TV, TV_PLUS_TEN, TV_MINUS_TEN);

    assertEquals(TV_PLUS_TEN, fn.mergeAccumulators(accums));
  }

  @Test
  public void testMergeAccumulatorsSingleValue() {
    assertEquals(TV, fn.mergeAccumulators(Lists.newArrayList(TV)));
  }

  @Test
  public void testMergeAccumulatorsEmptyIterable() {
    ArrayList<TimestampedValue<Long>> emptyAccums = Lists.newArrayList();
    assertEquals(TimestampedValue.atMinimumTimestamp(null), fn.mergeAccumulators(emptyAccums));
  }

  @Test
  public void testMergeAccumulatorsDefaultAccumulator() {
    TimestampedValue<Long> defaultAccum = fn.createAccumulator();
    assertEquals(TV, fn.mergeAccumulators(Lists.newArrayList(TV, defaultAccum)));
  }

  @Test
  public void testMergeAccumulatorsAllDefaultAccumulators() {
    TimestampedValue<Long> defaultAccum = fn.createAccumulator();
    assertEquals(
        defaultAccum, fn.mergeAccumulators(Lists.newArrayList(defaultAccum, defaultAccum)));
  }

  @Test
  public void testMergeAccumulatorsNullIterable() {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("accumulators");
    fn.mergeAccumulators(null);
  }

  @Test
  public void testExtractOutput() {
    assertEquals(TV.getValue(), fn.extractOutput(TV));
  }

  @Test
  public void testExtractOutputDefaultAccumulator() {
    TimestampedValue<Long> accum = fn.createAccumulator();
    assertThat(fn.extractOutput(accum), nullValue());
  }

  @Test
  public void testExtractOutputNullValue() {
    TimestampedValue<Long> accum = TimestampedValue.of(null, baseTimestamp);
    assertEquals(null, fn.extractOutput(accum));
  }

  @Test
  public void testDefaultCoderHandlesNull() throws CannotProvideCoderException {
    Latest.LatestFn<Long> fn = new Latest.LatestFn<>();

    CoderRegistry registry = CoderRegistry.createDefault();
    TimestampedValue.TimestampedValueCoder<Long> inputCoder =
        TimestampedValue.TimestampedValueCoder.of(VarLongCoder.of());

    assertThat(
        "Default output coder should handle null values",
        fn.getDefaultOutputCoder(registry, inputCoder),
        instanceOf(NullableCoder.class));
    assertThat(
        "Default accumulator coder should handle null values",
        fn.getAccumulatorCoder(registry, inputCoder),
        instanceOf(NullableCoder.class));
  }
}
