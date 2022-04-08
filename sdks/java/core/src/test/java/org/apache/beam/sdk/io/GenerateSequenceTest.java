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
package org.apache.beam.sdk.io;

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GenerateSequence}. */
@RunWith(JUnit4.class)
public class GenerateSequenceTest {
  public static void addCountingAsserts(PCollection<Long> input, long start, long end) {
    // Count == numElements
    PAssert.thatSingleton(input.apply("Count", Count.globally())).isEqualTo(end - start);
    // Unique count == numElements
    PAssert.thatSingleton(input.apply(Distinct.create()).apply("UniqueCount", Count.globally()))
        .isEqualTo(end - start);
    // Min == start
    PAssert.thatSingleton(input.apply("Min", Min.globally())).isEqualTo(start);
    // Max == end-1
    PAssert.thatSingleton(input.apply("Max", Max.globally())).isEqualTo(end - 1);
  }

  @Rule public TestPipeline p = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void testBoundedInput() {
    long numElements = 1000;
    PCollection<Long> input = p.apply(GenerateSequence.from(0).to(numElements));

    addCountingAsserts(input, 0, numElements);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testEmptyBoundedInput() {
    PCollection<Long> input = p.apply(GenerateSequence.from(0).to(0));

    PAssert.that(input).empty();
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testEmptyBoundedInputSubrange() {
    PCollection<Long> input = p.apply(GenerateSequence.from(42).to(42));

    PAssert.that(input).empty();
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testBoundedInputSubrange() {
    long start = 10;
    long end = 1000;
    PCollection<Long> input = p.apply(GenerateSequence.from(start).to(end));

    addCountingAsserts(input, start, end);
    p.run();
  }

  @Test
  public void testBoundedDisplayData() {
    PTransform<?, ?> input = GenerateSequence.from(0).to(1234);
    DisplayData displayData = DisplayData.from(input);
    assertThat(displayData, hasDisplayItem("from", 0));
    assertThat(displayData, hasDisplayItem("to", 1234));
  }

  @Test
  public void testBoundedDisplayDataSubrange() {
    PTransform<?, ?> input = GenerateSequence.from(12).to(1234);
    DisplayData displayData = DisplayData.from(input);
    assertThat(displayData, hasDisplayItem("from", 12));
    assertThat(displayData, hasDisplayItem("to", 1234));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testUnboundedInputRate() {
    long numElements = 5000;

    long elemsPerPeriod = 10L;
    Duration periodLength = Duration.millis(8);
    PCollection<Long> input =
        p.apply(GenerateSequence.from(0).to(numElements).withRate(elemsPerPeriod, periodLength));

    addCountingAsserts(input, 0, numElements);
    long expectedRuntimeMillis = (periodLength.getMillis() * numElements) / elemsPerPeriod;
    Instant startTime = Instant.now();
    p.run();
    Instant endTime = Instant.now();
    assertThat(endTime.isAfter(startTime.plus(Duration.millis(expectedRuntimeMillis))), is(true));
  }

  private static class ElementValueDiff extends DoFn<Long, Long> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      c.output(c.element() - c.timestamp().getMillis());
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testUnboundedInputTimestamps() {
    long numElements = 1000;

    PCollection<Long> input =
        p.apply(GenerateSequence.from(0).to(numElements).withTimestampFn(new ValueAsTimestampFn()));
    addCountingAsserts(input, 0, numElements);

    PCollection<Long> diffs =
        input
            .apply("TimestampDiff", ParDo.of(new ElementValueDiff()))
            .apply("DistinctTimestamps", Distinct.create());
    // This assert also confirms that diffs only has one unique value.
    PAssert.thatSingleton(diffs).isEqualTo(0L);

    p.run();
  }

  @Test
  public void testUnboundedDisplayData() {
    Duration maxReadTime = Duration.standardHours(5);
    SerializableFunction<Long, Instant> timestampFn = input -> Instant.now();

    PTransform<?, ?> input =
        GenerateSequence.from(0).to(1234).withMaxReadTime(maxReadTime).withTimestampFn(timestampFn);

    DisplayData displayData = DisplayData.from(input);

    assertThat(displayData, hasDisplayItem("maxReadTime", maxReadTime));
    assertThat(displayData, hasDisplayItem("timestampFn", timestampFn.getClass()));
  }

  @Test
  public void testBuildExternal() {
    @SuppressWarnings("NonCanonicalType")
    GenerateSequence.External.ExternalConfiguration externalConfig =
        new AutoValue_GenerateSequence.External.ExternalConfiguration();
    externalConfig.setStart(42L);
    externalConfig.setStop(43L);
    externalConfig.setElementsPerPeriod(1L);
    externalConfig.setMaxReadTime(2L);
    externalConfig.setPeriod(3L);

    AutoValue_GenerateSequence.Builder builder = new AutoValue_GenerateSequence.Builder();
    GenerateSequence object = builder.buildExternal(externalConfig);
    assertThat(object.getFrom(), is(42L));
    assertThat(object.getTo(), is(43L));
    assertThat(object.getElementsPerPeriod(), is(1L));
    assertThat(object.getMaxReadTime(), is(Duration.millis(2L)));
    assertThat(object.getPeriod(), is(Duration.millis(3L)));
  }

  /**
   * A timestamp function that uses the given value as the timestamp. Because the input values will
   * not wrap, this function is non-decreasing and meets the timestamp function criteria laid out in
   * {@link GenerateSequence#withTimestampFn(SerializableFunction)}.
   */
  private static class ValueAsTimestampFn implements SerializableFunction<Long, Instant> {
    @Override
    public Instant apply(Long input) {
      return new Instant(input);
    }
  }
}
