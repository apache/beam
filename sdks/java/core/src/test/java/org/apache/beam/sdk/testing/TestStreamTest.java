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

package org.apache.beam.sdk.testing;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;

/**
 * Tests for {@link TestStream}.
 */
@RunWith(JUnit4.class)
public class TestStreamTest implements Serializable {
  @Test
  @Category(NeedsRunner.class)
  public void testLateDataAccumulating() {
    Instant instant = new Instant(0);
    TestStream<Integer> source = TestStream.create(VarIntCoder.of())
        .addElements(TimestampedValue.of(1, instant),
            TimestampedValue.of(2, instant),
            TimestampedValue.of(3, instant))
        .advanceWatermarkTo(instant.plus(Duration.standardMinutes(6)))
        // These elements are late but within the allowed lateness
        .addElements(TimestampedValue.of(4, instant), TimestampedValue.of(5, instant))
        .advanceWatermarkTo(instant.plus(Duration.standardMinutes(20)))
        // These elements are droppably late
        .addElements(TimestampedValue.of(-1, instant),
            TimestampedValue.of(-2, instant),
            TimestampedValue.of(-3, instant))
        .advanceWatermarkToInfinity();

    TestPipeline p = TestPipeline.create();
    PCollection<Integer> windowed = p
        .apply(source)
        .apply(Window.<Integer>into(FixedWindows.of(Duration.standardMinutes(5))).triggering(
            AfterWatermark.pastEndOfWindow()
                .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
                    .plusDelayOf(Duration.standardMinutes(2)))
                .withLateFirings(AfterPane.elementCountAtLeast(1)))
            .accumulatingFiredPanes()
            .withAllowedLateness(Duration.standardMinutes(5), ClosingBehavior.FIRE_ALWAYS));
    PCollection<Integer> triggered = windowed.apply(WithKeys.<Integer, Integer>of(1))
        .apply(GroupByKey.<Integer, Integer>create())
        .apply(Values.<Iterable<Integer>>create())
        .apply(Flatten.<Integer>iterables());
    PCollection<Long> count = windowed.apply(Count.<Integer>globally().withoutDefaults());
    PCollection<Integer> sum = windowed.apply(Sum.integersGlobally().withoutDefaults());

    IntervalWindow window = new IntervalWindow(instant, instant.plus(Duration.standardMinutes(5L)));
    PAssert.that(triggered)
        .inFinalPane(window)
        .containsInAnyOrder(1, 2, 3, 4, 5);
    PAssert.that(triggered)
        .inOnTimePane(window)
        .containsInAnyOrder(1, 2, 3);
    PAssert.that(count)
        .inWindow(window)
        .satisfies(new SerializableFunction<Iterable<Long>, Void>() {
          @Override
          public Void apply(Iterable<Long> input) {
            for (Long count : input) {
              assertThat(count, allOf(greaterThanOrEqualTo(3L), lessThanOrEqualTo(5L)));
            }
            return null;
          }
        });
    PAssert.that(sum)
        .inWindow(window)
        .satisfies(new SerializableFunction<Iterable<Integer>, Void>() {
          @Override
          public Void apply(Iterable<Integer> input) {
            for (Integer sum : input) {
              assertThat(sum, allOf(greaterThanOrEqualTo(6), lessThanOrEqualTo(15)));
            }
            return null;
          }
        });

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testProcessingTimeTrigger() {
    TestStream<Long> source = TestStream.create(VarLongCoder.of())
        .addElements(TimestampedValue.of(1L, new Instant(1000L)),
            TimestampedValue.of(2L, new Instant(2000L)))
        .advanceProcessingTime(Duration.standardMinutes(12))
        .addElements(TimestampedValue.of(3L, new Instant(3000L)))
        .advanceProcessingTime(Duration.standardMinutes(6))
        .advanceWatermarkToInfinity();

    TestPipeline p = TestPipeline.create();
    PCollection<Long> sum = p.apply(source)
        .apply(Window.<Long>triggering(AfterWatermark.pastEndOfWindow()
            .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
                .plusDelayOf(Duration.standardMinutes(5)))).accumulatingFiredPanes()
            .withAllowedLateness(Duration.ZERO))
        .apply(Sum.longsGlobally());

    PAssert.that(sum).inEarlyGlobalWindowPanes().containsInAnyOrder(3L, 6L);

    p.run();
  }

  @Test
  public void testEncodeDecode() throws Exception {
    TestStream.Event<Integer> elems =
        TestStream.ElementEvent.add(
            TimestampedValue.of(1, new Instant()),
            TimestampedValue.of(-10, new Instant()),
            TimestampedValue.of(Integer.MAX_VALUE, new Instant()));
    TestStream.Event<Integer> wm = TestStream.WatermarkEvent.advanceTo(new Instant(100));
    TestStream.Event<Integer> procTime =
        TestStream.ProcessingTimeEvent.advanceBy(Duration.millis(90548));

    TestStream.EventCoder<Integer> coder = TestStream.EventCoder.of(VarIntCoder.of());

    CoderProperties.coderSerializable(coder);
    CoderProperties.coderDecodeEncodeEqual(coder, elems);
    CoderProperties.coderDecodeEncodeEqual(coder, wm);
    CoderProperties.coderDecodeEncodeEqual(coder, procTime);
  }
}
