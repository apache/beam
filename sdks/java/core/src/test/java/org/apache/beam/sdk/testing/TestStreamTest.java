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

import java.io.Serializable;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.testing.TestStream.Builder;
import org.apache.beam.sdk.transforms.Combine;
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
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Never;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link TestStream}.
 */
@RunWith(JUnit4.class)
public class TestStreamTest implements Serializable {
  @Rule public transient TestPipeline p = TestPipeline.create();
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @Test
  @Category({NeedsRunner.class, UsesTestStream.class})
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
    PCollection<Long> count =
        windowed.apply(Combine.globally(Count.<Integer>combineFn()).withoutDefaults());
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
  @Category({NeedsRunner.class, UsesTestStream.class})
  public void testProcessingTimeTrigger() {
    TestStream<Long> source = TestStream.create(VarLongCoder.of())
        .addElements(TimestampedValue.of(1L, new Instant(1000L)),
            TimestampedValue.of(2L, new Instant(2000L)))
        .advanceProcessingTime(Duration.standardMinutes(12))
        .addElements(TimestampedValue.of(3L, new Instant(3000L)))
        .advanceProcessingTime(Duration.standardMinutes(6))
        .advanceWatermarkToInfinity();

    PCollection<Long> sum = p.apply(source)
        .apply(Window.<Long>configure().triggering(AfterWatermark.pastEndOfWindow()
            .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
                .plusDelayOf(Duration.standardMinutes(5)))).accumulatingFiredPanes()
            .withAllowedLateness(Duration.ZERO))
        .apply(Sum.longsGlobally());

    PAssert.that(sum).inEarlyGlobalWindowPanes().containsInAnyOrder(3L, 6L);

    p.run();
  }

  @Test
  @Category({NeedsRunner.class, UsesTestStream.class})
  public void testDiscardingMode() {
    TestStream<String> stream =
        TestStream.create(StringUtf8Coder.of())
            .advanceWatermarkTo(new Instant(0))
            .addElements(
                TimestampedValue.of("firstPane", new Instant(100)),
                TimestampedValue.of("alsoFirstPane", new Instant(200)))
            .addElements(TimestampedValue.of("onTimePane", new Instant(500)))
            .advanceWatermarkTo(new Instant(1001L))
            .addElements(
                TimestampedValue.of("finalLatePane", new Instant(750)),
                TimestampedValue.of("alsoFinalLatePane", new Instant(250)))
            .advanceWatermarkToInfinity();

    FixedWindows windowFn = FixedWindows.of(Duration.millis(1000L));
    Duration allowedLateness = Duration.millis(5000L);
    PCollection<String> values =
        p.apply(stream)
            .apply(
                Window.<String>into(windowFn)
                    .triggering(
                        AfterWatermark.pastEndOfWindow()
                            .withEarlyFirings(AfterPane.elementCountAtLeast(2))
                            .withLateFirings(Never.ever()))
                    .discardingFiredPanes()
                    .withAllowedLateness(allowedLateness))
            .apply(WithKeys.<Integer, String>of(1))
            .apply(GroupByKey.<Integer, String>create())
            .apply(Values.<Iterable<String>>create())
            .apply(Flatten.<String>iterables());

    IntervalWindow window = windowFn.assignWindow(new Instant(100));
    PAssert.that(values)
        .inWindow(window)
        .containsInAnyOrder(
            "firstPane", "alsoFirstPane", "onTimePane", "finalLatePane", "alsoFinalLatePane");
    PAssert.that(values)
        .inCombinedNonLatePanes(window)
        .containsInAnyOrder("firstPane", "alsoFirstPane", "onTimePane");
    PAssert.that(values).inOnTimePane(window).containsInAnyOrder("onTimePane");
    PAssert.that(values)
        .inFinalPane(window)
        .containsInAnyOrder("finalLatePane", "alsoFinalLatePane");

    p.run();
  }

  @Test
  @Category({NeedsRunner.class, UsesTestStream.class})
  public void testFirstElementLate() {
    Instant lateElementTimestamp = new Instant(-1_000_000);
    TestStream<String> stream =
        TestStream.create(StringUtf8Coder.of())
            .advanceWatermarkTo(new Instant(0))
            .addElements(TimestampedValue.of("late", lateElementTimestamp))
            .addElements(TimestampedValue.of("onTime", new Instant(100)))
            .advanceWatermarkToInfinity();

    FixedWindows windowFn = FixedWindows.of(Duration.millis(1000L));
    Duration allowedLateness = Duration.millis(5000L);
    PCollection<String> values = p.apply(stream)
        .apply(Window.<String>into(windowFn).triggering(DefaultTrigger.of())
            .discardingFiredPanes()
            .withAllowedLateness(allowedLateness))
        .apply(WithKeys.<Integer, String>of(1))
        .apply(GroupByKey.<Integer, String>create())
        .apply(Values.<Iterable<String>>create())
        .apply(Flatten.<String>iterables());

    PAssert.that(values).inWindow(windowFn.assignWindow(lateElementTimestamp)).empty();
    PAssert.that(values)
        .inWindow(windowFn.assignWindow(new Instant(100)))
        .containsInAnyOrder("onTime");

    p.run();
  }

  @Test
  @Category({NeedsRunner.class, UsesTestStream.class})
  public void testElementsAtAlmostPositiveInfinity() {
    Instant endOfGlobalWindow = GlobalWindow.INSTANCE.maxTimestamp();
    TestStream<String> stream = TestStream.create(StringUtf8Coder.of())
        .addElements(TimestampedValue.of("foo", endOfGlobalWindow),
            TimestampedValue.of("bar", endOfGlobalWindow))
        .advanceWatermarkToInfinity();

    FixedWindows windows = FixedWindows.of(Duration.standardHours(6));
    PCollection<String> windowedValues = p.apply(stream)
        .apply(Window.<String>into(windows))
        .apply(WithKeys.<Integer, String>of(1))
        .apply(GroupByKey.<Integer, String>create())
        .apply(Values.<Iterable<String>>create())
        .apply(Flatten.<String>iterables());

    PAssert.that(windowedValues)
        .inWindow(windows.assignWindow(GlobalWindow.INSTANCE.maxTimestamp()))
        .containsInAnyOrder("foo", "bar");
    p.run();
  }

  @Test
  @Category({NeedsRunner.class, UsesTestStream.class})
  public void testMultipleStreams() {
    TestStream<String> stream = TestStream.create(StringUtf8Coder.of())
        .addElements("foo", "bar")
        .advanceWatermarkToInfinity();

    TestStream<Integer> other =
        TestStream.create(VarIntCoder.of()).addElements(1, 2, 3, 4).advanceWatermarkToInfinity();

    PCollection<String> createStrings =
        p.apply("CreateStrings", stream)
            .apply("WindowStrings",
                Window.<String>configure().triggering(AfterPane.elementCountAtLeast(2))
                    .withAllowedLateness(Duration.ZERO)
                    .accumulatingFiredPanes());
    PAssert.that(createStrings).containsInAnyOrder("foo", "bar");
    PCollection<Integer> createInts =
        p.apply("CreateInts", other)
            .apply("WindowInts",
                Window.<Integer>configure().triggering(AfterPane.elementCountAtLeast(4))
                    .withAllowedLateness(Duration.ZERO)
                    .accumulatingFiredPanes());
    PAssert.that(createInts).containsInAnyOrder(1, 2, 3, 4);

    p.run();
  }

  @Test
  public void testElementAtPositiveInfinityThrows() {
    Builder<Integer> stream =
        TestStream.create(VarIntCoder.of())
            .addElements(TimestampedValue.of(-1, BoundedWindow.TIMESTAMP_MAX_VALUE.minus(1L)));
    thrown.expect(IllegalArgumentException.class);
    stream.addElements(TimestampedValue.of(1, BoundedWindow.TIMESTAMP_MAX_VALUE));
  }

  @Test
  public void testAdvanceWatermarkNonMonotonicThrows() {
    Builder<Integer> stream =
        TestStream.create(VarIntCoder.of())
            .advanceWatermarkTo(new Instant(0L));
    thrown.expect(IllegalArgumentException.class);
    stream.advanceWatermarkTo(new Instant(-1L));
  }

  @Test
  public void testAdvanceWatermarkEqualToPositiveInfinityThrows() {
    Builder<Integer> stream =
        TestStream.create(VarIntCoder.of())
            .advanceWatermarkTo(BoundedWindow.TIMESTAMP_MAX_VALUE.minus(1L));
    thrown.expect(IllegalArgumentException.class);
    stream.advanceWatermarkTo(BoundedWindow.TIMESTAMP_MAX_VALUE);
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

  @Test
  public void testCoderIsSerializableWithWellKnownCoderType() {
    CoderProperties.coderSerializable(TestStream.EventCoder.of(GlobalWindow.Coder.INSTANCE));
  }
}
