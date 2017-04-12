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
package org.apache.beam.runners.spark.translation.streaming;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.runners.spark.PipelineRule;
import org.apache.beam.runners.spark.ReuseSparkContextRule;
import org.apache.beam.runners.spark.io.CreateStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.PAssert;
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
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


/**
 * A test suite to test Spark runner implementation of triggers and panes.
 *
 * <p>Since Spark is a micro-batch engine, and will process any test-sized input
 * within the same (first) batch, it is important to make sure inputs are ingested across
 * micro-batches using {@link org.apache.spark.streaming.dstream.QueueInputDStream}.
 * This test suite uses {@link CreateStream} to construct such
 * {@link org.apache.spark.streaming.dstream.QueueInputDStream} and advance the system's WMs.
 * //TODO: add synchronized/processing time trigger.
 */
public class CreateStreamTest implements Serializable {

  @Rule
  public final transient PipelineRule pipelineRule = PipelineRule.streaming();
  @Rule
  public final transient ReuseSparkContextRule noContextResue = ReuseSparkContextRule.no();
  @Rule
  public final transient ExpectedException thrown = ExpectedException.none();

  @Test
  public void testLateDataAccumulating() throws IOException {
    Pipeline p = pipelineRule.createPipeline();
    Instant instant = new Instant(0);
    CreateStream<Integer> source =
        CreateStream.of(VarIntCoder.of(), pipelineRule.batchDuration())
            .emptyBatch()
            .advanceWatermarkForNextBatch(instant.plus(Duration.standardMinutes(6)))
            .nextBatch(
                TimestampedValue.of(1, instant),
                TimestampedValue.of(2, instant),
                TimestampedValue.of(3, instant))
            .advanceWatermarkForNextBatch(instant.plus(Duration.standardMinutes(20)))
            // These elements are late but within the allowed lateness
            .nextBatch(
                TimestampedValue.of(4, instant),
                TimestampedValue.of(5, instant))
            // These elements are droppably late
            .advanceNextBatchWatermarkToInfinity()
            .nextBatch(
                TimestampedValue.of(-1, instant),
                TimestampedValue.of(-2, instant),
                TimestampedValue.of(-3, instant));

    PCollection<Integer> windowed = p
        .apply(source)
        .apply(Window.<Integer>into(FixedWindows.of(Duration.standardMinutes(5))).triggering(
            AfterWatermark.pastEndOfWindow()
                .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
                    .plusDelayOf(Duration.standardMinutes(2)))
                .withLateFirings(AfterPane.elementCountAtLeast(1)))
            .accumulatingFiredPanes()
            .withAllowedLateness(Duration.standardMinutes(5), Window.ClosingBehavior.FIRE_ALWAYS));
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
  public void testDiscardingMode() throws IOException {
    Pipeline p = pipelineRule.createPipeline();
    CreateStream<String> source =
        CreateStream.of(StringUtf8Coder.of(), pipelineRule.batchDuration())
            .nextBatch(
                TimestampedValue.of("firstPane", new Instant(100)),
                TimestampedValue.of("alsoFirstPane", new Instant(200)))
            .advanceWatermarkForNextBatch(new Instant(1001L))
            .nextBatch(
                TimestampedValue.of("onTimePane", new Instant(500)))
            .advanceNextBatchWatermarkToInfinity()
            .nextBatch(
                TimestampedValue.of("finalLatePane", new Instant(750)),
                TimestampedValue.of("alsoFinalLatePane", new Instant(250)));

    FixedWindows windowFn = FixedWindows.of(Duration.millis(1000L));
    Duration allowedLateness = Duration.millis(5000L);
    PCollection<String> values =
        p.apply(source)
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
  public void testFirstElementLate() throws IOException {
    Pipeline p = pipelineRule.createPipeline();
    Instant lateElementTimestamp = new Instant(-1_000_000);
    CreateStream<String> source =
        CreateStream.of(StringUtf8Coder.of(), pipelineRule.batchDuration())
            .emptyBatch()
            .advanceWatermarkForNextBatch(new Instant(0))
            .nextBatch(
                TimestampedValue.of("late", lateElementTimestamp),
                TimestampedValue.of("onTime", new Instant(100)))
            .advanceNextBatchWatermarkToInfinity();

    FixedWindows windowFn = FixedWindows.of(Duration.millis(1000L));
    Duration allowedLateness = Duration.millis(5000L);
    PCollection<String> values = p.apply(source)
        .apply(Window.<String>into(windowFn).triggering(DefaultTrigger.of())
            .discardingFiredPanes()
            .withAllowedLateness(allowedLateness))
        .apply(WithKeys.<Integer, String>of(1))
        .apply(GroupByKey.<Integer, String>create())
        .apply(Values.<Iterable<String>>create())
        .apply(Flatten.<String>iterables());

    PAssert.that(values)
        .inWindow(windowFn.assignWindow(lateElementTimestamp))
        .empty();
    PAssert.that(values)
        .inWindow(windowFn.assignWindow(new Instant(100)))
        .containsInAnyOrder("onTime");

    p.run();
  }

  @Test
  public void testElementsAtAlmostPositiveInfinity() throws IOException {
    Pipeline p = pipelineRule.createPipeline();
    Instant endOfGlobalWindow = GlobalWindow.INSTANCE.maxTimestamp();
    CreateStream<String> source =
        CreateStream.of(StringUtf8Coder.of(), pipelineRule.batchDuration())
            .nextBatch(
                TimestampedValue.of("foo", endOfGlobalWindow),
                TimestampedValue.of("bar", endOfGlobalWindow))
            .advanceNextBatchWatermarkToInfinity();

    FixedWindows windows = FixedWindows.of(Duration.standardHours(6));
    PCollection<String> windowedValues = p.apply(source)
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
  public void testMultipleStreams() throws IOException {
    Pipeline p = pipelineRule.createPipeline();
    CreateStream<String> source =
        CreateStream.of(StringUtf8Coder.of(), pipelineRule.batchDuration())
            .nextBatch("foo", "bar")
            .advanceNextBatchWatermarkToInfinity();
    CreateStream<Integer> other =
        CreateStream.of(VarIntCoder.of(), pipelineRule.batchDuration())
            .nextBatch(1, 2, 3, 4)
            .advanceNextBatchWatermarkToInfinity();

    PCollection<String> createStrings =
        p.apply("CreateStrings", source)
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
  public void testFlattenedWithWatermarkHold() throws IOException {
    Pipeline p = pipelineRule.createPipeline();
    Instant instant = new Instant(0);
    CreateStream<Integer> source1 =
        CreateStream.of(VarIntCoder.of(), pipelineRule.batchDuration())
            .emptyBatch()
            .advanceWatermarkForNextBatch(instant.plus(Duration.standardMinutes(5)))
            .nextBatch(
                TimestampedValue.of(1, instant),
                TimestampedValue.of(2, instant),
                TimestampedValue.of(3, instant))
            .advanceWatermarkForNextBatch(instant.plus(Duration.standardMinutes(10)));
    CreateStream<Integer> source2 =
        CreateStream.of(VarIntCoder.of(), pipelineRule.batchDuration())
            .emptyBatch()
            .advanceWatermarkForNextBatch(instant.plus(Duration.standardMinutes(1)))
            .nextBatch(
                TimestampedValue.of(4, instant))
            .advanceWatermarkForNextBatch(instant.plus(Duration.standardMinutes(2)))
            .nextBatch(
                TimestampedValue.of(5, instant))
            .advanceWatermarkForNextBatch(instant.plus(Duration.standardMinutes(5)))
            .emptyBatch()
            .advanceNextBatchWatermarkToInfinity();

    PCollection<Integer> windowed1 = p
        .apply(source1)
        .apply(Window.<Integer>into(FixedWindows.of(Duration.standardMinutes(5)))
            .triggering(AfterWatermark.pastEndOfWindow())
            .accumulatingFiredPanes()
            .withAllowedLateness(Duration.ZERO));
    PCollection<Integer> windowed2 = p
        .apply(source2)
        .apply(Window.<Integer>into(FixedWindows.of(Duration.standardMinutes(5)))
            .triggering(AfterWatermark.pastEndOfWindow())
            .accumulatingFiredPanes()
            .withAllowedLateness(Duration.ZERO));

    PCollectionList<Integer> pCollectionList = PCollectionList.of(windowed1).and(windowed2);
    PCollection<Integer> flattened = pCollectionList.apply(Flatten.<Integer>pCollections());
    PCollection<Integer> triggered = flattened
        .apply(WithKeys.<Integer, Integer>of(1))
        .apply(GroupByKey.<Integer, Integer>create())
        .apply(Values.<Iterable<Integer>>create())
        .apply(Flatten.<Integer>iterables());

    IntervalWindow window = new IntervalWindow(instant, instant.plus(Duration.standardMinutes(5L)));
    PAssert.that(triggered).inOnTimePane(window).containsInAnyOrder(1, 2, 3, 4, 5);

    p.run();
  }

  @Test
  public void testElementAtPositiveInfinityThrows() {
    CreateStream<Integer> source =
        CreateStream.of(VarIntCoder.of(), pipelineRule.batchDuration())
            .nextBatch(TimestampedValue.of(-1, BoundedWindow.TIMESTAMP_MAX_VALUE.minus(1L)))
            .advanceNextBatchWatermarkToInfinity();
    thrown.expect(IllegalArgumentException.class);
    source.nextBatch(TimestampedValue.of(1, BoundedWindow.TIMESTAMP_MAX_VALUE));
  }

  @Test
  public void testAdvanceWatermarkNonMonotonicThrows() {
    CreateStream<Integer> source =
        CreateStream.of(VarIntCoder.of(), pipelineRule.batchDuration())
            .advanceWatermarkForNextBatch(new Instant(0L));
    thrown.expect(IllegalArgumentException.class);
    source
        .advanceWatermarkForNextBatch(new Instant(-1L))
        .advanceNextBatchWatermarkToInfinity();
  }

  @Test
  public void testAdvanceWatermarkEqualToPositiveInfinityThrows() {
    CreateStream<Integer> source =
        CreateStream.of(VarIntCoder.of(), pipelineRule.batchDuration())
            .advanceWatermarkForNextBatch(BoundedWindow.TIMESTAMP_MAX_VALUE.minus(1L));
    thrown.expect(IllegalArgumentException.class);
    source.advanceWatermarkForNextBatch(BoundedWindow.TIMESTAMP_MAX_VALUE);
  }
}
