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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.StreamingTest;
import org.apache.beam.runners.spark.TestSparkPipelineOptions;
import org.apache.beam.runners.spark.io.CreateStream;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
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
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

/**
 * A test suite to test Spark runner implementation of triggers and panes.
 *
 * <p>Since Spark is a micro-batch engine, and will process any test-sized input within the same
 * (first) batch, it is important to make sure inputs are ingested across micro-batches using {@link
 * org.apache.spark.streaming.dstream.QueueInputDStream}. This test suite uses {@link CreateStream}
 * to construct such {@link org.apache.spark.streaming.dstream.QueueInputDStream} and advance the
 * system's WMs. //TODO: add synchronized/processing time trigger.
 */
@Category(StreamingTest.class)
public class CreateStreamTest implements Serializable {

  @Rule public final transient TestPipeline p = TestPipeline.fromOptions(streamingOptions());
  @Rule public final transient ExpectedException thrown = ExpectedException.none();

  @Test
  public void testLateDataAccumulating() throws IOException {
    Instant instant = new Instant(0);
    CreateStream<Integer> source =
        CreateStream.of(VarIntCoder.of(), batchDuration())
            .emptyBatch()
            .advanceWatermarkForNextBatch(instant.plus(Duration.standardMinutes(6)))
            .nextBatch(
                TimestampedValue.of(1, instant),
                TimestampedValue.of(2, instant),
                TimestampedValue.of(3, instant))
            .advanceWatermarkForNextBatch(instant.plus(Duration.standardMinutes(20)))
            // These elements are late but within the allowed lateness
            .nextBatch(TimestampedValue.of(4, instant), TimestampedValue.of(5, instant))
            // These elements are droppably late
            .advanceNextBatchWatermarkToInfinity()
            .nextBatch(
                TimestampedValue.of(-1, instant),
                TimestampedValue.of(-2, instant),
                TimestampedValue.of(-3, instant));

    PCollection<Integer> windowed =
        p.apply(source)
            .apply(
                Window.<Integer>into(FixedWindows.of(Duration.standardMinutes(5)))
                    .triggering(
                        AfterWatermark.pastEndOfWindow()
                            .withEarlyFirings(
                                AfterProcessingTime.pastFirstElementInPane()
                                    .plusDelayOf(Duration.standardMinutes(2)))
                            .withLateFirings(AfterPane.elementCountAtLeast(1)))
                    .accumulatingFiredPanes()
                    .withAllowedLateness(
                        Duration.standardMinutes(5), Window.ClosingBehavior.FIRE_ALWAYS));
    PCollection<Integer> triggered =
        windowed
            .apply(WithKeys.of(1))
            .apply(GroupByKey.create())
            .apply(Values.create())
            .apply(Flatten.iterables());
    PCollection<Long> count =
        windowed.apply(Combine.globally(Count.<Integer>combineFn()).withoutDefaults());
    PCollection<Integer> sum = windowed.apply(Sum.integersGlobally().withoutDefaults());

    IntervalWindow window = new IntervalWindow(instant, instant.plus(Duration.standardMinutes(5L)));
    PAssert.that(triggered).inFinalPane(window).containsInAnyOrder(1, 2, 3, 4, 5);
    PAssert.that(triggered).inOnTimePane(window).containsInAnyOrder(1, 2, 3);
    PAssert.that(count)
        .inWindow(window)
        .satisfies(
            input -> {
              for (Long count1 : input) {
                assertThat(count1, allOf(greaterThanOrEqualTo(3L), lessThanOrEqualTo(5L)));
              }
              return null;
            });
    PAssert.that(sum)
        .inWindow(window)
        .satisfies(
            input -> {
              for (Integer sum1 : input) {
                assertThat(sum1, allOf(greaterThanOrEqualTo(6), lessThanOrEqualTo(15)));
              }
              return null;
            });

    p.run();
  }

  @Test
  public void testDiscardingMode() throws IOException {
    CreateStream<String> source =
        CreateStream.of(StringUtf8Coder.of(), batchDuration())
            .nextBatch(
                TimestampedValue.of("firstPane", new Instant(100)),
                TimestampedValue.of("alsoFirstPane", new Instant(200)))
            .advanceWatermarkForNextBatch(new Instant(1001L))
            .nextBatch(TimestampedValue.of("onTimePane", new Instant(500)))
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
            .apply(WithKeys.of(1))
            .apply(GroupByKey.create())
            .apply(Values.create())
            .apply(Flatten.iterables());

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
    Instant lateElementTimestamp = new Instant(-1_000_000);
    CreateStream<String> source =
        CreateStream.of(StringUtf8Coder.of(), batchDuration())
            .emptyBatch()
            .advanceWatermarkForNextBatch(new Instant(0))
            .emptyBatch()
            .nextBatch(
                TimestampedValue.of("late", lateElementTimestamp),
                TimestampedValue.of("onTime", new Instant(100)))
            .advanceNextBatchWatermarkToInfinity();

    FixedWindows windowFn = FixedWindows.of(Duration.millis(1000L));
    Duration allowedLateness = Duration.millis(5000L);
    PCollection<String> values =
        p.apply(source)
            .apply(
                Window.<String>into(windowFn)
                    .triggering(DefaultTrigger.of())
                    .discardingFiredPanes()
                    .withAllowedLateness(allowedLateness))
            .apply(WithKeys.of(1))
            .apply(GroupByKey.create())
            .apply(Values.create())
            .apply(Flatten.iterables());

    PAssert.that(values).inWindow(windowFn.assignWindow(lateElementTimestamp)).empty();
    PAssert.that(values)
        .inWindow(windowFn.assignWindow(new Instant(100)))
        .containsInAnyOrder("onTime");

    p.run();
  }

  @Test
  public void testElementsAtAlmostPositiveInfinity() throws IOException {
    Instant endOfGlobalWindow = GlobalWindow.INSTANCE.maxTimestamp();
    CreateStream<String> source =
        CreateStream.of(StringUtf8Coder.of(), batchDuration())
            .nextBatch(
                TimestampedValue.of("foo", endOfGlobalWindow),
                TimestampedValue.of("bar", endOfGlobalWindow))
            .advanceNextBatchWatermarkToInfinity();

    FixedWindows windows = FixedWindows.of(Duration.standardHours(6));
    PCollection<String> windowedValues =
        p.apply(source)
            .apply(Window.into(windows))
            .apply(WithKeys.of(1))
            .apply(GroupByKey.create())
            .apply(Values.create())
            .apply(Flatten.iterables());

    PAssert.that(windowedValues)
        .inWindow(windows.assignWindow(GlobalWindow.INSTANCE.maxTimestamp()))
        .containsInAnyOrder("foo", "bar");
    p.run();
  }

  @Test
  public void testMultipleStreams() throws IOException {
    CreateStream<String> source =
        CreateStream.of(StringUtf8Coder.of(), batchDuration())
            .nextBatch("foo", "bar")
            .advanceNextBatchWatermarkToInfinity();
    CreateStream<Integer> other =
        CreateStream.of(VarIntCoder.of(), batchDuration())
            .nextBatch(1, 2, 3, 4)
            .advanceNextBatchWatermarkToInfinity();

    PCollection<String> createStrings =
        p.apply("CreateStrings", source)
            .apply(
                "WindowStrings",
                Window.<String>configure()
                    .triggering(AfterPane.elementCountAtLeast(2))
                    .withAllowedLateness(Duration.ZERO)
                    .accumulatingFiredPanes());
    PAssert.that(createStrings).containsInAnyOrder("foo", "bar");

    PCollection<Integer> createInts =
        p.apply("CreateInts", other)
            .apply(
                "WindowInts",
                Window.<Integer>configure()
                    .triggering(AfterPane.elementCountAtLeast(4))
                    .withAllowedLateness(Duration.ZERO)
                    .accumulatingFiredPanes());
    PAssert.that(createInts).containsInAnyOrder(1, 2, 3, 4);

    p.run();
  }

  @Test
  public void testFlattenedWithWatermarkHold() throws IOException {
    Instant instant = new Instant(0);
    CreateStream<Integer> source1 =
        CreateStream.of(VarIntCoder.of(), batchDuration())
            .emptyBatch()
            .advanceWatermarkForNextBatch(instant.plus(Duration.standardMinutes(5)))
            .nextBatch(
                TimestampedValue.of(1, instant),
                TimestampedValue.of(2, instant),
                TimestampedValue.of(3, instant))
            .advanceWatermarkForNextBatch(instant.plus(Duration.standardMinutes(10)));
    CreateStream<Integer> source2 =
        CreateStream.of(VarIntCoder.of(), batchDuration())
            .emptyBatch()
            .advanceWatermarkForNextBatch(instant.plus(Duration.standardMinutes(1)))
            .nextBatch(TimestampedValue.of(4, instant))
            .advanceWatermarkForNextBatch(instant.plus(Duration.standardMinutes(2)))
            .nextBatch(TimestampedValue.of(5, instant))
            .advanceWatermarkForNextBatch(instant.plus(Duration.standardMinutes(5)))
            .emptyBatch()
            .advanceNextBatchWatermarkToInfinity();

    PCollection<Integer> windowed1 =
        p.apply("CreateStream1", source1)
            .apply(
                "Window1",
                Window.<Integer>into(FixedWindows.of(Duration.standardMinutes(5)))
                    .triggering(AfterWatermark.pastEndOfWindow())
                    .accumulatingFiredPanes()
                    .withAllowedLateness(Duration.ZERO));
    PCollection<Integer> windowed2 =
        p.apply("CreateStream2", source2)
            .apply(
                "Window2",
                Window.<Integer>into(FixedWindows.of(Duration.standardMinutes(5)))
                    .triggering(AfterWatermark.pastEndOfWindow())
                    .accumulatingFiredPanes()
                    .withAllowedLateness(Duration.ZERO));

    PCollectionList<Integer> pCollectionList = PCollectionList.of(windowed1).and(windowed2);
    PCollection<Integer> flattened = pCollectionList.apply(Flatten.pCollections());
    PCollection<Integer> triggered =
        flattened
            .apply(WithKeys.of(1))
            .apply(GroupByKey.create())
            .apply(Values.create())
            .apply(Flatten.iterables());

    IntervalWindow window = new IntervalWindow(instant, instant.plus(Duration.standardMinutes(5L)));
    PAssert.that(triggered).inOnTimePane(window).containsInAnyOrder(1, 2, 3, 4, 5);

    p.run();
  }

  /**
   * Test multiple output {@link ParDo} in streaming pipelines. This is currently needed as a test
   * for https://issues.apache.org/jira/browse/BEAM-2029 since {@link
   * org.apache.beam.sdk.testing.ValidatesRunner} tests do not currently run for Spark runner in
   * streaming mode.
   */
  @Test
  public void testMultiOutputParDo() throws IOException {
    Instant instant = new Instant(0);
    CreateStream<Integer> source1 =
        CreateStream.of(VarIntCoder.of(), batchDuration())
            .emptyBatch()
            .advanceWatermarkForNextBatch(instant.plus(Duration.standardMinutes(5)))
            .nextBatch(
                TimestampedValue.of(1, instant),
                TimestampedValue.of(2, instant),
                TimestampedValue.of(3, instant))
            .advanceNextBatchWatermarkToInfinity();

    PCollection<Integer> inputs = p.apply(source1);

    final TupleTag<Integer> mainTag = new TupleTag<>();
    final TupleTag<Integer> additionalTag = new TupleTag<>();

    PCollectionTuple outputs =
        inputs.apply(
            ParDo.of(
                    new DoFn<Integer, Integer>() {

                      @SuppressWarnings("unused")
                      @ProcessElement
                      public void process(ProcessContext context) {
                        Integer element = context.element();
                        context.output(element);
                        context.output(additionalTag, element + 1);
                      }
                    })
                .withOutputTags(mainTag, TupleTagList.of(additionalTag)));

    PCollection<Integer> output1 = outputs.get(mainTag).setCoder(VarIntCoder.of());
    PCollection<Integer> output2 = outputs.get(additionalTag).setCoder(VarIntCoder.of());

    PAssert.that(output1).containsInAnyOrder(1, 2, 3);
    PAssert.that(output2).containsInAnyOrder(2, 3, 4);

    p.run();
  }

  /**
   * Test that {@link ParDo} aligns both setup and teardown calls in streaming pipelines. See
   * https://issues.apache.org/jira/browse/BEAM-6859.
   */
  @Test
  public void testParDoCallsSetupAndTeardown() {
    Instant instant = new Instant(0);

    p.apply(
            CreateStream.of(VarIntCoder.of(), batchDuration())
                .emptyBatch()
                .advanceWatermarkForNextBatch(instant.plus(Duration.standardMinutes(5)))
                .nextBatch(
                    TimestampedValue.of(1, instant),
                    TimestampedValue.of(2, instant),
                    TimestampedValue.of(3, instant))
                .advanceNextBatchWatermarkToInfinity())
        .apply(ParDo.of(new LifecycleDoFn()));

    p.run();

    assertThat(
        "Function should have been torn down",
        LifecycleDoFn.teardownCalls.intValue(),
        is(equalTo(LifecycleDoFn.setupCalls.intValue())));
  }

  @Test
  public void testElementAtPositiveInfinityThrows() {
    CreateStream<Integer> source =
        CreateStream.of(VarIntCoder.of(), batchDuration())
            .nextBatch(
                TimestampedValue.of(
                    -1, BoundedWindow.TIMESTAMP_MAX_VALUE.minus(Duration.millis(1L))))
            .advanceNextBatchWatermarkToInfinity();
    thrown.expect(IllegalArgumentException.class);
    source.nextBatch(TimestampedValue.of(1, BoundedWindow.TIMESTAMP_MAX_VALUE));
  }

  @Test
  public void testAdvanceWatermarkNonMonotonicThrows() {
    CreateStream<Integer> source =
        CreateStream.of(VarIntCoder.of(), batchDuration())
            .advanceWatermarkForNextBatch(new Instant(0L));
    thrown.expect(IllegalArgumentException.class);
    source.advanceWatermarkForNextBatch(new Instant(-1L)).advanceNextBatchWatermarkToInfinity();
  }

  @Test
  public void testAdvanceWatermarkEqualToPositiveInfinityThrows() {
    CreateStream<Integer> source =
        CreateStream.of(VarIntCoder.of(), batchDuration())
            .advanceWatermarkForNextBatch(
                BoundedWindow.TIMESTAMP_MAX_VALUE.minus(Duration.millis(1L)));
    thrown.expect(IllegalArgumentException.class);
    source.advanceWatermarkForNextBatch(BoundedWindow.TIMESTAMP_MAX_VALUE);
  }

  @Test
  public void testInStreamingModeCountByKey() throws Exception {
    Instant instant = new Instant(0);

    CreateStream<KV<Integer, Long>> kvSource =
        CreateStream.of(KvCoder.of(VarIntCoder.of(), VarLongCoder.of()), batchDuration())
            .emptyBatch()
            .advanceWatermarkForNextBatch(instant)
            .nextBatch(
                TimestampedValue.of(KV.of(1, 100L), instant.plus(Duration.standardSeconds(3L))),
                TimestampedValue.of(KV.of(1, 300L), instant.plus(Duration.standardSeconds(4L))))
            .advanceWatermarkForNextBatch(instant.plus(Duration.standardSeconds(7L)))
            .nextBatch(
                TimestampedValue.of(KV.of(1, 400L), instant.plus(Duration.standardSeconds(8L))))
            .advanceNextBatchWatermarkToInfinity();

    PCollection<KV<Integer, Long>> output =
        p.apply("create kv Source", kvSource)
            .apply(
                "window input",
                Window.<KV<Integer, Long>>into(FixedWindows.of(Duration.standardSeconds(3L)))
                    .withAllowedLateness(Duration.ZERO))
            .apply(Count.perKey());

    PAssert.that("Wrong count value ", output)
        .satisfies(
            (SerializableFunction<Iterable<KV<Integer, Long>>, Void>)
                input -> {
                  for (KV<Integer, Long> element : input) {
                    if (element.getKey() == 1) {
                      Long countValue = element.getValue();
                      assertNotEquals("Count Value is 0 !!!", 0L, countValue.longValue());
                    } else {
                      fail("Unknown key in the output PCollection");
                    }
                  }
                  return null;
                });
    p.run();
  }

  private Duration batchDuration() {
    return Duration.millis(p.getOptions().as(SparkPipelineOptions.class).getBatchIntervalMillis());
  }

  private static class LifecycleDoFn extends DoFn<Integer, Integer> {
    static AtomicInteger setupCalls = new AtomicInteger(0);
    static AtomicInteger teardownCalls = new AtomicInteger(0);

    @Setup
    public void setup() {
      setupCalls.incrementAndGet();
    }

    @Teardown
    public void teardown() {
      teardownCalls.incrementAndGet();
    }

    @SuppressWarnings("unused")
    @ProcessElement
    public void process(ProcessContext context) {
      Integer element = context.element();
      context.output(element);
    }
  }

  static PipelineOptions streamingOptions() {
    PipelineOptions options = TestPipeline.testingPipelineOptions();
    options.as(TestSparkPipelineOptions.class).setStreaming(true);
    return options;
  }
}
