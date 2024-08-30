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

import static org.apache.beam.sdk.TestUtils.KvMatcher.isKv;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThrows;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderProviders;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.testing.LargeKeys;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.UsesProcessingTimeTimers;
import org.apache.beam.sdk.testing.UsesTestStreamWithProcessingTime;
import org.apache.beam.sdk.testing.UsesTimersInParDo;
import org.apache.beam.sdk.testing.UsesUnboundedPCollections;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Streams;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Matcher;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for GroupByKey. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "unchecked",
  "unused"
})
@RunWith(Enclosed.class)
public class GroupByKeyTest implements Serializable {
  /** Shared test base class with setup/teardown helpers. */
  public abstract static class SharedTestBase {
    @Rule public transient TestPipeline p = TestPipeline.create();
  }

  /** Tests validating basic {@link GroupByKey} scenarios. */
  @RunWith(JUnit4.class)
  public static class BasicTests extends SharedTestBase implements Serializable {
    @Test
    @Category(ValidatesRunner.class)
    public void testGroupByKey() {
      List<KV<String, Integer>> ungroupedPairs =
          Arrays.asList(
              KV.of("k1", 3),
              KV.of("k5", Integer.MAX_VALUE),
              KV.of("k5", Integer.MIN_VALUE),
              KV.of("k2", 66),
              KV.of("k1", 4),
              KV.of("k2", -33),
              KV.of("k3", 0));

      PCollection<KV<String, Integer>> input =
          p.apply(
              Create.of(ungroupedPairs)
                  .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

      PCollection<KV<String, Iterable<Integer>>> output = input.apply(GroupByKey.create());

      SerializableFunction<Iterable<KV<String, Iterable<Integer>>>, Void> checker =
          containsKvs(
              kv("k1", 3, 4),
              kv("k5", Integer.MIN_VALUE, Integer.MAX_VALUE),
              kv("k2", 66, -33),
              kv("k3", 0));
      PAssert.that(output).satisfies(checker);
      PAssert.that(output).inWindow(GlobalWindow.INSTANCE).satisfies(checker);

      p.run();
    }

    @Test
    @Category(ValidatesRunner.class)
    public void testGroupByKeyEmpty() {
      List<KV<String, Integer>> ungroupedPairs = Arrays.asList();

      PCollection<KV<String, Integer>> input =
          p.apply(
              Create.of(ungroupedPairs)
                  .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

      PCollection<KV<String, Iterable<Integer>>> output = input.apply(GroupByKey.create());

      PAssert.that(output).empty();

      p.run();
    }

    /**
     * Tests that when a processing time timers comes in after a window is expired it does not cause
     * a spurious output.
     */
    @Test
    @Category({ValidatesRunner.class, UsesTestStreamWithProcessingTime.class})
    public void testCombiningAccumulatingProcessingTime() throws Exception {
      PCollection<Integer> triggeredSums =
          p.apply(
                  TestStream.create(VarIntCoder.of())
                      .advanceWatermarkTo(new Instant(0))
                      .addElements(
                          TimestampedValue.of(2, new Instant(2)),
                          TimestampedValue.of(5, new Instant(5)))
                      .advanceWatermarkTo(new Instant(100))
                      .advanceProcessingTime(Duration.millis(10))
                      .advanceWatermarkToInfinity())
              .apply(
                  Window.<Integer>into(FixedWindows.of(Duration.millis(100)))
                      .withTimestampCombiner(TimestampCombiner.EARLIEST)
                      .accumulatingFiredPanes()
                      .withAllowedLateness(Duration.ZERO)
                      .triggering(
                          Repeatedly.forever(
                              AfterProcessingTime.pastFirstElementInPane()
                                  .plusDelayOf(Duration.millis(10)))))
              .apply(Sum.integersGlobally().withoutDefaults());

      PAssert.that(triggeredSums).containsInAnyOrder(7);

      p.run();
    }

    /**
     * Tests that data from a processing time trigger flows through subsequent GroupByKey
     * transforms. To test this with TestStream, we check that it arrives in an early pane,
     * demonstrating that the watermark did not cause the output.
     */
    @Test
    @Category({ValidatesRunner.class, UsesTestStreamWithProcessingTime.class})
    public void testAfterProcessingTimeContinuationTriggerEarly() throws Exception {
      final long triggerMillis = 1; // Setting of the processing time trigger
      final long advanceMillis = 10; // How far we advance time for the first GBK to trigger
      final long waitMillis = 500; // How far we advance time for the second GBK to trigger

      PCollection<Integer> triggeredSums =
          p.apply(
                  TestStream.create(VarIntCoder.of())
                      .advanceWatermarkTo(new Instant(0))
                      .addElements(42)
                      .advanceProcessingTime(Duration.millis(advanceMillis))
                      .advanceProcessingTime(Duration.millis(waitMillis))
                      .advanceWatermarkToInfinity())
              .apply(
                  Window.<Integer>configure()
                      .triggering(
                          Repeatedly.forever(
                              AfterProcessingTime.pastFirstElementInPane()
                                  .plusDelayOf(Duration.millis(triggerMillis))))
                      .accumulatingFiredPanes()
                      .withOnTimeBehavior(Window.OnTimeBehavior.FIRE_IF_NON_EMPTY)
                      .withAllowedLateness(Duration.millis(0)))
              .apply("Triggered sum", Sum.integersGlobally().withoutDefaults())
              .apply("Second Triggered sum", Sum.integersGlobally().withoutDefaults());

      PAssert.that(triggeredSums).inEarlyGlobalWindowPanes().containsInAnyOrder(42);

      p.run();
    }

    /**
     * Tests that data from a processing time trigger flows through subsequent GroupByKey
     * transforms. This version does not depend on {@link TestStream} with processing time, because
     * many runners do not support it.
     *
     * <p>Since a delay in processing time is fundamental to this test, it unfortunately must have a
     * real-time sleep. Currently the sleep is sub-second so it is acceptable delay for a unit test
     * suite.
     */
    @Test
    @Category({
      ValidatesRunner.class,
      UsesTimersInParDo.class,
      UsesProcessingTimeTimers.class,
      UsesUnboundedPCollections.class
    })
    public void testAfterProcessingTimeContinuationTriggerUsingState() throws Exception {
      final long triggerMillis = 1;
      final long waitMillis = 500;

      PCollection<Integer> triggeredSums =
          p.apply(
                  GenerateSequence.from(0)
                      .to(1)) // forces unbounded so delay cannot be fast-forwarded
              .apply(WithKeys.of("dummy key"))
              .apply(
                  "output then delay",
                  ParDo.of(
                      new DoFn<KV<String, Long>, Integer>() {
                        private static final String DELAY_TIMER = "delay";

                        @TimerId(DELAY_TIMER)
                        private final TimerSpec delayTimerSpec =
                            TimerSpecs.timer(TimeDomain.EVENT_TIME);

                        @ProcessElement
                        public void process(
                            @Timestamp Instant timestamp,
                            BoundedWindow window,
                            OutputReceiver<Integer> out,
                            @TimerId(DELAY_TIMER) Timer delayTimer) {
                          out.output(42);

                          // wait a little bit while stopping downstream from firing the window
                          delayTimer.set(window.maxTimestamp().minus(Duration.millis(10)));
                        }

                        @OnTimer(DELAY_TIMER)
                        public void onDelay(@Timestamp Instant timestamp)
                            throws InterruptedException {
                          // noop, just here to force the pipeline to sleep so downstream GBK will
                          // trigger
                          Thread.sleep(waitMillis);
                        }
                      }))
              .apply(
                  Window.<Integer>configure()
                      .triggering(
                          Repeatedly.forever(
                              AfterProcessingTime.pastFirstElementInPane()
                                  .plusDelayOf(Duration.millis(triggerMillis))))
                      .accumulatingFiredPanes()
                      .withAllowedLateness(Duration.millis(0)))
              .apply("Triggered sum", Sum.integersGlobally().withoutDefaults())
              .apply("Second Triggered sum", Sum.integersGlobally().withoutDefaults());

      PAssert.that(triggeredSums).inEarlyGlobalWindowPanes().containsInAnyOrder(42);

      p.run();
    }

    @Test
    public void testGroupByKeyNonDeterministic() throws Exception {
      List<KV<Map<String, String>, Integer>> ungroupedPairs = Arrays.asList();

      PCollection<KV<Map<String, String>, Integer>> input =
          p.apply(
              Create.of(ungroupedPairs)
                  .withCoder(
                      KvCoder.of(
                          MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()),
                          BigEndianIntegerCoder.of())));

      assertThrows(
          "must be deterministic",
          IllegalStateException.class,
          () -> input.apply(GroupByKey.create()));
    }

    @Test
    public void testGroupByKeyOutputCoderUnmodifiedAfterApplyAndBeforePipelineRun()
        throws Exception {
      List<KV<String, Integer>> ungroupedPairs = Arrays.asList();

      PCollection<KV<String, Integer>> input =
          p.apply(
              Create.of(ungroupedPairs)
                  .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

      // Apply with a known good coder
      PCollection<KV<String, Iterable<Integer>>> output = input.apply(GroupByKey.create());

      // Change the output to have a different coder that doesn't match the input coder types
      output.setCoder(
          KvCoder.of(
              SerializableCoder.of(String.class), IterableCoder.of(BigEndianIntegerCoder.of())));
      assertThrows(
          "the GroupByKey requires its output coder", IllegalStateException.class, () -> p.run());
    }

    // AfterPane.elementCountAtLeast(1) is not OK
    @Test
    public void testGroupByKeyFinishingTriggerRejected() {
      PCollection<KV<String, String>> input =
          p.apply(Create.of(KV.of("hello", "goodbye")))
              .apply(
                  Window.<KV<String, String>>configure()
                      .discardingFiredPanes()
                      .triggering(AfterPane.elementCountAtLeast(1)));

      assertThrows(
          "Unsafe trigger", IllegalArgumentException.class, () -> input.apply(GroupByKey.create()));
    }

    // AfterWatermark.pastEndOfWindow() is OK with 0 allowed lateness
    @Test
    public void testGroupByKeyFinishingEndOfWindowTriggerOk() {
      PCollection<KV<String, String>> input =
          p.apply(Create.of(KV.of("hello", "goodbye")))
              .apply(
                  Window.<KV<String, String>>configure()
                      .discardingFiredPanes()
                      .triggering(AfterWatermark.pastEndOfWindow())
                      .withAllowedLateness(Duration.ZERO));

      // OK
      input.apply(GroupByKey.create());
    }

    // AfterWatermark.pastEndOfWindow().withEarlyFirings() is OK with 0 allowed lateness
    @Test
    public void testGroupByKeyFinishingEndOfWindowEarlyFiringsTriggerOk() {
      PCollection<KV<String, String>> input =
          p.apply(Create.of(KV.of("hello", "goodbye")))
              .apply(
                  Window.<KV<String, String>>configure()
                      .discardingFiredPanes()
                      .triggering(
                          AfterWatermark.pastEndOfWindow()
                              .withEarlyFirings(AfterPane.elementCountAtLeast(1)))
                      .withAllowedLateness(Duration.ZERO));

      // OK
      input.apply(GroupByKey.create());
    }

    // AfterWatermark.pastEndOfWindow() is not OK with > 0 allowed lateness
    @Test
    public void testGroupByKeyFinishingEndOfWindowTriggerNotOk() {
      PCollection<KV<String, String>> input =
          p.apply(Create.of(KV.of("hello", "goodbye")))
              .apply(
                  Window.<KV<String, String>>configure()
                      .discardingFiredPanes()
                      .triggering(AfterWatermark.pastEndOfWindow())
                      .withAllowedLateness(Duration.millis(10)));

      assertThrows(
          "Unsafe trigger", IllegalArgumentException.class, () -> input.apply(GroupByKey.create()));
    }

    // AfterWatermark.pastEndOfWindow().withEarlyFirings() is not OK with > 0 allowed lateness
    @Test
    public void testGroupByKeyFinishingEndOfWindowEarlyFiringsTriggerNotOk() {
      PCollection<KV<String, String>> input =
          p.apply(Create.of(KV.of("hello", "goodbye")))
              .apply(
                  Window.<KV<String, String>>configure()
                      .discardingFiredPanes()
                      .triggering(
                          AfterWatermark.pastEndOfWindow()
                              .withEarlyFirings(AfterPane.elementCountAtLeast(1)))
                      .withAllowedLateness(Duration.millis(10)));

      assertThrows(
          "Unsafe trigger", IllegalArgumentException.class, () -> input.apply(GroupByKey.create()));
    }

    // AfterWatermark.pastEndOfWindow().withLateFirings() is always OK
    @Test
    public void testGroupByKeyEndOfWindowLateFiringsOk() {
      PCollection<KV<String, String>> input =
          p.apply(Create.of(KV.of("hello", "goodbye")))
              .apply(
                  Window.<KV<String, String>>configure()
                      .discardingFiredPanes()
                      .triggering(
                          AfterWatermark.pastEndOfWindow()
                              .withLateFirings(AfterPane.elementCountAtLeast(1)))
                      .withAllowedLateness(Duration.millis(10)));

      // OK
      input.apply(GroupByKey.create());
    }

    @Test
    @Category(NeedsRunner.class)
    public void testRemerge() {
      List<KV<String, Integer>> ungroupedPairs = Arrays.asList();

      PCollection<KV<String, Integer>> input =
          p.apply(
                  Create.of(ungroupedPairs)
                      .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())))
              .apply(Window.into(Sessions.withGapDuration(Duration.standardMinutes(1))));

      PCollection<KV<String, Iterable<Iterable<Integer>>>> middle =
          input
              .apply("GroupByKey", GroupByKey.create())
              .apply("Remerge", Window.remerge())
              .apply("GroupByKeyAgain", GroupByKey.create())
              .apply("RemergeAgain", Window.remerge());

      p.run();

      Assert.assertTrue(
          middle
              .getWindowingStrategy()
              .getWindowFn()
              .isCompatible(Sessions.withGapDuration(Duration.standardMinutes(1))));
    }

    @Test
    public void testGroupByKeyDirectUnbounded() {
      PCollection<KV<String, Integer>> input =
          p.apply(
              new PTransform<PBegin, PCollection<KV<String, Integer>>>() {
                @Override
                public PCollection<KV<String, Integer>> expand(PBegin input) {
                  return PCollection.createPrimitiveOutputInternal(
                      input.getPipeline(),
                      WindowingStrategy.globalDefault(),
                      PCollection.IsBounded.UNBOUNDED,
                      KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()));
                }
              });

      assertThrows(
          "GroupByKey cannot be applied to non-bounded PCollection in the GlobalWindow without "
              + "a trigger. Use a Window.into or Window.triggering transform prior to GroupByKey.",
          IllegalStateException.class,
          () -> input.apply("GroupByKey", GroupByKey.create()));
    }

    /**
     * Tests that when two elements are combined via a GroupByKey their output timestamp agrees with
     * the windowing function customized to actually be the same as the default, the earlier of the
     * two values.
     */
    @Test
    @Category(ValidatesRunner.class)
    public void testTimestampCombinerEarliest() {
      p.apply(
              Create.timestamped(
                  TimestampedValue.of(KV.of(0, "hello"), new Instant(0)),
                  TimestampedValue.of(KV.of(0, "goodbye"), new Instant(10))))
          .apply(
              Window.<KV<Integer, String>>into(FixedWindows.of(Duration.standardMinutes(10)))
                  .withTimestampCombiner(TimestampCombiner.EARLIEST))
          .apply(GroupByKey.create())
          .apply(ParDo.of(new AssertTimestamp(new Instant(0))));

      p.run();
    }

    /**
     * Tests that when two elements are combined via a GroupByKey their output timestamp agrees with
     * the windowing function customized to use the latest value.
     */
    @Test
    @Category(ValidatesRunner.class)
    public void testTimestampCombinerLatest() {
      p.apply(
              Create.timestamped(
                  TimestampedValue.of(KV.of(0, "hello"), new Instant(0)),
                  TimestampedValue.of(KV.of(0, "goodbye"), new Instant(10))))
          .apply(
              Window.<KV<Integer, String>>into(FixedWindows.of(Duration.standardMinutes(10)))
                  .withTimestampCombiner(TimestampCombiner.LATEST))
          .apply(GroupByKey.create())
          .apply(ParDo.of(new AssertTimestamp(new Instant(10))));

      p.run();
    }

    @Test
    public void testGroupByKeyGetName() {
      Assert.assertEquals("GroupByKey", GroupByKey.<String, Integer>create().getName());
    }

    @Test
    public void testDisplayData() {
      GroupByKey<String, String> groupByKey = GroupByKey.create();
      GroupByKey<String, String> groupByFewKeys = GroupByKey.createWithFewKeys();

      DisplayData gbkDisplayData = DisplayData.from(groupByKey);
      DisplayData fewKeysDisplayData = DisplayData.from(groupByFewKeys);

      assertThat(gbkDisplayData.items(), empty());
      assertThat(fewKeysDisplayData, hasDisplayItem("fewKeys", true));
    }

    /** Verify that runners correctly hash/group on the encoded value and not the value itself. */
    @Test
    @Category({ValidatesRunner.class})
    public void testGroupByKeyWithBadEqualsHashCode() throws Exception {
      final int numValues = 10;
      final int numKeys = 5;

      p.getCoderRegistry()
          .registerCoderProvider(
              CoderProviders.fromStaticMethods(BadEqualityKey.class, DeterministicKeyCoder.class));

      // construct input data
      List<KV<BadEqualityKey, Long>> input = new ArrayList<>();
      for (int i = 0; i < numValues; i++) {
        for (int key = 0; key < numKeys; key++) {
          input.add(KV.of(new BadEqualityKey(key), 1L));
        }
      }

      // We first ensure that the values are randomly partitioned in the beginning.
      // Some runners might otherwise keep all values on the machine where
      // they are initially created.
      PCollection<KV<BadEqualityKey, Long>> dataset1 =
          p.apply(Create.of(input))
              .apply(ParDo.of(new AssignRandomKey()))
              .apply(Reshuffle.of())
              .apply(Values.create());

      // Make the GroupByKey and Count implicit, in real-world code
      // this would be a Count.perKey()
      PCollection<KV<BadEqualityKey, Long>> result =
          dataset1.apply(GroupByKey.create()).apply(Combine.groupedValues(new CountFn()));

      PAssert.that(result).satisfies(new AssertThatCountPerKeyCorrect(numValues));

      PAssert.that(result.apply(Keys.create())).satisfies(new AssertThatAllKeysExist(numKeys));

      p.run();
    }

    @Test
    @Category({ValidatesRunner.class, LargeKeys.Above10KB.class})
    public void testLargeKeys10KB() throws Exception {
      runLargeKeysTest(p, 10 << 10);
    }

    @Test
    @Category({ValidatesRunner.class, LargeKeys.Above100KB.class})
    public void testLargeKeys100KB() throws Exception {
      runLargeKeysTest(p, 100 << 10);
    }

    @Test
    @Category({ValidatesRunner.class, LargeKeys.Above1MB.class})
    public void testLargeKeys1MB() throws Exception {
      runLargeKeysTest(p, 1 << 20);
    }

    @Test
    @Category({ValidatesRunner.class, LargeKeys.Above10MB.class})
    public void testLargeKeys10MB() throws Exception {
      runLargeKeysTest(p, 10 << 20);
    }

    @Test
    @Category({ValidatesRunner.class, LargeKeys.Above100MB.class})
    public void testLargeKeys100MB() throws Exception {
      runLargeKeysTest(p, 100 << 20);
    }
  }

  /** Tests validating GroupByKey behaviors with windowing. */
  @RunWith(JUnit4.class)
  public static class WindowTests extends SharedTestBase {
    @Test
    @Category(ValidatesRunner.class)
    public void testGroupByKeyAndWindows() {
      List<KV<String, Integer>> ungroupedPairs =
          Arrays.asList(
              KV.of("k1", 3), // window [0, 5)
              KV.of("k5", Integer.MAX_VALUE), // window [0, 5)
              KV.of("k5", Integer.MIN_VALUE), // window [0, 5)
              KV.of("k2", 66), // window [0, 5)
              KV.of("k1", 4), // window [5, 10)
              KV.of("k2", -33), // window [5, 10)
              KV.of("k3", 0)); // window [5, 10)

      PCollection<KV<String, Integer>> input =
          p.apply(
              Create.timestamped(ungroupedPairs, Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L))
                  .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));
      PCollection<KV<String, Iterable<Integer>>> output =
          input.apply(Window.into(FixedWindows.of(Duration.millis(5)))).apply(GroupByKey.create());

      PAssert.that(output)
          .satisfies(
              containsKvs(
                  kv("k1", 3),
                  kv("k1", 4),
                  kv("k5", Integer.MAX_VALUE, Integer.MIN_VALUE),
                  kv("k2", 66),
                  kv("k2", -33),
                  kv("k3", 0)));
      PAssert.that(output)
          .inWindow(new IntervalWindow(new Instant(0L), Duration.millis(5L)))
          .satisfies(
              containsKvs(
                  kv("k1", 3), kv("k5", Integer.MIN_VALUE, Integer.MAX_VALUE), kv("k2", 66)));
      PAssert.that(output)
          .inWindow(new IntervalWindow(new Instant(5L), Duration.millis(5L)))
          .satisfies(containsKvs(kv("k1", 4), kv("k2", -33), kv("k3", 0)));

      p.run();
    }

    @Test
    @Category(ValidatesRunner.class)
    public void testGroupByKeyMultipleWindows() {
      PCollection<KV<String, Integer>> windowedInput =
          p.apply(
                  Create.timestamped(
                      TimestampedValue.of(KV.of("foo", 1), new Instant(1)),
                      TimestampedValue.of(KV.of("foo", 4), new Instant(4)),
                      TimestampedValue.of(KV.of("bar", 3), new Instant(3))))
              .apply(
                  Window.into(SlidingWindows.of(Duration.millis(5L)).every(Duration.millis(3L))));

      PCollection<KV<String, Iterable<Integer>>> output = windowedInput.apply(GroupByKey.create());

      PAssert.that(output)
          .satisfies(
              containsKvs(kv("foo", 1, 4), kv("foo", 1), kv("foo", 4), kv("bar", 3), kv("bar", 3)));
      PAssert.that(output)
          .inWindow(new IntervalWindow(new Instant(-3L), Duration.millis(5L)))
          .satisfies(containsKvs(kv("foo", 1)));
      PAssert.that(output)
          .inWindow(new IntervalWindow(new Instant(0L), Duration.millis(5L)))
          .satisfies(containsKvs(kv("foo", 1, 4), kv("bar", 3)));
      PAssert.that(output)
          .inWindow(new IntervalWindow(new Instant(3L), Duration.millis(5L)))
          .satisfies(containsKvs(kv("foo", 4), kv("bar", 3)));

      p.run();
    }

    @Test
    @Category(ValidatesRunner.class)
    public void testGroupByKeyMergingWindows() {
      PCollection<KV<String, Integer>> windowedInput =
          p.apply(
                  Create.timestamped(
                      TimestampedValue.of(KV.of("foo", 1), new Instant(1)),
                      TimestampedValue.of(KV.of("foo", 4), new Instant(4)),
                      TimestampedValue.of(KV.of("bar", 3), new Instant(3)),
                      TimestampedValue.of(KV.of("foo", 9), new Instant(9))))
              .apply(Window.into(Sessions.withGapDuration(Duration.millis(4L))));

      PCollection<KV<String, Iterable<Integer>>> output = windowedInput.apply(GroupByKey.create());

      PAssert.that(output).satisfies(containsKvs(kv("foo", 1, 4), kv("foo", 9), kv("bar", 3)));
      PAssert.that(output)
          .inWindow(new IntervalWindow(new Instant(1L), new Instant(8L)))
          .satisfies(containsKvs(kv("foo", 1, 4)));
      PAssert.that(output)
          .inWindow(new IntervalWindow(new Instant(3L), new Instant(7L)))
          .satisfies(containsKvs(kv("bar", 3)));
      PAssert.that(output)
          .inWindow(new IntervalWindow(new Instant(9L), new Instant(13L)))
          .satisfies(containsKvs(kv("foo", 9)));

      p.run();
    }

    @Test
    @Category(ValidatesRunner.class)
    public void testRewindowWithTimestampCombiner() {
      PCollection<KV<String, Integer>> input =
          p.apply(
                  Create.timestamped(
                      TimestampedValue.of(KV.of("foo", 1), new Instant(1)),
                      TimestampedValue.of(KV.of("foo", 4), new Instant(4)),
                      TimestampedValue.of(KV.of("bar", 3), new Instant(3)),
                      TimestampedValue.of(KV.of("foo", 9), new Instant(9))))
              .apply(
                  "GlobalWindows",
                  Window.<KV<String, Integer>>configure()
                      .withTimestampCombiner(TimestampCombiner.LATEST));

      PCollection<KV<String, Integer>> result =
          input
              .apply(GroupByKey.create())
              .apply(
                  MapElements.into(
                          TypeDescriptors.kvs(
                              TypeDescriptors.strings(), TypeDescriptors.integers()))
                      .via(kv -> KV.of(kv.getKey(), sum(kv.getValue()))))
              .apply("FixedWindows", Window.into(FixedWindows.of(Duration.millis(1))));

      PAssert.that(result)
          .inWindow(new IntervalWindow(new Instant(9), new Instant(10)))
          .containsInAnyOrder(KV.of("foo", 14))
          .inWindow(new IntervalWindow(new Instant(3), new Instant(4)))
          .containsInAnyOrder(KV.of("bar", 3));

      p.run();
    }

    private static int sum(Iterable<Integer> parts) {
      return Streams.stream(parts).mapToInt(e -> e).sum();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testIdentityWindowFnPropagation() {
      List<KV<String, Integer>> ungroupedPairs = Arrays.asList();

      PCollection<KV<String, Integer>> input =
          p.apply(
                  Create.of(ungroupedPairs)
                      .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())))
              .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))));

      PCollection<KV<String, Iterable<Integer>>> output = input.apply(GroupByKey.create());

      p.run();

      Assert.assertTrue(
          output
              .getWindowingStrategy()
              .getWindowFn()
              .isCompatible(FixedWindows.of(Duration.standardMinutes(1))));
    }

    @Test
    @Category(NeedsRunner.class)
    public void testWindowFnPostMerging() throws Exception {

      PCollection<KV<String, Integer>> windowedInput =
          p.apply(
                  Create.timestamped(
                      TimestampedValue.of(KV.of("foo", 1), new Instant(1)),
                      TimestampedValue.of(KV.of("foo", 4), new Instant(4)),
                      TimestampedValue.of(KV.of("bar", 3), new Instant(3)),
                      TimestampedValue.of(KV.of("foo", 9), new Instant(9))))
              .apply(Window.into(Sessions.withGapDuration(Duration.millis(4L))));

      PCollection<KV<String, Iterable<Integer>>> grouped =
          windowedInput.apply("First grouping", GroupByKey.create());
      PAssert.that(grouped).satisfies(containsKvs(kv("foo", 1, 4), kv("foo", 9), kv("bar", 3)));

      // Check that the WindowFn is carried along as-is but alreadyMerged bit is set
      assertThat(
          grouped.getWindowingStrategy().getWindowFn(),
          equalTo(windowedInput.getWindowingStrategy().getWindowFn()));

      assertThat(
          "WindowingStrategy should be already merged",
          grouped.getWindowingStrategy().isAlreadyMerged());

      // Second grouping should sum existing groupings, even those exploded, since the windows match
      // and are carried along.
      PCollection<Integer> sums =
          grouped
              .apply("Drop keys", Values.create())
              .apply("Explode iterables", Flatten.iterables())
              .apply("Map to same key", WithKeys.of("bizzle"))
              .apply("Summed grouping", Sum.integersPerKey())
              .apply("Pull out sums", Values.create());

      PAssert.that(sums)
          .containsInAnyOrder(
              5 /* sum originating from (foo, 1) and (foo, 4) that merged */,
              9 /* sum of just (foo, 9) which doesn't merge */,
              3 /* sum of just (bar, 3) which doesn't merge */);

      p.run();
    }
  }

  private static KV<String, Collection<Integer>> kv(String key, Integer... values) {
    return KV.of(key, ImmutableList.copyOf(values));
  }

  private static SerializableFunction<Iterable<KV<String, Iterable<Integer>>>, Void> containsKvs(
      KV<String, Collection<Integer>>... kvs) {
    return new ContainsKVs(ImmutableList.copyOf(kvs));
  }

  /**
   * A function that asserts that the input element contains the expected {@link KV KVs} in any
   * order, where values appear in any order.
   */
  private static class ContainsKVs
      implements SerializableFunction<Iterable<KV<String, Iterable<Integer>>>, Void> {
    private final List<KV<String, Collection<Integer>>> expectedKvs;

    private ContainsKVs(List<KV<String, Collection<Integer>>> expectedKvs) {
      this.expectedKvs = expectedKvs;
    }

    @Override
    public Void apply(Iterable<KV<String, Iterable<Integer>>> input) {
      List<Matcher<? super KV<String, Iterable<Integer>>>> matchers = new ArrayList<>();
      for (KV<String, Collection<Integer>> expected : expectedKvs) {
        Integer[] values = expected.getValue().toArray(new Integer[0]);
        matchers.add(isKv(equalTo(expected.getKey()), containsInAnyOrder(values)));
      }
      assertThat(input, containsInAnyOrder(matchers.toArray(new Matcher[0])));
      return null;
    }
  }

  private static class AssertTimestamp<K, V> extends DoFn<KV<K, V>, Void> {
    private final Instant timestamp;

    public AssertTimestamp(Instant timestamp) {
      this.timestamp = timestamp;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      assertThat(c.timestamp(), equalTo(timestamp));
    }
  }

  private static String bigString(char c, int size) {
    char[] buf = new char[size];
    for (int i = 0; i < size; i++) {
      buf[i] = c;
    }
    return new String(buf);
  }

  private static void runLargeKeysTest(TestPipeline p, final int keySize) throws Exception {
    PCollection<KV<String, Integer>> result =
        p.apply(Create.of("a", "a", "b"))
            .apply(
                "Expand",
                ParDo.of(
                    new DoFn<String, KV<String, String>>() {
                      @ProcessElement
                      public void process(ProcessContext c) {
                        c.output(KV.of(bigString(c.element().charAt(0), keySize), c.element()));
                      }
                    }))
            .apply(GroupByKey.create())
            .apply(
                "Count",
                ParDo.of(
                    new DoFn<KV<String, Iterable<String>>, KV<String, Integer>>() {
                      @ProcessElement
                      public void process(ProcessContext c) {
                        int size = 0;
                        for (String unused : c.element().getValue()) {
                          size++;
                        }
                        c.output(KV.of(c.element().getKey(), size));
                      }
                    }));

    PAssert.that(result)
        .satisfies(
            values -> {
              assertThat(
                  values,
                  containsInAnyOrder(
                      KV.of(bigString('a', keySize), 2), KV.of(bigString('b', keySize), 1)));
              return null;
            });

    p.run();
  }

  /**
   * This is a bogus key class that returns random hash values from {@link #hashCode()} and always
   * returns {@code false} for {@link #equals(Object)}. The results of the test are correct if the
   * runner correctly hashes and sorts on the encoded bytes.
   */
  protected static class BadEqualityKey {
    long key;

    public BadEqualityKey() {}

    public BadEqualityKey(long key) {
      this.key = key;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      return false;
    }

    @Override
    public int hashCode() {
      return ThreadLocalRandom.current().nextInt();
    }
  }

  /** Deterministic {@link Coder} for {@link BadEqualityKey}. */
  protected static class DeterministicKeyCoder extends AtomicCoder<BadEqualityKey> {

    public static DeterministicKeyCoder of() {
      return INSTANCE;
    }

    /////////////////////////////////////////////////////////////////////////////

    private static final DeterministicKeyCoder INSTANCE = new DeterministicKeyCoder();

    private DeterministicKeyCoder() {}

    @Override
    public void encode(BadEqualityKey value, OutputStream outStream) throws IOException {
      new DataOutputStream(outStream).writeLong(value.key);
    }

    @Override
    public BadEqualityKey decode(InputStream inStream) throws IOException {
      return new BadEqualityKey(new DataInputStream(inStream).readLong());
    }

    @Override
    public void verifyDeterministic() {}
  }

  /** Creates a KV that wraps the original KV together with a random key. */
  static class AssignRandomKey
      extends DoFn<KV<BadEqualityKey, Long>, KV<Long, KV<BadEqualityKey, Long>>> {

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      c.output(KV.of(ThreadLocalRandom.current().nextLong(), c.element()));
    }
  }

  static class CountFn implements SerializableFunction<Iterable<Long>, Long> {
    @Override
    public Long apply(Iterable<Long> input) {
      long result = 0L;
      for (Long in : input) {
        result += in;
      }
      return result;
    }
  }

  static class AssertThatCountPerKeyCorrect
      implements SerializableFunction<Iterable<KV<BadEqualityKey, Long>>, Void> {
    private final int numValues;

    AssertThatCountPerKeyCorrect(int numValues) {
      this.numValues = numValues;
    }

    @Override
    public Void apply(Iterable<KV<BadEqualityKey, Long>> input) {
      for (KV<BadEqualityKey, Long> val : input) {
        Assert.assertEquals(numValues, (long) val.getValue());
      }
      return null;
    }
  }

  static class AssertThatAllKeysExist
      implements SerializableFunction<Iterable<BadEqualityKey>, Void> {
    private final int numKeys;

    AssertThatAllKeysExist(int numKeys) {
      this.numKeys = numKeys;
    }

    private static <T> Iterable<Object> asStructural(
        final Iterable<T> iterable, final Coder<T> coder) {

      return StreamSupport.stream(iterable.spliterator(), false)
          .map(
              input -> {
                try {
                  return coder.structuralValue(input);
                } catch (Exception e) {
                  Assert.fail("Could not structural values.");
                  throw new RuntimeException(); // to satisfy the compiler...
                }
              })
          .collect(Collectors.toList());
    }

    @Override
    public Void apply(Iterable<BadEqualityKey> input) {
      final DeterministicKeyCoder keyCoder = DeterministicKeyCoder.of();

      List<BadEqualityKey> expectedList = new ArrayList<>();
      for (int key = 0; key < numKeys; key++) {
        expectedList.add(new BadEqualityKey(key));
      }

      Iterable<Object> structuralInput = asStructural(input, keyCoder);
      Iterable<Object> structuralExpected = asStructural(expectedList, keyCoder);

      for (Object expected : structuralExpected) {
        assertThat(structuralInput, hasItem(expected));
      }

      return null;
    }
  }
}
