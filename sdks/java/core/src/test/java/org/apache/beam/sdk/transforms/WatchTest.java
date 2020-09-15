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

import static org.apache.beam.sdk.transforms.Requirements.requiresSideInputs;
import static org.apache.beam.sdk.transforms.Watch.Growth.afterTimeSinceNewOutput;
import static org.apache.beam.sdk.transforms.Watch.Growth.allOf;
import static org.apache.beam.sdk.transforms.Watch.Growth.eitherOf;
import static org.apache.beam.sdk.transforms.Watch.Growth.never;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.joda.time.Duration.standardSeconds;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesUnboundedSplittableParDo;
import org.apache.beam.sdk.transforms.Watch.Growth;
import org.apache.beam.sdk.transforms.Watch.Growth.PollFn;
import org.apache.beam.sdk.transforms.Watch.Growth.PollResult;
import org.apache.beam.sdk.transforms.Watch.GrowthState;
import org.apache.beam.sdk.transforms.Watch.GrowthTracker;
import org.apache.beam.sdk.transforms.Watch.NonPollingGrowthState;
import org.apache.beam.sdk.transforms.Watch.PollingGrowthState;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Ordering;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Funnel;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Funnels;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.HashCode;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Hashing;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.ReadableDuration;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Watch}. */
@RunWith(JUnit4.class)
public class WatchTest implements Serializable {

  @Rule public transient TestPipeline p = TestPipeline.create();

  @Test
  @Category({NeedsRunner.class, UsesUnboundedSplittableParDo.class})
  public void testSinglePollMultipleInputs() {
    PCollection<KV<String, String>> res =
        p.apply(Create.of("a", "b"))
            .apply(
                Watch.growthOf(
                        new PollFn<String, String>() {
                          @Override
                          public PollResult<String> apply(String element, Context c)
                              throws Exception {
                            return PollResult.complete(
                                Instant.now(), Arrays.asList(element + ".foo", element + ".bar"));
                          }
                        })
                    .withPollInterval(Duration.ZERO));

    PAssert.that(res)
        .containsInAnyOrder(
            Arrays.asList(
                KV.of("a", "a.foo"), KV.of("a", "a.bar"),
                KV.of("b", "b.foo"), KV.of("b", "b.bar")));

    p.run();
  }

  @Test
  @Category({NeedsRunner.class, UsesUnboundedSplittableParDo.class})
  public void testSinglePollMultipleInputsWithSideInput() {
    final PCollectionView<String> moo =
        p.apply("moo", Create.of("moo")).apply("moo singleton", View.asSingleton());
    final PCollectionView<String> zoo =
        p.apply("zoo", Create.of("zoo")).apply("zoo singleton", View.asSingleton());
    PCollection<KV<String, String>> res =
        p.apply("input", Create.of("a", "b"))
            .apply(
                Watch.growthOf(
                        new PollFn<String, String>() {
                          @Override
                          public PollResult<String> apply(String element, Context c)
                              throws Exception {
                            return PollResult.complete(
                                Instant.now(),
                                Arrays.asList(
                                    element + " " + c.sideInput(moo) + " " + c.sideInput(zoo)));
                          }
                        },
                        requiresSideInputs(moo, zoo))
                    .withPollInterval(Duration.ZERO));

    PAssert.that(res)
        .containsInAnyOrder(Arrays.asList(KV.of("a", "a moo zoo"), KV.of("b", "b moo zoo")));

    p.run();
  }

  @Test
  @Category({NeedsRunner.class, UsesUnboundedSplittableParDo.class})
  public void testMultiplePollsWithTerminationBecauseOutputIsFinal() {
    testMultiplePolls(false);
  }

  @Test
  @Category({NeedsRunner.class, UsesUnboundedSplittableParDo.class})
  public void testMultiplePollsWithTerminationDueToTerminationCondition() {
    testMultiplePolls(true);
  }

  private void testMultiplePolls(boolean terminationConditionElapsesBeforeOutputIsFinal) {
    List<Integer> all = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

    PCollection<Integer> res =
        p.apply(Create.of("a"))
            .apply(
                Watch.growthOf(
                        new TimedPollFn<String, Integer>(
                            all,
                            standardSeconds(1) /* timeToOutputEverything */,
                            standardSeconds(3) /* timeToDeclareOutputFinal */,
                            standardSeconds(30) /* timeToFail */))
                    .withTerminationPerInput(
                        Growth.afterTotalOf(
                            standardSeconds(
                                // At 2 seconds, all output has been yielded, but not yet
                                // declared final - so polling should terminate per termination
                                // condition.
                                // At 3 seconds, all output has been yielded (and declared final),
                                // so polling should terminate because of that without waiting for
                                // 100 seconds.
                                terminationConditionElapsesBeforeOutputIsFinal ? 2 : 100)))
                    .withPollInterval(Duration.millis(300))
                    .withOutputCoder(VarIntCoder.of()))
            .apply("Drop input", Values.create());

    PAssert.that(res).containsInAnyOrder(all);

    p.run();
  }

  @Test
  @Category({NeedsRunner.class, UsesUnboundedSplittableParDo.class})
  public void testMultiplePollsWithKeyExtractor() {
    List<KV<Integer, String>> polls =
        Arrays.asList(
            KV.of(0, "0"),
            KV.of(10, "10"),
            KV.of(20, "20"),
            KV.of(30, "30"),
            KV.of(40, "40"),
            KV.of(40, "40.1"),
            KV.of(20, "20.1"),
            KV.of(50, "50"),
            KV.of(10, "10.1"),
            KV.of(10, "10.2"),
            KV.of(60, "60"),
            KV.of(70, "70"),
            KV.of(60, "60.1"),
            KV.of(80, "80"),
            KV.of(40, "40.2"),
            KV.of(90, "90"),
            KV.of(90, "90.1"));

    List<Integer> expected = Arrays.asList(0, 10, 20, 30, 40, 50, 60, 70, 80, 90);

    PCollection<Integer> res =
        p.apply(Create.of("a"))
            .apply(
                Watch.growthOf(
                        Contextful.of(
                            new TimedPollFn<String, KV<Integer, String>>(
                                polls,
                                standardSeconds(1) /* timeToOutputEverything */,
                                standardSeconds(3) /* timeToDeclareOutputFinal */,
                                standardSeconds(30) /* timeToFail */),
                            Requirements.empty()),
                        KV::getKey)
                    .withTerminationPerInput(Growth.afterTotalOf(standardSeconds(5)))
                    .withPollInterval(Duration.millis(100))
                    .withOutputCoder(KvCoder.of(VarIntCoder.of(), StringUtf8Coder.of()))
                    .withOutputKeyCoder(VarIntCoder.of()))
            .apply("Drop input", Values.create())
            .apply("Drop auxiliary string", Keys.create());

    PAssert.that(res).containsInAnyOrder(expected);

    p.run();
  }

  @Test
  @Category({NeedsRunner.class, UsesUnboundedSplittableParDo.class})
  public void testMultiplePollsStopAfterTimeSinceNewOutput() {
    List<Integer> all = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

    PCollection<Integer> res =
        p.apply(Create.of("a"))
            .apply(
                Watch.growthOf(
                        new TimedPollFn<String, Integer>(
                            all,
                            standardSeconds(1) /* timeToOutputEverything */,
                            // Never declare output final
                            standardSeconds(1000) /* timeToDeclareOutputFinal */,
                            standardSeconds(30) /* timeToFail */))
                    // Should terminate after 4 seconds - earlier than timeToFail
                    .withTerminationPerInput(afterTimeSinceNewOutput(standardSeconds(3)))
                    .withPollInterval(Duration.millis(300))
                    .withOutputCoder(VarIntCoder.of()))
            .apply("Drop input", Values.create());

    PAssert.that(res).containsInAnyOrder(all);

    p.run();
  }

  @Test
  @Category({NeedsRunner.class, UsesUnboundedSplittableParDo.class})
  public void testSinglePollWithManyResults() {
    // More than the default 100 elements per checkpoint for direct runner.
    final long numResults = 3000;
    PCollection<KV<String, Integer>> res =
        p.apply(Create.of("a"))
            .apply(
                Watch.growthOf(
                        new PollFn<String, KV<String, Integer>>() {
                          @Override
                          public PollResult<KV<String, Integer>> apply(String element, Context c)
                              throws Exception {
                            String pollId = UUID.randomUUID().toString();
                            List<KV<String, Integer>> output = Lists.newArrayList();
                            for (int i = 0; i < numResults; ++i) {
                              output.add(KV.of(pollId, i));
                            }
                            return PollResult.complete(Instant.now(), output);
                          }
                        })
                    .withTerminationPerInput(Growth.afterTotalOf(standardSeconds(1)))
                    .withPollInterval(Duration.millis(1))
                    .withOutputCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of())))
            .apply("Drop input", Values.create());

    PAssert.that("Poll called only once", res.apply(Keys.create()))
        .satisfies(
            pollIds -> {
              assertEquals(1, Sets.newHashSet(pollIds).size());
              return null;
            });
    PAssert.that("Yields all expected results", res.apply("Drop poll id", Values.create()))
        .satisfies(
            input -> {
              assertEquals(
                  "Total number of results mismatches",
                  numResults,
                  Lists.newArrayList(input).size());
              assertEquals("Results are not unique", numResults, Sets.newHashSet(input).size());
              return null;
            });

    p.run();
  }

  @Test
  @Category({NeedsRunner.class, UsesUnboundedSplittableParDo.class})
  @Ignore("https://issues.apache.org/jira/browse/BEAM-8035")
  public void testMultiplePollsWithManyResults() {
    final long numResults = 3000;
    List<Integer> all = Lists.newArrayList();
    for (int i = 0; i < numResults; ++i) {
      all.add(i);
    }

    PCollection<TimestampedValue<Integer>> res =
        p.apply(Create.of("a"))
            .apply(
                Watch.growthOf(
                        new TimedPollFn<String, Integer>(
                            all,
                            standardSeconds(1) /* timeToOutputEverything */,
                            standardSeconds(3) /* timeToDeclareOutputFinal */,
                            standardSeconds(30) /* timeToFail */))
                    .withPollInterval(Duration.millis(500))
                    .withOutputCoder(VarIntCoder.of()))
            .apply(Reify.timestampsInValue())
            .apply("Drop timestamped input", Values.create());

    PAssert.that(res)
        .satisfies(
            outputs -> {
              Function<TimestampedValue<Integer>, Integer> extractValueFn =
                  new Function<TimestampedValue<Integer>, Integer>() {
                    @Nullable
                    @Override
                    public Integer apply(@Nullable TimestampedValue<Integer> input) {
                      return input.getValue();
                    }
                  };
              Function<TimestampedValue<Integer>, Instant> extractTimestampFn =
                  new Function<TimestampedValue<Integer>, Instant>() {
                    @Nullable
                    @Override
                    public Instant apply(@Nullable TimestampedValue<Integer> input) {
                      return input.getTimestamp();
                    }
                  };

              Ordering<TimestampedValue<Integer>> byTimestamp =
                  Ordering.natural().onResultOf(extractTimestampFn);
              // New outputs appear in timestamp order because each output's assigned timestamp
              // is Instant.now() at the time of poll.
              assertTrue("Outputs must be in timestamp order", byTimestamp.isOrdered(outputs));
              assertEquals(
                  "Yields all expected values",
                  numResults,
                  Sets.newHashSet(
                          StreamSupport.stream(outputs.spliterator(), false)
                              .map(extractValueFn::apply)
                              .collect(Collectors.toList()))
                      .size());
              assertThat(
                  "Poll called more than once",
                  Sets.newHashSet(
                          StreamSupport.stream(outputs.spliterator(), false)
                              .map(extractTimestampFn::apply)
                              .collect(Collectors.toList()))
                      .size(),
                  greaterThan(1));
              return null;
            });

    p.run();
  }

  @Test
  public void testCoder() throws Exception {
    GrowthState pollingState =
        PollingGrowthState.of(
            ImmutableMap.of(
                HashCode.fromString("0123456789abcdef0123456789abcdef"), Instant.now(),
                HashCode.fromString("01230123012301230123012301230123"), Instant.now()),
            Instant.now(),
            "STATE");
    GrowthState nonPollingState =
        NonPollingGrowthState.of(
            Growth.PollResult.incomplete(Instant.now(), Arrays.asList("A", "B")));
    Coder<GrowthState> coder =
        Watch.GrowthStateCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    CoderProperties.coderDecodeEncodeEqual(coder, pollingState);
    CoderProperties.coderDecodeEncodeEqual(coder, nonPollingState);
  }

  /**
   * Gradually emits all items from the given list, pairing each one with a UUID that identifies the
   * round of polling, so a client can check how many rounds of polling there were.
   */
  private static class TimedPollFn<InputT, OutputT> extends PollFn<InputT, OutputT> {

    private final Instant baseTime;
    private final List<OutputT> outputs;
    private final Duration timeToOutputEverything;
    private final Duration timeToDeclareOutputFinal;
    private final Duration timeToFail;

    public TimedPollFn(
        List<OutputT> outputs,
        Duration timeToOutputEverything,
        Duration timeToDeclareOutputFinal,
        Duration timeToFail) {
      this.baseTime = Instant.now();
      this.outputs = outputs;
      this.timeToOutputEverything = timeToOutputEverything;
      this.timeToDeclareOutputFinal = timeToDeclareOutputFinal;
      this.timeToFail = timeToFail;
    }

    @Override
    public PollResult<OutputT> apply(InputT element, Context c) throws Exception {
      Instant now = Instant.now();
      Duration elapsed = new Duration(baseTime, Instant.now());
      if (elapsed.isLongerThan(timeToFail)) {
        fail(
            String.format(
                "Poll called %s after base time, which is longer than the threshold of %s",
                elapsed, timeToFail));
      }

      double fractionElapsed = 1.0 * elapsed.getMillis() / timeToOutputEverything.getMillis();
      int numToEmit = (int) Math.min(outputs.size(), fractionElapsed * outputs.size());
      List<TimestampedValue<OutputT>> toEmit = Lists.newArrayList();
      for (int i = 0; i < numToEmit; ++i) {
        toEmit.add(TimestampedValue.of(outputs.get(i), now));
      }
      return elapsed.isLongerThan(timeToDeclareOutputFinal)
          ? PollResult.complete(toEmit)
          : PollResult.incomplete(toEmit).withWatermark(now);
    }
  }

  @Test
  public void testTerminationConditionsNever() {
    Watch.Growth.Never<Object> c = never();
    Integer state = c.forNewInput(Instant.now(), null);
    assertFalse(c.canStopPolling(Instant.now(), state));
  }

  @Test
  public void testTerminationConditionsAfterTotalOf() {
    Instant now = Instant.now();
    Watch.Growth.AfterTotalOf<Object> c = Growth.afterTotalOf(standardSeconds(5));
    KV<Instant, ReadableDuration> state = c.forNewInput(now, null);
    assertFalse(c.canStopPolling(now, state));
    assertFalse(c.canStopPolling(now.plus(standardSeconds(3)), state));
    assertTrue(c.canStopPolling(now.plus(standardSeconds(6)), state));
  }

  @Test
  public void testTerminationConditionsAfterTimeSinceNewOutput() {
    Instant now = Instant.now();
    Watch.Growth.AfterTimeSinceNewOutput<Object> c = afterTimeSinceNewOutput(standardSeconds(5));
    KV<Instant, ReadableDuration> state = c.forNewInput(now, null);
    assertFalse(c.canStopPolling(now, state));
    assertFalse(c.canStopPolling(now.plus(standardSeconds(3)), state));
    assertFalse(c.canStopPolling(now.plus(standardSeconds(6)), state));

    state = c.onSeenNewOutput(now.plus(standardSeconds(3)), state);
    assertFalse(c.canStopPolling(now.plus(standardSeconds(3)), state));
    assertFalse(c.canStopPolling(now.plus(standardSeconds(6)), state));
    assertTrue(c.canStopPolling(now.plus(standardSeconds(9)), state));

    state = c.onSeenNewOutput(now.plus(standardSeconds(5)), state);
    assertFalse(c.canStopPolling(now.plus(standardSeconds(3)), state));
    assertFalse(c.canStopPolling(now.plus(standardSeconds(6)), state));
    assertFalse(c.canStopPolling(now.plus(standardSeconds(9)), state));
    assertTrue(c.canStopPolling(now.plus(standardSeconds(11)), state));
  }

  @Test
  public void testTerminationConditionsEitherOf() {
    Instant now = Instant.now();
    Watch.Growth.AfterTotalOf<Object> a = Growth.afterTotalOf(standardSeconds(5));
    Watch.Growth.AfterTotalOf<Object> b = Growth.afterTotalOf(standardSeconds(10));

    Watch.Growth.BinaryCombined<
            Object, KV<Instant, ReadableDuration>, KV<Instant, ReadableDuration>>
        c = eitherOf(a, b);
    KV<KV<Instant, ReadableDuration>, KV<Instant, ReadableDuration>> state =
        c.forNewInput(now, null);
    assertFalse(c.canStopPolling(now.plus(standardSeconds(3)), state));
    assertTrue(c.canStopPolling(now.plus(standardSeconds(7)), state));
    assertTrue(c.canStopPolling(now.plus(standardSeconds(12)), state));
  }

  @Test
  public void testTerminationConditionsAllOf() {
    Instant now = Instant.now();
    Watch.Growth.AfterTotalOf<Object> a = Growth.afterTotalOf(standardSeconds(5));
    Watch.Growth.AfterTotalOf<Object> b = Growth.afterTotalOf(standardSeconds(10));

    Watch.Growth.BinaryCombined<
            Object, KV<Instant, ReadableDuration>, KV<Instant, ReadableDuration>>
        c = allOf(a, b);
    KV<KV<Instant, ReadableDuration>, KV<Instant, ReadableDuration>> state =
        c.forNewInput(now, null);
    assertFalse(c.canStopPolling(now.plus(standardSeconds(3)), state));
    assertFalse(c.canStopPolling(now.plus(standardSeconds(7)), state));
    assertTrue(c.canStopPolling(now.plus(standardSeconds(12)), state));
  }

  private static GrowthTracker<String, Integer> newTracker(GrowthState state) {
    Funnel<String> coderFunnel =
        (from, into) -> {
          try {
            StringUtf8Coder.of().encode(from, Funnels.asOutputStream(into));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        };
    return new GrowthTracker<>(state, coderFunnel);
  }

  private static HashCode hash128(String value) {
    Funnel<String> coderFunnel =
        (from, into) -> {
          try {
            StringUtf8Coder.of().encode(from, Funnels.asOutputStream(into));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        };
    return Hashing.murmur3_128().hashObject(value, coderFunnel);
  }

  private static GrowthTracker<String, Integer> newPollingGrowthTracker() {
    return newTracker(PollingGrowthState.of(never().forNewInput(Instant.now(), null)));
  }

  @Test
  public void testPollingGrowthTrackerCheckpointNonEmpty() {
    Instant now = Instant.now();
    GrowthTracker<String, Integer> tracker = newPollingGrowthTracker();

    PollResult<String> claim =
        PollResult.incomplete(
                Arrays.asList(
                    TimestampedValue.of("d", now.plus(standardSeconds(4))),
                    TimestampedValue.of("c", now.plus(standardSeconds(3))),
                    TimestampedValue.of("a", now.plus(standardSeconds(1))),
                    TimestampedValue.of("b", now.plus(standardSeconds(2)))))
            .withWatermark(now.plus(standardSeconds(7)));

    assertTrue(tracker.tryClaim(KV.of(claim, 1 /* termination state */)));

    PollingGrowthState<Integer> residual =
        (PollingGrowthState<Integer>) tracker.trySplit(0).getResidual();
    NonPollingGrowthState<String> primary =
        (NonPollingGrowthState<String>) tracker.currentRestriction();
    tracker.checkDone();

    // Verify primary: should contain what the current tracker claimed, and nothing else.
    assertEquals(claim, primary.getPending());

    // Verify residual: should contain what the current tracker didn't claim.
    assertEquals(now.plus(standardSeconds(7)), residual.getPollWatermark());
    assertThat(
        residual.getCompleted().keySet(),
        containsInAnyOrder(hash128("a"), hash128("b"), hash128("c"), hash128("d")));
    assertEquals(1, (int) residual.getTerminationState());
  }

  @Test
  public void testPollingGrowthTrackerCheckpointEmpty() {
    GrowthTracker<String, Integer> tracker = newPollingGrowthTracker();

    PollingGrowthState<Integer> residual =
        (PollingGrowthState<Integer>) tracker.trySplit(0).getResidual();
    GrowthState primary = tracker.currentRestriction();
    tracker.checkDone();

    // Verify primary: should contain what the current tracker claimed, and nothing else.
    assertEquals(GrowthTracker.EMPTY_STATE, primary);

    // Verify residual: should contain what the current tracker didn't claim.
    assertNull(residual.getPollWatermark());
    assertEquals(0, residual.getCompleted().size());
    assertEquals(0, (int) residual.getTerminationState());
  }

  @Test
  public void testPollingGrowthTrackerHashAlreadyClaimed() {
    Instant now = Instant.now();
    GrowthTracker<String, Integer> tracker = newPollingGrowthTracker();

    PollResult<String> claim =
        PollResult.incomplete(
                Arrays.asList(
                    TimestampedValue.of("d", now.plus(standardSeconds(4))),
                    TimestampedValue.of("c", now.plus(standardSeconds(3))),
                    TimestampedValue.of("a", now.plus(standardSeconds(1))),
                    TimestampedValue.of("b", now.plus(standardSeconds(2)))))
            .withWatermark(now.plus(standardSeconds(7)));

    assertTrue(tracker.tryClaim(KV.of(claim, 1 /* termination state */)));

    PollingGrowthState<Integer> residual =
        (PollingGrowthState<Integer>) tracker.trySplit(0).getResidual();

    assertFalse(newTracker(residual).tryClaim(KV.of(claim, 2)));
  }

  @Test
  public void testNonPollingGrowthTrackerCheckpointNonEmpty() {
    Instant now = Instant.now();
    PollResult<String> claim =
        PollResult.incomplete(
                Arrays.asList(
                    TimestampedValue.of("d", now.plus(standardSeconds(4))),
                    TimestampedValue.of("c", now.plus(standardSeconds(3))),
                    TimestampedValue.of("a", now.plus(standardSeconds(1))),
                    TimestampedValue.of("b", now.plus(standardSeconds(2)))))
            .withWatermark(now.plus(standardSeconds(7)));

    GrowthTracker<String, Integer> tracker = newTracker(NonPollingGrowthState.of(claim));

    assertTrue(tracker.tryClaim(KV.of(claim, 1 /* termination state */)));
    GrowthState residual = tracker.trySplit(0).getResidual();
    NonPollingGrowthState<String> primary =
        (NonPollingGrowthState<String>) tracker.currentRestriction();
    tracker.checkDone();

    // Verify primary: should contain what the current tracker claimed, and nothing else.
    assertEquals(claim, primary.getPending());

    // Verify residual: should contain what the current tracker didn't claim.
    assertEquals(GrowthTracker.EMPTY_STATE, residual);
  }

  @Test
  public void testNonPollingGrowthTrackerCheckpointEmpty() {
    Instant now = Instant.now();
    PollResult<String> claim =
        PollResult.incomplete(
                Arrays.asList(
                    TimestampedValue.of("d", now.plus(standardSeconds(4))),
                    TimestampedValue.of("c", now.plus(standardSeconds(3))),
                    TimestampedValue.of("a", now.plus(standardSeconds(1))),
                    TimestampedValue.of("b", now.plus(standardSeconds(2)))))
            .withWatermark(now.plus(standardSeconds(7)));

    GrowthTracker<String, Integer> tracker = newTracker(NonPollingGrowthState.of(claim));

    NonPollingGrowthState<String> residual =
        (NonPollingGrowthState<String>) tracker.trySplit(0).getResidual();
    GrowthState primary = tracker.currentRestriction();
    tracker.checkDone();

    // Verify primary: should contain what the current tracker claimed, and nothing else.
    assertEquals(GrowthTracker.EMPTY_STATE, primary);

    // Verify residual: should contain what the current tracker didn't claim.
    assertEquals(claim, residual.getPending());
  }

  @Test
  public void testNonPollingGrowthTrackerFailedToClaimOtherPollResult() {
    Instant now = Instant.now();
    PollResult<String> claim =
        PollResult.incomplete(
                Arrays.asList(
                    TimestampedValue.of("d", now.plus(standardSeconds(4))),
                    TimestampedValue.of("c", now.plus(standardSeconds(3))),
                    TimestampedValue.of("a", now.plus(standardSeconds(1))),
                    TimestampedValue.of("b", now.plus(standardSeconds(2)))))
            .withWatermark(now.plus(standardSeconds(7)));

    GrowthTracker<String, Integer> tracker = newTracker(NonPollingGrowthState.of(claim));

    PollResult<String> otherClaim =
        PollResult.incomplete(
                Arrays.asList(
                    TimestampedValue.of("x", now.plus(standardSeconds(14))),
                    TimestampedValue.of("y", now.plus(standardSeconds(13))),
                    TimestampedValue.of("z", now.plus(standardSeconds(12)))))
            .withWatermark(now.plus(standardSeconds(17)));
    assertFalse(tracker.tryClaim(KV.of(otherClaim, 1)));
  }
}
