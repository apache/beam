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
package org.apache.beam.io.requestresponse;

import static org.apache.beam.io.requestresponse.Throttle.INPUT_ELEMENTS_COUNTER_NAME;
import static org.apache.beam.io.requestresponse.Throttle.OUTPUT_ELEMENTS_COUNTER_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.PeriodicImpulse;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.hamcrest.Matcher;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.ReadableDuration;
import org.junit.Rule;
import org.junit.Test;

/** Tests for {@link Throttle}. */
public class ThrottleTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();

  /**
   * Tests whether a pulse of elements totaled less than the maximum rate are just emitted
   * immediately without throttling.
   */
  @Test
  public void givenElementSizeNotExceedsRate_thenEmitsAllImmediately() {
    Rate rate = Rate.of(10, Duration.standardSeconds(1L));
    long expectedMillis = rate.getInterval().getMillis();
    long toleratedError = (long) (0.05 * (double) expectedMillis);
    // tolerate 5% error.
    Duration expectedInterval = Duration.millis(expectedMillis + toleratedError);
    List<Integer> items = Stream.iterate(0, i -> i + 1).limit(3).collect(Collectors.toList());
    PCollection<Integer> throttled = pipeline.apply(Create.of(items)).apply(transformOf(rate));

    PAssert.that(throttled).containsInAnyOrder(items);
    PAssert.that(timestampsOf(throttled))
        .satisfies(
            itr -> {
              List<Instant> timestamps =
                  StreamSupport.stream(itr.spliterator(), true)
                      .sorted()
                      .collect(Collectors.toList());
              assertTimestampIntervalsMatch(timestamps, lessThan(expectedInterval));
              return null;
            });

    pipeline.run();
  }

  /** Tests whether a pulse of elements totaled greater than the maximum rate are throttled. */
  @Test
  public void givenElementSizeExceedsRate_thenEmitsAtRate() {
    Rate rate = Rate.of(1, Duration.standardSeconds(1L));
    long expectedMillis = rate.getInterval().getMillis();
    // tolerate 5% error.
    long toleratedError = (long) (0.05 * (double) expectedMillis);
    Duration expectedInterval = Duration.millis(expectedMillis - toleratedError);
    List<Integer> items = Stream.iterate(0, i -> i + 1).limit(3).collect(Collectors.toList());
    PCollection<Integer> throttled = pipeline.apply(Create.of(items)).apply(transformOf(rate));

    PAssert.that(throttled).containsInAnyOrder(items);
    PAssert.that(timestampsOf(throttled))
        .satisfies(
            itr -> {
              List<Instant> timestamps =
                  StreamSupport.stream(itr.spliterator(), true)
                      .sorted()
                      .collect(Collectors.toList());
              assertTimestampIntervalsMatch(timestamps, greaterThan(expectedInterval));
              return null;
            });

    pipeline.run();
  }

  @Test
  public void givenLargerElementSize_noDataLost() {
    Rate rate = Rate.of(1_000, Duration.standardSeconds(1L));
    List<Integer> items = Stream.iterate(0, i -> i + 1).limit(3_000).collect(Collectors.toList());
    PCollection<Integer> throttled = pipeline.apply(Create.of(items)).apply(transformOf(rate));

    PAssert.that(throttled).containsInAnyOrder(items);

    pipeline.run();
  }

  /** Tests withMetricsCollected that Counters populate appropriately. */
  @Test
  public void givenCollectMetricsTrue_thenPopulatesMetrics() {
    long size = 300;
    Rate rate = Rate.of(100, Duration.standardSeconds(1L));

    List<Integer> list = Stream.iterate(0, i -> i + 1).limit(size).collect(Collectors.toList());

    pipeline.apply(Create.of(list)).apply(transformOf(rate).withMetricsCollected());

    PipelineResult pipelineResult = pipeline.run();
    pipelineResult.waitUntilFinish();
    MetricResults results = pipelineResult.metrics();

    assertThat(getCount(results, INPUT_ELEMENTS_COUNTER_NAME), equalTo(size));
    assertThat(getCount(results, OUTPUT_ELEMENTS_COUNTER_NAME), equalTo(size));
  }

  /** Tests that an upstream GlobalWindows gets reapplied to the resulting PCollection. */
  @Test
  public void givenUpstreamGlobalWindows_thenReassignedToGlobalWindow() {
    PCollection<Integer> throttled =
        pipeline
            .apply(Create.of(1, 2, 3))
            .apply(Window.into(new GlobalWindows()))
            .apply(transformOf(Rate.of(1, Duration.standardSeconds(1L))));

    assertThat(throttled.getWindowingStrategy().getWindowFn(), equalTo(new GlobalWindows()));

    pipeline.run();
  }

  /** Tests that an upstream SessionWindows gets reapplied to the resulting PCollection. */
  @Test
  public void givenUpstreamSessionWindows_thenReassignedToSessionWindows() {
    PCollection<Integer> throttled =
        pipeline
            .apply(Create.of(1, 2, 3))
            .apply(Window.into(Sessions.withGapDuration(Duration.standardSeconds(1L))))
            .apply(transformOf(Rate.of(1, Duration.standardSeconds(1L))));

    assertThat(
        throttled.getWindowingStrategy().getWindowFn(),
        equalTo(Sessions.withGapDuration(Duration.standardSeconds(1L))));

    pipeline.run();
  }

  /**
   * Tests that an upstream Default Window and Trigger gets reapplied to the resulting PCollection.
   */
  @Test
  public void givenUpstreamDefaultWindow_thenReassignedDefaultWindow() {
    PCollection<Integer> throttled =
        pipeline
            .apply(Create.of(1, 2, 3))
            .apply(transformOf(Rate.of(1, Duration.standardSeconds(1L))));

    assertThat(throttled.getWindowingStrategy().getWindowFn(), equalTo(new GlobalWindows()));
    assertThat(throttled.getWindowingStrategy().getTrigger(), equalTo(DefaultTrigger.of()));

    pipeline.run();
  }

  /** Tests whether downstream Windows can be applied. */
  @Test
  public void canReassignDownstreamWindow() {
    PCollection<Integer> throttled =
        pipeline
            .apply(Create.of(1, 2, 3))
            .apply(transformOf(Rate.of(1, Duration.standardSeconds(1L))))
            .apply(Window.into(Sessions.withGapDuration(Duration.standardSeconds(2L))));

    assertThat(
        throttled.getWindowingStrategy().getWindowFn(),
        equalTo(Sessions.withGapDuration(Duration.standardSeconds(2L))));
    assertThat(throttled.getWindowingStrategy().getTrigger(), equalTo(DefaultTrigger.of()));

    pipeline.run();
  }

  @Test
  public void givenStreamSource_thenThrottles() {
    Rate rate = Rate.of(1, Duration.standardSeconds(1L));
    long intervalMillis = rate.getInterval().getMillis();
    long allowedError = (long) ((double) intervalMillis * 0.05);
    Duration expectedInterval = Duration.millis(intervalMillis - allowedError);
    PCollection<Integer> stream =
        pipeline
            .apply(
                PeriodicImpulse.create().stopAfter(Duration.ZERO).withInterval(Duration.millis(1L)))
            .apply(generate(3))
            .setCoder(VarIntCoder.of());

    PAssert.that(stream).containsInAnyOrder(0, 1, 2);

    PCollection<Integer> throttled = stream.apply(transformOf(rate).withStreamingConfiguration(3L));
    PAssert.that(throttled).containsInAnyOrder(0, 1, 2);
    PAssert.that(timestampsOf(throttled))
        .satisfies(
            itr -> {
              List<Instant> timestamps =
                  StreamSupport.stream(itr.spliterator(), true)
                      .sorted()
                      .collect(Collectors.toList());
              assertTimestampIntervalsMatch(timestamps, greaterThan(expectedInterval));
              return null;
            });

    pipeline.run();
  }

  @Test
  public void givenLargerStreamSource_noDataLost() {
    Rate rate = Rate.of(2_000, Duration.standardSeconds(1L));
    List<Integer> items = Stream.iterate(0, i -> i + 1).limit(4_000).collect(Collectors.toList());
    PCollection<Integer> stream =
        pipeline
            .apply(
                PeriodicImpulse.create()
                    .stopAfter(Duration.millis(3L))
                    .withInterval(Duration.millis(1L)))
            .apply(generate(1_000))
            .setCoder(VarIntCoder.of());

    PAssert.that(stream).containsInAnyOrder(items);

    PCollection<Integer> throttled =
        stream.apply(transformOf(rate).withStreamingConfiguration(items.size()));
    PAssert.that(throttled).containsInAnyOrder(items);

    pipeline.run();
  }

  private static ParDo.SingleOutput<Instant, Integer> generate(int size) {
    AtomicInteger atomicInteger = new AtomicInteger();
    return ParDo.of(
        new Generator<>(
            ignored -> Stream.generate(atomicInteger::getAndIncrement).limit(size).iterator()));
  }

  private static class Generator<T> extends DoFn<Instant, T> {
    private final SerializableFunction<Instant, Iterator<T>> generatorFn;

    private Generator(SerializableFunction<Instant, Iterator<T>> generatorFn) {
      this.generatorFn = generatorFn;
    }

    @ProcessElement
    public void process(@Element Instant element, OutputReceiver<T> receiver) {
      Iterator<T> iterator = generatorFn.apply(element);
      while (iterator.hasNext()) {
        receiver.output(iterator.next());
      }
    }
  }

  private static Throttle<Integer> transformOf(Rate rate) {
    return Throttle.of(rate);
  }

  private static Long getCount(MetricResults metricResults, String name) {
    MetricQueryResults metricQueryResults =
        metricResults.queryMetrics(
            MetricsFilter.builder()
                .addNameFilter(MetricNameFilter.named(Throttle.class, name))
                .build());
    assertThat(metricQueryResults, notNullValue());
    Iterator<MetricResult<Long>> itr = metricQueryResults.getCounters().iterator();
    assertThat(itr.hasNext(), equalTo(true));
    return itr.next().getCommitted();
  }

  private static PCollection<Instant> timestampsOf(PCollection<?> pCollection) {
    return pCollection.apply(
        "timestampsOf",
        MapElements.into(TypeDescriptor.of(Instant.class)).via(ignored -> Instant.now()));
  }

  private static void assertTimestampIntervalsMatch(
      List<Instant> timestamps, Matcher<ReadableDuration> matcher) {
    assertThat("there should be more than 1 timestamp", timestamps.size(), greaterThan(1));
    Instant current = timestamps.get(0);
    for (int i = 1; i < timestamps.size(); i++) {
      Instant next = timestamps.get(i);
      Duration diff = Duration.millis(next.getMillis() - current.getMillis());
      assertThat(diff, matcher);
      current = next;
    }
  }
}
