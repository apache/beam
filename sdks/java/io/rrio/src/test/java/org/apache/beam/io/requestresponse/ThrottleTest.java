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
import static org.apache.beam.sdk.values.TypeDescriptors.longs;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterSynchronizedProcessingTime;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
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
  public void givenSparseElementPulse_thenEmitsAllImmediately() {
    Rate rate = Rate.of(1000, Duration.standardSeconds(1L));
    testEmitsAtRate(rate, 10, 1.0);
  }

  /** Tests whether a pulse of elements totaled greater than the maximum rate are throttled. */
  @Test
  public void givenNonSparseElementPulse_thenEmitsAtRate() {
    long size = 3000;
    Rate rate = Rate.of(1000, Duration.standardSeconds(1L));
    double expectedMean = (double) rate.getInterval().getMillis() / (double) rate.getNumElements();
    testEmitsAtRate(rate, size, expectedMean);
  }

  /** Tests withMetricsCollected that Counters populate appropriately. */
  @Test
  public void givenCollectMetricsTrue_thenPopulatesMetrics() {
    long size = 3000;
    Rate rate = Rate.of(1000, Duration.standardSeconds(1L));

    List<Integer> list = Stream.iterate(0, i -> i + 1).limit(size).collect(Collectors.toList());

    pipeline.apply(Create.of(list)).apply(transformOf(rate).withMetricsCollected());

    PipelineResult pipelineResult = pipeline.run();
    pipelineResult.waitUntilFinish();
    MetricResults results = pipelineResult.metrics();

    assertThat(getCount(results, INPUT_ELEMENTS_COUNTER_NAME), is(size));
    assertThat(getCount(results, OUTPUT_ELEMENTS_COUNTER_NAME), is(size));
  }

  /**
   * Helper method to test emission timestamp intervals according to the rate and size of the
   * element count, asserting that the resulting intervals between elements is the expectedMean +/-
   * 5%. Also validates upstream assignment of FixedWindows gets reapplied to resulting PCollection.
   */
  private void testEmitsAtRate(Rate rate, long size, double expectedMeanMillis) {
    checkArgument(size > 0);

    List<Integer> list = Stream.iterate(0, i -> i + 1).limit(size).collect(Collectors.toList());

    WindowFn<? super Integer, ?> windowFn = FixedWindows.of(Duration.standardHours(1L));
    Trigger trigger = Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane());

    PCollection<Integer> throttled =
        pipeline
            .apply(Create.of(list))
            .apply(
                Window.<Integer>into(windowFn)
                    .triggering(trigger)
                    .withAllowedLateness(Duration.standardSeconds(10L))
                    .discardingFiredPanes())
            .apply(transformOf(rate));

    PAssert.that(throttled).containsInAnyOrder(list);
    // Validate window reassignment.
    assertThat(throttled.getWindowingStrategy().getWindowFn(), is(windowFn));
    assertThat(
        throttled.getWindowingStrategy().getTrigger(),
        is(Repeatedly.forever(AfterSynchronizedProcessingTime.ofFirstElement())));

    PCollection<Long> elementTimestamps =
        throttled.apply(MapElements.into(longs()).via(ignored -> Instant.now().getMillis()));

    PAssert.that(elementTimestamps)
        .satisfies(
            itr -> {
              List<Long> timestamps =
                  StreamSupport.stream(itr.spliterator(), false)
                      .sorted()
                      .collect(Collectors.toList());

              double sum = 0.0;
              Long previous = timestamps.get(0);

              for (int i = 1; i < timestamps.size(); i++) {
                long interval = timestamps.get(i) - previous;
                sum += interval;
                previous = timestamps.get(i);
              }

              double mean = sum / (double) size;
              assertWithin5PercentOf(expectedMeanMillis, mean);

              return null;
            });

    pipeline.run();
  }

  /** Tests that an upstream GlobalWindows gets reapplied to the resulting PCollection. */
  @Test
  public void givenUpstreamGlobalWindows_thenReassignedToGlobalWindow() {
    PCollection<Integer> throttled =
        pipeline
            .apply(Create.of(1, 2, 3))
            .apply(Window.into(new GlobalWindows()))
            .apply(transformOf(Rate.of(1, Duration.standardSeconds(1L))));

    assertThat(throttled.getWindowingStrategy().getWindowFn(), is(new GlobalWindows()));

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
        is(Sessions.withGapDuration(Duration.standardSeconds(1L))));

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

    assertThat(throttled.getWindowingStrategy().getWindowFn(), is(new GlobalWindows()));
    assertThat(throttled.getWindowingStrategy().getTrigger(), is(DefaultTrigger.of()));

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
        is(Sessions.withGapDuration(Duration.standardSeconds(2L))));
    assertThat(throttled.getWindowingStrategy().getTrigger(), is(DefaultTrigger.of()));

    pipeline.run();
  }

  private static void assertWithin5PercentOf(double expected, double observed) {
    assertThat(0.95 * expected <= observed || observed <= 1.05 * expected, is(true));
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
    assertThat(itr.hasNext(), is(true));
    return itr.next().getCommitted();
  }
}
