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

import com.google.protobuf.ByteString;
import java.util.Arrays;
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
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.testinfra.mockapis.echo.v1.Echo;
import org.hamcrest.Matcher;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.ReadableDuration;
import org.junit.Ignore;
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
    int numElements = 10;
    Duration interval = Duration.standardSeconds(1L);
    long expectedMillis = interval.getMillis();
    long toleratedError = (long) (0.05 * (double) expectedMillis);
    // tolerate 5% error.
    Duration expectedInterval = Duration.millis(expectedMillis + toleratedError);
    List<Integer> items = Stream.iterate(0, i -> i + 1).limit(3).collect(Collectors.toList());
    PCollection<Integer> throttled =
        pipeline.apply(Create.of(items)).apply(Throttle.of(numElements, interval));

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
    int numElements = 1;
    Duration interval = Duration.standardSeconds(1L);
    long expectedMillis = interval.getMillis();
    // tolerate 5% error.
    long toleratedError = (long) (0.05 * (double) expectedMillis);
    Duration expectedInterval = Duration.millis(expectedMillis - toleratedError);
    List<Integer> items = Stream.iterate(0, i -> i + 1).limit(3).collect(Collectors.toList());
    PCollection<Integer> throttled =
        pipeline.apply(Create.of(items)).apply(Throttle.of(numElements, interval));

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
    List<Integer> items = Stream.iterate(0, i -> i + 1).limit(3_000).collect(Collectors.toList());
    PCollection<Integer> throttled =
        pipeline.apply(Create.of(items)).apply(Throttle.of(1_000, Duration.standardSeconds(1L)));

    PAssert.that(throttled).containsInAnyOrder(items);

    pipeline.run();
  }

  /** Tests withMetricsCollected that Counters populate appropriately. */
  @Test
  public void givenCollectMetricsTrue_thenPopulatesMetrics() {
    long size = 300;

    List<Integer> list = Stream.iterate(0, i -> i + 1).limit(size).collect(Collectors.toList());

    pipeline
        .apply(Create.of(list))
        .apply(Throttle.<Integer>of(100, Duration.standardSeconds(1L)).withMetricsCollected());

    PipelineResult pipelineResult = pipeline.run();
    pipelineResult.waitUntilFinish();
    MetricResults results = pipelineResult.metrics();

    assertThat(getCount(results, INPUT_ELEMENTS_COUNTER_NAME), equalTo(size));
    assertThat(getCount(results, OUTPUT_ELEMENTS_COUNTER_NAME), equalTo(size));
  }

  /**
   * Tests that a stream PCollection is throttled. Note that TestStream does not work well as it
   * fails to set the process timer clock. Therefore, PeriodicImpulse is used instead to test
   * against a more realistic stream PCollection instead of a fake one.
   */
  // TODO(damondouglas): remove @Ignore annotation when state and timer implementation finished.
  @Ignore("Needs state and timer implementation")
  @Test
  public void givenStreamSource_thenThrottles() {
    int numElements = 1;
    Duration interval = Duration.standardSeconds(1L);
    long intervalMillis = interval.getMillis();
    long allowedError = (long) ((double) intervalMillis * 0.05);
    Duration expectedInterval = Duration.millis(intervalMillis - allowedError);
    PCollection<Integer> stream =
        pipeline
            .apply(
                PeriodicImpulse.create().stopAfter(Duration.ZERO).withInterval(Duration.millis(1L)))
            .apply(generatePerImpulse(3))
            .setCoder(VarIntCoder.of());

    PAssert.that(stream).containsInAnyOrder(0, 1, 2);

    PCollection<Integer> throttled = stream.apply(Throttle.of(numElements, interval));
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

  /** Tests that given a feasibly larger dataset for unittests does not result in lost data. */
  // TODO(damondouglas): remove @Ignore annotation when state and timer implementation finished.
  @Ignore("Needs state and timer implementation")
  @Test
  public void givenLargerStreamSource_noDataLost() {
    int numElements = 2_000;
    Duration interval = Duration.standardSeconds(1L);
    List<Integer> items = Stream.iterate(0, i -> i + 1).limit(4_000).collect(Collectors.toList());
    PCollection<Integer> stream =
        pipeline
            .apply(
                PeriodicImpulse.create()
                    .stopAfter(Duration.millis(3L))
                    .withInterval(Duration.millis(1L)))
            .apply(generatePerImpulse(1_000))
            .setCoder(VarIntCoder.of());

    PAssert.that(stream).containsInAnyOrder(items);

    PCollection<Integer> throttled = stream.apply(Throttle.of(numElements, interval));
    PAssert.that(throttled).containsInAnyOrder(items);

    pipeline.run();
  }

  /** Validates that the transform doesn't complain about coders for custom user types. */
  @Test
  public void givenCustomUserType_canProcessWithoutComplainingAboutCoders() {
    List<Echo.EchoRequest> items =
        Arrays.asList(
            Echo.EchoRequest.newBuilder()
                .setId("1")
                .setPayload(ByteString.copyFromUtf8("1"))
                .build(),
            Echo.EchoRequest.newBuilder()
                .setId("2")
                .setPayload(ByteString.copyFromUtf8("2"))
                .build(),
            Echo.EchoRequest.newBuilder()
                .setId("3")
                .setPayload(ByteString.copyFromUtf8("3"))
                .build());

    PCollection<Echo.EchoRequest> throttled =
        pipeline.apply(Create.of(items)).apply(Throttle.of(3, Duration.standardSeconds(1L)));

    PAssert.that(throttled).containsInAnyOrder(items);

    pipeline.run();
  }

  /** During batch, validates upstream Window assignments are preserved. */
  @Test
  public void givenUpstreamNonGlobalWindow_thenPreservesWindowAssignments() {
    PCollection<Integer> unthrottled =
        pipeline
            .apply(
                Create.timestamped(
                    TimestampedValue.of(1, Instant.EPOCH),
                    TimestampedValue.of(1, epochPlus(1_000L)),
                    TimestampedValue.of(1, epochPlus(2_000L)),
                    TimestampedValue.of(1, epochPlus(3_000L)),
                    TimestampedValue.of(1, epochPlus(4_000L)),
                    TimestampedValue.of(2, epochPlus(5_000L)),
                    TimestampedValue.of(2, epochPlus(6_000L)),
                    TimestampedValue.of(2, epochPlus(7_000L)),
                    TimestampedValue.of(2, epochPlus(8_000L)),
                    TimestampedValue.of(2, epochPlus(9_000L))))
            .apply(
                Window.<Integer>into(FixedWindows.of(Duration.standardSeconds(5L)))
                    .triggering(AfterWatermark.pastEndOfWindow())
                    .withAllowedLateness(Duration.ZERO)
                    .discardingFiredPanes());

    PCollection<Integer> throttled =
        unthrottled.apply(Throttle.of(10, Duration.standardSeconds(1L)));
    PAssert.that(throttled.apply(Sum.integersGlobally().withoutDefaults()))
        .containsInAnyOrder(5, 10);

    pipeline.run();
  }

  @Test
  public void givenUpstreamNonGlobalWindow_canReassignDownstreamWindow() {
    PCollection<Integer> unthrottled =
        pipeline
            .apply(
                Create.timestamped(
                    TimestampedValue.of(1, Instant.EPOCH),
                    TimestampedValue.of(1, epochPlus(1_000L)),
                    TimestampedValue.of(1, epochPlus(2_000L)),
                    TimestampedValue.of(1, epochPlus(3_000L)),
                    TimestampedValue.of(1, epochPlus(4_000L)),
                    TimestampedValue.of(2, epochPlus(5_000L)),
                    TimestampedValue.of(2, epochPlus(6_000L)),
                    TimestampedValue.of(2, epochPlus(7_000L)),
                    TimestampedValue.of(2, epochPlus(8_000L)),
                    TimestampedValue.of(2, epochPlus(9_000L))))
            .apply(
                "before",
                Window.<Integer>into(FixedWindows.of(Duration.standardSeconds(1L)))
                    .triggering(AfterWatermark.pastEndOfWindow())
                    .withAllowedLateness(Duration.ZERO)
                    .discardingFiredPanes());

    PCollection<Integer> throttled =
        unthrottled
            .apply(Throttle.of(10, Duration.standardSeconds(1L)))
            .apply(
                "after",
                Window.<Integer>into(FixedWindows.of(Duration.standardSeconds(5L)))
                    .triggering(AfterWatermark.pastEndOfWindow())
                    .withAllowedLateness(Duration.ZERO)
                    .discardingFiredPanes());

    PAssert.that(throttled.apply(Sum.integersGlobally().withoutDefaults()))
        .containsInAnyOrder(5, 10);

    pipeline.run();
  }

  private static ParDo.SingleOutput<Instant, Integer> generatePerImpulse(int size) {
    AtomicInteger atomicInteger = new AtomicInteger();
    return ParDo.of(
        new GeneratePerImpulseFn<>(
            ignored -> Stream.generate(atomicInteger::getAndIncrement).limit(size).iterator()));
  }

  private static class GeneratePerImpulseFn<T> extends DoFn<Instant, T> {
    private final SerializableFunction<Instant, Iterator<T>> generatorFn;

    private GeneratePerImpulseFn(SerializableFunction<Instant, Iterator<T>> generatorFn) {
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

  private static Instant epochPlus(long millis) {
    return Instant.EPOCH.plus(Duration.millis(millis));
  }
}
