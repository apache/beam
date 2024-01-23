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

import static org.apache.beam.io.requestresponse.ThrottleWithoutExternalResource.DISTRIBUTION_METRIC_NAME;
import static org.apache.beam.io.requestresponse.ThrottleWithoutExternalResource.INPUT_ELEMENTS_COUNTER_NAME;
import static org.apache.beam.io.requestresponse.ThrottleWithoutExternalResource.OUTPUT_ELEMENTS_COUNTER_NAME;
import static org.apache.beam.sdk.values.TypeDescriptors.longs;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ThrottleWithoutExternalResource}. */
@RunWith(JUnit4.class)
public class ThrottleWithoutExternalResourceTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();

  /**
   * Tests whether a pulse of elements totaled less than the maximum rate are just emitted
   * immediately without throttling.
   */
  @Test
  public void givenSparseElementPulse_thenEmitsAllImmediately() {
    Rate rate = Rate.of(1000, Duration.standardSeconds(1L));
    testEmitsAtRate(rate, 100, 1.0);
  }

  /** Tests whether a pulse of elements totaled greater than the maximum rate are throttled. */
  @Test
  public void givenNonSparseElementPulse_thenEmitsAtRate() {
    long size = 3000;
    Rate rate = Rate.of(1000, Duration.standardSeconds(1L));
    double expectedMean = (double) rate.getInterval().getMillis() / (double) rate.getNumElements();
    testEmitsAtRate(rate, size, expectedMean);
  }

  /**
   * Helper method to test emission timestamp intervals according to the rate and size of the
   * element count, asserting that the resulting intervals between elements is the expectedMean +/-
   * 5%.
   */
  private void testEmitsAtRate(Rate rate, long size, double expectedMeanMillis) {
    checkArgument(size > 0);

    List<Integer> list = Stream.iterate(0, i -> i + 1).limit(size).collect(Collectors.toList());

    PCollection<Integer> throttled =
        pipeline.apply(Create.of(list)).apply(transformOf(rate)).getResponses();

    PAssert.that(throttled).containsInAnyOrder(list);

    PCollection<Long> elementTimestamps =
        throttled.apply(MapElements.into(longs()).via(ignored -> Instant.now().getMillis()));

    PAssert.that(elementTimestamps)
        .satisfies(
            itr -> {
              List<Long> timestamps =
                  StreamSupport.stream(itr.spliterator(), false)
                      .sorted()
                      .collect(Collectors.toList());
              assertThat(timestamps.size(), is((int) size));
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

  @Test
  public void givenCollectMetricsTrue_thenPopulatesMetrics() {
    long size = 1_000;
    Rate rate = Rate.of(10, Duration.standardSeconds(1L));
    double expectedMean = 1.0;
    double expectedMin = 0.0;
    double expectedMax = 100.0;

    List<Integer> list = Stream.iterate(0, i -> i + 1).limit(size).collect(Collectors.toList());

    pipeline.apply(Create.of(list)).apply(transformOf(rate).withMetricsCollected());

    PipelineResult pipelineResult = pipeline.run();
    pipelineResult.waitUntilFinish();
    MetricResults results = pipelineResult.metrics();
    DistributionResult distributionResult = getDistribution(results, DISTRIBUTION_METRIC_NAME);
    long count = distributionResult.getCount();

    double mean = distributionResult.getMean();
    double min = distributionResult.getMin();
    double max = distributionResult.getMax();

    assertThat(count, is(size - 1));
    assertWithin5PercentOf(expectedMean, mean);
    assertThat(min, greaterThanOrEqualTo(expectedMin));
    assertThat(max, lessThanOrEqualTo(expectedMax));

    assertThat(getCount(results, INPUT_ELEMENTS_COUNTER_NAME), is(size));
    assertThat(getCount(results, OUTPUT_ELEMENTS_COUNTER_NAME), is(size));
  }

  private static void assertWithin5PercentOf(double expected, double observed) {
    assertThat(0.95 * expected <= observed || observed <= 1.05 * expected, is(true));
  }

  private static ThrottleWithoutExternalResource<Integer> transformOf(Rate rate) {
    return ThrottleWithoutExternalResource.of(rate);
  }

  private static DistributionResult getDistribution(MetricResults metricResults, String name) {
    MetricQueryResults metricQueryResults =
        metricResults.queryMetrics(
            MetricsFilter.builder()
                .addNameFilter(
                    MetricNameFilter.named(ThrottleWithoutExternalResource.ThrottleFn.class, name))
                .build());
    assertThat(metricQueryResults, notNullValue());
    Iterator<MetricResult<DistributionResult>> itr =
        metricQueryResults.getDistributions().iterator();
    assertThat(itr.hasNext(), is(true));
    return itr.next().getCommitted();
  }

  private static Long getCount(MetricResults metricResults, String name) {
    MetricQueryResults metricQueryResults =
        metricResults.queryMetrics(
            MetricsFilter.builder()
                .addNameFilter(
                    MetricNameFilter.named(ThrottleWithoutExternalResource.ThrottleFn.class, name))
                .build());
    assertThat(metricQueryResults, notNullValue());
    Iterator<MetricResult<Long>> itr = metricQueryResults.getCounters().iterator();
    assertThat(itr.hasNext(), is(true));
    return itr.next().getCommitted();
  }
}
