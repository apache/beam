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

package org.apache.beam.runners.core.metrics;

import static org.apache.beam.runners.core.metrics.MetricsContainerStepMap.asAttemptedOnlyMetricResults;
import static org.apache.beam.runners.core.metrics.MetricsContainerStepMap.asMetricResults;
import static org.apache.beam.sdk.metrics.MetricResultsMatchers.metricsResult;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;

import java.io.Closeable;
import java.io.IOException;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.hamcrest.collection.IsIterableWithSize;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests for {@link MetricsContainerStepMap}.
 */
public class MetricsContainerStepMapTest {

  private static final String NAMESPACE = MetricsContainerStepMapTest.class.getName();
  private static final String STEP1 = "myStep1";
  private static final String STEP2 = "myStep2";
  private static final String COUNTER_NAME = "myCounter";
  private static final String DISTRIBUTION_NAME = "myDistribution";
  private static final String GAUGE_NAME = "myGauge";

  private static final long VALUE = 100;

  private static final Counter counter =
      Metrics.counter(MetricsContainerStepMapTest.class, COUNTER_NAME);
  private static final Distribution distribution =
      Metrics.distribution(MetricsContainerStepMapTest.class, DISTRIBUTION_NAME);
  private static final Gauge gauge =
      Metrics.gauge(MetricsContainerStepMapTest.class, GAUGE_NAME);

  private static final MetricsContainerImpl metricsContainer;

  static {
    metricsContainer = new MetricsContainerImpl(null);
    try (Closeable ignored = MetricsEnvironment.scopedMetricsContainer(metricsContainer)) {
      counter.inc(VALUE);
      distribution.update(VALUE);
      distribution.update(VALUE * 2);
      gauge.set(VALUE);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Rule
  public transient ExpectedException thrown = ExpectedException.none();

  @Test
  public void testAttemptedAccumulatedMetricResults() {
    MetricsContainerStepMap attemptedMetrics = new MetricsContainerStepMap();
    attemptedMetrics.update(STEP1, metricsContainer);
    attemptedMetrics.update(STEP2, metricsContainer);
    attemptedMetrics.update(STEP2, metricsContainer);

    MetricResults metricResults =
        asAttemptedOnlyMetricResults(attemptedMetrics);

    MetricQueryResults step1res =
        metricResults.queryMetrics(MetricsFilter.builder().addStep(STEP1).build());

    assertIterableSize(step1res.counters(), 1);
    assertIterableSize(step1res.distributions(), 1);
    assertIterableSize(step1res.gauges(), 1);

    assertCounter(COUNTER_NAME, step1res, STEP1, VALUE, false);
    assertDistribution(DISTRIBUTION_NAME,
        step1res, STEP1, DistributionResult.create(VALUE * 3, 2, VALUE, VALUE * 2),
        false);
    assertGauge(GAUGE_NAME, step1res, STEP1, GaugeResult.create(VALUE, Instant.now()), false);

    MetricQueryResults step2res =
        metricResults.queryMetrics(MetricsFilter.builder().addStep(STEP2).build());

    assertIterableSize(step2res.counters(), 1);
    assertIterableSize(step2res.distributions(), 1);
    assertIterableSize(step2res.gauges(), 1);

    assertCounter(COUNTER_NAME, step2res, STEP2, VALUE * 2, false);
    assertDistribution(
        DISTRIBUTION_NAME, step2res, STEP2,
        DistributionResult.create(VALUE * 6, 4, VALUE, VALUE * 2), false);
    assertGauge(
        GAUGE_NAME, step2res, STEP2, GaugeResult.create(VALUE, Instant.now()), false);

    MetricQueryResults allres =
        metricResults.queryMetrics(MetricsFilter.builder().build());

    assertIterableSize(allres.counters(), 2);
    assertIterableSize(allres.distributions(), 2);
    assertIterableSize(allres.gauges(), 2);
  }

  @Test
  public void testCounterCommittedUnsupportedInAttemptedAccumulatedMetricResults() {
    MetricsContainerStepMap attemptedMetrics = new MetricsContainerStepMap();
    attemptedMetrics.update(STEP1, metricsContainer);
    MetricResults metricResults =
        asAttemptedOnlyMetricResults(attemptedMetrics);

    MetricQueryResults step1res =
        metricResults.queryMetrics(MetricsFilter.builder().addStep(STEP1).build());

    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("This runner does not currently support committed metrics results.");

    assertCounter(COUNTER_NAME, step1res, STEP1, VALUE, true);
  }

  @Test
  public void testDistributionCommittedUnsupportedInAttemptedAccumulatedMetricResults() {
    MetricsContainerStepMap attemptedMetrics = new MetricsContainerStepMap();
    attemptedMetrics.update(STEP1, metricsContainer);
    MetricResults metricResults =
        asAttemptedOnlyMetricResults(attemptedMetrics);

    MetricQueryResults step1res =
        metricResults.queryMetrics(MetricsFilter.builder().addStep(STEP1).build());

    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("This runner does not currently support committed metrics results.");

    assertDistribution(
        DISTRIBUTION_NAME, step1res, STEP1, DistributionResult.IDENTITY_ELEMENT, true);
  }

  @Test
  public void testGaugeCommittedUnsupportedInAttemptedAccumulatedMetricResults() {
    MetricsContainerStepMap attemptedMetrics = new MetricsContainerStepMap();
    attemptedMetrics.update(STEP1, metricsContainer);
    MetricResults metricResults =
        asAttemptedOnlyMetricResults(attemptedMetrics);

    MetricQueryResults step1res =
        metricResults.queryMetrics(MetricsFilter.builder().addStep(STEP1).build());

    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("This runner does not currently support committed metrics results.");

    assertGauge(GAUGE_NAME, step1res, STEP1, GaugeResult.empty(), true);
  }

  @Test
  public void testAttemptedAndCommittedAccumulatedMetricResults() {
    MetricsContainerStepMap attemptedMetrics = new MetricsContainerStepMap();
    attemptedMetrics.update(STEP1, metricsContainer);
    attemptedMetrics.update(STEP1, metricsContainer);
    attemptedMetrics.update(STEP2, metricsContainer);
    attemptedMetrics.update(STEP2, metricsContainer);
    attemptedMetrics.update(STEP2, metricsContainer);

    MetricsContainerStepMap committedMetrics = new MetricsContainerStepMap();
    committedMetrics.update(STEP1, metricsContainer);
    committedMetrics.update(STEP2, metricsContainer);
    committedMetrics.update(STEP2, metricsContainer);

    MetricResults metricResults =
        asMetricResults(attemptedMetrics, committedMetrics);

    MetricQueryResults step1res =
        metricResults.queryMetrics(MetricsFilter.builder().addStep(STEP1).build());

    assertIterableSize(step1res.counters(), 1);
    assertIterableSize(step1res.distributions(), 1);
    assertIterableSize(step1res.gauges(), 1);

    assertCounter(COUNTER_NAME, step1res, STEP1, VALUE * 2, false);
    assertDistribution(
        DISTRIBUTION_NAME, step1res, STEP1,
        DistributionResult.create(VALUE * 6, 4, VALUE, VALUE * 2), false);
    assertGauge(GAUGE_NAME, step1res, STEP1, GaugeResult.create(VALUE, Instant.now()), false);

    assertCounter(COUNTER_NAME, step1res, STEP1, VALUE, true);
    assertDistribution(DISTRIBUTION_NAME, step1res, STEP1,
        DistributionResult.create(VALUE * 3, 2, VALUE, VALUE * 2), true);
    assertGauge(GAUGE_NAME, step1res, STEP1, GaugeResult.create(VALUE, Instant.now()), true);

    MetricQueryResults step2res =
        metricResults.queryMetrics(MetricsFilter.builder().addStep(STEP2).build());

    assertIterableSize(step2res.counters(), 1);
    assertIterableSize(step2res.distributions(), 1);
    assertIterableSize(step2res.gauges(), 1);

    assertCounter(COUNTER_NAME, step2res, STEP2, VALUE * 3, false);
    assertDistribution(DISTRIBUTION_NAME, step2res, STEP2,
        DistributionResult.create(VALUE * 9, 6, VALUE, VALUE * 2), false);
    assertGauge(GAUGE_NAME, step2res, STEP2, GaugeResult.create(VALUE, Instant.now()), false);

    assertCounter(COUNTER_NAME, step2res, STEP2, VALUE * 2, true);
    assertDistribution(DISTRIBUTION_NAME, step2res, STEP2,
        DistributionResult.create(VALUE * 6, 4, VALUE, VALUE * 2), true);
    assertGauge(GAUGE_NAME, step2res, STEP2, GaugeResult.create(VALUE, Instant.now()), true);

    MetricQueryResults allres =
        metricResults.queryMetrics(MetricsFilter.builder().build());

    assertIterableSize(allres.counters(), 2);
    assertIterableSize(allres.distributions(), 2);
    assertIterableSize(allres.gauges(), 2);
  }

  private <T> void assertIterableSize(Iterable<T> iterable, int size) {
    assertThat(iterable, IsIterableWithSize.iterableWithSize(size));
  }

  private void assertCounter(
      String name,
      MetricQueryResults metricQueryResults,
      String step,
      Long expected,
      boolean isCommitted) {
    assertThat(
        metricQueryResults.counters(),
        hasItem(metricsResult(NAMESPACE, name, step, expected, isCommitted)));
  }

  private void assertDistribution(
      String name,
      MetricQueryResults metricQueryResults,
      String step,
      DistributionResult expected,
      boolean isCommitted) {
    assertThat(
        metricQueryResults.distributions(),
        hasItem(metricsResult(NAMESPACE, name, step, expected, isCommitted)));
  }

  private void assertGauge(
      String name,
      MetricQueryResults metricQueryResults,
      String step,
      GaugeResult expected,
      boolean isCommitted) {
    assertThat(
        metricQueryResults.gauges(),
        hasItem(metricsResult(NAMESPACE, name, step, expected, isCommitted)));
  }
}
