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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.metrics.StringSet;
import org.apache.beam.sdk.metrics.StringSetResult;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.hamcrest.collection.IsIterableWithSize;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for {@link MetricsContainerStepMap}. */
public class MetricsContainerStepMapTest {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsContainerStepMapTest.class);

  private static final String NAMESPACE = MetricsContainerStepMapTest.class.getName();
  private static final String STEP1 = "myStep1";
  private static final String STEP2 = "myStep2";
  private static final String COUNTER_NAME = "myCounter";
  private static final String DISTRIBUTION_NAME = "myDistribution";
  private static final String GAUGE_NAME = "myGauge";

  private static final String STRING_SET_NAME = "myStringSet";

  private static final long VALUE = 100;

  private static final String FIRST_STRING = "first";
  private static final String SECOND_STRING = "second";

  private static final Counter counter =
      Metrics.counter(MetricsContainerStepMapTest.class, COUNTER_NAME);
  private static final Distribution distribution =
      Metrics.distribution(MetricsContainerStepMapTest.class, DISTRIBUTION_NAME);
  private static final Gauge gauge = Metrics.gauge(MetricsContainerStepMapTest.class, GAUGE_NAME);

  private static final StringSet stringSet =
      Metrics.stringSet(MetricsContainerStepMapTest.class, STRING_SET_NAME);

  private static final MetricsContainerImpl metricsContainer;

  static {
    metricsContainer = new MetricsContainerImpl(null);
    try (Closeable ignored = MetricsEnvironment.scopedMetricsContainer(metricsContainer)) {
      counter.inc(VALUE);
      distribution.update(VALUE);
      distribution.update(VALUE * 2);
      gauge.set(VALUE);
      stringSet.add(FIRST_STRING, SECOND_STRING);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
    }
  }

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @Test
  public void testAttemptedAccumulatedMetricResults() {
    MetricsContainerStepMap attemptedMetrics = new MetricsContainerStepMap();
    attemptedMetrics.update(STEP1, metricsContainer);
    attemptedMetrics.update(STEP2, metricsContainer);
    attemptedMetrics.update(STEP2, metricsContainer);

    MetricResults metricResults = asAttemptedOnlyMetricResults(attemptedMetrics);

    MetricQueryResults step1res =
        metricResults.queryMetrics(MetricsFilter.builder().addStep(STEP1).build());

    assertIterableSize(step1res.getCounters(), 1);
    assertIterableSize(step1res.getDistributions(), 1);
    assertIterableSize(step1res.getGauges(), 1);
    assertIterableSize(step1res.getStringSets(), 1);

    assertCounter(COUNTER_NAME, step1res, STEP1, VALUE, false);
    assertDistribution(
        DISTRIBUTION_NAME,
        step1res,
        STEP1,
        DistributionResult.create(VALUE * 3, 2, VALUE, VALUE * 2),
        false);
    assertGauge(GAUGE_NAME, step1res, STEP1, GaugeResult.create(VALUE, Instant.now()), false);

    assertStringSet(
        STRING_SET_NAME,
        step1res,
        STEP1,
        StringSetResult.create(ImmutableSet.of(FIRST_STRING, SECOND_STRING)),
        false);

    MetricQueryResults step2res =
        metricResults.queryMetrics(MetricsFilter.builder().addStep(STEP2).build());

    assertIterableSize(step2res.getCounters(), 1);
    assertIterableSize(step2res.getDistributions(), 1);
    assertIterableSize(step2res.getGauges(), 1);
    assertIterableSize(step2res.getStringSets(), 1);

    assertCounter(COUNTER_NAME, step2res, STEP2, VALUE * 2, false);
    assertDistribution(
        DISTRIBUTION_NAME,
        step2res,
        STEP2,
        DistributionResult.create(VALUE * 6, 4, VALUE, VALUE * 2),
        false);
    assertGauge(GAUGE_NAME, step2res, STEP2, GaugeResult.create(VALUE, Instant.now()), false);

    assertStringSet(
        STRING_SET_NAME,
        step2res,
        STEP2,
        StringSetResult.create(ImmutableSet.of(FIRST_STRING, SECOND_STRING)),
        false);

    MetricQueryResults allres = metricResults.allMetrics();

    assertIterableSize(allres.getCounters(), 2);
    assertIterableSize(allres.getDistributions(), 2);
    assertIterableSize(allres.getGauges(), 2);
    assertIterableSize(allres.getStringSets(), 2);
  }

  @Test
  public void testCounterCommittedUnsupportedInAttemptedAccumulatedMetricResults() {
    MetricsContainerStepMap attemptedMetrics = new MetricsContainerStepMap();
    attemptedMetrics.update(STEP1, metricsContainer);
    MetricResults metricResults = asAttemptedOnlyMetricResults(attemptedMetrics);

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
    MetricResults metricResults = asAttemptedOnlyMetricResults(attemptedMetrics);

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
    MetricResults metricResults = asAttemptedOnlyMetricResults(attemptedMetrics);

    MetricQueryResults step1res =
        metricResults.queryMetrics(MetricsFilter.builder().addStep(STEP1).build());

    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("This runner does not currently support committed metrics results.");

    assertGauge(GAUGE_NAME, step1res, STEP1, GaugeResult.empty(), true);
  }

  @Test
  public void testStringSetCommittedUnsupportedInAttemptedAccumulatedMetricResults() {
    MetricsContainerStepMap attemptedMetrics = new MetricsContainerStepMap();
    attemptedMetrics.update(STEP1, metricsContainer);
    MetricResults metricResults = asAttemptedOnlyMetricResults(attemptedMetrics);

    MetricQueryResults step1res =
        metricResults.queryMetrics(MetricsFilter.builder().addStep(STEP1).build());

    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("This runner does not currently support committed metrics results.");

    assertStringSet(STRING_SET_NAME, step1res, STEP1, StringSetResult.empty(), true);
  }

  @Test
  public void testUserMetricDroppedOnUnbounded() {
    MetricsContainerStepMap testObject = new MetricsContainerStepMap();
    CounterCell c1 = testObject.getUnboundContainer().getCounter(MetricName.named("ns", "name1"));
    c1.inc(5);

    List<MonitoringInfo> expected = new ArrayList<MonitoringInfo>();
    assertThat(testObject.getMonitoringInfos(), containsInAnyOrder(expected.toArray()));
  }

  @Test
  public void testUpdateAllUpdatesUnboundedAndBoundedContainers() {
    MetricsContainerStepMap baseMetricContainerRegistry = new MetricsContainerStepMap();

    CounterCell c1 =
        baseMetricContainerRegistry.getContainer(STEP1).getCounter(MetricName.named("ns", "name1"));
    CounterCell c2 =
        baseMetricContainerRegistry
            .getUnboundContainer()
            .getCounter(MonitoringInfoTestUtil.testElementCountName());

    c1.inc(7);
    c2.inc(14);

    MetricsContainerStepMap testObject = new MetricsContainerStepMap();
    testObject.updateAll(baseMetricContainerRegistry);

    List<MonitoringInfo> expected = new ArrayList<MonitoringInfo>();

    SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder();
    builder
        .setUrn(MonitoringInfoConstants.Urns.USER_SUM_INT64)
        .setLabel(MonitoringInfoConstants.Labels.NAMESPACE, "ns")
        .setLabel(MonitoringInfoConstants.Labels.NAME, "name1");
    builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, STEP1);
    builder.setInt64SumValue(7);
    expected.add(builder.build());

    expected.add(MonitoringInfoTestUtil.testElementCountMonitoringInfo(14));

    ArrayList<MonitoringInfo> actual = new ArrayList<MonitoringInfo>();

    for (MonitoringInfo mi : testObject.getMonitoringInfos()) {
      actual.add(mi);
    }
    assertThat(actual, containsInAnyOrder(expected.toArray()));
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

    MetricResults metricResults = asMetricResults(attemptedMetrics, committedMetrics);

    MetricQueryResults step1res =
        metricResults.queryMetrics(MetricsFilter.builder().addStep(STEP1).build());

    assertIterableSize(step1res.getCounters(), 1);
    assertIterableSize(step1res.getDistributions(), 1);
    assertIterableSize(step1res.getGauges(), 1);
    assertIterableSize(step1res.getStringSets(), 1);

    assertCounter(COUNTER_NAME, step1res, STEP1, VALUE * 2, false);
    assertDistribution(
        DISTRIBUTION_NAME,
        step1res,
        STEP1,
        DistributionResult.create(VALUE * 6, 4, VALUE, VALUE * 2),
        false);
    assertGauge(GAUGE_NAME, step1res, STEP1, GaugeResult.create(VALUE, Instant.now()), false);
    assertStringSet(
        STRING_SET_NAME,
        step1res,
        STEP1,
        StringSetResult.create(ImmutableSet.of(FIRST_STRING, SECOND_STRING)),
        false);

    assertCounter(COUNTER_NAME, step1res, STEP1, VALUE, true);
    assertDistribution(
        DISTRIBUTION_NAME,
        step1res,
        STEP1,
        DistributionResult.create(VALUE * 3, 2, VALUE, VALUE * 2),
        true);
    assertGauge(GAUGE_NAME, step1res, STEP1, GaugeResult.create(VALUE, Instant.now()), true);
    assertStringSet(
        STRING_SET_NAME,
        step1res,
        STEP1,
        StringSetResult.create(ImmutableSet.of(FIRST_STRING, SECOND_STRING)),
        true);

    MetricQueryResults step2res =
        metricResults.queryMetrics(MetricsFilter.builder().addStep(STEP2).build());

    assertIterableSize(step2res.getCounters(), 1);
    assertIterableSize(step2res.getDistributions(), 1);
    assertIterableSize(step2res.getGauges(), 1);
    assertIterableSize(step2res.getStringSets(), 1);

    assertCounter(COUNTER_NAME, step2res, STEP2, VALUE * 3, false);
    assertDistribution(
        DISTRIBUTION_NAME,
        step2res,
        STEP2,
        DistributionResult.create(VALUE * 9, 6, VALUE, VALUE * 2),
        false);
    assertGauge(GAUGE_NAME, step2res, STEP2, GaugeResult.create(VALUE, Instant.now()), false);
    assertStringSet(
        STRING_SET_NAME,
        step2res,
        STEP2,
        StringSetResult.create(ImmutableSet.of(FIRST_STRING, SECOND_STRING)),
        false);

    assertCounter(COUNTER_NAME, step2res, STEP2, VALUE * 2, true);
    assertDistribution(
        DISTRIBUTION_NAME,
        step2res,
        STEP2,
        DistributionResult.create(VALUE * 6, 4, VALUE, VALUE * 2),
        true);
    assertGauge(GAUGE_NAME, step2res, STEP2, GaugeResult.create(VALUE, Instant.now()), true);
    assertStringSet(
        STRING_SET_NAME,
        step2res,
        STEP2,
        StringSetResult.create(ImmutableSet.of(FIRST_STRING, SECOND_STRING)),
        true);
    assertStringSet(
        STRING_SET_NAME,
        step2res,
        STEP2,
        StringSetResult.create(ImmutableSet.of(FIRST_STRING, SECOND_STRING)),
        true);

    MetricQueryResults allres = metricResults.queryMetrics(MetricsFilter.builder().build());

    assertIterableSize(allres.getCounters(), 2);
    assertIterableSize(allres.getDistributions(), 2);
    assertIterableSize(allres.getGauges(), 2);
    assertIterableSize(allres.getStringSets(), 2);
  }

  @Test
  public void testEquals() {
    MetricsContainerStepMap metricsContainerStepMap = new MetricsContainerStepMap();
    MetricsContainerStepMap equal = new MetricsContainerStepMap();
    Assert.assertEquals(metricsContainerStepMap, equal);
    Assert.assertEquals(metricsContainerStepMap.hashCode(), equal.hashCode());
  }

  @Test
  public void testNotEquals() {
    MetricsContainerStepMap metricsContainerStepMap = new MetricsContainerStepMap();

    Assert.assertNotEquals(metricsContainerStepMap, new Object());

    MetricsContainerStepMap differentMetricsContainers = new MetricsContainerStepMap();
    differentMetricsContainers.getContainer("stepName");
    Assert.assertNotEquals(metricsContainerStepMap, differentMetricsContainers);
    Assert.assertNotEquals(
        metricsContainerStepMap.hashCode(), differentMetricsContainers.hashCode());

    MetricsContainerStepMap differentUnboundedContainer = new MetricsContainerStepMap();
    differentUnboundedContainer
        .getContainer(null)
        .getCounter(MetricName.named("namespace", "name"));
    Assert.assertNotEquals(metricsContainerStepMap, differentUnboundedContainer);
    Assert.assertNotEquals(
        metricsContainerStepMap.hashCode(), differentUnboundedContainer.hashCode());
  }

  @Test
  public void testReset() {
    MetricsContainerStepMap attemptedMetrics = new MetricsContainerStepMap();
    attemptedMetrics.update(STEP1, metricsContainer);
    attemptedMetrics.update(STEP2, metricsContainer);
    attemptedMetrics.update(STEP2, metricsContainer);

    MetricResults metricResults = asAttemptedOnlyMetricResults(attemptedMetrics);
    MetricQueryResults allres = metricResults.allMetrics();
    assertCounter(COUNTER_NAME, allres, STEP1, VALUE, false);
    assertDistribution(
        DISTRIBUTION_NAME,
        allres,
        STEP1,
        DistributionResult.create(VALUE * 3, 2, VALUE, VALUE * 2),
        false);
    assertGauge(GAUGE_NAME, allres, STEP1, GaugeResult.create(VALUE, Instant.now()), false);
    assertStringSet(
        STRING_SET_NAME,
        allres,
        STEP1,
        StringSetResult.create(ImmutableSet.of(FIRST_STRING, SECOND_STRING)),
        false);

    assertCounter(COUNTER_NAME, allres, STEP2, VALUE * 2, false);
    assertDistribution(
        DISTRIBUTION_NAME,
        allres,
        STEP2,
        DistributionResult.create(VALUE * 6, 4, VALUE, VALUE * 2),
        false);
    assertGauge(GAUGE_NAME, allres, STEP2, GaugeResult.create(VALUE, Instant.now()), false);
    assertStringSet(
        STRING_SET_NAME,
        allres,
        STEP2,
        StringSetResult.create(ImmutableSet.of(FIRST_STRING, SECOND_STRING)),
        false);

    attemptedMetrics.reset();
    metricResults = asAttemptedOnlyMetricResults(attemptedMetrics);
    allres = metricResults.allMetrics();

    // Check that the metrics container for STEP1 is reset
    assertCounter(COUNTER_NAME, allres, STEP1, 0L, false);
    assertDistribution(
        DISTRIBUTION_NAME, allres, STEP1, DistributionResult.IDENTITY_ELEMENT, false);
    assertGauge(GAUGE_NAME, allres, STEP1, GaugeResult.empty(), false);
    assertStringSet(STRING_SET_NAME, allres, STEP1, StringSetResult.empty(), false);

    // Check that the metrics container for STEP2 is reset
    assertCounter(COUNTER_NAME, allres, STEP2, 0L, false);
    assertDistribution(
        DISTRIBUTION_NAME, allres, STEP2, DistributionResult.IDENTITY_ELEMENT, false);
    assertGauge(GAUGE_NAME, allres, STEP2, GaugeResult.empty(), false);
    assertStringSet(STRING_SET_NAME, allres, STEP2, StringSetResult.empty(), false);
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
        metricQueryResults.getCounters(),
        hasItem(metricsResult(NAMESPACE, name, step, expected, isCommitted)));
  }

  private void assertDistribution(
      String name,
      MetricQueryResults metricQueryResults,
      String step,
      DistributionResult expected,
      boolean isCommitted) {
    assertThat(
        metricQueryResults.getDistributions(),
        hasItem(metricsResult(NAMESPACE, name, step, expected, isCommitted)));
  }

  private void assertGauge(
      String name,
      MetricQueryResults metricQueryResults,
      String step,
      GaugeResult expected,
      boolean isCommitted) {
    assertThat(
        metricQueryResults.getGauges(),
        hasItem(metricsResult(NAMESPACE, name, step, expected, isCommitted)));
  }

  private void assertStringSet(
      String name,
      MetricQueryResults metricQueryResults,
      String step,
      StringSetResult expected,
      boolean isCommitted) {
    assertThat(
        metricQueryResults.getStringSets(),
        hasItem(metricsResult(NAMESPACE, name, step, expected, isCommitted)));
  }
}
