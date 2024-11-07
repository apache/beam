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
package org.apache.beam.runners.direct;

import static org.apache.beam.sdk.metrics.MetricNameFilter.inNamespace;
import static org.apache.beam.sdk.metrics.MetricResultsMatchers.attemptedMetricsResult;
import static org.apache.beam.sdk.metrics.MetricResultsMatchers.committedMetricsResult;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.beam.runners.core.metrics.DistributionData;
import org.apache.beam.runners.core.metrics.GaugeData;
import org.apache.beam.runners.core.metrics.MetricUpdates;
import org.apache.beam.runners.core.metrics.MetricUpdates.MetricUpdate;
import org.apache.beam.runners.core.metrics.StringSetData;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.metrics.StringSetResult;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link DirectMetrics}. */
@RunWith(JUnit4.class)
public class DirectMetricsTest {

  @Mock private CommittedBundle<Object> bundle1;

  private static final MetricName NAME1 = MetricName.named("ns1", "name1");
  private static final MetricName NAME2 = MetricName.named("ns1", "name2");
  private static final MetricName NAME3 = MetricName.named("ns2", "name1");
  private static final MetricName NAME4 = MetricName.named("ns2", "name2");

  private DirectMetrics metrics;
  private ExecutorService executor;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    executor = Executors.newSingleThreadExecutor();
    metrics = new DirectMetrics(executor);
  }

  @After
  public void tearDown() {
    executor.shutdownNow();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testApplyCommittedNoFilter() {
    metrics.commitLogical(
        bundle1,
        MetricUpdates.create(
            ImmutableList.of(
                MetricUpdate.create(MetricKey.create("step1", NAME1), 5L),
                MetricUpdate.create(MetricKey.create("step1", NAME2), 8L)),
            ImmutableList.of(
                MetricUpdate.create(
                    MetricKey.create("step1", NAME1), DistributionData.create(8, 2, 3, 5))),
            ImmutableList.of(
                MetricUpdate.create(MetricKey.create("step1", NAME4), GaugeData.create(15L))),
            ImmutableList.of(
                MetricUpdate.create(
                    MetricKey.create("step1", NAME4),
                    MetricKey.create("step1", NAME4), StringSetData.create(ImmutableSet.of("ab")))),
                    ImmutableList.of()));    metrics.commitLogical(
        bundle1,
        MetricUpdates.create(
            ImmutableList.of(
                MetricUpdate.create(MetricKey.create("step2", NAME1), 7L),
                MetricUpdate.create(MetricKey.create("step1", NAME2), 4L)),
            ImmutableList.of(
                MetricUpdate.create(
                    MetricKey.create("step1", NAME1), DistributionData.create(4, 1, 4, 4))),
            ImmutableList.of(
                MetricUpdate.create(MetricKey.create("step1", NAME4), GaugeData.create(27L))),
            ImmutableList.of(
                MetricUpdate.create(
                    MetricKey.create("step1", NAME4), StringSetData.create(ImmutableSet.of("cd")))),
                    ImmutableList.of()));
    MetricQueryResults results = metrics.allMetrics();
    assertThat(
        results.getCounters(),
        containsInAnyOrder(
            attemptedMetricsResult("ns1", "name1", "step1", 0L),
            attemptedMetricsResult("ns1", "name2", "step1", 0L),
            attemptedMetricsResult("ns1", "name1", "step2", 0L)));
    assertThat(
        results.getCounters(),
        containsInAnyOrder(
            committedMetricsResult("ns1", "name1", "step1", 5L),
            committedMetricsResult("ns1", "name2", "step1", 12L),
            committedMetricsResult("ns1", "name1", "step2", 7L)));
    assertThat(
        results.getDistributions(),
        contains(
            attemptedMetricsResult("ns1", "name1", "step1", DistributionResult.IDENTITY_ELEMENT)));
    assertThat(
        results.getDistributions(),
        contains(
            committedMetricsResult(
                "ns1", "name1", "step1", DistributionResult.create(12, 3, 3, 5))));
    assertThat(
        results.getGauges(),
        contains(attemptedMetricsResult("ns2", "name2", "step1", GaugeResult.empty())));
    assertThat(
        results.getGauges(),
        contains(
            committedMetricsResult(
                "ns2", "name2", "step1", GaugeResult.create(27L, Instant.now()))));
    assertThat(
        results.getStringSets(),
        contains(
            committedMetricsResult(
                "ns2", "name2", "step1", StringSetResult.create(ImmutableSet.of("ab", "cd")))));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testApplyAttemptedCountersQueryOneNamespace() {
    metrics.updatePhysical(
        bundle1,
        MetricUpdates.create(
            ImmutableList.of(
                MetricUpdate.create(MetricKey.create("step1", NAME1), 5L),
                MetricUpdate.create(MetricKey.create("step1", NAME3), 8L)),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of()));
    metrics.updatePhysical(
        bundle1,
        MetricUpdates.create(
            ImmutableList.of(
                MetricUpdate.create(MetricKey.create("step2", NAME1), 7L),
                MetricUpdate.create(MetricKey.create("step1", NAME3), 4L)),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of()));

    MetricQueryResults results =
        metrics.queryMetrics(MetricsFilter.builder().addNameFilter(inNamespace("ns1")).build());

    assertThat(
        results.getCounters(),
        containsInAnyOrder(
            attemptedMetricsResult("ns1", "name1", "step1", 5L),
            attemptedMetricsResult("ns1", "name1", "step2", 7L)));

    assertThat(
        results.getCounters(),
        containsInAnyOrder(
            committedMetricsResult("ns1", "name1", "step1", 0L),
            committedMetricsResult("ns1", "name1", "step2", 0L)));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testApplyAttemptedQueryCompositeScope() {
    metrics.updatePhysical(
        bundle1,
        MetricUpdates.create(
            ImmutableList.of(
                MetricUpdate.create(MetricKey.create("Outer1/Inner1", NAME1), 5L),
                MetricUpdate.create(MetricKey.create("Outer1/Inner2", NAME1), 8L)),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of()));
    metrics.updatePhysical(
        bundle1,
        MetricUpdates.create(
            ImmutableList.of(
                MetricUpdate.create(MetricKey.create("Outer1/Inner1", NAME1), 12L),
                MetricUpdate.create(MetricKey.create("Outer2/Inner2", NAME1), 18L)),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of()));

    MetricQueryResults results =
        metrics.queryMetrics(MetricsFilter.builder().addStep("Outer1").build());

    assertThat(
        results.getCounters(),
        containsInAnyOrder(
            attemptedMetricsResult("ns1", "name1", "Outer1/Inner1", 12L),
            attemptedMetricsResult("ns1", "name1", "Outer1/Inner2", 8L)));

    assertThat(
        results.getCounters(),
        containsInAnyOrder(
            committedMetricsResult("ns1", "name1", "Outer1/Inner1", 0L),
            committedMetricsResult("ns1", "name1", "Outer1/Inner2", 0L)));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testPartialScopeMatchingInMetricsQuery() {
    metrics.updatePhysical(
        bundle1,
        MetricUpdates.create(
            ImmutableList.of(
                MetricUpdate.create(MetricKey.create("Top1/Outer1/Inner1", NAME1), 5L),
                MetricUpdate.create(MetricKey.create("Top1/Outer1/Inner2", NAME1), 8L)),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of()));
    metrics.updatePhysical(
        bundle1,
        MetricUpdates.create(
            ImmutableList.of(
                MetricUpdate.create(MetricKey.create("Top2/Outer1/Inner1", NAME1), 12L),
                MetricUpdate.create(MetricKey.create("Top1/Outer2/Inner2", NAME1), 18L)),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of()));

    MetricQueryResults results =
        metrics.queryMetrics(MetricsFilter.builder().addStep("Top1/Outer1").build());

    assertThat(
        results.getCounters(),
        containsInAnyOrder(
            attemptedMetricsResult("ns1", "name1", "Top1/Outer1/Inner1", 5L),
            attemptedMetricsResult("ns1", "name1", "Top1/Outer1/Inner2", 8L)));

    results = metrics.queryMetrics(MetricsFilter.builder().addStep("Inner2").build());

    assertThat(
        results.getCounters(),
        containsInAnyOrder(
            attemptedMetricsResult("ns1", "name1", "Top1/Outer1/Inner2", 8L),
            attemptedMetricsResult("ns1", "name1", "Top1/Outer2/Inner2", 18L)));
  }
}
