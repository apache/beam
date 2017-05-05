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

package org.apache.beam.sdk.metrics;

import static org.apache.beam.sdk.metrics.MetricMatchers.metricUpdate;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link MetricsContainer}.
 */
@RunWith(JUnit4.class)
public class MetricsContainerTest {

  @Test
  public void testCounterDeltas() {
    MetricsContainer container = new MetricsContainer("step1");
    CounterCell c1 = container.getCounter(MetricName.named("ns", "name1"));
    CounterCell c2 = container.getCounter(MetricName.named("ns", "name2"));
    assertThat("All counters should start out dirty",
        container.getUpdates().counterUpdates(), containsInAnyOrder(
        metricUpdate("name1", 0L),
        metricUpdate("name2", 0L)));
    container.commitUpdates();
    assertThat("After commit no counters should be dirty",
        container.getUpdates().counterUpdates(), emptyIterable());

    c1.update(5L);
    c2.update(4L);

    assertThat(container.getUpdates().counterUpdates(), containsInAnyOrder(
        metricUpdate("name1", 5L),
        metricUpdate("name2", 4L)));

    assertThat("Since we haven't committed, updates are still included",
        container.getUpdates().counterUpdates(), containsInAnyOrder(
        metricUpdate("name1", 5L),
        metricUpdate("name2", 4L)));

    container.commitUpdates();
    assertThat("After commit there are no updates",
        container.getUpdates().counterUpdates(), emptyIterable());

    c1.update(8L);
    assertThat(container.getUpdates().counterUpdates(), contains(
        metricUpdate("name1", 13L)));
  }

  @Test
  public void testCounterCumulatives() {
    MetricsContainer container = new MetricsContainer("step1");
    CounterCell c1 = container.getCounter(MetricName.named("ns", "name1"));
    CounterCell c2 = container.getCounter(MetricName.named("ns", "name2"));
    c1.update(2L);
    c2.update(4L);
    c1.update(3L);

    container.getUpdates();
    container.commitUpdates();
    assertThat("Committing updates shouldn't affect cumulative counter values",
        container.getCumulative().counterUpdates(), containsInAnyOrder(
        metricUpdate("name1", 5L),
        metricUpdate("name2", 4L)));

    c1.update(8L);
    assertThat(container.getCumulative().counterUpdates(), containsInAnyOrder(
        metricUpdate("name1", 13L),
        metricUpdate("name2", 4L)));
  }

  @Test
  public void testDistributionDeltas() {
    MetricsContainer container = new MetricsContainer("step1");
    DistributionCell c1 = container.getDistribution(MetricName.named("ns", "name1"));
    DistributionCell c2 = container.getDistribution(MetricName.named("ns", "name2"));

    assertThat("Initial update includes initial zero-values",
        container.getUpdates().distributionUpdates(), containsInAnyOrder(
        metricUpdate("name1", DistributionData.EMPTY),
        metricUpdate("name2", DistributionData.EMPTY)));

    container.commitUpdates();
    assertThat("No updates after commit",
        container.getUpdates().distributionUpdates(), emptyIterable());

    c1.update(5L);
    c2.update(4L);

    assertThat(container.getUpdates().distributionUpdates(), containsInAnyOrder(
        metricUpdate("name1", DistributionData.create(5, 1, 5, 5)),
        metricUpdate("name2", DistributionData.create(4, 1, 4, 4))));
    assertThat("Updates stay the same without commit",
        container.getUpdates().distributionUpdates(), containsInAnyOrder(
        metricUpdate("name1", DistributionData.create(5, 1, 5, 5)),
        metricUpdate("name2", DistributionData.create(4, 1, 4, 4))));

    container.commitUpdates();
    assertThat("No updatess after commit",
        container.getUpdates().distributionUpdates(), emptyIterable());

    c1.update(8L);
    c1.update(4L);
    assertThat(container.getUpdates().distributionUpdates(), contains(
        metricUpdate("name1", DistributionData.create(17, 3, 4, 8))));
    container.commitUpdates();
  }
}
