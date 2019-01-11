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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Test;

/**
 * Tests for {@link Metrics}.
 */
public class MetricsTest {

  private static final String NS = "test";
  private static final String NAME = "name";
  private static final MetricName METRIC_NAME = MetricName.named(NS, NAME);

  @After
  public void tearDown() {
    MetricsEnvironment.setCurrentContainer(null);
  }

  @Test
  public void distributionWithoutContainer() {
    assertNull(MetricsEnvironment.getCurrentContainer());
    // Should not fail even though there is no metrics container.
    Metrics.distribution(NS, NAME).update(5L);
  }

  @Test
  public void counterWithoutContainer() {
    assertNull(MetricsEnvironment.getCurrentContainer());
    // Should not fail even though there is no metrics container.
    Counter counter = Metrics.counter(NS, NAME);
    counter.inc();
    counter.inc(5L);
    counter.dec();
    counter.dec(5L);
  }

  @Test
  public void distributionToCell() {
    MetricsContainer container = new MetricsContainer("step");
    MetricsEnvironment.setCurrentContainer(container);

    Distribution distribution = Metrics.distribution(NS, NAME);

    distribution.update(5L);

    DistributionCell cell = container.getDistribution(METRIC_NAME);
    assertThat(cell.getCumulative(), equalTo(DistributionData.create(5, 1, 5, 5)));

    distribution.update(36L);
    assertThat(cell.getCumulative(), equalTo(DistributionData.create(41, 2, 5, 36)));

    distribution.update(1L);
    assertThat(cell.getCumulative(), equalTo(DistributionData.create(42, 3, 1, 36)));
  }

  @Test
  public void counterToCell() {
    MetricsContainer container = new MetricsContainer("step");
    MetricsEnvironment.setCurrentContainer(container);
    Counter counter = Metrics.counter(NS, NAME);
    CounterCell cell = container.getCounter(METRIC_NAME);
    counter.inc();
    assertThat(cell.getCumulative(), CoreMatchers.equalTo(1L));

    counter.inc(47L);
    assertThat(cell.getCumulative(), CoreMatchers.equalTo(48L));

    counter.dec(5L);
    assertThat(cell.getCumulative(), CoreMatchers.equalTo(43L));

    counter.dec();
    assertThat(cell.getCumulative(), CoreMatchers.equalTo(42L));
  }
}
