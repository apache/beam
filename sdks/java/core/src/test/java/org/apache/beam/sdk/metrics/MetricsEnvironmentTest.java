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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link MetricsEnvironment}.
 */
@RunWith(JUnit4.class)
public class MetricsEnvironmentTest {
  @After
  public void teardown() {
    MetricsEnvironment.setCurrentContainer(null);
  }

  @Test
  public void testUsesAppropriateMetricsContainer() {
    Counter counter = Metrics.counter("ns", "name");
    MetricsContainer c1 = new MetricsContainer("step1");
    MetricsContainer c2 = new MetricsContainer("step2");

    MetricsEnvironment.setCurrentContainer(c1);
    counter.inc();
    MetricsEnvironment.setCurrentContainer(c2);
    counter.dec();
    MetricsEnvironment.setCurrentContainer(null);

    MetricUpdates updates1 = c1.getUpdates();
    MetricUpdates updates2 = c2.getUpdates();
    assertThat(updates1.counterUpdates(), contains(metricUpdate("ns", "name", "step1", 1L)));
    assertThat(updates2.counterUpdates(), contains(metricUpdate("ns", "name", "step2", -1L)));
  }

  @Test
  public void testBehavesWithoutMetricsContainer() {
    assertNull(MetricsEnvironment.getCurrentContainer());
  }
}
