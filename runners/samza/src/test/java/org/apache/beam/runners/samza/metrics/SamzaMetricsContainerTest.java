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
package org.apache.beam.runners.samza.metrics;

import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.samza.config.MapConfig;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Tests for {@link SamzaMetricsContainer}. */
public class SamzaMetricsContainerTest {
  private static final String CONTAINER_NAME = "container-0";
  private MetricsRegistryMap metricsRegistryMap;
  private SamzaMetricsContainer samzaMetricsContainer;

  @Before
  public void setup() {
    metricsRegistryMap = new MetricsRegistryMap(CONTAINER_NAME);
    samzaMetricsContainer =
        new SamzaMetricsContainer(
            metricsRegistryMap,
            new MapConfig(
                ImmutableMap.of(SamzaMetricsContainer.USE_SHORT_METRIC_NAMES_CONFIG, "true")));
  }

  /** Test metrics update works. */
  @Test
  public void testMetricsUpdate() {
    increaseCounterForStep("stepName", "test", "counter");
    samzaMetricsContainer.updateMetrics("stepName");
    Counter counter = (Counter) metricsRegistryMap.getGroup("BeamMetrics").get("test:counter");
    Assert.assertEquals(1L, counter.getCount());
  }

  /** Test metrics update works when having stepName as part of Metric name. */
  @Test
  public void testMetricsUpdateWithStepName() {
    samzaMetricsContainer =
        new SamzaMetricsContainer(
            metricsRegistryMap,
            new MapConfig(
                ImmutableMap.of(SamzaMetricsContainer.USE_SHORT_METRIC_NAMES_CONFIG, "false")));
    increaseCounterForStep("stepName", "test", "counter");
    samzaMetricsContainer.updateMetrics("stepName");
    Counter counter =
        (Counter) metricsRegistryMap.getGroup("BeamMetrics").get("stepName:test:counter");
    Assert.assertEquals(1L, counter.getCount());
  }

  /**
   * Test {@link SamzaMetricsContainer#updateMetrics(String)} only update metrics for the specified
   * step for better performance.
   */
  @Test
  public void testMetricsUpdateForOnlyStep() {
    String step1 = "stepNameOne";
    String step2 = "stepNameTwo";
    String counterName1 = "counter1";
    String counterName2 = "counter2";
    increaseCounterForStep(step1, "test", counterName1);
    increaseCounterForStep(step2, "test", counterName2);
    Counter counter1 = metricsRegistryMap.newCounter("BeamMetrics", "test:" + counterName1);
    Counter counter2 = metricsRegistryMap.newCounter("BeamMetrics", "test:" + counterName2);

    // Beam metrics haven't been updated to Samza metrics
    Assert.assertEquals(0L, counter1.getCount());
    Assert.assertEquals(0L, counter2.getCount());

    // Update metrics for step 1
    samzaMetricsContainer.updateMetrics(step1);
    Assert.assertEquals(1L, counter1.getCount());
    Assert.assertEquals(0L, counter2.getCount());

    // Update metrics for step 2
    samzaMetricsContainer.updateMetrics(step2);
    Assert.assertEquals(1L, counter1.getCount());
    Assert.assertEquals(1L, counter2.getCount());
  }

  /**
   * When application updates metrics in a user thread, there is no MetricsContainer set to the
   * thread. Then the metrics will go to the "globalMetricsContainer". Updating metrics for any of
   * the step will also update the "globalMetricsContainer".
   */
  @Test
  public void testMetricsUpdateForGlobalMetricsContainer() {
    // Set globalMetricsContainer
    MetricsContainer globalMetricsContainer =
        samzaMetricsContainer.getContainer(SamzaMetricsContainer.GLOBAL_CONTAINER_STEP_NAME);
    MetricsEnvironment.setGlobalContainer(globalMetricsContainer);

    // Skip scoping the MetricsContainer for the step. It will use the "GlobalMetricsContainer".
    MetricsEnvironment.setCurrentContainer(null);
    Metrics.counter("test", "counter").inc();
    samzaMetricsContainer.updateMetrics("randomStepName");
    Counter counter = (Counter) metricsRegistryMap.getGroup("BeamMetrics").get("test:counter");
    Assert.assertEquals(1L, counter.getCount());
  }

  private void increaseCounterForStep(String stepName, String namespace, String counterName) {
    MetricsEnvironment.scopedMetricsContainer(samzaMetricsContainer.getContainer(stepName));
    Metrics.counter(namespace, counterName).inc();
  }
}
