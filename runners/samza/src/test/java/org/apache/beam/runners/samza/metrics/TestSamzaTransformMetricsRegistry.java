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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.samza.context.Context;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.Timer;
import org.apache.samza.system.WatermarkMessage;
import org.joda.time.Instant;
import org.junit.Test;

public class TestSamzaTransformMetricsRegistry {

  @Test
  public void testSamzaTransformMetricsRegistryForNonShuffleOperators() {
    final WatermarkMessage watermarkMessage =
        new WatermarkMessage(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis());
    final long avgInputArrivalTime = System.currentTimeMillis();

    Timer latency = new Timer("filter-latency");
    Gauge<Long> cacheSize = new Gauge<Long>("filter-cache-size", 0L);
    SamzaTransformMetrics samzaTransformMetrics = mock(SamzaTransformMetrics.class);
    doNothing().when(samzaTransformMetrics).register(any(), any());
    when(samzaTransformMetrics.getTransformLatencyMetric("filter")).thenReturn(latency);
    when(samzaTransformMetrics.getTransformCacheSize("filter")).thenReturn(cacheSize);

    SamzaTransformMetricRegistry samzaTransformMetricRegistry =
        spy(new SamzaTransformMetricRegistry(samzaTransformMetrics));

    samzaTransformMetricRegistry.register("filter", "dummy-pvalue.in", mock(Context.class));
    samzaTransformMetricRegistry.register("filter", "dummy-pvalue.out", mock(Context.class));

    // Update the avg arrival time
    samzaTransformMetricRegistry.updateArrivalTimeMap(
        "filter", "dummy-pvalue.in", watermarkMessage.getTimestamp(), avgInputArrivalTime);
    samzaTransformMetricRegistry.updateArrivalTimeMap(
        "filter", "dummy-pvalue.out", watermarkMessage.getTimestamp(), avgInputArrivalTime + 100);

    // Check the avg arrival time is updated
    assertEquals(
        avgInputArrivalTime,
        samzaTransformMetricRegistry
            .getAverageArrivalTimeMap("filter")
            .get("dummy-pvalue.in")
            .get(watermarkMessage.getTimestamp())
            .longValue());
    assertEquals(
        avgInputArrivalTime + 100,
        samzaTransformMetricRegistry
            .getAverageArrivalTimeMap("filter")
            .get("dummy-pvalue.out")
            .get(watermarkMessage.getTimestamp())
            .longValue());

    // Emit the latency metric
    samzaTransformMetricRegistry.emitLatencyMetric(
        "filter",
        ImmutableList.of("dummy-pvalue.in"),
        ImmutableList.of("dummy-pvalue.out"),
        watermarkMessage.getTimestamp(),
        "task 0");

    // Check the latency metric is updated
    assertTrue(100 == latency.getSnapshot().getAverage());

    // Check the avg arrival time is cleared
    assertFalse(
        samzaTransformMetricRegistry
            .getAverageArrivalTimeMap("filter")
            .get("dummy-pvalue.in")
            .containsKey(watermarkMessage.getTimestamp()));
    assertFalse(
        samzaTransformMetricRegistry
            .getAverageArrivalTimeMap("filter")
            .get("dummy-pvalue.out")
            .containsKey(watermarkMessage.getTimestamp()));
    // Cache size must be 0
    assertEquals(0, cacheSize.getValue().intValue());
  }

  @Test
  public void testSamzaTransformMetricsRegistryForDataShuffleOperators() {
    Timer latency = new Timer("Count-perKey-latency");
    Gauge<Long> cacheSize = new Gauge<Long>("Count-perKey-cache-size", 0L);

    SamzaTransformMetrics samzaTransformMetrics = mock(SamzaTransformMetrics.class);
    doNothing().when(samzaTransformMetrics).register(any(), any());
    when(samzaTransformMetrics.getTransformLatencyMetric("Count.perKey")).thenReturn(latency);
    when(samzaTransformMetrics.getTransformCacheSize("Count.perKey")).thenReturn(cacheSize);

    SamzaTransformMetricRegistry samzaTransformMetricRegistry =
        spy(new SamzaTransformMetricRegistry(samzaTransformMetrics));

    samzaTransformMetricRegistry.register("Count.perKey", "window-assign.in", mock(Context.class));
    samzaTransformMetricRegistry.register("Count.perKey", "window-assign.out", mock(Context.class));

    final BoundedWindow first =
        new BoundedWindow() {
          @Override
          public Instant maxTimestamp() {
            return new Instant(2048L);
          }
        };
    final BoundedWindow second =
        new BoundedWindow() {
          @Override
          public Instant maxTimestamp() {
            return new Instant(689743L);
          }
        };

    // Update the avg arrival time
    samzaTransformMetricRegistry.updateArrivalTimeMap("Count.perKey", first, 1048L);
    samzaTransformMetricRegistry.updateArrivalTimeMap("Count.perKey", second, 4897L);

    // Check the avg arrival time is updated
    assertEquals(
        1048L,
        samzaTransformMetricRegistry
            .getAverageArrivalTimeMapForGBK("Count.perKey")
            .get(first)
            .longValue());
    assertEquals(
        4897L,
        samzaTransformMetricRegistry
            .getAverageArrivalTimeMapForGBK("Count.perKey")
            .get(second)
            .longValue());

    // Emit the latency metric
    samzaTransformMetricRegistry.emitLatencyMetric("Count.perKey", first, 2048L, "task 0");
    samzaTransformMetricRegistry.emitLatencyMetric("Count.perKey", second, 5897L, "task 0");

    // Check the latency metric is updated
    assertTrue(1000 == latency.getSnapshot().getAverage());

    // Check the avg arrival time is cleared
    assertFalse(
        samzaTransformMetricRegistry
            .getAverageArrivalTimeMapForGBK("Count.perKey")
            .containsKey(first));
    assertFalse(
        samzaTransformMetricRegistry
            .getAverageArrivalTimeMapForGBK("Count.perKey")
            .containsKey(second));

    // Failure testing
    samzaTransformMetricRegistry.updateArrivalTimeMap("random-transform", first, 1048L);
    samzaTransformMetricRegistry.updateArrivalTimeMap("random-transform", first, 1048L);
    // No data updated
    assertFalse(
        samzaTransformMetricRegistry
            .getAverageArrivalTimeMapForGBK("Count.perKey")
            .containsKey(first));
    assertNull(samzaTransformMetricRegistry.getAverageArrivalTimeMapForGBK("random-transform"));
    // Emit the bad latency metric
    samzaTransformMetricRegistry.emitLatencyMetric("random-transform", first, 2048L, "task 0");
    samzaTransformMetricRegistry.emitLatencyMetric("random-transform", first, 0, "task 0");
    // Check the latency metric is same
    assertTrue(1000 == latency.getSnapshot().getAverage());
    // Cache size must be 0
    assertEquals(0, cacheSize.getValue().intValue());
  }
}
