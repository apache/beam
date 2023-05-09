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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.TestSamzaRunner;
import org.apache.beam.runners.samza.runtime.OpEmitter;
import org.apache.beam.runners.samza.util.InMemoryMetricsReporter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.samza.context.Context;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.Metric;
import org.apache.samza.metrics.Timer;
import org.apache.samza.system.WatermarkMessage;
import org.joda.time.Instant;
import org.junit.Test;

public class TestSamzaRunnerWithTransformMetrics {
  @Test
  public void testSamzaRunnerWithDefaultMetrics() {
    SamzaPipelineOptions options = PipelineOptionsFactory.create().as(SamzaPipelineOptions.class);
    InMemoryMetricsReporter inMemoryMetricsReporter = new InMemoryMetricsReporter();
    options.setMetricsReporters(ImmutableList.of(inMemoryMetricsReporter));
    options.setRunner(TestSamzaRunner.class);
    options.setEnableTransformMetrics(true);
    TestSamzaRunner testSamzaRunner = TestSamzaRunner.fromOptions(options);
    Pipeline pipeline = Pipeline.create(options);
    // Create a pipeline
    PCollection<KV<String, Integer>> output =
        pipeline
            .apply(
                "Mock data",
                Create.of(
                    KV.of("bad-key", KV.of("a", 97)),
                    KV.of("hello", KV.of("a", 97)),
                    KV.of("hello", KV.of("b", 42)),
                    KV.of("hello", KV.of("c", 12))))
            .apply("Filter valid keys", Filter.by(x -> x.getKey().equals("hello")))
            .apply(Values.create());
    // check pipeline is working fine
    PAssert.that(output).containsInAnyOrder(KV.of("a", 97), KV.of("b", 42), KV.of("c", 12));
    testSamzaRunner.run(pipeline);

    Map<String, Metric> pTransformContainerMetrics =
        inMemoryMetricsReporter
            .getMetricsRegistry("samza-container-1")
            .getGroup("SamzaBeamTransformMetrics");
    Map<String, Metric> pTransformTaskMetrics =
        inMemoryMetricsReporter
            .getMetricsRegistry("TaskName-Partition 0")
            .getGroup("SamzaBeamTransformMetrics");

    // SamzaTransformMetrics group must be initialized
    assertFalse(pTransformTaskMetrics.isEmpty());
    assertFalse(pTransformContainerMetrics.isEmpty());

    // Throughput Metrics are Per container by default
    assertEquals(
        4,
        ((Counter)
                pTransformContainerMetrics.get("Mock_data_Read_CreateSource_-num-output-messages"))
            .getCount());
    assertEquals(
        4,
        ((Counter)
                pTransformContainerMetrics.get(
                    "Filter_valid_keys_ParDo_Anonymous__ParMultiDo_Anonymous_-num-input-messages"))
            .getCount());
    // One message is dropped from filter
    assertEquals(
        3,
        ((Counter)
                pTransformContainerMetrics.get(
                    "Filter_valid_keys_ParDo_Anonymous__ParMultiDo_Anonymous_-num-output-messages"))
            .getCount());
    assertEquals(
        3,
        ((Counter)
                pTransformContainerMetrics.get(
                    "Values_Values_Map_ParMultiDo_Anonymous_-num-input-messages"))
            .getCount());
    assertEquals(
        3,
        ((Counter)
                pTransformContainerMetrics.get(
                    "Values_Values_Map_ParMultiDo_Anonymous_-num-output-messages"))
            .getCount());

    // Throughput Metrics are per container by default
    assertNotNull(
        pTransformContainerMetrics.get(
            "Filter_valid_keys_ParDo_Anonymous__ParMultiDo_Anonymous_-handle-message-ns"));
    // One message is dropped from filter
    assertNotNull(
        pTransformContainerMetrics.get(
            "Values_Values_Map_ParMultiDo_Anonymous_-handle-message-ns"));

    // Watermark Metrics are Per task by default
    assertTrue(
        ((Gauge<Long>)
                    pTransformTaskMetrics.get("Mock_data_Read_CreateSource_-output-watermark-ms"))
                .getValue()
            >= 0);
    assertNotNull(
        pTransformTaskMetrics.get(
            "Filter_valid_keys_ParDo_Anonymous__ParMultiDo_Anonymous_-output-watermark-ms"));
    assertNotNull(
        pTransformTaskMetrics.get("Values_Values_Map_ParMultiDo_Anonymous_-output-watermark-ms"));
  }

  @Test
  public void testSamzaInputAndOutputMetricOp() {
    final WindowedValue<String> windowedValue =
        WindowedValue.timestampedValueInGlobalWindow("value-1", new Instant());
    final WindowedValue<String> windowedValue2 =
        WindowedValue.timestampedValueInGlobalWindow("value-2", new Instant());
    final WatermarkMessage watermarkMessage =
        new WatermarkMessage(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis());

    OpEmitter<String> opEmitter = mock(OpEmitter.class);
    doNothing().when(opEmitter).emitElement(any());
    doNothing().when(opEmitter).emitWatermark(any());

    Counter inputCounter = new Counter("filter-input-counter");
    Counter outputCounter = new Counter("filter-output-counter");
    Gauge<Long> watermarkProgress = new Gauge<>("filter-output-watermark", 0L);
    Timer latency = new Timer("filter-latency");

    SamzaTransformMetrics samzaTransformMetrics = mock(SamzaTransformMetrics.class);
    doNothing().when(samzaTransformMetrics).register(any(), any());
    when(samzaTransformMetrics.getTransformInputThroughput("filter")).thenReturn(inputCounter);
    when(samzaTransformMetrics.getTransformOutputThroughput("filter")).thenReturn(outputCounter);
    when(samzaTransformMetrics.getTransformWatermarkProgress("filter"))
        .thenReturn(watermarkProgress);
    when(samzaTransformMetrics.getTransformLatencyMetric("filter")).thenReturn(latency);

    SamzaTransformMetricRegistry samzaTransformMetricRegistry =
        spy(new SamzaTransformMetricRegistry(samzaTransformMetrics));
    samzaTransformMetricRegistry.register("filter", "dummy-pvalue.in", mock(Context.class));
    samzaTransformMetricRegistry.register("filter", "dummy-pvalue.out", mock(Context.class));

    SamzaInputMetricOp<String> inputMetricOp =
        new SamzaInputMetricOp<>("dummy-pvalue.in", "filter", samzaTransformMetricRegistry);

    inputMetricOp.processElement(windowedValue, opEmitter);
    inputMetricOp.processElement(windowedValue2, opEmitter);
    inputMetricOp.processWatermark(new Instant(watermarkMessage.getTimestamp()), opEmitter);

    // Input throughput must be updated
    assertEquals(2, inputCounter.getCount());
    // Avg arrival time for the PValue must be updated
    assertTrue(
        samzaTransformMetricRegistry
            .getAverageArrivalTimeMap("filter")
            .get("dummy-pvalue.in")
            .containsKey(watermarkMessage.getTimestamp()));

    SamzaOutputMetricOp<String> outputMetricOp =
        new SamzaOutputMetricOp<>("dummy-pvalue.out", "filter", samzaTransformMetricRegistry);
    outputMetricOp.init(ImmutableList.of("dummy-pvalue.in"), ImmutableList.of("dummy-pvalue.out"));

    outputMetricOp.processElement(windowedValue, opEmitter);
    outputMetricOp.processElement(windowedValue2, opEmitter);
    outputMetricOp.processWatermark(new Instant(watermarkMessage.getTimestamp()), opEmitter);

    // Output throughput must be updated
    assertEquals(2, outputCounter.getCount());
    // Output watermark must be updated
    assertEquals(watermarkMessage.getTimestamp(), watermarkProgress.getValue().longValue());
    // Latency must be positive
    assertTrue(latency.getSnapshot().getAverage() > 0);
  }
}
