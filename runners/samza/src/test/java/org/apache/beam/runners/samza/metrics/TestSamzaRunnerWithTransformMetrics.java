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

import java.util.Map;
import org.apache.beam.runners.samza.TestSamzaRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.Metric;
import org.junit.Rule;
import org.junit.Test;

public class TestSamzaRunnerWithTransformMetrics {
  @Rule
  public final transient TestPipeline pipeline =
      TestPipeline.fromOptions(
          PipelineOptionsFactory.fromArgs("--runner=TestSamzaRunner").create());

  @Test
  public void testSamzaRunnerWithDefaultMetrics() {
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
    pipeline.run();
    TestSamzaRunner.InMemoryMetricsReporter inMemoryMetricsReporter =
        TestSamzaRunner.getTestMetricsReporter();
    Map<String, Metric> pTransformContainerMetrics =
        inMemoryMetricsReporter
            .getMetricsRegistry("samza-container-1")
            .getGroup("BeamTransformMetrics");
    Map<String, Metric> pTransformTaskMetrics =
        inMemoryMetricsReporter
            .getMetricsRegistry("TaskName-Partition 0")
            .getGroup("BeamTransformMetrics");

    // BeamTransformMetrics group must be initialized
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
}
