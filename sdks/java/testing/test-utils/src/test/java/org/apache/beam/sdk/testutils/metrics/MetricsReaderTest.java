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
package org.apache.beam.sdk.testutils.metrics;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link MetricsReader}. */
@RunWith(JUnit4.class)
public class MetricsReaderTest {

  @Rule public TestPipeline testPipeline = TestPipeline.create();

  private static final String NAMESPACE = "Testing";

  @Test
  public void testCounterMetricReceivedFromPipelineResult() {
    List<Integer> sampleInputData = Arrays.asList(1, 1, 1, 1, 1);

    createTestPipeline(sampleInputData, new MonitorWithCounter());
    PipelineResult result = testPipeline.run();

    MetricsReader reader = new MetricsReader(result, NAMESPACE);

    assertEquals(5, reader.getCounterMetric("counter"));
  }

  @Test
  public void testStartTimeIsTheMinimumFromAllCollectedDistributions() {
    List<Integer> sampleInputData = Arrays.asList(1, 2, 3, 4, 5);

    createTestPipelineWithBranches(sampleInputData);

    PipelineResult result = testPipeline.run();
    MetricsReader reader = new MetricsReader(result, NAMESPACE, 0);
    assertEquals(1, reader.getStartTimeMetric("timeDist"));
  }

  @Test
  public void testEndTimeIsTheMaximumOfAllCollectedDistributions() {
    List<Integer> sampleInputData = Arrays.asList(1, 2, 3, 4, 5);

    createTestPipelineWithBranches(sampleInputData);

    PipelineResult result = testPipeline.run();
    MetricsReader reader = new MetricsReader(result, NAMESPACE, 0);
    assertEquals(10, reader.getEndTimeMetric("timeDist"));
  }

  /**
   * Branching pipelines ensure that multiple metric results of the same name are created. Thanks to
   * that it is possible to test if MetricsReader can collect metrics in such case.
   */
  private void createTestPipelineWithBranches(List<Integer> sampleInputData) {
    PCollection<Integer> inputData = testPipeline.apply(Create.of(sampleInputData));
    inputData.apply("Monitor #1", ParDo.of(new MonitorWithTimeDistribution()));

    inputData
        .apply("Multiply input", MapElements.via(new MultiplyElements()))
        .apply("Monitor #2", ParDo.of(new MonitorWithTimeDistribution()));
  }

  @Test
  public void doesntThrowIllegalStateExceptionWhenThereIsNoMetricFound() {
    PipelineResult result = testPipeline.run();
    MetricsReader reader = new MetricsReader(result, NAMESPACE);
    reader.getCounterMetric("nonexistent");
  }

  @Test
  public void testTimeIsMinusOneIfTimeMetricIsTooFarFromNow() {
    List<Integer> sampleInputData = Arrays.asList(1, 5, 5, 5, 5);

    createTestPipeline(sampleInputData, new MonitorWithTimeDistribution());
    PipelineResult result = testPipeline.run();

    MetricsReader reader = new MetricsReader(result, NAMESPACE, 900000000001L);

    assertEquals(-1, reader.getStartTimeMetric("timeDist"));
    assertEquals(-1, reader.getEndTimeMetric("timeDist"));
  }

  private void createTestPipeline(List<Integer> sampleInputData, DoFn<Integer, Integer> monitor) {
    testPipeline.apply(Create.of(sampleInputData)).apply(ParDo.of(monitor));
  }

  /** Counts total elements of the input data provided. */
  private static class MonitorWithCounter extends DoFn<Integer, Integer> {
    private final Counter elementCounter = Metrics.counter(NAMESPACE, "counter");

    @ProcessElement
    public void processElement(ProcessContext c) {
      elementCounter.inc();
    }
  }

  /** Simulates time flow by updating the distribution metric with input collection elements. */
  private static class MonitorWithTimeDistribution extends DoFn<Integer, Integer> {
    private final Distribution timeDistribution = Metrics.distribution(NAMESPACE, "timeDist");

    @ProcessElement
    public void processElement(ProcessContext c) {
      timeDistribution.update(c.element().longValue());
    }
  }

  private static class MultiplyElements extends SimpleFunction<Integer, Integer> {
    @Override
    public Integer apply(Integer input) {
      return input * 2;
    }
  }
}
