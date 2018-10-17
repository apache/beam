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
import org.apache.beam.sdk.transforms.ParDo;
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

    assertEquals(5, reader.getCounterMetric("counter", -1));
  }

  @Test
  public void testStartTimeIsTheMinimumOfTheDistribution() {
    List<Integer> sampleInputData = Arrays.asList(1, 2, 3, 4, 5);

    createTestPipeline(sampleInputData, new MonitorWithTimeDistribution());
    PipelineResult result = testPipeline.run();

    MetricsReader reader = new MetricsReader(result, NAMESPACE);

    assertEquals(1, reader.getStartTimeMetric(0, "timeDist"));
  }

  @Test
  public void testEndTimeIsTheMaximumOfTheDistribution() {
    List<Integer> sampleInputData = Arrays.asList(1, 2, 3, 4, 5);

    createTestPipeline(sampleInputData, new MonitorWithTimeDistribution());

    PipelineResult result = testPipeline.run();

    MetricsReader reader = new MetricsReader(result, NAMESPACE);

    assertEquals(5, reader.getEndTimeMetric(0, "timeDist"));
  }

  @Test(expected = IllegalStateException.class)
  public void throwsIllegalStateExceptionWhenThereAreMultipleCountersOfTheSameNameAndType() {
    Metrics.counter(NAMESPACE, "counter");
    Metrics.counter(NAMESPACE, "counter");

    PipelineResult result = testPipeline.run();
    MetricsReader reader = new MetricsReader(result, NAMESPACE);
    reader.getCounterMetric("counter", -1);
  }

  @Test
  public void testTimeIsMinusOneIfTimeMetricIsTooFarFromNow() {
    List<Integer> sampleInputData = Arrays.asList(1, 5, 5, 5, 5);

    createTestPipeline(sampleInputData, new MonitorWithTimeDistribution());
    PipelineResult result = testPipeline.run();

    MetricsReader reader = new MetricsReader(result, NAMESPACE);

    assertEquals(-1, reader.getStartTimeMetric(900000000001L, "timeDist"));
    assertEquals(-1, reader.getEndTimeMetric(900000000001L, "timeDist"));
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
}
