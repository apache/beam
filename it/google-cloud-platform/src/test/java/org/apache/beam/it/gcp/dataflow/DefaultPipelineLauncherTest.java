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
package org.apache.beam.it.gcp.dataflow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Instant;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.gcp.IOLoadTestBase;
import org.apache.beam.it.gcp.IOLoadTestBase.PipelineMetricsType;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testutils.metrics.TimeMonitor;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link DefaultPipelineLauncher}. */
@RunWith(JUnit4.class)
public class DefaultPipelineLauncherTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testPipelineMetrics() throws IOException {
    DefaultPipelineLauncher launcher = DefaultPipelineLauncher.builder().build();
    final String timeMetrics = "run_time";
    final String counterMetrics = "counter";
    final long numElements = 1000L;

    pipeline
        .apply(GenerateSequence.from(0).to(numElements))
        .apply(ParDo.of(new TimeMonitor<>(IOLoadTestBase.BEAM_METRICS_NAMESPACE, timeMetrics)))
        .apply(ParDo.of(new IOLoadTestBase.CountingFn<>(counterMetrics)));

    PipelineLauncher.LaunchConfig options =
        PipelineLauncher.LaunchConfig.builder("test-bigquery-write")
            .setSdk(PipelineLauncher.Sdk.JAVA)
            .setPipeline(pipeline)
            .addParameter("runner", "DirectRunner")
            .build();

    PipelineLauncher.LaunchInfo launchInfo = launcher.launch("", "", options);
    long currentTime = System.currentTimeMillis();
    long startTime =
        launcher.getBeamMetric(launchInfo.jobId(), PipelineMetricsType.STARTTIME, timeMetrics);
    long endTime =
        launcher.getBeamMetric(launchInfo.jobId(), PipelineMetricsType.ENDTIME, timeMetrics);
    long runTime =
        launcher.getBeamMetric(launchInfo.jobId(), PipelineMetricsType.RUNTIME, timeMetrics);
    long count =
        launcher.getBeamMetric(launchInfo.jobId(), PipelineMetricsType.COUNTER, counterMetrics);

    // check the validity of time metrics: start time and end time metrics is within 1 minute of
    // current
    assertTrue(
        String.format(
            "start time metrics (%s) is not around current time", Instant.ofEpochMilli(startTime)),
        Math.abs(startTime - currentTime) < 10_000);
    assertTrue(
        String.format(
            "start time metrics (%s) is not around current time", Instant.ofEpochMilli(endTime)),
        Math.abs(endTime - currentTime) < 10_000);
    assertTrue("run time should be greater than 0", runTime > 0);

    assertEquals(numElements, count);
  }
}
