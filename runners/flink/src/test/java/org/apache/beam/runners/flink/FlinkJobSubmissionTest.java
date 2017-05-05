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
package org.apache.beam.runners.flink;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.Collections;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.joda.time.Duration;
import org.junit.Test;

/**
 * Tests for non-blocking job submission to Flink and for the querying methods of
 * {@link org.apache.beam.sdk.PipelineResult} on Flink.
 *
 * <p>Note: these tests take a bit longer because the Accumulator update code (that is used for
 * transporting Metrics on the Flink runner) only kicks in after a couple of seconds.
 */
public class FlinkJobSubmissionTest implements Serializable {

  /**
   * Without non-blocking job submission this test will time out.
   */
  @Test(timeout = 30_000)
  public void testLocalJobSubmissionAndCancellation() throws Exception {
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);

    options.setRunner(FlinkRunner.class);
    options.setStreaming(true);

    Pipeline p = Pipeline.create(options);

    p
        .apply(GenerateSequence.from(0))
        .apply("stepName", ParDo.of(new DoFn<Long, Long>() {

          Counter count = Metrics.counter("namespace", "name");

          @ProcessElement
          public void process(ProcessContext ctx) {
            count.inc();
          }
        }));

    PipelineResult job = p.run();

    boolean running = true;
    while (running) {
      MetricQueryResults metricQueryResults =
          job.metrics().queryMetrics(
              MetricsFilter.builder()
                  .addStep("stepName")
                  .build());

      // cancel the unbounded job if we see a high enough count
      for (MetricResult<Long> counter : metricQueryResults.counters()) {
        if (counter.attempted() > 1_000) {
          job.cancel();
          running = false;
        }
      }

      // don't hammer the JobManager to badly
      Thread.sleep(100);
    }

    PipelineResult.State state = job.waitUntilFinish();
    assertEquals(PipelineResult.State.CANCELLED, state);
  }

  /**
   * Without non-blocking job submission this test will time out.
   *
   * <p>This test uses the in-process {@link org.apache.flink.runtime.minicluster.FlinkMiniCluster}
   * but uses the remote submission process that would be used for submitting to a Flink
   * cluster.
   */
  @Test(timeout = 30_000)
  public void testRemoteJobSubmissionAndCancellation() throws Exception {
    Configuration configuration = new Configuration();

    configuration.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 8);

    LocalFlinkMiniCluster flinkMiniCluster = new LocalFlinkMiniCluster(configuration, false);
    flinkMiniCluster.start();

    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);

    options.setRunner(FlinkRunner.class);
    options.setStreaming(true);

    // retrieve the hostname and port from the mini cluster
    options.setFlinkMaster(flinkMiniCluster.hostname() + ":" + flinkMiniCluster.getLeaderRPCPort());

    // we don't submit any jar files
    options.setFilesToStage(Collections.<String>emptyList());

    Pipeline p = Pipeline.create(options);

    p
        .apply(GenerateSequence.from(0))
        .apply("stepName", ParDo.of(new DoFn<Long, Long>() {

          Counter count = Metrics.counter("namespace", "name");

          @ProcessElement
          public void process(ProcessContext ctx) {
            count.inc();
          }
        }));

    PipelineResult job = p.run();

    boolean running = true;
    while (running) {
      MetricQueryResults metricQueryResults =
          job.metrics().queryMetrics(
              MetricsFilter.builder()
                  .addStep("stepName")
                  .build());

      // cancel the unbounded job if we see a high enough count
      for (MetricResult<Long> counter : metricQueryResults.counters()) {
        if (counter.attempted() > 1_000) {
          job.cancel();
          running = false;
        }
      }

      // don't hammer the JobManager to badly
      Thread.sleep(100);
    }

    PipelineResult.State state = job.waitUntilFinish();
    assertEquals(PipelineResult.State.CANCELLED, state);
  }

  @Test(timeout = 30_000)
  public void testLocalJobSubmissionWaitForFinishTimeout() throws Exception {
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);

    options.setRunner(FlinkRunner.class);
    options.setStreaming(true);

    Pipeline p = Pipeline.create(options);

    p
        .apply(GenerateSequence.from(0))
        .apply("stepName", ParDo.of(new DoFn<Long, Long>() {
          @ProcessElement
          public void process(ProcessContext ctx) {
          }
        }));

    PipelineResult job = p.run();

    PipelineResult.State state = job.waitUntilFinish(Duration.standardSeconds(1));
    assertEquals(PipelineResult.State.RUNNING, state);
  }

  /**
   * This test uses the in-process {@link org.apache.flink.runtime.minicluster.FlinkMiniCluster}
   * but uses the remote submission process that would be used for submitting to a Flink
   * cluster.
   */
  @Test(timeout = 30_000)
  public void testRemoteJobSubmissionWaitForFinishTimeout() throws Exception {
    Configuration configuration = new Configuration();

    configuration.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 8);

    LocalFlinkMiniCluster flinkMiniCluster = new LocalFlinkMiniCluster(configuration, false);
    flinkMiniCluster.start();

    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);

    options.setRunner(FlinkRunner.class);
    options.setStreaming(true);

    // retrieve the hostname and port from the mini cluster
    options.setFlinkMaster(flinkMiniCluster.hostname() + ":" + flinkMiniCluster.getLeaderRPCPort());

    // we don't submit any jar files
    options.setFilesToStage(Collections.<String>emptyList());

    Pipeline p = Pipeline.create(options);

    p
        .apply(GenerateSequence.from(0))
        .apply("stepName", ParDo.of(new DoFn<Long, Long>() {

          @ProcessElement
          public void process(ProcessContext ctx) {
          }
        }));

    PipelineResult job = p.run();

    PipelineResult.State state = job.waitUntilFinish(Duration.standardSeconds(1));
    assertEquals(PipelineResult.State.RUNNING, state);
  }
}
