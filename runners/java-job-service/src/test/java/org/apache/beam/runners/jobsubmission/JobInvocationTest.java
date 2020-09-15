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
package org.apache.beam.runners.jobsubmission;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsSame.sameInstance;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.beam.model.jobmanagement.v1.JobApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.Struct;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.MoreExecutors;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Tests for {@link JobInvocation}. */
public class JobInvocationTest {

  private static ExecutorService executorService;

  private JobInvocation jobInvocation;
  private ControllablePipelineRunner runner;

  @Before
  public void setup() {
    executorService = Executors.newFixedThreadPool(1);
    JobInfo jobInfo =
        JobInfo.create("jobid", "jobName", "retrievalToken", Struct.getDefaultInstance());
    ListeningExecutorService listeningExecutorService =
        MoreExecutors.listeningDecorator(executorService);
    Pipeline pipeline = Pipeline.create();
    runner = new ControllablePipelineRunner();
    jobInvocation =
        new JobInvocation(
            jobInfo, listeningExecutorService, PipelineTranslation.toProto(pipeline), runner);
  }

  @After
  public void tearDown() throws Exception {
    executorService.shutdownNow();
    executorService = null;
  }

  @Test(timeout = 10_000)
  public void testStateAfterCompletion() throws Exception {
    jobInvocation.start();
    assertThat(jobInvocation.getState(), is(JobApi.JobState.Enum.RUNNING));

    TestPipelineResult pipelineResult = new TestPipelineResult(PipelineResult.State.DONE);
    runner.setResult(pipelineResult);

    awaitJobState(jobInvocation, JobApi.JobState.Enum.DONE);
  }

  @Test(timeout = 10_000)
  public void testStateAfterCompletionWithoutResult() throws Exception {
    jobInvocation.start();
    assertThat(jobInvocation.getState(), is(JobApi.JobState.Enum.RUNNING));

    // Let pipeline finish without a result.
    runner.setResult(null);

    awaitJobState(jobInvocation, JobApi.JobState.Enum.UNSPECIFIED);
  }

  @Test(timeout = 10_000)
  public void testStateAfterCancellation() throws Exception {
    jobInvocation.start();
    assertThat(jobInvocation.getState(), is(JobApi.JobState.Enum.RUNNING));

    jobInvocation.cancel();
    awaitJobState(jobInvocation, JobApi.JobState.Enum.CANCELLED);
  }

  @Test(timeout = 10_000)
  public void testStateAfterCancellationWithPipelineResult() throws Exception {
    jobInvocation.start();
    assertThat(jobInvocation.getState(), is(JobApi.JobState.Enum.RUNNING));

    TestPipelineResult pipelineResult = new TestPipelineResult(PipelineResult.State.FAILED);
    runner.setResult(pipelineResult);
    awaitJobState(jobInvocation, JobApi.JobState.Enum.FAILED);

    jobInvocation.cancel();
    pipelineResult.cancelLatch.await();
  }

  @Test(timeout = 10_000)
  public void testNoCancellationWhenDone() throws Exception {
    jobInvocation.start();
    assertThat(jobInvocation.getState(), is(JobApi.JobState.Enum.RUNNING));

    TestPipelineResult pipelineResult = new TestPipelineResult(PipelineResult.State.DONE);
    runner.setResult(pipelineResult);
    awaitJobState(jobInvocation, JobApi.JobState.Enum.DONE);

    jobInvocation.cancel();
    assertThat(jobInvocation.getState(), is(JobApi.JobState.Enum.DONE));
    // Ensure that cancel has not been called
    assertThat(pipelineResult.cancelLatch.getCount(), is(1L));
  }

  @Test(timeout = 10_000)
  public void testReturnsMetricsFromJobInvocationAfterSuccess() throws Exception {
    JobApi.MetricResults expectedMonitoringInfos = JobApi.MetricResults.newBuilder().build();
    TestPipelineResult result =
        new TestPipelineResult(PipelineResult.State.DONE, expectedMonitoringInfos);

    jobInvocation.start();
    runner.setResult(result);

    awaitJobState(jobInvocation, JobApi.JobState.Enum.DONE);

    assertThat(
        jobInvocation.getMetrics(),
        allOf(is(notNullValue()), is(sameInstance(result.portableMetrics()))));
  }

  private static void awaitJobState(JobInvocation jobInvocation, JobApi.JobState.Enum jobState)
      throws Exception {
    while (jobInvocation.getState() != jobState) {
      Thread.sleep(50);
    }
  }

  private static class ControllablePipelineRunner implements PortablePipelineRunner {

    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile PortablePipelineResult result;

    @Override
    public PortablePipelineResult run(RunnerApi.Pipeline pipeline, JobInfo jobInfo)
        throws Exception {
      latch.await();
      return result;
    }

    void setResult(PortablePipelineResult pipelineResult) {
      result = pipelineResult;
      latch.countDown();
    }
  }

  private static class TestPipelineResult implements PortablePipelineResult {

    private final State state;
    private final CountDownLatch cancelLatch = new CountDownLatch(1);
    private JobApi.MetricResults monitoringInfos;

    private TestPipelineResult(State state, JobApi.MetricResults monitoringInfos) {
      this.state = state;
      this.monitoringInfos = monitoringInfos;
    }

    private TestPipelineResult(State state) {
      this(state, JobApi.MetricResults.newBuilder().build());
    }

    @Override
    public State getState() {
      return state;
    }

    @Override
    public State cancel() {
      cancelLatch.countDown();
      return State.CANCELLED;
    }

    @Override
    public State waitUntilFinish(Duration duration) {
      return null;
    }

    @Override
    public State waitUntilFinish() {
      return null;
    }

    @Override
    public MetricResults metrics() {
      return null;
    }

    @Override
    public JobApi.MetricResults portableMetrics() {
      return monitoringInfos;
    }
  }
}
