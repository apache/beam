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
package org.apache.beam.runners.dataflow;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.JobMetrics;
import com.google.api.services.dataflow.model.MetricStructuredName;
import com.google.api.services.dataflow.model.MetricUpdate;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.dataflow.DataflowPipelineTranslator.JobSpecification;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.extensions.gcp.storage.NoopPathValidator;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Tests for {@link CompositeBatchPipeline} and {@link DataflowTestBatchCoordinator}. */
@RunWith(JUnit4.class)
public class DataflowTestBatchCoordinatorTest {

  @Mock private DataflowClient mockClient;
  private TestDataflowPipelineOptions options;

  private static class IdentityFn extends DoFn<Integer, Integer> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element());
    }
  }

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    options = createTestOptions("TestBatchApp");
  }

  private static TestDataflowPipelineOptions createTestOptions(String appName) {
    TestDataflowPipelineOptions opts = PipelineOptionsFactory.as(TestDataflowPipelineOptions.class);
    opts.setAppName(appName);
    opts.setProject("test-project");
    opts.setRegion("us-central1");
    opts.setTempLocation("gs://test/temp/location");
    opts.setTempRoot("gs://test");
    opts.setGcpCredential(new TestCredential());
    opts.setRunner(TestDataflowRunner.class);
    opts.setPathValidatorClass(NoopPathValidator.class);
    opts.setEnableTestBatching(true);
    opts.setTestBatchMaxSize(2);
    opts.setTestBatchWindowMs(5000L);
    return opts;
  }

  @Test
  public void testCompositeBatchPipelineTranslatesAndRestoresSnapshot() {
    Pipeline p0 = Pipeline.create(createTestOptions("App0"));
    PCollection<Integer> out0 =
        p0.apply("CreateValues", Create.of(1, 2, 3)).apply("MyStep", ParDo.of(new IdentityFn()));
    PAssert.that(out0).containsInAnyOrder(1, 2, 3);

    Pipeline p1 = Pipeline.create(createTestOptions("App1"));
    PCollection<Integer> out1 =
        p1.apply("CreateValues", Create.of(4, 5)).apply("MyStep", ParDo.of(new IdentityFn()));
    PAssert.that(out1).containsInAnyOrder(4, 5);

    Runnable restore0 = p0.captureStateSnapshot();
    Runnable restore1 = p1.captureStateSnapshot();

    p0.setRootNamePrefix("t0");
    p1.setRootNamePrefix("t1");

    CompositeBatchPipeline composite =
        new CompositeBatchPipeline(options, ImmutableList.of(p0, p1));
    assertEquals(2, PAssert.countAsserts(composite));

    DataflowRunner runner = DataflowRunner.fromOptions(options);
    SdkComponents portableComponents =
        SdkComponents.create(options, Environments.createDockerEnvironment("test-image"));
    RunnerApi.Pipeline portableProto =
        PipelineTranslation.toProto(composite, portableComponents, false);

    Set<String> uniqueNames = new HashSet<>();
    for (RunnerApi.PTransform transform :
        portableProto.getComponents().getTransformsMap().values()) {
      uniqueNames.add(transform.getUniqueName());
    }
    assertThat(uniqueNames, hasItem("t0/MyStep"));
    assertThat(uniqueNames, hasItem("t1/MyStep"));

    runner.replaceV1Transforms(composite);
    SdkComponents v1Components =
        SdkComponents.create(options, Environments.createDockerEnvironment("test-image"));
    RunnerApi.Pipeline v1Proto = PipelineTranslation.toProto(composite, v1Components, true, false);
    DataflowPipelineTranslator translator = DataflowPipelineTranslator.fromOptions(options);
    JobSpecification jobSpec =
        translator.translate(composite, v1Proto, v1Components, runner, Collections.emptyList());
    assertTrue(jobSpec.getStepNames().size() >= 2);

    // Restore snapshots and verify p0 returns to its original unprefixed state.
    restore0.run();
    restore1.run();
    assertEquals(null, p0.getRootNamePrefix());
    RunnerApi.Pipeline restoredProto = PipelineTranslation.toProto(p0);
    Set<String> restoredUniqueNames = new HashSet<>();
    for (RunnerApi.PTransform transform :
        restoredProto.getComponents().getTransformsMap().values()) {
      restoredUniqueNames.add(transform.getUniqueName());
    }
    assertThat(restoredUniqueNames, hasItem("MyStep"));
  }

  @Test
  public void testConcurrentPipelinesMergedIntoSingleBatchJobWithScopedMetrics() throws Exception {
    Pipeline p0 = Pipeline.create(createTestOptions("App0"));
    PAssert.that(p0.apply("Create0", Create.of(1, 2)).apply("StepA", ParDo.of(new IdentityFn())))
        .containsInAnyOrder(1, 2);

    Pipeline p1 = Pipeline.create(createTestOptions("App1"));
    PAssert.that(p1.apply("Create1", Create.of(3, 4)).apply("StepB", ParDo.of(new IdentityFn())))
        .containsInAnyOrder(3, 4);

    AtomicInteger submittedJobs = new AtomicInteger(0);
    DataflowRunner mockDelegateRunner = Mockito.mock(DataflowRunner.class);
    when(mockDelegateRunner.run(any(Pipeline.class)))
        .thenAnswer(
            invocation -> {
              submittedJobs.incrementAndGet();
              Pipeline submitted = invocation.getArgument(0);
              RunnerApi.Pipeline proto = PipelineTranslation.toProto(submitted);
              List<MetricUpdate> updates = new ArrayList<>();
              for (String transformId : proto.getComponents().getTransformsMap().keySet()) {
                RunnerApi.PTransform transform =
                    proto.getComponents().getTransformsMap().get(transformId);
                String uniqueName = transform.getUniqueName();
                if (uniqueName.endsWith("/StepA")) {
                  updates.addAll(createCounterUpdates(transformId, "CustomNs", "myCounter", 10L));
                  updates.add(
                      createTentativePAssertUpdate(transformId, PAssert.SUCCESS_COUNTER, 1));
                } else if (uniqueName.endsWith("/StepB")) {
                  updates.addAll(createCounterUpdates(transformId, "CustomNs", "myCounter", 20L));
                  updates.add(
                      createTentativePAssertUpdate(transformId, PAssert.SUCCESS_COUNTER, 1));
                }
              }
              JobMetrics jobMetrics = new JobMetrics().setMetrics(updates);
              when(mockClient.getJobMetrics(anyString())).thenReturn(jobMetrics);
              when(mockClient.getJob(anyString()))
                  .thenReturn(new Job().setCurrentState("JOB_STATE_DONE"));

              DataflowPipelineJob job =
                  Mockito.spy(
                      new DataflowPipelineJob(
                          mockClient, "batch-job-1", options, Collections.emptyMap(), proto));
              Mockito.doReturn(State.DONE).when(job).getState();
              Mockito.doReturn(State.DONE).when(job).waitUntilFinish(any(), any());
              return job;
            });

    TestDataflowRunner runner0 =
        TestDataflowRunner.fromOptionsAndClient(createTestOptions("App0"), mockClient);
    TestDataflowRunner runner1 =
        TestDataflowRunner.fromOptionsAndClient(createTestOptions("App1"), mockClient);

    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      Callable<DataflowPipelineJob> task0 = () -> runner0.run(p0, mockDelegateRunner);
      Callable<DataflowPipelineJob> task1 = () -> runner1.run(p1, mockDelegateRunner);
      List<Future<DataflowPipelineJob>> futures = pool.invokeAll(Arrays.asList(task0, task1));

      DataflowPipelineJob result0 = futures.get(0).get();
      DataflowPipelineJob result1 = futures.get(1).get();

      assertEquals(1, submittedJobs.get());

      MetricQueryResults metrics0 =
          result0
              .metrics()
              .queryMetrics(
                  MetricsFilter.builder()
                      .addNameFilter(MetricNameFilter.named("CustomNs", "myCounter"))
                      .build());
      List<MetricResult<Long>> counters0 = ImmutableList.copyOf(metrics0.getCounters());
      assertEquals(1, counters0.size());
      assertEquals(
          MetricKey.create("StepA", MetricName.named("CustomNs", "myCounter")),
          counters0.get(0).getKey());
      assertThat(counters0.get(0).getCommitted(), is(10L));

      MetricQueryResults metrics1 =
          result1
              .metrics()
              .queryMetrics(
                  MetricsFilter.builder()
                      .addNameFilter(MetricNameFilter.named("CustomNs", "myCounter"))
                      .build());
      List<MetricResult<Long>> counters1 = ImmutableList.copyOf(metrics1.getCounters());
      assertEquals(1, counters1.size());
      assertEquals(
          MetricKey.create("StepB", MetricName.named("CustomNs", "myCounter")),
          counters1.get(0).getKey());
      assertThat(counters1.get(0).getCommitted(), is(20L));
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void testFailedBatchFallsBackToStandaloneExecution() throws Exception {
    Pipeline p0 = Pipeline.create(createTestOptions("App0"));
    PAssert.that(p0.apply("Create0", Create.of(1, 2))).containsInAnyOrder(1, 2);

    Pipeline p1 = Pipeline.create(createTestOptions("App1"));
    PAssert.that(p1.apply("Create1", Create.of(3, 4))).containsInAnyOrder(3, 4);

    AtomicInteger submittedJobs = new AtomicInteger(0);
    DataflowRunner mockDelegateRunner = Mockito.mock(DataflowRunner.class);
    when(mockDelegateRunner.run(any(Pipeline.class)))
        .thenAnswer(
            invocation -> {
              int callNum = submittedJobs.incrementAndGet();
              Pipeline submitted = invocation.getArgument(0);
              if (submitted instanceof CompositeBatchPipeline) {
                throw new RuntimeException("Simulated batch failure");
              }
              // Verify snapshot was restored so standalone pipeline has null rootNamePrefix.
              assertEquals(null, submitted.getRootNamePrefix());
              DataflowPipelineJob standaloneJob = Mockito.mock(DataflowPipelineJob.class);
              when(standaloneJob.getState()).thenReturn(State.DONE);
              when(standaloneJob.getJobId()).thenReturn("standalone-job-" + callNum);
              when(mockClient.getJobMetrics(anyString()))
                  .thenReturn(
                      new JobMetrics()
                          .setMetrics(
                              Collections.singletonList(
                                  createTentativePAssertUpdate("s1", PAssert.SUCCESS_COUNTER, 1))));
              return standaloneJob;
            });

    TestDataflowRunner runner0 =
        TestDataflowRunner.fromOptionsAndClient(createTestOptions("App0"), mockClient);
    TestDataflowRunner runner1 =
        TestDataflowRunner.fromOptionsAndClient(createTestOptions("App1"), mockClient);

    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      Callable<DataflowPipelineJob> task0 = () -> runner0.run(p0, mockDelegateRunner);
      Callable<DataflowPipelineJob> task1 = () -> runner1.run(p1, mockDelegateRunner);
      List<Future<DataflowPipelineJob>> futures = pool.invokeAll(Arrays.asList(task0, task1));

      assertEquals(State.DONE, futures.get(0).get().getState());
      assertEquals(State.DONE, futures.get(1).get().getState());
      // 1 batch attempt + 2 standalone fallback executions = 3 total calls
      assertEquals(3, submittedJobs.get());
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void testPartialFailureInBatchOnlyRerunsAndFailsTheFailingTest() throws Exception {
    TestDataflowPipelineOptions opts0 = createTestOptions("GoodTest0");
    opts0.setTestBatchMaxSize(3);
    Pipeline p0 = Pipeline.create(opts0);
    PAssert.that(
            p0.apply("Create0", Create.of(1, 2)).apply("GoodStep0", ParDo.of(new IdentityFn())))
        .containsInAnyOrder(1, 2);

    TestDataflowPipelineOptions opts1 = createTestOptions("FailingTest1");
    opts1.setTestBatchMaxSize(3);
    Pipeline p1 = Pipeline.create(opts1);
    PAssert.that(
            p1.apply("Create1", Create.of(3, 4)).apply("FailingStep1", ParDo.of(new IdentityFn())))
        .containsInAnyOrder(99);

    TestDataflowPipelineOptions opts2 = createTestOptions("GoodTest2");
    opts2.setTestBatchMaxSize(3);
    Pipeline p2 = Pipeline.create(opts2);
    PAssert.that(
            p2.apply("Create2", Create.of(5, 6)).apply("GoodStep2", ParDo.of(new IdentityFn())))
        .containsInAnyOrder(5, 6);

    Map<String, JobMetrics> metricsByJobId = new ConcurrentHashMap<>();
    when(mockClient.getJobMetrics(anyString()))
        .thenAnswer(inv -> metricsByJobId.get(inv.<String>getArgument(0)));

    DataflowRunner realTransformReplacer = DataflowRunner.fromOptions(options);
    AtomicInteger submittedJobs = new AtomicInteger(0);
    DataflowRunner mockDelegateRunner = Mockito.mock(DataflowRunner.class);
    when(mockDelegateRunner.run(any(Pipeline.class)))
        .thenAnswer(
            invocation -> {
              int callNum = submittedJobs.incrementAndGet();
              Pipeline submitted = invocation.getArgument(0);
              // Apply real Dataflow V1 transform overrides to verify snapshot restoration works
              // even after composite pipeline graph surgery.
              realTransformReplacer.replaceV1Transforms(submitted);
              RunnerApi.Pipeline proto = PipelineTranslation.toProto(submitted);

              if (submitted instanceof CompositeBatchPipeline) {
                List<MetricUpdate> updates = new ArrayList<>();
                for (String transformId : proto.getComponents().getTransformsMap().keySet()) {
                  String uniqueName =
                      proto.getComponents().getTransformsMap().get(transformId).getUniqueName();
                  if (uniqueName.endsWith("/GoodStep0") || uniqueName.endsWith("/GoodStep2")) {
                    updates.add(
                        createTentativePAssertUpdate(transformId, PAssert.SUCCESS_COUNTER, 1));
                  } else if (uniqueName.endsWith("/FailingStep1")) {
                    updates.add(
                        createTentativePAssertUpdate(transformId, PAssert.FAILURE_COUNTER, 1));
                  }
                }
                metricsByJobId.put("batch-job", new JobMetrics().setMetrics(updates));
                DataflowPipelineJob batchJob =
                    Mockito.spy(
                        new DataflowPipelineJob(
                            mockClient, "batch-job", options, Collections.emptyMap(), proto));
                Mockito.doReturn(State.DONE).when(batchJob).getState();
                Mockito.doReturn(State.DONE).when(batchJob).waitUntilFinish(any(), any());
                return batchJob;
              } else {
                // Standalone fallback for FailingTest1: verify prefix was restored to null.
                assertEquals(null, submitted.getRootNamePrefix());
                String standaloneJobId = "standalone-failing-" + callNum;
                metricsByJobId.put(
                    standaloneJobId,
                    new JobMetrics()
                        .setMetrics(
                            Collections.singletonList(
                                createTentativePAssertUpdate("s1", PAssert.FAILURE_COUNTER, 1))));
                DataflowPipelineJob standaloneJob = Mockito.mock(DataflowPipelineJob.class);
                when(standaloneJob.getJobId()).thenReturn(standaloneJobId);
                when(standaloneJob.getState()).thenReturn(State.DONE);
                when(standaloneJob.waitUntilFinish(any(), any())).thenReturn(State.DONE);
                return standaloneJob;
              }
            });

    TestDataflowRunner runner0 = TestDataflowRunner.fromOptionsAndClient(opts0, mockClient);
    TestDataflowRunner runner1 = TestDataflowRunner.fromOptionsAndClient(opts1, mockClient);
    TestDataflowRunner runner2 = TestDataflowRunner.fromOptionsAndClient(opts2, mockClient);

    ExecutorService pool = Executors.newFixedThreadPool(3);
    try {
      Callable<DataflowPipelineJob> task0 = () -> runner0.run(p0, mockDelegateRunner);
      Callable<DataflowPipelineJob> task1 = () -> runner1.run(p1, mockDelegateRunner);
      Callable<DataflowPipelineJob> task2 = () -> runner2.run(p2, mockDelegateRunner);
      List<Future<DataflowPipelineJob>> futures =
          pool.invokeAll(Arrays.asList(task0, task1, task2));

      // GoodTest0 and GoodTest2 succeed directly from the batch job.
      assertEquals(State.DONE, futures.get(0).get().getState());
      assertEquals(State.DONE, futures.get(2).get().getState());

      // FailingTest1 falls back to standalone and throws AssertionError.
      ExecutionException e = assertThrows(ExecutionException.class, () -> futures.get(1).get());
      assertTrue(
          "Expected AssertionError but got: " + e.getCause(),
          e.getCause() instanceof AssertionError);

      // 1 merged batch job + 1 standalone fallback job (only for FailingTest1) = 2 total jobs!
      assertEquals(2, submittedJobs.get());
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void testBatchJobStateFailedRerunsAllInStandaloneAndOnlyFailingTestFails()
      throws Exception {
    Pipeline p0 = Pipeline.create(createTestOptions("GoodTest0"));
    PAssert.that(
            p0.apply("Create0", Create.of(1, 2)).apply("GoodStep0", ParDo.of(new IdentityFn())))
        .containsInAnyOrder(1, 2);

    Pipeline p1 = Pipeline.create(createTestOptions("FailingTest1"));
    PAssert.that(
            p1.apply("Create1", Create.of(3, 4)).apply("FailingStep1", ParDo.of(new IdentityFn())))
        .containsInAnyOrder(99);

    Map<String, JobMetrics> metricsByJobId = new ConcurrentHashMap<>();
    when(mockClient.getJobMetrics(anyString()))
        .thenAnswer(inv -> metricsByJobId.get(inv.<String>getArgument(0)));

    DataflowRunner realTransformReplacer = DataflowRunner.fromOptions(options);
    AtomicInteger submittedJobs = new AtomicInteger(0);
    DataflowRunner mockDelegateRunner = Mockito.mock(DataflowRunner.class);
    when(mockDelegateRunner.run(any(Pipeline.class)))
        .thenAnswer(
            invocation -> {
              int callNum = submittedJobs.incrementAndGet();
              Pipeline submitted = invocation.getArgument(0);
              realTransformReplacer.replaceV1Transforms(submitted);
              RunnerApi.Pipeline proto = PipelineTranslation.toProto(submitted);

              if (submitted instanceof CompositeBatchPipeline) {
                // Simulate the merged Dataflow job terminating in State.FAILED because one test
                // threw an AssertionError on the worker.
                DataflowPipelineJob failedBatchJob = Mockito.mock(DataflowPipelineJob.class);
                when(failedBatchJob.getJobId()).thenReturn("failed-batch-job");
                when(failedBatchJob.getState()).thenReturn(State.FAILED);
                when(failedBatchJob.waitUntilFinish(any(), any())).thenReturn(State.FAILED);
                return failedBatchJob;
              } else {
                assertEquals(null, submitted.getRootNamePrefix());
                boolean isFailingPipeline = false;
                for (RunnerApi.PTransform t : proto.getComponents().getTransformsMap().values()) {
                  if (t.getUniqueName().equals("FailingStep1")) {
                    isFailingPipeline = true;
                    break;
                  }
                }
                String jobId =
                    (isFailingPipeline ? "standalone-fail-" : "standalone-pass-") + callNum;
                MetricUpdate metric =
                    createTentativePAssertUpdate(
                        "s1",
                        isFailingPipeline ? PAssert.FAILURE_COUNTER : PAssert.SUCCESS_COUNTER,
                        1);
                metricsByJobId.put(
                    jobId, new JobMetrics().setMetrics(Collections.singletonList(metric)));
                DataflowPipelineJob standaloneJob = Mockito.mock(DataflowPipelineJob.class);
                when(standaloneJob.getJobId()).thenReturn(jobId);
                when(standaloneJob.getState())
                    .thenReturn(isFailingPipeline ? State.FAILED : State.DONE);
                when(standaloneJob.waitUntilFinish(any(), any()))
                    .thenReturn(isFailingPipeline ? State.FAILED : State.DONE);
                return standaloneJob;
              }
            });

    TestDataflowRunner runner0 =
        TestDataflowRunner.fromOptionsAndClient(createTestOptions("GoodTest0"), mockClient);
    TestDataflowRunner runner1 =
        TestDataflowRunner.fromOptionsAndClient(createTestOptions("FailingTest1"), mockClient);

    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      Callable<DataflowPipelineJob> task0 = () -> runner0.run(p0, mockDelegateRunner);
      Callable<DataflowPipelineJob> task1 = () -> runner1.run(p1, mockDelegateRunner);
      List<Future<DataflowPipelineJob>> futures = pool.invokeAll(Arrays.asList(task0, task1));

      // GoodTest0 succeeds after standalone fallback!
      assertEquals(State.DONE, futures.get(0).get().getState());

      // FailingTest1 fails with AssertionError after standalone fallback!
      ExecutionException e = assertThrows(ExecutionException.class, () -> futures.get(1).get());
      assertTrue(
          "Expected AssertionError but got: " + e.getCause(),
          e.getCause() instanceof AssertionError);

      // 1 failed batch job + 2 standalone fallback jobs = 3 total jobs
      assertEquals(3, submittedJobs.get());
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void testEligibilityChecks() {
    Pipeline boundedPipeline = Pipeline.create(options);
    boundedPipeline.apply(Create.of(1, 2, 3));
    assertTrue(DataflowTestBatchCoordinator.isEligibleForBatching(boundedPipeline, options));

    // Streaming pipelines bypass batching.
    TestDataflowPipelineOptions streamingOpts = createTestOptions("StreamingApp");
    streamingOpts.setStreaming(true);
    assertFalse(DataflowTestBatchCoordinator.isEligibleForBatching(boundedPipeline, streamingOpts));

    // Unbounded pipelines bypass batching.
    Pipeline unboundedPipeline = Pipeline.create(options);
    unboundedPipeline.apply(GenerateSequence.from(0));
    assertFalse(DataflowTestBatchCoordinator.isEligibleForBatching(unboundedPipeline, options));
  }

  private static MetricUpdate createTentativePAssertUpdate(
      String stepId, String counterName, int value) {
    MetricStructuredName name =
        new MetricStructuredName()
            .setName(counterName)
            .setOrigin("user")
            .setContext(
                ImmutableMap.of(
                    "step", stepId, "namespace", PAssert.class.getName(), "tentative", "true"));
    return new MetricUpdate().setName(name).setScalar(new BigDecimal(value));
  }

  private static List<MetricUpdate> createCounterUpdates(
      String stepId, String namespace, String counterName, long value) {
    MetricStructuredName committedName =
        new MetricStructuredName()
            .setName(counterName)
            .setOrigin("user")
            .setContext(ImmutableMap.of("step", stepId, "namespace", namespace));
    MetricStructuredName tentativeName =
        new MetricStructuredName()
            .setName(counterName)
            .setOrigin("user")
            .setContext(
                ImmutableMap.of("step", stepId, "namespace", namespace, "tentative", "true"));
    return ImmutableList.of(
        new MetricUpdate().setName(committedName).setScalar(new BigDecimal(value)),
        new MetricUpdate().setName(tentativeName).setScalar(new BigDecimal(value)));
  }
}
