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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.model.ComponentTransform;
import com.google.api.services.dataflow.model.ExecutionStageState;
import com.google.api.services.dataflow.model.ExecutionStageSummary;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.JobMetrics;
import com.google.api.services.dataflow.model.MetricStructuredName;
import com.google.api.services.dataflow.model.MetricUpdate;
import com.google.api.services.dataflow.model.PipelineDescription;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.dataflow.DataflowPipelineTranslator.JobSpecification;
import org.apache.beam.runners.dataflow.DataflowTestBatchCoordinator.BatchingConfig;
import org.apache.beam.runners.dataflow.DataflowTestBatchCoordinator.ScopedDataflowPipelineJob;
import org.apache.beam.runners.dataflow.DataflowTestBatchCoordinator.StageAttribution;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
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
import org.apache.beam.sdk.options.PipelineOptions;
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
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.After;
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

  private static final long WAIT_SECONDS = 60;
  private static final String STAGE_DONE = "JOB_STATE_DONE";
  private static final String STAGE_FAILED = "JOB_STATE_FAILED";
  private static final String STAGE_PENDING = "JOB_STATE_PENDING";

  @Mock private DataflowClient mockClient;
  private TestDataflowPipelineOptions options;
  private ExecutorService pool;

  /** Metrics served by {@link #mockClient}, keyed by job id. Safe to populate from any thread. */
  private final Map<String, JobMetrics> metricsByJobId = new ConcurrentHashMap<>();

  /** The {@code JOB_VIEW_ALL} job served by {@link #mockClient} for any job id. */
  private final AtomicReference<Job> executionDetails = new AtomicReference<>();

  private static class IdentityFn extends DoFn<Integer, Integer> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element());
    }
  }

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    options = createTestOptions("TestBatchApp");
    pool = Executors.newCachedThreadPool();
    configureBatching(2, 5000L);
    when(mockClient.getJobMetrics(anyString()))
        .thenAnswer(inv -> metricsByJobId.get(inv.<String>getArgument(0)));
    when(mockClient.getJob(anyString(), eq("JOB_VIEW_ALL")))
        .thenAnswer(inv -> executionDetails.get());
  }

  @After
  public void tearDown() {
    pool.shutdownNow();
    DataflowTestBatchCoordinator.setConfigForTesting(null);
  }

  /** Installs the run-wide batching configuration for the current test. */
  private static void configureBatching(int maxBatchSize, long windowMs) {
    DataflowTestBatchCoordinator.setConfigForTesting(
        new BatchingConfig(/* enabled= */ true, maxBatchSize, windowMs));
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
    return opts;
  }

  /** One test pipeline together with the options and runner it is submitted through. */
  private final class Member {
    final TestDataflowPipelineOptions opts;
    final Pipeline pipeline;
    final TestDataflowRunner runner;

    Member(String appName) {
      this.opts = createTestOptions(appName);
      this.pipeline = Pipeline.create(opts);
      this.runner = TestDataflowRunner.fromOptionsAndClient(opts, mockClient);
    }

    /** Applies {@code Create -> ParDo(stepName)} and a PAssert expecting {@code expected}. */
    Member withStep(String stepName, Integer... expected) {
      PCollection<Integer> out =
          pipeline
              .apply("Create-" + stepName, Create.of(1, 2))
              .apply(stepName, ParDo.of(new IdentityFn()));
      PAssert.that(out).containsInAnyOrder(expected);
      return this;
    }

    Future<DataflowPipelineJob> submit(DataflowRunner delegate) {
      return pool.submit(() -> runner.run(pipeline, delegate));
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Composite pipeline construction
  // ---------------------------------------------------------------------------------------------

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

    // Batching is a run-wide switch, not a per-pipeline option.
    DataflowTestBatchCoordinator.setConfigForTesting(new BatchingConfig(false, 2, 5000L));
    assertFalse(DataflowTestBatchCoordinator.isEligibleForBatching(boundedPipeline, options));
    DataflowTestBatchCoordinator.setConfigForTesting(new BatchingConfig(true, 1, 5000L));
    assertFalse(DataflowTestBatchCoordinator.isEligibleForBatching(boundedPipeline, options));
    configureBatching(2, 5000L);
    assertTrue(DataflowTestBatchCoordinator.isEligibleForBatching(boundedPipeline, options));
  }

  // ---------------------------------------------------------------------------------------------
  // Options compatibility
  // ---------------------------------------------------------------------------------------------

  /** An options interface the coordinator knows nothing about. */
  public interface CustomTestOptions extends PipelineOptions {
    @Nullable String getFlavor();

    void setFlavor(String value);

    @Nullable Object getOpaque();

    void setOpaque(Object value);
  }

  @Test
  public void testCompatibilityKeyIgnoresPerTestIdentityOptions() {
    TestDataflowPipelineOptions a = createTestOptions("AppA");
    TestDataflowPipelineOptions b = createTestOptions("AppB");
    b.setJobName("some-other-job-name");
    b.setTempLocation("gs://test/other/temp");
    b.setGcpTempLocation("gs://test/other/gcptemp");
    b.setOptionsId(a.getOptionsId() + 1);
    b.setTestTimeoutSeconds(42L);
    assertNotNull(DataflowTestBatchCoordinator.compatibilityKey(a));
    assertEquals(
        DataflowTestBatchCoordinator.compatibilityKey(a),
        DataflowTestBatchCoordinator.compatibilityKey(b));
  }

  @Test
  public void testCompatibilityKeyDiffersForAnyJobAffectingOption() {
    String base = DataflowTestBatchCoordinator.compatibilityKey(createTestOptions("App"));

    TestDataflowPipelineOptions machineType = createTestOptions("App");
    machineType.as(DataflowPipelineWorkerPoolOptions.class).setWorkerMachineType("n2-standard-8");
    assertNotEquals(base, DataflowTestBatchCoordinator.compatibilityKey(machineType));

    TestDataflowPipelineOptions experiments = createTestOptions("App");
    experiments.setExperiments(ImmutableList.of("use_runner_v2"));
    assertNotEquals(base, DataflowTestBatchCoordinator.compatibilityKey(experiments));

    TestDataflowPipelineOptions debug = createTestOptions("App");
    debug.as(DataflowPipelineDebugOptions.class).setNumberOfWorkerHarnessThreads(3);
    assertNotEquals(base, DataflowTestBatchCoordinator.compatibilityKey(debug));

    TestDataflowPipelineOptions region = createTestOptions("App");
    region.setRegion("europe-west1");
    assertNotEquals(base, DataflowTestBatchCoordinator.compatibilityKey(region));

    TestDataflowPipelineOptions streaming = createTestOptions("App");
    streaming.setStreaming(true);
    assertNotEquals(base, DataflowTestBatchCoordinator.compatibilityKey(streaming));

    // Options set through an interface the coordinator has never heard of still count.
    TestDataflowPipelineOptions vanilla = createTestOptions("App");
    vanilla.as(CustomTestOptions.class).setFlavor("vanilla");
    TestDataflowPipelineOptions vanillaAgain = createTestOptions("App");
    vanillaAgain.as(CustomTestOptions.class).setFlavor("vanilla");
    TestDataflowPipelineOptions chocolate = createTestOptions("App");
    chocolate.as(CustomTestOptions.class).setFlavor("chocolate");
    assertNotEquals(base, DataflowTestBatchCoordinator.compatibilityKey(vanilla));
    assertEquals(
        DataflowTestBatchCoordinator.compatibilityKey(vanilla),
        DataflowTestBatchCoordinator.compatibilityKey(vanillaAgain));
    assertNotEquals(
        DataflowTestBatchCoordinator.compatibilityKey(vanilla),
        DataflowTestBatchCoordinator.compatibilityKey(chocolate));
  }

  @Test
  public void testCompatibilityKeyIsNullForUnserializableOptions() {
    TestDataflowPipelineOptions opts = createTestOptions("App");
    opts.as(CustomTestOptions.class).setOpaque(new Object());
    assertNull(DataflowTestBatchCoordinator.compatibilityKey(opts));
  }

  @Test
  public void testMembersWithIncompatibleOptionsAreNeverMerged() throws Exception {
    // Each member ends up alone in its group; keep the wait for a partner short.
    configureBatching(2, 300L);
    Member small = new Member("SmallWorker").withStep("StepA", 1, 2);
    Member big = new Member("BigWorker").withStep("StepB", 1, 2);
    big.opts.as(DataflowPipelineWorkerPoolOptions.class).setWorkerMachineType("n2-standard-8");

    AtomicInteger submittedJobs = new AtomicInteger(0);
    DataflowRunner delegate = Mockito.mock(DataflowRunner.class);
    when(delegate.run(any(Pipeline.class)))
        .thenAnswer(
            invocation -> {
              int callNum = submittedJobs.incrementAndGet();
              Pipeline submitted = invocation.getArgument(0);
              assertThat(
                  "pipelines with different worker options must not share a job",
                  submitted,
                  not(instanceOf(CompositeBatchPipeline.class)));
              String jobId = "standalone-" + callNum;
              metricsByJobId.put(
                  jobId,
                  new JobMetrics()
                      .setMetrics(
                          ImmutableList.of(
                              createTentativePAssertUpdate("s1", PAssert.SUCCESS_COUNTER, 1))));
              return newStandaloneJob(jobId, State.DONE);
            });

    Future<DataflowPipelineJob> f0 = small.submit(delegate);
    Future<DataflowPipelineJob> f1 = big.submit(delegate);
    assertEquals(State.DONE, f0.get(WAIT_SECONDS, TimeUnit.SECONDS).getState());
    assertEquals(State.DONE, f1.get(WAIT_SECONDS, TimeUnit.SECONDS).getState());
    assertEquals(2, submittedJobs.get());
  }

  // ---------------------------------------------------------------------------------------------
  // Merged job succeeds
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testConcurrentPipelinesMergedIntoSingleBatchJobWithScopedMetrics() throws Exception {
    Member m0 = new Member("App0").withStep("StepA", 1, 2);
    Member m1 = new Member("App1").withStep("StepB", 1, 2);

    AtomicInteger submittedJobs = new AtomicInteger(0);
    DataflowRunner delegate = Mockito.mock(DataflowRunner.class);
    when(delegate.run(any(Pipeline.class)))
        .thenAnswer(
            invocation -> {
              submittedJobs.incrementAndGet();
              RunnerApi.Pipeline proto = PipelineTranslation.toProto(invocation.getArgument(0));
              List<MetricUpdate> updates = new ArrayList<>();
              updates.addAll(
                  createCounterUpdates(
                      transformIdOf(proto, "StepA"), "CustomNs", "myCounter", 10L));
              updates.add(passertUpdate(proto, "StepA", PAssert.SUCCESS_COUNTER));
              updates.addAll(
                  createCounterUpdates(
                      transformIdOf(proto, "StepB"), "CustomNs", "myCounter", 20L));
              updates.add(passertUpdate(proto, "StepB", PAssert.SUCCESS_COUNTER));
              metricsByJobId.put("batch-job-1", new JobMetrics().setMetrics(updates));
              return newBatchJob("batch-job-1", State.DONE, proto);
            });
    // The spy's DataflowMetrics was constructed around the un-spied job, so metric queries reach
    // the real getState(), which polls the client.
    when(mockClient.getJob(anyString())).thenReturn(new Job().setCurrentState(STAGE_DONE));

    Future<DataflowPipelineJob> f0 = m0.submit(delegate);
    Future<DataflowPipelineJob> f1 = m1.submit(delegate);
    DataflowPipelineJob result0 = f0.get(WAIT_SECONDS, TimeUnit.SECONDS);
    DataflowPipelineJob result1 = f1.get(WAIT_SECONDS, TimeUnit.SECONDS);

    assertEquals(1, submittedJobs.get());
    assertThat(result0, instanceOf(ScopedDataflowPipelineJob.class));
    assertThat(result1, instanceOf(ScopedDataflowPipelineJob.class));
    assertEquals(State.DONE, result0.getState());
    assertEquals(State.DONE, result1.waitUntilFinish());

    assertSingleCounter(result0, "StepA", 10L);
    assertSingleCounter(result1, "StepB", 20L);
    // Each member's pipeline is restored to its unprefixed state after the merged run.
    assertNull(m0.pipeline.getRootNamePrefix());
    assertNull(m1.pipeline.getRootNamePrefix());
  }

  @Test
  public void testPartialFailureInBatchOnlyRerunsAndFailsTheFailingTest() throws Exception {
    configureBatching(3, 5000L);
    Member good0 = new Member("GoodTest0").withStep("GoodStep0", 1, 2);
    Member failing1 = new Member("FailingTest1").withStep("FailingStep1", 99);
    Member good2 = new Member("GoodTest2").withStep("GoodStep2", 1, 2);

    DataflowRunner realTransformReplacer = DataflowRunner.fromOptions(options);
    AtomicInteger submittedJobs = new AtomicInteger(0);
    DataflowRunner delegate = Mockito.mock(DataflowRunner.class);
    when(delegate.run(any(Pipeline.class)))
        .thenAnswer(
            invocation -> {
              int callNum = submittedJobs.incrementAndGet();
              Pipeline submitted = invocation.getArgument(0);
              // Apply real Dataflow V1 transform overrides to verify snapshot restoration works
              // even after composite pipeline graph surgery.
              synchronized (realTransformReplacer) {
                realTransformReplacer.replaceV1Transforms(submitted);
              }
              RunnerApi.Pipeline proto = PipelineTranslation.toProto(submitted);

              if (submitted instanceof CompositeBatchPipeline) {
                metricsByJobId.put(
                    "batch-job",
                    new JobMetrics()
                        .setMetrics(
                            ImmutableList.of(
                                passertUpdate(proto, "GoodStep0", PAssert.SUCCESS_COUNTER),
                                passertUpdate(proto, "FailingStep1", PAssert.FAILURE_COUNTER),
                                passertUpdate(proto, "GoodStep2", PAssert.SUCCESS_COUNTER))));
                return newBatchJob("batch-job", State.DONE, proto);
              }
              // Standalone fallback for FailingTest1: verify prefix was restored to null.
              assertNull(submitted.getRootNamePrefix());
              assertTrue(hasTransform(proto, "FailingStep1"));
              String jobId = "standalone-failing-" + callNum;
              metricsByJobId.put(
                  jobId,
                  new JobMetrics()
                      .setMetrics(
                          ImmutableList.of(
                              createTentativePAssertUpdate("s1", PAssert.FAILURE_COUNTER, 1))));
              return newStandaloneJob(jobId, State.DONE);
            });

    Future<DataflowPipelineJob> f0 = good0.submit(delegate);
    Future<DataflowPipelineJob> f1 = failing1.submit(delegate);
    Future<DataflowPipelineJob> f2 = good2.submit(delegate);

    // GoodTest0 and GoodTest2 are credited directly from the batch job.
    assertThat(f0.get(WAIT_SECONDS, TimeUnit.SECONDS), instanceOf(ScopedDataflowPipelineJob.class));
    assertThat(f2.get(WAIT_SECONDS, TimeUnit.SECONDS), instanceOf(ScopedDataflowPipelineJob.class));

    // FailingTest1 falls back to standalone and throws AssertionError.
    assertThat(causeOf(f1), instanceOf(AssertionError.class));

    // 1 merged batch job + 1 standalone fallback job (only for FailingTest1) = 2 total jobs.
    assertEquals(2, submittedJobs.get());
  }

  // ---------------------------------------------------------------------------------------------
  // Merged job fails
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testFailedBatchJobWithoutStageStatesRerunsAllAndOnlyFailingTestFails()
      throws Exception {
    Member good0 = new Member("GoodTest0").withStep("GoodStep0", 1, 2);
    Member failing1 = new Member("FailingTest1").withStep("FailingStep1", 99);

    // executionDetails stays null: the service returned no per-stage information.
    AtomicInteger submittedJobs = new AtomicInteger(0);
    DataflowRunner delegate = Mockito.mock(DataflowRunner.class);
    when(delegate.run(any(Pipeline.class)))
        .thenAnswer(
            invocation -> {
              int callNum = submittedJobs.incrementAndGet();
              Pipeline submitted = invocation.getArgument(0);
              RunnerApi.Pipeline proto = PipelineTranslation.toProto(submitted);
              if (submitted instanceof CompositeBatchPipeline) {
                return newBatchJob("failed-batch-job", State.FAILED, proto);
              }
              assertNull(submitted.getRootNamePrefix());
              boolean isFailing = hasTransform(proto, "FailingStep1");
              String jobId = (isFailing ? "standalone-fail-" : "standalone-pass-") + callNum;
              metricsByJobId.put(
                  jobId,
                  new JobMetrics()
                      .setMetrics(
                          ImmutableList.of(
                              createTentativePAssertUpdate(
                                  "s1",
                                  isFailing ? PAssert.FAILURE_COUNTER : PAssert.SUCCESS_COUNTER,
                                  1))));
              return newStandaloneJob(jobId, isFailing ? State.FAILED : State.DONE);
            });

    Future<DataflowPipelineJob> f0 = good0.submit(delegate);
    Future<DataflowPipelineJob> f1 = failing1.submit(delegate);

    DataflowPipelineJob result0 = f0.get(WAIT_SECONDS, TimeUnit.SECONDS);
    assertThat(result0, not(instanceOf(ScopedDataflowPipelineJob.class)));
    assertEquals(State.DONE, result0.getState());
    assertThat(causeOf(f1), instanceOf(AssertionError.class));
    // 1 failed batch job + 2 standalone fallback jobs = 3 total jobs.
    assertEquals(3, submittedJobs.get());
  }

  @Test
  public void testFailedBatchJobCreditsMembersWhoseStagesAllCompleted() throws Exception {
    configureBatching(3, 5000L);
    Member good0 = new Member("GoodTest0").withStep("GoodStep0", 1, 2);
    Member failing1 = new Member("FailingTest1").withStep("FailingStep1", 99);
    Member pending2 = new Member("PendingTest2").withStep("PendingStep2", 1, 2);

    AtomicInteger submittedJobs = new AtomicInteger(0);
    DataflowRunner delegate = Mockito.mock(DataflowRunner.class);
    when(delegate.run(any(Pipeline.class)))
        .thenAnswer(
            invocation -> {
              int callNum = submittedJobs.incrementAndGet();
              Pipeline submitted = invocation.getArgument(0);
              RunnerApi.Pipeline proto = PipelineTranslation.toProto(submitted);
              if (submitted instanceof CompositeBatchPipeline) {
                String s0 = scopeOf(proto, "GoodStep0");
                String s1 = scopeOf(proto, "FailingStep1");
                String s2 = scopeOf(proto, "PendingStep2");
                // The service reports per-stage states; only the failing member's stage failed.
                // PendingTest2 has a success counter but one of its stages never reached DONE, so
                // it
                // must not be credited either.
                executionDetails.set(
                    executionDetails(
                        stage("F1", STAGE_DONE, s0 + "/Create-GoodStep0/Read", s0 + "/GoodStep0"),
                        stage("F2", STAGE_FAILED, s1 + "/FailingStep1"),
                        stage("F3", STAGE_DONE, s2 + "/PendingStep2"),
                        stage("F4", STAGE_PENDING, s2 + "/Create-PendingStep2/Read")));
                metricsByJobId.put(
                    "failed-batch-job",
                    new JobMetrics()
                        .setMetrics(
                            ImmutableList.of(
                                passertUpdate(proto, "GoodStep0", PAssert.SUCCESS_COUNTER),
                                passertUpdate(proto, "PendingStep2", PAssert.SUCCESS_COUNTER))));
                return newBatchJob("failed-batch-job", State.FAILED, proto);
              }
              assertNull(submitted.getRootNamePrefix());
              assertFalse("GoodTest0 must not be re-run", hasTransform(proto, "GoodStep0"));
              boolean isFailing = hasTransform(proto, "FailingStep1");
              String jobId = "standalone-" + callNum;
              metricsByJobId.put(
                  jobId,
                  new JobMetrics()
                      .setMetrics(
                          ImmutableList.of(
                              createTentativePAssertUpdate(
                                  "s1",
                                  isFailing ? PAssert.FAILURE_COUNTER : PAssert.SUCCESS_COUNTER,
                                  1))));
              return newStandaloneJob(jobId, isFailing ? State.FAILED : State.DONE);
            });

    Future<DataflowPipelineJob> f0 = good0.submit(delegate);
    Future<DataflowPipelineJob> f1 = failing1.submit(delegate);
    Future<DataflowPipelineJob> f2 = pending2.submit(delegate);

    DataflowPipelineJob result0 = f0.get(WAIT_SECONDS, TimeUnit.SECONDS);
    assertThat(result0, instanceOf(ScopedDataflowPipelineJob.class));
    assertEquals(State.DONE, result0.getState());
    assertEquals(State.DONE, result0.waitUntilFinish());

    assertThat(causeOf(f1), instanceOf(AssertionError.class));

    DataflowPipelineJob result2 = f2.get(WAIT_SECONDS, TimeUnit.SECONDS);
    assertThat(result2, not(instanceOf(ScopedDataflowPipelineJob.class)));
    assertEquals(State.DONE, result2.getState());

    // 1 failed batch job + standalone re-runs for FailingTest1 and PendingTest2 = 3 jobs.
    assertEquals(3, submittedJobs.get());
  }

  @Test
  public void testFailedBatchJobWithUnattributableFailedStageCreditsNobody() throws Exception {
    Member m0 = new Member("Test0").withStep("Step0", 1, 2);
    Member m1 = new Member("Test1").withStep("Step1", 1, 2);

    AtomicInteger submittedJobs = new AtomicInteger(0);
    DataflowRunner delegate = Mockito.mock(DataflowRunner.class);
    when(delegate.run(any(Pipeline.class)))
        .thenAnswer(
            invocation -> {
              int callNum = submittedJobs.incrementAndGet();
              Pipeline submitted = invocation.getArgument(0);
              RunnerApi.Pipeline proto = PipelineTranslation.toProto(submitted);
              if (submitted instanceof CompositeBatchPipeline) {
                // Both members' own stages are DONE and both have success counters, but a stage
                // that names no user steps failed. It cannot be attributed, so it counts against
                // everyone.
                executionDetails.set(
                    executionDetails(
                        stage("F1", STAGE_DONE, scopeOf(proto, "Step0") + "/Step0"),
                        stage("F2", STAGE_DONE, scopeOf(proto, "Step1") + "/Step1"),
                        stage("F99", STAGE_FAILED)));
                metricsByJobId.put(
                    "failed-batch-job",
                    new JobMetrics()
                        .setMetrics(
                            ImmutableList.of(
                                passertUpdate(proto, "Step0", PAssert.SUCCESS_COUNTER),
                                passertUpdate(proto, "Step1", PAssert.SUCCESS_COUNTER))));
                return newBatchJob("failed-batch-job", State.FAILED, proto);
              }
              String jobId = "standalone-" + callNum;
              metricsByJobId.put(
                  jobId,
                  new JobMetrics()
                      .setMetrics(
                          ImmutableList.of(
                              createTentativePAssertUpdate("s1", PAssert.SUCCESS_COUNTER, 1))));
              return newStandaloneJob(jobId, State.DONE);
            });

    Future<DataflowPipelineJob> f0 = m0.submit(delegate);
    Future<DataflowPipelineJob> f1 = m1.submit(delegate);
    assertThat(
        f0.get(WAIT_SECONDS, TimeUnit.SECONDS), not(instanceOf(ScopedDataflowPipelineJob.class)));
    assertThat(
        f1.get(WAIT_SECONDS, TimeUnit.SECONDS), not(instanceOf(ScopedDataflowPipelineJob.class)));
    assertEquals(3, submittedJobs.get());
  }

  @Test
  public void testStageAttributionMatching() {
    List<String> scopes = ImmutableList.of("t0", "t1", "t10");
    StageAttribution attribution =
        StageAttribution.fromJob(
            executionDetails(
                stage("F1", STAGE_DONE, "t0/A"),
                stage("F2", STAGE_FAILED, "t1/B"),
                // Runner V2 reports mangled names using '-' instead of '/'.
                stage("F3", STAGE_DONE, "t0-A-out0"),
                // "t10/..." must not be mistaken for scope "t1".
                stage("F4", STAGE_FAILED, "t10/C")));
    assertNotNull(attribution);
    assertTrue(attribution.statusForScope("t0", scopes).isComplete());
    assertFalse(attribution.statusForScope("t1", scopes).isComplete());
    assertFalse(attribution.statusForScope("t10", scopes).isComplete());
    assertThat(attribution.statusForScope("t0", scopes).toString(), is("2 stage(s) done"));
    assertThat(attribution.statusForScope("t1", scopes).toString(), containsString("F2"));
    assertThat(attribution.statusForScope("t1", scopes).toString(), not(containsString("F4")));

    // A scope that owns no stages at all is not complete.
    assertFalse(attribution.statusForScope("t7", scopes).isComplete());

    // A failed stage whose names fall under no known scope counts against every scope.
    StageAttribution withUnknown =
        StageAttribution.fromJob(
            executionDetails(
                stage("F1", STAGE_DONE, "t0/A"), stage("F5", STAGE_FAILED, "Mystery/X")));
    assertNotNull(withUnknown);
    assertFalse(withUnknown.statusForScope("t0", scopes).isComplete());

    // A stage with no state entry at all is treated as not done.
    StageAttribution missingState =
        new StageAttributionBuilder()
            .describe("F1", "t0/A")
            .describe("F2", "t0/B")
            .state("F1", STAGE_DONE)
            .build();
    assertNotNull(missingState);
    assertFalse(missingState.statusForScope("t0", scopes).isComplete());
    assertThat(missingState.statusForScope("t0", scopes).toString(), containsString("F2=UNKNOWN"));
  }

  @Test
  public void testStageAttributionRequiresBothStatesAndDescriptions() {
    assertNull(StageAttribution.fromJob(null));
    assertNull(StageAttribution.fromJob(new Job()));
    assertNull(
        StageAttribution.fromJob(
            new Job()
                .setStageStates(
                    ImmutableList.of(
                        new ExecutionStageState()
                            .setExecutionStageName("F1")
                            .setExecutionStageState(STAGE_DONE)))));
    assertNull(
        StageAttribution.fromJob(
            new Job()
                .setPipelineDescription(
                    new PipelineDescription()
                        .setExecutionPipelineStage(
                            ImmutableList.of(new ExecutionStageSummary().setName("F1"))))));
    assertNotNull(StageAttribution.fromJob(executionDetails(stage("F1", STAGE_DONE, "t0/A"))));
  }

  // ---------------------------------------------------------------------------------------------
  // Submission failures, interruption and unexpected errors
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testFailedSubmissionRerunsEachMemberOnItsOwnThread() throws Exception {
    Member m0 = new Member("App0").withStep("Step0", 1, 2);
    Member m1 = new Member("App1").withStep("Step1", 1, 2);

    AtomicInteger submittedJobs = new AtomicInteger(0);
    Set<Thread> standaloneThreads = ConcurrentHashMap.newKeySet();
    DataflowRunner delegate = Mockito.mock(DataflowRunner.class);
    when(delegate.run(any(Pipeline.class)))
        .thenAnswer(
            invocation -> {
              int callNum = submittedJobs.incrementAndGet();
              Pipeline submitted = invocation.getArgument(0);
              if (submitted instanceof CompositeBatchPipeline) {
                throw new RuntimeException("Simulated batch submission failure");
              }
              // Verify snapshot was restored so standalone pipeline has null rootNamePrefix.
              assertNull(submitted.getRootNamePrefix());
              standaloneThreads.add(Thread.currentThread());
              String jobId = "standalone-job-" + callNum;
              metricsByJobId.put(
                  jobId,
                  new JobMetrics()
                      .setMetrics(
                          ImmutableList.of(
                              createTentativePAssertUpdate("s1", PAssert.SUCCESS_COUNTER, 1))));
              return newStandaloneJob(jobId, State.DONE);
            });

    Set<Thread> testThreads = ConcurrentHashMap.newKeySet();
    Future<DataflowPipelineJob> f0 =
        pool.submit(
            () -> {
              testThreads.add(Thread.currentThread());
              return m0.runner.run(m0.pipeline, delegate);
            });
    Future<DataflowPipelineJob> f1 =
        pool.submit(
            () -> {
              testThreads.add(Thread.currentThread());
              return m1.runner.run(m1.pipeline, delegate);
            });

    assertEquals(State.DONE, f0.get(WAIT_SECONDS, TimeUnit.SECONDS).getState());
    assertEquals(State.DONE, f1.get(WAIT_SECONDS, TimeUnit.SECONDS).getState());
    // 1 batch attempt + 2 standalone fallback executions = 3 total calls.
    assertEquals(3, submittedJobs.get());
    // The fallbacks ran on the members' own test threads, not on a coordinator thread.
    assertEquals(testThreads, standaloneThreads);
  }

  @Test
  public void testInterruptedMemberDoesNotAffectOtherMembers() throws Exception {
    Member m0 = new Member("App0").withStep("Step0", 1, 2);
    Member m1 = new Member("App1").withStep("Step1", 1, 2);

    CountDownLatch compositeEntered = new CountDownLatch(1);
    CountDownLatch releaseComposite = new CountDownLatch(1);
    AtomicInteger submittedJobs = new AtomicInteger(0);
    DataflowRunner delegate = Mockito.mock(DataflowRunner.class);
    when(delegate.run(any(Pipeline.class)))
        .thenAnswer(
            invocation -> {
              submittedJobs.incrementAndGet();
              Pipeline submitted = invocation.getArgument(0);
              assertThat(submitted, instanceOf(CompositeBatchPipeline.class));
              compositeEntered.countDown();
              assertTrue(releaseComposite.await(WAIT_SECONDS, TimeUnit.SECONDS));
              RunnerApi.Pipeline proto = PipelineTranslation.toProto(submitted);
              metricsByJobId.put(
                  "batch-job",
                  new JobMetrics()
                      .setMetrics(
                          ImmutableList.of(
                              passertUpdate(proto, "Step0", PAssert.SUCCESS_COUNTER),
                              passertUpdate(proto, "Step1", PAssert.SUCCESS_COUNTER))));
              return newBatchJob("batch-job", State.DONE, proto);
            });

    AtomicReference<Throwable> failure0 = new AtomicReference<>();
    Future<DataflowPipelineJob> f0 =
        pool.submit(
            () -> {
              try {
                return m0.runner.run(m0.pipeline, delegate);
              } catch (RuntimeException e) {
                failure0.set(e);
                throw e;
              }
            });
    Future<DataflowPipelineJob> f1 = m1.submit(delegate);

    assertTrue(compositeEntered.await(WAIT_SECONDS, TimeUnit.SECONDS));
    // Member 0's test is interrupted (e.g. by a JUnit timeout) while the merged job is running.
    f0.cancel(true);
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
    while (failure0.get() == null && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertNotNull("member 0 should have observed the interrupt", failure0.get());
    assertThat(failure0.get().getCause(), instanceOf(InterruptedException.class));

    releaseComposite.countDown();
    DataflowPipelineJob result1 = f1.get(WAIT_SECONDS, TimeUnit.SECONDS);
    assertThat(result1, instanceOf(ScopedDataflowPipelineJob.class));
    assertEquals(1, submittedJobs.get());
  }

  @Test
  public void testUnexpectedCoordinatorFailureFailsAllMembersPromptly() throws Exception {
    Member m0 = new Member("App0").withStep("Step0", 1, 2);
    Member m1 = new Member("App1").withStep("Step1", 1, 2);

    // Not an IOException, so it escapes TestDataflowRunner.getJobMetrics and surfaces as an
    // unexpected failure inside the batch runner after the merged job has completed.
    when(mockClient.getJobMetrics(anyString()))
        .thenThrow(new IllegalStateException("metrics backend exploded"));

    AtomicInteger submittedJobs = new AtomicInteger(0);
    DataflowRunner delegate = Mockito.mock(DataflowRunner.class);
    when(delegate.run(any(Pipeline.class)))
        .thenAnswer(
            invocation -> {
              submittedJobs.incrementAndGet();
              Pipeline submitted = invocation.getArgument(0);
              assertThat(submitted, instanceOf(CompositeBatchPipeline.class));
              return newBatchJob("batch-job", State.DONE, PipelineTranslation.toProto(submitted));
            });

    Future<DataflowPipelineJob> f0 = m0.submit(delegate);
    Future<DataflowPipelineJob> f1 = m1.submit(delegate);
    for (Future<DataflowPipelineJob> f : Arrays.asList(f0, f1)) {
      Throwable cause = causeOf(f);
      assertThat(cause, instanceOf(IllegalStateException.class));
      assertThat(cause.getMessage(), containsString("metrics backend exploded"));
    }
    // Nobody fell back to a standalone run: the failure was in the test harness, not the job.
    assertEquals(1, submittedJobs.get());
  }

  // ---------------------------------------------------------------------------------------------
  // Scheduling and concurrency
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testStragglerIsNotBlockedByInFlightBatch() throws Exception {
    // A and B are submitted back-to-back and close a batch well within the window; the straggler
    // then waits out one full window alone, so keep it short.
    configureBatching(2, 1000L);
    Member a = new Member("A").withStep("StepA", 1, 2);
    Member b = new Member("B").withStep("StepB", 1, 2);
    Member straggler = new Member("Straggler").withStep("StepC", 1, 2);

    CountDownLatch compositeEntered = new CountDownLatch(1);
    CountDownLatch releaseComposite = new CountDownLatch(1);
    AtomicBoolean stragglerRanWhileBatchInFlight = new AtomicBoolean(false);
    AtomicInteger submittedJobs = new AtomicInteger(0);
    DataflowRunner delegate = Mockito.mock(DataflowRunner.class);
    when(delegate.run(any(Pipeline.class)))
        .thenAnswer(
            invocation -> {
              int callNum = submittedJobs.incrementAndGet();
              Pipeline submitted = invocation.getArgument(0);
              RunnerApi.Pipeline proto = PipelineTranslation.toProto(submitted);
              if (submitted instanceof CompositeBatchPipeline) {
                compositeEntered.countDown();
                assertTrue(releaseComposite.await(WAIT_SECONDS, TimeUnit.SECONDS));
                metricsByJobId.put(
                    "batch-job",
                    new JobMetrics()
                        .setMetrics(
                            ImmutableList.of(
                                passertUpdate(proto, "StepA", PAssert.SUCCESS_COUNTER),
                                passertUpdate(proto, "StepB", PAssert.SUCCESS_COUNTER))));
                return newBatchJob("batch-job", State.DONE, proto);
              }
              assertTrue(hasTransform(proto, "StepC"));
              stragglerRanWhileBatchInFlight.set(releaseComposite.getCount() == 1);
              String jobId = "standalone-" + callNum;
              metricsByJobId.put(
                  jobId,
                  new JobMetrics()
                      .setMetrics(
                          ImmutableList.of(
                              createTentativePAssertUpdate("s1", PAssert.SUCCESS_COUNTER, 1))));
              return newStandaloneJob(jobId, State.DONE);
            });

    Future<DataflowPipelineJob> fa = a.submit(delegate);
    Future<DataflowPipelineJob> fb = b.submit(delegate);
    assertTrue(compositeEntered.await(WAIT_SECONDS, TimeUnit.SECONDS));

    // While the {A, B} job is in flight, a third test arrives. Its batch window closes with no
    // partner, so it must run standalone right away rather than wait for the in-flight job.
    Future<DataflowPipelineJob> fc = straggler.submit(delegate);
    DataflowPipelineJob resultC = fc.get(WAIT_SECONDS, TimeUnit.SECONDS);
    assertThat(resultC, not(instanceOf(ScopedDataflowPipelineJob.class)));
    assertTrue(stragglerRanWhileBatchInFlight.get());

    releaseComposite.countDown();
    assertThat(fa.get(WAIT_SECONDS, TimeUnit.SECONDS), instanceOf(ScopedDataflowPipelineJob.class));
    assertThat(fb.get(WAIT_SECONDS, TimeUnit.SECONDS), instanceOf(ScopedDataflowPipelineJob.class));
    assertEquals(2, submittedJobs.get());
  }

  @Test
  public void testFallbackRerunsBypassStandaloneConcurrencyLimit() throws Exception {
    String previous =
        System.getProperty(TestDataflowRunner.MAX_CONCURRENT_STANDALONE_JOBS_PROPERTY);
    System.setProperty(TestDataflowRunner.MAX_CONCURRENT_STANDALONE_JOBS_PROPERTY, "1");
    try {
      Member m0 = new Member("App0").withStep("Step0", 1, 2);
      Member m1 = new Member("App1").withStep("Step1", 1, 2);

      // Both fallback runs must be inside the delegate at the same time for this latch to open.
      // If re-runs were subject to the (size 1) standalone limiter, the first would hold the only
      // permit while waiting here and the second could never enter.
      CountDownLatch bothRerunsRunning = new CountDownLatch(2);
      AtomicInteger submittedJobs = new AtomicInteger(0);
      DataflowRunner delegate = Mockito.mock(DataflowRunner.class);
      when(delegate.run(any(Pipeline.class)))
          .thenAnswer(
              invocation -> {
                int callNum = submittedJobs.incrementAndGet();
                Pipeline submitted = invocation.getArgument(0);
                if (submitted instanceof CompositeBatchPipeline) {
                  throw new RuntimeException("Simulated batch submission failure");
                }
                bothRerunsRunning.countDown();
                if (!bothRerunsRunning.await(10, TimeUnit.SECONDS)) {
                  throw new AssertionError("Fallback re-runs were serialized by the limiter");
                }
                String jobId = "standalone-" + callNum;
                metricsByJobId.put(
                    jobId,
                    new JobMetrics()
                        .setMetrics(
                            ImmutableList.of(
                                createTentativePAssertUpdate("s1", PAssert.SUCCESS_COUNTER, 1))));
                return newStandaloneJob(jobId, State.DONE);
              });

      Future<DataflowPipelineJob> f0 = m0.submit(delegate);
      Future<DataflowPipelineJob> f1 = m1.submit(delegate);
      assertEquals(State.DONE, f0.get(WAIT_SECONDS, TimeUnit.SECONDS).getState());
      assertEquals(State.DONE, f1.get(WAIT_SECONDS, TimeUnit.SECONDS).getState());
      assertEquals(3, submittedJobs.get());
    } finally {
      if (previous == null) {
        System.clearProperty(TestDataflowRunner.MAX_CONCURRENT_STANDALONE_JOBS_PROPERTY);
      } else {
        System.setProperty(TestDataflowRunner.MAX_CONCURRENT_STANDALONE_JOBS_PROPERTY, previous);
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  /** Returns the cause of the {@link ExecutionException} thrown by {@code future}. */
  private static Throwable causeOf(Future<?> future) {
    try {
      future.get(WAIT_SECONDS, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      return e.getCause();
    } catch (Exception e) {
      throw new AssertionError("Unexpected exception", e);
    }
    fail("Expected the future to fail");
    return null;
  }

  private DataflowPipelineJob newBatchJob(String jobId, State state, RunnerApi.Pipeline proto)
      throws Exception {
    DataflowPipelineJob job =
        Mockito.spy(
            new DataflowPipelineJob(mockClient, jobId, options, Collections.emptyMap(), proto));
    Mockito.doReturn(state).when(job).getState();
    Mockito.doReturn(state).when(job).waitUntilFinish(any(), any());
    return job;
  }

  private static DataflowPipelineJob newStandaloneJob(String jobId, State state) throws Exception {
    DataflowPipelineJob job = Mockito.mock(DataflowPipelineJob.class);
    when(job.getJobId()).thenReturn(jobId);
    when(job.getState()).thenReturn(state);
    when(job.waitUntilFinish(any(), any())).thenReturn(state);
    return job;
  }

  /** Returns the id of the transform whose unique name is {@code stepName} or ends in it. */
  private static String transformIdOf(RunnerApi.Pipeline proto, String stepName) {
    for (Map.Entry<String, RunnerApi.PTransform> entry :
        proto.getComponents().getTransformsMap().entrySet()) {
      String uniqueName = entry.getValue().getUniqueName();
      if (uniqueName.equals(stepName) || uniqueName.endsWith("/" + stepName)) {
        return entry.getKey();
      }
    }
    throw new AssertionError("No transform named " + stepName + " in pipeline");
  }

  private static boolean hasTransform(RunnerApi.Pipeline proto, String stepName) {
    for (RunnerApi.PTransform t : proto.getComponents().getTransformsMap().values()) {
      if (t.getUniqueName().equals(stepName) || t.getUniqueName().endsWith("/" + stepName)) {
        return true;
      }
    }
    return false;
  }

  /** Returns the batch scope prefix (e.g. {@code t1}) the member owning {@code stepName} got. */
  private static String scopeOf(RunnerApi.Pipeline proto, String stepName) {
    String uniqueName =
        proto
            .getComponents()
            .getTransformsMap()
            .get(transformIdOf(proto, stepName))
            .getUniqueName();
    int slash = uniqueName.indexOf('/');
    assertTrue("Expected a scoped name but got " + uniqueName, slash > 0);
    return uniqueName.substring(0, slash);
  }

  private static MetricUpdate passertUpdate(
      RunnerApi.Pipeline proto, String stepName, String counterName) {
    return createTentativePAssertUpdate(transformIdOf(proto, stepName), counterName, 1);
  }

  private static void assertSingleCounter(DataflowPipelineJob job, String step, long expected) {
    MetricQueryResults metrics =
        job.metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.named("CustomNs", "myCounter"))
                    .build());
    List<MetricResult<Long>> counters = ImmutableList.copyOf(metrics.getCounters());
    assertEquals(1, counters.size());
    assertEquals(
        MetricKey.create(step, MetricName.named("CustomNs", "myCounter")),
        counters.get(0).getKey());
    assertThat(counters.get(0).getCommitted(), is(expected));
  }

  private static final class StageInfo {
    final String name;
    final String state;
    final List<String> userNames;

    StageInfo(String name, String state, List<String> userNames) {
      this.name = name;
      this.state = state;
      this.userNames = userNames;
    }
  }

  private static StageInfo stage(String name, String state, String... userNames) {
    return new StageInfo(name, state, Arrays.asList(userNames));
  }

  /** Builds a {@code JOB_VIEW_ALL} job carrying stage states and stage descriptions. */
  private static Job executionDetails(StageInfo... stages) {
    StageAttributionBuilder builder = new StageAttributionBuilder();
    for (StageInfo s : stages) {
      builder.state(s.name, s.state);
      builder.describe(s.name, s.userNames.toArray(new String[0]));
    }
    return builder.job();
  }

  private static final class StageAttributionBuilder {
    private final List<ExecutionStageState> states = new ArrayList<>();
    private final List<ExecutionStageSummary> summaries = new ArrayList<>();

    StageAttributionBuilder state(String stageName, String state) {
      states.add(
          new ExecutionStageState().setExecutionStageName(stageName).setExecutionStageState(state));
      return this;
    }

    StageAttributionBuilder describe(String stageName, String... userNames) {
      List<ComponentTransform> transforms = new ArrayList<>();
      for (String userName : userNames) {
        transforms.add(new ComponentTransform().setUserName(userName));
      }
      summaries.add(
          new ExecutionStageSummary().setName(stageName).setComponentTransform(transforms));
      return this;
    }

    Job job() {
      return new Job()
          .setStageStates(states)
          .setPipelineDescription(new PipelineDescription().setExecutionPipelineStage(summaries));
    }

    StageAttribution build() {
      return StageAttribution.fromJob(job());
    }
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
