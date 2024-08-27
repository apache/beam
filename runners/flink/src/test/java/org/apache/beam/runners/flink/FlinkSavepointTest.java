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

import static org.hamcrest.MatcherAssert.assertThat;

import java.io.Serializable;
import java.net.URI;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.jobsubmission.JobInvocation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.InferableFunction;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.MoreExecutors;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.hamcrest.Matchers;
import org.hamcrest.core.IsIterableContaining;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests that Flink's Savepoints work with the Flink Runner. This includes taking a savepoint of a
 * running pipeline, shutting down the pipeline, and restarting the pipeline from the savepoint with
 * a different parallelism.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  // TODO(https://github.com/apache/beam/issues/21230): Remove when new version of
  // errorprone is released (2.11.0)
  "unused"
})
public class FlinkSavepointTest implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkSavepointTest.class);

  /** Flink cluster that runs over the lifespan of the tests. */
  private static transient MiniCluster flinkCluster;

  /** Static for synchronization between the pipeline state and the test. */
  private static volatile CountDownLatch oneShotLatch;

  /** Reusable executor for portable jobs. */
  private static ListeningExecutorService flinkJobExecutor;

  /** Temporary folder for savepoints. */
  @ClassRule public static transient TemporaryFolder tempFolder = new TemporaryFolder();

  /** Each test has a timeout of 60 seconds (for safety). */
  @Rule public Timeout timeout = new Timeout(2, TimeUnit.MINUTES);

  @BeforeClass
  public static void beforeClass() throws Exception {
    flinkJobExecutor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));

    Configuration config = new Configuration();
    // Avoid port collision in parallel tests
    config.setInteger(RestOptions.PORT, 0);
    config.setString(CheckpointingOptions.STATE_BACKEND, "filesystem");

    String savepointPath = "file://" + tempFolder.getRoot().getAbsolutePath();
    LOG.info("Savepoints will be written to {}", savepointPath);
    // It is necessary to configure the checkpoint directory for the state backend,
    // even though we only create savepoints in this test.
    config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, savepointPath);
    // Checkpoints will go into a subdirectory of this directory
    config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointPath);

    MiniClusterConfiguration clusterConfig =
        new MiniClusterConfiguration.Builder()
            .setConfiguration(config)
            .setNumTaskManagers(2)
            .setNumSlotsPerTaskManager(2)
            .build();

    flinkCluster = new MiniCluster(clusterConfig);
    flinkCluster.start();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    flinkCluster.close();
    flinkCluster = null;

    flinkJobExecutor.shutdown();
    flinkJobExecutor.awaitTermination(10, TimeUnit.SECONDS);
    if (!flinkJobExecutor.isShutdown()) {
      LOG.warn("Could not shutdown Flink job executor");
    }
    flinkJobExecutor = null;
  }

  @After
  public void afterTest() throws Exception {
    for (JobStatusMessage jobStatusMessage : flinkCluster.listJobs().get()) {
      if (jobStatusMessage.getJobState().name().equals("RUNNING")) {
        flinkCluster.cancelJob(jobStatusMessage.getJobId()).get();
      }
    }
    ensureNoJobRunning();
  }

  @Test
  public void testSavepointRestoreLegacy() throws Exception {
    runSavepointAndRestore(false);
  }

  @Test
  public void testSavepointRestorePortable() throws Exception {
    runSavepointAndRestore(true);
  }

  private void runSavepointAndRestore(boolean isPortablePipeline) throws Exception {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setStreaming(true);
    // Initial parallelism
    options.setParallelism(2);
    options.setRunner(FlinkRunner.class);
    // Avoid any task from shutting down which would prevent savepointing
    options.setShutdownSourcesAfterIdleMs(Long.MAX_VALUE);

    oneShotLatch = new CountDownLatch(1);
    options.setJobName("initial-" + UUID.randomUUID());
    Pipeline pipeline = Pipeline.create(options);
    createStreamingJob(pipeline, false, isPortablePipeline);

    final JobID jobID;
    if (isPortablePipeline) {
      jobID = executePortable(pipeline);
    } else {
      jobID = executeLegacy(pipeline);
    }
    oneShotLatch.await();
    String savepointDir = takeSavepoint(jobID);
    flinkCluster.cancelJob(jobID).get();
    ensureNoJobRunning();

    oneShotLatch = new CountDownLatch(1);
    // Increase parallelism
    options.setParallelism(4);
    options.setJobName("restored-" + UUID.randomUUID());
    pipeline = Pipeline.create(options);
    createStreamingJob(pipeline, true, isPortablePipeline);

    if (isPortablePipeline) {
      restoreFromSavepointPortable(pipeline, savepointDir);
    } else {
      restoreFromSavepointLegacy(pipeline, savepointDir);
    }
    oneShotLatch.await();
  }

  private JobID executeLegacy(Pipeline pipeline) throws Exception {
    JobGraph jobGraph = getJobGraph(pipeline);
    flinkCluster.submitJob(jobGraph).get();
    return waitForJobToBeReady(pipeline.getOptions().getJobName());
  }

  private JobID executePortable(Pipeline pipeline) throws Exception {
    pipeline
        .getOptions()
        .as(PortablePipelineOptions.class)
        .setDefaultEnvironmentType(Environments.ENVIRONMENT_EMBEDDED);
    pipeline.getOptions().as(FlinkPipelineOptions.class).setFlinkMaster(getFlinkMaster());

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline);

    FlinkPipelineOptions pipelineOptions = pipeline.getOptions().as(FlinkPipelineOptions.class);
    JobInvocation jobInvocation =
        FlinkJobInvoker.create(null)
            .createJobInvocation(
                "id",
                "none",
                flinkJobExecutor,
                pipelineProto,
                pipelineOptions,
                new FlinkPipelineRunner(pipelineOptions, null, Collections.emptyList()));

    jobInvocation.start();

    return waitForJobToBeReady(pipeline.getOptions().getJobName());
  }

  private String getFlinkMaster() throws Exception {
    URI uri = flinkCluster.getRestAddress().get();
    return uri.getHost() + ":" + uri.getPort();
  }

  private void ensureNoJobRunning() throws Exception {
    while (!flinkCluster.listJobs().get().stream()
        .allMatch(job -> job.getJobState().isTerminalState())) {
      Thread.sleep(50);
    }
  }

  private JobID waitForJobToBeReady(String jobName)
      throws InterruptedException, ExecutionException {
    while (true) {
      Optional<JobStatusMessage> jobId =
          flinkCluster.listJobs().get().stream()
              .filter((status) -> status.getJobName().equals(jobName))
              .findAny();
      if (jobId.isPresent()) {
        JobStatusMessage status = jobId.get();
        if (status.getJobState().equals(JobStatus.RUNNING)) {
          return status.getJobId();
        }
        LOG.info("Job '{}' is in state {}, waiting...", jobName, status.getJobState());
      } else {
        LOG.info("Job '{}' does not yet exist, waiting...", jobName);
      }
      Thread.sleep(100);
    }
  }

  private String takeSavepoint(JobID jobID) throws Exception {
    Exception exception = null;
    // try multiple times because the job might not be ready yet
    for (int i = 0; i < 10; i++) {
      try {
        CompletableFuture<String> savepointFuture =
            flinkCluster.triggerSavepoint(jobID, null, false, SavepointFormatType.DEFAULT);
        return savepointFuture.get();
      } catch (Exception e) {
        exception = e;
        LOG.debug("Exception while triggerSavepoint, trying again", e);
        Thread.sleep(100);
      }
    }
    throw exception;
  }

  private void restoreFromSavepointLegacy(Pipeline pipeline, String savepointDir)
      throws ExecutionException, InterruptedException {
    JobGraph jobGraph = getJobGraph(pipeline);
    SavepointRestoreSettings savepointSettings = SavepointRestoreSettings.forPath(savepointDir);
    jobGraph.setSavepointRestoreSettings(savepointSettings);
    flinkCluster.submitJob(jobGraph).get();
  }

  private void restoreFromSavepointPortable(Pipeline pipeline, String savepointDir)
      throws Exception {
    FlinkPipelineOptions flinkOptions = pipeline.getOptions().as(FlinkPipelineOptions.class);
    flinkOptions.setSavepointPath(savepointDir);
    executePortable(pipeline);
  }

  private JobGraph getJobGraph(Pipeline pipeline) {
    FlinkRunner flinkRunner = FlinkRunner.fromOptions(pipeline.getOptions());
    return flinkRunner.getJobGraph(pipeline);
  }

  private static PCollection createStreamingJob(
      Pipeline pipeline, boolean restored, boolean isPortablePipeline) {
    final PCollection<KV<String, Long>> key;
    if (isPortablePipeline) {
      key =
          pipeline
              .apply("ImpulseStage", Impulse.create())
              .apply(
                  "KvMapperStage",
                  MapElements.via(
                      new InferableFunction<byte[], KV<String, Void>>() {
                        @Override
                        public KV<String, Void> apply(byte[] input) {
                          // This only writes data to one of the two initial partitions.
                          // We want to test this due to
                          // https://jira.apache.org/jira/browse/BEAM-7144
                          return KV.of("key", null);
                        }
                      }))
              .apply(
                  "TimerStage",
                  ParDo.of(
                      new DoFn<KV<String, Void>, KV<String, Long>>() {

                        @StateId("nextInteger")
                        private final StateSpec<ValueState<Long>> valueStateSpec =
                            StateSpecs.value();

                        @TimerId("timer")
                        private final TimerSpec timer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

                        @ProcessElement
                        public void processElement(
                            ProcessContext context, @TimerId("timer") Timer timer) {
                          timer.set(new Instant(0));
                        }

                        @OnTimer("timer")
                        public void onTimer(
                            OnTimerContext context,
                            @StateId("nextInteger") ValueState<Long> nextInteger,
                            @TimerId("timer") Timer timer) {
                          Long current = nextInteger.read();
                          current = current != null ? current : 0L;
                          context.output(KV.of("key", current));
                          LOG.debug("triggering timer {}", current);
                          nextInteger.write(current + 1);
                          // Trigger timer again and continue to hold back the watermark
                          timer.withOutputTimestamp(new Instant(0)).set(context.fireTimestamp());
                        }
                      }));
    } else {
      key =
          pipeline
              .apply("IdGeneratorStage", GenerateSequence.from(0))
              .apply(
                  "KvMapperStage",
                  ParDo.of(
                      new DoFn<Long, KV<String, Long>>() {
                        @ProcessElement
                        public void processElement(ProcessContext context) {
                          context.output(KV.of("key", context.element()));
                        }
                      }));
    }
    if (restored) {
      return key.apply(
          "VerificationStage",
          ParDo.of(
              new DoFn<KV<String, Long>, String>() {

                @StateId("valueState")
                private final StateSpec<ValueState<Integer>> valueStateSpec = StateSpecs.value();

                @StateId("bagState")
                private final StateSpec<BagState<Integer>> bagStateSpec = StateSpecs.bag();

                @ProcessElement
                public void processElement(
                    ProcessContext context,
                    @StateId("valueState") ValueState<Integer> intValueState,
                    @StateId("bagState") BagState<Integer> intBagState) {
                  assertThat(intValueState.read(), Matchers.is(42));
                  assertThat(intBagState.read(), IsIterableContaining.hasItems(40, 1, 1));
                  oneShotLatch.countDown();
                }
              }));
    } else {
      return key.apply(
          "VerificationStage",
          ParDo.of(
              new DoFn<KV<String, Long>, String>() {

                @StateId("valueState")
                private final StateSpec<ValueState<Integer>> valueStateSpec = StateSpecs.value();

                @StateId("bagState")
                private final StateSpec<BagState<Integer>> bagStateSpec = StateSpecs.bag();

                @ProcessElement
                public void processElement(
                    ProcessContext context,
                    @StateId("valueState") ValueState<Integer> intValueState,
                    @StateId("bagState") BagState<Integer> intBagState) {
                  long value = Objects.requireNonNull(context.element().getValue());
                  LOG.debug("value: {} timestamp: {}", value, context.timestamp().getMillis());
                  if (value == 0L) {
                    intValueState.write(42);
                    intBagState.add(40);
                    intBagState.add(1);
                    intBagState.add(1);
                  } else if (value >= 1) {
                    oneShotLatch.countDown();
                  }
                }
              }));
    }
  }
}
