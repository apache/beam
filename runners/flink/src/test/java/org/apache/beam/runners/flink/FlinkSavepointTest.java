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

import static org.junit.Assert.assertNotNull;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.fnexecution.jobsubmission.JobInvocation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
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
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v20_0.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.beam.vendor.guava.v20_0.com.google.common.util.concurrent.MoreExecutors;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests that Flink's Savepoints work with the Flink Runner. */
public class FlinkSavepointTest implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkSavepointTest.class);

  /** Static for synchronization between the pipeline state and the test. */
  private static CountDownLatch oneShotLatch;

  @ClassRule public static transient TemporaryFolder tempFolder = new TemporaryFolder();

  private static transient MiniCluster flinkCluster;

  @BeforeClass
  public static void beforeClass() throws Exception {
    final int parallelism = 4;

    Configuration config = new Configuration();
    // Avoid port collision in parallel tests
    config.setInteger(RestOptions.PORT, 0);
    config.setString(CheckpointingOptions.STATE_BACKEND, "filesystem");
    // It is necessary to configure the checkpoint directory for the state backend,
    // even though we only create savepoints in this test.
    config.setString(
        CheckpointingOptions.CHECKPOINTS_DIRECTORY,
        "file://" + tempFolder.getRoot().getAbsolutePath());
    // Checkpoints will go into a subdirectory of this directory
    config.setString(
        CheckpointingOptions.SAVEPOINT_DIRECTORY,
        "file://" + tempFolder.getRoot().getAbsolutePath());

    MiniClusterConfiguration clusterConfig =
        new MiniClusterConfiguration.Builder()
            .setConfiguration(config)
            .setNumTaskManagers(2)
            .setNumSlotsPerTaskManager(2)
            .build();

    flinkCluster = new MiniCluster(clusterConfig);
    flinkCluster.start();

    TestStreamEnvironment.setAsContext(flinkCluster, parallelism);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TestStreamEnvironment.unsetAsContext();
    flinkCluster.close();
  }

  @After
  public void afterTest() throws Exception {
    for (JobStatusMessage jobStatusMessage : flinkCluster.listJobs().get()) {
      if (jobStatusMessage.getJobState() == JobStatus.RUNNING) {
        flinkCluster.cancelJob(jobStatusMessage.getJobId()).get();
      }
    }
  }

  @Test(timeout = 60_000)
  public void testSavepointRestoreLegacy() throws Exception {
    runSavepointAndRestore(false);
  }

  @Test(timeout = 60_000)
  public void testSavepointRestorePortable() throws Exception {
    runSavepointAndRestore(true);
  }

  private void runSavepointAndRestore(boolean isPortablePipeline) throws Exception {
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    options.setStreaming(true);
    // savepoint assumes local file system
    options.setParallelism(1);
    options.setRunner(FlinkRunner.class);

    oneShotLatch = new CountDownLatch(1);
    Pipeline pipeline = Pipeline.create(options);
    createStreamingJob(pipeline, false, isPortablePipeline);

    final JobID jobID;
    if (isPortablePipeline) {
      jobID = executePortable(pipeline);
    } else {
      jobID = executeLegacy(pipeline);
    }
    oneShotLatch.await();
    String savepointDir = takeSavepointAndCancelJob(jobID);

    oneShotLatch = new CountDownLatch(1);
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
    return jobGraph.getJobID();
  }

  private JobID executePortable(Pipeline pipeline) throws Exception {
    pipeline
        .getOptions()
        .as(PortablePipelineOptions.class)
        .setDefaultEnvironmentType(Environments.ENVIRONMENT_EMBEDDED);
    pipeline.getOptions().as(FlinkPipelineOptions.class).setFlinkMaster(getFlinkMaster());

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline);

    ListeningExecutorService executorService =
        MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    try {
      JobInvocation jobInvocation =
          FlinkJobInvoker.createJobInvocation(
              "id",
              "none",
              executorService,
              pipelineProto,
              pipeline.getOptions().as(FlinkPipelineOptions.class),
              null,
              Collections.emptyList());

      jobInvocation.start();

      return waitForJobToBeReady();
    } finally {
      executorService.shutdown();
    }
  }

  private String getFlinkMaster() throws Exception {
    final URI uri;
    Method getRestAddress = flinkCluster.getClass().getMethod("getRestAddress");
    if (getRestAddress.getReturnType().equals(URI.class)) {
      // Flink 1.5 way
      uri = (URI) getRestAddress.invoke(flinkCluster);
    } else if (getRestAddress.getReturnType().equals(CompletableFuture.class)) {
      @SuppressWarnings("unchecked")
      CompletableFuture<URI> future = (CompletableFuture<URI>) getRestAddress.invoke(flinkCluster);
      uri = future.get();
    } else {
      throw new RuntimeException("Could not determine Rest address for this Flink version.");
    }
    return uri.getHost() + ":" + uri.getPort();
  }

  private JobID waitForJobToBeReady() throws InterruptedException, ExecutionException {
    while (true) {
      JobStatusMessage jobStatus = Iterables.getFirst(flinkCluster.listJobs().get(), null);
      if (jobStatus != null && jobStatus.getJobState() == JobStatus.RUNNING) {
        return jobStatus.getJobId();
      }
      Thread.sleep(100);
    }
  }

  private String takeSavepointAndCancelJob(JobID jobID) throws Exception {
    Exception exception = null;
    // try multiple times because the job might not be ready yet
    for (int i = 0; i < 10; i++) {
      try {
        return flinkCluster.triggerSavepoint(jobID, null, true).get();
      } catch (Exception e) {
        exception = e;
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
              .apply(Impulse.create())
              .apply(
                  MapElements.via(
                      new InferableFunction<byte[], KV<String, Void>>() {
                        @Override
                        public KV<String, Void> apply(byte[] input) throws Exception {
                          return KV.of("key", null);
                        }
                      }))
              .apply(
                  ParDo.of(
                      new DoFn<KV<String, Void>, KV<String, Long>>() {
                        @StateId("valueState")
                        private final StateSpec<ValueState<Long>> valueStateSpec =
                            StateSpecs.value();

                        @TimerId("timer")
                        private final TimerSpec timer =
                            TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

                        @ProcessElement
                        public void processElement(
                            ProcessContext context, @TimerId("timer") Timer timer) {

                          timer.offset(Duration.ZERO).setRelative();
                        }

                        @OnTimer("timer")
                        public void onTimer(
                            OnTimerContext context,
                            @StateId("valueState") ValueState<Long> intValueState,
                            @TimerId("timer") Timer timer) {
                          Long current = intValueState.read();
                          if (current == null) {
                            current = -1L;
                          }
                          long next = current + 1;
                          context.output(KV.of("key", next));
                          intValueState.write(next);
                          timer.offset(Duration.millis(100)).setRelative();
                        }
                      }));
    } else {
      key =
          pipeline
              .apply(GenerateSequence.from(0))
              .apply(
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
                  Integer read = intValueState.read();
                  assertNotNull(read);
                  if (read == 42) {
                    intValueState.write(0);
                    oneShotLatch.countDown();
                  }
                }
              }));
    } else {
      return key.apply(
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
                  Long value = context.element().getValue();
                  assertNotNull(value);
                  if (value == 0L) {
                    intValueState.write(42);
                    intBagState.add(40);
                    intBagState.add(1);
                    intBagState.add(1);
                    oneShotLatch.countDown();
                  }
                }
              }));
    }
  }
}
