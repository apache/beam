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

import static org.apache.beam.sdk.testing.FileChecksumMatcher.fileContentsHaveChecksum;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.RequiresStableInputIT;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.FilePatternMatchingShardedFile;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests {@link DoFn.RequiresStableInput} with Flink. */
public class FlinkRequiresStableInputTest {

  @ClassRule public static TemporaryFolder tempFolder = new TemporaryFolder();

  private static CountDownLatch latch;

  private static final String VALUE = "value";
  // SHA-1 hash of string "value"
  private static final String VALUE_CHECKSUM = "f32b67c7e26342af42efabc674d441dca0a281c5";

  private static transient MiniCluster flinkCluster;

  @BeforeClass
  public static void beforeClass() throws Exception {
    final int parallelism = 1;

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
            .setNumTaskManagers(1)
            .setNumSlotsPerTaskManager(1)
            .build();

    flinkCluster = new MiniCluster(clusterConfig);
    flinkCluster.start();

    TestStreamEnvironment.setAsContext(flinkCluster, parallelism);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TestStreamEnvironment.unsetAsContext();
    flinkCluster.close();
    flinkCluster = null;
  }

  /**
   * Test for the support of {@link DoFn.RequiresStableInput} in both {@link ParDo.SingleOutput} and
   * {@link ParDo.MultiOutput}.
   *
   * <p>In each test, a singleton string value is paired with a random key. In the following
   * transform, the value is written to a file, whose path is specified by the random key, and then
   * the transform fails. When the pipeline retries, the latter transform should receive the same
   * input from the former transform, because its {@link DoFn} is annotated with {@link
   * DoFn.RequiresStableInput}, and it will not fail due to presence of the file. Therefore, only
   * one file for each transform is expected.
   *
   * <p>A Savepoint is taken until the desired state in the operators has been reached. We then
   * restore the savepoint to check if we produce impotent results.
   */
  @Test(timeout = 30_000)
  public void testParDoRequiresStableInput() throws Exception {
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    options.setParallelism(1);
    // We only want to trigger external savepoints but we require
    // checkpointing to be enabled for @RequiresStableInput
    options.setCheckpointingInterval(Long.MAX_VALUE);
    options.setRunner(FlinkRunner.class);
    options.setStreaming(true);

    ResourceId outputDir =
        FileSystems.matchNewResource(tempFolder.getRoot().getAbsolutePath(), true)
            .resolve(
                String.format("requires-stable-input-%tF-%<tH-%<tM-%<tS-%<tL", new Date()),
                ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY);
    String singleOutputPrefix =
        outputDir
            .resolve("pardo-single-output", ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("key-", ResolveOptions.StandardResolveOptions.RESOLVE_FILE)
            .toString();
    String multiOutputPrefix =
        outputDir
            .resolve("pardo-multi-output", ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("key-", ResolveOptions.StandardResolveOptions.RESOLVE_FILE)
            .toString();

    Pipeline p = createPipeline(options, singleOutputPrefix, multiOutputPrefix);

    // a latch used by the transforms to signal completion
    latch = new CountDownLatch(2);
    JobID jobID = executePipeline(p);
    String savepointDir;
    do {
      // Take a savepoint (checkpoint) which will trigger releasing the buffered elements
      // and trigger the latch
      savepointDir = takeSavepoint(jobID);
    } while (!latch.await(100, TimeUnit.MILLISECONDS));
    flinkCluster.cancelJob(jobID).get();

    options.setShutdownSourcesAfterIdleMs(0L);
    restoreFromSavepoint(p, savepointDir);
    waitUntilJobIsDone();

    assertThat(
        new FilePatternMatchingShardedFile(singleOutputPrefix + "*"),
        fileContentsHaveChecksum(VALUE_CHECKSUM));
    assertThat(
        new FilePatternMatchingShardedFile(multiOutputPrefix + "*"),
        fileContentsHaveChecksum(VALUE_CHECKSUM));
  }

  private JobGraph getJobGraph(Pipeline pipeline) {
    FlinkRunner flinkRunner = FlinkRunner.fromOptions(pipeline.getOptions());
    return flinkRunner.getJobGraph(pipeline);
  }

  private JobID executePipeline(Pipeline pipeline) throws Exception {
    JobGraph jobGraph = getJobGraph(pipeline);
    flinkCluster.submitJob(jobGraph).get();
    return jobGraph.getJobID();
  }

  private String takeSavepoint(JobID jobID) throws Exception {
    Exception exception = null;
    // try multiple times because the job might not be ready yet
    for (int i = 0; i < 10; i++) {
      try {
        return flinkCluster.triggerSavepoint(jobID, null, false).get();
      } catch (Exception e) {
        exception = e;
        Thread.sleep(100);
      }
    }
    throw exception;
  }

  private JobID restoreFromSavepoint(Pipeline pipeline, String savepointDir)
      throws ExecutionException, InterruptedException {
    JobGraph jobGraph = getJobGraph(pipeline);
    SavepointRestoreSettings savepointSettings = SavepointRestoreSettings.forPath(savepointDir);
    jobGraph.setSavepointRestoreSettings(savepointSettings);
    return flinkCluster.submitJob(jobGraph).get().getJobID();
  }

  private void waitUntilJobIsDone() throws InterruptedException, ExecutionException {
    while (flinkCluster.listJobs().get().stream()
        .anyMatch(message -> message.getJobState().name().equals("RUNNING"))) {
      Thread.sleep(100);
    }
  }

  private static Pipeline createPipeline(
      PipelineOptions options, String singleOutputPrefix, String multiOutputPrefix) {
    Pipeline p = Pipeline.create(options);

    SerializableFunction<Void, Void> firstTime =
        (SerializableFunction<Void, Void>)
            value -> {
              latch.countDown();
              return null;
            };

    PCollection<String> impulse = p.apply("CreatePCollectionOfOneValue", Create.of(VALUE));
    impulse
        .apply(
            "Single-PairWithRandomKey",
            MapElements.via(new RequiresStableInputIT.PairWithRandomKeyFn()))
        .apply(
            "Single-MakeSideEffectAndThenFail",
            ParDo.of(
                new RequiresStableInputIT.MakeSideEffectAndThenFailFn(
                    singleOutputPrefix, firstTime)));
    impulse
        .apply(
            "Multi-PairWithRandomKey",
            MapElements.via(new RequiresStableInputIT.PairWithRandomKeyFn()))
        .apply(
            "Multi-MakeSideEffectAndThenFail",
            ParDo.of(
                    new RequiresStableInputIT.MakeSideEffectAndThenFailFn(
                        multiOutputPrefix, firstTime))
                .withOutputTags(new TupleTag<>(), TupleTagList.empty()));

    return p;
  }
}
