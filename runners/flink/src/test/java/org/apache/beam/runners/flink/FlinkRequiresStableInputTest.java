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
import static org.hamcrest.Matchers.equalTo;

import java.util.Collections;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.Executors;
import org.apache.beam.model.jobmanagement.v1.JobApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.jobsubmission.JobInvocation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.RequiresStableInputIT;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.testing.CrashingRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.util.FilePatternMatchingShardedFile;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.MoreExecutors;
import org.joda.time.Instant;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests {@link DoFn.RequiresStableInput} with Flink. */
public class FlinkRequiresStableInputTest {

  @ClassRule public static TemporaryFolder tempFolder = new TemporaryFolder();

  private static final String VALUE = "value";
  // SHA-1 hash of string "value"
  private static final String VALUE_CHECKSUM = "f32b67c7e26342af42efabc674d441dca0a281c5";

  private static ListeningExecutorService flinkJobExecutor;
  private static final int PARALLELISM = 1;
  private static final long CHECKPOINT_INTERVAL = 2000L;
  private static final long FINISH_SOURCE_INTERVAL = 3 * CHECKPOINT_INTERVAL;

  @BeforeClass
  public static void setup() {
    // Restrict this to only one thread to avoid multiple Flink clusters up at the same time
    // which is not suitable for memory-constraint environments, i.e. Jenkins.
    flinkJobExecutor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
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
    runTest(false);
  }

  @Test(timeout = 30_000)
  public void testParDoRequiresStableInputPortable() throws Exception {
    runTest(true);
  }

  @Test(timeout = 30_000)
  public void testParDoRequiresStableInputStateful() throws Exception {
    testParDoRequiresStableInputStateful(false);
  }

  @Test(timeout = 30_000)
  public void testParDoRequiresStableInputStatefulPortable() throws Exception {
    testParDoRequiresStableInputStateful(true);
  }

  private void testParDoRequiresStableInputStateful(boolean portable) throws Exception {
    FlinkPipelineOptions opts = getFlinkOptions(portable);
    opts.as(FlinkPipelineOptions.class).setShutdownSourcesAfterIdleMs(FINISH_SOURCE_INTERVAL);
    opts.as(FlinkPipelineOptions.class).setNumberOfExecutionRetries(0);
    Pipeline pipeline = Pipeline.create(opts);
    PCollection<Integer> result =
        pipeline
            .apply(Create.of(1, 2, 3, 4))
            .apply(WithKeys.of((Void) null))
            .apply(ParDo.of(new StableDoFn()));
    PAssert.that(result).containsInAnyOrder(1, 2, 3, 4);
    executePipeline(pipeline, portable);
  }

  private void runTest(boolean portable) throws Exception {
    FlinkPipelineOptions options = getFlinkOptions(portable);

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

    executePipeline(p, portable);
    assertThat(
        new FilePatternMatchingShardedFile(singleOutputPrefix + "*"),
        fileContentsHaveChecksum(VALUE_CHECKSUM));
    assertThat(
        new FilePatternMatchingShardedFile(multiOutputPrefix + "*"),
        fileContentsHaveChecksum(VALUE_CHECKSUM));
  }

  private void executePipeline(Pipeline pipeline, boolean portable) throws Exception {
    if (portable) {
      RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline);
      FlinkPipelineOptions flinkOpts = pipeline.getOptions().as(FlinkPipelineOptions.class);
      // execute the pipeline
      JobInvocation jobInvocation =
          FlinkJobInvoker.create(null)
              .createJobInvocation(
                  "fakeId",
                  "fakeRetrievalToken",
                  flinkJobExecutor,
                  pipelineProto,
                  flinkOpts,
                  new FlinkPipelineRunner(flinkOpts, null, Collections.emptyList()));
      jobInvocation.start();
      while (jobInvocation.getState() != JobApi.JobState.Enum.DONE
          && jobInvocation.getState() != JobApi.JobState.Enum.FAILED) {

        Thread.sleep(1000);
      }
      assertThat(jobInvocation.getState(), equalTo(JobApi.JobState.Enum.DONE));
    } else {
      executePipelineLegacy(pipeline);
    }
  }

  private void executePipelineLegacy(Pipeline pipeline) {
    FlinkRunner flinkRunner = FlinkRunner.fromOptions(pipeline.getOptions());
    PipelineResult.State state = flinkRunner.run(pipeline).waitUntilFinish();
    assertThat(state, equalTo(PipelineResult.State.DONE));
  }

  private static Pipeline createPipeline(
      PipelineOptions options, String singleOutputPrefix, String multiOutputPrefix) {
    Pipeline p = Pipeline.create(options);
    SerializableFunction<Void, Void> sideEffect =
        ign -> {
          throw new IllegalStateException("Failing job to test @RequiresStableInput");
        };
    PCollection<String> impulse = p.apply("CreatePCollectionOfOneValue", Create.of(VALUE));
    impulse
        .apply(
            "Single-PairWithRandomKey",
            MapElements.via(new RequiresStableInputIT.PairWithRandomKeyFn()))
        // need Reshuffle due to https://github.com/apache/beam/issues/24655
        // can be removed once fixed
        .apply(Reshuffle.of())
        .apply(
            "Single-MakeSideEffectAndThenFail",
            ParDo.of(
                new RequiresStableInputIT.MakeSideEffectAndThenFailFn(
                    singleOutputPrefix, sideEffect)));
    impulse
        .apply(
            "Multi-PairWithRandomKey",
            MapElements.via(new RequiresStableInputIT.PairWithRandomKeyFn()))
        // need Reshuffle due to https://github.com/apache/beam/issues/24655
        // can be removed once fixed
        .apply(Reshuffle.of())
        .apply(
            "Multi-MakeSideEffectAndThenFail",
            ParDo.of(
                    new RequiresStableInputIT.MakeSideEffectAndThenFailFn(
                        multiOutputPrefix, sideEffect))
                .withOutputTags(new TupleTag<>(), TupleTagList.empty()));

    return p;
  }

  private FlinkPipelineOptions getFlinkOptions(boolean portable) {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setParallelism(PARALLELISM);
    options.setCheckpointingInterval(CHECKPOINT_INTERVAL);
    options.setShutdownSourcesAfterIdleMs(FINISH_SOURCE_INTERVAL);
    options.setFinishBundleBeforeCheckpointing(true);
    options.setMaxBundleTimeMills(100L);
    options.setStreaming(true);
    if (portable) {
      options.setRunner(CrashingRunner.class);
      options
          .as(PortablePipelineOptions.class)
          .setDefaultEnvironmentType(Environments.ENVIRONMENT_EMBEDDED);
    } else {
      options.setRunner(FlinkRunner.class);
    }
    return options;
  }

  private static class StableDoFn extends DoFn<KV<Void, Integer>, Integer> {

    @StateId("state")
    final StateSpec<BagState<Integer>> stateSpec = StateSpecs.bag();

    @TimerId("flush")
    final TimerSpec flushSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    @RequiresStableInput
    public void process(
        @Element KV<Void, Integer> input,
        @StateId("state") BagState<Integer> buffer,
        @TimerId("flush") Timer flush,
        OutputReceiver<Integer> output) {

      // Timers do not to work with stateful stable dofn,
      // see https://github.com/apache/beam/issues/24662
      // Once this is resolved, flush the buffer on timer
      // flush.set(GlobalWindow.INSTANCE.maxTimestamp());
      // buffer.add(input.getValue());
      output.output(input.getValue());
    }

    @OnTimer("flush")
    public void flush(
        @Timestamp Instant ts,
        @StateId("state") BagState<Integer> buffer,
        OutputReceiver<Integer> output) {

      Optional.ofNullable(buffer.read())
          .ifPresent(b -> b.forEach(e -> output.outputWithTimestamp(e, ts)));
      buffer.clear();
    }
  }
}
