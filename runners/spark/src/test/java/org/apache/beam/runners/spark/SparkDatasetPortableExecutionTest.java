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
package org.apache.beam.runners.spark;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobState;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.jobsubmission.JobInvocation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.CrashingRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.MoreExecutors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Runs portable pipelines end to end on the Dataset-based backend: job invocation, executable stage
 * translation, and execution with the embedded SDK harness.
 */
@RunWith(JUnit4.class)
public class SparkDatasetPortableExecutionTest implements Serializable {
  private static ListeningExecutorService executor;

  @BeforeClass
  public static void setUp() {
    executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
  }

  @AfterClass
  public static void tearDown() throws InterruptedException {
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);
    executor = null;
  }

  @Test(timeout = 180_000)
  public void boundedPipelineRunsOnDatasets() throws Exception {
    SparkPipelineOptions options = options();
    Pipeline p = Pipeline.create(options);
    PCollection<String> words =
        p.apply("impulse", Impulse.create())
            .apply(
                "create",
                ParDo.of(
                    new DoFn<byte[], String>() {
                      @ProcessElement
                      public void process(ProcessContext ctxt) {
                        ctxt.output("zero");
                        ctxt.output("one");
                        ctxt.output("two");
                      }
                    }));
    PCollection<String> more =
        p.apply("impulse2", Impulse.create())
            .apply(
                "create2",
                ParDo.of(
                    new DoFn<byte[], String>() {
                      @ProcessElement
                      public void process(ProcessContext ctxt) {
                        ctxt.output("three");
                      }
                    }));
    PCollection<String> result =
        PCollectionList.of(words)
            .and(more)
            .apply("flatten", Flatten.pCollections())
            .apply(
                "len",
                ParDo.of(
                    new DoFn<String, Long>() {
                      @ProcessElement
                      public void process(ProcessContext ctxt) {
                        ctxt.output((long) ctxt.element().length());
                      }
                    }))
            .apply("addKeys", WithKeys.of("foo"))
            // Use some unknown coders
            .setCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianLongCoder.of()))
            .apply("gbk", GroupByKey.create())
            .apply(
                "format",
                ParDo.of(
                    new DoFn<KV<String, Iterable<Long>>, String>() {
                      @ProcessElement
                      public void process(ProcessContext ctxt) {
                        // The order of grouped values is not defined, so sort before comparing.
                        List<Long> values = new ArrayList<>();
                        ctxt.element().getValue().forEach(values::add);
                        Collections.sort(values);
                        ctxt.output(ctxt.element().getKey() + ":" + values);
                      }
                    }));
    PAssert.that(result).containsInAnyOrder("foo:[3, 3, 4, 5]");

    List<String> messages = new CopyOnWriteArrayList<>();
    JobState.Enum state = run(p, options, "bounded", messages);
    assertEquals(String.join("\n", messages), JobState.Enum.DONE, state);
  }

  @Test(timeout = 180_000)
  public void unboundedInputIsRejectedAtTranslation() throws Exception {
    SparkPipelineOptions options = options();
    Pipeline p = Pipeline.create(options);
    p.apply("unbounded", GenerateSequence.from(0));

    List<String> messages = new CopyOnWriteArrayList<>();
    assertEquals(JobState.Enum.FAILED, run(p, options, "unbounded", messages));
    assertThat(messages, hasItem(containsString("bounded pipelines only")));
  }

  @Test(timeout = 180_000)
  public void statefulStageIsRejectedAtTranslation() throws Exception {
    SparkPipelineOptions options = options();
    Pipeline p = Pipeline.create(options);
    p.apply("impulse", Impulse.create())
        .apply("addKeys", WithKeys.of("foo"))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), ByteArrayCoder.of()))
        .apply(
            "stateful",
            ParDo.of(
                new DoFn<KV<String, byte[]>, Long>() {
                  @StateId("count")
                  private final StateSpec<ValueState<Long>> count = StateSpecs.value();

                  @ProcessElement
                  public void process(
                      @StateId("count") ValueState<Long> count, OutputReceiver<Long> out) {
                    long next = count.read() == null ? 1 : count.read() + 1;
                    count.write(next);
                    out.output(next);
                  }
                }));

    List<String> messages = new CopyOnWriteArrayList<>();
    assertEquals(JobState.Enum.FAILED, run(p, options, "stateful", messages));
    assertThat(messages, hasItem(containsString("uses state or timers")));
  }

  private static SparkPipelineOptions options() {
    PipelineOptions options = PipelineOptionsFactory.fromArgs("--experiments=beam_fn_api").create();
    options.setRunner(CrashingRunner.class);
    options
        .as(PortablePipelineOptions.class)
        .setDefaultEnvironmentType(Environments.ENVIRONMENT_EMBEDDED);
    SparkPipelineOptions sparkOptions = options.as(SparkPipelineOptions.class);
    sparkOptions.setSparkMaster("local[2]");
    sparkOptions.setUseStructuredStreaming(true);
    return sparkOptions;
  }

  /** Submits the pipeline through the job invoker and returns its terminal state. */
  private static JobState.Enum run(
      Pipeline p, SparkPipelineOptions options, String jobId, List<String> messages)
      throws Exception {
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);
    JobInvocation invocation =
        SparkJobInvoker.createJobInvocation(
            jobId, "fakeRetrievalToken", executor, pipelineProto, options);
    invocation.addMessageListener(message -> messages.add(message.getMessageText()));
    invocation.start();
    while (!JobInvocation.isTerminated(invocation.getState())) {
      Thread.sleep(200);
    }
    return invocation.getState();
  }
}
