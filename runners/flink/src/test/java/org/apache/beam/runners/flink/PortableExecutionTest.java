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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.Executors;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobState.Enum;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.InProcessServerFactory;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.control.ControlClientPool.Source;
import org.apache.beam.runners.fnexecution.control.DockerJobBundleFactory;
import org.apache.beam.runners.fnexecution.control.FnApiControlClientPoolService;
import org.apache.beam.runners.fnexecution.environment.EnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.InProcessEnvironmentFactory;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.provisioning.StaticGrpcProvisionService;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests the execution of a pipeline from specification to execution on the portable Flink runner.
 * Exercises job invocation, executable stage translation and deployment with embedded Flink for
 * batch and streaming.
 */
@RunWith(Parameterized.class)
public class PortableExecutionTest implements Serializable {

  @Parameters
  public static Object[] data() {
    return new Object[] {true, false};
  }

  @Parameter public boolean isStreaming;

  private transient ListeningExecutorService flinkJobExecutor;

  private DockerJobBundleFactory createJobBundleFactory(JobInfo jobInfo) throws Exception {
    return new DockerJobBundleFactory(jobInfo) {

      @Override
      protected ServerFactory getServerFactory() {
        return InProcessServerFactory.create();
      }

      @Override
      protected EnvironmentFactory getEnvironmentFactory(
          GrpcFnServer<FnApiControlClientPoolService> controlServer,
          GrpcFnServer<GrpcLoggingService> loggingServer,
          GrpcFnServer<ArtifactRetrievalService> retrievalServer,
          GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
          Source clientSource,
          IdGenerator idGenerator) {
        return InProcessEnvironmentFactory.create(
            PipelineOptionsFactory.create(), loggingServer, controlServer, clientSource);
      }
    };
  }

  @Before
  public void setup() {
    DockerJobBundleFactory.FACTORY.set(this::createJobBundleFactory);
    flinkJobExecutor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
  }

  @After
  public void tearDown() {
    flinkJobExecutor.shutdown();
  }

  private static ArrayList<KV<String, Iterable<Long>>> outputValues = new ArrayList<>();

  @Test
  public void testExecution() throws Exception {
    Pipeline p = Pipeline.create();
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
                }))
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
        // Force the output to be materialized
        .apply("gbk", GroupByKey.create())
        .apply(
            "collect",
            ParDo.of(
                new DoFn<KV<String, Iterable<Long>>, Void>() {
                  @ProcessElement
                  public void process(ProcessContext ctx) {
                    outputValues.add(ctx.element());
                  }
                }));

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);

    outputValues.clear();
    // execute the pipeline
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    options.setFlinkMaster("[local]");
    options.setStreaming(isStreaming);
    FlinkJobInvocation jobInvocation =
        FlinkJobInvocation.create(
            "fakeId",
            "fakeRetrievalToken",
            flinkJobExecutor,
            pipelineProto,
            options,
            Collections.EMPTY_LIST);
    jobInvocation.start();
    long timeout = System.currentTimeMillis() + 60 * 1000;
    while (jobInvocation.getState() != Enum.DONE && System.currentTimeMillis() < timeout) {
      Thread.sleep(1000);
    }
    assertEquals("job state", Enum.DONE, jobInvocation.getState());

    assertEquals(1, outputValues.size());
    assertEquals("foo", outputValues.get(0).getKey());
    assertThat(outputValues.get(0).getValue(), containsInAnyOrder(4L, 3L, 3L));
  }
}
