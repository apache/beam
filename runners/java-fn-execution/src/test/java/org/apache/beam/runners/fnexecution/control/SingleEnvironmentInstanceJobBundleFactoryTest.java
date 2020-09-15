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
package org.apache.beam.runners.fnexecution.control;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.GreedyPipelineFuser;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.InProcessServerFactory;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.environment.EnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.RemoteEnvironment;
import org.apache.beam.runners.fnexecution.state.GrpcStateService;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link SingleEnvironmentInstanceJobBundleFactory}. */
@RunWith(JUnit4.class)
public class SingleEnvironmentInstanceJobBundleFactoryTest {
  @Mock private EnvironmentFactory environmentFactory;
  @Mock private InstructionRequestHandler instructionRequestHandler;

  private ExecutorService executor = Executors.newCachedThreadPool();

  private GrpcFnServer<GrpcDataService> dataServer;
  private GrpcFnServer<GrpcStateService> stateServer;
  private JobBundleFactory factory;

  private static final String GENERATED_ID = "staticId";

  @Before
  public void setup() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(instructionRequestHandler.handle(any(InstructionRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(InstructionResponse.getDefaultInstance()));

    InProcessServerFactory serverFactory = InProcessServerFactory.create();
    dataServer =
        GrpcFnServer.allocatePortAndCreateFor(
            GrpcDataService.create(
                PipelineOptionsFactory.create(), executor, OutboundObserverFactory.serverDirect()),
            serverFactory);
    stateServer = GrpcFnServer.allocatePortAndCreateFor(GrpcStateService.create(), serverFactory);

    factory =
        SingleEnvironmentInstanceJobBundleFactory.create(
            environmentFactory, dataServer, stateServer, () -> GENERATED_ID);
  }

  @After
  public void teardown() throws Exception {
    try (AutoCloseable data = dataServer;
        AutoCloseable state = stateServer) {
      executor.shutdownNow();
    }
  }

  @Test
  public void closeShutsDownEnvironments() throws Exception {
    Pipeline p = Pipeline.create();
    ExperimentalOptions.addExperiment(p.getOptions().as(ExperimentalOptions.class), "beam_fn_api");
    p.apply("Create", Create.of(1, 2, 3));

    ExecutableStage stage =
        GreedyPipelineFuser.fuse(PipelineTranslation.toProto(p)).getFusedStages().stream()
            .findFirst()
            .get();
    RemoteEnvironment remoteEnv = mock(RemoteEnvironment.class);
    when(remoteEnv.getInstructionRequestHandler()).thenReturn(instructionRequestHandler);
    when(environmentFactory.createEnvironment(stage.getEnvironment(), GENERATED_ID))
        .thenReturn(remoteEnv);

    factory.forStage(stage);
    factory.close();
    verify(remoteEnv).close();
  }

  @Test
  public void closeShutsDownEnvironmentsWhenSomeFail() throws Exception {
    Pipeline p = Pipeline.create();
    ExperimentalOptions.addExperiment(p.getOptions().as(ExperimentalOptions.class), "beam_fn_api");
    p.apply("Create", Create.of(1, 2, 3));

    ExecutableStage firstEnvStage =
        GreedyPipelineFuser.fuse(PipelineTranslation.toProto(p)).getFusedStages().stream()
            .findFirst()
            .get();
    ExecutableStagePayload basePayload =
        ExecutableStagePayload.parseFrom(firstEnvStage.toPTransform("foo").getSpec().getPayload());

    Environment secondEnv = Environments.createDockerEnvironment("second_env");
    ExecutableStage secondEnvStage =
        ExecutableStage.fromPayload(basePayload.toBuilder().setEnvironment(secondEnv).build());

    Environment thirdEnv = Environments.createDockerEnvironment("third_env");
    ExecutableStage thirdEnvStage =
        ExecutableStage.fromPayload(basePayload.toBuilder().setEnvironment(thirdEnv).build());

    RemoteEnvironment firstRemoteEnv = mock(RemoteEnvironment.class, "First Remote Env");
    RemoteEnvironment secondRemoteEnv = mock(RemoteEnvironment.class, "Second Remote Env");
    RemoteEnvironment thirdRemoteEnv = mock(RemoteEnvironment.class, "Third Remote Env");
    when(environmentFactory.createEnvironment(firstEnvStage.getEnvironment(), GENERATED_ID))
        .thenReturn(firstRemoteEnv);
    when(environmentFactory.createEnvironment(secondEnvStage.getEnvironment(), GENERATED_ID))
        .thenReturn(secondRemoteEnv);
    when(environmentFactory.createEnvironment(thirdEnvStage.getEnvironment(), GENERATED_ID))
        .thenReturn(thirdRemoteEnv);
    when(firstRemoteEnv.getInstructionRequestHandler()).thenReturn(instructionRequestHandler);
    when(secondRemoteEnv.getInstructionRequestHandler()).thenReturn(instructionRequestHandler);
    when(thirdRemoteEnv.getInstructionRequestHandler()).thenReturn(instructionRequestHandler);

    factory.forStage(firstEnvStage);
    factory.forStage(secondEnvStage);
    factory.forStage(thirdEnvStage);

    IllegalStateException firstException = new IllegalStateException("first stage");
    doThrow(firstException).when(firstRemoteEnv).close();
    IllegalStateException thirdException = new IllegalStateException("third stage");
    doThrow(thirdException).when(thirdRemoteEnv).close();

    try {
      factory.close();
      fail("Factory close should have thrown");
    } catch (IllegalStateException expected) {
      if (expected.equals(firstException)) {
        assertThat(ImmutableList.copyOf(expected.getSuppressed()), contains(thirdException));
      } else if (expected.equals(thirdException)) {
        assertThat(ImmutableList.copyOf(expected.getSuppressed()), contains(firstException));
      } else {
        throw expected;
      }

      verify(firstRemoteEnv).close();
      verify(secondRemoteEnv).close();
      verify(thirdRemoteEnv).close();
    }
  }
}
