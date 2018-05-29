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

package org.apache.beam.runners.direct.portable;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload;
import org.apache.beam.runners.core.construction.JavaReadViaImpulse;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.GreedyPipelineFuser;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.InProcessServerFactory;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.environment.EnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.RemoteEnvironment;
import org.apache.beam.runners.fnexecution.state.GrpcStateService;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link DirectJobBundleFactory}. */
@RunWith(JUnit4.class)
public class DirectJobBundleFactoryTest {
  @Mock private EnvironmentFactory environmentFactory;
  @Mock private InstructionRequestHandler instructionRequestHandler;

  private ExecutorService executor = Executors.newCachedThreadPool();

  private GrpcFnServer<GrpcDataService> dataServer;
  private GrpcFnServer<GrpcStateService> stateServer;
  private JobBundleFactory factory;

  @Before
  public void setup() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(instructionRequestHandler.handle(any(InstructionRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(InstructionResponse.getDefaultInstance()));

    InProcessServerFactory serverFactory = InProcessServerFactory.create();
    dataServer =
        GrpcFnServer.allocatePortAndCreateFor(GrpcDataService.create(executor), serverFactory);
    stateServer = GrpcFnServer.allocatePortAndCreateFor(GrpcStateService.create(), serverFactory);

    factory = DirectJobBundleFactory.create(environmentFactory, dataServer, stateServer);
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
    p.apply("Create", Create.of(1, 2, 3));
    p.replaceAll(Collections.singletonList(JavaReadViaImpulse.boundedOverride()));

    ExecutableStage stage =
        GreedyPipelineFuser.fuse(PipelineTranslation.toProto(p))
            .getFusedStages()
            .stream()
            .findFirst()
            .get();
    RemoteEnvironment remoteEnv = mock(RemoteEnvironment.class);
    when(remoteEnv.getInstructionRequestHandler()).thenReturn(instructionRequestHandler);
    when(environmentFactory.createEnvironment(stage.getEnvironment())).thenReturn(remoteEnv);

    factory.forStage(stage);
    factory.close();
    verify(remoteEnv).close();
  }

  @Test
  public void closeShutsDownEnvironmentsWhenSomeFail() throws Exception {
    Pipeline p = Pipeline.create();
    p.apply("Create", Create.of(1, 2, 3));
    p.replaceAll(Collections.singletonList(JavaReadViaImpulse.boundedOverride()));

    ExecutableStage firstEnvStage =
        GreedyPipelineFuser.fuse(PipelineTranslation.toProto(p))
            .getFusedStages()
            .stream()
            .findFirst()
            .get();
    ExecutableStagePayload basePayload =
        ExecutableStagePayload.parseFrom(firstEnvStage.toPTransform().getSpec().getPayload());

    Environment secondEnv = Environment.newBuilder().setUrl("second_env").build();
    ExecutableStage secondEnvStage =
        ExecutableStage.fromPayload(basePayload.toBuilder().setEnvironment(secondEnv).build());

    Environment thirdEnv = Environment.newBuilder().setUrl("third_env").build();
    ExecutableStage thirdEnvStage =
        ExecutableStage.fromPayload(basePayload.toBuilder().setEnvironment(thirdEnv).build());

    RemoteEnvironment firstRemoteEnv = mock(RemoteEnvironment.class, "First Remote Env");
    RemoteEnvironment secondRemoteEnv = mock(RemoteEnvironment.class, "Second Remote Env");
    RemoteEnvironment thirdRemoteEnv = mock(RemoteEnvironment.class, "Third Remote Env");
    when(environmentFactory.createEnvironment(firstEnvStage.getEnvironment()))
        .thenReturn(firstRemoteEnv);
    when(environmentFactory.createEnvironment(secondEnvStage.getEnvironment()))
        .thenReturn(secondRemoteEnv);
    when(environmentFactory.createEnvironment(thirdEnvStage.getEnvironment()))
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
