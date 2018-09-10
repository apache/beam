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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse;
import org.apache.beam.model.pipeline.v1.RunnerApi.Coder;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.SdkFunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowingStrategy;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.ModelCoders;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.environment.ProcessEnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.RemoteEnvironment;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.provisioning.StaticGrpcProvisionService;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.IdGenerators;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link ProcessEnvironmentFactory}. */
@RunWith(JUnit4.class)
public class ProcessJobBundleFactoryTest {

  @Mock private ProcessEnvironmentFactory envFactory;
  @Mock private RemoteEnvironment remoteEnvironment;
  @Mock private InstructionRequestHandler instructionHandler;
  @Mock private ServerFactory serverFactory;
  @Mock GrpcFnServer<FnApiControlClientPoolService> controlServer;
  @Mock GrpcFnServer<GrpcLoggingService> loggingServer;
  @Mock GrpcFnServer<ArtifactRetrievalService> retrievalServer;
  @Mock GrpcFnServer<StaticGrpcProvisionService> provisioningServer;

  private final Environment environment = Environments.createDockerEnvironment("env-url");
  private final IdGenerator stageIdGenerator = IdGenerators.incrementingLongs();
  private final InstructionResponse instructionResponse =
      InstructionResponse.newBuilder().setInstructionId("instruction-id").build();

  @Before
  public void setUpMocks() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(envFactory.createEnvironment(environment)).thenReturn(remoteEnvironment);
    when(remoteEnvironment.getInstructionRequestHandler()).thenReturn(instructionHandler);
    when(instructionHandler.handle(any()))
        .thenReturn(CompletableFuture.completedFuture(instructionResponse));
  }

  @Test
  public void createsCorrectEnvironment() throws Exception {
    try (ProcessJobBundleFactory bundleFactory =
        new ProcessJobBundleFactory(
            envFactory,
            serverFactory,
            stageIdGenerator,
            controlServer,
            loggingServer,
            retrievalServer,
            provisioningServer)) {
      bundleFactory.forStage(getExecutableStage(environment));
      verify(envFactory).createEnvironment(environment);
    }
  }

  @Test
  public void closesEnvironmentOnCleanup() throws Exception {
    ProcessJobBundleFactory bundleFactory =
        new ProcessJobBundleFactory(
            envFactory,
            serverFactory,
            stageIdGenerator,
            controlServer,
            loggingServer,
            retrievalServer,
            provisioningServer);
    try (AutoCloseable unused = bundleFactory) {
      bundleFactory.forStage(getExecutableStage(environment));
    }
    verify(remoteEnvironment).close();
  }

  @Test
  public void cachesEnvironment() throws Exception {
    try (ProcessJobBundleFactory bundleFactory =
        new ProcessJobBundleFactory(
            envFactory,
            serverFactory,
            stageIdGenerator,
            controlServer,
            loggingServer,
            retrievalServer,
            provisioningServer)) {
      StageBundleFactory bf1 = bundleFactory.forStage(getExecutableStage(environment));
      StageBundleFactory bf2 = bundleFactory.forStage(getExecutableStage(environment));
      // NOTE: We hang on to stage bundle references to ensure their underlying environments are not
      // garbage collected. For additional safety, we print the factories to ensure the referernces
      // are not optimized away.
      System.out.println("bundle factory 1:" + bf1);
      System.out.println("bundle factory 1:" + bf2);
      verify(envFactory).createEnvironment(environment);
      verifyNoMoreInteractions(envFactory);
    }
  }

  @Test
  public void doesNotCacheDifferentEnvironments() throws Exception {
    Environment envFoo = Environments.createDockerEnvironment("foo-env-url");
    RemoteEnvironment remoteEnvFoo = mock(RemoteEnvironment.class);
    InstructionRequestHandler fooInstructionHandler = mock(InstructionRequestHandler.class);
    when(envFactory.createEnvironment(envFoo)).thenReturn(remoteEnvFoo);
    when(remoteEnvFoo.getInstructionRequestHandler()).thenReturn(fooInstructionHandler);
    // Don't bother creating a distinct instruction response because we don't use it here.
    when(fooInstructionHandler.handle(any()))
        .thenReturn(CompletableFuture.completedFuture(instructionResponse));

    try (ProcessJobBundleFactory bundleFactory =
        new ProcessJobBundleFactory(
            envFactory,
            serverFactory,
            stageIdGenerator,
            controlServer,
            loggingServer,
            retrievalServer,
            provisioningServer)) {
      bundleFactory.forStage(getExecutableStage(environment));
      bundleFactory.forStage(getExecutableStage(envFoo));
      verify(envFactory).createEnvironment(environment);
      verify(envFactory).createEnvironment(envFoo);
      verifyNoMoreInteractions(envFactory);
    }
  }

  private static ExecutableStage getExecutableStage(Environment environment) {
    return ExecutableStage.fromPayload(
        ExecutableStagePayload.newBuilder()
            .setInput("input-pc")
            .setEnvironment(environment)
            .setComponents(
                Components.newBuilder()
                    .putPcollections(
                        "input-pc",
                        PCollection.newBuilder()
                            .setWindowingStrategyId("windowing-strategy")
                            .setCoderId("coder-id")
                            .build())
                    .putWindowingStrategies(
                        "windowing-strategy",
                        WindowingStrategy.newBuilder().setWindowCoderId("coder-id").build())
                    .putCoders(
                        "coder-id",
                        Coder.newBuilder()
                            .setSpec(
                                SdkFunctionSpec.newBuilder()
                                    .setSpec(
                                        FunctionSpec.newBuilder()
                                            .setUrn(ModelCoders.INTERVAL_WINDOW_CODER_URN)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build());
  }
}
