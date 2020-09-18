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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi.Coder;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowingStrategy;
import org.apache.beam.runners.core.construction.ModelCoders;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.environment.EnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.EnvironmentFactory.Provider;
import org.apache.beam.runners.fnexecution.environment.RemoteEnvironment;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.provisioning.StaticGrpcProvisionService;
import org.apache.beam.runners.fnexecution.state.GrpcStateService;
import org.apache.beam.runners.fnexecution.state.StateDelegator;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.Struct;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Tests for {@link DefaultJobBundleFactory}. */
@RunWith(JUnit4.class)
public class DefaultJobBundleFactoryTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Mock private EnvironmentFactory envFactory;
  @Mock private RemoteEnvironment remoteEnvironment;
  @Mock private InstructionRequestHandler instructionHandler;
  @Mock GrpcFnServer<FnApiControlClientPoolService> controlServer;
  @Mock GrpcFnServer<GrpcLoggingService> loggingServer;
  @Mock GrpcFnServer<ArtifactRetrievalService> retrievalServer;
  @Mock GrpcFnServer<StaticGrpcProvisionService> provisioningServer;
  @Mock private StaticGrpcProvisionService provisionService;
  @Mock private GrpcFnServer<GrpcDataService> dataServer;
  @Mock private GrpcFnServer<GrpcStateService> stateServer;

  private final Environment environment = Environment.newBuilder().setUrn("dummy:urn").build();
  private final IdGenerator stageIdGenerator = IdGenerators.incrementingLongs();
  private final InstructionResponse instructionResponse =
      InstructionResponse.newBuilder().setInstructionId("instruction-id").build();
  private final EnvironmentFactory.Provider envFactoryProvider =
      (GrpcFnServer<FnApiControlClientPoolService> controlServiceServer,
          GrpcFnServer<GrpcLoggingService> loggingServiceServer,
          GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer,
          GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
          ControlClientPool clientPool,
          IdGenerator idGenerator) -> envFactory;
  private final Map<String, EnvironmentFactory.Provider> envFactoryProviderMap =
      ImmutableMap.of(environment.getUrn(), envFactoryProvider);
  private DefaultJobBundleFactory.ServerInfo serverInfo;

  @Before
  public void setUpMocks() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(envFactory.createEnvironment(eq(environment), any())).thenReturn(remoteEnvironment);
    when(remoteEnvironment.getInstructionRequestHandler()).thenReturn(instructionHandler);
    when(instructionHandler.handle(any()))
        .thenReturn(CompletableFuture.completedFuture(instructionResponse));
    when(dataServer.getApiServiceDescriptor())
        .thenReturn(ApiServiceDescriptor.getDefaultInstance());
    GrpcDataService dataService = mock(GrpcDataService.class);
    when(dataService.send(any(), any())).thenReturn(mock(CloseableFnDataReceiver.class));
    when(dataServer.getService()).thenReturn(dataService);
    when(stateServer.getApiServiceDescriptor())
        .thenReturn(ApiServiceDescriptor.getDefaultInstance());
    GrpcStateService stateService = mock(GrpcStateService.class);
    when(stateService.registerForProcessBundleInstructionId(any(), any()))
        .thenReturn(mock(StateDelegator.Registration.class));
    when(stateServer.getService()).thenReturn(stateService);
    when(provisioningServer.getService()).thenReturn(provisionService);
    serverInfo =
        new AutoValue_DefaultJobBundleFactory_ServerInfo.Builder()
            .setControlServer(controlServer)
            .setLoggingServer(loggingServer)
            .setRetrievalServer(retrievalServer)
            .setProvisioningServer(provisioningServer)
            .setDataServer(dataServer)
            .setStateServer(stateServer)
            .build();
  }

  @Test
  public void createsCorrectEnvironment() throws Exception {
    try (DefaultJobBundleFactory bundleFactory =
        createDefaultJobBundleFactory(envFactoryProviderMap)) {
      bundleFactory.forStage(getExecutableStage(environment));
      verify(envFactory).createEnvironment(eq(environment), any());
    }
  }

  @Test
  public void createsMultipleEnvironmentOfSingleType() throws Exception {
    ServerFactory serverFactory = ServerFactory.createDefault();

    Environment environmentA =
        Environment.newBuilder()
            .setUrn("env:urn:a")
            .setPayload(ByteString.copyFrom(new byte[1]))
            .build();
    Environment environmentAA =
        Environment.newBuilder()
            .setUrn("env:urn:a")
            .setPayload(ByteString.copyFrom(new byte[2]))
            .build();
    EnvironmentFactory envFactoryA = mock(EnvironmentFactory.class);
    when(envFactoryA.createEnvironment(eq(environmentA), any())).thenReturn(remoteEnvironment);
    when(envFactoryA.createEnvironment(eq(environmentAA), any())).thenReturn(remoteEnvironment);
    EnvironmentFactory.Provider environmentProviderFactoryA =
        mock(EnvironmentFactory.Provider.class);
    when(environmentProviderFactoryA.createEnvironmentFactory(
            any(), any(), any(), any(), any(), any()))
        .thenReturn(envFactoryA);
    when(environmentProviderFactoryA.getServerFactory()).thenReturn(serverFactory);

    Environment environmentB = Environment.newBuilder().setUrn("env:urn:b").build();
    EnvironmentFactory envFactoryB = mock(EnvironmentFactory.class);
    when(envFactoryB.createEnvironment(eq(environmentB), any())).thenReturn(remoteEnvironment);
    EnvironmentFactory.Provider environmentProviderFactoryB =
        mock(EnvironmentFactory.Provider.class);
    when(environmentProviderFactoryB.createEnvironmentFactory(
            any(), any(), any(), any(), any(), any()))
        .thenReturn(envFactoryB);
    when(environmentProviderFactoryB.getServerFactory()).thenReturn(serverFactory);

    Map<String, Provider> environmentFactoryProviderMap =
        ImmutableMap.of(
            environmentA.getUrn(), environmentProviderFactoryA,
            environmentB.getUrn(), environmentProviderFactoryB);
    try (DefaultJobBundleFactory bundleFactory =
        createDefaultJobBundleFactory(environmentFactoryProviderMap)) {
      bundleFactory.forStage(getExecutableStage(environmentA));
      verify(environmentProviderFactoryA, Mockito.times(1))
          .createEnvironmentFactory(any(), any(), any(), any(), any(), any());
      verify(environmentProviderFactoryB, Mockito.times(0))
          .createEnvironmentFactory(any(), any(), any(), any(), any(), any());
      verify(envFactoryA, Mockito.times(1)).createEnvironment(eq(environmentA), any());
      verify(envFactoryA, Mockito.times(0)).createEnvironment(eq(environmentAA), any());

      bundleFactory.forStage(getExecutableStage(environmentAA));
      verify(environmentProviderFactoryA, Mockito.times(2))
          .createEnvironmentFactory(any(), any(), any(), any(), any(), any());
      verify(environmentProviderFactoryB, Mockito.times(0))
          .createEnvironmentFactory(any(), any(), any(), any(), any(), any());
      verify(envFactoryA, Mockito.times(1)).createEnvironment(eq(environmentA), any());
      verify(envFactoryA, Mockito.times(1)).createEnvironment(eq(environmentAA), any());
    }
  }

  @Test
  public void createsMultipleEnvironmentsWithSdkWorkerParallelism() throws Exception {
    ServerFactory serverFactory = ServerFactory.createDefault();
    Environment environmentA =
        Environment.newBuilder()
            .setUrn("env:urn:a")
            .setPayload(ByteString.copyFrom(new byte[1]))
            .build();
    EnvironmentFactory envFactoryA = mock(EnvironmentFactory.class);
    when(envFactoryA.createEnvironment(eq(environmentA), any())).thenReturn(remoteEnvironment);
    EnvironmentFactory.Provider environmentProviderFactoryA =
        mock(EnvironmentFactory.Provider.class);
    when(environmentProviderFactoryA.createEnvironmentFactory(
            any(), any(), any(), any(), any(), any()))
        .thenReturn(envFactoryA);
    when(environmentProviderFactoryA.getServerFactory()).thenReturn(serverFactory);

    Map<String, Provider> environmentFactoryProviderMap =
        ImmutableMap.of(environmentA.getUrn(), environmentProviderFactoryA);

    PortablePipelineOptions portableOptions =
        PipelineOptionsFactory.as(PortablePipelineOptions.class);
    portableOptions.setSdkWorkerParallelism(2);
    Struct pipelineOptions = PipelineOptionsTranslation.toProto(portableOptions);

    try (DefaultJobBundleFactory bundleFactory =
        new DefaultJobBundleFactory(
            JobInfo.create("testJob", "testJob", "token", pipelineOptions),
            environmentFactoryProviderMap,
            stageIdGenerator,
            serverInfo)) {
      bundleFactory.forStage(getExecutableStage(environmentA));
      verify(environmentProviderFactoryA, Mockito.times(1))
          .createEnvironmentFactory(any(), any(), any(), any(), any(), any());
      verify(envFactoryA, Mockito.times(1)).createEnvironment(eq(environmentA), any());

      bundleFactory.forStage(getExecutableStage(environmentA));
      verify(environmentProviderFactoryA, Mockito.times(2))
          .createEnvironmentFactory(any(), any(), any(), any(), any(), any());
      verify(envFactoryA, Mockito.times(2)).createEnvironment(eq(environmentA), any());

      // round robin, no new environment created
      bundleFactory.forStage(getExecutableStage(environmentA));
      verify(environmentProviderFactoryA, Mockito.times(2))
          .createEnvironmentFactory(any(), any(), any(), any(), any(), any());
      verify(envFactoryA, Mockito.times(2)).createEnvironment(eq(environmentA), any());
    }

    portableOptions.setSdkWorkerParallelism(0);
    pipelineOptions = PipelineOptionsTranslation.toProto(portableOptions);
    Mockito.reset(envFactoryA);
    when(envFactoryA.createEnvironment(eq(environmentA), any())).thenReturn(remoteEnvironment);
    int expectedParallelism = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);
    try (DefaultJobBundleFactory bundleFactory =
        new DefaultJobBundleFactory(
            JobInfo.create("testJob", "testJob", "token", pipelineOptions),
            environmentFactoryProviderMap,
            stageIdGenerator,
            serverInfo)) {
      HashSet<StageBundleFactory> stageBundleFactorySet = new HashSet<>();
      // more factories than parallelism for round-robin
      int numStageBundleFactories = expectedParallelism + 5;
      for (int i = 0; i < numStageBundleFactories; i++) {
        stageBundleFactorySet.add(bundleFactory.forStage(getExecutableStage(environmentA)));
      }
      verify(envFactoryA, Mockito.times(expectedParallelism))
          .createEnvironment(eq(environmentA), any());
      Assert.assertEquals(numStageBundleFactories, stageBundleFactorySet.size());
    }
  }

  @Test
  public void creatingMultipleEnvironmentFromMultipleTypes() throws Exception {
    ServerFactory serverFactory = ServerFactory.createDefault();

    Environment environmentA = Environment.newBuilder().setUrn("env:urn:a").build();
    EnvironmentFactory envFactoryA = mock(EnvironmentFactory.class);
    when(envFactoryA.createEnvironment(eq(environmentA), any())).thenReturn(remoteEnvironment);
    EnvironmentFactory.Provider environmentProviderFactoryA =
        mock(EnvironmentFactory.Provider.class);
    when(environmentProviderFactoryA.createEnvironmentFactory(
            any(), any(), any(), any(), any(), any()))
        .thenReturn(envFactoryA);
    when(environmentProviderFactoryA.getServerFactory()).thenReturn(serverFactory);

    Environment environmentB = Environment.newBuilder().setUrn("env:urn:b").build();
    EnvironmentFactory envFactoryB = mock(EnvironmentFactory.class);
    when(envFactoryB.createEnvironment(eq(environmentB), any())).thenReturn(remoteEnvironment);
    EnvironmentFactory.Provider environmentProviderFactoryB =
        mock(EnvironmentFactory.Provider.class);
    when(environmentProviderFactoryB.createEnvironmentFactory(
            any(), any(), any(), any(), any(), any()))
        .thenReturn(envFactoryB);
    when(environmentProviderFactoryB.getServerFactory()).thenReturn(serverFactory);

    Map<String, Provider> environmentFactoryProviderMap =
        ImmutableMap.of(
            environmentA.getUrn(), environmentProviderFactoryA,
            environmentB.getUrn(), environmentProviderFactoryB);
    try (DefaultJobBundleFactory bundleFactory =
        DefaultJobBundleFactory.create(
            JobInfo.create("testJob", "testJob", "token", Struct.getDefaultInstance()),
            environmentFactoryProviderMap)) {
      bundleFactory.forStage(getExecutableStage(environmentB));
      bundleFactory.forStage(getExecutableStage(environmentA));
    }
    verify(envFactoryA).createEnvironment(eq(environmentA), any());
    verify(envFactoryB).createEnvironment(eq(environmentB), any());
  }

  @Test
  public void expiresEnvironment() throws Exception {
    ServerFactory serverFactory = ServerFactory.createDefault();

    Environment environmentA = Environment.newBuilder().setUrn("env:urn:a").build();
    EnvironmentFactory envFactoryA = mock(EnvironmentFactory.class);
    when(envFactoryA.createEnvironment(eq(environmentA), any())).thenReturn(remoteEnvironment);
    EnvironmentFactory.Provider environmentProviderFactoryA =
        mock(EnvironmentFactory.Provider.class);
    when(environmentProviderFactoryA.createEnvironmentFactory(
            any(), any(), any(), any(), any(), any()))
        .thenReturn(envFactoryA);
    when(environmentProviderFactoryA.getServerFactory()).thenReturn(serverFactory);

    Map<String, Provider> environmentFactoryProviderMap =
        ImmutableMap.of(environmentA.getUrn(), environmentProviderFactoryA);

    PortablePipelineOptions portableOptions =
        PipelineOptionsFactory.as(PortablePipelineOptions.class);
    portableOptions.setEnvironmentExpirationMillis(1);
    Struct pipelineOptions = PipelineOptionsTranslation.toProto(portableOptions);

    try (DefaultJobBundleFactory bundleFactory =
        new DefaultJobBundleFactory(
            JobInfo.create("testJob", "testJob", "token", pipelineOptions),
            environmentFactoryProviderMap,
            stageIdGenerator,
            serverInfo)) {
      OutputReceiverFactory orf = mock(OutputReceiverFactory.class);
      StateRequestHandler srh = mock(StateRequestHandler.class);
      when(srh.getCacheTokens()).thenReturn(Collections.emptyList());
      StageBundleFactory sbf = bundleFactory.forStage(getExecutableStage(environmentA));
      Thread.sleep(10); // allow environment to expire
      sbf.getBundle(orf, srh, BundleProgressHandler.ignored()).close();
      Thread.sleep(10); // allow environment to expire
      sbf.getBundle(orf, srh, BundleProgressHandler.ignored()).close();
    }
    verify(envFactoryA, Mockito.times(3)).createEnvironment(eq(environmentA), any());
    verify(remoteEnvironment, Mockito.times(3)).close();
  }

  @Test
  public void closesEnvironmentOnCleanup() throws Exception {
    try (DefaultJobBundleFactory bundleFactory =
        createDefaultJobBundleFactory(envFactoryProviderMap)) {
      bundleFactory.forStage(getExecutableStage(environment));
    }
    verify(remoteEnvironment).close();
  }

  @Test
  public void closesFnServices() throws Exception {
    InOrder inOrder =
        Mockito.inOrder(
            loggingServer,
            controlServer,
            dataServer,
            stateServer,
            provisioningServer,
            remoteEnvironment);

    try (DefaultJobBundleFactory bundleFactory =
        createDefaultJobBundleFactory(envFactoryProviderMap)) {
      bundleFactory.forStage(getExecutableStage(environment));
    }

    // Close logging service first to avoid spaming the logs
    inOrder.verify(loggingServer).close();
    inOrder.verify(controlServer).close();
    inOrder.verify(dataServer).close();
    inOrder.verify(stateServer).close();
    inOrder.verify(provisioningServer).close();
    inOrder.verify(remoteEnvironment).close();
  }

  @Test
  public void cachesEnvironment() throws Exception {
    try (DefaultJobBundleFactory bundleFactory =
        createDefaultJobBundleFactory(envFactoryProviderMap)) {
      StageBundleFactory bf1 = bundleFactory.forStage(getExecutableStage(environment));
      StageBundleFactory bf2 = bundleFactory.forStage(getExecutableStage(environment));
      // NOTE: We hang on to stage bundle references to ensure their underlying environments are not
      // garbage collected. For additional safety, we print the factories to ensure the references
      // are not optimized away.
      System.out.println("bundle factory 1:" + bf1);
      System.out.println("bundle factory 1:" + bf2);
      verify(envFactory).createEnvironment(eq(environment), any());
      verifyNoMoreInteractions(envFactory);
    }
  }

  @Test
  public void doesNotCacheDifferentEnvironments() throws Exception {
    Environment envFoo = Environment.newBuilder().setUrn("dummy:urn:another").build();
    RemoteEnvironment remoteEnvFoo = mock(RemoteEnvironment.class);
    InstructionRequestHandler fooInstructionHandler = mock(InstructionRequestHandler.class);
    Map<String, EnvironmentFactory.Provider> envFactoryProviderMapFoo =
        ImmutableMap.of(
            environment.getUrn(), envFactoryProvider, envFoo.getUrn(), envFactoryProvider);
    when(envFactory.createEnvironment(eq(envFoo), any())).thenReturn(remoteEnvFoo);
    when(remoteEnvFoo.getInstructionRequestHandler()).thenReturn(fooInstructionHandler);
    // Don't bother creating a distinct instruction response because we don't use it here.
    when(fooInstructionHandler.handle(any()))
        .thenReturn(CompletableFuture.completedFuture(instructionResponse));

    try (DefaultJobBundleFactory bundleFactory =
        createDefaultJobBundleFactory(envFactoryProviderMapFoo)) {
      bundleFactory.forStage(getExecutableStage(environment));
      bundleFactory.forStage(getExecutableStage(envFoo));
      verify(envFactory).createEnvironment(eq(environment), any());
      verify(envFactory).createEnvironment(eq(envFoo), any());
      verifyNoMoreInteractions(envFactory);
    }
  }

  @Test
  public void loadBalancesBundles() throws Exception {
    PortablePipelineOptions portableOptions =
        PipelineOptionsFactory.as(PortablePipelineOptions.class);
    portableOptions.setSdkWorkerParallelism(2);
    portableOptions.setLoadBalanceBundles(true);
    Struct pipelineOptions = PipelineOptionsTranslation.toProto(portableOptions);

    try (DefaultJobBundleFactory bundleFactory =
        new DefaultJobBundleFactory(
            JobInfo.create("testJob", "testJob", "token", pipelineOptions),
            envFactoryProviderMap,
            stageIdGenerator,
            serverInfo)) {
      OutputReceiverFactory orf = mock(OutputReceiverFactory.class);
      StateRequestHandler srh = mock(StateRequestHandler.class);
      when(srh.getCacheTokens()).thenReturn(Collections.emptyList());
      StageBundleFactory sbf = bundleFactory.forStage(getExecutableStage(environment));
      RemoteBundle b1 = sbf.getBundle(orf, srh, BundleProgressHandler.ignored());
      verify(envFactory, Mockito.times(1)).createEnvironment(eq(environment), any());
      final RemoteBundle b2 = sbf.getBundle(orf, srh, BundleProgressHandler.ignored());
      verify(envFactory, Mockito.times(2)).createEnvironment(eq(environment), any());

      long tms = System.currentTimeMillis();
      AtomicBoolean closed = new AtomicBoolean();
      // close to free up environment for another bundle
      TimerTask closeBundleTask =
          new TimerTask() {
            @Override
            public void run() {
              try {
                b2.close();
                closed.set(true);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            }
          };
      new Timer().schedule(closeBundleTask, 100);

      RemoteBundle b3 = sbf.getBundle(orf, srh, BundleProgressHandler.ignored());

      // ensure we waited for close
      Assert.assertThat(System.currentTimeMillis() - tms, greaterThanOrEqualTo(100L));
      Assert.assertThat(closed.get(), is(true));

      verify(envFactory, Mockito.times(2)).createEnvironment(eq(environment), any());
      b3.close();
      b1.close();
    }
  }

  @Test
  public void rejectsStateCachingWithLoadBalancing() throws Exception {
    PortablePipelineOptions portableOptions =
        PipelineOptionsFactory.as(PortablePipelineOptions.class);
    portableOptions.setLoadBalanceBundles(true);
    ExperimentalOptions options = portableOptions.as(ExperimentalOptions.class);
    ExperimentalOptions.addExperiment(options, "state_cache_size=1");
    Struct pipelineOptions = PipelineOptionsTranslation.toProto(options);

    Exception e =
        Assert.assertThrows(
            IllegalArgumentException.class,
            () ->
                new DefaultJobBundleFactory(
                        JobInfo.create("testJob", "testJob", "token", pipelineOptions),
                        envFactoryProviderMap,
                        stageIdGenerator,
                        serverInfo)
                    .close());
    Assert.assertThat(e.getMessage(), containsString("state_cache_size"));
  }

  private DefaultJobBundleFactory createDefaultJobBundleFactory(
      Map<String, EnvironmentFactory.Provider> envFactoryProviderMap) {
    return new DefaultJobBundleFactory(
        JobInfo.create("testJob", "testJob", "token", Struct.getDefaultInstance()),
        envFactoryProviderMap,
        stageIdGenerator,
        serverInfo);
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
                                FunctionSpec.newBuilder()
                                    .setUrn(ModelCoders.INTERVAL_WINDOW_CODER_URN)
                                    .build())
                            .build())
                    .build())
            .build());
  }
}
