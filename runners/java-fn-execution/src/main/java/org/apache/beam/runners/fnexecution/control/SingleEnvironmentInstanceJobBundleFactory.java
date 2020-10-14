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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.core.construction.Timer;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors.ExecutableProcessBundleDescriptor;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors.TimerSpec;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.environment.EnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.RemoteEnvironment;
import org.apache.beam.runners.fnexecution.state.GrpcStateService;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

/**
 * A {@link JobBundleFactory} which can manage a single instance of an {@link Environment}.
 *
 * @deprecated replace with a {@link DefaultJobBundleFactory} when appropriate if the {@link
 *     EnvironmentFactory} is a {@link
 *     org.apache.beam.runners.fnexecution.environment.DockerEnvironmentFactory}, or create an
 *     {@code InProcessJobBundleFactory} and inline the creation of the environment if appropriate.
 */
@Deprecated
public class SingleEnvironmentInstanceJobBundleFactory implements JobBundleFactory {
  public static JobBundleFactory create(
      EnvironmentFactory environmentFactory,
      GrpcFnServer<GrpcDataService> data,
      GrpcFnServer<GrpcStateService> state,
      IdGenerator idGenerator) {
    return new SingleEnvironmentInstanceJobBundleFactory(
        environmentFactory, data, state, idGenerator);
  }

  private final EnvironmentFactory environmentFactory;

  private final GrpcFnServer<GrpcDataService> dataService;
  private final GrpcFnServer<GrpcStateService> stateService;

  private final ConcurrentMap<ExecutableStage, StageBundleFactory> stageBundleFactories =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<Environment, RemoteEnvironment> environments =
      new ConcurrentHashMap<>();

  private final IdGenerator idGenerator;

  private SingleEnvironmentInstanceJobBundleFactory(
      EnvironmentFactory environmentFactory,
      GrpcFnServer<GrpcDataService> dataService,
      GrpcFnServer<GrpcStateService> stateService,
      IdGenerator idGenerator) {
    this.environmentFactory = environmentFactory;
    this.dataService = dataService;
    this.stateService = stateService;
    this.idGenerator = idGenerator;
  }

  @Override
  public StageBundleFactory forStage(ExecutableStage executableStage) {
    return stageBundleFactories.computeIfAbsent(executableStage, this::createBundleFactory);
  }

  private StageBundleFactory createBundleFactory(ExecutableStage stage) {
    RemoteEnvironment remoteEnv =
        environments.computeIfAbsent(
            stage.getEnvironment(),
            env -> {
              try {
                return environmentFactory.createEnvironment(env, idGenerator.getId());
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    SdkHarnessClient sdkHarnessClient =
        SdkHarnessClient.usingFnApiClient(
                remoteEnv.getInstructionRequestHandler(), dataService.getService())
            .withIdGenerator(idGenerator);
    ExecutableProcessBundleDescriptor descriptor;
    try {
      descriptor =
          ProcessBundleDescriptors.fromExecutableStage(
              idGenerator.getId(),
              stage,
              dataService.getApiServiceDescriptor(),
              stateService.getApiServiceDescriptor());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    SdkHarnessClient.BundleProcessor bundleProcessor =
        sdkHarnessClient.getProcessor(
            descriptor.getProcessBundleDescriptor(),
            descriptor.getRemoteInputDestinations(),
            stateService.getService());
    return new BundleProcessorStageBundleFactory(descriptor, bundleProcessor, sdkHarnessClient);
  }

  @Override
  public void close() throws Exception {
    Exception thrown = null;
    for (RemoteEnvironment remoteEnvironment : environments.values()) {
      try {
        remoteEnvironment.close();
      } catch (Exception e) {
        if (thrown == null) {
          thrown = e;
        } else {
          thrown.addSuppressed(e);
        }
      }
    }
    if (thrown != null) {
      throw thrown;
    }
  }

  private static class BundleProcessorStageBundleFactory implements StageBundleFactory {
    private final ExecutableProcessBundleDescriptor descriptor;
    private final SdkHarnessClient client;
    private final SdkHarnessClient.BundleProcessor processor;

    private BundleProcessorStageBundleFactory(
        ExecutableProcessBundleDescriptor descriptor,
        SdkHarnessClient.BundleProcessor processor,
        SdkHarnessClient client) {
      this.descriptor = descriptor;
      this.processor = processor;
      this.client = client;
    }

    @Override
    public RemoteBundle getBundle(
        OutputReceiverFactory outputReceiverFactory,
        TimerReceiverFactory timerReceiverFactory,
        StateRequestHandler stateRequestHandler,
        BundleProgressHandler progressHandler,
        BundleFinalizationHandler finalizationHandler) {
      Map<String, RemoteOutputReceiver<?>> outputReceivers = new HashMap<>();
      for (Map.Entry<String, Coder> remoteOutputCoder :
          descriptor.getRemoteOutputCoders().entrySet()) {
        String bundleOutputPCollection =
            Iterables.getOnlyElement(
                descriptor
                    .getProcessBundleDescriptor()
                    .getTransformsOrThrow(remoteOutputCoder.getKey())
                    .getInputsMap()
                    .values());
        FnDataReceiver<?> outputReceiver = outputReceiverFactory.create(bundleOutputPCollection);
        outputReceivers.put(
            remoteOutputCoder.getKey(),
            RemoteOutputReceiver.of(remoteOutputCoder.getValue(), outputReceiver));
      }
      Map<KV<String, String>, RemoteOutputReceiver<Timer<?>>> timerReceivers = new HashMap<>();
      for (Map.Entry<String, Map<String, TimerSpec>> transformTimerSpecs :
          descriptor.getTimerSpecs().entrySet()) {
        for (TimerSpec timerSpec : transformTimerSpecs.getValue().values()) {
          FnDataReceiver<Timer<?>> receiver =
              (FnDataReceiver)
                  timerReceiverFactory.create(timerSpec.transformId(), timerSpec.timerId());
          timerReceivers.put(
              KV.of(timerSpec.transformId(), timerSpec.timerId()),
              RemoteOutputReceiver.of(timerSpec.coder(), receiver));
        }
      }
      return processor.newBundle(
          outputReceivers,
          timerReceivers,
          stateRequestHandler,
          progressHandler,
          finalizationHandler);
    }

    @Override
    public ExecutableProcessBundleDescriptor getProcessBundleDescriptor() {
      return descriptor;
    }

    @Override
    public InstructionRequestHandler getInstructionRequestHandler() {
      return client.getInstructionRequestHandler();
    }

    @Override
    public void close() {}
  }
}
