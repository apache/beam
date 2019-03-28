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
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Target;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors.ExecutableProcessBundleDescriptor;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.environment.EnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.RemoteEnvironment;
import org.apache.beam.runners.fnexecution.state.GrpcStateService;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables;

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
                return environmentFactory.createEnvironment(env);
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
    return new BundleProcessorStageBundleFactory(descriptor, bundleProcessor);
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
    private final SdkHarnessClient.BundleProcessor processor;

    private BundleProcessorStageBundleFactory(
        ExecutableProcessBundleDescriptor descriptor, SdkHarnessClient.BundleProcessor processor) {
      this.descriptor = descriptor;
      this.processor = processor;
    }

    @Override
    public RemoteBundle getBundle(
        OutputReceiverFactory outputReceiverFactory,
        StateRequestHandler stateRequestHandler,
        BundleProgressHandler progressHandler) {
      Map<Target, RemoteOutputReceiver<?>> outputReceivers = new HashMap<>();
      for (Map.Entry<Target, Coder<WindowedValue<?>>> targetCoders :
          descriptor.getOutputTargetCoders().entrySet()) {
        String bundleOutputPCollection =
            Iterables.getOnlyElement(
                descriptor
                    .getProcessBundleDescriptor()
                    .getTransformsOrThrow(targetCoders.getKey().getPrimitiveTransformReference())
                    .getInputsMap()
                    .values());
        FnDataReceiver<WindowedValue<?>> outputReceiver =
            outputReceiverFactory.create(bundleOutputPCollection);
        outputReceivers.put(
            targetCoders.getKey(),
            RemoteOutputReceiver.of(targetCoders.getValue(), outputReceiver));
      }
      return processor.newBundle(outputReceivers, stateRequestHandler, progressHandler);
    }

    @Override
    public ExecutableProcessBundleDescriptor getProcessBundleDescriptor() {
      return descriptor;
    }

    @Override
    public void close() {}
  }
}
