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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Target;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.GrpcContextHeaderAccessorProvider;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.artifact.BeamFileSystemArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors.ExecutableProcessBundleDescriptor;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient.BundleProcessor;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.environment.EnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.RemoteEnvironment;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.logging.Slf4jLogWriter;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.provisioning.StaticGrpcProvisionService;
import org.apache.beam.runners.fnexecution.state.GrpcStateService;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.function.ThrowingFunction;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.util.WindowedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link JobBundleFactory} for which the implementation can specify a custom {@link
 * EnvironmentFactory} for environment management. Note that returned {@link StageBundleFactory
 * stage bundle factories} are not thread-safe. Instead, a new stage factory should be created for
 * each client. {@link DefaultJobBundleFactory} initializes the Environment lazily when the forStage
 * is called for a stage. This factory is not capable of handling a mixed types of environment.
 */
@ThreadSafe
public class DefaultJobBundleFactory implements JobBundleFactory {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultJobBundleFactory.class);

  private final IdGenerator stageIdGenerator;
  private final LoadingCache<Environment, WrappedSdkHarnessClient> environmentCache;
  // Using environment as the initialization marker.
  private Environment environment;
  private ExecutorService executor;
  private GrpcFnServer<FnApiControlClientPoolService> controlServer;
  private GrpcFnServer<GrpcLoggingService> loggingServer;
  private GrpcFnServer<ArtifactRetrievalService> retrievalServer;
  private GrpcFnServer<StaticGrpcProvisionService> provisioningServer;
  private GrpcFnServer<GrpcDataService> dataServer;
  private GrpcFnServer<GrpcStateService> stateServer;
  private MapControlClientPool clientPool;
  private EnvironmentFactory environmentFactory;

  public static DefaultJobBundleFactory create(
      JobInfo jobInfo, Map<String, EnvironmentFactory.Provider> environmentFactoryProviderMap) {
    return new DefaultJobBundleFactory(jobInfo, environmentFactoryProviderMap);
  }

  DefaultJobBundleFactory(
      JobInfo jobInfo, Map<String, EnvironmentFactory.Provider> environmentFactoryMap) {
    IdGenerator stageIdGenerator = IdGenerators.incrementingLongs();
    this.stageIdGenerator = stageIdGenerator;
    this.environmentCache =
        createEnvironmentCache(
            (environment) -> {
              synchronized (this) {
                checkAndInitialize(jobInfo, environmentFactoryMap, environment);
              }
              return environmentFactory.createEnvironment(environment);
            });
  }

  @VisibleForTesting
  DefaultJobBundleFactory(
      EnvironmentFactory environmentFactory,
      IdGenerator stageIdGenerator,
      GrpcFnServer<FnApiControlClientPoolService> controlServer,
      GrpcFnServer<GrpcLoggingService> loggingServer,
      GrpcFnServer<ArtifactRetrievalService> retrievalServer,
      GrpcFnServer<StaticGrpcProvisionService> provisioningServer,
      GrpcFnServer<GrpcDataService> dataServer,
      GrpcFnServer<GrpcStateService> stateServer)
      throws Exception {
    this.executor = Executors.newCachedThreadPool();
    this.stageIdGenerator = stageIdGenerator;
    this.controlServer = controlServer;
    this.loggingServer = loggingServer;
    this.retrievalServer = retrievalServer;
    this.provisioningServer = provisioningServer;
    this.dataServer = dataServer;
    this.stateServer = stateServer;
    this.environmentCache =
        createEnvironmentCache((env) -> environmentFactory.createEnvironment(env));
  }

  private LoadingCache<Environment, WrappedSdkHarnessClient> createEnvironmentCache(
      ThrowingFunction<Environment, RemoteEnvironment> environmentCreator) {
    return CacheBuilder.newBuilder()
        .removalListener(
            ((RemovalNotification<Environment, WrappedSdkHarnessClient> notification) -> {
              LOG.debug("Cleaning up for environment {}", notification.getKey().getUrn());
              try {
                notification.getValue().close();
              } catch (Exception e) {
                LOG.warn(
                    String.format("Error cleaning up environment %s", notification.getKey()), e);
              }
            }))
        .build(
            new CacheLoader<Environment, WrappedSdkHarnessClient>() {
              @Override
              public WrappedSdkHarnessClient load(Environment environment) throws Exception {
                return WrappedSdkHarnessClient.wrapping(
                    environmentCreator.apply(environment), dataServer);
              }
            });
  }

  @Override
  public StageBundleFactory forStage(ExecutableStage executableStage) {
    WrappedSdkHarnessClient wrappedClient =
        environmentCache.getUnchecked(executableStage.getEnvironment());
    ExecutableProcessBundleDescriptor processBundleDescriptor;
    try {
      processBundleDescriptor =
          ProcessBundleDescriptors.fromExecutableStage(
              stageIdGenerator.getId(),
              executableStage,
              dataServer.getApiServiceDescriptor(),
              stateServer.getApiServiceDescriptor());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return SimpleStageBundleFactory.create(wrappedClient, processBundleDescriptor, stateServer);
  }

  @Override
  public void close() throws Exception {
    // Clear the cache. This closes all active environments.
    // note this may cause open calls to be cancelled by the peer
    environmentCache.invalidateAll();
    environmentCache.cleanUp();

    // Tear down common servers.
    stateServer.close();
    dataServer.close();
    controlServer.close();
    loggingServer.close();
    retrievalServer.close();
    provisioningServer.close();

    executor.shutdown();
  }

  /** A simple stage bundle factory for remotely processing bundles. */
  protected static class SimpleStageBundleFactory implements StageBundleFactory {

    private final BundleProcessor processor;
    private final ExecutableProcessBundleDescriptor processBundleDescriptor;

    // Store the wrapped client in order to keep a live reference into the cache.
    @SuppressFBWarnings private WrappedSdkHarnessClient wrappedClient;

    static SimpleStageBundleFactory create(
        WrappedSdkHarnessClient wrappedClient,
        ExecutableProcessBundleDescriptor processBundleDescriptor,
        GrpcFnServer<GrpcStateService> stateServer) {
      @SuppressWarnings("unchecked")
      BundleProcessor processor =
          wrappedClient
              .getClient()
              .getProcessor(
                  processBundleDescriptor.getProcessBundleDescriptor(),
                  processBundleDescriptor.getRemoteInputDestinations(),
                  stateServer.getService());
      return new SimpleStageBundleFactory(processBundleDescriptor, processor, wrappedClient);
    }

    SimpleStageBundleFactory(
        ExecutableProcessBundleDescriptor processBundleDescriptor,
        BundleProcessor processor,
        WrappedSdkHarnessClient wrappedClient) {
      this.processBundleDescriptor = processBundleDescriptor;
      this.processor = processor;
      this.wrappedClient = wrappedClient;
    }

    @Override
    public RemoteBundle getBundle(
        OutputReceiverFactory outputReceiverFactory,
        StateRequestHandler stateRequestHandler,
        BundleProgressHandler progressHandler)
        throws Exception {
      // TODO: Consider having BundleProcessor#newBundle take in an OutputReceiverFactory rather
      // than constructing the receiver map here. Every bundle factory will need this.
      ImmutableMap.Builder<Target, RemoteOutputReceiver<?>> outputReceivers =
          ImmutableMap.builder();
      for (Map.Entry<Target, Coder<WindowedValue<?>>> targetCoder :
          processBundleDescriptor.getOutputTargetCoders().entrySet()) {
        Target target = targetCoder.getKey();
        Coder<WindowedValue<?>> coder = targetCoder.getValue();
        String bundleOutputPCollection =
            Iterables.getOnlyElement(
                processBundleDescriptor
                    .getProcessBundleDescriptor()
                    .getTransformsOrThrow(target.getPrimitiveTransformReference())
                    .getInputsMap()
                    .values());
        FnDataReceiver<WindowedValue<?>> outputReceiver =
            outputReceiverFactory.create(bundleOutputPCollection);
        outputReceivers.put(target, RemoteOutputReceiver.of(coder, outputReceiver));
      }
      // TODO ajamato this bundle contains the response proto?
      return processor.newBundle(outputReceivers.build(), stateRequestHandler, progressHandler);
    }

    @Override
    public ExecutableProcessBundleDescriptor getProcessBundleDescriptor() {
      return processBundleDescriptor;
    }

    @Override
    public void close() throws Exception {
      // Clear reference to encourage cache eviction. Values are weakly referenced.
      wrappedClient = null;
    }
  }

  /**
   * Holder for an {@link SdkHarnessClient} along with its associated state and data servers. As of
   * now, there is a 1:1 relationship between data services and harness clients. The servers are
   * packaged here to tie server lifetimes to harness client lifetimes.
   */
  protected static class WrappedSdkHarnessClient implements AutoCloseable {

    private final RemoteEnvironment environment;
    private final SdkHarnessClient client;

    static WrappedSdkHarnessClient wrapping(
        RemoteEnvironment environment, GrpcFnServer<GrpcDataService> dataServer) {
      SdkHarnessClient client =
          SdkHarnessClient.usingFnApiClient(
              environment.getInstructionRequestHandler(), dataServer.getService());
      return new WrappedSdkHarnessClient(environment, client);
    }

    private WrappedSdkHarnessClient(RemoteEnvironment environment, SdkHarnessClient client) {
      this.environment = environment;
      this.client = client;
    }

    SdkHarnessClient getClient() {
      return client;
    }

    @Override
    public void close() throws Exception {
      try (AutoCloseable envCloser = environment) {
        // Wrap resources in try-with-resources to ensure all are cleaned up.
      }
      // TODO: Wait for executor shutdown?
    }
  }

  @GuardedBy("this")
  private void checkAndInitialize(
      JobInfo jobInfo,
      Map<String, EnvironmentFactory.Provider> environmentFactoryProviderMap,
      Environment environment)
      throws IOException {
    Preconditions.checkNotNull(environment, "Environment can not be null");
    if (this.environment != null) {
      Preconditions.checkArgument(
          this.environment.getUrn().equals(environment.getUrn()),
          "Unsupported: Mixing environment types (%s, %s) is not supported for a job.",
          this.environment.getUrn(),
          environment.getUrn());
      // Nothing to do. Already initialized.
      return;
    }

    EnvironmentFactory.Provider environmentFactoryProvider =
        environmentFactoryProviderMap.get(environment.getUrn());
    ServerFactory serverFactory = environmentFactoryProvider.getServerFactory();

    this.clientPool = MapControlClientPool.create();
    this.executor = Executors.newCachedThreadPool();
    this.controlServer =
        GrpcFnServer.allocatePortAndCreateFor(
            FnApiControlClientPoolService.offeringClientsToPool(
                clientPool.getSink(), GrpcContextHeaderAccessorProvider.getHeaderAccessor()),
            serverFactory);
    this.loggingServer =
        GrpcFnServer.allocatePortAndCreateFor(
            GrpcLoggingService.forWriter(Slf4jLogWriter.getDefault()), serverFactory);
    this.retrievalServer =
        GrpcFnServer.allocatePortAndCreateFor(
            BeamFileSystemArtifactRetrievalService.create(), serverFactory);
    this.provisioningServer =
        GrpcFnServer.allocatePortAndCreateFor(
            StaticGrpcProvisionService.create(jobInfo.toProvisionInfo()), serverFactory);
    this.dataServer =
        GrpcFnServer.allocatePortAndCreateFor(
            GrpcDataService.create(executor, OutboundObserverFactory.serverDirect()),
            serverFactory);
    this.stateServer =
        GrpcFnServer.allocatePortAndCreateFor(GrpcStateService.create(), serverFactory);

    this.environmentFactory =
        environmentFactoryProvider.createEnvironmentFactory(
            controlServer,
            loggingServer,
            retrievalServer,
            provisioningServer,
            clientPool,
            stageIdGenerator);
    this.environment = environment;
  }
}
