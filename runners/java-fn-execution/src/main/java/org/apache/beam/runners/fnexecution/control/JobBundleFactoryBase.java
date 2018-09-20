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
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.util.WindowedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A base for a {@link JobBundleFactory} for which the implementation can specify a custom {@link
 * EnvironmentFactory} for environment management. Note that returned {@link StageBundleFactory
 * stage bundle factories} are not thread-safe. Instead, a new stage factory should be created for
 * each client.
 */
@ThreadSafe
public abstract class JobBundleFactoryBase implements JobBundleFactory {
  private static final Logger LOG = LoggerFactory.getLogger(JobBundleFactoryBase.class);

  private final IdGenerator stageIdGenerator;
  private final GrpcFnServer<FnApiControlClientPoolService> controlServer;
  private final GrpcFnServer<GrpcLoggingService> loggingServer;
  private final GrpcFnServer<ArtifactRetrievalService> retrievalServer;
  private final GrpcFnServer<StaticGrpcProvisionService> provisioningServer;

  private final LoadingCache<Environment, WrappedSdkHarnessClient> environmentCache;

  JobBundleFactoryBase(JobInfo jobInfo) throws Exception {
    ServerFactory serverFactory = getServerFactory();
    IdGenerator stageIdGenerator = IdGenerators.incrementingLongs();
    ControlClientPool clientPool = MapControlClientPool.create();

    GrpcFnServer<FnApiControlClientPoolService> controlServer =
        GrpcFnServer.allocatePortAndCreateFor(
            FnApiControlClientPoolService.offeringClientsToPool(
                clientPool.getSink(), GrpcContextHeaderAccessorProvider.getHeaderAccessor()),
            serverFactory);
    GrpcFnServer<GrpcLoggingService> loggingServer =
        GrpcFnServer.allocatePortAndCreateFor(
            GrpcLoggingService.forWriter(Slf4jLogWriter.getDefault()), serverFactory);
    GrpcFnServer<ArtifactRetrievalService> retrievalServer =
        GrpcFnServer.allocatePortAndCreateFor(
            BeamFileSystemArtifactRetrievalService.create(), serverFactory);
    GrpcFnServer<StaticGrpcProvisionService> provisioningServer =
        GrpcFnServer.allocatePortAndCreateFor(
            StaticGrpcProvisionService.create(jobInfo.toProvisionInfo()), serverFactory);
    EnvironmentFactory environmentFactory =
        getEnvironmentFactory(
            controlServer,
            loggingServer,
            retrievalServer,
            provisioningServer,
            clientPool.getSource(),
            IdGenerators.incrementingLongs());
    this.stageIdGenerator = stageIdGenerator;
    this.controlServer = controlServer;
    this.loggingServer = loggingServer;
    this.retrievalServer = retrievalServer;
    this.provisioningServer = provisioningServer;
    this.environmentCache = createEnvironmentCache(environmentFactory, serverFactory);
  }

  @VisibleForTesting
  JobBundleFactoryBase(
      EnvironmentFactory environmentFactory,
      ServerFactory serverFactory,
      IdGenerator stageIdGenerator,
      GrpcFnServer<FnApiControlClientPoolService> controlServer,
      GrpcFnServer<GrpcLoggingService> loggingServer,
      GrpcFnServer<ArtifactRetrievalService> retrievalServer,
      GrpcFnServer<StaticGrpcProvisionService> provisioningServer) {
    this.stageIdGenerator = stageIdGenerator;
    this.controlServer = controlServer;
    this.loggingServer = loggingServer;
    this.retrievalServer = retrievalServer;
    this.provisioningServer = provisioningServer;
    this.environmentCache = createEnvironmentCache(environmentFactory, serverFactory);
  }

  /** Create {@link EnvironmentFactory} for the given services. */
  abstract EnvironmentFactory getEnvironmentFactory(
      GrpcFnServer<FnApiControlClientPoolService> controlServiceServer,
      GrpcFnServer<GrpcLoggingService> loggingServiceServer,
      GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer,
      GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
      ControlClientPool.Source clientSource,
      IdGenerator idGenerator);

  private LoadingCache<Environment, WrappedSdkHarnessClient> createEnvironmentCache(
      EnvironmentFactory environmentFactory, ServerFactory serverFactory) {
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
                RemoteEnvironment remoteEnvironment =
                    environmentFactory.createEnvironment(environment);
                return WrappedSdkHarnessClient.wrapping(remoteEnvironment, serverFactory);
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
              wrappedClient.getDataServer().getApiServiceDescriptor(),
              wrappedClient.getStateServer().getApiServiceDescriptor());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return SimpleStageBundleFactory.create(wrappedClient, processBundleDescriptor);
  }

  @Override
  public void close() throws Exception {
    // Clear the cache. This closes all active environments.
    environmentCache.invalidateAll();
    environmentCache.cleanUp();

    // Tear down common servers.
    controlServer.close();
    loggingServer.close();
    retrievalServer.close();
    provisioningServer.close();
  }

  protected ServerFactory getServerFactory() {
    return ServerFactory.createDefault();
  }

  /** A simple stage bundle factory for remotely processing bundles. */
  protected static class SimpleStageBundleFactory implements StageBundleFactory {

    private final BundleProcessor processor;
    private final ExecutableProcessBundleDescriptor processBundleDescriptor;

    // Store the wrapped client in order to keep a live reference into the cache.
    @SuppressFBWarnings private WrappedSdkHarnessClient wrappedClient;

    static SimpleStageBundleFactory create(
        WrappedSdkHarnessClient wrappedClient,
        ExecutableProcessBundleDescriptor processBundleDescriptor) {
      @SuppressWarnings("unchecked")
      BundleProcessor processor =
          wrappedClient
              .getClient()
              .getProcessor(
                  processBundleDescriptor.getProcessBundleDescriptor(),
                  processBundleDescriptor.getRemoteInputDestinations(),
                  wrappedClient.getStateServer().getService());
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
      return processor.newBundle(outputReceivers.build(), stateRequestHandler, progressHandler);
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
    private final ExecutorService executor;
    // TODO: How should data server lifetime be scoped? It is necessary here for now because
    // SdkHarnessClient requires one at construction.
    private final GrpcFnServer<GrpcDataService> dataServer;
    private final GrpcFnServer<GrpcStateService> stateServer;
    private final SdkHarnessClient client;

    static WrappedSdkHarnessClient wrapping(
        RemoteEnvironment environment, ServerFactory serverFactory) throws Exception {
      ExecutorService executor = Executors.newCachedThreadPool();
      GrpcFnServer<GrpcDataService> dataServer =
          GrpcFnServer.allocatePortAndCreateFor(
              GrpcDataService.create(executor, OutboundObserverFactory.serverDirect()),
              serverFactory);
      GrpcFnServer<GrpcStateService> stateServer =
          GrpcFnServer.allocatePortAndCreateFor(GrpcStateService.create(), serverFactory);
      SdkHarnessClient client =
          SdkHarnessClient.usingFnApiClient(
              environment.getInstructionRequestHandler(), dataServer.getService());
      return new WrappedSdkHarnessClient(environment, executor, dataServer, stateServer, client);
    }

    private WrappedSdkHarnessClient(
        RemoteEnvironment environment,
        ExecutorService executor,
        GrpcFnServer<GrpcDataService> dataServer,
        GrpcFnServer<GrpcStateService> stateServer,
        SdkHarnessClient client) {
      this.executor = executor;
      this.environment = environment;
      this.dataServer = dataServer;
      this.stateServer = stateServer;
      this.client = client;
    }

    SdkHarnessClient getClient() {
      return client;
    }

    GrpcFnServer<GrpcStateService> getStateServer() {
      return stateServer;
    }

    GrpcFnServer<GrpcDataService> getDataServer() {
      return dataServer;
    }

    @Override
    public void close() throws Exception {
      try (AutoCloseable stateServerCloser = stateServer;
          AutoCloseable dataServerCloser = dataServer;
          AutoCloseable envCloser = environment;
          AutoCloseable executorCloser = executor::shutdown) {
        // Wrap resources in try-with-resources to ensure all are cleaned up.
      }
      // TODO: Wait for executor shutdown?
    }
  }
}
