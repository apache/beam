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

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardEnvironments;
import org.apache.beam.runners.core.construction.BeamUrns;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.GrpcContextHeaderAccessorProvider;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.artifact.BeamFileSystemArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors.ExecutableProcessBundleDescriptor;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient.BundleProcessor;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.environment.DockerEnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.EmbeddedEnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.EnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.ExternalEnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.ProcessEnvironmentFactory;
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
import org.apache.beam.sdk.function.ThrowingFunction;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.LoadingCache;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.RemovalNotification;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link JobBundleFactory} for which the implementation can specify a custom {@link
 * EnvironmentFactory} for environment management. Note that returned {@link StageBundleFactory
 * stage bundle factories} are not thread-safe. Instead, a new stage factory should be created for
 * each client. {@link DefaultJobBundleFactory} initializes the Environment lazily when the forStage
 * is called for a stage.
 */
@ThreadSafe
public class DefaultJobBundleFactory implements JobBundleFactory {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultJobBundleFactory.class);
  private static final IdGenerator factoryIdGenerator = IdGenerators.incrementingLongs();

  private final String factoryId = factoryIdGenerator.getId();
  private final LoadingCache<Environment, WrappedSdkHarnessClient> environmentCache;
  private final Map<String, EnvironmentFactory.Provider> environmentFactoryProviderMap;
  private final ExecutorService executor;
  private final MapControlClientPool clientPool;
  private final IdGenerator stageIdGenerator;
  private final int environmentExpirationMillis;

  public static DefaultJobBundleFactory create(JobInfo jobInfo) {
    Map<String, EnvironmentFactory.Provider> environmentFactoryProviderMap =
        ImmutableMap.of(
            BeamUrns.getUrn(StandardEnvironments.Environments.DOCKER),
            new DockerEnvironmentFactory.Provider(
                PipelineOptionsTranslation.fromProto(jobInfo.pipelineOptions())),
            BeamUrns.getUrn(StandardEnvironments.Environments.PROCESS),
            new ProcessEnvironmentFactory.Provider(),
            BeamUrns.getUrn(StandardEnvironments.Environments.EXTERNAL),
            new ExternalEnvironmentFactory.Provider(),
            Environments.ENVIRONMENT_EMBEDDED, // Non Public urn for testing.
            new EmbeddedEnvironmentFactory.Provider(
                PipelineOptionsTranslation.fromProto(jobInfo.pipelineOptions())));
    return new DefaultJobBundleFactory(jobInfo, environmentFactoryProviderMap);
  }

  public static DefaultJobBundleFactory create(
      JobInfo jobInfo, Map<String, EnvironmentFactory.Provider> environmentFactoryProviderMap) {
    return new DefaultJobBundleFactory(jobInfo, environmentFactoryProviderMap);
  }

  DefaultJobBundleFactory(
      JobInfo jobInfo, Map<String, EnvironmentFactory.Provider> environmentFactoryMap) {
    IdGenerator stageIdSuffixGenerator = IdGenerators.incrementingLongs();
    this.environmentFactoryProviderMap = environmentFactoryMap;
    this.executor = Executors.newCachedThreadPool();
    this.clientPool = MapControlClientPool.create();
    this.stageIdGenerator = () -> factoryId + "-" + stageIdSuffixGenerator.getId();
    this.environmentExpirationMillis = getEnvironmentExpirationMillis(jobInfo);
    this.environmentCache =
        createEnvironmentCache(serverFactory -> createServerInfo(jobInfo, serverFactory));
  }

  @VisibleForTesting
  DefaultJobBundleFactory(
      JobInfo jobInfo,
      Map<String, EnvironmentFactory.Provider> environmentFactoryMap,
      IdGenerator stageIdGenerator,
      ServerInfo serverInfo) {
    this.environmentFactoryProviderMap = environmentFactoryMap;
    this.executor = Executors.newCachedThreadPool();
    this.clientPool = MapControlClientPool.create();
    this.stageIdGenerator = stageIdGenerator;
    this.environmentExpirationMillis = getEnvironmentExpirationMillis(jobInfo);
    this.environmentCache = createEnvironmentCache(serverFactory -> serverInfo);
  }

  private static int getEnvironmentExpirationMillis(JobInfo jobInfo) {
    PipelineOptions pipelineOptions =
        PipelineOptionsTranslation.fromProto(jobInfo.pipelineOptions());
    return pipelineOptions.as(PortablePipelineOptions.class).getEnvironmentExpirationMillis();
  }

  private LoadingCache<Environment, WrappedSdkHarnessClient> createEnvironmentCache(
      ThrowingFunction<ServerFactory, ServerInfo> serverInfoCreator) {
    CacheBuilder builder =
        CacheBuilder.newBuilder()
            .removalListener(
                (RemovalNotification<Environment, WrappedSdkHarnessClient> notification) -> {
                  int refCount = notification.getValue().unref();
                  LOG.debug(
                      "Removed environment {} with {} remaining bundle references.",
                      notification.getKey(),
                      refCount);
                });

    if (environmentExpirationMillis > 0) {
      builder = builder.expireAfterWrite(environmentExpirationMillis, TimeUnit.MILLISECONDS);
    }
    return builder.build(
        new CacheLoader<Environment, WrappedSdkHarnessClient>() {
          @Override
          public WrappedSdkHarnessClient load(Environment environment) throws Exception {
            EnvironmentFactory.Provider environmentFactoryProvider =
                environmentFactoryProviderMap.get(environment.getUrn());
            ServerFactory serverFactory = environmentFactoryProvider.getServerFactory();
            ServerInfo serverInfo = serverInfoCreator.apply(serverFactory);
            EnvironmentFactory environmentFactory =
                environmentFactoryProvider.createEnvironmentFactory(
                    serverInfo.getControlServer(),
                    serverInfo.getLoggingServer(),
                    serverInfo.getRetrievalServer(),
                    serverInfo.getProvisioningServer(),
                    clientPool,
                    stageIdGenerator);
            return WrappedSdkHarnessClient.wrapping(
                environmentFactory.createEnvironment(environment), serverInfo);
          }
        });
  }

  @Override
  public StageBundleFactory forStage(ExecutableStage executableStage) {
    return new SimpleStageBundleFactory(executableStage);
  }

  @Override
  public void close() throws Exception {
    // Clear the cache. This closes all active environments.
    // note this may cause open calls to be cancelled by the peer
    environmentCache.invalidateAll();
    environmentCache.cleanUp();

    executor.shutdown();
  }

  /**
   * A {@link StageBundleFactory} for remotely processing bundles that supports environment
   * expiration.
   */
  private class SimpleStageBundleFactory implements StageBundleFactory {

    private final ExecutableStage executableStage;
    private BundleProcessor processor;
    private ExecutableProcessBundleDescriptor processBundleDescriptor;
    private WrappedSdkHarnessClient wrappedClient;

    private SimpleStageBundleFactory(ExecutableStage executableStage) {
      this.executableStage = executableStage;
      prepare(environmentCache.getUnchecked(executableStage.getEnvironment()));
    }

    private void prepare(WrappedSdkHarnessClient wrappedClient) {
      try {
        this.wrappedClient = wrappedClient;
        this.processBundleDescriptor =
            ProcessBundleDescriptors.fromExecutableStage(
                stageIdGenerator.getId(),
                executableStage,
                wrappedClient.getServerInfo().getDataServer().getApiServiceDescriptor(),
                wrappedClient.getServerInfo().getStateServer().getApiServiceDescriptor());
      } catch (IOException e) {
        throw new RuntimeException("Failed to create ProcessBundleDescriptor.", e);
      }

      this.processor =
          wrappedClient
              .getClient()
              .getProcessor(
                  processBundleDescriptor.getProcessBundleDescriptor(),
                  processBundleDescriptor.getRemoteInputDestinations(),
                  wrappedClient.getServerInfo().getStateServer().getService());
    }

    @Override
    public RemoteBundle getBundle(
        OutputReceiverFactory outputReceiverFactory,
        StateRequestHandler stateRequestHandler,
        BundleProgressHandler progressHandler)
        throws Exception {
      // TODO: Consider having BundleProcessor#newBundle take in an OutputReceiverFactory rather
      // than constructing the receiver map here. Every bundle factory will need this.
      ImmutableMap.Builder<String, RemoteOutputReceiver<?>> outputReceivers =
          ImmutableMap.builder();
      for (Map.Entry<String, Coder<WindowedValue<?>>> remoteOutputCoder :
          processBundleDescriptor.getRemoteOutputCoders().entrySet()) {
        String outputTransform = remoteOutputCoder.getKey();
        Coder<WindowedValue<?>> coder = remoteOutputCoder.getValue();
        String bundleOutputPCollection =
            Iterables.getOnlyElement(
                processBundleDescriptor
                    .getProcessBundleDescriptor()
                    .getTransformsOrThrow(outputTransform)
                    .getInputsMap()
                    .values());
        FnDataReceiver<WindowedValue<?>> outputReceiver =
            outputReceiverFactory.create(bundleOutputPCollection);
        outputReceivers.put(outputTransform, RemoteOutputReceiver.of(coder, outputReceiver));
      }

      if (environmentExpirationMillis == 0) {
        return processor.newBundle(outputReceivers.build(), stateRequestHandler, progressHandler);
      }

      final WrappedSdkHarnessClient client =
          environmentCache.getUnchecked(executableStage.getEnvironment());
      client.ref();

      if (client != wrappedClient) {
        // reset after environment expired
        prepare(client);
      }

      final RemoteBundle bundle =
          processor.newBundle(outputReceivers.build(), stateRequestHandler, progressHandler);
      return new RemoteBundle() {
        @Override
        public String getId() {
          return bundle.getId();
        }

        @Override
        public Map<String, FnDataReceiver<WindowedValue<?>>> getInputReceivers() {
          return bundle.getInputReceivers();
        }

        @Override
        public void close() throws Exception {
          bundle.close();
          client.unref();
        }
      };
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
    private final ServerInfo serverInfo;
    private final AtomicInteger bundleRefCount = new AtomicInteger();

    static WrappedSdkHarnessClient wrapping(RemoteEnvironment environment, ServerInfo serverInfo) {
      SdkHarnessClient client =
          SdkHarnessClient.usingFnApiClient(
              environment.getInstructionRequestHandler(), serverInfo.getDataServer().getService());
      return new WrappedSdkHarnessClient(environment, client, serverInfo);
    }

    private WrappedSdkHarnessClient(
        RemoteEnvironment environment, SdkHarnessClient client, ServerInfo serverInfo) {
      this.environment = environment;
      this.client = client;
      this.serverInfo = serverInfo;
      ref();
    }

    SdkHarnessClient getClient() {
      return client;
    }

    ServerInfo getServerInfo() {
      return serverInfo;
    }

    @Override
    public void close() throws Exception {
      try (AutoCloseable envCloser = environment) {
        // Wrap resources in try-with-resources to ensure all are cleaned up.
      }
      try (AutoCloseable stateServer = serverInfo.getStateServer();
          AutoCloseable dateServer = serverInfo.getDataServer();
          AutoCloseable controlServer = serverInfo.getControlServer();
          AutoCloseable loggingServer = serverInfo.getLoggingServer();
          AutoCloseable retrievalServer = serverInfo.getRetrievalServer();
          AutoCloseable provisioningServer = serverInfo.getProvisioningServer()) {}
      // TODO: Wait for executor shutdown?
    }

    private int ref() {
      return bundleRefCount.incrementAndGet();
    }

    private int unref() {
      int count = bundleRefCount.decrementAndGet();
      if (count == 0) {
        // Close environment after it was removed from cache and all bundles finished.
        LOG.info("Closing environment {}", environment.getEnvironment());
        try {
          close();
        } catch (Exception e) {
          LOG.warn("Error cleaning up environment {}", environment.getEnvironment(), e);
        }
      }
      return count;
    }
  }

  private ServerInfo createServerInfo(JobInfo jobInfo, ServerFactory serverFactory)
      throws IOException {
    Preconditions.checkNotNull(serverFactory, "serverFactory can not be null");

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
    GrpcFnServer<GrpcDataService> dataServer =
        GrpcFnServer.allocatePortAndCreateFor(
            GrpcDataService.create(executor, OutboundObserverFactory.serverDirect()),
            serverFactory);
    GrpcFnServer<GrpcStateService> stateServer =
        GrpcFnServer.allocatePortAndCreateFor(GrpcStateService.create(), serverFactory);

    ServerInfo serverInfo =
        new AutoValue_DefaultJobBundleFactory_ServerInfo.Builder()
            .setControlServer(controlServer)
            .setLoggingServer(loggingServer)
            .setRetrievalServer(retrievalServer)
            .setProvisioningServer(provisioningServer)
            .setDataServer(dataServer)
            .setStateServer(stateServer)
            .build();
    return serverInfo;
  }

  /** A container for EnvironmentFactory and its corresponding Grpc servers. */
  @AutoValue
  public abstract static class ServerInfo {
    abstract GrpcFnServer<FnApiControlClientPoolService> getControlServer();

    abstract GrpcFnServer<GrpcLoggingService> getLoggingServer();

    abstract GrpcFnServer<ArtifactRetrievalService> getRetrievalServer();

    abstract GrpcFnServer<StaticGrpcProvisionService> getProvisioningServer();

    abstract GrpcFnServer<GrpcDataService> getDataServer();

    abstract GrpcFnServer<GrpcStateService> getStateServer();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setControlServer(GrpcFnServer<FnApiControlClientPoolService> server);

      abstract Builder setLoggingServer(GrpcFnServer<GrpcLoggingService> server);

      abstract Builder setRetrievalServer(GrpcFnServer<ArtifactRetrievalService> server);

      abstract Builder setProvisioningServer(GrpcFnServer<StaticGrpcProvisionService> server);

      abstract Builder setDataServer(GrpcFnServer<GrpcDataService> server);

      abstract Builder setStateServer(GrpcFnServer<GrpcStateService> server);

      abstract ServerInfo build();
    }
  }
}
