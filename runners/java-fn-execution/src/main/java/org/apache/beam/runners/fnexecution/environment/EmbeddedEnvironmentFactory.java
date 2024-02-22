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
package org.apache.beam.runners.fnexecution.environment;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import org.apache.beam.fn.harness.Caches;
import org.apache.beam.fn.harness.FnHarness;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardRunnerProtocols;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.control.ControlClientPool;
import org.apache.beam.runners.fnexecution.control.ControlClientPool.Source;
import org.apache.beam.runners.fnexecution.control.FnApiControlClientPoolService;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.provisioning.StaticGrpcProvisionService;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.channel.ManagedChannelFactory;
import org.apache.beam.sdk.fn.server.GrpcFnServer;
import org.apache.beam.sdk.fn.server.InProcessServerFactory;
import org.apache.beam.sdk.fn.server.ServerFactory;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.construction.BeamUrns;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link EnvironmentFactory} that communicates to a {@link FnHarness} which is executing in the
 * same process.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class EmbeddedEnvironmentFactory implements EnvironmentFactory {
  private static final Logger LOG = LoggerFactory.getLogger(EmbeddedEnvironmentFactory.class);

  private final PipelineOptions options;

  private final GrpcFnServer<GrpcLoggingService> loggingServer;
  private final GrpcFnServer<FnApiControlClientPoolService> controlServer;

  private final ControlClientPool.Source clientSource;

  public static EnvironmentFactory create(
      PipelineOptions options,
      GrpcFnServer<GrpcLoggingService> loggingServer,
      GrpcFnServer<FnApiControlClientPoolService> controlServer,
      ControlClientPool.Source clientSource) {
    return new EmbeddedEnvironmentFactory(options, loggingServer, controlServer, clientSource);
  }

  private EmbeddedEnvironmentFactory(
      PipelineOptions options,
      GrpcFnServer<GrpcLoggingService> loggingServer,
      GrpcFnServer<FnApiControlClientPoolService> controlServer,
      Source clientSource) {
    this.options = options;
    this.loggingServer = loggingServer;
    this.controlServer = controlServer;
    checkArgument(
        loggingServer.getApiServiceDescriptor() != null,
        "Logging Server cannot have a null %s",
        ApiServiceDescriptor.class.getSimpleName());
    checkArgument(
        controlServer.getApiServiceDescriptor() != null,
        "Control Server cannot have a null %s",
        ApiServiceDescriptor.class.getSimpleName());
    this.clientSource = clientSource;
  }

  @Override
  @SuppressWarnings("FutureReturnValueIgnored") // no need to monitor shutdown thread
  public RemoteEnvironment createEnvironment(Environment environment, String workerId)
      throws Exception {
    ExecutorService executor =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("CreateEnvironment-thread")
                .build());
    Future<?> fnHarness =
        executor.submit(
            () -> {
              try {
                FnHarness.main(
                    workerId,
                    options,
                    Collections.singleton(
                        BeamUrns.getUrn(
                            StandardRunnerProtocols.Enum
                                .CONTROL_RESPONSE_ELEMENTS_EMBEDDING)), // Runner capabilities.
                    loggingServer.getApiServiceDescriptor(),
                    controlServer.getApiServiceDescriptor(),
                    null,
                    ManagedChannelFactory.createInProcess(),
                    OutboundObserverFactory.clientDirect(),
                    Caches.fromOptions(options));
              } catch (NoClassDefFoundError e) {
                // TODO: https://github.com/apache/beam/issues/18762 load the FnHarness in a
                // Restricted classpath that we control for any user.
                LOG.error(
                    "{} while executing an in-process FnHarness. "
                        + "To use the {}, "
                        + "the 'org.apache.beam:beam-sdks-java-harness' artifact "
                        + "and its dependencies must be on the classpath",
                    NoClassDefFoundError.class.getSimpleName(),
                    EmbeddedEnvironmentFactory.class.getSimpleName(),
                    e);
                throw e;
              }
              return null;
            });
    executor.submit(
        () -> {
          try {
            fnHarness.get();
          } catch (Throwable t) {
            executor.shutdownNow();
          }
        });

    InstructionRequestHandler handler = null;
    // Wait on a client from the gRPC server.
    while (handler == null) {
      try {
        // If the thread is not alive anymore, we abort.
        if (executor.isShutdown()) {
          throw new IllegalStateException("FnHarness startup failed");
        }
        handler = clientSource.take(workerId, Duration.ofSeconds(5L));
      } catch (TimeoutException timeoutEx) {
        LOG.info("Still waiting for startup of FnHarness");
      } catch (InterruptedException interruptEx) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(interruptEx);
      }
    }
    return RemoteEnvironment.forHandler(environment, handler);
  }

  /** Provider of EmbeddedEnvironmentFactory. */
  public static class Provider implements EnvironmentFactory.Provider {

    private final PipelineOptions pipelineOptions;

    public Provider(PipelineOptions pipelineOptions) {
      this.pipelineOptions = pipelineOptions;
    }

    @Override
    public EnvironmentFactory createEnvironmentFactory(
        GrpcFnServer<FnApiControlClientPoolService> controlServer,
        GrpcFnServer<GrpcLoggingService> loggingServer,
        GrpcFnServer<ArtifactRetrievalService> retrievalServer,
        GrpcFnServer<StaticGrpcProvisionService> provisioningServer,
        ControlClientPool clientPool,
        IdGenerator idGenerator) {
      return EmbeddedEnvironmentFactory.create(
          pipelineOptions, loggingServer, controlServer, clientPool.getSource());
    }

    @Override
    public ServerFactory getServerFactory() {
      return InProcessServerFactory.create();
    }
  }
}
