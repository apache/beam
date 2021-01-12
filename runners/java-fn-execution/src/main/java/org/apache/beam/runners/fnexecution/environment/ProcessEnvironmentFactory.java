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

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.core.construction.BeamUrns;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.control.ControlClientPool;
import org.apache.beam.runners.fnexecution.control.FnApiControlClientPoolService;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.provisioning.StaticGrpcProvisionService;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.RemoteEnvironmentOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link EnvironmentFactory} which forks processes based on the parameters in the Environment.
 * The returned {@link ProcessEnvironment} has to make sure to stop the processes.
 */
public class ProcessEnvironmentFactory implements EnvironmentFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ProcessEnvironmentFactory.class);

  public static ProcessEnvironmentFactory create(
      ProcessManager processManager,
      GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
      ControlClientPool.Source clientSource,
      IdGenerator idGenerator,
      PipelineOptions pipelineOptions) {
    return new ProcessEnvironmentFactory(
        processManager, provisioningServiceServer, idGenerator, clientSource, pipelineOptions);
  }

  private final ProcessManager processManager;
  private final GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer;
  private final IdGenerator idGenerator;
  private final ControlClientPool.Source clientSource;
  private final PipelineOptions pipelineOptions;

  private ProcessEnvironmentFactory(
      ProcessManager processManager,
      GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
      IdGenerator idGenerator,
      ControlClientPool.Source clientSource,
      PipelineOptions pipelineOptions) {
    this.processManager = processManager;
    this.provisioningServiceServer = provisioningServiceServer;
    this.idGenerator = idGenerator;
    this.clientSource = clientSource;
    this.pipelineOptions = pipelineOptions;
  }

  /** Creates a new, active {@link RemoteEnvironment} backed by a forked process. */
  @Override
  public RemoteEnvironment createEnvironment(Environment environment, String workerId)
      throws Exception {
    Preconditions.checkState(
        environment
            .getUrn()
            .equals(BeamUrns.getUrn(RunnerApi.StandardEnvironments.Environments.PROCESS)),
        "The passed environment does not contain a ProcessPayload.");
    final RunnerApi.ProcessPayload processPayload =
        RunnerApi.ProcessPayload.parseFrom(environment.getPayload());

    String executable = processPayload.getCommand();
    String provisionEndpoint = provisioningServiceServer.getApiServiceDescriptor().getUrl();

    String semiPersistDir = pipelineOptions.as(RemoteEnvironmentOptions.class).getSemiPersistDir();
    ImmutableList.Builder<String> argsBuilder =
        ImmutableList.<String>builder()
            .add(String.format("--id=%s", workerId))
            .add(String.format("--provision_endpoint=%s", provisionEndpoint));
    if (semiPersistDir != null) {
      argsBuilder.add(String.format("--semi_persist_dir=%s", semiPersistDir));
    }

    LOG.debug("Creating Process for worker ID {}", workerId);
    // Wrap the blocking call to clientSource.get in case an exception is thrown.
    InstructionRequestHandler instructionHandler = null;
    try {
      ProcessManager.RunningProcess process =
          processManager.startProcess(
              workerId, executable, argsBuilder.build(), processPayload.getEnvMap());
      // Wait on a client from the gRPC server.
      while (instructionHandler == null) {
        try {
          // If the process is not alive anymore, we abort.
          process.isAliveOrThrow();
          instructionHandler = clientSource.take(workerId, Duration.ofSeconds(5));
        } catch (TimeoutException timeoutEx) {
          LOG.info(
              "Still waiting for startup of environment '{}' for worker id {}",
              processPayload.getCommand(),
              workerId);
        } catch (InterruptedException interruptEx) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(interruptEx);
        }
      }
    } catch (Exception e) {
      try {
        processManager.stopProcess(workerId);
      } catch (Exception processKillException) {
        e.addSuppressed(processKillException);
      }
      throw e;
    }

    return ProcessEnvironment.create(processManager, environment, workerId, instructionHandler);
  }

  /** Provider of ProcessEnvironmentFactory. */
  public static class Provider implements EnvironmentFactory.Provider {
    private final PipelineOptions pipelineOptions;

    public Provider(PipelineOptions options) {
      this.pipelineOptions = options;
    }

    @Override
    public EnvironmentFactory createEnvironmentFactory(
        GrpcFnServer<FnApiControlClientPoolService> controlServiceServer,
        GrpcFnServer<GrpcLoggingService> loggingServiceServer,
        GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer,
        GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
        ControlClientPool clientPool,
        IdGenerator idGenerator) {
      return create(
          ProcessManager.create(),
          provisioningServiceServer,
          clientPool.getSource(),
          idGenerator,
          pipelineOptions);
    }
  }
}
