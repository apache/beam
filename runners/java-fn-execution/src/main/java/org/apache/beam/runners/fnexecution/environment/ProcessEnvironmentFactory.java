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
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
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
      GrpcFnServer<FnApiControlClientPoolService> controlServiceServer,
      GrpcFnServer<GrpcLoggingService> loggingServiceServer,
      GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer,
      GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
      ControlClientPool.Source clientSource,
      IdGenerator idGenerator) {
    return new ProcessEnvironmentFactory(
        processManager,
        controlServiceServer,
        loggingServiceServer,
        retrievalServiceServer,
        provisioningServiceServer,
        idGenerator,
        clientSource);
  }

  private final ProcessManager processManager;
  private final GrpcFnServer<FnApiControlClientPoolService> controlServiceServer;
  private final GrpcFnServer<GrpcLoggingService> loggingServiceServer;
  private final GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer;
  private final GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer;
  private final IdGenerator idGenerator;
  private final ControlClientPool.Source clientSource;

  private ProcessEnvironmentFactory(
      ProcessManager processManager,
      GrpcFnServer<FnApiControlClientPoolService> controlServiceServer,
      GrpcFnServer<GrpcLoggingService> loggingServiceServer,
      GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer,
      GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
      IdGenerator idGenerator,
      ControlClientPool.Source clientSource) {
    this.processManager = processManager;
    this.controlServiceServer = controlServiceServer;
    this.loggingServiceServer = loggingServiceServer;
    this.retrievalServiceServer = retrievalServiceServer;
    this.provisioningServiceServer = provisioningServiceServer;
    this.idGenerator = idGenerator;
    this.clientSource = clientSource;
  }

  /** Creates a new, active {@link RemoteEnvironment} backed by a forked process. */
  @Override
  public RemoteEnvironment createEnvironment(Environment environment) throws Exception {
    Preconditions.checkState(
        environment
            .getUrn()
            .equals(BeamUrns.getUrn(RunnerApi.StandardEnvironments.Environments.PROCESS)),
        "The passed environment does not contain a ProcessPayload.");
    final RunnerApi.ProcessPayload processPayload =
        RunnerApi.ProcessPayload.parseFrom(environment.getPayload());
    final String workerId = idGenerator.getId();

    String executable = processPayload.getCommand();
    String loggingEndpoint = loggingServiceServer.getApiServiceDescriptor().getUrl();
    String artifactEndpoint = retrievalServiceServer.getApiServiceDescriptor().getUrl();
    String provisionEndpoint = provisioningServiceServer.getApiServiceDescriptor().getUrl();
    String controlEndpoint = controlServiceServer.getApiServiceDescriptor().getUrl();

    ImmutableList<String> args =
        ImmutableList.of(
            String.format("--id=%s", workerId),
            String.format("--logging_endpoint=%s", loggingEndpoint),
            String.format("--artifact_endpoint=%s", artifactEndpoint),
            String.format("--provision_endpoint=%s", provisionEndpoint),
            String.format("--control_endpoint=%s", controlEndpoint));

    LOG.debug("Creating Process for worker ID {}", workerId);
    // Wrap the blocking call to clientSource.get in case an exception is thrown.
    InstructionRequestHandler instructionHandler = null;
    try {
      ProcessManager.RunningProcess process =
          processManager.startProcess(workerId, executable, args, processPayload.getEnvMap());
      // Wait on a client from the gRPC server.
      while (instructionHandler == null) {
        try {
          // If the process is not alive anymore, we abort.
          process.isAliveOrThrow();
          instructionHandler = clientSource.take(workerId, Duration.ofMinutes(2));
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
          controlServiceServer,
          loggingServiceServer,
          retrievalServiceServer,
          provisioningServiceServer,
          clientPool.getSource(),
          idGenerator);
    }
  }
}
