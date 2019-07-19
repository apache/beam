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
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnExternalWorkerPoolGrpc;
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
import org.apache.beam.sdk.fn.channel.ManagedChannelFactory;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An {@link EnvironmentFactory} which requests workers via the given URL in the Environment. */
public class ExternalEnvironmentFactory implements EnvironmentFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ExternalEnvironmentFactory.class);

  public static ExternalEnvironmentFactory create(
      GrpcFnServer<FnApiControlClientPoolService> controlServiceServer,
      GrpcFnServer<GrpcLoggingService> loggingServiceServer,
      GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer,
      GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
      ControlClientPool.Source clientSource,
      IdGenerator idGenerator) {
    return new ExternalEnvironmentFactory(
        controlServiceServer,
        loggingServiceServer,
        retrievalServiceServer,
        provisioningServiceServer,
        idGenerator,
        clientSource);
  }

  private final GrpcFnServer<FnApiControlClientPoolService> controlServiceServer;
  private final GrpcFnServer<GrpcLoggingService> loggingServiceServer;
  private final GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer;
  private final GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer;
  private final IdGenerator idGenerator;
  private final ControlClientPool.Source clientSource;

  private ExternalEnvironmentFactory(
      GrpcFnServer<FnApiControlClientPoolService> controlServiceServer,
      GrpcFnServer<GrpcLoggingService> loggingServiceServer,
      GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer,
      GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
      IdGenerator idGenerator,
      ControlClientPool.Source clientSource) {
    this.controlServiceServer = controlServiceServer;
    this.loggingServiceServer = loggingServiceServer;
    this.retrievalServiceServer = retrievalServiceServer;
    this.provisioningServiceServer = provisioningServiceServer;
    this.idGenerator = idGenerator;
    this.clientSource = clientSource;
  }

  /** Creates a new, active {@link RemoteEnvironment} backed by an unmanaged worker. */
  @Override
  public RemoteEnvironment createEnvironment(Environment environment) throws Exception {
    Preconditions.checkState(
        environment
            .getUrn()
            .equals(BeamUrns.getUrn(RunnerApi.StandardEnvironments.Environments.EXTERNAL)),
        "The passed environment does not contain an ExternalPayload.");
    final RunnerApi.ExternalPayload externalPayload =
        RunnerApi.ExternalPayload.parseFrom(environment.getPayload());
    final String workerId = idGenerator.getId();

    BeamFnApi.NotifyRunnerAvailableRequest notifyRunnerAvailableRequest =
        BeamFnApi.NotifyRunnerAvailableRequest.newBuilder()
            .setWorkerId(workerId)
            .setControlEndpoint(controlServiceServer.getApiServiceDescriptor())
            .setLoggingEndpoint(loggingServiceServer.getApiServiceDescriptor())
            .setArtifactEndpoint(retrievalServiceServer.getApiServiceDescriptor())
            .setProvisionEndpoint(provisioningServiceServer.getApiServiceDescriptor())
            .putAllParams(externalPayload.getParamsMap())
            .build();

    LOG.debug("Requesting worker ID {}", workerId);
    BeamFnApi.NotifyRunnerAvailableResponse notifyRunnerAvailableResponse =
        BeamFnExternalWorkerPoolGrpc.newBlockingStub(
                ManagedChannelFactory.createDefault().forDescriptor(externalPayload.getEndpoint()))
            .notifyRunnerAvailable(notifyRunnerAvailableRequest);
    if (!notifyRunnerAvailableResponse.getError().isEmpty()) {
      throw new RuntimeException(notifyRunnerAvailableResponse.getError());
    }

    // Wait on a client from the gRPC server.
    InstructionRequestHandler instructionHandler = null;
    while (instructionHandler == null) {
      try {
        instructionHandler = clientSource.take(workerId, Duration.ofMinutes(2));
      } catch (TimeoutException timeoutEx) {
        LOG.info(
            "Still waiting for startup of environment from {} for worker id {}",
            externalPayload.getEndpoint().getUrl(),
            workerId);
      } catch (InterruptedException interruptEx) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(interruptEx);
      }
    }
    final InstructionRequestHandler finalInstructionHandler = instructionHandler;

    return new RemoteEnvironment() {
      @Override
      public Environment getEnvironment() {
        return environment;
      }

      @Override
      public InstructionRequestHandler getInstructionRequestHandler() {
        return finalInstructionHandler;
      }
    };
  }

  /** Provider of ExternalEnvironmentFactory. */
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
          controlServiceServer,
          loggingServiceServer,
          retrievalServiceServer,
          provisioningServiceServer,
          clientPool.getSource(),
          idGenerator);
    }
  }
}
