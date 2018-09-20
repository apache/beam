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

import static com.google.common.base.MoreObjects.firstNonNull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link EnvironmentFactory} that creates docker containers by shelling out to docker. Returned
 * {@link RemoteEnvironment RemoteEnvironments} own their respective docker containers. Not
 * thread-safe.
 */
public class DockerEnvironmentFactory implements EnvironmentFactory {

  private static final Logger LOG = LoggerFactory.getLogger(DockerEnvironmentFactory.class);

  /**
   * Returns a {@link DockerEnvironmentFactory} for the provided {@link GrpcFnServer servers} using
   * the default {@link DockerCommand}.
   */
  public static DockerEnvironmentFactory forServices(
      GrpcFnServer<FnApiControlClientPoolService> controlServiceServer,
      GrpcFnServer<GrpcLoggingService> loggingServiceServer,
      GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer,
      GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
      ControlClientPool.Source clientSource,
      IdGenerator idGenerator) {
    return forServicesWithDocker(
        DockerCommand.getDefault(),
        controlServiceServer,
        loggingServiceServer,
        retrievalServiceServer,
        provisioningServiceServer,
        clientSource,
        idGenerator);
  }

  static DockerEnvironmentFactory forServicesWithDocker(
      DockerCommand docker,
      GrpcFnServer<FnApiControlClientPoolService> controlServiceServer,
      GrpcFnServer<GrpcLoggingService> loggingServiceServer,
      GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer,
      GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
      ControlClientPool.Source clientSource,
      IdGenerator idGenerator) {
    return new DockerEnvironmentFactory(
        docker,
        controlServiceServer,
        loggingServiceServer,
        retrievalServiceServer,
        provisioningServiceServer,
        idGenerator,
        clientSource);
  }

  private final DockerCommand docker;
  private final GrpcFnServer<FnApiControlClientPoolService> controlServiceServer;
  private final GrpcFnServer<GrpcLoggingService> loggingServiceServer;
  private final GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer;
  private final GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer;
  private final IdGenerator idGenerator;
  private final ControlClientPool.Source clientSource;

  private DockerEnvironmentFactory(
      DockerCommand docker,
      GrpcFnServer<FnApiControlClientPoolService> controlServiceServer,
      GrpcFnServer<GrpcLoggingService> loggingServiceServer,
      GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer,
      GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
      IdGenerator idGenerator,
      ControlClientPool.Source clientSource) {
    this.docker = docker;
    this.controlServiceServer = controlServiceServer;
    this.loggingServiceServer = loggingServiceServer;
    this.retrievalServiceServer = retrievalServiceServer;
    this.provisioningServiceServer = provisioningServiceServer;
    this.idGenerator = idGenerator;
    this.clientSource = clientSource;
  }

  /** Creates a new, active {@link RemoteEnvironment} backed by a local Docker container. */
  @Override
  public RemoteEnvironment createEnvironment(Environment environment) throws Exception {
    Preconditions.checkState(
        environment
            .getUrn()
            .equals(BeamUrns.getUrn(RunnerApi.StandardEnvironments.Environments.DOCKER)),
        "The passed environment does not contain a DockerPayload.");
    final RunnerApi.DockerPayload dockerPayload =
        RunnerApi.DockerPayload.parseFrom(environment.getPayload());
    final String workerId = idGenerator.getId();

    // Prepare docker invocation.
    String containerImage = dockerPayload.getContainerImage();
    // TODO: https://issues.apache.org/jira/browse/BEAM-4148 The default service address will not
    // work for Docker for Mac.
    String loggingEndpoint = loggingServiceServer.getApiServiceDescriptor().getUrl();
    String artifactEndpoint = retrievalServiceServer.getApiServiceDescriptor().getUrl();
    String provisionEndpoint = provisioningServiceServer.getApiServiceDescriptor().getUrl();
    String controlEndpoint = controlServiceServer.getApiServiceDescriptor().getUrl();

    List<String> volArg =
        ImmutableList.<String>builder()
            .addAll(gcsCredentialArgs())
            // NOTE: Host networking does not work on Mac, but the command line flag is accepted.
            .add("--network=host")
            // We need to pass on the information about Docker-on-Mac environment (due to missing host networking on Mac)
            .add("--env=DOCKER_MAC_CONTAINER=" + System.getenv("DOCKER_MAC_CONTAINER"))
            .build();

    List<String> args =
        ImmutableList.of(
            String.format("--id=%s", workerId),
            String.format("--logging_endpoint=%s", loggingEndpoint),
            String.format("--artifact_endpoint=%s", artifactEndpoint),
            String.format("--provision_endpoint=%s", provisionEndpoint),
            String.format("--control_endpoint=%s", controlEndpoint));

    LOG.debug("Creating Docker Container with ID {}", workerId);
    // Wrap the blocking call to clientSource.get in case an exception is thrown.
    String containerId = null;
    InstructionRequestHandler instructionHandler = null;
    try {
      containerId = docker.runImage(containerImage, volArg, args);
      LOG.debug("Created Docker Container with Container ID {}", containerId);
      // Wait on a client from the gRPC server.
      while (instructionHandler == null) {
        try {
          instructionHandler = clientSource.take(workerId, Duration.ofMinutes(2));
        } catch (TimeoutException timeoutEx) {
          LOG.info(
              "Still waiting for startup of environment {} for worker id {}",
              dockerPayload.getContainerImage(),
              workerId);
        } catch (InterruptedException interruptEx) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(interruptEx);
        }
      }
    } catch (Exception e) {
      if (containerId != null) {
        // Kill the launched docker container if we can't retrieve a client for it.
        try {
          docker.killContainer(containerId);
        } catch (Exception dockerException) {
          e.addSuppressed(dockerException);
        }
      }
      throw e;
    }

    return DockerContainerEnvironment.create(docker, environment, containerId, instructionHandler);
  }

  private List<String> gcsCredentialArgs() {
    String dockerGcloudConfig = "/root/.config/gcloud";
    String localGcloudConfig =
        firstNonNull(
            System.getenv("CLOUDSDK_CONFIG"),
            Paths.get(System.getProperty("user.home"), ".config", "gcloud").toString());
    // TODO(BEAM-4729): Allow this to be disabled manually.
    if (Files.exists(Paths.get(localGcloudConfig))) {
      return ImmutableList.of(
          "--mount",
          String.format("type=bind,src=%s,dst=%s", localGcloudConfig, dockerGcloudConfig));
    } else {
      return ImmutableList.of();
    }
  }
}
