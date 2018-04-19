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

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.control.ControlClientPool;
import org.apache.beam.runners.fnexecution.control.FnApiControlClientPoolService;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.provisioning.StaticGrpcProvisionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link EnvironmentFactory} that creates docker containers by shelling out to docker. Returned
 * {@link RemoteEnvironment RemoteEnvironments} own their respective docker containers. Not
 * thread-safe.
 */
public class DockerEnvironmentFactory implements EnvironmentFactory {

  private static final Logger LOG = LoggerFactory.getLogger(DockerEnvironmentFactory.class);

  public static DockerEnvironmentFactory forServices(
      DockerCommand docker,
      GrpcFnServer<FnApiControlClientPoolService> controlServiceServer,
      GrpcFnServer<GrpcLoggingService> loggingServiceServer,
      GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer,
      GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
      ControlClientPool.Source clientSource,
      // TODO: Refine this to IdGenerator when we determine where that should live.
      Supplier<String> idGenerator) {
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
  private final Supplier<String> idGenerator;
  private final ControlClientPool.Source clientSource;

  private DockerEnvironmentFactory(
      DockerCommand docker,
      GrpcFnServer<FnApiControlClientPoolService> controlServiceServer,
      GrpcFnServer<GrpcLoggingService> loggingServiceServer,
      GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer,
      GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
      Supplier<String> idGenerator,
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
    String workerId = idGenerator.get();

    // Prepare docker invocation.
    Path workerPersistentDirectory = Files.createTempDirectory("worker_persistent_directory");
    Path semiPersistentDirectory = Files.createTempDirectory("semi_persistent_dir");
    String containerImage = environment.getUrl();
    // TODO: https://issues.apache.org/jira/browse/BEAM-4148 The default service address will not
    // work for Docker for Mac.
    String loggingEndpoint = loggingServiceServer.getApiServiceDescriptor().getUrl();
    String artifactEndpoint = retrievalServiceServer.getApiServiceDescriptor().getUrl();
    String provisionEndpoint = provisioningServiceServer.getApiServiceDescriptor().getUrl();
    String controlEndpoint = controlServiceServer.getApiServiceDescriptor().getUrl();
    List<String> args =
        Arrays.asList(
            "-v",
            // TODO: Mac only allows temporary mounts under /tmp by default (as of 17.12).
            String.format("%s:%s", workerPersistentDirectory, semiPersistentDirectory),
            // NOTE: Host networking does not work on Mac, but the command line flag is accepted.
            "--network=host",
            containerImage,
            String.format("--id=%s", workerId),
            String.format("--logging_endpoint=%s", loggingEndpoint),
            String.format("--artifact_endpoint=%s", artifactEndpoint),
            String.format("--provision_endpoint=%s", provisionEndpoint),
            String.format("--control_endpoint=%s", controlEndpoint),
            String.format("--semi_persist_dir=%s", semiPersistentDirectory));

    // Wrap the blocking call to clientSource.get in case an exception is thrown.
    String containerId = null;
    InstructionRequestHandler instructionHandler = null;
    try {
      containerId = docker.runImage(containerImage, args);
      // Wait on a client from the gRPC server.
      while (instructionHandler == null) {
        try {
          instructionHandler = clientSource.take(workerId, Duration.ofMinutes(2));
        } catch (TimeoutException timeoutEx) {
          LOG.info(
              "Still waiting for startup of environment {} for worker id {}",
              environment.getUrl(),
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
}
