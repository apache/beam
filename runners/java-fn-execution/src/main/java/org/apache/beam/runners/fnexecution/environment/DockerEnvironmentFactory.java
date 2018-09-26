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
import com.google.common.net.HostAndPort;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.core.construction.BeamUrns;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.ServerFactory;
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

  /**
   * NOTE: Deployment on Macs is intended for local development. As of 18.03, Docker-for-Mac does
   * not implement host networking (--networking=host is effectively a no-op). Instead, we use a
   * special DNS entry that points to the host:
   * https://docs.docker.com/docker-for-mac/networking/#use-cases-and-workarounds The special
   * hostname has historically changed between versions, so this is subject to breakages and will
   * likely only support the latest version at any time.
   */
  private static class DockerOnMac {
    // TODO: This host name seems to change with every other Docker release. Do we attempt to keep up
    // or attempt to document the supported Docker version(s)?
    private static final String DOCKER_FOR_MAC_HOST = "host.docker.internal";

    // True if we're inside a container (i.e. job-server container) with MacOS as the host system
    private static final boolean RUNNING_INSIDE_DOCKER_ON_MAC =
        "1".equals(System.getenv("DOCKER_MAC_CONTAINER"));
    // Port offset for MacOS since we don't have host networking and need to use published ports
    private static final int MAC_PORT_START = 8100;
    private static final int MAC_PORT_END = 8200;
    private static final AtomicInteger MAC_PORT = new AtomicInteger(MAC_PORT_START);

    private static ServerFactory getServerFactory() {
      ServerFactory.UrlFactory dockerUrlFactory =
          (host, port) -> HostAndPort.fromParts(DOCKER_FOR_MAC_HOST, port).toString();
      if (RUNNING_INSIDE_DOCKER_ON_MAC) {
        // If we're already running in a container, we need to use a fixed port range due to
        // non-existing host networking in Docker-for-Mac. The port range needs to be published
        // when bringing up the Docker container, see DockerEnvironmentFactory.
        return ServerFactory.createWithUrlFactoryAndPortSupplier(
            dockerUrlFactory,
            // We only use the published Docker ports 8100-8200 in a round-robin fashion
            () -> MAC_PORT.getAndUpdate(val -> val == MAC_PORT_END ? MAC_PORT_START : val + 1));
      } else {
        return ServerFactory.createWithUrlFactory(dockerUrlFactory);
      }
    }
  }

  /** Provider for DockerEnvironmentFactory. */
  public static class Provider implements EnvironmentFactory.Provider {

    @Override
    public EnvironmentFactory createEnvironmentFactory(
        GrpcFnServer<FnApiControlClientPoolService> controlServiceServer,
        GrpcFnServer<GrpcLoggingService> loggingServiceServer,
        GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer,
        GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
        ControlClientPool clientPool,
        IdGenerator idGenerator) {
      return DockerEnvironmentFactory.forServicesWithDocker(
          DockerCommand.getDefault(),
          controlServiceServer,
          loggingServiceServer,
          retrievalServiceServer,
          provisioningServiceServer,
          clientPool.getSource(),
          idGenerator);
    }

    @Override
    public ServerFactory getServerFactory() {
      switch (getPlatform()) {
        case LINUX:
          return ServerFactory.createDefault();
        case MAC:
          return DockerOnMac.getServerFactory();
        default:
          LOG.warn("Unknown Docker platform. Falling back to default server factory");
          return ServerFactory.createDefault();
      }
    }

    private static Platform getPlatform() {
      String osName = System.getProperty("os.name").toLowerCase();
      // TODO: Make this more robust?
      // The DOCKER_MAC_CONTAINER environment variable is necessary to detect whether we run on
      // a container on MacOs. MacOs internally uses a Linux VM which makes it indistinguishable from Linux.
      // We still need to apply port mapping due to missing host networking.
      if (osName.startsWith("mac") || DockerOnMac.RUNNING_INSIDE_DOCKER_ON_MAC) {
        return Platform.MAC;
      } else if (osName.startsWith("linux")) {
        return Platform.LINUX;
      }
      return Platform.OTHER;
    }

    private enum Platform {
      MAC,
      LINUX,
      OTHER,
    }
  }
}
