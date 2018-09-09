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
import com.google.common.net.HostAndPort;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.environment.DockerEnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.EnvironmentFactory;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.provisioning.StaticGrpcProvisionService;
import org.apache.beam.sdk.fn.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link JobBundleFactory} that uses a {@link DockerEnvironmentFactory} for environment
 * management. Note that returned {@link StageBundleFactory stage bundle factories} are not
 * thread-safe. Instead, a new stage factory should be created for each client.
 */
@ThreadSafe
public class DockerJobBundleFactory extends JobBundleFactoryBase {
  private static final Logger LOG = LoggerFactory.getLogger(DockerJobBundleFactory.class);

  /** Factory that creates {@link JobBundleFactory} for the given {@link JobInfo}. */
  public interface JobBundleFactoryFactory {
    JobBundleFactory create(JobInfo jobInfo) throws Exception;
  }
  // TODO (BEAM-4819): a hacky way to override the factory for testing.
  // Should be replaced with mechanism that let's users configure their own factory
  public static final AtomicReference<JobBundleFactoryFactory> FACTORY =
      new AtomicReference(
          new JobBundleFactoryFactory() {
            @Override
            public JobBundleFactory create(JobInfo jobInfo) throws Exception {
              return new DockerJobBundleFactory(jobInfo);
            }
          });

  protected DockerJobBundleFactory(JobInfo jobInfo) throws Exception {
    super(jobInfo);
  }

  @VisibleForTesting
  DockerJobBundleFactory(
      EnvironmentFactory environmentFactory,
      ServerFactory serverFactory,
      IdGenerator stageIdGenerator,
      GrpcFnServer<FnApiControlClientPoolService> controlServer,
      GrpcFnServer<GrpcLoggingService> loggingServer,
      GrpcFnServer<ArtifactRetrievalService> retrievalServer,
      GrpcFnServer<StaticGrpcProvisionService> provisioningServer) {
    super(
        environmentFactory,
        serverFactory,
        stageIdGenerator,
        controlServer,
        loggingServer,
        retrievalServer,
        provisioningServer);
  }

  @Override
  protected ServerFactory getServerFactory() {
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

  /** Create {@link EnvironmentFactory} for the given services. */
  @Override
  protected EnvironmentFactory getEnvironmentFactory(
      GrpcFnServer<FnApiControlClientPoolService> controlServiceServer,
      GrpcFnServer<GrpcLoggingService> loggingServiceServer,
      GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer,
      GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
      ControlClientPool.Source clientSource,
      IdGenerator idGenerator) {
    return DockerEnvironmentFactory.forServices(
        controlServiceServer,
        loggingServiceServer,
        retrievalServiceServer,
        provisioningServiceServer,
        clientSource,
        idGenerator);
  }
}
