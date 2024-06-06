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
package org.apache.beam.runners.jobsubmission;

import java.io.IOException;
import java.nio.file.Paths;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.runners.fnexecution.artifact.ArtifactStagingService;
import org.apache.beam.sdk.expansion.service.ExpansionServer;
import org.apache.beam.sdk.expansion.service.ExpansionService;
import org.apache.beam.sdk.fn.server.GrpcFnServer;
import org.apache.beam.sdk.fn.server.ServerFactory;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.ExplicitBooleanOptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Shared code for starting and serving an {@link InMemoryJobService}. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public abstract class JobServerDriver implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(JobServerDriver.class);

  @VisibleForTesting public ServerConfiguration configuration;

  private final ServerFactory jobServerFactory;
  private final ServerFactory artifactServerFactory;
  private final JobInvokerFactory jobInvokerFactory;

  private volatile GrpcFnServer<InMemoryJobService> jobServer;
  private volatile GrpcFnServer<ArtifactStagingService> artifactStagingServer;
  private volatile ExpansionServer expansionServer;

  public interface JobInvokerFactory {
    JobInvoker create();
  }

  protected InMemoryJobService createJobService() throws IOException {
    artifactStagingServer = createArtifactStagingService();
    expansionServer = createExpansionService();

    JobInvoker invoker = jobInvokerFactory.create();
    return InMemoryJobService.create(
        artifactStagingServer,
        this::createSessionToken,
        (String stagingSessionToken) -> {
          if (configuration.cleanArtifactsPerJob) {
            artifactStagingServer.getService().removeStagedArtifacts(stagingSessionToken);
          }
        },
        invoker,
        configuration.getMaxInvocationHistory());
  }

  /** Configuration for the jobServer. */
  public static class ServerConfiguration {
    @Option(name = "--job-host", usage = "The job server host name")
    private String host = "localhost";

    @Option(
        name = "--job-port",
        usage = "The job service port. 0 to use a dynamic port. (Default: 8099)")
    private int port = 8099;

    @Option(
        name = "--artifact-port",
        usage = "The artifact service port. 0 to use a dynamic port. (Default: 8098)")
    private int artifactPort = 8098;

    @Option(
        name = "--expansion-port",
        usage =
            "The Java expansion service port. 0 to use a dynamic port, "
                + "negative number to disable expansion service creation (Default: 8097)")
    private int expansionPort = 8097;

    @Option(
        name = "--artifacts-dir",
        usage =
            "The location to store staged artifact files. "
                + "If artifact staging is needed, this directory must be accessible by the execution engine's workers.")
    private String artifactStagingPath =
        Paths.get(System.getProperty("java.io.tmpdir"), "beam-artifact-staging").toString();

    @Option(
        name = "--clean-artifacts-per-job",
        usage = "When true, remove each job's staged artifacts when it completes",
        // Allows setting boolean parameters to false which default to true
        handler = ExplicitBooleanOptionHandler.class)
    private boolean cleanArtifactsPerJob = true;

    @Option(name = "--history-size", usage = "The maximum number of completed jobs to keep.")
    private int maxInvocationHistory = 10;

    public String getHost() {
      return host;
    }

    public int getPort() {
      return port;
    }

    public void setPort(int port) {
      this.port = port;
    }

    public int getArtifactPort() {
      return artifactPort;
    }

    public void setArtifactPort(int artifactPort) {
      this.artifactPort = artifactPort;
    }

    public int getExpansionPort() {
      return expansionPort;
    }

    public void setExpansionPort(int expansionPort) {
      this.expansionPort = expansionPort;
    }

    public String getArtifactStagingPath() {
      return artifactStagingPath;
    }

    public boolean isCleanArtifactsPerJob() {
      return cleanArtifactsPerJob;
    }

    public int getMaxInvocationHistory() {
      return maxInvocationHistory;
    }
  }

  protected static ServerFactory createJobServerFactory(ServerConfiguration configuration) {
    return ServerFactory.createWithPortSupplier(() -> configuration.port);
  }

  protected static ServerFactory createArtifactServerFactory(ServerConfiguration configuration) {
    return ServerFactory.createWithPortSupplier(() -> configuration.artifactPort);
  }

  protected JobServerDriver(
      ServerConfiguration configuration,
      ServerFactory jobServerFactory,
      ServerFactory artifactServerFactory,
      JobInvokerFactory jobInvokerFactory) {
    this.configuration = configuration;
    this.jobServerFactory = jobServerFactory;
    this.artifactServerFactory = artifactServerFactory;
    this.jobInvokerFactory = jobInvokerFactory;
  }

  // Can be used to discover the address of the job server, and if it is ready
  public String getJobServerUrl() {
    return (jobServer != null) ? jobServer.getApiServiceDescriptor().getUrl() : null;
  }

  // This method is executed by TestPortableRunner via Reflection
  public String start() throws IOException {
    jobServer = createJobServer();
    return jobServer.getApiServiceDescriptor().getUrl();
  }

  @Override
  public void run() {
    try {
      jobServer = createJobServer();
      LOG.info("Job server now running, terminate with Ctrl+C");
      jobServer.getServer().awaitTermination();
    } catch (InterruptedException e) {
      LOG.warn("Job server interrupted", e);
    } catch (Exception e) {
      LOG.warn("Exception during job server creation", e);
    } finally {
      stop();
    }
  }

  // This method is executed by TestPortableRunner via Reflection
  // Needs to be synchronized to prevent concurrency issues in testing shutdown
  @SuppressWarnings("WeakerAccess")
  public synchronized void stop() {
    if (jobServer != null) {
      try {
        jobServer.close();
        LOG.info("JobServer stopped on {}", jobServer.getApiServiceDescriptor().getUrl());
        jobServer = null;
      } catch (Exception e) {
        LOG.error("Error while closing the jobServer.", e);
      }
    }
    if (artifactStagingServer != null) {
      try {
        artifactStagingServer.close();
        LOG.info(
            "ArtifactStagingServer stopped on {}",
            artifactStagingServer.getApiServiceDescriptor().getUrl());
        artifactStagingServer = null;
      } catch (Exception e) {
        LOG.error("Error while closing the artifactStagingServer.", e);
      }
    }
    if (expansionServer != null) {
      try {
        expansionServer.close();
        LOG.info(
            "Expansion stopped on {}:{}", expansionServer.getHost(), expansionServer.getPort());
        expansionServer = null;
      } catch (Exception e) {
        LOG.error("Error while closing the Expansion Service.", e);
      }
    }
  }

  protected String createSessionToken(String session) {
    return session;
  }

  private GrpcFnServer<InMemoryJobService> createJobServer() throws IOException {
    InMemoryJobService service = createJobService();
    GrpcFnServer<InMemoryJobService> jobServiceGrpcFnServer;
    if (configuration.port == 0) {
      jobServiceGrpcFnServer = GrpcFnServer.allocatePortAndCreateFor(service, jobServerFactory);
    } else {
      Endpoints.ApiServiceDescriptor descriptor =
          Endpoints.ApiServiceDescriptor.newBuilder()
              .setUrl(configuration.host + ":" + configuration.port)
              .build();
      jobServiceGrpcFnServer = GrpcFnServer.create(service, descriptor, jobServerFactory);
    }
    LOG.info("JobService started on {}", jobServiceGrpcFnServer.getApiServiceDescriptor().getUrl());
    return jobServiceGrpcFnServer;
  }

  private GrpcFnServer<ArtifactStagingService> createArtifactStagingService() throws IOException {
    ArtifactStagingService service =
        new ArtifactStagingService(
            ArtifactStagingService.beamFilesystemArtifactDestinationProvider(
                configuration.artifactStagingPath));
    GrpcFnServer<ArtifactStagingService> server;
    if (configuration.artifactPort == 0) {
      server = GrpcFnServer.allocatePortAndCreateFor(service, artifactServerFactory);
    } else {
      Endpoints.ApiServiceDescriptor descriptor =
          Endpoints.ApiServiceDescriptor.newBuilder()
              .setUrl(configuration.host + ":" + configuration.artifactPort)
              .build();
      server = GrpcFnServer.create(service, descriptor, artifactServerFactory);
    }
    LOG.info("ArtifactStagingService started on {}", server.getApiServiceDescriptor().getUrl());
    return server;
  }

  @Nullable
  private ExpansionServer createExpansionService() throws IOException {
    // Skip creating expansion server if configured port is negative.
    if (configuration.expansionPort < 0) {
      return null;
    }

    ExpansionServer expansionServer =
        ExpansionServer.create(
            new ExpansionService(), configuration.host, configuration.expansionPort);
    LOG.info(
        "Java ExpansionService started on {}:{}",
        expansionServer.getHost(),
        expansionServer.getPort());
    return expansionServer;
  }
}
