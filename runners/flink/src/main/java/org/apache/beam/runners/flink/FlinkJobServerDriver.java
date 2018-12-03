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
package org.apache.beam.runners.flink;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.artifact.BeamFileSystemArtifactStagingService;
import org.apache.beam.runners.fnexecution.jobsubmission.InMemoryJobService;
import org.apache.beam.runners.fnexecution.jobsubmission.JobInvoker;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Driver program that starts a job server. */
public class FlinkJobServerDriver implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkJobServerDriver.class);

  private final ListeningExecutorService executor;
  @VisibleForTesting ServerConfiguration configuration;
  private final ServerFactory jobServerFactory;
  private final ServerFactory artifactServerFactory;
  private GrpcFnServer<InMemoryJobService> jobServer;
  private GrpcFnServer<BeamFileSystemArtifactStagingService> artifactStagingServer;

  /** Configuration for the jobServer. */
  public static class ServerConfiguration {
    @Option(name = "--job-host", usage = "The job server host name")
    String host = "localhost";

    @Option(
      name = "--job-port",
      usage = "The job service port. 0 to use a dynamic port. (Default: 8099)"
    )
    int port = 8099;

    @Option(
      name = "--artifact-port",
      usage = "The artifact service port. 0 to use a dynamic port. (Default: 8098)"
    )
    int artifactPort = 8098;

    @Option(name = "--artifacts-dir", usage = "The location to store staged artifact files")
    String artifactStagingPath =
        Paths.get(System.getProperty("java.io.tmpdir"), "beam-artifact-staging").toString();

    @Option(
      name = "--clean-artifacts-per-job",
      usage = "When true, remove each job's staged artifacts when it completes"
    )
    boolean cleanArtifactsPerJob = false;

    @Option(name = "--flink-master-url", usage = "Flink master url to submit job.")
    String flinkMasterUrl = "[auto]";

    String getFlinkMasterUrl() {
      return this.flinkMasterUrl;
    }

    @Option(
      name = "--sdk-worker-parallelism",
      usage = "Default parallelism for SDK worker processes (see portable pipeline options)"
    )
    Long sdkWorkerParallelism = 1L;

    Long getSdkWorkerParallelism() {
      return this.sdkWorkerParallelism;
    }

    @Option(
      name = "--flink-conf-dir",
      usage =
          "Directory containing Flink YAML configuration files. "
              + "These properties will be set to all jobs submitted to Flink and take precedence "
              + "over configurations in FLINK_CONF_DIR."
    )
    String flinkConfDir = null;

    @Nullable
    String getFlinkConfDir() {
      return flinkConfDir;
    }
  }

  public static void main(String[] args) throws Exception {
    //TODO: Expose the fileSystem related options.
    // Register standard file systems.
    FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create());
    fromParams(args).run();
  }

  private static void printUsage(CmdLineParser parser) {
    System.err.println(
        String.format("Usage: java %s arguments...", FlinkJobServerDriver.class.getSimpleName()));
    parser.printUsage(System.err);
    System.err.println();
  }

  public static FlinkJobServerDriver fromParams(String[] args) {
    ServerConfiguration configuration = new ServerConfiguration();
    CmdLineParser parser = new CmdLineParser(configuration);
    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      LOG.error("Unable to parse command line arguments.", e);
      printUsage(parser);
      throw new IllegalArgumentException("Unable to parse command line arguments.", e);
    }

    return fromConfig(configuration);
  }

  public static FlinkJobServerDriver fromConfig(ServerConfiguration configuration) {
    ThreadFactory threadFactory =
        new ThreadFactoryBuilder().setNameFormat("flink-runner-job-server").setDaemon(true).build();
    ListeningExecutorService executor =
        MoreExecutors.listeningDecorator(Executors.newCachedThreadPool(threadFactory));
    ServerFactory jobServerFactory = ServerFactory.createWithPortSupplier(() -> configuration.port);
    ServerFactory artifactServerFactory =
        ServerFactory.createWithPortSupplier(() -> configuration.artifactPort);
    return create(configuration, executor, jobServerFactory, artifactServerFactory);
  }

  public static FlinkJobServerDriver create(
      ServerConfiguration configuration,
      ListeningExecutorService executor,
      ServerFactory jobServerFactory,
      ServerFactory artifactServerFactory) {
    return new FlinkJobServerDriver(
        configuration, executor, jobServerFactory, artifactServerFactory);
  }

  private FlinkJobServerDriver(
      ServerConfiguration configuration,
      ListeningExecutorService executor,
      ServerFactory jobServerFactory,
      ServerFactory artifactServerFactory) {
    this.configuration = configuration;
    this.executor = executor;
    this.jobServerFactory = jobServerFactory;
    this.artifactServerFactory = artifactServerFactory;
  }

  @Override
  public void run() {
    try {
      jobServer = createJobServer();
      jobServer.getServer().awaitTermination();
    } catch (InterruptedException e) {
      LOG.warn("Job server interrupted", e);
    } catch (Exception e) {
      LOG.warn("Exception during job server creation", e);
    } finally {
      stop();
    }
  }

  public String start() throws IOException {
    jobServer = createJobServer();
    return jobServer.getApiServiceDescriptor().getUrl();
  }

  public void stop() {
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

  private InMemoryJobService createJobService() throws IOException {
    artifactStagingServer = createArtifactStagingService();
    JobInvoker invoker = createJobInvoker();
    return InMemoryJobService.create(
        artifactStagingServer.getApiServiceDescriptor(),
        (String session) -> {
          try {
            return BeamFileSystemArtifactStagingService.generateStagingSessionToken(
                session, configuration.artifactStagingPath);
          } catch (Exception exn) {
            throw new RuntimeException(exn);
          }
        },
        (String stagingSessionToken) -> {
          if (configuration.cleanArtifactsPerJob) {
            artifactStagingServer.getService().removeArtifacts(stagingSessionToken);
          }
        },
        invoker);
  }

  private GrpcFnServer<BeamFileSystemArtifactStagingService> createArtifactStagingService()
      throws IOException {
    BeamFileSystemArtifactStagingService service = new BeamFileSystemArtifactStagingService();
    final GrpcFnServer<BeamFileSystemArtifactStagingService> artifactStagingService;
    if (configuration.artifactPort == 0) {
      artifactStagingService =
          GrpcFnServer.allocatePortAndCreateFor(service, artifactServerFactory);
    } else {
      Endpoints.ApiServiceDescriptor descriptor =
          Endpoints.ApiServiceDescriptor.newBuilder()
              .setUrl(configuration.host + ":" + configuration.artifactPort)
              .build();
      artifactStagingService = GrpcFnServer.create(service, descriptor, artifactServerFactory);
    }
    LOG.info(
        "ArtifactStagingService started on {}",
        artifactStagingService.getApiServiceDescriptor().getUrl());
    return artifactStagingService;
  }

  private JobInvoker createJobInvoker() {
    return FlinkJobInvoker.create(executor, configuration);
  }
}
