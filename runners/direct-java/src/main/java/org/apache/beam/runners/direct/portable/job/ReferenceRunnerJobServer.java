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
package org.apache.beam.runners.direct.portable.job;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A program that runs a {@link ReferenceRunnerJobService}. */
public class ReferenceRunnerJobServer {
  private static final Logger LOG = LoggerFactory.getLogger(ReferenceRunnerJobServer.class);
  private final ServerConfiguration configuration;
  private GrpcFnServer<ReferenceRunnerJobService> server;

  private ReferenceRunnerJobServer(ServerConfiguration configuration) {
    this.configuration = configuration;
  }

  public static void main(String[] args) throws Exception {
    try {
      runServer(parseConfiguration(args));
    } catch (CmdLineException ignored) {
    }
  }

  private static ServerConfiguration parseConfiguration(String[] args) throws CmdLineException {
    ServerConfiguration configuration = new ServerConfiguration();
    CmdLineParser parser = new CmdLineParser(configuration);
    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      e.printStackTrace(System.err);
      printUsage(parser);
      throw e;
    }
    return configuration;
  }

  private static void printUsage(CmdLineParser parser) {
    System.err.println(
        String.format(
            "Usage: java %s arguments...", ReferenceRunnerJobService.class.getSimpleName()));
    parser.printUsage(System.err);
    System.err.println();
  }

  private static void runServer(ServerConfiguration configuration) throws Exception {
    ServerFactory serverFactory = ServerFactory.createDefault();
    ReferenceRunnerJobService.Configuration jobServiceConfig =
        createJobServiceConfig(configuration);
    ReferenceRunnerJobService service =
        ReferenceRunnerJobService.create(serverFactory, jobServiceConfig);
    try (GrpcFnServer<ReferenceRunnerJobService> server =
        createServer(configuration, serverFactory, service)) {
      System.out.println(
          String.format(
              "Started %s at %s",
              ReferenceRunnerJobService.class.getSimpleName(),
              server.getApiServiceDescriptor().getUrl()));
      server.getServer().awaitTermination();
    }
    System.out.println("Server shut down, exiting");
  }

  public static ReferenceRunnerJobServer fromParams(String[] args) {
    try {
      return new ReferenceRunnerJobServer(parseConfiguration(args));
    } catch (CmdLineException e) {
      throw new IllegalArgumentException(
          "Unable to parse command line arguments " + Arrays.asList(args), e);
    }
  }

  public String start() throws Exception {
    ServerFactory serverFactory = ServerFactory.createDefault();
    ReferenceRunnerJobService.Configuration jobServiceConfig =
        createJobServiceConfig(configuration);
    server =
        createServer(
            configuration,
            serverFactory,
            ReferenceRunnerJobService.create(serverFactory, jobServiceConfig));

    return server.getApiServiceDescriptor().getUrl();
  }

  public void stop() {
    if (server != null) {
      try {
        server.close();
      } catch (Exception e) {
        LOG.error("Unable to stop job server.", e);
      }
    }
  }

  private static GrpcFnServer<ReferenceRunnerJobService> createServer(
      ServerConfiguration configuration,
      ServerFactory serverFactory,
      ReferenceRunnerJobService service)
      throws IOException {
    if (configuration.port <= 0) {
      return GrpcFnServer.allocatePortAndCreateFor(service, serverFactory);
    }
    return GrpcFnServer.create(
        service,
        ApiServiceDescriptor.newBuilder().setUrl("localhost:" + configuration.port).build(),
        serverFactory);
  }

  /**
   * Helper function to fill out a {@code ReferenceRunnerJobService.Configuration Configuration}
   * object for {@code ReferenceRunnerJobService}.
   */
  private static ReferenceRunnerJobService.Configuration createJobServiceConfig(
      ServerConfiguration configuration) {
    ReferenceRunnerJobService.Configuration jobServiceConfig =
        new ReferenceRunnerJobService.Configuration();
    jobServiceConfig.artifactStagingPath = configuration.artifactStagingPath;
    jobServiceConfig.keepArtifacts = configuration.keepArtifacts;
    return jobServiceConfig;
  }

  /** Command-line options to configure the JobServer. */
  public static class ServerConfiguration {
    @Option(
        name = "-p",
        aliases = {"--port"},
        usage = "The local port to expose the server on. 0 to use a dynamic port. (Default: 8099)")
    private int port = 8099;

    @Option(name = "--artifacts-dir", usage = "The location to store staged artifact files")
    String artifactStagingPath =
        Paths.get(System.getProperty("java.io.tmpdir"), "beam-artifact-staging").toString();

    @Option(
        name = "--keep-artifacts",
        usage = "When enabled, do not delete staged artifacts when a job completes")
    boolean keepArtifacts;
  }
}
