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

package org.apache.beam.runners.reference.job;

import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/** A program that runs a {@link ReferenceRunnerJobService}. */
public class ReferenceRunnerJobServer {

  public static void main(String[] args) throws Exception {
    ServerConfiguration configuration = new ServerConfiguration();
    CmdLineParser parser = new CmdLineParser(configuration);
    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      e.printStackTrace(System.err);
      printUsage(parser);
      return;
    }
    runServer(configuration);
  }

  private static void printUsage(CmdLineParser parser) {
    System.err.println(
        String.format(
            "Usage: java %s arguments...", ReferenceRunnerJobService.class.getSimpleName()));
    parser.printUsage(System.err);
    System.err.println();
  }

  private static void runServer(ServerConfiguration configuration)
      throws Exception {
    ServerFactory serverFactory = ServerFactory.createDefault();
    ReferenceRunnerJobService service = ReferenceRunnerJobService.create(serverFactory);
    try (GrpcFnServer<ReferenceRunnerJobService> server =
        GrpcFnServer.create(
            service,
            ApiServiceDescriptor.newBuilder().setUrl("localhost:" + configuration.port).build(),
            serverFactory)) {
      System.out.println(
          String.format(
              "Started %s on port %s",
              ReferenceRunnerJobService.class.getSimpleName(), configuration.port));
      server.getServer().awaitTermination();
    }
    System.out.println("Server shut down, exiting");
  }

  private static class ServerConfiguration {
    @Option(
      name = "-p",
      aliases = {"--port"},
      required = true,
      usage = "The local port to expose the server on"
    )
    private int port = -1;
  }
}
