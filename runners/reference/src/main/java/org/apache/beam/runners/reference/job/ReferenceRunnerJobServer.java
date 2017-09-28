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

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/** A program that runs a {@link ReferenceRunnerJobService}. */
public class ReferenceRunnerJobServer {
  public static void main(String[] args) throws IOException, InterruptedException {
    ServerConfiguration configuration = new ServerConfiguration();
    try {
      new CmdLineParser(configuration).parseArgument(args);
    } catch (CmdLineException e) {
      throw new IllegalArgumentException(e);
    }
    runServer(configuration);
  }

  private static void runServer(ServerConfiguration configuration) throws IOException, InterruptedException {
    ReferenceRunnerJobService service = ReferenceRunnerJobService.create();
    Server server = ServerBuilder.forPort(configuration.port).addService(service).build();
    server.start();
    server.awaitTermination();
  }

  private static class ServerConfiguration {
    @Option(
      name = "p",
      aliases = {"port"},
      required = true,
      usage = "The local port to expose the server on"
    )
    private int port = -1;
  }
}
