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

/** A program that runs a {@link ReferenceRunnerJobService}. */
public class ReferenceRunnerJobServer {
  public static void main(String[] args) throws IOException, InterruptedException {
    if (args.length != 1) {
      System.out.println(
          String.format("Usage: %s [Port]", ReferenceRunnerJobServer.class.getSimpleName()));
      System.exit(1);
    }
    ReferenceRunnerJobService service = ReferenceRunnerJobService.create();
    int port = Integer.parseInt(args[0]);
    Server server = ServerBuilder.forPort(port).addService(service).build();
    server.start();
    server.awaitTermination();
  }
}
