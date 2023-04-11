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
package org.apache.beam.transformservice.controller;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.grpc.v1p48p1.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p48p1.io.grpc.ServerBuilder;

@SuppressWarnings("nullness")
public class Controller {

  List<Endpoints.ApiServiceDescriptor> endpoints;

  private final TransformServiceOptions options;

  public Controller(String[] args) {
    // We use PipelineOptions just a library for parsing arguments here.
    this(PipelineOptionsFactory.fromArgs(args).create());
    endpoints = new ArrayList<>();
    for (String expansionService : options.getTransformServiceConfig().getExpansionservices()) {
      endpoints.add(Endpoints.ApiServiceDescriptor.newBuilder().setUrl(expansionService).build());
    }
  }

  public Controller(PipelineOptions opts) {
    this.options = opts.as(TransformServiceOptions.class);
  }

  private void start() throws Exception {
    ExpansionService expansionService = new ExpansionService(endpoints);
    ArtifactService artifactService = new ArtifactService(endpoints);

    System.out.println("Starting transform service at port: " + options.getPort());

    Server server =
        ServerBuilder.forPort(options.getPort())
            .addService(expansionService)
            .addService(artifactService)
            .build();
    server.start();
    server.awaitTermination();
  }

  public static void main(String[] args) throws Exception {
    PipelineOptionsFactory.register(TransformServiceOptions.class);

    Controller controller = new Controller(args);
    controller.start();
  }
}
