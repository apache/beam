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
package org.apache.beam.sdk.transformservice;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ServerBuilder;

/**
 * A component that controlls the transform service.
 *
 * <p>Forwards the operations to underlying transform services as needed. This class is expected to
 * be used along with the wrapping container which is a part of an overall transform service. For
 * more details please see https://s.apache.org/beam-transform-service.
 */
public class Controller {

  List<Endpoints.ApiServiceDescriptor> endpoints;

  private final TransformServiceOptions options;

  public Controller(String[] args) {
    // We use PipelineOptions just a library for parsing arguments here.
    this(PipelineOptionsFactory.fromArgs(args).create());
  }

  public Controller(PipelineOptions opts) {
    this.options = opts.as(TransformServiceOptions.class);
    endpoints = new ArrayList<>();
    for (String expansionService : options.getTransformServiceConfig().getExpansionservices()) {
      endpoints.add(Endpoints.ApiServiceDescriptor.newBuilder().setUrl(expansionService).build());
    }
  }

  private void start() throws Exception {
    ExpansionService expansionService = new ExpansionService(endpoints, null);
    ArtifactService artifactService = new ArtifactService(endpoints, null);

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
