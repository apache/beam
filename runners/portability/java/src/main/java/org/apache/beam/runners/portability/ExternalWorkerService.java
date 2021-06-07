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
package org.apache.beam.runners.portability;

import java.util.Collections;
import java.util.function.Function;
import org.apache.beam.fn.harness.FnHarness;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StartWorkerRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StartWorkerResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StopWorkerRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StopWorkerResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnExternalWorkerPoolGrpc.BeamFnExternalWorkerPoolImplBase;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.fnexecution.FnService;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.vendor.grpc.v1p36p0.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the BeamFnExternalWorkerPool service by starting a fresh SDK harness for each request.
 */
public class ExternalWorkerService extends BeamFnExternalWorkerPoolImplBase implements FnService {

  private static final Logger LOG = LoggerFactory.getLogger(ExternalWorkerService.class);
  private static final String PIPELINE_OPTIONS = "PIPELINE_OPTIONS";

  private final PipelineOptions options;
  private final ServerFactory serverFactory = ServerFactory.createDefault();

  public ExternalWorkerService(PipelineOptions options) {
    this.options = options;
  }

  @Override
  public void startWorker(
      StartWorkerRequest request, StreamObserver<StartWorkerResponse> responseObserver) {
    LOG.info(
        "Starting worker {} pointing at {}.",
        request.getWorkerId(),
        request.getControlEndpoint().getUrl());
    LOG.debug("Worker request {}.", request);
    Thread th =
        new Thread(
            () -> {
              try {
                FnHarness.main(
                    request.getWorkerId(),
                    options,
                    Collections.emptySet(),
                    request.getLoggingEndpoint(),
                    request.getControlEndpoint(),
                    null);
                LOG.info("Successfully started worker {}.", request.getWorkerId());
              } catch (Exception exn) {
                LOG.error(String.format("Failed to start worker %s.", request.getWorkerId()), exn);
              }
            });
    th.setName("SDK-worker-" + request.getWorkerId());
    th.setDaemon(true);
    th.start();

    responseObserver.onNext(StartWorkerResponse.newBuilder().build());
    responseObserver.onCompleted();
  }

  @Override
  public void stopWorker(
      StopWorkerRequest request, StreamObserver<StopWorkerResponse> responseObserver) {
    // Thread based workers terminate automatically
    responseObserver.onNext(StopWorkerResponse.newBuilder().build());
    responseObserver.onCompleted();
  }

  @Override
  public void close() {}

  public GrpcFnServer<ExternalWorkerService> start() throws Exception {
    final String externalServiceAddress =
        Environments.getExternalServiceAddress(options.as(PortablePipelineOptions.class));
    GrpcFnServer<ExternalWorkerService> server;
    if (externalServiceAddress.isEmpty()) {
      server = GrpcFnServer.allocatePortAndCreateFor(this, serverFactory);
    } else {
      server =
          GrpcFnServer.create(
              this,
              Endpoints.ApiServiceDescriptor.newBuilder().setUrl(externalServiceAddress).build(),
              serverFactory);
    }
    LOG.debug(
        "Listening for worker start requests at {}.", server.getApiServiceDescriptor().getUrl());
    return server;
  }

  /**
   * Worker pool entry point.
   *
   * <p>The worker pool exposes an RPC service that is used with EXTERNAL environment to start and
   * stop the SDK workers.
   *
   * <p>The worker pool uses threads for parallelism;
   *
   * <p>This entry point is used by the Java SDK container in worker pool mode.
   */
  public static void main(String[] args) throws Exception {
    main(System::getenv);
  }

  public static void main(Function<String, String> environmentVarGetter) throws Exception {
    System.out.format("Starting external worker service%n");
    System.out.format("Pipeline options %s%n", environmentVarGetter.apply(PIPELINE_OPTIONS));
    PipelineOptions options =
        PipelineOptionsTranslation.fromJson(environmentVarGetter.apply(PIPELINE_OPTIONS));

    try (GrpcFnServer<ExternalWorkerService> server = new ExternalWorkerService(options).start()) {
      System.out.format(
          "External worker service started at address: %s",
          server.getApiServiceDescriptor().getUrl());
      while (true) {
        // Wait indefinitely to keep ExternalWorkerService running
        Sleeper.DEFAULT.sleep(60 * 60 * 24 * 1000);
      }
    } catch (Exception e) {
      System.out.println("Error running worker service:\n" + e);
    } finally {
      System.out.println("External worker service stopped.");
    }
  }
}
