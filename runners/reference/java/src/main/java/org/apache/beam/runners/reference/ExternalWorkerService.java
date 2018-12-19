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
package org.apache.beam.runners.reference;

import org.apache.beam.fn.harness.FnHarness;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.NotifyRunnerAvailableRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.NotifyRunnerAvailableResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnExternalWorkerPoolGrpc.BeamFnExternalWorkerPoolImplBase;
import org.apache.beam.runners.fnexecution.FnService;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.grpc.v1_13_1.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the BeamFnExternalWorkerPool service by starting a fresh SDK harness for each request.
 */
public class ExternalWorkerService extends BeamFnExternalWorkerPoolImplBase implements FnService {

  private static final Logger LOG = LoggerFactory.getLogger(BeamFnExternalWorkerPoolImplBase.class);

  private final PipelineOptions options;
  private final ServerFactory serverFactory = ServerFactory.createDefault();

  public ExternalWorkerService(PipelineOptions options) throws Exception {
    this.options = options;
  }

  @Override
  public void notifyRunnerAvailable(
      NotifyRunnerAvailableRequest request,
      StreamObserver<NotifyRunnerAvailableResponse> responseObserver) {
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
                    request.getLoggingEndpoint(),
                    request.getControlEndpoint());
                LOG.info("Successfully started worker {}.", request.getWorkerId());
              } catch (Exception exn) {
                LOG.error(String.format("Failed to start worker %s.", request.getWorkerId()), exn);
              }
            });
    th.setName("SDK-worker-" + request.getWorkerId());
    th.setDaemon(true);
    th.start();

    responseObserver.onNext(NotifyRunnerAvailableResponse.newBuilder().build());
    responseObserver.onCompleted();
  }

  @Override
  public void close() {}

  public GrpcFnServer<ExternalWorkerService> start() throws Exception {
    GrpcFnServer<ExternalWorkerService> server =
        GrpcFnServer.allocatePortAndCreateFor(this, serverFactory);
    LOG.debug(
        "Listening for worker start requests at {}.", server.getApiServiceDescriptor().getUrl());
    return server;
  }
}
