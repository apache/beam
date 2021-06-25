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
package org.apache.beam.fn.harness;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StartWorkerRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StartWorkerResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StopWorkerRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StopWorkerResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnExternalWorkerPoolGrpc.BeamFnExternalWorkerPoolImplBase;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.sdk.fn.ProcessManager;
import org.apache.beam.sdk.fn.server.FnService;
import org.apache.beam.sdk.fn.server.GrpcFnServer;
import org.apache.beam.sdk.fn.server.ServerFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.grpc.v1p36p0.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the BeamFnExternalWorkerPool service by starting a fresh SDK harness for each request.
 */
public class ExternalWorkerService extends BeamFnExternalWorkerPoolImplBase implements FnService {

  private static final Logger LOG = LoggerFactory.getLogger(ExternalWorkerService.class);
  private static final String PIPELINE_OPTIONS_ENV_VAR = "PIPELINE_OPTIONS";

  private final PipelineOptions options;
  private final String processCommand;
  private final ProcessManager manager = ProcessManager.create();
  private final ServerFactory serverFactory = ServerFactory.createDefault();

  public ExternalWorkerService(PipelineOptions options) {
    this.options = options;
    this.processCommand = Environments.getExternalServiceExecutable(options.as(PortablePipelineOptions.class));
  }

  @Override
  public void startWorker(
      StartWorkerRequest request, StreamObserver<StartWorkerResponse> responseObserver) {
    LOG.info(
        "Starting worker {} pointing at {}.",
        request.getWorkerId(),
        request.getControlEndpoint().getUrl());
    LOG.debug("Worker request {}.", request);

    if (StringUtils.isNotBlank(processCommand)) {
      startWorkerProcess(request, responseObserver);
    } else {
      startWorkerThread(request, responseObserver);
    }
  }

  @Override
  public void stopWorker(
      StopWorkerRequest request, StreamObserver<StopWorkerResponse> responseObserver) {
    if (StringUtils.isNotBlank(processCommand)) {
      manager.stopProcess("SDK-worker-" + request.getWorkerId());
    }

    // Thread based workers terminate automatically
    responseObserver.onNext(StopWorkerResponse.newBuilder().build());
    responseObserver.onCompleted();
  }

  private void startWorkerProcess(
      StartWorkerRequest request, StreamObserver<StartWorkerResponse> responseObserver) {
    List<String> arguments = ImmutableList.<String>builder()
        .add(String.format("--id=%s", request.getWorkerId()))
        .add(String.format("--logging_endpoint=%s", request.getLoggingEndpoint().getUrl()))
        .add(String.format("--artifact_endpoint=%s", request.getArtifactEndpoint().getUrl()))
        .add(String.format("--provision_endpoint=%s", request.getProvisionEndpoint().getUrl()))
        .add(String.format("--control_endpoint=%s", request.getControlEndpoint().getUrl()))
        .build();
    Map<String, String> env = ImmutableMap.<String, String>builder()
        .put(PIPELINE_OPTIONS_ENV_VAR, PipelineOptionsTranslation.toJson(options))
        .build();

    try {
      manager.startProcess(
          "SDK-worker-" + request.getWorkerId(), processCommand, arguments, env);
      LOG.info("Successfully started worker {}.", request.getWorkerId());
      responseObserver.onNext(StartWorkerResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      try {
        manager.stopProcess(request.getWorkerId());
      } catch (Exception processKillException) {
        e.addSuppressed(processKillException);
      }
      responseObserver.onError(e);
    }
  }

  private void startWorkerThread(
      StartWorkerRequest request, StreamObserver<StartWorkerResponse> responseObserver) {
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
   * <p>This entry point is used by the Java SDK container in worker pool mode and expects the
   * following environment variables:
   *
   * <ul>
   *   <li>PIPELINE_OPTIONS: A serialized form of {@link PipelineOptions}. It needs to be known
   *       up-front and matches the running job. See {@link PipelineOptions} for further details.
   * </ul>
   */
  public static void main(String[] args) {
    LOG.info("Starting external worker service");
    final String optionsEnv =
        checkArgumentNotNull(
            System.getenv(PIPELINE_OPTIONS_ENV_VAR),
            "No pipeline options provided in environment variables " + PIPELINE_OPTIONS_ENV_VAR);
    LOG.info("Pipeline options {}", optionsEnv);
    PipelineOptions options = PipelineOptionsTranslation.fromJson(optionsEnv);

    try (GrpcFnServer<ExternalWorkerService> server = new ExternalWorkerService(options).start()) {
      LOG.info(
          "External worker service started at address: {}",
          server.getApiServiceDescriptor().getUrl());
      // Wait to keep ExternalWorkerService running
      Sleeper.DEFAULT.sleep(Long.MAX_VALUE);
    } catch (Exception e) {
      LOG.error("Error running worker service", e);
    } finally {
      LOG.info("External worker service stopped.");
    }
  }
}
