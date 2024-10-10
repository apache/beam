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
import java.util.Set;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StartWorkerRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StartWorkerResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StopWorkerRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StopWorkerResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnExternalWorkerPoolGrpc.BeamFnExternalWorkerPoolImplBase;
import org.apache.beam.model.fnexecution.v1.ProvisionApi;
import org.apache.beam.model.fnexecution.v1.ProvisionServiceGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.fn.channel.AddHarnessIdInterceptor;
import org.apache.beam.sdk.fn.channel.ManagedChannelFactory;
import org.apache.beam.sdk.fn.server.FnService;
import org.apache.beam.sdk.fn.server.GrpcFnServer;
import org.apache.beam.sdk.fn.server.ServerFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.PipelineOptionsTranslation;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the BeamFnExternalWorkerPool service by starting a fresh SDK harness for each request.
 */
public class ExternalWorkerService extends BeamFnExternalWorkerPoolImplBase implements FnService {

  private static final Logger LOG = LoggerFactory.getLogger(ExternalWorkerService.class);
  private static final String PIPELINE_OPTIONS_ENV_VAR = "PIPELINE_OPTIONS";

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

    Endpoints.ApiServiceDescriptor loggingEndpoint = request.getLoggingEndpoint();
    Endpoints.ApiServiceDescriptor controlEndpoint = request.getControlEndpoint();
    Set<String> runnerCapabilites = Collections.emptySet();
    if (request.hasProvisionEndpoint()) {
      ManagedChannelFactory channelFactory =
          ManagedChannelFactory.createDefault()
              .withInterceptors(
                  ImmutableList.of(AddHarnessIdInterceptor.create(request.getWorkerId())));

      ProvisionServiceGrpc.ProvisionServiceBlockingStub provisionStub =
          ProvisionServiceGrpc.newBlockingStub(
              channelFactory.forDescriptor(request.getProvisionEndpoint()));
      ProvisionApi.ProvisionInfo provisionInfo =
          provisionStub
              .getProvisionInfo(ProvisionApi.GetProvisionInfoRequest.newBuilder().build())
              .getInfo();

      runnerCapabilites = Sets.newHashSet(provisionInfo.getRunnerCapabilitiesList());
      if (provisionInfo.hasControlEndpoint()) {
        controlEndpoint = provisionInfo.getControlEndpoint();
      }
      if (provisionInfo.hasLoggingEndpoint()) {
        loggingEndpoint = provisionInfo.getLoggingEndpoint();
      }
    }
    // Lambda closured variables must be final.
    final Endpoints.ApiServiceDescriptor logEndpoint = loggingEndpoint;
    final Endpoints.ApiServiceDescriptor ctrlEndpoint = controlEndpoint;
    final Set<String> capabilities = runnerCapabilites;
    Thread th =
        new Thread(
            () -> {
              try {
                FnHarness.main(
                    request.getWorkerId(), options, capabilities, logEndpoint, ctrlEndpoint, null);
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
