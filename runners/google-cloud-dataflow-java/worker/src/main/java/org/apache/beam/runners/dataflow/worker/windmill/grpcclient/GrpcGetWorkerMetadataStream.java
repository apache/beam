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
package org.apache.beam.runners.dataflow.worker.windmill.grpcclient;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.io.PrintWriter;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.windmill.AbstractWindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.StreamObserverFactory;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkerMetadataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkerMetadataResponse;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillEndpoints;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillStream.GetWorkerMetadataStream;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class GrpcGetWorkerMetadataStream
    extends AbstractWindmillStream<WorkerMetadataRequest, WorkerMetadataResponse>
    implements GetWorkerMetadataStream {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcGetWorkerMetadataStream.class);
  private static final WorkerMetadataRequest HEALTH_CHECK_REQUEST =
      WorkerMetadataRequest.getDefaultInstance();
  private final WorkerMetadataRequest workerMetadataRequest;
  private final ThrottleTimer getWorkerMetadataThrottleTimer;
  private final Consumer<WindmillEndpoints> serverMappingConsumer;
  private final Object metadataLock;

  @GuardedBy("metadataLock")
  private long metadataVersion;

  @GuardedBy("metadataLock")
  private WorkerMetadataResponse latestResponse;

  private GrpcGetWorkerMetadataStream(
      Function<StreamObserver<WorkerMetadataResponse>, StreamObserver<WorkerMetadataRequest>>
          startGetWorkerMetadataRpcFn,
      BackOff backoff,
      StreamObserverFactory streamObserverFactory,
      Set<AbstractWindmillStream<?, ?>> streamRegistry,
      int logEveryNStreamFailures,
      JobHeader jobHeader,
      long metadataVersion,
      ThrottleTimer getWorkerMetadataThrottleTimer,
      Consumer<WindmillEndpoints> serverMappingConsumer) {
    super(
        startGetWorkerMetadataRpcFn,
        backoff,
        streamObserverFactory,
        streamRegistry,
        logEveryNStreamFailures);
    this.workerMetadataRequest = WorkerMetadataRequest.newBuilder().setHeader(jobHeader).build();
    this.metadataVersion = metadataVersion;
    this.getWorkerMetadataThrottleTimer = getWorkerMetadataThrottleTimer;
    this.serverMappingConsumer = serverMappingConsumer;
    this.latestResponse = WorkerMetadataResponse.getDefaultInstance();
    this.metadataLock = new Object();
  }

  public static GrpcGetWorkerMetadataStream create(
      Function<StreamObserver<WorkerMetadataResponse>, StreamObserver<WorkerMetadataRequest>>
          startGetWorkerMetadataRpcFn,
      BackOff backoff,
      StreamObserverFactory streamObserverFactory,
      Set<AbstractWindmillStream<?, ?>> streamRegistry,
      int logEveryNStreamFailures,
      JobHeader jobHeader,
      int metadataVersion,
      ThrottleTimer getWorkerMetadataThrottleTimer,
      Consumer<WindmillEndpoints> serverMappingUpdater) {
    GrpcGetWorkerMetadataStream getWorkerMetadataStream =
        new GrpcGetWorkerMetadataStream(
            startGetWorkerMetadataRpcFn,
            backoff,
            streamObserverFactory,
            streamRegistry,
            logEveryNStreamFailures,
            jobHeader,
            metadataVersion,
            getWorkerMetadataThrottleTimer,
            serverMappingUpdater);
    getWorkerMetadataStream.startStream();
    return getWorkerMetadataStream;
  }

  /**
   * Each instance of {@link AbstractWindmillStream} owns its own responseObserver that calls
   * onResponse().
   */
  @Override
  protected void onResponse(WorkerMetadataResponse response) {
    extractWindmillEndpointsFrom(response).ifPresent(serverMappingConsumer);
  }

  /**
   * Acquires the {@link #metadataLock} Returns {@link Optional<WindmillEndpoints>} if the
   * metadataVersion in the response is not stale (older or equal to {@link #metadataVersion}), else
   * returns empty {@link Optional}.
   */
  private Optional<WindmillEndpoints> extractWindmillEndpointsFrom(
      WorkerMetadataResponse response) {
    synchronized (metadataLock) {
      if (response.getMetadataVersion() > this.metadataVersion) {
        this.metadataVersion = response.getMetadataVersion();
        this.latestResponse = response;
        return Optional.of(WindmillEndpoints.from(response));
      } else {
        // If the currentMetadataVersion is greater than or equal to one in the response, the
        // response data is stale, and we do not want to do anything.
        LOG.info(
            "Received WorkerMetadataResponse={}; Received metadata version={}; Current metadata version={}. "
                + "Skipping update because received stale metadata",
            response,
            response.getMetadataVersion(),
            this.metadataVersion);
      }
    }

    return Optional.empty();
  }

  @Override
  protected synchronized void onNewStream() {
    send(workerMetadataRequest);
  }

  @Override
  protected boolean hasPendingRequests() {
    return false;
  }

  @Override
  protected void startThrottleTimer() {
    getWorkerMetadataThrottleTimer.start();
  }

  @Override
  protected void sendHealthCheck() {
    send(HEALTH_CHECK_REQUEST);
  }

  @Override
  protected void appendSpecificHtml(PrintWriter writer) {
    synchronized (metadataLock) {
      writer.format(
          "GetWorkerMetadataStream: version=[%d] , job_header=[%s], latest_response=[%s]",
          this.metadataVersion, workerMetadataRequest.getHeader(), this.latestResponse);
    }
  }
}
