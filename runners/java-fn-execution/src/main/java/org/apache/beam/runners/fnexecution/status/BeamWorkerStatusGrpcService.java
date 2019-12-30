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
package org.apache.beam.runners.fnexecution.status;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnWorkerStatusGrpc.BeamFnWorkerStatusImplBase;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.runners.fnexecution.FnService;
import org.apache.beam.runners.fnexecution.HeaderAccessor;
import org.apache.beam.sdk.util.MoreFutures;
import org.apache.beam.vendor.grpc.v1p21p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Fn Status service which can collect run-time status information from SDK Harnesses for
 * debugging purpose.
 */
public class BeamWorkerStatusGrpcService extends BeamFnWorkerStatusImplBase implements FnService {

  static final String DEFAULT_ERROR_RESPONSE = "Error getting status from SDK harness";

  private static final Logger LOG = LoggerFactory.getLogger(BeamWorkerStatusGrpcService.class);
  private static final long DEFAULT_CLIENT_CONNECTION_WAIT_TIME_SECONDS = 5;
  private final HeaderAccessor headerAccessor;
  private final Map<String, CompletableFuture<WorkerStatusClient>> connectedClient =
      new ConcurrentHashMap<>();

  private BeamWorkerStatusGrpcService(
      ApiServiceDescriptor apiServiceDescriptor, HeaderAccessor headerAccessor) {
    this.headerAccessor = headerAccessor;
    LOG.info("Launched Beam Fn Status service at {}", apiServiceDescriptor);
  }

  /**
   * Create new instance of {@link BeamWorkerStatusGrpcService}.
   *
   * @param apiServiceDescriptor describes the configuration for the endpoint the server will
   *     expose.
   * @param headerAccessor headerAccessor gRPC header accessor used to obtain SDK harness worker id.
   * @return {@link BeamWorkerStatusGrpcService}
   */
  public static BeamWorkerStatusGrpcService create(
      ApiServiceDescriptor apiServiceDescriptor, HeaderAccessor headerAccessor) {
    return new BeamWorkerStatusGrpcService(apiServiceDescriptor, headerAccessor);
  }

  @Override
  public void close() throws Exception {
    for (CompletableFuture<WorkerStatusClient> clientFuture : connectedClient.values()) {
      if (clientFuture.isDone()) {
        clientFuture.get().close();
      }
    }
    connectedClient.clear();
  }

  @Override
  public StreamObserver<WorkerStatusResponse> workerStatus(
      StreamObserver<WorkerStatusRequest> requestObserver) {
    String workerId = headerAccessor.getSdkWorkerId();
    LOG.info("Beam Fn Status client connected with id {}", workerId);

    WorkerStatusClient fnApiStatusClient =
        WorkerStatusClient.forRequestObserver(workerId, requestObserver);
    fnApiStatusClient.setDeregisterCallback(this.connectedClient::remove);
    if (connectedClient.containsKey(workerId) && connectedClient.get(workerId).isDone()) {
      LOG.info(
          "SDK Worker {} was connected to status server previously, disconnecting the old client",
          workerId);
      try {
        WorkerStatusClient oldClient = connectedClient.get(workerId).get();
        oldClient.close();
      } catch (IOException | InterruptedException | ExecutionException e) {
        LOG.warn("Error closing worker status client", e);
      }
    }
    connectedClient
        .computeIfAbsent(workerId, k -> new CompletableFuture<>())
        .complete(fnApiStatusClient);
    return fnApiStatusClient.getResponseObserver();
  }

  /**
   * Get the latest SDK worker status from the client's corresponding SDK Harness.
   *
   * @param workerId worker id of the SDK harness.
   * @return {@link CompletableFuture} of WorkerStatusResponse from SDK harness.
   */
  public CompletableFuture<WorkerStatusResponse> getWorkerStatus(String workerId) {
    try {
      return getStatusClient(workerId).getWorkerStatus();
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      CompletableFuture<WorkerStatusResponse> error = new CompletableFuture<>();
      error.completeExceptionally(e);
      return error;
    }
  }

  /**
   * Get all the statuses from all connected SDK harnesses within specified timeout.
   *
   * @param timeout max time waiting for the response from all SDK harness.
   * @param timeUnit timeout time unit.
   * @return All the statuses in a Map which key is the SDK harness id and value is the status
   *     content.
   */
  public Map<String, String> getAllWorkerStatuses(long timeout, TimeUnit timeUnit) {
    // return result in worker id sorted map.
    Map<String, String> allStatuses = new ConcurrentSkipListMap<>(Comparator.naturalOrder());

    List<CompletableFuture<WorkerStatusResponse>> responses = new ArrayList<>();
    for (String id : connectedClient.keySet()) {
      responses.add(
          getWorkerStatus(id)
              .whenComplete(
                  (response, ex) -> {
                    allStatuses.put(
                        id, ex != null ? DEFAULT_ERROR_RESPONSE : getStatusErrorOrInfo(response));
                  }));
    }

    try {
      MoreFutures.allAsList(responses).toCompletableFuture().get(timeout, timeUnit);
    } catch (ExecutionException | TimeoutException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      LOG.warn("Error encountered while waiting for worker statuses of all SDK harnesses", e);
    }

    return allStatuses;
  }

  /**
   * Get the status api client connected to the SDK harness with specified workerId.
   *
   * @param workerId worker id of the SDK Harness.
   * @return {@link WorkerStatusClient} if SDK harness with the specified workerId.
   */
  @VisibleForTesting
  WorkerStatusClient getStatusClient(String workerId)
      throws InterruptedException, ExecutionException, TimeoutException {
    CompletableFuture<WorkerStatusClient> clientFuture =
        connectedClient.computeIfAbsent(workerId, k -> new CompletableFuture<>());
    return clientFuture.get(DEFAULT_CLIENT_CONNECTION_WAIT_TIME_SECONDS, TimeUnit.SECONDS);
  }

  /**
   * Return Error field from WorkerStatusResponse if not empty, otherwise return the StatusInfo
   * field.
   */
  private String getStatusErrorOrInfo(WorkerStatusResponse response) {
    return !Strings.isNullOrEmpty(response.getError())
        ? response.getError()
        : response.getStatusInfo();
  }
}
