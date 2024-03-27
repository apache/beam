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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnWorkerStatusGrpc.BeamFnWorkerStatusImplBase;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.sdk.fn.server.FnService;
import org.apache.beam.sdk.fn.server.HeaderAccessor;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Fn Status service which can collect run-time status information from SDK harnesses for
 * debugging purpose.
 */
public class BeamWorkerStatusGrpcService extends BeamFnWorkerStatusImplBase implements FnService {

  private static final Logger LOG = LoggerFactory.getLogger(BeamWorkerStatusGrpcService.class);
  private static final String DEFAULT_EXCEPTION_RESPONSE =
      "Error: exception encountered getting status from SDK harness";

  private final HeaderAccessor headerAccessor;
  private final Map<String, CompletableFuture<WorkerStatusClient>> connectedClient =
      Collections.synchronizedMap(new HashMap<>());
  private final AtomicBoolean isClosed = new AtomicBoolean();

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
    if (isClosed.getAndSet(true)) {
      return;
    }
    synchronized (connectedClient) {
      for (CompletableFuture<WorkerStatusClient> clientFuture : connectedClient.values()) {
        if (clientFuture.isDone()) {
          clientFuture.get().close();
        }
      }
      connectedClient.clear();
    }
  }

  @Override
  public StreamObserver<WorkerStatusResponse> workerStatus(
      StreamObserver<WorkerStatusRequest> requestObserver) {
    if (isClosed.get()) {
      throw new IllegalStateException("BeamWorkerStatusGrpcService already closed.");
    }
    String workerId = headerAccessor.getSdkWorkerId();
    LOG.info("Beam Fn Status client connected with id {}", workerId);

    WorkerStatusClient fnApiStatusClient =
        WorkerStatusClient.forRequestObserver(workerId, requestObserver);
    connectedClient.compute(
        workerId,
        (k, existingClientFuture) -> {
          if (existingClientFuture != null) {
            try {
              if (existingClientFuture.isDone()) {
                LOG.info(
                    "SDK Worker {} was connected to status server previously, disconnecting old client",
                    workerId);
                existingClientFuture.get().close();
              } else {
                existingClientFuture.complete(fnApiStatusClient);
                return existingClientFuture;
              }
            } catch (IOException | InterruptedException | ExecutionException e) {
              LOG.warn("Error closing worker status client", e);
            }
          }
          return CompletableFuture.completedFuture(fnApiStatusClient);
        });
    return fnApiStatusClient.getResponseObserver();
  }

  /**
   * Get the latest SDK worker status from the client's corresponding SDK harness.
   *
   * @param workerId worker id of the SDK harness.
   * @return {@link CompletableFuture} of WorkerStatusResponse from SDK harness.
   */
  public String getSingleWorkerStatus(String workerId, long timeout, TimeUnit timeUnit) {
    if (isClosed.get()) {
      throw new IllegalStateException("BeamWorkerStatusGrpcService already closed.");
    }
    try {
      return getWorkerStatus(workerId).get(timeout, timeUnit);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      return handleAndReturnExceptionResponse(e);
    }
  }

  /**
   * Get all the statuses from all connected SDK harnesses within specified timeout. Any errors
   * getting status from the SDK harnesses will be returned in the map.
   *
   * @param timeout max time waiting for the response from each SDK harness.
   * @param timeUnit timeout time unit.
   * @return All the statuses in a map keyed by the SDK harness id.
   */
  public Map<String, String> getAllWorkerStatuses(long timeout, TimeUnit timeUnit) {
    if (isClosed.get()) {
      throw new IllegalStateException("BeamWorkerStatusGrpcService already closed.");
    }
    // return result in worker id sorted map.
    Map<String, String> allStatuses = new ConcurrentSkipListMap<>(Comparator.naturalOrder());
    Set<String> connectedClientIdsCopy;
    synchronized (connectedClient) {
      connectedClientIdsCopy = ImmutableSet.copyOf(connectedClient.keySet());
    }
    connectedClientIdsCopy
        .parallelStream()
        .forEach(
            workerId ->
                allStatuses.put(workerId, getSingleWorkerStatus(workerId, timeout, timeUnit)));

    return allStatuses;
  }

  @VisibleForTesting
  CompletableFuture<String> getWorkerStatus(String workerId) {
    CompletableFuture<WorkerStatusClient> statusClient;
    try {
      statusClient = getStatusClient(workerId);
      if (!statusClient.isDone()) {
        return CompletableFuture.completedFuture("Error: Not connected.");
      }
      CompletableFuture<WorkerStatusResponse> future = statusClient.get().getWorkerStatus();
      return future.thenApply(this::getStatusErrorOrInfo);
    } catch (ExecutionException | InterruptedException e) {
      return CompletableFuture.completedFuture(handleAndReturnExceptionResponse(e));
    }
  }

  /**
   * Get the status api client connected to the SDK harness with specified workerId.
   *
   * @param workerId worker id of the SDK harness.
   * @return CompletableFuture of {@link WorkerStatusClient}.
   */
  @VisibleForTesting
  CompletableFuture<WorkerStatusClient> getStatusClient(String workerId) {
    return connectedClient.computeIfAbsent(workerId, k -> new CompletableFuture<>());
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

  private String handleAndReturnExceptionResponse(Exception e) {
    LOG.warn(DEFAULT_EXCEPTION_RESPONSE, e);
    if (e instanceof InterruptedException) {
      Thread.currentThread().interrupt();
    }
    StringBuilder response = new StringBuilder();
    response
        .append(DEFAULT_EXCEPTION_RESPONSE)
        .append(": ")
        .append(e.getClass().getCanonicalName());
    if (e.getMessage() != null) {
      response.append(": ").append(e.getMessage());
    }
    return response.toString();
  }
}
