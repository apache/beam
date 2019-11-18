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
package org.apache.beam.runners.dataflow.worker.fn.status;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnWorkerStatusGrpc.BeamFnWorkerStatusImplBase;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.runners.fnexecution.FnService;
import org.apache.beam.runners.fnexecution.HeaderAccessor;
import org.apache.beam.runners.fnexecution.status.FnApiWorkerStatusClient;
import org.apache.beam.vendor.grpc.v1p21p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Fn Status service which can collect run-time status information from SDK Harnesses for
 * debugging purpose
 */
public class BeamFnWorkerStatusGrpcService extends BeamFnWorkerStatusImplBase implements FnService {

  private static final Logger LOG = LoggerFactory.getLogger(BeamFnWorkerStatusGrpcService.class);
  private static final String UNKNOW_SDK_ID_PREFIX = "unknown_sdk";
  private final AtomicLong idGenerator = new AtomicLong();
  private final HeaderAccessor headerAccessor;
  private final Map<String, FnApiWorkerStatusClient> connectedClient = new ConcurrentHashMap<>();

  private BeamFnWorkerStatusGrpcService(
      ApiServiceDescriptor apiServiceDescriptor, HeaderAccessor headerAccessor) {
    this.headerAccessor = headerAccessor;
    LOG.info("Launched Beam Fn Status service at {}", apiServiceDescriptor);
  }

  /**
   * Create new instance of {@link BeamFnWorkerStatusGrpcService}.
   *
   * @param apiServiceDescriptor ApiServiceDescriptor used for hosting to the server.
   * @param headerAccessor Grpc head accessor used to obtain SDK Harness worker id.
   * @return {@link BeamFnWorkerStatusGrpcService} if apiServiceDescriptor is valid, otherwise
   *     returns null.
   */
  @Nullable
  public static BeamFnWorkerStatusGrpcService create(
      ApiServiceDescriptor apiServiceDescriptor, HeaderAccessor headerAccessor) {
    if (apiServiceDescriptor == null || Strings.isNullOrEmpty(apiServiceDescriptor.getUrl())) {
      LOG.info(
          "Received empty api service descriptor for status service,"
              + " the sdk status report will be skipped");
      return null;
    }
    return new BeamFnWorkerStatusGrpcService(apiServiceDescriptor, headerAccessor);
  }

  @Override
  public void close() throws Exception {
    for (FnApiWorkerStatusClient client : this.connectedClient.values()) {
      client.close();
    }
    this.connectedClient.clear();
  }

  @Override
  public StreamObserver<WorkerStatusResponse> workerStatus(
      StreamObserver<WorkerStatusRequest> requestObserver) {
    String workerId = headerAccessor.getSdkWorkerId();
    if (Strings.isNullOrEmpty(workerId)) {
      // SDK harness status report should be enforced with proper sdk worker id.
      // If SDK harness connect without proper sdk worker id, generate one with name unknown_sdkXX.
      // TODO: enforce proper worker id.
      workerId = UNKNOW_SDK_ID_PREFIX + idGenerator.getAndIncrement();
      LOG.info(
          "No worker_id header provided in status response. Will use generated id: {}", workerId);
    }

    LOG.info("Beam Fn Status client connected with id {}", workerId);
    FnApiWorkerStatusClient fnApiStatusClient =
        FnApiWorkerStatusClient.forRequestObserver(workerId, requestObserver);
    connectedClient.put(workerId, fnApiStatusClient);
    return fnApiStatusClient.getResponseObserver();
  }

  /**
   * Get the status api client connected to the SDK harness with specified workerId.
   *
   * @param workerId worker id of the SDK Harness.
   * @param maxWaitTimeInMills The maximum waiting time for client to connect in milliseconds.
   * @return {@link Optional} containing the client, if there is no connected SDK Harness with the
   *     specified workerId, Optional of null will be returned.
   */
  public Optional<FnApiWorkerStatusClient> getStatusClient(
      String workerId, long maxWaitTimeInMills) {
    long timeout = System.currentTimeMillis() + maxWaitTimeInMills;
    FnApiWorkerStatusClient client = null;
    try {
      while ((client = this.connectedClient.getOrDefault(workerId, null)) == null
          && System.currentTimeMillis() < timeout) {
        Thread.sleep(500);
      }
    } catch (InterruptedException e) {
      LOG.info("Thread interrupted while waiting for status client to connect");
    }
    return Optional.ofNullable(client);
  }

  /** Get all connected SDK Harness worker ids in a sorted set. */
  public Set<String> getConnectedSdkIds() {
    TreeSet<String> result =
        new TreeSet<>(
            (first, second) -> {
              if (first.length() != second.length()) {
                return first.length() - second.length();
              }
              return first.compareTo(second);
            });
    result.addAll(this.connectedClient.keySet());
    return result;
  }
}
