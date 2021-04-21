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
package org.apache.beam.runners.dataflow.worker.fn;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.SynchronousQueue;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnControlGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.runners.dataflow.worker.fn.grpc.BeamFnService;
import org.apache.beam.runners.fnexecution.HeaderAccessor;
import org.apache.beam.runners.fnexecution.control.FnApiControlClient;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.StatusException;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Starts a gRPC server hosting the BeamFnControl service. Clients returned by {@link #get()} are
 * owned by the caller and must be shutdown.
 */
public class BeamFnControlService extends BeamFnControlGrpc.BeamFnControlImplBase
    implements BeamFnService, Supplier<FnApiControlClient> {
  private static final Logger LOG = LoggerFactory.getLogger(BeamFnControlService.class);
  private final Endpoints.ApiServiceDescriptor apiServiceDescriptor;
  private final Function<
          StreamObserver<BeamFnApi.InstructionRequest>,
          StreamObserver<BeamFnApi.InstructionRequest>>
      streamObserverFactory;
  private final SynchronousQueue<FnApiControlClient> newClients;
  private final HeaderAccessor headerAccessor;
  private final ConcurrentMap<String, BeamFnApi.ProcessBundleDescriptor> processBundleDescriptors =
      new ConcurrentHashMap<>();

  public BeamFnControlService(
      Endpoints.ApiServiceDescriptor serviceDescriptor,
      Function<
              StreamObserver<BeamFnApi.InstructionRequest>,
              StreamObserver<BeamFnApi.InstructionRequest>>
          streamObserverFactory,
      HeaderAccessor headerAccessor)
      throws Exception {
    this.headerAccessor = headerAccessor;
    this.newClients = new SynchronousQueue<>(true /* fair */);
    this.streamObserverFactory = streamObserverFactory;
    this.apiServiceDescriptor = serviceDescriptor;
    LOG.info("Launched Beam Fn Control service {}", this.apiServiceDescriptor);
  }

  @Override
  public Endpoints.ApiServiceDescriptor getApiServiceDescriptor() {
    return apiServiceDescriptor;
  }

  @Override
  public StreamObserver<BeamFnApi.InstructionResponse> control(
      StreamObserver<BeamFnApi.InstructionRequest> outboundObserver) {
    LOG.info("Beam Fn Control client connected with id {}", headerAccessor.getSdkWorkerId());
    FnApiControlClient newClient =
        FnApiControlClient.forRequestObserver(
            headerAccessor.getSdkWorkerId(),
            streamObserverFactory.apply(outboundObserver),
            processBundleDescriptors);
    try {
      newClients.put(newClient);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    return newClient.asResponseObserver();
  }

  /** Callers own the client returned and must shut it down before shutting down the service. */
  @Override
  public FnApiControlClient get() {
    try {
      return newClients.take();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void getProcessBundleDescriptor(
      BeamFnApi.GetProcessBundleDescriptorRequest request,
      StreamObserver<BeamFnApi.ProcessBundleDescriptor> responseObserver) {
    String bundleDescriptorId = request.getProcessBundleDescriptorId();
    LOG.info("getProcessBundleDescriptor request with id {}", bundleDescriptorId);
    BeamFnApi.ProcessBundleDescriptor descriptor = processBundleDescriptors.get(bundleDescriptorId);
    if (descriptor == null) {
      String msg =
          String.format("ProcessBundleDescriptor with id %s not found", bundleDescriptorId);
      responseObserver.onError(new StatusException(Status.NOT_FOUND.withDescription(msg)));
      LOG.error(msg);
    } else {
      responseObserver.onNext(descriptor);
      responseObserver.onCompleted();
    }
  }

  @Override
  public void close() throws Exception {
    // TODO: Track multiple clients and disconnect them cleanly instead of forcing termination
  }
}
