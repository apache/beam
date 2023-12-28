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
package org.apache.beam.fn.harness.control;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables.getStackTraceAsString;

import java.util.EnumMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.beam.fn.harness.logging.BeamFnLoggingMDC;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnControlGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.sdk.fn.channel.ManagedChannelFactory;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.function.ThrowingFunction;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A client for the Beam Fn Control API. Uses an unbounded internal queue to pull down an unbounded
 * number of requests.
 *
 * <p>Also can delegate to a set of handlers based upon the {@link
 * BeamFnApi.InstructionRequest.RequestCase request type}.
 *
 * <p>When the inbound instruction stream finishes successfully, the {@code onFinish} is completed
 * successfully signaling to the caller that this client will not produce any more {@link
 * BeamFnApi.InstructionRequest}s. If the inbound instruction stream errors, the {@code onFinish} is
 * completed exceptionally propagating the failure reason to the caller and signaling that this
 * client will not produce any more {@link BeamFnApi.InstructionRequest}s.
 */
public class BeamFnControlClient {
  private static final Logger LOG = LoggerFactory.getLogger(BeamFnControlClient.class);

  private final StreamObserver<BeamFnApi.InstructionResponse> outboundObserver;
  private final EnumMap<
          BeamFnApi.InstructionRequest.RequestCase,
          ThrowingFunction<BeamFnApi.InstructionRequest, BeamFnApi.InstructionResponse.Builder>>
      handlers;
  private final CompletableFuture<Object> onFinish;

  public BeamFnControlClient(
      ApiServiceDescriptor apiServiceDescriptor,
      ManagedChannelFactory channelFactory,
      OutboundObserverFactory outboundObserverFactory,
      Executor executor,
      EnumMap<
              BeamFnApi.InstructionRequest.RequestCase,
              ThrowingFunction<BeamFnApi.InstructionRequest, BeamFnApi.InstructionResponse.Builder>>
          handlers) {
    this(
        BeamFnControlGrpc.newStub(channelFactory.forDescriptor(apiServiceDescriptor)),
        outboundObserverFactory,
        executor,
        handlers);
  }

  public BeamFnControlClient(
      BeamFnControlGrpc.BeamFnControlStub controlStub,
      OutboundObserverFactory outboundObserverFactory,
      Executor executor,
      EnumMap<
              BeamFnApi.InstructionRequest.RequestCase,
              ThrowingFunction<BeamFnApi.InstructionRequest, BeamFnApi.InstructionResponse.Builder>>
          handlers) {
    this.onFinish = new CompletableFuture<>();
    this.handlers = handlers;
    this.outboundObserver =
        outboundObserverFactory.outboundObserverFor(
            controlStub::control, new InboundObserver(executor));
  }

  private static final Object COMPLETED = new Object();

  /**
   * A {@link StreamObserver} for the inbound stream that completes the future on stream
   * termination.
   */
  private class InboundObserver implements StreamObserver<BeamFnApi.InstructionRequest> {
    private final Executor executor;

    InboundObserver(Executor executorService) {
      this.executor = executorService;
    }

    @Override
    public void onNext(BeamFnApi.InstructionRequest request) {
      try {
        BeamFnLoggingMDC.setInstructionId(request.getInstructionId());
        LOG.debug("Received InstructionRequest {}", request);
        executor.execute(
            () -> {
              try {
                // Ensure that we set and clear the MDC since processing the request will occur
                // in a separate thread.
                BeamFnLoggingMDC.setInstructionId(request.getInstructionId());
                BeamFnApi.InstructionResponse response = delegateOnInstructionRequestType(request);
                sendInstructionResponse(response);
              } catch (Error e) {
                sendErrorResponse(e);
                throw e;
              } finally {
                BeamFnLoggingMDC.reset();
              }
            });
      } finally {
        BeamFnLoggingMDC.reset();
      }
    }

    @Override
    public void onError(Throwable t) {
      onFinish.completeExceptionally(t);
    }

    @Override
    public void onCompleted() {
      onFinish.complete(COMPLETED);
    }
  }

  /** This method blocks until the control stream has completed. */
  public CompletableFuture<Object> terminationFuture() {
    return onFinish;
  }

  public BeamFnApi.InstructionResponse delegateOnInstructionRequestType(
      BeamFnApi.InstructionRequest value) {
    try {
      return handlers
          .getOrDefault(value.getRequestCase(), this::missingHandler)
          .apply(value)
          .setInstructionId(value.getInstructionId())
          .build();
    } catch (Exception e) {
      LOG.error(
          "Exception while trying to handle {} {}",
          BeamFnApi.InstructionRequest.class.getSimpleName(),
          value.getInstructionId(),
          e);
      return BeamFnApi.InstructionResponse.newBuilder()
          .setInstructionId(value.getInstructionId())
          .setError(getStackTraceAsString(e))
          .build();
    } catch (Error e) {
      LOG.error(
          "Error thrown when handling {} {}",
          BeamFnApi.InstructionRequest.class.getSimpleName(),
          value.getInstructionId(),
          e);
      throw e;
    }
  }

  public void sendInstructionResponse(BeamFnApi.InstructionResponse value) {
    LOG.debug("Sending InstructionResponse {}", value);
    outboundObserver.onNext(value);
  }

  private void sendErrorResponse(Error e) {
    onFinish.completeExceptionally(e);
    outboundObserver.onError(
        Status.INTERNAL
            .withDescription(String.format("%s: %s", e.getClass().getName(), e.getMessage()))
            .asException());
  }

  private BeamFnApi.InstructionResponse.Builder missingHandler(
      BeamFnApi.InstructionRequest request) {
    return BeamFnApi.InstructionResponse.newBuilder()
        .setError(String.format("Unknown InstructionRequest type %s", request.getRequestCase()));
  }
}
