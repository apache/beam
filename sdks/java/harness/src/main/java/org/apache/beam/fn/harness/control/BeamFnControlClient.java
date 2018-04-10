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

import static com.google.common.base.Throwables.getStackTraceAsString;

import com.google.common.util.concurrent.Uninterruptibles;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.util.EnumMap;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.beam.fn.harness.fn.ThrowingFunction;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnControlGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.fn.stream.StreamObserverFactory.StreamObserverClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A client for the Beam Fn Control API. Uses an unbounded internal queue to pull down
 * an unbounded number of requests.
 *
 * <p>Also can delegate to a set of handlers based upon the
 * {@link BeamFnApi.InstructionRequest.RequestCase request type}.
 *
 * <p>When the inbound instruction stream finishes successfully, the {@code onFinish} is
 * completed successfully signaling to the caller that this client will not produce any more
 * {@link BeamFnApi.InstructionRequest}s. If the inbound instruction stream
 * errors, the {@code onFinish} is completed exceptionally propagating the failure reason
 * to the caller and signaling that this client will not produce any more
 * {@link BeamFnApi.InstructionRequest}s.
 */
public class BeamFnControlClient {
  private static final String FAKE_INSTRUCTION_ID = "FAKE_INSTRUCTION_ID";
  private static final Logger LOG = LoggerFactory.getLogger(BeamFnControlClient.class);
  private static final BeamFnApi.InstructionRequest POISON_PILL =
      BeamFnApi.InstructionRequest.newBuilder().setInstructionId(FAKE_INSTRUCTION_ID).build();

  private final StreamObserver<BeamFnApi.InstructionResponse> outboundObserver;
  private final BlockingDeque<BeamFnApi.InstructionRequest> bufferedInstructions;
  private final EnumMap<BeamFnApi.InstructionRequest.RequestCase,
                        ThrowingFunction<BeamFnApi.InstructionRequest,
                                         BeamFnApi.InstructionResponse.Builder>> handlers;
  private final CompletableFuture<Object> onFinish;

  public BeamFnControlClient(
      Endpoints.ApiServiceDescriptor apiServiceDescriptor,
      Function<Endpoints.ApiServiceDescriptor, ManagedChannel> channelFactory,
      BiFunction<
              StreamObserverClientFactory<InstructionRequest, BeamFnApi.InstructionResponse>,
              StreamObserver<BeamFnApi.InstructionRequest>,
              StreamObserver<BeamFnApi.InstructionResponse>>
          streamObserverFactory,
      EnumMap<
              BeamFnApi.InstructionRequest.RequestCase,
              ThrowingFunction<BeamFnApi.InstructionRequest, BeamFnApi.InstructionResponse.Builder>>
          handlers) {
    this.bufferedInstructions = new LinkedBlockingDeque<>();
    this.outboundObserver = streamObserverFactory.apply(
        BeamFnControlGrpc.newStub(channelFactory.apply(apiServiceDescriptor))::control,
        new InboundObserver());
    this.handlers = handlers;
    this.onFinish = new CompletableFuture<>();
  }

  private static final Object COMPLETED = new Object();

  /**
   * A {@link StreamObserver} for the inbound stream that completes the future on stream
   * termination.
   */
  private class InboundObserver implements StreamObserver<BeamFnApi.InstructionRequest> {

    @Override
    public void onNext(BeamFnApi.InstructionRequest value) {
      LOG.debug("Received InstructionRequest {}", value);
      Uninterruptibles.putUninterruptibly(bufferedInstructions, value);
    }

    @Override
    public void onError(Throwable t) {
      placePoisonPillIntoQueue();
      onFinish.completeExceptionally(t);
    }

    @Override
    public void onCompleted() {
      placePoisonPillIntoQueue();
      onFinish.complete(COMPLETED);
    }

    /**
     * This method emulates {@link Uninterruptibles#putUninterruptibly} but placing the
     * element at the front of the queue.
     *
     * <p>We place the poison pill at the front of the queue because if the server shutdown,
     * any remaining instructions can be discarded.
     */
    private void placePoisonPillIntoQueue() {
      while (true) {
        try {
          bufferedInstructions.putFirst(POISON_PILL);
          return;
        } catch (InterruptedException e) {
          // Ignored until we place the poison pill into the queue
        }
      }
    }
  }

  /**
   * Note that this method continuously submits work to the supplied executor until the
   * Beam Fn Control server hangs up or fails exceptionally.
   */
  public void processInstructionRequests(Executor executor)
      throws InterruptedException, ExecutionException {
    BeamFnApi.InstructionRequest request;
    while ((request = bufferedInstructions.take()) != POISON_PILL) {
      BeamFnApi.InstructionRequest currentRequest = request;
      executor.execute(
          () -> sendInstructionResponse(delegateOnInstructionRequestType(currentRequest)));
    }
    onFinish.get();
  }

  public BeamFnApi.InstructionResponse delegateOnInstructionRequestType(
      BeamFnApi.InstructionRequest value) {
    try {
      return handlers.getOrDefault(value.getRequestCase(), this::missingHandler)
          .apply(value)
          .setInstructionId(value.getInstructionId())
          .build();
    } catch (Exception e) {
      return BeamFnApi.InstructionResponse.newBuilder()
          .setInstructionId(value.getInstructionId())
          .setError(getStackTraceAsString(e))
          .build();
    }
  }

  public void sendInstructionResponse(BeamFnApi.InstructionResponse value) {
    LOG.debug("Sending InstructionResponse {}", value);
    outboundObserver.onNext(value);
  }

  private BeamFnApi.InstructionResponse.Builder missingHandler(
      BeamFnApi.InstructionRequest request) {
    return BeamFnApi.InstructionResponse.newBuilder().setError(
        String.format("Unknown InstructionRequest type %s", request.getRequestCase()));
  }
}
