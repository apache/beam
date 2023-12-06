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
package org.apache.beam.sdk.fn.data;

import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A gRPC multiplexer for a specific {@link Endpoints.ApiServiceDescriptor}.
 *
 * <p>Multiplexes data for inbound consumers based upon their {@code instructionId}.
 *
 * <p>Multiplexing inbound and outbound streams is as thread safe as the consumers of those streams.
 * For inbound streams, this is as thread safe as the inbound observers. For outbound streams, this
 * is as thread safe as the underlying stream observer.
 *
 * <p>TODO: Add support for multiplexing over multiple outbound observers by stickying the output
 * location with a specific outbound observer.
 */
public class BeamFnDataGrpcMultiplexer implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(BeamFnDataGrpcMultiplexer.class);
  private final Endpoints.@Nullable ApiServiceDescriptor apiServiceDescriptor;
  private final StreamObserver<BeamFnApi.Elements> inboundObserver;
  private final StreamObserver<BeamFnApi.Elements> outboundObserver;
  private final ConcurrentMap<
          /*instructionId=*/ String, CompletableFuture<CloseableFnDataReceiver<BeamFnApi.Elements>>>
      receivers;
  private final ConcurrentMap<String, Boolean> erroredInstructionIds;

  public BeamFnDataGrpcMultiplexer(
      Endpoints.@Nullable ApiServiceDescriptor apiServiceDescriptor,
      OutboundObserverFactory outboundObserverFactory,
      OutboundObserverFactory.BasicFactory<BeamFnApi.Elements, BeamFnApi.Elements>
          baseOutboundObserverFactory) {
    this.apiServiceDescriptor = apiServiceDescriptor;
    this.receivers = new ConcurrentHashMap<>();
    this.erroredInstructionIds = new ConcurrentHashMap<>();
    this.inboundObserver = new InboundObserver();
    this.outboundObserver =
        outboundObserverFactory.outboundObserverFor(baseOutboundObserverFactory, inboundObserver);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .omitNullValues()
        .add("apiServiceDescriptor", apiServiceDescriptor)
        .add("consumers", receivers)
        .toString();
  }

  public StreamObserver<BeamFnApi.Elements> getInboundObserver() {
    return inboundObserver;
  }

  public StreamObserver<BeamFnApi.Elements> getOutboundObserver() {
    return outboundObserver;
  }

  private CompletableFuture<CloseableFnDataReceiver<BeamFnApi.Elements>> receiverFuture(
      String instructionId) {
    return receivers.computeIfAbsent(instructionId, (unused) -> new CompletableFuture<>());
  }

  /**
   * Registers a consumer for the specified instruction id.
   *
   * <p>The {@link BeamFnDataGrpcMultiplexer} partitions {@link BeamFnApi.Elements} with multiple
   * instruction ids ensuring that the receiver will only see {@link BeamFnApi.Elements} with a
   * single instruction id.
   *
   * <p>The caller must {@link #unregisterConsumer unregister the consumer} when they no longer wish
   * to receive messages.
   */
  public void registerConsumer(
      String instructionId, CloseableFnDataReceiver<BeamFnApi.Elements> receiver) {
    receiverFuture(instructionId).complete(receiver);
  }

  /** Unregisters a consumer. */
  public void unregisterConsumer(String instructionId) {
    receivers.remove(instructionId);
  }

  @VisibleForTesting
  boolean hasConsumer(String instructionId) {
    return receivers.containsKey(instructionId);
  }

  @Override
  public void close() throws Exception {
    Exception exception = null;
    for (CompletableFuture<CloseableFnDataReceiver<BeamFnApi.Elements>> receiver :
        ImmutableList.copyOf(receivers.values())) {
      // Cancel any observer waiting for the client to complete. If the receiver has already been
      // completed or cancelled, this call will be ignored.
      receiver.cancel(true);
      if (!receiver.isCompletedExceptionally()) {
        try {
          receiver.get().close();
        } catch (Exception e) {
          if (exception == null) {
            exception = e;
          } else {
            exception.addSuppressed(e);
          }
        }
      }
    }
    // Cancel any outbound calls and complete any inbound calls, as this multiplexer is hanging up
    outboundObserver.onError(
        Status.CANCELLED.withDescription("Multiplexer hanging up").asException());
    inboundObserver.onCompleted();
    if (exception != null) {
      throw exception;
    }
  }

  /**
   * A multiplexing {@link StreamObserver} that selects the inbound {@link Consumer} to pass the
   * elements to.
   *
   * <p>The inbound observer blocks until the {@link Consumer} is bound allowing for the sending
   * harness to initiate transmitting data without needing for the receiving harness to signal that
   * it is ready to consume that data.
   */
  private final class InboundObserver implements StreamObserver<BeamFnApi.Elements> {
    @Override
    public void onNext(BeamFnApi.Elements value) {
      // Have a fast path to handle the common case and provide a short circuit to exit if we detect
      // multiple instruction ids.
      SINGLE_INSTRUCTION_ID:
      {
        String instructionId = null;
        for (BeamFnApi.Elements.Data data : value.getDataList()) {
          if (instructionId == null) {
            instructionId = data.getInstructionId();
          } else if (!instructionId.equals(data.getInstructionId())) {
            // Multiple instruction ids detected, break out of this block
            break SINGLE_INSTRUCTION_ID;
          }
        }
        for (BeamFnApi.Elements.Timers timers : value.getTimersList()) {
          if (instructionId == null) {
            instructionId = timers.getInstructionId();
          } else if (!instructionId.equals(timers.getInstructionId())) {
            // Multiple instruction ids detected, break out of this block
            break SINGLE_INSTRUCTION_ID;
          }
        }
        if (instructionId == null) {
          return;
        }
        forwardToConsumerForInstructionId(instructionId, value);
        return;
      }

      // Handle the case if there are multiple instruction ids.
      HashSet<String> instructionIds = new HashSet<>();
      for (BeamFnApi.Elements.Data data : value.getDataList()) {
        instructionIds.add(data.getInstructionId());
      }
      for (BeamFnApi.Elements.Timers timers : value.getTimersList()) {
        instructionIds.add(timers.getInstructionId());
      }
      for (String instructionId : instructionIds) {
        BeamFnApi.Elements.Builder builder = BeamFnApi.Elements.newBuilder();
        for (BeamFnApi.Elements.Data data : value.getDataList()) {
          if (instructionId.equals(data.getInstructionId())) {
            builder.addData(data);
          }
        }
        for (BeamFnApi.Elements.Timers timers : value.getTimersList()) {
          if (instructionId.equals(timers.getInstructionId())) {
            builder.addTimers(timers);
          }
        }
        forwardToConsumerForInstructionId(instructionId, builder.build());
      }
    }

    private void forwardToConsumerForInstructionId(String instructionId, BeamFnApi.Elements value) {
      if (erroredInstructionIds.containsKey(instructionId)) {
        LOG.debug("Ignoring inbound data for failed instruction {}", instructionId);
        return;
      }
      CompletableFuture<CloseableFnDataReceiver<BeamFnApi.Elements>> consumerFuture =
          receiverFuture(instructionId);
      if (!consumerFuture.isDone()) {
        LOG.debug(
            "Received data for instruction {} without consumer ready. "
                + "Waiting for consumer to be registered.",
            instructionId);
      }
      CloseableFnDataReceiver<BeamFnApi.Elements> consumer;
      try {
        consumer = consumerFuture.get();

        /*
         * TODO: On failure we should fail any bundles that were impacted eagerly
         * instead of relying on the Runner harness to do all the failure handling.
         */
      } catch (ExecutionException | InterruptedException e) {
        LOG.error(
            "Client interrupted during handling of data for instruction {}", instructionId, e);
        outboundObserver.onError(e);
        return;
      } catch (RuntimeException e) {
        LOG.error("Client failed to handle data for instruction {}", instructionId, e);
        outboundObserver.onError(e);
        return;
      }
      try {
        consumer.accept(value);
      } catch (Exception e) {
        erroredInstructionIds.put(instructionId, true);
      }
    }

    @Override
    public void onError(Throwable t) {
      LOG.error(
          "Failed to handle for {}",
          apiServiceDescriptor == null ? "unknown endpoint" : apiServiceDescriptor,
          t);
    }

    @Override
    public void onCompleted() {
      LOG.info(
          "Hanged up for {}.",
          apiServiceDescriptor == null ? "unknown endpoint" : apiServiceDescriptor);
    }
  }
}
