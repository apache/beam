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

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.fn.stream.StalledPollHandler;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.ClientCallStreamObserver;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.ClientResponseObserver;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
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
  // The inbound stream uses manual gRPC flow control, so onNext never blocks. A blocked onNext
  // also blocks the onReady callback of the outbound stream, because gRPC runs both on one
  // executor.
  //
  // Number of inbound messages that the stream asks for when no output waits.
  private static final int STEADY_STATE_INBOUND_WINDOW = 16;
  // Largest number of inbound messages for each active bundle while output waits.
  private static final int MAX_INBOUND_WINDOW = 100;
  // Inbound messages that the stream asked for and that no bundle has finished.
  private final AtomicInteger inFlightInbound = new AtomicInteger(STEADY_STATE_INBOUND_WINDOW);

  /**
   * Asks for one more inbound message while output waits on flow control, up to {@code
   * MAX_INBOUND_WINDOW} for each active bundle. The extra message lets a bundle that waits for
   * input continue, so the runner can read the output.
   */
  public void onStalledPoll() {
    synchronized (inboundObserver) {
      int maxWindow = Math.max(1, receivers.size()) * MAX_INBOUND_WINDOW;
      if (inFlightInbound.get() < maxWindow) {
        inFlightInbound.incrementAndGet();
        inboundObserver.request(1);
      }
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(BeamFnDataGrpcMultiplexer.class);
  private static final Duration POISONED_INSTRUCTION_ID_CACHE_TIMEOUT = Duration.ofMinutes(20);
  private final Endpoints.@Nullable ApiServiceDescriptor apiServiceDescriptor;
  private final InboundObserver inboundObserver;
  private final StreamObserver<BeamFnApi.Elements> outboundObserver;

  private static final class PendingElement {
    BeamFnApi.Elements elements;
    Runnable callback;

    PendingElement(BeamFnApi.Elements elements, Runnable callback) {
      this.elements = elements;
      this.callback = callback;
    }
  }

  private static final class ReceiverHolder {
    @Nullable CloseableFnDataReceiver<BeamFnApi.Elements> receiver;

    ArrayDeque<PendingElement> pending = new ArrayDeque<>();
    boolean poisoned;

    private Runnable wrapCallback(Runnable callback) {
      return () -> {
        drainPending();
        callback.run();
      };
    }

    synchronized void drainPending() {
      if (poisoned || !(receiver instanceof BeamFnDataInboundObserver)) {
        return;
      }
      BeamFnDataInboundObserver inbound = (BeamFnDataInboundObserver) receiver;
      while (!pending.isEmpty()) {
        PendingElement next = pending.peekFirst();
        if (next == null) {
          break;
        }
        try {
          if (inbound.tryAcceptWithCallback(next.elements, wrapCallback(next.callback))) {
            pending.removeFirst();
          } else {
            break;
          }
        } catch (Exception e) {
          pending.removeFirst();
          next.callback.run();
        }
      }
    }

    synchronized boolean accept(BeamFnApi.Elements elements, Runnable callback) throws Exception {
      if (poisoned) {
        return false;
      }
      if (receiver != null) {
        if (receiver instanceof BeamFnDataInboundObserver) {
          BeamFnDataInboundObserver inbound = (BeamFnDataInboundObserver) receiver;
          drainPending();
          if (!pending.isEmpty()
              || !inbound.tryAcceptWithCallback(elements, wrapCallback(callback))) {
            pending.addLast(new PendingElement(elements, callback));
          }
        } else {
          receiver.accept(elements);
          callback.run();
        }
        return true;
      } else {
        pending.addLast(new PendingElement(elements, callback));
        return true;
      }
    }

    /**
     * Sets the receiver and hands it the buffered elements. Returns false if the holder already has
     * a receiver or is poisoned. Every buffered callback runs, even if the receiver throws.
     */
    synchronized boolean complete(CloseableFnDataReceiver<BeamFnApi.Elements> receiver)
        throws Exception {
      if (this.receiver != null || poisoned) {
        return false;
      }
      this.receiver = receiver;
      if (receiver instanceof BeamFnDataInboundObserver) {
        drainPending();
      } else {
        try {
          while (!pending.isEmpty()) {
            PendingElement p = pending.removeFirst();
            try {
              receiver.accept(p.elements);
            } finally {
              p.callback.run();
            }
          }
        } finally {
          // If the receiver threw, release the callbacks of the elements it did not see.
          for (PendingElement p : pending) {
            p.callback.run();
          }
          pending.clear();
        }
      }
      return true;
    }

    synchronized void poison() {
      poisoned = true;
      for (PendingElement p : pending) {
        p.callback.run();
      }
      pending.clear();
    }

    synchronized boolean isDone() {
      return receiver != null || poisoned;
    }

    synchronized boolean isPoisoned() {
      return poisoned;
    }

    synchronized void close() throws Exception {
      if (receiver != null) {
        receiver.close();
      }
      for (PendingElement p : pending) {
        p.callback.run();
      }
      pending.clear();
    }
  }

  private final ConcurrentMap</* instructionId= */ String, ReceiverHolder> receivers;
  private final Cache</* instructionId= */ String, /* unused= */ Boolean> poisonedInstructionIds;

  public BeamFnDataGrpcMultiplexer(
      Endpoints.@Nullable ApiServiceDescriptor apiServiceDescriptor,
      OutboundObserverFactory outboundObserverFactory,
      OutboundObserverFactory.BasicFactory<BeamFnApi.Elements, BeamFnApi.Elements>
          baseOutboundObserverFactory) {
    this.apiServiceDescriptor = apiServiceDescriptor;
    this.receivers = new ConcurrentHashMap<>();
    this.poisonedInstructionIds =
        CacheBuilder.newBuilder().expireAfterWrite(POISONED_INSTRUCTION_ID_CACHE_TIMEOUT).build();
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

  /**
   * Called when a bundle finishes one inbound message. Asks for more messages until {@code
   * STEADY_STATE_INBOUND_WINDOW} messages are open again.
   */
  public void requestInbound(int count) {
    inFlightInbound.addAndGet(-count);
    synchronized (inboundObserver) {
      while (inFlightInbound.get() < STEADY_STATE_INBOUND_WINDOW) {
        inFlightInbound.incrementAndGet();
        inboundObserver.request(1);
      }
    }
  }

  public StreamObserver<BeamFnApi.Elements> getInboundObserver() {
    return inboundObserver;
  }

  public StreamObserver<BeamFnApi.Elements> getOutboundObserver() {
    return outboundObserver;
  }

  /**
   * Registers a consumer for the specified instruction id.
   *
   * <p>The {@link BeamFnDataGrpcMultiplexer} partitions {@link BeamFnApi.Elements} with multiple
   * instruction ids ensuring that the receiver will only see {@link BeamFnApi.Elements} with a
   * single instruction id.
   *
   * <p>The caller must either {@link #unregisterConsumer unregister the consumer} when all messages
   * have been processed or {@link #poisonInstructionId(String) poison the instruction} if messages
   * for the instruction should be dropped.
   */
  public void registerConsumer(
      String instructionId, CloseableFnDataReceiver<BeamFnApi.Elements> receiver) {
    receivers.compute(
        instructionId,
        (unused, existing) -> {
          if (existing != null) {
            if (existing.isPoisoned()) {
              throw new IllegalArgumentException("Instruction id was poisoned");
            }
            if (!complete(existing, receiver)) {
              throw new IllegalArgumentException("Instruction id was registered twice");
            }
            return existing;
          }
          if (poisonedInstructionIds.getIfPresent(instructionId) != null) {
            throw new IllegalArgumentException("Instruction id was poisoned");
          }
          ReceiverHolder h = new ReceiverHolder();
          complete(h, receiver);
          return h;
        });
  }

  /** {@link ReceiverHolder#complete} that rethrows runtime exceptions as they are. */
  private static boolean complete(
      ReceiverHolder holder, CloseableFnDataReceiver<BeamFnApi.Elements> receiver) {
    try {
      return holder.complete(receiver);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** Unregisters a previously registered consumer. */
  public void unregisterConsumer(String instructionId) {
    @Nullable ReceiverHolder receiverFuture = receivers.remove(instructionId);
    if (receiverFuture != null) {
      if (!receiverFuture.isDone()) {
        // The inbound observer inserted this holder before registerConsumer completed it.
        receiverFuture.poison();
        throw new IllegalArgumentException("Unregistering consumer which was not registered.");
      }
      receiverFuture.poison();
    }
  }

  /**
   * Poisons an instruction id.
   *
   * <p>Any records for the instruction on the inbound observer will be dropped for the next {@link
   * #POISONED_INSTRUCTION_ID_CACHE_TIMEOUT}.
   */
  public void poisonInstructionId(String instructionId) {
    poisonedInstructionIds.put(instructionId, Boolean.TRUE);
    @Nullable ReceiverHolder receiverFuture = receivers.remove(instructionId);
    if (receiverFuture != null) {
      // Release pending callbacks and close any registered receiver.
      receiverFuture.poison();
      try {
        receiverFuture.close();
      } catch (Exception e) {
        LOG.warn("Unexpected error closing existing observer");
      }
    }
  }

  @VisibleForTesting
  boolean hasConsumer(String instructionId) {
    return receivers.containsKey(instructionId);
  }

  @Override
  public void close() throws Exception {
    Exception exception = null;
    for (ReceiverHolder receiver : ImmutableList.copyOf(receivers.values())) {
      // Cancel any observer waiting for the client to complete. If the receiver has already been
      // completed or cancelled, this call will be ignored.
      receiver.poison();
      try {
        receiver.close();
      } catch (Exception e) {
        if (exception == null) {
          exception = e;
        } else {
          exception.addSuppressed(e);
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
   * <p>Data for an instruction with no {@link Consumer} yet waits in a buffer until the consumer is
   * registered, so the sending harness can send data before the receiving harness is ready. The
   * observer does not block: when a bundle queue is full, the message waits in the buffer.
   */
  private final class InboundObserver
      implements ClientResponseObserver<BeamFnApi.Elements, BeamFnApi.Elements>,
          StalledPollHandler {
    @Override
    public void onStalledPoll() {
      BeamFnDataGrpcMultiplexer.this.onStalledPoll();
    }

    private @Nullable ClientCallStreamObserver<BeamFnApi.Elements> requestStream;

    @Override
    public void beforeStart(ClientCallStreamObserver<BeamFnApi.Elements> requestStream) {
      this.requestStream = requestStream;
      requestStream.disableAutoRequestWithInitial(STEADY_STATE_INBOUND_WINDOW);
    }

    public void request(int count) {
      if (requestStream != null) {
        requestStream.request(count);
      }
    }

    @SuppressWarnings("LabelledBreakTarget")
    @Override
    public void onNext(BeamFnApi.Elements value) {
      Runnable callback = () -> BeamFnDataGrpcMultiplexer.this.requestInbound(1);
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
          callback.run();
          return;
        }
        if (!forwardToConsumerForInstructionId(instructionId, value, callback)) {
          callback.run();
        }
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
      AtomicInteger countdown = new AtomicInteger(instructionIds.size());
      Runnable multiCallback =
          () -> {
            if (countdown.decrementAndGet() == 0) {
              callback.run();
            }
          };

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
        if (!forwardToConsumerForInstructionId(instructionId, builder.build(), multiCallback)) {
          multiCallback.run();
        }
      }
    }

    // computeIfAbsent documents that a null result inserts no mapping; the nullness checker's
    // model of the function does not allow null. The poison check stays inside computeIfAbsent so
    // it cannot race with poisonInstructionId.
    @SuppressWarnings("return")
    private boolean forwardToConsumerForInstructionId(
        String instructionId, BeamFnApi.Elements value, Runnable callback) {
      try {
        // computeIfAbsent inserts nothing when the function returns null, so data for a poisoned
        // instruction leaves no entry in receivers.
        @Nullable ReceiverHolder consumerFuture =
            receivers.computeIfAbsent(
                instructionId,
                (unused) -> {
                  if (poisonedInstructionIds.getIfPresent(instructionId) != null) {
                    return null;
                  }
                  LOG.debug(
                      "Received data for instruction {} without consumer ready. "
                          + "Buffering elements until consumer is registered.",
                      instructionId);
                  return new ReceiverHolder();
                });
        if (consumerFuture == null) {
          LOG.debug("Received data for poisoned instruction {}. Dropping input.", instructionId);
          return false;
        }
        return consumerFuture.accept(value, callback);
      } catch (Exception e) {
        poisonInstructionId(instructionId);
        return false;
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
