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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
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
  private static final Logger LOG = LoggerFactory.getLogger(BeamFnDataGrpcMultiplexer.class);
  private static final Duration POISONED_INSTRUCTION_ID_CACHE_TIMEOUT = Duration.ofMinutes(20);
  private static final VarHandle FLOW_CONTROL_STATE;

  private static final Object UNREGISTERED = new Object();
  private static final Object DRAINING = new Object();
  private static final Object POISONED = new Object();

  static {
    try {
      FLOW_CONTROL_STATE =
          MethodHandles.lookup()
              .findVarHandle(BeamFnDataGrpcMultiplexer.class, "flowControlState", int.class);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private final Endpoints.@Nullable ApiServiceDescriptor apiServiceDescriptor;
  private final InboundObserver inboundObserver;
  private final StreamObserver<BeamFnApi.Elements> outboundObserver;
  private final ConcurrentHashMap</* instructionId= */ String, ReceiverEntry> receivers;
  private final Cache</* instructionId= */ String, /* unused= */ Boolean> poisonedInstructionIds;

  /**
   * Encodes {@code (paused ? Integer.MIN_VALUE : 0) | bufferedElements}.
   *
   * <p>The sign bit is {@code 1} ({@code flowControlState < 0}) iff the single gRPC {@code
   * request(1)} flow-control token is parked because {@code bufferedElements >=
   * BeamFnDataInboundObserver.DEFAULT_QUEUE_CAPACITY}.
   */
  @SuppressWarnings("UnusedVariable")
  private volatile int flowControlState;

  /**
   * A Treiber-stack node for {@link BeamFnApi.Elements} that arrive before a consumer has finished
   * registering and draining. Once detached via CAS/getAndSet on {@link ReceiverEntry#STATE}, the
   * chain is exclusively owned by the detaching thread and reversed in-place via {@link #reverse}
   * for FIFO delivery.
   */
  private static final class BufferedNode {
    final BeamFnApi.Elements value;
    final boolean hasPermit;
    final boolean draining;
    @Nullable BufferedNode nextNode;

    BufferedNode(BeamFnApi.Elements value, boolean hasPermit, Object nextState) {
      this.value = value;
      this.hasPermit = hasPermit;
      if (nextState instanceof BufferedNode) {
        BufferedNode next = (BufferedNode) nextState;
        this.draining = next.draining;
        this.nextNode = next;
      } else {
        this.draining = (nextState == DRAINING);
        this.nextNode = null;
      }
    }

    static @Nullable BufferedNode reverse(@Nullable BufferedNode head) {
      @Nullable BufferedNode prev = null;
      @Nullable BufferedNode curr = head;
      while (curr != null) {
        @Nullable BufferedNode next = curr.nextNode;
        curr.nextNode = prev;
        prev = curr;
        curr = next;
      }
      return prev;
    }

    static int countPermits(@Nullable BufferedNode head) {
      int permits = 0;
      for (BufferedNode curr = head; curr != null; curr = curr.nextNode) {
        if (curr.hasPermit) {
          permits++;
        }
      }
      return permits;
    }
  }

  /**
   * Lock-free state holder for an {@code instructionId} in {@link #receivers}.
   *
   * <p>{@link #state} transitions monotonically through:
   *
   * <ol>
   *   <li>{@link #UNREGISTERED} or a {@link BufferedNode} stack with {@code draining == false}
   *   <li>{@link #DRAINING} or a {@link BufferedNode} stack with {@code draining == true} while
   *       {@link #registerConsumer} drains pre-registration elements in FIFO order
   *   <li>The registered {@link CloseableFnDataReceiver} once drained (or immediately on
   *       construction if registered before any inbound data arrives)
   *   <li>{@link #POISONED} when unregistered, poisoned, or closed
   * </ol>
   */
  private static final class ReceiverEntry {
    private static final VarHandle STATE;

    static {
      try {
        STATE = MethodHandles.lookup().findVarHandle(ReceiverEntry.class, "state", Object.class);
      } catch (ReflectiveOperationException e) {
        throw new ExceptionInInitializerError(e);
      }
    }

    @SuppressWarnings("UnusedVariable")
    private volatile Object state;

    ReceiverEntry() {
      this.state = UNREGISTERED;
    }

    ReceiverEntry(CloseableFnDataReceiver<BeamFnApi.Elements> receiver) {
      this.state = receiver;
    }
  }

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
    if (poisonedInstructionIds.getIfPresent(instructionId) != null) {
      throw new IllegalArgumentException("Instruction id was poisoned");
    }
    // beforeStart() is invoked synchronously during BeamFnDataGrpcMultiplexer construction on
    // client-side gRPC streams (setting requestObserver) and is never invoked on server-side
    // streams or plain StreamObserver test doubles (where forwardToConsumerForInstructionId falls
    // back to blocking consumer.accept()). We must also set onElementsConsumed before publishing
    // the receiver in receivers below to establish a happens-before edge to the inbound gRPC
    // thread reading ReceiverEntry.STATE -> consumer.add().
    if (receiver instanceof BeamFnDataInboundObserver && inboundObserver.requestObserver != null) {
      ((BeamFnDataInboundObserver) receiver).setOnElementsConsumed(inboundObserver);
    }
    ReceiverEntry newEntry = new ReceiverEntry(receiver);
    @Nullable ReceiverEntry existing = receivers.putIfAbsent(instructionId, newEntry);
    if (existing == null) {
      if (poisonedInstructionIds.getIfPresent(instructionId) != null
          && receivers.remove(instructionId, newEntry)) {
        ReceiverEntry.STATE.setVolatile(newEntry, POISONED);
        throw new IllegalArgumentException("Instruction id was poisoned");
      }
      return;
    }

    Object s = ReceiverEntry.STATE.getVolatile(existing);
    while (true) {
      if (s instanceof CloseableFnDataReceiver
          || s == DRAINING
          || (s instanceof BufferedNode && ((BufferedNode) s).draining)) {
        throw new IllegalArgumentException("Instruction id was registered twice");
      }
      if (s == POISONED || poisonedInstructionIds.getIfPresent(instructionId) != null) {
        throw new IllegalArgumentException("Instruction id was poisoned");
      }
      Object target = (s == UNREGISTERED) ? receiver : DRAINING;
      Object prev = ReceiverEntry.STATE.compareAndExchange(existing, s, target);
      if (prev == s) {
        if (s != UNREGISTERED) {
          inboundObserver.drain(instructionId, existing, (BufferedNode) s, receiver);
        }
        return;
      }
      s = prev;
    }
  }

  /** Unregisters a previously registered consumer. */
  public void unregisterConsumer(String instructionId) {
    @Nullable ReceiverEntry entry = receivers.remove(instructionId);
    if (entry != null) {
      Object prev = ReceiverEntry.STATE.getAndSet(entry, POISONED);
      if (prev instanceof BufferedNode) {
        BufferedNode node = (BufferedNode) prev;
        int permits = BufferedNode.countPermits(node);
        if (permits > 0) {
          inboundObserver.accept(permits);
        }
        if (!node.draining) {
          throw new IllegalArgumentException("Unregistering consumer which was not registered.");
        }
      } else if (prev == UNREGISTERED) {
        // The entry must have been inserted by the inbound observer since registerConsumer
        // transitions the state out of UNREGISTERED.
        throw new IllegalArgumentException("Unregistering consumer which was not registered.");
      }
    }
  }

  @SuppressWarnings("unchecked")
  private @Nullable CloseableFnDataReceiver<BeamFnApi.Elements> releaseEntry(ReceiverEntry entry) {
    Object prev = ReceiverEntry.STATE.getAndSet(entry, POISONED);
    if (prev instanceof BufferedNode) {
      int permits = BufferedNode.countPermits((BufferedNode) prev);
      if (permits > 0) {
        inboundObserver.accept(permits);
      }
      return null;
    }
    if (prev instanceof CloseableFnDataReceiver) {
      return (CloseableFnDataReceiver<BeamFnApi.Elements>) prev;
    }
    return null;
  }

  private static void closeReceiverQuietly(CloseableFnDataReceiver<BeamFnApi.Elements> receiver) {
    try {
      receiver.close();
    } catch (Exception e) {
      LOG.warn("Unexpected error closing existing observer", e);
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
    @Nullable ReceiverEntry entry = receivers.remove(instructionId);
    if (entry != null) {
      @Nullable CloseableFnDataReceiver<BeamFnApi.Elements> receiver = releaseEntry(entry);
      if (receiver != null) {
        closeReceiverQuietly(receiver);
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
    for (ReceiverEntry entry : ImmutableList.copyOf(receivers.values())) {
      @Nullable CloseableFnDataReceiver<BeamFnApi.Elements> receiver = releaseEntry(entry);
      if (receiver != null) {
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
   * <p>If data arrives for an instruction before its {@link Consumer} is registered, the elements
   * are buffered in {@link ReceiverEntry} without blocking the inbound gRPC thread, allowing the
   * sending harness to initiate transmitting data without needing the receiving harness to signal
   * that it is ready to consume that data.
   */
  private final class InboundObserver
      implements ClientResponseObserver<BeamFnApi.Elements, BeamFnApi.Elements>, IntConsumer {
    private volatile @Nullable ClientCallStreamObserver<BeamFnApi.Elements> requestObserver;

    @Override
    public void beforeStart(ClientCallStreamObserver<BeamFnApi.Elements> requestObserver) {
      this.requestObserver = requestObserver;
      requestObserver.disableAutoInboundFlowControl();
    }

    @Override
    public void accept(int count) {
      if (count <= 0) {
        throw new IllegalArgumentException("Count must be positive");
      }
      int s = (int) FLOW_CONTROL_STATE.getVolatile(BeamFnDataGrpcMultiplexer.this);
      while (true) {
        int next = s - count;
        if ((s ^ next) < 0) {
          throw new IllegalStateException("Count exceeds buffered elements");
        }
        boolean wakeUp =
            next < (Integer.MIN_VALUE | BeamFnDataInboundObserver.DEFAULT_QUEUE_CAPACITY);
        int nextState = wakeUp ? (next & Integer.MAX_VALUE) : next;
        int prev =
            (int)
                FLOW_CONTROL_STATE.compareAndExchange(BeamFnDataGrpcMultiplexer.this, s, nextState);
        if (prev == s) {
          if (wakeUp) {
            @Nullable ClientCallStreamObserver<BeamFnApi.Elements> observer = this.requestObserver;
            if (observer != null) {
              observer.request(1);
            }
          }
          return;
        }
        s = prev;
      }
    }

    private void releaseOrParkFlowControlToken() {
      @Nullable ClientCallStreamObserver<BeamFnApi.Elements> observer = this.requestObserver;
      if (observer == null) {
        return;
      }
      int s = (int) FLOW_CONTROL_STATE.getVolatile(BeamFnDataGrpcMultiplexer.this);
      while (true) {
        if (Integer.compareUnsigned(s, BeamFnDataInboundObserver.DEFAULT_QUEUE_CAPACITY) < 0) {
          observer.request(1);
          return;
        }
        int prev =
            (int)
                FLOW_CONTROL_STATE.compareAndExchange(
                    BeamFnDataGrpcMultiplexer.this, s, s | Integer.MIN_VALUE);
        if (prev == s) {
          return;
        }
        s = prev;
      }
    }

    @SuppressWarnings("LabelledBreakTarget")
    @Override
    public void onNext(BeamFnApi.Elements value) {
      try {
        // Have a fast path to handle the common case and provide a short circuit to exit if we
        // detect multiple instruction ids.
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
      } finally {
        releaseOrParkFlowControlToken();
      }
    }

    private @Nullable ReceiverEntry getOrCreateEntry(String instructionId) {
      @Nullable ReceiverEntry entry = receivers.get(instructionId);
      if (entry != null) {
        return entry;
      }
      if (poisonedInstructionIds.getIfPresent(instructionId) != null) {
        return null;
      }
      ReceiverEntry newEntry = new ReceiverEntry();
      @Nullable ReceiverEntry existing = receivers.putIfAbsent(instructionId, newEntry);
      if (existing != null) {
        return existing;
      }
      if (poisonedInstructionIds.getIfPresent(instructionId) != null) {
        if (receivers.remove(instructionId, newEntry)) {
          releaseEntry(newEntry);
        }
        return null;
      }
      LOG.debug(
          "Received data for instruction {} without consumer ready. "
              + "Buffering until consumer is registered.",
          instructionId);
      return newEntry;
    }

    @SuppressWarnings("unchecked")
    private void forwardToConsumerForInstructionId(String instructionId, BeamFnApi.Elements value) {
      @Nullable ReceiverEntry entry = getOrCreateEntry(instructionId);
      if (entry == null) {
        LOG.debug("Received data for poisoned instruction {}. Dropping input.", instructionId);
        return;
      }

      Object s = ReceiverEntry.STATE.getVolatile(entry);
      if (!(s instanceof CloseableFnDataReceiver)) {
        boolean hasPermit = this.requestObserver != null;
        if (hasPermit) {
          FLOW_CONTROL_STATE.getAndAdd(BeamFnDataGrpcMultiplexer.this, 1);
        }
        while (true) {
          if (s == POISONED) {
            if (hasPermit) {
              accept(1);
            }
            LOG.debug("Received data for poisoned instruction {}. Dropping input.", instructionId);
            return;
          }
          if (s instanceof CloseableFnDataReceiver) {
            if (hasPermit) {
              accept(1);
            }
            break;
          }
          BufferedNode node = new BufferedNode(value, hasPermit, s);
          Object prev = ReceiverEntry.STATE.compareAndExchange(entry, s, node);
          if (prev == s) {
            return;
          }
          s = prev;
        }
      }

      CloseableFnDataReceiver<BeamFnApi.Elements> consumer =
          (CloseableFnDataReceiver<BeamFnApi.Elements>) s;
      if (consumer instanceof BeamFnDataInboundObserver && this.requestObserver != null) {
        FLOW_CONTROL_STATE.getAndAdd(BeamFnDataGrpcMultiplexer.this, 1);
        try {
          ((BeamFnDataInboundObserver) consumer).add(value);
        } catch (Exception e) {
          accept(1);
          poisonInstructionId(instructionId);
        }
      } else {
        try {
          consumer.accept(value);
        } catch (Exception e) {
          poisonInstructionId(instructionId);
        }
      }
    }

    private void drain(
        String instructionId,
        ReceiverEntry entry,
        BufferedNode initialBatch,
        CloseableFnDataReceiver<BeamFnApi.Elements> receiver) {
      boolean isFlowControlledInboundObserver =
          receiver instanceof BeamFnDataInboundObserver && this.requestObserver != null;
      @Nullable BufferedNode batch = initialBatch;
      while (true) {
        for (BufferedNode curr = BufferedNode.reverse(batch); curr != null; curr = curr.nextNode) {
          if (ReceiverEntry.STATE.getVolatile(entry) == POISONED) {
            int remainingPermits = BufferedNode.countPermits(curr);
            if (remainingPermits > 0) {
              accept(remainingPermits);
            }
            closeReceiverQuietly(receiver);
            return;
          }
          boolean hasPermit = curr.hasPermit;
          if (isFlowControlledInboundObserver) {
            if (!hasPermit) {
              FLOW_CONTROL_STATE.getAndAdd(BeamFnDataGrpcMultiplexer.this, 1);
            }
            try {
              ((BeamFnDataInboundObserver) receiver).add(curr.value);
            } catch (Exception e) {
              int remainingPermits = 1 + BufferedNode.countPermits(curr.nextNode);
              accept(remainingPermits);
              poisonInstructionId(instructionId);
              closeReceiverQuietly(receiver);
              return;
            }
          } else {
            try {
              receiver.accept(curr.value);
            } catch (Exception e) {
              int remainingPermits = (hasPermit ? 1 : 0) + BufferedNode.countPermits(curr.nextNode);
              if (remainingPermits > 0) {
                accept(remainingPermits);
              }
              poisonInstructionId(instructionId);
              closeReceiverQuietly(receiver);
              if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
              }
              throw new RuntimeException(e);
            }
            if (hasPermit) {
              accept(1);
            }
          }
        }

        Object s = ReceiverEntry.STATE.getVolatile(entry);
        while (true) {
          if (s == POISONED) {
            closeReceiverQuietly(receiver);
            return;
          }
          if (s == DRAINING) {
            Object prev = ReceiverEntry.STATE.compareAndExchange(entry, DRAINING, receiver);
            if (prev == DRAINING) {
              return;
            }
            s = prev;
          } else {
            Object prev = ReceiverEntry.STATE.compareAndExchange(entry, s, DRAINING);
            if (prev == s) {
              batch = (BufferedNode) s;
              break;
            }
            s = prev;
          }
        }
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
