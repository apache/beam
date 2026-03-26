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
package org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.CallCredentials;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.CallOptions;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ClientCall;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ConnectivityState;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.Metadata;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.MethodDescriptor;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ManagedChannel} that wraps a primary and a fallback channel.
 *
 * <p>Routes requests to either primary or fallback channel based on two independent failover modes:
 *
 * <ul>
 *   <li><b>Connection Status Failover:</b> If the primary channel is not ready for 10+ seconds
 *       (e.g., during network issues), routes to fallback channel. Switches back as soon as the
 *       primary channel becomes READY again.
 *   <li><b>RPC Failover:</b> If primary channel RPC fails with transient errors ({@link
 *       Status.Code#UNAVAILABLE} or {@link Status.Code#UNKNOWN}), or with {@link
 *       Status.Code#DEADLINE_EXCEEDED} before receiving any response (indicating the connection was
 *       never established) and connection status is not READY, switches to fallback channel and
 *       waits for a 1-hour cooling period before retrying primary.
 * </ul>
 */
@Internal
public final class FailoverChannel extends ManagedChannel {
  private static final Logger LOG = LoggerFactory.getLogger(FailoverChannel.class);
  private static final AtomicInteger CHANNEL_ID_COUNTER = new AtomicInteger(0);
  // Time to wait before retrying the primary channel after an RPC-based fallback.
  private static final long FALLBACK_COOLING_PERIOD_NANOS = TimeUnit.HOURS.toNanos(1);
  private static final long PRIMARY_NOT_READY_WAIT_NANOS = TimeUnit.SECONDS.toNanos(10);

  private final ManagedChannel primary;
  private final ManagedChannel fallback;
  private final int channelId;
  @Nullable private final CallCredentials fallbackCallCredentials;
  private final LongSupplier nanoClock;
  // Held only during registration to prevent duplicate listener registration.
  private final AtomicBoolean stateChangeListenerRegistered = new AtomicBoolean(false);
  // All mutable routing state is consolidated here to ensure related fields are updated atomically.
  private final FailoverState state;

  private static final class FailoverState {
    // Set when primary's connection state has been unavailable for too long.
    @GuardedBy("this")
    boolean useFallbackDueToState;
    // Set when an RPC on primary fails with an error.
    @GuardedBy("this")
    boolean useFallbackDueToRPC;
    // Timestamp when RPC-based fallback was triggered. Only meaningful when useFallbackDueToRPC
    // is true.
    @GuardedBy("this")
    long lastRPCFallbackTimeNanos;
    // Time when primary first became not-ready. -1 when primary is currently READY.
    @GuardedBy("this")
    long primaryNotReadySinceNanos = -1;

    private final int channelId;

    FailoverState(int channelId) {
      this.channelId = channelId;
    }

    /**
     * Determines whether the next RPC should route to the fallback channel, updating internal state
     * as needed.
     */
    synchronized boolean computeUseFallback(long nowNanos, ConnectivityState primaryState) {
      // Clear RPC-based fallback if the cooling period has elapsed.
      if (useFallbackDueToRPC
          && nowNanos - lastRPCFallbackTimeNanos >= FALLBACK_COOLING_PERIOD_NANOS) {
        useFallbackDueToRPC = false;
        LOG.info(
            "[channel-{}] Primary channel cooling period elapsed; switching back from fallback.",
            channelId);
      }
      // If not already on fallback, check primary connectivity state.
      // gRPC's state machine only transitions to IDLE from READY. Treat both as healthy.
      if (!useFallbackDueToRPC && !useFallbackDueToState) {
        if (primaryState == ConnectivityState.READY || primaryState == ConnectivityState.IDLE) {
          primaryNotReadySinceNanos = -1;
        } else {
          if (primaryNotReadySinceNanos < 0) {
            primaryNotReadySinceNanos = nowNanos;
          }
          if (nowNanos - primaryNotReadySinceNanos > PRIMARY_NOT_READY_WAIT_NANOS
              && !useFallbackDueToState) {
            useFallbackDueToState = true;
            LOG.warn(
                "[channel-{}] Primary connection unavailable. Switching to secondary connection.",
                channelId);
          }
        }
      }
      return useFallbackDueToRPC || useFallbackDueToState;
    }

    /**
     * Transitions the fallback state.
     * When toFallback is true (RPC failure) it enables RPC-based fallback if
     * not already active and returns true so the caller can log the failure details.
     * When toFallback is false (primary recovered) it clears all fallback flags
     * and returns true if recovery actually changed state, so the caller can log it.
     */
    synchronized boolean transitionFallback(boolean toFallback, long nowNanos) {
      if (toFallback) {
        if (!useFallbackDueToRPC) {
          useFallbackDueToRPC = true;
          lastRPCFallbackTimeNanos = nowNanos;
          // Return true to indicate fallback state was changed and caller should log the event.
          return true;
        }
        // Already in RPC-based fallback, no state change.
        return false;
      }
      // Clear all fallback state as primary has recovered.
      boolean wasOnFallback = useFallbackDueToState || useFallbackDueToRPC;
      useFallbackDueToState = false;
      useFallbackDueToRPC = false;
      primaryNotReadySinceNanos = -1;
      return wasOnFallback;
    }
  }

  private FailoverChannel(
      ManagedChannel primary,
      ManagedChannel fallback,
      @Nullable CallCredentials fallbackCallCredentials,
      LongSupplier nanoClock) {
    this.primary = primary;
    this.fallback = fallback;
    this.channelId = CHANNEL_ID_COUNTER.getAndIncrement();
    this.state = new FailoverState(channelId);
    this.fallbackCallCredentials = fallbackCallCredentials;
    this.nanoClock = nanoClock;
    // Register callback to monitor primary channel state changes
    registerPrimaryStateChangeListener();
  }

  public static FailoverChannel create(
      ManagedChannel primary, ManagedChannel fallback, CallCredentials fallbackCallCredentials) {
    return new FailoverChannel(primary, fallback, fallbackCallCredentials, System::nanoTime);
  }

  static FailoverChannel forTest(
      ManagedChannel primary,
      ManagedChannel fallback,
      CallCredentials fallbackCallCredentials,
      LongSupplier nanoClock) {
    return new FailoverChannel(primary, fallback, fallbackCallCredentials, nanoClock);
  }

  @Override
  public String authority() {
    return primary.authority();
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
      MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions) {
    // Read connectivity state and clock before the synchronized call to avoid holding external
    // APIs under the state lock.
    ConnectivityState primaryState = primary.getState(false);
    long nowNanos = nanoClock.getAsLong();
    boolean useFallback = state.computeUseFallback(nowNanos, primaryState);

    if (useFallback) {
      return new FailoverClientCall<>(
          fallback.newCall(methodDescriptor, applyFallbackCredentials(callOptions)),
          true,
          methodDescriptor.getFullMethodName());
    }

    return new FailoverClientCall<>(
        primary.newCall(methodDescriptor, callOptions),
        false,
        methodDescriptor.getFullMethodName());
  }

  @Override
  public ManagedChannel shutdown() {
    primary.shutdown();
    fallback.shutdown();
    return this;
  }

  @Override
  public ManagedChannel shutdownNow() {
    primary.shutdownNow();
    fallback.shutdownNow();
    return this;
  }

  @Override
  public boolean isShutdown() {
    return primary.isShutdown() && fallback.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return primary.isTerminated() && fallback.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    long endTimeNanos = nanoClock.getAsLong() + unit.toNanos(timeout);
    boolean primaryTerminated = primary.awaitTermination(timeout, unit);
    long remainingNanos = Math.max(0, endTimeNanos - nanoClock.getAsLong());
    return primaryTerminated && fallback.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS);
  }

  private boolean shouldFallbackBasedOnRPCStatus(Status status, boolean receivedResponse) {
    // If a response was received, the connection was healthy and any error is an application-level
    // issue, not a connectivity problem. Never failover in this case regardless of status.
    if (receivedResponse) {
      return false;
    }
    switch (status.getCode()) {
      case UNAVAILABLE:
      case UNKNOWN:
      case DEADLINE_EXCEEDED:
        return true;
      default:
        return false;
    }
  }

  private CallOptions applyFallbackCredentials(CallOptions callOptions) {
    if (fallbackCallCredentials != null) {
      return callOptions.withCallCredentials(fallbackCallCredentials);
    }
    return callOptions;
  }

  private void notifyCallDone(
      Status status, boolean isFallback, String methodName, boolean receivedResponse) {
    if (!status.isOk() && !isFallback && shouldFallbackBasedOnRPCStatus(status, receivedResponse)) {
      if (state.transitionFallback(true, nanoClock.getAsLong())) {
        LOG.warn(
            "[channel-{}] Primary connection failed for method: {}. Switching to secondary"
                + " connection. Status: {}",
            channelId,
            methodName,
            status.getCode());
      }
    } else if (isFallback && !status.isOk()) {
      LOG.warn(
          "[channel-{}] Secondary connection failed for method: {}. Status: {}",
          channelId,
          methodName,
          status.getCode());
    }
  }

  private final class FailoverClientCall<ReqT, RespT>
      extends SimpleForwardingClientCall<ReqT, RespT> {
    private final boolean isFallback;
    private final String methodName;
    // Tracks whether any response message was received. Volatile ensures the write in onMessage
    // is visible to the read in onClose even if they execute on different threads within gRPC's
    // SerializingExecutor.
    private volatile boolean receivedResponse = false;

    /**
     * @param delegate the underlying ClientCall (either primary or fallback)
     * @param isFallback true if {@code delegate} is a fallback channel call, false if it is a
     *     primary channel call. This flag is inspected by {@link #notifyCallDone} to determine
     *     whether a failure should trigger switching to the fallback channel (only primary failures
     *     do).
     * @param methodName gRPC method name (for logging)
     */
    FailoverClientCall(ClientCall<ReqT, RespT> delegate, boolean isFallback, String methodName) {
      super(delegate);
      this.isFallback = isFallback;
      this.methodName = methodName;
    }

    @Override
    public void start(Listener<RespT> responseListener, Metadata headers) {
      super.start(
          new SimpleForwardingClientCallListener<RespT>(responseListener) {
            @Override
            public void onMessage(RespT message) {
              receivedResponse = true;
              super.onMessage(message);
            }

            @Override
            public void onClose(Status status, Metadata trailers) {
              notifyCallDone(status, isFallback, methodName, receivedResponse);
              super.onClose(status, trailers);
            }
          },
          headers);
    }
  }

  /** Registers callback for primary channel state changes. */
  private void registerPrimaryStateChangeListener() {
    if (!stateChangeListenerRegistered.getAndSet(true)) {
      try {
        ConnectivityState currentState = primary.getState(false);
        primary.notifyWhenStateChanged(currentState, this::onPrimaryStateChanged);
      } catch (Exception e) {
        LOG.warn(
            "[channel-{}] Failed to register channel state monitor. Continuing with fallback detection.",
            channelId,
            e);
        stateChangeListenerRegistered.set(false);
      }
    }
  }

  /** Callback invoked when primary channel connectivity state changes. */
  private void onPrimaryStateChanged() {
    if (isShutdown() || isTerminated()) {
      return;
    }

    // If primary is READY, clear both fallback flags so we immediately resume routing there,
    // regardless of which failover mode triggered the switch.
    if (primary.getState(false) == ConnectivityState.READY) {
      if (state.transitionFallback(false, 0)) {
        LOG.info(
            "[channel-{}] Primary channel recovered; switching back from fallback.", channelId);
      }
    }

    // Always re-register for next state change (unless shutdown).
    if (!isShutdown() && !isTerminated()) {
      stateChangeListenerRegistered.set(false);
      registerPrimaryStateChangeListener();
    }
  }
}
