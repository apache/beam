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
import java.util.function.Supplier;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Suppliers;
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
 *   <li><b>RPC Failover:</b> If primary channel RPCs fail continuously with transient errors
 *       ({@link Status.Code#UNAVAILABLE} or {@link Status.Code#UNKNOWN}), or with {@link
 *       Status.Code#DEADLINE_EXCEEDED} before receiving any response (indicating the connection was
 *       never established), for 30+ seconds without any successful response, switches to fallback
 *       channel and waits for a 1-hour cooling period before retrying primary.
 * </ul>
 */
@Internal
public final class FailoverChannel extends ManagedChannel {
  private static final Logger LOG = LoggerFactory.getLogger(FailoverChannel.class);
  private static final AtomicInteger CHANNEL_ID_COUNTER = new AtomicInteger(0);
  // Time to wait before retrying the primary channel after an RPC-based fallback.
  private static final long FALLBACK_COOLING_PERIOD_NANOS = TimeUnit.HOURS.toNanos(1);
  private static final long PRIMARY_NOT_READY_WAIT_NANOS = TimeUnit.SECONDS.toNanos(10);
  // Minimum duration of continuous RPC failures required before switching to fallback.
  private static final long RPC_FAILURE_THRESHOLD_NANOS = TimeUnit.SECONDS.toNanos(30);

  private final ManagedChannel primary;
  private final Supplier<ManagedChannel> fallbackSupplier;
  // Non-null once the fallback channel has been created.
  @Nullable private volatile ManagedChannel fallback;
  private final int channelId;
  @Nullable private final CallCredentials fallbackCallCredentials;
  private final LongSupplier nanoClock;
  // Held only during registration to prevent duplicate listener registration.
  private final AtomicBoolean stateChangeListenerRegistered = new AtomicBoolean(false);
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
    // Time when the first consecutive RPC failure was observed. -1 when no failure streak.
    @GuardedBy("this")
    long firstRPCFailureSinceNanos = -1;

    private final int channelId;
    private final long rpcFailureThresholdNanos;

    FailoverState(int channelId, long rpcFailureThresholdNanos) {
      this.channelId = channelId;
      this.rpcFailureThresholdNanos = rpcFailureThresholdNanos;
    }

    /**
     * Determines whether the next RPC should route to the fallback channel, updating internal state
     * as needed.
     */
    synchronized boolean computeUseFallback(long nowNanos) {
      // Clear RPC-based fallback if the cooling period has elapsed.
      if (useFallbackDueToRPC
          && nowNanos - lastRPCFallbackTimeNanos >= FALLBACK_COOLING_PERIOD_NANOS) {
        useFallbackDueToRPC = false;
        firstRPCFailureSinceNanos = -1;
        LOG.info(
            "[channel-{}] Primary channel cooling period elapsed; switching back from fallback.",
            channelId);
      }
      // Check if primary has been not-ready long enough to switch to fallback.
      // primaryNotReadySinceNanos is set by the state-change callback when primary is not ready.
      if (!useFallbackDueToRPC
          && !useFallbackDueToState
          && primaryNotReadySinceNanos >= 0
          && nowNanos - primaryNotReadySinceNanos > PRIMARY_NOT_READY_WAIT_NANOS) {
        useFallbackDueToState = true;
        LOG.warn(
            "[channel-{}] Primary connection unavailable. Switching to secondary connection.",
            channelId);
      }
      return useFallbackDueToRPC || useFallbackDueToState;
    }

    /**
     * Starts the not-ready grace period timer. Called by the state-change callback when primary
     * transitions to a non-ready state. Has no effect if already tracking or already on fallback.
     */
    synchronized void markPrimaryNotReady(long nowNanos) {
      if (!useFallbackDueToRPC && !useFallbackDueToState && primaryNotReadySinceNanos < 0) {
        primaryNotReadySinceNanos = nowNanos;
      }
    }

    /**
     * Clears all fallback state when the primary channel recovers (READY/IDLE callback). Returns
     * true if any fallback state was actually cleared, so the caller can log the recovery.
     */
    synchronized boolean markPrimaryReady() {
      boolean wasOnFallback = useFallbackDueToState || useFallbackDueToRPC;
      useFallbackDueToState = false;
      useFallbackDueToRPC = false;
      primaryNotReadySinceNanos = -1;
      firstRPCFailureSinceNanos = -1;
      return wasOnFallback;
    }

    /**
     * Records an RPC failure on the primary channel. Switches to RPC-based fallback only after
     * failures have persisted for {@link FailoverChannel#RPC_FAILURE_THRESHOLD_NANOS}. Returns true
     * if fallback was newly triggered so the caller can log the event.
     */
    synchronized boolean notePrimaryRpcFailure(long nowNanos) {
      if (useFallbackDueToRPC) {
        return false;
      }
      if (firstRPCFailureSinceNanos < 0) {
        if (rpcFailureThresholdNanos <= 0) {
          useFallbackDueToRPC = true;
          lastRPCFallbackTimeNanos = nowNanos;
          return true;
        }
        // This is the first failure. Start the timer.
        firstRPCFailureSinceNanos = nowNanos;
        return false;
      }
      if (nowNanos - firstRPCFailureSinceNanos >= rpcFailureThresholdNanos) {
        // Failures have persisted long enough. Switch to fallback.
        useFallbackDueToRPC = true;
        lastRPCFallbackTimeNanos = nowNanos;
        firstRPCFailureSinceNanos = -1;
        return true;
      }
      return false;
    }

    /** Resets the RPC failure streak. Called when a primary RPC succeeds. */
    synchronized void notePrimaryRpcSuccess() {
      firstRPCFailureSinceNanos = -1;
    }
  }

  private FailoverChannel(
      ManagedChannel primary,
      Supplier<ManagedChannel> fallbackSupplier,
      @Nullable CallCredentials fallbackCallCredentials,
      LongSupplier nanoClock,
      long rpcFailureThresholdNanos) {
    this.primary = primary;
    this.fallbackSupplier = Suppliers.memoize(fallbackSupplier::get);
    this.channelId = CHANNEL_ID_COUNTER.getAndIncrement();
    this.state = new FailoverState(channelId, rpcFailureThresholdNanos);
    this.fallbackCallCredentials = fallbackCallCredentials;
    this.nanoClock = nanoClock;
    // Register callback to monitor primary channel state changes
    registerPrimaryStateChangeListener();
  }

  public static FailoverChannel create(
      ManagedChannel primary,
      Supplier<ManagedChannel> fallbackSupplier,
      CallCredentials fallbackCallCredentials) {
    return new FailoverChannel(
        primary,
        fallbackSupplier,
        fallbackCallCredentials,
        System::nanoTime,
        RPC_FAILURE_THRESHOLD_NANOS);
  }

  static FailoverChannel forTest(
      ManagedChannel primary,
      ManagedChannel fallback,
      CallCredentials fallbackCallCredentials,
      LongSupplier nanoClock,
      long rpcFailureThresholdNanos) {
    return new FailoverChannel(
        primary, () -> fallback, fallbackCallCredentials, nanoClock, rpcFailureThresholdNanos);
  }

  /** Returns the fallback channel, creating it from the supplier at most once. */
  private ManagedChannel getOrCreateFallback() {
    if (fallback == null) {
      fallback = fallbackSupplier.get();
    }
    return fallback;
  }

  @Override
  public String authority() {
    return primary.authority();
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
      MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions) {
    // Read the clock before the synchronized call to avoid holding it under the state lock.
    long nowNanos = nanoClock.getAsLong();
    boolean useFallback = state.computeUseFallback(nowNanos);

    if (useFallback) {
      return new FailoverClientCall<>(
          getOrCreateFallback().newCall(methodDescriptor, applyFallbackCredentials(callOptions)),
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
    if (fallback != null) {
      fallback.shutdown();
    }
    return this;
  }

  @Override
  public ManagedChannel shutdownNow() {
    primary.shutdownNow();
    if (fallback != null) {
      fallback.shutdownNow();
    }
    return this;
  }

  @Override
  public boolean isShutdown() {
    return primary.isShutdown() && (fallback == null || fallback.isShutdown());
  }

  @Override
  public boolean isTerminated() {
    return primary.isTerminated() && (fallback == null || fallback.isTerminated());
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    long endTimeNanos = nanoClock.getAsLong() + unit.toNanos(timeout);
    boolean primaryTerminated = primary.awaitTermination(timeout, unit);
    ManagedChannel fb = fallback;
    if (fb == null) {
      return primaryTerminated;
    }
    long remainingNanos = Math.max(0, endTimeNanos - nanoClock.getAsLong());
    return primaryTerminated && fb.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS);
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
      if (state.notePrimaryRpcFailure(nanoClock.getAsLong())) {
        LOG.warn(
            "[channel-{}] Primary connection failed for method: {}. Switching to secondary"
                + " connection. Status: {}",
            channelId,
            methodName,
            status.getCode());
      }
    } else if (!isFallback && (status.isOk() || receivedResponse)) {
      // Primary RPC succeeded (clean close or received at least one response).
      // Reset the failure streak so transient errors don't accumulate toward failover.
      state.notePrimaryRpcSuccess();
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
        // Seed failover state from the current connectivity state at registration time.
        // Without this, if primary starts in a non-ready state (e.g. TRANSIENT_FAILURE) and
        // never transitions, markPrimaryNotReady() would never be called and state-based
        // failover would not trigger even after the grace period.
        if (currentState == ConnectivityState.READY || currentState == ConnectivityState.IDLE) {
          state.markPrimaryReady();
        } else {
          // Seed the not-ready timer even if there is no future state transition.
          state.markPrimaryNotReady(nanoClock.getAsLong());
        }
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

    ConnectivityState newState = primary.getState(false);
    // IDLE means the channel was READY but has no active RPCs — treat as healthy.
    if (newState == ConnectivityState.READY || newState == ConnectivityState.IDLE) {
      if (state.markPrimaryReady()) {
        LOG.info(
            "[channel-{}] Primary channel recovered; switching back from fallback.", channelId);
      }
    } else {
      // Primary is not ready; start the grace period timer so computeUseFallback can
      // switch to fallback once PRIMARY_NOT_READY_WAIT_NANOS elapses.
      state.markPrimaryNotReady(nanoClock.getAsLong());
    }

    // Always re-register for next state change (unless shutdown).
    if (!isShutdown() && !isTerminated()) {
      stateChangeListenerRegistered.set(false);
      registerPrimaryStateChangeListener();
    }
  }
}
