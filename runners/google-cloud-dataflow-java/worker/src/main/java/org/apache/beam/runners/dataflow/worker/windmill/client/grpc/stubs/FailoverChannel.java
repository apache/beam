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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import javax.annotation.Nullable;
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
 *       Status.Code#UNAVAILABLE}, {@link Status.Code#DEADLINE_EXCEEDED}, or {@link
 *       Status.Code#UNKNOWN}), switches to fallback channel and waits for a 1-hour cooling period
 *       before retrying primary.
 * </ul>
 */
@Internal
public final class FailoverChannel extends ManagedChannel {
  private static final Logger LOG = LoggerFactory.getLogger(FailoverChannel.class);
  // Time to wait before retrying the primary channel after an RPC-based fallback.
  private static final long FALLBACK_COOLING_PERIOD_NANOS = TimeUnit.HOURS.toNanos(1);
  private static final long PRIMARY_NOT_READY_WAIT_NANOS = TimeUnit.SECONDS.toNanos(10);
  private final ManagedChannel primary;
  @Nullable private final ManagedChannel fallback;
  @Nullable private final CallCredentials fallbackCallCredentials;
  // Set when primary's connection state has been unavailable for too long.
  private final AtomicBoolean useFallbackDueToState = new AtomicBoolean(false);
  // Set when an RPC on primary fails with a transient error.
  private final AtomicBoolean useFallbackDueToRPC = new AtomicBoolean(false);
  private final AtomicLong lastRPCFallbackTimeNanos = new AtomicLong(0);
  private final AtomicLong primaryNotReadySinceNanos = new AtomicLong(-1);
  private final LongSupplier nanoClock;
  private final AtomicBoolean stateChangeListenerRegistered = new AtomicBoolean(false);

  private FailoverChannel(
      ManagedChannel primary,
      @Nullable ManagedChannel fallback,
      @Nullable CallCredentials fallbackCallCredentials,
      LongSupplier nanoClock) {
    this.primary = primary;
    this.fallback = fallback;
    this.fallbackCallCredentials = fallbackCallCredentials;
    this.nanoClock = nanoClock;
    // Register callback to monitor primary channel state changes
    registerPrimaryStateChangeListener();
  }

  // Test-only.
  public static FailoverChannel create(ManagedChannel primary, ManagedChannel fallback) {
    return new FailoverChannel(primary, fallback, null, System::nanoTime);
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
    // Check if the RPC-based cooling period has elapsed.
    if (useFallbackDueToRPC.get()) {
      long timeSinceLastFallback = nanoClock.getAsLong() - lastRPCFallbackTimeNanos.get();
      if (timeSinceLastFallback >= FALLBACK_COOLING_PERIOD_NANOS) {
        if (useFallbackDueToRPC.compareAndSet(true, false)) {
          LOG.info("Primary channel cooling period elapsed; switching back from fallback.");
        }
      }
    }

    if (fallback != null && (useFallbackDueToRPC.get() || useFallbackDueToState.get())) {
      return new FailoverClientCall<>(
          fallback.newCall(methodDescriptor, applyFallbackCredentials(callOptions)),
          true,
          methodDescriptor.getFullMethodName());
    }

    // If primary has not become ready for a sustained period, fail over to fallback.
    if (fallback != null && shouldFallBackDueToPrimaryState()) {
      if (useFallbackDueToState.compareAndSet(false, true)) {
        LOG.warn("Primary connection unavailable. Switching to secondary connection.");
      }
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
    if (fallback != null) {
      long remainingNanos = Math.max(0, endTimeNanos - nanoClock.getAsLong());
      return primaryTerminated && fallback.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS);
    }
    return primaryTerminated;
  }

  private boolean shouldFallbackBasedOnRPCStatus(Status status) {
    switch (status.getCode()) {
      case UNAVAILABLE:
      case DEADLINE_EXCEEDED:
      case UNKNOWN:
        return true;
      default:
        return false;
    }
  }

  private boolean hasFallbackChannel() {
    return fallback != null;
  }

  private CallOptions applyFallbackCredentials(CallOptions callOptions) {
    if (fallbackCallCredentials != null && callOptions.getCredentials() == null) {
      return callOptions.withCallCredentials(fallbackCallCredentials);
    }
    return callOptions;
  }

  private boolean shouldFallBackDueToPrimaryState() {
    ConnectivityState connectivityState = primary.getState(true);
    if (connectivityState == ConnectivityState.READY) {
      primaryNotReadySinceNanos.set(-1);
      return false;
    }
    long currentTimeNanos = nanoClock.getAsLong();
    if (primaryNotReadySinceNanos.get() < 0) {
      primaryNotReadySinceNanos.set(currentTimeNanos);
    }
    return currentTimeNanos - primaryNotReadySinceNanos.get() > PRIMARY_NOT_READY_WAIT_NANOS;
  }

  private void notifyFailure(Status status, boolean isFallback, String methodName) {
    if (!status.isOk()
        && !isFallback
        && hasFallbackChannel()
        && shouldFallbackBasedOnRPCStatus(status)) {
      if (useFallbackDueToRPC.compareAndSet(false, true)) {
        lastRPCFallbackTimeNanos.set(nanoClock.getAsLong());
        LOG.warn(
            "Primary connection failed for method: {}. Switching to secondary connection. Status: {}",
            methodName,
            status.getCode());
      }
    } else if (isFallback && !status.isOk()) {
      LOG.warn(
          "Secondary connection failed for method: {}. Status: {}", methodName, status.getCode());
    }
  }

  private final class FailoverClientCall<ReqT, RespT>
      extends SimpleForwardingClientCall<ReqT, RespT> {
    private final boolean isFallback;
    private final String methodName;

    /**
     * @param delegate the underlying ClientCall (either primary or fallback)
     * @param isFallback true if {@code delegate} is a fallback channel call, false if it is a
     *     primary channel call. This flag is inspected by {@link #notifyFailure} to determine
     *     whether a failure should trigger switching to the fallback channel (only primary failures
     *     do).
     * @param methodName full gRPC method name (for logging)
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
            public void onClose(Status status, Metadata trailers) {
              notifyFailure(status, isFallback, methodName);
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
            "Failed to register channel state monitor. Continuing with fallback detection.", e);
        stateChangeListenerRegistered.set(false);
      }
    }
  }

  /** Callback invoked when primary channel connectivity state changes. */
  private void onPrimaryStateChanged() {
    if (isShutdown() || isTerminated()) {
      return;
    }

    // If primary is READY, clear state-based fallback immediately.
    if (primary.getState(false) == ConnectivityState.READY) {
      if (useFallbackDueToState.compareAndSet(true, false)) {
        LOG.info("Primary channel recovered; switching back from fallback.");
      }
    }

    // Always re-register for next state change (unless shutdown)
    if (!isShutdown() && !isTerminated()) {
      stateChangeListenerRegistered.set(false);
      registerPrimaryStateChangeListener();
    }
  }
}
