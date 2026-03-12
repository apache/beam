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
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.Metadata;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.MethodDescriptor;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ManagedChannel} that wraps a primary and a fallback channel. It fails over to the
 * fallback channel if the primary channel returns {@link Status#UNAVAILABLE}.
 */
@Internal
public final class FailoverChannel extends ManagedChannel {
  private static final Logger LOG = LoggerFactory.getLogger(FailoverChannel.class);
  // Time to wait before retrying the primary channel after a failure, to avoid retrying too quickly
  private static final long FALLBACK_COOLING_PERIOD_NANOS = TimeUnit.HOURS.toNanos(1);
  private final ManagedChannel primary;
  private final ManagedChannel fallback;
  @Nullable private final CallCredentials fallbackCallCredentials;
  private final AtomicBoolean useFallback = new AtomicBoolean(false);
  private final AtomicLong lastFallbackTimeNanos = new AtomicLong(0);
  private final LongSupplier nanoClock;

  private FailoverChannel(
      ManagedChannel primary,
      ManagedChannel fallback,
      @Nullable CallCredentials fallbackCallCredentials,
      LongSupplier nanoClock) {
    this.primary = primary;
    this.fallback = fallback;
    this.fallbackCallCredentials = fallbackCallCredentials;
    this.nanoClock = nanoClock;
  }

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
    if (useFallback.get()) {
      long elapsedNanos = nanoClock.getAsLong() - lastFallbackTimeNanos.get();
      if (elapsedNanos > FALLBACK_COOLING_PERIOD_NANOS) {
        if (useFallback.compareAndSet(true, false)) {
          LOG.info("Fallback cooling period elapsed. Retrying direct path.");
        }
      } else {
        CallOptions fallbackCallOptions = callOptions;
        if (fallbackCallCredentials != null && callOptions.getCredentials() == null) {
          fallbackCallOptions = callOptions.withCallCredentials(fallbackCallCredentials);
        }
        // The boolean `true` marks that the ClientCall is using the
        // fallback (cloudpath) channel. The inner call listener uses this
        // flag so `notifyFailure` will only transition to fallback when a
        // non-fallback (primary) call fails; fallback calls simply log
        // failures and do not re-trigger another fallback transition.
        return new FailoverClientCall<>(
            fallback.newCall(methodDescriptor, fallbackCallOptions),
            true,
            methodDescriptor.getFullMethodName());
      }
    }
    // The boolean `false` marks that the ClientCall is using the
    // primary (direct) channel. If this call closes with a non-OK status,
    // `notifyFailure` will flip `useFallback` to true, causing subsequent
    // calls to go to the fallback channel for the cooling period.
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

  private void notifyFailure(Status status, boolean isFallback, String methodName) {
    if (!status.isOk() && !isFallback && fallback != null) {
      if (useFallback.compareAndSet(false, true)) {
        lastFallbackTimeNanos.set(nanoClock.getAsLong());
        LOG.warn(
            "Direct path connection failed with status {} for method: {}. Falling back to"
                + " cloudpath for 1 hour.",
            status,
            methodName);
      }
    } else if (isFallback) {
      LOG.warn("Fallback channel call for method: {} closed with status: {}", methodName, status);
    }
  }

  private final class FailoverClientCall<ReqT, RespT>
      extends SimpleForwardingClientCall<ReqT, RespT> {
    private final boolean isFallback;
    private final String methodName;

    /**
     * @param delegate the underlying ClientCall (either primary or fallback)
     * @param isFallback true if `delegate` is a fallback channel call, false if it is a primary
     *     channel call. This flag is inspected by {@link #notifyFailure} to determine whether a
     *     failure should trigger switching to the fallback channel (only primary failures do).
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
}
