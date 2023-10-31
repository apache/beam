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
package org.apache.beam.runners.dataflow.worker.windmill.client;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.observers.StreamObserverFactory;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.StatusRuntimeException;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for persistent streams connecting to Windmill.
 *
 * <p>This class handles the underlying gRPC StreamObservers, and automatically reconnects the
 * stream if it is broken. Subclasses are responsible for retrying requests that have been lost on a
 * broken stream.
 *
 * <p>Subclasses should override onResponse to handle responses from the server, and onNewStream to
 * perform any work that must be done when a new stream is created, such as sending headers or
 * retrying requests.
 *
 * <p>send and startStream should not be called from onResponse; use executor() instead.
 *
 * <p>Synchronization on this is used to synchronize the gRpc stream state and internal data
 * structures. Since grpc channel operations may block, synchronization on this stream may also
 * block. This is generally not a problem since streams are used in a single-threaded manner.
 * However, some accessors used for status page and other debugging need to take care not to require
 * synchronizing on this.
 */
public abstract class AbstractWindmillStream<RequestT, ResponseT> implements WindmillStream {
  public static final long DEFAULT_STREAM_RPC_DEADLINE_SECONDS = 300;
  // Default gRPC streams to 2MB chunks, which has shown to be a large enough chunk size to reduce
  // per-chunk overhead, and small enough that we can still perform granular flow-control.
  protected static final int RPC_STREAM_CHUNK_SIZE = 2 << 20;
  private static final Logger LOG = LoggerFactory.getLogger(AbstractWindmillStream.class);
  protected final AtomicBoolean clientClosed;
  private final AtomicLong lastSendTimeMs;
  private final Executor executor;
  private final BackOff backoff;
  private final AtomicLong startTimeMs;
  private final AtomicLong lastResponseTimeMs;
  private final AtomicInteger errorCount;
  private final AtomicReference<String> lastError;
  private final AtomicLong sleepUntil;
  private final CountDownLatch finishLatch;
  private final Set<AbstractWindmillStream<?, ?>> streamRegistry;
  private final int logEveryNStreamFailures;
  private final Supplier<StreamObserver<RequestT>> requestObserverSupplier;
  // Indicates if the current stream in requestObserver is closed by calling close() method
  private final AtomicBoolean streamClosed;
  private @Nullable StreamObserver<RequestT> requestObserver;

  protected AbstractWindmillStream(
      Function<StreamObserver<ResponseT>, StreamObserver<RequestT>> clientFactory,
      BackOff backoff,
      StreamObserverFactory streamObserverFactory,
      Set<AbstractWindmillStream<?, ?>> streamRegistry,
      int logEveryNStreamFailures) {
    this.executor =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("WindmillStream-thread")
                .build());
    this.backoff = backoff;
    this.streamRegistry = streamRegistry;
    this.logEveryNStreamFailures = logEveryNStreamFailures;
    this.clientClosed = new AtomicBoolean();
    this.streamClosed = new AtomicBoolean();
    this.startTimeMs = new AtomicLong();
    this.lastSendTimeMs = new AtomicLong();
    this.lastResponseTimeMs = new AtomicLong();
    this.errorCount = new AtomicInteger();
    this.lastError = new AtomicReference<>();
    this.sleepUntil = new AtomicLong();
    this.finishLatch = new CountDownLatch(1);
    this.requestObserverSupplier =
        () ->
            streamObserverFactory.from(
                clientFactory, new AbstractWindmillStream<RequestT, ResponseT>.ResponseObserver());
  }

  private static long debugDuration(long nowMs, long startMs) {
    if (startMs <= 0) {
      return -1;
    }
    return Math.max(0, nowMs - startMs);
  }

  /** Called on each response from the server. */
  protected abstract void onResponse(ResponseT response);

  /** Called when a new underlying stream to the server has been opened. */
  protected abstract void onNewStream();

  /** Returns whether there are any pending requests that should be retried on a stream break. */
  protected abstract boolean hasPendingRequests();

  /**
   * Called when the client side stream is throttled due to resource exhausted errors. Will be
   * called for each resource exhausted error not just the first. onResponse() must stop throttling
   * on receipt of the first good message.
   */
  protected abstract void startThrottleTimer();

  private StreamObserver<RequestT> requestObserver() {
    if (requestObserver == null) {
      throw new NullPointerException(
          "requestObserver cannot be null. Missing a call to startStream() to initialize.");
    }

    return requestObserver;
  }

  /** Send a request to the server. */
  protected final void send(RequestT request) {
    lastSendTimeMs.set(Instant.now().getMillis());
    synchronized (this) {
      if (streamClosed.get()) {
        throw new IllegalStateException("Send called on a client closed stream.");
      }

      requestObserver().onNext(request);
    }
  }

  /** Starts the underlying stream. */
  protected final void startStream() {
    // Add the stream to the registry after it has been fully constructed.
    streamRegistry.add(this);
    while (true) {
      try {
        synchronized (this) {
          startTimeMs.set(Instant.now().getMillis());
          lastResponseTimeMs.set(0);
          streamClosed.set(false);
          // lazily initialize the requestObserver. Gets reset whenever the stream is reopened.
          requestObserver = requestObserverSupplier.get();
          onNewStream();
          if (clientClosed.get()) {
            close();
          }
          return;
        }
      } catch (Exception e) {
        LOG.error("Failed to create new stream, retrying: ", e);
        try {
          long sleep = backoff.nextBackOffMillis();
          sleepUntil.set(Instant.now().getMillis() + sleep);
          Thread.sleep(sleep);
        } catch (InterruptedException | IOException i) {
          // Keep trying to create the stream.
        }
      }
    }
  }

  protected final Executor executor() {
    return executor;
  }

  public final synchronized void maybeSendHealthCheck(Instant lastSendThreshold) {
    if (lastSendTimeMs.get() < lastSendThreshold.getMillis() && !clientClosed.get()) {
      try {
        sendHealthCheck();
      } catch (RuntimeException e) {
        LOG.debug("Received exception sending health check.", e);
      }
    }
  }

  protected abstract void sendHealthCheck();

  // Care is taken that synchronization on this is unnecessary for all status page information.
  // Blocking sends are made beneath this stream object's lock which could block status page
  // rendering.
  public final void appendSummaryHtml(PrintWriter writer) {
    appendSpecificHtml(writer);
    if (errorCount.get() > 0) {
      writer.format(", %d errors, last error [ %s ]", errorCount.get(), lastError.get());
    }
    if (clientClosed.get()) {
      writer.write(", client closed");
    }
    long nowMs = Instant.now().getMillis();
    long sleepLeft = sleepUntil.get() - nowMs;
    if (sleepLeft > 0) {
      writer.format(", %dms backoff remaining", sleepLeft);
    }
    writer.format(
        ", current stream is %dms old, last send %dms, last response %dms, closed: %s",
        debugDuration(nowMs, startTimeMs.get()),
        debugDuration(nowMs, lastSendTimeMs.get()),
        debugDuration(nowMs, lastResponseTimeMs.get()),
        streamClosed.get());
  }

  // Don't require synchronization on stream, see the appendSummaryHtml comment.
  protected abstract void appendSpecificHtml(PrintWriter writer);

  @Override
  public final synchronized void close() {
    // Synchronization of close and onCompleted necessary for correct retry logic in onNewStream.
    clientClosed.set(true);
    requestObserver().onCompleted();
    streamClosed.set(true);
  }

  @Override
  public final boolean awaitTermination(int time, TimeUnit unit) throws InterruptedException {
    return finishLatch.await(time, unit);
  }

  @Override
  public final Instant startTime() {
    return new Instant(startTimeMs.get());
  }

  private class ResponseObserver implements StreamObserver<ResponseT> {
    @Override
    public void onNext(ResponseT response) {
      try {
        backoff.reset();
      } catch (IOException e) {
        // Ignore.
      }
      lastResponseTimeMs.set(Instant.now().getMillis());
      onResponse(response);
    }

    @Override
    public void onError(Throwable t) {
      onStreamFinished(t);
    }

    @Override
    public void onCompleted() {
      onStreamFinished(null);
    }

    private void onStreamFinished(@Nullable Throwable t) {
      synchronized (this) {
        if (clientClosed.get() && !hasPendingRequests()) {
          streamRegistry.remove(AbstractWindmillStream.this);
          finishLatch.countDown();
          return;
        }
      }
      if (t != null) {
        Status status = null;
        if (t instanceof StatusRuntimeException) {
          status = ((StatusRuntimeException) t).getStatus();
        }
        String statusError = status == null ? "" : status.toString();
        lastError.set(statusError);
        if (errorCount.getAndIncrement() % logEveryNStreamFailures == 0) {
          long nowMillis = Instant.now().getMillis();
          String responseDebug;
          if (lastResponseTimeMs.get() == 0) {
            responseDebug = "never received response";
          } else {
            responseDebug =
                "received response " + (nowMillis - lastResponseTimeMs.get()) + "ms ago";
          }
          LOG.debug(
              "{} streaming Windmill RPC errors for {}, last was: {} with status {}."
                  + " created {}ms ago, {}. This is normal with autoscaling.",
              AbstractWindmillStream.this.getClass(),
              errorCount.get(),
              t,
              statusError,
              nowMillis - startTimeMs.get(),
              responseDebug);
        }
        // If the stream was stopped due to a resource exhausted error then we are throttled.
        if (status != null && status.getCode() == Status.Code.RESOURCE_EXHAUSTED) {
          startThrottleTimer();
        }

        try {
          long sleep = backoff.nextBackOffMillis();
          sleepUntil.set(Instant.now().getMillis() + sleep);
          Thread.sleep(sleep);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } catch (IOException e) {
          // Ignore.
        }
      } else {
        errorCount.incrementAndGet();
        String error =
            "Stream completed successfully but did not complete requested operations, "
                + "recreating";
        LOG.warn(error);
        lastError.set(error);
      }
      executor.execute(AbstractWindmillStream.this::startStream);
    }
  }
}
