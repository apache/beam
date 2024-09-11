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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.observers.StreamObserverCancelledException;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.observers.StreamObserverFactory;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.api.client.util.Sleeper;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.slf4j.Logger;

/**
 * Base class for persistent streams connecting to Windmill.
 *
 * <p>This class handles the underlying gRPC StreamObservers, and automatically reconnects the
 * stream if it is broken. Subclasses are responsible for retrying requests that have been lost on a
 * broken stream.
 *
 * <p>Subclasses should override {@link #onResponse(ResponseT)} to handle responses from the server,
 * and {@link #onNewStream()} to perform any work that must be done when a new stream is created,
 * such as sending headers or retrying requests.
 *
 * <p>{@link #send(RequestT)} and {@link #startStream()} should not be called from {@link
 * #onResponse(ResponseT)}; use {@link #executeSafely(Runnable)} instead.
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

  protected final AtomicBoolean clientClosed;
  protected final Sleeper sleeper;
  private final AtomicLong lastSendTimeMs;
  private final ExecutorService executor;
  private final BackOff backoff;
  private final AtomicLong startTimeMs;
  private final AtomicLong lastResponseTimeMs;
  private final AtomicInteger errorCount;
  private final AtomicReference<String> lastError;
  private final AtomicReference<DateTime> lastErrorTime;
  private final AtomicLong sleepUntil;
  private final CountDownLatch finishLatch;
  private final Set<AbstractWindmillStream<?, ?>> streamRegistry;
  private final int logEveryNStreamFailures;
  private final String backendWorkerToken;
  private final ResettableRequestObserver<RequestT> requestObserver;
  private final AtomicBoolean isShutdown;
  private final AtomicBoolean streamClosed;
  private final Logger logger;

  protected AbstractWindmillStream(
      Logger logger,
      String debugStreamType,
      Function<StreamObserver<ResponseT>, StreamObserver<RequestT>> clientFactory,
      BackOff backoff,
      StreamObserverFactory streamObserverFactory,
      Set<AbstractWindmillStream<?, ?>> streamRegistry,
      int logEveryNStreamFailures,
      String backendWorkerToken) {
    this.backendWorkerToken = backendWorkerToken;
    this.executor =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(createThreadName(debugStreamType, backendWorkerToken))
                .build());
    this.backoff = backoff;
    this.streamRegistry = streamRegistry;
    this.logEveryNStreamFailures = logEveryNStreamFailures;
    this.clientClosed = new AtomicBoolean();
    this.isShutdown = new AtomicBoolean(false);
    this.streamClosed = new AtomicBoolean(false);
    this.startTimeMs = new AtomicLong();
    this.lastSendTimeMs = new AtomicLong();
    this.lastResponseTimeMs = new AtomicLong();
    this.errorCount = new AtomicInteger();
    this.lastError = new AtomicReference<>();
    this.lastErrorTime = new AtomicReference<>();
    this.sleepUntil = new AtomicLong();
    this.finishLatch = new CountDownLatch(1);
    this.requestObserver =
        new ResettableRequestObserver<>(
            () ->
                streamObserverFactory.from(
                    clientFactory,
                    new AbstractWindmillStream<RequestT, ResponseT>.ResponseObserver()));
    this.sleeper = Sleeper.DEFAULT;
    this.logger = logger;
  }

  private static String createThreadName(String streamType, String backendWorkerToken) {
    return !backendWorkerToken.isEmpty()
        ? String.format("%s-%s-WindmillStream-thread", streamType, backendWorkerToken)
        : String.format("%s-WindmillStream-thread", streamType);
  }

  private static long debugDuration(long nowMs, long startMs) {
    return startMs <= 0 ? -1 : Math.max(0, nowMs - startMs);
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

  /** Reflects that {@link #shutdown()} was explicitly called. */
  protected boolean isShutdown() {
    return isShutdown.get();
  }

  private StreamObserver<RequestT> requestObserver() {
    if (requestObserver == null) {
      throw new NullPointerException(
          "requestObserver cannot be null. Missing a call to startStream() to initialize.");
    }

    return requestObserver;
  }

  /** Send a request to the server. */
  protected final synchronized void send(RequestT request) {
    if (isShutdown()) {
      return;
    }

    if (streamClosed.get()) {
      throw new IllegalStateException("Send called on a client closed stream.");
    }

    try {
      lastSendTimeMs.set(Instant.now().getMillis());
      requestObserver.onNext(request);
    } catch (StreamObserverCancelledException e) {
      if (isShutdown()) {
        logger.debug("Stream was closed or shutdown during send.", e);
        return;
      }

      requestObserver.onError(e);
    }
  }

  /** Starts the underlying stream. */
  protected final void startStream() {
    // Add the stream to the registry after it has been fully constructed.
    streamRegistry.add(this);
    while (true) {
      try {
        synchronized (this) {
          if (isShutdown.get()) {
            break;
          }
          startTimeMs.set(Instant.now().getMillis());
          lastResponseTimeMs.set(0);
          streamClosed.set(false);
          requestObserver.reset();
          onNewStream();
          if (clientClosed.get()) {
            halfClose();
          }
          return;
        }
      } catch (Exception e) {
        logger.error("Failed to create new stream, retrying: ", e);
        try {
          long sleep = backoff.nextBackOffMillis();
          sleepUntil.set(Instant.now().getMillis() + sleep);
          sleeper.sleep(sleep);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        } catch (IOException ioe) {
          // Keep trying to create the stream.
        }
      }
    }

    // We were never able to start the stream, remove it from the stream registry.
    streamRegistry.remove(this);
  }

  /**
   * Execute the runnable using the {@link #executor} handling the executor being in a shutdown
   * state.
   */
  protected final void executeSafely(Runnable runnable) {
    try {
      executor.execute(runnable);
    } catch (RejectedExecutionException e) {
      logger.debug("{}-{} has been shutdown.", getClass(), backendWorkerToken);
    } catch (IllegalStateException e) {
      // Stream was closed.
    }
  }

  public final void maybeSendHealthCheck(Instant lastSendThreshold) {
    if (!clientClosed.get() && lastSendTimeMs.get() < lastSendThreshold.getMillis()) {
      try {
        sendHealthCheck();
      } catch (RuntimeException e) {
        logger.debug("Received exception sending health check.", e);
      }
    }
  }

  protected abstract void sendHealthCheck();

  /**
   * @implNote Care is taken that synchronization on this is unnecessary for all status page
   *     information. Blocking sends are made beneath this stream object's lock which could block
   *     status page rendering.
   */
  public final void appendSummaryHtml(PrintWriter writer) {
    appendSpecificHtml(writer);
    if (errorCount.get() > 0) {
      writer.format(
          ", %d errors, last error [ %s ] at [%s]",
          errorCount.get(), lastError.get(), lastErrorTime.get());
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

  /**
   * @implNote Don't require synchronization on stream, see the {@link
   *     #appendSummaryHtml(PrintWriter)} comment.
   */
  protected abstract void appendSpecificHtml(PrintWriter writer);

  @Override
  public final void halfClose() {
    clientClosed.set(true);
    requestObserver.onCompleted();
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

  @Override
  public String backendWorkerToken() {
    return backendWorkerToken;
  }

  @Override
  public final void shutdown() {
    // Don't lock here as isShutdown checks are used in the stream to free blocked
    // threads or as exit conditions to loops.
    if (isShutdown.compareAndSet(false, true)) {
      requestObserver()
          .onError(new WindmillStreamShutdownException("Explicit call to shutdown stream."));
      shutdownInternal();
    }
  }

  private void setLastError(String error) {
    lastError.set(error);
    lastErrorTime.set(DateTime.now());
  }

  protected abstract void shutdownInternal();

  public static class WindmillStreamShutdownException extends RuntimeException {
    public WindmillStreamShutdownException(String message) {
      super(message);
    }
  }

  /**
   * Request observer that allows resetting its internal delegate using the given {@link
   * #requestObserverSupplier}.
   */
  @ThreadSafe
  private static class ResettableRequestObserver<RequestT> implements StreamObserver<RequestT> {

    private final Supplier<StreamObserver<RequestT>> requestObserverSupplier;

    @GuardedBy("this")
    private volatile @Nullable StreamObserver<RequestT> delegateRequestObserver;

    private ResettableRequestObserver(Supplier<StreamObserver<RequestT>> requestObserverSupplier) {
      this.requestObserverSupplier = requestObserverSupplier;
      this.delegateRequestObserver = null;
    }

    private synchronized StreamObserver<RequestT> delegate() {
      if (delegateRequestObserver == null) {
        throw new NullPointerException(
            "requestObserver cannot be null. Missing a call to startStream() to initialize.");
      }

      return delegateRequestObserver;
    }

    private synchronized void reset() {
      delegateRequestObserver = requestObserverSupplier.get();
    }

    @Override
    public void onNext(RequestT requestT) {
      delegate().onNext(requestT);
    }

    @Override
    public void onError(Throwable throwable) {
      delegate().onError(throwable);
    }

    @Override
    public synchronized void onCompleted() {
      delegate().onCompleted();
    }
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
      if (maybeTeardownStream()) {
        return;
      }

      Status status = Status.fromThrowable(t);
      setLastError(status.toString());

      if (t instanceof StreamObserverCancelledException) {
        logger.error(
            "StreamObserver was unexpectedly cancelled for stream={}, worker={}. stacktrace={}",
            getClass(),
            backendWorkerToken,
            t.getStackTrace(),
            t);
      } else if (errorCount.getAndIncrement() % logEveryNStreamFailures == 0) {
        // Don't log every error since it will get noisy, and many errors transient.
        logError(t, status);
      }

      // If the stream was stopped due to a resource exhausted error then we are throttled.
      if (status.getCode() == Status.Code.RESOURCE_EXHAUSTED) {
        startThrottleTimer();
      }

      try {
        long sleep = backoff.nextBackOffMillis();
        sleepUntil.set(Instant.now().getMillis() + sleep);
        sleeper.sleep(sleep);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      } catch (IOException e) {
        // Ignore.
      }

      executeSafely(AbstractWindmillStream.this::startStream);
    }

    private void logError(Throwable exception, Status status) {
      long nowMillis = Instant.now().getMillis();
      String responseDebug =
          lastResponseTimeMs.get() == 0
              ? "never received response"
              : "received response " + (nowMillis - lastResponseTimeMs.get()) + "ms ago";

      logger.debug(
          "{} streaming Windmill RPC errors for {}, last was: {} with status {}."
              + " created {}ms ago, {}. This is normal with autoscaling.",
          AbstractWindmillStream.this.getClass(),
          errorCount.get(),
          exception,
          status,
          nowMillis - startTimeMs.get(),
          responseDebug);
    }

    @Override
    public void onCompleted() {
      if (maybeTeardownStream()) {
        return;
      }

      errorCount.incrementAndGet();
      String error =
          "Stream completed successfully but did not complete requested operations, "
              + "recreating";
      logger.warn(error);
      setLastError(error);

      executeSafely(AbstractWindmillStream.this::startStream);
    }

    private synchronized boolean maybeTeardownStream() {
      if (isShutdown() || (clientClosed.get() && !hasPendingRequests())) {
        streamRegistry.remove(AbstractWindmillStream.this);
        finishLatch.countDown();
        executor.shutdownNow();
        return true;
      }

      return false;
    }
  }
}
