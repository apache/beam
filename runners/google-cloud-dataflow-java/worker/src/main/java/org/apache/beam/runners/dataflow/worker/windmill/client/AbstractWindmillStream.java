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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.observers.StreamObserverCancelledException;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.observers.StreamObserverFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.observers.TerminatingStreamObserver;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.api.client.util.Sleeper;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.StreamObserver;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Instant;
import org.slf4j.Logger;

/**
 * Base class for persistent streams connecting to Windmill.
 *
 * <p>This class handles the underlying gRPC StreamObservers, and automatically reconnects the
 * stream if it is broken. Subclasses are responsible for retrying requests that have been lost on a
 * broken stream.
 *
 * <p>Subclasses should override {@link #newResponseHandler()} to implement a handler for physical
 * stream connection. {@link #onNewStream()} to perform any work that must be done when a new stream
 * is created, such as sending headers or retrying requests.
 *
 * <p>{@link #trySend(RequestT)} and {@link #startStream()} should not be called when handling
 * responses; use {@link #executeSafely(Runnable)} instead.
 *
 * <p>Synchronization on this is used to synchronize the gRpc stream state and internal data
 * structures. Since grpc channel operations may block, synchronization on this stream may also
 * block. This is generally not a problem since streams are used in a single-threaded manner.
 * However, some accessors used for status page and other debugging need to take care not to require
 * synchronizing on this.
 *
 * <p>{@link #start()} and {@link #shutdown()} are called once in the lifetime of the stream. Once
 * {@link #shutdown()}, a stream in considered invalid and cannot be restarted/reused.
 */
public abstract class AbstractWindmillStream<RequestT, ResponseT> implements WindmillStream {

  // Default gRPC streams to 2MB chunks, which has shown to be a large enough chunk size to reduce
  // per-chunk overhead, and small enough that we can still perform granular flow-control.
  protected static final int RPC_STREAM_CHUNK_SIZE = 2 << 20;
  // Indicates that the logical stream has been half-closed and is waiting for clean server
  // shutdown.
  private static final Status OK_STATUS = Status.fromCode(Status.Code.OK);
  private static final String NEVER_RECEIVED_RESPONSE_LOG_STRING = "never received response";
  protected final Sleeper sleeper;

  private final Logger logger;
  private final ScheduledExecutorService executor;
  private final BackOff backoff;
  private final CountDownLatch finishLatch;
  private final Set<AbstractWindmillStream<?, ?>> streamRegistry;
  private final int logEveryNStreamFailures;
  private final String backendWorkerToken;

  private final Function<StreamObserver<ResponseT>, TerminatingStreamObserver<RequestT>>
      physicalStreamFactory;
  protected final long physicalStreamDeadlineSeconds;
  private final Duration halfClosePhysicalStreamAfter;
  private final ResettableThrowingStreamObserver<RequestT> requestObserver;

  private final StreamDebugMetrics debugMetrics;
  private final AtomicBoolean isHealthCheckScheduled;

  @GuardedBy("this")
  protected boolean clientClosed;

  @GuardedBy("this")
  protected boolean isShutdown;

  // The active physical grpc stream. trySend will send messages on the bi-directional stream
  // associated with this handler. The instances are created by subclasses via newResponseHandler.
  // Subclasses may wish to store additional per-physical stream state within the handler.
  @GuardedBy("this")
  protected @Nullable PhysicalStreamHandler currentPhysicalStream;

  @GuardedBy("this")
  @Nullable
  Future<?> halfCloseFuture = null;

  // Physical streams that have been half-closed and are waiting for responses or stream failure.
  @GuardedBy("this")
  protected final Set<PhysicalStreamHandler> closingPhysicalStreams;

  private final Set<PhysicalStreamHandler> closingPhysicalStreamsForDebug =
      Collections.newSetFromMap(new ConcurrentHashMap<PhysicalStreamHandler, Boolean>());

  // Generally the same as currentPhysicalStream, set under synchronization of this but can be read
  // without.
  private final AtomicReference<PhysicalStreamHandler> currentPhysicalStreamForDebug =
      new AtomicReference<>();

  @GuardedBy("this")
  private boolean started;

  // If halfClosePhysicalStream is non-zero, substreams created for the logical
  // AbstractWindmillStream
  // will be half-closed and a new physical stream will be created after this duraction.
  protected AbstractWindmillStream(
      Logger logger,
      Function<StreamObserver<ResponseT>, StreamObserver<RequestT>> clientFactory,
      BackOff backoff,
      StreamObserverFactory streamObserverFactory,
      Set<AbstractWindmillStream<?, ?>> streamRegistry,
      int logEveryNStreamFailures,
      String backendWorkerToken,
      Duration halfClosePhysicalStreamAfter,
      ScheduledExecutorService executor) {
    checkArgument(!halfClosePhysicalStreamAfter.isNegative());
    this.backendWorkerToken = backendWorkerToken;
    this.physicalStreamFactory =
        (StreamObserver<ResponseT> observer) -> streamObserverFactory.from(clientFactory, observer);
    this.physicalStreamDeadlineSeconds = streamObserverFactory.getDeadlineSeconds();
    if (!halfClosePhysicalStreamAfter.isZero()
        && halfClosePhysicalStreamAfter.compareTo(Duration.ofSeconds(physicalStreamDeadlineSeconds))
            >= 0) {
      logger.debug("Not attempting to half-close cleanly as stream deadline is shorter.");
      halfClosePhysicalStreamAfter = Duration.ZERO;
    }
    this.halfClosePhysicalStreamAfter = halfClosePhysicalStreamAfter;
    this.closingPhysicalStreams = Collections.newSetFromMap(new IdentityHashMap<>());
    this.executor = executor;
    this.backoff = backoff;
    this.streamRegistry = streamRegistry;
    this.logEveryNStreamFailures = logEveryNStreamFailures;
    this.clientClosed = false;
    this.isShutdown = false;
    this.started = false;
    this.isHealthCheckScheduled = new AtomicBoolean(false);
    this.finishLatch = new CountDownLatch(1);
    this.logger = logger;
    this.requestObserver = new ResettableThrowingStreamObserver<>(logger);
    this.sleeper = Sleeper.DEFAULT;
    this.debugMetrics = StreamDebugMetrics.create();
  }

  /** Represents a physical grpc stream that is part of the logical windmill stream. */
  protected abstract class PhysicalStreamHandler {

    /** Called on each response from the server. */
    public abstract void onResponse(ResponseT response);

    /** Returns whether there are any pending requests that should be retried on a stream break. */
    public abstract boolean hasPendingRequests();

    /**
     * Called when the physical stream has finished. For streams with requests that should be
     * retried, requests should be moved to parent state so that it is captured by the next
     * flushPendingToStream call.
     */
    public abstract void onDone(Status status);

    /**
     * Renders information useful for debugging as html.
     *
     * @implNote Don't require synchronization on AbstractWindmillStream.this, see the {@link
     *     #appendSummaryHtml(PrintWriter)} comment.
     */
    public abstract void appendHtml(PrintWriter writer);

    private final StreamDebugMetrics streamDebugMetrics = StreamDebugMetrics.create();

    @Override
    public final boolean equals(@Nullable Object obj) {
      return this == obj;
    }

    @Override
    public final int hashCode() {
      return System.identityHashCode(this);
    }
  }

  /* Constructs and returns a new handler to be associated with a physical stream. */
  protected abstract PhysicalStreamHandler newResponseHandler();

  protected abstract void onFlushPending(boolean isNewStream)
      throws WindmillStreamShutdownException;

  /** Try to send a request to the server. Returns true if the request was successfully sent. */
  @CanIgnoreReturnValue
  protected final synchronized boolean trySend(RequestT request)
      throws WindmillStreamShutdownException {
    if (currentPhysicalStream == null) {
      return false;
    }
    currentPhysicalStream.streamDebugMetrics.recordSend();
    debugMetrics.recordSend();
    try {
      requestObserver.onNext(request);
      return true;
    } catch (ResettableThrowingStreamObserver.StreamClosedException e) {
      // Stream was broken, requests may be retried when stream is reopened.
    }

    return false;
  }

  @Override
  public final void start() {
    boolean shouldStartStream = false;
    synchronized (this) {
      if (!isShutdown && !started) {
        started = true;
        shouldStartStream = true;
      }
    }

    if (shouldStartStream) {
      // Add the stream to the registry after it has been fully constructed.
      streamRegistry.add(this);
      startStream();
    }
  }

  /** Starts the underlying stream. */
  private void startStream() {
    while (true) {
      @NonNull PhysicalStreamHandler streamHandler = newResponseHandler();
      synchronized (this) {
        try {
          checkState(currentPhysicalStream == null, "Overwriting existing physical stream");
          checkState(halfCloseFuture == null, "Unexpected half-close future");
          if (isShutdown) {
            // No need to start the stream. shutdown() or onPhysicalStreamCompletion will be
            // responsible for completing shutdown.
            return;
          }
          debugMetrics.recordStart();
          streamHandler.streamDebugMetrics.recordStart();
          currentPhysicalStream = streamHandler;
          currentPhysicalStreamForDebug.set(currentPhysicalStream);
          requestObserver.reset(physicalStreamFactory.apply(new ResponseObserver(streamHandler)));
          onFlushPending(true);
          if (clientClosed) {
            // The logical stream is half-closed so after flushing the remaining requests close the
            // physical stream.
            streamHandler.streamDebugMetrics.recordHalfClose();
            requestObserver.onCompleted();
          } else if (!halfClosePhysicalStreamAfter.isZero()) {
            halfCloseFuture =
                executor.schedule(
                    () -> onHalfClosePhysicalStreamTimeout(streamHandler),
                    halfClosePhysicalStreamAfter.getSeconds(),
                    TimeUnit.SECONDS);
          }
          return;
        } catch (WindmillStreamShutdownException e) {
          logger.debug("Stream was shutdown while creating new stream.", e);
          clearCurrentPhysicalStream(true);
          break;
        } catch (Exception e) {
          logger.error("Failed to create new stream, retrying: ", e);
          clearCurrentPhysicalStream(true);
          debugMetrics.recordRestartReason("Failed to create new stream, retrying: " + e);
        }
      }
      // Backoff outside the synchronized block.
      try {
        long sleep = backoff.nextBackOffMillis();
        debugMetrics.recordSleep(sleep);
        sleeper.sleep(sleep);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        logger.info(
            "Interrupted during {} creation backoff. The stream will not be created.", getClass());
        // Shutdown the stream to clean up any dangling resources and pending requests.
        shutdown();
        break;
      }
    }
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
    }
  }

  /**
   * Schedule an application level keep-alive health check to be sent on the stream.
   *
   * @implNote This is sent asynchronously via an executor to minimize blocking. Messages are sent
   *     serially. If we recently sent a message before we attempt to schedule the health check, the
   *     stream has been restarted/closed, there is a scheduled health check that hasn't completed
   *     or there was a more recent send by the time we enter the synchronized block, we skip the
   *     attempt to send the health check.
   */
  public final void maybeScheduleHealthCheck(Instant lastSendThreshold) {
    if (debugMetrics.getLastSendTimeMs() < lastSendThreshold.getMillis()
        && isHealthCheckScheduled.compareAndSet(false, true)) {
      // Don't block other streams when sending health check.
      executeSafely(
          () -> {
            synchronized (this) {
              try {
                if (!clientClosed
                    && debugMetrics.getLastSendTimeMs() < lastSendThreshold.getMillis()) {
                  sendHealthCheck();
                }
              } catch (Exception e) {
                logger.debug("Received exception sending health check.", e);
              } finally {
                // Ready to send another health check after we attempt the scheduled health check.
                isHealthCheckScheduled.set(false);
              }
            }
          });
    }
  }

  protected abstract void sendHealthCheck() throws WindmillStreamShutdownException;

  /**
   * @implNote Care is taken that synchronization on this is unnecessary for all status page
   *     information. Blocking sends are made beneath this stream object's lock which could block
   *     status page rendering.
   */
  public final void appendSummaryHtml(PrintWriter writer) {
    appendSpecificHtml(writer);
    StreamDebugMetrics.Snapshot summaryMetrics = debugMetrics.getSummaryMetrics();
    summaryMetrics
        .restartMetrics()
        .ifPresent(
            metrics ->
                writer.format(
                    ", %d restarts, last restart reason [ %s ] at [%s], %d errors, isHealthCheckScheduled=[%s]",
                    metrics.restartCount(),
                    metrics.lastRestartReason(),
                    metrics.lastRestartTime().orElse(null),
                    metrics.errorCount(),
                    isHealthCheckScheduled.get()));

    if (summaryMetrics.isClientClosed()) {
      writer.write(", client closed");
    }

    if (summaryMetrics.sleepLeft() > 0) {
      writer.format(", %dms backoff remaining", summaryMetrics.sleepLeft());
    }

    writer.format(
        ", stream is %dms old, last send %dms, last response %dms",
        summaryMetrics.streamAge(),
        summaryMetrics.timeSinceLastSend(),
        summaryMetrics.timeSinceLastResponse());
    if (requestObserver.isClosed()) {
      writer.append(", observer closed");
    }
    summaryMetrics
        .shutdownTime()
        .ifPresent(dateTime -> writer.format(", shutdown at %s", dateTime));

    @Nullable PhysicalStreamHandler currentHandler = currentPhysicalStreamForDebug.get();
    if (currentHandler != null) {
      writer.format("<br>current physical stream: ");
      appendPhysicalStream(writer, currentHandler);
    }

    List<PhysicalStreamHandler> closingStreamsSnapshot =
        new ArrayList<>(closingPhysicalStreamsForDebug);
    for (int i = 0; i < closingStreamsSnapshot.size(); ++i) {
      writer.format("<br>closing physical stream #%d: ", i);
      appendPhysicalStream(writer, closingStreamsSnapshot.get(i));
    }
  }

  private void appendPhysicalStream(
      PrintWriter writer, PhysicalStreamHandler physicalStreamHandler) {
    physicalStreamHandler.appendHtml(writer);
    StreamDebugMetrics.Snapshot summaryMetrics =
        physicalStreamHandler.streamDebugMetrics.getSummaryMetrics();
    if (summaryMetrics.isClientClosed()) {
      writer.write(" client closed");
    }
    writer.format(
        " started %dms ago, last send %dms, last response %dms\n",
        summaryMetrics.streamAge(),
        summaryMetrics.timeSinceLastSend(),
        summaryMetrics.timeSinceLastResponse());
  }

  /**
   * Add specific debug state for the logical stream.
   *
   * @implNote Don't require synchronization on stream, see the {@link
   *     #appendSummaryHtml(PrintWriter)} comment.
   */
  protected abstract void appendSpecificHtml(PrintWriter writer);

  @Override
  public final synchronized void halfClose() {
    if (clientClosed) {
      logger.warn("Stream was previously closed.");
      return;
    }
    // Synchronization of close and onCompleted necessary for correct retry logic in
    // onPhysicalStreamCompleted.
    debugMetrics.recordHalfClose();
    clientClosed = true;
    try {
      if (currentPhysicalStream != null) {
        currentPhysicalStream.streamDebugMetrics.recordHalfClose();
      }
      requestObserver.onCompleted();
    } catch (ResettableThrowingStreamObserver.StreamClosedException e) {
      logger.warn("Stream was previously closed.");
    } catch (WindmillStreamShutdownException e) {
      logger.warn("Stream was previously shutdown.");
    } catch (IllegalStateException e) {
      logger.warn("Unexpected error when trying to close stream", e);
    }
  }

  @Override
  public final boolean awaitTermination(int time, TimeUnit unit) throws InterruptedException {
    return finishLatch.await(time, unit);
  }

  @Override
  public final Instant startTime() {
    return Instant.ofEpochMilli(debugMetrics.getStartTimeMs());
  }

  @Override
  public String backendWorkerToken() {
    return backendWorkerToken;
  }

  @Override
  public final void shutdown() {
    // Don't lock on "this" before poisoning the request observer since otherwise the observer may
    // be blocking in send().
    requestObserver.poison();
    synchronized (this) {
      if (!isShutdown) {
        isShutdown = true;
        debugMetrics.recordShutdown();
        shutdownInternal();
        if (currentPhysicalStream == null && closingPhysicalStreams.isEmpty()) {
          completeShutdown();
        }
      }
    }
  }

  private void completeShutdown() {
    logger.debug("Completing shutdown of stream after shutdown and all streams terminated.");
    streamRegistry.remove(AbstractWindmillStream.this);
    finishLatch.countDown();
    executor.shutdownNow();
  }

  protected synchronized void shutdownInternal() {}

  private class ResponseObserver implements StreamObserver<ResponseT> {
    private final PhysicalStreamHandler handler;

    ResponseObserver(PhysicalStreamHandler handler) {
      this.handler = handler;
    }

    @Override
    public void onNext(ResponseT response) {
      backoff.reset();
      debugMetrics.recordResponse();
      handler.streamDebugMetrics.recordResponse();
      handler.onResponse(response);
    }

    @Override
    public void onError(Throwable t) {
      executeSafely(() -> onPhysicalStreamCompletion(Status.fromThrowable(t), handler));
    }

    @Override
    public void onCompleted() {
      executeSafely(() -> onPhysicalStreamCompletion(OK_STATUS, handler));
    }
  }

  @SuppressWarnings("ReferenceEquality")
  private void onHalfClosePhysicalStreamTimeout(PhysicalStreamHandler handler) {
    synchronized (this) {
      if (currentPhysicalStream != handler || clientClosed || isShutdown) {
        return;
      }
      handler.streamDebugMetrics.recordHalfClose();
      closingPhysicalStreams.add(handler);
      closingPhysicalStreamsForDebug.add(handler);
      clearCurrentPhysicalStream(false);
      try {
        requestObserver.onCompleted();
      } catch (Exception e) {
        logger.debug(
            "Exception while half-closing handler, onPhysicalStreamCompletion will be called for the stream",
            e);
      }
    }
    startStream();
  }

  @SuppressWarnings("ReferenceEquality")
  private void onPhysicalStreamCompletion(Status status, PhysicalStreamHandler handler) {
    synchronized (this) {
      final boolean wasActiveStream = currentPhysicalStream == handler;
      if (wasActiveStream) {
        clearCurrentPhysicalStream(true);
      } else {
        checkState(closingPhysicalStreams.remove(handler));
        closingPhysicalStreamsForDebug.remove(handler);
      }
      boolean doneHandlerHadRequests = handler.hasPendingRequests();
      handler.onDone(status);
      if (currentPhysicalStream == null && closingPhysicalStreams.isEmpty()) {
        if (clientClosed && !doneHandlerHadRequests && !isShutdown) {
          shutdown();
        }
        if (isShutdown) {
          completeShutdown();
          return;
        }
      }
      if (currentPhysicalStream != null) {
        if (!clientClosed) {
          // Don't bother attempting to flush the requests if the active stream is closed.
          try {
            onFlushPending(false);
          } catch (WindmillStreamShutdownException e) {
            logger.debug(
                "Requests will be flushed by onPhysicalStreamCompletion of the current stream.", e);
          }
        }
        return;
      }
      if (clientClosed && !doneHandlerHadRequests) {
        // We didn't have any leftover requests and are closing so we skip restarting a stream.
        return;
      }
      // We're not shutting down and we don't have an active stream, create one.
    }

    // Backoff on errors.;
    if (!status.isOk()) {
      try {
        long sleep = backoff.nextBackOffMillis();
        debugMetrics.recordSleep(sleep);
        sleeper.sleep(sleep);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
    recordStreamRestart(status);
    startStream();
  }

  @SuppressWarnings("nullness")
  private synchronized void clearCurrentPhysicalStream(boolean cancelHalfCloseFuture) {
    currentPhysicalStream = null;
    if (halfCloseFuture != null && cancelHalfCloseFuture) {
      halfCloseFuture.cancel(false);
    }
    halfCloseFuture = null;
    currentPhysicalStreamForDebug.set(null);
  }

  private void recordStreamRestart(Status status) {
    int currentRestartCount = debugMetrics.incrementAndGetRestarts();
    if (status.isOk()) {
      String restartReason =
          "Stream completed successfully but did not complete requested operations, "
              + "recreating";
      logger.warn(restartReason);
      debugMetrics.recordRestartReason(restartReason);
    } else {
      int currentErrorCount = debugMetrics.incrementAndGetErrors();
      debugMetrics.recordRestartReason(status.toString());
      Throwable t = status.getCause();
      if (t instanceof StreamObserverCancelledException) {
        logger.error(
            "StreamObserver was unexpectedly cancelled for stream={}, worker={}. stacktrace={}",
            getClass(),
            backendWorkerToken,
            t.getStackTrace(),
            t);
      } else if (currentRestartCount % logEveryNStreamFailures == 0) {
        // Don't log every restart since it will get noisy, and many errors transient.
        long nowMillis = Instant.now().getMillis();
        logger.debug(
            "{} has been restarted {} times. Streaming Windmill RPC Error Count: {}; last was: {}"
                + " with status: {}. created {}ms ago; {}. This is normal with autoscaling.",
            AbstractWindmillStream.this.getClass(),
            currentRestartCount,
            currentErrorCount,
            t,
            status,
            nowMillis - debugMetrics.getStartTimeMs(),
            debugMetrics.responseDebugString(nowMillis).orElse(NEVER_RECEIVED_RESPONSE_LOG_STRING));
      }
    }
  }
}
