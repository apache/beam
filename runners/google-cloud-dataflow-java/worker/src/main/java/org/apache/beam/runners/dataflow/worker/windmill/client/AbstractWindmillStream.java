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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.annotation.concurrent.GuardedBy;
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
 * <p>{@link #trySend(RequestT)} and {@link #startStream()} should not be called from {@link
 * #onResponse(ResponseT)}; use {@link #executeSafely(Runnable)} instead.
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
  private static final String NOT_SHUTDOWN = "not shutdown";
  protected final Sleeper sleeper;

  private final Logger logger;
  private final ExecutorService executor;
  private final BackOff backoff;
  private final CountDownLatch finishLatch;
  private final Set<AbstractWindmillStream<?, ?>> streamRegistry;
  private final int logEveryNStreamFailures;
  private final String backendWorkerToken;
  private final ResettableThrowingStreamObserver<RequestT> requestObserver;
  private final StreamDebugMetrics debugMetrics;

  @GuardedBy("this")
  protected boolean clientClosed;

  @GuardedBy("this")
  protected boolean isShutdown;

  @GuardedBy("this")
  private boolean started;

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
    this.clientClosed = false;
    this.isShutdown = false;
    this.started = false;
    this.finishLatch = new CountDownLatch(1);
    this.logger = logger;
    this.requestObserver =
        new ResettableThrowingStreamObserver<>(
            () ->
                streamObserverFactory.from(
                    clientFactory,
                    new AbstractWindmillStream<RequestT, ResponseT>.ResponseObserver()),
            logger);
    this.sleeper = Sleeper.DEFAULT;
    this.debugMetrics = StreamDebugMetrics.create();
  }

  private static String createThreadName(String streamType, String backendWorkerToken) {
    return !backendWorkerToken.isEmpty()
        ? String.format("%s-%s-WindmillStream-thread", streamType, backendWorkerToken)
        : String.format("%s-WindmillStream-thread", streamType);
  }

  /** Called on each response from the server. */
  protected abstract void onResponse(ResponseT response);

  /** Called when a new underlying stream to the server has been opened. */
  protected abstract void onNewStream() throws WindmillStreamShutdownException;

  /** Returns whether there are any pending requests that should be retried on a stream break. */
  protected abstract boolean hasPendingRequests();

  /**
   * Called when the client side stream is throttled due to resource exhausted errors. Will be
   * called for each resource exhausted error not just the first. onResponse() must stop throttling
   * on receipt of the first good message.
   */
  protected abstract void startThrottleTimer();

  /** Try to send a request to the server. Returns true if the request was successfully sent. */
  @CanIgnoreReturnValue
  protected final synchronized boolean trySend(RequestT request)
      throws WindmillStreamShutdownException {
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
      startStream();
    }
  }

  /** Starts the underlying stream. */
  private void startStream() {
    // Add the stream to the registry after it has been fully constructed.
    streamRegistry.add(this);
    while (true) {
      try {
        synchronized (this) {
          debugMetrics.recordStart();
          requestObserver.reset();
          onNewStream();
          if (clientClosed) {
            halfClose();
          }
          return;
        }
      } catch (WindmillStreamShutdownException e) {
        // shutdown() is responsible for cleaning up pending requests.
        logger.debug("Stream was shutdown while creating new stream.", e);
        break;
      } catch (Exception e) {
        logger.error("Failed to create new stream, retrying: ", e);
        try {
          long sleep = backoff.nextBackOffMillis();
          debugMetrics.recordSleep(sleep);
          sleeper.sleep(sleep);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          logger.info(
              "Interrupted during {} creation backoff. The stream will not be created.",
              getClass());
          // Shutdown the stream to clean up any dangling resources and pending requests.
          shutdown();
          break;
        } catch (IOException ioe) {
          // Keep trying to create the stream.
        }
      }
    }

    // We were never able to start the stream, remove it from the stream registry. Otherwise, it is
    // removed when closed.
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
    }
  }

  public final synchronized void maybeSendHealthCheck(Instant lastSendThreshold) {
    if (!clientClosed && debugMetrics.getLastSendTimeMs() < lastSendThreshold.getMillis()) {
      try {
        sendHealthCheck();
      } catch (Exception e) {
        logger.debug("Received exception sending health check.", e);
      }
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
                    ", %d restarts, last restart reason [ %s ] at [%s], %d errors",
                    metrics.restartCount(),
                    metrics.lastRestartReason(),
                    metrics.lastRestartTime().orElse(null),
                    metrics.errorCount()));

    if (summaryMetrics.isClientClosed()) {
      writer.write(", client closed");
    }

    if (summaryMetrics.sleepLeft() > 0) {
      writer.format(", %dms backoff remaining", summaryMetrics.sleepLeft());
    }

    writer.format(
        ", current stream is %dms old, last send %dms, last response %dms, closed: %s, "
            + "shutdown time: %s",
        summaryMetrics.streamAge(),
        summaryMetrics.timeSinceLastSend(),
        summaryMetrics.timeSinceLastResponse(),
        requestObserver.isClosed(),
        summaryMetrics.shutdownTime().map(DateTime::toString).orElse(NOT_SHUTDOWN));
  }

  /**
   * @implNote Don't require synchronization on stream, see the {@link
   *     #appendSummaryHtml(PrintWriter)} comment.
   */
  protected abstract void appendSpecificHtml(PrintWriter writer);

  @Override
  public final synchronized void halfClose() {
    // Synchronization of close and onCompleted necessary for correct retry logic in onNewStream.
    debugMetrics.recordHalfClose();
    clientClosed = true;
    try {
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
    return new Instant(debugMetrics.getStartTimeMs());
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
      }
    }
  }

  protected abstract void shutdownInternal();

  /** Returns true if the stream was torn down and should not be restarted internally. */
  private synchronized boolean maybeTearDownStream() {
    if (isShutdown || (clientClosed && !hasPendingRequests())) {
      streamRegistry.remove(AbstractWindmillStream.this);
      finishLatch.countDown();
      executor.shutdownNow();
      return true;
    }

    return false;
  }

  private class ResponseObserver implements StreamObserver<ResponseT> {

    @Override
    public void onNext(ResponseT response) {
      try {
        backoff.reset();
      } catch (IOException e) {
        // Ignore.
      }
      debugMetrics.recordResponse();
      onResponse(response);
    }

    @Override
    public void onError(Throwable t) {
      if (maybeTearDownStream()) {
        return;
      }

      Status errorStatus = Status.fromThrowable(t);
      recordStreamStatus(errorStatus);

      // If the stream was stopped due to a resource exhausted error then we are throttled.
      if (errorStatus.getCode() == Status.Code.RESOURCE_EXHAUSTED) {
        startThrottleTimer();
      }

      try {
        long sleep = backoff.nextBackOffMillis();
        debugMetrics.recordSleep(sleep);
        sleeper.sleep(sleep);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      } catch (IOException e) {
        // Ignore.
      }

      executeSafely(AbstractWindmillStream.this::startStream);
    }

    @Override
    public void onCompleted() {
      if (maybeTearDownStream()) {
        return;
      }
      recordStreamStatus(OK_STATUS);
      executeSafely(AbstractWindmillStream.this::startStream);
    }

    private void recordStreamStatus(Status status) {
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
              debugMetrics
                  .responseDebugString(nowMillis)
                  .orElse(NEVER_RECEIVED_RESPONSE_LOG_STRING));
        }
      }
    }
  }
}
