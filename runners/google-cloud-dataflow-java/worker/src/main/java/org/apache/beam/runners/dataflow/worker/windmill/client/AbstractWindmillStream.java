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
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.observers.StreamObserverCancelledException;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.observers.StreamObserverFactory;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
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
  private final WindmillStream.Id streamId;
  private final AtomicLong lastSendTimeMs;
  private final Executor executor;
  private final ExecutorService requestSender;
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
  private final UpdatableDelegateRequestObserver<RequestT> requestObserver;
  // Indicates if the current stream in requestObserver is closed by calling close() method
  private final AtomicBoolean streamClosed;
  private final AtomicBoolean isShutdown;

  protected AbstractWindmillStream(
      Function<StreamObserver<ResponseT>, StreamObserver<RequestT>> clientFactory,
      BackOff backoff,
      StreamObserverFactory streamObserverFactory,
      Set<AbstractWindmillStream<?, ?>> streamRegistry,
      int logEveryNStreamFailures,
      String backendWorkerToken) {
    this.streamId =
        WindmillStream.Id.create(this, backendWorkerToken, !backendWorkerToken.isEmpty());
    this.executor =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat(streamId + "-thread").build());
    this.requestSender =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(streamId + "-RequestThread")
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
    this.lastErrorTime = new AtomicReference<>();
    this.sleepUntil = new AtomicLong();
    this.finishLatch = new CountDownLatch(1);
    this.isShutdown = new AtomicBoolean(false);
    this.requestObserver =
        new UpdatableDelegateRequestObserver<>(
            () ->
                streamObserverFactory.from(
                    clientFactory,
                    new AbstractWindmillStream<RequestT, ResponseT>.ResponseObserver()));
  }

  private static String debugDuration(long nowMs, long startMs) {
    return (startMs <= 0 ? -1 : Math.max(0, nowMs - startMs)) + "ms";
  }

  @Override
  public final Id id() {
    return streamId;
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

  /**
   * Send a request to the server.
   *
   * @implNote Requests are sent on a dedicated RequestThread via {@link #requestSender} to not
   *     block external callers from closing/shutting down the stream.
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  protected final void send(RequestT request) {
    lastSendTimeMs.set(Instant.now().getMillis());
    synchronized (this) {
      if (streamClosed.get()) {
        throw new IllegalStateException("Send called on a client closed stream.");
      }

      try {
        requestSender.submit(
            () -> {
              if (isShutdown()) {
                return;
              }
              try {
                requestObserver.onNext(request);
              } catch (StreamObserverCancelledException e) {
                if (isClosed() || isShutdown()) {
                  LOG.warn("Stream was closed or shutdown during send.", e);
                  return;
                }
                LOG.error("StreamObserver was unexpectedly cancelled.", e);
                throw e;
              }
            });
      } catch (RejectedExecutionException e) {
        LOG.warn(
            "{} is shutdown and will not send or receive any more requests or responses.",
            streamId,
            e);
      }
    }
  }

  /** Starts the underlying stream. */
  protected final void startStream() {
    // Add the stream to the registry after it has been fully constructed.
    streamRegistry.add(this);
    while (!isShutdown()) {
      try {
        synchronized (this) {
          startTimeMs.set(Instant.now().getMillis());
          lastResponseTimeMs.set(0);
          streamClosed.set(false);
          // lazily initialize the requestObserver. Gets reset whenever the stream is reopened.
          requestObserver.reset();
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

  /**
   * @implNote Care is taken that synchronization on this is unnecessary for all status page
   *     information.Blocking sends are made beneath this stream object's lock which could block
   *     status page rendering.
   */
  public final void appendSummaryHtml(PrintWriter writer) {
    writer.format("<h3>%s:</h3>", streamId);
    appendSpecificHtml(writer);
    writer.println("<strong>Status:</strong>");
    writer.println(
        "<table border=\"1\" "
            + "style=\"border-collapse:collapse;padding:5px;border-spacing:5px;border:1px\">");
    writer.println(
        "<tr>"
            + "<th>Error Count</th>"
            + "<th>Is Shutdown</th>"
            + "<th>Last Error</th>"
            + "<th>Last Error Received Time</th>"
            + "<th>Is Client Closed</th>"
            + "<th>Is Closed</th>"
            + "<th>BackOff Remaining</th>"
            + "<th>Current Stream Age Millis</th>"
            + "<th>Last Request Sent Time</th>"
            + "<th>Last Received Response Time</th>"
            + "</tr>");

    StringBuilder statusString = new StringBuilder();
    statusString.append("<tr>");
    statusString.append("<td>");
    if (errorCount.get() > 0) {
      statusString.append(errorCount.get());
      statusString.append("</td><td>");
      statusString.append(isShutdown.get());
      statusString.append("</td><td>");
      statusString.append(lastError.get());
      statusString.append("</td><td>");
      statusString.append(lastErrorTime.get());
    } else {
      statusString.append(0);
      statusString.append("</td><td>");
      statusString.append("N/A");
      statusString.append("</td><td>");
      statusString.append("N/A");
    }
    statusString.append("</td><td>");

    statusString.append(clientClosed.get());
    statusString.append("</td><td>");
    statusString.append(isClosed());
    statusString.append("</td><td>");
    long nowMs = Instant.now().getMillis();
    long sleepLeft = sleepUntil.get() - nowMs;
    statusString.append(Math.max(sleepLeft, 0));
    statusString.append("</td><td>");
    statusString.append(debugDuration(nowMs, startTimeMs.get()));
    statusString.append("</td><td>");
    statusString.append(debugDuration(nowMs, lastSendTimeMs.get()));
    statusString.append("</td><td>");
    statusString.append(debugDuration(nowMs, lastResponseTimeMs.get()));
    statusString.append("</td></tr>\n");
    writer.print(statusString);
    writer.println("</table>");
  }

  // Don't require synchronization on stream, see the appendSummaryHtml comment.
  protected abstract void appendSpecificHtml(PrintWriter writer);

  @Override
  public final synchronized void close() {
    // Synchronization of close and onCompleted necessary for correct retry logic in onNewStream.
    clientClosed.set(true);
    requestObserver.onCompleted();
    streamClosed.set(true);
  }

  @Override
  public synchronized void shutdown() {
    if (isShutdown.compareAndSet(false, true)) {
      close();
      requestSender.shutdownNow();
    }
  }

  @Override
  public boolean isShutdown() {
    return isShutdown.get();
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
  public final boolean isClosed() {
    return clientClosed.get() || streamClosed.get();
  }

  private void setLastError(String error) {
    lastError.set(error);
    lastErrorTime.set(DateTime.now());
  }

  /** Request observer that allows updating its internal delegate. */
  @ThreadSafe
  private static class UpdatableDelegateRequestObserver<RequestT>
      implements StreamObserver<RequestT> {
    private final Supplier<StreamObserver<RequestT>> requestObserverSupplier;
    private final AtomicReference<StreamObserver<RequestT>> delegateRequestObserver;

    private UpdatableDelegateRequestObserver(
        Supplier<StreamObserver<RequestT>> requestObserverSupplier) {
      this.requestObserverSupplier = requestObserverSupplier;
      this.delegateRequestObserver = new AtomicReference<>();
    }

    private synchronized StreamObserver<RequestT> delegate() {
      if (delegateRequestObserver.get() == null) {
        throw new NullPointerException(
            "requestObserver cannot be null. Missing a call to startStream() to initialize.");
      }

      return delegateRequestObserver.get();
    }

    private synchronized void reset() {
      delegateRequestObserver.set(requestObserverSupplier.get());
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
    public void onCompleted() {
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
      onStreamFinished(t);
    }

    @Override
    public void onCompleted() {
      onStreamFinished(null);
    }

    private void onStreamFinished(@Nullable Throwable t) {
      synchronized (this) {
        if (isShutdown() || (clientClosed.get() && !hasPendingRequests())) {
          streamRegistry.remove(AbstractWindmillStream.this);
          finishLatch.countDown();
          return;
        }
      }
      if (t != null) {
        handleStreamErrorResponse(t);
      } else {
        errorCount.incrementAndGet();
        String error =
            "Stream completed successfully but did not complete requested operations, "
                + "recreating.";
        LOG.warn(error);
        setLastError(error);
      }

      executor.execute(AbstractWindmillStream.this::startStream);
    }

    private void handleStreamErrorResponse(Throwable t) {
      Status errorStatus = Status.fromThrowable(t);
      setLastError(errorStatus.toString());
      if (errorCount.getAndIncrement() % logEveryNStreamFailures == 0) {
        long nowMillis = Instant.now().getMillis();
        String logMessage =
            lastResponseTimeMs.get() == 0
                ? "never received response"
                : "received response " + (nowMillis - lastResponseTimeMs.get()) + "ms ago";

        LOG.debug(
            "{} streaming Windmill RPC errors for {}, last was: {} with status {}."
                + " created {}ms ago, {}. This is normal with autoscaling.",
            AbstractWindmillStream.this.getClass(),
            errorCount.get(),
            t,
            errorStatus,
            nowMillis - startTimeMs.get(),
            logMessage);
      }
      // If the stream was stopped due to a resource exhausted error then we are throttled.
      if (errorStatus.getCode() == Status.Code.RESOURCE_EXHAUSTED) {
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
    }
  }
}
