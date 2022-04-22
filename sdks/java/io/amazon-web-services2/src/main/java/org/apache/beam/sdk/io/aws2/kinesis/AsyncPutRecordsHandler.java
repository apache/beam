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
package org.apache.beam.sdk.io.aws2.kinesis;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Streams;
import org.joda.time.DateTimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;

/**
 * Async handler for {@link KinesisAsyncClient#putRecords(PutRecordsRequest)} that automatically
 * retries unprocessed records in case of a partial success.
 *
 * <p>The handler enforces the provided upper limit of concurrent requests. Once that limit is
 * reached any further call to {@link #putRecords(String, List)} will block until another request
 * completed.
 *
 * <p>The handler is fail fast and won't submit any further request after a failure. Async failures
 * can be polled using {@link #checkForAsyncFailure()}.
 */
@NotThreadSafe
@Internal
class AsyncPutRecordsHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncPutRecordsHandler.class);
  private final KinesisAsyncClient kinesis;
  private final Supplier<BackOff> backoff;
  private final int concurrentRequests;
  private final Stats stats;

  private AtomicBoolean hasErrored;
  private AtomicReference<Throwable> asyncFailure;
  private Semaphore pendingRequests;

  AsyncPutRecordsHandler(
      KinesisAsyncClient kinesis, int concurrency, Supplier<BackOff> backoff, Stats stats) {
    this.kinesis = kinesis;
    this.backoff = backoff;
    this.concurrentRequests = concurrency;
    this.hasErrored = new AtomicBoolean(false);
    this.asyncFailure = new AtomicReference<>();
    this.pendingRequests = new Semaphore(concurrentRequests);
    this.stats = stats;
  }

  AsyncPutRecordsHandler(
      KinesisAsyncClient kinesis, int concurrency, FluentBackoff backoff, Stats stats) {
    this(kinesis, concurrency, () -> backoff.backoff(), stats);
  }

  protected int pendingRequests() {
    return concurrentRequests - pendingRequests.availablePermits();
  }

  void reset() {
    hasErrored = new AtomicBoolean(false);
    asyncFailure = new AtomicReference<>();
    pendingRequests = new Semaphore(concurrentRequests);
  }

  /** If this handler has errored since it was last reset. */
  boolean hasErrored() {
    return hasErrored.get();
  }

  /**
   * Check if any failure happened async.
   *
   * @throws Throwable The last async failure, afterwards reset it.
   */
  void checkForAsyncFailure() throws Throwable {
    @SuppressWarnings("nullness")
    Throwable failure = asyncFailure.getAndSet(null);
    if (failure != null) {
      throw failure;
    }
  }

  /**
   * Wait for all pending requests to complete and check for failures.
   *
   * @throws Throwable The last async failure if present using {@link #checkForAsyncFailure()}
   */
  void waitForCompletion() throws Throwable {
    pendingRequests.acquireUninterruptibly(concurrentRequests);
    checkForAsyncFailure();
  }

  /**
   * Asynchronously trigger a put records request.
   *
   * <p>This will respect the concurrency limit of the handler and first wait for a permit.
   *
   * @throws Throwable The last async failure if present using {@link #checkForAsyncFailure()}
   */
  void putRecords(String stream, List<PutRecordsRequestEntry> records) throws Throwable {
    pendingRequests.acquireUninterruptibly();
    new RetryHandler(stream, records).run();
    checkForAsyncFailure();
  }

  interface Stats {
    void addPutRecordsRequest(long latencyMillis, boolean isPartialRetry);
  }

  /**
   * This handler coordinates retries in case of a partial success.
   *
   * <ul>
   *   <li>Release permit if all (remaining) records are successful to allow for a new request to
   *       start.
   *   <li>Attempt retry in case of partial success for all erroneous records using backoff. Set
   *       async failure once retries are exceeded.
   *   <li>Set async failure if the entire request fails. Retries, if configured & applicable, have
   *       already been attempted by the AWS SDK in that case.
   * </ul>
   *
   * The next call of {@link #checkForAsyncFailure()}, {@link #putRecords(String, List)} or {@link
   * #waitForCompletion()} will check for the last async failure and throw it. Afterwards the
   * failure state is reset.
   */
  private class RetryHandler implements BiConsumer<PutRecordsResponse, Throwable> {
    private final int totalRecords;
    private final String stream;
    private final BackOff backoff; // backoff in case of throttling

    private final long handlerStartTime;
    private long requestStartTime;
    private int requests;

    private List<PutRecordsRequestEntry> records;

    RetryHandler(String stream, List<PutRecordsRequestEntry> records) {
      this.stream = stream;
      this.totalRecords = records.size();
      this.records = records;
      this.backoff = AsyncPutRecordsHandler.this.backoff.get();
      this.handlerStartTime = DateTimeUtils.currentTimeMillis();
      this.requestStartTime = 0;
      this.requests = 0;
    }

    @SuppressWarnings({"FutureReturnValueIgnored"})
    void run() {
      if (!hasErrored.get()) {
        try {
          requests++;
          requestStartTime = DateTimeUtils.currentTimeMillis();
          PutRecordsRequest request =
              PutRecordsRequest.builder().streamName(stream).records(records).build();
          kinesis.putRecords(request).whenComplete(this);
        } catch (Throwable e) {
          setAsyncFailure(e);
        }
      }
    }

    @Override
    public void accept(PutRecordsResponse response, Throwable throwable) {
      try {
        long now = DateTimeUtils.currentTimeMillis();
        long latencyMillis = now - requestStartTime;
        synchronized (stats) {
          stats.addPutRecordsRequest(latencyMillis, requests > 1);
        }
        if (response != null && !hasErrored.get()) {
          if (!hasErrors(response)) {
            // Request succeeded, release one permit
            pendingRequests.release();
            LOG.debug(
                "Done writing {} records [{} ms, {} request(s)]",
                totalRecords,
                now - handlerStartTime,
                requests);
          } else {
            try {
              if (BackOffUtils.next(Sleeper.DEFAULT, backoff)) {
                LOG.info(summarizeErrors("Attempting retry", response));
                records = failedRecords(response);
                run();
              } else {
                throwable = new IOException(summarizeErrors("Exceeded retries", response));
              }
            } catch (Throwable e) {
              throwable = new IOException(summarizeErrors("Aborted retries", response), e);
            }
          }
        }
      } catch (Throwable e) {
        throwable = e;
      }
      if (throwable != null) {
        setAsyncFailure(throwable);
      }
    }

    private void setAsyncFailure(Throwable throwable) {
      LOG.warn("Error when writing to Kinesis.", throwable);
      hasErrored.set(true);
      asyncFailure.updateAndGet(
          ex -> {
            if (ex != null) {
              throwable.addSuppressed(ex);
            }
            return throwable;
          });
      pendingRequests.release(concurrentRequests); // unblock everything to fail fast
    }

    private boolean hasErrors(PutRecordsResponse response) {
      return response.records().stream().anyMatch(e -> e.errorCode() != null);
    }

    private List<PutRecordsRequestEntry> failedRecords(PutRecordsResponse response) {
      return Streams.zip(records.stream(), response.records().stream(), Pair::of)
          .filter(p -> p.getRight().errorCode() != null)
          .map(p -> p.getLeft())
          .collect(toList());
    }

    private String summarizeErrors(String prefix, PutRecordsResponse response) {
      Map<String, Long> countPerError =
          response.records().stream()
              .filter(e -> e.errorCode() != null)
              .map(e -> e.errorCode())
              .collect(groupingBy(identity(), counting()));
      return countPerError.entrySet().stream()
          .map(kv -> String.format("%s for %d record(s)", kv.getKey(), kv.getValue()))
          .collect(joining(", ", prefix + " after failure when writing to Kinesis: ", "."));
    }
  }
}
