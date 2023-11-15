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
package org.apache.beam.sdk.io.aws2.common;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Predicates.notNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.joda.time.DateTimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Async handler that automatically retries unprocessed records in case of a partial success.
 *
 * <p>The handler enforces the provided upper limit of concurrent requests. Once that limit is
 * reached any further call to {@link #batchWrite(String, List)} will block until another request
 * completed.
 *
 * <p>The handler is fail fast and won't submit any further request after a failure. Async failures
 * can be polled using {@link #checkForAsyncFailure()}.
 *
 * @param <RecT> Record type in batch
 * @param <ResT> Potentially erroneous result that needs to be correlated to a record using {@link
 *     #failedRecords(List, List)}
 */
@NotThreadSafe
@Internal
public abstract class AsyncBatchWriteHandler<RecT, ResT> {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncBatchWriteHandler.class);
  private final FluentBackoff backoff;
  private final int concurrentRequests;
  private final Stats stats;
  protected final BiFunction<String, List<RecT>, CompletableFuture<List<ResT>>> submitFn;
  protected final Function<ResT, String> errorCodeFn;

  private AtomicBoolean hasErrored;
  private AtomicReference<Throwable> asyncFailure;
  private Semaphore requestPermits;

  protected AsyncBatchWriteHandler(
      int concurrency,
      FluentBackoff backoff,
      Stats stats,
      Function<ResT, String> errorCodeFn,
      BiFunction<String, List<RecT>, CompletableFuture<List<ResT>>> submitFn) {
    this.backoff = backoff;
    this.concurrentRequests = concurrency;
    this.errorCodeFn = errorCodeFn;
    this.submitFn = submitFn;
    this.hasErrored = new AtomicBoolean(false);
    this.asyncFailure = new AtomicReference<>();
    this.requestPermits = new Semaphore(concurrentRequests);
    this.stats = stats;
  }

  public final int requestsInProgress() {
    return concurrentRequests - requestPermits.availablePermits();
  }

  public final void reset() {
    hasErrored = new AtomicBoolean(false);
    asyncFailure = new AtomicReference<>();
    requestPermits = new Semaphore(concurrentRequests);
  }

  /** If this handler has errored since it was last reset. */
  public final boolean hasErrored() {
    return hasErrored.get();
  }

  /**
   * Check if any failure happened async.
   *
   * @throws Throwable The last async failure, afterwards reset it.
   */
  public final void checkForAsyncFailure() throws Throwable {
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
  public final void waitForCompletion() throws Throwable {
    requestPermits.acquireUninterruptibly(concurrentRequests);
    checkForAsyncFailure();
  }

  /**
   * Asynchronously trigger a batch write request (unless already in error state).
   *
   * <p>This will respect the concurrency limit of the handler and first wait for a permit.
   *
   * @throws Throwable The last async failure if present using {@link #checkForAsyncFailure()}
   */
  public final void batchWrite(String destination, List<RecT> records) throws Throwable {
    batchWrite(destination, records, true);
  }

  /**
   * Asynchronously trigger a batch write request (unless already in error state).
   *
   * <p>This will respect the concurrency limit of the handler and first wait for a permit.
   *
   * @param throwAsyncFailures If to check and throw pending async failures
   * @throws Throwable The last async failure if present using {@link #checkForAsyncFailure()}
   */
  public final void batchWrite(String destination, List<RecT> records, boolean throwAsyncFailures)
      throws Throwable {
    if (!hasErrored()) {
      requestPermits.acquireUninterruptibly();
      new RetryHandler(destination, records).run();
    }
    if (throwAsyncFailures) {
      checkForAsyncFailure();
    }
  }

  protected abstract List<RecT> failedRecords(List<RecT> records, List<ResT> results);

  protected abstract boolean hasFailedRecords(List<ResT> results);

  /** Statistics on the batch request. */
  public interface Stats {
    Stats NONE = new Stats() {};

    default void addBatchWriteRequest(long latencyMillis, boolean isPartialRetry) {}
  }

  /**
   * AsyncBatchWriteHandler that correlates records and results by position in the respective list.
   */
  public static <RecT, ResT> AsyncBatchWriteHandler<RecT, ResT> byPosition(
      int concurrency,
      int partialRetries,
      @Nullable RetryConfiguration retry,
      Stats stats,
      BiFunction<String, List<RecT>, CompletableFuture<List<ResT>>> submitFn,
      Function<ResT, String> errorCodeFn) {
    FluentBackoff backoff = retryBackoff(partialRetries, retry);
    return byPosition(concurrency, backoff, stats, submitFn, errorCodeFn);
  }

  /**
   * AsyncBatchWriteHandler that correlates records and results by position in the respective list.
   */
  public static <RecT, ResT> AsyncBatchWriteHandler<RecT, ResT> byPosition(
      int concurrency,
      FluentBackoff backoff,
      Stats stats,
      BiFunction<String, List<RecT>, CompletableFuture<List<ResT>>> submitFn,
      Function<ResT, String> errorCodeFn) {
    return new AsyncBatchWriteHandler<RecT, ResT>(
        concurrency, backoff, stats, errorCodeFn, submitFn) {

      @Override
      protected boolean hasFailedRecords(List<ResT> results) {
        for (int i = 0; i < results.size(); i++) {
          if (errorCodeFn.apply(results.get(i)) != null) {
            return true;
          }
        }
        return false;
      }

      @Override
      protected List<RecT> failedRecords(List<RecT> records, List<ResT> results) {
        int size = Math.min(records.size(), results.size());
        List<RecT> filtered = new ArrayList<>();
        for (int i = 0; i < size; i++) {
          if (errorCodeFn.apply(results.get(i)) != null) {
            filtered.add(records.get(i));
          }
        }
        return filtered;
      }
    };
  }

  /**
   * AsyncBatchWriteHandler that correlates records and results by id, all results are erroneous.
   */
  public static <RecT, ErrT> AsyncBatchWriteHandler<RecT, ErrT> byId(
      int concurrency,
      int partialRetries,
      @Nullable RetryConfiguration retry,
      Stats stats,
      BiFunction<String, List<RecT>, CompletableFuture<List<ErrT>>> submitFn,
      Function<ErrT, String> errorCodeFn,
      Function<RecT, String> recordIdFn,
      Function<ErrT, String> errorIdFn) {
    FluentBackoff backoff = retryBackoff(partialRetries, retry);
    return byId(concurrency, backoff, stats, submitFn, errorCodeFn, recordIdFn, errorIdFn);
  }

  /**
   * AsyncBatchWriteHandler that correlates records and results by id, all results are erroneous.
   */
  public static <RecT, ErrT> AsyncBatchWriteHandler<RecT, ErrT> byId(
      int concurrency,
      FluentBackoff backoff,
      Stats stats,
      BiFunction<String, List<RecT>, CompletableFuture<List<ErrT>>> submitFn,
      Function<ErrT, String> errorCodeFn,
      Function<RecT, String> recordIdFn,
      Function<ErrT, String> errorIdFn) {
    return new AsyncBatchWriteHandler<RecT, ErrT>(
        concurrency, backoff, stats, errorCodeFn, submitFn) {
      @Override
      protected boolean hasFailedRecords(List<ErrT> errors) {
        return !errors.isEmpty();
      }

      @Override
      protected List<RecT> failedRecords(List<RecT> records, List<ErrT> errors) {
        Set<String> ids = Sets.newHashSetWithExpectedSize(errors.size());
        errors.forEach(e -> ids.add(errorIdFn.apply(e)));

        List<RecT> filtered = new ArrayList<>(errors.size());
        for (int i = 0; i < records.size(); i++) {
          RecT rec = records.get(i);
          if (ids.contains(recordIdFn.apply(rec))) {
            filtered.add(rec);
            if (filtered.size() == errors.size()) {
              return filtered;
            }
          }
        }
        return filtered;
      }
    };
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
   * The next call of {@link #checkForAsyncFailure()}, {@link #batchWrite(String, List< RecT >)}} or
   * {@link #waitForCompletion()} will check for the last async failure and throw it. Afterwards the
   * failure state is reset.
   */
  private class RetryHandler implements BiConsumer<List<ResT>, Throwable> {
    private final String destination;
    private final int totalRecords;
    private final BackOff backoff; // backoff in case of throttling

    private final long handlerStartTime;
    private long requestStartTime;
    private int requests;

    private List<RecT> records;

    RetryHandler(String destination, List<RecT> records) {
      this.destination = destination;
      this.totalRecords = records.size();
      this.records = records;
      this.backoff = AsyncBatchWriteHandler.this.backoff.backoff();
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
          submitFn.apply(destination, records).whenComplete(this);
        } catch (Throwable e) {
          setAsyncFailure(e);
        }
      }
    }

    @Override
    public void accept(List<ResT> results, Throwable throwable) {
      try {
        long now = DateTimeUtils.currentTimeMillis();
        long latencyMillis = now - requestStartTime;
        synchronized (stats) {
          stats.addBatchWriteRequest(latencyMillis, requests > 1);
        }
        if (results != null && !hasErrored.get()) {
          if (!hasFailedRecords(results)) {
            // Request succeeded, release one permit
            requestPermits.release();
            LOG.debug(
                "Done writing {} records [{} ms, {} request(s)]",
                totalRecords,
                now - handlerStartTime,
                requests);
          } else {
            try {
              if (BackOffUtils.next(Sleeper.DEFAULT, backoff)) {
                LOG.info(summarizeErrors("Attempting retry", results));
                records = failedRecords(records, results);
                run();
              } else {
                throwable = new IOException(summarizeErrors("Exceeded retries", results));
              }
            } catch (Throwable e) {
              throwable = new IOException(summarizeErrors("Aborted retries", results), e);
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
      LOG.warn("Error when writing batch.", throwable);
      hasErrored.set(true);
      asyncFailure.updateAndGet(
          ex -> {
            if (ex != null) {
              throwable.addSuppressed(ex);
            }
            return throwable;
          });
      requestPermits.release(concurrentRequests); // unblock everything to fail fast
    }

    private String summarizeErrors(String prefix, List<ResT> results) {
      Map<String, Long> countsPerError =
          results.stream()
              .map(errorCodeFn)
              .filter(notNull())
              .collect(groupingBy(identity(), counting()));
      return countsPerError.entrySet().stream()
          .map(kv -> String.format("code %s for %d record(s)", kv.getKey(), kv.getValue()))
          .collect(joining(", ", prefix + " after partial failure: ", "."));
    }
  }

  private static FluentBackoff retryBackoff(int retries, @Nullable RetryConfiguration retry) {
    FluentBackoff backoff = FluentBackoff.DEFAULT.withMaxRetries(retries);
    if (retry != null) {
      if (retry.throttledBaseBackoff() != null) {
        backoff = backoff.withInitialBackoff(retry.throttledBaseBackoff());
      }
      if (retry.maxBackoff() != null) {
        backoff = backoff.withMaxBackoff(retry.maxBackoff());
      }
    }
    return backoff;
  }
}
