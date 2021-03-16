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
package org.apache.beam.sdk.io.gcp.firestore;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.common.annotations.VisibleForTesting;
import com.google.rpc.Code;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.function.Function;
import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcAttempt.Context;
import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcWriteAttempt.Element;
import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcWriteAttempt.FlushBuffer;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.MovingFunction;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Ints;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class RpcQosImpl implements RpcQos {

  /**
   * Non-retryable errors. See https://cloud.google.com/apis/design/errors#handling_errors.
   */
  private static final Set<Integer> NON_RETRYABLE_ERROR_NUMBERS =
      ImmutableSet.of(
          Code.ALREADY_EXISTS,
          Code.DATA_LOSS,
          Code.FAILED_PRECONDITION,
          Code.INVALID_ARGUMENT,
          Code.OUT_OF_RANGE,
          Code.NOT_FOUND,
          Code.PERMISSION_DENIED,
          Code.UNIMPLEMENTED
      ).stream()
          .map(Code::getNumber)
          .collect(ImmutableSet.toImmutableSet());
  /**
   * The target minimum number of requests per samplePeriodMs, even if no requests succeed. Must be
   * greater than 0, else we could throttle to zero. Because every decision is probabilistic, there
   * is no guarantee that the request rate in any given interval will not be zero. (This is the +1
   * from the formula in https://landing.google.com/sre/book/chapters/handling-overload.html
   */
  private static final double MIN_REQUESTS = 1;

  private final RpcQosOptions options;

  private final AdaptiveThrottler at;
  private final WriteBatcher wb;
  private final WriteRampUp writeRampUp;
  private final FluentBackoff fb;

  private final WeakHashMap<Context, Counters> counters;
  private final Random random;
  private final Sleeper sleeper;
  private final Function<Context, Counters> computeCounters;

  RpcQosImpl(
      RpcQosOptions options,
      Random random,
      Sleeper sleeper,
      CounterFactory counterFactory
  ) {
    this.options = options;
    this.random = random;
    this.sleeper = sleeper;
    at = new AdaptiveThrottler();
    wb = new WriteBatcher();
    writeRampUp = new WriteRampUp(
        Math.max(1, 500 / options.getHintMaxNumWorkers())
    );
    fb = FluentBackoff.DEFAULT
        .withMaxRetries(options.getMaxAttempts() - 1) // maxRetries is an inclusive value, we want exclusive since we are tracking all attempts
        .withInitialBackoff(options.getInitialBackoff());
    counters = new WeakHashMap<>();
    computeCounters = (Context c) -> Counters.getCounters(counterFactory, c);
  }

  @Override
  public RpcWriteAttemptImpl newWriteAttempt(Context context) {
    return new RpcWriteAttemptImpl(
        context,
        counters.computeIfAbsent(context, computeCounters),
        fb.backoff(),
        sleeper);
  }

  @Override
  public RpcReadAttemptImpl newReadAttempt(Context context) {
    return new RpcReadAttemptImpl(
        context,
        counters.computeIfAbsent(context, computeCounters),
        fb.backoff(),
        sleeper);
  }

  @Override
  public boolean bytesOverLimit(long bytes) {
    return bytes > options.getBatchMaxBytes();
  }

  private static MovingFunction createMovingFunction(Duration samplePeriod, Duration sampleUpdate) {
    return new MovingFunction(
        samplePeriod.getMillis(),
        sampleUpdate.getMillis(),
        1 /* numSignificantBuckets */,
        1 /* numSignificantSamples */,
        Sum.ofLongs()
    );
  }

  interface CounterFactory extends Serializable {
    CounterFactory DEFAULT = Metrics::counter;

    Counter getCounter(String namespace, String name);
  }

  private enum AttemptState {
    Active,
    Active_Started,
    Complete_Success,
    Complete_Error;

    public void checkActive() {
      switch (this) {
        case Active:
        case Active_Started:
          return;
        case Complete_Success:
          throw new IllegalStateException("Expected state to be Active, but was Complete_Success");
        case Complete_Error:
          throw new IllegalStateException("Expected state to be Active, but was Complete_Error");
      }
    }

    public void checkStarted() {
      switch (this) {
        case Active_Started:
          return;
        case Active:
          throw new IllegalStateException("Expected state to be Active_Started, but was Active");
        case Complete_Success:
          throw new IllegalStateException("Expected state to be Active_Started, but was Complete_Success");
        case Complete_Error:
          throw new IllegalStateException("Expected state to be Active_Started, but was Complete_Error");
      }
    }
  }

  private abstract class BaseRpcAttempt implements RpcAttempt {
    private final Logger logger;
    protected final Counters counters;
    protected final BackOff backoff;
    protected final Sleeper sleeper;

    protected AttemptState state;
    protected Instant start;

    @SuppressWarnings("initialization.fields.uninitialized") // allow transient fields to be managed by component lifecycle
    protected BaseRpcAttempt(
        Context context, Counters counters, BackOff backoff, Sleeper sleeper) {
      this.logger = LoggerFactory.getLogger(String.format("%s.RpcQos", context.getNamespace()));
      this.counters = counters;
      this.backoff = backoff;
      this.sleeper = sleeper;
      this.state = AttemptState.Active;
    }

    @Override
    public boolean awaitSafeToProceed(Instant instant) throws InterruptedException {
      state.checkActive();
      Duration shouldThrottleRequest = at.shouldThrottleRequest(instant);
      if (shouldThrottleRequest.compareTo(Duration.ZERO) > 0) {
        logger.info("Delaying request by {}ms", shouldThrottleRequest.getMillis());
        throttleRequest(shouldThrottleRequest);
        return false;
      }

      return true;
    }

    @Override
    public void checkCanRetry(RuntimeException exception)
        throws InterruptedException {
      state.checkActive();

      Optional<ApiException> findApiException = findApiException(exception);

      if (findApiException.isPresent()) {
        ApiException apiException = findApiException.get();
        // order here is semi-important
        // First we always want to test if the error code is one of the codes we have deemed
        // non-retryable before delegating to the exceptions default set.
        if (
            maxAttemptsExhausted()
                || getStatusCodeNumber(apiException).map(NON_RETRYABLE_ERROR_NUMBERS::contains).orElse(false)
                || !apiException.isRetryable()
        ) {
          state = AttemptState.Complete_Error;
          throw apiException;
        }
      } else {
        state = AttemptState.Complete_Error;
        throw exception;
      }
    }

    @Override
    public void recordStartRequest(Instant instantSinceEpoch) {
      at.recordStartRequest(instantSinceEpoch);
      start = instantSinceEpoch;
      state = AttemptState.Active_Started;
    }

    @Override
    public void completeSuccess() {
      state.checkActive();
      state = AttemptState.Complete_Success;
    }

    @Override
    public boolean isCodeRetryable(Code code) {
      return !NON_RETRYABLE_ERROR_NUMBERS.contains(code.getNumber());
    }

    private boolean maxAttemptsExhausted() throws InterruptedException {
      try {
        boolean exhausted = !BackOffUtils.next(sleeper, backoff);
        if (exhausted) {
          logger.error("Max attempts exhausted after {} attempts.", options.getMaxAttempts());
        }
        return exhausted;
      } catch (IOException e) {
        // We are using FluentBackoff which does not ever throw an IOException from its methods
        // Catch and wrap any potential IOException as a RuntimeException since it won't ever
        // happen unless the implementation of FluentBackoff changes.
        throw new RuntimeException(e);
      }
    }

    protected Logger getLogger() {
      return logger;
    }

    protected final void throttleRequest(Duration shouldThrottleRequest) throws InterruptedException {
      counters.throttlingMs.inc(shouldThrottleRequest.getMillis());
      sleeper.sleep(shouldThrottleRequest.getMillis());
    }

    private Optional<Integer> getStatusCodeNumber(ApiException apiException) {
      StatusCode statusCode = apiException.getStatusCode();
      if (statusCode instanceof GrpcStatusCode) {
        GrpcStatusCode grpcStatusCode = (GrpcStatusCode) statusCode;
        return Optional.of(grpcStatusCode.getTransportCode().value());
      }
      return Optional.empty();
    }

    private Optional<ApiException> findApiException(Throwable throwable) {
      if (throwable instanceof ApiException) {
        ApiException apiException = (ApiException) throwable;
        return Optional.of(apiException);
      } else {
        Throwable cause = throwable.getCause();
        if (cause != null) {
          return findApiException(cause);
        } else {
          return Optional.empty();
        }
      }
    }
  }

  private final class RpcReadAttemptImpl extends BaseRpcAttempt implements RpcReadAttempt {
    private RpcReadAttemptImpl(Context context,
        Counters counters, BackOff backoff, Sleeper sleeper) {
      super(context, counters, backoff, sleeper);
    }

    @Override
    public void recordSuccessfulRequest(Instant end) {
      state.checkStarted();
      counters.rpcSuccesses.inc();
      at.recordSuccessfulRequest(start);
    }

    @Override
    public void recordFailedRequest(Instant end) {
      state.checkStarted();
      counters.rpcFailures.inc();
      at.recordFailedRequest(start);
    }

    @Override
    public void recordStreamValue(Instant now) {
      state.checkActive();
      counters.rpcStreamValueReceived.inc();
    }
  }

  final class RpcWriteAttemptImpl extends BaseRpcAttempt implements RpcWriteAttempt {

    private RpcWriteAttemptImpl(Context context,
        Counters counters, BackOff backoff, Sleeper sleeper) {
      super(context, counters, backoff, sleeper);
    }

    @Override
    public boolean awaitSafeToProceed(Instant instant) throws InterruptedException {
      state.checkActive();
      Optional<Duration> shouldThrottle = writeRampUp.shouldThrottle(instant);
      if (shouldThrottle.isPresent()) {
        Duration throttleDuration = shouldThrottle.get();
        getLogger().debug("Still ramping up, Delaying request by {}ms", throttleDuration.getMillis());
        throttleRequest(throttleDuration);
        return false;
      } else {
        return super.awaitSafeToProceed(instant);
      }
    }

    @Override
    public <T, E extends Element<T>> FlushBufferImpl<T, E> newFlushBuffer(Instant instantSinceEpoch) {
      state.checkActive();
      int availableWriteCountBudget = writeRampUp.getAvailableWriteCountBudget(instantSinceEpoch);
      int nextBatchMaxCount = wb.nextBatchMaxCount(instantSinceEpoch);
      int batchMaxCount = Ints.min(
          Math.max(0, availableWriteCountBudget),
          Math.max(0, nextBatchMaxCount),
          options.getBatchMaxCount()
      );
      return new FlushBufferImpl<>(
          batchMaxCount,
          options.getBatchMaxBytes()
      );
    }

    @Override
    public void recordSuccessfulRequest(Instant end, int numWrites) {
      state.checkStarted();
      counters.rpcSuccesses.inc();
      writeRampUp.recordWriteCount(start, numWrites);
      at.recordSuccessfulRequest(start);
      wb.recordRequestLatency(start, end, numWrites);
    }

    @Override
    public void recordFailedRequest(Instant end, int numWrites) {
      state.checkStarted();
      counters.rpcFailures.inc();
      writeRampUp.recordWriteCount(start, numWrites);
      at.recordFailedRequest(start);
      wb.recordRequestLatency(start, end, numWrites);
    }

  }

  /**
   * Determines batch sizes for commit RPCs based on past performance.
   *
   * <p>It aims for a target response time per RPC: it uses the response times for previous RPCs and
   * the number of entities contained in them, calculates a rolling average time-per-document, and
   * chooses the number of entities for future writes to hit the target time.
   *
   * <p>This enables us to send large batches without sending over-large requests in the case of
   * expensive document writes that may timeout before the server can apply them all.
   */
  private final class WriteBatcher {

    private final MovingAverage meanLatencyPerDocumentMs;

    private WriteBatcher() {
      this.meanLatencyPerDocumentMs =
          new MovingAverage(
              options.getSamplePeriod(),
              options.getSamplePeriodBucketSize()
              /* numSignificantBuckets */
              /* numSignificantSamples */
          );
    }

    private void recordRequestLatency(Instant start, Instant end, int numWrites) {
      Interval interval = new Interval(start, end);
      long msPerWrite = numWrites == 0 ? 0 : interval.toDurationMillis() / numWrites;
      meanLatencyPerDocumentMs.add(end, msPerWrite);
    }

    private int nextBatchMaxCount(Instant instantSinceEpoch) {
      if (!meanLatencyPerDocumentMs.hasValue(instantSinceEpoch)) {
        return options.getBatchInitialCount();
      }
      long recentMeanLatency = Math.max(meanLatencyPerDocumentMs.get(instantSinceEpoch), 1);
      long nextBatchMaxCount =  options.getBatchTargetLatency().getMillis() /  recentMeanLatency;
      return Math.toIntExact(nextBatchMaxCount);
    }

  }

  /**
   * An implementation of client-side adaptive throttling. See
   * https://sre.google/sre-book/handling-overload/#client-side-throttling-a7sYUg
   * for a full discussion of the use case and algorithm applied.
   */
  private final class AdaptiveThrottler {
    private final MovingFunction successfulRequestsMovingFunction;
    private final MovingFunction failedRequestsMovingFunction;
    private final MovingFunction allRequestsMovingFunction;

    private AdaptiveThrottler() {
      allRequestsMovingFunction = createMovingFunction(options.getSamplePeriod(), options.getSamplePeriodBucketSize());
      successfulRequestsMovingFunction = createMovingFunction(options.getSamplePeriod(), options.getSamplePeriodBucketSize());
      failedRequestsMovingFunction = createMovingFunction(options.getSamplePeriod(), options.getSamplePeriodBucketSize());
    }

    private Duration shouldThrottleRequest(Instant instantSinceEpoch) {
      double delayProbability = throttlingProbability(instantSinceEpoch);

      return (random.nextDouble() < delayProbability) ? options.getThrottleDuration() : Duration.ZERO;
    }

    private void recordStartRequest(Instant instantSinceEpoch) {
      allRequestsMovingFunction.add(instantSinceEpoch.getMillis(), 1);
    }

    private void recordSuccessfulRequest(Instant instantSinceEpoch) {
      successfulRequestsMovingFunction.add(instantSinceEpoch.getMillis(), 1);
    }

    private void recordFailedRequest(Instant instantSinceEpoch) {
      failedRequestsMovingFunction.add(instantSinceEpoch.getMillis(), 1);
    }

    /**
     * Implementation of the formula from https://sre.google/sre-book/handling-overload/#eq2101
     */
    private double throttlingProbability(Instant instantSinceEpoch) {
      if (!allRequestsMovingFunction.isSignificant()) {
        return 0;
      }
      long nowMsSinceEpoch = instantSinceEpoch.getMillis();
      long allRequestsCount = allRequestsMovingFunction.get(nowMsSinceEpoch);
      long successfulRequestsCount = successfulRequestsMovingFunction.get(nowMsSinceEpoch);

      double overloadMaxCount = options.getOverloadRatio() * successfulRequestsCount;
      double overloadUsage = allRequestsCount - overloadMaxCount;

      double calcProbability = overloadUsage / (allRequestsCount + MIN_REQUESTS);
      return Math.max(0, calcProbability);
    }
  }

  /**
   * An implementation providing the 500/50/5 ramp up strategy recommended by <a
   * href="https://cloud.google.com/firestore/docs/best-practices#ramping_up_traffic">Ramping up
   * traffic</a>
   */
  @VisibleForTesting
  static final class WriteRampUp {
    private static final Duration RAMP_UP_INTERVAL = Duration.standardMinutes(5);
    private final int baseMax;
    private final long rampUpIntervalMinutes;
    private final MovingFunction writeCounts;
    private final LinearBackoff backoff;

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private Optional<Instant> firstInstant = Optional.empty();

    WriteRampUp(int baseMax) {
      this.baseMax = baseMax;
      this.rampUpIntervalMinutes = RAMP_UP_INTERVAL.getStandardMinutes();
      this.writeCounts = createMovingFunction(
          // track up to one second of budget usage.
          //   this determines the full duration of time we want to keep track of request counts
          Duration.standardSeconds(1),
          // refill the budget each second
          //   this determines the sub-granularity the full duration will be broken into. So if we
          //   wanted budget to refill twice per second, this could be passed Duration.millis(500)
          Duration.standardSeconds(1)
      );
      this.backoff = new LinearBackoff(Duration.standardSeconds(1));
    }

    int getAvailableWriteCountBudget(Instant instant) {
      if (!firstInstant.isPresent()) {
        firstInstant = Optional.of(instant);
        return baseMax;
      }

      Instant first = firstInstant.get();
      int maxRequestBudget = calcMaxRequestBudget(instant, first);
      long writeCount = writeCounts.get(instant.getMillis());
      long availableBudget = maxRequestBudget - writeCount;
      return Math.toIntExact(availableBudget);
    }

    // 500 * 1.5^max(0, (x-5)/5)
    private int calcMaxRequestBudget(Instant instant, Instant first) {
      Duration durationSinceFirst = new Duration(first, instant);
      long calculatedGrowth = (durationSinceFirst.getStandardMinutes() - rampUpIntervalMinutes) / rampUpIntervalMinutes;
      long growth = Math.max(0, calculatedGrowth);
      double maxRequestCountBudget = baseMax * Math.pow(1.5, growth);
      return (int) maxRequestCountBudget;
    }

    void recordWriteCount(Instant instant, int numWrites) {
      writeCounts.add(instant.getMillis(), numWrites);
    }

    Optional<Duration> shouldThrottle(Instant instant) {
      int availableWriteCountBudget = getAvailableWriteCountBudget(instant);
      if (availableWriteCountBudget <= 0) {
        long nextBackOffMillis = backoff.nextBackOffMillis();
        if (nextBackOffMillis > BackOff.STOP) {
          return Optional.of(Duration.millis(nextBackOffMillis));
        } else {
          // we've exhausted our backoff, and have moved into the next time window try again
          backoff.reset();
          return Optional.empty();
        }
      } else {
        // budget is available reset backoff tracking
        backoff.reset();
        return Optional.empty();
      }
    }

    /**
     * For ramp up we're following a simplistic linear growth formula, when calculating backoff
     * we're calculating the next time to check a client side budget and we don't need the
     * randomness introduced by FluentBackoff. (Being linear also makes our test simulations easier
     * to model and verify)
     */
    private static class LinearBackoff implements BackOff {
      private static final long MAX_BACKOFF_MILLIS = 60_000;
      private final long startBackoffMillis;
      private long currentBackoffMillis;

      public LinearBackoff(Duration throttleDuration) {
        startBackoffMillis = throttleDuration.getMillis();
        currentBackoffMillis = startBackoffMillis;
      }

      @Override
      public void reset() {
        currentBackoffMillis = startBackoffMillis;
      }

      @Override
      public long nextBackOffMillis() {
        if (currentBackoffMillis > MAX_BACKOFF_MILLIS) {
          reset();
          return MAX_BACKOFF_MILLIS;
        } else {
          long retVal = currentBackoffMillis;
          currentBackoffMillis = (long) (currentBackoffMillis * 1.5);
          return retVal;
        }
      }
    }
  }

  private static class MovingAverage {
    private final MovingFunction sum;
    private final MovingFunction count;

    private MovingAverage(Duration samplePeriod, Duration sampleUpdate) {
      sum = createMovingFunction(samplePeriod, sampleUpdate);
      count = createMovingFunction(samplePeriod, sampleUpdate);
    }

    private void add(Instant instantSinceEpoch, long value) {
      sum.add(instantSinceEpoch.getMillis(), value);
      count.add(instantSinceEpoch.getMillis(), 1);
    }

    private long get(Instant instantSinceEpoch) {
      return sum.get(instantSinceEpoch.getMillis()) / count.get(instantSinceEpoch.getMillis());
    }

    private boolean hasValue(Instant instantSinceEpoch) {
      return sum.isSignificant() && count.isSignificant() && count.get(instantSinceEpoch.getMillis()) > 0;
    }
  }

  private static final class Counters {
    final Counter throttlingMs;
    final Counter rpcFailures;
    final Counter rpcSuccesses;
    final Counter rpcStreamValueReceived;

    private Counters(Counter throttlingMs, Counter rpcFailures,
        Counter rpcSuccesses, Counter rpcStreamValueReceived) {
      this.throttlingMs = throttlingMs;
      this.rpcFailures = rpcFailures;
      this.rpcSuccesses = rpcSuccesses;
      this.rpcStreamValueReceived = rpcStreamValueReceived;
    }

    private static Counters getCounters(CounterFactory factory, Context context) {
      return new Counters(
          factory.getCounter(context.getNamespace(), "throttlingMs"),
          factory.getCounter(context.getNamespace(), "rpcFailures"),
          factory.getCounter(context.getNamespace(), "rpcSuccesses"),
          factory.getCounter(context.getNamespace(), "rpcStreamValueReceived")
      );
    }
  }

  static class FlushBufferImpl<T, E extends Element<T>> implements FlushBuffer<E> {

    final int nextBatchMaxCount;
    final long nextBatchMaxBytes;
    final ImmutableList.Builder<E> elements;

    int offersAcceptedCount = 0;
    long offersAcceptedBytes = 0;

    public FlushBufferImpl(int nextBatchMaxCount, long nextBatchMaxBytes) {
      this.nextBatchMaxCount = nextBatchMaxCount;
      this.nextBatchMaxBytes = nextBatchMaxBytes;
      this.elements = ImmutableList.builder();
    }

    @Override
    public boolean offer(E newElement) {
      if (offersAcceptedCount < nextBatchMaxCount) {
        long newBytesTotal = offersAcceptedBytes + newElement.getSerializedSize();
        if (newBytesTotal <= nextBatchMaxBytes) {
          elements.add(newElement);
          offersAcceptedCount++;
          offersAcceptedBytes = newBytesTotal;
          return true;
        } else {
          return false;
        }
      } else {
        return false;
      }
    }

    @Override
    public Iterator<E> iterator() {
      return elements.build().iterator();
    }

    @Override
    public int getBufferedElementsCount() {
      return offersAcceptedCount;
    }

    @Override
    public long getBufferedElementsBytes() {
      return offersAcceptedBytes;
    }

    @Override
    public boolean isFull() {
      return (offersAcceptedCount == nextBatchMaxCount || offersAcceptedBytes >= nextBatchMaxBytes);
    }
  }

}
