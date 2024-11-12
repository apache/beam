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
import com.google.rpc.Code;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.function.Function;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1RpcAttemptContexts.V1FnRpcAttemptContext;
import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcAttempt.Context;
import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcWriteAttempt.Element;
import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcWriteAttempt.FlushBuffer;
import org.apache.beam.sdk.io.gcp.firestore.RpcQosImpl.StatusCodeAwareBackoff.BackoffDuration;
import org.apache.beam.sdk.io.gcp.firestore.RpcQosImpl.StatusCodeAwareBackoff.BackoffResult;
import org.apache.beam.sdk.io.gcp.firestore.RpcQosImpl.StatusCodeAwareBackoff.BackoffResults;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.MovingFunction;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Ints;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class RpcQosImpl implements RpcQos {

  /** Non-retryable errors. See https://cloud.google.com/apis/design/errors#handling_errors. */
  private static final Set<Integer> NON_RETRYABLE_ERROR_NUMBERS =
      ImmutableSet.of(
              Code.ALREADY_EXISTS,
              Code.DATA_LOSS,
              Code.FAILED_PRECONDITION,
              Code.INVALID_ARGUMENT,
              Code.OUT_OF_RANGE,
              Code.NOT_FOUND,
              Code.PERMISSION_DENIED,
              Code.UNIMPLEMENTED)
          .stream()
          .map(Code::getNumber)
          .collect(ImmutableSet.toImmutableSet());
  /**
   * The target minimum number of requests per samplePeriodMs, even if no requests succeed. Must be
   * greater than 0, else we could throttle to zero. Because every decision is probabilistic, there
   * is no guarantee that the request rate in any given interval will not be zero. (This is the +1
   * from the formula in https://landing.google.com/sre/book/chapters/handling-overload.html)
   */
  private static final double MIN_REQUESTS = 1;

  private final RpcQosOptions options;

  private final AdaptiveThrottler at;
  private final WriteBatcher wb;
  private final WriteRampUp writeRampUp;

  private final WeakHashMap<Context, O11y> counters;
  private final Random random;
  private final Sleeper sleeper;
  private final Function<Context, O11y> computeCounters;
  private final DistributionFactory distributionFactory;

  RpcQosImpl(
      RpcQosOptions options,
      Random random,
      Sleeper sleeper,
      CounterFactory counterFactory,
      DistributionFactory distributionFactory) {
    this.options = options;
    this.random = random;
    this.sleeper = sleeper;
    DistributionFactory filteringDistributionFactory =
        new DiagnosticOnlyFilteringDistributionFactory(
            !options.isShouldReportDiagnosticMetrics(), distributionFactory);
    this.distributionFactory = filteringDistributionFactory;
    at =
        new AdaptiveThrottler(
            options.getSamplePeriod(),
            options.getSamplePeriodBucketSize(),
            options.getThrottleDuration(),
            options.getOverloadRatio());
    wb =
        new WriteBatcher(
            options.getSamplePeriod(),
            options.getSamplePeriodBucketSize(),
            options.getBatchInitialCount(),
            options.getBatchTargetLatency(),
            filteringDistributionFactory);
    writeRampUp =
        new WriteRampUp(500.0 / options.getHintMaxNumWorkers(), filteringDistributionFactory);
    counters = new WeakHashMap<>();
    computeCounters = (Context c) -> O11y.create(c, counterFactory, filteringDistributionFactory);
  }

  @Override
  public RpcWriteAttemptImpl newWriteAttempt(Context context) {
    return new RpcWriteAttemptImpl(
        context,
        counters.computeIfAbsent(context, computeCounters),
        new StatusCodeAwareBackoff(
            random,
            options.getMaxAttempts(),
            options.getThrottleDuration(),
            Collections.emptySet()),
        sleeper);
  }

  @Override
  public RpcReadAttemptImpl newReadAttempt(Context context) {
    Set<Integer> graceStatusCodeNumbers = Collections.emptySet();
    // When reading results from a RunQuery or BatchGet the stream returning the results has a
    //   maximum lifetime of 60 seconds at which point it will be broken with an UNAVAILABLE
    //   status code. Since this is expected for semi-large query result set sizes we specify
    //   it as a grace value for backoff evaluation.
    if (V1FnRpcAttemptContext.RunQuery.equals(context)
        || V1FnRpcAttemptContext.BatchGetDocuments.equals(context)) {
      graceStatusCodeNumbers = ImmutableSet.of(Code.UNAVAILABLE_VALUE);
    }
    return new RpcReadAttemptImpl(
        context,
        counters.computeIfAbsent(context, computeCounters),
        new StatusCodeAwareBackoff(
            random,
            options.getMaxAttempts(),
            options.getThrottleDuration(),
            graceStatusCodeNumbers),
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
        Sum.ofLongs());
  }

  private enum AttemptState {
    PENDING,
    STARTED,
    COMPLETE_SUCCESS,
    COMPLETE_ERROR;

    public void checkActive() {
      switch (this) {
        case PENDING:
        case STARTED:
          return;
        case COMPLETE_SUCCESS:
          throw new IllegalStateException(
              "Expected state to be PENDING or STARTED, but was COMPLETE_SUCCESS");
        case COMPLETE_ERROR:
          throw new IllegalStateException(
              "Expected state to be PENDING or STARTED, but was COMPLETE_ERROR");
      }
    }

    public void checkStarted() {
      switch (this) {
        case STARTED:
          return;
        case PENDING:
          throw new IllegalStateException("Expected state to be STARTED, but was PENDING");
        case COMPLETE_SUCCESS:
          throw new IllegalStateException("Expected state to be STARTED, but was COMPLETE_SUCCESS");
        case COMPLETE_ERROR:
          throw new IllegalStateException("Expected state to be STARTED, but was COMPLETE_ERROR");
      }
    }
  }

  private abstract class BaseRpcAttempt implements RpcAttempt {
    private final Logger logger;
    final O11y o11y;
    final StatusCodeAwareBackoff backoff;
    final Sleeper sleeper;

    AttemptState state;
    Instant start;

    @SuppressWarnings(
        "initialization.fields.uninitialized") // allow transient fields to be managed by component
    // lifecycle
    BaseRpcAttempt(Context context, O11y o11y, StatusCodeAwareBackoff backoff, Sleeper sleeper) {
      this.logger = LoggerFactory.getLogger(String.format("%s.RpcQos", context.getNamespace()));
      this.o11y = o11y;
      this.backoff = backoff;
      this.sleeper = sleeper;
      this.state = AttemptState.PENDING;
    }

    @Override
    public boolean awaitSafeToProceed(Instant instant) throws InterruptedException {
      state.checkActive();
      Duration shouldThrottleRequest = at.shouldThrottleRequest(instant);
      if (shouldThrottleRequest.compareTo(Duration.ZERO) > 0) {
        long throttleRequestMillis = shouldThrottleRequest.getMillis();
        logger.debug("Delaying request by {}ms", throttleRequestMillis);
        throttleRequest(shouldThrottleRequest);
        return false;
      }

      return true;
    }

    @Override
    public void checkCanRetry(Instant instant, RuntimeException exception)
        throws InterruptedException {
      state.checkActive();

      Optional<ApiException> findApiException = findApiException(exception);

      if (findApiException.isPresent()) {
        ApiException apiException = findApiException.get();
        // order here is semi-important
        // First we always want to test if the error code is one of the codes we have deemed
        // non-retryable before delegating to the exceptions default set.
        Optional<Integer> statusCodeNumber = getStatusCodeNumber(apiException);
        if (maxAttemptsExhausted(instant, statusCodeNumber.orElse(Code.UNKNOWN_VALUE))
            || statusCodeNumber.map(NON_RETRYABLE_ERROR_NUMBERS::contains).orElse(false)
            || !apiException.isRetryable()) {
          state = AttemptState.COMPLETE_ERROR;
          throw apiException;
        }
      } else {
        state = AttemptState.COMPLETE_ERROR;
        throw exception;
      }
    }

    @Override
    public void completeSuccess() {
      state.checkActive();
      state = AttemptState.COMPLETE_SUCCESS;
    }

    @Override
    public boolean isCodeRetryable(Code code) {
      return !NON_RETRYABLE_ERROR_NUMBERS.contains(code.getNumber());
    }

    @Override
    public void recordRequestSuccessful(Instant end) {
      state.checkStarted();
      o11y.rpcSuccesses.inc();
      o11y.rpcDurationMs.update(durationMs(end));
      at.recordRequestSuccessful(start);
    }

    @Override
    public void recordRequestFailed(Instant end) {
      state.checkStarted();
      o11y.rpcFailures.inc();
      o11y.rpcDurationMs.update(durationMs(end));
      at.recordRequestFailed(start);
    }

    private boolean maxAttemptsExhausted(Instant now, int statusCodeNumber)
        throws InterruptedException {
      BackoffResult backoffResult = backoff.nextBackoff(now, statusCodeNumber);
      if (BackoffResults.EXHAUSTED.equals(backoffResult)) {
        logger.error("Max attempts exhausted after {} attempts.", options.getMaxAttempts());
        return true;
      } else if (backoffResult instanceof BackoffDuration) {
        BackoffDuration result = (BackoffDuration) backoffResult;
        sleeper.sleep(result.getDuration().getMillis());
        return false;
      } else {
        return false;
      }
    }

    Logger getLogger() {
      return logger;
    }

    final void throttleRequest(Duration shouldThrottleRequest) throws InterruptedException {
      o11y.throttlingMs.inc(shouldThrottleRequest.getMillis());
      sleeper.sleep(shouldThrottleRequest.getMillis());
    }

    final long durationMs(Instant end) {
      return end.minus(Duration.millis(start.getMillis())).getMillis();
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
    private RpcReadAttemptImpl(
        Context context, O11y o11y, StatusCodeAwareBackoff backoff, Sleeper sleeper) {
      super(context, o11y, backoff, sleeper);
    }

    @Override
    public void recordRequestStart(Instant start) {
      at.recordRequestStart(start);
      this.start = start;
      state = AttemptState.STARTED;
    }

    @Override
    public void recordStreamValue(Instant now) {
      state.checkActive();
      o11y.rpcStreamValueReceived.inc();
    }
  }

  final class RpcWriteAttemptImpl extends BaseRpcAttempt implements RpcWriteAttempt {

    private RpcWriteAttemptImpl(
        Context context, O11y o11y, StatusCodeAwareBackoff backoff, Sleeper sleeper) {
      super(context, o11y, backoff, sleeper);
    }

    @Override
    public boolean awaitSafeToProceed(Instant instant) throws InterruptedException {
      state.checkActive();
      Optional<Duration> shouldThrottle = writeRampUp.shouldThrottle(instant);
      if (shouldThrottle.isPresent()) {
        Duration throttleDuration = shouldThrottle.get();
        long throttleDurationMillis = throttleDuration.getMillis();
        getLogger().debug("Still ramping up, Delaying request by {}ms", throttleDurationMillis);
        throttleRequest(throttleDuration);
        return false;
      } else {
        return super.awaitSafeToProceed(instant);
      }
    }

    @Override
    public <ElementT extends Element<?>> FlushBufferImpl<ElementT> newFlushBuffer(
        Instant instantSinceEpoch) {
      state.checkActive();
      int availableWriteCountBudget = writeRampUp.getAvailableWriteCountBudget(instantSinceEpoch);
      int nextBatchMaxCount = wb.nextBatchMaxCount(instantSinceEpoch);
      int batchMaxCount =
          Ints.min(
              Math.max(0, availableWriteCountBudget),
              Math.max(0, nextBatchMaxCount),
              options.getBatchMaxCount());
      o11y.batchCapacityCount.update(batchMaxCount);
      return new FlushBufferImpl<>(batchMaxCount, options.getBatchMaxBytes());
    }

    @Override
    public void recordRequestStart(Instant start, int numWrites) {
      at.recordRequestStart(start, numWrites);
      writeRampUp.recordWriteCount(start, numWrites);
      this.start = start;
      state = AttemptState.STARTED;
    }

    @Override
    public void recordWriteCounts(Instant end, int successfulWrites, int failedWrites) {
      int totalWrites = successfulWrites + failedWrites;
      state.checkStarted();
      wb.recordRequestLatency(start, end, totalWrites, o11y.latencyPerDocumentMs);
      if (successfulWrites > 0) {
        at.recordRequestSuccessful(start, successfulWrites);
      }
      if (failedWrites > 0) {
        at.recordRequestFailed(start, failedWrites);
      }
    }
  }

  /**
   * Determines batch sizes based on past performance.
   *
   * <p>It aims for a target response time per RPC: it uses the response times for previous RPCs and
   * the number of documents contained in them, calculates a rolling average time-per-document, and
   * chooses the number of documents for future writes to hit the target time.
   *
   * <p>This enables us to send large batches without sending overly-large requests in the case of
   * expensive document writes that may timeout before the server can apply them all.
   */
  private static final class WriteBatcher {
    private static final Logger LOG = LoggerFactory.getLogger(WriteBatcher.class);

    private final int batchInitialCount;
    private final Duration batchTargetLatency;
    private final MovingAverage meanLatencyPerDocumentMs;
    private final Distribution batchMaxCount;

    private WriteBatcher(
        Duration samplePeriod,
        Duration samplePeriodBucketSize,
        int batchInitialCount,
        Duration batchTargetLatency,
        DistributionFactory distributionFactory) {
      this.batchInitialCount = batchInitialCount;
      this.batchTargetLatency = batchTargetLatency;
      this.meanLatencyPerDocumentMs = new MovingAverage(samplePeriod, samplePeriodBucketSize);
      this.batchMaxCount =
          distributionFactory.get(RpcQos.class.getName(), "qos_writeBatcher_batchMaxCount");
    }

    private void recordRequestLatency(
        Instant start, Instant end, int numWrites, Distribution distribution) {
      try {
        Interval interval = new Interval(start, end);
        long msPerWrite = numWrites == 0 ? 0 : interval.toDurationMillis() / numWrites;
        distribution.update(msPerWrite);
        meanLatencyPerDocumentMs.add(end, msPerWrite);
      } catch (IllegalArgumentException e) {
        LOG.warn("Invalid time interval start = {} end = {}", start, end, e);
      }
    }

    private int nextBatchMaxCount(Instant instantSinceEpoch) {
      if (!meanLatencyPerDocumentMs.hasValue(instantSinceEpoch)) {
        return batchInitialCount;
      }
      long recentMeanLatency = Math.max(meanLatencyPerDocumentMs.get(instantSinceEpoch), 1);
      long nextBatchMaxCount = batchTargetLatency.getMillis() / recentMeanLatency;
      int count = Math.toIntExact(nextBatchMaxCount);
      batchMaxCount.update(count);
      return count;
    }
  }

  /**
   * An implementation of client-side adaptive throttling. See
   * https://sre.google/sre-book/handling-overload/#client-side-throttling-a7sYUg for a full
   * discussion of the use case and algorithm applied.
   */
  private final class AdaptiveThrottler {
    private final MovingFunction successfulRequestsMovingFunction;
    private final MovingFunction failedRequestsMovingFunction;
    private final MovingFunction allRequestsMovingFunction;
    private final Distribution allRequestsCountDist;
    private final Distribution successfulRequestsCountDist;
    private final Distribution overloadMaxCountDist;
    private final Distribution overloadUsageDist;
    private final Distribution throttleProbabilityDist;
    private final Distribution throttlingMs;
    private final LinearBackoff backoff;
    private final double overloadRatio;

    private AdaptiveThrottler(
        Duration samplePeriod,
        Duration samplePeriodBucketSize,
        Duration throttleDuration,
        double overloadRatio) {
      allRequestsMovingFunction = createMovingFunction(samplePeriod, samplePeriodBucketSize);
      successfulRequestsMovingFunction = createMovingFunction(samplePeriod, samplePeriodBucketSize);
      failedRequestsMovingFunction = createMovingFunction(samplePeriod, samplePeriodBucketSize);
      allRequestsCountDist =
          distributionFactory.get(RpcQos.class.getName(), "qos_adaptiveThrottler_allRequestsCount");
      successfulRequestsCountDist =
          distributionFactory.get(
              RpcQos.class.getName(), "qos_adaptiveThrottler_successfulRequestsCount");
      overloadMaxCountDist =
          distributionFactory.get(RpcQos.class.getName(), "qos_adaptiveThrottler_overloadMaxCount");
      overloadUsageDist =
          distributionFactory.get(RpcQos.class.getName(), "qos_adaptiveThrottler_overloadUsagePct");
      throttleProbabilityDist =
          distributionFactory.get(
              RpcQos.class.getName(), "qos_adaptiveThrottler_throttleProbabilityPct");
      throttlingMs =
          distributionFactory.get(RpcQos.class.getName(), "qos_adaptiveThrottler_throttlingMs");
      backoff = new LinearBackoff(throttleDuration);
      this.overloadRatio = overloadRatio;
    }

    private Duration shouldThrottleRequest(Instant instantSinceEpoch) {
      double delayProbability = throttlingProbability(instantSinceEpoch);

      if (random.nextDouble() < delayProbability) {
        long millis = backoff.nextBackOffMillis();
        throttlingMs.update(millis);
        return Duration.millis(millis);
      } else {
        backoff.reset();
        return Duration.ZERO;
      }
    }

    private void recordRequestStart(Instant instantSinceEpoch) {
      recordRequestStart(instantSinceEpoch, 1);
    }

    private void recordRequestStart(Instant instantSinceEpoch, int value) {
      allRequestsMovingFunction.add(instantSinceEpoch.getMillis(), value);
    }

    private void recordRequestSuccessful(Instant instantSinceEpoch) {
      recordRequestSuccessful(instantSinceEpoch, 1);
    }

    private void recordRequestSuccessful(Instant instantSinceEpoch, int value) {
      successfulRequestsMovingFunction.add(instantSinceEpoch.getMillis(), value);
    }

    private void recordRequestFailed(Instant instantSinceEpoch) {
      recordRequestFailed(instantSinceEpoch, 1);
    }

    private void recordRequestFailed(Instant instantSinceEpoch, int value) {
      failedRequestsMovingFunction.add(instantSinceEpoch.getMillis(), value);
    }

    /**
     * Implementation of the formula from <a target="_blank" rel="noopener noreferrer"
     * href="https://sre.google/sre-book/handling-overload/#eq2101">Handling Overload from SRE
     * Book</a>.
     */
    private double throttlingProbability(Instant instantSinceEpoch) {
      if (!allRequestsMovingFunction.isSignificant()) {
        return 0;
      }
      long nowMsSinceEpoch = instantSinceEpoch.getMillis();
      long allRequestsCount = allRequestsMovingFunction.get(nowMsSinceEpoch);
      long successfulRequestsCount = successfulRequestsMovingFunction.get(nowMsSinceEpoch);

      double overloadMaxCount = overloadRatio * successfulRequestsCount;
      double overloadUsage = allRequestsCount - overloadMaxCount;

      double calcProbability = overloadUsage / (allRequestsCount + MIN_REQUESTS);
      allRequestsCountDist.update(allRequestsCount);
      successfulRequestsCountDist.update(successfulRequestsCount);
      overloadMaxCountDist.update((long) overloadMaxCount);
      overloadUsageDist.update((long) (overloadUsage * 100));
      throttleProbabilityDist.update((long) (calcProbability * 100));
      return Math.max(0, calcProbability);
    }
  }

  /**
   * An implementation providing the 500/50/5 ramp up strategy recommended by <a target="_blank"
   * rel="noopener noreferrer"
   * href="https://cloud.google.com/firestore/docs/best-practices#ramping_up_traffic">Ramping up
   * traffic</a>.
   */
  @VisibleForTesting
  static final class WriteRampUp {

    private static final Duration RAMP_UP_INTERVAL = Duration.standardMinutes(5);
    private final double baseBatchBudget;
    private final long rampUpIntervalMinutes;
    private final MovingFunction writeCounts;
    private final LinearBackoff backoff;
    private final Distribution throttlingMs;
    private final Distribution availableWriteCountBudget;

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private Optional<Instant> firstInstant = Optional.empty();

    WriteRampUp(double baseBatchBudget, DistributionFactory distributionFactory) {
      this.baseBatchBudget = baseBatchBudget;
      this.rampUpIntervalMinutes = RAMP_UP_INTERVAL.getStandardMinutes();
      this.writeCounts =
          createMovingFunction(
              // track up to one second of budget usage.
              //   this determines the full duration of time we want to keep track of request counts
              Duration.standardSeconds(1),
              // refill the budget each second
              //   this determines the sub-granularity the full duration will be broken into. So if
              //   we wanted budget to refill twice per second, this could be passed
              //   Duration.millis(500)
              Duration.standardSeconds(1));
      this.backoff = new LinearBackoff(Duration.standardSeconds(1));
      this.throttlingMs =
          distributionFactory.get(RpcQos.class.getName(), "qos_rampUp_throttlingMs");
      this.availableWriteCountBudget =
          distributionFactory.get(RpcQos.class.getName(), "qos_rampUp_availableWriteCountBudget");
    }

    int getAvailableWriteCountBudget(Instant instant) {
      if (!firstInstant.isPresent()) {
        firstInstant = Optional.of(instant);
        return (int) Math.max(1, baseBatchBudget);
      }

      Instant first = firstInstant.get();
      double maxRequestBudget = calcMaxRequestBudget(instant, first);
      long writeCount = writeCounts.get(instant.getMillis());
      double availableBudget = maxRequestBudget - writeCount;
      int budget = Ints.saturatedCast((long) availableBudget);
      availableWriteCountBudget.update(budget);
      return budget;
    }

    /**
     * Calculate the value relative to the growth line relative to the {@code first}.
     *
     * <p>For a {@code baseBatchBudget} of 500 the line would look like <a target="_blank"
     * rel="noopener noreferrer"
     * href="https://www.wolframalpha.com/input/?i=500+*+1.5%5Emax%280%2C+%28x-5%29%2F5%29+from+0+to+30">this</a>
     */
    private double calcMaxRequestBudget(Instant instant, Instant first) {
      Duration durationSinceFirst = new Duration(first, instant);
      long calculatedGrowth =
          (durationSinceFirst.getStandardMinutes() - rampUpIntervalMinutes) / rampUpIntervalMinutes;
      long growth = Math.max(0, calculatedGrowth);
      return baseBatchBudget * Math.pow(1.5, growth);
    }

    void recordWriteCount(Instant instant, int numWrites) {
      writeCounts.add(instant.getMillis(), numWrites);
    }

    Optional<Duration> shouldThrottle(Instant instant) {
      int availableWriteCountBudget = getAvailableWriteCountBudget(instant);
      if (availableWriteCountBudget <= 0) {
        long nextBackOffMillis = backoff.nextBackOffMillis();
        if (nextBackOffMillis > BackOff.STOP) {
          Duration throttleDuration = Duration.millis(nextBackOffMillis);
          throttlingMs.update(throttleDuration.getMillis());
          return Optional.of(throttleDuration);
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
  }

  /**
   * For ramp up we're following a simplistic linear growth formula, when calculating backoff we're
   * calculating the next time to check a client side budget and we don't need the randomness
   * introduced by FluentBackoff. (Being linear also makes our test simulations easier to model and
   * verify)
   */
  private static class LinearBackoff implements BackOff {
    private static final long MAX_BACKOFF_MILLIS = 60_000;
    private static final long MAX_CUMULATIVE_MILLIS = 60_000;
    private final long startBackoffMillis;
    private long currentBackoffMillis;
    private long cumulativeMillis;

    public LinearBackoff(Duration throttleDuration) {
      startBackoffMillis = throttleDuration.getMillis();
      currentBackoffMillis = startBackoffMillis;
      cumulativeMillis = 0;
    }

    @Override
    public void reset() {
      currentBackoffMillis = startBackoffMillis;
      cumulativeMillis = 0;
    }

    @Override
    public long nextBackOffMillis() {
      if (currentBackoffMillis > MAX_BACKOFF_MILLIS) {
        reset();
        return MAX_BACKOFF_MILLIS;
      } else {
        long remainingBudget = Math.max(MAX_CUMULATIVE_MILLIS - cumulativeMillis, 0);
        if (remainingBudget == 0) {
          reset();
          return STOP;
        }
        long retVal = Math.min(currentBackoffMillis, remainingBudget);
        currentBackoffMillis = (long) (currentBackoffMillis * 1.5);
        cumulativeMillis += retVal;
        return retVal;
      }
    }
  }

  /**
   * This class implements a backoff algorithm similar to that of {@link
   * org.apache.beam.sdk.util.FluentBackoff} with some key differences:
   *
   * <ol>
   *   <li>A set of status code numbers may be specified to have a graceful evaluation
   *   <li>Gracefully evaluated status code numbers will increment a decaying counter to ensure if
   *       the graceful status code numbers occur more than once in the previous 60 seconds the
   *       regular backoff behavior will kick in.
   *   <li>The random number generator used to induce jitter is provided via constructor parameter
   *       rather than using {@link Math#random()}}
   * </ol>
   *
   * The primary motivation for creating this implementation is to support streamed responses from
   * Firestore. In the case of RunQuery and BatchGet the results are returned via stream. The result
   * stream has a maximum lifetime of 60 seconds before it will be broken and an UNAVAILABLE status
   * code will be raised. Give that UNAVAILABLE is expected for streams, this class allows for
   * defining a set of status code numbers which are given a grace count of 1 before backoff kicks
   * in. When backoff does kick in, it is implemented using the same calculations as {@link
   * org.apache.beam.sdk.util.FluentBackoff}.
   */
  static final class StatusCodeAwareBackoff {
    private static final double RANDOMIZATION_FACTOR = 0.5;
    private static final Duration MAX_BACKOFF = Duration.standardMinutes(1);
    private static final Duration MAX_CUMULATIVE_BACKOFF = Duration.standardMinutes(1);

    private final Random rand;
    private final int maxAttempts;
    private final Duration initialBackoff;
    private final Set<Integer> graceStatusCodeNumbers;
    private final MovingFunction graceStatusCodeTracker;

    private Duration cumulativeBackoff;
    private int attempt;

    StatusCodeAwareBackoff(
        Random rand,
        int maxAttempts,
        Duration throttleDuration,
        Set<Integer> graceStatusCodeNumbers) {
      this.rand = rand;
      this.graceStatusCodeNumbers = graceStatusCodeNumbers;
      this.maxAttempts = maxAttempts;
      this.initialBackoff = throttleDuration;
      this.graceStatusCodeTracker = createGraceStatusCodeTracker();
      this.cumulativeBackoff = Duration.ZERO;
      this.attempt = 1;
    }

    BackoffResult nextBackoff(Instant now, int statusCodeNumber) {
      if (graceStatusCodeNumbers.contains(statusCodeNumber)) {
        long nowMillis = now.getMillis();
        long numGraceStatusCode = graceStatusCodeTracker.get(nowMillis);
        graceStatusCodeTracker.add(nowMillis, 1);
        if (numGraceStatusCode < 1) {
          return BackoffResults.NONE;
        } else {
          return doBackoff();
        }
      } else {
        return doBackoff();
      }
    }

    private BackoffResult doBackoff() {
      // Maximum number of retries reached.
      if (attempt >= maxAttempts) {
        return BackoffResults.EXHAUSTED;
      }
      // Maximum cumulative backoff reached.
      if (cumulativeBackoff.compareTo(MAX_CUMULATIVE_BACKOFF) >= 0) {
        return BackoffResults.EXHAUSTED;
      }

      double currentIntervalMillis =
          Math.min(
              initialBackoff.getMillis() * Math.pow(1.5, attempt - 1), MAX_BACKOFF.getMillis());
      double randomOffset =
          (rand.nextDouble() * 2 - 1) * RANDOMIZATION_FACTOR * currentIntervalMillis;
      long nextBackoffMillis = Math.round(currentIntervalMillis + randomOffset);
      // Cap to limit on cumulative backoff
      Duration remainingCumulative = MAX_CUMULATIVE_BACKOFF.minus(cumulativeBackoff);
      nextBackoffMillis = Math.min(nextBackoffMillis, remainingCumulative.getMillis());

      // Update state and return backoff.
      cumulativeBackoff = cumulativeBackoff.plus(Duration.millis(nextBackoffMillis));
      attempt += 1;
      return new BackoffDuration(Duration.millis(nextBackoffMillis));
    }

    private static MovingFunction createGraceStatusCodeTracker() {
      return createMovingFunction(Duration.standardMinutes(1), Duration.millis(500));
    }

    interface BackoffResult {}

    enum BackoffResults implements BackoffResult {
      EXHAUSTED,
      NONE
    }

    static final class BackoffDuration implements BackoffResult {
      private final Duration duration;

      BackoffDuration(Duration duration) {
        this.duration = duration;
      }

      Duration getDuration() {
        return duration;
      }

      @Override
      public boolean equals(@Nullable Object o) {
        if (this == o) {
          return true;
        }
        if (!(o instanceof BackoffDuration)) {
          return false;
        }
        BackoffDuration that = (BackoffDuration) o;
        return Objects.equals(duration, that.duration);
      }

      @Override
      public int hashCode() {
        return Objects.hash(duration);
      }

      @Override
      public String toString() {
        return "BackoffDuration{" + "duration=" + duration + '}';
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
      return sum.isSignificant()
          && count.isSignificant()
          && count.get(instantSinceEpoch.getMillis()) > 0;
    }
  }

  /**
   * Observability (o11y) related metrics. Contains handles to counters and distributions related to
   * QoS.
   */
  private static final class O11y {
    final Counter throttlingMs;
    final Counter rpcFailures;
    final Counter rpcSuccesses;
    final Counter rpcStreamValueReceived;
    final Distribution rpcDurationMs;
    final Distribution latencyPerDocumentMs;
    final Distribution batchCapacityCount;

    private O11y(
        Counter throttlingMs,
        Counter rpcFailures,
        Counter rpcSuccesses,
        Counter rpcStreamValueReceived,
        Distribution rpcDurationMs,
        Distribution latencyPerDocumentMs,
        Distribution batchCapacityCount) {
      this.throttlingMs = throttlingMs;
      this.rpcFailures = rpcFailures;
      this.rpcSuccesses = rpcSuccesses;
      this.rpcStreamValueReceived = rpcStreamValueReceived;
      this.rpcDurationMs = rpcDurationMs;
      this.latencyPerDocumentMs = latencyPerDocumentMs;
      this.batchCapacityCount = batchCapacityCount;
    }

    private static O11y create(
        Context context, CounterFactory counterFactory, DistributionFactory distributionFactory) {
      // metrics are named using '_' (underscore) instead of '/' (slash) as separators, because
      // metric names become gcp resources and get unique URLs, so some parts of the UI will only
      // show the value after the last '/'
      return new O11y(
          // throttlingMs is a special counter used by dataflow. When we are having to throttle,
          // we signal to dataflow that fact by adding to this counter.
          // Signaling to dataflow is important so that a bundle isn't categorised as hung.
          counterFactory.get(context.getNamespace(), "throttlingMs"),
          // metrics specific to each rpc
          counterFactory.get(context.getNamespace(), "rpc_failures"),
          counterFactory.get(context.getNamespace(), "rpc_successes"),
          counterFactory.get(context.getNamespace(), "rpc_streamValueReceived"),
          distributionFactory.get(context.getNamespace(), "rpc_durationMs"),
          // qos wide metrics
          distributionFactory.get(RpcQos.class.getName(), "qos_write_latencyPerDocumentMs"),
          distributionFactory.get(RpcQos.class.getName(), "qos_write_batchCapacityCount"));
    }
  }

  static class FlushBufferImpl<ElementT extends Element<?>> implements FlushBuffer<ElementT> {

    final int nextBatchMaxCount;
    final long nextBatchMaxBytes;
    final ImmutableList.Builder<ElementT> elements;

    int offersAcceptedCount = 0;
    long offersAcceptedBytes = 0;

    public FlushBufferImpl(int nextBatchMaxCount, long nextBatchMaxBytes) {
      this.nextBatchMaxCount = nextBatchMaxCount;
      this.nextBatchMaxBytes = nextBatchMaxBytes;
      this.elements = ImmutableList.builder();
    }

    @Override
    public boolean offer(ElementT newElement) {
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
    public Iterator<ElementT> iterator() {
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
      return isNonEmpty()
          && (offersAcceptedCount == nextBatchMaxCount || offersAcceptedBytes >= nextBatchMaxBytes);
    }

    @Override
    public boolean isNonEmpty() {
      return offersAcceptedCount > 0;
    }
  }

  private static final class DiagnosticOnlyFilteringDistributionFactory
      implements DistributionFactory {

    private static final Set<String> DIAGNOSTIC_ONLY_METRIC_NAMES =
        ImmutableSet.of(
            "qos_adaptiveThrottler_allRequestsCount",
            "qos_adaptiveThrottler_overloadMaxCount",
            "qos_adaptiveThrottler_overloadUsagePct",
            "qos_adaptiveThrottler_successfulRequestsCount",
            "qos_adaptiveThrottler_throttleProbabilityPct",
            "qos_adaptiveThrottler_throttlingMs",
            "qos_rampUp_availableWriteCountBudget",
            "qos_rampUp_throttlingMs",
            "qos_writeBatcher_batchMaxCount",
            "qos_write_latencyPerDocumentMs");

    private final boolean excludeMetrics;
    private final DistributionFactory delegate;

    private DiagnosticOnlyFilteringDistributionFactory(
        boolean excludeMetrics, DistributionFactory delegate) {
      this.excludeMetrics = excludeMetrics;
      this.delegate = delegate;
    }

    @Override
    public Distribution get(String namespace, String name) {
      if (excludeMetrics && DIAGNOSTIC_ONLY_METRIC_NAMES.contains(name)) {
        return new NullDistribution(new SimpleMetricName(namespace, name));
      } else {
        return delegate.get(namespace, name);
      }
    }
  }

  private static final class NullDistribution implements Distribution {

    private final MetricName name;

    private NullDistribution(MetricName name) {
      this.name = name;
    }

    @Override
    public void update(long value) {}

    @Override
    public void update(long sum, long count, long min, long max) {}

    @Override
    public MetricName getName() {
      return name;
    }
  }

  private static class SimpleMetricName extends MetricName {

    private final String namespace;
    private final String name;

    public SimpleMetricName(String namespace, String name) {
      this.namespace = namespace;
      this.name = name;
    }

    @Override
    public String getNamespace() {
      return namespace;
    }

    @Override
    public String getName() {
      return name;
    }
  }
}
