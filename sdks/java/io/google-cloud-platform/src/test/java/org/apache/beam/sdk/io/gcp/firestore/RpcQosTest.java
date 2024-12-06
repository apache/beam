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

import static org.apache.beam.sdk.io.gcp.firestore.FirestoreProtoHelpers.newWrite;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.firestore.v1.Write;
import com.google.rpc.Code;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.grpc.Status;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1WriteFn.WriteElement;
import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcAttempt.Context;
import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcReadAttempt;
import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcWriteAttempt;
import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcWriteAttempt.Element;
import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcWriteAttempt.FlushBuffer;
import org.apache.beam.sdk.io.gcp.firestore.RpcQosImpl.FlushBufferImpl;
import org.apache.beam.sdk.io.gcp.firestore.RpcQosImpl.RpcWriteAttemptImpl;
import org.apache.beam.sdk.io.gcp.firestore.RpcQosImpl.StatusCodeAwareBackoff;
import org.apache.beam.sdk.io.gcp.firestore.RpcQosImpl.StatusCodeAwareBackoff.BackoffDuration;
import org.apache.beam.sdk.io.gcp.firestore.RpcQosImpl.StatusCodeAwareBackoff.BackoffResult;
import org.apache.beam.sdk.io.gcp.firestore.RpcQosImpl.StatusCodeAwareBackoff.BackoffResults;
import org.apache.beam.sdk.io.gcp.firestore.RpcQosImpl.WriteRampUp;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings(
    "initialization.fields.uninitialized") // mockito fields are initialized via the Mockito Runner
public final class RpcQosTest {
  private static final ApiException RETRYABLE_ERROR =
      ApiExceptionFactory.createException(
          new SocketTimeoutException("retryableError"),
          GrpcStatusCode.of(Status.Code.CANCELLED),
          true);
  private static final ApiException NON_RETRYABLE_ERROR =
      ApiExceptionFactory.createException(
          new IOException("nonRetryableError"),
          GrpcStatusCode.of(Status.Code.FAILED_PRECONDITION),
          false);
  private static final ApiException RETRYABLE_ERROR_WITH_NON_RETRYABLE_CODE =
      ApiExceptionFactory.createException(
          new SocketTimeoutException("retryableError"),
          GrpcStatusCode.of(Status.Code.INVALID_ARGUMENT),
          true);

  private static final Context RPC_ATTEMPT_CONTEXT = RpcQosTest.class::getName;

  @Mock(lenient = true)
  private Sleeper sleeper;

  @Mock(lenient = true)
  private CounterFactory counterFactory;

  @Mock(lenient = true)
  private DistributionFactory distributionFactory;

  @Mock(lenient = true)
  private Counter counterThrottlingMs;

  @Mock(lenient = true)
  private Counter counterRpcFailures;

  @Mock(lenient = true)
  private Counter counterRpcSuccesses;

  @Mock(lenient = true)
  private Counter counterRpcStreamValueReceived;

  @Mock(lenient = true)
  private BoundedWindow window;

  // A clock that increments one from the epoch each time it's called
  private final JodaClock monotonicClock =
      new JodaClock() {
        private long counter = 0;

        @Override
        public Instant instant() {
          return Instant.ofEpochMilli(counter++);
        }
      };
  // should not be static, important to reinitialize for each test
  private final Random random =
      new Random(1234567890); // fix the seed so we have deterministic tests

  private RpcQosOptions options;

  @Before
  public void setUp() {
    when(counterFactory.get(RPC_ATTEMPT_CONTEXT.getNamespace(), "throttlingMs"))
        .thenReturn(counterThrottlingMs);
    when(counterFactory.get(RPC_ATTEMPT_CONTEXT.getNamespace(), "rpc_failures"))
        .thenReturn(counterRpcFailures);
    when(counterFactory.get(RPC_ATTEMPT_CONTEXT.getNamespace(), "rpc_successes"))
        .thenReturn(counterRpcSuccesses);
    when(counterFactory.get(RPC_ATTEMPT_CONTEXT.getNamespace(), "rpc_streamValueReceived"))
        .thenReturn(counterRpcStreamValueReceived);
    when(distributionFactory.get(any(), any()))
        .thenAnswer(
            invocation -> mock(Distribution.class, invocation.getArgument(1, String.class)));

    // init here after mocks have been initialized
    options =
        RpcQosOptions.defaultOptions()
            .toBuilder()
            .withInitialBackoff(Duration.millis(1))
            .withSamplePeriod(Duration.millis(100))
            .withSamplePeriodBucketSize(Duration.millis(10))
            .withOverloadRatio(2.0)
            .withThrottleDuration(Duration.millis(50))
            .withHintMaxNumWorkers(1)
            .unsafeBuild();
  }

  @Test
  public void reads_processedWhenNoErrors() throws InterruptedException {

    RpcQos qos = new RpcQosImpl(options, random, sleeper, counterFactory, distributionFactory);

    int numSuccesses = 100;
    int numStreamElements = 25;
    // record enough successful requests to fill up the sample period
    for (int i = 0; i < numSuccesses; i++) {
      RpcReadAttempt readAttempt = qos.newReadAttempt(RPC_ATTEMPT_CONTEXT);
      Instant start = monotonicClock.instant();
      assertTrue(readAttempt.awaitSafeToProceed(start));
      for (int j = 0; j < numStreamElements; j++) {
        readAttempt.recordStreamValue(monotonicClock.instant());
      }
      readAttempt.recordRequestStart(monotonicClock.instant());
      readAttempt.recordRequestSuccessful(monotonicClock.instant());
    }

    verify(sleeper, times(0)).sleep(anyLong());
    verify(counterThrottlingMs, times(0)).inc(anyLong());
    verify(counterRpcFailures, times(0)).inc();
    verify(counterRpcSuccesses, times(numSuccesses)).inc();
    verify(counterRpcStreamValueReceived, times(numSuccesses * numStreamElements)).inc();
  }

  @Test
  public void reads_blockWhenNotSafeToProceed() throws InterruptedException {

    RpcQos qos = new RpcQosImpl(options, random, sleeper, counterFactory, distributionFactory);

    // Based on the defined options, 3 failures is the upper bound before the next attempt
    // will have to wait before proceeding
    int numFailures = 3;
    for (int i = 0; i < numFailures; i++) {
      RpcReadAttempt readAttempt = qos.newReadAttempt(RPC_ATTEMPT_CONTEXT);
      Instant start = monotonicClock.instant();
      assertTrue(readAttempt.awaitSafeToProceed(start));
      Instant end = monotonicClock.instant();
      readAttempt.recordRequestStart(start);
      readAttempt.recordRequestFailed(end);
    }

    RpcReadAttempt readAttempt2 = qos.newReadAttempt(RPC_ATTEMPT_CONTEXT);
    assertFalse(readAttempt2.awaitSafeToProceed(monotonicClock.instant()));

    long sleepMillis = options.getInitialBackoff().getMillis();

    verify(sleeper, times(0)).sleep(sleepMillis);
    verify(counterThrottlingMs, times(0)).inc(sleepMillis);
    verify(counterRpcFailures, times(numFailures)).inc();
    verify(counterRpcSuccesses, times(0)).inc();
    verify(counterRpcStreamValueReceived, times(0)).inc();
  }

  @Test
  public void writes_blockWhenNotSafeToProceed() throws InterruptedException {

    RpcQos qos = new RpcQosImpl(options, random, sleeper, counterFactory, distributionFactory);

    // Based on the defined options, 3 failures is the upper bound before the next attempt
    // will have to wait before proceeding
    int numFailures = 3;
    for (int i = 0; i < numFailures; i++) {
      RpcWriteAttempt writeAttempt = qos.newWriteAttempt(RPC_ATTEMPT_CONTEXT);
      Instant start = monotonicClock.instant();
      assertTrue(writeAttempt.awaitSafeToProceed(start));
      writeAttempt.recordRequestStart(start, 1);
      Instant end = monotonicClock.instant();
      writeAttempt.recordWriteCounts(end, 0, 1);
      writeAttempt.recordRequestFailed(end);
    }

    RpcWriteAttempt writeAttempt2 = qos.newWriteAttempt(RPC_ATTEMPT_CONTEXT);
    assertFalse(writeAttempt2.awaitSafeToProceed(monotonicClock.instant()));

    long sleepMillis = options.getInitialBackoff().getMillis();

    verify(sleeper, times(0)).sleep(sleepMillis);
    verify(counterThrottlingMs, times(0)).inc(sleepMillis);
    verify(counterRpcFailures, times(numFailures)).inc();
    verify(counterRpcSuccesses, times(0)).inc();
    verify(counterRpcStreamValueReceived, times(0)).inc();
  }

  @Test
  public void writes_shouldFlush_numWritesHigherThanBatchCount_newTimeBucket_lteq0() {
    doTest_writes_shouldFlush_numWritesHigherThanBatchCount_newTimeBucket(false, 0);
  }

  @Test
  public void writes_shouldFlush_numWritesHigherThanBatchCount_newTimeBucket_lt() {
    doTest_writes_shouldFlush_numWritesHigherThanBatchCount_newTimeBucket(false, 9);
  }

  @Test
  public void writes_shouldFlush_numWritesHigherThanBatchCount_newTimeBucket_eq() {
    doTest_writes_shouldFlush_numWritesHigherThanBatchCount_newTimeBucket(true, 10);
  }

  @Test
  public void writes_shouldFlush_numWritesHigherThanBatchCount_newTimeBucket_gt() {
    doTest_writes_shouldFlush_numWritesHigherThanBatchCount_newTimeBucket(true, 11);
  }

  @Test
  public void writes_shouldFlush_numWritesHigherThanBatchCount_existingTimeBucket_lteq0() {
    doTest_writes_shouldFlush_numWritesHigherThanBatchCount_existingTimeBucket(false, 0);
  }

  @Test
  public void writes_shouldFlush_numWritesHigherThanBatchCount_existingTimeBucket_lt() {
    doTest_writes_shouldFlush_numWritesHigherThanBatchCount_existingTimeBucket(false, 9);
  }

  @Test
  public void writes_shouldFlush_numWritesHigherThanBatchCount_existingTimeBucket_eq() {
    doTest_writes_shouldFlush_numWritesHigherThanBatchCount_existingTimeBucket(true, 10);
  }

  @Test
  public void writes_shouldFlush_numWritesHigherThanBatchCount_existingTimeBucket_gt() {
    doTest_writes_shouldFlush_numWritesHigherThanBatchCount_existingTimeBucket(true, 11);
  }

  @Test
  public void writes_shouldFlush_numBytes_lt() {
    doTest_writes_shouldFlush_numBytes(false, 2999);
  }

  @Test
  public void writes_shouldFlush_numBytes_eq() {
    doTest_writes_shouldFlush_numBytes(true, 3000);
  }

  @Test
  public void attemptsExhaustCorrectly() throws InterruptedException {

    RpcQosOptions rpcQosOptions = options.toBuilder().withMaxAttempts(3).unsafeBuild();
    RpcQos qos =
        new RpcQosImpl(rpcQosOptions, random, sleeper, counterFactory, distributionFactory);

    RpcReadAttempt readAttempt = qos.newReadAttempt(RPC_ATTEMPT_CONTEXT);
    // try 1
    readAttempt.recordRequestStart(monotonicClock.instant());
    readAttempt.recordRequestFailed(monotonicClock.instant());
    readAttempt.checkCanRetry(monotonicClock.instant(), RETRYABLE_ERROR);
    // try 2
    readAttempt.recordRequestStart(monotonicClock.instant());
    readAttempt.recordRequestFailed(monotonicClock.instant());
    readAttempt.checkCanRetry(monotonicClock.instant(), RETRYABLE_ERROR);
    // try 3
    readAttempt.recordRequestStart(monotonicClock.instant());
    readAttempt.recordRequestFailed(monotonicClock.instant());
    try {
      readAttempt.checkCanRetry(monotonicClock.instant(), RETRYABLE_ERROR);
      fail("expected retry to be exhausted after third attempt");
    } catch (ApiException e) {
      assertSame(e, RETRYABLE_ERROR);
    }

    verify(counterThrottlingMs, times(0)).inc(anyLong());
    verify(counterRpcFailures, times(3)).inc();
    verify(counterRpcSuccesses, times(0)).inc();
    verify(counterRpcStreamValueReceived, times(0)).inc();
  }

  @Test
  public void attemptThrowsOnNonRetryableError() throws InterruptedException {

    RpcQosOptions rpcQosOptions = options.toBuilder().withMaxAttempts(3).unsafeBuild();
    RpcQos qos =
        new RpcQosImpl(rpcQosOptions, random, sleeper, counterFactory, distributionFactory);

    RpcReadAttempt readAttempt = qos.newReadAttempt(RPC_ATTEMPT_CONTEXT);
    readAttempt.recordRequestStart(monotonicClock.instant());
    // try 1
    readAttempt.recordRequestFailed(monotonicClock.instant());
    try {
      readAttempt.checkCanRetry(monotonicClock.instant(), NON_RETRYABLE_ERROR);
      fail("expected non-retryable error to throw error on first occurrence");
    } catch (ApiException e) {
      assertSame(e, NON_RETRYABLE_ERROR);
    }

    verify(counterThrottlingMs, times(0)).inc(anyLong());
    verify(counterRpcFailures, times(1)).inc();
    verify(counterRpcSuccesses, times(0)).inc();
    verify(counterRpcStreamValueReceived, times(0)).inc();
  }

  @Test
  public void attemptThrowsOnNonRetryableErrorCode() throws InterruptedException {

    RpcQosOptions rpcQosOptions = options.toBuilder().withMaxAttempts(3).unsafeBuild();
    RpcQos qos =
        new RpcQosImpl(rpcQosOptions, random, sleeper, counterFactory, distributionFactory);

    RpcReadAttempt readAttempt = qos.newReadAttempt(RPC_ATTEMPT_CONTEXT);
    readAttempt.recordRequestStart(monotonicClock.instant());
    // try 1
    readAttempt.recordRequestFailed(monotonicClock.instant());
    try {
      readAttempt.checkCanRetry(monotonicClock.instant(), RETRYABLE_ERROR_WITH_NON_RETRYABLE_CODE);
      fail("expected non-retryable error to throw error on first occurrence");
    } catch (ApiException e) {
      assertSame(e, RETRYABLE_ERROR_WITH_NON_RETRYABLE_CODE);
    }

    verify(counterThrottlingMs, times(0)).inc(anyLong());
    verify(counterRpcFailures, times(1)).inc();
    verify(counterRpcSuccesses, times(0)).inc();
    verify(counterRpcStreamValueReceived, times(0)).inc();
  }

  @Test
  public void attemptEnforcesActiveStateToPerformOperations_maxAttemptsExhausted()
      throws InterruptedException {
    RpcQosOptions rpcQosOptions = options.toBuilder().withMaxAttempts(1).unsafeBuild();
    RpcQos qos =
        new RpcQosImpl(rpcQosOptions, random, sleeper, counterFactory, distributionFactory);

    RpcReadAttempt readAttempt = qos.newReadAttempt(RPC_ATTEMPT_CONTEXT);

    readAttempt.recordRequestStart(monotonicClock.instant());
    readAttempt.recordRequestFailed(monotonicClock.instant());
    try {
      readAttempt.checkCanRetry(monotonicClock.instant(), RETRYABLE_ERROR);
      fail("expected error to be re-thrown due to max attempts exhaustion");
    } catch (ApiException e) {
      // expected
    }

    try {
      readAttempt.recordStreamValue(monotonicClock.instant());
      fail("expected IllegalStateException due to attempt being in terminal state");
    } catch (IllegalStateException e) {
      // expected
    }

    verify(sleeper, times(0))
        .sleep(anyLong()); // happens in checkCanRetry when the backoff is checked
    verify(counterThrottlingMs, times(0)).inc(anyLong());
    verify(counterRpcFailures, times(1)).inc();
    verify(counterRpcSuccesses, times(0)).inc();
    verify(counterRpcStreamValueReceived, times(0)).inc();
  }

  @Test
  public void attemptEnforcesActiveStateToPerformOperations_successful()
      throws InterruptedException {
    RpcQosOptions rpcQosOptions = options.toBuilder().withMaxAttempts(1).unsafeBuild();
    RpcQos qos =
        new RpcQosImpl(rpcQosOptions, random, sleeper, counterFactory, distributionFactory);

    RpcReadAttempt readAttempt = qos.newReadAttempt(RPC_ATTEMPT_CONTEXT);

    readAttempt.recordRequestStart(monotonicClock.instant());
    readAttempt.recordRequestSuccessful(monotonicClock.instant());
    readAttempt.completeSuccess();
    try {
      readAttempt.recordStreamValue(monotonicClock.instant());
      fail("expected IllegalStateException due to attempt being in terminal state");
    } catch (IllegalStateException e) {
      // expected
    }

    verify(sleeper, times(0))
        .sleep(anyLong()); // happens in checkCanRetry when the backoff is checked
    verify(counterThrottlingMs, times(0)).inc(anyLong());
    verify(counterRpcFailures, times(0)).inc();
    verify(counterRpcSuccesses, times(1)).inc();
    verify(counterRpcStreamValueReceived, times(0)).inc();
  }

  @Test
  public void offerOfElementWhichWouldCrossMaxBytesReturnFalse() {
    RpcQosOptions rpcQosOptions = options.toBuilder().withBatchMaxBytes(5000).unsafeBuild();
    RpcQos qos =
        new RpcQosImpl(rpcQosOptions, random, sleeper, counterFactory, distributionFactory);

    RpcWriteAttempt attempt = qos.newWriteAttempt(RPC_ATTEMPT_CONTEXT);
    FlushBuffer<Element<Write>> accumulator = attempt.newFlushBuffer(monotonicClock.instant());
    assertFalse(accumulator.offer(new FixedSerializationSize<>(newWrite(), 5001)));

    assertFalse(accumulator.isFull());
    assertEquals(0, accumulator.getBufferedElementsBytes());
    assertEquals(0, accumulator.getBufferedElementsCount());
  }

  @Test
  public void flushBuffer_doesNotErrorWhenMaxIsOne() {
    FlushBufferImpl<Element<String>> buffer = new FlushBufferImpl<>(1, 1000);
    assertTrue(buffer.offer(new FixedSerializationSize<>("a", 1)));
    assertFalse(buffer.offer(new FixedSerializationSize<>("b", 1)));
    assertEquals(1, buffer.getBufferedElementsCount());
  }

  @Test
  public void flushBuffer_doesNotErrorWhenMaxIsZero() {
    FlushBufferImpl<Element<String>> buffer = new FlushBufferImpl<>(0, 1000);
    assertFalse(buffer.offer(new FixedSerializationSize<>("a", 1)));
    assertEquals(0, buffer.getBufferedElementsCount());
    assertFalse(buffer.isFull());
    assertFalse(buffer.isNonEmpty());
  }

  @Test
  public void rampUp_calcForWorkerCount1() {
    Instant t0 = Instant.ofEpochMilli(0);
    Instant t5 = Instant.ofEpochSecond(60 * 5);
    Instant t10 = Instant.ofEpochSecond(60 * 10);
    Instant t15 = Instant.ofEpochSecond(60 * 15);
    Instant t90 = Instant.ofEpochSecond(60 * 90);

    WriteRampUp tracker = new WriteRampUp(500, distributionFactory);
    assertEquals(500, tracker.getAvailableWriteCountBudget(t0));
    assertEquals(500, tracker.getAvailableWriteCountBudget(t5));
    assertEquals(750, tracker.getAvailableWriteCountBudget(t10));
    assertEquals(1_125, tracker.getAvailableWriteCountBudget(t15));
    assertEquals(492_630, tracker.getAvailableWriteCountBudget(t90));
  }

  @Test
  public void rampUp_calcForWorkerCount100() {
    Instant t0 = Instant.ofEpochMilli(0);
    Instant t5 = Instant.ofEpochSecond(60 * 5);
    Instant t10 = Instant.ofEpochSecond(60 * 10);
    Instant t15 = Instant.ofEpochSecond(60 * 15);
    Instant t90 = Instant.ofEpochSecond(60 * 90);

    WriteRampUp tracker = new WriteRampUp(5, distributionFactory);
    assertEquals(5, tracker.getAvailableWriteCountBudget(t0));
    assertEquals(5, tracker.getAvailableWriteCountBudget(t5));
    assertEquals(7, tracker.getAvailableWriteCountBudget(t10));
    assertEquals(11, tracker.getAvailableWriteCountBudget(t15));
    assertEquals(4_926, tracker.getAvailableWriteCountBudget(t90));
  }

  @Test
  public void rampUp_calcForWorkerCount1000() {
    Instant t0 = Instant.ofEpochMilli(0);
    Instant t5 = Instant.ofEpochSecond(60 * 5);
    Instant t10 = Instant.ofEpochSecond(60 * 10);
    Instant t15 = Instant.ofEpochSecond(60 * 15);
    Instant t90 = Instant.ofEpochSecond(60 * 90);

    WriteRampUp tracker = new WriteRampUp(1, distributionFactory);
    assertEquals(1, tracker.getAvailableWriteCountBudget(t0));
    assertEquals(1, tracker.getAvailableWriteCountBudget(t5));
    assertEquals(1, tracker.getAvailableWriteCountBudget(t10));
    assertEquals(2, tracker.getAvailableWriteCountBudget(t15));
    assertEquals(985, tracker.getAvailableWriteCountBudget(t90));
  }

  @Test
  public void rampUp_calcFor90Minutes() {
    int increment = 5;
    // 500 * 1.5^max(0, (x-5)/5)
    List<Integer> expected =
        from0To90By(increment)
            .map(x -> (int) (500 * Math.pow(1.5, Math.max(0, (x - increment) / increment))))
            .boxed()
            .collect(Collectors.toList());

    WriteRampUp tracker = new WriteRampUp(500, distributionFactory);
    List<Integer> actual =
        from0To90By(increment)
            .mapToObj(i -> Instant.ofEpochSecond(60 * i))
            .map(tracker::getAvailableWriteCountBudget)
            .collect(Collectors.toList());

    assertEquals(expected, actual);
  }

  @Test
  public void initialBatchSizeRelativeToWorkerCount_10000() {
    doTest_initialBatchSizeRelativeToWorkerCount(10000, 1);
  }

  @Test
  public void initialBatchSizeRelativeToWorkerCount_1000() {
    doTest_initialBatchSizeRelativeToWorkerCount(1000, 1);
  }

  @Test
  public void initialBatchSizeRelativeToWorkerCount_100() {
    doTest_initialBatchSizeRelativeToWorkerCount(100, 5);
  }

  @Test
  public void initialBatchSizeRelativeToWorkerCount_10() {
    doTest_initialBatchSizeRelativeToWorkerCount(10, 50);
  }

  @Test
  public void initialBatchSizeRelativeToWorkerCount_1() {
    doTest_initialBatchSizeRelativeToWorkerCount(1, 500);
  }

  @Test
  public void isCodeRetryable() {
    doTest_isCodeRetryable(Code.ABORTED, true);
    doTest_isCodeRetryable(Code.ALREADY_EXISTS, false);
    doTest_isCodeRetryable(Code.CANCELLED, true);
    doTest_isCodeRetryable(Code.DATA_LOSS, false);
    doTest_isCodeRetryable(Code.DEADLINE_EXCEEDED, true);
    doTest_isCodeRetryable(Code.FAILED_PRECONDITION, false);
    doTest_isCodeRetryable(Code.INTERNAL, true);
    doTest_isCodeRetryable(Code.INVALID_ARGUMENT, false);
    doTest_isCodeRetryable(Code.NOT_FOUND, false);
    doTest_isCodeRetryable(Code.OK, true);
    doTest_isCodeRetryable(Code.OUT_OF_RANGE, false);
    doTest_isCodeRetryable(Code.PERMISSION_DENIED, false);
    doTest_isCodeRetryable(Code.RESOURCE_EXHAUSTED, true);
    doTest_isCodeRetryable(Code.UNAUTHENTICATED, true);
    doTest_isCodeRetryable(Code.UNAVAILABLE, true);
    doTest_isCodeRetryable(Code.UNIMPLEMENTED, false);
    doTest_isCodeRetryable(Code.UNKNOWN, true);
  }

  @Test
  public void statusCodeAwareBackoff_graceCodeBackoffWithin60sec() {

    StatusCodeAwareBackoff backoff =
        new StatusCodeAwareBackoff(
            random, 5, Duration.standardSeconds(5), ImmutableSet.of(Code.UNAVAILABLE_VALUE));

    BackoffResult backoffResult1 =
        backoff.nextBackoff(Instant.ofEpochMilli(1), Code.UNAVAILABLE_VALUE);
    assertEquals(BackoffResults.NONE, backoffResult1);

    BackoffResult backoffResult2 =
        backoff.nextBackoff(Instant.ofEpochMilli(2), Code.UNAVAILABLE_VALUE);
    assertEquals(new BackoffDuration(Duration.millis(6_091)), backoffResult2);

    BackoffResult backoffResult3 =
        backoff.nextBackoff(Instant.ofEpochMilli(60_100), Code.UNAVAILABLE_VALUE);
    assertEquals(BackoffResults.NONE, backoffResult3);
  }

  @Test
  public void statusCodeAwareBackoff_exhausted_attemptCount() {

    StatusCodeAwareBackoff backoff =
        new StatusCodeAwareBackoff(random, 1, Duration.standardSeconds(5), Collections.emptySet());

    BackoffResult backoffResult1 =
        backoff.nextBackoff(Instant.ofEpochMilli(1), Code.UNAVAILABLE_VALUE);
    assertEquals(BackoffResults.EXHAUSTED, backoffResult1);
  }

  @Test
  public void statusCodeAwareBackoff_exhausted_cumulativeBackoff() {

    StatusCodeAwareBackoff backoff =
        new StatusCodeAwareBackoff(random, 3, Duration.standardSeconds(60), Collections.emptySet());

    BackoffDuration backoff60Sec = new BackoffDuration(Duration.standardMinutes(1));
    BackoffResult backoffResult1 =
        backoff.nextBackoff(Instant.ofEpochMilli(1), Code.DEADLINE_EXCEEDED_VALUE);
    assertEquals(backoff60Sec, backoffResult1);

    BackoffResult backoffResult2 =
        backoff.nextBackoff(Instant.ofEpochMilli(2), Code.DEADLINE_EXCEEDED_VALUE);
    assertEquals(BackoffResults.EXHAUSTED, backoffResult2);
  }

  private IntStream from0To90By(int increment) {
    return IntStream.iterate(0, i -> i + increment).limit((90 / increment) + 1);
  }

  private void doTest_writes_shouldFlush_numWritesHigherThanBatchCount_newTimeBucket(
      boolean expectFlush, int batchCount) {
    doTest_shouldFlush_numWritesHigherThanBatchCount(expectFlush, batchCount, qos -> {});
  }

  private void doTest_writes_shouldFlush_numWritesHigherThanBatchCount_existingTimeBucket(
      boolean expectFlush, int batchCount) {
    doTest_shouldFlush_numWritesHigherThanBatchCount(
        expectFlush,
        batchCount,
        (qos) -> {
          RpcWriteAttempt attempt = qos.newWriteAttempt(RPC_ATTEMPT_CONTEXT);
          attempt.recordRequestStart(monotonicClock.instant(), 1);
          attempt.recordWriteCounts(monotonicClock.instant(), 1, 0);
        });
  }

  private void doTest_shouldFlush_numWritesHigherThanBatchCount(
      boolean expectFlush, int batchCount, Consumer<RpcQos> preAttempt) {
    RpcQosOptions rpcQosOptions =
        options.toBuilder().withBatchInitialCount(10).withBatchMaxCount(10).unsafeBuild();
    RpcQos qos =
        new RpcQosImpl(rpcQosOptions, random, sleeper, counterFactory, distributionFactory);

    preAttempt.accept(qos);

    RpcWriteAttempt attempt = qos.newWriteAttempt(RPC_ATTEMPT_CONTEXT);
    FlushBuffer<Element<Write>> accumulator = attempt.newFlushBuffer(monotonicClock.instant());
    for (int i = 0; i < batchCount; i++) {
      accumulator.offer(new WriteElement(i, newWrite(), window));
    }

    if (expectFlush) {
      assertTrue(accumulator.isFull());
      assertEquals(10, accumulator.getBufferedElementsCount());
    } else {
      assertFalse(accumulator.isFull());
      assertEquals(batchCount, accumulator.getBufferedElementsCount());
    }
  }

  private void doTest_writes_shouldFlush_numBytes(boolean expectFlush, long numBytes) {
    RpcQosOptions rpcQosOptions = options.toBuilder().withBatchMaxBytes(3000).unsafeBuild();
    RpcQos qos =
        new RpcQosImpl(rpcQosOptions, random, sleeper, counterFactory, distributionFactory);

    RpcWriteAttempt attempt = qos.newWriteAttempt(RPC_ATTEMPT_CONTEXT);
    FlushBuffer<Element<Write>> accumulator = attempt.newFlushBuffer(monotonicClock.instant());
    assertTrue(accumulator.offer(new FixedSerializationSize<>(newWrite(), numBytes)));

    assertEquals(expectFlush, accumulator.isFull());
    assertEquals(numBytes, accumulator.getBufferedElementsBytes());
    assertEquals(
        newArrayList(newWrite()),
        StreamSupport.stream(accumulator.spliterator(), false)
            .map(Element::getValue)
            .collect(Collectors.toList()));
  }

  private void doTest_initialBatchSizeRelativeToWorkerCount(
      int hintWorkerCount, int expectedBatchMaxCount) {
    RpcQosOptions options =
        RpcQosOptions.newBuilder()
            .withHintMaxNumWorkers(hintWorkerCount)
            .withBatchInitialCount(500)
            .build();
    RpcQosImpl qos = new RpcQosImpl(options, random, sleeper, counterFactory, distributionFactory);
    RpcWriteAttemptImpl attempt = qos.newWriteAttempt(RPC_ATTEMPT_CONTEXT);
    FlushBufferImpl<Element<Object>> buffer = attempt.newFlushBuffer(Instant.EPOCH);
    assertEquals(expectedBatchMaxCount, buffer.nextBatchMaxCount);
  }

  private void doTest_isCodeRetryable(Code code, boolean shouldBeRetryable) {
    RpcQosOptions options = RpcQosOptions.defaultOptions();
    RpcQosImpl qos = new RpcQosImpl(options, random, sleeper, counterFactory, distributionFactory);
    RpcWriteAttemptImpl attempt = qos.newWriteAttempt(RPC_ATTEMPT_CONTEXT);
    assertEquals(shouldBeRetryable, attempt.isCodeRetryable(code));
  }

  private static final class FixedSerializationSize<T> implements Element<T> {
    private final long serializedSize;
    private final T write;

    public FixedSerializationSize(T write, long serializedSize) {
      this.write = write;
      this.serializedSize = serializedSize;
    }

    @Override
    public T getValue() {
      return write;
    }

    @Override
    public long getSerializedSize() {
      return serializedSize;
    }

    @Override
    public String toString() {
      return "FixedSerializationSize{"
          + "serializedSize="
          + serializedSize
          + ", write="
          + write
          + '}';
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof FixedSerializationSize)) {
        return false;
      }
      FixedSerializationSize<?> that = (FixedSerializationSize<?>) o;
      return serializedSize == that.serializedSize && Objects.equals(write, that.write);
    }

    @Override
    public int hashCode() {
      return Objects.hash(serializedSize, write);
    }
  }
}
