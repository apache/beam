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

import static org.joda.time.Duration.ZERO;
import static org.joda.time.Duration.millis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Random;
import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcAttempt.Context;
import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcWriteAttempt.Element;
import org.apache.beam.sdk.io.gcp.firestore.RpcQosImpl.FlushBufferImpl;
import org.apache.beam.sdk.io.gcp.firestore.RpcQosImpl.RpcWriteAttemptImpl;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.util.Sleeper;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings(
    "initialization.fields.uninitialized") // mockito fields are initialized via the Mockito Runner
public final class RpcQosSimulationTest {

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

  @Rule public final TestName testName = new TestName();

  // should not be static, important to reinitialize for each test
  private final Random random =
      new Random(1234567890); // fix the seed so we have deterministic tests

  private Context rpcAttemptContext;

  @Before
  public void setUp() {
    rpcAttemptContext =
        () -> String.format("%s.%s", this.getClass().getName(), testName.getMethodName());
    when(counterFactory.get(rpcAttemptContext.getNamespace(), "throttlingMs"))
        .thenReturn(counterThrottlingMs);
    when(counterFactory.get(rpcAttemptContext.getNamespace(), "rpc_failures"))
        .thenReturn(counterRpcFailures);
    when(counterFactory.get(rpcAttemptContext.getNamespace(), "rpc_successes"))
        .thenReturn(counterRpcSuccesses);
    when(counterFactory.get(rpcAttemptContext.getNamespace(), "rpc_streamValueReceived"))
        .thenReturn(counterRpcStreamValueReceived);
    when(distributionFactory.get(any(), any()))
        .thenAnswer(
            invocation -> mock(Distribution.class, invocation.getArgument(1, String.class)));
  }

  @Test
  public void writeRampUp_shouldScaleAlongTheExpectedLine() throws InterruptedException {
    RpcQosOptions options =
        RpcQosOptions.newBuilder()
            .withHintMaxNumWorkers(1)
            .withThrottleDuration(Duration.standardSeconds(5))
            .withBatchInitialCount(200)
            // in the test we're jumping ahead by 5 minutes at multiple points, so the default 2
            // minute
            // history for the adaptive throttler empties and the batch count drops back to the
            // initial value. Increase the sample period to 10 minutes to ease the amount of state
            // tracking that needs to be taken into account for calculating the ramp up values being
            // asserted.
            .withSamplePeriod(Duration.standardMinutes(10))
            .withSamplePeriodBucketSize(Duration.standardMinutes(2))
            .build();

    RpcQosImpl qos = new RpcQosImpl(options, random, sleeper, counterFactory, distributionFactory);
    /*
    Ramp up budgets for 0 -> 90 minutes
      1970-01-01T00:00:00.000Z ->     500
      1970-01-01T00:05:00.000Z ->     500
      1970-01-01T00:10:00.000Z ->     750
      1970-01-01T00:15:00.000Z ->   1,125
      1970-01-01T00:20:00.000Z ->   1,687
      1970-01-01T00:25:00.000Z ->   2,531
      1970-01-01T00:30:00.000Z ->   3,796
      1970-01-01T00:35:00.000Z ->   5,695
      1970-01-01T00:40:00.000Z ->   8,542
      1970-01-01T00:45:00.000Z ->  12,814
      1970-01-01T00:50:00.000Z ->  19,221
      1970-01-01T00:55:00.000Z ->  28,832
      1970-01-01T01:00:00.000Z ->  43,248
      1970-01-01T01:05:00.000Z ->  64,873
      1970-01-01T01:10:00.000Z ->  97,309
      1970-01-01T01:15:00.000Z -> 145,964
      1970-01-01T01:20:00.000Z -> 218,946
      1970-01-01T01:25:00.000Z -> 328,420
      1970-01-01T01:30:00.000Z -> 492,630
    */
    // timeline values (names are of the format t'minute'm'seconds's'millis'ms)
    Instant t00m00s000ms = t(ZERO);
    Instant t00m00s010ms = t(millis(10));
    Instant t00m00s020ms = t(millis(20));
    Instant t00m00s999ms = t(millis(999));

    Instant t00m01s000ms = t(seconds(1));
    Instant t00m01s001ms = t(seconds(1), millis(1));

    Instant t05m00s000ms = t(minutes(5));
    Instant t05m00s001ms = t(minutes(5), millis(1));
    Instant t05m00s002ms = t(minutes(5), millis(2));

    Instant t10m00s000ms = t(minutes(10));
    Instant t10m00s001ms = t(minutes(10), millis(1));
    Instant t10m00s002ms = t(minutes(10), millis(2));

    Instant t15m00s000ms = t(minutes(15), millis(1));

    Instant t20m00s000ms = t(minutes(20));
    Instant t20m01s000ms = t(minutes(20), seconds(1));

    safeToProceedAndWithBudgetAndWrite(qos, t00m00s000ms, 200, 200, "write 200");
    safeToProceedAndWithBudgetAndWrite(qos, t00m00s010ms, 300, 300, "write 300");
    unsafeToProceed(qos, t00m00s020ms);
    unsafeToProceed(qos, t00m00s999ms);
    safeToProceedAndWithBudgetAndWrite(
        qos, t00m01s000ms, 500, 500, "wait 1 second for budget to refill, write 500");
    unsafeToProceed(qos, t00m01s001ms);
    safeToProceedAndWithBudgetAndWrite(
        qos, t05m00s000ms, 500, 100, "jump ahead to next ramp up interval and write 100");
    safeToProceedAndWithBudgetAndWrite(
        qos, t05m00s001ms, 400, 400, "write another 400 exhausting budget");
    unsafeToProceed(qos, t05m00s002ms);
    safeToProceedAndWithBudgetAndWrite(
        qos,
        t10m00s000ms,
        500,
        500,
        "after 10 minutes the ramp up should allow 750 writes, write 500");
    safeToProceedAndWithBudgetAndWrite(qos, t10m00s001ms, 250, 250, "write 250 more");
    unsafeToProceed(qos, t10m00s002ms);
    safeToProceedAndWithBudgetAndWrite(
        qos,
        t15m00s000ms,
        500,
        500,
        "after 15 minutes the ramp up should allow 1,125 writes, write 500");
    safeToProceedAndWithBudgetAndWrite(qos, t15m00s000ms, 500, 500, "write 500 more");
    safeToProceedAndWithBudgetAndWrite(qos, t15m00s000ms, 125, 125, "write 125 more");
    unsafeToProceed(qos, t15m00s000ms);
    safeToProceedAndWithBudgetAndWrite(
        qos,
        t20m00s000ms,
        500,
        500,
        "after 20 minutes the ramp up should allow 1,687 writes, write 500");
    safeToProceedAndWithBudgetAndWrite(qos, t20m00s000ms, 500, 500, "write 500 more");
    safeToProceedAndWithBudgetAndWrite(qos, t20m00s000ms, 500, 500, "write 500 more");
    safeToProceedAndWithBudgetAndWrite(qos, t20m00s000ms, 187, 187, "write 125 more");
    unsafeToProceed(qos, t20m00s000ms);
    safeToProceedAndWithBudgetAndWrite(
        qos, t20m01s000ms, 500, 500, "wait 1 second for the budget to refill, write 500");
    safeToProceedAndWithBudgetAndWrite(qos, t20m01s000ms, 500, 500, "write 500 more");
    safeToProceedAndWithBudgetAndWrite(qos, t20m01s000ms, 500, 500, "write 500 more");
    safeToProceedAndWithBudgetAndWrite(qos, t20m01s000ms, 187, 187, "write 125 more");
    unsafeToProceed(qos, t20m01s000ms);
  }

  private static Instant t(Duration d) {
    return Instant.ofEpochMilli(d.getMillis());
  }

  private static Instant t(Duration... ds) {
    Duration sum = Arrays.stream(ds).reduce(Duration.ZERO, Duration::plus, Duration::plus);
    return t(sum);
  }

  private static Duration minutes(int i) {
    return Duration.standardMinutes(i);
  }

  private static Duration seconds(int i) {
    return Duration.standardSeconds(i);
  }

  private void unsafeToProceed(RpcQosImpl qos, Instant t) throws InterruptedException {
    RpcWriteAttemptImpl attempt = qos.newWriteAttempt(rpcAttemptContext);
    assertFalse(
        msg("verify budget depleted", t, "awaitSafeToProceed was true, expected false"),
        attempt.awaitSafeToProceed(t));
  }

  private void safeToProceedAndWithBudgetAndWrite(
      RpcQosImpl qos, Instant t, int expectedBatchMaxCount, int writeCount, String description)
      throws InterruptedException {
    RpcWriteAttemptImpl attempt = qos.newWriteAttempt(rpcAttemptContext);
    assertTrue(
        msg(description, t, "awaitSafeToProceed was false, expected true"),
        attempt.awaitSafeToProceed(t));
    FlushBufferImpl<Element<Object>> buffer = attempt.newFlushBuffer(t);
    assertEquals(
        msg(description, t, "unexpected batchMaxCount"),
        expectedBatchMaxCount,
        buffer.nextBatchMaxCount);
    attempt.recordRequestStart(t, writeCount);
    attempt.recordWriteCounts(t, writeCount, 0);
  }

  private static String msg(String description, Instant t, String message) {
    return String.format("[%s @ t = %s] %s", description, t, message);
  }
}
