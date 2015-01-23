/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.transforms;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

import com.google.cloud.dataflow.sdk.TestUtils;
import com.google.cloud.dataflow.sdk.runners.DirectPipeline;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for RateLimiter.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("serial")
public class RateLimitingTest {

  /**
   * Pass-thru function.
   */
  private static class IdentityFn<T> extends DoFn<T, T> {
    @Override
    public void processElement(ProcessContext c) {
      c.output(c.element());
    }
  }

  /**
   * Introduces a delay in processing, then passes thru elements.
   */
  private static class DelayFn<T> extends DoFn<T, T> {
    public static final long DELAY_MS = 250;

    @Override
    public void processElement(ProcessContext c) {
      try {
        Thread.sleep(DELAY_MS);
      } catch (InterruptedException e) {
        e.printStackTrace();
        throw new RuntimeException("Interrupted");
      }
      c.output(c.element());
    }
  }

  /**
   * Throws an exception after some number of calls.
   */
  private static class ExceptionThrowingFn<T> extends DoFn<T, T> {
    private final AtomicInteger numSuccesses;
    private final AtomicInteger numProcessed = new AtomicInteger();
    private final AtomicInteger numFailures = new AtomicInteger();

    private ExceptionThrowingFn(int numSuccesses) {
      this.numSuccesses = new AtomicInteger(numSuccesses);
    }

    @Override
    public void processElement(ProcessContext c) {
      numProcessed.incrementAndGet();
      if (numSuccesses.decrementAndGet() > 0) {
        c.output(c.element());
        return;
      }

      numFailures.incrementAndGet();
      throw new RuntimeException("Expected failure");
    }
  }

  /**
   * Measures concurrency of the processElement method.
   *
   * <p> Note: this only works when
   * {@link DirectPipelineRunner#testSerializability} is disabled, otherwise
   * the counters are not available after the run.
   */
  private static class ConcurrencyMeasuringFn<T> extends DoFn<T, T> {
    private int concurrentElements = 0;
    private int maxConcurrency = 0;

    @Override
    public void processElement(ProcessContext c) {
      synchronized (this) {
        concurrentElements++;
        if (concurrentElements > maxConcurrency) {
          maxConcurrency = concurrentElements;
        }
      }

      c.output(c.element());

      synchronized (this) {
        concurrentElements--;
      }
    }
  }

  @Test
  public void testRateLimitingMax() {
    int n = 10;
    double rate = 10.0;
    long duration = runWithRate(n, rate, new IdentityFn<Integer>());

    long perElementPause = (long) (1000L / rate);
    long minDuration = (n - 1) * perElementPause;
    Assert.assertThat(duration, greaterThanOrEqualTo(minDuration));
  }

  @Test(timeout = 5000L)
  public void testExceptionHandling() {
    ExceptionThrowingFn<Integer> fn = new ExceptionThrowingFn<>(10);
    try {
      runWithRate(100, 0.0, fn);
      Assert.fail("Expected exception to propagate");
    } catch (RuntimeException e) {
      Assert.assertThat(e.getMessage(), containsString("Expected failure"));
    }

    // Should have processed 10 elements, but stopped before processing all
    // of them.
    Assert.assertThat(fn.numProcessed.get(),
        is(both(greaterThanOrEqualTo(10))
            .and(lessThan(100))));

    // The first failure should prevent the scheduling of any more elements.
    Assert.assertThat(fn.numFailures.get(),
        is(both(greaterThanOrEqualTo(1))
            .and(lessThan(RateLimiting.DEFAULT_MAX_PARALLELISM))));
  }

  /**
   * Test exception handling on the last element to be processed.
   */
  @Test(timeout = 5000L)
  public void testExceptionHandling2() {
    ExceptionThrowingFn<Integer> fn = new ExceptionThrowingFn<>(10);
    try {
      runWithRate(10, 0.0, fn);
      Assert.fail("Expected exception to propagate");
    } catch (RuntimeException e) {
      Assert.assertThat(e.getMessage(), containsString("Expected failure"));
    }

    // Should have processed 10 elements, but stopped before processing all
    // of them.
    Assert.assertEquals(10, fn.numProcessed.get());
    Assert.assertEquals(1, fn.numFailures.get());
  }

  /**
   * Provides more elements than can be scheduled at once, testing that the
   * backlog limit is applied.
   */
  @Test
  public void testBacklogLimiter() {
    long duration = runWithRate(2 * RateLimiting.DEFAULT_MAX_PARALLELISM,
        -1.0 /* unlimited */, new DelayFn<Integer>());

    // Should take >= 2x the delay interval, since no more than half the
    // elements can be scheduled at once.
    Assert.assertThat(duration,
        greaterThanOrEqualTo(2 * DelayFn.DELAY_MS));
  }

  private long runWithRate(int numElements, double rateLimit,
      DoFn<Integer, Integer> doFn) {
    DirectPipeline p = DirectPipeline.createForTest();
    // Run with serializability testing disabled so that our tests can inspect
    // the DoFns after the test.
    p.getRunner().withSerializabilityTesting(false);

    ArrayList<Integer> data = new ArrayList<>(numElements);
    for (int i = 0; i < numElements; ++i) {
      data.add(i);
    }

    PCollection<Integer> input = TestUtils.createInts(p, data);

    ConcurrencyMeasuringFn<Integer> downstream = new ConcurrencyMeasuringFn<>();

    PCollection<Integer> output = input
        .apply(RateLimiting.perWorker(doFn)
            .withRateLimit(rateLimit))
        .apply(ParDo
            .of(downstream));

    long startTime = System.currentTimeMillis();

    DirectPipelineRunner.EvaluationResults results = p.run();

    // Downstream methods should not see parallel threads.
    Assert.assertEquals(1, downstream.maxConcurrency);

    long endTime = System.currentTimeMillis();
    return endTime - startTime;
  }
}
