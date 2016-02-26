/*
 * Copyright (C) 2015 Google Inc.
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

import static com.google.cloud.dataflow.sdk.testing.SystemNanoTimeSleeper.sleepMillis;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for RateLimiter.
 */
@RunWith(JUnit4.class)
public class IntraBundleParallelizationTest {
  private static final int PARALLELISM_FACTOR = 16;
  private static final AtomicInteger numSuccesses = new AtomicInteger();
  private static final AtomicInteger numProcessed = new AtomicInteger();
  private static final AtomicInteger numFailures = new AtomicInteger();
  private static int concurrentElements = 0;
  private static int maxDownstreamConcurrency = 0;

  private static final AtomicInteger maxFnConcurrency = new AtomicInteger();
  private static final AtomicInteger currentFnConcurrency = new AtomicInteger();

  @Before
  public void setUp() {
    numSuccesses.set(0);
    numProcessed.set(0);
    numFailures.set(0);
    concurrentElements = 0;
    maxDownstreamConcurrency = 0;

    maxFnConcurrency.set(0);
    currentFnConcurrency.set(0);
  }

  /**
   * Introduces a delay in processing, then passes thru elements.
   */
  private static class DelayFn<T> extends DoFn<T, T> {
    public static final long DELAY_MS = 25;

    @Override
    public void processElement(ProcessContext c) {
      startConcurrentCall();
      try {
        sleepMillis(DELAY_MS);
      } catch (InterruptedException e) {
        e.printStackTrace();
        throw new RuntimeException("Interrupted");
      }
      c.output(c.element());
      finishConcurrentCall();
    }
  }

  /**
   * Throws an exception after some number of calls.
   */
  private static class ExceptionThrowingFn<T> extends DoFn<T, T> {
    private ExceptionThrowingFn(int numSuccesses) {
      IntraBundleParallelizationTest.numSuccesses.set(numSuccesses);
    }

    @Override
    public void processElement(ProcessContext c) {
      startConcurrentCall();
      try {
        numProcessed.incrementAndGet();
        if (numSuccesses.decrementAndGet() >= 0) {
          c.output(c.element());
          return;
        }

        numFailures.incrementAndGet();
        throw new RuntimeException("Expected failure");
      } finally {
        finishConcurrentCall();
      }
    }
  }

  /**
   * Measures concurrency of the processElement method.
   */
  private static class ConcurrencyMeasuringFn<T> extends DoFn<T, T> {
    @Override
    public void processElement(ProcessContext c) {
      // Synchronize on the class to provide synchronous access irrespective of
      // how this DoFn is called.
      synchronized (ConcurrencyMeasuringFn.class) {
        concurrentElements++;
        if (concurrentElements > maxDownstreamConcurrency) {
          maxDownstreamConcurrency = concurrentElements;
        }
      }

      c.output(c.element());

      synchronized (ConcurrencyMeasuringFn.class) {
        concurrentElements--;
      }
    }
  }

  private static void startConcurrentCall() {
    int currentlyExecuting = currentFnConcurrency.incrementAndGet();
    int maxConcurrency;
    do {
      maxConcurrency = maxFnConcurrency.get();
    } while (maxConcurrency < currentlyExecuting
        && !maxFnConcurrency.compareAndSet(maxConcurrency, currentlyExecuting));
  }

  private static void finishConcurrentCall() {
    currentFnConcurrency.decrementAndGet();
  }

  /**
   * Test that the DoFn is parallelized up the the Max Parallelism factor within a bundle, but not
   * greater than that amount.
   */
  @Test
  public void testParallelization() {
    int maxConcurrency = Integer.MIN_VALUE;
    // Take the minimum from multiple runs.
    for (int i = 0; i < 5; ++i) {
      maxConcurrency = Math.max(maxConcurrency,
          run(2 * PARALLELISM_FACTOR, PARALLELISM_FACTOR, new DelayFn<Integer>()));
    }

    // We should run at least some elements in parallel on some run
    assertThat(maxConcurrency,
        greaterThanOrEqualTo(2));
    // No run should execute more elements concurrency than the maximum concurrency allowed.
    assertThat(maxConcurrency,
        lessThanOrEqualTo(PARALLELISM_FACTOR));
  }

  @Test(timeout = 5000L)
  public void testExceptionHandling() {
    ExceptionThrowingFn<Integer> fn = new ExceptionThrowingFn<>(10);
    try {
      run(100, PARALLELISM_FACTOR, fn);
      fail("Expected exception to propagate");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), containsString("Expected failure"));
    }

    // Should have processed 10 elements, but stopped before processing all
    // of them.
    assertThat(numProcessed.get(),
        is(both(greaterThanOrEqualTo(10))
            .and(lessThan(100))));

    // The first failure should prevent the scheduling of any more elements.
    assertThat(numFailures.get(),
        is(both(greaterThanOrEqualTo(1))
            .and(lessThanOrEqualTo(PARALLELISM_FACTOR))));
  }

  @Test(timeout = 5000L)
  public void testExceptionHandlingOnLastElement() {
    ExceptionThrowingFn<Integer> fn = new ExceptionThrowingFn<>(9);
    try {
      run(10, PARALLELISM_FACTOR, fn);
      fail("Expected exception to propagate");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), containsString("Expected failure"));
    }

    // Should have processed 10 elements, but stopped before processing all
    // of them.
    assertEquals(10, numProcessed.get());
    assertEquals(1, numFailures.get());
  }

  @Test
  public void testIntraBundleParallelizationGetName() {
    assertEquals(
        "IntraBundleParallelization",
        IntraBundleParallelization.of(new DelayFn<Integer>()).withMaxParallelism(1).getName());
  }

  /**
   * Runs the provided doFn inside of an {@link IntraBundleParallelization} transform.
   *
   * <p>This method assumes that the DoFn passed to it will call {@link #startConcurrentCall()}
   * before processing each elements and {@link #finishConcurrentCall()} after each element.
   *
   * @param numElements the size of the input
   * @param maxParallelism how many threads to execute in parallel
   * @param doFn the DoFn to execute
   * @return the maximum observed parallelism of the DoFn
   */
  private int run(int numElements, int maxParallelism, DoFn<Integer, Integer> doFn) {
    Pipeline pipeline = TestPipeline.create();

    ArrayList<Integer> data = new ArrayList<>(numElements);
    for (int i = 0; i < numElements; ++i) {
      data.add(i);
    }

    ConcurrencyMeasuringFn<Integer> downstream = new ConcurrencyMeasuringFn<>();
    pipeline
        .apply(Create.of(data))
        .apply(IntraBundleParallelization.of(doFn).withMaxParallelism(maxParallelism))
        .apply(ParDo.of(downstream));

    pipeline.run();

    // All elements should have completed.
    assertEquals(0, currentFnConcurrency.get());
    // Downstream methods should not see parallel threads.
    assertEquals(1, maxDownstreamConcurrency);

    return maxFnConcurrency.get();
  }
}
