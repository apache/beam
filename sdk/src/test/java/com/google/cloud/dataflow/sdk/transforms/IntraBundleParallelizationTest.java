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

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

import com.google.cloud.dataflow.sdk.runners.DirectPipeline;

import org.junit.Assert;
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
@SuppressWarnings("serial")
public class IntraBundleParallelizationTest {
  private static final int PARALLELISM_FACTOR = 16;
  private static final AtomicInteger numSuccesses = new AtomicInteger();
  private static final AtomicInteger numProcessed = new AtomicInteger();
  private static final AtomicInteger numFailures = new AtomicInteger();
  private static int concurrentElements = 0;
  private static int maxConcurrency = 0;

  @Before
  public void setUp() {
    numSuccesses.set(0);
    numProcessed.set(0);
    numFailures.set(0);
    concurrentElements = 0;
    maxConcurrency = 0;
  }

  /**
   * Introduces a delay in processing, then passes thru elements.
   */
  private static class DelayFn<T> extends DoFn<T, T> {
    public static final long DELAY_MS = 25;

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
    private ExceptionThrowingFn(int numSuccesses) {
      IntraBundleParallelizationTest.numSuccesses.set(numSuccesses);
    }

    @Override
    public void processElement(ProcessContext c) {
      numProcessed.incrementAndGet();
      if (numSuccesses.decrementAndGet() >= 0) {
        c.output(c.element());
        return;
      }

      numFailures.incrementAndGet();
      throw new RuntimeException("Expected failure");
    }
  }

  /**
   * Measures concurrency of the processElement method.
   */
  private static class ConcurrencyMeasuringFn<T> extends DoFn<T, T> {
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
  public void testParallelization() {
    long minDuration = Long.MAX_VALUE;
    // Take the minimum from multiple runs.
    for (int i = 0; i < 5; ++i) {
      minDuration = Math.min(minDuration,
          run(2 * PARALLELISM_FACTOR, PARALLELISM_FACTOR, new DelayFn<Integer>()));
    }

    // The minimum is guaranteed to be >= 2x the delay interval, since no more than half the
    // elements can be scheduled at once.
    Assert.assertThat(minDuration,
        greaterThanOrEqualTo(2 * DelayFn.DELAY_MS));
    // Also, it should take <= 8x the delay interval since we should be at least
    // parallelizing some of the work.
    Assert.assertThat(minDuration,
        lessThanOrEqualTo(8 * DelayFn.DELAY_MS));
  }

  @Test(timeout = 5000L)
  public void testExceptionHandling() {
    ExceptionThrowingFn<Integer> fn = new ExceptionThrowingFn<>(10);
    try {
      run(100, PARALLELISM_FACTOR, fn);
      Assert.fail("Expected exception to propagate");
    } catch (RuntimeException e) {
      Assert.assertThat(e.getMessage(), containsString("Expected failure"));
    }

    // Should have processed 10 elements, but stopped before processing all
    // of them.
    Assert.assertThat(numProcessed.get(),
        is(both(greaterThanOrEqualTo(10))
            .and(lessThan(100))));

    // The first failure should prevent the scheduling of any more elements.
    Assert.assertThat(numFailures.get(),
        is(both(greaterThanOrEqualTo(1))
            .and(lessThanOrEqualTo(PARALLELISM_FACTOR))));
  }

  @Test(timeout = 5000L)
  public void testExceptionHandlingOnLastElement() {
    ExceptionThrowingFn<Integer> fn = new ExceptionThrowingFn<>(9);
    try {
      run(10, PARALLELISM_FACTOR, fn);
      Assert.fail("Expected exception to propagate");
    } catch (RuntimeException e) {
      Assert.assertThat(e.getMessage(), containsString("Expected failure"));
    }

    // Should have processed 10 elements, but stopped before processing all
    // of them.
    Assert.assertEquals(10, numProcessed.get());
    Assert.assertEquals(1, numFailures.get());
  }

  private long run(int numElements, int maxParallelism, DoFn<Integer, Integer> doFn) {
    DirectPipeline p = DirectPipeline.createForTest();

    ArrayList<Integer> data = new ArrayList<>(numElements);
    for (int i = 0; i < numElements; ++i) {
      data.add(i);
    }

    ConcurrencyMeasuringFn<Integer> downstream = new ConcurrencyMeasuringFn<>();
    p
    .apply(Create.of(data))
    .apply(IntraBundleParallelization.of(doFn).withMaxParallelism(maxParallelism))
    .apply(ParDo.of(downstream));

    long startTime = System.currentTimeMillis();

    p.run();

    // Downstream methods should not see parallel threads.
    Assert.assertEquals(1, maxConcurrency);

    long endTime = System.currentTimeMillis();
    return endTime - startTime;
  }
}
