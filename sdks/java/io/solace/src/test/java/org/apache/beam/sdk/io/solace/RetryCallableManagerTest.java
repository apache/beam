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
package org.apache.beam.sdk.io.solace;

import static org.junit.Assert.assertEquals;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.RetryHelper.RetryHelperException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;

public class RetryCallableManagerTest {
  private static final int NUMBER_OF_RETRIES = 4;
  private static final int RETRY_INTERVAL_SECONDS = 0;
  private static final int RETRY_MULTIPLIER = 2;
  private static final int MAX_DELAY = 0;

  private RetryCallableManager retryCallableManager;

  @Before
  public void setUp() {
    retryCallableManager =
        RetryCallableManager.builder()
            .setRetrySettings(
                RetrySettings.newBuilder()
                    .setInitialRetryDelay(
                        org.threeten.bp.Duration.ofSeconds(RETRY_INTERVAL_SECONDS))
                    .setMaxAttempts(NUMBER_OF_RETRIES)
                    .setMaxRetryDelay(org.threeten.bp.Duration.ofSeconds(MAX_DELAY))
                    .setRetryDelayMultiplier(RETRY_MULTIPLIER)
                    .build())
            .build();
  }

  @Test
  public void testRetryCallable_ReturnsExpected() {
    AtomicInteger executeCounter = new AtomicInteger(0);
    Callable<Integer> incrementingFunction =
        () -> {
          executeCounter.incrementAndGet();
          if (executeCounter.get() < 2) {
            throw new MyException();
          }
          return executeCounter.get();
        };
    Integer result =
        retryCallableManager.retryCallable(
            incrementingFunction, ImmutableSet.of(MyException.class));
    assertEquals(String.format("Should return 2, instead returned %d.", result), 2, (int) result);
  }

  @Test
  public void testRetryCallable_RetriesExpectedNumberOfTimes() {
    AtomicInteger executeCounter = new AtomicInteger(0);
    Callable<Integer> incrementingFunction =
        () -> {
          executeCounter.incrementAndGet();
          if (executeCounter.get() < 2) {
            throw new MyException();
          }
          return executeCounter.get();
        };
    retryCallableManager.retryCallable(incrementingFunction, ImmutableSet.of(MyException.class));
    assertEquals(
        String.format("Should run 2 times, instead ran %d times.", executeCounter.get()),
        2,
        executeCounter.get());
  }

  @Test(expected = RetryHelperException.class)
  public void testRetryCallable_ThrowsRetryHelperException() {
    Callable<Integer> incrementingFunction =
        () -> {
          {
            throw new MyException();
          }
        };
    retryCallableManager.retryCallable(incrementingFunction, ImmutableSet.of(MyException.class));
  }

  @Test
  public void testRetryCallable_ExecutionCountIsCorrectAfterMultipleExceptions() {
    AtomicInteger executeCounter = new AtomicInteger(0);
    Callable<Integer> incrementingFunction =
        () -> {
          executeCounter.incrementAndGet();
          throw new MyException();
        };
    try {
      retryCallableManager.retryCallable(incrementingFunction, ImmutableSet.of(MyException.class));
    } catch (RetryHelperException e) {
      // ignore exception to check the executeCounter
    }
    assertEquals(
        String.format("Should execute 4 times, instead executed %d times", executeCounter.get()),
        4,
        executeCounter.get());
  }

  @Test(expected = RetryHelperException.class)
  public void testRetryCallable_ThrowsRetryHelperExceptionOnUnspecifiedException() {
    Callable<Integer> incrementingFunction =
        () -> {
          throw new DoNotIgnoreException();
        };
    retryCallableManager.retryCallable(incrementingFunction, ImmutableSet.of(MyException.class));
  }

  @Test
  public void testRetryCallable_ChecksForAllDefinedExceptions() {
    AtomicInteger executeCounter = new AtomicInteger(0);
    Callable<Integer> incrementingFunction =
        () -> {
          executeCounter.incrementAndGet();
          if (executeCounter.get() % 2 == 0) {
            throw new MyException();
          } else if (executeCounter.get() % 2 == 1) {
            throw new AnotherException();
          }
          return 0;
        };
    try {
      retryCallableManager.retryCallable(
          incrementingFunction, ImmutableSet.of(MyException.class, AnotherException.class));
    } catch (RetryHelperException e) {
      // ignore exception to check the executeCounter
    }
    assertEquals(
        String.format("Should execute 4 times, instead executed %d times", executeCounter.get()),
        4,
        executeCounter.get());
  }

  private static class MyException extends Exception {
    public MyException() {
      super();
    }
  }

  private static class AnotherException extends Exception {
    public AnotherException() {
      super();
    }
  }

  private static class DoNotIgnoreException extends Exception {
    public DoNotIgnoreException() {
      super();
    }
  }
}
