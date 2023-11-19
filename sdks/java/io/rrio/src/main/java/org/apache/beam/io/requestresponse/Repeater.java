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
package org.apache.beam.io.requestresponse;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

/** Repeats a method invocation when it encounters an error. */
public class Repeater<InputT, OutputT> {

  /**
   * {@link Set} of {@link UserCodeExecutionException}s that warrant repeating. A public modifier is
   * applied to communicate to users of this class which {@link UserCodeExecutionException}s
   * constitute warrant repeat execution.
   */
  public static final Set<Class<? extends UserCodeExecutionException>> REPEATABLE_ERROR_TYPES =
      ImmutableSet.of(
          UserCodeRemoteSystemException.class,
          UserCodeTimeoutException.class,
          UserCodeQuotaException.class);

  /** Instantiates a {@link Repeater}. */
  public static <InputT, OutputT> Repeater<InputT, OutputT> of(
      ThrowableFunction<InputT, OutputT> throwableFunction, Sleeper sleeper, Integer limit) {
    return new Repeater<>(throwableFunction, sleeper, limit);
  }

  private final ThrowableFunction<InputT, OutputT> throwableFunction;

  private final Sleeper sleeper;
  private final int limit;

  private Repeater(
      ThrowableFunction<InputT, OutputT> throwableFunction, Sleeper sleeper, int limit) {
    this.throwableFunction = throwableFunction;
    this.sleeper = sleeper;
    this.limit = limit;
  }

  /**
   * Applies the {@link InputT} to the {@link ThrowableFunction}. If the function throws an
   * exception that {@link #REPEATABLE_ERROR_TYPES} contains, repeats the invocation up to the
   * limit, otherwise throws immediately. Throws the last exception, if the limit reached.
   */
  public OutputT apply(InputT input) throws UserCodeExecutionException, InterruptedException {
    @MonotonicNonNull UserCodeExecutionException lastException = null;
    for (int i = 0; i < limit - 1; i++) {
      try {
        return throwableFunction.apply(input);
      } catch (UserCodeExecutionException e) {
        if (!REPEATABLE_ERROR_TYPES.contains(e.getClass())) {
          throw e;
        }
        lastException = e;
        sleeper.sleep();
      }
    }
    throw checkStateNotNull(lastException);
  }

  /**
   * A {@link FunctionalInterface} for executing a {@link UserCodeExecutionException} throwable
   * function.
   */
  @FunctionalInterface
  public interface ThrowableFunction<InputT, OutputT> {
    /** Returns the result of invoking this function on the given input. */
    OutputT apply(InputT input) throws UserCodeExecutionException;
  }

  /** Interfaces implementation details for pausing an execution. */
  public interface Sleeper {

    /** Pauses the execution. */
    void sleep() throws InterruptedException;
  }

  /**
   * A {@link Sleeper} implementation that uses a {@link BackOff} to determine how long to pause
   * execution.
   */
  public static class DefaultSleeper implements Sleeper {

    public static DefaultSleeper of() {
      return of(new ExponentialBackOff());
    }

    public static DefaultSleeper of(BackOff backOff) {
      return new DefaultSleeper(backOff);
    }

    private final BackOff backOff;

    private DefaultSleeper(BackOff backOff) {
      this.backOff = backOff;
    }

    @Override
    public void sleep() throws InterruptedException {
      ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
      try {
        Future<?> future =
            executorService.schedule(() -> {}, backOff.nextBackOffMillis(), TimeUnit.MILLISECONDS);
        future.get();
      } catch (IOException | ExecutionException e) {
        throw new RuntimeException(e);
      }
      executorService.shutdown();
      boolean ignored = executorService.awaitTermination(1L, TimeUnit.SECONDS);
    }
  }
}
