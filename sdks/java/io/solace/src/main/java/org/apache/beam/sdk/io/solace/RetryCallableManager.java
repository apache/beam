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

import com.google.api.core.NanoClock;
import com.google.api.gax.retrying.RetrySettings;
import com.google.auto.value.AutoValue;
import com.google.cloud.ExceptionHandler;
import com.google.cloud.ExceptionHandler.Interceptor;
import com.google.cloud.RetryHelper;
import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.Callable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;

/**
 * A class that manages retrying of callables based on the exceptions they throw.
 *
 * <p>This class provides a way to retry a callable if it throws an exception. The retry behavior is
 * configurable using {@link RetrySettings}.
 *
 * <p>This class is internal and should not be used directly.
 */
@Internal
@AutoValue
public abstract class RetryCallableManager implements Serializable {

  private static final int NUMBER_OF_RETRIES = 4;
  private static final int RETRY_INTERVAL_SECONDS = 1;
  private static final int RETRY_DELAY_MULTIPLIER = 2;
  private static final int MAX_DELAY_SECONDS =
      NUMBER_OF_RETRIES * RETRY_DELAY_MULTIPLIER * RETRY_INTERVAL_SECONDS + 1;

  /** Creates a new {@link RetryCallableManager} with default retry settings. */
  public static RetryCallableManager create() {
    return builder().build();
  }

  /**
   * Method that executes and repeats the execution of the callable argument, if it throws one of
   * the exceptions from the exceptionsToIntercept Set.
   *
   * @param callable The callable to execute.
   * @param exceptionsToIntercept The set of exceptions to intercept.
   * @param <V> The type of the callable's return value.
   * @return The return value of the callable.
   */
  public <V> V retryCallable(
      Callable<V> callable, Set<Class<? extends Exception>> exceptionsToIntercept) {
    return RetryHelper.runWithRetries(
        callable,
        getRetrySettings(),
        getExceptionHandlerForExceptions(exceptionsToIntercept),
        NanoClock.getDefaultClock());
  }

  private ExceptionHandler getExceptionHandlerForExceptions(
      Set<Class<? extends Exception>> exceptionsToIntercept) {
    return ExceptionHandler.newBuilder()
        .abortOn(RuntimeException.class)
        .addInterceptors(new ExceptionSetInterceptor(ImmutableSet.copyOf(exceptionsToIntercept)))
        .build();
  }

  abstract RetrySettings getRetrySettings();

  abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_RetryCallableManager.Builder()
        .setRetrySettings(
            RetrySettings.newBuilder()
                .setInitialRetryDelay(org.threeten.bp.Duration.ofSeconds(RETRY_INTERVAL_SECONDS))
                .setMaxAttempts(NUMBER_OF_RETRIES)
                .setMaxRetryDelay(org.threeten.bp.Duration.ofSeconds(MAX_DELAY_SECONDS))
                .setRetryDelayMultiplier(RETRY_DELAY_MULTIPLIER)
                .build());
  }

  @AutoValue.Builder
  public abstract static class Builder {
    abstract Builder setRetrySettings(RetrySettings retrySettings);

    abstract RetryCallableManager build();
  }

  private static class ExceptionSetInterceptor implements Interceptor {
    private static final long serialVersionUID = -8429573586820467828L;
    private final Set<Class<? extends Exception>> exceptionsToIntercept;

    public ExceptionSetInterceptor(Set<Class<? extends Exception>> exceptionsToIntercept) {
      this.exceptionsToIntercept = exceptionsToIntercept;
    }

    @Override
    public RetryResult afterEval(Exception exception, RetryResult retryResult) {
      return Interceptor.RetryResult.CONTINUE_EVALUATION;
    }

    @Override
    public RetryResult beforeEval(Exception exceptionToEvaluate) {
      for (Class<? extends Exception> exceptionToIntercept : exceptionsToIntercept) {
        if (isOf(exceptionToIntercept, exceptionToEvaluate)) {
          return Interceptor.RetryResult.RETRY;
        }
      }
      return Interceptor.RetryResult.CONTINUE_EVALUATION;
    }

    private boolean isOf(Class<?> clazz, Object obj) {
      return clazz.isInstance(obj);
    }
  }
}
