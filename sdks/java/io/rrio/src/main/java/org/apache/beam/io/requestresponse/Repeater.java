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

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;

/**
 * Repeats a method invocation when it encounters an error, pausing invocations using {@link
 * Sleeper} for a {@link BackOff#nextBackOffMillis}.
 */
@AutoValue
abstract class Repeater<InputT, OutputT> {

  /** {@link Set} of {@link UserCodeExecutionException}s that warrant repeating. */
  static final Set<Class<? extends UserCodeExecutionException>> REPEATABLE_ERROR_TYPES =
      ImmutableSet.of(
          UserCodeRemoteSystemException.class,
          UserCodeTimeoutException.class,
          UserCodeQuotaException.class);

  static <InputT, OutputT> Builder<InputT, OutputT> builder() {
    return new AutoValue_Repeater.Builder<>();
  }

  /**
   * The {@link ThrowableFunction} to invoke repeatedly until it succeeds, throws a {@link
   * UserCodeExecutionException} that is not {@link #REPEATABLE_ERROR_TYPES}, or {@link
   * BackOff#STOP}.
   */
  abstract ThrowableFunction<InputT, OutputT> getThrowableFunction();

  /**
   * The {@link Sleeper} that pauses execution of the {@link #getThrowableFunction} when it throws a
   * {@link #REPEATABLE_ERROR_TYPES} {@link UserCodeExecutionException}. Uses {@link
   * Sleeper#DEFAULT} by default.
   */
  abstract Sleeper getSleeper();

  /**
   * The {@link BackOff} that reports to {@link #getSleeper} how long to pause execution. It reports
   * a {@link BackOff#STOP} to stop repeating invocation attempts. Uses {@link
   * FluentBackoff#DEFAULT#getBackOff} by default.
   */
  abstract BackOff getBackOff();

  /**
   * Applies the {@link InputT} to the {@link ThrowableFunction}, returning the {@link OutputT} if
   * successful. If the function throws an exception that {@link #REPEATABLE_ERROR_TYPES} contains,
   * repeats the invocation after {@link Sleeper#sleep} for the amount of time reported by {@link
   * BackOff#nextBackOffMillis}. Throws the latest encountered {@link UserCodeExecutionException}
   * when {@link BackOff} reports a {@link BackOff#STOP}.
   */
  OutputT apply(InputT input) throws UserCodeExecutionException {
    Optional<UserCodeExecutionException> latestError = Optional.empty();
    long waitFor = 0L;
    while (waitFor != BackOff.STOP) {
      try {
        getSleeper().sleep(waitFor);
        return getThrowableFunction().apply(input);
      } catch (UserCodeExecutionException e) {
        if (!REPEATABLE_ERROR_TYPES.contains(e.getClass())) {
          throw e;
        }
        latestError = Optional.of(e);
      } catch (InterruptedException ignored) {
      }
      try {
        waitFor = getBackOff().nextBackOffMillis();
      } catch (IOException e) {
        throw new UserCodeExecutionException(e);
      }
    }
    throw latestError.orElse(
        new UserCodeExecutionException("failed to process for input: " + input));
  }

  /**
   * A {@link FunctionalInterface} for executing a {@link UserCodeExecutionException} throwable
   * function.
   */
  @FunctionalInterface
  interface ThrowableFunction<InputT, OutputT> {
    /** Returns the result of invoking this function on the given input. */
    OutputT apply(InputT input) throws UserCodeExecutionException;
  }

  @AutoValue.Builder
  abstract static class Builder<InputT, OutputT> {

    /** See {@link #getThrowableFunction}. */
    abstract Builder<InputT, OutputT> setThrowableFunction(
        ThrowableFunction<InputT, OutputT> value);

    /** See {@link #getSleeper}. */
    abstract Builder<InputT, OutputT> setSleeper(Sleeper value);

    abstract Optional<Sleeper> getSleeper();

    /** See {@link #getBackOff}. */
    abstract Builder<InputT, OutputT> setBackOff(BackOff value);

    abstract Optional<BackOff> getBackOff();

    abstract Repeater<InputT, OutputT> autoBuild();

    final Repeater<InputT, OutputT> build() {
      if (!getSleeper().isPresent()) {
        setSleeper(Sleeper.DEFAULT);
      }

      if (!getBackOff().isPresent()) {
        setBackOff(FluentBackoff.DEFAULT.backoff());
      }

      return autoBuild();
    }
  }
}
