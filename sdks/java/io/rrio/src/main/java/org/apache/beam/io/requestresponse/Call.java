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

import static org.apache.beam.io.requestresponse.Monitoring.incIfPresent;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.SerializableSupplier;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.joda.time.Duration;

/**
 * {@link Call} transforms a {@link RequestT} {@link PCollection} into a {@link ResponseT} {@link
 * PCollection} and {@link ApiIOError} {@link PCollection}, both wrapped in a {@link Result}.
 */
class Call<RequestT, ResponseT> extends PTransform<PCollection<RequestT>, Result<ResponseT>> {

  /**
   * Instantiates a {@link Call} {@link PTransform} with the required {@link Caller} and {@link
   * ResponseT} {@link Coder}. Checks for the {@link Caller}'s {@link
   * SerializableUtils#ensureSerializable} serializable errors.
   */
  static <RequestT, ResponseT> Call<RequestT, ResponseT> of(
      Caller<RequestT, ResponseT> caller, Coder<ResponseT> responseTCoder) {
    caller = SerializableUtils.ensureSerializable(caller);
    return new Call<>(
        Configuration.<RequestT, ResponseT>builder()
            .setCaller(caller)
            .setResponseCoder(responseTCoder)
            .build());
  }

  /**
   * Instantiates a {@link Call} {@link PTransform} with an implementation of both the {@link
   * Caller} and {@link SetupTeardown} in one class and the required {@link ResponseT} {@link
   * Coder}. Checks for {@link SerializableUtils#ensureSerializable} to report serializable errors.
   */
  static <
          RequestT,
          ResponseT,
          CallerSetupTeardownT extends Caller<RequestT, ResponseT> & SetupTeardown>
      Call<RequestT, ResponseT> ofCallerAndSetupTeardown(
          CallerSetupTeardownT implementsCallerAndSetupTeardown, Coder<ResponseT> responseTCoder) {
    implementsCallerAndSetupTeardown =
        SerializableUtils.ensureSerializable(implementsCallerAndSetupTeardown);
    return new Call<>(
        Configuration.<RequestT, ResponseT>builder()
            .setCaller(implementsCallerAndSetupTeardown)
            .setResponseCoder(responseTCoder)
            .setSetupTeardown(implementsCallerAndSetupTeardown)
            .build());
  }

  /** Instantiates a {@link Call} using the {@link Configuration}. */
  static <RequestT, ResponseT> Call<RequestT, ResponseT> of(
      Configuration<RequestT, ResponseT> configuration) {
    return new Call<>(configuration);
  }

  // TupleTags need to be instantiated for each Call instance. We cannot use a shared
  // static instance that is shared for multiple PCollectionTuples when Call is
  // instantiated multiple times as it is reused throughout code in this library.
  private final TupleTag<ResponseT> responseTag = new TupleTag<ResponseT>() {};
  private final TupleTag<ApiIOError> failureTag = new TupleTag<ApiIOError>() {};

  private final Configuration<RequestT, ResponseT> configuration;

  private Call(Configuration<RequestT, ResponseT> configuration) {
    this.configuration = SerializableUtils.ensureSerializable(configuration);
  }

  /**
   * Sets the {@link SetupTeardown} to the {@link Call} {@link PTransform} instance. Checks for
   * {@link SerializableUtils#ensureSerializable} serializable errors.
   */
  Call<RequestT, ResponseT> withSetupTeardown(SetupTeardown setupTeardown) {
    setupTeardown = SerializableUtils.ensureSerializable(setupTeardown);
    return new Call<>(configuration.toBuilder().setSetupTeardown(setupTeardown).build());
  }

  /**
   * Overrides the default {@link RequestResponseIO#DEFAULT_TIMEOUT}. A {@link
   * UserCodeTimeoutException} is thrown when {@link Caller#call}, {@link SetupTeardown#setup}, or
   * {@link SetupTeardown#teardown} exceed the timeout.
   */
  Call<RequestT, ResponseT> withTimeout(Duration timeout) {
    return new Call<>(configuration.toBuilder().setTimeout(timeout).build());
  }

  @Override
  public Result<ResponseT> expand(PCollection<RequestT> input) {

    PCollectionTuple pct =
        input.apply(
            CallFn.class.getSimpleName(),
            ParDo.of(new CallFn<>(responseTag, failureTag, configuration))
                .withOutputTags(responseTag, TupleTagList.of(failureTag)));

    return Result.of(configuration.getResponseCoder(), responseTag, failureTag, pct);
  }

  private static class CallFn<RequestT, ResponseT> extends DoFn<RequestT, ResponseT> {
    private final TupleTag<ResponseT> responseTag;
    private final TupleTag<ApiIOError> failureTag;
    private final CallerWithTimeout<RequestT, ResponseT> caller;
    private final SetupTeardownWithTimeout setupTeardown;
    private final Configuration<RequestT, ResponseT> configuration;
    private @MonotonicNonNull Counter requestsCounter = null;
    private @MonotonicNonNull Counter responsesCounter = null;
    private @MonotonicNonNull Counter failuresCounter = null;
    private @MonotonicNonNull Counter callCounter = null;
    private @MonotonicNonNull Counter setupCounter = null;
    private @MonotonicNonNull Counter teardownCounter = null;
    private @MonotonicNonNull Counter backoffCounter = null;
    private @MonotonicNonNull Counter sleeperCounter = null;
    private @MonotonicNonNull Counter shouldBackoffCounter = null;

    private transient @MonotonicNonNull ExecutorService executor;

    private CallFn(
        TupleTag<ResponseT> responseTag,
        TupleTag<ApiIOError> failureTag,
        Configuration<RequestT, ResponseT> configuration) {
      this.responseTag = responseTag;
      this.failureTag = failureTag;
      this.caller = new CallerWithTimeout<>(configuration.getTimeout(), configuration.getCaller());
      this.setupTeardown =
          new SetupTeardownWithTimeout(
              configuration.getTimeout(), configuration.getSetupTeardown());
      this.configuration = configuration;
    }

    private void setupMetrics() {
      Monitoring monitoring = configuration.getMonitoringConfiguration();
      if (monitoring.getCountRequests()) {
        requestsCounter = Metrics.counter(Call.class, Monitoring.REQUESTS_COUNTER_NAME);
      }
      if (monitoring.getCountResponses()) {
        responsesCounter = Metrics.counter(Call.class, Monitoring.RESPONSES_COUNTER_NAME);
      }
      if (monitoring.getCountFailures()) {
        failuresCounter = Metrics.counter(Call.class, Monitoring.FAILURES_COUNTER_NAME);
      }
      if (monitoring.getCountCalls()) {
        callCounter =
            Metrics.counter(Call.class, Monitoring.callCounterNameOf(configuration.getCaller()));
      }
      if (monitoring.getCountSetup()) {
        setupCounter =
            Metrics.counter(
                Call.class, Monitoring.setupCounterNameOf(configuration.getSetupTeardown()));
      }
      if (monitoring.getCountTeardown()) {
        teardownCounter =
            Metrics.counter(
                Call.class, Monitoring.teardownCounterNameOf(configuration.getSetupTeardown()));
      }
      if (monitoring.getCountBackoffs()) {
        backoffCounter =
            Metrics.counter(
                Call.class,
                Monitoring.backoffCounterNameOf(configuration.getBackOffSupplier().get()));
      }
      if (monitoring.getCountSleeps()) {
        sleeperCounter =
            Metrics.counter(
                Call.class,
                Monitoring.sleeperCounterNameOf(configuration.getSleeperSupplier().get()));
      }
      if (monitoring.getCountShouldBackoff()) {
        shouldBackoffCounter =
            Metrics.counter(
                Call.class,
                Monitoring.shouldBackoffCounterName(configuration.getCallShouldBackoff()));
      }
    }

    private void setupWithoutRepeat() throws UserCodeExecutionException {
      incIfPresent(setupCounter);
      this.setupTeardown.setup();
    }

    /**
     * Invokes {@link SetupTeardown#setup} forwarding its {@link UserCodeExecutionException}, if
     * thrown.
     */
    @Setup
    public void setup() throws UserCodeExecutionException {

      setupMetrics();

      this.executor = Executors.newSingleThreadExecutor();
      caller.setExecutor(executor);
      setupTeardown.setExecutor(executor);

      if (!this.configuration.getShouldRepeat()) {
        setupWithoutRepeat();
        return;
      }

      BackOff backOff = this.configuration.getBackOffSupplier().get();
      Sleeper sleeper = this.configuration.getSleeperSupplier().get();

      backoffIfNeeded(backOff, sleeper);

      Repeater<Void, Void> repeater =
          Repeater.<Void, Void>builder()
              .setBackOff(backOff)
              .setSleeper(sleeper)
              .setThrowableFunction(
                  ignored -> {
                    incIfPresent(setupCounter);
                    this.setupTeardown.setup();
                    return null;
                  })
              .build()
              .withBackoffCounter(backoffCounter)
              .withSleeperCounter(sleeperCounter);

      repeater.apply(null);
    }

    /**
     * Invokes {@link SetupTeardown#teardown} forwarding its {@link UserCodeExecutionException}, if
     * thrown.
     */
    @Teardown
    public void teardown() throws UserCodeExecutionException {
      BackOff backOff = configuration.getBackOffSupplier().get();
      Sleeper sleeper = configuration.getSleeperSupplier().get();

      backoffIfNeeded(backOff, sleeper);

      if (!configuration.getShouldRepeat()) {
        incIfPresent(teardownCounter);
        setupTeardown.teardown();
        return;
      }

      Repeater<Void, Void> repeater =
          Repeater.<Void, Void>builder()
              .setBackOff(backOff)
              .setSleeper(sleeper)
              .setThrowableFunction(
                  ignored -> {
                    incIfPresent(teardownCounter);
                    setupTeardown.teardown();
                    return null;
                  })
              .build()
              .withBackoffCounter(backoffCounter)
              .withSleeperCounter(sleeperCounter);

      repeater.apply(null);

      checkStateNotNull(executor).shutdown();
      try {
        boolean ignored = executor.awaitTermination(3L, TimeUnit.SECONDS);
      } catch (InterruptedException ignored) {
      }
    }

    @ProcessElement
    public void process(@Element RequestT request, MultiOutputReceiver receiver) {

      BackOff backOff = configuration.getBackOffSupplier().get();
      Sleeper sleeper = configuration.getSleeperSupplier().get();

      incIfPresent(requestsCounter);
      backoffIfNeeded(backOff, sleeper);

      if (!configuration.getShouldRepeat()) {
        incIfPresent(callCounter);
        try {
          // TODO(damondouglas): https://github.com/apache/beam/issues/29248
          ResponseT response = caller.call(request);
          receiver.get(responseTag).output(response);
          incIfPresent(responsesCounter);
        } catch (UserCodeExecutionException e) {
          incIfPresent(failuresCounter);
          receiver.get(failureTag).output(ApiIOError.of(e, request));
        }

        return;
      }

      Repeater<RequestT, ResponseT> repeater =
          Repeater.<RequestT, ResponseT>builder()
              .setSleeper(sleeper)
              .setBackOff(backOff)
              .setThrowableFunction(caller::call)
              .build()
              .withSleeperCounter(sleeperCounter)
              .withBackoffCounter(backoffCounter)
              .withCallCounter(callCounter);

      try {
        ResponseT response = repeater.apply(request);
        receiver.get(responseTag).output(response);
        incIfPresent(responsesCounter);
      } catch (UserCodeExecutionException e) {
        incIfPresent(failuresCounter);
        receiver.get(failureTag).output(ApiIOError.of(e, request));
      }
    }

    private void backoffIfNeeded(BackOff backOff, Sleeper sleeper) {
      if (configuration.getCallShouldBackoff().isTrue()) {
        incIfPresent(shouldBackoffCounter);
        incIfPresent(backoffCounter);
        try {
          incIfPresent(sleeperCounter);
          sleeper.sleep(backOff.nextBackOffMillis());
        } catch (InterruptedException ignored) {
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  /** Configuration details for {@link Call}. */
  @AutoValue
  abstract static class Configuration<RequestT, ResponseT> implements Serializable {

    static <RequestT, ResponseT> Builder<RequestT, ResponseT> builder() {
      return new AutoValue_Call_Configuration.Builder<RequestT, ResponseT>();
    }

    /** The user custom code that converts a {@link RequestT} into a {@link ResponseT}. */
    abstract Caller<RequestT, ResponseT> getCaller();

    /** The user custom code that implements setup and teardown methods. */
    abstract SetupTeardown getSetupTeardown();

    /**
     * The expected timeout of all user custom code. If user custom code exceeds this timeout, then
     * a {@link UserCodeTimeoutException} is thrown. User custom code may throw this exception prior
     * to the configured timeout value on their own.
     */
    abstract Duration getTimeout();

    /**
     * The {@link Coder} for the {@link ResponseT}. Note that the {@link RequestT}'s {@link Coder}
     * is derived from the input {@link PCollection} but can't be determined for the {@link
     * ResponseT} and therefore requires explicit setting in the {@link Configuration}.
     */
    abstract Coder<ResponseT> getResponseCoder();

    /**
     * Configures whether the {@link DoFn} should repeat {@link SetupTeardown} and {@link Caller}
     * invocations, using the {@link Repeater}, in the setting of {@link
     * RequestResponseIO#REPEATABLE_ERROR_TYPES}. Defaults to false.
     */
    abstract Boolean getShouldRepeat();

    /**
     * The {@link CallShouldBackoff} that determines whether the {@link DoFn} should hold {@link
     * RequestT}s. Defaults to a private no-op implementation; no {@link RequestT}s are held during
     * {@link ProcessElement}.
     */
    abstract CallShouldBackoff<ResponseT> getCallShouldBackoff();

    /**
     * The {@link SerializableSupplier} of a {@link Sleeper} that pauses code execution of when user
     * custom code throws a {@link RequestResponseIO#REPEATABLE_ERROR_TYPES} {@link
     * UserCodeExecutionException}. Supplies with {@link Sleeper#DEFAULT} by default. The need for a
     * {@link SerializableSupplier} instead of setting this directly is that some implementations of
     * {@link Sleeper} may not be {@link Serializable}.
     */
    abstract SerializableSupplier<Sleeper> getSleeperSupplier();

    /**
     * The {@link SerializableSupplier} of a {@link BackOff} that reports to a {@link Sleeper} how
     * long to pause execution. It reports a {@link BackOff#STOP} to stop repeating invocation
     * attempts. Supplies with {@link FluentBackoff#DEFAULT} by default. The need for a {@link
     * SerializableSupplier} instead of setting this directly is that some {@link BackOff}
     * implementations, such as {@link FluentBackoff} are not {@link Serializable}.
     */
    abstract SerializableSupplier<BackOff> getBackOffSupplier();

    abstract Monitoring getMonitoringConfiguration();

    abstract Builder<RequestT, ResponseT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<RequestT, ResponseT> {

      /** See {@link Configuration#getCaller}. */
      abstract Builder<RequestT, ResponseT> setCaller(Caller<RequestT, ResponseT> value);

      /** See {@link Configuration#getResponseCoder}. */
      abstract Builder<RequestT, ResponseT> setResponseCoder(Coder<ResponseT> value);

      /** See {@link Configuration#getSetupTeardown}. */
      abstract Builder<RequestT, ResponseT> setSetupTeardown(SetupTeardown value);

      abstract Optional<SetupTeardown> getSetupTeardown();

      /** See {@link Configuration#getTimeout}. */
      abstract Builder<RequestT, ResponseT> setTimeout(Duration value);

      abstract Optional<Duration> getTimeout();

      /** See {@link Configuration#getShouldRepeat}. */
      abstract Builder<RequestT, ResponseT> setShouldRepeat(Boolean value);

      abstract Optional<Boolean> getShouldRepeat();

      /** See {@link Configuration#getCallShouldBackoff}. */
      abstract Builder<RequestT, ResponseT> setCallShouldBackoff(
          CallShouldBackoff<ResponseT> value);

      abstract Optional<CallShouldBackoff<ResponseT>> getCallShouldBackoff();

      /** See {@link Configuration#getSleeperSupplier}. */
      abstract Builder<RequestT, ResponseT> setSleeperSupplier(SerializableSupplier<Sleeper> value);

      abstract Optional<SerializableSupplier<Sleeper>> getSleeperSupplier();

      /** See {@link Configuration#getBackOffSupplier}. */
      abstract Builder<RequestT, ResponseT> setBackOffSupplier(SerializableSupplier<BackOff> value);

      abstract Optional<SerializableSupplier<BackOff>> getBackOffSupplier();

      abstract Builder<RequestT, ResponseT> setMonitoringConfiguration(Monitoring value);

      abstract Optional<Monitoring> getMonitoringConfiguration();

      abstract Configuration<RequestT, ResponseT> autoBuild();

      final Configuration<RequestT, ResponseT> build() {
        if (!getSetupTeardown().isPresent()) {
          setSetupTeardown(new NoopSetupTeardown());
        }

        if (!getShouldRepeat().isPresent()) {
          setShouldRepeat(false);
        }

        if (!getTimeout().isPresent()) {
          setTimeout(RequestResponseIO.DEFAULT_TIMEOUT);
        }

        if (!getCallShouldBackoff().isPresent()) {
          setCallShouldBackoff(new NoopCallShouldBackoff<>());
        }

        if (!getSleeperSupplier().isPresent()) {
          setSleeperSupplier((SerializableSupplier<Sleeper>) () -> Sleeper.DEFAULT);
        }

        if (!getBackOffSupplier().isPresent()) {
          setBackOffSupplier(new DefaultSerializableBackoffSupplier());
        }

        if (!getMonitoringConfiguration().isPresent()) {
          setMonitoringConfiguration(Monitoring.builder().build());
        }

        return autoBuild();
      }
    }
  }

  static class NoopSetupTeardown implements SetupTeardown {

    @Override
    public void setup() throws UserCodeExecutionException {
      // Noop
    }

    @Override
    public void teardown() throws UserCodeExecutionException {
      // Noop
    }
  }

  private static class NoopCallShouldBackoff<ResponseT> implements CallShouldBackoff<ResponseT> {

    @Override
    public void update(UserCodeExecutionException exception) {
      // Noop
    }

    @Override
    public void update(ResponseT response) {
      // Noop
    }

    @Override
    public boolean isTrue() {
      return false;
    }
  }

  private static class CallerWithTimeout<RequestT, ResponseT>
      implements Caller<RequestT, ResponseT> {
    private final Duration timeout;
    private final Caller<RequestT, ResponseT> caller;
    private @MonotonicNonNull ExecutorService executor;

    private CallerWithTimeout(Duration timeout, Caller<RequestT, ResponseT> caller) {
      this.timeout = timeout;
      this.caller = caller;
    }

    private void setExecutor(ExecutorService executor) {
      this.executor = executor;
    }

    @Override
    public ResponseT call(RequestT request) throws UserCodeExecutionException {
      Future<ResponseT> future = checkStateNotNull(executor).submit(() -> caller.call(request));
      try {
        return future.get(timeout.getMillis(), TimeUnit.MILLISECONDS);
      } catch (TimeoutException | InterruptedException e) {
        throw new UserCodeTimeoutException(e);
      } catch (ExecutionException e) {
        parseAndThrow(future, e);
      }
      throw new UserCodeExecutionException("could not complete request");
    }
  }

  private static class SetupTeardownWithTimeout implements SetupTeardown {
    private final Duration timeout;
    private final SetupTeardown setupTeardown;
    private @MonotonicNonNull ExecutorService executor;

    SetupTeardownWithTimeout(Duration timeout, SetupTeardown setupTeardown) {
      this.timeout = timeout;
      this.setupTeardown = setupTeardown;
    }

    private void setExecutor(ExecutorService executor) {
      this.executor = executor;
    }

    @Override
    public void setup() throws UserCodeExecutionException {
      Callable<Void> callable =
          () -> {
            setupTeardown.setup();
            return null;
          };

      executeAsync(callable);
    }

    @Override
    public void teardown() throws UserCodeExecutionException {
      Callable<Void> callable =
          () -> {
            setupTeardown.teardown();
            return null;
          };

      executeAsync(callable);
    }

    private void executeAsync(Callable<Void> callable) throws UserCodeExecutionException {
      Future<Void> future = checkStateNotNull(executor).submit(callable);
      try {
        future.get(timeout.getMillis(), TimeUnit.MILLISECONDS);
      } catch (TimeoutException | InterruptedException e) {
        future.cancel(true);
        throw new UserCodeTimeoutException(e);
      } catch (ExecutionException e) {
        parseAndThrow(future, e);
      }
    }
  }

  private static <T> void parseAndThrow(Future<T> future, ExecutionException e)
      throws UserCodeExecutionException {
    future.cancel(true);
    if (e.getCause() == null) {
      throw new UserCodeExecutionException(e);
    }
    Throwable cause = checkStateNotNull(e.getCause());
    if (cause instanceof UserCodeQuotaException) {
      throw new UserCodeQuotaException(cause);
    }
    throw new UserCodeExecutionException(cause);
  }
}
