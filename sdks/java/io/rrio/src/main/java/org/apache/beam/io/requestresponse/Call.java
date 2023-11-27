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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.beam.io.requestresponse.Call.Result;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Duration;

/**
 * {@link Call} transforms a {@link RequestT} {@link PCollection} into a {@link ResponseT} {@link
 * PCollection} and {@link ApiIOError} {@link PCollection}, both wrapped in a {@link Result}.
 */
class Call<RequestT, ResponseT>
    extends PTransform<@NonNull PCollection<RequestT>, @NonNull Result<ResponseT>> {

  /**
   * The default {@link Duration} to wait until completion of user code. A {@link
   * UserCodeTimeoutException} is thrown when {@link Caller#call}, {@link SetupTeardown#setup}, or
   * {@link SetupTeardown#teardown} exceed this timeout.
   */
  static final Duration DEFAULT_TIMEOUT = Duration.standardSeconds(30L);

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

  // TupleTags need to be instantiated for each Call instance. We cannot use a shared
  // static instance that is shared for multiple PCollectionTuples when Call is
  // instantiated multiple times as it is reused throughout code in this library.
  private final TupleTag<ResponseT> responseTag = new TupleTag<ResponseT>() {};
  private final TupleTag<ApiIOError> failureTag = new TupleTag<ApiIOError>() {};

  private final Configuration<RequestT, ResponseT> configuration;

  private Call(Configuration<RequestT, ResponseT> configuration) {
    this.configuration = configuration;
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
   * Overrides the default {@link #DEFAULT_TIMEOUT}. A {@link UserCodeTimeoutException} is thrown
   * when {@link Caller#call}, {@link SetupTeardown#setup}, or {@link SetupTeardown#teardown} exceed
   * the timeout.
   */
  Call<RequestT, ResponseT> withTimeout(Duration timeout) {
    return new Call<>(configuration.toBuilder().setTimeout(timeout).build());
  }

  @Override
  public @NonNull Result<ResponseT> expand(PCollection<RequestT> input) {

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
    }

    /**
     * Invokes {@link SetupTeardown#setup} forwarding its {@link UserCodeExecutionException}, if
     * thrown.
     */
    @Setup
    public void setup() throws UserCodeExecutionException {
      this.executor = Executors.newSingleThreadExecutor();
      this.caller.setExecutor(executor);
      this.setupTeardown.setExecutor(executor);

      // TODO(damondouglas): Incorporate repeater when https://github.com/apache/beam/issues/28926
      //  resolves.
      this.setupTeardown.setup();
    }

    /**
     * Invokes {@link SetupTeardown#teardown} forwarding its {@link UserCodeExecutionException}, if
     * thrown.
     */
    @Teardown
    public void teardown() throws UserCodeExecutionException {
      // TODO(damondouglas): Incorporate repeater when https://github.com/apache/beam/issues/28926
      //  resolves.
      this.setupTeardown.teardown();
      checkStateNotNull(executor).shutdown();
      try {
        boolean ignored = executor.awaitTermination(3L, TimeUnit.SECONDS);
      } catch (InterruptedException ignored) {
      }
    }

    @ProcessElement
    public void process(@Element @NonNull RequestT request, MultiOutputReceiver receiver)
        throws JsonProcessingException {
      try {
        // TODO(damondouglas): https://github.com/apache/beam/issues/29248
        ResponseT response = this.caller.call(request);
        receiver.get(responseTag).output(response);
      } catch (UserCodeExecutionException e) {
        receiver.get(failureTag).output(ApiIOError.of(e, request));
      }
    }
  }

  /** Configuration details for {@link Call}. */
  @AutoValue
  abstract static class Configuration<RequestT, ResponseT> implements Serializable {

    static <RequestT, ResponseT> Builder<RequestT, ResponseT> builder() {
      return new AutoValue_Call_Configuration.Builder<>();
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

    abstract Builder<RequestT, ResponseT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<RequestT, ResponseT> {

      /** See {@link #getCaller()}. */
      abstract Builder<RequestT, ResponseT> setCaller(Caller<RequestT, ResponseT> value);

      /** See {@link #getSetupTeardown()}. */
      abstract Builder<RequestT, ResponseT> setSetupTeardown(SetupTeardown value);

      abstract Optional<SetupTeardown> getSetupTeardown();

      /** See {@link #getTimeout()}. */
      abstract Builder<RequestT, ResponseT> setTimeout(Duration value);

      abstract Optional<Duration> getTimeout();

      abstract Builder<RequestT, ResponseT> setResponseCoder(Coder<ResponseT> value);

      abstract Configuration<RequestT, ResponseT> autoBuild();

      final Configuration<RequestT, ResponseT> build() {
        if (!getSetupTeardown().isPresent()) {
          setSetupTeardown(new NoopSetupTeardown());
        }

        if (!getTimeout().isPresent()) {
          setTimeout(DEFAULT_TIMEOUT);
        }

        return autoBuild();
      }
    }
  }

  /**
   * The {@link Result} of processing request {@link PCollection} into response {@link PCollection}.
   */
  static class Result<ResponseT> implements POutput {

    static <ResponseT> Result<ResponseT> of(
        Coder<ResponseT> responseTCoder,
        TupleTag<ResponseT> responseTag,
        TupleTag<ApiIOError> failureTag,
        PCollectionTuple pct) {
      return new Result<>(responseTCoder, responseTag, pct, failureTag);
    }

    private final Pipeline pipeline;
    private final TupleTag<ResponseT> responseTag;
    private final TupleTag<ApiIOError> failureTag;
    private final PCollection<ResponseT> responses;
    private final PCollection<ApiIOError> failures;

    private Result(
        Coder<ResponseT> responseTCoder,
        TupleTag<ResponseT> responseTag,
        PCollectionTuple pct,
        TupleTag<ApiIOError> failureTag) {
      this.pipeline = pct.getPipeline();
      this.responseTag = responseTag;
      this.failureTag = failureTag;
      this.responses = pct.get(responseTag).setCoder(responseTCoder);
      this.failures = pct.get(this.failureTag);
    }

    public PCollection<ResponseT> getResponses() {
      return responses;
    }

    public PCollection<ApiIOError> getFailures() {
      return failures;
    }

    @Override
    public @NonNull Pipeline getPipeline() {
      return this.pipeline;
    }

    @Override
    public @NonNull Map<TupleTag<?>, PValue> expand() {
      return ImmutableMap.of(
          responseTag, responses,
          failureTag, failures);
    }

    @Override
    public void finishSpecifyingOutput(
        @NonNull String transformName,
        @NonNull PInput input,
        @NonNull PTransform<?, ?> transform) {}
  }

  private static class NoopSetupTeardown implements SetupTeardown {

    @Override
    public void setup() throws UserCodeExecutionException {
      // Noop
    }

    @Override
    public void teardown() throws UserCodeExecutionException {
      // Noop
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
