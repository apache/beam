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

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Set;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Triple;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.Partition.PartitionFn;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.SerializableSupplier;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * {@link PTransform} for reading from and writing to Web APIs.
 *
 * <p>{@link RequestResponseIO} is recommended for interacting with external systems that offer RPCs
 * that execute relatively quickly and do not offer advance features to make RPC execution
 * efficient.
 *
 * <p>For systems that offer features for more efficient reading, for example, tracking progress of
 * RPCs, support for splitting RPCs (deduct two or more RPCs which when combined return the same
 * result), consider using the Apache Beam's `Splittable DoFn` interface instead.
 *
 * <h2>Basic Usage</h2>
 *
 * {@link RequestResponseIO} minimally requires implementing the {@link Caller} interface:
 *
 * <pre>{@code class MyCaller implements Caller<SomeRequest, SomeResponse> {
 *    public SomeResponse call(SomeRequest request) throws UserCodeExecutionException {
 *      // calls the API submitting SomeRequest payload and returning SomeResponse
 *    }
 * }}</pre>
 *
 * <p>Then provide {@link RequestResponseIO}'s {@link #of} method your {@link Caller}
 * implementation.
 *
 * <pre>{@code
 * Coder<SomeResponse> responseCoder = ...
 * PCollection<SomeRequest> requests = ...
 * Result result = requests.apply(RequestResponseIO.of(new MyCaller(), responseCoder));
 * result.getResponses().apply( ... );
 * result.getFailures().apply( ... );
 * }</pre>
 */
public class RequestResponseIO<RequestT, ResponseT>
    extends PTransform<PCollection<RequestT>, Result<ResponseT>> {

  /**
   * The default {@link Duration} to wait until completion of user code. A {@link
   * UserCodeTimeoutException} is thrown when {@link Caller#call}, {@link SetupTeardown#setup}, or
   * {@link SetupTeardown#teardown} exceed this timeout.
   */
  public static final Duration DEFAULT_TIMEOUT = Duration.standardSeconds(30L);

  /**
   * {@link Set} of {@link UserCodeExecutionException}s that warrant repeating. Not all errors
   * should be repeat execution such as bad or unauthenticated requests. However, certain errors
   * such as timeouts, remote system or quota exceed errors may not be related to the code but due
   * to the remote system and thus warrant repeating.
   */
  public static final Set<Class<? extends UserCodeExecutionException>> REPEATABLE_ERROR_TYPES =
      ImmutableSet.of(
          UserCodeRemoteSystemException.class,
          UserCodeTimeoutException.class,
          UserCodeQuotaException.class);

  private static final String CALL_NAME = Call.class.getSimpleName();

  private static final String CACHE_READ_NAME = "CacheRead";

  private static final String CACHE_WRITE_NAME = "CacheWrite";

  private final TupleTag<ResponseT> responseTag = new TupleTag<ResponseT>() {};
  private final TupleTag<ApiIOError> failureTag = new TupleTag<ApiIOError>() {};

  private final Configuration<RequestT, ResponseT> rrioConfiguration;
  private final Call.Configuration<RequestT, ResponseT> callConfiguration;

  private RequestResponseIO(
      Configuration<RequestT, ResponseT> rrioConfiguration,
      Call.Configuration<RequestT, ResponseT> callConfiguration) {
    this.rrioConfiguration = rrioConfiguration;
    this.callConfiguration = callConfiguration;
  }

  /**
   * Instantiates a {@link RequestResponseIO} with a {@link Caller} and a {@link ResponseT} {@link
   * Coder}. Checks for the {@link Caller}'s {@link SerializableUtils#ensureSerializable}
   * serializable errors.
   */
  public static <RequestT, ResponseT> RequestResponseIO<RequestT, ResponseT> of(
      Caller<RequestT, ResponseT> caller, Coder<ResponseT> responseTCoder) {

    caller = SerializableUtils.ensureSerializable(caller);

    return new RequestResponseIO<>(
            Configuration.<RequestT, ResponseT>builder().setResponseTCoder(responseTCoder).build(),
            Call.Configuration.<RequestT, ResponseT>builder()
                .setCaller(caller)
                .setResponseCoder(responseTCoder)
                .build())
        .withDefaults();
  }

  /**
   * Instantiates a {@link RequestResponseIO} with a {@link ResponseT} {@link Coder} and an
   * implementation of both the {@link Caller} and {@link SetupTeardown} interfaces. Checks {@link
   * SerializableUtils#ensureSerializable} serializable errors.
   */
  public static <
          RequestT,
          ResponseT,
          CallerSetupTeardownT extends Caller<RequestT, ResponseT> & SetupTeardown>
      RequestResponseIO<RequestT, ResponseT> ofCallerAndSetupTeardown(
          CallerSetupTeardownT implementsCallerAndSetupTeardown, Coder<ResponseT> responseTCoder) {

    implementsCallerAndSetupTeardown =
        SerializableUtils.ensureSerializable(implementsCallerAndSetupTeardown);

    return new RequestResponseIO<>(
            Configuration.<RequestT, ResponseT>builder().setResponseTCoder(responseTCoder).build(),
            Call.Configuration.<RequestT, ResponseT>builder()
                .setCaller(implementsCallerAndSetupTeardown)
                .setSetupTeardown(implementsCallerAndSetupTeardown)
                .setResponseCoder(responseTCoder)
                .build())
        .withDefaults();
  }

  private RequestResponseIO<RequestT, ResponseT> withDefaults() {
    return withTimeout(DEFAULT_TIMEOUT)
        .shouldRepeat(true)
        .withBackOffSupplier(new DefaultSerializableBackoffSupplier())
        .withSleeperSupplier((SerializableSupplier<Sleeper>) () -> Sleeper.DEFAULT)
        .withCallShouldBackoff(new CallShouldBackoffBasedOnRejectionProbability<>());
  }

  /**
   * Overrides the {@link #DEFAULT_TIMEOUT} expected timeout of all user custom code. If user custom
   * code exceeds this timeout, then a {@link UserCodeTimeoutException} is thrown. User custom code
   * may throw this exception prior to the configured timeout value on their own.
   */
  public RequestResponseIO<RequestT, ResponseT> withTimeout(Duration value) {
    return new RequestResponseIO<>(
        rrioConfiguration, callConfiguration.toBuilder().setTimeout(value).build());
  }

  /**
   * Turns off repeat invocations (default is on) of {@link SetupTeardown} and {@link Caller}, using
   * the {@link Repeater}, in the setting of {@link RequestResponseIO#REPEATABLE_ERROR_TYPES}.
   */
  public RequestResponseIO<RequestT, ResponseT> withoutRepeater() {
    return shouldRepeat(false);
  }

  private RequestResponseIO<RequestT, ResponseT> shouldRepeat(boolean value) {
    return new RequestResponseIO<>(
        rrioConfiguration, callConfiguration.toBuilder().setShouldRepeat(value).build());
  }

  /**
   * Overrides the private no-op implementation of {@link CallShouldBackoff} that determines whether
   * the {@link DoFn} should hold {@link RequestT}s. Without this configuration, {@link RequestT}s
   * are never held; no-op implemented {@link CallShouldBackoff#isTrue} always returns {@code
   * false}.
   */
  public RequestResponseIO<RequestT, ResponseT> withCallShouldBackoff(
      CallShouldBackoff<ResponseT> value) {
    return new RequestResponseIO<>(
        rrioConfiguration, callConfiguration.toBuilder().setCallShouldBackoff(value).build());
  }

  /**
   * Overrides the default {@link SerializableSupplier} of a {@link Sleeper} that pauses code
   * execution when user custom code throws a {@link RequestResponseIO#REPEATABLE_ERROR_TYPES}
   * {@link UserCodeExecutionException}. The default supplies with {@link Sleeper#DEFAULT}. The need
   * for a {@link SerializableSupplier} instead of setting this directly is that some
   * implementations of {@link Sleeper} may not be {@link Serializable}.
   */
  RequestResponseIO<RequestT, ResponseT> withSleeperSupplier(SerializableSupplier<Sleeper> value) {
    return new RequestResponseIO<>(
        rrioConfiguration, callConfiguration.toBuilder().setSleeperSupplier(value).build());
  }

  /**
   * Overrides the default {@link SerializableSupplier} of a {@link BackOff} that reports to a
   * {@link Sleeper} how long to pause execution. It reports a {@link BackOff#STOP} to stop
   * repeating invocation attempts. The default supplies with {@link FluentBackoff#DEFAULT}. The
   * need for a {@link SerializableSupplier} instead of setting this directly is that some {@link
   * BackOff} implementations, such as {@link FluentBackoff} are not {@link Serializable}.
   */
  RequestResponseIO<RequestT, ResponseT> withBackOffSupplier(SerializableSupplier<BackOff> value) {
    return new RequestResponseIO<>(
        rrioConfiguration, callConfiguration.toBuilder().setBackOffSupplier(value).build());
  }

  /**
   * Configures {@link RequestResponseIO} for reading and writing {@link RequestT} and {@link
   * ResponseT} pairs using a cache. {@link RequestResponseIO}, by default, does not interact with a
   * cache.
   *
   * <pre>When reading, the transform {@link Flatten}s the {@link ResponseT} {@link PCollection}
   * of successful pairs with that resulting from API calls of {@link RequestT}s of unsuccessful pairs.
   * </pre>
   *
   * <pre>When writing, the transform {@link Flatten}s the {@link ResponseT} {@link PCollection} of
   *    successful pairs with that resulting from API calls of {@link RequestT}s.
   * </pre>
   */
  public RequestResponseIO<RequestT, ResponseT> withCache(Cache.Pair<RequestT, ResponseT> pair) {
    return new RequestResponseIO<>(
        rrioConfiguration
            .toBuilder()
            .setCacheRead(pair.getRead())
            .setCacheWrite(pair.getWrite())
            .build(),
        callConfiguration);
  }

  public RequestResponseIO<RequestT, ResponseT> withMonitoringConfiguration(Monitoring value) {
    return new RequestResponseIO<>(
        rrioConfiguration, callConfiguration.toBuilder().setMonitoringConfiguration(value).build());
  }

  /** Exposes the transform's {@link Call.Configuration} for testing. */
  @VisibleForTesting
  Call.Configuration<RequestT, ResponseT> getCallConfiguration() {
    return callConfiguration;
  }

  /**
   * Configuration details for {@link RequestResponseIO}. Package-private as minimally required by
   * {@link AutoValue}.
   */
  @AutoValue
  abstract static class Configuration<RequestT, ResponseT> {

    static <RequestT, ResponseT> Builder<RequestT, ResponseT> builder() {
      return new AutoValue_RequestResponseIO_Configuration.Builder<>();
    }

    /**
     * Required by {@link Call} and applied to the resulting {@link ResponseT} {@link PCollection}.
     */
    abstract Coder<ResponseT> getResponseTCoder();

    /**
     * Reads a {@link RequestT} {@link PCollection} from an optional cache and returns a {@link KV}
     * {@link PCollection} of the original {@link RequestT}s and associated {@link ResponseT}, null
     * if no association persists in the cache.
     */
    abstract @Nullable PTransform<PCollection<RequestT>, Result<KV<RequestT, @Nullable ResponseT>>>
        getCacheRead();

    /** Writes {@link RequestT} and {@link ResponseT} associations to a cache. */
    abstract @Nullable PTransform<
            PCollection<KV<RequestT, ResponseT>>, Result<KV<RequestT, ResponseT>>>
        getCacheWrite();

    abstract Builder<RequestT, ResponseT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<RequestT, ResponseT> {

      /** See {@link #getResponseTCoder}. */
      abstract Builder<RequestT, ResponseT> setResponseTCoder(Coder<ResponseT> value);

      /** See {@link #getCacheRead}. */
      abstract Builder<RequestT, ResponseT> setCacheRead(
          PTransform<PCollection<RequestT>, Result<KV<RequestT, @Nullable ResponseT>>> value);

      /** See {@link #getCacheWrite}. */
      abstract Builder<RequestT, ResponseT> setCacheWrite(
          PTransform<PCollection<KV<RequestT, ResponseT>>, Result<KV<RequestT, ResponseT>>> value);

      abstract Configuration<RequestT, ResponseT> build();
    }
  }

  @Override
  public Result<ResponseT> expand(PCollection<RequestT> input) {

    // Initialize empty ResponseT and APIIOError PCollectionLists.
    PCollectionList<ResponseT> responseList = PCollectionList.empty(input.getPipeline());
    PCollectionList<ApiIOError> failureList = PCollectionList.empty(input.getPipeline());

    // Apply cache reads, if available, updating the input, responses, and failures.
    Triple<PCollection<RequestT>, PCollectionList<ResponseT>, PCollectionList<ApiIOError>>
        cacheRead = expandCacheRead(input, responseList, failureList);
    input = cacheRead.getLeft();
    responseList = cacheRead.getMiddle();
    failureList = cacheRead.getRight();

    // Invoke the Caller for each RequestT input and write associated RequestT and ResponseT to
    // the cache, if available.
    Pair<PCollectionList<ResponseT>, PCollectionList<ApiIOError>> call =
        expandCallWithOptionalCacheWrites(input, responseList, failureList);
    responseList = call.getLeft();
    failureList = call.getRight();

    // Flatten the responses and failures.
    PCollection<ResponseT> responses =
        responseList.apply("FlattenResponses", Flatten.pCollections());
    PCollection<ApiIOError> failures = failureList.apply("FlattenErrors", Flatten.pCollections());

    // Prepare and return final result.
    PCollectionTuple pct = PCollectionTuple.of(responseTag, responses).and(failureTag, failures);
    return Result.of(responses.getCoder(), responseTag, failureTag, pct);
  }

  /**
   * Expands with {@link Configuration#getCacheRead} if available. Otherwise, returns a {@link
   * Triple} of original arguments.
   *
   * <pre>Algorithm is as follows.
   * <ol>
   *   <li>Attempts reads of associated {@link RequestT}/{@link ResponseT} pairs in a cache.</li>
   *   <li>{@link Partition}s {@link RequestT} elements based on whether an associated {@link ResponseT} exists.</li>
   *   <li>Returns a new {@link RequestT} {@link PCollection} for elements not associated with a {@link ResponseT} in the cache.</li>
   *   <li>Returns a new {@link ResponseT} {@link PCollectionList} appending elements found associated with a {@link RequestT}.</li>
   *   <li>Returns a new {@link ApiIOError} {@link PCollectionList} with any errors encountered reading from the cache.</li>
   * </ol></pre>
   */
  Triple<PCollection<RequestT>, PCollectionList<ResponseT>, PCollectionList<ApiIOError>>
      expandCacheRead(
          PCollection<RequestT> input,
          PCollectionList<ResponseT> responseList,
          PCollectionList<ApiIOError> failureList) {
    if (rrioConfiguration.getCacheRead() == null) {
      return Triple.of(input, responseList, failureList);
    }
    Result<KV<RequestT, @Nullable ResponseT>> cacheReadResult =
        input.apply(CACHE_READ_NAME, checkStateNotNull(rrioConfiguration.getCacheRead()));

    // Partition KV PCollection into elements that have null or not null response values.
    PCollectionList<KV<RequestT, ResponseT>> cacheReadList =
        cacheReadResult
            .getResponses()
            .apply(
                "PartitionCacheReads",
                Partition.of(PartitionCacheReadsFn.NUM_PARTITIONS, new PartitionCacheReadsFn<>()));

    // Requests of null response values will be sent for Call downstream.
    input =
        cacheReadList
            .get(PartitionCacheReadsFn.NULL_PARTITION)
            .apply("UncachedRequests", Keys.create());

    // Responses of non-null values will be returned to the final RequestResponseIO output.
    responseList =
        responseList.and(
            cacheReadList
                .get(PartitionCacheReadsFn.NON_NULL_PARTITION)
                .apply("CachedResponses", Values.create()));

    // Append any failures to the final output.
    failureList = failureList.and(cacheReadResult.getFailures());

    return Triple.of(input, responseList, failureList);
  }

  /**
   * Expands with a {@link Call} and {@link Configuration#getCacheWrite}, if available.
   *
   * <pre>Algorithm is as follows:
   * <ol>
   *   <li>If {@link Configuration#getCacheWrite} not available, instantiates and applies a
   *   {@link Call} using original {@link Caller} and optionally
   *   {@link SetupTeardown}. </li>
   *   <li>Otherwise, wraps the original {@link Caller} using
   *   {@link WrappedAssociatingRequestResponseCaller} that preserves the association between the
   *   {@link RequestT} and its {@link ResponseT}.</li>
   *   <li>Applies the resulting {@link KV} of the {@link RequestT} and its {@link ResponseT}
   *   to the {@link Configuration#getCacheWrite}, returning appended failures to the failureList
   *   and appended responses to the responseList.</li>
   * </ol></pre>
   */
  Pair<PCollectionList<ResponseT>, PCollectionList<ApiIOError>> expandCallWithOptionalCacheWrites(
      PCollection<RequestT> input,
      PCollectionList<ResponseT> responseList,
      PCollectionList<ApiIOError> failureList) {

    if (rrioConfiguration.getCacheWrite() == null) {
      Call<RequestT, ResponseT> call = Call.of(callConfiguration);
      Result<ResponseT> result = input.apply(CALL_NAME, call);
      return Pair.of(
          responseList.and(result.getResponses()), failureList.and(result.getFailures()));
    }

    // Wrap caller to associate RequestT with ResponseT as a KV.
    Caller<RequestT, KV<RequestT, ResponseT>> caller =
        new WrappedAssociatingRequestResponseCaller<>(callConfiguration.getCaller());

    Coder<KV<RequestT, ResponseT>> coder =
        KvCoder.of(input.getCoder(), rrioConfiguration.getResponseTCoder());

    // Could not re-use original configuration because of different type parameters.
    Call.Configuration<RequestT, KV<RequestT, ResponseT>> configuration =
        Call.Configuration.<RequestT, KV<RequestT, ResponseT>>builder()
            .setResponseCoder(coder)
            .setCaller(caller)
            .setSetupTeardown(callConfiguration.getSetupTeardown())
            .setBackOffSupplier(callConfiguration.getBackOffSupplier())
            .setCallShouldBackoff(
                new WrappedAssociatingRequestResponseCallShouldBackoff<>(
                    callConfiguration.getCallShouldBackoff()))
            .setShouldRepeat(callConfiguration.getShouldRepeat())
            .setSleeperSupplier(callConfiguration.getSleeperSupplier())
            .setTimeout(callConfiguration.getTimeout())
            .build();

    Call<RequestT, KV<RequestT, ResponseT>> call = Call.of(configuration);

    // Extract ResponseT from KV; append failures.
    Result<KV<RequestT, ResponseT>> result = input.apply(CALL_NAME, call);
    PCollection<ResponseT> responses =
        result.getResponses().apply(CALL_NAME + "Responses", Values.create());
    responseList = responseList.and(responses);
    failureList = failureList.and(result.getFailures());

    // Write RequestT and ResponseT pairs to the cache; append failures.
    Result<KV<RequestT, ResponseT>> cacheWriteResult =
        result
            .getResponses()
            .apply(CACHE_WRITE_NAME, checkStateNotNull(rrioConfiguration.getCacheWrite()));
    failureList = failureList.and(cacheWriteResult.getFailures());

    return Pair.of(responseList, failureList);
  }

  /**
   * The {@link PartitionFn} used by {@link #expandCacheRead} to separate non-null {@link ResponseT}
   * value {@link KV}s from null ones. This is so that {@link RequestT}s associated with non-null
   * {@link ResponseT}s are not forwarded to the {@link Caller} and {@link PCollection} of the
   * associated {@link ResponseT}s flatten and merge with the final result output.
   */
  private static class PartitionCacheReadsFn<RequestT, ResponseT>
      implements PartitionFn<KV<RequestT, ResponseT>> {

    /** Used as input into {@link Partition#of}. */
    private static final int NUM_PARTITIONS = 2;

    /**
     * Prevents supplying the wrong index when calling {@link PCollectionList#get} for the
     * associated request and non-null response KVs.
     */
    private static final int NON_NULL_PARTITION = 0;

    /**
     * Prevents supplying the wrong index when calling {@link PCollectionList#get} for the
     * associated request and null response KVs.
     */
    private static final int NULL_PARTITION = 1;

    @Override
    public int partitionFor(KV<RequestT, ResponseT> elem, int numPartitions) {
      if (checkStateNotNull(elem).getValue() != null) {
        return NON_NULL_PARTITION;
      }
      return NULL_PARTITION;
    }
  }

  /**
   * Used by {@link #expandCallWithOptionalCacheWrites}, in the setting of a non-null {@link
   * Configuration#getCacheWrite}, to wrap the original {@link Caller} so that it returns a {@link
   * KV} of {@link RequestT}s and their associated {@link ResponseT}s for use as input into the
   * {@link Configuration#getCacheWrite} {@link PTransform}.
   */
  private static class WrappedAssociatingRequestResponseCaller<RequestT, ResponseT>
      implements Caller<RequestT, KV<RequestT, ResponseT>> {

    private final Caller<RequestT, ResponseT> caller;

    private WrappedAssociatingRequestResponseCaller(Caller<RequestT, ResponseT> caller) {
      this.caller = caller;
    }

    @Override
    public KV<RequestT, ResponseT> call(RequestT request) throws UserCodeExecutionException {
      ResponseT response = caller.call(request);
      return KV.of(request, response);
    }
  }

  /**
   * Required by {@link #expandCallWithOptionalCacheWrites}, in the setting of a non-null {@link
   * Configuration#getCacheWrite}, to match the signature of {@link CallShouldBackoff} with the
   * corresponding {@link WrappedAssociatingRequestResponseCaller}.
   */
  private static class WrappedAssociatingRequestResponseCallShouldBackoff<RequestT, ResponseT>
      implements CallShouldBackoff<KV<RequestT, ResponseT>> {
    private final CallShouldBackoff<ResponseT> basis;

    private WrappedAssociatingRequestResponseCallShouldBackoff(CallShouldBackoff<ResponseT> basis) {
      this.basis = basis;
    }

    @Override
    public void update(UserCodeExecutionException exception) {
      basis.update(exception);
    }

    @Override
    public void update(KV<RequestT, ResponseT> response) {
      basis.update(response.getValue());
    }

    @Override
    public boolean isTrue() {
      return basis.isTrue();
    }
  }
}
