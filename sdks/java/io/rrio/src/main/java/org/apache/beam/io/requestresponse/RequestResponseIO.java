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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.List;
import java.util.Set;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Triple;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.Partition.PartitionFn;
import org.apache.beam.sdk.transforms.PeriodicImpulse;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
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
 * <pre>{@code  PCollection<SomeRequest> requests = ...
 *  Result result = requests.apply(RequestResponseIO.create(new MyCaller()));
 *  result.getResponses().apply( ... );
 *  result.getFailures().apply( ... );
 * }</pre>
 */
public class RequestResponseIO<RequestT, ResponseT>
    extends PTransform<PCollection<RequestT>, Result<ResponseT>> {

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

  /**
   * static final Strings below are {@link VisibleForTesting} to test composite pipeline
   * construction of {@link RequestResponseIO}.
   */
  @VisibleForTesting static final String ROOT_NAME = RequestResponseIO.class.getSimpleName();

  @VisibleForTesting static final String CALL_NAME = Call.class.getSimpleName();

  @VisibleForTesting static final String CACHE_READ_NAME = "CacheRead";

  @VisibleForTesting static final String CACHE_WRITE_NAME = "CacheWrite";

  @VisibleForTesting static final String THROTTLE_NAME = "Throttle";

  /**
   * {@link VisibleForTesting} to test {@link RequestResponseIO} composite transform construction
   * without exposing {@link WrappedAssociatingRequestResponseCaller}.
   */
  @VisibleForTesting
  static final String WRAPPED_CALLER = WrappedAssociatingRequestResponseCaller.class.getName();
  /**
   * The default {@link Duration} to wait until completion of user code. A {@link
   * UserCodeTimeoutException} is thrown when {@link Caller#call}, {@link SetupTeardown#setup}, or
   * {@link SetupTeardown#teardown} exceed this timeout.
   */
  public static final Duration DEFAULT_TIMEOUT = Duration.standardSeconds(30L);

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
   * are never held; no-op implemented {@link CallShouldBackoff#value} always returns {@code false}.
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
   * Configures {@link RequestResponseIO} to use a <a href="https://redis.io">Redis</a> cache to
   * configure {@link #withCacheRead} and {@link #withCacheWrite}, to read and write {@link
   * RequestT} and {@link ResponseT} pairs. The purpose of the cache is to offload {@link RequestT}s
   * from the API and instead return the {@link ResponseT} if the association is known. Since the
   * {@link RequestT}s and {@link ResponseT}s need encoding and decoding, checks are made whether
   * the requestTCoder and {@link Configuration#getResponseTCoder} are {@link
   * Coder#verifyDeterministic}.
   *
   * <pre>Below describes the parameters in more detail and their usage.</pre>
   *
   * <ul>
   *   <li>{@code URI uri} - the {@link URI} of the Redis instance.
   *   <li>{@code Coder<RequestT> requestTCoder} - the {@link RequestT} {@link Coder} to encode and
   *       decode {@link RequestT}s during cache read and writes.
   *   <li>{@code Duration expiry} - the duration to hold {@link RequestT} and {@link ResponseT}
   *       pairs in the cache.
   * </ul>
   */
  public RequestResponseIO<RequestT, ResponseT> withRedisCache(
      URI uri, Coder<RequestT> requestTCoder, Duration expiry) throws NonDeterministicException {

    requestTCoder.verifyDeterministic();
    rrioConfiguration.getResponseTCoder().verifyDeterministic();

    PTransform<PCollection<RequestT>, Result<KV<RequestT, @Nullable ResponseT>>> cacheRead =
        Cache.<RequestT, @Nullable ResponseT>readUsingRedis(
            new RedisClient(uri),
            requestTCoder,
            new CacheResponseCoder<>(rrioConfiguration.getResponseTCoder()));

    PTransform<PCollection<KV<RequestT, ResponseT>>, Result<KV<RequestT, ResponseT>>> cacheWrite =
        // Type arguments needed to resolve "error: [assignment] incompatible types in assignment."
        Cache.<RequestT, ResponseT>writeUsingRedis(
            expiry,
            new RedisClient(uri),
            requestTCoder,
            new CacheResponseCoder<>(rrioConfiguration.getResponseTCoder()));

    return withCacheRead(cacheRead).withCacheWrite(cacheWrite);
  }

  /**
   * Configures {@link RequestResponseIO} for reading {@link RequestT} and {@link ResponseT} pairs
   * from a cache. The transform {@link Flatten}s the {@link ResponseT} {@link PCollection} of
   * successful pairs with that resulting from API calls of {@link RequestT}s of unsuccessful pairs.
   * It is intended for this method and {@link #withCacheWrite} to be used together.
   */
  public RequestResponseIO<RequestT, ResponseT> withCacheRead(
      PTransform<PCollection<RequestT>, Result<KV<RequestT, @Nullable ResponseT>>> cacheRead) {
    return new RequestResponseIO<>(
        rrioConfiguration.toBuilder().setCacheRead(cacheRead).build(), callConfiguration);
  }

  /**
   * Configures {@link RequestResponseIO} for writing {@link RequestT} and {@link ResponseT} pairs
   * to a cache. The transform applies a result of {@link Call} to the {@link PTransform} supplied
   * by this method. It is intended for this method and {@link #withCacheRead} to be used together.
   */
  public RequestResponseIO<RequestT, ResponseT> withCacheWrite(
      PTransform<PCollection<KV<RequestT, ResponseT>>, Result<KV<RequestT, ResponseT>>>
          cacheWrite) {
    return new RequestResponseIO<>(
        rrioConfiguration.toBuilder().setCacheWrite(cacheWrite).build(), callConfiguration);
  }

  /**
   * Configures {@link RequestResponseIO} with a {@link #withPreventiveThrottle} using a <a
   * href="https://redis.io">Redis</a> cache. <strong>Take care not to mix up the {@code quotaKey}
   * from the {@code queueKey}.</strong> Additionally, usage of this should consider that the {@link
   * PTransform} uses {@link PeriodicImpulse} to fulfill its aim. <strong>Therefore, usage of this
   * method will convert a batch pipeline to a streaming one.</strong>
   *
   * <pre>The algorithm is as follows.</pre>
   *
   * <ol>
   *   <li>The transform queues {@link RequestT} elements into a Redis list identified by the {@code
   *       queueKey}
   *   <li>A refresher uses {@link PeriodicImpulse} to refresh a shared cached {@link
   *       Quota#getNumRequests} quota value, identified by the {@code quotaKey}, every {@link
   *       Quota#getInterval}.
   *   <li>Finally, a {@link DoFn} emits dequeued {@link RequestT} elements when there's available
   *       quota.
   * </ol>
   */
  public RequestResponseIO<RequestT, ResponseT> withPreventiveThrottleUsingRedis(
      URI uri, Coder<RequestT> requestTCoder, Quota quota, String quotaKey, String queueKey)
      throws NonDeterministicException {
    requestTCoder.verifyDeterministic();
    return withPreventiveThrottle(
        ThrottleWithExternalResource.usingRedis(uri, quotaKey, queueKey, quota, requestTCoder));
  }

  // TODO(damondouglas): implement after resolving https://github.com/apache/beam/issues/28932.
  //  public RequestResponseIO<RequestT, ResponseT> withPreventiveThrottleWithoutExternalResource()

  /**
   * Configures {@link RequestResponseIO} with a {@link PTransform} that holds back {@link
   * RequestT}s to prevent quota errors such as HTTP 429 or gRPC RESOURCE_EXHAUSTION errors.
   */
  public RequestResponseIO<RequestT, ResponseT> withPreventiveThrottle(
      PTransform<PCollection<RequestT>, Result<RequestT>> throttle) {
    return new RequestResponseIO<>(
        rrioConfiguration.toBuilder().setThrottle(throttle).build(), callConfiguration);
  }

  /** Configuration details for {@link RequestResponseIO}. */
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
     * if if no association persists in the cache.
     */
    abstract @Nullable PTransform<PCollection<RequestT>, Result<KV<RequestT, @Nullable ResponseT>>>
        getCacheRead();

    /** Writes {@link RequestT} and {@link ResponseT} associations to an optional cache. */
    abstract @Nullable PTransform<
            PCollection<KV<RequestT, ResponseT>>, Result<KV<RequestT, ResponseT>>>
        getCacheWrite();

    /** Throttles a {@link RequestT} {@link PCollection}. */
    abstract @Nullable PTransform<PCollection<RequestT>, Result<RequestT>> getThrottle();

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

      /** See {@link #getThrottle}. */
      abstract Builder<RequestT, ResponseT> setThrottle(
          PTransform<PCollection<RequestT>, Result<RequestT>> value);

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

    // Throttle the RequestT input PCollection.
    Pair<PCollection<RequestT>, PCollectionList<ApiIOError>> throttle =
        expandThrottle(input, failureList);
    input = throttle.getLeft();
    failureList = throttle.getRight();

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

    PCollectionList<KV<RequestT, ResponseT>> cacheReadList =
        cacheReadResult
            .getResponses()
            .apply(
                "PartitionCacheReads",
                Partition.of(PartitionCacheReadsFn.NUM_PARTITIONS, new PartitionCacheReadsFn<>()));

    input =
        cacheReadList
            .get(PartitionCacheReadsFn.NULL_PARTITION)
            .apply("UncachedRequests", Keys.create());

    responseList =
        responseList.and(
            cacheReadList
                .get(PartitionCacheReadsFn.NON_NULL_PARTITION)
                .apply("CachedResponses", Values.create()));

    failureList = failureList.and(cacheReadResult.getFailures());

    return Triple.of(input, responseList, failureList);
  }

  /**
   * Expands with {@link Configuration#getThrottle}, if available. Otherwise, returns a {@link Pair}
   * of original arguments.
   *
   * <pre>Algorithm is as follows:
   * <ol>
   *   <li>Applies throttle transform to {@link RequestT} {@link PCollection} input.</li>
   *   <li>Returns throttled {@link PCollection} of {@link RequestT} elements.</li>
   *   <li>Returns appended {@link PCollection} of {@link ApiIOError}s to the failureList.</li>
   * </ol></pre>
   */
  Pair<PCollection<RequestT>, PCollectionList<ApiIOError>> expandThrottle(
      PCollection<RequestT> input, PCollectionList<ApiIOError> failureList) {
    if (rrioConfiguration.getThrottle() == null) {
      return Pair.of(input, failureList);
    }

    Result<RequestT> throttleResult =
        input.apply(THROTTLE_NAME, checkStateNotNull(rrioConfiguration.getThrottle()));

    return Pair.of(throttleResult.getResponses(), failureList.and(throttleResult.getFailures()));
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
      Call<RequestT, ResponseT> call =
          Call.of(callConfiguration.getCaller(), rrioConfiguration.getResponseTCoder());
      if (callConfiguration.getSetupTeardown() != null) {
        call = call.withSetupTeardown(checkStateNotNull(callConfiguration.getSetupTeardown()));
      }
      Result<ResponseT> result = input.apply(CALL_NAME, call);
      return Pair.of(
          responseList.and(result.getResponses()), failureList.and(result.getFailures()));
    }

    // Wrap caller to associate RequestT with ResponseT as a KV.
    Caller<RequestT, KV<RequestT, ResponseT>> caller =
        new WrappedAssociatingRequestResponseCaller<>(callConfiguration.getCaller());
    Coder<KV<RequestT, ResponseT>> coder =
        KvCoder.of(input.getCoder(), rrioConfiguration.getResponseTCoder());
    Call<RequestT, KV<RequestT, ResponseT>> call = Call.of(caller, coder);
    if (callConfiguration.getSetupTeardown() != null) {
      call = call.withSetupTeardown(checkStateNotNull(callConfiguration.getSetupTeardown()));
    }

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

  private static class PartitionCacheReadsFn<RequestT, ResponseT>
      implements PartitionFn<KV<RequestT, ResponseT>> {
    private static final int NUM_PARTITIONS = 2;

    private static final int NON_NULL_PARTITION = 0;
    private static final int NULL_PARTITION = 1;

    @Override
    public int partitionFor(KV<RequestT, ResponseT> elem, int numPartitions) {
      if (checkStateNotNull(elem).getValue() != null) {
        return NON_NULL_PARTITION;
      }
      return NULL_PARTITION;
    }
  }

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

  /** Resolves checker error: incompatible argument for parameter ResponseT Coder. */
  private static class CacheResponseCoder<ResponseT> extends CustomCoder<@Nullable ResponseT> {
    private final NullableCoder<ResponseT> basis;

    private CacheResponseCoder(Coder<ResponseT> basis) {
      this.basis = NullableCoder.of(basis);
    }

    @Override
    public void encode(@Nullable ResponseT value, OutputStream outStream)
        throws CoderException, IOException {
      basis.encode(value, outStream);
    }

    @Override
    public @Nullable ResponseT decode(InputStream inStream) throws CoderException, IOException {
      return basis.decode(inStream);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return basis.getCoderArguments();
    }

    @Override
    public void verifyDeterministic()
        throws @UnknownKeyFor @NonNull @Initialized NonDeterministicException {
      basis.verifyDeterministic();
    }
  }
}
