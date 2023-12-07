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
import java.net.URI;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Triple;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.Partition.PartitionFn;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
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
 * <pre>{@code  PCollection<SomeRequest> requests = ...
 *  Result result = requests.apply(RequestResponseIO.create(new MyCaller()));
 *  result.getResponses().apply( ... );
 *  result.getFailures().apply( ... );
 * }</pre>
 */
public class RequestResponseIO<RequestT, ResponseT>
    extends PTransform<PCollection<RequestT>, Result<ResponseT>> {

  private final TupleTag<ResponseT> responseTag = new TupleTag<ResponseT>() {};
  private final TupleTag<ApiIOError> failureTag = new TupleTag<ApiIOError>() {};

  private final Configuration<RequestT, ResponseT> configuration;

  private RequestResponseIO(Configuration<RequestT, ResponseT> configuration) {
    this.configuration = configuration;
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
        Configuration.<RequestT, ResponseT>builder()
            .setCaller(caller)
            .setResponseTCoder(responseTCoder)
            .build());
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
    return new RequestResponseIO<>(
        Configuration.<RequestT, ResponseT>builder()
            .setCaller(implementsCallerAndSetupTeardown)
            .setResponseTCoder(responseTCoder)
            .setSetupTeardown(implementsCallerAndSetupTeardown)
            .build());
  }

  /**
   * Configures {@link RequestResponseIO} to use a <a href="https://redis.io">Redis</a> cache that
   * reads and writes associated {@link RequestT} and {@link ResponseT} pairs. The purpose of the
   * cache is to offload {@link RequestT}s from the API and instead return the {@link ResponseT} if
   * the association is known. Since the {@link RequestT}s and {@link ResponseT}s need encoding and
   * decoding, checks are made whether the requestTCoder and {@link Configuration#getResponseTCoder}
   * are {@link Coder#verifyDeterministic}.
   *
   * <pre>Below describes the paramters in more detail and their usage.</pre>
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
    configuration.getResponseTCoder().verifyDeterministic();

    PTransform<PCollection<RequestT>, Result<KV<RequestT, @Nullable ResponseT>>> cacheRead =
        Cache.<RequestT, @Nullable ResponseT>readUsingRedis(
            new RedisClient(uri),
            requestTCoder,
            new CacheResponseCoder<>(configuration.getResponseTCoder()));

    PTransform<PCollection<KV<RequestT, ResponseT>>, Result<KV<RequestT, ResponseT>>> cacheWrite =
        Cache.<RequestT, ResponseT>writeUsingRedis(
            expiry,
            new RedisClient(uri),
            requestTCoder,
            new CacheResponseCoder<>(configuration.getResponseTCoder()));

    return new RequestResponseIO<>(
        configuration.toBuilder().setCacheRead(cacheRead).setCacheWrite(cacheWrite).build());
  }

  public RequestResponseIO<RequestT, ResponseT> withCacheRead(
      PTransform<PCollection<RequestT>, Result<KV<RequestT, @Nullable ResponseT>>> cacheRead) {
    return new RequestResponseIO<>(configuration.toBuilder().setCacheRead(cacheRead).build());
  }

  public RequestResponseIO<RequestT, ResponseT> withCacheWrite(
      PTransform<PCollection<KV<RequestT, ResponseT>>, Result<KV<RequestT, ResponseT>>>
          cacheWrite) {
    return new RequestResponseIO<>(configuration.toBuilder().setCacheWrite(cacheWrite).build());
  }

  public RequestResponseIO<RequestT, ResponseT> withPreventiveThrottleUsingRedis(
      URI uri, Coder<RequestT> requestTCoder, Quota quota, String quotaKey, String queueKey)
      throws NonDeterministicException {
    requestTCoder.verifyDeterministic();
    return new RequestResponseIO<>(
        configuration
            .toBuilder()
            .setThrottle(
                ThrottleWithExternalResource.usingRedis(
                    uri, quotaKey, queueKey, quota, requestTCoder))
            .build());
  }

  public RequestResponseIO<RequestT, ResponseT> withPreventiveThrottle(
      PTransform<PCollection<RequestT>, Result<RequestT>> throttle) {
    return new RequestResponseIO<>(configuration.toBuilder().setThrottle(throttle).build());
  }

  /** Configuration details for {@link RequestResponseIO}. */
  @AutoValue
  abstract static class Configuration<RequestT, ResponseT> {

    static <RequestT, ResponseT> Builder<RequestT, ResponseT> builder() {
      return new AutoValue_RequestResponseIO_Configuration.Builder<>();
    }

    /** The user custom code to execute for each {@link RequestT}. */
    abstract Caller<RequestT, ResponseT> getCaller();

    /**
     * Required by {@link Call} and applied to the resulting {@link ResponseT} {@link PCollection}.
     */
    abstract Coder<ResponseT> getResponseTCoder();

    /**
     * Optional parameter to tell {@link Call} what to call during a {@link
     * org.apache.beam.sdk.transforms.DoFn.Setup} and {@link
     * org.apache.beam.sdk.transforms.DoFn.Teardown}, respectively.
     */
    abstract @Nullable SetupTeardown getSetupTeardown();

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

      /** See {@link #getCaller}. */
      abstract Builder<RequestT, ResponseT> setCaller(Caller<RequestT, ResponseT> value);

      /** See {@link #getResponseTCoder}. */
      abstract Builder<RequestT, ResponseT> setResponseTCoder(Coder<ResponseT> value);

      /** See {@link #getSetupTeardown}. */
      abstract Builder<RequestT, ResponseT> setSetupTeardown(SetupTeardown value);

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
        responseList.apply(name("flatten responses"), Flatten.pCollections());
    PCollection<ApiIOError> failures =
        failureList.apply(name("flatten failures"), Flatten.pCollections());

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
    if (configuration.getCacheRead() == null) {
      return Triple.of(input, responseList, failureList);
    }
    Result<KV<RequestT, @Nullable ResponseT>> cacheReadResult =
        input.apply(checkStateNotNull(configuration.getCacheRead()));

    PCollectionList<KV<RequestT, ResponseT>> cacheReadList =
        cacheReadResult
            .getResponses()
            .apply(
                name("partition cache reads"),
                Partition.of(PartitionCacheReadsFn.NUM_PARTITIONS, new PartitionCacheReadsFn<>()));

    input =
        cacheReadList
            .get(PartitionCacheReadsFn.NULL_PARTITION)
            .apply(name("uncached requests"), Keys.create());

    responseList =
        responseList.and(
            cacheReadList
                .get(PartitionCacheReadsFn.NON_NULL_PARTITION)
                .apply("cached responses", Values.create()));

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
    if (configuration.getThrottle() == null) {
      return Pair.of(input, failureList);
    }

    Result<RequestT> throttleResult =
        input.apply(name("throttle"), checkStateNotNull(configuration.getThrottle()));

    return Pair.of(throttleResult.getResponses(), failureList.and(throttleResult.getFailures()));
  }

  /**
   * Expands with a {@link Call} and {@link Configuration#getCacheWrite}, if available.
   *
   * <pre>Algorithm is as follows:
   * <ol>
   *   <li>If {@link Configuration#getCacheWrite} not available, instantiates and applies a
   *   {@link Call} using {@link Configuration#getCaller} and optionally
   *   {@link Configuration#getSetupTeardown}. </li>
   *   <li>Otherwise, wraps the original {@link Configuration#getCaller} using
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

    if (configuration.getCacheWrite() == null) {
      Call<RequestT, ResponseT> call =
          Call.of(configuration.getCaller(), configuration.getResponseTCoder());
      if (configuration.getSetupTeardown() != null) {
        call = call.withSetupTeardown(checkStateNotNull(configuration.getSetupTeardown()));
      }
      Result<ResponseT> result = input.apply(name("call"), call);
      return Pair.of(
          responseList.and(result.getResponses()), failureList.and(result.getFailures()));
    }

    // Wrap caller to associate RequestT with ResponseT as a KV.
    Caller<RequestT, KV<RequestT, ResponseT>> caller =
        new WrappedAssociatingRequestResponseCaller<>(configuration.getCaller());
    Coder<KV<RequestT, ResponseT>> coder =
        KvCoder.of(input.getCoder(), configuration.getResponseTCoder());
    Call<RequestT, KV<RequestT, ResponseT>> call = Call.of(caller, coder);
    if (configuration.getSetupTeardown() != null) {
      call = call.withSetupTeardown(checkStateNotNull(configuration.getSetupTeardown()));
    }

    // Extract ResponseT from KV; append failures.
    Result<KV<RequestT, ResponseT>> result = input.apply(name("call"), call);
    PCollection<ResponseT> responses =
        result.getResponses().apply(name("responses"), Values.create());
    responseList = responseList.and(responses);
    failureList = failureList.and(result.getFailures());

    // Write RequestT and ResponseT pairs to the cache; append failures.
    Result<KV<RequestT, ResponseT>> cacheWriteResult =
        result
            .getResponses()
            .apply("cache write", checkStateNotNull(configuration.getCacheWrite()));
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

  static class WrappedAssociatingRequestResponseCaller<RequestT, ResponseT>
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

  private static String name(String... parts) {
    return RequestResponseIO.class.getSimpleName() + String.join(" ", parts);
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
  }
}
