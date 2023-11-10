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
import java.net.URI;
import java.util.Map;
import org.apache.beam.io.requestresponse.CacheRead.Result;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@link CacheRead} reads associated {@link ResponseT} types from {@link RequestT} types, if any
 * exist, returning a {@link Result} where {@link Result#getResponses}'s {@link KV} will have null
 * {@link ResponseT} for {@link RequestT}s that have no associated {@link ResponseT} in the cache.
 */
class CacheRead<@NonNull RequestT, @Nullable ResponseT>
    extends PTransform<
        @NonNull PCollection<@NonNull RequestT>,
        @NonNull Result<@NonNull RequestT, @Nullable ResponseT>> {

  static <@NonNull RequestT, @Nullable ResponseT>
      CacheRead<@NonNull RequestT, @Nullable ResponseT> usingRedis(
          URI uri,
          CacheSerializer<@NonNull RequestT> requestSerializer,
          CacheSerializer<@NonNull ResponseT> responseSerializer,
          Coder<@NonNull RequestT> requestCoder,
          Coder<@Nullable ResponseT> responseCoder) {
    RedisClient client = new RedisClient(uri);
    CacheReadUsingRedis<@NonNull RequestT, @Nullable ResponseT> caller =
        new CacheReadUsingRedis<>(client, requestSerializer, responseSerializer);
    return new CacheRead<>(
        Configuration.<@NonNull RequestT, @Nullable ResponseT>builder()
            .setCaller(caller)
            .setSetupTeardown(caller)
            .setResponseCoder(KvCoder.of(requestCoder, responseCoder))
            .build());
  }

  private final Configuration<@NonNull RequestT, @Nullable ResponseT> configuration;

  private CacheRead(Configuration<@NonNull RequestT, @Nullable ResponseT> configuration) {
    this.configuration = configuration;
  }

  @Override
  public @NonNull Result<@NonNull RequestT, @Nullable ResponseT> expand(
      PCollection<@NonNull RequestT> input) {
    Call.Result<KV<@NonNull RequestT, @Nullable ResponseT>> result =
        input.apply(
            Call.of(configuration.getCaller(), configuration.getResponseCoder())
                .withSetupTeardown(configuration.getSetupTeardown()));
    return new Result<>(
        input.getPipeline(),
        new TupleTag<KV<@NonNull RequestT, @Nullable ResponseT>>() {},
        result.getResponses(),
        new TupleTag<ApiIOError>() {},
        result.getFailures());
  }

  /** Configuration details for {@link CacheRead}. */
  @AutoValue
  abstract static class Configuration<@NonNull RequestT, @Nullable ResponseT> {

    static <@NonNull RequestT, @Nullable ResponseT>
        Builder<@NonNull RequestT, @Nullable ResponseT> builder() {
      return new AutoValue_CacheRead_Configuration.Builder<>();
    }

    abstract Caller<@NonNull RequestT, KV<@NonNull RequestT, @Nullable ResponseT>> getCaller();

    abstract SetupTeardown getSetupTeardown();

    abstract Coder<KV<RequestT, ResponseT>> getResponseCoder();

    abstract Builder<@NonNull RequestT, @Nullable ResponseT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<@NonNull RequestT, @Nullable ResponseT> {

      abstract Builder<RequestT, ResponseT> setCaller(
          Caller<RequestT, KV<RequestT, ResponseT>> value);

      abstract Builder<RequestT, ResponseT> setSetupTeardown(SetupTeardown value);

      abstract Builder<RequestT, ResponseT> setResponseCoder(Coder<KV<RequestT, ResponseT>> value);

      abstract Configuration<@NonNull RequestT, @Nullable ResponseT> build();
    }
  }

  /**
   * The {@link Result} of reading @NonNull RequestT {@link PCollection} elements yielding @Nullable
   * ResponseT {@link PCollection} elements.
   */
  static class Result<@NonNull RequestT, @Nullable ResponseT> implements POutput {

    private final Pipeline pipeline;
    private final TupleTag<KV<@NonNull RequestT, @Nullable ResponseT>> responseTag;
    private final PCollection<KV<@NonNull RequestT, @Nullable ResponseT>> responses;
    private final TupleTag<ApiIOError> failureTag;
    private final PCollection<ApiIOError> failures;

    private Result(
        Pipeline pipeline,
        TupleTag<KV<@NonNull RequestT, @Nullable ResponseT>> responseTag,
        PCollection<KV<@NonNull RequestT, @Nullable ResponseT>> responses,
        TupleTag<ApiIOError> failureTag,
        PCollection<ApiIOError> failures) {
      this.pipeline = pipeline;
      this.responseTag = responseTag;
      this.responses = responses;
      this.failureTag = failureTag;
      this.failures = failures;
    }

    PCollection<KV<@NonNull RequestT, @Nullable ResponseT>> getResponses() {
      return responses;
    }

    PCollection<ApiIOError> getFailures() {
      return failures;
    }

    @Override
    public @NonNull Pipeline getPipeline() {
      return this.pipeline;
    }

    @Override
    public @NonNull Map<@NonNull TupleTag<?>, @NonNull PValue> expand() {
      return ImmutableMap.of(
          responseTag, responses,
          failureTag, failures);
    }

    @Override
    public void finishSpecifyingOutput(
        String transformName, PInput input, PTransform<?, ?> transform) {}
  }
}
