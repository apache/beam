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
import java.util.Map;
import org.apache.beam.io.requestresponse.CacheWrite.Result;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * {@link CacheWrite} writes associated {@link RequestT} and {@link ResponseT} pairs to a cache.
 * Using {@link RequestT} and {@link ResponseT}'s {@link org.apache.beam.sdk.coders.Coder}, this
 * transform writes encoded representations of this association.
 */
class CacheWrite<RequestT, ResponseT>
    extends PTransform<PCollection<KV<RequestT, ResponseT>>, Result<RequestT, ResponseT>> {

  private static final TupleTag<ApiIOError> FAILURE_TAG = new TupleTag<ApiIOError>() {};

  // TODO(damondouglas): remove suppress warnings after configuration is used.
  @SuppressWarnings({"unused"})
  private final Configuration<RequestT, ResponseT> configuration;

  private CacheWrite(Configuration<RequestT, ResponseT> configuration) {
    this.configuration = configuration;
  }

  /** Configuration details for {@link CacheWrite}. */
  @AutoValue
  abstract static class Configuration<RequestT, ResponseT> {

    static <RequestT, ResponseT> Builder<RequestT, ResponseT> builder() {
      return new AutoValue_CacheWrite_Configuration.Builder<>();
    }

    abstract Builder<RequestT, ResponseT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<RequestT, ResponseT> {

      abstract Configuration<RequestT, ResponseT> build();
    }
  }

  @Override
  public Result<RequestT, ResponseT> expand(PCollection<KV<RequestT, ResponseT>> input) {
    return Result.of(
        new TupleTag<KV<RequestT, ResponseT>>() {}, PCollectionTuple.empty(input.getPipeline()));
  }

  /** The {@link Result} of writing a request/response {@link KV} {@link PCollection}. */
  static class Result<RequestT, ResponseT> implements POutput {

    static <RequestT, ResponseT> Result<RequestT, ResponseT> of(
        TupleTag<KV<RequestT, ResponseT>> responseTag, PCollectionTuple pct) {
      return new Result<>(responseTag, pct);
    }

    private final Pipeline pipeline;
    private final TupleTag<KV<RequestT, ResponseT>> responseTag;
    private final PCollection<KV<RequestT, ResponseT>> responses;
    private final PCollection<ApiIOError> failures;

    private Result(TupleTag<KV<RequestT, ResponseT>> responseTag, PCollectionTuple pct) {
      this.pipeline = pct.getPipeline();
      this.responseTag = responseTag;
      this.responses = pct.get(responseTag);
      this.failures = pct.get(FAILURE_TAG);
    }

    public PCollection<KV<RequestT, ResponseT>> getResponses() {
      return responses;
    }

    public PCollection<ApiIOError> getFailures() {
      return failures;
    }

    @Override
    public Pipeline getPipeline() {
      return this.pipeline;
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
      return ImmutableMap.of(
          responseTag, responses,
          FAILURE_TAG, failures);
    }

    @Override
    public void finishSpecifyingOutput(
        String transformName, PInput input, PTransform<?, ?> transform) {}
  }
}
