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
import org.apache.beam.io.requestresponse.Call.Result;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * {@link Call} transforms a {@link RequestT} {@link PCollection} into a {@link ResponseT} {@link
 * PCollection} and {@link ApiIOError} {@link PCollection}, both wrapped in a {@link Result}.
 */
class Call<RequestT, ResponseT> extends PTransform<PCollection<RequestT>, Result<ResponseT>> {

  private static final TupleTag<ApiIOError> FAILURE_TAG = new TupleTag<ApiIOError>() {};

  // TODO(damondouglas): remove suppress warnings when configuration utilized in future PR.
  @SuppressWarnings({"unused"})
  private final Configuration<RequestT, ResponseT> configuration;

  private Call(Configuration<RequestT, ResponseT> configuration) {
    this.configuration = configuration;
  }

  /** Configuration details for {@link Call}. */
  @AutoValue
  abstract static class Configuration<RequestT, ResponseT> {

    static <RequestT, ResponseT> Builder<RequestT, ResponseT> builder() {
      return new AutoValue_Call_Configuration.Builder<>();
    }

    abstract Builder<RequestT, ResponseT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<RequestT, ResponseT> {

      abstract Configuration<RequestT, ResponseT> build();
    }
  }

  @Override
  public Result<ResponseT> expand(PCollection<RequestT> input) {
    return Result.of(new TupleTag<ResponseT>() {}, PCollectionTuple.empty(input.getPipeline()));
  }

  /**
   * The {@link Result} of processing request {@link PCollection} into response {@link PCollection}.
   */
  static class Result<ResponseT> implements POutput {

    static <ResponseT> Result<ResponseT> of(TupleTag<ResponseT> responseTag, PCollectionTuple pct) {
      return new Result<>(responseTag, pct);
    }

    private final Pipeline pipeline;
    private final TupleTag<ResponseT> responseTag;
    private final PCollection<ResponseT> responses;
    private final PCollection<ApiIOError> failures;

    private Result(TupleTag<ResponseT> responseTag, PCollectionTuple pct) {
      this.pipeline = pct.getPipeline();
      this.responseTag = responseTag;
      this.responses = pct.get(responseTag);
      this.failures = pct.get(FAILURE_TAG);
    }

    public PCollection<ResponseT> getResponses() {
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
