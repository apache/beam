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
import org.apache.beam.io.requestresponse.ThrottleDequeue.Result;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Instant;

/**
 * {@link ThrottleDequeue} dequeues {@link RequestT} elements at a fixed rate yielding a {@link
 * Result} containing the dequeued {@link RequestT} {@link PCollection} and a {@link ApiIOError}
 * {@link PCollection} of any errors.
 */
class ThrottleDequeue<RequestT> extends PTransform<PCollection<Instant>, Result<RequestT>> {

  private static final TupleTag<ApiIOError> FAILURE_TAG = new TupleTag<ApiIOError>() {};

  // TODO(damondouglas): remove suppress warnings after instance utilized.
  @SuppressWarnings({"unused"})
  private final Configuration<RequestT> configuration;

  private ThrottleDequeue(Configuration<RequestT> configuration) {
    this.configuration = configuration;
  }

  @Override
  public Result<RequestT> expand(PCollection<Instant> input) {
    // TODO(damondouglas): expand in a future PR.
    return new Result<>(new TupleTag<RequestT>() {}, PCollectionTuple.empty(input.getPipeline()));
  }

  @AutoValue
  abstract static class Configuration<RequestT> {

    @AutoValue.Builder
    abstract static class Builder<RequestT> {
      abstract Configuration<RequestT> build();
    }
  }

  /** The {@link Result} of dequeuing {@link RequestT}s. */
  static class Result<RequestT> implements POutput {

    static <RequestT> Result<RequestT> of(TupleTag<RequestT> requestsTag, PCollectionTuple pct) {
      return new Result<>(requestsTag, pct);
    }

    private final Pipeline pipeline;
    private final TupleTag<RequestT> requestsTag;
    private final PCollection<RequestT> requests;
    private final PCollection<ApiIOError> failures;

    private Result(TupleTag<RequestT> requestsTag, PCollectionTuple pct) {
      this.pipeline = pct.getPipeline();
      this.requestsTag = requestsTag;
      this.requests = pct.get(requestsTag);
      this.failures = pct.get(FAILURE_TAG);
    }

    @Override
    public Pipeline getPipeline() {
      return pipeline;
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
      return ImmutableMap.of(
          requestsTag, requests,
          FAILURE_TAG, failures);
    }

    @Override
    public void finishSpecifyingOutput(
        String transformName, PInput input, PTransform<?, ?> transform) {}
  }
}
