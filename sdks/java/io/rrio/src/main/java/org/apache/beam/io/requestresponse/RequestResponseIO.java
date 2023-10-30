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
import org.apache.beam.io.requestresponse.RequestResponseIO.Result;
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
 * <p>Then provide {@link RequestResponseIO}'s {@link #create} method your {@link Caller}
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

  private static final TupleTag<ApiIOError> FAILURE_TAG = new TupleTag<ApiIOError>() {};

  // TODO(damondouglas): remove when utilized.
  @SuppressWarnings({"unused"})
  private final Configuration<RequestT, ResponseT> configuration;

  private RequestResponseIO(Configuration<RequestT, ResponseT> configuration) {
    this.configuration = configuration;
  }

  public static <RequestT, ResponseT> RequestResponseIO<RequestT, ResponseT> of(
      Caller<RequestT, ResponseT> caller) {
    return new RequestResponseIO<>(
        Configuration.<RequestT, ResponseT>builder().setCaller(caller).build());
  }

  /** Configuration details for {@link RequestResponseIO}. */
  @AutoValue
  abstract static class Configuration<RequestT, ResponseT> {

    static <RequestT, ResponseT> Builder<RequestT, ResponseT> builder() {
      return new AutoValue_RequestResponseIO_Configuration.Builder<>();
    }

    /**
     * The {@link Caller} that interfaces user custom code to process a {@link RequestT} into a
     * {@link ResponseT}.
     */
    abstract Caller<RequestT, ResponseT> getCaller();

    abstract Builder<RequestT, ResponseT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<RequestT, ResponseT> {

      abstract Builder<RequestT, ResponseT> setCaller(Caller<RequestT, ResponseT> value);

      abstract Configuration<RequestT, ResponseT> build();
    }
  }

  @Override
  public Result<ResponseT> expand(PCollection<RequestT> input) {
    // TODO(damondouglas; https://github.com/apache/beam/issues?q=is%3Aissue+is%3Aopen+%5BRRIO%5D):
    //  expand pipeline as more dependencies develop.
    return Result.of(new TupleTag<ResponseT>() {}, PCollectionTuple.empty(input.getPipeline()));
  }

  /**
   * The {@link Result} of processing request {@link PCollection} into response {@link PCollection}
   * using custom {@link Caller} code.
   */
  public static class Result<ResponseT> implements POutput {

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
