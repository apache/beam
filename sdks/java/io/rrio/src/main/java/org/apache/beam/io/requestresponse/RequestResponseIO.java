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
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

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
    return null;
  }
}
