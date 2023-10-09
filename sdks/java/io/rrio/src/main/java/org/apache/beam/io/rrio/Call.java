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
package org.apache.beam.io.rrio;

import com.google.auto.value.AutoValue;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.io.rrio.Call.Result;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Instant;

// TODO: this class is a placeholder to prevent checker errors for unused class dependencies.
/** {@link Call}s a users custom {@link Caller} and optionally a {@link SetupTeardown}. */
@AutoValue
abstract class Call<RequestT, ResponseT>
    extends PTransform<PCollection<RequestT>, Result<ResponseT>> {

  private final TupleTag<ResponseT> responseTag = new TupleTag<ResponseT>() {};

  static <RequestT, ResponseT> Builder<RequestT, ResponseT> builder() {
    return new AutoValue_Call.Builder<>();
  }

  abstract Caller<RequestT, ResponseT> getCaller();

  abstract SetupTeardown getSetupTeardown();

  @Override
  public Result<ResponseT> expand(PCollection<RequestT> input) {
    PCollectionTuple pct =
        input.apply(
            CallFn.class.getSimpleName(),
            ParDo.of(new CallFn<RequestT, ResponseT>())
                // TODO: add error tag and PCollection
                .withOutputTags(responseTag, TupleTagList.empty()));
    return Result.of(responseTag, pct);
  }

  private static class CallFn<RequestT, ResponseT> extends DoFn<RequestT, ResponseT> {
    @ProcessElement
    public void process(
        @Element RequestT request, @Timestamp Instant ts, MultiOutputReceiver receiver) {}
  }

  static class Result<ResponseT> implements POutput {

    static <ResponseT> Result<ResponseT> of(TupleTag<ResponseT> responseTag, PCollectionTuple pct) {
      return new Result<>(responseTag, pct);
    }

    private final Pipeline pipeline;
    private final TupleTag<ResponseT> responseTag;
    private final PCollection<ResponseT> responses;

    private Result(TupleTag<ResponseT> responseTag, PCollectionTuple pct) {
      this.pipeline = pct.getPipeline();
      this.responseTag = responseTag;
      this.responses = pct.get(responseTag);
    }

    @Override
    public Pipeline getPipeline() {
      return this.pipeline;
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
      return ImmutableMap.of(
          // TODO: add error type
          responseTag, responses);
    }

    @Override
    public void finishSpecifyingOutput(
        String transformName, PInput input, PTransform<?, ?> transform) {}
  }

  @AutoValue.Builder
  abstract static class Builder<RequestT, ResponseT> {

    abstract Builder<RequestT, ResponseT> setCaller(Caller<RequestT, ResponseT> value);

    abstract Builder<RequestT, ResponseT> setSetupTeardown(SetupTeardown value);

    abstract Optional<SetupTeardown> getSetupTeardown();

    abstract Call<RequestT, ResponseT> autoBuild();

    final Call<RequestT, ResponseT> build() {

      if (!getSetupTeardown().isPresent()) {
        setSetupTeardown(new NoopSetupTeardown());
      }

      return autoBuild();
    }
  }

  private static class NoopSetupTeardown implements SetupTeardown {
    @Override
    public void setup() throws UserCodeExecutionException {}

    @Override
    public void teardown() throws UserCodeExecutionException {}
  }
}
