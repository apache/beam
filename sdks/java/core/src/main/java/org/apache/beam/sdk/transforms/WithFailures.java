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
package org.apache.beam.sdk.transforms;

import com.google.auto.value.AutoValue;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A collection of utilities for writing transforms that can handle exceptions raised during
 * processing of elements.
 *
 * <p>Consuming transforms such as {@link MapElements.MapWithFailures} follow the general pattern of
 * taking in a user-defined exception handler of type {@code
 * ProcessFunction<ExceptionElement<InputT>, FailureOutputT>} where the input {@link
 * ExceptionElement} contains an exception along with the input element that was being processed
 * when the exception was raised. This handler is responsible for producing some output element that
 * captures relevant details of the failure and can be encoded as part of a failure output {@link
 * PCollection}. Transforms can then package together their output and failure collections in a
 * {@link WithFailures.Result} that avoids users needing to interact with {@code TupleTag}s and
 * indexing into a {@link PCollectionTuple}.
 *
 * <p>Exception handlers can narrow their scope by rethrowing the passed {@link
 * ExceptionElement#exception()} and catching only specific subclasses of {@code Exception}.
 * Unhandled exceptions will generally bubble up to a top-level {@link
 * org.apache.beam.sdk.Pipeline.PipelineExecutionException} that halts progress.
 *
 * <p>Users can take advantage of {@link Result#failuresTo(List)} for fluent chaining of transforms
 * that handle exceptions:
 *
 * <pre>{@code
 * PCollection<Integer> input = ...
 * List<PCollection<Map<String, String>> failureCollections = new ArrayList<>();
 * input.apply(MapElements.via(...).exceptionsVia(...))
 *      .failuresTo(failureCollections)
 *      .apply(MapElements.via(...).exceptionsVia(...))
 *      .failuresTo(failureCollections);
 * PCollection<Map<String, String>> failures = PCollectionList.of(failureCollections)
 *      .apply("FlattenFailureCollections", Flatten.pCollections());
 * }</pre>
 */
@Experimental(Kind.WITH_EXCEPTIONS)
public class WithFailures {

  /**
   * The value type passed as input to exception handlers. It wraps an exception together with the
   * input element that was being processed at the time the exception was raised.
   *
   * <p>Exception handlers may want to re-raise the exception and catch only specific subclasses in
   * order to limit the scope of handled exceptions or access subclass-specific data.
   */
  @AutoValue
  public abstract static class ExceptionElement<T> {
    public abstract T element();

    public abstract Exception exception();

    public static <T> ExceptionElement<T> of(T element, Exception exception) {
      return new AutoValue_WithFailures_ExceptionElement<>(element, exception);
    }
  }

  /**
   * A simple handler that extracts information from an exception to a {@code Map<String, String>}
   * and returns a {@link KV} where the key is the input element that failed processing, and the
   * value is the map of exception attributes.
   *
   * <p>Extends {@link SimpleFunction} so that full type information is captured. Map and {@link KV}
   * coders are well supported by Beam, so coder inference can be successfully applied if the
   * consuming transform passes type information to the failure collection's {@link TupleTag}.
   *
   * <p>The keys populated in the map are "className", "message", and "stackTrace" of the exception.
   */
  public static class ExceptionAsMapHandler<T>
      extends SimpleFunction<ExceptionElement<T>, KV<T, Map<String, String>>> {
    @Override
    public KV<T, Map<String, String>> apply(ExceptionElement<T> f) {
      return KV.of(
          f.element(),
          ImmutableMap.of(
              "className", f.exception().getClass().getName(),
              "message", f.exception().getMessage(),
              "stackTrace", Arrays.toString(f.exception().getStackTrace())));
    }
  }

  /**
   * An intermediate output type for PTransforms that allows an output collection to live alongside
   * a collection of elements that failed the transform.
   *
   * @param <OutputT> Output type
   * @param <FailureElementT> Element type for the failure {@code PCollection}
   */
  @AutoValue
  public abstract static class Result<OutputT extends POutput, FailureElementT>
      implements PInput, POutput {

    public abstract OutputT output();

    abstract @Nullable TupleTag<?> outputTag();

    public abstract PCollection<FailureElementT> failures();

    abstract TupleTag<FailureElementT> failuresTag();

    public static <OutputT extends POutput, FailureElementT> Result<OutputT, FailureElementT> of(
        OutputT output, PCollection<FailureElementT> failures) {
      return new AutoValue_WithFailures_Result<>(
          output, null, failures, new TupleTag<FailureElementT>());
    }

    public static <OutputElementT, FailureElementT>
        Result<PCollection<OutputElementT>, FailureElementT> of(
            PCollection<OutputElementT> output, PCollection<FailureElementT> failures) {
      return new AutoValue_WithFailures_Result<>(
          output, new TupleTag<OutputElementT>(), failures, new TupleTag<FailureElementT>());
    }

    public static <OutputElementT, FailureElementT>
        Result<PCollection<OutputElementT>, FailureElementT> of(
            PCollectionTuple tuple,
            TupleTag<OutputElementT> outputTag,
            TupleTag<FailureElementT> failureTag) {
      return new AutoValue_WithFailures_Result<>(
          tuple.get(outputTag), outputTag, tuple.get(failureTag), failureTag);
    }

    /** Adds the failure collection to the passed list and returns just the output collection. */
    public OutputT failuresTo(List<PCollection<FailureElementT>> failureCollections) {
      failureCollections.add(failures());
      return output();
    }

    @Override
    public Pipeline getPipeline() {
      return output().getPipeline();
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
      Map<TupleTag<?>, PValue> values = new HashMap<>();
      values.put(failuresTag(), failures());
      if (outputTag() != null && output() instanceof PValue) {
        values.put(outputTag(), (PValue) output());
      }
      return values;
    }

    @Override
    public void finishSpecifyingOutput(
        String transformName, PInput input, PTransform<?, ?> transform) {}
  }
}
