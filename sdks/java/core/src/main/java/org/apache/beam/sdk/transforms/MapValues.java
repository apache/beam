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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.transforms.Contextful.Fn;
import org.apache.beam.sdk.transforms.WithFailures.ExceptionElement;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeParameter;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.RequiresNonNull;

/**
 * {@code MapValues} maps a {@code SerializableFunction<V1,V2>} over values of a {@code
 * PCollection<KV<K,V1>>} and returns a {@code PCollection<KV<K, V2>>}.
 *
 * <p>Example of use:
 *
 * <pre>{@code
 * PCollection<KV<String, Integer>> input = ...;
 * PCollection<KV<String, Double> output =
 *      input.apply(MapValues.into(TypeDescriptors.doubles()).via(Integer::doubleValue));
 * }</pre>
 *
 * <p>See also {@link MapKeys}.
 *
 * @param <V1> the type of the values in the input {@code PCollection}
 * @param <V2> the type of the elements in the output {@code PCollection}
 */
public class MapValues<K, V1, V2>
    extends PTransform<PCollection<KV<K, V1>>, PCollection<KV<K, V2>>> {

  private final transient TypeDescriptor<V2> outputType;
  private final @Nullable Contextful<Fn<KV<K, V1>, KV<K, V2>>> fn;

  /**
   * Returns a {@link MapValues} transform for a {@code ProcessFunction<NewV1, V2>} with predefined
   * {@link #outputType}.
   *
   * @param <NewKeyT> the type of the keys in the input and output {@code PCollection}s
   * @param <NewValueT> the type of the values in the input {@code PCollection}
   */
  public <NewKeyT, NewValueT> MapValues<NewKeyT, NewValueT, V2> via(
      SerializableFunction<NewValueT, V2> fn) {
    return new MapValues<>(
        Contextful.fn(
            (element, c) -> KV.of(element.getKey(), fn.apply(element.getValue())),
            Requirements.empty()),
        outputType);
  }

  /**
   * Returns a new {@link MapValues} transform with the given type descriptor for the output type,
   * but the mapping function yet to be specified using {@link #via(SerializableFunction)}.
   */
  public static <V2> MapValues<?, ?, V2> into(final TypeDescriptor<V2> outputType) {
    return new MapValues<>(null, outputType);
  }

  private MapValues(
      @Nullable Contextful<Fn<KV<K, V1>, KV<K, V2>>> fn, TypeDescriptor<V2> outputType) {
    this.fn = fn;
    this.outputType = outputType;
  }

  /**
   * Returns a new {@link SimpleMapWithFailures} transform that catches exceptions raised while
   * mapping elements, with the given type descriptor used for the failure collection but the
   * exception handler yet to be specified using {@link
   * SimpleMapWithFailures#exceptionsVia(ProcessFunction)}.
   *
   * <p>See {@link WithFailures} documentation for usage patterns of the returned {@link
   * WithFailures.Result}.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * Result<PCollection<KV<String, Integer>>, String> result =
   *         input.apply(
   *             MapValues.into(TypeDescriptors.integers())
   *                 .<String, String>via(word -> 1 / word.length)  // Could throw ArithmeticException
   *                 .exceptionsInto(TypeDescriptors.strings())
   *                 .exceptionsVia(ee -> ee.exception().getMessage()));
   * PCollection<KV<String, Integer>> output = result.output();
   * PCollection<String> failures = result.failures();
   * }</pre>
   */
  @RequiresNonNull("fn")
  public <FailureT> SimpleMapWithFailures<KV<K, V1>, KV<K, V2>, FailureT> exceptionsInto(
      TypeDescriptor<FailureT> failureTypeDescriptor) {
    return new SimpleMapWithFailures<>(
        "MapValuesWithFailures", fn, getKvTypeDescriptor(), null, failureTypeDescriptor);
  }

  /**
   * Returns a new {@link SimpleMapWithFailures} transform that catches exceptions raised while
   * mapping elements, passing the raised exception instance and the input element being processed
   * through the given {@code exceptionHandler} and emitting the result to a failure collection.
   *
   * <p>This method takes advantage of the type information provided by {@link InferableFunction},
   * meaning that a call to {@link #exceptionsInto(TypeDescriptor)} may not be necessary.
   *
   * <p>See {@link WithFailures} documentation for usage patterns of the returned {@link
   * WithFailures.Result}.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * Result<PCollection<KV<String, Integer>>, String> result =
   *         input.apply(
   *             MapValues.into(TypeDescriptors.integers())
   *                 .<String, String>via(word -> 1 / word.length)  // Could throw ArithmeticException
   *                 .exceptionsVia(
   *                     new InferableFunction<ExceptionElement<KV<String, String>>, String>() {
   *                       @Override
   *                       public String apply(ExceptionElement<KV<String, String>> input) {
   *                         return input.exception().getMessage();
   *                       }
   *                     }));
   * PCollection<KV<String, Integer>> output = result.output();
   * PCollection<String> failures = result.failures();
   * }</pre>
   */
  @RequiresNonNull("fn")
  public <FailureT> SimpleMapWithFailures<KV<K, V1>, KV<K, V2>, FailureT> exceptionsVia(
      InferableFunction<ExceptionElement<KV<K, V1>>, FailureT> exceptionHandler) {
    return new SimpleMapWithFailures<>(
        "MapValuesWithFailures",
        fn,
        getKvTypeDescriptor(),
        exceptionHandler,
        exceptionHandler.getOutputTypeDescriptor());
  }

  @Override
  public PCollection<KV<K, V2>> expand(PCollection<KV<K, V1>> input) {
    return input.apply(
        "MapValues",
        MapElements.into(getKvTypeDescriptor())
            .via(checkNotNull(fn, "Must specify a function on MapValues using .via()")));
  }

  private TypeDescriptor<KV<K, V2>> getKvTypeDescriptor() {
    return new TypeDescriptor<KV<K, V2>>() {}.where(new TypeParameter<V2>() {}, outputType);
  }
}
