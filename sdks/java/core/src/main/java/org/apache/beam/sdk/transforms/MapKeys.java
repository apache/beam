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
 * {@code MapKeys} maps a {@code SerializableFunction<K1,K2>} over keys of a {@code
 * PCollection<KV<K1,V>>} and returns a {@code PCollection<KV<K2, V>>}.
 *
 * <p>Example of use:
 *
 * <pre>{@code
 * PCollection<KV<Integer, String>> input = ...;
 * PCollection<KV<Double, String> output =
 *      input.apply(MapKeys.into(TypeDescriptors.doubles()).via(Integer::doubleValue));
 * }</pre>
 *
 * <p>See also {@link MapValues}.
 *
 * @param <K1> the type of the keys in the input {@code PCollection}
 * @param <K2> the type of the keys in the output {@code PCollection}
 */
public class MapKeys<K1, K2, V> extends PTransform<PCollection<KV<K1, V>>, PCollection<KV<K2, V>>> {

  private final transient TypeDescriptor<K2> outputType;
  private final @Nullable Contextful<Fn<KV<K1, V>, KV<K2, V>>> fn;

  /**
   * Returns a {@code MapKeys<K1, K2, V>} {@code PTransform} for a {@code ProcessFunction<NewK1,
   * K2>} with predefined {@link #outputType}.
   *
   * @param <NewKeyT> the type of the keys in the input {@code PCollection}
   * @param <NewValueT> the type of the values in the input and output {@code PCollection}s
   */
  public <NewValueT, NewKeyT> MapKeys<NewKeyT, K2, NewValueT> via(
      SerializableFunction<NewKeyT, K2> fn) {
    return new MapKeys<>(
        Contextful.fn(
            (element, c) -> KV.of(fn.apply(element.getKey()), element.getValue()),
            Requirements.empty()),
        outputType);
  }

  /**
   * Returns a new {@link MapKeys} transform with the given type descriptor for the output type, but
   * the mapping function yet to be specified using {@link #via(SerializableFunction)}.
   */
  public static <K2> MapKeys<?, K2, ?> into(final TypeDescriptor<K2> outputType) {
    return new MapKeys<>(null, outputType);
  }

  private MapKeys(
      @Nullable Contextful<Fn<KV<K1, V>, KV<K2, V>>> fn, TypeDescriptor<K2> outputType) {
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
   * Result<PCollection<KV<Integer, String>>, String> result =
   *         input.apply(
   *             MapKeys.into(TypeDescriptors.integers())
   *                 .<String, String>via(word -> 1 / word.length)  // Could throw ArithmeticException
   *                 .exceptionsInto(TypeDescriptors.strings())
   *                 .exceptionsVia(ee -> ee.exception().getMessage()));
   * PCollection<KV<Integer, String>> output = result.output();
   * PCollection<String> failures = result.failures();
   * }</pre>
   */
  @RequiresNonNull("fn")
  public <FailureT> SimpleMapWithFailures<KV<K1, V>, KV<K2, V>, FailureT> exceptionsInto(
      TypeDescriptor<FailureT> failureTypeDescriptor) {
    return new SimpleMapWithFailures<>(
        "MapKeysWithFailures", fn, getKvTypeDescriptor(), null, failureTypeDescriptor);
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
   * Result<PCollection<KV<Integer, String>>, String> result =
   *         input.apply(
   *             MapKeys.into(TypeDescriptors.integers())
   *                 .<String, String>via(word -> 1 / word.length)  // Could throw ArithmeticException
   *                 .exceptionsVia(
   *                     new InferableFunction<ExceptionElement<KV<String, String>>, String>() {
   *                       @Override
   *                       public String apply(ExceptionElement<KV<String, String>> input) {
   *                         return input.exception().getMessage();
   *                       }
   *                     }));
   * PCollection<KV<Integer, String>> output = result.output();
   * PCollection<String> failures = result.failures();
   * }</pre>
   */
  @RequiresNonNull("fn")
  public <FailureT> SimpleMapWithFailures<KV<K1, V>, KV<K2, V>, FailureT> exceptionsVia(
      InferableFunction<ExceptionElement<KV<K1, V>>, FailureT> exceptionHandler) {
    return new SimpleMapWithFailures<>(
        "MapKeysWithFailures",
        fn,
        getKvTypeDescriptor(),
        exceptionHandler,
        exceptionHandler.getOutputTypeDescriptor());
  }

  @Override
  public PCollection<KV<K2, V>> expand(PCollection<KV<K1, V>> input) {
    return input.apply(
        "MapKeys",
        MapElements.into(getKvTypeDescriptor())
            .via(checkNotNull(fn, "Must specify a function on MapKeys using .via()")));
  }

  private TypeDescriptor<KV<K2, V>> getKvTypeDescriptor() {
    return new TypeDescriptor<KV<K2, V>>() {}.where(new TypeParameter<K2>() {}, outputType);
  }
}
