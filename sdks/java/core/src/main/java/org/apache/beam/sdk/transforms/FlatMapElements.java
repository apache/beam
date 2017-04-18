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

import static com.google.common.base.Preconditions.checkNotNull;

import java.lang.reflect.ParameterizedType;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * {@code PTransform}s for mapping a simple function that returns iterables over the elements of a
 * {@link PCollection} and merging the results.
 */
public class FlatMapElements<InputT, OutputT>
extends PTransform<PCollection<? extends InputT>, PCollection<OutputT>> {
  /**
   * Temporarily stores the argument of {@link #into(TypeDescriptor)} until combined with the
   * argument of {@link #via(SerializableFunction)} into the fully-specified {@link #fn}. Stays null
   * if constructed using {@link #via(SimpleFunction)} directly.
   */
  @Nullable
  private final transient TypeDescriptor<Iterable<OutputT>> outputType;

  /**
   * Non-null on a fully specified transform - is null only when constructed using {@link
   * #into(TypeDescriptor)}, until the fn is specified using {@link #via(SerializableFunction)}.
   */
  @Nullable
  private final SimpleFunction<InputT, Iterable<OutputT>> fn;
  private final DisplayData.ItemSpec<?> fnClassDisplayData;

  private FlatMapElements(
      @Nullable SimpleFunction<InputT, Iterable<OutputT>> fn,
      @Nullable TypeDescriptor<Iterable<OutputT>> outputType,
      @Nullable Class<?> fnClass) {
    this.fn = fn;
    this.outputType = outputType;
    this.fnClassDisplayData = DisplayData.item("flatMapFn", fnClass).withLabel("FlatMap Function");

  }

  /**
   * For a {@code SimpleFunction<InputT, ? extends Iterable<OutputT>>} {@code fn},
   * return a {@link PTransform} that applies {@code fn} to every element of the input
   * {@code PCollection<InputT>} and outputs all of the elements to the output
   * {@code PCollection<OutputT>}.
   *
   * <p>This overload is intended primarily for use in Java 7. In Java 8, the overload
   * {@link #via(SerializableFunction)} supports use of lambda for greater concision.
   *
   * <p>Example of use in Java 7:
   * <pre>{@code
   * PCollection<String> lines = ...;
   * PCollection<String> words = lines.apply(FlatMapElements.via(
   *     new SimpleFunction<String, List<String>>() {
   *       public Integer apply(String line) {
   *         return Arrays.asList(line.split(" "));
   *       }
   *     });
   * }</pre>
   *
   * <p>To use a Java 8 lambda, see {@link #via(SerializableFunction)}.
   */
  public static <InputT, OutputT> FlatMapElements<InputT, OutputT>
  via(SimpleFunction<? super InputT, ? extends Iterable<OutputT>> fn) {
    return new FlatMapElements(fn, null, fn.getClass());
  }

  /**
   * Returns a new {@link FlatMapElements} transform with the given type descriptor for the output
   * type, but the mapping function yet to be specified using {@link #via(SerializableFunction)}.
   */
  public static <OutputT> FlatMapElements<?, OutputT>
  into(final TypeDescriptor<OutputT> outputType) {
    return new FlatMapElements<>(null, TypeDescriptors.iterables(outputType), null);
  }

  /**
   * For a {@code SerializableFunction<InputT, ? extends Iterable<OutputT>>} {@code fn},
   * returns a {@link PTransform} that applies {@code fn} to every element of the input
   * {@code PCollection<InputT>} and outputs all of the elements to the output
   * {@code PCollection<OutputT>}.
   *
   * <p>Example of use in Java 8:
   * <pre>{@code
   * PCollection<String> words = lines.apply(
   *     FlatMapElements.into(TypeDescriptors.strings())
   *                    .via((String line) -> Arrays.asList(line.split(" ")))
   * }</pre>
   *
   * <p>In Java 7, the overload {@link #via(SimpleFunction)} is more concise as the output type
   * descriptor need not be provided.
   */
  public <NewInputT> FlatMapElements<NewInputT, OutputT>
  via(SerializableFunction<NewInputT, ? extends Iterable<OutputT>> fn) {
    return new FlatMapElements(
        SimpleFunction.fromSerializableFunctionWithOutputType(fn, (TypeDescriptor) outputType),
        null,
        fn.getClass());
  }

  @Override
  public PCollection<OutputT> expand(PCollection<? extends InputT> input) {
    checkNotNull(fn, "Must specify a function on FlatMapElements using .via()");
    return input.apply(
        "FlatMap",
        ParDo.of(
            new DoFn<InputT, OutputT>() {
              private static final long serialVersionUID = 0L;

              @ProcessElement
              public void processElement(ProcessContext c) {
                for (OutputT element : fn.apply(c.element())) {
                  c.output(element);
                }
              }

              @Override
              public TypeDescriptor<InputT> getInputTypeDescriptor() {
                return fn.getInputTypeDescriptor();
              }

              @Override
              public TypeDescriptor<OutputT> getOutputTypeDescriptor() {
                @SuppressWarnings({"rawtypes", "unchecked"}) // safe by static typing
                TypeDescriptor<Iterable<?>> iterableType =
                    (TypeDescriptor) fn.getOutputTypeDescriptor();

                @SuppressWarnings("unchecked") // safe by correctness of getIterableElementType
                TypeDescriptor<OutputT> outputType =
                    (TypeDescriptor<OutputT>) getIterableElementType(iterableType);

                return outputType;
              }
            }));
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder
        .include("flatMapFn", fn)
        .add(fnClassDisplayData);
  }

  /**
   * Does a best-effort job of getting the best {@link TypeDescriptor} for the type of the
   * elements contained in the iterable described by the given {@link TypeDescriptor}.
   */
  private static TypeDescriptor<?> getIterableElementType(
      TypeDescriptor<Iterable<?>> iterableTypeDescriptor) {

    // If a rawtype was used, the type token may be for Object, not a subtype of Iterable.
    // In this case, we rely on static typing of the function elsewhere to ensure it is
    // at least some kind of iterable, and grossly overapproximate the element type to be Object.
    if (!iterableTypeDescriptor.isSubtypeOf(new TypeDescriptor<Iterable<?>>() {})) {
      return new TypeDescriptor<Object>() {};
    }

    // Otherwise we can do the proper thing and get the actual type parameter.
    ParameterizedType iterableType =
        (ParameterizedType) iterableTypeDescriptor.getSupertype(Iterable.class).getType();
    return TypeDescriptor.of(iterableType.getActualTypeArguments()[0]);
  }
}
