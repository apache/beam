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

import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * {@code PTransform}s for mapping a simple function over the elements of a {@link PCollection}.
 */
public class MapElements<InputT, OutputT>
extends PTransform<PCollection<? extends InputT>, PCollection<OutputT>> {
  /**
   * Temporarily stores the argument of {@link #into(TypeDescriptor)} until combined with the
   * argument of {@link #via(SerializableFunction)} into the fully-specified {@link #fn}. Stays null
   * if constructed using {@link #via(SimpleFunction)} directly.
   */
  @Nullable private final transient TypeDescriptor<OutputT> outputType;

  /**
   * Non-null on a fully specified transform - is null only when constructed using {@link
   * #into(TypeDescriptor)}, until the fn is specified using {@link #via(SerializableFunction)}.
   */
  @Nullable private final SimpleFunction<InputT, OutputT> fn;
  private final DisplayData.ItemSpec<?> fnClassDisplayData;

  private MapElements(
      @Nullable SimpleFunction<InputT, OutputT> fn,
      @Nullable TypeDescriptor<OutputT> outputType,
      @Nullable Class<?> fnClass) {
    this.fn = fn;
    this.outputType = outputType;
    this.fnClassDisplayData = DisplayData.item("mapFn", fnClass).withLabel("Map Function");
  }

  /**
   * For a {@code SimpleFunction<InputT, OutputT>} {@code fn}, returns a {@code PTransform} that
   * takes an input {@code PCollection<InputT>} and returns a {@code PCollection<OutputT>}
   * containing {@code fn.apply(v)} for every element {@code v} in the input.
   *
   * <p>This overload is intended primarily for use in Java 7. In Java 8, the overload
   * {@link #via(SerializableFunction)} supports use of lambda for greater concision.
   *
   * <p>Example of use in Java 7:
   * <pre>{@code
   * PCollection<String> words = ...;
   * PCollection<Integer> wordsPerLine = words.apply(MapElements.via(
   *     new SimpleFunction<String, Integer>() {
   *       public Integer apply(String word) {
   *         return word.length();
   *       }
   *     }));
   * }</pre>
   */
  public static <InputT, OutputT> MapElements<InputT, OutputT> via(
      final SimpleFunction<InputT, OutputT> fn) {
    return new MapElements<>(fn, null, fn.getClass());
  }

  /**
   * Returns a new {@link MapElements} transform with the given type descriptor for the output
   * type, but the mapping function yet to be specified using {@link #via(SerializableFunction)}.
   */
  public static <OutputT> MapElements<?, OutputT>
  into(final TypeDescriptor<OutputT> outputType) {
    return new MapElements<>(null, outputType, null);
  }

  /**
   * For a {@code SerializableFunction<InputT, OutputT>} {@code fn} and output type descriptor,
   * returns a {@code PTransform} that takes an input {@code PCollection<InputT>} and returns a
   * {@code PCollection<OutputT>} containing {@code fn.apply(v)} for every element {@code v} in the
   * input.
   *
   * <p>Example of use in Java 8:
   *
   * <pre>{@code
   * PCollection<Integer> wordLengths = words.apply(
   *     MapElements.into(TypeDescriptors.integers())
   *                .via((String word) -> word.length()));
   * }</pre>
   *
   * <p>In Java 7, the overload {@link #via(SimpleFunction)} is more concise as the output type
   * descriptor need not be provided.
   */
  public <NewInputT> MapElements<NewInputT, OutputT> via(
      SerializableFunction<NewInputT, OutputT> fn) {
    return new MapElements<>(
        SimpleFunction.fromSerializableFunctionWithOutputType(fn, outputType),
        null,
        fn.getClass());
  }

  @Override
  public PCollection<OutputT> expand(PCollection<? extends InputT> input) {
    checkNotNull(fn, "Must specify a function on MapElements using .via()");
    return input.apply(
        "Map",
        ParDo.of(
            new DoFn<InputT, OutputT>() {
              @ProcessElement
              public void processElement(ProcessContext c) {
                c.output(fn.apply(c.element()));
              }

              @Override
              public void populateDisplayData(DisplayData.Builder builder) {
                builder.delegate(MapElements.this);
              }

              @Override
              public TypeDescriptor<InputT> getInputTypeDescriptor() {
                return fn.getInputTypeDescriptor();
              }

              @Override
              public TypeDescriptor<OutputT> getOutputTypeDescriptor() {
                return fn.getOutputTypeDescriptor();
              }
            }));
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder
        .include("mapFn", fn)
        .add(fnClassDisplayData);
  }
}
