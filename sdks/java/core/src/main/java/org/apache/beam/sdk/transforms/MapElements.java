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
import static com.google.common.base.Preconditions.checkState;

import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.transforms.Contextful.Fn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * {@code PTransform}s for mapping a simple function over the elements of a {@link PCollection}.
 */
public class MapElements<InputT, OutputT>
extends PTransform<PCollection<? extends InputT>, PCollection<OutputT>> {
  @Nullable private final transient TypeDescriptor<InputT> inputType;
  @Nullable private final transient TypeDescriptor<OutputT> outputType;
  @Nullable private final transient Object originalFnForDisplayData;
  @Nullable private final Contextful<Fn<InputT, OutputT>> fn;

  private MapElements(
      @Nullable Contextful<Fn<InputT, OutputT>> fn,
      @Nullable Object originalFnForDisplayData,
      @Nullable TypeDescriptor<InputT> inputType,
      TypeDescriptor<OutputT> outputType) {
    this.fn = fn;
    this.originalFnForDisplayData = originalFnForDisplayData;
    this.inputType = inputType;
    this.outputType = outputType;
  }

  /**
   * For a {@code SimpleFunction<InputT, OutputT>} {@code fn}, returns a {@code PTransform} that
   * takes an input {@code PCollection<InputT>} and returns a {@code PCollection<OutputT>}
   * containing {@code fn.apply(v)} for every element {@code v} in the input.
   *
   * <p>This overload is intended primarily for use in Java 7. In Java 8, the overload {@link
   * #via(SerializableFunction)} supports use of lambda for greater concision.
   *
   * <p>Example of use in Java 7:
   *
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
    return new MapElements<>(
        Contextful.fn(fn), fn, fn.getInputTypeDescriptor(), fn.getOutputTypeDescriptor());
  }

  /**
   * Returns a new {@link MapElements} transform with the given type descriptor for the output
   * type, but the mapping function yet to be specified using {@link #via(SerializableFunction)}.
   */
  public static <OutputT> MapElements<?, OutputT>
  into(final TypeDescriptor<OutputT> outputType) {
    return new MapElements<>(null, null, null, outputType);
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
    return new MapElements<>(Contextful.fn(fn), fn, TypeDescriptors.inputOf(fn), outputType);
  }

  /**
   * Like {@link #via(SerializableFunction)}, but supports access to context, such as side inputs.
   */
  @Experimental(Kind.CONTEXTFUL)
  public <NewInputT> MapElements<NewInputT, OutputT> via(Contextful<Fn<NewInputT, OutputT>> fn) {
    return new MapElements<>(
        fn, fn.getClosure(), TypeDescriptors.inputOf(fn.getClosure()), outputType);
  }

  @Override
  public PCollection<OutputT> expand(PCollection<? extends InputT> input) {
    checkNotNull(fn, "Must specify a function on MapElements using .via()");
    return input.apply(
        "Map",
        ParDo.of(
            new DoFn<InputT, OutputT>() {
              @ProcessElement
              public void processElement(@Element InputT element,
                                         OutputReceiver<OutputT> receiver,
                                         ProcessContext c) throws Exception {
                receiver.output(fn.getClosure().apply(element, Fn.Context.wrapProcessContext(c)));
              }

              @Override
              public void populateDisplayData(DisplayData.Builder builder) {
                builder.delegate(MapElements.this);
              }

              @Override
              public TypeDescriptor<InputT> getInputTypeDescriptor() {
                return inputType;
              }

              @Override
              public TypeDescriptor<OutputT> getOutputTypeDescriptor() {
                checkState(
                    outputType != null,
                    "%s output type descriptor was null; "
                        + "this probably means that getOutputTypeDescriptor() was called after "
                        + "serialization/deserialization, but it is only available prior to "
                        + "serialization, for constructing a pipeline and inferring coders",
                    MapElements.class.getSimpleName());
                return outputType;
              }
            }).withSideInputs(fn.getRequirements().getSideInputs()));
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("class", originalFnForDisplayData.getClass()));
    if (originalFnForDisplayData instanceof HasDisplayData) {
      builder.include("fn", (HasDisplayData) originalFnForDisplayData);
    }
  }
}
