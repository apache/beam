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

import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * {@code PTransform}s for mapping a simple function over the elements of a {@link PCollection}.
 */
public class MapElements<InputT, OutputT>
extends PTransform<PCollection<InputT>, PCollection<OutputT>> {

  /**
   * For a {@code SerializableFunction<InputT, OutputT>} {@code fn} and output type descriptor,
   * returns a {@code PTransform} that takes an input {@code PCollection<InputT>} and returns
   * a {@code PCollection<OutputT>} containing {@code fn.apply(v)} for every element {@code v} in
   * the input.
   *
   * <p>Example of use in Java 8:
   * <pre>{@code
   * PCollection<Integer> wordLengths = words.apply(
   *     MapElements.via((String word) -> word.length())
   *         .withOutputType(new TypeDescriptor<Integer>() {});
   * }</pre>
   *
   * <p>In Java 7, the overload {@link #via(SimpleFunction)} is more concise as the output type
   * descriptor need not be provided.
   */
  public static <InputT, OutputT> MissingOutputTypeDescriptor<InputT, OutputT>
  via(SerializableFunction<InputT, OutputT> fn) {
    return new MissingOutputTypeDescriptor<>(fn);
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
  public static <InputT, OutputT> MapElements<InputT, OutputT>
  via(final SimpleFunction<InputT, OutputT> fn) {
    return new MapElements<>(fn, fn.getOutputTypeDescriptor());
  }

  /**
   * An intermediate builder for a {@link MapElements} transform. To complete the transform, provide
   * an output type descriptor to {@link MissingOutputTypeDescriptor#withOutputType}. See
   * {@link #via(SerializableFunction)} for a full example of use.
   */
  public static final class MissingOutputTypeDescriptor<InputT, OutputT> {

    private final SerializableFunction<InputT, OutputT> fn;

    private MissingOutputTypeDescriptor(SerializableFunction<InputT, OutputT> fn) {
      this.fn = fn;
    }

    public MapElements<InputT, OutputT> withOutputType(TypeDescriptor<OutputT> outputType) {
      return new MapElements<>(fn, outputType);
    }
  }

  ///////////////////////////////////////////////////////////////////

  private final SerializableFunction<InputT, OutputT> fn;
  private final transient TypeDescriptor<OutputT> outputType;

  private MapElements(
      SerializableFunction<InputT, OutputT> fn,
      TypeDescriptor<OutputT> outputType) {
    this.fn = fn;
    this.outputType = outputType;
  }

  @Override
  public PCollection<OutputT> apply(PCollection<InputT> input) {
    return input.apply("Map", ParDo.of(new OldDoFn<InputT, OutputT>() {
      @Override
      public void processElement(ProcessContext c) {
        c.output(fn.apply(c.element()));
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        MapElements.this.populateDisplayData(builder);
      }
    })).setTypeDescriptorInternal(outputType);
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("mapFn", fn.getClass())
      .withLabel("Map Function"));
  }
}
