/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.transforms;

import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

/**
 * {@code Count<T>} takes a {@code PCollection<T>} and returns a
 * {@code PCollection<KV<T, Long>>} representing a map from each
 * distinct element of the input {@code PCollection} to the number of times
 * that element occurs in the input.  Each of the keys in the output
 * {@code PCollection} is unique.
 *
 * <p> Two values of type {@code T} are compared for equality <b>not</b> by
 * regular Java {@link Object#equals}, but instead by first encoding
 * each of the elements using the {@code PCollection}'s {@code Coder}, and then
 * comparing the encoded bytes.  This admits efficient parallel
 * evaluation.
 *
 * <p> By default, the {@code Coder} of the keys of the output
 * {@code PCollection} is the same as the {@code Coder} of the
 * elements of the input {@code PCollection}.
 *
 * <p> Each output element is in the window by which its corresponding input
 * was grouped, and has the timestamp of the end of that window.  The output
 * {@code PCollection} has the same
 * {@link com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn}
 * as the input.
 *
 * <p> Example of use:
 * <pre> {@code
 * PCollection<String> words = ...;
 * PCollection<KV<String, Long>> wordCounts =
 *     words.apply(Count.<String>perElement());
 * } </pre>
 */
public class Count {

  /**
   * Returns a {@link Globally Count.Globally} {@link PTransform}
   * that counts the number of elements in its input {@link PCollection}.
   *
   * <p> See {@link Globally Count.Globally} for more details.
   */
  public static <T> Globally<T> globally() {
    return new Globally<>();
  }

  /**
   * Returns a {@link PerElement Count.PerElement} {@link PTransform}
   * that counts the number of occurrences of each element in its
   * input {@link PCollection}.
   *
   * <p> See {@link PerElement Count.PerElement} for more details.
   */
  public static <T> PerElement<T> perElement() {
    return new PerElement<>();
  }

  ///////////////////////////////////////

  /**
   * {@code Count.Globally<T>} takes a {@code PCollection<T>} and returns a
   * {@code PCollection<Long>} containing a single element which is the total
   * number of elements in the {@code PCollection}.
   *
   * <p> Example of use:
   * <pre> {@code
   * PCollection<String> words = ...;
   * PCollection<Long> wordCount =
   *     words.apply(Count.<String>globally());
   * } </pre>
   *
   * @param <T> the type of the elements of the input {@code PCollection}
   */
  @SuppressWarnings("serial")
  public static class Globally<T>
      extends PTransform<PCollection<T>, PCollection<Long>> {

    public Globally() { }

    @Override
    public PCollection<Long> apply(PCollection<T> input) {
      return
          input
          .apply(ParDo.named("Init")
                 .of(new DoFn<T, Long>() {
                     @Override
                     public void processElement(ProcessContext c) {
                       c.output(1L);
                     }
                   }))
          .apply(Sum.longsGlobally());
    }
  }

  /**
   * {@code Count.PerElement<T>} takes a {@code PCollection<T>} and returns a
   * {@code PCollection<KV<T, Long>>} representing a map from each
   * distinct element of the input {@code PCollection} to the number of times
   * that element occurs in the input.  Each of the keys in the output
   * {@code PCollection} is unique.
   *
   * <p> This transform compares two values of type {@code T} by first
   * encoding each element using the input {@code PCollection}'s
   * {@code Coder}, then comparing the encoded bytes. Because of this,
   * the input coder must be deterministic. (See
   * {@link com.google.cloud.dataflow.sdk.coders.Coder#isDeterministic()} for more detail).
   * Performing the comparison in this manner admits efficient parallel evaluation.
   *
   * <p> By default, the {@code Coder} of the keys of the output
   * {@code PCollection} is the same as the {@code Coder} of the
   * elements of the input {@code PCollection}.
   *
   * <p> Example of use:
   * <pre> {@code
   * PCollection<String> words = ...;
   * PCollection<KV<String, Long>> wordCounts =
   *     words.apply(Count.<String>perElement());
   * } </pre>
   *
   * @param <T> the type of the elements of the input {@code PCollection}, and
   * the type of the keys of the output {@code PCollection}
   */
  @SuppressWarnings("serial")
  public static class PerElement<T>
      extends PTransform<PCollection<T>, PCollection<KV<T, Long>>> {

    public PerElement() { }

    @Override
    public PCollection<KV<T, Long>> apply(PCollection<T> input) {
      return
          input
          .apply(ParDo.named("Init")
                 .of(new DoFn<T, KV<T, Long>>() {
                     @Override
                     public void processElement(ProcessContext c) {
                       c.output(KV.of(c.element(), 1L));
                     }
                   }))
          .apply(Sum.<T>longsPerKey());
    }

    @Override
    public String getKindString() {
      return "Count.PerElement";
    }
  }
}
