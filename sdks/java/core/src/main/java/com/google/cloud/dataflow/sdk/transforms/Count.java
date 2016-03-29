/*
 * Copyright (C) 2015 Google Inc.
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

import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

/**
 * {@code PTransorm}s to count the elements in a {@link PCollection}.
 *
 * <p>{@link Count#perElement()} can be used to count the number of occurrences of each
 * distinct element in the PCollection, {@link Count#perKey()} can be used to count the
 * number of values per key, and {@link Count#globally()} can be used to count the total
 * number of elements in a PCollection.
 */
public class Count {
  private Count() {
    // do not instantiate
  }

  /**
   * Returns a {@link Combine.Globally} {@link PTransform} that counts the number of elements in
   * its input {@link PCollection}.
   */
  public static <T> Combine.Globally<T, Long> globally() {
    return Combine.globally(new CountFn<T>()).named("Count.Globally");
  }

  /**
   * Returns a {@link Combine.PerKey} {@link PTransform} that counts the number of elements
   * associated with each key of its input {@link PCollection}.
   */
  public static <K, V> Combine.PerKey<K, V, Long> perKey() {
    return Combine.<K, V, Long>perKey(new CountFn<V>()).named("Count.PerKey");
  }

  /**
   * Returns a {@link PerElement Count.PerElement} {@link PTransform} that counts the number of
   * occurrences of each element in its input {@link PCollection}.
   *
   * <p>See {@link PerElement Count.PerElement} for more details.
   */
  public static <T> PerElement<T> perElement() {
    return new PerElement<>();
  }

  /**
   * {@code Count.PerElement<T>} takes a {@code PCollection<T>} and returns a
   * {@code PCollection<KV<T, Long>>} representing a map from each distinct element of the input
   * {@code PCollection} to the number of times that element occurs in the input. Each key in the
   * output {@code PCollection} is unique.
   *
   * <p>This transform compares two values of type {@code T} by first encoding each element using
   * the input {@code PCollection}'s {@code Coder}, then comparing the encoded bytes. Because of
   * this, the input coder must be deterministic.
   * (See {@link com.google.cloud.dataflow.sdk.coders.Coder#verifyDeterministic()} for more detail).
   * Performing the comparison in this manner admits efficient parallel evaluation.
   *
   * <p>By default, the {@code Coder} of the keys of the output {@code PCollection} is the same as
   * the {@code Coder} of the elements of the input {@code PCollection}.
   *
   * <p>Example of use:
   * <pre> {@code
   * PCollection<String> words = ...;
   * PCollection<KV<String, Long>> wordCounts =
   *     words.apply(Count.<String>perElement());
   * } </pre>
   *
   * @param <T> the type of the elements of the input {@code PCollection}, and the type of the keys
   * of the output {@code PCollection}
   */
  public static class PerElement<T>
      extends PTransform<PCollection<T>, PCollection<KV<T, Long>>> {

    public PerElement() { }

    @Override
    public PCollection<KV<T, Long>> apply(PCollection<T> input) {
      return
          input
          .apply(ParDo.named("Init").of(new DoFn<T, KV<T, Void>>() {
            @Override
            public void processElement(ProcessContext c) {
              c.output(KV.of(c.element(), (Void) null));
            }
          }))
          .apply(Count.<T, Void>perKey());
    }
  }

  /**
   * A {@link CombineFn} that counts elements.
   */
  private static class CountFn<T> extends CombineFn<T, Long, Long> {

    @Override
    public Long createAccumulator() {
      return 0L;
    }

    @Override
    public Long addInput(Long accumulator, T input) {
      return accumulator + 1;
    }

    @Override
    public Long mergeAccumulators(Iterable<Long> accumulators) {
      long result = 0L;
      for (Long accum : accumulators) {
        result += accum;
      }
      return result;
    }

    @Override
    public Long extractOutput(Long accumulator) {
      return accumulator;
    }
  }
}
