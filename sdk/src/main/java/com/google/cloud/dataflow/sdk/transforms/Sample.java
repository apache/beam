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

import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.coders.IterableCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * {@code PTransform}s for taking samples of the elements in a
 * {@code PCollection}, or samples of the values associated with each
 * key in a {@code PCollection} of {@code KV}s.
 **/
public class Sample {
  /**
   * Returns a {@code PTransform} that takes a {@code PCollection<T>},
   * selects {@code sampleSize} elements, uniformly at random, and returns a
   * {@code PCollection<Iterable<T>>} containing the selected elements.
   * If the input {@code PCollection} has fewer than
   * {@code sampleSize} elements, then the output {@code Iterable<T>}
   * will be all the input's elements.
   *
   * <p> Example of use:
   * <pre> {@code
   * PCollection<String> pc = ...;
   * PCollection<Iterable<String>> sampleOfSize10 =
   *     pc.apply(Sample.fixedSizeGlobally(10));
   * } </pre>
   *
   * @param sampleSize the number of elements to select; must be {@code >= 0}
   * @param <T> the type of the elements
   */
  public static <T> PTransform<PCollection<T>, PCollection<Iterable<T>>>
      fixedSizeGlobally(int sampleSize) {
    return Combine.globally(new FixedSizedSampleFn<T>(sampleSize));
  }

  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<KV<K, V>>} and returns a
   * {@code PCollection<KV<K, Iterable<V>>>} that contains an output
   * element mapping each distinct key in the input
   * {@code PCollection} to a sample of {@code sampleSize} values
   * associated with that key in the input {@code PCollection}, taken
   * uniformly at random.  If a key in the input {@code PCollection}
   * has fewer than {@code sampleSize} values associated with it, then
   * the output {@code Iterable<V>} associated with that key will be
   * all the values associated with that key in the input
   * {@code PCollection}.
   *
   * <p> Example of use:
   * <pre> {@code
   * PCollection<KV<String, Integer>> pc = ...;
   * PCollection<KV<String, Iterable<Integer>>> sampleOfSize10PerKey =
   *     pc.apply(Sample.<String, Integer>fixedSizePerKey());
   * } </pre>
   *
   * @param sampleSize the number of values to select for each
   * distinct key; must be {@code >= 0}
   * @param <K> the type of the keys
   * @param <V> the type of the values
   */
  public static <K, V> PTransform<PCollection<KV<K, V>>,
                                  PCollection<KV<K, Iterable<V>>>>
      fixedSizePerKey(int sampleSize) {
    return Combine.perKey(new FixedSizedSampleFn<V>(sampleSize));
  }


  /////////////////////////////////////////////////////////////////////////////

  /**
   * {@code CombineFn} that computes a fixed-size sample of a
   * collection of values.
   *
   * @param <T> the type of the elements
   */
  public static class FixedSizedSampleFn<T>
      extends CombineFn<T, Top.TopCombineFn<KV<Integer, T>>.Heap, Iterable<T>> {
    private final Top.TopCombineFn<KV<Integer, T>> topCombineFn;
    private final Random rand = new Random();

    private FixedSizedSampleFn(int sampleSize) {
      if (sampleSize < 0) {
        throw new IllegalArgumentException("sample size must be >= 0");
      }
      topCombineFn = new Top.TopCombineFn<>(sampleSize,
                                            new KV.OrderByKey<Integer, T>());
    }

    @Override
    public Top.TopCombineFn<KV<Integer, T>>.Heap createAccumulator() {
      return topCombineFn.createAccumulator();
    }

    @Override
    public void addInput(Top.TopCombineFn<KV<Integer, T>>.Heap accumulator,
                         T input) {
      accumulator.addInput(KV.of(rand.nextInt(), input));
    }

    @Override
    public Top.TopCombineFn<KV<Integer, T>>.Heap mergeAccumulators(
        Iterable<Top.TopCombineFn<KV<Integer, T>>.Heap> accumulators) {
      return topCombineFn.mergeAccumulators(accumulators);
    }

    @Override
    public Iterable<T> extractOutput(
        Top.TopCombineFn<KV<Integer, T>>.Heap accumulator) {
      List<T> out = new ArrayList<>();
      for (KV<Integer, T> element : accumulator.extractOutput()) {
        out.add(element.getValue());
      }
      return out;
    }

    @Override
    public Coder<Top.TopCombineFn<KV<Integer, T>>.Heap> getAccumulatorCoder(
        CoderRegistry registry, Coder<T> inputCoder) {
      return topCombineFn.getAccumulatorCoder(
          registry, KvCoder.of(BigEndianIntegerCoder.of(), inputCoder));
    }

    @Override
    public Coder<Iterable<T>> getDefaultOutputCoder(
        CoderRegistry registry, Coder<T> inputCoder) {
      return IterableCoder.of(inputCoder);
    }
  }
}
