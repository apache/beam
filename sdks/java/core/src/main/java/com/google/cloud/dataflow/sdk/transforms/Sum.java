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

import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind;
import com.google.cloud.dataflow.sdk.util.common.CounterProvider;

/**
 * {@code PTransform}s for computing the sum of the elements in a
 * {@code PCollection}, or the sum of the values associated with
 * each key in a {@code PCollection} of {@code KV}s.
 *
 * <p>Example 1: get the sum of a {@code PCollection} of {@code Double}s.
 * <pre> {@code
 * PCollection<Double> input = ...;
 * PCollection<Double> sum = input.apply(Sum.doublesGlobally());
 * } </pre>
 *
 * <p>Example 2: calculate the sum of the {@code Integer}s
 * associated with each unique key (which is of type {@code String}).
 * <pre> {@code
 * PCollection<KV<String, Integer>> input = ...;
 * PCollection<KV<String, Integer>> sumPerKey = input
 *     .apply(Sum.<String>integersPerKey());
 * } </pre>
 */
public class Sum {

  private Sum() {
    // do not instantiate
  }

  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<Integer>} and returns a
   * {@code PCollection<Integer>} whose contents is the sum of the
   * input {@code PCollection}'s elements, or
   * {@code 0} if there are no elements.
   */
  public static Combine.Globally<Integer, Integer> integersGlobally() {
    return Combine.globally(new SumIntegerFn()).named("Sum.Globally");
  }

  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<KV<K, Integer>>} and returns a
   * {@code PCollection<KV<K, Integer>>} that contains an output
   * element mapping each distinct key in the input
   * {@code PCollection} to the sum of the values associated with
   * that key in the input {@code PCollection}.
   */
  public static <K> Combine.PerKey<K, Integer, Integer> integersPerKey() {
    return Combine.<K, Integer, Integer>perKey(new SumIntegerFn()).named("Sum.PerKey");
  }

  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<Long>} and returns a
   * {@code PCollection<Long>} whose contents is the sum of the
   * input {@code PCollection}'s elements, or
   * {@code 0} if there are no elements.
   */
  public static Combine.Globally<Long, Long> longsGlobally() {
    return Combine.globally(new SumLongFn()).named("Sum.Globally");
  }

  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<KV<K, Long>>} and returns a
   * {@code PCollection<KV<K, Long>>} that contains an output
   * element mapping each distinct key in the input
   * {@code PCollection} to the sum of the values associated with
   * that key in the input {@code PCollection}.
   */
  public static <K> Combine.PerKey<K, Long, Long> longsPerKey() {
    return Combine.<K, Long, Long>perKey(new SumLongFn()).named("Sum.PerKey");
  }

  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<Double>} and returns a
   * {@code PCollection<Double>} whose contents is the sum of the
   * input {@code PCollection}'s elements, or
   * {@code 0} if there are no elements.
   */
  public static Combine.Globally<Double, Double> doublesGlobally() {
    return Combine.globally(new SumDoubleFn()).named("Sum.Globally");
  }

  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<KV<K, Double>>} and returns a
   * {@code PCollection<KV<K, Double>>} that contains an output
   * element mapping each distinct key in the input
   * {@code PCollection} to the sum of the values associated with
   * that key in the input {@code PCollection}.
   */
  public static <K> Combine.PerKey<K, Double, Double> doublesPerKey() {
    return Combine.<K, Double, Double>perKey(new SumDoubleFn()).named("Sum.PerKey");
  }


  /////////////////////////////////////////////////////////////////////////////

  /**
   * A {@code SerializableFunction} that computes the sum of an
   * {@code Iterable} of {@code Integer}s, useful as an argument to
   * {@link Combine#globally} or {@link Combine#perKey}.
   */
  public static class SumIntegerFn
      extends Combine.BinaryCombineIntegerFn implements CounterProvider<Integer> {
    @Override
    public int apply(int a, int b) {
      return a + b;
    }

    @Override
    public int identity() {
      return 0;
    }

    @Override
    public Counter<Integer> getCounter(String name) {
      return Counter.ints(name, AggregationKind.SUM);
    }
  }

  /**
   * A {@code SerializableFunction} that computes the sum of an
   * {@code Iterable} of {@code Long}s, useful as an argument to
   * {@link Combine#globally} or {@link Combine#perKey}.
   */
  public static class SumLongFn
      extends Combine.BinaryCombineLongFn implements CounterProvider<Long> {
    @Override
    public long apply(long a, long b) {
      return a + b;
    }

    @Override
    public long identity() {
      return 0;
    }

    @Override
    public Counter<Long> getCounter(String name) {
      return Counter.longs(name, AggregationKind.SUM);
    }
  }

  /**
   * A {@code SerializableFunction} that computes the sum of an
   * {@code Iterable} of {@code Double}s, useful as an argument to
   * {@link Combine#globally} or {@link Combine#perKey}.
   */
  public static class SumDoubleFn
      extends Combine.BinaryCombineDoubleFn implements CounterProvider<Double> {
    @Override
    public double apply(double a, double b) {
      return a + b;
    }

    @Override
    public double identity() {
      return 0;
    }

    @Override
    public Counter<Double> getCounter(String name) {
      return Counter.doubles(name, AggregationKind.SUM);
    }
  }
}
