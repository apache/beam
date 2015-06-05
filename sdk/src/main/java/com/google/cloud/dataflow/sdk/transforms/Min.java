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
 * {@code PTransform}s for computing the minimum of the elements in a
 * {@code PCollection}, or the minimum of the values associated with
 * each key in a {@code PCollection} of {@code KV}s.
 *
 * <p> Example 1: get the minimum of a {@code PCollection} of {@code Double}s.
 * <pre> {@code
 * PCollection<Double> input = ...;
 * PCollection<Double> min = input.apply(Min.doublesGlobally());
 * } </pre>
 *
 * <p> Example 2: calculate the minimum of the {@code Integer}s
 * associated with each unique key (which is of type {@code String}).
 * <pre> {@code
 * PCollection<KV<String, Integer>> input = ...;
 * PCollection<KV<String, Integer>> minPerKey = input
 *     .apply(Min.<String>integersPerKey());
 * } </pre>
 */
public class Min {

  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<Integer>} and returns a
   * {@code PCollection<Integer>} whose contents is a single value that is
   * the minimum of the input {@code PCollection}'s elements, or
   * {@code Integer.MAX_VALUE} if there are no elements.
   */
  public static Combine.Globally<Integer, Integer> integersGlobally() {
    return Combine.globally(new MinIntegerFn()).named("Min");
  }

  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<KV<K, Integer>>} and returns a
   * {@code PCollection<KV<K, Integer>>} that contains an output
   * element mapping each distinct key in the input
   * {@code PCollection} to the minimum of the values associated with
   * that key in the input {@code PCollection}.
   *
   * <p> See {@link Combine.PerKey} for how this affects timestamps and windowing.
   */
  public static <K> Combine.PerKey<K, Integer, Integer> integersPerKey() {
    return Combine.<K, Integer, Integer>perKey(new MinIntegerFn()).named("Min.PerKey");
  }

  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<Long>} and returns a
   * {@code PCollection<Long>} whose contents is the minimum of the
   * input {@code PCollection}'s elements, or
   * {@code Long.MAX_VALUE} if there are no elements.
   */
  public static Combine.Globally<Long, Long> longsGlobally() {
    return Combine.globally(new MinLongFn()).named("Min");
  }

  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<KV<K, Long>>} and returns a
   * {@code PCollection<KV<K, Long>>} that contains an output
   * element mapping each distinct key in the input
   * {@code PCollection} to the minimum of the values associated with
   * that key in the input {@code PCollection}.
   *
   * <p> See {@link Combine.PerKey} for how this affects timestamps and windowing.
   */
  public static <K> Combine.PerKey<K, Long, Long> longsPerKey() {
   return Combine.<K, Long, Long>perKey(new MinLongFn()).named("Min.PerKey");
  }

  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<Double>} and returns a
   * {@code PCollection<Double>} whose contents is the minimum of the
   * input {@code PCollection}'s elements, or
   * {@code Double.POSITIVE_INFINITY} if there are no elements.
   */
  public static Combine.Globally<Double, Double> doublesGlobally() {
    return Combine.globally(new MinDoubleFn()).named("Min");
  }

  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<KV<K, Double>>} and returns a
   * {@code PCollection<KV<K, Double>>} that contains an output
   * element mapping each distinct key in the input
   * {@code PCollection} to the minimum of the values associated with
   * that key in the input {@code PCollection}.
   *
   * <p> See {@link Combine.PerKey} for how this affects timestamps and windowing.
   */
  public static <K> Combine.PerKey<K, Double, Double> doublesPerKey() {
    return Combine.<K, Double, Double>perKey(new MinDoubleFn()).named("Min.PerKey");
  }


  /////////////////////////////////////////////////////////////////////////////

  /**
   * A {@code CombineFn} that computes the minimum of a collection
   * of elements of type {@code N}, useful as an
   * argument to {@link Combine#globally} or {@link Combine#perKey}.
   *
   * @param <NumT> the type of the {@code Number}s being compared
   */
  public static class MinFn<NumT extends Comparable<NumT>>
      extends Combine.BinaryCombineFn<NumT> {
    private static final long serialVersionUID = 0;

    /** The largest value of type NumT. */
    private final NumT initialValue;

    /**
     * Constructs a combining function that computes the minimum over
     * a collection of values of type {@code N}, given the largest
     * value of type {@code N}, which is the identity value for the
     * minimum operation over {@code N}s.
     */
    public MinFn(NumT initialValue) {
      this.initialValue = initialValue;
    }

    @Override
    public NumT apply(NumT a, NumT b) {
      return a.compareTo(b) <= 0 ? a : b;
    }

    @Override
    public NumT identity() {
      return initialValue;
    }
  }

  /**
   * A {@code CombineFn} that computes the minimum of a collection
   * of {@code Integer}s, useful as an argument to
   * {@link Combine#globally} or {@link Combine#perKey}.
   */
  public static class MinIntegerFn extends MinFn<Integer> implements
      CounterProvider<Integer> {
    private static final long serialVersionUID = 0;

    public MinIntegerFn() {
      super(Integer.MAX_VALUE);
    }

    @Override
    public Counter<Integer> getCounter(String name) {
      return Counter.ints(name, AggregationKind.MIN);
    }
  }

  /**
   * A {@code CombineFn} that computes the minimum of a collection
   * of {@code Long}s, useful as an argument to
   * {@link Combine#globally} or {@link Combine#perKey}.
   */
  public static class MinLongFn extends MinFn<Long> implements
      CounterProvider<Long> {
    private static final long serialVersionUID = 0;

    public MinLongFn() {
      super(Long.MAX_VALUE);
    }

    @Override
    public Counter<Long> getCounter(String name) {
      return Counter.longs(name, AggregationKind.MIN);
    }
  }

  /**
   * A {@code CombineFn} that computes the minimum of a collection
   * of {@code Double}s, useful as an argument to
   * {@link Combine#globally} or {@link Combine#perKey}.
   */
  public static class MinDoubleFn extends MinFn<Double> implements
      CounterProvider<Double> {
    private static final long serialVersionUID = 0;

    public MinDoubleFn() {
      super(Double.POSITIVE_INFINITY);
    }

    @Override
    public Counter<Double> getCounter(String name) {
      return Counter.doubles(name, AggregationKind.MIN);
    }
  }
}
