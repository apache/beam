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

import org.apache.beam.sdk.transforms.Combine.BinaryCombineFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.common.Counter;
import org.apache.beam.sdk.util.common.Counter.AggregationKind;
import org.apache.beam.sdk.util.common.CounterProvider;

import java.io.Serializable;
import java.util.Comparator;

/**
 * {@code PTransform}s for computing the minimum of the elements in a {@code PCollection}, or the
 * minimum of the values associated with each key in a {@code PCollection} of {@code KV}s.
 *
 * <p>Example 1: get the minimum of a {@code PCollection} of {@code Double}s.
 * <pre> {@code
 * PCollection<Double> input = ...;
 * PCollection<Double> min = input.apply(Min.doublesGlobally());
 * } </pre>
 *
 * <p>Example 2: calculate the minimum of the {@code Integer}s
 * associated with each unique key (which is of type {@code String}).
 * <pre> {@code
 * PCollection<KV<String, Integer>> input = ...;
 * PCollection<KV<String, Integer>> minPerKey = input
 *     .apply(Min.<String>integersPerKey());
 * } </pre>
 */
public class Min {

  private Min() {
    // do not instantiate
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<Integer>} and returns a
   * {@code PCollection<Integer>} whose contents is a single value that is the minimum of the input
   * {@code PCollection}'s elements, or {@code Integer.MAX_VALUE} if there are no elements.
   */
  public static Combine.Globally<Integer, Integer> integersGlobally() {
    return Combine.globally(new MinIntegerFn()).named("Min.Globally");
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<KV<K, Integer>>} and
   * returns a {@code PCollection<KV<K, Integer>>} that contains an output element mapping each
   * distinct key in the input {@code PCollection} to the minimum of the values associated with that
   * key in the input {@code PCollection}.
   *
   * <p>See {@link Combine.PerKey} for how this affects timestamps and windowing.
   */
  public static <K> Combine.PerKey<K, Integer, Integer> integersPerKey() {
    return Combine.<K, Integer, Integer>perKey(new MinIntegerFn()).named("Min.PerKey");
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<Long>} and returns a {@code
   * PCollection<Long>} whose contents is the minimum of the input {@code PCollection}'s elements,
   * or {@code Long.MAX_VALUE} if there are no elements.
   */
  public static Combine.Globally<Long, Long> longsGlobally() {
    return Combine.globally(new MinLongFn()).named("Min.Globally");
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<KV<K, Long>>} and returns a
   * {@code PCollection<KV<K, Long>>} that contains an output element mapping each distinct key in
   * the input {@code PCollection} to the minimum of the values associated with that key in the
   * input {@code PCollection}.
   *
   * <p>See {@link Combine.PerKey} for how this affects timestamps and windowing.
   */
  public static <K> Combine.PerKey<K, Long, Long> longsPerKey() {
   return Combine.<K, Long, Long>perKey(new MinLongFn()).named("Min.PerKey");
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<Double>} and returns a
   * {@code PCollection<Double>} whose contents is the minimum of the input {@code PCollection}'s
   * elements, or {@code Double.POSITIVE_INFINITY} if there are no elements.
   */
  public static Combine.Globally<Double, Double> doublesGlobally() {
    return Combine.globally(new MinDoubleFn()).named("Min.Globally");
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<KV<K, Double>>} and returns
   * a {@code PCollection<KV<K, Double>>} that contains an output element mapping each distinct key
   * in the input {@code PCollection} to the minimum of the values associated with that key in the
   * input {@code PCollection}.
   *
   * <p>See {@link Combine.PerKey} for how this affects timestamps and windowing.
   */
  public static <K> Combine.PerKey<K, Double, Double> doublesPerKey() {
    return Combine.<K, Double, Double>perKey(new MinDoubleFn()).named("Min.PerKey");
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<T>} and returns a {@code
   * PCollection<T>} whose contents is the minimum according to the natural ordering of {@code T}
   * of the input {@code PCollection}'s elements, or {@code null} if there are no elements.
   */
  public static <T extends Comparable<? super T>>
  Combine.Globally<T, T> globally() {
    return Combine.<T, T>globally(MinFn.<T>naturalOrder()).named("Min.Globally");
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<KV<K, T>>} and returns a
   * {@code PCollection<KV<K, T>>} that contains an output element mapping each distinct key in the
   * input {@code PCollection} to the minimum according to the natural ordering of {@code T} of the
   * values associated with that key in the input {@code PCollection}.
   *
   * <p>See {@link Combine.PerKey} for how this affects timestamps and windowing.
   */
  public static <K, T extends Comparable<? super T>>
  Combine.PerKey<K, T, T> perKey() {
    return Combine.<K, T, T>perKey(MinFn.<T>naturalOrder()).named("Min.PerKey");
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<T>} and returns a {@code
   * PCollection<T>} whose contents is the minimum of the input {@code PCollection}'s elements, or
   * {@code null} if there are no elements.
   */
  public static <T, ComparatorT extends Comparator<? super T> & Serializable>
  Combine.Globally<T, T> globally(ComparatorT comparator) {
    return Combine.<T, T>globally(MinFn.of(comparator)).named("Min.Globally");
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<KV<K, T>>} and returns a
   * {@code PCollection<KV<K, T>>} that contains one output element per key mapping each
   * to the minimum of the values associated with that key in the input {@code PCollection}.
   *
   * <p>See {@link Combine.PerKey} for how this affects timestamps and windowing.
   */
  public static <K, T, ComparatorT extends Comparator<? super T> & Serializable>
  Combine.PerKey<K, T, T> perKey(ComparatorT comparator) {
    return Combine.<K, T, T>perKey(MinFn.of(comparator)).named("Min.PerKey");
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * A {@code CombineFn} that computes the maximum of a collection of elements of type {@code T}
   * using an arbitrary {@link Comparator}, useful as an argument to {@link Combine#globally} or
   * {@link Combine#perKey}.
   *
   * @param <T> the type of the values being compared
   */
  public static class MinFn<T> extends BinaryCombineFn<T> {

    private final T identity;
    private final Comparator<? super T> comparator;

    private <ComparatorT extends Comparator<? super T> & Serializable> MinFn(
        T identity, ComparatorT comparator) {
      this.identity = identity;
      this.comparator = comparator;
    }

    public static <T, ComparatorT extends Comparator<? super T> & Serializable>
    MinFn<T> of(T identity, ComparatorT comparator) {
      return new MinFn<T>(identity, comparator);
    }

    public static <T, ComparatorT extends Comparator<? super T> & Serializable>
    MinFn<T> of(ComparatorT comparator) {
      return new MinFn<T>(null, comparator);
    }

    public static <T extends Comparable<? super T>> MinFn<T> naturalOrder(T identity) {
      return new MinFn<T>(identity, new Top.Largest<T>());
    }

    public static <T extends Comparable<? super T>> MinFn<T> naturalOrder() {
      return new MinFn<T>(null, new Top.Largest<T>());
    }

    @Override
    public T identity() {
      return identity;
    }

    @Override
    public T apply(T left, T right) {
      return comparator.compare(left, right) <= 0 ? left : right;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("comparer", comparator.getClass())
        .withLabel("Record Comparer"));
    }
  }

  /**
   * A {@code CombineFn} that computes the minimum of a collection of {@code Integer}s, useful as an
   * argument to {@link Combine#globally} or {@link Combine#perKey}.
   */
  public static class MinIntegerFn extends Combine.BinaryCombineIntegerFn
      implements CounterProvider<Integer> {

    @Override
    public int apply(int left, int right) {
      return left <= right ? left : right;
    }

    @Override
    public int identity() {
      return Integer.MAX_VALUE;
    }

    @Override
    public Counter<Integer> getCounter(String name) {
      return Counter.ints(name, AggregationKind.MIN);
    }
  }

  /**
   * A {@code CombineFn} that computes the minimum of a collection of {@code Long}s, useful as an
   * argument to {@link Combine#globally} or {@link Combine#perKey}.
   */
  public static class MinLongFn extends Combine.BinaryCombineLongFn
      implements CounterProvider<Long> {

    @Override
    public long apply(long left, long right) {
      return left <= right ? left : right;
    }

    @Override
    public long identity() {
      return Long.MAX_VALUE;
    }

    @Override
    public Counter<Long> getCounter(String name) {
      return Counter.longs(name, AggregationKind.MIN);
    }
  }

  /**
   * A {@code CombineFn} that computes the minimum of a collection of {@code Double}s, useful as an
   * argument to {@link Combine#globally} or {@link Combine#perKey}.
   */
  public static class MinDoubleFn extends Combine.BinaryCombineDoubleFn
      implements CounterProvider<Double> {

    @Override
    public double apply(double left, double right) {
      return left <= right ? left : right;
    }

    @Override
    public double identity() {
      return Double.POSITIVE_INFINITY;
    }

    @Override
    public Counter<Double> getCounter(String name) {
      return Counter.doubles(name, AggregationKind.MIN);
    }
  }
}
