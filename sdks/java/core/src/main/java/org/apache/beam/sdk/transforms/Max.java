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

import java.io.Serializable;
import java.util.Comparator;
import org.apache.beam.sdk.transforms.Combine.BinaryCombineFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@code PTransform}s for computing the maximum of the elements in a {@code PCollection}, or the
 * maximum of the values associated with each key in a {@code PCollection} of {@code KV}s.
 *
 * <p>Example 1: get the maximum of a {@code PCollection} of {@code Double}s.
 *
 * <pre>{@code
 * PCollection<Double> input = ...;
 * PCollection<Double> max = input.apply(Max.doublesGlobally());
 * }</pre>
 *
 * <p>Example 2: calculate the maximum of the {@code Integer}s associated with each unique key
 * (which is of type {@code String}).
 *
 * <pre>{@code
 * PCollection<KV<String, Integer>> input = ...;
 * PCollection<KV<String, Integer>> maxPerKey = input
 *     .apply(Max.<String>integersPerKey());
 * }</pre>
 */
public class Max {

  private Max() {
    // do not instantiate
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<Integer>} and returns a
   * {@code PCollection<Integer>} whose contents is the maximum of the input {@code PCollection}'s
   * elements, or {@code Integer.MIN_VALUE} if there are no elements.
   */
  public static Combine.Globally<Integer, Integer> integersGlobally() {
    return Combine.globally(new MaxIntegerFn());
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<KV<K, Integer>>} and
   * returns a {@code PCollection<KV<K, Integer>>} that contains an output element mapping each
   * distinct key in the input {@code PCollection} to the maximum of the values associated with that
   * key in the input {@code PCollection}.
   *
   * <p>See {@link Combine.PerKey} for how this affects timestamps and windowing.
   */
  public static <K> Combine.PerKey<K, Integer, Integer> integersPerKey() {
    return Combine.perKey(new MaxIntegerFn());
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<Long>} and returns a {@code
   * PCollection<Long>} whose contents is the maximum of the input {@code PCollection}'s elements,
   * or {@code Long.MIN_VALUE} if there are no elements.
   */
  public static Combine.Globally<Long, Long> longsGlobally() {
    return Combine.globally(new MaxLongFn());
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<KV<K, Long>>} and returns a
   * {@code PCollection<KV<K, Long>>} that contains an output element mapping each distinct key in
   * the input {@code PCollection} to the maximum of the values associated with that key in the
   * input {@code PCollection}.
   *
   * <p>See {@link Combine.PerKey} for how this affects timestamps and windowing.
   */
  public static <K> Combine.PerKey<K, Long, Long> longsPerKey() {
    return Combine.perKey(new MaxLongFn());
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<Double>} and returns a
   * {@code PCollection<Double>} whose contents is the maximum of the input {@code PCollection}'s
   * elements, or {@code Double.NEGATIVE_INFINITY} if there are no elements.
   */
  public static Combine.Globally<Double, Double> doublesGlobally() {
    return Combine.globally(new MaxDoubleFn());
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<KV<K, Double>>} and returns
   * a {@code PCollection<KV<K, Double>>} that contains an output element mapping each distinct key
   * in the input {@code PCollection} to the maximum of the values associated with that key in the
   * input {@code PCollection}.
   *
   * <p>See {@link Combine.PerKey} for how this affects timestamps and windowing.
   */
  public static <K> Combine.PerKey<K, Double, Double> doublesPerKey() {
    return Combine.perKey(new MaxDoubleFn());
  }

  /**
   * A {@code CombineFn} that computes the maximum of a collection of {@code Integer}s, useful as an
   * argument to {@link Combine#globally} or {@link Combine#perKey}.
   */
  public static Combine.BinaryCombineIntegerFn ofIntegers() {
    return new Max.MaxIntegerFn();
  }

  /**
   * A {@code CombineFn} that computes the maximum of a collection of {@code Long}s, useful as an
   * argument to {@link Combine#globally} or {@link Combine#perKey}.
   */
  public static Combine.BinaryCombineLongFn ofLongs() {
    return new Max.MaxLongFn();
  }

  /**
   * A {@code CombineFn} that computes the maximum of a collection of {@code Double}s, useful as an
   * argument to {@link Combine#globally} or {@link Combine#perKey}.
   */
  public static Combine.BinaryCombineDoubleFn ofDoubles() {
    return new Max.MaxDoubleFn();
  }

  /**
   * A {@code CombineFn} that computes the maximum of a collection of elements of type {@code T}
   * using an arbitrary {@link Comparator} and {@code identity}, useful as an argument to {@link
   * Combine#globally} or {@link Combine#perKey}.
   *
   * @param <T> the type of the values being compared
   */
  public static <T, ComparatorT extends Comparator<? super T> & Serializable> BinaryCombineFn<T> of(
      final T identity, final ComparatorT comparator) {
    return new MaxFn<>(identity, comparator);
  }

  /**
   * A {@code CombineFn} that computes the maximum of a collection of elements of type {@code T}
   * using an arbitrary {@link Comparator}, useful as an argument to {@link Combine#globally} or
   * {@link Combine#perKey}.
   *
   * @param <T> the type of the values being compared
   */
  public static <T, ComparatorT extends Comparator<? super T> & Serializable> BinaryCombineFn<T> of(
      final ComparatorT comparator) {
    return new MaxFn<>(null, comparator);
  }

  public static <T extends Comparable<? super T>> BinaryCombineFn<T> naturalOrder(T identity) {
    return new MaxFn<>(identity, new Top.Natural<>());
  }

  public static <T extends Comparable<? super T>> BinaryCombineFn<T> naturalOrder() {
    return new MaxFn<>(null, new Top.Natural<>());
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<T>} and returns a {@code
   * PCollection<T>} whose contents is the maximum according to the natural ordering of {@code T} of
   * the input {@code PCollection}'s elements, or {@code null} if there are no elements.
   */
  public static <T extends Comparable<? super T>> Combine.Globally<T, T> globally() {
    return Combine.globally(Max.<T>naturalOrder());
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<KV<K, T>>} and returns a
   * {@code PCollection<KV<K, T>>} that contains an output element mapping each distinct key in the
   * input {@code PCollection} to the maximum according to the natural ordering of {@code T} of the
   * values associated with that key in the input {@code PCollection}.
   *
   * <p>See {@link Combine.PerKey} for how this affects timestamps and windowing.
   */
  public static <K, T extends Comparable<? super T>> Combine.PerKey<K, T, T> perKey() {
    return Combine.perKey(Max.<T>naturalOrder());
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<T>} and returns a {@code
   * PCollection<T>} whose contents is the maximum of the input {@code PCollection}'s elements, or
   * {@code null} if there are no elements.
   */
  public static <T, ComparatorT extends Comparator<? super T> & Serializable>
      Combine.Globally<T, T> globally(ComparatorT comparator) {
    return Combine.globally(Max.of(comparator));
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<KV<K, T>>} and returns a
   * {@code PCollection<KV<K, T>>} that contains one output element per key mapping each to the
   * maximum of the values associated with that key in the input {@code PCollection}.
   *
   * <p>See {@link Combine.PerKey} for how this affects timestamps and windowing.
   */
  public static <K, T, ComparatorT extends Comparator<? super T> & Serializable>
      Combine.PerKey<K, T, T> perKey(ComparatorT comparator) {
    return Combine.perKey(Max.of(comparator));
  }

  /////////////////////////////////////////////////////////////////////////////

  private static class MaxFn<T> extends BinaryCombineFn<T> {

    private final @Nullable T identity;
    private final Comparator<? super T> comparator;

    private <ComparatorT extends Comparator<? super T> & Serializable> MaxFn(
        @Nullable T identity, ComparatorT comparator) {
      this.identity = identity;
      this.comparator = comparator;
    }

    @Override
    public T identity() {
      return identity;
    }

    @Override
    public T apply(T left, T right) {
      return comparator.compare(left, right) >= 0 ? left : right;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("comparer", comparator.getClass()).withLabel("Record Comparer"));
    }
  }

  private static class MaxIntegerFn extends Combine.BinaryCombineIntegerFn {

    @Override
    public int apply(int left, int right) {
      return Math.max(left, right);
    }

    @Override
    public int identity() {
      return Integer.MIN_VALUE;
    }
  }

  private static class MaxLongFn extends Combine.BinaryCombineLongFn {

    @Override
    public long apply(long left, long right) {
      return Math.max(left, right);
    }

    @Override
    public long identity() {
      return Long.MIN_VALUE;
    }
  }

  private static class MaxDoubleFn extends Combine.BinaryCombineDoubleFn {

    @Override
    public double apply(double left, double right) {
      return Math.max(left, right);
    }

    @Override
    public double identity() {
      return Double.NEGATIVE_INFINITY;
    }
  }
}
