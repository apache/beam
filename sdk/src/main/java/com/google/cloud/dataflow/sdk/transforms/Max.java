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

/**
 * {@code PTransform}s for computing the maximum of the elements in a
 * {@code PCollection}, or the maximum of the values associated with
 * each key in a {@code PCollection} of {@code KV}s.
 *
 * <p> Example 1: get the maximum of a {@code PCollection} of {@code Double}s.
 * <pre> {@code
 * PCollection<Double> input = ...;
 * PCollection<Double> max = input.apply(Max.doublesGlobally());
 * } </pre>
 *
 * <p> Example 2: calculate the maximum of the {@code Integer}s
 * associated with each unique key (which is of type {@code String}).
 * <pre> {@code
 * PCollection<KV<String, Integer>> input = ...;
 * PCollection<KV<String, Integer>> maxPerKey = input
 *     .apply(Max.<String>integersPerKey());
 * } </pre>
 */
public class Max {

  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<Integer>} and returns a
   * {@code PCollection<Integer>} whose contents is the maximum of the
   * input {@code PCollection}'s elements, or
   * {@code Integer.MIN_VALUE} if there are no elements.
   */
  public static Combine.Globally<Integer, Integer> integersGlobally() {
    Combine.Globally<Integer, Integer> combine = Combine
        .globally(new MaxIntegerFn());
    combine.setName("Max");
    return combine;
  }

  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<KV<K, Integer>>} and returns a
   * {@code PCollection<KV<K, Integer>>} that contains an output
   * element mapping each distinct key in the input
   * {@code PCollection} to the maximum of the values associated with
   * that key in the input {@code PCollection}.
   *
   * <p> See {@link Combine.PerKey} for how this affects timestamps and windowing.
   */
  public static <K> Combine.PerKey<K, Integer, Integer> integersPerKey() {
    Combine.PerKey<K, Integer, Integer> combine = Combine
        .perKey(new MaxIntegerFn());
    combine.setName("Max.PerKey");
    return combine;
  }

  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<Long>} and returns a
   * {@code PCollection<Long>} whose contents is the maximum of the
   * input {@code PCollection}'s elements, or
   * {@code Long.MIN_VALUE} if there are no elements.
   */
  public static Combine.Globally<Long, Long> longsGlobally() {
    Combine.Globally<Long, Long> combine = Combine.globally(new MaxLongFn());
    combine.setName("Max");
    return combine;
  }

  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<KV<K, Long>>} and returns a
   * {@code PCollection<KV<K, Long>>} that contains an output
   * element mapping each distinct key in the input
   * {@code PCollection} to the maximum of the values associated with
   * that key in the input {@code PCollection}.
   *
   * <p> See {@link Combine.PerKey} for how this affects timestamps and windowing.
   */
  public static <K> Combine.PerKey<K, Long, Long> longsPerKey() {
    Combine.PerKey<K, Long, Long> combine = Combine
        .perKey(new MaxLongFn());
    combine.setName("Max.PerKey");
    return combine;
  }

  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<Double>} and returns a
   * {@code PCollection<Double>} whose contents is the maximum of the
   * input {@code PCollection}'s elements, or
   * {@code Double.MIN_VALUE} if there are no elements.
   */
  public static Combine.Globally<Double, Double> doublesGlobally() {
    Combine.Globally<Double, Double> combine = Combine
        .globally(new MaxDoubleFn());
    combine.setName("Max");
    return combine;
  }

  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<KV<K, Double>>} and returns a
   * {@code PCollection<KV<K, Double>>} that contains an output
   * element mapping each distinct key in the input
   * {@code PCollection} to the maximum of the values associated with
   * that key in the input {@code PCollection}.
   *
   * <p> See {@link Combine.PerKey} for how this affects timestamps and windowing.
   */
  public static <K> Combine.PerKey<K, Double, Double> doublesPerKey() {
    Combine.PerKey<K, Double, Double> combine = Combine
        .perKey(new MaxDoubleFn());
    combine.setName("Max.PerKey");
    return combine;
  }


  /////////////////////////////////////////////////////////////////////////////

  /**
   * A {@code SerializableFunction} that computes the maximum of an
   * {@code Iterable} of numbers of type {@code N}, useful as an
   * argument to {@link Combine#globally} or {@link Combine#perKey}.
   *
   * @param <N> the type of the {@code Number}s being compared
   */
  @SuppressWarnings("serial")
  public static class MaxFn<N extends Number & Comparable<N>>
      implements SerializableFunction<Iterable<N>, N> {

    /** The smallest value of type N. */
    private final N initialValue;

    /**
     * Constructs a combining function that computes the maximum over
     * a collection of values of type {@code N}, given the smallest
     * value of type {@code N}, which is the identity value for the
     * maximum operation over {@code N}s.
     */
    public MaxFn(N initialValue) {
      this.initialValue = initialValue;
    }

    @Override
    public N apply(Iterable<N> input) {
      N max = initialValue;
      for (N value : input) {
        if (value.compareTo(max) > 0) {
          max = value;
        }
      }
      return max;
    }
  }

  /**
   * A {@code SerializableFunction} that computes the maximum of an
   * {@code Iterable} of {@code Integer}s, useful as an argument to
   * {@link Combine#globally} or {@link Combine#perKey}.
   */
  @SuppressWarnings("serial")
  public static class MaxIntegerFn extends MaxFn<Integer> {
    public MaxIntegerFn() { super(Integer.MIN_VALUE); }
  }

  /**
   * A {@code SerializableFunction} that computes the maximum of an
   * {@code Iterable} of {@code Long}s, useful as an argument to
   * {@link Combine#globally} or {@link Combine#perKey}.
   */
  @SuppressWarnings("serial")
  public static class MaxLongFn extends MaxFn<Long> {
    public MaxLongFn() { super(Long.MIN_VALUE); }
  }

  /**
   * A {@code SerializableFunction} that computes the maximum of an
   * {@code Iterable} of {@code Double}s, useful as an argument to
   * {@link Combine#globally} or {@link Combine#perKey}.
   */
  @SuppressWarnings("serial")
  public static class MaxDoubleFn extends MaxFn<Double> {
    public MaxDoubleFn() { super(Double.MIN_VALUE); }
  }
}
