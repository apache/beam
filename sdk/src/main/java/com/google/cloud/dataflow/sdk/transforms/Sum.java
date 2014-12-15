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
 * {@code PTransform}s for computing the sum of the elements in a
 * {@code PCollection}, or the sum of the values associated with
 * each key in a {@code PCollection} of {@code KV}s.
 *
 * <p> Example 1: get the sum of a {@code PCollection} of {@code Double}s.
 * <pre> {@code
 * PCollection<Double> input = ...;
 * PCollection<Double> sum = input.apply(Sum.doublesGlobally());
 * } </pre>
 *
 * <p> Example 2: calculate the sum of the {@code Integer}s
 * associated with each unique key (which is of type {@code String}).
 * <pre> {@code
 * PCollection<KV<String, Integer>> input = ...;
 * PCollection<KV<String, Integer>> sumPerKey = input
 *     .apply(Sum.<String>integersPerKey());
 * } </pre>
 */
@SuppressWarnings("serial")
public class Sum {

  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<Integer>} and returns a
   * {@code PCollection<Integer>} whose contents is the sum of the
   * input {@code PCollection}'s elements, or
   * {@code 0} if there are no elements.
   */
  public static Combine.Globally<Integer, Integer> integersGlobally() {
    Combine.Globally<Integer, Integer> combine = Combine
        .globally(new SumIntegerFn());
    combine.setName("Sum");
    return combine;
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
    Combine.PerKey<K, Integer, Integer> combine = Combine
        .perKey(new SumIntegerFn());
    combine.setName("Sum.PerKey");
    return combine;
  }

  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<Long>} and returns a
   * {@code PCollection<Long>} whose contents is the sum of the
   * input {@code PCollection}'s elements, or
   * {@code 0} if there are no elements.
   */
  public static Combine.Globally<Long, Long> longsGlobally() {
    Combine.Globally<Long, Long> combine = Combine.globally(new SumLongFn());
    combine.setName("Sum");
    return combine;
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
    Combine.PerKey<K, Long, Long> combine = Combine
        .perKey(new SumLongFn());
    combine.setName("Sum.PerKey");
    return combine;
  }

  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<Double>} and returns a
   * {@code PCollection<Double>} whose contents is the sum of the
   * input {@code PCollection}'s elements, or
   * {@code 0} if there are no elements.
   */
  public static Combine.Globally<Double, Double> doublesGlobally() {
    Combine.Globally<Double, Double> combine = Combine
        .globally(new SumDoubleFn());
    combine.setName("Sum");
    return combine;
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
    Combine.PerKey<K, Double, Double> combine = Combine
        .perKey(new SumDoubleFn());
    combine.setName("Sum.PerKey");
    return combine;
  }


  /////////////////////////////////////////////////////////////////////////////

  /**
   * A {@code SerializableFunction} that computes the sum of an
   * {@code Iterable} of {@code Integer}s, useful as an argument to
   * {@link Combine#globally} or {@link Combine#perKey}.
   */
  public static class SumIntegerFn
      implements SerializableFunction<Iterable<Integer>, Integer> {
    @Override
    public Integer apply(Iterable<Integer> input) {
      int sum = 0;
      for (int value : input) {
        sum += value;
      }
      return sum;
    }
  }

  /**
   * A {@code SerializableFunction} that computes the sum of an
   * {@code Iterable} of {@code Long}s, useful as an argument to
   * {@link Combine#globally} or {@link Combine#perKey}.
   */
  public static class SumLongFn
      implements SerializableFunction<Iterable<Long>, Long> {
    @Override
    public Long apply(Iterable<Long> input) {
      long sum = 0;
      for (long value : input) {
        sum += value;
      }
      return sum;
    }
  }

  /**
   * A {@code SerializableFunction} that computes the sum of an
   * {@code Iterable} of {@code Double}s, useful as an argument to
   * {@link Combine#globally} or {@link Combine#perKey}.
   */
  public static class SumDoubleFn
      implements SerializableFunction<Iterable<Double>, Double> {
    @Override
    public Double apply(Iterable<Double> input) {
      double sum = 0;
      for (double value : input) {
        sum += value;
      }
      return sum;
    }
  }
}
