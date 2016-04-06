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
package com.google.cloud.dataflow.sdk.transforms;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.values.PCollection;

/**
 * {@code PTransform}s for filtering from a {@code PCollection} the
 * elements satisfying a predicate, or satisfying an inequality with
 * a given value based on the elements' natural ordering.
 *
 * @param <T> the type of the values in the input {@code PCollection},
 * and the type of the elements in the output {@code PCollection}
 */
public class Filter<T> extends PTransform<PCollection<T>, PCollection<T>> {

  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<T>} and returns a {@code PCollection<T>} with
   * elements that satisfy the given predicate.  The predicate must be
   * a {@code SerializableFunction<T, Boolean>}.
   *
   * <p>Example of use:
   * <pre> {@code
   * PCollection<String> wordList = ...;
   * PCollection<String> longWords =
   *     wordList.apply(Filter.byPredicate(new MatchIfWordLengthGT(6)));
   * } </pre>
   *
   * <p>See also {@link #lessThan}, {@link #lessThanEq},
   * {@link #greaterThan}, {@link #greaterThanEq}, which return elements
   * satisfying various inequalities with the specified value based on
   * the elements' natural ordering.
   */
  public static <T, PredicateT extends SerializableFunction<T, Boolean>> Filter<T>
  byPredicate(PredicateT predicate) {
    return new Filter<T>("Filter", predicate);
  }

  /**
   * @deprecated use {@link #byPredicate}, which returns a {@link Filter} transform instead of
   * a {@link ParDo.Bound}.
   */
  @Deprecated
  public static <T, PredicateT extends SerializableFunction<T, Boolean>> ParDo.Bound<T, T>
  by(final PredicateT filterPred) {
    return ParDo.named("Filter").of(new DoFn<T, T>() {
      @Override
      public void processElement(ProcessContext c) {
        if (filterPred.apply(c.element()) == true) {
          c.output(c.element());
        }
      }
    });
  }

  /**
   * Returns a {@code PTransform} that takes an input
   * {@link PCollection} and returns a {@link PCollection} with
   * elements that are less than a given value, based on the
   * elements' natural ordering. Elements must be {@code Comparable}.
   *
   * <p>Example of use:
   * <pre> {@code
   * PCollection<Integer> listOfNumbers = ...;
   * PCollection<Integer> smallNumbers =
   *     listOfNumbers.apply(Filter.lessThan(10));
   * } </pre>
   *
   * <p>See also {@link #lessThanEq}, {@link #greaterThanEq},
   * and {@link #greaterThan}, which return elements satisfying various
   * inequalities with the specified value based on the elements'
   * natural ordering.
   *
   * <p>See also {@link #byPredicate}, which returns elements
   * that satisfy the given predicate.
   */
  public static <T extends Comparable<T>> ParDo.Bound<T, T> lessThan(final T value) {
    return ParDo.named("Filter.lessThan").of(new DoFn<T, T>() {
      @Override
      public void processElement(ProcessContext c) {
        if (c.element().compareTo(value) < 0) {
          c.output(c.element());
        }
      }
    });
  }

  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<T>} and returns a {@code PCollection<T>} with
   * elements that are greater than a given value, based on the
   * elements' natural ordering. Elements must be {@code Comparable}.
   *
   * <p>Example of use:
   * <pre> {@code
   * PCollection<Integer> listOfNumbers = ...;
   * PCollection<Integer> largeNumbers =
   *     listOfNumbers.apply(Filter.greaterThan(1000));
   * } </pre>
   *
   * <p>See also {@link #greaterThanEq}, {@link #lessThan},
   * and {@link #lessThanEq}, which return elements satisfying various
   * inequalities with the specified value based on the elements'
   * natural ordering.
   *
   * <p>See also {@link #byPredicate}, which returns elements
   * that satisfy the given predicate.
   */
  public static <T extends Comparable<T>> ParDo.Bound<T, T> greaterThan(final T value) {
    return ParDo.named("Filter.greaterThan").of(new DoFn<T, T>() {
      @Override
      public void processElement(ProcessContext c) {
        if (c.element().compareTo(value) > 0) {
          c.output(c.element());
        }
      }
    });
  }

  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<T>} and returns a {@code PCollection<T>} with
   * elements that are less than or equal to a given value, based on the
   * elements' natural ordering. Elements must be {@code Comparable}.
   *
   * <p>Example of use:
   * <pre> {@code
   * PCollection<Integer> listOfNumbers = ...;
   * PCollection<Integer> smallOrEqualNumbers =
   *     listOfNumbers.apply(Filter.lessThanEq(10));
   * } </pre>
   *
   * <p>See also {@link #lessThan}, {@link #greaterThanEq},
   * and {@link #greaterThan}, which return elements satisfying various
   * inequalities with the specified value based on the elements'
   * natural ordering.
   *
   * <p>See also {@link #byPredicate}, which returns elements
   * that satisfy the given predicate.
   */
  public static <T extends Comparable<T>> ParDo.Bound<T, T> lessThanEq(final T value) {
    return ParDo.named("Filter.lessThanEq").of(new DoFn<T, T>() {
      @Override
      public void processElement(ProcessContext c) {
        if (c.element().compareTo(value) <= 0) {
          c.output(c.element());
        }
      }
    });
  }

  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<T>} and returns a {@code PCollection<T>} with
   * elements that are greater than or equal to a given value, based on
   * the elements' natural ordering. Elements must be {@code Comparable}.
   *
   * <p>Example of use:
   * <pre> {@code
   * PCollection<Integer> listOfNumbers = ...;
   * PCollection<Integer> largeOrEqualNumbers =
   *     listOfNumbers.apply(Filter.greaterThanEq(1000));
   * } </pre>
   *
   * <p>See also {@link #greaterThan}, {@link #lessThan},
   * and {@link #lessThanEq}, which return elements satisfying various
   * inequalities with the specified value based on the elements'
   * natural ordering.
   *
   * <p>See also {@link #byPredicate}, which returns elements
   * that satisfy the given predicate.
   */
  public static <T extends Comparable<T>> ParDo.Bound<T, T> greaterThanEq(final T value) {
    return ParDo.named("Filter.greaterThanEq").of(new DoFn<T, T>() {
      @Override
      public void processElement(ProcessContext c) {
        if (c.element().compareTo(value) >= 0) {
          c.output(c.element());
        }
      }
    });
  }

  ///////////////////////////////////////////////////////////////////////////////

  private SerializableFunction<T, Boolean> predicate;

  private Filter(SerializableFunction<T, Boolean> predicate) {
    this.predicate = predicate;
  }

  private Filter(String name, SerializableFunction<T, Boolean> predicate) {
    super(name);
    this.predicate = predicate;
  }

  public Filter<T> named(String name) {
    return new Filter<>(name, predicate);
  }

  @Override
  public PCollection<T> apply(PCollection<T> input) {
    PCollection<T> output = input.apply(ParDo.named("Filter").of(new DoFn<T, T>() {
      @Override
      public void processElement(ProcessContext c) {
        if (predicate.apply(c.element()) == true) {
          c.output(c.element());
        }
      }
    }));
    return output;
  }

  @Override
  protected Coder<T> getDefaultOutputCoder(PCollection<T> input) {
    return input.getCoder();
  }
}
