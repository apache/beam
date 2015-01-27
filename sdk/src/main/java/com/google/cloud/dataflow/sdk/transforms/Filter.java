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

import com.google.cloud.dataflow.sdk.values.PCollection;

/**
 * {@code PTransform}s for filtering from a {@code PCollection} the
 * elements satisfying a predicate, or satisfying an inequality with
 * a given value based on the elements' natural ordering.
 *
 * @param <T> the type of the values in the input {@code PCollection},
 * and the type of the elements in the output {@code PCollection}
 */
@SuppressWarnings("serial")
public class Filter<T> extends PTransform<PCollection<T>,
                                          PCollection<T>> {
  /**
   * Returns a {@code PTransform} that takes an input
   * {@code PCollection<T>} and returns a {@code PCollection<T>} with
   * elements that satisfy the given predicate.  The predicate must be
   * a {@code SerializableFunction<T, Boolean>}.
   *
   * <p> Example of use:
   * <pre> {@code
   * PCollection<String> wordList = ...;
   * PCollection<String> longWords =
   *     wordList.apply(Filter.by(new MatchIfWordLengthGT(6)));
   * } </pre>
   *
   * <p> See also {@link #lessThan}, {@link #lessThanEq},
   * {@link #greaterThan}, {@link #greaterThanEq}, which return elements
   * satisfying various inequalities with the specified value based on
   * the elements' natural ordering.
   */
  public static <T, C extends SerializableFunction<T, Boolean>>
      ParDo.Bound<T, T> by(final C filterPred) {
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
   * {@code PCollection<T>} and returns a {@code PCollection<T>} with
   * elements that are less than a given value, based on the
   * elements' natural ordering. Elements must be {@code Comparable}.
   *
   * <p> Example of use:
   * <pre> {@code
   * PCollection<String> listOfNumbers = ...;
   * PCollection<String> smallNumbers =
   *     listOfNumbers.apply(Filter.lessThan(10));
   * } </pre>
   *
   * <p> See also {@link #lessThanEq}, {@link #greaterThanEq},
   * {@link #greaterThan} which return elements satisfying various
   * inequalities with the specified value based on the elements'
   * natural ordering.
   *
   * <p> See also {@link #by}, which returns elements
   * that satisfy the given predicate.
   */
  public static <T extends Comparable<T>>
      ParDo.Bound<T, T> lessThan(final T value) {
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
   * <p> Example of use:
   * <pre> {@code
   * PCollection<String> listOfNumbers = ...;
   * PCollection<String> largeNumbers =
   *     listOfNumbers.apply(Filter.greaterThan(1000));
   * } </pre>
   *
   * <p> See also {@link #greaterThanEq}, {@link #lessThan},
   * {@link #lessThanEq} which return elements satisfying various
   * inequalities with the specified value based on the elements'
   * natural ordering.
   *
   * <p> See also {@link #by}, which returns elements
   * that satisfy the given predicate.
   */
  public static <T extends Comparable<T>>
      ParDo.Bound<T, T> greaterThan(final T value) {
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
   * <p> Example of use:
   * <pre> {@code
   * PCollection<String> listOfNumbers = ...;
   * PCollection<String> smallOrEqualNumbers =
   *     listOfNumbers.apply(Filter.lessThanEq(10));
   * } </pre>
   *
   * <p> See also {@link #lessThan}, {@link #greaterThanEq},
   * {@link #greaterThan} which return elements satisfying various
   * inequalities with the specified value based on the elements'
   * natural ordering.
   *
   * <p> See also {@link #by}, which returns elements
   * that satisfy the given predicate.
   */
  public static <T extends Comparable<T>>
      ParDo.Bound<T, T> lessThanEq(final T value) {
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
   * <p> Example of use:
   * <pre> {@code
   * PCollection<String> listOfNumbers = ...;
   * PCollection<String> largeOrEqualNumbers =
   *     listOfNumbers.apply(Filter.greaterThanEq(1000));
   * } </pre>
   *
   * <p> See also {@link #greaterThan}, {@link #lessThan},
   * {@link #lessThanEq} which return elements satisfying various
   * inequalities with the specified value based on the elements'
   * natural ordering.
   *
   * <p> See also {@link #by}, which returns elements
   * that satisfy the given predicate.
   */
  public static <T extends Comparable<T>>
      ParDo.Bound<T, T> greaterThanEq(final T value) {
    return ParDo.named("Filter.greaterThanEq").of(new DoFn<T, T>() {
            @Override
            public void processElement(ProcessContext c) {
              if (c.element().compareTo(value) >= 0) {
                c.output(c.element());
              }
            }
        });
  }
}
