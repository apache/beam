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

import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;

/**
 * {@code PTransform}s for filtering from a {@code PCollection} the elements satisfying a predicate,
 * or satisfying an inequality with a given value based on the elements' natural ordering.
 *
 * @param <T> the type of the values in the input {@code PCollection}, and the type of the elements
 *     in the output {@code PCollection}
 */
public class Filter<T> extends PTransform<PCollection<T>, PCollection<T>> {

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<T>} and returns a {@code
   * PCollection<T>} with elements that satisfy the given predicate. The predicate must be a {@code
   * ProcessFunction<T, Boolean>}.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<String> wordList = ...;
   * PCollection<String> longWords =
   *     wordList.apply(Filter.by(new MatchIfWordLengthGT(6)));
   * }</pre>
   *
   * <p>See also {@link #lessThan}, {@link #lessThanEq}, {@link #greaterThan}, {@link
   * #greaterThanEq}, which return elements satisfying various inequalities with the specified value
   * based on the elements' natural ordering.
   */
  public static <T, PredicateT extends ProcessFunction<T, Boolean>> Filter<T> by(
      PredicateT predicate) {
    return new Filter<>(predicate);
  }

  /** Binary compatibility adapter for {@link #by(ProcessFunction)}. */
  public static <T, PredicateT extends SerializableFunction<T, Boolean>> Filter<T> by(
      PredicateT predicate) {
    return by((ProcessFunction<T, Boolean>) predicate);
  }

  /**
   * Returns a {@code PTransform} that takes an input {@link PCollection} and returns a {@link
   * PCollection} with elements that are less than a given value, based on the elements' natural
   * ordering. Elements must be {@code Comparable}.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<Integer> listOfNumbers = ...;
   * PCollection<Integer> smallNumbers =
   *     listOfNumbers.apply(Filter.lessThan(10));
   * }</pre>
   *
   * <p>See also {@link #lessThanEq}, {@link #greaterThanEq}, {@link #equal} and {@link
   * #greaterThan}, which return elements satisfying various inequalities with the specified value
   * based on the elements' natural ordering.
   *
   * <p>See also {@link #by}, which returns elements that satisfy the given predicate.
   */
  public static <T extends Comparable<T>> Filter<T> lessThan(final T value) {
    return by((ProcessFunction<T, Boolean>) input -> input.compareTo(value) < 0)
        .described(String.format("x < %s", value));
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<T>} and returns a {@code
   * PCollection<T>} with elements that are greater than a given value, based on the elements'
   * natural ordering. Elements must be {@code Comparable}.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<Integer> listOfNumbers = ...;
   * PCollection<Integer> largeNumbers =
   *     listOfNumbers.apply(Filter.greaterThan(1000));
   * }</pre>
   *
   * <p>See also {@link #greaterThanEq}, {@link #lessThan}, {@link #equal} and {@link #lessThanEq},
   * which return elements satisfying various inequalities with the specified value based on the
   * elements' natural ordering.
   *
   * <p>See also {@link #by}, which returns elements that satisfy the given predicate.
   */
  public static <T extends Comparable<T>> Filter<T> greaterThan(final T value) {
    return by((ProcessFunction<T, Boolean>) input -> input.compareTo(value) > 0)
        .described(String.format("x > %s", value));
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<T>} and returns a {@code
   * PCollection<T>} with elements that are less than or equal to a given value, based on the
   * elements' natural ordering. Elements must be {@code Comparable}.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<Integer> listOfNumbers = ...;
   * PCollection<Integer> smallOrEqualNumbers =
   *     listOfNumbers.apply(Filter.lessThanEq(10));
   * }</pre>
   *
   * <p>See also {@link #lessThan}, {@link #greaterThanEq}, {@link #equal} and {@link #greaterThan},
   * which return elements satisfying various inequalities with the specified value based on the
   * elements' natural ordering.
   *
   * <p>See also {@link #by}, which returns elements that satisfy the given predicate.
   */
  public static <T extends Comparable<T>> Filter<T> lessThanEq(final T value) {
    return by((ProcessFunction<T, Boolean>) input -> input.compareTo(value) <= 0)
        .described(String.format("x ≤ %s", value));
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<T>} and returns a {@code
   * PCollection<T>} with elements that are greater than or equal to a given value, based on the
   * elements' natural ordering. Elements must be {@code Comparable}.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<Integer> listOfNumbers = ...;
   * PCollection<Integer> largeOrEqualNumbers =
   *     listOfNumbers.apply(Filter.greaterThanEq(1000));
   * }</pre>
   *
   * <p>See also {@link #greaterThan}, {@link #lessThan}, {@link #equal} and {@link #lessThanEq},
   * which return elements satisfying various inequalities with the specified value based on the
   * elements' natural ordering.
   *
   * <p>See also {@link #by}, which returns elements that satisfy the given predicate.
   */
  public static <T extends Comparable<T>> Filter<T> greaterThanEq(final T value) {
    return by((ProcessFunction<T, Boolean>) input -> input.compareTo(value) >= 0)
        .described(String.format("x ≥ %s", value));
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<T>} and returns a {@code
   * PCollection<T>} with elements that equals to a given value. Elements must be {@code
   * Comparable}.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<Integer> listOfNumbers = ...;
   * PCollection<Integer> equalNumbers = listOfNumbers.apply(Filter.equal(1000));
   * }</pre>
   *
   * <p>See also {@link #greaterThan}, {@link #lessThan}, {@link #lessThanEq} and {@link
   * #greaterThanEq}, which return elements satisfying various inequalities with the specified value
   * based on the elements' natural ordering.
   *
   * <p>See also {@link #by}, which returns elements that satisfy the given predicate.
   */
  public static <T extends Comparable<T>> Filter<T> equal(final T value) {
    return by((ProcessFunction<T, Boolean>) input -> input.compareTo(value) == 0)
        .described(String.format("x == %s", value));
  }

  ///////////////////////////////////////////////////////////////////////////////

  private ProcessFunction<T, Boolean> predicate;
  private String predicateDescription;

  private Filter(ProcessFunction<T, Boolean> predicate) {
    this(predicate, "Filter.predicate");
  }

  private Filter(ProcessFunction<T, Boolean> predicate, String predicateDescription) {
    this.predicate = predicate;
    this.predicateDescription = predicateDescription;
  }

  /**
   * Returns a new {@link Filter} {@link PTransform} that's like this {@link PTransform} but with
   * the specified description for {@link DisplayData}. Does not modify this {@link PTransform}.
   */
  Filter<T> described(String description) {
    return new Filter<>(predicate, description);
  }

  @Override
  public PCollection<T> expand(PCollection<T> input) {
    return input
        .apply(
            ParDo.of(
                new DoFn<T, T>() {
                  @ProcessElement
                  public void processElement(@Element T element, OutputReceiver<T> r)
                      throws Exception {
                    if (predicate.apply(element)) {
                      r.output(element);
                    }
                  }
                }))
        .setCoder(input.getCoder());
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("predicate", predicateDescription).withLabel("Filter Predicate"));
  }
}
