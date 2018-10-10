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

import org.apache.beam.sdk.transforms.Failure.FailureTagList;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

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
   * SerializableFunction<T, Boolean>}.
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
  public static <T, PredicateT extends SerializableFunction<T, Boolean>> Filter<T> by(
      PredicateT predicate) {
    return new Filter<>(predicate);
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
    return by((SerializableFunction<T, Boolean>) input -> input.compareTo(value) < 0)
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
    return by((SerializableFunction<T, Boolean>) input -> input.compareTo(value) > 0)
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
    return by((SerializableFunction<T, Boolean>) input -> input.compareTo(value) <= 0)
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
    return by((SerializableFunction<T, Boolean>) input -> input.compareTo(value) >= 0)
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
    return by((SerializableFunction<T, Boolean>) input -> input.compareTo(value) == 0)
        .described(String.format("x == %s", value));
  }

  ///////////////////////////////////////////////////////////////////////////////

  private SerializableFunction<T, Boolean> predicate;
  private String predicateDescription;

  private Filter(SerializableFunction<T, Boolean> predicate) {
    this(predicate, "Filter.predicate");
  }

  private Filter(SerializableFunction<T, Boolean> predicate, String predicateDescription) {
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
                  public void processElement(@Element T element, OutputReceiver<T> r) {
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

  /**
   * Sets a {@link TupleTag} to associate with successes, converting this {@link PTransform} into
   * one that returns a {@link PCollectionTuple}. This allows you to make subsequent {@link
   * WithFailures#withFailureTag(TupleTag, Class[])} calls to capture thrown exceptions to failure
   * collections.
   */
  public WithFailures withSuccessTag(TupleTag<T> successTag) {
    return new WithFailures(successTag, FailureTagList.empty());
  }

  /**
   * Variant of {@link Filter} that that can handle exceptions and output one or more failure
   * collections wrapped in a {@link PCollectionTuple}. Specify how to handle exceptions by calling
   * {@link #withFailureTag(TupleTag, Class[])}.
   */
  public class WithFailures extends PTransform<PCollection<T>, PCollectionTuple> {
    private final TupleTag<T> successTag;
    private final FailureTagList<T> failureTagList;

    WithFailures(TupleTag<T> successTag, FailureTagList<T> failureTagList) {
      this.successTag = successTag;
      this.failureTagList = failureTagList;
    }

    /**
     * Returns a modified {@link PTransform} that will catch exceptions of type {@code ExceptionT}
     * (as specified by the given {@code TupleTag<Failure<ExceptionT, InputT>>}).
     *
     * <p>If you only want to catch specific subtypes of {@code ExceptionT}, you may pass class
     * instances for those narrower types as additional parameters.
     */
    public <ExceptionT extends Exception> WithFailures withFailureTag(
        TupleTag<Failure<ExceptionT, T>> tag, Class... exceptionsToCatch) {
      return new WithFailures(successTag, failureTagList.and(tag, exceptionsToCatch));
    }

    @Override
    public PCollectionTuple expand(PCollection<T> input) {
      PCollectionTuple pcs =
          input.apply(
              ParDo.of(
                      new DoFn<T, T>() {
                        @ProcessElement
                        public void processElement(@Element T element, MultiOutputReceiver receiver)
                            throws Exception {
                          Boolean accepted = null;
                          try {
                            accepted = predicate.apply(element);
                          } catch (Exception e) {
                            failureTagList.outputOrRethrow(e, element, receiver);
                          }
                          if (accepted != null && accepted) {
                            receiver.get(successTag).output(element);
                          }
                        }
                      })
                  .withOutputTags(successTag, failureTagList.tags()));
      pcs.get(successTag).setCoder(input.getCoder());
      return failureTagList.applyFailureCoders(input.getCoder(), pcs);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(
          DisplayData.item("predicate", predicateDescription).withLabel("Filter Predicate"));
    }
  }
}
