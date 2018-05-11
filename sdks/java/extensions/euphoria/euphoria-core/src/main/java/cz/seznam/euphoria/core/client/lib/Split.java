/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.core.client.lib;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.annotation.operator.Derived;
import cz.seznam.euphoria.core.annotation.operator.StateComplexity;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.functional.UnaryPredicate;
import cz.seznam.euphoria.core.client.operator.Filter;
import java.io.Serializable;
import java.util.Objects;

/**
 * Composite operator using two {@link Filter} operators to split a {@link Dataset} into two subsets
 * using provided {@link UnaryPredicate}.
 */
@Audience(Audience.Type.CLIENT)
@Derived(state = StateComplexity.ZERO, repartitions = 0)
public class Split<IN> {

  static final String DEFAULT_NAME = "Split";
  static final String POSITIVE_FILTER_SUFFIX = "-positive";
  static final String NEGATIVE_FILTER_SUFFIX = "-negative";

  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  public static <IN> UsingBuilder<IN> of(Dataset<IN> input) {
    return new UsingBuilder<IN>(DEFAULT_NAME, input);
  }

  public static class OfBuilder {
    private final String name;

    OfBuilder(String name) {
      this.name = Objects.requireNonNull(name);
    }

    public <IN> Split.UsingBuilder<IN> of(Dataset<IN> input) {
      return new Split.UsingBuilder<>(name, input);
    }
  }

  public static class UsingBuilder<IN> {
    private final String name;
    private final Dataset<IN> input;

    UsingBuilder(String name, Dataset<IN> input) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
    }

    public Split.OutputBuilder<IN> using(UnaryPredicate<IN> predicate) {
      return new Split.OutputBuilder<>(name, input, predicate);
    }
  }

  public static class OutputBuilder<IN> implements Serializable {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryPredicate<IN> predicate;

    OutputBuilder(String name, Dataset<IN> input, UnaryPredicate<IN> predicate) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.predicate = Objects.requireNonNull(predicate);
    }

    public Output<IN> output() {
      Dataset<IN> positiveOutput =
          Filter.named(name + POSITIVE_FILTER_SUFFIX).of(input).by(predicate).output();
      Dataset<IN> negativeOutput =
          Filter.named(name + NEGATIVE_FILTER_SUFFIX)
              .of(input)
              .by((UnaryPredicate<IN>) what -> !predicate.apply(what))
              .output();
      return new Output<>(positiveOutput, negativeOutput);
    }
  }

  /** Pair of positive and negative output as a result of the {@link Split} operator */
  public static class Output<T> {
    private final Dataset<T> positive;
    private final Dataset<T> negative;

    private Output(Dataset<T> positive, Dataset<T> negative) {
      this.positive = Objects.requireNonNull(positive);
      this.negative = Objects.requireNonNull(negative);
    }
    /** @return positive split result */
    public Dataset<T> positive() {
      return positive;
    }
    /** @return negative split result */
    public Dataset<T> negative() {
      return negative;
    }
  }
}
