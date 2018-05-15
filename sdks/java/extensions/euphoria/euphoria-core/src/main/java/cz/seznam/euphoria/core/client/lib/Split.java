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
public class Split<InputT> {

  static final String DEFAULT_NAME = "Split";
  static final String POSITIVE_FILTER_SUFFIX = "-positive";
  static final String NEGATIVE_FILTER_SUFFIX = "-negative";

  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  public static <InputT> UsingBuilder<InputT> of(Dataset<InputT> input) {
    return new UsingBuilder<InputT>(DEFAULT_NAME, input);
  }

  /** TODO: complete javadoc. */
  public static class OfBuilder {
    private final String name;

    OfBuilder(String name) {
      this.name = Objects.requireNonNull(name);
    }

    public <InputT> Split.UsingBuilder<InputT> of(Dataset<InputT> input) {
      return new Split.UsingBuilder<>(name, input);
    }
  }

  /** TODO: complete javadoc. */
  public static class UsingBuilder<InputT> {
    private final String name;
    private final Dataset<InputT> input;

    UsingBuilder(String name, Dataset<InputT> input) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
    }

    public Split.OutputBuilder<InputT> using(UnaryPredicate<InputT> predicate) {
      return new Split.OutputBuilder<>(name, input, predicate);
    }
  }

  /** TODO: complete javadoc. */
  public static class OutputBuilder<InputT> implements Serializable {
    private final String name;
    private final Dataset<InputT> input;
    private final UnaryPredicate<InputT> predicate;

    OutputBuilder(String name, Dataset<InputT> input, UnaryPredicate<InputT> predicate) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.predicate = Objects.requireNonNull(predicate);
    }

    public Output<InputT> output() {
      Dataset<InputT> positiveOutput =
          Filter.named(name + POSITIVE_FILTER_SUFFIX).of(input).by(predicate).output();
      Dataset<InputT> negativeOutput =
          Filter.named(name + NEGATIVE_FILTER_SUFFIX)
              .of(input)
              .by((UnaryPredicate<InputT>) what -> !predicate.apply(what))
              .output();
      return new Output<>(positiveOutput, negativeOutput);
    }
  }

  /** Pair of positive and negative output as a result of the {@link Split} operator. */
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
