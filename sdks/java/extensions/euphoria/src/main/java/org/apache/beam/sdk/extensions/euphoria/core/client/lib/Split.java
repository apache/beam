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
package org.apache.beam.sdk.extensions.euphoria.core.client.lib;

import java.util.Objects;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.Derived;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.StateComplexity;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryPredicate;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Filter;
import org.apache.beam.sdk.values.PCollection;

/**
 * Composite operator using two {@link Filter} operators to split a {@link PCollection} into two
 * subsets using provided {@link UnaryPredicate}.
 *
 * @deprecated Use Java SDK directly, Euphoria is scheduled for removal in a future release.
 */
@Audience(Audience.Type.CLIENT)
@Derived(state = StateComplexity.ZERO, repartitions = 0)
@Deprecated
public class Split {

  static final String DEFAULT_NAME = "Split";
  static final String POSITIVE_FILTER_SUFFIX = "-positive";
  static final String NEGATIVE_FILTER_SUFFIX = "-negative";

  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  public static <InputT> UsingBuilder<InputT> of(PCollection<InputT> input) {
    return new UsingBuilder<>(DEFAULT_NAME, input);
  }

  /** Starting builder. */
  public static class OfBuilder {
    private final String name;

    OfBuilder(String name) {
      this.name = Objects.requireNonNull(name);
    }

    public <InputT> Split.UsingBuilder<InputT> of(PCollection<InputT> input) {
      return new Split.UsingBuilder<>(name, input);
    }
  }

  /** Builder adding filtering predicate. */
  public static class UsingBuilder<InputT> {
    private final String name;
    private final PCollection<InputT> input;

    UsingBuilder(String name, PCollection<InputT> input) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
    }

    public Split.OutputBuilder<InputT> using(UnaryPredicate<InputT> predicate) {
      return new Split.OutputBuilder<>(name, input, predicate);
    }
  }

  /** Last builder in a chain. It concludes this operators creation by calling {@link #output()}. */
  public static class OutputBuilder<InputT> {
    private final String name;
    private final PCollection<InputT> input;
    private final UnaryPredicate<InputT> predicate;

    OutputBuilder(String name, PCollection<InputT> input, UnaryPredicate<InputT> predicate) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.predicate = Objects.requireNonNull(predicate);
    }

    public Output<InputT> output() {
      PCollection<InputT> positiveOutput =
          Filter.named(name + POSITIVE_FILTER_SUFFIX).of(input).by(predicate).output();
      PCollection<InputT> negativeOutput =
          Filter.named(name + NEGATIVE_FILTER_SUFFIX)
              .of(input)
              .by((UnaryPredicate<InputT>) what -> !predicate.apply(what))
              .output();
      return new Output<>(positiveOutput, negativeOutput);
    }
  }

  /** KV of positive and negative output as a result of the {@link Split} operator. */
  public static class Output<T> {
    private final PCollection<T> positive;
    private final PCollection<T> negative;

    private Output(PCollection<T> positive, PCollection<T> negative) {
      this.positive = Objects.requireNonNull(positive);
      this.negative = Objects.requireNonNull(negative);
    }

    /** @return positive split result */
    public PCollection<T> positive() {
      return positive;
    }

    /** @return negative split result */
    public PCollection<T> negative() {
      return negative;
    }
  }
}
