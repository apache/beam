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
package org.apache.beam.sdk.extensions.euphoria.core.client.operator;

import static java.util.Objects.requireNonNull;

import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.Derived;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.StateComplexity;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryPredicate;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Builders;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.hint.OutputHint;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.PCollectionLists;
import org.apache.beam.sdk.extensions.euphoria.core.translate.OperatorTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Operator performing a filter operation.
 *
 * <p>Output elements that pass given condition.
 *
 * <h3>Builders:</h3>
 *
 * <ol>
 *   <li>{@code [named] ..................} give name to the operator [optional]
 *   <li>{@code of .......................} input dataset
 *   <li>{@code by .......................} apply {@link UnaryPredicate} to input elements
 *   <li>{@code output ...................} build output dataset
 * </ol>
 */
@Audience(Audience.Type.CLIENT)
@Derived(state = StateComplexity.ZERO, repartitions = 0)
public class Filter<InputT> extends Operator<InputT> implements CompositeOperator<InputT, InputT> {

  /**
   * Starts building a nameless {@link Filter} operator to process the given input dataset.
   *
   * @param <InputT> the type of elements of the input dataset
   * @param input the input data set to be processed
   * @return a builder to complete the setup of the new operator
   * @see #named(String)
   * @see OfBuilder#of(PCollection)
   */
  public static <InputT> ByBuilder<InputT> of(PCollection<InputT> input) {
    return named(null).of(input);
  }

  /**
   * Starts building a named {@link Filter} operator.
   *
   * @param name a user provided name of the new operator to build
   * @return a builder to complete the setup of the new operator
   */
  public static OfBuilder named(@Nullable String name) {
    return new Builder(name);
  }

  /** Builder for the 'of' step. */
  public interface OfBuilder extends Builders.Of {

    @Override
    <InputT> ByBuilder<InputT> of(PCollection<InputT> input);
  }

  /** Builder for the 'by' step. */
  public interface ByBuilder<InputT> {

    /**
     * Specifies the function that is capable of input elements filtering.
     *
     * @param predicate the function that filters out elements if the return value for the element
     *     is false
     * @return the next builder to complete the setup of the operator
     */
    Builders.Output<InputT> by(UnaryPredicate<InputT> predicate);
  }

  private static class Builder<InputT>
      implements OfBuilder, ByBuilder<InputT>, Builders.Output<InputT> {

    private final @Nullable String name;
    private PCollection<InputT> input;
    private UnaryPredicate<InputT> predicate;

    private Builder(@Nullable String name) {
      this.name = name;
    }

    @Override
    public <T> ByBuilder<T> of(PCollection<T> input) {
      @SuppressWarnings("unchecked")
      final Builder<T> cast = (Builder) this;
      cast.input = requireNonNull(input);
      return cast;
    }

    @Override
    public Builders.Output<InputT> by(UnaryPredicate<InputT> predicate) {
      this.predicate = requireNonNull(predicate);
      return this;
    }

    @Override
    public PCollection<InputT> output(OutputHint... outputHints) {
      final Filter<InputT> filter = new Filter<>(name, predicate, input.getTypeDescriptor());
      return OperatorTransform.apply(filter, PCollectionList.of(input));
    }
  }

  private final UnaryPredicate<InputT> predicate;

  private Filter(
      @Nullable String name,
      UnaryPredicate<InputT> predicate,
      @Nullable TypeDescriptor<InputT> outputType) {
    super(name, outputType);
    this.predicate = predicate;
  }

  public UnaryPredicate<InputT> getPredicate() {
    return predicate;
  }

  @Override
  public PCollection<InputT> expand(PCollectionList<InputT> inputs) {
    return FlatMap.named(getName().orElse(null))
        .of(PCollectionLists.getOnlyElement(inputs))
        .using(
            (InputT element, Collector<InputT> collector) -> {
              if (getPredicate().apply(element)) {
                collector.collect(element);
              }
            },
            getOutputType().orElse(null))
        .output();
  }
}
