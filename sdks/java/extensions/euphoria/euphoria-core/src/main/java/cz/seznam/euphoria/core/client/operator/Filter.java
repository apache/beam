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
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.annotation.operator.Derived;
import cz.seznam.euphoria.core.annotation.operator.StateComplexity;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryPredicate;
import cz.seznam.euphoria.core.client.operator.hint.OutputHint;
import cz.seznam.euphoria.core.executor.graph.DAG;
import cz.seznam.euphoria.shadow.com.google.common.collect.Sets;

import java.util.Objects;
import java.util.Set;

/**
 * Operator performing a filter operation.
 *
 * Output elements that pass given condition.
 *
 * <h3>Builders:</h3>
 * <ol>
 *   <li>{@code [named] ..................} give name to the operator [optional]
 *   <li>{@code of .......................} input dataset
 *   <li>{@code by .......................} apply {@link UnaryPredicate} to input elements
 *   <li>{@code output ...................} build output dataset
 * </ol>
 *
 */
@Audience(Audience.Type.CLIENT)
@Derived(
    state = StateComplexity.ZERO,
    repartitions = 0
)
public class Filter<IN> extends ElementWiseOperator<IN, IN> {

  public static class OfBuilder implements Builders.Of {
    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    @Override
    public <IN> ByBuilder<IN> of(Dataset<IN> input) {
      return new ByBuilder<>(name, input);
    }
  }

  public static class ByBuilder<IN> {
    private final String name;
    private final Dataset<IN> input;

    ByBuilder(String name, Dataset<IN> input) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
    }

    /**
     * Specifies the function that is capable of input elements filtering.
     *
     * @param predicate the function that filters out elements if the return value
     *        for the element is false
     * @return the next builder to complete the setup of the operator
     */
    public Builders.Output<IN> by(UnaryPredicate<IN> predicate) {
      return new OutputBuilder<>(name, input, predicate);
    }

  }

  public static class OutputBuilder<IN> implements Builders.Output<IN> {

    private final String name;
    private final Dataset<IN> input;
    private final UnaryPredicate<IN> predicate;

    private OutputBuilder(String name, Dataset<IN> input, UnaryPredicate<IN> predicate) {
      this.name = name;
      this.input = input;
      this.predicate = predicate;
    }

    @Override
    public Dataset<IN> output(OutputHint... outputHints) {
      Flow flow = input.getFlow();
      Filter<IN> filter = new Filter<>(name, flow, input, predicate, Sets.newHashSet(outputHints));
      flow.add(filter);

      return filter.output();
    }

  }

  /**
   * Starts building a nameless {@link Filter} operator to process
   * the given input dataset.
   *
   * @param <IN> the type of elements of the input dataset
   *
   * @param input the input data set to be processed
   *
   * @return a builder to complete the setup of the new operator
   *
   * @see #named(String)
   * @see OfBuilder#of(Dataset)
   */
  public static <IN> ByBuilder<IN> of(Dataset<IN> input) {
    return new ByBuilder<>("Filter", input);
  }

  /**
   * Starts building a named {@link Filter} operator.
   *
   * @param name a user provided name of the new operator to build
   *
   * @return a builder to complete the setup of the new operator
   */
  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  final UnaryPredicate<IN> predicate;

  Filter(String name, Flow flow, Dataset<IN> input, UnaryPredicate<IN> predicate, Set<OutputHint>
      outputHints) {
    super(name, flow, input, outputHints);
    this.predicate = predicate;
  }

  public UnaryPredicate<IN> getPredicate() {
    return predicate;
  }

  /** This operator can be implemented using FlatMap. */
  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    return DAG.of(new FlatMap<>(getName(), getFlow(), input,
        (elem, collector) -> {
          if (predicate.apply(elem)) {
            collector.collect(elem);
          }
        }, null));
  }
}
