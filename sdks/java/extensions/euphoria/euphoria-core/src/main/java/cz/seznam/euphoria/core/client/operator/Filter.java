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
package cz.seznam.euphoria.core.client.operator;

import com.google.common.collect.Sets;
import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.annotation.operator.Derived;
import cz.seznam.euphoria.core.annotation.operator.StateComplexity;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryPredicate;
import cz.seznam.euphoria.core.client.operator.hint.OutputHint;
import cz.seznam.euphoria.core.executor.graph.DAG;
import java.util.Objects;
import java.util.Set;

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
public class Filter<InputT> extends ElementWiseOperator<InputT, InputT> {

  final UnaryPredicate<InputT> predicate;

  Filter(
      String name,
      Flow flow,
      Dataset<InputT> input,
      UnaryPredicate<InputT> predicate,
      Set<OutputHint> outputHints) {
    super(name, flow, input, outputHints);
    this.predicate = predicate;
  }

  /**
   * Starts building a nameless {@link Filter} operator to process the given input dataset.
   *
   * @param <InputT> the type of elements of the input dataset
   * @param input the input data set to be processed
   * @return a builder to complete the setup of the new operator
   * @see #named(String)
   * @see OfBuilder#of(Dataset)
   */
  public static <InputT> ByBuilder<InputT> of(Dataset<InputT> input) {
    return new ByBuilder<>("Filter", input);
  }

  /**
   * Starts building a named {@link Filter} operator.
   *
   * @param name a user provided name of the new operator to build
   * @return a builder to complete the setup of the new operator
   */
  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  public UnaryPredicate<InputT> getPredicate() {
    return predicate;
  }

  /** This operator can be implemented using FlatMap. */
  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    return DAG.of(
        new FlatMap<>(
            getName(),
            getFlow(),
            input,
            (elem, collector) -> {
              if (predicate.apply(elem)) {
                collector.collect(elem);
              }
            },
            null,
            getHints()));
  }

  /** TODO: complete javadoc. */
  public static class OfBuilder implements Builders.Of {
    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    @Override
    public <InputT> ByBuilder<InputT> of(Dataset<InputT> input) {
      return new ByBuilder<>(name, input);
    }
  }

  /** TODO: complete javadoc. */
  public static class ByBuilder<InputT> {
    private final String name;
    private final Dataset<InputT> input;

    ByBuilder(String name, Dataset<InputT> input) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
    }

    /**
     * Specifies the function that is capable of input elements filtering.
     *
     * @param predicate the function that filters out elements if the return value for the element
     *     is false
     * @return the next builder to complete the setup of the operator
     */
    public Builders.Output<InputT> by(UnaryPredicate<InputT> predicate) {
      return new OutputBuilder<>(name, input, predicate);
    }
  }

  /** TODO: complete javadoc. */
  public static class OutputBuilder<InputT> implements Builders.Output<InputT> {

    private final String name;
    private final Dataset<InputT> input;
    private final UnaryPredicate<InputT> predicate;

    private OutputBuilder(String name, Dataset<InputT> input, UnaryPredicate<InputT> predicate) {
      this.name = name;
      this.input = input;
      this.predicate = predicate;
    }

    @Override
    public Dataset<InputT> output(OutputHint... outputHints) {
      Flow flow = input.getFlow();
      Filter<InputT> filter =
          new Filter<>(name, flow, input, predicate, Sets.newHashSet(outputHints));
      flow.add(filter);

      return filter.output();
    }
  }
}
