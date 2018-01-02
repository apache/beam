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
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunctionEnv;
import cz.seznam.euphoria.core.executor.graph.DAG;

import java.util.Objects;

/**
 * Simple one-to-one transformation of input elements. It is a special case of
 * {@link FlatMap} with exactly one output element for every one input element.
 * No context is provided inside the map function.
 *
 * <h3>Builders:</h3>
 * <ol>
 *   <li>{@code [named] ..................} give name to the operator [optional]
 *   <li>{@code of .......................} input dataset
 *   <li>{@code using ....................} apply {@link UnaryFunction} or {@link UnaryFunctionEnv} to input elements
 *   <li>{@code output ...................} build output dataset
 * </ol>
 *
 */
@Audience(Audience.Type.CLIENT)
@Derived(
    state = StateComplexity.ZERO,
    repartitions = 0
)
public class MapElements<IN, OUT> extends ElementWiseOperator<IN, OUT> {

  public static class OfBuilder implements Builders.Of {
    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    @Override
    public <IN> UsingBuilder<IN> of(Dataset<IN> input) {
      return new UsingBuilder<>(name, input);
    }
  }

  public static class UsingBuilder<IN> {
    private final String name;
    private final Dataset<IN> input;

    UsingBuilder(String name, Dataset<IN> input) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
    }

    /**
     * The mapping function that takes input element and outputs the OUT type element.
     * If you want use aggregators use rather {@link #using(UnaryFunctionEnv)}.
     *
     * @param <OUT> type of output elements
     * @param mapper the mapping function
     * @return the next builder to complete the setup of the
     *         {@link MapElements} operator
     */
    public <OUT> OutputBuilder<IN, OUT> using(UnaryFunction<IN, OUT> mapper) {
      return new OutputBuilder<>(name, input, ((el, ctx) -> mapper.apply(el)));
    }

    /**
     * The mapping function that takes input element and outputs the OUT type element.
     *
     * @param <OUT> type of output elements
     * @param mapper the mapping function
     * @return the next builder to complete the setup of the
     *         {@link MapElements} operator
     */
    public <OUT> OutputBuilder<IN, OUT> using(UnaryFunctionEnv<IN, OUT> mapper) {
      return new OutputBuilder<>(name, input, mapper);
    }
  }

  public static class OutputBuilder<IN, OUT> implements Builders.Output<OUT> {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunctionEnv<IN, OUT> mapper;

    OutputBuilder(String name, Dataset<IN> input, UnaryFunctionEnv<IN, OUT> mapper) {
      this.name = name;
      this.input = input;
      this.mapper = mapper;
    }

    @Override
    public Dataset<OUT> output() {
      Flow flow = input.getFlow();
      MapElements<IN, OUT> map = new MapElements<>(name, flow, input, mapper);
      flow.add(map);

      return map.output();
    }
  }

  /**
   * Starts building a nameless {@link MapElements} operator to process
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
  public static <IN> UsingBuilder<IN> of(Dataset<IN> input) {
    return new UsingBuilder<>("MapElements", input);
  }

  /**
   * Starts building a named {@link MapElements} operator.
   *
   * @param name a user provided name of the new operator to build
   *
   * @return a builder to complete the setup of the new operator
   */
  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  final UnaryFunctionEnv<IN, OUT> mapper;

  MapElements(String name,
              Flow flow,
              Dataset<IN> input,
              UnaryFunction<IN, OUT> mapper) {
    this(name, flow, input, (el, ctx) -> mapper.apply(el));
  }

  MapElements(String name,
              Flow flow,
              Dataset<IN> input,
              UnaryFunctionEnv<IN, OUT> mapper) {
    super(name, flow, input);
    this.mapper = mapper;
  }

  /**
   * This is not a basic operator. It can be straightforwardly implemented
   * by using {@code FlatMap} operator.
   * @return the operator chain representing this operation including FlatMap
   */
  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    return DAG.of(
        // do not use the client API here, because it modifies the Flow!
        new FlatMap<IN, OUT>(getName(), getFlow(), input,
            (i, c) -> c.collect(mapper.apply(i, c.asContext())), null));
  }

  public UnaryFunctionEnv<IN, OUT> getMapper() {
    return mapper;
  }
}
