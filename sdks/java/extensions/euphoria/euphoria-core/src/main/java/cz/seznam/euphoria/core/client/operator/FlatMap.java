/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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

import cz.seznam.euphoria.core.annotation.operator.Basic;
import cz.seznam.euphoria.core.annotation.operator.StateComplexity;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;

import java.util.Objects;

/**
 * A transformation of a dataset from one type into another allowing user code
 * to generate zero, one, or many output elements for a given input element.<p>
 *
 * The user supplied map function is supposed to be stateless. It is fed items
 * from the input in no specified order and the results of the map function are
 * "flattened" to the output (equally in no specified order.)<p>
 *
 * Example:
 *
 * <pre>{@code
 *  Dataset<String> strings = ...;
 *  Dataset<Integer> ints =
 *         FlatMap.named("TO-INT")
 *            .of(strings)
 *            .using((String s, Context<String> c) -> {
 *              try {
 *                int i = Integer.parseInt(s);
 *                c.collect(i);
 *              } catch (NumberFormatException e) {
 *                // ~ ignore the input if we failed to parse it
 *              }
 *            })
 *            .output();
 * }</pre>
 *
 * The above example tries to parse incoming strings as integers, silently
 * skipping those which cannot be successfully converted. While
 * {@link cz.seznam.euphoria.core.client.io.Context#collect(Object)} has
 * been used only once here, a {@link FlatMap} operator is free
 * to invoke it multiple times or not at all to generate that many elements
 * to the output dataset.
 */
@Basic(
    state = StateComplexity.ZERO,
    repartitions = 0
)
public class FlatMap<IN, OUT> extends ElementWiseOperator<IN, OUT> {

  public static class OfBuilder {
    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    /**
     * Specifies the input dataset to be transformed.
     *
     * @param <IN> the type of elements of the input dataset
     *
     * @param input the input dataset
     *
     * @return the next builder to complete the setup
     *          of the {@link FlatMap} operator
     */
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
     * Specifies the user defined map function by which to transform
     * the final operator's input dataset.
     *
     * @param <OUT> the type of elements the user defined map function
     *            will produce to the output dataset
     *
     * @param functor the user defined map function
     *
     * @return the next builder to complete the setup
     *          of the {@link FlatMap} operator
     */
    public <OUT> OutputBuilder<IN, OUT> using(UnaryFunctor<IN, OUT> functor) {
      return new OutputBuilder<>(name, input, functor);
    }
  }

  public static class OutputBuilder<IN, OUT>
      implements cz.seznam.euphoria.core.client.operator.OutputBuilder<OUT>
  {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunctor<IN, OUT> functor;

    OutputBuilder(String name, Dataset<IN> input, UnaryFunctor<IN, OUT> functor) {
      this.name = name;
      this.input = input;
      this.functor = functor;
    }

    /**
     * Finalizes the setup of the {@link FlatMap} operator and retrieves the
     * transformed dataset.
     *
     * @return the dataset representing the operator's transformed input
     */
    @Override
    public Dataset<OUT> output() {
      Flow flow = input.getFlow();
      FlatMap<IN, OUT> map = new FlatMap<>(name, flow, input, functor);
      flow.add(map);
      return map.output();
    }
  }

  /**
   * Starts building a nameless {@link FlatMap} operator to transform the given
   * input dataset.
   *
   * @param <IN> the type of elements of the input dataset
   *
   * @param input the input data set to be transformed
   *
   * @return a builder to complete the setup of the new {@link FlatMap} operator
   *
   * @see #named(String)
   * @see OfBuilder#of(Dataset)
   */
  public static <IN> UsingBuilder<IN> of(Dataset<IN> input) {
    return new UsingBuilder<>("FlatMap", input);
  }

  /**
   * Starts building a named {@link FlatMap} operator.
   *
   * @param name a user provided name of the new operator to build
   *
   * @return a builder to complete the setup of the new {@link FlatMap} operator
   */
  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  private final UnaryFunctor<IN, OUT> functor;

  FlatMap(String name, Flow flow, Dataset<IN> input, UnaryFunctor<IN, OUT> functor) {
    super(name, flow, input);
    this.functor = functor;
  }

  /**
   * Retrieves the user defined map function to be applied to this operator's
   * input elements.
   *
   * @return the user defined map function; never {@code null}
   */
  public UnaryFunctor<IN, OUT> getFunctor() {
    return functor;
  }
}