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
import cz.seznam.euphoria.core.annotation.operator.Recommended;
import cz.seznam.euphoria.core.annotation.operator.StateComplexity;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.hint.OutputHint;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.graph.DAG;
import cz.seznam.euphoria.shadow.com.google.common.collect.Sets;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Operator outputting distinct (based on {@link Object#equals}) elements.
 *
 * <h3>Builders:</h3>
 * <ol>
 *   <li>{@code [named] ..................} give name to the operator [optional]
 *   <li>{@code of .......................} input dataset
 *   <li>{@code [mapped] .................} compare objects retrieved by this {@link UnaryFunction} instead of raw input elements
 *   <li>{@code [windowBy] ...............} windowing function (see {@link Windowing}), default attached windowing
 *   <li>{@code output ...................} build output dataset
 * </ol>
 *
 */
@Audience(Audience.Type.CLIENT)
@Recommended(
    reason =
        "Might be useful to override the default "
      + "implementation because of performance reasons"
      + "(e.g. using bloom filters), which might reduce the space complexity",
    state = StateComplexity.CONSTANT,
    repartitions = 1
)
public class Distinct<IN, ELEM, W extends Window>
    extends StateAwareWindowWiseSingleInputOperator<
        IN, IN, IN, ELEM, ELEM, W, Distinct<IN, ELEM, W>> {

  public static class OfBuilder implements Builders.Of {
    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    @Override
    public <IN> MappedBuilder<IN, IN> of(Dataset<IN> input) {
      return new MappedBuilder<>(name, input);
    }
  }

  public static class MappedBuilder<IN, ELEM>
      extends WindowingBuilder<IN, ELEM> {

    @SuppressWarnings("unchecked")
    private MappedBuilder(
        String name,
        Dataset<IN> input) {

      super(name, input, (UnaryFunction) e -> e);
    }

    /**
     * Optionally specifies a function to transform the input elements into
     * another type among which to find the distincts.<p>
     *
     * This is, while windowing will be applied on basis of original input
     * elements, the distinct operator will be carried out on the
     * transformed elements.
     *
     * @param <ELEM> the type of the transformed elements
     *
     * @param mapper a transform function applied to input element
     *
     * @return the next builder to complete the setup of the {@link Distinct}
     *          operator
     */
    public <ELEM> WindowingBuilder<IN, ELEM> mapped(UnaryFunction<IN, ELEM> mapper) {
      return new WindowingBuilder<>(name, input, mapper);
    }

  }

  public static class WindowingBuilder<IN, ELEM>
      implements Builders.WindowBy<IN, WindowingBuilder<IN, ELEM>>,
      Builders.Output<ELEM>,
      OptionalMethodBuilder<WindowingBuilder<IN, ELEM>> {

    final String name;
    final Dataset<IN> input;
    final UnaryFunction<IN, ELEM> mapper;

    private WindowingBuilder(
        String name,
        Dataset<IN> input,
        UnaryFunction<IN, ELEM> mapper) {

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.mapper = Objects.requireNonNull(mapper);
    }

    @Override
    public <W extends Window> OutputBuilder<IN, ELEM, W>
    windowBy(Windowing<IN, W> windowing) {
      return new OutputBuilder<>(name, input, mapper, windowing);
    }

    public Dataset<ELEM> output(OutputHint... outputHints) {
      return new OutputBuilder<>(name, input, mapper, null).output();
    }
  }

  public static class OutputBuilder<IN, ELEM, W extends Window>
      extends WindowingBuilder<IN, ELEM>
      implements Builders.Output<ELEM> {

    @Nullable
    private final Windowing<IN, W> windowing;

    OutputBuilder(String name,
                  Dataset<IN> input,
                  UnaryFunction<IN, ELEM> mapper,
                  @Nullable Windowing<IN, W> windowing) {

      super(name, input, mapper);
      this.windowing = windowing;
    }

    @Override
    public Dataset<ELEM> output(OutputHint... outputHints) {
      Flow flow = input.getFlow();
      Distinct<IN, ELEM, W> distinct = new Distinct<>(
          name, flow, input, mapper, windowing, Sets.newHashSet(outputHints));
      flow.add(distinct);
      return distinct.output();
    }
  }

  /**
   * Starts building a nameless {@link Distinct} operator to process
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
  public static <IN> MappedBuilder<IN, IN> of(Dataset<IN> input) {
    return new MappedBuilder<>("Distinct", input);
  }

  /**
   * Starts building a named {@link Distinct} operator.
   *
   * @param name a user provided name of the new operator to build
   *
   * @return a builder to complete the setup of the new operator
   */
  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  Distinct(String name,
           Flow flow,
           Dataset<IN> input,
           UnaryFunction<IN, ELEM> mapper,
           @Nullable Windowing<IN, W> windowing,
           Set<OutputHint> outputHints) {

    super(name, flow, input, mapper, windowing, outputHints);
  }

  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    Flow flow = input.getFlow();
    String name = getName() + "::" + "ReduceByKey";
    ReduceByKey<IN, ELEM, Void, Void, W> reduce =
        new ReduceByKey<>(name,
            flow, input, getKeyExtractor(), e -> null,
            windowing,
            (CombinableReduceFunction<Void>) e -> null,
            Collections.emptySet());

    MapElements format = new MapElements<>(
        getName() + "::" + "Map", flow, reduce.output(), Pair::getFirst, getHints());

    DAG<Operator<?, ?>> dag = DAG.of(reduce);
    dag.add(format, reduce);
    return dag;
  }
}
