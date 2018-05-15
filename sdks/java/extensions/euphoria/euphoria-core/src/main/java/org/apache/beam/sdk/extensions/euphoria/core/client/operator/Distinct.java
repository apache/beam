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

import com.google.common.collect.Sets;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.Recommended;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.StateComplexity;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Window;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Windowing;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.CombinableReduceFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.hint.OutputHint;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.extensions.euphoria.core.executor.graph.DAG;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Operator outputting distinct (based on {@link Object#equals}) elements.
 *
 * <h3>Builders:</h3>
 *
 * <ol>
 *   <li>{@code [named] ..................} give name to the operator [optional]
 *   <li>{@code of .......................} input dataset
 *   <li>{@code [mapped] .................} compare objects retrieved by this {@link UnaryFunction}
 *       instead of raw input elements
 *   <li>{@code [windowBy] ...............} windowing function (see {@link Windowing}), default
 *       attached windowing
 *   <li>{@code output ...................} build output dataset
 * </ol>
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
public class Distinct<InputT, OutputT, W extends Window<W>>
    extends StateAwareWindowWiseSingleInputOperator<
        InputT, InputT, InputT, OutputT, OutputT, W, Distinct<InputT, OutputT, W>> {

  Distinct(
      String name,
      Flow flow,
      Dataset<InputT> input,
      UnaryFunction<InputT, OutputT> mapper,
      @Nullable Windowing<InputT, W> windowing,
      Set<OutputHint> outputHints) {

    super(name, flow, input, mapper, windowing, outputHints);
  }

  /**
   * Starts building a nameless {@link Distinct} operator to process the given input dataset.
   *
   * @param <InputT> the type of elements of the input dataset
   * @param input the input data set to be processed
   * @return a builder to complete the setup of the new operator
   * @see #named(String)
   * @see OfBuilder#of(Dataset)
   */
  public static <InputT> MappedBuilder<InputT, InputT> of(Dataset<InputT> input) {
    return new MappedBuilder<>("Distinct", input);
  }

  /**
   * Starts building a named {@link Distinct} operator.
   *
   * @param name a user provided name of the new operator to build
   * @return a builder to complete the setup of the new operator
   */
  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    Flow flow = input.getFlow();
    String name = getName() + "::" + "ReduceByKey";
    ReduceByKey<InputT, OutputT, Void, Void, W> reduce =
        new ReduceByKey<>(
            name,
            flow,
            input,
            getKeyExtractor(),
            e -> null,
            windowing,
            (CombinableReduceFunction<Void>) e -> null,
            Collections.emptySet());

    MapElements format =
        new MapElements<>(
            getName() + "::" + "Map", flow, reduce.output(), Pair::getFirst, getHints());

    DAG<Operator<?, ?>> dag = DAG.of(reduce);
    dag.add(format, reduce);
    return dag;
  }

  /** TODO: complete javadoc. */
  public static class OfBuilder implements Builders.Of {
    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    @Override
    public <InputT> MappedBuilder<InputT, InputT> of(Dataset<InputT> input) {
      return new MappedBuilder<>(name, input);
    }
  }

  /** TODO: complete javadoc. */
  public static class MappedBuilder<InputT, OutputT> extends WindowingBuilder<InputT, OutputT> {

    @SuppressWarnings("unchecked")
    private MappedBuilder(String name, Dataset<InputT> input) {

      super(name, input, (UnaryFunction) e -> e);
    }

    /**
     * Optionally specifies a function to transform the input elements into another type among which
     * to find the distincts.
     *
     * <p>This is, while windowing will be applied on basis of original input elements, the distinct
     * operator will be carried out on the transformed elements.
     *
     * @param <OutputT> the type of the transformed elements
     * @param mapper a transform function applied to input element
     * @return the next builder to complete the setup of the {@link Distinct} operator
     */
    public <OutputT> WindowingBuilder<InputT, OutputT> mapped(
        UnaryFunction<InputT, OutputT> mapper) {
      return new WindowingBuilder<>(name, input, mapper);
    }
  }

  /** TODO: complete javadoc. */
  public static class WindowingBuilder<InputT, OutputT>
      implements Builders.WindowBy<InputT, WindowingBuilder<InputT, OutputT>>,
          Builders.Output<OutputT>,
          OptionalMethodBuilder<WindowingBuilder<InputT, OutputT>> {

    final String name;
    final Dataset<InputT> input;
    final UnaryFunction<InputT, OutputT> mapper;

    private WindowingBuilder(
        String name, Dataset<InputT> input, UnaryFunction<InputT, OutputT> mapper) {

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.mapper = Objects.requireNonNull(mapper);
    }

    @Override
    public <W extends Window<W>> OutputBuilder<InputT, OutputT, W> windowBy(
        Windowing<InputT, W> windowing) {
      return new OutputBuilder<>(name, input, mapper, windowing);
    }

    public Dataset<OutputT> output(OutputHint... outputHints) {
      return new OutputBuilder<>(name, input, mapper, null).output();
    }
  }

  /** TODO: complete javadoc. */
  public static class OutputBuilder<InputT, OutputT, W extends Window<W>>
      extends WindowingBuilder<InputT, OutputT> implements Builders.Output<OutputT> {

    @Nullable private final Windowing<InputT, W> windowing;

    OutputBuilder(
        String name,
        Dataset<InputT> input,
        UnaryFunction<InputT, OutputT> mapper,
        @Nullable Windowing<InputT, W> windowing) {

      super(name, input, mapper);
      this.windowing = windowing;
    }

    @Override
    public Dataset<OutputT> output(OutputHint... outputHints) {
      Flow flow = input.getFlow();
      Distinct<InputT, OutputT, W> distinct =
          new Distinct<>(name, flow, input, mapper, windowing, Sets.newHashSet(outputHints));
      flow.add(distinct);
      return distinct.output();
    }
  }
}
