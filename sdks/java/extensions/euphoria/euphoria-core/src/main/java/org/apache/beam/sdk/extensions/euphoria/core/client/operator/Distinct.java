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
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.Recommended;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.StateComplexity;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Window;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Windowing;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.CombinableReduceFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Builders;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.OptionalMethodBuilder;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.StateAwareWindowWiseSingleInputOperator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.hint.OutputHint;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.windowing.WindowingDesc;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeUtils;
import org.apache.beam.sdk.extensions.euphoria.core.executor.graph.DAG;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.WindowingStrategy;

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
 *   <li>{@code [windowBy] ...............} windowing (see {@link WindowFn}), default is no
 *       windowing
 *   <li>{@code [triggeredBy] ............} defines windowing trigger, follows [windowBy] if called
 *   <li>{@code [accumulationMode] .......} windowing accumulation mode, follows [triggeredBy]
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
public class Distinct<InputT, OutputT, W extends BoundedWindow>
    extends StateAwareWindowWiseSingleInputOperator<
        InputT, InputT, OutputT, OutputT, W, Distinct<InputT, OutputT, W>> {

  Distinct(
      String name,
      Flow flow,
      Dataset<InputT> input,
      UnaryFunction<InputT, OutputT> mapper,
      TypeDescriptor<OutputT> outputType,
      @Nullable WindowingDesc<Object, W> windowing,
      @Nullable Windowing euphoriaWindowing,
      Set<OutputHint> outputHints) {

    super(
        name,
        flow,
        input,
        outputType,
        mapper,
        outputType,
        windowing,
        euphoriaWindowing,
        outputHints);
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
  public static <InputT> MappedBuilder<InputT> of(Dataset<InputT> input) {
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
            outputType,
            e -> null,
            TypeDescriptors.nulls(),
            windowing,
            euphoriaWindowing,
            (CombinableReduceFunction<Void>) e -> null,
            Collections.emptySet(),
            TypeUtils.keyValues(outputType, TypeDescriptors.nulls()));

    MapElements format =
        new MapElements<>(
            getName() + "::" + "Map", flow, reduce.output(), KV::getKey, getHints(), outputType);

    DAG<Operator<?, ?>> dag = DAG.of(reduce);
    dag.add(format, reduce);
    return dag;
  }

  private static class BuilderParams<InputT, OutputT, W extends BoundedWindow>
      extends WindowingParams<W> {

    String name;
    Dataset<InputT> input;
    UnaryFunction<InputT, OutputT> mapper;
    TypeDescriptor<OutputT> outputTypeDescriptor;

    public BuilderParams(String name, Dataset<InputT> input) {
      this.name = name;
      this.input = input;
    }
  }

  /** TODO: complete javadoc. */
  public static class OfBuilder implements Builders.Of {

    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    @Override
    public <InputT> MappedBuilder<InputT> of(Dataset<InputT> input) {
      return new MappedBuilder<>(name, input);
    }
  }

  /** TODO: complete javadoc. */
  public static class MappedBuilder<InputT>
      implements Builders.WindowBy<TriggerByBuilder<InputT, InputT, ?>>,
          Builders.Output<InputT>,
          OptionalMethodBuilder<MappedBuilder<InputT>, OutputBuilder<InputT, ?, ?>> {

    private final BuilderParams<InputT, ?, ?> params;

    private MappedBuilder(String name, Dataset<InputT> input) {
      params = new BuilderParams<>(Objects.requireNonNull(name), Objects.requireNonNull(input));
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

      return mapped(mapper, null);
    }

    public <OutputT> WindowingBuilder<InputT, OutputT> mapped(
        UnaryFunction<InputT, OutputT> mapper, TypeDescriptor<OutputT> outputTypeDescriptor) {

      @SuppressWarnings("unchecked")
      BuilderParams<InputT, OutputT, ?> paramsCasted = (BuilderParams<InputT, OutputT, ?>) params;

      paramsCasted.mapper = Objects.requireNonNull(mapper);
      paramsCasted.outputTypeDescriptor = outputTypeDescriptor;

      return new WindowingBuilder<>(paramsCasted);
    }

    @Override
    public Dataset<InputT> output(OutputHint... outputHints) {

      @SuppressWarnings("unchecked")
      BuilderParams<InputT, InputT, ?> paramsCasted = (BuilderParams<InputT, InputT, ?>) params;

      setIdentityMapper(paramsCasted);
      return new OutputBuilder<>(paramsCasted).output();
    }

    @Override
    public <W extends BoundedWindow> TriggerByBuilder<InputT, InputT, ?> windowBy(
        WindowFn<Object, W> windowing) {

      @SuppressWarnings("unchecked")
      BuilderParams<InputT, InputT, W> paramsCasted = (BuilderParams<InputT, InputT, W>) params;

      setIdentityMapper(paramsCasted);
      paramsCasted.windowFn = Objects.requireNonNull(windowing);

      return new TriggerByBuilder<>(paramsCasted);
    }

    private void setIdentityMapper(BuilderParams<InputT, InputT, ?> paramsCasted) {
      paramsCasted.mapper = UnaryFunction.identity();

      // retrieve output type from previous operator input
      paramsCasted.outputTypeDescriptor = TypeUtils.getDatasetElementType(paramsCasted.input);
    }

    @Override
    public <W extends Window<W>> OutputBuilder<InputT, ?, ?> windowBy(Windowing<?, W> windowing) {
      params.euphoriaWindowing = Objects.requireNonNull(windowing);
      return new OutputBuilder<>(params);
    }

    @Override
    public OutputBuilder<InputT, ?, ?> applyIf(
        boolean cond,
        UnaryFunction<MappedBuilder<InputT>, OutputBuilder<InputT, ?, ?>> applyWhenConditionHolds) {
      Objects.requireNonNull(applyWhenConditionHolds);

      if (cond) {
        return applyWhenConditionHolds.apply(this);
      } else {
        return new OutputBuilder<>(params);
      }
    }
  }

  /** TODO: complete javadoc. */
  public static class WindowingBuilder<InputT, OutputT>
      implements Builders.WindowBy<TriggerByBuilder<InputT, OutputT, ?>>,
          Builders.Output<OutputT>,
          OptionalMethodBuilder<
              WindowingBuilder<InputT, OutputT>, OutputBuilder<InputT, OutputT, ?>> {

    private final BuilderParams<InputT, OutputT, ?> params;

    private WindowingBuilder(BuilderParams<InputT, OutputT, ?> params) {
      this.params = params;
    }

    @Override
    public <W extends BoundedWindow> TriggerByBuilder<InputT, OutputT, W> windowBy(
        WindowFn<Object, W> windowing) {

      @SuppressWarnings("unchecked")
      BuilderParams<InputT, OutputT, W> paramsCasted = (BuilderParams<InputT, OutputT, W>) params;

      paramsCasted.windowFn = Objects.requireNonNull(windowing);

      return new TriggerByBuilder<>(paramsCasted);
    }

    @Override
    public Dataset<OutputT> output(OutputHint... outputHints) {
      return new OutputBuilder<>(params).output();
    }

    @Override
    public <W extends Window<W>> OutputBuilder<InputT, OutputT, ?> windowBy(
        Windowing<?, W> windowing) {
      params.euphoriaWindowing = Objects.requireNonNull(windowing);
      return new OutputBuilder<>(params);
    }

    @Override
    public OutputBuilder<InputT, OutputT, ?> applyIf(
        boolean cond,
        UnaryFunction<WindowingBuilder<InputT, OutputT>, OutputBuilder<InputT, OutputT, ?>>
            applyWhenConditionHolds) {
      Objects.requireNonNull(applyWhenConditionHolds);

      if (cond) {
        return applyWhenConditionHolds.apply(this);
      } else {
        return new OutputBuilder<>(params);
      }
    }
  }

  /** Trigger defining operator builder. */
  public static class TriggerByBuilder<InputT, OutputT, W extends BoundedWindow>
      implements Builders.TriggeredBy<AccumulatorModeBuilder<InputT, OutputT, W>> {

    private final BuilderParams<InputT, OutputT, W> params;

    TriggerByBuilder(BuilderParams<InputT, OutputT, W> params) {
      this.params = params;
    }

    @Override
    public AccumulatorModeBuilder<InputT, OutputT, W> triggeredBy(Trigger trigger) {
      params.trigger = Objects.requireNonNull(trigger);
      return new AccumulatorModeBuilder<>(params);
    }
  }

  /** {@link WindowingStrategy.AccumulationMode} defining operator builder. */
  public static class AccumulatorModeBuilder<InputT, OutputT, W extends BoundedWindow>
      implements Builders.AccumulatorMode<OutputBuilder<InputT, OutputT, W>> {

    private final BuilderParams<InputT, OutputT, W> params;

    AccumulatorModeBuilder(BuilderParams<InputT, OutputT, W> params) {
      this.params = params;
    }

    @Override
    public OutputBuilder<InputT, OutputT, W> accumulationMode(
        WindowingStrategy.AccumulationMode accumulationMode) {

      params.accumulationMode = Objects.requireNonNull(accumulationMode);
      return new OutputBuilder<>(params);
    }
  }

  /**
   * Last builder in a chain. It concludes this operators creation by calling {@link
   * #output(OutputHint...)}.
   */
  public static class OutputBuilder<InputT, OutputT, W extends BoundedWindow>
      implements Builders.Output<OutputT> {

    private final BuilderParams<InputT, OutputT, W> params;

    OutputBuilder(BuilderParams<InputT, OutputT, W> params) {
      this.params = params;
    }

    @Override
    public Dataset<OutputT> output(OutputHint... outputHints) {
      Flow flow = params.input.getFlow();
      Distinct<InputT, OutputT, W> distinct =
          new Distinct<>(
              params.name,
              flow,
              params.input,
              params.mapper,
              params.outputTypeDescriptor,
              params.getWindowing(),
              params.euphoriaWindowing,
              Sets.newHashSet(outputHints));
      flow.add(distinct);
      return distinct.output();
    }
  }
}
