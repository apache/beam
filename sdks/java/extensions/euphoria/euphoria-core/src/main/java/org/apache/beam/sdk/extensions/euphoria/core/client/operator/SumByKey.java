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
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.Derived;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.StateComplexity;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Window;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Windowing;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Builders;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.OptionalMethodBuilder;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.StateAwareWindowWiseSingleInputOperator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.hint.OutputHint;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.windowing.WindowingDesc;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeAwareUnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeHint;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Sums;
import org.apache.beam.sdk.extensions.euphoria.core.executor.graph.DAG;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * Operator for summing of long values extracted from elements. The sum is operated upon defined key
 * and window.
 *
 * <p>Example:
 *
 * <pre>{@code
 * Dataset<Pair<String, Long>> summed = SumByKey.of(elements)
 *     .keyBy(Pair::getFirst)
 *     .valueBy(Pair::getSecond)
 *     .output();
 * }</pre>
 *
 * <h3>Builders:</h3>
 *
 * <ol>
 * <li>{@code [named] ..................} give name to the operator [optional]
 * <li>{@code of .......................} input dataset
 * <li>{@code keyBy ....................} key extractor function
 * <li>{@code [valueBy] ................} {@link UnaryFunction} transforming from input element to
 * long (default: {@code e -> 1L})
 * <li>{@code [windowBy] ...............} windowing (see {@link WindowFn}), default is no windowing
 * <li>{@code [triggeredBy] ............} defines windowing trigger, follows [windowBy] if called
 * <li>{@code [accumulationMode] .......} windowing accumulation mode, follows [triggeredBy]
 * <li>{@code (output | outputValues) ..} build output dataset
 * </ol>
 */
@Audience(Audience.Type.CLIENT)
@Derived(state = StateComplexity.CONSTANT, repartitions = 1)
public class SumByKey<InputT, K, W extends BoundedWindow>
    extends StateAwareWindowWiseSingleInputOperator<
        InputT, InputT, K, Pair<K, Long>, W, SumByKey<InputT, K, W>> {

  private final UnaryFunction<InputT, Long> valueExtractor;

  SumByKey(
      String name,
      Flow flow,
      Dataset<InputT> input,
      UnaryFunction<InputT, K> keyExtractor,
      UnaryFunction<InputT, Long> valueExtractor,
      @Nullable WindowingDesc<Object, W> windowing,
      @Nullable Windowing euphoriaWindowing) {
    this(name, flow, input, keyExtractor, valueExtractor, windowing, euphoriaWindowing,
        Collections.emptySet());
  }

  SumByKey(
      String name,
      Flow flow,
      Dataset<InputT> input,
      UnaryFunction<InputT, K> keyExtractor,
      UnaryFunction<InputT, Long> valueExtractor,
      @Nullable WindowingDesc<Object, W> windowing,
      @Nullable Windowing euphoriaWindowing,
      Set<OutputHint> outputHints) {
    super(name, flow, input, keyExtractor, windowing, euphoriaWindowing, outputHints);
    this.valueExtractor = valueExtractor;
  }

  /**
   * Starts building a nameless {@link SumByKey} operator to process the given input dataset.
   *
   * @param <InputT> the type of elements of the input dataset
   * @param input the input data set to be processed
   * @return a builder to complete the setup of the new operator
   * @see #named(String)
   * @see OfBuilder#of(Dataset)
   */
  public static <InputT> KeyByBuilder<InputT> of(Dataset<InputT> input) {
    return new KeyByBuilder<>("SumByKey", input);
  }

  /**
   * Starts building a named {@link SumByKey} operator.
   *
   * @param name a user provided name of the new operator to build
   * @return a builder to complete the setup of the new operator
   */
  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    ReduceByKey<InputT, K, Long, Long, W> reduceByKey =
        new ReduceByKey<>(
            getName(),
            input.getFlow(),
            input,
            keyExtractor,
            valueExtractor,
            windowing,
            euphoriaWindowing,
            Sums.ofLongs(),
            getHints());
    return DAG.of(reduceByKey);
  }

  /**
   * Parameters of this operator used in builders.
   */
  private static class BuilderParams<InputT, K, W extends BoundedWindow>
      extends WindowingParams<W> {

    String name;
    Dataset<InputT> input;
    UnaryFunction<InputT, K> keyExtractor;
    UnaryFunction<InputT, Long> valueExtractor;

    BuilderParams(String name,
        Dataset<InputT> input) {
      this.name = name;
      this.input = input;
    }

  }

  /**
   * TODO: complete javadoc.
   */
  public static class OfBuilder implements Builders.Of {

    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    @Override
    public <InputT> KeyByBuilder<InputT> of(Dataset<InputT> input) {
      return new KeyByBuilder<>(name, input);
    }
  }

  /**
   * TODO: complete javadoc.
   */
  public static class KeyByBuilder<InputT> implements Builders.KeyBy<InputT> {

    private final BuilderParams<InputT, ?, ?> params;

    KeyByBuilder(String name, Dataset<InputT> input) {
      this.params = new BuilderParams<>(
          Objects.requireNonNull(name), Objects.requireNonNull(input));
    }

    @Override
    public <K> ValueByWindowByBuilder<InputT, K> keyBy(UnaryFunction<InputT, K> keyExtractor) {

      @SuppressWarnings("unchecked") BuilderParams<InputT, K, ?> paramsCasted =
          (BuilderParams<InputT, K, ?>) params;

      paramsCasted.keyExtractor = Objects.requireNonNull(keyExtractor);

      return new ValueByWindowByBuilder<>(paramsCasted);
    }

    @Override
    public <K> ValueByWindowByBuilder<InputT, K> keyBy(
        UnaryFunction<InputT, K> keyExtractor, TypeHint<K> typeHint) {
      return keyBy(TypeAwareUnaryFunction.of(keyExtractor, typeHint));
    }
  }

  /**
   * TODO: complete javadoc.
   */
  public static class ValueByWindowByBuilder<InputT, K>
      implements Builders.WindowBy<TriggerByBuilder<InputT, K, ?>>,
      Builders.Output<Pair<K, Long>>,
      Builders.OutputValues<K, Long> {

    private final BuilderParams<InputT, K, ?> params;

    ValueByWindowByBuilder(BuilderParams<InputT, K, ?> params) {
      this.params = params;
    }

    public WindowByBuilder<InputT, K> valueBy(UnaryFunction<InputT, Long> valueExtractor) {
      params.valueExtractor = Objects.requireNonNull(valueExtractor);
      return new WindowByBuilder<>(params);
    }

    @Override
    public <W extends BoundedWindow> TriggerByBuilder<InputT, K, W> windowBy(
        WindowFn<Object, W> windowing) {

      @SuppressWarnings("unchecked") BuilderParams<InputT, K, W> paramsCasted =
          (BuilderParams<InputT, K, W>) params;

      paramsCasted.windowFn = Objects.requireNonNull(windowing);
      return new TriggerByBuilder<>(paramsCasted);
    }

    @Override
    public <W extends Window<W>> OutputBuilder<InputT, K, ?> windowBy(Windowing<?, W> windowing) {
      params.euphoriaWindowing = Objects.requireNonNull(windowing);
      return new OutputBuilder<>(params);
    }

    @Override
    public Dataset<Pair<K, Long>> output(OutputHint... outputHints) {

      params.valueExtractor = e -> 1L;

      return new OutputBuilder<>(params).output(outputHints);
    }
  }

  /**
   * TODO: complete javadoc.
   */
  public static class WindowByBuilder<InputT, K>
      implements Builders.WindowBy<TriggerByBuilder<InputT, K, ?>>,
      Builders.Output<Pair<K, Long>>,
      Builders.OutputValues<K, Long>,
      OptionalMethodBuilder<WindowByBuilder<InputT, K>, OutputBuilder<InputT, K, ?>> {

    private final BuilderParams<InputT, K, ?> params;

    WindowByBuilder(BuilderParams<InputT, K, ?> params) {
      this.params = params;
    }

    @Override
    public <W extends BoundedWindow> TriggerByBuilder<InputT, K, W> windowBy(
        WindowFn<Object, W> windowing) {

      @SuppressWarnings("unchecked") BuilderParams<InputT, K, W> paramsCasted =
          (BuilderParams<InputT, K, W>) params;

      paramsCasted.windowFn = Objects.requireNonNull(windowing);
      return new TriggerByBuilder<>(paramsCasted);
    }

    @Override
    public <W extends Window<W>> OutputBuilder<InputT, K, ?> windowBy(Windowing<?, W> windowing) {
      params.euphoriaWindowing = Objects.requireNonNull(windowing);
      return new OutputBuilder<>(params);
    }

    @Override
    public Dataset<Pair<K, Long>> output(OutputHint... outputHints) {
      return new OutputBuilder<>(params).output(outputHints);
    }

    @Override
    public OutputBuilder<InputT, K, ?> applyIf(boolean cond,
        UnaryFunction<WindowByBuilder<InputT, K>,
            OutputBuilder<InputT, K, ?>> applyWhenConditionHolds) {
      Objects.requireNonNull(applyWhenConditionHolds);

      if (cond) {
        return applyWhenConditionHolds.apply(this);
      }

      return new OutputBuilder<>(params);
    }
  }

  /**
   * Trigger defining operator builder.
   */
  public static class TriggerByBuilder<InputT, K, W extends BoundedWindow>
      implements Builders.TriggeredBy<AccumulatorModeBuilder<InputT, K, W>> {

    private final BuilderParams<InputT, K, W> params;

    TriggerByBuilder(BuilderParams<InputT, K, W> params) {
      this.params = params;
    }

    public AccumulatorModeBuilder<InputT, K, W> triggeredBy(Trigger trigger) {
      params.trigger = Objects.requireNonNull(trigger);
      return new AccumulatorModeBuilder<>(params);
    }

  }

  /**
   * {@link WindowingStrategy.AccumulationMode} defining operator builder.
   */
  public static class AccumulatorModeBuilder<InputT, K, W extends BoundedWindow>
      implements Builders.AccumulatorMode<OutputBuilder<InputT, K, W>> {

    private final BuilderParams<InputT, K, W> params;

    AccumulatorModeBuilder(BuilderParams<InputT, K, W> params) {
      this.params = params;
    }

    public OutputBuilder<InputT, K, W> accumulationMode(
        WindowingStrategy.AccumulationMode accumulationMode) {

      params.accumulationMode = Objects.requireNonNull(accumulationMode);
      return new OutputBuilder<>(params);
    }

  }

  /**
   * Last builder in a chain. It concludes this operators creation by calling {@link
   * #output(OutputHint...)}.
   */
  public static class OutputBuilder<InputT, K, W extends BoundedWindow>
      implements Builders.Output<Pair<K, Long>>,
      Builders.OutputValues<K, Long> {

    private final BuilderParams<InputT, K, W> params;

    OutputBuilder(BuilderParams<InputT, K, W> params) {
      this.params = params;
    }

    @Override
    public Dataset<Pair<K, Long>> output(OutputHint... outputHints) {
      Flow flow = params.input.getFlow();
      SumByKey<InputT, K, W> sumByKey =
          new SumByKey<>(
              params.name,
              flow,
              params.input,
              params.keyExtractor,
              params.valueExtractor,
              params.getWindowing(),
              params.euphoriaWindowing,
              Sets.newHashSet(outputHints));
      flow.add(sumByKey);
      return sumByKey.output();
    }
  }
}
