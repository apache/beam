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

import java.util.Collections;
import java.util.Objects;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.Derived;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.StateComplexity;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Window;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Windowing;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.BinaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.CombinableReduceFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.ReduceFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.ReduceFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Builders;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.OptionalMethodBuilder;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.StateAwareWindowWiseSingleInputOperator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.windowing.WindowingDesc;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.extensions.euphoria.core.executor.graph.DAG;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * Reduces all elements in a window. The operator corresponds to {@link ReduceByKey} with the same
 * key for all elements, so the actual key is defined only by window.
 *
 * <p>Custom {@link Windowing} can be set, otherwise values from input operator are used.
 *
 * <h3>Builders:</h3>
 *
 * <ol>
 * <li>{@code [named] ..................} give name to the operator [optional]
 * <li>{@code of .......................} input dataset
 * <li>{@code [valueBy] ................} value extractor function (default: identity)
 * <li>{@code (combineBy | reduceBy)....} {@link CombinableReduceFunction} or {@link
 * ReduceFunction} for combinable or non-combinable function
 * <li>{@code [withSortedValues] .......} use comparator for sorting values prior to being passed
 * to {@link ReduceFunction} function (applicable only for non-combinable version)
 * <li>{@code [windowBy] ...............} windowing (see {@link WindowFn}), default is no windowing
 * <li>{@code [triggeredBy] ............} defines windowing trigger, follows [windowBy] if called
 * <li>{@code [accumulationMode] .......} windowing accumulation mode, follows [triggeredBy]
 * <li>{@code output ...................} build output dataset
 * </ol>
 */
@Audience(Audience.Type.CLIENT)
@Derived(state = StateComplexity.CONSTANT_IF_COMBINABLE, repartitions = 1)
public class ReduceWindow<InputT, V, OutputT, W extends BoundedWindow>
    extends StateAwareWindowWiseSingleInputOperator<
        InputT, InputT, InputT, Byte, OutputT, W, ReduceWindow<InputT, V, OutputT, W>> {

  private static final Byte B_ZERO = (byte) 0;
  final UnaryFunction<InputT, V> valueExtractor;
  final BinaryFunction<V, V, Integer> valueComparator;
  private final ReduceFunctor<V, OutputT> reducer;

  private ReduceWindow(
      String name,
      Flow flow,
      Dataset<InputT> input,
      UnaryFunction<InputT, V> valueExtractor,
      @Nullable WindowingDesc<Object, W> windowing,
      @Nullable Windowing euphoriaWindowing,
      ReduceFunctor<V, OutputT> reducer,
      @Nullable BinaryFunction<V, V, Integer> valueComparator) {

    super(name, flow, input, e -> B_ZERO, windowing, euphoriaWindowing, Collections.emptySet());
    this.reducer = reducer;
    this.valueExtractor = valueExtractor;
    this.valueComparator = valueComparator;
  }

  /**
   * Starts building a nameless {@link ReduceWindow} operator to process the given input dataset.
   *
   * @param <InputT> the type of elements of the input dataset
   * @param input the input data set to be processed
   * @return a builder to complete the setup of the new operator
   * @see #named(String)
   * @see OfBuilder#of(Dataset)
   */
  public static <InputT> ValueBuilder<InputT> of(Dataset<InputT> input) {
    return new ValueBuilder<>("ReduceWindow", input);
  }

  /**
   * Starts building a named {@link ReduceWindow} operator.
   *
   * @param name a user provided name of the new operator to build
   * @return a builder to complete the setup of the new operator
   */
  public static OfBuilder named(String name) {
    return new OfBuilder(Objects.requireNonNull(name));
  }

  public ReduceFunctor<V, OutputT> getReducer() {
    return reducer;
  }

  @SuppressWarnings("unchecked")
  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    // implement this operator via `ReduceByKey`
    final ReduceByKey rbk;
    final DAG<Operator<?, ?>> dag = DAG.empty();
    if (windowing != null) {
      rbk =
          new ReduceByKey<>(
              getName() + "::ReduceByKey",
              getFlow(),
              input,
              getKeyExtractor(),
              valueExtractor,
              windowing,
              euphoriaWindowing,
              reducer,
              valueComparator,
              getHints());
      dag.add(rbk);
    } else {
      // otherwise we use attached windowing, therefore
      // we already know the window lables and can do group-by these
      // labels to increase parallelism
      FlatMap<InputT, Pair<Window<?>, InputT>> map =
          new FlatMap<>(
              getName() + "::window-to-key",
              getFlow(),
              input,
              (InputT in, Collector<Pair<Window<?>, InputT>> c) -> {
                c.collect(Pair.of(c.getWindow(), in));
              },
              null);
      rbk =
          new ReduceByKey<>(
              getName() + "::ReduceByKey::attached",
              getFlow(),
              map.output(),
              Pair::getFirst,
              p -> valueExtractor.apply(p.getSecond()),
              null,
              null,
              reducer,
              valueComparator,
              getHints());
      dag.add(map);
      dag.add(rbk);
    }

    MapElements<Pair<Object, OutputT>, OutputT> format =
        new MapElements<Pair<Object, OutputT>, OutputT>(
            getName() + "::MapElements", getFlow(), (Dataset) rbk.output(), Pair::getSecond);

    dag.add(format);
    return dag;
  }

  private static <V, OutputT> ReduceFunctor<V, OutputT> reduceFunctionToFunctor(
      ReduceFunction<V, OutputT> reducer) {
    return (Stream<V> in, Collector<OutputT> ctx) -> ctx.collect(reducer.apply(in));
  }

  /**
   * Parameters of this operator used in builders.
   */
  private static class BuilderParams<InputT, V, OutputT, W extends BoundedWindow>
      extends WindowingParams<W> {

    String name;
    Dataset<InputT> input;
    UnaryFunction<InputT, V> valueExtractor;
    ReduceFunctor<V, OutputT> reducer;
    @Nullable
    BinaryFunction<V, V, Integer> valueComparator;

    public BuilderParams(String name,
        Dataset<InputT> input) {
      this.name = name;
      this.input = input;
    }

  }

  /**
   * TODO: complete javadoc.
   */
  public static class OfBuilder implements Builders.Of {

    final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    @Override
    public <T> ValueBuilder<T> of(Dataset<T> input) {
      return new ValueBuilder<>(name, Objects.requireNonNull(input));
    }

  }

  /**
   * TODO: complete javadoc.
   */
  public static class ValueBuilder<InputT> {

    private final BuilderParams<InputT, ?, ?, ?> params;

    ValueBuilder(String name, Dataset<InputT> input) {
      this.params = new BuilderParams<>(name, input);
    }

    public <V> ReduceBuilder<InputT, V> valueBy(UnaryFunction<InputT, V> valueExtractor) {

      @SuppressWarnings("unchecked") BuilderParams<InputT, V, ?, ?> paramsCasted =
          (BuilderParams<InputT, V, ?, ?>) params;

      paramsCasted.valueExtractor = Objects.requireNonNull(valueExtractor);
      return new ReduceBuilder<>(paramsCasted);
    }

    public <OutputT> SortableOutputBuilder<InputT, InputT, OutputT> reduceBy(
        ReduceFunction<InputT, OutputT> reducer) {

      Objects.requireNonNull(reducer);
      @SuppressWarnings("unchecked") BuilderParams<InputT, InputT, OutputT, ?> paramsCasted =
          (BuilderParams<InputT, InputT, OutputT, ?>) params;

      paramsCasted.valueExtractor = e -> e;
      paramsCasted.reducer = reduceFunctionToFunctor(reducer);

      return new SortableOutputBuilder<>(paramsCasted);
    }

    public <OutputT> SortableOutputBuilder<InputT, InputT, OutputT> reduceBy(
        ReduceFunctor<InputT, OutputT> reducer) {

      Objects.requireNonNull(reducer);
      @SuppressWarnings("unchecked") BuilderParams<InputT, InputT, OutputT, ?> paramsCasted =
          (BuilderParams<InputT, InputT, OutputT, ?>) params;

      paramsCasted.valueExtractor = e -> e;
      paramsCasted.reducer = reducer;

      return new SortableOutputBuilder<>(paramsCasted);
    }

    public WindowByBuilder<InputT, InputT, InputT> combineBy(
        CombinableReduceFunction<InputT> reducer) {

      Objects.requireNonNull(reducer);
      @SuppressWarnings("unchecked") BuilderParams<InputT, InputT, InputT, ?> paramsCasted =
          (BuilderParams<InputT, InputT, InputT, ?>) params;

      paramsCasted.valueExtractor = e -> e;
      paramsCasted.reducer = reduceFunctionToFunctor(reducer);

      return new WindowByBuilder<>(paramsCasted);
    }

  }

  /**
   * TODO: complete javadoc.
   */
  public static class ReduceBuilder<InputT, V> {

    private final BuilderParams<InputT, V, ?, ?> params;

    public ReduceBuilder(BuilderParams<InputT, V, ?, ?> params) {
      this.params = params;
    }

    public <OutputT> SortableOutputBuilder<InputT, V, OutputT> reduceBy(
        ReduceFunction<V, OutputT> reducer) {
      return reduceBy(reduceFunctionToFunctor(Objects.requireNonNull(reducer)));
    }

    public <OutputT> SortableOutputBuilder<InputT, V, OutputT> reduceBy(
        ReduceFunctor<V, OutputT> reducer) {

      @SuppressWarnings("unchecked") BuilderParams<InputT, V, OutputT, ?> paramsCasted =
          (BuilderParams<InputT, V, OutputT, ?>) params;

      paramsCasted.reducer = Objects.requireNonNull(reducer);

      return new SortableOutputBuilder<>(paramsCasted);
    }

    public WindowByBuilder<InputT, V, V> combineBy(CombinableReduceFunction<V> reducer) {

      @SuppressWarnings("unchecked") BuilderParams<InputT, V, V, ?> paramsCasted =
          (BuilderParams<InputT, V, V, ?>) params;

      Objects.requireNonNull(reducer);
      paramsCasted.reducer = ReduceByKey.toReduceFunctor(reducer);
      return new WindowByBuilder<>(paramsCasted);
    }
  }

  /**
   * TODO: complete javadoc.
   */
  public static class WindowByBuilder<InputT, V, OutputT>
      implements Builders.WindowBy<TriggerByBuilder<InputT, V, OutputT, ?>>,
      OptionalMethodBuilder<WindowByBuilder<InputT, V, OutputT>,
                OutputBuilder<InputT, V, OutputT, ?>> {

    private final BuilderParams<InputT, V, OutputT, ?> params;

    public WindowByBuilder(BuilderParams<InputT, V, OutputT, ?> params) {
      this.params = params;
    }

    public Dataset<OutputT> output() {
      return new OutputBuilder<>(params).output();
    }

    @Override
    public <W extends BoundedWindow> TriggerByBuilder<InputT, V, OutputT, W> windowBy(
        WindowFn<Object, W> windowing) {

      @SuppressWarnings("unchecked") BuilderParams<InputT, V, OutputT, W> paramsCasted =
          (BuilderParams<InputT, V, OutputT, W>) params;

      paramsCasted.windowFn = Objects.requireNonNull(windowing);
      return new TriggerByBuilder<>(paramsCasted);
    }

    @Override
    public <W extends Window<W>> OutputBuilder<InputT, V, OutputT, ?> windowBy(
        Windowing<?, W> windowing) {
      params.euphoriaWindowing = Objects.requireNonNull(windowing);
      return new OutputBuilder<>(params);
    }

    @Override
    public OutputBuilder<InputT, V, OutputT, ?> applyIf(boolean cond,
        UnaryFunction<WindowByBuilder<InputT, V, OutputT>,
            OutputBuilder<InputT, V, OutputT, ?>> applyWhenConditionHolds) {
      Objects.requireNonNull(applyWhenConditionHolds);
      if (cond) {
        return applyWhenConditionHolds.apply(this);
      }

      return new OutputBuilder<>(params);
    }
  }

  /**
   * Last builder in a chain. It concludes this operators creation by calling {@link #output()}.
   */
  public static class OutputBuilder<InputT, V, OutputT, W extends BoundedWindow> {

    private final BuilderParams<InputT, V, OutputT, W> params;

    public OutputBuilder(BuilderParams<InputT, V, OutputT, W> params) {
      this.params = params;
    }

    public Dataset<OutputT> output() {
      Flow flow = params.input.getFlow();
      ReduceWindow<InputT, V, OutputT, ?> operator =
          new ReduceWindow<>(
              params.name, flow, params.input, params.valueExtractor, params.getWindowing(),
              params.euphoriaWindowing, params.reducer, params.valueComparator);
      flow.add(operator);
      return operator.output();
    }

  }

  /**
   * Trigger defining operator builder.
   */
  public static class TriggerByBuilder<InputT, V, OutputT, W extends BoundedWindow>
      implements Builders.TriggeredBy<AccumulatorModeBuilder<InputT, V, OutputT, W>> {

    private final BuilderParams<InputT, V, OutputT, W> params;

    TriggerByBuilder(BuilderParams<InputT, V, OutputT, W> params) {
      this.params = params;
    }

    public AccumulatorModeBuilder<InputT, V, OutputT, W> triggeredBy(Trigger trigger) {
      params.trigger = Objects.requireNonNull(trigger);
      return new AccumulatorModeBuilder<>(params);
    }

  }

  /**
   * {@link WindowingStrategy.AccumulationMode} defining operator builder.
   */
  public static class AccumulatorModeBuilder<InputT, V, OutputT, W extends BoundedWindow>
      implements Builders.AccumulatorMode<OutputBuilder<InputT, V, OutputT, W>> {

    private final BuilderParams<InputT, V, OutputT, W> params;

    AccumulatorModeBuilder(BuilderParams<InputT, V, OutputT, W> params) {
      this.params = params;
    }

    public OutputBuilder<InputT, V, OutputT, W> accumulationMode(
        WindowingStrategy.AccumulationMode accumulationMode) {

      params.accumulationMode = Objects.requireNonNull(accumulationMode);
      return new OutputBuilder<>(params);
    }

  }

  /**
   * TODO: complete javadoc.
   */
  public static class SortableOutputBuilder<InputT, V, OutputT>
      implements Builders.WindowBy<TriggerByBuilder<InputT, V, OutputT, ?>> {

    private final BuilderParams<InputT, V, OutputT, ?> params;

    public SortableOutputBuilder(BuilderParams<InputT, V, OutputT, ?> params) {
      this.params = params;
    }

    /**
     * Sort values going to `reduceBy` function by given comparator.
     *
     * @param comparator function with contract defined by {@code java.util.Comparator#compare}.
     * @return next step builder
     */
    public WindowByBuilder<InputT, V, OutputT> withSortedValues(
        BinaryFunction<V, V, Integer> comparator) {
      params.valueComparator = Objects.requireNonNull(comparator);

      return new WindowByBuilder<>(params);
    }

    public Dataset<OutputT> output() {
      return new OutputBuilder<>(params).output();
    }

    @Override
    public <W extends BoundedWindow> TriggerByBuilder<InputT, V, OutputT, ?> windowBy(
        WindowFn<Object, W> windowing) {

      return new WindowByBuilder<>(params).windowBy(windowing);
    }

    @Override
    public <W extends Window<W>> OutputBuilder<InputT, V, OutputT, ?> windowBy(
        Windowing<?, W> windowing) {
      params.euphoriaWindowing = Objects.requireNonNull(windowing);
      return new OutputBuilder<>(params);
    }
  }
}
