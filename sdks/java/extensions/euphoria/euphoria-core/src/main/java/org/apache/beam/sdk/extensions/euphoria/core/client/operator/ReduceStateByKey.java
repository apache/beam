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
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.Basic;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.StateComplexity;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.MergingWindowing;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Window;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Windowing;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Builders;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.OptionalMethodBuilder;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.StateAwareWindowWiseSingleInputOperator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.hint.OutputHint;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.State;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.StateFactory;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.StateMerger;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.windowing.WindowingDesc;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeAwareUnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeHint;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * A {@link ReduceStateByKey} operator is a stateful, complex, lower-level-api, but very powerful
 * processor and serves as the basis for easier-to-use operators. Client API users have generally
 * little motivation to use it directly and should prefer {@link ReduceByKey} when possible.
 *
 * <p>The operator assigns each input item to a set of windows (through a user provided {@link
 * Windowing} implementation) and turns the item into a key/value pair. For each of the assigned
 * windows the extracted value is accumulated using a user provided {@link StateFactory state
 * implementation} under the extracted key. I.e. the value is accumulated into a state identified by
 * a key/window pair.
 *
 * <p>Example:
 *
 * <pre>{@code
 * Dataset<String> words = ...;
 * Dataset<Pair<String, Integer>> counts =
 *     ReduceStateByKey.named("WORD-COUNT")
 *         .of(words)
 *         .keyBy(s -> s)
 *         .valueBy(s -> 1)
 *         .stateFactory(WordCountState::new)
 *         .mergeStatesBy(WordCountState::merge)
 *         .windowBy(Time.of(Duration.ofHours(1))
 *         .output();
 * }</pre>
 *
 * <p>This example constitutes a windowed word-count program. Each input element is treated as a
 * key to identify an imaginary {@code WordCountState} within each time window assigned to the input
 * element. For such a key/window combination the value {@code 1} gets sent to the corresponding
 * state.
 *
 * <p>States are emitted based on the used {@link Windowing} strategy. See that for more
 * information. Generally speaking, when a window is emitted/triggered, the states of all keys
 * within the emitted window are flushed.
 *
 * <p>Note that defining a {@link Windowing} strategy is actually optional. If none is defined the
 * operator will act in the so-call "attached windowing" mode, i.e. it will attach itself to the
 * windowing strategy active on they way from its input.
 *
 * <h3>Builders:</h3>
 *
 * <ol>
 * <li>{@code [named] ..................} give name to the operator [optional]
 * <li>{@code of .......................} input dataset
 * <li>{@code keyBy ....................} key extractor function
 * <li>{@code valueBy ..................} value extractor function
 * <li>{@code stateFactory .............} factory method for {@link State} (see {@link
 * StateFactory})
 * <li>{@code mergeStatesBy ............} state merge function (see {@link StateMerger})
 * <li>{@code [windowBy] ...............} windowing (see {@link WindowFn}), default is no windowing
 * <li>{@code [triggeredBy] ............} defines windowing trigger, follows [windowBy] if called
 * <li>{@code [accumulationMode] .......} windowing accumulation mode, follows [triggeredBy]
 * <li>{@code (output | outputValues) ..} build output dataset
 * </ol>
 *
 * @param <InputT> the type of input elements
 * @param <K> the type of the key (result type of {@code #keyBy}
 * @param <V> the type of the accumulated values (result type of {@code #valueBy})
 * @param <OutputT> the type of the output elements (result type of the accumulated state)
 */
@Audience(Audience.Type.CLIENT)
@Basic(state = StateComplexity.CONSTANT_IF_COMBINABLE, repartitions = 1)
public class ReduceStateByKey<
    InputT, K, V, OutputT, StateT extends State<V, OutputT>, W extends BoundedWindow>
    extends StateAwareWindowWiseSingleInputOperator<
        InputT, InputT, K, Pair<K, OutputT>, W,
        ReduceStateByKey<InputT, K, V, OutputT, StateT, W>> {

  private final StateFactory<V, OutputT, StateT> stateFactory;

  // builder classes used when input is Dataset<InputT> ----------------------
  private final UnaryFunction<InputT, V> valueExtractor;
  private final StateMerger<V, OutputT, StateT> stateCombiner;

  ReduceStateByKey(
      String name,
      Flow flow,
      Dataset<InputT> input,
      UnaryFunction<InputT, K> keyExtractor,
      UnaryFunction<InputT, V> valueExtractor,
      @Nullable WindowingDesc<Object, W> windowing,
      @Nullable Windowing euphoriaWindowing,
      StateFactory<V, OutputT, StateT> stateFactory,
      StateMerger<V, OutputT, StateT> stateMerger,
      Set<OutputHint> outputHints) {
    super(name, flow, input, keyExtractor, windowing, euphoriaWindowing, outputHints);
    this.stateFactory = stateFactory;
    this.valueExtractor = valueExtractor;
    this.stateCombiner = stateMerger;
  }

  /**
   * Starts building a nameless {@link ReduceStateByKey} operator to process the given input
   * dataset.
   *
   * @param <InputT> the type of elements of the input dataset
   * @param input the input data set to be processed
   * @return a builder to complete the setup of the new operator
   * @see #named(String)
   * @see OfBuilder#of(Dataset)
   */
  public static <InputT> KeyByBuilder<InputT> of(Dataset<InputT> input) {
    return new KeyByBuilder<>("ReduceStateByKey", Objects.requireNonNull(input));
  }

  /**
   * Starts building a named {@link ReduceStateByKey} operator.
   *
   * @param name a user provided name of the new operator to build
   * @return a builder to complete the setup of the new operator
   */
  public static OfBuilder named(String name) {
    return new OfBuilder(Objects.requireNonNull(name));
  }

  /**
   * Retrieves the user defined state factory function.
   *
   * @return the user provided state factory
   */
  public StateFactory<V, OutputT, StateT> getStateFactory() {
    return stateFactory;
  }

  /**
   * Retrieves the user defined state merger function.
   *
   * @return the user provided state merger
   */
  public StateMerger<V, OutputT, StateT> getStateMerger() {
    return stateCombiner;
  }

  /**
   * Retrieves the user defined key extractor function.
   *
   * @return the user provided key extractor function
   */
  public UnaryFunction<InputT, V> getValueExtractor() {
    return valueExtractor;
  }

  /**
   * Parameters of this operator used in builders.
   */
  public static class BuilderParams<
      InputT, K, V, OutputT, StateT extends State<V, OutputT>, W extends BoundedWindow>
      extends WindowingParams<W> {

    String name;
    Dataset<InputT> input;
    UnaryFunction<InputT, K> keyExtractor;
    UnaryFunction<InputT, V> valueExtractor;
    StateFactory<V, OutputT, StateT> stateFactory;
    StateMerger<V, OutputT, StateT> stateMerger;

    public BuilderParams(String name, Dataset<InputT> input,
        UnaryFunction<InputT, K> keyExtractor) {
      this.name = name;
      this.input = input;
      this.keyExtractor = keyExtractor;
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
      return new KeyByBuilder<>(name, Objects.requireNonNull(input));
    }
  }

  /**
   * TODO: complete javadoc.
   */
  public static class KeyByBuilder<InputT> implements Builders.KeyBy<InputT> {

    private final String name;
    private final Dataset<InputT> input;

    KeyByBuilder(String name, Dataset<InputT> input) {
      this.name = name;
      this.input = input;
    }

    @Override
    public <K> ValueByBuilder<InputT, K> keyBy(UnaryFunction<InputT, K> keyExtractor) {

      BuilderParams<InputT, K, ?, ?, ?, ?> params = new BuilderParams<>(
          name, input, Objects.requireNonNull(keyExtractor));

      return new ValueByBuilder<>(params);
    }

    public <K> ValueByBuilder<InputT, K> keyBy(
        UnaryFunction<InputT, K> keyExtractor, TypeHint<K> typeHint) {

      return keyBy(TypeAwareUnaryFunction.of(keyExtractor, typeHint));
    }

    public <K> ValueByBuilder<InputT, K> keyBy(
        UnaryFunction<InputT, K> keyExtractor, Class<K> typeHint) {
      return keyBy(TypeAwareUnaryFunction.of(keyExtractor, TypeHint.of(typeHint)));
    }
  }

  /**
   * TODO: complete javadoc.
   */
  public static class ValueByBuilder<InputT, K> {

    private final BuilderParams<InputT, K, ?, ?, ?, ?> params;

    ValueByBuilder(BuilderParams<InputT, K, ?, ?, ?, ?> params) {
      this.params = params;
    }

    /**
     * Specifies the function to derive a value from the {@link ReduceStateByKey} operator's input
     * elements to get accumulated by a later supplied state implementation.
     *
     * @param <V> the type of the extracted values
     * @param valueExtractor a user defined function to extract values from the processed input
     * dataset's elements for later accumulation
     * @return the next builder to complete the setup of the {@link ReduceStateByKey} operator
     */
    public <V> StateFactoryBuilder<InputT, K, V> valueBy(UnaryFunction<InputT, V> valueExtractor) {
      @SuppressWarnings("unchecked") final BuilderParams<InputT, K, V, ?, ?, ?> paramsCasted =
          (BuilderParams<InputT, K, V, ?, ?, ?>) params;

      paramsCasted.valueExtractor = Objects.requireNonNull(valueExtractor);
      return new StateFactoryBuilder<>(paramsCasted);
    }
  }

  /**
   * TODO: complete javadoc.
   */
  public static class StateFactoryBuilder<InputT, K, V> {

    private final BuilderParams<InputT, K, V, ?, ?, ?> params;

    StateFactoryBuilder(BuilderParams<InputT, K, V, ?, ?, ?> params) {
      this.params = params;
    }

    /**
     * Specifies a factory for creating new/empty/blank state instances.
     *
     * @param <OutputT> the type of output elements state instances will produce; along with the
     * "key", this is part of the type of output elements the {@link ReduceStateByKey} operator will
     * produce as such
     * @param <StateT> the type of the state (implementation)
     * @param stateFactory a user supplied function to create new state instances
     * @return the next builder to complete the setup of the {@link ReduceStateByKey} operator
     */
    public <OutputT, StateT extends State<V, OutputT>>
    MergeStateByBuilder<InputT, K, V, OutputT, StateT> stateFactory(
        StateFactory<V, OutputT, StateT> stateFactory) {

      @SuppressWarnings("unchecked") final BuilderParams<InputT, K, V, OutputT, StateT, ?>
          paramsCasted = (BuilderParams<InputT, K, V, OutputT, StateT, ?>) params;

      paramsCasted.stateFactory = Objects.requireNonNull(stateFactory);

      return new MergeStateByBuilder<>(paramsCasted);
    }
  }

  /**
   * TODO: complete javadoc.
   */
  public static class MergeStateByBuilder<InputT, K, V, OutputT, StateT extends State<V, OutputT>> {

    private final BuilderParams<InputT, K, V, OutputT, StateT, ?> params;

    MergeStateByBuilder(BuilderParams<InputT, K, V, OutputT, StateT, ?> params) {
      this.params = params;
    }

    /**
     * Specifies the merging function to be utilized when states get merged as part of {@link
     * MergingWindowing window merging}.
     *
     * @param stateMerger a user defined function to merge mutilple states into a specified target
     * state
     * @return the next builder to complete the setup of the {@link ReduceStateByKey} operator
     */
    public WindowOfBuilder<InputT, K, V, OutputT, StateT> mergeStatesBy(
        StateMerger<V, OutputT, StateT> stateMerger) {

      params.stateMerger = Objects.requireNonNull(stateMerger);
      return new WindowOfBuilder<>(params);
    }
  }

  /**
   * TODO: complete javadoc.
   */
  public static class WindowOfBuilder<InputT, K, V, OutputT, StateT extends State<V, OutputT>>
      implements Builders.WindowBy<TriggerByBuilder<InputT, K, V, OutputT, StateT, ?>>,
      Builders.Output<Pair<K, OutputT>>,
      Builders.OutputValues<K, OutputT>,
      OptionalMethodBuilder<WindowOfBuilder<InputT, K, V, OutputT, StateT>,
                OutputBuilder<InputT, K, V, OutputT, StateT, ?>> {

    private final BuilderParams<InputT, K, V, OutputT, StateT, ?> params;

    WindowOfBuilder(BuilderParams<InputT, K, V, OutputT, StateT, ?> params) {
      this.params = params;
    }

    @Override
    public <W extends BoundedWindow> TriggerByBuilder
        <InputT, K, V, OutputT, StateT, W> windowBy(WindowFn<Object, W> windowing) {

      @SuppressWarnings("unchecked") final BuilderParams<InputT, K, V, OutputT, StateT, W>
          paramsCasted = (BuilderParams<InputT, K, V, OutputT, StateT, W>) params;

      paramsCasted.windowFn = Objects.requireNonNull(windowing);

      return new TriggerByBuilder<>(paramsCasted);
    }

    @Override
    public <W extends Window<W>> OutputBuilder<InputT, K, V, OutputT, StateT, ?> windowBy(
        Windowing<?, W> windowing) {
      params.euphoriaWindowing = Objects.requireNonNull(windowing);
      return new OutputBuilder<>(params);
    }

    @Override
    public Dataset<Pair<K, OutputT>> output(OutputHint... outputHints) {
      return new OutputBuilder<>(params).output(outputHints);
    }

    @Override
    public OutputBuilder<InputT, K, V, OutputT, StateT, ?> applyIf(boolean cond,
        UnaryFunction<WindowOfBuilder<InputT, K, V, OutputT, StateT>,
            OutputBuilder<InputT, K, V, OutputT, StateT, ?>> applyWhenConditionHolds) {
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
  public static class TriggerByBuilder
      <InputT, K, V, OutputT, StateT extends State<V, OutputT>, W extends BoundedWindow>
      implements Builders.TriggeredBy<AccumulatorModeBuilder<InputT, K, V, OutputT, StateT, W>> {

    private final BuilderParams<InputT, K, V, OutputT, StateT, W> params;

    TriggerByBuilder(BuilderParams<InputT, K, V, OutputT, StateT, W> params) {
      this.params = params;
    }

    public AccumulatorModeBuilder<InputT, K, V, OutputT, StateT, W> triggeredBy(Trigger trigger) {
      params.trigger = Objects.requireNonNull(trigger);
      return new AccumulatorModeBuilder<>(params);
    }

  }

  /**
   * {@link WindowingStrategy.AccumulationMode} defining operator builder.
   */
  public static class AccumulatorModeBuilder
      <InputT, K, V, OutputT, StateT extends State<V, OutputT>, W extends BoundedWindow>
      implements Builders.AccumulatorMode<OutputBuilder<InputT, K, V, OutputT, StateT, W>> {

    private final BuilderParams<InputT, K, V, OutputT, StateT, W> params;

    AccumulatorModeBuilder(BuilderParams<InputT, K, V, OutputT, StateT, W> params) {
      this.params = params;
    }

    public OutputBuilder<InputT, K, V, OutputT, StateT, W> accumulationMode(
        WindowingStrategy.AccumulationMode accumulationMode) {

      params.accumulationMode = Objects.requireNonNull(accumulationMode);
      return new OutputBuilder<>(params);
    }

  }

  /**
   * Last builder in a chain. It concludes this operators creation by calling {@link
   * #output(OutputHint...)}.
   */
  public static class OutputBuilder<
      InputT, K, V, OutputT, StateT extends State<V, OutputT>, W extends BoundedWindow>
      implements Builders.Output<Pair<K, OutputT>>, Builders.OutputValues<K, OutputT> {

    /**
     * TODO: complete javadoc.
     */
    private final BuilderParams<InputT, K, V, OutputT, StateT, W> params;

    OutputBuilder(BuilderParams<InputT, K, V, OutputT, StateT, W> params) {
      this.params = params;
    }

    @Override
    public Dataset<Pair<K, OutputT>> output(OutputHint... outputHints) {
      Flow flow = params.input.getFlow();

      ReduceStateByKey<InputT, K, V, OutputT, StateT, W> reduceStateByKey =
          new ReduceStateByKey<>(
              params.name,
              flow,
              params.input,
              params.keyExtractor,
              params.valueExtractor,
              params.getWindowing(),
              params.euphoriaWindowing,
              params.stateFactory,
              params.stateMerger,
              Sets.newHashSet(outputHints));
      flow.add(reduceStateByKey);

      return reduceStateByKey.output();
    }
  }
}
