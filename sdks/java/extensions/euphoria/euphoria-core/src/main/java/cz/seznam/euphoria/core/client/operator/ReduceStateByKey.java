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

import com.google.common.collect.Sets;
import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.annotation.operator.Basic;
import cz.seznam.euphoria.core.annotation.operator.StateComplexity;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.hint.OutputHint;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StateFactory;
import cz.seznam.euphoria.core.client.operator.state.StateMerger;
import cz.seznam.euphoria.core.client.type.TypeAwareUnaryFunction;
import cz.seznam.euphoria.core.client.type.TypeHint;
import cz.seznam.euphoria.core.client.util.Pair;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

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
 * <p>This example constitutes a windowed word-count program. Each input element is treated as a key
 * to identify an imaginary {@code WordCountState} within each time window assigned to the input
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
 *   <li>{@code [named] ..................} give name to the operator [optional]
 *   <li>{@code of .......................} input dataset
 *   <li>{@code keyBy ....................} key extractor function
 *   <li>{@code valueBy ..................} value extractor function
 *   <li>{@code stateFactory .............} factory method for {@link State} (see {@link
 *       StateFactory})
 *   <li>{@code mergeStatesBy ............} state merge function (see {@link StateMerger})
 *   <li>{@code [windowBy] ...............} windowing function (see {@link Windowing}), default
 *       attached windowing
 *   <li>{@code (output | outputValues) ..} build output dataset
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
        InputT, K, V, OutputT, StateT extends State<V, OutputT>, W extends Window<W>>
    extends StateAwareWindowWiseSingleInputOperator<
        InputT, InputT, InputT, K, Pair<K, OutputT>, W,
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
      @Nullable Windowing<InputT, W> windowing,
      StateFactory<V, OutputT, StateT> stateFactory,
      StateMerger<V, OutputT, StateT> stateMerger,
      Set<OutputHint> outputHints) {
    super(name, flow, input, keyExtractor, windowing, outputHints);
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
    return new KeyByBuilder<>("ReduceStateByKey", input);
  }

  /**
   * Starts building a named {@link ReduceStateByKey} operator.
   *
   * @param name a user provided name of the new operator to build
   * @return a builder to complete the setup of the new operator
   */
  public static OfBuilder named(String name) {
    return new OfBuilder(name);
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

  /** TODO: complete javadoc. */
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

  /** TODO: complete javadoc. */
  public static class KeyByBuilder<InputT> implements Builders.KeyBy<InputT> {

    private final String name;
    private final Dataset<InputT> input;

    KeyByBuilder(String name, Dataset<InputT> input) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
    }

    @Override
    public <K> DatasetBuilder2<InputT, K> keyBy(UnaryFunction<InputT, K> keyExtractor) {
      return new DatasetBuilder2<>(name, input, keyExtractor);
    }

    public <K> DatasetBuilder2<InputT, K> keyBy(
        UnaryFunction<InputT, K> keyExtractor, TypeHint<K> typeHint) {
      return new DatasetBuilder2<>(name, input, TypeAwareUnaryFunction.of(keyExtractor, typeHint));
    }
  }

  /** TODO: complete javadoc. */
  public static class DatasetBuilder2<InputT, K> {
    private final String name;
    private final Dataset<InputT> input;
    private final UnaryFunction<InputT, K> keyExtractor;

    DatasetBuilder2(String name, Dataset<InputT> input, UnaryFunction<InputT, K> keyExtractor) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
    }

    /**
     * Specifies the function to derive a value from the {@link ReduceStateByKey} operator's input
     * elements to get accumulated by a later supplied state implementation.
     *
     * @param <V> the type of the extracted values
     * @param valueExtractor a user defined function to extract values from the processed input
     *     dataset's elements for later accumulation
     * @return the next builder to complete the setup of the {@link ReduceStateByKey} operator
     */
    public <V> DatasetBuilder3<InputT, K, V> valueBy(UnaryFunction<InputT, V> valueExtractor) {
      return new DatasetBuilder3<>(name, input, keyExtractor, valueExtractor);
    }
  }

  /** TODO: complete javadoc. */
  public static class DatasetBuilder3<InputT, K, V> {
    private final String name;
    private final Dataset<InputT> input;
    private final UnaryFunction<InputT, K> keyExtractor;
    private final UnaryFunction<InputT, V> valueExtractor;

    DatasetBuilder3(
        String name,
        Dataset<InputT> input,
        UnaryFunction<InputT, K> keyExtractor,
        UnaryFunction<InputT, V> valueExtractor) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
      this.valueExtractor = Objects.requireNonNull(valueExtractor);
    }

    /**
     * Specifies a factory for creating new/empty/blank state instances.
     *
     * @param <OutputT> the type of output elements state instances will produce; along with the
     *     "key", this is part of the type of output elements the {@link ReduceStateByKey} operator
     *     will produce as such
     * @param <StateT> the type of the state (implementation)
     * @param stateFactory a user supplied function to create new state instances
     * @return the next builder to complete the setup of the {@link ReduceStateByKey} operator
     */
    public <OutputT, StateT extends State<V, OutputT>>
        DatasetBuilder4<InputT, K, V, OutputT, StateT> stateFactory(
            StateFactory<V, OutputT, StateT> stateFactory) {
      return new DatasetBuilder4<>(name, input, keyExtractor, valueExtractor, stateFactory);
    }
  }

  /** TODO: complete javadoc. */
  public static class DatasetBuilder4<InputT, K, V, OutputT, StateT extends State<V, OutputT>> {
    private final String name;
    private final Dataset<InputT> input;
    private final UnaryFunction<InputT, K> keyExtractor;
    private final UnaryFunction<InputT, V> valueExtractor;
    private final StateFactory<V, OutputT, StateT> stateFactory;

    DatasetBuilder4(
        String name,
        Dataset<InputT> input,
        UnaryFunction<InputT, K> keyExtractor,
        UnaryFunction<InputT, V> valueExtractor,
        StateFactory<V, OutputT, StateT> stateFactory) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
      this.valueExtractor = Objects.requireNonNull(valueExtractor);
      this.stateFactory = Objects.requireNonNull(stateFactory);
    }

    /**
     * Specifies the merging function to be utilized when states get merged as part of {@link
     * cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing window merging}.
     *
     * @param stateMerger a user defined function to merge mutilple states into a specified target
     *     state
     * @return the next builder to complete the setup of the {@link ReduceStateByKey} operator
     */
    public DatasetBuilder5<InputT, K, V, OutputT, StateT> mergeStatesBy(
        StateMerger<V, OutputT, StateT> stateMerger) {
      return new DatasetBuilder5<>(
          name, input, keyExtractor, valueExtractor, stateFactory, stateMerger);
    }
  }

  /** TODO: complete javadoc. */
  public static class DatasetBuilder5<InputT, K, V, OutputT, StateT extends State<V, OutputT>>
      implements Builders.WindowBy<InputT, DatasetBuilder5<InputT, K, V, OutputT, StateT>>,
          Builders.Output<Pair<K, OutputT>>,
          Builders.OutputValues<K, OutputT> {

    final String name;
    final Dataset<InputT> input;
    final UnaryFunction<InputT, K> keyExtractor;
    final UnaryFunction<InputT, V> valueExtractor;
    final StateFactory<V, OutputT, StateT> stateFactory;
    final StateMerger<V, OutputT, StateT> stateMerger;

    DatasetBuilder5(
        String name,
        Dataset<InputT> input,
        UnaryFunction<InputT, K> keyExtractor,
        UnaryFunction<InputT, V> valueExtractor,
        StateFactory<V, OutputT, StateT> stateFactory,
        StateMerger<V, OutputT, StateT> stateMerger) {

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
      this.valueExtractor = Objects.requireNonNull(valueExtractor);
      this.stateFactory = Objects.requireNonNull(stateFactory);
      this.stateMerger = Objects.requireNonNull(stateMerger);
    }

    @Override
    public <W extends Window<W>> DatasetBuilder6<InputT, K, V, OutputT, StateT, W> windowBy(
        Windowing<InputT, W> windowing) {
      return new DatasetBuilder6<>(
          name,
          input,
          keyExtractor,
          valueExtractor,
          stateFactory,
          stateMerger,
          Objects.requireNonNull(windowing));
    }

    @Override
    public Dataset<Pair<K, OutputT>> output(OutputHint... outputHints) {
      return new DatasetBuilder6<>(
              name, input, keyExtractor, valueExtractor, stateFactory, stateMerger, null)
          .output(outputHints);
    }
  }

  /** TODO: complete javadoc. */
  public static class DatasetBuilder6<
          InputT, K, V, OutputT, StateT extends State<V, OutputT>, W extends Window<W>>
      extends DatasetBuilder5<InputT, K, V, OutputT, StateT> {

    @Nullable private final Windowing<InputT, W> windowing;

    DatasetBuilder6(
        String name,
        Dataset<InputT> input,
        UnaryFunction<InputT, K> keyExtractor,
        UnaryFunction<InputT, V> valueExtractor,
        StateFactory<V, OutputT, StateT> stateFactory,
        StateMerger<V, OutputT, StateT> stateMerger,
        @Nullable Windowing<InputT, W> windowing) {
      super(name, input, keyExtractor, valueExtractor, stateFactory, stateMerger);
      this.windowing = windowing;
    }

    @Override
    public Dataset<Pair<K, OutputT>> output(OutputHint... outputHints) {
      Flow flow = input.getFlow();

      ReduceStateByKey<InputT, K, V, OutputT, StateT, W> reduceStateByKey =
          new ReduceStateByKey<>(
              name,
              flow,
              input,
              keyExtractor,
              valueExtractor,
              windowing,
              stateFactory,
              stateMerger,
              Sets.newHashSet(outputHints));
      flow.add(reduceStateByKey);

      return reduceStateByKey.output();
    }
  }
}
