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
import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StateFactory;
import cz.seznam.euphoria.core.client.operator.state.StateMerger;
import cz.seznam.euphoria.core.client.util.Pair;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A {@link ReduceStateByKey} operator is a stateful, complex, lower-level-api,
 * but very powerful processor and serves as the basis for easier-to-use
 * operators. Client API users have generally little motivation to use it
 * directly and should prefer {@link ReduceByKey} when possible.<p>
 *
 * The operator assigns each input item to a set of windows (through a user
 * provided {@link Windowing} implementation) and turns the item into a
 * key/value pair. For each of the assigned windows the extracted value is
 * accumulated using a user provided {@link StateFactory state implementation}
 * under the extracted key. I.e. the value is accumulated into a state
 * identified by a key/window pair.<p>
 *
 * Example:
 *
 * <pre>{@code
 *  Dataset<String> words = ...;
 *  Dataset<Pair<String, Integer>> counts =
 *      ReduceStateByKey.named("WORD-COUNT")
 *          .of(words)
 *          .keyBy(s -> s)
 *          .valueBy(s -> 1)
 *          .stateFactory(WordCountState::new)
 *          .mergeStatesBy(WordCountState::merge)
 *          .windowBy(Time.of(Duration.ofHours(1))
 *          .output();
 * }</pre>
 *
 * This example constitutes a windowed word-count program. Each input element
 * is treated as a key to identify an imaginary {@code WordCountState} within
 * each time window assigned to the input element. For such a key/window
 * combination the value {@code 1} gets sent to the corresponding state.<p>
 *
 * States are emitted based on the used {@link Windowing} strategy. See that
 * for more information. Generally speaking, when a window is emitted/triggered,
 * the states of all keys within the emitted window are flushed.
 *
 * Note that defining a {@link Windowing} strategy is actually optional. If
 * none is defined the operator will act in the so-call "attached windowing"
 * mode, i.e. it will attach itself to the windowing strategy active on they
 * way from its input.
 *
 * @param <IN>     the type of input elements
 * @param <KEY>    the type of the key (result type of {@code #keyBy}
 * @param <VALUE>  the type of the accumulated values (result type of {@code #valueBy})
 * @param <OUT>    the type of the output elements (result type of the accumulated state)
 */
@Basic(
    state = StateComplexity.CONSTANT_IF_COMBINABLE,
    repartitions = 1
)
public class ReduceStateByKey<
    IN, KEY, VALUE, OUT, STATE extends State<VALUE, OUT>, W extends Window>
    extends StateAwareWindowWiseSingleInputOperator<
        IN, IN, IN, KEY, Pair<KEY, OUT>, W, ReduceStateByKey<IN, KEY, VALUE, OUT, STATE, W>>
{

  public static class OfBuilder implements Builders.Of {
    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    @Override
    public <IN> KeyByBuilder<IN> of(Dataset<IN> input) {
      return new KeyByBuilder<>(name, input);
    }
  }

  // builder classes used when input is Dataset<IN> ----------------------

  public static class KeyByBuilder<IN> implements Builders.KeyBy<IN> {
    private final String name;
    private final Dataset<IN> input;

    KeyByBuilder(String name, Dataset<IN> input) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
    }

    @Override
    public <KEY> DatasetBuilder2<IN, KEY> keyBy(UnaryFunction<IN, KEY> keyExtractor) {
      return new DatasetBuilder2<>(name, input, keyExtractor);
    }
  }

  public static class DatasetBuilder2<IN, KEY> {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, KEY> keyExtractor;

    DatasetBuilder2(String name, Dataset<IN> input, UnaryFunction<IN, KEY> keyExtractor) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
    }

    /**
     * Specifies the function to derive a value from the
     * {@link ReduceStateByKey} operator's input elements to get
     * accumulated by a later supplied state implementation.
     *
     * @param <VALUE> the type of the extracted values
     *
     * @param valueExtractor a user defined function to extract values from the
     *                        processed input dataset's elements for later
     *                        accumulation
     *
     * @return the next builder to complete the setup of the
     *          {@link ReduceStateByKey} operator
     */
    public <VALUE> DatasetBuilder3<IN, KEY, VALUE> valueBy(UnaryFunction<IN, VALUE> valueExtractor) {
      return new DatasetBuilder3<>(name, input, keyExtractor, valueExtractor);
    }
  }

  public static class DatasetBuilder3<IN, KEY, VALUE> {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, KEY> keyExtractor;
    private final UnaryFunction<IN, VALUE> valueExtractor;

    DatasetBuilder3(String name,
                    Dataset<IN> input,
                    UnaryFunction<IN, KEY> keyExtractor,
                    UnaryFunction<IN, VALUE> valueExtractor)
    {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
      this.valueExtractor = Objects.requireNonNull(valueExtractor);
    }

    /**
     * Specifies a factory for creating new/empty/blank state instances.
     *
     * @param <OUT> the type of output elements state instances will produce;
     *               along with the "key", this is part of the type of output
     *               elements the {@link ReduceStateByKey} operator will produce
     *               as such
     * @param <STATE> the type of the state (implementation)
     *
     * @param stateFactory a user supplied function to create new state instances
     *
     * @return the next builder to complete the setup of the
     *          {@link ReduceStateByKey} operator
     */
    public <OUT, STATE extends State<VALUE, OUT>> DatasetBuilder4<
            IN, KEY, VALUE, OUT, STATE> stateFactory(
            StateFactory<VALUE, OUT, STATE> stateFactory) {
      return new DatasetBuilder4<>(
          name, input, keyExtractor, valueExtractor, stateFactory);
    }
  }

  public static class DatasetBuilder4<
      IN, KEY, VALUE, OUT, STATE extends State<VALUE, OUT>> {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, KEY> keyExtractor;
    private final UnaryFunction<IN, VALUE> valueExtractor;
    private final StateFactory<VALUE, OUT, STATE> stateFactory;

    DatasetBuilder4(String name,
                    Dataset<IN> input,
                    UnaryFunction<IN, KEY> keyExtractor,
                    UnaryFunction<IN, VALUE> valueExtractor,
                    StateFactory<VALUE, OUT, STATE> stateFactory)
    {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
      this.valueExtractor = Objects.requireNonNull(valueExtractor);
      this.stateFactory = Objects.requireNonNull(stateFactory);
    }

    /**
     * Specifies the merging function to be utilized when states get merged
     * as part of {@link cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing window merging}.
     *
     * @param stateMerger a user defined function to merge mutilple states into
     *                   a specified target state
     *
     * @return the next builder to complete the setup of the
     *          {@link ReduceStateByKey} operator
     */
    public DatasetBuilder5<IN, KEY, VALUE, OUT, STATE>
    mergeStatesBy(StateMerger<VALUE, OUT, STATE> stateMerger) {
      return new DatasetBuilder5<>(name, input, keyExtractor, valueExtractor,
              stateFactory, stateMerger);
    }
  }

  public static class DatasetBuilder5<
      IN, KEY, VALUE, OUT, STATE extends State<VALUE, OUT>>
          extends PartitioningBuilder<KEY,  DatasetBuilder5<IN, KEY, VALUE, OUT, STATE>>
      implements Builders.WindowBy<IN>, Builders.Output<Pair<KEY, OUT>>
  {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, KEY> keyExtractor;
    private final UnaryFunction<IN, VALUE> valueExtractor;
    private final StateFactory<VALUE, OUT, STATE> stateFactory;
    private final StateMerger<VALUE, OUT, STATE> stateMerger;

    DatasetBuilder5(String name,
                    Dataset<IN> input,
                    UnaryFunction<IN, KEY> keyExtractor,
                    UnaryFunction<IN, VALUE> valueExtractor,
                    StateFactory<VALUE, OUT, STATE> stateFactory,
                    StateMerger<VALUE, OUT, STATE> stateMerger) {
      // initialize default partitioning according to input
      super(new DefaultPartitioning<>(input.getNumPartitions()));

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
      this.valueExtractor = Objects.requireNonNull(valueExtractor);
      this.stateFactory = Objects.requireNonNull(stateFactory);
      this.stateMerger = Objects.requireNonNull(stateMerger);
    }

    @Override
    public <W extends Window>
    DatasetBuilder6<IN, KEY, VALUE, OUT, STATE, W>
    windowBy(Windowing<IN, W> windowing)
    {
      return windowBy(windowing, null);
    }

    @Override
    public <W extends Window>
    DatasetBuilder6<IN, KEY, VALUE, OUT, STATE, W>
    windowBy(Windowing<IN, W> windowing, ExtractEventTime<IN> eventTimeAssigner) {
      return new DatasetBuilder6<>(name, input, keyExtractor, valueExtractor,
              stateFactory, stateMerger,
              Objects.requireNonNull(windowing), eventTimeAssigner, this);
    }

    @Override
    public Dataset<Pair<KEY, OUT>> output() {
      return new DatasetBuilder6<>(name, input, keyExtractor, valueExtractor,
          stateFactory, stateMerger, null, null, this)
          .output();
    }
  }

  public static class DatasetBuilder6<
          IN, KEY, VALUE, OUT, STATE extends State<VALUE, OUT>,
          W extends Window>
      extends PartitioningBuilder<
          KEY,DatasetBuilder6<IN, KEY, VALUE, OUT, STATE, W>>
      implements Builders.Output<Pair<KEY, OUT>> {

    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, KEY> keyExtractor;
    private final UnaryFunction<IN, VALUE> valueExtractor;
    private final StateFactory<VALUE, OUT, STATE> stateFactory;
    private final StateMerger<VALUE, OUT, STATE> stateMerger;
    @Nullable
    private final Windowing<IN, W> windowing;
    @Nullable
    private final ExtractEventTime<IN> eventTimeAssigner;

    DatasetBuilder6(String name,
                    Dataset<IN> input,
                    UnaryFunction<IN, KEY> keyExtractor,
                    UnaryFunction<IN, VALUE> valueExtractor,
                    StateFactory<VALUE, OUT, STATE> stateFactory,
                    StateMerger<VALUE, OUT, STATE> stateMerger,
                    @Nullable Windowing<IN, W> windowing,
                    @Nullable ExtractEventTime<IN> eventTimeAssigner,
                    PartitioningBuilder<KEY, ?> partitioning)
    {
      // initialize partitioning
      super(partitioning);

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
      this.valueExtractor = Objects.requireNonNull(valueExtractor);
      this.stateFactory = Objects.requireNonNull(stateFactory);
      this.stateMerger = Objects.requireNonNull(stateMerger);
      this.windowing = windowing;
      this.eventTimeAssigner = eventTimeAssigner;
    }

    @Override
    public Dataset<Pair<KEY, OUT>> output() {
      Flow flow = input.getFlow();

      ReduceStateByKey<IN, KEY, VALUE, OUT, STATE, W>
          reduceStateByKey =
          new ReduceStateByKey<>(name, flow, input, keyExtractor, valueExtractor,
              windowing, eventTimeAssigner, stateFactory, stateMerger, getPartitioning());
      flow.add(reduceStateByKey);

      return reduceStateByKey.output();
    }
  }

  /**
   * Starts building a nameless {@link ReduceStateByKey} operator to process
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
  public static <IN> KeyByBuilder<IN> of(Dataset<IN> input) {
    return new KeyByBuilder<>("ReduceStateByKey", input);
  }

  /**
   * Starts building a named {@link ReduceStateByKey} operator.
   *
   * @param name a user provided name of the new operator to build
   *
   * @return a builder to complete the setup of the new operator
   */
  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  private final StateFactory<VALUE, OUT, STATE> stateFactory;
  private final UnaryFunction<IN, VALUE> valueExtractor;
  private final StateMerger<VALUE, OUT, STATE> stateCombiner;

  ReduceStateByKey(String name,
                   Flow flow,
                   Dataset<IN> input,
                   UnaryFunction<IN, KEY> keyExtractor,
                   UnaryFunction<IN, VALUE> valueExtractor,
                   @Nullable Windowing<IN, W> windowing,
                   @Nullable ExtractEventTime<IN> eventTimeAssigner,
                   StateFactory<VALUE, OUT, STATE> stateFactory,
                   StateMerger<VALUE, OUT, STATE> stateMerger,
                   Partitioning<KEY> partitioning)
  {
    super(name, flow, input, keyExtractor, windowing, eventTimeAssigner, partitioning);
    this.stateFactory = stateFactory;
    this.valueExtractor = valueExtractor;
    this.stateCombiner = stateMerger;
  }

  /**
   * Retrieves the user defined state factory function.
   *
   * @return the user provided state factory
   */
  public StateFactory<VALUE, OUT, STATE> getStateFactory() {
    return stateFactory;
  }

  /**
   * Retrieves the user defined state merger function.
   *
   * @return the user provided state merger
   */
  public StateMerger<VALUE, OUT, STATE> getStateMerger() {
    return stateCombiner;
  }

  /**
   * Retrieves the user defined key extractor function.
   *
   * @return the user provided key extractor function
   */
  public UnaryFunction<IN, VALUE> getValueExtractor() {
    return valueExtractor;
  }
}
