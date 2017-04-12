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
 * Operator reducing state by given reduce function.
 *
 * @param <IN>     Type of input records
 * @param <KIN>    Type of records entering #keyBy and #valueBy methods
 * @param <WIN>    Type of input records being windowed
 * @param <KEY>    Output type of #keyBy method
 * @param <VALUE>  Output type of #valueBy method
 * @param <KEYOUT> Type of output key
 * @param <OUT>    Type of output value
 */
@Basic(
    state = StateComplexity.CONSTANT_IF_COMBINABLE,
    repartitions = 1
)
public class ReduceStateByKey<
    IN, KIN, WIN, KEY, VALUE, KEYOUT, OUT, STATE extends State<VALUE, OUT>,
    W extends Window>
    extends StateAwareWindowWiseSingleInputOperator<IN, WIN, KIN, KEY,
        Pair<KEYOUT, OUT>, W,
        ReduceStateByKey<IN, KIN, WIN, KEY, VALUE, KEYOUT, OUT, STATE, W>>
{

  public static class OfBuilder {
    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    public <IN> DatasetBuilder1<IN> of(Dataset<IN> input) {
      return new DatasetBuilder1<>(name, input);
    }
  }

  // builder classes used when input is Dataset<IN> ----------------------

  public static class DatasetBuilder1<IN> {
    private final String name;
    private final Dataset<IN> input;
    DatasetBuilder1(String name, Dataset<IN> input) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
    }
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

    public DatasetBuilder5<IN, KEY, VALUE, OUT, STATE>
    mergeStatesBy(StateMerger<VALUE, OUT, STATE> stateMerger) {
      return new DatasetBuilder5<>(name, input, keyExtractor, valueExtractor,
              stateFactory, stateMerger);
    }
  }

  public static class DatasetBuilder5<
      IN, KEY, VALUE, OUT, STATE extends State<VALUE, OUT>>
          extends PartitioningBuilder<KEY,  DatasetBuilder5<IN, KEY, VALUE, OUT, STATE>>
      implements OutputBuilder<Pair<KEY, OUT>>
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

    public <WIN, W extends Window>
    DatasetBuilder6<IN, WIN, KEY, VALUE, OUT, STATE, W>
    windowBy(Windowing<WIN, W> windowing)
    {
      return windowBy(windowing, null);
    }

    public <WIN, W extends Window>
    DatasetBuilder6<IN, WIN, KEY, VALUE, OUT, STATE, W>
    windowBy(Windowing<WIN, W> windowing, ExtractEventTime<WIN> eventTimeAssigner) {
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
          IN, WIN, KEY, VALUE, OUT, STATE extends State<VALUE, OUT>,
          W extends Window>
      extends PartitioningBuilder<
          KEY,DatasetBuilder6<IN, WIN, KEY, VALUE, OUT, STATE, W>>
      implements OutputBuilder<Pair<KEY, OUT>> {

    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, KEY> keyExtractor;
    private final UnaryFunction<IN, VALUE> valueExtractor;
    private final StateFactory<VALUE, OUT, STATE> stateFactory;
    private final StateMerger<VALUE, OUT, STATE> stateMerger;
    @Nullable
    private final Windowing<WIN, W> windowing;
    @Nullable
    private final ExtractEventTime<WIN> eventTimeAssigner;

    DatasetBuilder6(String name,
                    Dataset<IN> input,
                    UnaryFunction<IN, KEY> keyExtractor,
                    UnaryFunction<IN, VALUE> valueExtractor,
                    StateFactory<VALUE, OUT, STATE> stateFactory,
                    StateMerger<VALUE, OUT, STATE> stateMerger,
                    @Nullable Windowing<WIN, W> windowing,
                    @Nullable ExtractEventTime<WIN> eventTimeAssigner,
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

      ReduceStateByKey<IN, IN, WIN, KEY, VALUE, KEY, OUT, STATE, W>
          reduceStateByKey =
          new ReduceStateByKey<>(name, flow, input, keyExtractor, valueExtractor,
              windowing, eventTimeAssigner, stateFactory, stateMerger, getPartitioning());
      flow.add(reduceStateByKey);

      return reduceStateByKey.output();
    }
  }

  // -------------

  public static <IN> DatasetBuilder1<IN> of(Dataset<IN> input) {
    return new DatasetBuilder1<>("ReduceStateByKey", input);
  }

  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  private final StateFactory<VALUE, OUT, STATE> stateFactory;
  private final UnaryFunction<KIN, VALUE> valueExtractor;
  private final StateMerger<VALUE, OUT, STATE> stateCombiner;

  ReduceStateByKey(String name,
                   Flow flow,
                   Dataset<IN> input,
                   UnaryFunction<KIN, KEY> keyExtractor,
                   UnaryFunction<KIN, VALUE> valueExtractor,
                   @Nullable Windowing<WIN, W> windowing,
                   @Nullable ExtractEventTime<WIN> eventTimeAssigner,
                   StateFactory<VALUE, OUT, STATE> stateFactory,
                   StateMerger<VALUE, OUT, STATE> stateMerger,
                   Partitioning<KEY> partitioning)
  {
    super(name, flow, input, keyExtractor, windowing, eventTimeAssigner, partitioning);
    this.stateFactory = stateFactory;
    this.valueExtractor = valueExtractor;
    this.stateCombiner = stateMerger;
  }

  public StateFactory<VALUE, OUT, STATE> getStateFactory() {
    return stateFactory;
  }

  public UnaryFunction<KIN, VALUE> getValueExtractor() {
    return valueExtractor;
  }

  public StateMerger<VALUE, OUT, STATE> getStateMerger() {
    return stateCombiner;
  }
}
