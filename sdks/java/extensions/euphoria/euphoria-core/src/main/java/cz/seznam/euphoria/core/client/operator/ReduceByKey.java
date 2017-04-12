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

import cz.seznam.euphoria.core.annotation.operator.Recommended;
import cz.seznam.euphoria.core.annotation.operator.StateComplexity;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.ReduceFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StateFactory;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.core.client.util.Pair;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Objects;

/**
 * Operator performing state-less aggregation by given reduce function.
 *
 * @param <IN> Type of input records
 * @param <KIN> Type of records entering #keyBy and #valueBy methods
 * @param <KEY> Output type of #keyBy method
 * @param <VALUE> Output type of #valueBy method
 * @param <KEYOUT> Type of output key
 * @param <OUT> Type of output value
 */
@Recommended(
    reason =
        "Is very recommended to override because of performance in "
      + "a specific area of (mostly) batch calculations where combiners "
      + "can be efficiently used in the executor-specific implementation",
    state = StateComplexity.CONSTANT_IF_COMBINABLE,
    repartitions = 1
)
public class ReduceByKey<
    IN, KIN, KEY, VALUE, KEYOUT, OUT, W extends Window>
    extends StateAwareWindowWiseSingleInputOperator<
        IN, IN, KIN, KEY, Pair<KEYOUT, OUT>, W,
        ReduceByKey<IN, KIN, KEY, VALUE, KEYOUT, OUT, W>> {

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
    public <OUT> DatasetBuilder4<IN, KEY, IN, OUT> reduceBy(ReduceFunction<IN, OUT> reducer) {
      return new DatasetBuilder4<>(name, input, keyExtractor, e-> e, reducer);
    }
    @SuppressWarnings("unchecked")
    public DatasetBuilder4<IN, KEY, IN, IN> combineBy(CombinableReduceFunction<IN> reducer) {
      return new DatasetBuilder4(name, input, keyExtractor, e -> e, reducer);
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
                    UnaryFunction<IN, VALUE> valueExtractor) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
      this.valueExtractor = Objects.requireNonNull(valueExtractor);
    }
    public <OUT> DatasetBuilder4<IN, KEY, VALUE, OUT> reduceBy(
        ReduceFunction<VALUE, OUT> reducer) {
      return new DatasetBuilder4<>(name, input, keyExtractor, valueExtractor, reducer);
    }
    public DatasetBuilder4<IN, KEY, VALUE, VALUE> combineBy(
        CombinableReduceFunction<VALUE> reducer) {
      return new DatasetBuilder4<>(name, input, keyExtractor, valueExtractor, reducer);
    }
  }

  public static class DatasetBuilder4<IN, KEY, VALUE, OUT>
          extends PartitioningBuilder<KEY, DatasetBuilder4<IN, KEY, VALUE, OUT>>
          implements OutputBuilder<Pair<KEY, OUT>> {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, KEY> keyExtractor;
    private final UnaryFunction<IN, VALUE> valueExtractor;
    private final ReduceFunction<VALUE, OUT> reducer;
    DatasetBuilder4(String name,
                    Dataset<IN> input,
                    UnaryFunction<IN, KEY> keyExtractor,
                    UnaryFunction<IN, VALUE> valueExtractor,
                    ReduceFunction<VALUE, OUT> reducer) {

      // initialize default partitioning according to input
      super(new DefaultPartitioning<>(input.getNumPartitions()));

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
      this.valueExtractor = Objects.requireNonNull(valueExtractor);
      this.reducer = Objects.requireNonNull(reducer);
    }
    public  <W extends Window>
    DatasetBuilder5<IN, KEY, VALUE, OUT, W>
    windowBy(Windowing<IN, W> windowing) {
      return windowBy(windowing, null);
    }
    public  <W extends Window>
    DatasetBuilder5<IN, KEY, VALUE, OUT, W>
    windowBy(Windowing<IN, W> windowing, ExtractEventTime<IN> eventTimeAssigner) {
      return new DatasetBuilder5<>(name, input, keyExtractor, valueExtractor,
              reducer, Objects.requireNonNull(windowing), eventTimeAssigner, this);
    }

    @Override
    public Dataset<Pair<KEY, OUT>> output() {
      return new DatasetBuilder5<>(name, input, keyExtractor, valueExtractor,
              reducer, null, null, this)
          .output();
    }
  }

  public static class DatasetBuilder5<
          IN, KEY, VALUE, OUT, W extends Window>
      extends PartitioningBuilder<KEY, DatasetBuilder5<IN, KEY, VALUE, OUT, W>>
      implements OutputBuilder<Pair<KEY, OUT>> {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, KEY> keyExtractor;
    private final UnaryFunction<IN, VALUE> valueExtractor;
    private final ReduceFunction<VALUE, OUT> reducer;
    @Nullable
    private final Windowing<IN, W> windowing;
    @Nullable
    private final ExtractEventTime<IN> eventTimeAssigner;

    DatasetBuilder5(String name,
                    Dataset<IN> input,
                    UnaryFunction<IN, KEY> keyExtractor,
                    UnaryFunction<IN, VALUE> valueExtractor,
                    ReduceFunction<VALUE, OUT> reducer,
                    @Nullable Windowing<IN, W> windowing,
                    @Nullable ExtractEventTime<IN> eventTimeAssigner,
                    PartitioningBuilder<KEY, ?> partitioning) {

      // initialize default partitioning according to input
      super(partitioning);

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
      this.valueExtractor = Objects.requireNonNull(valueExtractor);
      this.reducer = Objects.requireNonNull(reducer);
      this.windowing = windowing;
      this.eventTimeAssigner = eventTimeAssigner;
    }

    @Override
    public Dataset<Pair<KEY, OUT>> output() {
      Flow flow = input.getFlow();
      ReduceByKey<IN, IN, KEY, VALUE, KEY, OUT, W>
          reduce =
          new ReduceByKey<>(name, flow, input, keyExtractor, valueExtractor,
              windowing, eventTimeAssigner, reducer, getPartitioning());
      flow.add(reduce);
      return reduce.output();
    }
  }

  public static <IN> DatasetBuilder1<IN> of(Dataset<IN> input) {
    return new DatasetBuilder1<>("ReduceByKey", input);
  }

  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  final ReduceFunction<VALUE, OUT> reducer;
  final UnaryFunction<KIN, VALUE> valueExtractor;

  ReduceByKey(String name,
              Flow flow,
              Dataset<IN> input,
              UnaryFunction<KIN, KEY> keyExtractor,
              UnaryFunction<KIN, VALUE> valueExtractor,
              @Nullable Windowing<IN, W> windowing,
              @Nullable ExtractEventTime<IN> eventTimeAssigner,
              ReduceFunction<VALUE, OUT> reducer,
              Partitioning<KEY> partitioning) {
    super(name, flow, input, keyExtractor, windowing, eventTimeAssigner, partitioning);
    this.reducer = reducer;
    this.valueExtractor = valueExtractor;
  }

  public ReduceFunction<VALUE, OUT> getReducer() {
    return reducer;
  }

  public UnaryFunction<KIN, VALUE> getValueExtractor() {
    return valueExtractor;
  }

  /**
   * @return {@code TRUE} when combinable reduce function provided
   */
  public boolean isCombinable() {
    return reducer instanceof CombinableReduceFunction;
  }

  @SuppressWarnings("unchecked")
  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    StateSupport.MergeFromStateMerger stateCombine =
            new StateSupport.MergeFromStateMerger<>();
    StateFactory stateFactory = isCombinable()
            ? new CombiningReduceState.Factory<>((CombinableReduceFunction) reducer)
            : new NonCombiningReduceState.Factory<>(reducer);
    Flow flow = getFlow();
    Operator reduceState = new ReduceStateByKey(getName(),
        flow, input, keyExtractor, valueExtractor,
        windowing, eventTimeAssigner,
        stateFactory, stateCombine,
        partitioning);
    return DAG.of(reduceState);
  }


  static class CombiningReduceState<E>
          extends State<E, E>
          implements StateSupport.MergeFrom<CombiningReduceState<E>> {

    static final class Factory<E> implements StateFactory<E, E, State<E, E>> {
      private final CombinableReduceFunction<E> r;

      Factory(CombinableReduceFunction<E> r) {
        this.r = Objects.requireNonNull(r);
      }

      @Override
      public State<E, E> createState(Context<E> context, StorageProvider storageProvider) {
        return new CombiningReduceState<>(context, storageProvider, r);
      }
    }

    @SuppressWarnings("unchecked")
    private static final ValueStorageDescriptor STORAGE_DESC =
            ValueStorageDescriptor.of("rbsk-value", (Class) Object.class, null);

    private final CombinableReduceFunction<E> reducer;
    private final ValueStorage<E> storage;

    CombiningReduceState(Context<E> context,
                         StorageProvider storageProvider,
                         CombinableReduceFunction<E> reducer) {
      super(context);
      this.reducer = Objects.requireNonNull(reducer);

      @SuppressWarnings("unchecked")
      ValueStorage<E> vs = storageProvider.getValueStorage(STORAGE_DESC);
      this.storage = vs;
    }

    @Override
    public void add(E element) {
      E v = this.storage.get();
      if (v == null) {
        this.storage.set(element);
      } else {
        this.storage.set(this.reducer.apply(Arrays.asList(v, element)));
      }
    }

    @Override
    public void flush() {
      getContext().collect(this.storage.get());
    }

    @Override
    public void close() {
      this.storage.clear();
    }

    @Override
    public void mergeFrom(CombiningReduceState<E> other) {
      this.add(other.storage.get());
    }
  }

  private static class NonCombiningReduceState<IN, OUT>
          extends State<IN, OUT>
          implements StateSupport.MergeFrom<NonCombiningReduceState<IN, OUT>> {

    static final class Factory<IN, OUT>
            implements StateFactory<IN, OUT, NonCombiningReduceState<IN, OUT>> {
      private final ReduceFunction<IN, OUT> r;

      Factory(ReduceFunction<IN, OUT> r) {
        this.r = Objects.requireNonNull(r);
      }

      @Override
      public NonCombiningReduceState<IN, OUT>
      createState(Context<OUT> context, StorageProvider storageProvider) {
        return new NonCombiningReduceState<>(context, storageProvider, r);
      }
    }

    @SuppressWarnings("unchecked")
    private static final ListStorageDescriptor STORAGE_DESC =
            ListStorageDescriptor.of("values", (Class) Object.class);

    private final ReduceFunction<IN, OUT> reducer;
    private final ListStorage<IN> reducibleValues;

    NonCombiningReduceState(Context<OUT> context,
                            StorageProvider storageProvider,
                            ReduceFunction<IN, OUT> reducer) {
      super(context);
      this.reducer = Objects.requireNonNull(reducer);

      @SuppressWarnings("unchecked")
      ListStorage<IN> ls = storageProvider.getListStorage(STORAGE_DESC);
      reducibleValues = ls;
    }

    @Override
    public void add(IN element) {
      reducibleValues.add(element);
    }

    @Override
    public void flush() {
      OUT result = reducer.apply(reducibleValues.get());
      getContext().collect(result);
    }

    @Override
    public void close() {
      reducibleValues.clear();
    }

    @Override
    public void mergeFrom(NonCombiningReduceState<IN, OUT> other) {
      this.reducibleValues.addAll(other.reducibleValues.get());
    }
  }
}
