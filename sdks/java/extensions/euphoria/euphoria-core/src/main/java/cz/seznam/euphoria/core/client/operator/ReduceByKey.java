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
import cz.seznam.euphoria.core.client.functional.BinaryFunction;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.ReduceFunction;
import cz.seznam.euphoria.core.client.functional.ReduceFunctor;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.executor.graph.DAG;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.ExternalIterable;
import cz.seznam.euphoria.core.client.io.SpillTools;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StateContext;
import cz.seznam.euphoria.core.client.operator.state.StateFactory;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.util.SingleValueContext;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * <p>
 * Operator performing state-less aggregation by given reduce function. The reduction
 * is performed on all extracted values on each key-window.
 * </p>
 *
 * <p>
 * If provided function is {@link CombinableReduceFunction} partial reduction is performed
 * before shuffle. If the function is not combinable all values must be first sent through the
 * network and the reduction is done afterwards on target machines.
 * </p>
 *
 * <p>
 * Custom {@link Windowing} can be set, otherwise values from
 * input operator are used.
 * </p>
 *
 * <h3>Builders:</h3>
 * <ol>
 *   <li>{@code [named] ..................} give name to the operator [optional]
 *   <li>{@code of .......................} input dataset
 *   <li>{@code keyBy ....................} key extractor function
 *   <li>{@code [valueBy] ................} value extractor function (default: identity)
 *   <li>{@code (combineBy | reduceBy)....} {@link CombinableReduceFunction} or {@link ReduceFunction} for combinable or non-combinable function
 *   <li>{@code [withSortedValues] .......} use comparator for sorting values prior to being passed to {@link ReduceFunction} function (applicable only for non-combinable version)
 *   <li>{@code [windowBy] ...............} windowing function (see {@link Windowing}), default attached windowing
 *   <li>{@code (output | outputValues) ..} build output dataset
 * </ol>
 *
 * @param <IN> Type of input records
 * @param <KEY> Output type of #keyBy method
 * @param <VALUE> Output type of #valueBy method
 * @param <OUT> Type of output value
 */
@Audience(Audience.Type.CLIENT)
@Recommended(
    reason =
        "Is very recommended to override because of performance in "
      + "a specific area of (mostly) batch calculations where combiners "
      + "can be efficiently used in the executor-specific implementation",
    state = StateComplexity.CONSTANT_IF_COMBINABLE,
    repartitions = 1
)
public class ReduceByKey<IN, KEY, VALUE, OUT, W extends Window>
    extends StateAwareWindowWiseSingleInputOperator<
        IN, IN, IN, KEY, Pair<KEY, OUT>, W,
        ReduceByKey<IN, KEY, VALUE, OUT, W>>
    implements Builders.OutputValues<KEY, OUT> {

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

  public interface ReduceBy<IN, KEY, VALUE> {

    /**
     * Define a function that reduces all values related to one key into one result object.
     * The function is not combinable - i.e. partial results cannot be made up before shuffle.
     * To get better performance use {@link #combineBy} method.
     *
     * @param <OUT> type of output element
     *
     * @param reducer function that reduces all values into one output object
     *
     * @return next builder to complete the setup of the {@link ReduceByKey} operator
     */
    default <OUT> SortableDatasetBuilder4<IN, KEY, VALUE, OUT> reduceBy(
        ReduceFunction<VALUE, OUT> reducer) {

      return reduceBy((Stream<VALUE> in, Collector<OUT> ctx) -> {
        ctx.collect(reducer.apply(in));
      });
    }


    /**
     * Define a function that reduces all values related to one key into one or more
     * result objects.
     * The function is not combinable - i.e. partial results cannot be made up before shuffle.
     * To get better performance use {@link #combineBy} method.
     *
     * @param <OUT> type of output element
     *
     * @param reducer function that reduces all values into output values
     *
     * @return next builder to complete the setup of the {@link ReduceByKey} operator
     */
    <OUT> SortableDatasetBuilder4<IN, KEY, VALUE, OUT> reduceBy(
        ReduceFunctor<VALUE, OUT> reducer);

    /**
     * Define a function that reduces all values related to one key into one result object.
     * The function is combinable (associative and commutative) so it can be used to
     * compute partial results before shuffle.
     *
     * @param reducer function that reduces all values into one output object
     * @return next builder to complete the setup of the {@link ReduceByKey} operator
     */
    default DatasetBuilder4<IN, KEY, VALUE, VALUE> combineBy(
        CombinableReduceFunction<VALUE> reducer) {
      return reduceBy(toReduceFunctor(reducer));
    }

  }

  public static class DatasetBuilder2<IN, KEY> implements ReduceBy<IN, KEY, IN> {
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
     * {@link ReduceByKey} operator's input elements to get
     * reduced by a later supplied reduce function.
     *
     * @param <VALUE> the type of the extracted values
     *
     * @param valueExtractor a user defined function to extract values from the
     *                        processed input dataset's elements for later
     *                        reduction
     *
     * @return the next builder to complete the setup of the {@link ReduceByKey} operator
     */
    public <VALUE> DatasetBuilder3<IN, KEY, VALUE> valueBy(
        UnaryFunction<IN, VALUE> valueExtractor) {

      return new DatasetBuilder3<>(name, input, keyExtractor, valueExtractor);
    }

    @Override
    public <OUT> SortableDatasetBuilder4<IN, KEY, IN, OUT> reduceBy(
        ReduceFunctor<IN, OUT> reducer) {

      return new SortableDatasetBuilder4<>(
          name, input, keyExtractor, e-> e, reducer, null);
    }
  }

  public static class DatasetBuilder3<IN, KEY, VALUE> implements ReduceBy<IN, KEY, VALUE> {
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

    @Override
    public <OUT> SortableDatasetBuilder4<IN, KEY, VALUE, OUT> reduceBy(
        ReduceFunctor<VALUE, OUT> reducer) {

      return new SortableDatasetBuilder4<>(
          name, input, keyExtractor, valueExtractor, reducer, null);
    }
  }

  public static class DatasetBuilder4<IN, KEY, VALUE, OUT>
      implements Builders.Output<Pair<KEY, OUT>>,
          Builders.OutputValues<KEY, OUT>,
          Builders.WindowBy<IN, DatasetBuilder4<IN, KEY, VALUE, OUT>> {

    final String name;
    final Dataset<IN> input;
    final UnaryFunction<IN, KEY> keyExtractor;
    final UnaryFunction<IN, VALUE> valueExtractor;
    final ReduceFunctor<VALUE, OUT> reducer;
    final @Nullable BinaryFunction<VALUE, VALUE, Integer> valuesComparator;

    DatasetBuilder4(String name,
                    Dataset<IN> input,
                    UnaryFunction<IN, KEY> keyExtractor,
                    UnaryFunction<IN, VALUE> valueExtractor,
                    ReduceFunctor<VALUE, OUT> reducer,
                    @Nullable BinaryFunction<VALUE, VALUE, Integer> valuesComparator) {

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
      this.valueExtractor = Objects.requireNonNull(valueExtractor);
      this.reducer = Objects.requireNonNull(reducer);
      this.valuesComparator = valuesComparator;
    }

    @Override
    public <W extends Window>
    DatasetBuilder5<IN, KEY, VALUE, OUT, W>
    windowBy(Windowing<IN, W> windowing) {
      return new DatasetBuilder5<>(
          name, input, keyExtractor, valueExtractor,
          reducer, Objects.requireNonNull(windowing),
          valuesComparator);
    }

    @Override
    public Dataset<Pair<KEY, OUT>> output() {
      return new DatasetBuilder5<>(
          name, input, keyExtractor, valueExtractor,
          reducer, null, valuesComparator).output();
    }
  }


  public static class SortableDatasetBuilder4<IN, KEY, VALUE, OUT>
      extends DatasetBuilder4<IN, KEY, VALUE, OUT> {

    SortableDatasetBuilder4(
        String name,
        Dataset<IN> input,
        UnaryFunction<IN, KEY> keyExtractor,
        UnaryFunction<IN, VALUE> valueExtractor,
        ReduceFunctor<VALUE, OUT> reducer,
        @Nullable BinaryFunction<VALUE, VALUE, Integer> valuesComparator) {

      super(name, input, keyExtractor, valueExtractor, reducer, valuesComparator);
    }


    /**
     * Sort values going to `reduceBy` function by given comparator.
     * @param comparator function with contract defined by {@code java.util.Comparator#compare}.
     *
     * @return next step builder
     */
    public DatasetBuilder4<IN, KEY, VALUE, OUT> withSortedValues(
        BinaryFunction<VALUE, VALUE, Integer> comparator) {

      return new SortableDatasetBuilder4<>(
          name, input, keyExtractor, valueExtractor,
          reducer, comparator);
    }

  }



  public static class DatasetBuilder5<IN, KEY, VALUE, OUT, W extends Window>
      extends DatasetBuilder4<IN, KEY, VALUE, OUT>
      implements Builders.OutputValues<KEY, OUT> {

    @Nullable
    private final Windowing<IN, W> windowing;

    DatasetBuilder5(
        String name,
        Dataset<IN> input,
        UnaryFunction<IN, KEY> keyExtractor,
        UnaryFunction<IN, VALUE> valueExtractor,
        ReduceFunctor<VALUE, OUT> reducer,
        @Nullable Windowing<IN, W> windowing,
        @Nullable BinaryFunction<VALUE, VALUE, Integer> valuesComparator) {

      super(name, input, keyExtractor, valueExtractor, reducer, valuesComparator);
      this.windowing = windowing;
    }

    @Override
    public Dataset<Pair<KEY, OUT>> output() {
      Flow flow = input.getFlow();
      ReduceByKey<IN, KEY, VALUE, OUT, W> reduce = new ReduceByKey<>(
              name, flow, input, keyExtractor, valueExtractor,
              windowing, reducer, valuesComparator);
      flow.add(reduce);
      return reduce.output();
    }
  }

  /**
   * Starts building a nameless {@link ReduceByKey} operator to process
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
    return new KeyByBuilder<>("ReduceByKey", input);
  }

  /**
   * Starts building a named {@link ReduceByKey} operator.
   *
   * @param name a user provided name of the new operator to build
   *
   * @return a builder to complete the setup of the new operator
   */
  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  final ReduceFunctor<VALUE, OUT> reducer;
  final UnaryFunction<IN, VALUE> valueExtractor;
  @Nullable
  final BinaryFunction<VALUE, VALUE, Integer> valueComparator;

  @SuppressWarnings("unchecked")
  ReduceByKey(String name,
              Flow flow,
              Dataset<IN> input,
              UnaryFunction<IN, KEY> keyExtractor,
              UnaryFunction<IN, VALUE> valueExtractor,
              @Nullable Windowing<IN, W> windowing,
              CombinableReduceFunction<OUT> reducer) {
    this(
        name, flow, input, keyExtractor, valueExtractor,
        windowing, (ReduceFunctor<VALUE, OUT>) toReduceFunctor(reducer),
        null);
  }


  ReduceByKey(String name,
              Flow flow,
              Dataset<IN> input,
              UnaryFunction<IN, KEY> keyExtractor,
              UnaryFunction<IN, VALUE> valueExtractor,
              @Nullable Windowing<IN, W> windowing,
              ReduceFunctor<VALUE, OUT> reducer,
              @Nullable BinaryFunction<VALUE, VALUE, Integer> valueComparator) {

    super(name, flow, input, keyExtractor, windowing);
    this.reducer = reducer;
    this.valueExtractor = valueExtractor;
    this.valueComparator = valueComparator;
  }

  public ReduceFunctor<VALUE, OUT> getReducer() {
    return reducer;
  }

  public boolean isCombinable() {
    return reducer.isCombinable();
  }

  public UnaryFunction<IN, VALUE> getValueExtractor() {
    return valueExtractor;
  }

  @SuppressWarnings("unchecked")
  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    StateSupport.MergeFromStateMerger stateCombine =
            new StateSupport.MergeFromStateMerger<>();
    StateFactory stateFactory = reducer.isCombinable()
            ? new CombiningReduceState.Factory<>((ReduceFunctor) reducer)
            : new NonCombiningReduceState.Factory<>(reducer, valueComparator);
    Flow flow = getFlow();
    Operator reduceState = new ReduceStateByKey(getName(),
        flow, input, keyExtractor, valueExtractor,
        windowing,
        stateFactory, stateCombine);
    return DAG.of(reduceState);
  }

  static <VALUE> ReduceFunctor<VALUE, VALUE> toReduceFunctor(
      CombinableReduceFunction<VALUE> reducer1) {

    return new ReduceFunctor<VALUE, VALUE>() {
      @Override
      public boolean isCombinable() {
        return true;
      }

      @Override
      public void apply(Stream<VALUE> elem, Collector<VALUE> context) {
        context.collect(reducer1.apply(elem));
      }
    };
  }


  static class CombiningReduceState<E>
          implements State<E, E>, StateSupport.MergeFrom<CombiningReduceState<E>> {

    static final class Factory<E> implements StateFactory<E, E, State<E, E>> {
      private final ReduceFunctor<E, E> r;

      Factory(ReduceFunctor<E, E> r) {
        this.r = Objects.requireNonNull(r);
      }

      @Override
      public State<E, E> createState(
          StateContext context, Collector<E> collector) {
        return new CombiningReduceState<>(context.getStorageProvider(), r);
      }
    }

    @SuppressWarnings("unchecked")
    private static final ValueStorageDescriptor STORAGE_DESC =
            ValueStorageDescriptor.of("rbsk-value", (Class) Object.class, null);

    private final ReduceFunctor<E, E> reducer;
    private final ValueStorage<E> storage;
    private final SingleValueContext<E> context = new SingleValueContext<>();

    CombiningReduceState(StorageProvider storageProvider,
                         ReduceFunctor<E, E> reducer) {
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
        this.reducer.apply(Stream.of(v, element), context);
        this.storage.set(context.getAndResetValue());
      }
    }

    @Override
    public void flush(Collector<E> context) {
      context.collect(storage.get());
    }

    @Override
    public void close() {
      storage.clear();
    }

    @Override
    public void mergeFrom(CombiningReduceState<E> other) {
      this.add(other.storage.get());
    }
  }

  private static class NonCombiningReduceState<IN, OUT>
          implements State<IN, OUT>, StateSupport.MergeFrom<NonCombiningReduceState<IN, OUT>> {

    static final class Factory<IN, OUT>
            implements StateFactory<IN, OUT, NonCombiningReduceState<IN, OUT>> {

      private final ReduceFunctor<IN, OUT> r;
      private final BinaryFunction<IN, IN, Integer> comparator;

      Factory(
          ReduceFunctor<IN, OUT> r,
          @Nullable BinaryFunction<IN, IN, Integer> comparator) {

        this.r = Objects.requireNonNull(r);
        this.comparator = comparator;
      }

      @Override
      public NonCombiningReduceState<IN, OUT>
      createState(StateContext context, Collector<OUT> collector) {
        return new NonCombiningReduceState<>(context, r, comparator);
      }
    }

    @SuppressWarnings("unchecked")
    private static final ListStorageDescriptor STORAGE_DESC =
            ListStorageDescriptor.of("values", (Class) Object.class);

    private final ReduceFunctor<IN, OUT> reducer;
    private final ListStorage<IN> reducibleValues;
    private final SpillTools spill;
    @Nullable
    private final BinaryFunction<IN, IN, Integer> comparator;

    NonCombiningReduceState(
        StateContext context,
        ReduceFunctor<IN, OUT> reducer,
        BinaryFunction<IN, IN, Integer> comparator) {

      this.reducer = Objects.requireNonNull(reducer);
      this.comparator = comparator;

      @SuppressWarnings("unchecked")
      ListStorage<IN> ls = context.getStorageProvider().getListStorage(STORAGE_DESC);
      reducibleValues = ls;
      this.spill = context.getSpillTools();
    }

    @Override
    public void add(IN element) {
      reducibleValues.add(element);
    }

    @Override
    public void flush(Collector<OUT> ctx) {
      if (comparator != null) {
        try {
          Comparator<IN> c = comparator::apply;
          Iterable<IN> values = reducibleValues.get();
          try (ExternalIterable<IN> sorted = spill.sorted(values, c)) {
            reducer.apply(StreamSupport.stream(sorted.spliterator(), false), ctx);
          }
        } catch (InterruptedException ex) {
          throw new RuntimeException(ex);
        }
      } else {
        reducer.apply(
            StreamSupport.stream(reducibleValues.get().spliterator(), false),
            ctx);
      }
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
