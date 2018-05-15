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
package cz.seznam.euphoria.core.client.operator;

import com.google.common.collect.Sets;
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
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.ExternalIterable;
import cz.seznam.euphoria.core.client.io.SpillTools;
import cz.seznam.euphoria.core.client.operator.hint.OutputHint;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StateContext;
import cz.seznam.euphoria.core.client.operator.state.StateFactory;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.core.client.type.TypeAwareReduceFunctor;
import cz.seznam.euphoria.core.client.type.TypeAwareUnaryFunction;
import cz.seznam.euphoria.core.client.type.TypeHint;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.graph.DAG;
import cz.seznam.euphoria.core.executor.util.SingleValueContext;
import java.util.Comparator;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

/**
 * Operator performing state-less aggregation by given reduce function. The reduction is performed
 * on all extracted values on each key-window.
 *
 * <p>If provided function is {@link CombinableReduceFunction} partial reduction is performed before
 * shuffle. If the function is not combinable all values must be first sent through the network and
 * the reduction is done afterwards on target machines.
 *
 * <p>Custom {@link Windowing} can be set, otherwise values from input operator are used.
 *
 * <h3>Builders:</h3>
 *
 * <ol>
 *   <li>{@code [named] ..................} give name to the operator [optional]
 *   <li>{@code of .......................} input dataset
 *   <li>{@code keyBy ....................} key extractor function
 *   <li>{@code [valueBy] ................} value extractor function (default: identity)
 *   <li>{@code (combineBy | reduceBy)....} {@link CombinableReduceFunction} or {@link
 *       ReduceFunction} for combinable or non-combinable function
 *   <li>{@code [withSortedValues] .......} use comparator for sorting values prior to being passed
 *       to {@link ReduceFunction} function (applicable only for non-combinable version)
 *   <li>{@code [windowBy] ...............} windowing function (see {@link Windowing}), default
 *       attached windowing
 *   <li>{@code (output | outputValues) ..} build output dataset
 * </ol>
 *
 * @param <InputT> Type of input records
 * @param <K> Output type of #keyBy method
 * @param <V> Output type of #valueBy method
 * @param <OutputT> Type of output value
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
public class ReduceByKey<InputT, K, V, OutputT, W extends Window<W>>
    extends StateAwareWindowWiseSingleInputOperator<
        InputT, InputT, InputT, K, Pair<K, OutputT>, W, ReduceByKey<InputT, K, V, OutputT, W>> {

  final ReduceFunctor<V, OutputT> reducer;

  // builder classes used when input is Dataset<InputT> ----------------------
  final UnaryFunction<InputT, V> valueExtractor;
  @Nullable final BinaryFunction<V, V, Integer> valueComparator;

  @SuppressWarnings("unchecked")
  ReduceByKey(
      String name,
      Flow flow,
      Dataset<InputT> input,
      UnaryFunction<InputT, K> keyExtractor,
      UnaryFunction<InputT, V> valueExtractor,
      @Nullable Windowing<InputT, W> windowing,
      CombinableReduceFunction<OutputT> reducer,
      Set<OutputHint> outputHints) {
    this(
        name,
        flow,
        input,
        keyExtractor,
        valueExtractor,
        windowing,
        (ReduceFunctor<V, OutputT>) toReduceFunctor(reducer),
        null,
        outputHints);
  }

  ReduceByKey(
      String name,
      Flow flow,
      Dataset<InputT> input,
      UnaryFunction<InputT, K> keyExtractor,
      UnaryFunction<InputT, V> valueExtractor,
      @Nullable Windowing<InputT, W> windowing,
      ReduceFunctor<V, OutputT> reducer,
      @Nullable BinaryFunction<V, V, Integer> valueComparator,
      Set<OutputHint> outputHints) {

    super(name, flow, input, keyExtractor, windowing, outputHints);
    this.reducer = reducer;
    this.valueExtractor = valueExtractor;
    this.valueComparator = valueComparator;
  }

  /**
   * Starts building a nameless {@link ReduceByKey} operator to process the given input dataset.
   *
   * @param <InputT> the type of elements of the input dataset
   * @param input the input data set to be processed
   * @return a builder to complete the setup of the new operator
   * @see #named(String)
   * @see OfBuilder#of(Dataset)
   */
  public static <InputT> KeyByBuilder<InputT> of(Dataset<InputT> input) {
    return new KeyByBuilder<>("ReduceByKey", input);
  }

  /**
   * Starts building a named {@link ReduceByKey} operator.
   *
   * @param name a user provided name of the new operator to build
   * @return a builder to complete the setup of the new operator
   */
  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  /** TODO: complete javadoc. */
  static <V> ReduceFunctor<V, V> toReduceFunctor(CombinableReduceFunction<V> reducer1) {

    return new ReduceFunctor<V, V>() {

      @Override
      public boolean isCombinable() {
        return true;
      }

      @Override
      public void apply(Stream<V> elem, Collector<V> context) {
        context.collect(reducer1.apply(elem));
      }
    };
  }

  public ReduceFunctor<V, OutputT> getReducer() {
    return reducer;
  }

  public boolean isCombinable() {
    return reducer.isCombinable();
  }

  public UnaryFunction<InputT, V> getValueExtractor() {
    return valueExtractor;
  }

  @Nullable
  public BinaryFunction<V, V, Integer> getValueComparator() {
    return valueComparator;
  }

  @SuppressWarnings("unchecked")
  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    StateSupport.MergeFromStateMerger stateCombine = new StateSupport.MergeFromStateMerger<>();
    StateFactory stateFactory =
        reducer.isCombinable()
            ? new CombiningReduceState.Factory<>((ReduceFunctor) reducer)
            : new NonCombiningReduceState.Factory<>(reducer, valueComparator);
    Flow flow = getFlow();
    Operator reduceState =
        new ReduceStateByKey(
            getName(),
            flow,
            input,
            keyExtractor,
            valueExtractor,
            windowing,
            stateFactory,
            stateCombine,
            getHints());
    return DAG.of(reduceState);
  }

  /** TODO: complete javadoc. */
  public interface ReduceBy<InputT, K, V> {

    /**
     * Define a function that reduces all values related to one key into one result object. The
     * function is not combinable - i.e. partial results cannot be made up before shuffle. To get
     * better performance use {@link #combineBy} method.
     *
     * @param <OutputT> type of output element
     * @param reducer function that reduces all values into one output object
     * @return next builder to complete the setup of the {@link ReduceByKey} operator
     */
    default <OutputT> SortableDatasetBuilder4<InputT, K, V, OutputT> reduceBy(
        ReduceFunction<V, OutputT> reducer) {
      return reduceBy((Stream<V> in, Collector<OutputT> ctx) -> ctx.collect(reducer.apply(in)));
    }

    /**
     * Define a function that reduces all values related to one key into one or more result objects.
     * The function is not combinable - i.e. partial results cannot be made up before shuffle. To
     * get better performance use {@link #combineBy} method.
     *
     * @param <OutputT> type of output element
     * @param reducer function that reduces all values into output values
     * @return next builder to complete the setup of the {@link ReduceByKey} operator
     */
    <OutputT> SortableDatasetBuilder4<InputT, K, V, OutputT> reduceBy(
        ReduceFunctor<V, OutputT> reducer);

    /**
     * Define a function that reduces all values related to one key into one result object. The
     * function is combinable (associative and commutative) so it can be used to compute partial
     * results before shuffle.
     *
     * @param reducer function that reduces all values into one output object
     * @return next builder to complete the setup of the {@link ReduceByKey} operator
     */
    default DatasetBuilder4<InputT, K, V, V> combineBy(CombinableReduceFunction<V> reducer) {
      return reduceBy(toReduceFunctor(reducer));
    }

    default DatasetBuilder4<InputT, K, V, V> combineBy(
        CombinableReduceFunction<V> reducer, TypeHint<V> typeHint) {
      return reduceBy(TypeAwareReduceFunctor.of(toReduceFunctor(reducer), typeHint));
    }
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

    @Override
    public <K> DatasetBuilder2<InputT, K> keyBy(
        UnaryFunction<InputT, K> keyExtractor, TypeHint<K> typeHint) {
      return new DatasetBuilder2<>(name, input, TypeAwareUnaryFunction.of(keyExtractor, typeHint));
    }
  }

  /** TODO: complete javadoc. */
  public static class DatasetBuilder2<InputT, K> implements ReduceBy<InputT, K, InputT> {

    private final String name;
    private final Dataset<InputT> input;
    private final UnaryFunction<InputT, K> keyExtractor;

    DatasetBuilder2(String name, Dataset<InputT> input, UnaryFunction<InputT, K> keyExtractor) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
    }

    /**
     * Specifies the function to derive a value from the {@link ReduceByKey} operator's input
     * elements to get reduced by a later supplied reduce function.
     *
     * @param <V> the type of the extracted values
     * @param valueExtractor a user defined function to extract values from the processed input
     *     dataset's elements for later reduction
     * @return the next builder to complete the setup of the {@link ReduceByKey} operator
     */
    public <V> DatasetBuilder3<InputT, K, V> valueBy(UnaryFunction<InputT, V> valueExtractor) {
      return new DatasetBuilder3<>(name, input, keyExtractor, valueExtractor);
    }

    public <V> DatasetBuilder3<InputT, K, V> valueBy(
        UnaryFunction<InputT, V> valueExtractor, TypeHint<V> typeHint) {
      return valueBy(TypeAwareUnaryFunction.of(valueExtractor, typeHint));
    }

    @Override
    public <OutputT> SortableDatasetBuilder4<InputT, K, InputT, OutputT> reduceBy(
        ReduceFunctor<InputT, OutputT> reducer) {

      return new SortableDatasetBuilder4<>(name, input, keyExtractor, e -> e, reducer, null);
    }
  }

  /** TODO: complete javadoc. */
  public static class DatasetBuilder3<InputT, K, V> implements ReduceBy<InputT, K, V> {
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

    @Override
    public <OutputT> SortableDatasetBuilder4<InputT, K, V, OutputT> reduceBy(
        ReduceFunctor<V, OutputT> reducer) {

      return new SortableDatasetBuilder4<>(
          name, input, keyExtractor, valueExtractor, reducer, null);
    }
  }

  /** TODO: complete javadoc. */
  public static class DatasetBuilder4<InputT, K, V, OutputT>
      implements Builders.Output<Pair<K, OutputT>>,
          Builders.OutputValues<K, OutputT>,
          Builders.WindowBy<InputT, DatasetBuilder4<InputT, K, V, OutputT>> {

    final String name;
    final Dataset<InputT> input;
    final UnaryFunction<InputT, K> keyExtractor;
    final UnaryFunction<InputT, V> valueExtractor;
    final ReduceFunctor<V, OutputT> reducer;
    final @Nullable BinaryFunction<V, V, Integer> valuesComparator;

    DatasetBuilder4(
        String name,
        Dataset<InputT> input,
        UnaryFunction<InputT, K> keyExtractor,
        UnaryFunction<InputT, V> valueExtractor,
        ReduceFunctor<V, OutputT> reducer,
        @Nullable BinaryFunction<V, V, Integer> valuesComparator) {

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
      this.valueExtractor = Objects.requireNonNull(valueExtractor);
      this.reducer = Objects.requireNonNull(reducer);
      this.valuesComparator = valuesComparator;
    }

    @Override
    public <W extends Window<W>> DatasetBuilder5<InputT, K, V, OutputT, W> windowBy(
        Windowing<InputT, W> windowing) {
      return new DatasetBuilder5<>(
          name,
          input,
          keyExtractor,
          valueExtractor,
          reducer,
          Objects.requireNonNull(windowing),
          valuesComparator);
    }

    @Override
    public Dataset<Pair<K, OutputT>> output(OutputHint... outputHints) {
      return new DatasetBuilder5<>(
              name, input, keyExtractor, valueExtractor, reducer, null, valuesComparator)
          .output(outputHints);
    }
  }

  /** TODO: complete javadoc. */
  public static class SortableDatasetBuilder4<InputT, K, V, OutputT>
      extends DatasetBuilder4<InputT, K, V, OutputT> {

    SortableDatasetBuilder4(
        String name,
        Dataset<InputT> input,
        UnaryFunction<InputT, K> keyExtractor,
        UnaryFunction<InputT, V> valueExtractor,
        ReduceFunctor<V, OutputT> reducer,
        @Nullable BinaryFunction<V, V, Integer> valuesComparator) {

      super(name, input, keyExtractor, valueExtractor, reducer, valuesComparator);
    }

    /**
     * Sort values going to `reduceBy` function by given comparator.
     *
     * @param comparator function with contract defined by {@code java.util.Comparator#compare}.
     * @return next step builder
     */
    public DatasetBuilder4<InputT, K, V, OutputT> withSortedValues(
        BinaryFunction<V, V, Integer> comparator) {

      return new SortableDatasetBuilder4<>(
          name, input, keyExtractor, valueExtractor, reducer, comparator);
    }
  }

  /** TODO: complete javadoc. */
  public static class DatasetBuilder5<InputT, K, V, OutputT, W extends Window<W>>
      extends DatasetBuilder4<InputT, K, V, OutputT> implements Builders.OutputValues<K, OutputT> {

    @Nullable private final Windowing<InputT, W> windowing;

    DatasetBuilder5(
        String name,
        Dataset<InputT> input,
        UnaryFunction<InputT, K> keyExtractor,
        UnaryFunction<InputT, V> valueExtractor,
        ReduceFunctor<V, OutputT> reducer,
        @Nullable Windowing<InputT, W> windowing,
        @Nullable BinaryFunction<V, V, Integer> valuesComparator) {

      super(name, input, keyExtractor, valueExtractor, reducer, valuesComparator);
      this.windowing = windowing;
    }

    @Override
    public Dataset<Pair<K, OutputT>> output(OutputHint... outputHints) {
      Flow flow = input.getFlow();
      ReduceByKey<InputT, K, V, OutputT, W> reduce =
          new ReduceByKey<>(
              name,
              flow,
              input,
              keyExtractor,
              valueExtractor,
              windowing,
              reducer,
              valuesComparator,
              Sets.newHashSet(outputHints));
      flow.add(reduce);
      return reduce.output();
    }
  }

  /** TODO: complete javadoc. */
  static class CombiningReduceState<V1>
      implements State<V1, V1>, StateSupport.MergeFrom<CombiningReduceState<V1>> {

    @SuppressWarnings("unchecked")
    private static final ValueStorageDescriptor STORAGE_DESC =
        ValueStorageDescriptor.of("rbsk-value", (Class) Object.class, null);

    private final ReduceFunctor<V1, V1> reducer;
    private final ValueStorage<V1> storage;
    private final SingleValueContext<V1> context = new SingleValueContext<>();

    CombiningReduceState(StorageProvider storageProvider, ReduceFunctor<V1, V1> reducer) {
      this.reducer = Objects.requireNonNull(reducer);

      @SuppressWarnings("unchecked")
      ValueStorage<V1> vs = storageProvider.getValueStorage(STORAGE_DESC);
      this.storage = vs;
    }

    @Override
    public void add(V1 element) {
      V1 v = this.storage.get();
      if (v == null) {
        this.storage.set(element);
      } else {
        this.reducer.apply(Stream.of(v, element), context);
        this.storage.set(context.getAndResetValue());
      }
    }

    @Override
    public void flush(Collector<V1> context) {
      context.collect(storage.get());
    }

    @Override
    public void close() {
      storage.clear();
    }

    @Override
    public void mergeFrom(CombiningReduceState<V1> other) {
      this.add(other.storage.get());
    }

    static final class Factory<T> implements StateFactory<T, T, State<T, T>> {
      private final ReduceFunctor<T, T> r;

      Factory(ReduceFunctor<T, T> r) {
        this.r = Objects.requireNonNull(r);
      }

      @Override
      public State<T, T> createState(StateContext context, Collector<T> collector) {
        return new CombiningReduceState<>(context.getStorageProvider(), r);
      }
    }
  }

  /** TODO: complete javadoc. */
  private static class NonCombiningReduceState<InputT, OutputT>
      implements State<InputT, OutputT>,
          StateSupport.MergeFrom<NonCombiningReduceState<InputT, OutputT>> {

    @SuppressWarnings("unchecked")
    private static final ListStorageDescriptor STORAGE_DESC =
        ListStorageDescriptor.of("values", (Class) Object.class);

    private final ReduceFunctor<InputT, OutputT> reducer;
    private final ListStorage<InputT> reducibleValues;
    private final SpillTools spill;
    @Nullable private final BinaryFunction<InputT, InputT, Integer> comparator;

    NonCombiningReduceState(
        StateContext context,
        ReduceFunctor<InputT, OutputT> reducer,
        BinaryFunction<InputT, InputT, Integer> comparator) {

      this.reducer = Objects.requireNonNull(reducer);
      this.comparator = comparator;

      @SuppressWarnings("unchecked")
      ListStorage<InputT> ls = context.getStorageProvider().getListStorage(STORAGE_DESC);
      reducibleValues = ls;
      this.spill = context.getSpillTools();
    }

    @Override
    public void add(InputT element) {
      reducibleValues.add(element);
    }

    @Override
    public void flush(Collector<OutputT> ctx) {
      if (comparator != null) {
        try {
          Comparator<InputT> c = comparator::apply;
          Iterable<InputT> values = reducibleValues.get();
          try (ExternalIterable<InputT> sorted = spill.sorted(values, c)) {
            reducer.apply(StreamSupport.stream(sorted.spliterator(), false), ctx);
          }
        } catch (InterruptedException ex) {
          throw new RuntimeException(ex);
        }
      } else {
        reducer.apply(StreamSupport.stream(reducibleValues.get().spliterator(), false), ctx);
      }
    }

    @Override
    public void close() {
      reducibleValues.clear();
    }

    @Override
    public void mergeFrom(NonCombiningReduceState<InputT, OutputT> other) {
      this.reducibleValues.addAll(other.reducibleValues.get());
    }

    static final class Factory<InputT, OutputT>
        implements StateFactory<InputT, OutputT, NonCombiningReduceState<InputT, OutputT>> {

      private final ReduceFunctor<InputT, OutputT> r;
      private final BinaryFunction<InputT, InputT, Integer> comparator;

      Factory(
          ReduceFunctor<InputT, OutputT> r,
          @Nullable BinaryFunction<InputT, InputT, Integer> comparator) {

        this.r = Objects.requireNonNull(r);
        this.comparator = comparator;
      }

      @Override
      public NonCombiningReduceState<InputT, OutputT> createState(
          StateContext context, Collector<OutputT> collector) {
        return new NonCombiningReduceState<>(context, r, comparator);
      }
    }
  }
}
