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
import java.util.Comparator;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.Recommended;
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
import org.apache.beam.sdk.extensions.euphoria.core.client.io.ExternalIterable;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.SpillTools;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Builders;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.OptionalMethodBuilder;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.StateAwareWindowWiseSingleInputOperator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.hint.OutputHint;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.ListStorage;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.ListStorageDescriptor;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.State;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.StateContext;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.StateFactory;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.StorageProvider;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.ValueStorage;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.ValueStorageDescriptor;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.windowing.WindowingDesc;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeAwareReduceFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeAwareUnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.extensions.euphoria.core.executor.graph.DAG;
import org.apache.beam.sdk.extensions.euphoria.core.executor.util.SingleValueContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * Operator performing state-less aggregation by given reduce function. The reduction is performed
 * on all extracted values on each key-window.
 *
 * <p>If provided function is {@link CombinableReduceFunction} partial reduction is performed
 * before shuffle. If the function is not combinable all values must be first sent through the
 * network and the reduction is done afterwards on target machines.
 *
 * <p>Custom {@link Windowing} can be set, otherwise values from input operator are used.
 *
 * <h3>Builders:</h3>
 *
 * <ol>
 * <li>{@code [named] ..................} give name to the operator [optional]
 * <li>{@code of .......................} input dataset
 * <li>{@code keyBy ....................} key extractor function
 * <li>{@code [valueBy] ................} value extractor function (default: identity)
 * <li>{@code (combineBy | reduceBy)....} {@link CombinableReduceFunction} or {@link
 * ReduceFunction} for combinable or non-combinable function
 * <li>{@code [withSortedValues] .......} use comparator for sorting values prior to being passed
 * to {@link ReduceFunction} function (applicable only for non-combinable version)
 * <li>{@code [windowBy] ...............} windowing (see {@link WindowFn}), default is no windowing
 * <li>{@code [triggeredBy] ............} defines windowing trigger, follows [windowBy] if called
 * <li>{@code [accumulationMode] .......} windowing accumulation mode, follows [triggeredBy]
 * <li>{@code (output | outputValues) ..} build output dataset
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
public class ReduceByKey<InputT, K, V, OutputT, W extends BoundedWindow>
    extends StateAwareWindowWiseSingleInputOperator<
        InputT, InputT, K, Pair<K, OutputT>, W, ReduceByKey<InputT, K, V, OutputT, W>> {

  final ReduceFunctor<V, OutputT> reducer;

  // builder classes used when input is Dataset<InputT> ----------------------
  final UnaryFunction<InputT, V> valueExtractor;
  @Nullable
  final BinaryFunction<V, V, Integer> valueComparator;

  @SuppressWarnings("unchecked")
  ReduceByKey(
      String name,
      Flow flow,
      Dataset<InputT> input,
      UnaryFunction<InputT, K> keyExtractor,
      UnaryFunction<InputT, V> valueExtractor,
      @Nullable WindowingDesc<Object, W> windowing,
      @Nullable Windowing euphoriaWindowing,
      CombinableReduceFunction<OutputT> reducer,
      Set<OutputHint> outputHints) {
    this(
        name,
        flow,
        input,
        keyExtractor,
        valueExtractor,
        windowing,
        euphoriaWindowing,
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
      @Nullable WindowingDesc<Object, W> windowing,
      @Nullable Windowing euphoriaWindowing,
      ReduceFunctor<V, OutputT> reducer,
      @Nullable BinaryFunction<V, V, Integer> valueComparator,
      Set<OutputHint> outputHints) {

    super(name, flow, input, keyExtractor, windowing, euphoriaWindowing, outputHints);
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

  /**
   * TODO: complete javadoc.
   */
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
            euphoriaWindowing,
            stateFactory,
            stateCombine,
            getHints());
    return DAG.of(reduceState);
  }

  /**
   * TODO: complete javadoc.
   */
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
    default <OutputT> WithSortedValuesBuilder<InputT, K, V, OutputT> reduceBy(
        ReduceFunction<V, OutputT> reducer) {
      return reduceBy((Stream<V> in, Collector<OutputT> ctx) -> ctx.collect(reducer.apply(in)));
    }

    default <OutputT> WithSortedValuesBuilder<InputT, K, V, OutputT> reduceBy(
        ReduceFunction<V, OutputT> reducer, TypeDescriptor<OutputT> outputTypeDescriptor) {
      return reduceBy(
          (Stream<V> in, Collector<OutputT> ctx) ->
              ctx.collect(reducer.apply(in)), outputTypeDescriptor);
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
    <OutputT> WithSortedValuesBuilder<InputT, K, V, OutputT> reduceBy(
        ReduceFunctor<V, OutputT> reducer);

    default <OutputT> WithSortedValuesBuilder<InputT, K, V, OutputT> reduceBy(
        ReduceFunctor<V, OutputT> reducer, TypeDescriptor<OutputT> outputTypeDescriptor) {
      return reduceBy(TypeAwareReduceFunctor.of(reducer, outputTypeDescriptor));
    }

    /**
     * Define a function that reduces all values related to one key into one result object. The
     * function is combinable (associative and commutative) so it can be used to compute partial
     * results before shuffle.
     *
     * @param reducer function that reduces all values into one output object
     * @return next builder to complete the setup of the {@link ReduceByKey} operator
     */
    default WindowByBuilder<InputT, K, V, V> combineBy(CombinableReduceFunction<V> reducer) {
      return reduceBy(toReduceFunctor(reducer));
    }

    default WindowByBuilder<InputT, K, V, V> combineBy(
        CombinableReduceFunction<V> reducer, TypeDescriptor<V> typeHint) {
      return reduceBy(TypeAwareReduceFunctor.of(toReduceFunctor(reducer), typeHint));
    }
  }

  /**
   * Parameters of this operator used in builders.
   */
  private static final class BuilderParams<InputT, K, V, OutputT, W extends BoundedWindow>
      extends WindowingParams<W> {

    String name;
    Dataset<InputT> input;
    UnaryFunction<InputT, K> keyExtractor;
    UnaryFunction<InputT, V> valueExtractor;
    ReduceFunctor<V, OutputT> reducer;
    @Nullable
    BinaryFunction<V, V, Integer> valuesComparator;
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

    private final BuilderParams<InputT, ?, ?, ?, ?> params =
        new BuilderParams<>();

    KeyByBuilder(String name, Dataset<InputT> input) {
      params.name = Objects.requireNonNull(name);
      params.input = Objects.requireNonNull(input);
    }

    @Override
    public <K> ValueByReduceByBuilder<InputT, K> keyBy(UnaryFunction<InputT, K> keyExtractor) {

      @SuppressWarnings("unchecked")
      BuilderParams<InputT, K, ?, ?, ?> paramsCasted =
          (BuilderParams<InputT, K, ?, ?, ?>) params;

      paramsCasted.keyExtractor = Objects.requireNonNull(keyExtractor);

      return new ValueByReduceByBuilder<>(paramsCasted);
    }

    @Override
    public <K> ValueByReduceByBuilder<InputT, K> keyBy(
        UnaryFunction<InputT, K> keyExtractor, TypeDescriptor<K> typeHint) {
      return keyBy(TypeAwareUnaryFunction.of(keyExtractor, typeHint));
    }
  }

  /**
   * TODO: complete javadoc.
   */
  public static class ValueByReduceByBuilder<InputT, K> implements ReduceBy<InputT, K, InputT> {

    private final BuilderParams<InputT, K, ?, ?, ?> params;

    ValueByReduceByBuilder(BuilderParams<InputT, K, ?, ?, ?> params) {
      this.params = params;
    }

    /**
     * Specifies the function to derive a value from the {@link ReduceByKey} operator's input
     * elements to get reduced by a later supplied reduce function.
     *
     * @param <V> the type of the extracted values
     * @param valueExtractor a user defined function to extract values from the processed input
     * dataset's elements for later reduction
     * @return the next builder to complete the setup of the {@link ReduceByKey} operator
     */
    public <V> ReduceByCombineByBuilder<InputT, K, V> valueBy(
        UnaryFunction<InputT, V> valueExtractor) {

      @SuppressWarnings("unchecked")
      BuilderParams<InputT, K, V, ?, ?> paramsCasted =
          (BuilderParams<InputT, K, V, ?, ?>) params;

      paramsCasted.valueExtractor = Objects.requireNonNull(valueExtractor);
      return new ReduceByCombineByBuilder<>(paramsCasted);
    }

    public <V> ReduceByCombineByBuilder<InputT, K, V> valueBy(
        UnaryFunction<InputT, V> valueExtractor, TypeDescriptor<V> typeHint) {
      return valueBy(TypeAwareUnaryFunction.of(valueExtractor, typeHint));
    }

    @Override
    public <OutputT> WithSortedValuesBuilder<InputT, K, InputT, OutputT> reduceBy(
        ReduceFunctor<InputT, OutputT> reducer) {

      @SuppressWarnings("unchecked") final BuilderParams<InputT, K, InputT, OutputT, ?>
          paramsCasted = (BuilderParams<InputT, K, InputT, OutputT, ?>) params;

      paramsCasted.valueExtractor = e -> e;
      paramsCasted.reducer = Objects.requireNonNull(reducer);

      return new WithSortedValuesBuilder<>(paramsCasted);
    }
  }

  /**
   * TODO: complete javadoc.
   */
  public static class ReduceByCombineByBuilder<InputT, K, V> implements ReduceBy<InputT, K, V> {

    private final BuilderParams<InputT, K, V, ?, ?> params;

    ReduceByCombineByBuilder(BuilderParams<InputT, K, V, ?, ?> params) {
      this.params = params;
    }

    @Override
    public <OutputT> WithSortedValuesBuilder<InputT, K, V, OutputT> reduceBy(
        ReduceFunctor<V, OutputT> reducer) {

      @SuppressWarnings("unchecked") final BuilderParams<InputT, K, V, OutputT, ?> paramsCasted =
          (BuilderParams<InputT, K, V, OutputT, ?>) params;

      paramsCasted.reducer = Objects.requireNonNull(reducer);

      return new WithSortedValuesBuilder<>(paramsCasted);
    }
  }

  /**
   * TODO: complete javadoc.
   */
  public static class WindowByBuilder<InputT, K, V, OutputT>
      implements Builders.Output<Pair<K, OutputT>>,
      Builders.OutputValues<K, OutputT>,
      Builders.WindowBy<TriggerByBuilder<InputT, K, V, OutputT, ?>>,
      OptionalMethodBuilder<WindowByBuilder<InputT, K, V, OutputT>,
          OutputBuilder<InputT, K, V, OutputT, ?>> {

    final BuilderParams<InputT, K, V, OutputT, ?> params;

    WindowByBuilder(BuilderParams<InputT, K, V, OutputT, ?> params) {
      this.params = params;
    }

    @Override
    public <W extends BoundedWindow> TriggerByBuilder<InputT, K, V, OutputT, W> windowBy(
        WindowFn<Object, W> windowing) {

      @SuppressWarnings("unchecked")
      BuilderParams<InputT, K, V, OutputT, W> paramsCast =
          (BuilderParams<InputT, K, V, OutputT, W>) params;

      paramsCast.windowFn = Objects.requireNonNull(windowing);
      return new TriggerByBuilder<>(paramsCast);
    }

    @Override
    public <W extends Window<W>> OutputBuilder<InputT, K, V, OutputT, ?> windowBy(
        Windowing<?, W> windowing) {
      params.euphoriaWindowing = Objects.requireNonNull(windowing);
      return new OutputBuilder<>(params);
    }

    @Override
    public Dataset<Pair<K, OutputT>> output(OutputHint... outputHints) {
      return new OutputBuilder<>(params).output(outputHints);
    }

    @Override
    public OutputBuilder<InputT, K, V, OutputT, ?> applyIf(boolean cond,
        UnaryFunction<WindowByBuilder<InputT, K, V, OutputT>,
            OutputBuilder<InputT, K, V, OutputT, ?>> applyWhenConditionHolds) {

      Objects.requireNonNull(applyWhenConditionHolds);

      if (cond) {
        return applyWhenConditionHolds.apply(this);
      }

      return new OutputBuilder<>(params);
    }
  }

  /**
   * TODO: complete javadoc.
   */
  public static class WithSortedValuesBuilder<InputT, K, V, OutputT>
      extends WindowByBuilder<InputT, K, V, OutputT> { //TODO chceme tady tenhle extends ?

    WithSortedValuesBuilder(BuilderParams<InputT, K, V, OutputT, ?> params) {
      super(params);
    }

    /**
     * Sort values going to `reduceBy` function by given comparator.
     *
     * @param comparator function with contract defined by {@code java.util.Comparator#compare}.
     * @return next step builder
     */
    public WindowByBuilder<InputT, K, V, OutputT> withSortedValues(
        BinaryFunction<V, V, Integer> comparator) {

      params.valuesComparator = Objects.requireNonNull(comparator);

      return new WithSortedValuesBuilder<>(params);
    }
  }

  /**
   * Last builder in a chain. It concludes this operators creation by calling {@link
   * #output(OutputHint...)}.
   */
  public static class OutputBuilder<InputT, K, V, OutputT, W extends BoundedWindow>
      implements Builders.OutputValues<K, OutputT> {

    private final BuilderParams<InputT, K, V, OutputT, W> params;

    OutputBuilder(BuilderParams<InputT, K, V, OutputT, W> params) {
      this.params = params;
    }

    @Override
    public Dataset<Pair<K, OutputT>> output(OutputHint... outputHints) {
      Flow flow = params.input.getFlow();
      ReduceByKey<InputT, K, V, OutputT, W> reduce =
          new ReduceByKey<>(
              params.name,
              flow,
              params.input,
              params.keyExtractor,
              params.valueExtractor,
              params.getWindowing(),
              params.euphoriaWindowing,
              params.reducer,
              params.valuesComparator,
              Sets.newHashSet(outputHints));
      flow.add(reduce);
      return reduce.output();
    }
  }

  /**
   * Trigger defining operator builder.
   */
  public static class TriggerByBuilder<InputT, K, V, OutputT, W extends BoundedWindow>
      implements Builders.TriggeredBy<AccumulatorModeBuilder<InputT, K, V, OutputT, W>> {

    private final BuilderParams<InputT, K, V, OutputT, W> params;

    TriggerByBuilder(BuilderParams<InputT, K, V, OutputT, W> params) {
      this.params = params;
    }

    @Override
    public AccumulatorModeBuilder<InputT, K, V, OutputT, W> triggeredBy(Trigger trigger) {
      params.trigger = Objects.requireNonNull(trigger);
      return new AccumulatorModeBuilder<>(params);
    }

  }

  /**
   * {@link WindowingStrategy.AccumulationMode} defining operator builder.
   */
  public static class AccumulatorModeBuilder<InputT, K, V, OutputT, W extends BoundedWindow>
      implements Builders.AccumulatorMode<OutputBuilder<InputT, K, V, OutputT, W>> {

    private final BuilderParams<InputT, K, V, OutputT, W> params;

    AccumulatorModeBuilder(BuilderParams<InputT, K, V, OutputT, W> params) {
      this.params = params;
    }

    @Override
    public OutputBuilder<InputT, K, V, OutputT, W> accumulationMode(
        WindowingStrategy.AccumulationMode accumulationMode) {

      params.accumulationMode = Objects.requireNonNull(accumulationMode);
      return new OutputBuilder<>(params);
    }

  }

  /**
   * TODO: complete javadoc.
   */
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

  /**
   * TODO: complete javadoc.
   */
  private static class NonCombiningReduceState<InputT, OutputT>
      implements State<InputT, OutputT>,
      StateSupport.MergeFrom<NonCombiningReduceState<InputT, OutputT>> {

    @SuppressWarnings("unchecked")
    private static final ListStorageDescriptor STORAGE_DESC =
        ListStorageDescriptor.of("values", (Class) Object.class);

    private final ReduceFunctor<InputT, OutputT> reducer;
    private final ListStorage<InputT> reducibleValues;
    private final SpillTools spill;
    @Nullable
    private final BinaryFunction<InputT, InputT, Integer> comparator;

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
