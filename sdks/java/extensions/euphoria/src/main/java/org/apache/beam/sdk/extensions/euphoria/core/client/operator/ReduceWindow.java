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

import static java.util.Objects.requireNonNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.util.Optional;
import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.Derived;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.StateComplexity;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.BinaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.CombinableBinaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.CombinableReduceFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.ReduceFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.ReduceFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.VoidFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Builders;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.OptionalMethodBuilder;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.ShuffleOperator;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeAware;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.PCollectionLists;
import org.apache.beam.sdk.extensions.euphoria.core.translate.OperatorTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * Reduces all elements in a window. The operator corresponds to {@link ReduceByKey} with the same
 * key for all elements, so the actual key is defined only by window.
 *
 * <p>Custom windowing can be set, otherwise values from input operator are used.
 *
 * <h3>Builders:</h3>
 *
 * <ol>
 *   <li>{@code [named] ..................} give name to the operator [optional]
 *   <li>{@code of .......................} input dataset
 *   <li>{@code [valueBy] ................} value extractor function (default: identity)
 *   <li>{@code (combineBy | reduceBy)....} {@link CombinableReduceFunction} or {@link
 *       ReduceFunction} for combinable or non-combinable function
 *   <li>{@code [withSortedValues] .......} use comparator for sorting values prior to being passed
 *       to {@link ReduceFunction} function (applicable only for non-combinable version)
 *   <li>{@code [windowBy] ...............} windowing (see {@link WindowFn}), default is no
 *       windowing
 *   <li>{@code [triggeredBy] ............} defines windowing trigger, follows [windowBy] if called
 *   <li>{@code [accumulationMode] .......} windowing accumulation mode, follows [triggeredBy]
 *   <li>{@code output ...................} build output dataset
 * </ol>
 *
 * @deprecated Use Java SDK directly, Euphoria is scheduled for removal in a future release.
 */
@Audience(Audience.Type.CLIENT)
@Derived(state = StateComplexity.CONSTANT_IF_COMBINABLE, repartitions = 1)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@Deprecated
public class ReduceWindow<InputT, ValueT, AccT, OutputT>
    extends ShuffleOperator<InputT, Byte, OutputT>
    implements TypeAware.Value<ValueT>, CompositeOperator<InputT, OutputT> {

  private static final Byte B_ZERO = (byte) 0;

  /**
   * Starts building a nameless {@link ReduceWindow} operator to process the given input dataset.
   *
   * @param <InputT> the type of elements of the input dataset
   * @param input the input data set to be processed
   * @return a builder to complete the setup of the new operator
   * @see #named(String)
   * @see OfBuilder#of(PCollection)
   */
  public static <InputT> ValueByReduceByBuilder<InputT, InputT> of(PCollection<InputT> input) {
    return named(null).of(input);
  }

  /**
   * Starts building a named {@link ReduceWindow} operator.
   *
   * @param name a user provided name of the new operator to build
   * @return a builder to complete the setup of the new operator
   */
  public static OfBuilder named(@Nullable String name) {
    return new Builder(name);
  }

  /** Builder for 'of' step. */
  public interface OfBuilder extends Builders.Of {

    @Override
    <InputT> ValueByReduceByBuilder<InputT, InputT> of(PCollection<InputT> input);
  }

  /** Builder for 'reduceBy' step. */
  public interface ReduceByBuilder<ValueT> {

    /**
     * Define a function that reduces all values related to one key into one result object. The
     * function is not combinable - i.e. partial results cannot be made up before shuffle. To get
     * better performance use {@link #combineBy} method.
     *
     * @param <OutputT> type of output element
     * @param reducer function that reduces all values into one output object
     * @return next builder to complete the setup of the {@link ReduceByKey} operator
     */
    default <OutputT> WithSortedValuesBuilder<ValueT, OutputT> reduceBy(
        ReduceFunction<ValueT, OutputT> reducer) {
      return reduceBy(
          (Stream<ValueT> in, Collector<OutputT> ctx) -> ctx.collect(reducer.apply(in)));
    }

    default <OutputT> WithSortedValuesBuilder<ValueT, OutputT> reduceBy(
        ReduceFunction<ValueT, OutputT> reducer, TypeDescriptor<OutputT> outputType) {
      return reduceBy(
          (Stream<ValueT> in, Collector<OutputT> ctx) -> ctx.collect(reducer.apply(in)),
          outputType);
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
    default <OutputT> WithSortedValuesBuilder<ValueT, OutputT> reduceBy(
        ReduceFunctor<ValueT, OutputT> reducer) {
      return reduceBy(reducer, null);
    }

    <OutputT> WithSortedValuesBuilder<ValueT, OutputT> reduceBy(
        ReduceFunctor<ValueT, OutputT> reducer, @Nullable TypeDescriptor<OutputT> outputType);

    /**
     * Define a function that reduces all values related to one key into one result object. The
     * function is combinable (associative and commutative) so it can be used to compute partial
     * results before shuffle.
     *
     * @param reducer function that reduces all values into one output object
     * @return next builder to complete the setup of the {@link ReduceByKey} operator
     */
    default WindowByBuilder<ValueT> combineBy(CombinableReduceFunction<ValueT> reducer) {
      return reduceBy(ReduceFunctor.of(reducer));
    }

    default WindowByBuilder<ValueT> combineBy(
        CombinableReduceFunction<ValueT> reducer, TypeDescriptor<ValueT> outputType) {
      return reduceBy(ReduceFunctor.of(reducer), outputType);
    }

    /**
     * Syntactic sugar to enable #combineBy to take only single argument and be used in helpers like
     * #combineBy(Sums.ofLongs()).
     */
    default WindowByBuilder<ValueT> combineBy(
        ReduceByKey.CombineFunctionWithIdentity<ValueT> reduce) {
      return combineBy(reduce.identity(), reduce, reduce.valueDesc());
    }

    /**
     * Syntactic sugar to enable #combineBy to take only single argument and be used in helpers like
     * #combineBy(Sums.ofLongs()).
     *
     * @deprecated Replaced by @{link #combineBy(ReduceByKey.CombineFunctionWithIdentity)}.
     */
    @Deprecated
    default WindowByBuilder<ValueT> combineBy(
        ReduceByKey.CombineFunctionWithIdentity<ValueT> reduce, TypeDescriptor<ValueT> ignored) {
      return combineBy(reduce.identity(), reduce, reduce.valueDesc());
    }

    /**
     * Define a function that reduces all values related to one key into one result object.
     *
     * @param identity zero (identity) element, must be {@link java.io.Serializable}.
     * @param reducer function combining two values into result
     * @return next builder to complete the setup of the {@link ReduceByKey} operator
     */
    default WindowByBuilder<ValueT> combineBy(
        ValueT identity, CombinableBinaryFunction<ValueT> reducer) {
      return combineBy(identity, reducer, null);
    }

    @SuppressWarnings("unchecked")
    default WindowByBuilder<ValueT> combineBy(
        ValueT identity,
        CombinableBinaryFunction<ValueT> reducer,
        @Nullable TypeDescriptor<ValueT> valueType) {
      return (WindowByBuilder<ValueT>)
          this.<ValueT, ValueT>combineBy(
              () -> identity, (BinaryFunction) reducer, reducer, e -> e, valueType, valueType);
    }

    /**
     * Combine with full semantics defined by {@link Combine.CombineFn}.
     *
     * @param <AccT> type parameter of accumulator
     * @param accumulatorFactory factory of accumulator
     * @param accumulate accumulation function
     * @param mergeAccumulators merging function
     * @param outputFn output function
     * @return next builder to complete the setup of the {@link ReduceByKey} operator
     */
    default <AccT> WindowByBuilder<ValueT> combineBy(
        VoidFunction<AccT> accumulatorFactory,
        BinaryFunction<AccT, ValueT, AccT> accumulate,
        CombinableBinaryFunction<AccT> mergeAccumulators,
        UnaryFunction<AccT, ValueT> outputFn) {
      return combineBy(accumulatorFactory, accumulate, mergeAccumulators, outputFn, null, null);
    }

    <AccT, OutputT> WindowByBuilder<OutputT> combineBy(
        VoidFunction<AccT> accumulatorFactory,
        BinaryFunction<AccT, ValueT, AccT> accumulate,
        CombinableBinaryFunction<AccT> mergeAccumulators,
        UnaryFunction<AccT, OutputT> outputFn,
        @Nullable TypeDescriptor<AccT> accumulatorDescriptor,
        @Nullable TypeDescriptor<OutputT> outputDescriptor);
  }

  /** Builder for 'valueBy' / 'reduceBy' step. */
  public interface ValueByReduceByBuilder<InputT, ValueT> extends ReduceByBuilder<ValueT> {

    /**
     * Specifies the function to derive a value from the {@link ReduceByKey} operator's input
     * elements to get reduced by a later supplied reduce function.
     *
     * @param <T> the type of the extracted values
     * @param valueExtractor a user defined function to extract values from the processed input
     *     dataset's elements for later reduction
     * @param valueType type of extracted value
     * @return the next builder to complete the setup of the {@link ReduceByKey} operator
     */
    <T> ReduceByBuilder<T> valueBy(
        UnaryFunction<InputT, T> valueExtractor, @Nullable TypeDescriptor<T> valueType);

    default <T> ReduceByBuilder<T> valueBy(UnaryFunction<InputT, T> valueExtractor) {
      return valueBy(valueExtractor, null);
    }
  }

  /** Builder for 'withSortedValues' step. */
  public interface WithSortedValuesBuilder<ValueT, OutputT> extends WindowByBuilder<OutputT> {

    /**
     * Sort values going to `reduceBy` function by given comparator.
     *
     * @param comparator function with contract defined by {@code java.util.Comparator#compare}.
     * @return next step.builder
     */
    WindowByBuilder<OutputT> withSortedValues(BinaryFunction<ValueT, ValueT, Integer> comparator);
  }

  /** Builder for 'windowBy' step. */
  public interface WindowByBuilder<OutputT>
      extends Builders.WindowBy<TriggeredByBuilder<OutputT>>,
          OptionalMethodBuilder<WindowByBuilder<OutputT>, Builders.Output<OutputT>>,
          Builders.Output<OutputT> {

    @Override
    <W extends BoundedWindow> TriggeredByBuilder<OutputT> windowBy(WindowFn<Object, W> windowing);

    @Override
    default Builders.Output<OutputT> applyIf(
        boolean cond, UnaryFunction<WindowByBuilder<OutputT>, Builders.Output<OutputT>> fn) {
      requireNonNull(fn);
      return cond ? fn.apply(this) : this;
    }
  }

  /** Builder for 'triggeredBy' step. */
  public interface TriggeredByBuilder<OutputT>
      extends Builders.TriggeredBy<AccumulationModeBuilder<OutputT>> {

    @Override
    AccumulationModeBuilder<OutputT> triggeredBy(Trigger trigger);
  }

  /** Builder for 'accumulationMode' step. */
  public interface AccumulationModeBuilder<OutputT>
      extends Builders.AccumulationMode<WindowedOutputBuilder<OutputT>> {

    @Override
    WindowedOutputBuilder<OutputT> accumulationMode(
        WindowingStrategy.AccumulationMode accumulationMode);
  }

  /** Builder for 'windowed output' step. */
  public interface WindowedOutputBuilder<OutputT>
      extends Builders.WindowedOutput<WindowedOutputBuilder<OutputT>>, Builders.Output<OutputT> {}

  /**
   * Builder for ReduceByKey operator.
   *
   * @param <InputT> type of input
   * @param <ValueT> type of value
   * @param <OutputT> type ouf output
   */
  private static class Builder<InputT, ValueT, AccT, OutputT>
      implements OfBuilder,
          ValueByReduceByBuilder<InputT, ValueT>,
          WithSortedValuesBuilder<ValueT, OutputT>,
          WindowByBuilder<OutputT>,
          TriggeredByBuilder<OutputT>,
          AccumulationModeBuilder<OutputT>,
          WindowedOutputBuilder<OutputT>,
          Builders.Output<OutputT> {

    private final WindowBuilder<InputT> windowBuilder = new WindowBuilder<>();

    private final @Nullable String name;
    private final @Nullable ReduceFunctor<ValueT, OutputT> reducer;
    private final @Nullable VoidFunction<AccT> accumulatorFactory;
    private final @Nullable BinaryFunction<AccT, ValueT, AccT> accumulate;
    private final @Nullable CombinableBinaryFunction<AccT> mergeAccumulators;
    private final @Nullable UnaryFunction<AccT, OutputT> outputFn;
    private final @Nullable TypeDescriptor<AccT> accumulatorType;

    private PCollection<InputT> input;
    private @Nullable UnaryFunction<InputT, ValueT> valueExtractor;
    private @Nullable TypeDescriptor<ValueT> valueType;

    private @Nullable TypeDescriptor<OutputT> outputType;
    private @Nullable BinaryFunction<ValueT, ValueT, Integer> valueComparator;

    Builder(@Nullable String name) {
      this.name = name;
      this.reducer = null;
      this.accumulatorFactory = null;
      this.accumulate = null;
      this.mergeAccumulators = null;
      this.outputFn = null;
      this.accumulatorType = null;
    }

    private Builder(Builder parent, ReduceFunctor<ValueT, OutputT> reducer) {
      this.name = parent.name;
      this.reducer = requireNonNull(reducer);
      this.accumulatorFactory = null;
      this.accumulate = null;
      this.mergeAccumulators = null;
      this.outputFn = null;
      this.accumulatorType = null;
      this.input = parent.input;
      this.valueExtractor = parent.valueExtractor;
      this.valueType = parent.valueType;
      this.outputType = parent.outputType;
      this.valueComparator = parent.valueComparator;
    }

    private Builder(
        Builder parent,
        VoidFunction<AccT> accumulatorFactory,
        BinaryFunction<AccT, ValueT, AccT> accumulate,
        CombinableBinaryFunction<AccT> mergeAccumulators,
        UnaryFunction<AccT, OutputT> outputFn,
        TypeDescriptor<AccT> accumulatorType) {
      this.name = parent.name;
      this.reducer = null;
      this.accumulatorFactory = requireNonNull(accumulatorFactory);
      this.accumulate = requireNonNull(accumulate);
      this.mergeAccumulators = requireNonNull(mergeAccumulators);
      this.outputFn = requireNonNull(outputFn);
      this.accumulatorType = accumulatorType;
      this.input = parent.input;
      this.valueExtractor = parent.valueExtractor;
      this.valueType = parent.valueType;
      this.outputType = parent.outputType;
      this.valueComparator = parent.valueComparator;
    }

    @Override
    public <T> ValueByReduceByBuilder<T, T> of(PCollection<T> input) {
      @SuppressWarnings("unchecked")
      final Builder<T, T, ?, ?> cast = (Builder) this;
      cast.input = requireNonNull(input);
      return cast;
    }

    @Override
    public <T> ReduceByBuilder<T> valueBy(
        UnaryFunction<InputT, T> valueExtractor, @Nullable TypeDescriptor<T> valueType) {
      @SuppressWarnings("unchecked")
      final Builder<InputT, T, ?, ?> cast = (Builder) this;
      cast.valueExtractor = requireNonNull(valueExtractor);
      cast.valueType = valueType;
      return cast;
    }

    @Override
    public <T> WithSortedValuesBuilder<ValueT, T> reduceBy(
        ReduceFunctor<ValueT, T> reducer, @Nullable TypeDescriptor<T> outputType) {
      @SuppressWarnings("unchecked")
      final Builder<InputT, ValueT, ?, T> cast = new Builder<>(this, reducer);
      cast.outputType = outputType;
      return cast;
    }

    @Override
    public <NewAccT, T> WindowByBuilder<T> combineBy(
        VoidFunction<NewAccT> accumulatorFactory,
        BinaryFunction<NewAccT, ValueT, NewAccT> accumulate,
        CombinableBinaryFunction<NewAccT> mergeAccumulators,
        UnaryFunction<NewAccT, T> outputFn,
        @Nullable TypeDescriptor<NewAccT> accumulatorDescriptor,
        @Nullable TypeDescriptor<T> outputDescriptor) {
      Builder<InputT, ValueT, NewAccT, T> ret =
          new Builder<>(
              this,
              accumulatorFactory,
              accumulate,
              mergeAccumulators,
              outputFn,
              accumulatorDescriptor);
      ret.valueType = valueType;
      ret.outputType = outputDescriptor;
      return ret;
    }

    @Override
    public WindowByBuilder<OutputT> withSortedValues(
        BinaryFunction<ValueT, ValueT, Integer> valueComparator) {
      this.valueComparator = requireNonNull(valueComparator);
      return this;
    }

    @Override
    public <T extends BoundedWindow> TriggeredByBuilder<OutputT> windowBy(
        WindowFn<Object, T> windowFn) {
      windowBuilder.windowBy(windowFn);
      return this;
    }

    @Override
    public AccumulationModeBuilder<OutputT> triggeredBy(Trigger trigger) {
      windowBuilder.triggeredBy(trigger);
      return this;
    }

    @Override
    public WindowedOutputBuilder<OutputT> accumulationMode(
        WindowingStrategy.AccumulationMode accumulationMode) {
      windowBuilder.accumulationMode(accumulationMode);
      return this;
    }

    @Override
    public WindowedOutputBuilder<OutputT> withAllowedLateness(Duration allowedLateness) {
      windowBuilder.withAllowedLateness(allowedLateness);
      return this;
    }

    @Override
    public WindowedOutputBuilder<OutputT> withAllowedLateness(
        Duration allowedLateness, Window.ClosingBehavior closingBehavior) {
      windowBuilder.withAllowedLateness(allowedLateness, closingBehavior);
      return this;
    }

    @Override
    public WindowedOutputBuilder<OutputT> withTimestampCombiner(
        TimestampCombiner timestampCombiner) {
      windowBuilder.withTimestampCombiner(timestampCombiner);
      return this;
    }

    @Override
    public WindowedOutputBuilder<OutputT> withOnTimeBehavior(Window.OnTimeBehavior behavior) {
      windowBuilder.withOnTimeBehavior(behavior);
      return this;
    }

    @Override
    public PCollection<OutputT> output() {
      final ReduceWindow<InputT, ValueT, ?, OutputT> rw;
      if (valueExtractor == null) {
        valueExtractor = identity();
      }
      if (reducer != null) {
        rw =
            new ReduceWindow<>(
                name,
                valueExtractor,
                valueType,
                reducer,
                valueComparator,
                windowBuilder.getWindow().orElse(null),
                outputType);
      } else {
        rw =
            new ReduceWindow<>(
                name,
                valueExtractor,
                valueType,
                accumulatorFactory,
                accumulate,
                mergeAccumulators,
                outputFn,
                accumulatorType,
                valueComparator,
                windowBuilder.getWindow().orElse(null),
                outputType);
      }
      return OperatorTransform.apply(rw, PCollectionList.of(input));
    }

    @SuppressWarnings("unchecked")
    private UnaryFunction<InputT, ValueT> identity() {
      return (UnaryFunction) UnaryFunction.identity();
    }
  }

  // ReduceFunctor style
  private final @Nullable ReduceFunctor<ValueT, OutputT> reducer;
  // CombineFn style
  private final @Nullable VoidFunction<AccT> accumulatorFactory;
  private final @Nullable BinaryFunction<AccT, ValueT, AccT> accumulate;
  private final @Nullable CombinableBinaryFunction<AccT> mergeAccumulators;
  private final @Nullable UnaryFunction<AccT, OutputT> outputFn;
  private final @Nullable TypeDescriptor<AccT> accumulatorType;

  private final UnaryFunction<InputT, ValueT> valueExtractor;
  private final @Nullable BinaryFunction<ValueT, ValueT, Integer> valueComparator;
  private final @Nullable TypeDescriptor<ValueT> valueType;

  private ReduceWindow(
      @Nullable String name,
      UnaryFunction<InputT, ValueT> valueExtractor,
      @Nullable TypeDescriptor<ValueT> valueType,
      ReduceFunctor<ValueT, OutputT> reducer,
      @Nullable BinaryFunction<ValueT, ValueT, Integer> valueComparator,
      @Nullable Window<InputT> window,
      TypeDescriptor<OutputT> outputType) {

    super(name, outputType, e -> B_ZERO, TypeDescriptors.bytes(), window);
    this.reducer = requireNonNull(reducer);
    this.valueExtractor = valueExtractor;
    this.valueType = valueType;
    this.valueComparator = valueComparator;

    this.accumulatorFactory = null;
    this.accumulate = null;
    this.mergeAccumulators = null;
    this.outputFn = null;
    this.accumulatorType = null;
  }

  private ReduceWindow(
      @Nullable String name,
      UnaryFunction<InputT, ValueT> valueExtractor,
      @Nullable TypeDescriptor<ValueT> valueType,
      VoidFunction<AccT> accumulatorFactory,
      BinaryFunction<AccT, ValueT, AccT> accumulate,
      CombinableBinaryFunction<AccT> mergeAccumulators,
      UnaryFunction<AccT, OutputT> outputFn,
      @Nullable TypeDescriptor<AccT> accumulatorType,
      @Nullable BinaryFunction<ValueT, ValueT, Integer> valueComparator,
      @Nullable Window<InputT> window,
      TypeDescriptor<OutputT> outputType) {
    super(name, outputType, e -> B_ZERO, TypeDescriptors.bytes(), window);
    this.accumulatorFactory = requireNonNull(accumulatorFactory);
    this.accumulate = requireNonNull(accumulate);
    this.mergeAccumulators = requireNonNull(mergeAccumulators);
    this.outputFn = requireNonNull(outputFn);
    this.accumulatorType = accumulatorType;
    this.valueExtractor = requireNonNull(valueExtractor);
    this.valueType = valueType;
    this.valueComparator = valueComparator;
    this.reducer = null;
  }

  public boolean isCombineFnStyle() {
    return reducer == null;
  }

  public ReduceFunctor<ValueT, OutputT> getReducer() {
    return requireNonNull(reducer, "Don't call #getReducer is #isCombineFnStyle() == true");
  }

  public boolean isCombinable() {
    return isCombineFnStyle() || reducer.isCombinable();
  }

  public UnaryFunction<InputT, ValueT> getValueExtractor() {
    return valueExtractor;
  }

  public Optional<BinaryFunction<ValueT, ValueT, Integer>> getValueComparator() {
    return Optional.ofNullable(valueComparator);
  }

  @Override
  public Optional<TypeDescriptor<ValueT>> getValueType() {
    return Optional.ofNullable(valueType);
  }

  @Override
  @SuppressWarnings("unchecked")
  public PCollection<OutputT> expand(PCollectionList<InputT> inputs) {
    if (isCombinable()) {
      // sanity check
      checkState(valueComparator == null, "Sorting is not supported for combinable reducers.");
    }
    final ReduceByKey.WindowByBuilder<Byte, OutputT> windowBy;
    if (isCombineFnStyle()) {
      windowBy =
          ReduceByKey.named(getName().orElse("") + "::reduce-by")
              .of(PCollectionLists.getOnlyElement(inputs))
              .keyBy(getKeyExtractor())
              .valueBy(valueExtractor, valueType)
              .combineBy(
                  accumulatorFactory,
                  accumulate,
                  mergeAccumulators,
                  outputFn,
                  accumulatorType,
                  getOutputType().orElse(null));
    } else {
      ReduceByKey.WithSortedValuesBuilder<Byte, ValueT, OutputT> reduceBy;
      reduceBy =
          ReduceByKey.named(getName().orElse("") + "::reduce-by")
              .of(PCollectionLists.getOnlyElement(inputs))
              .keyBy(getKeyExtractor())
              .valueBy(valueExtractor, valueType)
              .reduceBy(reducer, getOutputType().orElse(null));
      if (getValueComparator().isPresent()) {
        windowBy = reduceBy.withSortedValues(valueComparator);
      } else {
        windowBy = reduceBy;
      }
    }
    return windowBy
        .applyIf(
            getWindow().isPresent(),
            builder -> {
              @SuppressWarnings("unchecked")
              final ReduceByKey.WindowByInternalBuilder<InputT, Byte, OutputT> cast =
                  (ReduceByKey.WindowByInternalBuilder) builder;
              return cast.windowBy(
                  getWindow()
                      .orElseThrow(
                          () ->
                              new IllegalStateException(
                                  "Unable to resolve windowing for ReduceWindow expansion.")));
            })
        .outputValues();
  }
}
