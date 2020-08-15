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

import java.util.Optional;
import java.util.stream.Stream;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.Recommended;
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
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeAwareness;
import org.apache.beam.sdk.extensions.euphoria.core.translate.OperatorTransform;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * Operator performing state-less aggregation by given reduce function. The reduction is performed
 * on all extracted values on each key-window.
 *
 * <p>If provided function is {@link CombinableReduceFunction} partial reduction is performed before
 * shuffle. If the function is not combinable all values must be first sent through the network and
 * the reduction is done afterwards on target machines.
 *
 * <p>Custom windowing can be set, otherwise values from input operator are used.
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
 *   <li>{@code [windowBy] ...............} windowing (see {@link WindowFn}), default is no
 *       windowing
 *   <li>{@code [triggeredBy] ............} defines windowing trigger, follows [windowBy] if called
 *   <li>{@code [accumulationMode] .......} windowing accumulation mode, follows [triggeredBy]
 *   <li>{@code (output | outputValues) ..} build output dataset
 * </ol>
 *
 * @param <InputT> Type of input records
 * @param <KeyT> Output type of #keyBy method
 * @param <ValueT> Output type of #valueBy method
 * @param <AccT> type of accumulator (if CombineFn used)
 * @param <OutputT> Type of output value
 */
@Audience(Audience.Type.CLIENT)
@Recommended(
    reason =
        "Is very recommended to override because of performance in "
            + "a specific area of (mostly) batch calculations where combiners "
            + "can be efficiently used in the executor-specific implementation",
    state = StateComplexity.CONSTANT_IF_COMBINABLE,
    repartitions = 1)
public class ReduceByKey<InputT, KeyT, ValueT, AccT, OutputT>
    extends ShuffleOperator<InputT, KeyT, KV<KeyT, OutputT>> implements TypeAware.Value<ValueT> {

  /**
   * A syntactic sugar interface to enable #combineBy(Sums.ofLongs()) to use Combine.CombineFn style
   * combine logic.
   *
   * @param <T> type paramter
   */
  public interface CombineFunctionWithIdentity<T> extends CombinableBinaryFunction<T> {

    /** @return identity value with respect to the combining function. */
    T identity();

    /** @return type descriptor of the value */
    default TypeDescriptor<T> valueDesc() {
      return null;
    }
  }

  /**
   * Starts building a nameless {@link ReduceByKey} operator to process the given input dataset.
   *
   * @param <InputT> the type of elements of the input dataset
   * @param input the input data set to be processed
   * @return a builder to complete the setup of the new operator
   * @see #named(String)
   * @see OfBuilder#of(PCollection)
   */
  public static <InputT> KeyByBuilder<InputT> of(PCollection<InputT> input) {
    return named(null).of(input);
  }

  /**
   * Starts building a named {@link ReduceByKey} operator.
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
    <InputT> KeyByBuilder<InputT> of(PCollection<InputT> input);
  }

  /** Builder for 'keyBy' step. */
  public interface KeyByBuilder<InputT> extends Builders.KeyBy<InputT> {

    @Override
    <T> ValueByReduceByBuilder<InputT, T, InputT> keyBy(
        UnaryFunction<InputT, T> keyExtractor, TypeDescriptor<T> keyType);

    @Override
    default <T> ValueByReduceByBuilder<InputT, T, InputT> keyBy(
        UnaryFunction<InputT, T> keyExtractor) {
      return keyBy(keyExtractor, null);
    }
  }

  /** Builder for 'reduceBy' step. */
  public interface ReduceByBuilder<KeyT, ValueT> {

    /**
     * Define a function that reduces all values related to one key into one result object. The
     * function is not combinable - i.e. partial results cannot be made up before shuffle. To get
     * better performance use {@link #combineBy} method.
     *
     * @param <OutputT> type of output element
     * @param reducer function that reduces all values into one output object
     * @return next builder to complete the setup of the {@link ReduceByKey} operator
     */
    default <OutputT> WithSortedValuesBuilder<KeyT, ValueT, OutputT> reduceBy(
        ReduceFunction<ValueT, OutputT> reducer) {
      return reduceBy(
          (Stream<ValueT> in, Collector<OutputT> ctx) -> ctx.collect(reducer.apply(in)));
    }

    default <OutputT> WithSortedValuesBuilder<KeyT, ValueT, OutputT> reduceBy(
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
    default <OutputT> WithSortedValuesBuilder<KeyT, ValueT, OutputT> reduceBy(
        ReduceFunctor<ValueT, OutputT> reducer) {
      return reduceBy(reducer, null);
    }

    <OutputT> WithSortedValuesBuilder<KeyT, ValueT, OutputT> reduceBy(
        ReduceFunctor<ValueT, OutputT> reducer, @Nullable TypeDescriptor<OutputT> outputType);

    /**
     * Define a function that reduces all values related to one key into one result object. The
     * function is combinable (associative and commutative) so it can be used to compute partial
     * results before shuffle.
     *
     * <p>Note: this might be less efficient, so you should use #combineBy(ValueT identity,
     * BinaryFunction reducer) whenever it is possible.
     *
     * @param reducer function that reduces all values into one output object
     * @return next builder to complete the setup of the {@link ReduceByKey} operator
     */
    default WindowByBuilder<KeyT, ValueT> combineBy(CombinableReduceFunction<ValueT> reducer) {
      return reduceBy(ReduceFunctor.of(reducer));
    }

    default WindowByBuilder<KeyT, ValueT> combineBy(
        CombinableReduceFunction<ValueT> reducer, TypeDescriptor<ValueT> outputType) {
      return reduceBy(ReduceFunctor.of(reducer), outputType);
    }

    /**
     * Syntactic sugar to enable #combineBy to take only single argument and be used in helpers like
     * #combineBy(Sums.ofLongs()).
     */
    default WindowByBuilder<KeyT, ValueT> combineBy(CombineFunctionWithIdentity<ValueT> reduce) {
      return combineBy(reduce.identity(), reduce, reduce.valueDesc());
    }

    /**
     * Syntactic sugar to enable #combineBy to take only single argument and be used in helpers like
     * #combineBy(Sums.ofLongs()).
     *
     * @deprecated Replaced by @{link #combineBy(CombineFunctionWithIdentity)}.
     */
    @Deprecated
    default WindowByBuilder<KeyT, ValueT> combineBy(
        CombineFunctionWithIdentity<ValueT> reduce, TypeDescriptor<ValueT> ignored) {
      return combineBy(reduce);
    }

    /**
     * Define a function that reduces all values related to one key into one result object.
     *
     * @param identity zero (identity) element, must be {@link java.io.Serializable}.
     * @param reducer function combining two values into result
     * @return next builder to complete the setup of the {@link ReduceByKey} operator
     */
    default WindowByBuilder<KeyT, ValueT> combineBy(
        ValueT identity, CombinableBinaryFunction<ValueT> reducer) {
      return combineBy(identity, reducer, null);
    }

    @SuppressWarnings("unchecked")
    default WindowByBuilder<KeyT, ValueT> combineBy(
        ValueT identity,
        CombinableBinaryFunction<ValueT> reducer,
        @Nullable TypeDescriptor<ValueT> valueType) {
      return (WindowByBuilder<KeyT, ValueT>)
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
    default <AccT> WindowByBuilder<KeyT, ValueT> combineBy(
        VoidFunction<AccT> accumulatorFactory,
        BinaryFunction<AccT, ValueT, AccT> accumulate,
        CombinableBinaryFunction<AccT> mergeAccumulators,
        UnaryFunction<AccT, ValueT> outputFn) {
      return combineBy(accumulatorFactory, accumulate, mergeAccumulators, outputFn, null, null);
    }

    <AccT, OutputT> WindowByBuilder<KeyT, OutputT> combineBy(
        VoidFunction<AccT> accumulatorFactory,
        BinaryFunction<AccT, ValueT, AccT> accumulate,
        CombinableBinaryFunction<AccT> mergeAccumulators,
        UnaryFunction<AccT, OutputT> outputFn,
        @Nullable TypeDescriptor<AccT> accumulatorDescriptor,
        @Nullable TypeDescriptor<OutputT> outputDescriptor);
  }

  /** Builder for 'valueBy' / 'reduceBy' step. */
  public interface ValueByReduceByBuilder<InputT, KeyT, ValueT>
      extends ReduceByBuilder<KeyT, ValueT> {

    /**
     * Specifies the function to derive a value from the {@link ReduceByKey} operator's input
     * elements to get reduced by a later supplied reduce function.
     *
     * @param <T> the type of the extracted values
     * @param valueExtractor a user defined function to extract values from the processed input
     *     dataset's elements for later reduction
     * @param valueType {@link TypeDescriptor} of value type {@code <V>}
     * @return the next builder to complete the setup of the {@link ReduceByKey} operator
     */
    <T> ReduceByBuilder<KeyT, T> valueBy(
        UnaryFunction<InputT, T> valueExtractor, @Nullable TypeDescriptor<T> valueType);

    default <T> ReduceByBuilder<KeyT, T> valueBy(UnaryFunction<InputT, T> valueExtractor) {
      return valueBy(valueExtractor, null);
    }
  }

  /** Builder for 'withSortedValues' step. */
  public interface WithSortedValuesBuilder<KeyT, ValueT, OutputT>
      extends WindowByBuilder<KeyT, OutputT> {

    /**
     * Sort values going to `reduceBy` function by given comparator.
     *
     * @param comparator function with contract defined by {@code java.util.Comparator#compare}.
     * @return next step builder
     */
    WindowByBuilder<KeyT, OutputT> withSortedValues(
        BinaryFunction<ValueT, ValueT, Integer> comparator);
  }

  /** Internal builder for 'windowBy' step. */
  @Internal
  public interface WindowByInternalBuilder<InputT, KeyT, OutputT> {

    /**
     * For internal use only. Set already constructed {@link Window}. This allows easier
     * construction of composite operators.
     *
     * @param window beam window
     * @return output builder
     */
    OutputBuilder<KeyT, OutputT> windowBy(Window<InputT> window);
  }

  /** Builder for 'windowBy' step. */
  public interface WindowByBuilder<KeyT, OutputT>
      extends Builders.WindowBy<TriggeredByBuilder<KeyT, OutputT>>,
          OptionalMethodBuilder<WindowByBuilder<KeyT, OutputT>, OutputBuilder<KeyT, OutputT>>,
          OutputBuilder<KeyT, OutputT> {

    @Override
    <W extends BoundedWindow> TriggeredByBuilder<KeyT, OutputT> windowBy(
        WindowFn<Object, W> windowing);

    @Override
    default OutputBuilder<KeyT, OutputT> applyIf(
        boolean cond,
        UnaryFunction<WindowByBuilder<KeyT, OutputT>, OutputBuilder<KeyT, OutputT>> fn) {
      requireNonNull(fn);
      return cond ? fn.apply(this) : this;
    }
  }

  /** Builder for 'triggeredBy' step. */
  public interface TriggeredByBuilder<KeyT, OutputT>
      extends Builders.TriggeredBy<AccumulationModeBuilder<KeyT, OutputT>> {

    @Override
    AccumulationModeBuilder<KeyT, OutputT> triggeredBy(Trigger trigger);
  }

  /** Builder for 'accumulationMode' step. */
  public interface AccumulationModeBuilder<KeyT, OutputT>
      extends Builders.AccumulationMode<WindowedOutputBuilder<KeyT, OutputT>> {

    @Override
    WindowedOutputBuilder<KeyT, OutputT> accumulationMode(
        WindowingStrategy.AccumulationMode accumulationMode);
  }

  /** Builder for 'windowed output' step. */
  public interface WindowedOutputBuilder<KeyT, OutputT>
      extends Builders.WindowedOutput<WindowedOutputBuilder<KeyT, OutputT>>,
          OutputBuilder<KeyT, OutputT> {}

  /** Builder for 'output' step. */
  public interface OutputBuilder<KeyT, OutputT>
      extends Builders.Output<KV<KeyT, OutputT>>, Builders.OutputValues<KeyT, OutputT> {}

  /**
   * Builder for ReduceByKey operator.
   *
   * @param <InputT> type of input
   * @param <KeyT> type of key
   * @param <ValueT> type of value
   * @param <AccT> type of accumulator (if using CombineFn)
   * @param <OutputT> type of output
   */
  static class Builder<InputT, KeyT, ValueT, AccT, OutputT>
      implements OfBuilder,
          KeyByBuilder<InputT>,
          ValueByReduceByBuilder<InputT, KeyT, ValueT>,
          WithSortedValuesBuilder<KeyT, ValueT, OutputT>,
          WindowByInternalBuilder<InputT, KeyT, OutputT>,
          WindowByBuilder<KeyT, OutputT>,
          TriggeredByBuilder<KeyT, OutputT>,
          AccumulationModeBuilder<KeyT, OutputT>,
          WindowedOutputBuilder<KeyT, OutputT>,
          OutputBuilder<KeyT, OutputT> {

    private final WindowBuilder<InputT> windowBuilder = new WindowBuilder<>();

    private final @Nullable String name;
    private PCollection<InputT> input;
    private UnaryFunction<InputT, KeyT> keyExtractor;
    private @Nullable TypeDescriptor<KeyT> keyType;
    private @Nullable UnaryFunction<InputT, ValueT> valueExtractor;
    private @Nullable TypeDescriptor<ValueT> valueType;
    private @Nullable TypeDescriptor<OutputT> outputType;
    private @Nullable BinaryFunction<ValueT, ValueT, Integer> valueComparator;

    // following are defined for RBK using ReduceFunctor
    private final @Nullable ReduceFunctor<ValueT, OutputT> reducer;

    // following are defined when combineFnStyle == true
    private final @Nullable VoidFunction<AccT> accumulatorFactory;
    private final @Nullable BinaryFunction<AccT, ValueT, AccT> accumulate;
    private final @Nullable CombinableBinaryFunction<AccT> mergeAccumulators;
    private final @Nullable UnaryFunction<AccT, OutputT> outputFn;
    private final @Nullable TypeDescriptor<AccT> accumulatorTypeDescriptor;

    Builder(@Nullable String name) {
      this.name = name;
      this.reducer = null;
      this.accumulatorFactory = null;
      this.accumulate = null;
      this.mergeAccumulators = null;
      this.outputFn = null;
      this.accumulatorTypeDescriptor = null;
    }

    // constructor for combine style
    private Builder(
        Builder parent,
        VoidFunction<AccT> accumulatorFactory,
        BinaryFunction<AccT, ValueT, AccT> accumulate,
        CombinableBinaryFunction<AccT> mergeAccumulators,
        UnaryFunction<AccT, OutputT> outputFn,
        @Nullable TypeDescriptor<AccT> accumulatorTypeDescriptor) {
      this.name = parent.name;
      this.input = parent.input;
      this.keyExtractor = parent.keyExtractor;
      this.keyType = parent.keyType;
      this.valueExtractor = parent.valueExtractor;
      this.valueType = parent.valueType;
      this.outputType = parent.outputType;
      this.valueComparator = parent.valueComparator;

      this.accumulatorFactory = requireNonNull(accumulatorFactory);
      this.accumulate = requireNonNull(accumulate);
      this.mergeAccumulators = requireNonNull(mergeAccumulators);
      this.outputFn = requireNonNull(outputFn);
      this.accumulatorTypeDescriptor = accumulatorTypeDescriptor;
      this.reducer = null;
    }

    // constructor for ReduceFunctor style
    private Builder(Builder parent, ReduceFunctor<ValueT, OutputT> reducer) {
      this.name = parent.name;
      this.input = parent.input;
      this.keyExtractor = parent.keyExtractor;
      this.keyType = parent.keyType;
      this.valueExtractor = parent.valueExtractor;
      this.valueType = parent.valueType;
      this.outputType = parent.outputType;
      this.valueComparator = parent.valueComparator;

      this.accumulatorFactory = null;
      this.accumulate = null;
      this.mergeAccumulators = null;
      this.outputFn = null;
      this.accumulatorTypeDescriptor = null;
      this.reducer = requireNonNull(reducer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> KeyByBuilder<T> of(PCollection<T> input) {
      @SuppressWarnings("unchecked")
      final Builder<T, ?, ?, ?, ?> cast = (Builder) this;
      cast.input = input;
      return cast;
    }

    @Override
    public <T> ValueByReduceByBuilder<InputT, T, InputT> keyBy(
        UnaryFunction<InputT, T> keyExtractor, @Nullable TypeDescriptor<T> keyType) {
      @SuppressWarnings("unchecked")
      final Builder<InputT, T, InputT, ?, ?> cast = (Builder) this;
      cast.keyExtractor = requireNonNull(keyExtractor);
      cast.keyType = keyType;
      return cast;
    }

    @Override
    public <T> ReduceByBuilder<KeyT, T> valueBy(
        UnaryFunction<InputT, T> valueExtractor, @Nullable TypeDescriptor<T> valueType) {
      @SuppressWarnings("unchecked")
      final Builder<InputT, KeyT, T, ?, ?> cast = (Builder) this;
      cast.valueExtractor = requireNonNull(valueExtractor);
      cast.valueType = valueType;
      return cast;
    }

    @Override
    public <T> WithSortedValuesBuilder<KeyT, ValueT, T> reduceBy(
        ReduceFunctor<ValueT, T> reducer, @Nullable TypeDescriptor<T> outputType) {
      @SuppressWarnings("unchecked")
      final Builder<InputT, KeyT, ValueT, ?, T> cast = new Builder(this, reducer);
      cast.outputType = outputType;
      return cast;
    }

    @Override
    public <NewAccT, T> WindowByBuilder<KeyT, T> combineBy(
        VoidFunction<NewAccT> accumulatorFactory,
        BinaryFunction<NewAccT, ValueT, NewAccT> accumulate,
        CombinableBinaryFunction<NewAccT> mergeAccumulators,
        UnaryFunction<NewAccT, T> outputFn,
        TypeDescriptor<NewAccT> accumulatorDescriptor,
        TypeDescriptor<T> outputDescriptor) {
      Builder<InputT, KeyT, ValueT, NewAccT, T> ret =
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
    public WindowByBuilder<KeyT, OutputT> withSortedValues(
        BinaryFunction<ValueT, ValueT, Integer> valueComparator) {
      this.valueComparator = requireNonNull(valueComparator);
      return this;
    }

    @Override
    public OutputBuilder<KeyT, OutputT> windowBy(Window<InputT> window) {
      windowBuilder.setWindow(window);
      return this;
    }

    @Override
    public <W extends BoundedWindow> TriggeredByBuilder<KeyT, OutputT> windowBy(
        WindowFn<Object, W> windowFn) {
      windowBuilder.windowBy(windowFn);
      return this;
    }

    @Override
    public AccumulationModeBuilder<KeyT, OutputT> triggeredBy(Trigger trigger) {
      windowBuilder.triggeredBy(trigger);
      return this;
    }

    @Override
    public WindowedOutputBuilder<KeyT, OutputT> accumulationMode(
        WindowingStrategy.AccumulationMode accumulationMode) {
      windowBuilder.accumulationMode(accumulationMode);
      return this;
    }

    @Override
    public WindowedOutputBuilder<KeyT, OutputT> withAllowedLateness(Duration allowedLateness) {
      windowBuilder.withAllowedLateness(allowedLateness);
      return this;
    }

    @Override
    public WindowedOutputBuilder<KeyT, OutputT> withAllowedLateness(
        Duration allowedLateness, Window.ClosingBehavior closingBehavior) {
      windowBuilder.withAllowedLateness(allowedLateness, closingBehavior);
      return this;
    }

    @Override
    public WindowedOutputBuilder<KeyT, OutputT> withTimestampCombiner(
        TimestampCombiner timestampCombiner) {
      windowBuilder.withTimestampCombiner(timestampCombiner);
      return this;
    }

    @Override
    public WindowedOutputBuilder<KeyT, OutputT> withOnTimeBehavior(Window.OnTimeBehavior behavior) {
      windowBuilder.withOnTimeBehavior(behavior);
      return this;
    }

    @Override
    public PCollection<KV<KeyT, OutputT>> output() {
      return OperatorTransform.apply(createOperator(), PCollectionList.of(input));
    }

    @Override
    public PCollection<OutputT> outputValues() {
      return OperatorTransform.apply(
          new OutputValues<>(name, outputType, createOperator()), PCollectionList.of(input));
    }

    private ReduceByKey<InputT, KeyT, ValueT, AccT, OutputT> createOperator() {
      if (valueExtractor == null) {
        valueExtractor = identity();
      }
      if (reducer != null) {
        return new ReduceByKey<>(
            name,
            keyExtractor,
            keyType,
            valueExtractor,
            valueType,
            reducer,
            valueComparator,
            windowBuilder.getWindow().orElse(null),
            TypeDescriptors.kvs(
                TypeAwareness.orObjects(Optional.ofNullable(keyType)),
                TypeAwareness.orObjects(Optional.ofNullable(outputType))));
      }
      return new ReduceByKey<>(
          name,
          keyExtractor,
          keyType,
          valueExtractor,
          valueType,
          accumulatorFactory,
          accumulate,
          mergeAccumulators,
          outputFn,
          accumulatorTypeDescriptor,
          valueComparator,
          windowBuilder.getWindow().orElse(null),
          TypeDescriptors.kvs(
              TypeAwareness.orObjects(Optional.ofNullable(keyType)),
              TypeAwareness.orObjects(Optional.ofNullable(outputType))));
    }

    @SuppressWarnings("unchecked")
    private UnaryFunction<InputT, ValueT> identity() {
      return (UnaryFunction) UnaryFunction.identity();
    }
  }

  // ReduceFunctor variant
  private final @Nullable ReduceFunctor<ValueT, OutputT> reducer;

  // CombineFn variant
  private final @Nullable VoidFunction<AccT> accumulatorFactory;
  private final @Nullable BinaryFunction<AccT, ValueT, AccT> accumulate;
  private final @Nullable CombinableBinaryFunction<AccT> mergeAccumulators;
  private final @Nullable UnaryFunction<AccT, OutputT> outputFn;
  private final @Nullable TypeDescriptor<AccT> accumulatorType;

  private final UnaryFunction<InputT, ValueT> valueExtractor;
  private final @Nullable BinaryFunction<ValueT, ValueT, Integer> valueComparator;
  private final @Nullable TypeDescriptor<ValueT> valueType;

  private ReduceByKey(
      @Nullable String name,
      UnaryFunction<InputT, KeyT> keyExtractor,
      @Nullable TypeDescriptor<KeyT> keyType,
      UnaryFunction<InputT, ValueT> valueExtractor,
      @Nullable TypeDescriptor<ValueT> valueType,
      ReduceFunctor<ValueT, OutputT> reducer,
      @Nullable BinaryFunction<ValueT, ValueT, Integer> valueComparator,
      @Nullable Window<InputT> window,
      @Nullable TypeDescriptor<KV<KeyT, OutputT>> outputType) {
    super(name, outputType, keyExtractor, keyType, window);
    this.reducer = requireNonNull(reducer);
    this.valueExtractor = requireNonNull(valueExtractor);
    this.valueType = valueType;
    this.valueComparator = valueComparator;

    this.accumulatorFactory = null;
    this.accumulate = null;
    this.mergeAccumulators = null;
    this.outputFn = null;
    this.accumulatorType = null;
  }

  private ReduceByKey(
      @Nullable String name,
      UnaryFunction<InputT, KeyT> keyExtractor,
      @Nullable TypeDescriptor<KeyT> keyType,
      UnaryFunction<InputT, ValueT> valueExtractor,
      @Nullable TypeDescriptor<ValueT> valueType,
      VoidFunction<AccT> accumulatorFactory,
      BinaryFunction<AccT, ValueT, AccT> accumulate,
      CombinableBinaryFunction<AccT> mergeAccumulators,
      UnaryFunction<AccT, OutputT> outputFn,
      TypeDescriptor<AccT> accumulatorType,
      @Nullable BinaryFunction<ValueT, ValueT, Integer> valueComparator,
      @Nullable Window<InputT> window,
      @Nullable TypeDescriptor<KV<KeyT, OutputT>> outputType) {
    super(name, outputType, keyExtractor, keyType, window);
    this.reducer = null;
    this.valueExtractor = requireNonNull(valueExtractor);
    this.valueType = valueType;
    this.valueComparator = valueComparator;

    this.accumulatorFactory = requireNonNull(accumulatorFactory);
    this.accumulate = requireNonNull(accumulate);
    this.mergeAccumulators = requireNonNull(mergeAccumulators);
    this.outputFn = requireNonNull(outputFn);
    this.accumulatorType = accumulatorType;
  }

  public boolean isCombineFnStyle() {
    return reducer == null;
  }

  public ReduceFunctor<ValueT, OutputT> getReducer() {
    return requireNonNull(reducer, "Don't call #getReducer when #isCombinableFnStyle() == true");
  }

  public VoidFunction<AccT> getAccumulatorFactory() {
    return requireNonNull(
        accumulatorFactory,
        "Don't vall #getAccumulatorFactory when #isCombinableFnStyle() == false");
  }

  public BinaryFunction<AccT, ValueT, AccT> getAccumulate() {
    return requireNonNull(
        accumulate, "Don't vall #getAccumulate when #isCombinableFnStyle() == false");
  }

  public CombinableBinaryFunction<AccT> getMergeAccumulators() {
    return requireNonNull(
        mergeAccumulators, "Don't vall #getMergeAccumulators when #isCombinableFnStyle() == false");
  }

  public UnaryFunction<AccT, OutputT> getOutputFn() {
    return requireNonNull(outputFn, "Don't vall #getOutputFn when #isCombinableFnStyle() == false");
  }

  public TypeDescriptor<AccT> getAccumulatorType() {
    return accumulatorType;
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
}
