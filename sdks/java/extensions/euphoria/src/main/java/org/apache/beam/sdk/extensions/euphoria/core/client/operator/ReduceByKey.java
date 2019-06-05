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
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.Recommended;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.StateComplexity;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.BinaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.CombinableReduceFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.ReduceFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.ReduceFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Builders;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.OptionalMethodBuilder;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.ShuffleOperator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.hint.OutputHint;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeAware;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeAwareness;
import org.apache.beam.sdk.extensions.euphoria.core.translate.OperatorTransform;
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
public class ReduceByKey<InputT, KeyT, ValueT, OutputT>
    extends ShuffleOperator<InputT, KeyT, KV<KeyT, OutputT>> implements TypeAware.Value<ValueT> {

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
   * @param <OutputT> type ouf output
   */
  static class Builder<InputT, KeyT, ValueT, OutputT>
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

    @Nullable private final String name;
    private PCollection<InputT> input;
    private UnaryFunction<InputT, KeyT> keyExtractor;
    @Nullable private TypeDescriptor<KeyT> keyType;
    @Nullable private UnaryFunction<InputT, ValueT> valueExtractor;
    @Nullable private TypeDescriptor<ValueT> valueType;
    private ReduceFunctor<ValueT, OutputT> reducer;
    @Nullable private TypeDescriptor<OutputT> outputType;
    @Nullable private BinaryFunction<ValueT, ValueT, Integer> valueComparator;

    Builder(@Nullable String name) {
      this.name = name;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> KeyByBuilder<T> of(PCollection<T> input) {
      @SuppressWarnings("unchecked")
      final Builder<T, ?, ?, ?> cast = (Builder) this;
      cast.input = input;
      return cast;
    }

    @Override
    public <T> ValueByReduceByBuilder<InputT, T, InputT> keyBy(
        UnaryFunction<InputT, T> keyExtractor, @Nullable TypeDescriptor<T> keyType) {
      @SuppressWarnings("unchecked")
      final Builder<InputT, T, InputT, ?> cast = (Builder) this;
      cast.keyExtractor = requireNonNull(keyExtractor);
      cast.keyType = keyType;
      return cast;
    }

    @Override
    public <T> ReduceByBuilder<KeyT, T> valueBy(
        UnaryFunction<InputT, T> valueExtractor, @Nullable TypeDescriptor<T> valueType) {
      @SuppressWarnings("unchecked")
      final Builder<InputT, KeyT, T, ?> cast = (Builder) this;
      cast.valueExtractor = requireNonNull(valueExtractor);
      cast.valueType = valueType;
      return cast;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> WithSortedValuesBuilder<KeyT, ValueT, T> reduceBy(
        ReduceFunctor<ValueT, T> reducer, @Nullable TypeDescriptor<T> outputType) {
      if (valueExtractor == null) {
        // if the valueExtractor was not set in 'valueBy' step, we use untouched input element
        valueExtractor = (UnaryFunction) UnaryFunction.identity();
      }
      @SuppressWarnings("unchecked")
      final Builder<InputT, KeyT, ValueT, T> cast = (Builder) this;
      cast.reducer = requireNonNull(reducer);
      cast.outputType = outputType;
      return cast;
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
    public PCollection<KV<KeyT, OutputT>> output(OutputHint... outputHints) {
      return OperatorTransform.apply(createOperator(), PCollectionList.of(input));
    }

    @Override
    public PCollection<OutputT> outputValues(OutputHint... outputHints) {
      return OperatorTransform.apply(
          new OutputValues<>(name, outputType, createOperator()), PCollectionList.of(input));
    }

    private ReduceByKey<InputT, KeyT, ValueT, OutputT> createOperator() {
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
  }

  private final ReduceFunctor<ValueT, OutputT> reducer;
  private final UnaryFunction<InputT, ValueT> valueExtractor;
  @Nullable private final BinaryFunction<ValueT, ValueT, Integer> valueComparator;
  @Nullable private final TypeDescriptor<ValueT> valueType;

  private ReduceByKey(
      @Nullable String name,
      UnaryFunction<InputT, KeyT> keyExtractor,
      @Nullable TypeDescriptor<KeyT> keyType,
      UnaryFunction<InputT, ValueT> valueExtractor,
      @Nullable TypeDescriptor<ValueT> valueType,
      ReduceFunctor<ValueT, OutputT> reducer,
      @Nullable BinaryFunction<ValueT, ValueT, Integer> valueComparator,
      @Nullable Window<InputT> window,
      TypeDescriptor<KV<KeyT, OutputT>> outputType) {
    super(name, outputType, keyExtractor, keyType, window);
    this.reducer = reducer;
    this.valueExtractor = valueExtractor;
    this.valueType = valueType;
    this.valueComparator = valueComparator;
  }

  public ReduceFunctor<ValueT, OutputT> getReducer() {
    return reducer;
  }

  public boolean isCombinable() {
    return reducer.isCombinable();
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
