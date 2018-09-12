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

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.Iterables;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.Derived;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.StateComplexity;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
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
import org.apache.beam.sdk.extensions.euphoria.core.translate.Translation;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowDesc;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.WindowingStrategy;

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
 */
@Audience(Audience.Type.CLIENT)
@Derived(state = StateComplexity.CONSTANT_IF_COMBINABLE, repartitions = 1)
public class ReduceWindow<InputT, ValueT, OutputT> extends ShuffleOperator<InputT, Byte, OutputT>
    implements TypeAware.Value<ValueT>, CompositeOperator<InputT, OutputT> {

  private static final Byte B_ZERO = (byte) 0;

  /**
   * Starts building a nameless {@link ReduceWindow} operator to process the given input dataset.
   *
   * @param <InputT> the type of elements of the input dataset
   * @param input the input data set to be processed
   * @return a builder to complete the setup of the new operator
   * @see #named(String)
   * @see OfBuilder#of(Dataset)
   */
  public static <InputT> ValueByReduceByBuilder<InputT, InputT> of(Dataset<InputT> input) {
    return named("ReduceWindow").of(input);
  }

  /**
   * Starts building a named {@link ReduceWindow} operator.
   *
   * @param name a user provided name of the new operator to build
   * @return a builder to complete the setup of the new operator
   */
  public static OfBuilder named(String name) {
    return new Builder(requireNonNull(name));
  }

  /** Builder for 'of' step */
  public interface OfBuilder extends Builders.Of {

    @Override
    <InputT> ValueByReduceByBuilder<InputT, InputT> of(Dataset<InputT> input);
  }

  /** Builder for 'reduceBy' step */
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
  }

  /** Builder for 'valueBy' / 'reduceBy' step */
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

  /** Builder for 'withSortedValues' step */
  public interface WithSortedValuesBuilder<ValueT, OutputT> extends WindowByBuilder<OutputT> {

    /**
     * Sort values going to `reduceBy` function by given comparator.
     *
     * @param comparator function with contract defined by {@code java.util.Comparator#compare}.
     * @return next step builder
     */
    WindowByBuilder<OutputT> withSortedValues(BinaryFunction<ValueT, ValueT, Integer> comparator);
  }

  /** Builder for 'windowBy' step */
  public interface WindowByBuilder<OutputT>
      extends Builders.WindowBy<TriggeredByBuilder<OutputT>>,
          OptionalMethodBuilder<WindowByBuilder<OutputT>, OutputBuilder<OutputT>>,
          OutputBuilder<OutputT> {

    @Override
    <W extends BoundedWindow> TriggeredByBuilder<OutputT> windowBy(WindowFn<Object, W> windowing);

    @Override
    default OutputBuilder<OutputT> applyIf(
        boolean cond, UnaryFunction<WindowByBuilder<OutputT>, OutputBuilder<OutputT>> fn) {
      requireNonNull(fn);
      return cond ? fn.apply(this) : this;
    }
  }

  /** Builder for 'triggeredBy' step */
  public interface TriggeredByBuilder<OutputT>
      extends Builders.TriggeredBy<AccumulatorModeBuilder<OutputT>> {

    @Override
    AccumulatorModeBuilder<OutputT> triggeredBy(Trigger trigger);
  }

  /** Builder for 'accumulationMode' step */
  public interface AccumulatorModeBuilder<OutputT>
      extends Builders.AccumulatorMode<OutputBuilder<OutputT>> {

    @Override
    OutputBuilder<OutputT> accumulationMode(WindowingStrategy.AccumulationMode accumulationMode);
  }

  public interface OutputBuilder<OutputT> extends Builders.Output<OutputT> {}

  /**
   * Builder for ReduceByKey operator.
   *
   * @param <InputT> type of input
   * @param <ValueT> type of value
   * @param <OutputT> type ouf output
   */
  private static class Builder<InputT, ValueT, OutputT>
      implements OfBuilder,
          ValueByReduceByBuilder<InputT, ValueT>,
          WithSortedValuesBuilder<ValueT, OutputT>,
          WindowByBuilder<OutputT>,
          TriggeredByBuilder<OutputT>,
          AccumulatorModeBuilder<OutputT>,
          OutputBuilder<OutputT> {

    private final String name;
    private Dataset<InputT> input;
    @Nullable private UnaryFunction<InputT, ValueT> valueExtractor;
    @Nullable private TypeDescriptor<ValueT> valueType;
    private ReduceFunctor<ValueT, OutputT> reducer;
    @Nullable private TypeDescriptor<OutputT> outputType;
    @Nullable private BinaryFunction<ValueT, ValueT, Integer> valueComparator;
    @Nullable private Window<InputT> window;

    Builder(String name) {
      this.name = requireNonNull(name);
    }

    @Override
    public <T> ValueByReduceByBuilder<T, T> of(Dataset<T> input) {
      @SuppressWarnings("unchecked")
      final Builder<T, T, ?> casted = (Builder) this;
      casted.input = requireNonNull(input);
      return casted;
    }

    @Override
    public <T> ReduceByBuilder<T> valueBy(
        UnaryFunction<InputT, T> valueExtractor, @Nullable TypeDescriptor<T> valueType) {
      @SuppressWarnings("unchecked")
      final Builder<InputT, T, ?> casted = (Builder) this;
      casted.valueExtractor = requireNonNull(valueExtractor);
      casted.valueType = valueType;
      return casted;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> WithSortedValuesBuilder<ValueT, T> reduceBy(
        ReduceFunctor<ValueT, T> reducer, @Nullable TypeDescriptor<T> outputType) {
      if (valueExtractor == null) {
        // if the valueExtractor was not set in 'valueBy' step, we use untouched input element
        valueExtractor = (UnaryFunction) UnaryFunction.identity();
      }
      @SuppressWarnings("unchecked")
      final Builder<InputT, ValueT, T> casted = (Builder) this;
      casted.reducer = requireNonNull(reducer);
      casted.outputType = outputType;
      return casted;
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
      window = Window.into(requireNonNull(windowFn));
      return this;
    }

    @Override
    public AccumulatorModeBuilder<OutputT> triggeredBy(Trigger trigger) {
      window = requireNonNull(window).triggering(requireNonNull(trigger));
      return this;
    }

    @Override
    public OutputBuilder<OutputT> accumulationMode(
        WindowingStrategy.AccumulationMode accumulationMode) {
      switch (requireNonNull(accumulationMode)) {
        case DISCARDING_FIRED_PANES:
          window = requireNonNull(window).discardingFiredPanes();
          break;
        case ACCUMULATING_FIRED_PANES:
          window = requireNonNull(window).accumulatingFiredPanes();
          break;
        default:
          throw new IllegalArgumentException(
              "Unknown accumulation mode [" + accumulationMode + "]");
      }
      return this;
    }

    @Override
    public Dataset<OutputT> output(OutputHint... outputHints) {
      final ReduceWindow<InputT, ValueT, OutputT> rw =
          new ReduceWindow<>(
              name, valueExtractor, valueType, reducer, valueComparator, window, outputType);
      return Translation.apply(rw, Collections.singletonList(input));
    }
  }

  private final ReduceFunctor<ValueT, OutputT> reducer;
  private final UnaryFunction<InputT, ValueT> valueExtractor;
  @Nullable private final BinaryFunction<ValueT, ValueT, Integer> valueComparator;
  @Nullable private final TypeDescriptor<ValueT> valueType;

  private ReduceWindow(
      String name,
      UnaryFunction<InputT, ValueT> valueExtractor,
      @Nullable TypeDescriptor<ValueT> valueType,
      ReduceFunctor<ValueT, OutputT> reducer,
      @Nullable BinaryFunction<ValueT, ValueT, Integer> valueComparator,
      @Nullable Window<InputT> window,
      TypeDescriptor<OutputT> outputType) {

    super(name, outputType, e -> B_ZERO, TypeDescriptors.bytes(), window);
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

  @Override
  @SuppressWarnings("unchecked")
  public Dataset<OutputT> expand(List<Dataset<InputT>> inputs) {
    final ReduceByKey.ReduceByBuilder<Byte, ValueT> reduceBy =
        ReduceByKey.named(getName())
            .of(Iterables.getOnlyElement(inputs))
            .keyBy(e -> B_ZERO)
            .valueBy(valueExtractor, valueType);
    final ReduceByKey.WithSortedValuesBuilder<Byte, ValueT, OutputT> sortBy =
        reduceBy.reduceBy(reducer);
    if (isCombinable()) {
      // sanity check
      checkState(valueComparator == null, "Sorting is not supported for combinable reducers.");
    }
    final ReduceByKey.WindowByBuilder<Byte, OutputT> windowBy =
        getValueComparator().isPresent()
            ? sortBy.withSortedValues(getValueComparator().get())
            : sortBy;
    return windowBy
        .applyIf(
            getWindow().isPresent(),
            (builder) -> {
              final WindowDesc<InputT> desc =
                  WindowDesc.of(
                      getWindow()
                          .orElseThrow(
                              () ->
                                  new IllegalStateException(
                                      "Unable to resolve windowing for CountByKey expansion.")));
              return builder
                  .windowBy(desc.getWindowFn())
                  .triggeredBy(desc.getTrigger())
                  .accumulationMode(desc.getAccumulationMode());
            })
        .outputValues();
  }
}
