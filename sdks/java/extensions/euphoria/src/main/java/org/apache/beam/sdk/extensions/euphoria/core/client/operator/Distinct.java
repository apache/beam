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

import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.Recommended;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.StateComplexity;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Builders;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.OptionalMethodBuilder;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.ShuffleOperator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.hint.OutputHint;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.PCollectionLists;
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
 * Operator outputting distinct (based on {@link Object#equals}) elements.
 *
 * <h3>Builders:</h3>
 *
 * <ol>
 *   <li>{@code [named] ..................} give name to the operator [optional]
 *   <li>{@code of .......................} input dataset
 *   <li>{@code [mapped] .................} compare objects retrieved by this {@link UnaryFunction}
 *       instead of raw input elements
 *   <li>{@code [windowBy] ...............} windowing (see {@link WindowFn}), default is no
 *       windowing
 *   <li>{@code [triggeredBy] ............} defines windowing trigger, follows [windowBy] if called
 *   <li>{@code [accumulationMode] .......} windowing accumulation mode, follows [triggeredBy]
 *   <li>{@code output ...................} build output dataset
 * </ol>
 */
@Audience(Audience.Type.CLIENT)
@Recommended(
  reason =
      "Might be useful to override the default "
          + "implementation because of performance reasons"
          + "(e.g. using bloom filters), which might reduce the space complexity",
  state = StateComplexity.CONSTANT,
  repartitions = 1
)
public class Distinct<InputT, OutputT> extends ShuffleOperator<InputT, OutputT, OutputT>
    implements CompositeOperator<InputT, OutputT> {

  /**
   * Starts building a nameless {@link Distinct} operator to process the given input dataset.
   *
   * @param <InputT> the type of elements of the input dataset
   * @param input the input data set to be processed
   * @return a builder to complete the setup of the new operator
   * @see #named(String)
   * @see OfBuilder#of(PCollection)
   */
  public static <InputT> MappedBuilder<InputT, InputT> of(PCollection<InputT> input) {
    return named(null).of(input);
  }

  /**
   * Starts building a named {@link Distinct} operator.
   *
   * @param name a user provided name of the new operator to build
   * @return a builder to complete the setup of the new operator
   */
  public static OfBuilder named(@Nullable String name) {
    return new Builder(name);
  }

  /** Builder for the 'of' step. */
  public interface OfBuilder extends Builders.Of {

    @Override
    <InputT> MappedBuilder<InputT, InputT> of(PCollection<InputT> input);
  }

  /** Builder for the 'mapped' step. */
  public interface MappedBuilder<InputT, OutputT> extends WindowByBuilder<OutputT> {

    /**
     * Optionally specifies a function to transform the input elements into another type among which
     * to find the distincts.
     *
     * <p>This is, while windowing will be applied on basis of original input elements, the distinct
     * operator will be carried out on the transformed elements.
     *
     * @param <T> the type of the transformed elements
     * @param mapper a transform function applied to input element
     * @return the next builder to complete the setup of the {@link Distinct} operator
     */
    default <T> WindowByBuilder<T> mapped(UnaryFunction<InputT, T> mapper) {
      return mapped(mapper, null);
    }

    <T> WindowByBuilder<T> mapped(
        UnaryFunction<InputT, T> mapper, @Nullable TypeDescriptor<T> outputType);
  }

  /** Builder for the 'windowBy' step. */
  public interface WindowByBuilder<OutputT>
      extends Builders.WindowBy<TriggerByBuilder<OutputT>>,
          OptionalMethodBuilder<WindowByBuilder<OutputT>, OutputBuilder<OutputT>>,
          OutputBuilder<OutputT> {

    @Override
    <T extends BoundedWindow> TriggerByBuilder<OutputT> windowBy(WindowFn<Object, T> windowing);

    @Override
    default OutputBuilder<OutputT> applyIf(
        boolean cond, UnaryFunction<WindowByBuilder<OutputT>, OutputBuilder<OutputT>> fn) {
      return cond ? requireNonNull(fn).apply(this) : this;
    }
  }

  /** Builder for the 'triggeredBy' step. */
  public interface TriggerByBuilder<OutputT>
      extends Builders.TriggeredBy<AccumulationModeBuilder<OutputT>> {

    @Override
    AccumulationModeBuilder<OutputT> triggeredBy(Trigger trigger);
  }

  /** Builder for the 'accumulationMode' step. */
  public interface AccumulationModeBuilder<OutputT>
      extends Builders.AccumulationMode<WindowedOutputBuilder<OutputT>> {

    @Override
    WindowedOutputBuilder<OutputT> accumulationMode(
        WindowingStrategy.AccumulationMode accumulationMode);
  }

  /** Builder for 'windowed output' step. */
  public interface WindowedOutputBuilder<OutputT>
      extends Builders.WindowedOutput<WindowedOutputBuilder<OutputT>>, OutputBuilder<OutputT> {}

  /** Builder for the 'output' step. */
  public interface OutputBuilder<OutputT> extends Builders.Output<OutputT> {}

  private static class Builder<InputT, OutputT>
      implements OfBuilder,
          MappedBuilder<InputT, OutputT>,
          WindowByBuilder<OutputT>,
          TriggerByBuilder<OutputT>,
          AccumulationModeBuilder<OutputT>,
          WindowedOutputBuilder<OutputT>,
          OutputBuilder<OutputT> {

    private final WindowBuilder<InputT> windowBuilder = new WindowBuilder<>();

    @Nullable private final String name;
    private PCollection<InputT> input;
    @Nullable private UnaryFunction<InputT, OutputT> mapper;
    @Nullable private TypeDescriptor<OutputT> outputType;

    Builder(@Nullable String name) {
      this.name = name;
    }

    @Override
    public <T> MappedBuilder<T, T> of(PCollection<T> input) {
      @SuppressWarnings("unchecked")
      final Builder<T, T> casted = (Builder) this;
      casted.input = requireNonNull(input);
      return casted;
    }

    @Override
    public <T> WindowByBuilder<T> mapped(
        UnaryFunction<InputT, T> mapper, @Nullable TypeDescriptor<T> outputType) {
      @SuppressWarnings("unchecked")
      final Builder<InputT, T> casted = (Builder) this;
      casted.mapper = requireNonNull(mapper);
      casted.outputType = outputType;
      return casted;
    }

    @Override
    public <T extends BoundedWindow> TriggerByBuilder<OutputT> windowBy(
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
    @SuppressWarnings("unchecked")
    public PCollection<OutputT> output(OutputHint... outputHints) {
      if (mapper == null) {
        this.mapper = (UnaryFunction) UnaryFunction.identity();
      }
      final Distinct<InputT, OutputT> distinct =
          new Distinct<>(name, mapper, outputType, windowBuilder.getWindow().orElse(null));
      return OperatorTransform.apply(distinct, PCollectionList.of(input));
    }
  }

  private Distinct(
      @Nullable String name,
      UnaryFunction<InputT, OutputT> mapper,
      @Nullable TypeDescriptor<OutputT> outputType,
      @Nullable Window<InputT> window) {
    super(name, outputType, mapper, outputType, window);
  }

  @Override
  public PCollection<OutputT> expand(PCollectionList<InputT> inputs) {
    final PCollection<KV<OutputT, Void>> distinct =
        ReduceByKey.named(getName().orElse(null))
            .of(PCollectionLists.getOnlyElement(inputs))
            .keyBy(getKeyExtractor())
            .valueBy(e -> null, TypeDescriptors.nulls())
            .combineBy(e -> null)
            .applyIf(
                getWindow().isPresent(),
                builder -> {
                  @SuppressWarnings("unchecked")
                  final ReduceByKey.WindowByInternalBuilder<InputT, OutputT, Void> casted =
                      (ReduceByKey.WindowByInternalBuilder) builder;
                  return casted.windowBy(
                      getWindow()
                          .orElseThrow(
                              () ->
                                  new IllegalStateException(
                                      "Unable to resolve windowing for Distinct expansion.")));
                })
            .output();
    return MapElements.named(getName().orElse("") + "::extract-keys")
        .of(distinct)
        .using(KV::getKey, getKeyType().orElse(null))
        .output();
  }
}
