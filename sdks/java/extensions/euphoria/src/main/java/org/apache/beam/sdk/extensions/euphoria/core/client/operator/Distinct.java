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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.Recommended;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.StateComplexity;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.CombinableReduceFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Builders;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.OptionalMethodBuilder;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.ShuffleOperator;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.PCollectionLists;
import org.apache.beam.sdk.extensions.euphoria.core.translate.OperatorTransform;
import org.apache.beam.sdk.extensions.euphoria.core.translate.TimestampExtractTransform;
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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * Operator outputting distinct (based on {@link Object#equals}) elements.
 *
 * <h3>Builders:</h3>
 *
 * <ol>
 *   <li>{@code [named] ..................} give name to the operator [optional]
 *   <li>{@code of .......................} input dataset
 *   <li>{@code [projected] ..............} compare objects retrieved by this {@link UnaryFunction}
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
    repartitions = 1)
public class Distinct<InputT, KeyT> extends ShuffleOperator<InputT, KeyT, InputT>
    implements CompositeOperator<InputT, InputT> {

  /**
   * Starts building a nameless {@link Distinct} operator to process the given input dataset.
   *
   * @param <InputT> the type of elements of the input dataset
   * @param input the input data set to be processed
   * @return a builder to complete the setup of the new operator
   * @see #named(String)
   * @see OfBuilder#of(PCollection)
   */
  public static <InputT> ProjectedBuilder<InputT, InputT> of(PCollection<InputT> input) {
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
    <InputT> ProjectedBuilder<InputT, InputT> of(PCollection<InputT> input);
  }

  /**
   * A policy to be applied when multiple elements exist for the same comparison key. Note that this
   * can be specified only when specifying the projection, because otherwise complete elements are
   * compared and therefore it makes no sense to select between identical elements.
   */
  public enum SelectionPolicy {

    /** Select any element (non deterministically). */
    ANY,

    /** Select element with lowest timestamp. */
    OLDEST,

    /** Select element with highest timestamp. */
    NEWEST
  }

  /** Builder for the 'projected' step. */
  public interface ProjectedBuilder<InputT, KeyT> extends WindowByBuilder<InputT, KeyT> {

    /**
     * Optionally specifies a function to transform the input elements into another type among which
     * to find the distincts.
     *
     * <p>This is, while windowing will be applied on basis of original input elements, the distinct
     * operator will be carried out on the transformed elements.
     *
     * @param <KeyT> the type of the transformed elements
     * @param transform a transform function applied to input element
     * @return the next builder to complete the setup of the {@link Distinct} operator
     */
    default <KeyT> WindowByBuilder<InputT, KeyT> projected(UnaryFunction<InputT, KeyT> transform) {
      return projected(transform, SelectionPolicy.ANY, null);
    }

    default <KeyT> WindowByBuilder<InputT, KeyT> projected(
        UnaryFunction<InputT, KeyT> transform, TypeDescriptor<KeyT> projectedType) {

      return projected(transform, SelectionPolicy.ANY, requireNonNull(projectedType));
    }

    default <KeyT> WindowByBuilder<InputT, KeyT> projected(
        UnaryFunction<InputT, KeyT> transform, SelectionPolicy policy) {

      return projected(transform, policy, null);
    }

    <KeyT> WindowByBuilder<InputT, KeyT> projected(
        UnaryFunction<InputT, KeyT> transform,
        SelectionPolicy policy,
        @Nullable TypeDescriptor<KeyT> projectedType);
  }

  /** Builder for the 'windowBy' step. */
  public interface WindowByBuilder<InputT, KeyT>
      extends Builders.WindowBy<TriggerByBuilder<InputT>>,
          OptionalMethodBuilder<WindowByBuilder<InputT, KeyT>, Builders.Output<InputT>>,
          Builders.Output<InputT> {

    @Override
    <W extends BoundedWindow> TriggerByBuilder<InputT> windowBy(WindowFn<Object, W> windowing);

    @Override
    default Builders.Output<InputT> applyIf(
        boolean cond, UnaryFunction<WindowByBuilder<InputT, KeyT>, Builders.Output<InputT>> fn) {

      return cond ? requireNonNull(fn).apply(this) : this;
    }
  }

  /** Builder for the 'triggeredBy' step. */
  public interface TriggerByBuilder<T> extends Builders.TriggeredBy<AccumulationModeBuilder<T>> {

    @Override
    AccumulationModeBuilder<T> triggeredBy(Trigger trigger);
  }

  /** Builder for the 'accumulationMode' step. */
  public interface AccumulationModeBuilder<T>
      extends Builders.AccumulationMode<WindowedOutputBuilder<T>> {

    @Override
    WindowedOutputBuilder<T> accumulationMode(WindowingStrategy.AccumulationMode accumulationMode);
  }

  /** Builder for 'windowed output' step. */
  public interface WindowedOutputBuilder<T>
      extends Builders.WindowedOutput<WindowedOutputBuilder<T>>, Builders.Output<T> {}

  private static class Builder<InputT, KeyT>
      implements OfBuilder,
          ProjectedBuilder<InputT, KeyT>,
          WindowByBuilder<InputT, KeyT>,
          TriggerByBuilder<InputT>,
          AccumulationModeBuilder<InputT>,
          WindowedOutputBuilder<InputT>,
          Builders.Output<InputT> {

    private final WindowBuilder<InputT> windowBuilder = new WindowBuilder<>();

    private final @Nullable String name;
    private PCollection<InputT> input;

    @SuppressWarnings("unchecked")
    private UnaryFunction<InputT, KeyT> transform = (UnaryFunction) e -> e;

    private SelectionPolicy policy = null;

    private @Nullable TypeDescriptor<KeyT> projectedType;

    @SuppressFBWarnings("UWF_NULL_FIELD")
    private @Nullable TypeDescriptor<InputT> outputType =
        null; // spotbugs says this field could be inlined to null

    private boolean projected = false;

    Builder(@Nullable String name) {
      this.name = name;
    }

    @Override
    public <T> ProjectedBuilder<T, T> of(PCollection<T> input) {
      @SuppressWarnings("unchecked")
      final Builder<T, T> cast = (Builder) this;
      cast.input = requireNonNull(input);
      return cast;
    }

    @Override
    public <K> WindowByBuilder<InputT, K> projected(
        UnaryFunction<InputT, K> transform,
        SelectionPolicy policy,
        @Nullable TypeDescriptor<K> projectedType) {

      @SuppressWarnings("unchecked")
      final Builder<InputT, K> cast = (Builder) this;
      cast.transform = requireNonNull(transform);
      cast.policy = requireNonNull(policy);
      cast.projectedType = projectedType;
      cast.projected = true;
      return cast;
    }

    @Override
    public <W extends BoundedWindow> TriggerByBuilder<InputT> windowBy(
        WindowFn<Object, W> windowFn) {

      windowBuilder.windowBy(windowFn);
      return this;
    }

    @Override
    public AccumulationModeBuilder<InputT> triggeredBy(Trigger trigger) {
      windowBuilder.triggeredBy(trigger);
      return this;
    }

    @Override
    public WindowedOutputBuilder<InputT> accumulationMode(
        WindowingStrategy.AccumulationMode accumulationMode) {
      windowBuilder.accumulationMode(accumulationMode);
      return this;
    }

    @Override
    public WindowedOutputBuilder<InputT> withAllowedLateness(Duration allowedLateness) {
      windowBuilder.withAllowedLateness(allowedLateness);
      return this;
    }

    @Override
    public WindowedOutputBuilder<InputT> withAllowedLateness(
        Duration allowedLateness, Window.ClosingBehavior closingBehavior) {
      windowBuilder.withAllowedLateness(allowedLateness, closingBehavior);
      return this;
    }

    @Override
    public WindowedOutputBuilder<InputT> withTimestampCombiner(
        TimestampCombiner timestampCombiner) {
      windowBuilder.withTimestampCombiner(timestampCombiner);
      return this;
    }

    @Override
    public WindowedOutputBuilder<InputT> withOnTimeBehavior(Window.OnTimeBehavior behavior) {
      windowBuilder.withOnTimeBehavior(behavior);
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public PCollection<InputT> output() {
      if (transform == null) {
        this.transform = (UnaryFunction) UnaryFunction.identity();
      }
      final Distinct<InputT, KeyT> distinct =
          new Distinct<>(
              name,
              transform,
              outputType,
              projectedType,
              windowBuilder.getWindow().orElse(null),
              policy,
              projected);
      return OperatorTransform.apply(distinct, PCollectionList.of(input));
    }
  }

  private final boolean projected;
  private final @Nullable SelectionPolicy policy;

  private Distinct(
      @Nullable String name,
      UnaryFunction<InputT, KeyT> transform,
      @Nullable TypeDescriptor<InputT> outputType,
      @Nullable TypeDescriptor<KeyT> projectedType,
      @Nullable Window<InputT> window,
      @Nullable SelectionPolicy policy,
      boolean projected) {

    super(name, outputType, transform, projectedType, window);
    this.projected = projected;
    this.policy = policy;

    Preconditions.checkState(
        !projected || policy != null,
        "Please specify selection policy when using projected distinct.");
  }

  @Override
  public PCollection<InputT> expand(PCollectionList<InputT> inputs) {
    PCollection<InputT> tmp = PCollectionLists.getOnlyElement(inputs);
    PCollection<InputT> input =
        getWindow()
            .map(
                w -> {
                  PCollection<InputT> ret = tmp.apply(w);
                  ret.setTypeDescriptor(tmp.getTypeDescriptor());
                  return ret;
                })
            .orElse(tmp);
    if (!projected) {
      PCollection<KV<InputT, Void>> distinct =
          ReduceByKey.named(getName().orElse(null))
              .of(input)
              .keyBy(e -> e, input.getTypeDescriptor())
              .valueBy(e -> null, TypeDescriptors.nulls())
              .combineBy(e -> null, TypeDescriptors.nulls())
              .output();
      return MapElements.named(getName().orElse("") + "::extract-keys")
          .of(distinct)
          .using(KV::getKey, input.getTypeDescriptor())
          .output();
    }
    UnaryFunction<PCollection<InputT>, PCollection<InputT>> transformFn = getTransformFn();
    return transformFn.apply(input);
  }

  private UnaryFunction<PCollection<InputT>, PCollection<InputT>> getTransformFn() {
    switch (policy) {
      case NEWEST:
      case OLDEST:
        String name = getName().orElse(null);
        return input -> input.apply(TimestampExtractTransform.of(name, this::reduceTimestamped));
      case ANY:
        return this::reduceSelectingAny;
      default:
        throw new IllegalArgumentException("Unknown policy " + policy);
    }
  }

  private PCollection<InputT> reduceSelectingAny(PCollection<InputT> input) {
    return ReduceByKey.named(getName().orElse(null))
        .of(input)
        .keyBy(getKeyExtractor(), getKeyType().orElse(null))
        .valueBy(e -> e, getOutputType().orElse(null))
        .combineBy(values -> nonEmpty(values.findAny()), getOutputType().orElse(null))
        .outputValues();
  }

  private PCollection<InputT> reduceTimestamped(PCollection<KV<Long, InputT>> input) {
    CombinableReduceFunction<KV<Long, InputT>> select = getReduceFn();
    PCollection<KV<Long, InputT>> outputValues =
        ReduceByKey.named(getName().orElse(null))
            .of(input)
            .keyBy(e -> getKeyExtractor().apply(e.getValue()), getKeyType().orElse(null))
            .valueBy(e -> e, requireNonNull(input.getTypeDescriptor()))
            .combineBy(select, requireNonNull(input.getTypeDescriptor()))
            .outputValues();
    return MapElements.named(getName().map(n -> n + "::unwrap").orElse(null))
        .of(outputValues)
        .using(KV::getValue, getOutputType().orElse(null))
        .output();
  }

  private CombinableReduceFunction<KV<Long, InputT>> getReduceFn() {
    return policy == SelectionPolicy.NEWEST
        ? values ->
            nonEmpty(
                values.collect(Collectors.maxBy((a, b) -> Long.compare(a.getKey(), b.getKey()))))
        : values ->
            nonEmpty(
                values.collect(Collectors.minBy((a, b) -> Long.compare(a.getKey(), b.getKey()))));
  }

  private static <T> T nonEmpty(Optional<T> in) {
    return in.orElseThrow(() -> new IllegalStateException("Empty reduce values?"));
  }
}
