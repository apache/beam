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
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.Derived;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.StateComplexity;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Builders;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.OptionalMethodBuilder;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.ShuffleOperator;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeAware;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeUtils;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.PCollectionLists;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Triple;
import org.apache.beam.sdk.extensions.euphoria.core.translate.OperatorTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * Emits top element for defined keys and windows. The elements are compared by comparable objects
 * extracted by user defined function applied on input elements.
 *
 * <p>Custom windowing can be set, otherwise values from input operator are used.
 *
 * <p>Example:
 *
 * <pre>{@code
 * TopPerKey.of(elements)
 *      .keyBy(e -> (byte) 0)
 *      .valueBy(e -> e)
 *      .scoreBy(KV::getValue)
 *      .output();
 * }</pre>
 *
 * <p>The examples above finds global maximum of all elements.
 *
 * <h3>Builders:</h3>
 *
 * <ol>
 *   <li>{@code [named] ..................} give name to the operator [optional]
 *   <li>{@code of .......................} input dataset
 *   <li>{@code keyBy ....................} key extractor function
 *   <li>{@code valueBy ..................} value extractor function
 *   <li>{@code scoreBy ..................} {@link UnaryFunction} transforming input elements to
 *       {@link Comparable} scores
 *   <li>{@code [windowBy] ...............} windowing (see {@link WindowFn}), default is no
 *       windowing
 *   <li>{@code [triggeredBy] ............} defines windowing trigger, follows [windowBy] if called
 *   <li>{@code [accumulationMode] .......} windowing accumulation mode, follows [triggeredBy]
 *   <li>{@code output ...................} build output dataset
 * </ol>
 */
@Audience(Audience.Type.CLIENT)
@Derived(state = StateComplexity.CONSTANT, repartitions = 1)
public class TopPerKey<InputT, KeyT, ValueT, ScoreT extends Comparable<ScoreT>>
    extends ShuffleOperator<InputT, KeyT, Triple<KeyT, ValueT, ScoreT>>
    implements TypeAware.Value<ValueT>, CompositeOperator<InputT, Triple<KeyT, ValueT, ScoreT>> {

  /**
   * Starts building a nameless {@link TopPerKey} operator to process the given input dataset.
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
   * Starts building a named {@link TopPerKey} operator.
   *
   * @param name a user provided name of the new operator to build
   * @return a builder to complete the setup of the new operator
   */
  public static OfBuilder named(@Nullable String name) {
    return new Builder<>(name);
  }

  /** Builder for 'of' step. */
  public interface OfBuilder extends Builders.Of {

    @Override
    <InputT> KeyByBuilder<InputT> of(PCollection<InputT> input);
  }

  /** Builder for 'keyBy' step. */
  public interface KeyByBuilder<InputT> extends Builders.KeyBy<InputT> {

    @Override
    <T> ValueByBuilder<InputT, T> keyBy(
        UnaryFunction<InputT, T> keyExtractor, TypeDescriptor<T> keyType);

    @Override
    default <T> ValueByBuilder<InputT, T> keyBy(UnaryFunction<InputT, T> keyExtractor) {
      return keyBy(keyExtractor, null);
    }
  }

  /** Builder for 'valueBy' step. */
  public interface ValueByBuilder<InputT, KeyT> {

    default <ValueT> ScoreBy<InputT, KeyT, ValueT> valueBy(
        UnaryFunction<InputT, ValueT> valueExtractor) {
      return valueBy(valueExtractor, null);
    }

    <ValueT> ScoreBy<InputT, KeyT, ValueT> valueBy(
        UnaryFunction<InputT, ValueT> valueExtractor, @Nullable TypeDescriptor<ValueT> valueType);
  }

  /** Builder for 'scoreBy' step. */
  public interface ScoreBy<InputT, KeyT, ValueT> {

    default <ScoreT extends Comparable<ScoreT>> WindowByBuilder<KeyT, ValueT, ScoreT> scoreBy(
        UnaryFunction<InputT, ScoreT> scoreFn) {
      return scoreBy(scoreFn, null);
    }

    <ScoreT extends Comparable<ScoreT>> WindowByBuilder<KeyT, ValueT, ScoreT> scoreBy(
        UnaryFunction<InputT, ScoreT> scoreFn, @Nullable TypeDescriptor<ScoreT> scoreType);
  }

  /** Builder for 'windowBy' step. */
  public interface WindowByBuilder<KeyT, ValueT, ScoreT extends Comparable<ScoreT>>
      extends Builders.WindowBy<TriggeredByBuilder<KeyT, ValueT, ScoreT>>,
          OptionalMethodBuilder<
              WindowByBuilder<KeyT, ValueT, ScoreT>, OutputBuilder<KeyT, ValueT, ScoreT>>,
          OutputBuilder<KeyT, ValueT, ScoreT> {

    @Override
    <W extends BoundedWindow> TriggeredByBuilder<KeyT, ValueT, ScoreT> windowBy(
        WindowFn<Object, W> windowing);

    @Override
    default OutputBuilder<KeyT, ValueT, ScoreT> applyIf(
        boolean cond,
        UnaryFunction<WindowByBuilder<KeyT, ValueT, ScoreT>, OutputBuilder<KeyT, ValueT, ScoreT>>
            fn) {
      return cond ? requireNonNull(fn).apply(this) : this;
    }
  }

  /** Builder for 'triggeredBy' step. */
  public interface TriggeredByBuilder<KeyT, ValueT, ScoreT extends Comparable<ScoreT>>
      extends Builders.TriggeredBy<AccumulationModeBuilder<KeyT, ValueT, ScoreT>> {

    @Override
    AccumulationModeBuilder<KeyT, ValueT, ScoreT> triggeredBy(Trigger trigger);
  }

  /** Builder for 'accumulationMode' step. */
  public interface AccumulationModeBuilder<KeyT, ValueT, ScoreT extends Comparable<ScoreT>>
      extends Builders.AccumulationMode<WindowedOutputBuilder<KeyT, ValueT, ScoreT>> {

    @Override
    WindowedOutputBuilder<KeyT, ValueT, ScoreT> accumulationMode(
        WindowingStrategy.AccumulationMode accumulationMode);
  }

  /** Builder for 'windowed output' step. */
  public interface WindowedOutputBuilder<KeyT, ValueT, ScoreT extends Comparable<ScoreT>>
      extends Builders.WindowedOutput<WindowedOutputBuilder<KeyT, ValueT, ScoreT>>,
          OutputBuilder<KeyT, ValueT, ScoreT> {}

  /** Builder for 'output' step. */
  public interface OutputBuilder<KeyT, ValueT, ScoreT extends Comparable<ScoreT>>
      extends Builders.Output<Triple<KeyT, ValueT, ScoreT>> {}

  /**
   * Builder for TopPerKey operator.
   *
   * @param <InputT> type of input
   * @param <KeyT> type of key
   */
  private static class Builder<InputT, KeyT, ValueT, ScoreT extends Comparable<ScoreT>>
      implements OfBuilder,
          KeyByBuilder<InputT>,
          ValueByBuilder<InputT, KeyT>,
          ScoreBy<InputT, KeyT, ValueT>,
          WindowByBuilder<KeyT, ValueT, ScoreT>,
          TriggeredByBuilder<KeyT, ValueT, ScoreT>,
          AccumulationModeBuilder<KeyT, ValueT, ScoreT>,
          WindowedOutputBuilder<KeyT, ValueT, ScoreT>,
          OutputBuilder<KeyT, ValueT, ScoreT> {

    private final WindowBuilder<InputT> windowBuilder = new WindowBuilder<>();

    private final @Nullable String name;
    private PCollection<InputT> input;
    private UnaryFunction<InputT, KeyT> keyExtractor;
    private @Nullable TypeDescriptor<KeyT> keyType;
    private UnaryFunction<InputT, ValueT> valueExtractor;
    private @Nullable TypeDescriptor<ValueT> valueType;
    private UnaryFunction<InputT, ScoreT> scoreExtractor;
    private @Nullable TypeDescriptor<ScoreT> scoreType;

    Builder(@Nullable String name) {
      this.name = name;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> KeyByBuilder<T> of(PCollection<T> input) {
      this.input = (PCollection<InputT>) requireNonNull(input);
      return (KeyByBuilder) this;
    }

    @Override
    public <T> ValueByBuilder<InputT, T> keyBy(
        UnaryFunction<InputT, T> keyExtractor, @Nullable TypeDescriptor<T> keyType) {
      @SuppressWarnings("unchecked")
      final Builder<InputT, T, ?, ?> cast = (Builder) this;
      cast.keyExtractor = requireNonNull(keyExtractor);
      cast.keyType = keyType;
      return cast;
    }

    @Override
    public <T> ScoreBy<InputT, KeyT, T> valueBy(
        UnaryFunction<InputT, T> valueExtractor, @Nullable TypeDescriptor<T> valueType) {
      @SuppressWarnings("unchecked")
      final Builder<InputT, KeyT, T, ?> cast = (Builder) this;
      cast.valueExtractor = requireNonNull(valueExtractor);
      cast.valueType = valueType;
      return cast;
    }

    @Override
    public <T extends Comparable<T>> WindowByBuilder<KeyT, ValueT, T> scoreBy(
        UnaryFunction<InputT, T> scoreExtractor, @Nullable TypeDescriptor<T> scoreType) {
      @SuppressWarnings("unchecked")
      final Builder<InputT, KeyT, ValueT, T> cast = (Builder) this;
      cast.scoreExtractor = requireNonNull(scoreExtractor);
      cast.scoreType = scoreType;
      return cast;
    }

    @Override
    public <W extends BoundedWindow> TriggeredByBuilder<KeyT, ValueT, ScoreT> windowBy(
        WindowFn<Object, W> windowFn) {
      windowBuilder.windowBy(windowFn);
      return this;
    }

    @Override
    public AccumulationModeBuilder<KeyT, ValueT, ScoreT> triggeredBy(Trigger trigger) {
      windowBuilder.triggeredBy(trigger);
      return this;
    }

    @Override
    public WindowedOutputBuilder<KeyT, ValueT, ScoreT> accumulationMode(
        WindowingStrategy.AccumulationMode accumulationMode) {
      windowBuilder.accumulationMode(accumulationMode);
      return this;
    }

    @Override
    public WindowedOutputBuilder<KeyT, ValueT, ScoreT> withAllowedLateness(
        Duration allowedLateness) {
      windowBuilder.withAllowedLateness(allowedLateness);
      return this;
    }

    @Override
    public WindowedOutputBuilder<KeyT, ValueT, ScoreT> withAllowedLateness(
        Duration allowedLateness, Window.ClosingBehavior closingBehavior) {
      windowBuilder.withAllowedLateness(allowedLateness, closingBehavior);
      return this;
    }

    @Override
    public WindowedOutputBuilder<KeyT, ValueT, ScoreT> withTimestampCombiner(
        TimestampCombiner timestampCombiner) {
      windowBuilder.withTimestampCombiner(timestampCombiner);
      return this;
    }

    @Override
    public WindowedOutputBuilder<KeyT, ValueT, ScoreT> withOnTimeBehavior(
        Window.OnTimeBehavior behavior) {
      windowBuilder.withOnTimeBehavior(behavior);
      return this;
    }

    @Override
    public PCollection<Triple<KeyT, ValueT, ScoreT>> output() {
      final TopPerKey<InputT, KeyT, ValueT, ScoreT> sbk =
          new TopPerKey<>(
              name,
              keyExtractor,
              keyType,
              valueExtractor,
              valueType,
              scoreExtractor,
              scoreType,
              windowBuilder.getWindow().orElse(null),
              TypeUtils.triplets(keyType, valueType, scoreType));
      return OperatorTransform.apply(sbk, PCollectionList.of(input));
    }
  }

  private UnaryFunction<InputT, ValueT> valueExtractor;
  private @Nullable TypeDescriptor<ValueT> valueType;
  private UnaryFunction<InputT, ScoreT> scoreExtractor;
  private @Nullable TypeDescriptor<ScoreT> scoreType;

  private TopPerKey(
      @Nullable String name,
      UnaryFunction<InputT, KeyT> keyExtractor,
      @Nullable TypeDescriptor<KeyT> keyType,
      UnaryFunction<InputT, ValueT> valueExtractor,
      @Nullable TypeDescriptor<ValueT> valueType,
      UnaryFunction<InputT, ScoreT> scoreExtractor,
      @Nullable TypeDescriptor<ScoreT> scoreType,
      @Nullable Window<InputT> window,
      @Nullable TypeDescriptor<Triple<KeyT, ValueT, ScoreT>> outputType) {
    super(name, outputType, keyExtractor, keyType, window);

    this.valueExtractor = valueExtractor;
    this.valueType = valueType;
    this.scoreExtractor = scoreExtractor;
    this.scoreType = scoreType;
  }

  public UnaryFunction<InputT, ValueT> getValueExtractor() {
    return valueExtractor;
  }

  @Override
  public Optional<TypeDescriptor<ValueT>> getValueType() {
    return Optional.ofNullable(valueType);
  }

  public UnaryFunction<InputT, ScoreT> getScoreExtractor() {
    return scoreExtractor;
  }

  public Optional<TypeDescriptor<ScoreT>> getScoreType() {
    return Optional.ofNullable(scoreType);
  }

  @Override
  public PCollection<Triple<KeyT, ValueT, ScoreT>> expand(PCollectionList<InputT> inputs) {
    final PCollection<Triple<KeyT, ValueT, ScoreT>> extracted =
        MapElements.named("extract-key-value-score")
            .of(PCollectionLists.getOnlyElement(inputs))
            .using(
                elem ->
                    Triple.of(
                        getKeyExtractor().apply(elem),
                        getValueExtractor().apply(elem),
                        getScoreExtractor().apply(elem)),
                getOutputType().orElse(null))
            .output();
    return ReduceByKey.named("combine-by-key")
        .of(extracted)
        .keyBy(Triple::getFirst, getKeyType().orElse(null))
        .combineBy(
            (Stream<Triple<KeyT, ValueT, ScoreT>> triplets) ->
                triplets
                    .reduce((a, b) -> a.getThird().compareTo(b.getThird()) > 0 ? a : b)
                    .orElseThrow(IllegalStateException::new))
        .applyIf(
            getWindow().isPresent(),
            builder -> {
              @SuppressWarnings("unchecked")
              final ReduceByKey.WindowByInternalBuilder<InputT, KeyT, Triple<KeyT, ValueT, ScoreT>>
                  cast = (ReduceByKey.WindowByInternalBuilder) builder;
              return cast.windowBy(
                  getWindow()
                      .orElseThrow(
                          () ->
                              new IllegalStateException(
                                  "Unable to resolve windowing for TopPerKey expansion.")));
            })
        .outputValues();
  }
}
