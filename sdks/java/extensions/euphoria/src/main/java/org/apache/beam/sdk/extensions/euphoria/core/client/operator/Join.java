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

import java.util.Arrays;
import java.util.Optional;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.Recommended;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.StateComplexity;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.BinaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Builders;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.OptionalMethodBuilder;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.ShuffleOperator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.hint.OutputHint;
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
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * Inner join of two datasets by given key producing single new dataset.
 *
 * <p>When joining two streams, the join has to specify windowing which groups elements from streams
 * into {@link Window}s. The join operation is performed within same windows produced on left and
 * right side of input {@link PCollection}s.
 *
 * <h3>Builders:</h3>
 *
 * <ol>
 *   <li>{@code [named] ..................} give name to the operator [optional]
 *   <li>{@code of .......................} left and right input dataset
 *   <li>{@code by .......................} {@link UnaryFunction}s transforming left and right
 *       elements into keys
 *   <li>{@code using ....................} {@link BinaryFunctor} receiving left and right element
 *       from joined window
 *   <li>{@code [windowBy] ...............} windowing (see {@link WindowFn}), default is no
 *       windowing
 *   <li>{@code [triggeredBy] ............} defines windowing trigger, follows [windowBy] if called
 *   <li>{@code [accumulationMode] .......} windowing accumulation mode, follows [triggeredBy]
 *   <li>{@code (output | outputValues) ..} build output dataset
 * </ol>
 */
@Audience(Audience.Type.CLIENT)
@Recommended(
    reason =
        "Might be useful to override because of performance reasons in a "
            + "specific join types (e.g. sort join), which might reduce the space "
            + "complexity",
    state = StateComplexity.LINEAR,
    repartitions = 1)
public class Join<LeftT, RightT, KeyT, OutputT>
    extends ShuffleOperator<Object, KeyT, KV<KeyT, OutputT>> {

  public static <LeftT, RightT> ByBuilder<LeftT, RightT> of(
      PCollection<LeftT> left, PCollection<RightT> right) {
    return named(null).of(left, right);
  }

  /**
   * Name of join operator.
   *
   * @param name of operator
   * @return OfBuilder
   */
  public static OfBuilder named(@Nullable String name) {
    return new Builder<>(name, Type.INNER);
  }

  /** Type of join. */
  public enum Type {
    INNER,
    LEFT,
    RIGHT,
    FULL
  }

  /** Builder for the 'of' step. */
  public interface OfBuilder {

    <LeftT, RightT> ByBuilder<LeftT, RightT> of(PCollection<LeftT> left, PCollection<RightT> right);
  }

  /** Builder for the 'by' step. */
  public interface ByBuilder<LeftT, RightT> {

    <K> UsingBuilder<LeftT, RightT, K> by(
        UnaryFunction<LeftT, K> leftKeyExtractor,
        UnaryFunction<RightT, K> rightKeyExtractor,
        @Nullable TypeDescriptor<K> keyType);

    default <T> UsingBuilder<LeftT, RightT, T> by(
        UnaryFunction<LeftT, T> leftKeyExtractor, UnaryFunction<RightT, T> rightKeyExtractor) {
      return by(leftKeyExtractor, rightKeyExtractor, null);
    }
  }

  /** Builder for the 'using' step. */
  public interface UsingBuilder<LeftT, RightT, KeyT> {

    <OutputT> WindowByBuilder<KeyT, OutputT> using(
        BinaryFunctor<LeftT, RightT, OutputT> joinFunc,
        @Nullable TypeDescriptor<OutputT> outputTypeDescriptor);

    default <OutputT> WindowByBuilder<KeyT, OutputT> using(
        BinaryFunctor<LeftT, RightT, OutputT> joinFunc) {
      return using(joinFunc, null);
    }
  }

  /** Builder for the 'windowBy' step. */
  public interface WindowByBuilder<KeyT, OutputT>
      extends OptionalMethodBuilder<WindowByBuilder<KeyT, OutputT>, OutputBuilder<KeyT, OutputT>>,
          Builders.WindowBy<TriggeredByBuilder<KeyT, OutputT>>,
          OutputBuilder<KeyT, OutputT> {

    @Override
    default OutputBuilder<KeyT, OutputT> applyIf(
        boolean cond,
        UnaryFunction<WindowByBuilder<KeyT, OutputT>, OutputBuilder<KeyT, OutputT>> fn) {
      return cond ? requireNonNull(fn).apply(this) : this;
    }
  }

  /** Builder for the 'triggeredBy' step. */
  public interface TriggeredByBuilder<KeyT, OutputT>
      extends Builders.TriggeredBy<AccumulationModeBuilder<KeyT, OutputT>> {}

  /** Builder for the 'accumulatorMode' step. */
  public interface AccumulationModeBuilder<KeyT, OutputT>
      extends Builders.AccumulationMode<WindowedOutputBuilder<KeyT, OutputT>> {}

  /** Builder for 'windowed output' step. */
  public interface WindowedOutputBuilder<KeyT, OutputT>
      extends Builders.WindowedOutput<WindowedOutputBuilder<KeyT, OutputT>>,
          OutputBuilder<KeyT, OutputT> {}

  /**
   * Last builder in a chain. It concludes this operators creation by calling {@link
   * #output(OutputHint...)}.
   */
  public interface OutputBuilder<KeyT, OutputT>
      extends Builders.Output<KV<KeyT, OutputT>>, Builders.OutputValues<KeyT, OutputT> {}

  /** Parameters of this operator used in builders. */
  static class Builder<LeftT, RightT, KeyT, OutputT>
      implements OfBuilder,
          ByBuilder<LeftT, RightT>,
          UsingBuilder<LeftT, RightT, KeyT>,
          WindowByBuilder<KeyT, OutputT>,
          TriggeredByBuilder<KeyT, OutputT>,
          AccumulationModeBuilder<KeyT, OutputT>,
          WindowedOutputBuilder<KeyT, OutputT>,
          OutputBuilder<KeyT, OutputT> {

    private final WindowBuilder<Object> windowBuilder = new WindowBuilder<>();

    private final @Nullable String name;
    private final Type type;
    private PCollection<LeftT> left;
    private PCollection<RightT> right;
    private UnaryFunction<LeftT, KeyT> leftKeyExtractor;
    private UnaryFunction<RightT, KeyT> rightKeyExtractor;
    private @Nullable TypeDescriptor<KeyT> keyType;
    private BinaryFunctor<LeftT, RightT, OutputT> joinFunc;
    private @Nullable TypeDescriptor<OutputT> outputType;

    Builder(@Nullable String name, Type type) {
      this.name = name;
      this.type = type;
    }

    @Override
    public <FirstT, SecondT> ByBuilder<FirstT, SecondT> of(
        PCollection<FirstT> left, PCollection<SecondT> right) {
      @SuppressWarnings("unchecked")
      final Builder<FirstT, SecondT, ?, ?> cast = (Builder) this;
      cast.left = requireNonNull(left);
      cast.right = requireNonNull(right);
      return cast;
    }

    @Override
    public <T> UsingBuilder<LeftT, RightT, T> by(
        UnaryFunction<LeftT, T> leftKeyExtractor,
        UnaryFunction<RightT, T> rightKeyExtractor,
        @Nullable TypeDescriptor<T> keyType) {
      @SuppressWarnings("unchecked")
      final Builder<LeftT, RightT, T, ?> cast = (Builder) this;
      cast.leftKeyExtractor = leftKeyExtractor;
      cast.rightKeyExtractor = rightKeyExtractor;
      cast.keyType = keyType;
      return cast;
    }

    @Override
    public <T> WindowByBuilder<KeyT, T> using(
        BinaryFunctor<LeftT, RightT, T> joinFunc, @Nullable TypeDescriptor<T> outputType) {
      @SuppressWarnings("unchecked")
      final Builder<LeftT, RightT, KeyT, T> cast = (Builder) this;
      cast.joinFunc = requireNonNull(joinFunc);
      cast.outputType = outputType;
      return cast;
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
      @SuppressWarnings("unchecked")
      final PCollectionList<Object> inputs =
          PCollectionList.of(Arrays.asList((PCollection) left, (PCollection) right));
      return OperatorTransform.apply(createOperator(), inputs);
    }

    @Override
    public PCollection<OutputT> outputValues(OutputHint... outputHints) {
      @SuppressWarnings("unchecked")
      final PCollectionList<Object> inputs =
          PCollectionList.of(Arrays.asList((PCollection) left, (PCollection) right));
      return OperatorTransform.apply(
          new OutputValues<>(name, outputType, createOperator()), inputs);
    }

    private Join<LeftT, RightT, KeyT, OutputT> createOperator() {
      return new Join<>(
          name,
          type,
          leftKeyExtractor,
          rightKeyExtractor,
          keyType,
          joinFunc,
          TypeDescriptors.kvs(
              TypeAwareness.orObjects(Optional.ofNullable(keyType)),
              TypeAwareness.orObjects(Optional.ofNullable(outputType))),
          windowBuilder.getWindow().orElse(null));
    }
  }

  private final Type type;
  private final UnaryFunction<LeftT, KeyT> leftKeyExtractor;
  private final UnaryFunction<RightT, KeyT> rightKeyExtractor;
  private final BinaryFunctor<LeftT, RightT, OutputT> functor;

  private Join(
      @Nullable String name,
      Type type,
      UnaryFunction<LeftT, KeyT> leftKeyExtractor,
      UnaryFunction<RightT, KeyT> rightKeyExtractor,
      @Nullable TypeDescriptor<KeyT> keyType,
      BinaryFunctor<LeftT, RightT, OutputT> functor,
      @Nullable TypeDescriptor<KV<KeyT, OutputT>> outputType,
      @Nullable Window<Object> window) {
    super(name, outputType, null, keyType, window);
    this.type = type;
    this.leftKeyExtractor = leftKeyExtractor;
    this.rightKeyExtractor = rightKeyExtractor;
    this.functor = functor;
  }

  public Type getType() {
    return type;
  }

  public UnaryFunction<LeftT, KeyT> getLeftKeyExtractor() {
    return leftKeyExtractor;
  }

  public UnaryFunction<RightT, KeyT> getRightKeyExtractor() {
    return rightKeyExtractor;
  }

  public BinaryFunctor<LeftT, RightT, OutputT> getJoiner() {
    return functor;
  }
}
