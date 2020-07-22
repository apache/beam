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
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.BinaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Join.Type;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Right outer join of two input datasets producing single new dataset.
 *
 * <p>When joining two streams, the join has to specify windowing which groups elements from streams
 * into {@link org.apache.beam.sdk.transforms.windowing.Window}s. The join operation is performed
 * within same windows produced on left and right side of input {@link PCollection}s.
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
public class RightJoin {

  /**
   * Create builder.
   *
   * @param left dataset
   * @param right dataset
   * @param <LeftT> type of left dataset
   * @param <RightT> type of right dataset
   * @return ByBuilder
   */
  public static <LeftT, RightT> ByBuilder<LeftT, RightT> of(
      PCollection<LeftT> left, PCollection<RightT> right) {
    return named("RightJoin").of(left, right);
  }

  /**
   * Optional setter to give operator name.
   *
   * @param name of the operator
   * @return OfBuilder
   */
  public static OfBuilder named(String name) {
    return new Builder<>(name);
  }

  /** Builder for the 'of' step. */
  public interface OfBuilder {

    <LeftT, RightT> ByBuilder<LeftT, RightT> of(PCollection<LeftT> left, PCollection<RightT> right);
  }

  /** Builder for the 'by' step. */
  public interface ByBuilder<LeftT, RightT> {

    <KeyT> UsingBuilder<LeftT, RightT, KeyT> by(
        UnaryFunction<LeftT, KeyT> leftKeyExtractor,
        UnaryFunction<RightT, KeyT> rightKeyExtractor,
        @Nullable TypeDescriptor<KeyT> keyType);

    default <KeyT> UsingBuilder<LeftT, RightT, KeyT> by(
        UnaryFunction<LeftT, KeyT> leftKeyExtractor,
        UnaryFunction<RightT, KeyT> rightKeyExtractor) {
      return by(leftKeyExtractor, rightKeyExtractor, null);
    }
  }

  /** Builder for the 'using' step. */
  public interface UsingBuilder<LeftT, RightT, KeyT> {

    <OutputT> Join.WindowByBuilder<KeyT, OutputT> using(
        BinaryFunctor<Optional<LeftT>, RightT, OutputT> joinFunc,
        @Nullable TypeDescriptor<OutputT> outputType);

    default <OutputT> Join.WindowByBuilder<KeyT, OutputT> using(
        BinaryFunctor<Optional<LeftT>, RightT, OutputT> joinFunc) {
      return using(joinFunc, null);
    }
  }

  private static class Builder<LeftT, RightT, KeyT>
      implements OfBuilder, ByBuilder<LeftT, RightT>, UsingBuilder<LeftT, RightT, KeyT> {

    private final String name;
    private PCollection<LeftT> left;
    private PCollection<RightT> right;
    private UnaryFunction<LeftT, KeyT> leftKeyExtractor;
    private UnaryFunction<RightT, KeyT> rightKeyExtractor;
    @Nullable TypeDescriptor<KeyT> keyType;

    private Builder(String name) {
      this.name = name;
    }

    @Override
    public <FirstT, SecondT> ByBuilder<FirstT, SecondT> of(
        PCollection<FirstT> left, PCollection<SecondT> right) {
      @SuppressWarnings("unchecked")
      final Builder<FirstT, SecondT, ?> cast = (Builder) this;
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
      final Builder<LeftT, RightT, T> cast = (Builder) this;
      cast.leftKeyExtractor = requireNonNull(leftKeyExtractor);
      cast.rightKeyExtractor = requireNonNull(rightKeyExtractor);
      cast.keyType = keyType;
      return cast;
    }

    @Override
    public <OutputT> Join.WindowByBuilder<KeyT, OutputT> using(
        BinaryFunctor<Optional<LeftT>, RightT, OutputT> joinFunc,
        @Nullable TypeDescriptor<OutputT> outputType) {
      return new Join.Builder<>(name, Type.RIGHT)
          .of(left, right)
          .by(leftKeyExtractor, rightKeyExtractor, keyType)
          .using(
              (LeftT l, RightT r, Collector<OutputT> c) ->
                  joinFunc.apply(Optional.ofNullable(l), r, c),
              outputType);
    }
  }
}
