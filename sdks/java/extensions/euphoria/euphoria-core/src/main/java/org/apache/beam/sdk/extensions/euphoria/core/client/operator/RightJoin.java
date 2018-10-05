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

import java.util.Objects;
import java.util.Optional;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Window;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Windowing;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.BinaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Join.BuilderParams;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Join.Type;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Right outer join of two input datasets producing single new dataset.
 *
 * <p>When joining two streams, the join has to specify {@link Windowing} which groups elements from
 * streams into {@link Window}s. The join operation is performed within same windows produced on
 * left and right side of input {@link Dataset}s.
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
      Dataset<LeftT> left, Dataset<RightT> right) {
    return new OfBuilder("RightJoin").of(left, right);
  }

  /**
   * Optional setter to give operator name.
   *
   * @param name of the operator
   * @return OfBuilder
   */
  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  /** RightJoin builder which adds input {@link Dataset} to operator under build. */
  public static class OfBuilder {

    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    public <LeftT, RightT> ByBuilder<LeftT, RightT> of(Dataset<LeftT> left, Dataset<RightT> right) {
      if (right.getFlow() != left.getFlow()) {
        throw new IllegalArgumentException("Pass inputs from the same flow");
      }

      final BuilderParams<LeftT, RightT, ?, ?, ?> params =
          new BuilderParams<>(
              Objects.requireNonNull(name),
              Objects.requireNonNull(left),
              Objects.requireNonNull(right),
              Type.RIGHT);

      return new ByBuilder<>(params);
    }
  }

  /** RightJoin builder which adds key extractors to operator under build. */
  public static class ByBuilder<LeftT, RightT> {

    private final BuilderParams<LeftT, RightT, ?, ?, ?> params;

    ByBuilder(BuilderParams<LeftT, RightT, ?, ?, ?> params) {
      this.params = params;
    }

    public <K> UsingBuilder<LeftT, RightT, K> by(
        UnaryFunction<LeftT, K> leftKeyExtractor,
        UnaryFunction<RightT, K> rightKeyExtractor,
        TypeDescriptor<K> keyType) {

      @SuppressWarnings("unchecked")
      BuilderParams<LeftT, RightT, K, ?, ?> paramsCasted =
          (BuilderParams<LeftT, RightT, K, ?, ?>) params;

      paramsCasted.leftKeyExtractor = Objects.requireNonNull(leftKeyExtractor);
      paramsCasted.rightKeyExtractor = Objects.requireNonNull(rightKeyExtractor);
      paramsCasted.keyType = keyType;
      return new UsingBuilder<>(paramsCasted);
    }

    public <K> UsingBuilder<LeftT, RightT, K> by(
        UnaryFunction<LeftT, K> leftKeyExtractor, UnaryFunction<RightT, K> rightKeyExtractor) {
      return by(leftKeyExtractor, rightKeyExtractor, null);
    }
  }

  /** RightJoin builder which adds join function to operator under build. */
  public static class UsingBuilder<LeftT, RightT, K> {

    private final BuilderParams<LeftT, RightT, K, ?, ?> params;

    UsingBuilder(BuilderParams<LeftT, RightT, K, ?, ?> params) {
      this.params = params;
    }

    public <OutputT> Join.WindowingBuilder<LeftT, RightT, K, OutputT> using(
        BinaryFunctor<Optional<LeftT>, RightT, OutputT> joinFunc,
        TypeDescriptor<OutputT> outputTypeDescriptor) {

      Objects.requireNonNull(joinFunc);

      @SuppressWarnings("unchecked")
      BuilderParams<LeftT, RightT, K, OutputT, ?> paramsCasted =
          (BuilderParams<LeftT, RightT, K, OutputT, ?>) params;

      paramsCasted.joinFunc =
          (left, right, context) -> joinFunc.apply(Optional.ofNullable(left), right, context);
      paramsCasted.outType = outputTypeDescriptor;

      return new Join.WindowingBuilder<>(paramsCasted);
    }

    public <OutputT> Join.WindowingBuilder<LeftT, RightT, K, OutputT> using(
        BinaryFunctor<Optional<LeftT>, RightT, OutputT> joinFunc) {

      return using(joinFunc, null);
    }
  }
}
