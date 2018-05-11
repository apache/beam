/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.BinaryFunctor;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import java.util.Objects;
import java.util.Optional;

/**
 * Full outer join of two input datasets producing single new dataset.
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
 *   <li>{@code [windowBy] ...............} windowing function (see {@link Windowing}), default
 *       attached windowing
 *   <li>{@code (output | outputValues) ..} build output dataset
 * </ol>
 */
@Audience(Audience.Type.CLIENT)
public class FullJoin {

  public static <LeftT, RightT> ByBuilder<LeftT, RightT> of(
      Dataset<LeftT> left, Dataset<RightT> right) {
    return new OfBuilder("RightJoin").of(left, right);
  }

  /** TODO: complete javadoc. */
  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  /** TODO: complete javadoc. */
  public static class OfBuilder {

    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    public <LeftT, RightT> ByBuilder<LeftT, RightT> of(Dataset<LeftT> left, Dataset<RightT> right) {
      if (right.getFlow() != left.getFlow()) {
        throw new IllegalArgumentException("Pass inputs from the same flow");
      }
      return new ByBuilder<>(name, left, right);
    }
  }

  /** TODO: complete javadoc. */
  public static class ByBuilder<LeftT, RightT> {

    private final String name;
    private final Dataset<LeftT> left;
    private final Dataset<RightT> right;

    ByBuilder(String name, Dataset<LeftT> left, Dataset<RightT> right) {
      this.name = Objects.requireNonNull(name);
      this.left = Objects.requireNonNull(left);
      this.right = Objects.requireNonNull(right);
    }

    public <K> UsingBuilder<LeftT, RightT, K> by(
        UnaryFunction<LeftT, K> leftKeyExtractor, UnaryFunction<RightT, K> rightKeyExtractor) {
      return new UsingBuilder<>(name, left, right, leftKeyExtractor, rightKeyExtractor);
    }
  }

  /** TODO: complete javadoc. */
  public static class UsingBuilder<LeftT, RightT, K> {

    private final String name;
    private final Dataset<LeftT> left;
    private final Dataset<RightT> right;
    private final UnaryFunction<LeftT, K> leftKeyExtractor;
    private final UnaryFunction<RightT, K> rightKeyExtractor;

    UsingBuilder(
        String name,
        Dataset<LeftT> left,
        Dataset<RightT> right,
        UnaryFunction<LeftT, K> leftKeyExtractor,
        UnaryFunction<RightT, K> rightKeyExtractor) {
      this.name = name;
      this.left = left;
      this.right = right;
      this.leftKeyExtractor = leftKeyExtractor;
      this.rightKeyExtractor = rightKeyExtractor;
    }

    public <OutputT> Join.WindowingBuilder<LeftT, RightT, K, OutputT> using(
        BinaryFunctor<Optional<LeftT>, Optional<RightT>, OutputT> functor) {
      final BinaryFunctor<LeftT, RightT, OutputT> wrappedFunctor =
          (left, right, context) ->
              functor.apply(Optional.ofNullable(left), Optional.ofNullable(right), context);
      return new Join.WindowingBuilder<>(
          name, left, right, leftKeyExtractor, rightKeyExtractor, wrappedFunctor, Join.Type.FULL);
    }
  }
}
