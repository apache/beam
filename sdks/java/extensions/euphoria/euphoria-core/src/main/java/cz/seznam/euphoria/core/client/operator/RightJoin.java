/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
import cz.seznam.euphoria.core.client.functional.BinaryFunctor;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;

import java.util.Objects;
import java.util.Optional;

@Audience(Audience.Type.CLIENT)
public class RightJoin {

  public static class OfBuilder {

    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    public <LEFT, RIGHT> ByBuilder<LEFT, RIGHT> of(Dataset<LEFT> left, Dataset<RIGHT> right) {
      if (right.getFlow() != left.getFlow()) {
        throw new IllegalArgumentException("Pass inputs from the same flow");
      }
      return new ByBuilder<>(name, left, right);
    }
  }

  public static class ByBuilder<LEFT, RIGHT> {

    private final String name;
    private final Dataset<LEFT> left;
    private final Dataset<RIGHT> right;

    ByBuilder(String name, Dataset<LEFT> left, Dataset<RIGHT> right) {
      this.name = Objects.requireNonNull(name);
      this.left = Objects.requireNonNull(left);
      this.right = Objects.requireNonNull(right);
    }

    public <KEY> UsingBuilder<LEFT, RIGHT, KEY> by(
        UnaryFunction<LEFT, KEY> leftKeyExtractor,
        UnaryFunction<RIGHT, KEY> rightKeyExtractor) {
      return new UsingBuilder<>(name, left, right, leftKeyExtractor, rightKeyExtractor);
    }
  }

  public static class UsingBuilder<LEFT, RIGHT, KEY> {

    private final String name;
    private final Dataset<LEFT> left;
    private final Dataset<RIGHT> right;
    private final UnaryFunction<LEFT, KEY> leftKeyExtractor;
    private final UnaryFunction<RIGHT, KEY> rightKeyExtractor;

    UsingBuilder(String name,
                 Dataset<LEFT> left,
                 Dataset<RIGHT> right,
                 UnaryFunction<LEFT, KEY> leftKeyExtractor,
                 UnaryFunction<RIGHT, KEY> rightKeyExtractor) {
      this.name = name;
      this.left = left;
      this.right = right;
      this.leftKeyExtractor = leftKeyExtractor;
      this.rightKeyExtractor = rightKeyExtractor;
    }

    public <OUT> Join.WindowingBuilder<LEFT, RIGHT, KEY, OUT> using(
        BinaryFunctor<Optional<LEFT>, RIGHT, OUT> functor) {
      final BinaryFunctor<LEFT, RIGHT, OUT> wrappedFunctor = (left, right, context) ->
          functor.apply(Optional.ofNullable(left), right, context);
      return new Join.WindowingBuilder<>(name, left, right,
          leftKeyExtractor, rightKeyExtractor, wrappedFunctor, Join.Type.RIGHT);
    }
  }

  public static <LEFT, RIGHT> ByBuilder<LEFT, RIGHT> of(
      Dataset<LEFT> left, Dataset<RIGHT> right) {
    return new OfBuilder("RightJoin").of(left, right);
  }

  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

}
