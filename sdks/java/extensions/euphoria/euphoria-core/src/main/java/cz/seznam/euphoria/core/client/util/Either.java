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
package cz.seznam.euphoria.core.client.util;

import static java.util.Objects.requireNonNull;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import javax.annotation.Nullable;

/** Either LEFT or RIGHT element. */
@Audience(Audience.Type.INTERNAL)
public final class Either<LEFT, RIGHT> {

  @Nullable final LEFT left;
  @Nullable final RIGHT right;

  private Either(@Nullable LEFT left, @Nullable RIGHT right) {
    this.left = left;
    this.right = right;
  }

  public static <LEFT, RIGHT> Either<LEFT, RIGHT> left(LEFT left) {
    requireNonNull(left);
    return new Either<>(left, (RIGHT) null);
  }

  public static <LEFT, RIGHT> Either<LEFT, RIGHT> right(RIGHT right) {
    requireNonNull(right);
    return new Either<>((LEFT) null, right);
  }

  public boolean isLeft() {
    return left != null;
  }

  public boolean isRight() {
    return right != null;
  }

  public LEFT left() {
    return left;
  }

  public RIGHT right() {
    return right;
  }

  @Override
  public String toString() {
    return "Either{" + "left=" + left + ", right=" + right + '}';
  }
}
