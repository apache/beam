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
package cz.seznam.euphoria.core.client.util;

import static java.util.Objects.requireNonNull;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import javax.annotation.Nullable;

/** Either LeftT or RightT element. */
@Audience(Audience.Type.INTERNAL)
public final class Either<LeftT, RightT> {

  @Nullable final LeftT left;
  @Nullable final RightT right;

  private Either(@Nullable LeftT left, @Nullable RightT right) {
    this.left = left;
    this.right = right;
  }

  public static <LeftT, RightT> Either<LeftT, RightT> left(LeftT left) {
    requireNonNull(left);
    return new Either<>(left, (RightT) null);
  }

  public static <LeftT, RightT> Either<LeftT, RightT> right(RightT right) {
    requireNonNull(right);
    return new Either<>((LeftT) null, right);
  }

  public boolean isLeft() {
    return left != null;
  }

  public boolean isRight() {
    return right != null;
  }

  public LeftT left() {
    return left;
  }

  public RightT right() {
    return right;
  }

  @Override
  public String toString() {
    return "Either{" + "left=" + left + ", right=" + right + '}';
  }
}
