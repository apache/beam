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

import cz.seznam.euphoria.core.annotation.audience.Audience;
import java.util.Objects;

/**
 * Triple of any types.
 */
@Audience(Audience.Type.CLIENT)
public final class Triple<F, S, T> {
  final F first;
  final S second;
  final T third;

  private Triple(F first, S second, T third) {
    this.first = first;
    this.second = second;
    this.third = third;
  }

  public static <F, S, T> Triple<F, S, T>of(F first, S second, T third) {
    return new Triple<>(first, second, third);
  }

  public F getFirst() {
    return first;
  }

  public S getSecond() {
    return second;
  }

  public T getThird() {
    return third;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof Triple) {
      Triple<?, ?, ?> triple = (Triple<?, ?, ?>) o;
      return Objects.equals(first, triple.first) &&
          Objects.equals(second, triple.second) &&
          Objects.equals(third, triple.third);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(first, second, third);
  }

  @Override
  public String toString() {
    return "Triple{" +
        "first=" + first +
        ", second=" + second +
        ", third=" + third +
        '}';
  }
}
