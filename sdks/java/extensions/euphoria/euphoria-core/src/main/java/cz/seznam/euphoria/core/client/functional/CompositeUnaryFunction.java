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
package cz.seznam.euphoria.core.client.functional;

import cz.seznam.euphoria.core.annotation.audience.Audience;

/**
 * A function that is composition of two unary functions.
 */
@Audience(Audience.Type.CLIENT)
public class CompositeUnaryFunction<IN, OUT, X> implements UnaryFunction<IN, OUT> {

  private final UnaryFunction<IN, X> first;
  private final UnaryFunction<X, OUT> second;

  public static <IN, OUT, X> CompositeUnaryFunction<IN, OUT, X> of(
      UnaryFunction<IN, X> first, UnaryFunction<X, OUT> second) {
    return new CompositeUnaryFunction<>(first, second);
  }

  private CompositeUnaryFunction(
      UnaryFunction<IN, X> first, UnaryFunction<X, OUT> second) {
    this.first = first;
    this.second = second;
  }

  @Override
  @SuppressWarnings("unchecked")
  public OUT apply(IN what) {
    return second.apply(first.apply(what));
  }

}
