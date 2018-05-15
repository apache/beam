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
package cz.seznam.euphoria.core.client.functional;

import cz.seznam.euphoria.core.annotation.audience.Audience;

/** A function that is composition of two unary functions. */
@Audience(Audience.Type.CLIENT)
public class CompositeUnaryFunction<InputT, OutputT, X> implements UnaryFunction<InputT, OutputT> {

  private final UnaryFunction<InputT, X> first;
  private final UnaryFunction<X, OutputT> second;

  private CompositeUnaryFunction(UnaryFunction<InputT, X> first, UnaryFunction<X, OutputT> second) {
    this.first = first;
    this.second = second;
  }

  public static <InputT, OutputT, X> CompositeUnaryFunction<InputT, OutputT, X> of(
      UnaryFunction<InputT, X> first, UnaryFunction<X, OutputT> second) {
    return new CompositeUnaryFunction<>(first, second);
  }

  @Override
  @SuppressWarnings("unchecked")
  public OutputT apply(InputT what) {
    return second.apply(first.apply(what));
  }
}
