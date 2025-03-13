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
package org.apache.beam.sdk.extensions.euphoria.core.client.functional;

import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;

/**
 * Reduce function reducing iterable of elements into multiple elements (of possibly different
 * type).
 *
 * @deprecated Use Java SDK directly, Euphoria is scheduled for removal in a future release.
 */
@Audience(Audience.Type.CLIENT)
@FunctionalInterface
@Deprecated
public interface ReduceFunctor<InputT, OutputT> extends UnaryFunctor<Stream<InputT>, OutputT> {

  /**
   * Create reduce functor from combinable function.
   *
   * @param combinableFunction combinable function
   * @param <V> value type
   * @return reduce functor
   */
  static <V> ReduceFunctor<V, V> of(CombinableReduceFunction<V> combinableFunction) {

    return new ReduceFunctor<V, V>() {

      @Override
      public boolean isCombinable() {
        return true;
      }

      @Override
      public void apply(Stream<V> elem, Collector<V> context) {
        context.collect(combinableFunction.apply(elem));
      }
    };
  }

  /**
   * Is this a commutative associative function with single final output?
   *
   * @return {@code true} if this is combinable function. NOTE: user code should not need to
   *     override this
   */
  default boolean isCombinable() {
    return false;
  }
}
