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
package org.apache.beam.sdk.transforms;

import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * {@code MapToValues} maps a {@code SerializableFunction} over values of a {@code PCollection} of
 * {@code KV<K, V1>}s and returns a {@code PCollection} of the values.
 *
 * <p>Example of use:
 *
 * <pre>{@code
 * PCollection<KV<String, Integer>> input = ...;
 * PCollection<Double> output =
 *      input.apply(MapToValues.via(Integer::doubleValue));
 * }</pre>
 *
 * <p>See also {@link MapToKeys}.
 *
 * @param <V1> the type of the values in the input {@code PCollection}
 * @param <V2> the type of the elements in the output {@code PCollection}
 */
public class MapToValues<V1, V2>
    extends PTransform<PCollection<? extends KV<?, V1>>, PCollection<V2>> {

  private final SerializableFunction<V1, V2> fn;

  /**
   * Returns a {@code MapToValues<V1, V2>} {@code PTransform}.
   *
   * @param <V1> the type of the values in the input {@code PCollection}
   * @param <V2> the type of the elements in the output {@code PCollection}
   */
  public static <V1, V2> MapToValues<V1, V2> via(SerializableFunction<V1, V2> fn) {
    return new MapToValues<>(fn);
  }

  private MapToValues(SerializableFunction<V1, V2> fn) {
    this.fn = fn;
  }

  @Override
  public PCollection<V2> expand(PCollection<? extends KV<?, V1>> input) {
    return input.apply(
        "MapToValues",
        MapElements.via(
            new SimpleFunction<KV<?, V1>, V2>() {
              @Override
              public V2 apply(KV<?, V1> input) {
                return fn.apply(input.getValue());
              }
            }));
  }
}
