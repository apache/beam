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
 * {@code MapToKeys} maps a {@code SerializableFunction} over keys of a {@code PCollection} of
 * {@code KV<K1, V>}s and returns a {@code PCollection} of the keys.
 *
 * <p>Example of use:
 *
 * <pre>{@code
 * PCollection<KV<Integer, String>> input = ...;
 * PCollection<Double> output =
 *      input.apply(MapToKeys.via(Integer::doubleValue));
 * }</pre>
 *
 * <p>See also {@link MapToValues}.
 *
 * @param <K1> the type of the keys in the input {@code PCollection}
 * @param <K2> the type of the elements in the output {@code PCollection}
 */
public class MapToKeys<K1, K2> extends
    PTransform<PCollection<? extends KV<K1, ?>>, PCollection<K2>> {

  private final SerializableFunction<K1, K2> fn;

  /**
   * Returns a {@code MapToKeys<K1, K2>} {@code PTransform}.
   *
   * @param <K1> the type of the keys in the input {@code PCollection}
   * @param <K2> the type of the elements in the output {@code PCollection}
   */
  public static <K1, K2> MapToKeys<K1, K2> via(SerializableFunction<K1, K2> fn) {
    return new MapToKeys<>(fn);
  }

  private MapToKeys(SerializableFunction<K1, K2> fn) {
    this.fn = fn;
  }

  @Override
  public PCollection<K2> expand(PCollection<? extends KV<K1, ?>> input) {
    return input.apply("MapToKeys",
        MapElements.via(
            new SimpleFunction<KV<K1, ?>, K2>() {
              @Override
              public K2 apply(KV<K1, ?> input) {
                return fn.apply(input.getKey());
              }
            }));
  }
}
