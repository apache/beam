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
 * {@code MapValues} maps a {@code SerializableFunction<V1,V2>} over values of a {@code
 * PCollection<KV<K,V1>>} and returns a {@code PCollection<KV<K, V2>>}.
 *
 * <p>Example of use:
 *
 * <pre>{@code
 * PCollection<KV<String, Integer>> input = ...;
 * PCollection<KV<String, Double> output =
 *      input.apply(MapValues.via(Integer::doubleValue));
 * }</pre>
 *
 * <p>See also {@link MapKeys}.
 *
 * @param <V1> the type of the values in the input {@code PCollection}
 * @param <V2> the type of the elements in the output {@code PCollection}
 */
public class MapValues<K, V1, V2>
    extends PTransform<PCollection<KV<K, V1>>, PCollection<KV<K, V2>>> {

  private final SerializableFunction<V1, V2> fn;

  /**
   * Returns a {@code MapValues<K, V1, V2>} {@code PTransform}.
   *
   * @param <K> the type of the keys in the input and output {@code PCollection}s
   * @param <V1> the type of the values in the input {@code PCollection}
   * @param <V2> the type of the values in the output {@code PCollection}
   */
  public static <K, V1, V2> MapValues<K, V1, V2> via(SerializableFunction<V1, V2> fn) {
    return new MapValues<>(fn);
  }

  private MapValues(SerializableFunction<V1, V2> fn) {
    this.fn = fn;
  }

  @Override
  public PCollection<KV<K, V2>> expand(PCollection<KV<K, V1>> input) {
    return input.apply(
        "MapValues",
        MapElements.via(
            new SimpleFunction<KV<K, V1>, KV<K, V2>>() {
              @Override
              public KV<K, V2> apply(KV<K, V1> input) {
                return KV.of(input.getKey(), fn.apply(input.getValue()));
              }
            }));
  }
}
