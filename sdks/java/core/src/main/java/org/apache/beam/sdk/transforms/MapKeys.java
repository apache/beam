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
 * {@code MapKeys} maps a {@code SerializableFunction} over keys of a {@code PCollection} of {@code
 * KV<K1, V>}s and returns a {@code PCollection} of {@code KV<K2, V>}s.
 *
 * <p>Example of use:
 *
 * <pre>{@code
 * PCollection<KV<Integer, String>> input = ...;
 * PCollection<KV<Double, String> output =
 *      input.apply(MapKeys.via(Integer::doubleValue));
 * }</pre>
 *
 * <p>See also {@link MapValues}.
 *
 * @param <K1> the type of the keys in the input {@code PCollection}
 * @param <K2> the type of the keys in the output {@code PCollection}
 */
public class MapKeys<K1, K2, V> extends PTransform<PCollection<KV<K1, V>>, PCollection<KV<K2, V>>> {

  private final SerializableFunction<K1, K2> fn;

  /**
   * Returns a {@code MapKeys<K1, K2, V>} {@code PTransform}.
   *
   * @param <K1> the type of the keys in the input {@code PCollection}
   * @param <K2> the type of the keys in the output {@code PCollection}
   * @param <V> the type of the values in the input and output {@code PCollection}s
   */
  public static <K1, K2, V> MapKeys<K1, K2, V> via(SerializableFunction<K1, K2> fn) {
    return new MapKeys<>(fn);
  }

  private MapKeys(SerializableFunction<K1, K2> fn) {
    this.fn = fn;
  }

  @Override
  public PCollection<KV<K2, V>> expand(PCollection<KV<K1, V>> input) {
    return input.apply("MapKeys",
        MapElements.via(
            new SimpleFunction<KV<K1, V>, KV<K2, V>>() {
              @Override
              public KV<K2, V> apply(KV<K1, V> input) {
                return KV.of(fn.apply(input.getKey()), input.getValue());
              }
            }));
  }
}
