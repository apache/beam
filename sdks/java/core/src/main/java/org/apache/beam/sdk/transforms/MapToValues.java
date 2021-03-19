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
 * {@code KV<K, IV>}s and returns a {@code PCollection} of the values.
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
 * @param <IV> the type of the values in the input {@code PCollection}
 * @param <OV> the type of the elements in the output {@code PCollection}
 */
public class MapToValues<IV, OV> extends
    PTransform<PCollection<? extends KV<?, IV>>, PCollection<OV>> {

  private final SerializableFunction<IV, OV> fn;

  /**
   * Returns a {@code MapToValues<IV, OV>} {@code PTransform}.
   *
   * @param <IV> the type of the values in the input {@code PCollection}
   * @param <OV> the type of the elements in the output {@code PCollection}
   */
  public static <IV, OV> MapToValues<IV, OV> via(SerializableFunction<IV, OV> fn) {
    return new MapToValues<>(fn);
  }

  private MapToValues(SerializableFunction<IV, OV> fn) {
    this.fn = fn;
  }

  @Override
  public PCollection<OV> expand(PCollection<? extends KV<?, IV>> input) {
    return input.apply("MapToValues",
        MapElements.via(
            new SimpleFunction<KV<?, IV>, OV>() {
              @Override
              public OV apply(KV<?, IV> input) {
                return fn.apply(input.getValue());
              }
            }));
  }
}
