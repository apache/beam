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
 * {@code MapValues} maps a {@code SerializableFunction} over values
 * of a {@code PCollection} of {@code KV<K, IV>}s
 * and returns a {@code PCollection} of {@code KV<K, OV>}s.
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
 * @param <IV> the type of the values in the input {@code PCollection}
 * @param <OV> the type of the elements in the output {@code PCollection}
 */
public class MapValues<K, IV, OV> extends PTransform<PCollection<KV<K, IV>>, PCollection<KV<K, OV>>> {

    private final SerializableFunction<IV, OV> fn;

    /**
     * Returns a {@code MapValues<K, IV, OV>} {@code PTransform}.
     *
     * @param <K> the type of the keys in the input and output {@code PCollection}s
     * @param <IV> the type of the values in the input {@code PCollection}
     * @param <OV>  the type of the values in the output {@code PCollection}
     */
    public static <K, IV, OV> MapValues<K, IV, OV> via(SerializableFunction<IV, OV> fn) {
        return new MapValues<>(fn);
    }

    private MapValues(SerializableFunction<IV, OV> fn) {
        this.fn = fn;
    }

    @Override
    public PCollection<KV<K, OV>> expand(PCollection<KV<K, IV>> input) {
        return input.apply("MapValues",
                MapElements.via(
                        new SimpleFunction<KV<K, IV>, KV<K, OV>>() {
                            @Override
                            public KV<K, OV> apply(KV<K, IV> input) {
                                return KV.of(input.getKey(), fn.apply(input.getValue()));
                            }
                        }));
    }
}