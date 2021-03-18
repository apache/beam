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
 * {@code MapKeys} maps a {@code SerializableFunction} over keys
 * of a {@code PCollection} of {@code KV<IK, V>}s
 * and returns a {@code PCollection} of {@code KV<OK, V>}s.
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
 * @param <IK> the type of the keys in the input {@code PCollection}
 * @param <OK> the type of the keys in the output {@code PCollection}
 */
public class MapKeys<IK, OK, V> extends PTransform<PCollection<KV<IK, V>>, PCollection<KV<OK, V>>> {

    private final SerializableFunction<IK, OK> fn;

    /**
     * Returns a {@code MapKeys<IK, OK, V>} {@code PTransform}.
     *
     * @param <IK> the type of the keys in the input {@code PCollection}
     * @param <OK> the type of the keys in the output {@code PCollection}
     * @param <V>  the type of the values in the input and output {@code PCollection}s
     */
    public static <IK, OK, V> MapKeys<IK, OK, V> via(SerializableFunction<IK, OK> fn) {
        return new MapKeys<>(fn);
    }

    private MapKeys(SerializableFunction<IK, OK> fn) {
        this.fn = fn;
    }

    @Override
    public PCollection<KV<OK, V>> expand(PCollection<KV<IK, V>> input) {
        return input.apply("MapKeys",
                MapElements.via(
                        new SimpleFunction<KV<IK, V>, KV<OK, V>>() {
                            @Override
                            public KV<OK, V> apply(KV<IK, V> input) {
                                return KV.of(fn.apply(input.getKey()), input.getValue());
                            }
                        }));
    }
}