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
 * {@code MapToKeys} maps a {@code SerializableFunction} over keys
 * of a {@code PCollection} of {@code KV<IK, V>}s
 * and returns a {@code PCollection} of the keys.
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
 * @param <IK> the type of the keys in the input {@code PCollection}
 * @param <OK> the type of the elements in the output {@code PCollection}
 */
public class MapToKeys<IK, OK> extends PTransform<PCollection<? extends KV<IK, ?>>, PCollection<OK>> {

    private final SerializableFunction<IK, OK> fn;

    /**
     * Returns a {@code MapToKeys<IK, OK>} {@code PTransform}.
     *
     * @param <IK> the type of the keys in the input {@code PCollection}
     * @param <OK> the type of the elements in the output {@code PCollection}
     */
    public static <IK, OK> MapToKeys<IK, OK> via(SerializableFunction<IK, OK> fn) {
        return new MapToKeys<>(fn);
    }

    private MapToKeys(SerializableFunction<IK, OK> fn) {
        this.fn = fn;
    }

    @Override
    public PCollection<OK> expand(PCollection<? extends KV<IK, ?>> input) {
        return input.apply("MapToKeys",
                MapElements.via(
                        new SimpleFunction<KV<IK, ?>, OK>() {
                            @Override
                            public OK apply(KV<IK, ?> input) {
                                return fn.apply(input.getKey());
                            }
                        }));
    }
}