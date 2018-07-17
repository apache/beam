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
 * {@code Keys<K>} takes a {@code PCollection} of {@code KV<K, V>}s and returns a {@code
 * PCollection<K>} of the keys.
 *
 * <p>Example of use:
 *
 * <pre>{@code
 * PCollection<KV<String, Long>> wordCounts = ...;
 * PCollection<String> words = wordCounts.apply(Keys.<String>create());
 * }</pre>
 *
 * <p>Each output element has the same timestamp and is in the same windows as its corresponding
 * input element, and the output {@code PCollection} has the same {@link
 * org.apache.beam.sdk.transforms.windowing.WindowFn} associated with it as the input.
 *
 * <p>See also {@link Values}.
 *
 * @param <K> the type of the keys in the input {@code PCollection}, and the type of the elements in
 *     the output {@code PCollection}
 */
public class Keys<K> extends PTransform<PCollection<? extends KV<K, ?>>, PCollection<K>> {
  /**
   * Returns a {@code Keys<K>} {@code PTransform}.
   *
   * @param <K> the type of the keys in the input {@code PCollection}, and the type of the elements
   *     in the output {@code PCollection}
   */
  public static <K> Keys<K> create() {
    return new Keys<>();
  }

  private Keys() {}

  @Override
  public PCollection<K> expand(PCollection<? extends KV<K, ?>> in) {
    return in.apply(
        "Keys",
        MapElements.via(
            new SimpleFunction<KV<K, ?>, K>() {
              @Override
              public K apply(KV<K, ?> kv) {
                return kv.getKey();
              }
            }));
  }
}
