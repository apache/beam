/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.transforms;

import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

/**
 * {@code KvSwap<K, V>} takes a {@code PCollection<KV<K, V>>} and
 * returns a {@code PCollection<KV<V, K>>}, where all the keys and
 * values have been swapped.
 *
 * <p>Example of use:
 * <pre> {@code
 * PCollection<String, Long> wordsToCounts = ...;
 * PCollection<Long, String> countsToWords =
 *     wordToCounts.apply(KvSwap.<String, Long>create());
 * } </pre>
 *
 * <p>Each output element has the same timestamp and is in the same windows
 * as its corresponding input element, and the output {@code PCollection}
 * has the same
 * {@link com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn}
 * associated with it as the input.
 *
 * @param <K> the type of the keys in the input {@code PCollection}
 * and the values in the output {@code PCollection}
 * @param <V> the type of the values in the input {@code PCollection}
 * and the keys in the output {@code PCollection}
 */
public class KvSwap<K, V> extends PTransform<PCollection<KV<K, V>>,
                                             PCollection<KV<V, K>>> {
  /**
   * Returns a {@code KvSwap<K, V>} {@code PTransform}.
   *
   * @param <K> the type of the keys in the input {@code PCollection}
   * and the values in the output {@code PCollection}
   * @param <V> the type of the values in the input {@code PCollection}
   * and the keys in the output {@code PCollection}
   */
  public static <K, V> KvSwap<K, V> create() {
    return new KvSwap<>();
  }

  private KvSwap() { }

  @Override
  public PCollection<KV<V, K>> apply(PCollection<KV<K, V>> in) {
    return
        in.apply(ParDo.named("KvSwap")
                 .of(new DoFn<KV<K, V>, KV<V, K>>() {
                     @Override
                     public void processElement(ProcessContext c) {
                       KV<K, V> e = c.element();
                       c.output(KV.of(e.getValue(), e.getKey()));
                     }
                    }));
  }
}
