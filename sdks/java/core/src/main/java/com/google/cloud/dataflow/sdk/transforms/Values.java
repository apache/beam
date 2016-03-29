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
 * {@code Values<V>} takes a {@code PCollection} of {@code KV<K, V>}s and
 * returns a {@code PCollection<V>} of the values.
 *
 * <p>Example of use:
 * <pre> {@code
 * PCollection<KV<String, Long>> wordCounts = ...;
 * PCollection<Long> counts = wordCounts.apply(Values.<String>create());
 * } </pre>
 *
 * <p>Each output element has the same timestamp and is in the same windows
 * as its corresponding input element, and the output {@code PCollection}
 * has the same
 * {@link com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn}
 * associated with it as the input.
 *
 * <p>See also {@link Keys}.
 *
 * @param <V> the type of the values in the input {@code PCollection},
 * and the type of the elements in the output {@code PCollection}
 */
public class Values<V> extends PTransform<PCollection<? extends KV<?, V>>,
                                          PCollection<V>> {
  /**
   * Returns a {@code Values<V>} {@code PTransform}.
   *
   * @param <V> the type of the values in the input {@code PCollection},
   * and the type of the elements in the output {@code PCollection}
   */
  public static <V> Values<V> create() {
    return new Values<>();
  }

  private Values() { }

  @Override
  public PCollection<V> apply(PCollection<? extends KV<?, V>> in) {
    return
        in.apply(ParDo.named("Values")
                 .of(new DoFn<KV<?, V>, V>() {
                     @Override
                     public void processElement(ProcessContext c) {
                       c.output(c.element().getValue());
                     }
                    }));
  }
}
