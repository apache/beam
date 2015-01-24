/*
 * Copyright (C) 2014 Google Inc.
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
 * {@code KvSwap<A, B>} takes a {@code PCollection<KV<A, B>>} and
 * returns a {@code PCollection<KV<B, A>>}, where all the keys and
 * values have been swapped.
 *
 * <p> Example of use:
 * <pre> {@code
 * PCollection<String, Long> wordsToCounts = ...;
 * PCollection<Long, String> countsToWords =
 *     wordToCounts.apply(KvSwap.<String, Long>create());
 * } </pre>
 *
 * <p> Each output element has the same timestamp and is in the same windows
 * as its corresponding input element, and the output {@code PCollection}
 * has the same
 * {@link com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn}
 * associated with it as the input.
 *
 * @param <A> the type of the keys in the input {@code PCollection}
 * and the values in the output {@code PCollection}
 * @param <B> the type of the values in the input {@code PCollection}
 * and the keys in the output {@code PCollection}
 */
@SuppressWarnings("serial")
public class KvSwap<A, B> extends PTransform<PCollection<KV<A, B>>,
                                             PCollection<KV<B, A>>> {
  /**
   * Returns a {@code KvSwap<A, B>} {@code PTransform}.
   *
   * @param <A> the type of the keys in the input {@code PCollection}
   * and the values in the output {@code PCollection}
   * @param <B> the type of the values in the input {@code PCollection}
   * and the keys in the output {@code PCollection}
   */
  public static <A, B> KvSwap<A, B> create() {
    return new KvSwap<>();
  }

  private KvSwap() { }

  @Override
  public PCollection<KV<B, A>> apply(PCollection<KV<A, B>> in) {
    return
        in.apply(ParDo.named("KvSwap")
                 .of(new DoFn<KV<A, B>, KV<B, A>>() {
                     @Override
                     public void processElement(ProcessContext c) {
                       KV<A, B> e = c.element();
                       c.output(KV.of(e.getValue(), e.getKey()));
                     }
                    }));
  }
}
